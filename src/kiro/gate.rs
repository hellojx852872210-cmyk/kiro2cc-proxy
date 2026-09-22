// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 单账号在飞令牌桶 + 单账号退避 + 全局 429 冷却。
//!
//! 默认值可由 Admin「设置」热更新；0 个在飞上限表示不限制。

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::Stream;
use futures::stream::{BoxStream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::model::concurrency::{ConcurrencySettings, account_backoff_ms};

/// Instant 加法上限保护，避免 `now + Duration::from_secs(u64::MAX)` panic。
fn saturating_add_instant(now: Instant, d: Duration) -> Instant {
    now.checked_add(d).unwrap_or(now)
}

struct AccountState {
    inflight: u32,
    consecutive_failures: u32,
    backoff_until: Option<Instant>,
    /// 每次 429/suspended 递增。成功只在 started_gen 仍等于当前值时清退避。
    throttle_gen: u64,
}

struct AccountSlot {
    notify: Notify,
    state: Mutex<AccountState>,
}

impl AccountSlot {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            state: Mutex::new(AccountState {
                inflight: 0,
                consecutive_failures: 0,
                backoff_until: None,
                throttle_gen: 0,
            }),
        }
    }
}

struct GlobalState {
    level: u32,
    last_429: Option<Instant>,
    until: Option<Instant>,
}

/// 设置与全局冷却必须同一把锁，避免「读到开启 → 关闭清空 → 迟到 429 又写回」。
struct GateInner {
    settings: ConcurrencySettings,
    global: GlobalState,
    accounts: HashMap<u64, Arc<AccountSlot>>,
}

enum Admit {
    Taken(u64),
    WaitGlobal(Instant),
    WaitAccount(Instant),
    Full,
}

/// 进程内闸：设置可热更新。
pub struct ConcurrencyGate {
    inner: Mutex<GateInner>,
    global_notify: Notify,
}

impl ConcurrencyGate {
    pub fn new(settings: ConcurrencySettings) -> Self {
        Self {
            inner: Mutex::new(GateInner {
                settings: settings.sanitize(),
                global: GlobalState {
                    level: 0,
                    last_429: None,
                    until: None,
                },
                accounts: HashMap::new(),
            }),
            global_notify: Notify::new(),
        }
    }

    pub fn settings(&self) -> ConcurrencySettings {
        self.inner.lock().settings.clone()
    }

    pub fn update_settings(&self, settings: ConcurrencySettings) {
        let s = settings.sanitize();
        let enabled = s.global_cooldown_enabled;
        let mut inner = self.inner.lock();
        inner.settings = s;
        if !enabled {
            inner.global.until = None;
            inner.global.level = 0;
        }
        let slots: Vec<Arc<AccountSlot>> = inner.accounts.values().cloned().collect();
        drop(inner);
        self.global_notify.notify_waiters();
        for slot in slots {
            slot.notify.notify_waiters();
        }
    }

    fn slot(&self, cred_id: u64) -> Arc<AccountSlot> {
        self.inner
            .lock()
            .accounts
            .entry(cred_id)
            .or_insert_with(|| Arc::new(AccountSlot::new()))
            .clone()
    }

    /// 设置、全局窗、账号窗、容量、计数、代次：一次锁序判断。
    fn try_admit(&self, slot: &AccountSlot) -> Admit {
        let inner = self.inner.lock();
        let mut st = slot.state.lock();
        let now = Instant::now();
        if inner.settings.global_cooldown_enabled {
            if let Some(until) = inner.global.until {
                if now < until {
                    return Admit::WaitGlobal(until);
                }
            }
        }
        if let Some(until) = st.backoff_until {
            if now < until {
                return Admit::WaitAccount(until);
            }
        }
        let max = inner.settings.max_inflight_per_credential;
        if max != 0 && st.inflight >= max {
            return Admit::Full;
        }
        st.inflight = st.inflight.saturating_add(1);
        Admit::Taken(st.throttle_gen)
    }

    /// 等到全局冷却结束。关开关立即放行。
    pub async fn wait_global(&self) {
        self.admit_wait_only_global().await;
    }

    async fn admit_wait_only_global(&self) {
        loop {
            let notified = self.global_notify.notified();
            tokio::pin!(notified);
            let wait_for = {
                let inner = self.inner.lock();
                if !inner.settings.global_cooldown_enabled {
                    return;
                }
                match inner.global.until {
                    Some(until) if Instant::now() < until => {
                        Some(until.saturating_duration_since(Instant::now()))
                    }
                    _ => None,
                }
            };
            match wait_for {
                None => return,
                Some(d) if d.is_zero() => return,
                Some(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = notified => {}
                    }
                }
            }
        }
    }

    /// 等到该账号退避窗结束。
    pub async fn wait_account_backoff(&self, cred_id: u64) {
        let slot = self.slot(cred_id);
        loop {
            let notified = slot.notify.notified();
            tokio::pin!(notified);
            let wait_for = {
                let st = slot.state.lock();
                match st.backoff_until {
                    Some(t) if Instant::now() < t => Some(t.saturating_duration_since(Instant::now())),
                    _ => None,
                }
            };
            match wait_for {
                None => return,
                Some(d) if d.is_zero() => return,
                Some(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = notified => {}
                    }
                }
            }
        }
    }

    /// 一次锁序同时判断冷却、退避、容量。等待时不持锁、不持令牌。
    pub async fn admit(&self, cred_id: u64) -> InFlightGuard {
        let slot = self.slot(cred_id);
        loop {
            let n_slot = slot.notify.notified();
            let n_global = self.global_notify.notified();
            tokio::pin!(n_slot);
            tokio::pin!(n_global);
            match self.try_admit(&slot) {
                Admit::Taken(started_gen) => {
                    return InFlightGuard {
                        slot: slot.clone(),
                        started_gen,
                        released: AtomicBool::new(false),
                    };
                }
                Admit::WaitGlobal(until) => {
                    let d = until.saturating_duration_since(Instant::now());
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = n_global => {}
                        _ = n_slot => {}
                    }
                }
                Admit::WaitAccount(until) => {
                    let d = until.saturating_duration_since(Instant::now());
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = n_slot => {}
                        _ = n_global => {}
                    }
                }
                Admit::Full => {
                    n_slot.await;
                }
            }
        }
    }

    /// 领取 1 个在飞令牌（不含冷却/退避）。满了就等到有人归还。
    pub async fn acquire(&self, cred_id: u64) -> InFlightGuard {
        self.admit(cred_id).await
    }

    pub fn on_success(&self, cred_id: u64) {
        let slot = self.slot(cred_id);
        let mut st = slot.state.lock();
        st.consecutive_failures = 0;
        st.backoff_until = None;
        drop(st);
        slot.notify.notify_waiters();
    }

    /// 仅当该请求开始之后没有更新的 429 时，才清退避。
    pub fn on_success_gen(&self, cred_id: u64, started_gen: u64) {
        let slot = self.slot(cred_id);
        let mut st = slot.state.lock();
        if st.throttle_gen == started_gen {
            st.consecutive_failures = 0;
            st.backoff_until = None;
        }
        drop(st);
        slot.notify.notify_waiters();
    }

    /// 账号 429 或 suspended：同一把锁更新失败次数与截止时间。
    pub fn on_account_throttle(&self, cred_id: u64, suspended: bool) {
        let slot = self.slot(cred_id);
        let inner = self.inner.lock();
        let settings = inner.settings.clone();
        let mut st = slot.state.lock();
        st.throttle_gen = st.throttle_gen.saturating_add(1);
        let delay = if suspended {
            Duration::from_millis(settings.suspended_backoff_ms.max(1))
        } else {
            st.consecutive_failures = st.consecutive_failures.saturating_add(1);
            Duration::from_millis(account_backoff_ms(st.consecutive_failures, &settings))
        };
        let until = saturating_add_instant(Instant::now(), delay);
        st.backoff_until = Some(match st.backoff_until {
            Some(prev) if prev > until => prev,
            _ => until,
        });
        drop(st);
        slot.notify.notify_waiters();
        tracing::info!(
            credential = cred_id,
            suspended,
            delay_ms = delay.as_millis() as u64,
            "[GATE] 账号退避"
        );
    }

    /// 任意 429 触发全局冷却。冷却只延长、不缩短。关开关时不会写入。
    pub fn on_global_429(&self) {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        if !inner.settings.global_cooldown_enabled {
            return;
        }
        let settings = inner.settings.clone();
        if let Some(last) = inner.global.last_429 {
            if now.duration_since(last) >= Duration::from_secs(settings.global_reset_idle_secs) {
                inner.global.level = 0;
            }
        }
        inner.global.level = inner.global.level.saturating_add(1);
        inner.global.last_429 = Some(now);
        let pause = match inner.global.level {
            1 => Duration::from_secs(settings.global_first_pause_secs),
            2 => Duration::from_secs(settings.global_second_pause_secs),
            _ => {
                let lo = settings.global_third_pause_min_secs;
                let hi = settings.global_third_pause_max_secs.max(lo);
                let extra = if hi > lo {
                    fastrand::u64(0..=(hi - lo))
                } else {
                    0
                };
                Duration::from_secs(lo + extra)
            }
        };
        let until = saturating_add_instant(now, pause);
        inner.global.until = Some(match inner.global.until {
            Some(prev) if prev > until => prev,
            _ => until,
        });
        let level = inner.global.level;
        drop(inner);
        self.global_notify.notify_waiters();
        tracing::warn!(
            level,
            pause_ms = pause.as_millis() as u64,
            "[GATE] 全局 429 冷却"
        );
    }
}

/// 在飞令牌。Drop 时归还，不能超额领取。
pub struct InFlightGuard {
    slot: Arc<AccountSlot>,
    started_gen: u64,
    released: AtomicBool,
}

impl InFlightGuard {
    pub fn noop() -> Self {
        Self {
            slot: Arc::new(AccountSlot::new()),
            started_gen: 0,
            released: AtomicBool::new(true),
        }
    }

    pub fn started_gen(&self) -> u64 {
        self.started_gen
    }

    pub fn mark_http_ok(&self, gate: &ConcurrencyGate, cred_id: u64) {
        gate.on_success_gen(cred_id, self.started_gen);
    }

    fn release(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let mut st = self.slot.state.lock();
            if st.inflight > 0 {
                st.inflight -= 1;
            }
            drop(st);
            self.slot.notify.notify_waiters();
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.release();
    }
}

/// 把在飞令牌绑在字节流上：EOF 或 Drop 时归还。
pub fn hold_bytes_stream(
    response: reqwest::Response,
    guard: InFlightGuard,
) -> BoxStream<'static, Result<bytes::Bytes, reqwest::Error>> {
    struct Hold<S> {
        inner: S,
        guard: InFlightGuard,
    }
    impl<S: Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin> Stream for Hold<S> {
        type Item = Result<bytes::Bytes, reqwest::Error>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(None) => {
                    self.guard.release();
                    Poll::Ready(None)
                }
                other => other,
            }
        }
    }
    Hold {
        inner: response.bytes_stream(),
        guard,
    }
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inflight_of(gate: &ConcurrencyGate, id: u64) -> u32 {
        gate.slot(id).state.lock().inflight
    }

    #[tokio::test]
    async fn inflight_cap_five() {
        let gate = ConcurrencyGate::new(ConcurrencySettings::default());
        let mut guards = Vec::new();
        for _ in 0..5 {
            guards.push(gate.admit(1).await);
        }
        assert_eq!(inflight_of(&gate, 1), 5);
        drop(guards);
        assert_eq!(inflight_of(&gate, 1), 0);
    }

    #[tokio::test]
    async fn waiter_wakes_when_token_returned() {
        let gate = Arc::new(ConcurrencyGate::new(ConcurrencySettings {
            max_inflight_per_credential: 1,
            ..Default::default()
        }));
        let g1 = gate.admit(7).await;
        let g = gate.clone();
        let waiter = tokio::spawn(async move { g.admit(7).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(g1);
        let g2 = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter timed out")
            .expect("join");
        drop(g2);
    }

    #[tokio::test]
    async fn disable_cooldown_unblocks() {
        let gate = ConcurrencyGate::new(ConcurrencySettings::default());
        gate.on_global_429();
        let mut s = gate.settings();
        s.global_cooldown_enabled = false;
        gate.update_settings(s);
        tokio::time::timeout(Duration::from_millis(200), gate.wait_global())
            .await
            .expect("wait_global must return after disable");
    }

    #[tokio::test]
    async fn later_success_does_not_clear_newer_backoff() {
        let gate = ConcurrencyGate::new(ConcurrencySettings::default());
        let g = gate.admit(3).await;
        let gen0 = g.started_gen();
        gate.on_account_throttle(3, false);
        gate.on_success_gen(3, gen0);
        let until = gate.slot(3).state.lock().backoff_until;
        assert!(until.is_some(), "old success must not clear newer backoff");
    }

    #[test]
    fn sanitize_rejects_huge_durations() {
        let s = ConcurrencySettings {
            global_first_pause_secs: u64::MAX,
            backoff_base_ms: u64::MAX,
            ..Default::default()
        }
        .sanitize();
        assert!(s.global_first_pause_secs <= 3600);
        assert!(s.backoff_base_ms <= 3_600_000);
        let now = Instant::now();
        let d = Duration::from_secs(s.global_first_pause_secs);
        let _ = saturating_add_instant(now, d);
    }
}
