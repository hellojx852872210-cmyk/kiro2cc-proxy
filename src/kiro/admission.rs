// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 准入票与双许可 try-acquire。
//!
//! 准入票在 token 之前非阻塞获取，覆盖建连/重试准备；成功返回 LeasedResponse 时归还。
//! 活跃流许可用 try-acquire 组合，不能持一个等另一个。

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::error::RateLimitError;
use super::gate::{AdmitBlocked, ConcurrencyGate, InFlightGuard};

pub struct AdmissionGate {
    tickets: Arc<Semaphore>,
    timeout: Duration,
}

pub struct AdmissionTicket {
    _permit: OwnedSemaphorePermit,
    pub deadline: Instant,
}

impl AdmissionGate {
    pub fn new(max_waiters: usize, timeout: Duration) -> Self {
        Self {
            tickets: Arc::new(Semaphore::new(max_waiters.max(1))),
            timeout,
        }
    }

    /// 非阻塞获取准入票。满员立即 local busy。
    pub fn try_enter(&self) -> Result<AdmissionTicket, RateLimitError> {
        match Arc::clone(&self.tickets).try_acquire_owned() {
            Ok(permit) => Ok(AdmissionTicket {
                _permit: permit,
                deadline: Instant::now() + self.timeout,
            }),
            Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => {
                Err(RateLimitError::local_busy(self.timeout))
            }
        }
    }

    #[cfg(test)]
    pub fn available(&self) -> usize {
        self.tickets.available_permits()
    }
}

impl AdmissionTicket {
    pub fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

/// 组合获取失败原因。等待前两者都不得持有。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamBlocked {
    GlobalCooldown(Instant),
    AccountBackoff(Instant),
    AccountFull,
    GlobalFull,
}

impl From<AdmitBlocked> for StreamBlocked {
    fn from(value: AdmitBlocked) -> Self {
        match value {
            AdmitBlocked::Global(until) => StreamBlocked::GlobalCooldown(until),
            AdmitBlocked::Account(until) => StreamBlocked::AccountBackoff(until),
            AdmitBlocked::Full => StreamBlocked::AccountFull,
        }
    }
}

impl StreamBlocked {
    #[allow(dead_code)]
    pub fn until(&self) -> Option<Instant> {
        match self {
            StreamBlocked::GlobalCooldown(t) | StreamBlocked::AccountBackoff(t) => Some(*t),
            StreamBlocked::AccountFull | StreamBlocked::GlobalFull => None,
        }
    }

    /// 仅账号满/退避时可改试其他合格账号；全局满或全局冷却约束全池。
    pub fn try_other_accounts(&self) -> bool {
        matches!(
            self,
            StreamBlocked::AccountFull | StreamBlocked::AccountBackoff(_)
        )
    }
}

/// 同时尝试全局许可与 gate 账号令牌；一个失败立即释放另一个。无 await。
/// 先 global.try_acquire 再 gate.try_admit：GlobalFull 不碰账号计数，避免回滚互唤醒。
pub fn try_acquire_stream(
    global: &Arc<Semaphore>,
    global_release: &tokio::sync::Notify,
    gate: &ConcurrencyGate,
    cred_id: u64,
) -> Result<(OwnedSemaphorePermit, InFlightGuard), StreamBlocked> {
    let permit = match Arc::clone(global).try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return Err(StreamBlocked::GlobalFull),
    };
    match gate.try_admit(cred_id) {
        Ok(guard) => Ok((permit, guard)),
        Err(blocked) => {
            drop(permit);
            global_release.notify_waiters();
            Err(StreamBlocked::from(blocked))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::concurrency::ConcurrencySettings;

    #[test]
    fn ticket_nonblocking_and_drop_releases() {
        let gate = AdmissionGate::new(1, Duration::from_secs(5));
        let t1 = gate.try_enter().unwrap();
        assert_eq!(gate.available(), 0);
        assert!(gate.try_enter().is_err());
        drop(t1);
        assert_eq!(gate.available(), 1);
        assert!(gate.try_enter().is_ok());
    }

    #[tokio::test]
    async fn pair_does_not_hold_one_waiting_for_the_other() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let global = Arc::new(Semaphore::new(0));
        let gate = ConcurrencyGate::new(ConcurrencySettings {
            max_inflight_per_credential: 1,
            global_cooldown_enabled: false,
            ..Default::default()
        });
        assert!(matches!(
            try_acquire_stream(&global, &notify, &gate, 1),
            Err(StreamBlocked::GlobalFull)
        ));
        assert_eq!(gate.inflight(1), 0);

        let global = Arc::new(Semaphore::new(1));
        let g1 = gate.try_admit(1).unwrap();
        assert!(matches!(
            try_acquire_stream(&global, &notify, &gate, 1),
            Err(StreamBlocked::AccountFull)
        ));
        assert_eq!(global.available_permits(), 1);
        drop(g1);

        let pair = try_acquire_stream(&global, &notify, &gate, 1);
        assert!(pair.is_ok());
        assert_eq!(global.available_permits(), 0);
        drop(pair);
        assert_eq!(global.available_permits(), 1);
        assert_eq!(gate.inflight(1), 0);
    }

    #[test]
    fn deadline_from_entry_not_reset_by_construction() {
        let gate = AdmissionGate::new(2, Duration::from_millis(50));
        let t = gate.try_enter().unwrap();
        std::thread::sleep(Duration::from_millis(60));
        assert!(t.expired());
        assert!(t.remaining().is_zero());
    }
}
