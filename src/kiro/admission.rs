// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 准入票与双许可 try-acquire。
//!
//! 准入票在 token 之前非阻塞获取，覆盖建连/重试准备；成功返回 LeasedResponse 时归还。
//! 活跃流许可用 try-acquire 组合，不能持一个等另一个。

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::error::RateLimitError;

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

/// 同时尝试全局与账号许可；一个失败立即释放另一个。
pub fn try_acquire_pair(
    global: &Arc<Semaphore>,
    account: &Arc<Semaphore>,
) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
    let g = Arc::clone(global).try_acquire_owned().ok()?;
    match Arc::clone(account).try_acquire_owned() {
        Ok(a) => Some((g, a)),
        Err(_) => {
            drop(g);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let global = Arc::new(Semaphore::new(1));
        let account = Arc::new(Semaphore::new(0));
        assert!(try_acquire_pair(&global, &account).is_none());
        assert_eq!(global.available_permits(), 1);
        let account = Arc::new(Semaphore::new(1));
        let pair = try_acquire_pair(&global, &account);
        assert!(pair.is_some());
        assert_eq!(global.available_permits(), 0);
        drop(pair);
        assert_eq!(global.available_permits(), 1);
        assert_eq!(account.available_permits(), 1);
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
