// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 类型化限流错误与 Retry-After 解析。
//!
//! 不携带凭据或上游正文给客户。

use chrono::{DateTime, Utc};
use reqwest::header::HeaderValue;
use std::fmt;
use std::time::{Duration, Instant};

/// 限流来源
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitKind {
    Upstream,
    Rpm,
    LocalBusy,
    Refresh,
}

/// 类型化 429。可 downcast，handler 据此写出安全 Retry-After。
#[derive(Debug, Clone)]
pub struct RateLimitError {
    pub kind: RateLimitKind,
    _retry_after: Option<Duration>,
    wait_until: Instant,
}

impl RateLimitError {
    pub fn new(kind: RateLimitKind, retry_after: Option<Duration>) -> Self {
        let wait = ceil_secs(retry_after.unwrap_or(Duration::from_secs(5)));
        Self {
            kind,
            _retry_after: retry_after,
            wait_until: saturating_deadline(wait),
        }
    }

    pub fn at(kind: RateLimitKind, wait_until: Instant) -> Self {
        Self {
            kind,
            _retry_after: None,
            wait_until,
        }
    }

    pub fn upstream(retry_after: Option<Duration>) -> Self {
        let wait = retry_after.unwrap_or(Duration::from_secs(5));
        Self {
            kind: RateLimitKind::Upstream,
            _retry_after: retry_after,
            wait_until: saturating_deadline(wait),
        }
    }

    pub fn rpm(wait: Duration) -> Self {
        Self::new(RateLimitKind::Rpm, Some(wait))
    }

    pub fn local_busy(wait: Duration) -> Self {
        Self::new(RateLimitKind::LocalBusy, Some(wait))
    }

    pub fn refresh(retry_after: Option<Duration>) -> Self {
        let wait = retry_after.unwrap_or(Duration::from_secs(5));
        Self {
            kind: RateLimitKind::Refresh,
            _retry_after: retry_after,
            wait_until: saturating_deadline(wait),
        }
    }

    pub fn wait_until(&self) -> Instant {
        self.wait_until
    }

    pub fn keep_earliest(slot: &mut Option<Self>, new: Self) {
        match slot {
            Some(old) if old.wait_until <= new.wait_until => {}
            _ => *slot = Some(new),
        }
    }

    /// 只收录真实限流（upstream/RPM/refresh），不把 local_busy 写进历史。
    pub fn keep_earliest_real(slot: &mut Option<Self>, new: Self) {
        if new.kind == RateLimitKind::LocalBusy {
            return;
        }
        Self::keep_earliest(slot, new);
    }

    /// 剩余等待秒数（向上取整，至少 1）
    pub fn retry_after_secs(&self) -> u64 {
        ceil_secs(self.wait_until.saturating_duration_since(Instant::now())).as_secs()
    }

    pub fn retry_after_header(&self) -> HeaderValue {
        HeaderValue::from_str(&self.retry_after_secs().to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("1"))
    }
}

impl fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.kind {
            RateLimitKind::Upstream => "upstream",
            RateLimitKind::Rpm => "rpm",
            RateLimitKind::LocalBusy => "local_busy",
            RateLimitKind::Refresh => "refresh",
        };
        write!(
            f,
            "429 Too Many Requests ({kind}), retry after {}s",
            self.retry_after_secs()
        )
    }
}

impl std::error::Error for RateLimitError {}

/// 解析 HTTP Retry-After。支持整数秒与 IMF-fixdate；非法/负数/溢出丢弃。
pub fn parse_retry_after(value: Option<&HeaderValue>) -> Option<Duration> {
    let raw = value?.to_str().ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.starts_with('-') {
        return None;
    }
    if let Ok(secs) = raw.parse::<u64>() {
        return acceptable_wait(Duration::from_secs(secs));
    }
    if let Ok(secs) = raw.parse::<i64>() {
        if secs < 0 {
            return None;
        }
        return acceptable_wait(Duration::from_secs(secs as u64));
    }
    let dt = DateTime::parse_from_rfc2822(raw).ok()?;
    let delta = dt.with_timezone(&Utc).signed_duration_since(Utc::now());
    match delta.to_std() {
        Ok(d) => acceptable_wait(d),
        Err(_) => Some(Duration::ZERO),
    }
}

fn acceptable_wait(d: Duration) -> Option<Duration> {
    Instant::now().checked_add(d).map(|_| d)
}

fn saturating_deadline(wait: Duration) -> Instant {
    Instant::now()
        .checked_add(wait)
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(5))
}

pub fn parse_retry_after_from_headers(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    parse_retry_after(headers.get(reqwest::header::RETRY_AFTER))
}

fn ceil_secs(d: Duration) -> Duration {
    if d.is_zero() {
        return Duration::from_secs(1);
    }
    let secs = d.as_secs();
    if d.subsec_nanos() > 0 {
        Duration::from_secs(secs.saturating_add(1).max(1))
    } else {
        Duration::from_secs(secs.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hv(s: &str) -> HeaderValue {
        HeaderValue::from_str(s).unwrap()
    }

    #[test]
    fn parse_integer_seconds() {
        assert_eq!(
            parse_retry_after(Some(&hv("37"))),
            Some(Duration::from_secs(37))
        );
        assert_eq!(parse_retry_after(Some(&hv("0"))), Some(Duration::ZERO));
    }

    #[test]
    fn parse_http_date_future() {
        let future = Utc::now() + chrono::Duration::seconds(90);
        let formatted = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let parsed = parse_retry_after(Some(&hv(&formatted))).unwrap();
        assert!(
            parsed >= Duration::from_secs(80) && parsed <= Duration::from_secs(100),
            "got {parsed:?} from {formatted}"
        );
    }

    #[test]
    fn parse_rejects_invalid_negative_overflow() {
        assert_eq!(parse_retry_after(Some(&hv("-3"))), None);
        assert_eq!(parse_retry_after(Some(&hv("nope"))), None);
        assert_eq!(parse_retry_after(Some(&hv("99999999999999999999"))), None);
        assert_eq!(parse_retry_after(None), None);
    }

    #[test]
    fn parse_u64_max_is_invalid_and_constructors_do_not_panic() {
        assert_eq!(parse_retry_after(Some(&hv(&u64::MAX.to_string()))), None);
        let _ = RateLimitError::upstream(Some(Duration::from_secs(u64::MAX)));
        let _ = RateLimitError::refresh(Some(Duration::from_secs(u64::MAX)));
        let _ = RateLimitError::new(
            RateLimitKind::LocalBusy,
            Some(Duration::from_secs(u64::MAX)),
        );
    }

    #[test]
    fn zero_retry_after_is_valid_and_client_header_is_at_least_one() {
        assert_eq!(parse_retry_after(Some(&hv("0"))), Some(Duration::ZERO));
        let err = RateLimitError::upstream(Some(Duration::ZERO));
        assert_eq!(err.retry_after_secs(), 1);
        assert!(err.wait_until() <= Instant::now() + Duration::from_millis(50));
    }

    #[test]
    fn http_date_keeps_subsecond_and_does_not_floor_to_zero() {
        let future = Utc::now() + chrono::Duration::milliseconds(1800);
        let formatted = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let parsed = parse_retry_after(Some(&hv(&formatted))).unwrap();
        assert!(
            parsed >= Duration::from_millis(500),
            "HTTP-date 不得向下取整提前解封, got {parsed:?}"
        );
    }

    #[test]
    fn keep_earliest_prefers_sooner_deadline() {
        let soon = RateLimitError::at(RateLimitKind::Rpm, Instant::now() + Duration::from_secs(1));
        let late = RateLimitError::at(RateLimitKind::Rpm, Instant::now() + Duration::from_secs(60));
        let mut slot = Some(late.clone());
        RateLimitError::keep_earliest(&mut slot, soon.clone());
        assert_eq!(slot.unwrap().wait_until(), soon.wait_until());
    }

    #[test]
    fn keep_earliest_real_skips_local_busy() {
        let rpm = RateLimitError::rpm(Duration::from_secs(60));
        let mut slot = Some(rpm.clone());
        RateLimitError::keep_earliest_real(
            &mut slot,
            RateLimitError::local_busy(Duration::from_secs(1)),
        );
        assert_eq!(slot.as_ref().unwrap().kind, RateLimitKind::Rpm);
        assert!((59..=60).contains(&slot.unwrap().retry_after_secs()));
        let mut empty = None;
        RateLimitError::keep_earliest_real(
            &mut empty,
            RateLimitError::local_busy(Duration::from_secs(1)),
        );
        assert!(empty.is_none(), "local_busy 不得写入空历史");
    }

    #[test]
    fn typed_error_header_is_at_least_one_and_has_no_body() {
        let err = RateLimitError::upstream(Some(Duration::from_secs(0)));
        assert_eq!(err.retry_after_secs(), 1);
        let display = err.to_string();
        assert!(display.contains("429"));
        assert!(!display.contains("credential"));
        assert!(!display.contains("{"));
    }

    #[test]
    fn rpm_and_local_busy_keep_computed_wait() {
        let err = RateLimitError::rpm(Duration::from_secs(12));
        assert_eq!(err.kind, RateLimitKind::Rpm);
        assert!((11..=12).contains(&err.retry_after_secs()));
        let busy = RateLimitError::local_busy(Duration::from_millis(1500));
        assert_eq!(busy.retry_after_secs(), 2);
    }
}
