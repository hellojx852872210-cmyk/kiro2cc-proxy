// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 单账号在飞 / 退避 / 全局 429 冷却的可配置参数。

use serde::{Deserialize, Serialize};

/// 写入 config.json 的 `concurrency` 对象（camelCase）。管理台可热改。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
pub struct ConcurrencySettings {
    /// 单账号同时在飞上限。请求开始领取、结束归还。0 = 不限制。
    pub max_inflight_per_credential: u32,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
    pub backoff_multiplier: f64,
    pub suspended_backoff_ms: u64,
    pub global_cooldown_enabled: bool,
    pub global_first_pause_secs: u64,
    pub global_second_pause_secs: u64,
    pub global_third_pause_min_secs: u64,
    pub global_third_pause_max_secs: u64,
    pub global_reset_idle_secs: u64,
}

impl Default for ConcurrencySettings {
    fn default() -> Self {
        Self {
            max_inflight_per_credential: 5,
            backoff_base_ms: 500,
            backoff_max_ms: 3000,
            backoff_multiplier: 1.5,
            suspended_backoff_ms: 1000,
            global_cooldown_enabled: true,
            global_first_pause_secs: 5,
            global_second_pause_secs: 15,
            global_third_pause_min_secs: 30,
            global_third_pause_max_secs: 60,
            global_reset_idle_secs: 120,
        }
    }
}

impl ConcurrencySettings {
    const MAX_MS: u64 = 3_600_000;
    const MAX_SECS: u64 = 3_600;

    pub fn sanitize(mut self) -> Self {
        if !self.backoff_multiplier.is_finite() || self.backoff_multiplier < 1.0 {
            self.backoff_multiplier = 1.0;
        }
        self.backoff_base_ms = self.backoff_base_ms.min(Self::MAX_MS);
        self.backoff_max_ms = self.backoff_max_ms.min(Self::MAX_MS);
        self.suspended_backoff_ms = self.suspended_backoff_ms.min(Self::MAX_MS);
        if self.backoff_max_ms < self.backoff_base_ms {
            self.backoff_max_ms = self.backoff_base_ms;
        }
        self.global_first_pause_secs = self.global_first_pause_secs.min(Self::MAX_SECS);
        self.global_second_pause_secs = self.global_second_pause_secs.min(Self::MAX_SECS);
        self.global_third_pause_min_secs = self.global_third_pause_min_secs.min(Self::MAX_SECS);
        self.global_third_pause_max_secs = self.global_third_pause_max_secs.min(Self::MAX_SECS);
        self.global_reset_idle_secs = self.global_reset_idle_secs.min(Self::MAX_SECS);
        if self.global_third_pause_max_secs < self.global_third_pause_min_secs {
            self.global_third_pause_max_secs = self.global_third_pause_min_secs;
        }
        self
    }
}

/// 第 `consecutive` 次失败（从 1 起）应等待的毫秒数。
pub fn account_backoff_ms(consecutive: u32, settings: &ConcurrencySettings) -> u64 {
    if consecutive == 0 {
        return 0;
    }
    let mut v = settings.backoff_base_ms as f64;
    for _ in 1..consecutive {
        v *= settings.backoff_multiplier;
        if v >= settings.backoff_max_ms as f64 {
            return settings.backoff_max_ms;
        }
    }
    (v as u64).min(settings.backoff_max_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_curve() {
        let s = ConcurrencySettings::default();
        assert_eq!(account_backoff_ms(0, &s), 0);
        assert_eq!(account_backoff_ms(1, &s), 500);
        assert_eq!(account_backoff_ms(2, &s), 750);
        assert_eq!(account_backoff_ms(3, &s), 1125);
        assert_eq!(account_backoff_ms(4, &s), 1687);
        assert_eq!(account_backoff_ms(5, &s), 2531);
        assert_eq!(account_backoff_ms(6, &s), 3000);
        assert_eq!(account_backoff_ms(20, &s), 3000);
    }

    #[test]
    fn sanitize_caps_huge_values() {
        let s = ConcurrencySettings {
            global_first_pause_secs: u64::MAX,
            backoff_base_ms: u64::MAX,
            backoff_max_ms: 1,
            ..Default::default()
        }
        .sanitize();
        assert!(s.global_first_pause_secs <= 3600);
        assert!(s.backoff_base_ms <= 3_600_000);
        assert!(s.backoff_max_ms >= s.backoff_base_ms);
    }
}
