// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! Prompt Cache 模块
//!
//! - `simulation` - 三角分布与比例模式模拟（旧 cache.rs 内容）
//! - `fingerprint` - 账号级前缀指纹追踪（替代末层兜底）
//!
//! 公共 API 保持 `crate::cache::PromptCacheUsage` 路径不变。
//!
//! cache_read 派生主路径已切换为 `token::count_prefix_tokens` 前缀字符估算，
//! `simulation` 模拟与 `fingerprint` 追踪降级为兜底分支。

pub mod fingerprint;
pub mod simulation;

pub use simulation::{
    CacheSimulationRatioConfig, PromptCacheUsage, split_creation_by_ephemeral_ratio,
};
#[allow(unused_imports)]
pub use simulation::{
    DEFAULT_CACHE_SIMULATION_RATIO_FOCUS_PROBABILITY, DEFAULT_CACHE_SIMULATION_RATIO_FOCUS_RADIUS,
};

/// 从 prefix 估算出的 cache_read 中，再标注一部分为 cache_creation。
///
/// 起因：Kiro metering 不透传 cache_read/creation，prefix 估算层因此把整段稳定前缀
/// （system + tools + history）全部记为 cache_read，下游按 0.1x 读价计费；而同类上游
/// 会把其中一部分记为 creation（1.25x）。本函数按 `CACHE_CREATION_SPLIT_RATIO` 把
/// 同一批 token 改记到 creation 档，**总量守恒**：返回的 (read, creation) 之和恒等于入参。
///
/// 默认 0.0 = 关闭，返回 `(read, 0)`，与改动前逐字节一致。
/// 取值需落在 [0.0, 1.0)，越界或无法解析一律退回 0.0。
pub fn split_prefix_read(read: i32) -> (i32, i32) {
    split_prefix_read_with(read, creation_split_ratio())
}

/// `split_prefix_read` 的纯逻辑部分，比例显式传入，便于测试。
pub(crate) fn split_prefix_read_with(read: i32, ratio: f64) -> (i32, i32) {
    if !ratio.is_finite() || ratio <= 0.0 || ratio >= 1.0 || read <= 0 {
        return (read, 0);
    }
    let creation = ((read as f64) * ratio) as i32;
    // creation 必须真正小于 read，否则 cache_read 会被清零，偏离“再标注”的本意。
    if creation <= 0 || creation >= read {
        return (read, 0);
    }
    (read - creation, creation)
}

/// 运行时比例，f64 以 bits 存进 AtomicU64：每个请求都要读，用无锁避免争用。
/// 初值 0 的 bits 正好是 0.0，即“未配置 = 关闭”。
static CREATION_SPLIT_RATIO: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// 当前生效比例。0.0 = 关闭。
pub fn creation_split_ratio() -> f64 {
    f64::from_bits(CREATION_SPLIT_RATIO.load(std::sync::atomic::Ordering::Relaxed))
}

/// 设置比例，**立即生效、无需重启**。返回实际落地的值。
///
/// 只接受 [0.0, 1.0) 内的有限数；越界、NaN、Inf 一律落成 0.0（关闭），
/// 这样一个手滑的输入只会把功能关掉，不会把 cache_read 整段搬走。
pub fn set_creation_split_ratio(ratio: f64) -> f64 {
    let sane = if ratio.is_finite() && (0.0..1.0).contains(&ratio) {
        ratio
    } else {
        0.0
    };
    CREATION_SPLIT_RATIO.store(sane.to_bits(), std::sync::atomic::Ordering::Relaxed);
    sane
}

/// 启动时的初值：配置文件优先，其次环境变量 `CACHE_CREATION_SPLIT_RATIO`。
pub fn init_creation_split_ratio(from_config: Option<f64>) -> f64 {
    let from_env = std::env::var("CACHE_CREATION_SPLIT_RATIO")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok());
    set_creation_split_ratio(from_config.or(from_env).unwrap_or(0.0))
}

/// 四层降级链选择终值 usage：
///
/// 优先级（高→低）：
/// 1. **metering 真值**：上游 Kiro 返回的 cache_read / cache_creation 原始值
/// 2. **prefix 估算结果**：调用方用 `token::count_prefix_tokens` 估算的 system+tools+history[0..n-1]
///    本地 token 数（饱和裁剪到 final_input_tokens）
/// 3. **fingerprint 命中**：账号级前缀指纹追踪输出
/// 4. **ratio 兜底**：比例模拟（`from_ratio_config`）的产出
///
/// 所有分支输出均经 `clamp_to_total(final_input_tokens)` 截断，保证 5m/1h 不变性。
pub fn select_final_usage(
    final_input_tokens: i32,
    metering: Option<(i32, i32)>,
    prefix_estimated_read: Option<i32>,
    fingerprint_usage: Option<PromptCacheUsage>,
    ratio_fallback: PromptCacheUsage,
) -> PromptCacheUsage {
    if let Some((read, creation)) = metering {
        // Kiro metering 不返回 5m/1h 拆分，默认全部归为 5m
        return PromptCacheUsage {
            input_tokens: final_input_tokens
                .saturating_sub(read)
                .saturating_sub(creation),
            cache_creation_input_tokens: creation,
            cache_read_input_tokens: read,
            cache_creation_5m_input_tokens: creation,
            cache_creation_1h_input_tokens: 0,
        }
        .clamp_to_total(final_input_tokens);
    }
    if let Some(estimated) = prefix_estimated_read {
        let estimated = estimated.min(final_input_tokens);
        // 总量守恒：read + creation 恒等于 estimated，只改计价档位。
        let (read, creation) = split_prefix_read(estimated);
        return PromptCacheUsage {
            input_tokens: final_input_tokens.saturating_sub(estimated),
            cache_creation_input_tokens: creation,
            cache_read_input_tokens: read,
            // 全部归 5m（1.25x）；1h 会按 2x 计，超出对齐目标。
            cache_creation_5m_input_tokens: creation,
            cache_creation_1h_input_tokens: 0,
        }
        .clamp_to_total(final_input_tokens);
    }
    if let Some(fp) = fingerprint_usage {
        return fp.clamp_to_total(final_input_tokens);
    }
    ratio_fallback.clamp_to_total(final_input_tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KONGJI_RATIO: f64 = 0.1768;

    #[test]
    fn split_prefix_read_is_off_by_default() {
        // 未设 CACHE_CREATION_SPLIT_RATIO 时行为与改动前完全一致
        // 纯函数：比例为 0 时与改动前逐字节一致。
        // 不在这里断言全局比例——测试并行执行，全局态归下面那个独占用例管。
        assert_eq!(split_prefix_read_with(23000, 0.0), (23000, 0));
        assert_eq!(split_prefix_read_with(1, 0.0), (1, 0));
    }

    /// 独占全局比例的用例。与 `split_prefix_read_is_off_by_default` 的分工：
    /// 那个只测纯函数，本用例是唯一读写全局态的地方，避免并行互相干扰。
    #[test]
    fn set_creation_split_ratio_takes_effect_and_sanitises() {
        // 进程启动未初始化时应为关闭
        assert_eq!(creation_split_ratio(), 0.0, "默认必须是关闭状态");
        assert_eq!(set_creation_split_ratio(KONGJI_RATIO), KONGJI_RATIO);
        assert_eq!(creation_split_ratio(), KONGJI_RATIO);
        let (r, c) = split_prefix_read(23_000);
        assert_eq!(r + c, 23_000);
        assert!(c > 0, "设过比例后必须真的拆出 creation");

        // 越界输入一律落成 0.0（关闭），不允许把 read 整段搬走
        for bad in [-0.5, 1.0, 2.0, f64::NAN, f64::INFINITY] {
            assert_eq!(set_creation_split_ratio(bad), 0.0, "bad={bad}");
            assert_eq!(split_prefix_read(23_000), (23_000, 0));
        }
        set_creation_split_ratio(0.0);
    }

    #[test]
    fn split_prefix_read_conserves_total() {
        for read in [1, 7, 100, 23000, 480_000, i32::MAX / 2] {
            let (r, c) = split_prefix_read_with(read, KONGJI_RATIO);
            assert_eq!(r + c, read, "read={read}");
            assert!(r >= 0 && c >= 0);
        }
    }

    #[test]
    fn split_prefix_read_matches_target_multiplier() {
        // 0.1768 记 1.25x、其余记 0.1x → 等效 0.3033x
        let read = 1_000_000;
        let (r, c) = split_prefix_read_with(read, KONGJI_RATIO);
        let effective = (c as f64 * 1.25 + r as f64 * 0.1) / read as f64;
        assert!((effective - 0.3033).abs() < 0.0005, "effective={effective}");
    }

    #[test]
    fn split_prefix_read_rejects_bad_ratios() {
        for bad in [-0.1, 0.0, 1.0, 1.5, f64::NAN, f64::INFINITY] {
            assert_eq!(split_prefix_read_with(1000, bad), (1000, 0), "ratio={bad}");
        }
    }

    #[test]
    fn split_prefix_read_keeps_tiny_reads_whole() {
        // 摊不出至少 1 个 token 就不动，避免把 read 清零
        assert_eq!(split_prefix_read_with(3, KONGJI_RATIO), (3, 0));
        assert_eq!(split_prefix_read_with(0, KONGJI_RATIO), (0, 0));
        assert_eq!(split_prefix_read_with(-5, KONGJI_RATIO), (-5, 0));
    }

    #[test]
    fn prefix_layer_splits_and_keeps_5m_equal_to_total() {
        let total = 30_000;
        let (r, c) = split_prefix_read_with(23_000, KONGJI_RATIO);
        let u = PromptCacheUsage {
            input_tokens: total - 23_000,
            cache_creation_input_tokens: c,
            cache_read_input_tokens: r,
            cache_creation_5m_input_tokens: c,
            cache_creation_1h_input_tokens: 0,
        }
        .clamp_to_total(total);
        // clamp 不得破坏拆分，5m 必须仍等于顶层 creation
        assert_eq!(u.cache_creation_input_tokens, c);
        assert_eq!(u.cache_read_input_tokens, r);
        assert_eq!(u.cache_creation_5m_input_tokens, c);
        assert_eq!(u.cache_creation_1h_input_tokens, 0);
        assert_eq!(u.input_tokens + u.cache_read_input_tokens
            + u.cache_creation_input_tokens, total);
    }

    #[test]
    fn select_final_usage_prefix_layer_is_unchanged_when_off() {
        let u = select_final_usage(45_409, None, Some(36_348), None,
                                   PromptCacheUsage::uncached(45_409));
        assert_eq!(u.cache_creation_input_tokens, 0);
        assert_eq!(u.cache_read_input_tokens, 36_348);
        assert_eq!(u.input_tokens, 45_409 - 36_348);
    }

    fn ratio_fallback(total: i32) -> PromptCacheUsage {
        // 模拟 from_ratios 的产出：50% 缓存，其中 30% creation
        let cached = ((total as f64) * 0.5) as i32;
        let creation = ((cached as f64) * 0.3) as i32;
        let read = cached - creation;
        PromptCacheUsage {
            input_tokens: total - cached,
            cache_creation_input_tokens: creation,
            cache_read_input_tokens: read,
            cache_creation_5m_input_tokens: creation,
            cache_creation_1h_input_tokens: 0,
        }
    }

    fn invariant_holds(u: PromptCacheUsage, total: i32) -> bool {
        u.input_tokens >= 0
            && u.cache_creation_5m_input_tokens + u.cache_creation_1h_input_tokens
                == u.cache_creation_input_tokens
            && u.cache_read_input_tokens + u.cache_creation_input_tokens <= total
            && u.total_input_tokens() == total
    }

    #[test]
    fn layer1_metering_wins_over_all() {
        let total = 1000;
        let metering = Some((600, 200));
        let credits = Some(500); // 应被忽略
        let fp = Some(PromptCacheUsage {
            input_tokens: 0,
            cache_creation_input_tokens: 100,
            cache_read_input_tokens: 900,
            cache_creation_5m_input_tokens: 100,
            cache_creation_1h_input_tokens: 0,
        });
        let final_u = select_final_usage(total, metering, credits, fp, ratio_fallback(total));
        assert_eq!(final_u.cache_read_input_tokens, 600);
        assert_eq!(final_u.cache_creation_input_tokens, 200);
        assert_eq!(final_u.input_tokens, 200);
        assert!(invariant_holds(final_u, total));
    }

    #[test]
    fn layer2_credits_wins_when_metering_absent() {
        let total = 1000;
        let credits = Some(400);
        let fp = Some(PromptCacheUsage {
            input_tokens: 0,
            cache_creation_input_tokens: 100,
            cache_read_input_tokens: 900,
            cache_creation_5m_input_tokens: 100,
            cache_creation_1h_input_tokens: 0,
        });
        let final_u = select_final_usage(total, None, credits, fp, ratio_fallback(total));
        assert_eq!(final_u.cache_read_input_tokens, 400);
        assert_eq!(final_u.cache_creation_input_tokens, 0);
        assert_eq!(final_u.input_tokens, 600);
        assert!(invariant_holds(final_u, total));
    }

    #[test]
    fn layer3_fingerprint_wins_when_metering_and_credits_absent() {
        let total = 1000;
        let fp = Some(PromptCacheUsage {
            input_tokens: 200,
            cache_creation_input_tokens: 100,
            cache_read_input_tokens: 700,
            cache_creation_5m_input_tokens: 70,
            cache_creation_1h_input_tokens: 30,
        });
        let final_u = select_final_usage(total, None, None, fp, ratio_fallback(total));
        assert_eq!(final_u.cache_read_input_tokens, 700);
        assert_eq!(final_u.cache_creation_input_tokens, 100);
        assert_eq!(final_u.cache_creation_1h_input_tokens, 30);
        assert!(invariant_holds(final_u, total));
    }

    #[test]
    fn layer4_ratio_fallback_when_all_higher_absent() {
        let total = 1000;
        let fallback = ratio_fallback(total);
        let final_u = select_final_usage(total, None, None, None, fallback);
        assert_eq!(
            final_u.cache_read_input_tokens,
            fallback.cache_read_input_tokens
        );
        assert_eq!(
            final_u.cache_creation_input_tokens,
            fallback.cache_creation_input_tokens
        );
        assert!(invariant_holds(final_u, total));
    }

    #[test]
    fn metering_over_total_is_clamped() {
        // metering 数值大于 total，需被截断
        let total = 100;
        let metering = Some((80, 50)); // 80+50 = 130 > 100
        let final_u = select_final_usage(total, metering, None, None, ratio_fallback(total));
        // cache_read 优先保留：80，剩余 20 全给 creation
        assert_eq!(final_u.cache_read_input_tokens, 80);
        assert_eq!(final_u.cache_creation_input_tokens, 20);
        assert_eq!(final_u.input_tokens, 0);
        assert!(invariant_holds(final_u, total));
    }

    #[test]
    fn credits_inferred_zero_is_valid_layer2() {
        // credits 反推为 0（baseline ≤ credits） — 仍走 Layer 2，cache_read = 0
        let total = 1000;
        let final_u = select_final_usage(total, None, Some(0), None, ratio_fallback(total));
        assert_eq!(final_u.cache_read_input_tokens, 0);
        assert_eq!(final_u.cache_creation_input_tokens, 0);
        assert_eq!(final_u.input_tokens, 1000);
        assert!(invariant_holds(final_u, total));
    }

    #[test]
    fn fingerprint_with_5m_1h_ratio_preserved() {
        // 指纹层输出含 5m/1h 拆分，clamp 后比例需保持
        let total = 1000;
        let fp = Some(PromptCacheUsage {
            input_tokens: 0,
            cache_creation_input_tokens: 200,
            cache_read_input_tokens: 800,
            cache_creation_5m_input_tokens: 140,
            cache_creation_1h_input_tokens: 60,
        });
        let final_u = select_final_usage(total, None, None, fp, ratio_fallback(total));
        // 1h 比例 60/200 = 0.3 保持
        assert_eq!(final_u.cache_creation_1h_input_tokens, 60);
        assert_eq!(final_u.cache_creation_5m_input_tokens, 140);
        assert!(invariant_holds(final_u, total));
    }
}
