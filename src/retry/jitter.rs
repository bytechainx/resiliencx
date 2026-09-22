//! 退避延迟与抖动的纯函数。
//!
//! 自 `src/retry.rs` 下沉而来：`retry_delay_ms` / `retry_delay_ms_with_seed` 与
//! `apply_deterministic_jitter` / `apply_seeded_jitter`。公开项经门面 `pub use` 转出，
//! 故 `resiliencx::retry::{…}` 与 crate 根的 re-export 路径不变。

use super::{Backoff, RetryConfig};

/// 计算第 `attempt` 次失败后、发起下一次尝试前应等待的毫秒数。
///
/// `attempt` 为刚失败的尝试序号（从 1 起）。
/// 本兼容入口仅由 `attempt` 驱动 jitter，不同进程使用相同配置时会得到相同序列，
/// 因而**不具备抗群聚保证**；生产调用方可改用 [`retry_delay_ms_with_seed`] 注入实例 seed。
#[must_use]
pub fn retry_delay_ms(config: &RetryConfig, attempt: u32) -> u64 {
    retry_delay_ms_with_seed(config, attempt, 0)
}

/// 使用调用方 seed 计算退避延迟，便于不同实例去相关。
///
/// seed 仅用于非加密 jitter；调用方应为不同进程或请求选择不同 seed。
#[must_use]
pub fn retry_delay_ms_with_seed(config: &RetryConfig, attempt: u32, seed: u64) -> u64 {
    if config.base_delay_ms == 0 || attempt == 0 {
        return 0;
    }
    let raw = match config.backoff {
        Backoff::Constant => config.base_delay_ms,
        Backoff::Exponential {
            factor,
            max_delay_ms,
        } => {
            let f = factor.max(1);
            // base * f^(attempt-1)，饱和到 max
            let mut d = config.base_delay_ms;
            let mut i = 1u32;
            while i < attempt {
                d = d.saturating_mul(u64::from(f));
                if d >= max_delay_ms {
                    d = max_delay_ms;
                    break;
                }
                i = i.saturating_add(1);
            }
            d.min(max_delay_ms)
        }
    };
    apply_seeded_jitter(raw, config.jitter_bps, attempt, seed)
}

/// 确定性伪抖动：`delay * (10000 - offset) / 10000`，`offset ∈ [0, jitter_bps]`。
///
/// 本兼容入口等价于 seed 为 0 的 [`apply_seeded_jitter`]；仅依赖 attempt，
/// 多实例会产生相同序列，**不具备抗群聚保证**。
///
/// # Examples
///
/// ```
/// use resiliencx::apply_deterministic_jitter;
///
/// // 抖动比例为 0 时原样返回。
/// assert_eq!(apply_deterministic_jitter(1000, 0, 3), 1000);
/// // 抖动上限 1000 bps（10%）时，结果落在 [900, 1000]。
/// let jittered = apply_deterministic_jitter(1000, 1000, 1);
/// assert!((900..=1000).contains(&jittered), "jittered={jittered}");
/// // 只依赖入参：同一组参数每次得到同一结果。
/// assert_eq!(jittered, apply_deterministic_jitter(1000, 1000, 1));
/// ```
#[must_use]
pub fn apply_deterministic_jitter(delay_ms: u64, jitter_bps: u32, attempt: u32) -> u64 {
    apply_seeded_jitter(delay_ms, jitter_bps, attempt, 0)
}

/// 使用调用方 seed 的确定性伪抖动。
///
/// 相同 `(delay_ms, jitter_bps, attempt, seed)` 始终得到相同结果；这不是加密 RNG。
#[must_use]
pub fn apply_seeded_jitter(delay_ms: u64, jitter_bps: u32, attempt: u32, seed: u64) -> u64 {
    if delay_ms == 0 || jitter_bps == 0 {
        return delay_ms;
    }
    let span = u64::from(jitter_bps.min(10_000));
    // 简单确定性序列：与 attempt、调用方 seed 绑定，避免 flaky。
    let offset = u64::from(attempt)
        .wrapping_mul(7919)
        .wrapping_add(seed.wrapping_mul(104_729))
        % span.saturating_add(1);
    let keep = 10_000u64.saturating_sub(offset);
    delay_ms.saturating_mul(keep) / 10_000
}
