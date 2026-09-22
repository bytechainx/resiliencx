//! 重试（active SSOT §2 + 本仓退避/可注入 wait）。

use crate::error::{ResiliencxError, ResiliencxResult};
use crate::Instrumentation;
use std::any::Any;
use std::future::Future;
use std::time::Duration;

pub use self::jitter::{
    apply_deterministic_jitter, apply_seeded_jitter, retry_delay_ms, retry_delay_ms_with_seed,
};
#[cfg(feature = "tokio")]
pub use self::wait::TokioSleepWait;
pub use self::wait::{AsyncWait, Backoff, NoWait, RecordingWait, ThreadSleepWait, Wait};

mod jitter;
mod wait;

// ── Config ─────────────────────────────────────────────────────────────────

/// 重试配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryConfig {
    /// 最大尝试次数（**含**首次调用）。
    pub max_attempts: u32,
    /// 退避基准延迟（毫秒）。`0` 表示不 wait。
    pub base_delay_ms: u64,
    /// 退避策略。
    pub backoff: Backoff,
    /// 全抖动幅度（basis points，0..=10000）。
    ///
    /// `0`：无抖动。`>0`：在计算出的延迟上施加**确定性**伪抖动
    /// （由 `attempt` 驱动，非加密 RNG），便于单测。
    pub jitter_bps: u32,
}

/// 调用方对操作副作用语义的显式声明。
///
/// 生产安全入口在 `max_attempts > 1` 时只允许只读或幂等操作。该声明由调用方负责，
/// resiliencx 无法从闭包内容自动证明幂等性。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetrySafety {
    /// 操作只读取状态，不产生外部副作用。
    ReadOnly,
    /// 操作可能产生副作用，但调用方保证重复执行具有幂等语义。
    Idempotent,
    /// 操作包含无法安全重复的不幂等副作用。
    UnsafeSideEffect,
}

impl RetrySafety {
    /// 校验该安全声明能否用于给定重试配置。
    pub fn validate(self, config: &RetryConfig) -> ResiliencxResult<()> {
        if config.max_attempts > 1 && self == Self::UnsafeSideEffect {
            return Err(ResiliencxError::invalid("不安全副作用禁止配置多次尝试"));
        }
        Ok(())
    }
}

/// 生产安全重试入口的共同执行上下文。
///
/// 通过 builder 方法注入可选 budget 与 caller seed，避免多个入口重复携带同一参数组。
#[derive(Clone, Copy)]
pub struct RetryContext<'a> {
    config: &'a RetryConfig,
    safety: RetrySafety,
    instrumentation: &'a dyn Instrumentation,
    op: &'a str,
    budget: Option<&'a crate::RetryBudget>,
    jitter_seed: u64,
}

impl<'a> RetryContext<'a> {
    /// 构造不带 budget、seed 为 0 的执行上下文。
    #[must_use]
    pub fn new(
        config: &'a RetryConfig,
        safety: RetrySafety,
        instrumentation: &'a dyn Instrumentation,
        op: &'a str,
    ) -> Self {
        Self {
            config,
            safety,
            instrumentation,
            op,
            budget: None,
            jitter_seed: 0,
        }
    }

    /// 注入可选重试预算。
    #[must_use]
    pub const fn with_budget(mut self, budget: &'a crate::RetryBudget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// 注入用于实际退避计算的 caller seed。
    #[must_use]
    pub const fn with_jitter_seed(mut self, jitter_seed: u64) -> Self {
        self.jitter_seed = jitter_seed;
        self
    }
}

impl std::fmt::Debug for RetryContext<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetryContext")
            .field("config", self.config)
            .field("safety", &self.safety)
            .field("op", &self.op)
            .field("has_budget", &self.budget.is_some())
            .field("jitter_seed", &self.jitter_seed)
            .finish_non_exhaustive()
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 0,
            backoff: Backoff::Constant,
            jitter_bps: 0,
        }
    }
}

impl RetryConfig {
    /// 仅设置 attempts + 固定 base delay（兼容旧构造习惯）。
    #[must_use]
    pub const fn fixed(max_attempts: u32, base_delay_ms: u64) -> Self {
        Self {
            max_attempts,
            base_delay_ms,
            backoff: Backoff::Constant,
            jitter_bps: 0,
        }
    }
}

// ── retry_fn ───────────────────────────────────────────────────────────────

/// 装箱成功值（无泛型 monomorph，保证行覆盖可测）。
pub type RetryValue = Box<dyn Any + Send>;

/// 重试执行操作 `f`（默认 [`ThreadSleepWait`]；unchecked compatibility）。
///
/// 见 [`retry_fn_with_wait`]。本兼容入口不校验 [`RetrySafety`]；新生产接线使用
/// [`retry_fn_safe`]。
pub fn retry_fn(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    f: &mut dyn FnMut() -> ResiliencxResult<RetryValue>,
) -> ResiliencxResult<RetryValue> {
    retry_fn_with_wait(config, instrumentation, op, &ThreadSleepWait, f)
}

/// 带可注入 [`Wait`] 的重试（unchecked compatibility）。
///
/// - 最多尝试 `max_attempts` 次（含首次）。
/// - **仅**当错误 [`ResiliencxError::is_retryable`]（`Transient`）且未达上限时退避重试。
/// - 非可重试错误立即返回；耗尽后返回最后一次原始错误。
/// - 每次真正发起 retry 前：`record_retry` 然后 `wait.wait_ms(retry_delay_ms(...))`。
/// - `max_attempts == 0` → [`ResiliencxError::invalid`]。
///
/// 本兼容入口不校验 [`RetrySafety`]；新生产接线使用 [`retry_fn_safe`]。
pub fn retry_fn_with_wait(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    wait: &dyn Wait,
    f: &mut dyn FnMut() -> ResiliencxResult<RetryValue>,
) -> ResiliencxResult<RetryValue> {
    retry_fn_with_wait_budget(config, instrumentation, op, wait, None, f)
}

/// 带可选 [`crate::RetryBudget`] 的重试（unchecked compatibility）。
///
/// 在即将发起第 N 次重试（非首次）前 `try_consume`；预算耗尽返回 budget 错误。
/// 本兼容入口不校验 [`RetrySafety`]；generic Adapter 使用
/// [`crate::call_with_retry_budget_safe`]，装箱路径使用 [`retry_fn_safe`]。
pub fn retry_fn_with_budget(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    budget: &crate::RetryBudget,
    f: &mut dyn FnMut() -> ResiliencxResult<RetryValue>,
) -> ResiliencxResult<RetryValue> {
    retry_fn_with_wait_budget(
        config,
        instrumentation,
        op,
        &ThreadSleepWait,
        Some(budget),
        f,
    )
}

/// 完整：Wait + 可选预算（unchecked compatibility）。
///
/// 本兼容入口不校验 [`RetrySafety`]；新生产接线使用 [`retry_fn_safe`]。
pub fn retry_fn_with_wait_budget(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    wait: &dyn Wait,
    budget: Option<&crate::RetryBudget>,
    f: &mut dyn FnMut() -> ResiliencxResult<RetryValue>,
) -> ResiliencxResult<RetryValue> {
    retry_fn_with_wait_budget_inner(config, instrumentation, op, wait, budget, 0, f)
}

fn retry_fn_with_wait_budget_inner(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    wait: &dyn Wait,
    budget: Option<&crate::RetryBudget>,
    jitter_seed: u64,
    f: &mut dyn FnMut() -> ResiliencxResult<RetryValue>,
) -> ResiliencxResult<RetryValue> {
    let mut last_err = None;
    for attempt in 1..=config.max_attempts {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) => {
                let retryable = e.is_retryable();
                last_err = Some(e);
                if retryable && attempt < config.max_attempts {
                    if let Some(b) = budget {
                        if !b.try_consume() {
                            return Err(crate::budget_exhausted_error());
                        }
                    }
                    instrumentation.record_retry(op, attempt);
                    let delay = retry_delay_ms_with_seed(config, attempt, jitter_seed);
                    wait.wait_ms(delay);
                } else {
                    break;
                }
            }
        }
    }
    match last_err {
        Some(e) => Err(e),
        None => Err(ResiliencxError::invalid("max_attempts 必须大于或等于 1")),
    }
}

/// 生产安全的同步重试入口。
///
/// 调用方必须显式声明 [`RetrySafety`]；当 `max_attempts > 1` 且操作包含不安全副作用时，
/// 本函数在首次调用前返回 [`ResiliencxError::invalid`]。低层 [`retry_fn`]、[`retry_fn_with_wait`]
/// 和 [`retry_fn_with_wait_budget`] 为兼容保留，不执行此策略校验。`jitter_seed` 由调用方
/// 为不同实例或请求注入，使 jitter 序列去相关。
pub fn retry_fn_safe(
    context: RetryContext<'_>,
    wait: &dyn Wait,
    f: &mut dyn FnMut() -> ResiliencxResult<RetryValue>,
) -> ResiliencxResult<RetryValue> {
    context.safety.validate(context.config)?;
    retry_fn_with_wait_budget_inner(
        context.config,
        context.instrumentation,
        context.op,
        wait,
        context.budget,
        context.jitter_seed,
        f,
    )
}

/// 异步重试（unchecked compatibility）：退避时 `await` [`AsyncWait`]，不阻塞 worker 线程。
///
/// 语义与 [`retry_fn_with_wait`] 一致，但操作 `f` 为返回 `Future` 的闭包。
///
/// 本兼容入口不校验 [`RetrySafety`]，不得直接描述为生产安全路径。已在更高层完成 safety
/// 验证的兼容组合可注入 [`AsyncWait`]；新生产接线使用 [`retry_async_safe`]。在 async 任务内
/// 不得调用 [`retry_fn`]，因为其默认 [`ThreadSleepWait`] 会阻塞 runtime 线程。
pub async fn retry_async<F, Fut>(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    wait: &dyn AsyncWait,
    f: F,
) -> ResiliencxResult<RetryValue>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ResiliencxResult<RetryValue>> + Send,
{
    retry_async_inner(config, instrumentation, op, wait, None, 0, f).await
}

/// 带 [`crate::RetryBudget`] 的异步重试（unchecked compatibility）。
///
/// 每次真正发起重试前消耗一个预算令牌；预算耗尽统一返回
/// [`crate::budget_exhausted_error`]，不会泄漏前一次瞬态错误。
/// 本兼容入口不校验 [`RetrySafety`]；generic Adapter 使用
/// [`crate::call_with_retry_budget_async_safe`]，装箱路径使用 [`retry_async_safe`]。
pub async fn retry_async_with_budget<F, Fut>(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    wait: &dyn AsyncWait,
    budget: &crate::RetryBudget,
    f: F,
) -> ResiliencxResult<RetryValue>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ResiliencxResult<RetryValue>> + Send,
{
    retry_async_inner(config, instrumentation, op, wait, Some(budget), 0, f).await
}

/// 生产安全的异步重试入口。
///
/// 调用方必须显式声明 [`RetrySafety`]；当 `max_attempts > 1` 且操作包含不安全副作用时，
/// 本函数在创建首次操作 future 前返回 [`ResiliencxError::invalid`]。低层 [`retry_async`] 与
/// [`retry_async_with_budget`] 为兼容保留，不执行此策略校验。`jitter_seed` 由调用方
/// 为不同实例或请求注入，使 jitter 序列去相关。
pub async fn retry_async_safe<F, Fut>(
    context: RetryContext<'_>,
    wait: &dyn AsyncWait,
    f: F,
) -> ResiliencxResult<RetryValue>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ResiliencxResult<RetryValue>> + Send,
{
    context.safety.validate(context.config)?;
    retry_async_inner(
        context.config,
        context.instrumentation,
        context.op,
        wait,
        context.budget,
        context.jitter_seed,
        f,
    )
    .await
}

/// 在整次生产安全重试外施加 deadline（feature `tokio`）。
///
/// deadline 覆盖所有尝试与退避等待；超时统一映射为 [`ResiliencxError::deadline_exceeded`]。
/// Tokio 通过丢弃 future 实施 cooperative cancellation：尚未完成的 future 会停止被轮询，
/// 但本函数**不保证撤销**已经发生的外部副作用，操作自行派生的后台任务也可能继续运行。
#[cfg(feature = "tokio")]
pub async fn retry_async_with_deadline<F, Fut>(
    context: RetryContext<'_>,
    wait: &dyn AsyncWait,
    deadline: Duration,
    f: F,
) -> ResiliencxResult<RetryValue>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ResiliencxResult<RetryValue>> + Send,
{
    context.safety.validate(context.config)?;
    match tokio::time::timeout(
        deadline,
        retry_async_inner(
            context.config,
            context.instrumentation,
            context.op,
            wait,
            context.budget,
            context.jitter_seed,
            f,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ResiliencxError::deadline_exceeded(
            "resiliencx 整次重试已超过截止时间",
        )),
    }
}

async fn retry_async_inner<F, Fut>(
    config: &RetryConfig,
    instrumentation: &dyn Instrumentation,
    op: &str,
    wait: &dyn AsyncWait,
    budget: Option<&crate::RetryBudget>,
    jitter_seed: u64,
    mut f: F,
) -> ResiliencxResult<RetryValue>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ResiliencxResult<RetryValue>> + Send,
{
    let mut last_err = None;
    for attempt in 1..=config.max_attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let retryable = e.is_retryable();
                last_err = Some(e);
                if retryable && attempt < config.max_attempts {
                    let reservation = match budget {
                        Some(budget) => {
                            Some(budget.reserve().ok_or_else(crate::budget_exhausted_error)?)
                        }
                        None => None,
                    };
                    let delay = retry_delay_ms_with_seed(config, attempt, jitter_seed);
                    wait.wait_ms(delay).await;
                    if let Some(reservation) = reservation {
                        reservation.commit();
                    }
                    instrumentation.record_retry(op, attempt);
                } else {
                    break;
                }
            }
        }
    }
    match last_err {
        Some(e) => Err(e),
        None => Err(ResiliencxError::invalid("max_attempts 必须大于或等于 1")),
    }
}

/// 将具体成功值装箱为 [`RetryValue`]。
#[must_use]
pub fn retry_ok<T: Any + Send>(value: T) -> RetryValue {
    Box::new(value)
}

/// 将 [`RetryValue`] downcast 为 `T`；类型不匹配时返回 `Invalid`。
pub fn retry_downcast<T: Any>(value: RetryValue) -> ResiliencxResult<T> {
    match value.downcast::<T>() {
        Ok(b) => Ok(*b),
        Err(_) => Err(ResiliencxError::invalid("重试返回值类型不匹配")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn async_retry_success() -> ResiliencxResult<RetryValue> {
        Ok(retry_ok(()))
    }

    #[test]
    fn delay_zero_base_or_attempt() {
        let cfg = RetryConfig::fixed(3, 0);
        assert_eq!(retry_delay_ms(&cfg, 1), 0);
        let cfg = RetryConfig::fixed(3, 10);
        assert_eq!(retry_delay_ms(&cfg, 0), 0);
    }

    #[test]
    fn constant_backoff() {
        let cfg = RetryConfig {
            max_attempts: 5,
            base_delay_ms: 10,
            backoff: Backoff::Constant,
            jitter_bps: 0,
        };
        assert_eq!(retry_delay_ms(&cfg, 1), 10);
        assert_eq!(retry_delay_ms(&cfg, 3), 10);
    }

    #[test]
    fn exponential_backoff_caps() {
        let cfg = RetryConfig {
            max_attempts: 10,
            base_delay_ms: 10,
            backoff: Backoff::Exponential {
                factor: 2,
                max_delay_ms: 40,
            },
            jitter_bps: 0,
        };
        assert_eq!(retry_delay_ms(&cfg, 1), 10);
        assert_eq!(retry_delay_ms(&cfg, 2), 20);
        assert_eq!(retry_delay_ms(&cfg, 3), 40);
        assert_eq!(retry_delay_ms(&cfg, 4), 40);
    }

    #[test]
    fn exponential_factor_one_equals_constant_until_cap() {
        let cfg = RetryConfig {
            max_attempts: 5,
            base_delay_ms: 7,
            backoff: Backoff::Exponential {
                factor: 0,
                max_delay_ms: 100,
            }, // 0 → max(1)
            jitter_bps: 0,
        };
        assert_eq!(retry_delay_ms(&cfg, 1), 7);
        assert_eq!(retry_delay_ms(&cfg, 3), 7);
    }

    #[test]
    fn jitter_zero_unchanged() {
        assert_eq!(apply_deterministic_jitter(100, 0, 1), 100);
        assert_eq!(apply_deterministic_jitter(0, 5000, 1), 0);
    }

    #[test]
    fn jitter_reduces_or_keeps() {
        let j = apply_deterministic_jitter(1000, 5000, 1);
        assert!(j <= 1000);
        // 同一 attempt 确定性
        assert_eq!(j, apply_deterministic_jitter(1000, 5000, 1));
        assert_ne!(j, apply_deterministic_jitter(1000, 5000, 2));
    }

    #[test]
    fn recording_wait_captures_exponential_delays() {
        let cfg = RetryConfig {
            max_attempts: 4,
            base_delay_ms: 5,
            backoff: Backoff::Exponential {
                factor: 2,
                max_delay_ms: 100,
            },
            jitter_bps: 0,
        };
        let wait = RecordingWait::new();
        let hits = std::sync::Mutex::new(0u32);
        let mut op = || {
            let mut g = hits.lock().expect("h");
            *g += 1;
            if *g < 4 {
                Err(ResiliencxError::transient("t"))
            } else {
                Ok(retry_ok(()))
            }
        };
        let instr = crate::NoopInstrumentation;
        retry_fn_with_wait(&cfg, &instr, "op", &wait, &mut op).expect("ok");
        assert_eq!(wait.delays(), vec![5, 10, 20]);
    }

    #[test]
    fn no_wait_and_thread_sleep_zero() {
        Wait::wait_ms(&NoWait, 0);
        Wait::wait_ms(&NoWait, 1);
        Wait::wait_ms(&ThreadSleepWait, 0);
        let _ = format!("{ThreadSleepWait:?}");
        let _ = format!("{NoWait:?}");
        let _ = format!("{:?}", Backoff::default());
        let _ = RetryConfig::fixed(2, 0);
    }

    #[test]
    fn config_default_has_constant_backoff() {
        let d = RetryConfig::default();
        assert_eq!(d.backoff, Backoff::Constant);
        assert_eq!(d.jitter_bps, 0);
    }

    #[test]
    fn budget_stops_further_retries() {
        let cfg = RetryConfig::fixed(10, 0);
        let budget = crate::RetryBudget::new(1); // only one retry token
        let hits = std::sync::Mutex::new(0u32);
        let mut op = || {
            let mut g = hits.lock().unwrap();
            *g += 1;
            Err(ResiliencxError::transient("t"))
        };
        let instr = crate::NoopInstrumentation;
        let err = retry_fn_with_wait_budget(&cfg, &instr, "op", &NoWait, Some(&budget), &mut op)
            .unwrap_err();
        // 第 1 次失败 → consume → 第 2 次失败 → consume 失败 → Unavailable
        assert_eq!(err.kind(), crate::error::ErrorKind::Unavailable);
        assert_eq!(*hits.lock().unwrap(), 2);
        assert!(budget.is_exhausted());
        budget.reset();
        assert_eq!(budget.remaining(), 1);
    }

    #[test]
    fn retry_fn_with_budget_wrapper() {
        let cfg = RetryConfig::fixed(3, 0);
        let budget = crate::RetryBudget::new(5);
        let hits = std::sync::Mutex::new(0u32);
        let mut op = || {
            let mut g = hits.lock().unwrap();
            *g += 1;
            if *g < 2 {
                Err(ResiliencxError::transient("t"))
            } else {
                Ok(retry_ok(7))
            }
        };
        let v = retry_fn_with_budget(&cfg, &crate::NoopInstrumentation, "wrap", &budget, &mut op)
            .expect("ok");
        assert_eq!(retry_downcast::<i32>(v).unwrap(), 7);
        assert_eq!(*hits.lock().unwrap(), 2);
    }

    #[test]
    fn retry_safety_validate_contract() {
        assert!(RetrySafety::ReadOnly
            .validate(&RetryConfig::fixed(2, 0))
            .is_ok());
        assert!(RetrySafety::Idempotent
            .validate(&RetryConfig::fixed(2, 0))
            .is_ok());
        let err = RetrySafety::UnsafeSideEffect
            .validate(&RetryConfig::fixed(2, 0))
            .expect_err("多次不安全副作用应拒绝");
        assert_eq!(err.kind(), crate::error::ErrorKind::Invalid);
    }

    #[test]
    fn seeded_jitter_public_helpers_are_deterministic_and_decorrelated() {
        let config = RetryConfig {
            max_attempts: 2,
            base_delay_ms: 1_000,
            backoff: Backoff::Constant,
            jitter_bps: 5_000,
        };
        assert_eq!(
            retry_delay_ms_with_seed(&config, 1, 7),
            apply_seeded_jitter(1_000, 5_000, 1, 7)
        );
        assert_ne!(
            retry_delay_ms_with_seed(&config, 1, 7),
            retry_delay_ms_with_seed(&config, 1, 8)
        );
    }

    #[test]
    fn retry_fn_safe_uses_caller_jitter_seed() {
        let config = RetryConfig {
            max_attempts: 2,
            base_delay_ms: 1_000,
            backoff: Backoff::Constant,
            jitter_bps: 5_000,
        };
        let wait = RecordingWait::new();
        let mut calls = 0u32;
        let mut operation = || {
            calls += 1;
            if calls == 1 {
                Err(ResiliencxError::transient("重试"))
            } else {
                Ok(retry_ok(()))
            }
        };
        retry_fn_safe(
            RetryContext::new(
                &config,
                RetrySafety::ReadOnly,
                &crate::NoopInstrumentation,
                "seeded",
            )
            .with_jitter_seed(7),
            &wait,
            &mut operation,
        )
        .expect("安全重试");
        assert_eq!(wait.delays(), vec![retry_delay_ms_with_seed(&config, 1, 7)]);
    }

    #[tokio::test]
    async fn retry_async_with_budget_public_contract() {
        let budget = crate::RetryBudget::new(0);
        let err = retry_async_with_budget(
            &RetryConfig::fixed(2, 0),
            &crate::NoopInstrumentation,
            "budget",
            &NoWait,
            &budget,
            || async { Err(ResiliencxError::transient("重试")) },
        )
        .await
        .expect_err("预算耗尽");
        assert_eq!(err.to_string(), crate::budget_exhausted_error().to_string());
    }

    #[tokio::test]
    async fn retry_async_safe_public_contract() {
        let err = retry_async_safe(
            RetryContext::new(
                &RetryConfig::fixed(2, 0),
                RetrySafety::UnsafeSideEffect,
                &crate::NoopInstrumentation,
                "unsafe",
            ),
            &NoWait,
            async_retry_success,
        )
        .await
        .expect_err("多次不安全副作用应拒绝");
        assert_eq!(err.kind(), crate::error::ErrorKind::Invalid);

        let value = retry_async_safe(
            RetryContext::new(
                &RetryConfig::fixed(2, 0),
                RetrySafety::ReadOnly,
                &crate::NoopInstrumentation,
                "safe",
            ),
            &NoWait,
            async_retry_success,
        )
        .await
        .expect("只读异步操作应进入实际执行路径");
        retry_downcast::<()>(value).expect("成功值类型");
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn retry_async_with_deadline_public_contract() {
        let err = retry_async_with_deadline(
            RetryContext::new(
                &RetryConfig::fixed(2, 0),
                RetrySafety::ReadOnly,
                &crate::NoopInstrumentation,
                "deadline",
            ),
            &NoWait,
            Duration::from_millis(1),
            std::future::pending,
        )
        .await
        .expect_err("deadline");
        assert_eq!(err.kind(), crate::error::ErrorKind::DeadlineExceeded);
    }

    #[test]
    fn retry_context_builders_preserve_common_policy() {
        let config = RetryConfig::fixed(2, 0);
        let budget = crate::RetryBudget::new(1);
        let context = RetryContext::new(
            &config,
            RetrySafety::ReadOnly,
            &crate::NoopInstrumentation,
            "context",
        )
        .with_budget(&budget)
        .with_jitter_seed(9);

        let debug = format!("{context:?}");
        assert!(debug.contains("context"));
        assert!(debug.contains("has_budget: true"));
        assert!(debug.contains("jitter_seed: 9"));
    }
}
