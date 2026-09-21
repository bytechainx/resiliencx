//! 重试（active SSOT §2 + 本仓退避/可注入 wait）。

use crate::error::{ResiliencxError, ResiliencxResult};
use crate::Instrumentation;
use async_trait::async_trait;
use std::any::Any;
use std::future::Future;
use std::thread;
use std::time::Duration;

// ── Backoff / Wait ─────────────────────────────────────────────────────────

/// 退避策略（相对 [`RetryConfig::base_delay_ms`]）。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Backoff {
    /// 每次重试使用固定 `base_delay_ms`（历史默认）。
    #[default]
    Constant,
    /// 指数：`min(base * factor^(attempt-1), max_delay_ms)`；`attempt` 为已完成失败次数（≥1）。
    Exponential {
        /// 倍数，须 ≥ 1；`1` 等价常数。
        factor: u32,
        /// 上限毫秒。
        max_delay_ms: u64,
    },
}

/// 等待策略（对象安全）。生产默认 [`ThreadSleepWait`]；测试可用 [`NoWait`] / [`RecordingWait`]。
pub trait Wait: Send + Sync {
    /// 等待 `ms` 毫秒（`0` 应为空操作）。
    fn wait_ms(&self, ms: u64);
}

/// 使用 [`thread::sleep`] 的默认 wait（会阻塞调用线程）。
#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadSleepWait;

impl Wait for ThreadSleepWait {
    fn wait_ms(&self, ms: u64) {
        if ms > 0 {
            thread::sleep(Duration::from_millis(ms));
        }
    }
}

/// 空 wait（不睡眠；配合 delay 计算测试）。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoWait;

impl Wait for NoWait {
    fn wait_ms(&self, _ms: u64) {}
}

/// 记录每次请求延迟的 wait（测试用；不睡眠）。
#[derive(Debug, Default)]
pub struct RecordingWait {
    delays: std::sync::Mutex<Vec<u64>>,
}

/// 取得延迟记录锁；锁中毒（此前持锁线程 panic）时恢复内层数据，避免诊断辅助路径 panic。
#[track_caller]
fn lock_delays_or_recover(
    delays: &std::sync::Mutex<Vec<u64>>,
) -> std::sync::MutexGuard<'_, Vec<u64>> {
    match delays.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(
                lock = "resiliencx::retry::RecordingWait::delays",
                at = %std::panic::Location::caller(),
                "锁中毒已恢复：持锁线程此前 panic，返回已持有数据"
            );
            poisoned.into_inner()
        }
    }
}

impl RecordingWait {
    /// 构造空记录器。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 已记录的延迟序列（毫秒）。
    #[must_use]
    pub fn delays(&self) -> Vec<u64> {
        lock_delays_or_recover(&self.delays).clone()
    }
}

impl Wait for RecordingWait {
    fn wait_ms(&self, ms: u64) {
        lock_delays_or_recover(&self.delays).push(ms);
    }
}

// ── AsyncWait / 非阻塞重试 ─────────────────────────────────────────────────

/// 异步等待策略（对象安全）。
///
/// async 服务路径应使用本 trait + [`retry_async`]，**禁止**在 async 任务中直接调用
/// 默认阻塞的 [`retry_fn`] / [`ThreadSleepWait`]。
#[async_trait]
pub trait AsyncWait: Send + Sync {
    /// 异步等待 `ms` 毫秒（`0` 应为空操作）。
    async fn wait_ms(&self, ms: u64);
}

#[async_trait]
impl AsyncWait for NoWait {
    async fn wait_ms(&self, _ms: u64) {}
}

#[async_trait]
impl AsyncWait for RecordingWait {
    async fn wait_ms(&self, ms: u64) {
        lock_delays_or_recover(&self.delays).push(ms);
    }
}

/// 基于 `tokio::time::sleep` 的非阻塞 wait（feature `tokio`）。
///
/// 在 async runtime 内 `await` 等待，**不**占用阻塞线程。
#[cfg(feature = "tokio")]
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioSleepWait;

#[cfg(feature = "tokio")]
#[async_trait]
impl AsyncWait for TokioSleepWait {
    async fn wait_ms(&self, ms: u64) {
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }
}

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
