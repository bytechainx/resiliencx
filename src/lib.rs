//! resiliencx —— 弹性原语：重试 + 熔断 + 限流 + 舱壁 + 重试预算。
//!
//! | 能力 | 类型 | 说明 |
//! |------|------|------|
//! | 安全重试 | [`RetrySafety`] / [`retry_fn_safe`] / [`retry_async_safe`] | 多次尝试前显式声明只读/幂等 |
//! | 安全 Adapter budget | [`call_with_retry_budget_safe`] / [`call_with_retry_budget_async_safe`] | generic 返回值；首次 operation 前校验 safety |
//! | Unchecked 兼容面 | [`RetryConfig`] / [`retry_fn`] / [`retry_async`] / [`call_with_retry_budget`] / [`call_with_retry_budget_async`] | 不校验副作用安全性 |
//! | 预算 | [`RetryBudget`] | 令牌式重试上限；安全入口耗尽统一错误 |
//! | deadline | `retry_async_with_deadline`（feature `tokio`） | 整次 async retry；cooperative cancellation |
//! | 熔断 | [`CircuitBreaker`] | 三态；**无墙钟**（拒绝计数推进 HalfOpen） |
//! | 限流 | [`RateLimiter`] | 令牌桶；**无墙钟**（显式 [`RateLimiter::refill`]） |
//! | 舱壁 | [`Bulkhead`] | 并发上限；满载立即拒绝；RAII 许可 |
//! | 错误分类 | [`ErrorKind`] / [`ResiliencxError`] | 语义分类决定「值不值得重试」 |
//!
//! **仍未交付**：package stable、分布式协调、已发生副作用的自动撤销。
//!
//! 可观测性通过 [`Instrumentation`]（`instrumentationx` 契约）注入；
//! **禁止**直接依赖 `observex`，以免把 `tracing` 拖进本 crate 的依赖图。
//!
//! # 最小示例
//!
//! ```
//! use resiliencx::{
//!     NoWait, NoopInstrumentation, ResiliencxResult, RetryConfig, RetryContext, RetrySafety,
//!     retry_downcast, retry_fn_safe, retry_ok,
//! };
//!
//! fn main() -> ResiliencxResult<()> {
//!     let config = RetryConfig::fixed(3, 0);
//!     let mut operation = || Ok(retry_ok(42u8));
//!     let value = retry_fn_safe(
//!         RetryContext::new(&config, RetrySafety::ReadOnly, &NoopInstrumentation, "read-profile")
//!             .with_jitter_seed(7),
//!         &NoWait,
//!         &mut operation,
//!     )?;
//!     assert_eq!(retry_downcast::<u8>(value)?, 42);
//!     Ok(())
//! }
//! ```

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(unreachable_pub)]

pub mod budget;
pub mod bulkhead;
pub mod circuit;
pub mod error;
pub mod rate_limit;
pub mod retry;

pub use budget::{
    budget_exhausted_error, call_with_retry_budget, call_with_retry_budget_async,
    call_with_retry_budget_async_safe, call_with_retry_budget_safe, ensure_budget, RetryBudget,
};
pub use bulkhead::{Bulkhead, BulkheadConfig, BulkheadPermit};
pub use circuit::{CircuitBreaker, CircuitConfig, CircuitState};
pub use error::{BoxError, ErrorKind, ResiliencxError, ResiliencxResult};
pub use rate_limit::{RateLimitConfig, RateLimiter};
pub use retry::{
    apply_deterministic_jitter, apply_seeded_jitter, retry_async, retry_async_safe,
    retry_async_with_budget, retry_delay_ms, retry_delay_ms_with_seed, retry_downcast, retry_fn,
    retry_fn_safe, retry_fn_with_budget, retry_fn_with_wait, retry_fn_with_wait_budget, retry_ok,
    AsyncWait, Backoff, NoWait, RecordingWait, RetryConfig, RetryContext, RetrySafety, RetryValue,
    ThreadSleepWait, Wait,
};

#[cfg(feature = "tokio")]
pub use retry::{retry_async_with_deadline, TokioSleepWait};

/// 可观测性注入点——权威定义在 `instrumentationx`。
pub use instrumentationx::Instrumentation;

/// 空操作 instrumentation（生产可注入真实实现）。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopInstrumentation;

impl Instrumentation for NoopInstrumentation {
    fn record_retry(&self, _op: &str, _attempt: u32) {}
    fn record_circuit_open(&self, _op: &str) {}
    fn record_circuit_close(&self, _op: &str) {}
}
