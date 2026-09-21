//! 重试预算（Retry Budget）。
//!
//! 限制一段逻辑窗口内可消耗的重试次数，防止雪崩。与 [`crate::retry`] 组合使用。

use std::future::Future;
use std::sync::{Arc, Mutex};

use crate::error::{ResiliencxError, ResiliencxResult};

/// 预算耗尽时的标准错误。
#[must_use]
pub fn budget_exhausted_error() -> ResiliencxError {
    ResiliencxError::unavailable("重试预算已耗尽")
}

/// 若预算已耗尽则返回错误。
pub fn ensure_budget(budget: &RetryBudget) -> ResiliencxResult<()> {
    if budget.is_exhausted() {
        Err(budget_exhausted_error())
    } else {
        Ok(())
    }
}

/// 重试预算：共享令牌计数。
///
/// - `try_consume`：成功消耗返回 `true`；耗尽返回 `false`；
/// - `refund`：可选退还；
/// - `reset`：恢复满额。
///
/// 内部使用 [`Mutex`] 保证线程安全，避免 CAS 竞态分支无法单测覆盖。
#[derive(Debug, Clone)]
pub struct RetryBudget {
    remaining: Arc<Mutex<u32>>,
    capacity: u32,
}

/// async 退避期间持有的预算预留。
///
/// 未提交即 drop 时自动退还，保证 cooperative cancellation 不泄漏预算。
pub(crate) struct RetryBudgetReservation<'a> {
    budget: &'a RetryBudget,
    is_committed: bool,
}

impl RetryBudgetReservation<'_> {
    /// 确认下一次 retry 将实际发起；提交后 drop 不再退还。
    pub(crate) fn commit(mut self) {
        self.is_committed = true;
    }
}

impl Drop for RetryBudgetReservation<'_> {
    fn drop(&mut self) {
        if !self.is_committed {
            self.budget.refund();
        }
    }
}

impl RetryBudget {
    /// 构造满额预算。
    #[must_use]
    pub fn new(capacity: u32) -> Self {
        Self {
            remaining: Arc::new(Mutex::new(capacity)),
            capacity,
        }
    }

    /// 容量。
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// 剩余令牌。
    #[must_use]
    pub fn remaining(&self) -> u32 {
        *self.remaining.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// 是否已耗尽。
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.remaining() == 0
    }

    /// 尝试消耗 1 个令牌；成功 `true`，耗尽 `false`。
    pub fn try_consume(&self) -> bool {
        let mut g = self.remaining.lock().unwrap_or_else(|p| p.into_inner());
        if *g == 0 {
            return false;
        }
        *g -= 1;
        true
    }

    /// 原子预留一个 retry 令牌；预留未提交即 drop 时自动退还。
    pub(crate) fn reserve(&self) -> Option<RetryBudgetReservation<'_>> {
        if self.try_consume() {
            Some(RetryBudgetReservation {
                budget: self,
                is_committed: false,
            })
        } else {
            None
        }
    }

    /// 退还 1 个令牌（不超过 capacity）。
    pub fn refund(&self) {
        let mut g = self.remaining.lock().unwrap_or_else(|p| p.into_inner());
        if *g >= self.capacity {
            return;
        }
        *g += 1;
    }

    /// 重置为满额。
    pub fn reset(&self) {
        let mut g = self.remaining.lock().unwrap_or_else(|p| p.into_inner());
        *g = self.capacity;
    }
}

/// Adapter 调用路径：带预算的可重试同步闭包（unchecked compatibility）。
///
/// 本兼容入口不接收、也不校验 [`crate::RetrySafety`]。新的生产 adapter 接线必须使用
/// [`call_with_retry_budget_safe`]。
pub fn call_with_retry_budget<F, T>(
    budget: &RetryBudget,
    max_attempts: u32,
    op: &str,
    instr: &dyn crate::Instrumentation,
    mut f: F,
) -> ResiliencxResult<T>
where
    F: FnMut() -> ResiliencxResult<T>,
{
    if max_attempts == 0 {
        return Err(ResiliencxError::invalid("max_attempts 必须大于或等于 1"));
    }
    let mut attempt = 1u32;
    loop {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if e.is_retryable() && attempt < max_attempts => {
                if !budget.try_consume() {
                    return Err(budget_exhausted_error());
                }
                instr.record_retry(op, attempt);
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

fn validate_max_attempts(max_attempts: u32) -> ResiliencxResult<()> {
    if max_attempts == 0 {
        return Err(ResiliencxError::invalid("max_attempts 必须大于或等于 1"));
    }
    Ok(())
}

fn validate_adapter_safety(max_attempts: u32, safety: crate::RetrySafety) -> ResiliencxResult<()> {
    validate_max_attempts(max_attempts)?;
    safety.validate(&crate::RetryConfig::fixed(max_attempts, 0))
}

/// 生产安全的 generic 同步 Adapter budget 入口。
///
/// 在首次调用 `f` 前校验 [`crate::RetrySafety`]；预算消费与 attempt 观测语义和
/// [`crate::retry_fn_safe`] 一致。多次尝试的不安全副作用会在闭包执行前返回 `Invalid`。
pub fn call_with_retry_budget_safe<F, T>(
    budget: &RetryBudget,
    max_attempts: u32,
    safety: crate::RetrySafety,
    op: &str,
    instr: &dyn crate::Instrumentation,
    f: F,
) -> ResiliencxResult<T>
where
    F: FnMut() -> ResiliencxResult<T>,
{
    validate_adapter_safety(max_attempts, safety)?;
    call_with_retry_budget(budget, max_attempts, op, instr, f)
}

/// Generic 异步 Adapter budget 核心（unchecked compatibility）。
///
/// 本兼容入口不接收、也不校验 [`crate::RetrySafety`]。它只保证：零次尝试在首次 future 前返回
/// `Invalid`；每次 retry 前消费预算；预算耗尽返回 [`budget_exhausted_error`]；`record_retry`
/// 记录刚失败的 attempt（从 1 起）。新生产接线必须使用 [`call_with_retry_budget_async_safe`]。
pub async fn call_with_retry_budget_async<F, Fut, T>(
    budget: &RetryBudget,
    max_attempts: u32,
    op: &str,
    instr: &dyn crate::Instrumentation,
    mut f: F,
) -> ResiliencxResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ResiliencxResult<T>>,
{
    validate_max_attempts(max_attempts)?;

    let mut attempt = 1u32;
    loop {
        match f().await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_retryable() && attempt < max_attempts => {
                if !budget.try_consume() {
                    return Err(budget_exhausted_error());
                }
                instr.record_retry(op, attempt);
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// 生产安全的 generic 异步 Adapter budget 入口。
///
/// 在构造首次 operation future 前校验 [`crate::RetrySafety`]，再委托
/// [`call_with_retry_budget_async`] 执行统一的预算、错误与 attempt 语义。
pub async fn call_with_retry_budget_async_safe<F, Fut, T>(
    budget: &RetryBudget,
    max_attempts: u32,
    safety: crate::RetrySafety,
    op: &str,
    instr: &dyn crate::Instrumentation,
    f: F,
) -> ResiliencxResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ResiliencxResult<T>>,
{
    validate_adapter_safety(max_attempts, safety)?;
    call_with_retry_budget_async(budget, max_attempts, op, instr, f).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::{Instrumentation, NoopInstrumentation, RetrySafety};

    #[derive(Default)]
    struct RetryAttempts {
        retries: Mutex<Vec<u32>>,
        opens: Mutex<Vec<String>>,
        closes: Mutex<Vec<String>>,
    }

    impl Instrumentation for RetryAttempts {
        fn record_retry(&self, _op: &str, attempt: u32) {
            self.retries.lock().expect("记录重试事件").push(attempt);
        }

        fn record_circuit_open(&self, op: &str) {
            self.opens
                .lock()
                .expect("记录熔断开启事件")
                .push(op.to_owned());
        }

        fn record_circuit_close(&self, op: &str) {
            self.closes
                .lock()
                .expect("记录熔断关闭事件")
                .push(op.to_owned());
        }
    }

    #[test]
    fn budget_consume_and_exhaust() {
        let b = RetryBudget::new(2);
        assert_eq!(b.capacity(), 2);
        assert!(b.try_consume());
        assert!(b.try_consume());
        assert!(!b.try_consume());
        assert!(b.is_exhausted());
        ensure_budget(&b).unwrap_err();
        b.refund();
        assert_eq!(b.remaining(), 1);
        b.reset();
        assert_eq!(b.remaining(), 2);
        // 满额时 refund 不超容
        b.refund();
        assert_eq!(b.remaining(), 2);
        assert_eq!(b.capacity(), 2);
        assert_eq!(budget_exhausted_error().kind(), ErrorKind::Unavailable);
    }

    #[test]
    fn call_with_retry_budget_path() {
        let b = RetryBudget::new(1);
        let mut n = 0u32;
        let err = call_with_retry_budget(&b, 5, "op", &NoopInstrumentation, || {
            n += 1;
            Err::<(), _>(ResiliencxError::transient("t"))
        })
        .unwrap_err();
        assert!(n >= 2);
        assert!(matches!(
            err.kind(),
            ErrorKind::Unavailable | ErrorKind::Transient
        ));

        // 预算充足、尝试次数用尽 → 走循环结束后的 last_err 返回
        let b2 = RetryBudget::new(10);
        let mut n2 = 0u32;
        let err2 = call_with_retry_budget(&b2, 3, "ex", &NoopInstrumentation, || {
            n2 += 1;
            Err::<(), _>(ResiliencxError::transient("t"))
        })
        .unwrap_err();
        assert_eq!(n2, 3);
        assert_eq!(err2.kind(), ErrorKind::Transient);
    }

    #[test]
    fn call_with_retry_budget_ok_and_non_retryable() {
        let b = RetryBudget::new(3);
        let v = call_with_retry_budget(&b, 3, "ok", &NoopInstrumentation, || Ok(42)).unwrap();
        assert_eq!(v, 42);
        let err = call_with_retry_budget(&b, 3, "nr", &NoopInstrumentation, || {
            Err::<(), _>(ResiliencxError::invalid("nope"))
        })
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Invalid);
        let zero = call_with_retry_budget(&b, 0, "z", &NoopInstrumentation, || Ok(())).unwrap_err();
        assert_eq!(zero.kind(), ErrorKind::Invalid);
    }

    #[test]
    fn budget_recovers_from_poison() {
        let b = RetryBudget::new(1);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = b.remaining.lock().expect("lock");
            panic!("poison budget");
        }));
        assert!(b.try_consume());
        assert!(b.is_exhausted());
        b.refund();
        assert_eq!(b.remaining(), 1);
        b.reset();
        assert_eq!(b.remaining(), 1);
    }

    #[test]
    fn call_budget_exhaustion_is_standard_and_observes_failed_attempt() {
        let budget = RetryBudget::new(1);
        let attempts = RetryAttempts::default();
        let mut calls = 0u32;

        let err = call_with_retry_budget(&budget, 5, "op", &attempts, || {
            calls += 1;
            Err::<(), _>(ResiliencxError::transient("稍后重试"))
        })
        .expect_err("预算应在第三次调用前耗尽");

        assert_eq!(calls, 2);
        assert_eq!(err.to_string(), budget_exhausted_error().to_string());
        assert_eq!(*attempts.retries.lock().expect("读取重试事件"), vec![1]);
        attempts.record_circuit_open("circuit-a");
        attempts.record_circuit_close("circuit-a");
        assert_eq!(
            *attempts.opens.lock().expect("读取熔断开启事件"),
            vec!["circuit-a".to_owned()]
        );
        assert_eq!(
            *attempts.closes.lock().expect("读取熔断关闭事件"),
            vec!["circuit-a".to_owned()]
        );
    }

    #[test]
    fn reservation_refunds_on_drop_and_commit_consumes_once() {
        let budget = RetryBudget::new(1);
        let reservation = budget.reserve().expect("预留");
        assert_eq!(budget.remaining(), 0);
        drop(reservation);
        assert_eq!(budget.remaining(), 1);

        budget.reserve().expect("再次预留").commit();
        assert_eq!(budget.remaining(), 0);
        assert!(budget.reserve().is_none());
        assert_eq!(
            budget.remaining(),
            0,
            "失败的预留不得凭空 refund 未消费令牌"
        );
    }

    #[test]
    fn safe_adapter_sync_rejects_unsafe_before_operation_and_allows_read_only() {
        let budget = RetryBudget::new(2);
        let zero_calls = std::cell::Cell::new(0u32);
        let zero_operation = || {
            zero_calls.set(zero_calls.get() + 1);
            Ok::<(), ResiliencxError>(())
        };
        zero_operation().expect("控制探针可执行");
        zero_calls.set(0);
        let err = call_with_retry_budget_safe(
            &budget,
            0,
            RetrySafety::ReadOnly,
            "adapter.zero",
            &NoopInstrumentation,
            zero_operation,
        )
        .expect_err("零次尝试必须在闭包前拒绝");
        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert_eq!(zero_calls.get(), 0);

        let unsafe_calls = std::cell::Cell::new(0u32);
        let unsafe_operation = || {
            unsafe_calls.set(unsafe_calls.get() + 1);
            Ok::<(), ResiliencxError>(())
        };
        unsafe_operation().expect("控制探针可执行");
        unsafe_calls.set(0);
        let err = call_with_retry_budget_safe(
            &budget,
            2,
            RetrySafety::UnsafeSideEffect,
            "adapter.write",
            &NoopInstrumentation,
            unsafe_operation,
        )
        .expect_err("多次不安全副作用必须在闭包前拒绝");
        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert_eq!(unsafe_calls.get(), 0);

        let mut read_calls = 0u32;
        let value = call_with_retry_budget_safe(
            &budget,
            2,
            RetrySafety::ReadOnly,
            "adapter.read",
            &NoopInstrumentation,
            || {
                read_calls += 1;
                if read_calls == 1 {
                    Err(ResiliencxError::transient("暂时失败"))
                } else {
                    Ok(7u8)
                }
            },
        )
        .expect("只读操作可安全重试");
        assert_eq!(value, 7);
        assert_eq!(read_calls, 2);
        assert_eq!(budget.remaining(), 1);
    }

    #[tokio::test]
    async fn safe_adapter_async_validates_before_future_and_uses_standard_budget_error() {
        let budget = RetryBudget::new(0);
        let constructed = std::cell::Cell::new(0u32);
        let unsafe_operation = || {
            constructed.set(constructed.get() + 1);
            async { Ok::<_, ResiliencxError>(()) }
        };
        unsafe_operation().await.expect("控制探针可执行");
        constructed.set(0);
        let err = call_with_retry_budget_async_safe(
            &budget,
            3,
            RetrySafety::UnsafeSideEffect,
            "adapter.async.write",
            &NoopInstrumentation,
            unsafe_operation,
        )
        .await
        .expect_err("必须在构造首次 future 前拒绝");
        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert_eq!(constructed.get(), 0);

        let mut attempts = 0u32;
        let err = call_with_retry_budget_async_safe(
            &budget,
            2,
            RetrySafety::Idempotent,
            "adapter.async.idempotent",
            &NoopInstrumentation,
            || {
                attempts += 1;
                async { Err::<(), _>(ResiliencxError::transient("暂时失败")) }
            },
        )
        .await
        .expect_err("预算耗尽");
        assert_eq!(err.to_string(), budget_exhausted_error().to_string());
        assert_eq!(attempts, 1);

        let non_retryable_budget = RetryBudget::new(1);
        let mut non_retryable_attempts = 0u32;
        let err = call_with_retry_budget_async_safe(
            &non_retryable_budget,
            2,
            RetrySafety::ReadOnly,
            "adapter.async.invalid",
            &NoopInstrumentation,
            || {
                non_retryable_attempts += 1;
                async { Err::<(), _>(ResiliencxError::invalid("请求无效")) }
            },
        )
        .await
        .expect_err("不可重试错误必须原样返回");
        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert_eq!(non_retryable_attempts, 1);
        assert_eq!(non_retryable_budget.remaining(), 1);

        let success_budget = RetryBudget::new(1);
        let mut success_attempts = 0u32;
        let value = call_with_retry_budget_async_safe(
            &success_budget,
            2,
            RetrySafety::Idempotent,
            "adapter.async.idempotent",
            &NoopInstrumentation,
            || {
                success_attempts += 1;
                let attempt = success_attempts;
                async move {
                    if attempt == 1 {
                        Err(ResiliencxError::transient("暂时失败"))
                    } else {
                        Ok(attempt)
                    }
                }
            },
        )
        .await
        .expect("幂等异步操作可安全重试");
        assert_eq!(value, 2);
        assert_eq!(success_budget.remaining(), 0);
    }

    #[tokio::test]
    async fn unchecked_async_budget_core_uses_standard_error_and_failed_attempt_numbers() {
        let empty_budget = RetryBudget::new(0);
        let empty_attempts = RetryAttempts::default();
        let mut empty_calls = 0u32;
        let err = call_with_retry_budget_async(
            &empty_budget,
            3,
            "adapter.async.unchecked",
            &empty_attempts,
            || {
                empty_calls += 1;
                async { Err::<(), _>(ResiliencxError::transient("暂时失败")) }
            },
        )
        .await
        .expect_err("预算耗尽必须返回标准错误");
        assert_eq!(err.to_string(), budget_exhausted_error().to_string());
        assert_eq!(empty_calls, 1);
        assert!(empty_attempts
            .retries
            .lock()
            .expect("读取重试事件")
            .is_empty());

        let budget = RetryBudget::new(2);
        let attempts = RetryAttempts::default();
        let mut calls = 0u32;
        let value =
            call_with_retry_budget_async(&budget, 3, "adapter.async.unchecked", &attempts, || {
                calls += 1;
                let attempt = calls;
                async move {
                    if attempt < 3 {
                        Err(ResiliencxError::transient("暂时失败"))
                    } else {
                        Ok(attempt)
                    }
                }
            })
            .await
            .expect("第三次尝试成功");
        assert_eq!(value, 3);
        assert_eq!(budget.remaining(), 0);
        assert_eq!(*attempts.retries.lock().expect("读取重试事件"), vec![1, 2]);
    }
}
