#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）。
//!
//! 入口集合 = `specs/002-*/contracts/public-api-contract.md` 中 `resiliencx` 全部入口。
//! 下表每个入口先在变异副本上观测应红、再在本树观测绿；实际执行的变异与红绿结果
//! 见 PR 描述（变异描述 + 失败用例名 + 复现命令）。
//!
//! 本文件聚焦**统一入口与既有契约测试未覆盖的形态**，不复刻 `retry_contract.rs` /
//! `retry_async_contract.rs` 已有的断言。
//!
//! // TDD-PROBE: retry::retry_fn | 变异：record_retry 恒记 attempt=0 | 红=retry_fn_records_attempt_sequence | 绿=retry_fn_records_attempt_sequence
//! // TDD-PROBE: retry::retry_fn_safe | 变异：忽略 context.budget 直接重试 | 红=retry_fn_safe_consumes_context_budget | 绿=retry_fn_safe_consumes_context_budget
//! // TDD-PROBE: retry::retry_async | 变异：异步路径少记一次失败 | 红=retry_async_returns_last_error_and_records_attempts | 绿=retry_async_returns_last_error_and_records_attempts
//! // TDD-PROBE: RetryContext::new | 变异：Debug 丢弃 op 与有预算标记 | 红=retry_context_new_wires_common_policy | 绿=retry_context_new_wires_common_policy
//! // TDD-PROBE: RetryBudget::new | 变异：构造即耗尽（remaining 恒 0） | 红=retry_budget_new_starts_full | 绿=retry_budget_new_starts_full
//! // TDD-PROBE: RetryBudget::try_consume | 变异：耗尽后仍返回 true | 红=retry_budget_try_consume_decrements_and_fails_at_zero | 绿=retry_budget_try_consume_decrements_and_fails_at_zero
//! // TDD-PROBE: RetryBudget::refund | 变异：归还不设容量上限 | 红=retry_budget_refund_never_exceeds_capacity | 绿=retry_budget_refund_never_exceeds_capacity
//! // TDD-PROBE: CircuitBreaker::call | 变异：Open 下仍调用业务闭包 | 红=circuit_breaker_call_short_circuits_when_open | 绿=circuit_breaker_call_short_circuits_when_open
//! // TDD-PROBE: RateLimiter::try_acquire | 变异：令牌不足时部分扣减 | 红=rate_limiter_try_acquire_rejects_without_partial_deduction | 绿=rate_limiter_try_acquire_rejects_without_partial_deduction
//! // TDD-PROBE: RateLimiter::refill | 变异：补充可超过容量 | 红=rate_limiter_refill_caps_at_capacity | 绿=rate_limiter_refill_caps_at_capacity
//! // TDD-PROBE: Bulkhead::try_enter | 变异：许可 drop 不归还并发槽 | 红=bulkhead_try_enter_permit_raii_returns_slot | 绿=bulkhead_try_enter_permit_raii_returns_slot
//! // TDD-PROBE: NoopInstrumentation | 变异：空操作实现改变重试语义 | 红=noop_instrumentation_is_a_valid_instrumentation | 绿=noop_instrumentation_is_a_valid_instrumentation

use std::sync::{Arc, Mutex};

use resiliencx::{
    retry_async, retry_fn, retry_fn_safe, Bulkhead, BulkheadConfig, CircuitBreaker, CircuitConfig,
    CircuitState, ErrorKind, Instrumentation, NoWait, NoopInstrumentation, RateLimitConfig,
    RateLimiter, ResiliencxError, RetryBudget, RetryConfig, RetryContext, RetrySafety, RetryValue,
};

/// 记录 attempt 序号与熔断迁移的观测替身。
#[derive(Debug, Default)]
struct AttemptLog {
    attempts: Mutex<Vec<u32>>,
    opens: Mutex<Vec<String>>,
    closes: Mutex<Vec<String>>,
}

impl Instrumentation for AttemptLog {
    fn record_retry(&self, _op: &str, attempt: u32) {
        self.attempts.lock().expect("记录重试").push(attempt);
    }

    fn record_circuit_open(&self, op: &str) {
        self.opens.lock().expect("记录熔断开启").push(op.to_owned());
    }

    fn record_circuit_close(&self, op: &str) {
        self.closes
            .lock()
            .expect("记录熔断关闭")
            .push(op.to_owned());
    }
}

fn recorded_attempts(log: &AttemptLog) -> Vec<u32> {
    log.attempts.lock().expect("读取重试记录").clone()
}

/// `retry::retry_fn`：耗尽后返回最后一次原始错误，且 attempt 观测序号从 1 起、只记真正发起的重试。
#[test]
fn retry_fn_records_attempt_sequence() {
    let log = AttemptLog::default();
    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        Err::<RetryValue, _>(ResiliencxError::transient("暂时失败"))
    };
    let error = retry_fn(
        &RetryConfig::fixed(3, 0),
        &log,
        "contract.retry_fn",
        &mut operation,
    )
    .expect_err("三次尝试全部失败应返回最后错误");
    assert_eq!(calls, 3, "尝试次数须等于 max_attempts");
    assert_eq!(error.kind(), ErrorKind::Transient);
    assert_eq!(
        recorded_attempts(&log),
        vec![1, 2],
        "仅两次真正发起重试，序号从 1 起"
    );
}

/// `retry::retry_fn_safe`：`RetryContext` 注入的预算对 safe 入口同样生效。
#[test]
fn retry_fn_safe_consumes_context_budget() {
    let budget = RetryBudget::new(1);
    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        Err::<RetryValue, _>(ResiliencxError::transient("暂时失败"))
    };
    let error = retry_fn_safe(
        RetryContext::new(
            &RetryConfig::fixed(5, 0),
            RetrySafety::ReadOnly,
            &NoopInstrumentation,
            "contract.safe",
        )
        .with_budget(&budget),
        &NoWait,
        &mut operation,
    )
    .expect_err("预算耗尽");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(calls, 2, "只允许消费一次重试令牌");
    assert!(budget.is_exhausted());
}

/// `retry::retry_async`：语义与同步入口一致（最后一次错误 + 一次 attempt 观测）。
#[tokio::test]
async fn retry_async_returns_last_error_and_records_attempts() {
    let log = AttemptLog::default();
    let mut calls = 0u32;
    let error = retry_async(
        &RetryConfig::fixed(2, 0),
        &log,
        "contract.async",
        &NoWait,
        || {
            calls += 1;
            async { Err::<RetryValue, _>(ResiliencxError::transient("暂时失败")) }
        },
    )
    .await
    .expect_err("两次失败后返回最后错误");
    assert_eq!(calls, 2);
    assert_eq!(error.kind(), ErrorKind::Transient);
    assert_eq!(recorded_attempts(&log), vec![1]);
}

/// `RetryContext::new`：共同策略（config / safety / op / budget / seed）经 builder 完整接线。
#[test]
fn retry_context_new_wires_common_policy() {
    let budget = RetryBudget::new(2);
    let config = RetryConfig::fixed(2, 0);
    let context = RetryContext::new(
        &config,
        RetrySafety::ReadOnly,
        &NoopInstrumentation,
        "contract.context",
    )
    .with_budget(&budget)
    .with_jitter_seed(11);
    let debug = format!("{context:?}");
    assert!(
        debug.contains("contract.context"),
        "op 应进入上下文: {debug}"
    );
    assert!(debug.contains("has_budget: true"), "预算应被接线: {debug}");
    assert!(debug.contains("jitter_seed: 11"), "seed 应被接线: {debug}");
}

/// `RetryBudget::new`：满额起步。
#[test]
fn retry_budget_new_starts_full() {
    let budget = RetryBudget::new(3);
    assert_eq!(budget.capacity(), 3);
    assert_eq!(budget.remaining(), 3);
    assert!(!budget.is_exhausted());
}

/// `RetryBudget::try_consume`：逐次扣减，耗尽即拒绝。
#[test]
fn retry_budget_try_consume_decrements_and_fails_at_zero() {
    let budget = RetryBudget::new(2);
    assert!(budget.try_consume());
    assert!(budget.try_consume());
    assert!(!budget.try_consume(), "耗尽后必须拒绝");
    assert_eq!(budget.remaining(), 0);
    assert!(budget.is_exhausted());
}

/// `RetryBudget::refund`：归还不超过容量。
#[test]
fn retry_budget_refund_never_exceeds_capacity() {
    let budget = RetryBudget::new(2);
    budget.refund();
    assert_eq!(budget.remaining(), 2, "满额时归还不超容");
    assert!(budget.try_consume());
    budget.refund();
    assert_eq!(budget.remaining(), 2);
}

/// `CircuitBreaker::call`：Open 状态短路拒绝，业务闭包不得被调用。
#[test]
fn circuit_breaker_call_short_circuits_when_open() {
    let log = AttemptLog::default();
    let mut breaker = CircuitBreaker::new(CircuitConfig {
        failure_threshold: 1,
        success_threshold: 1,
        open_to_half_open_after_rejects: 5,
    })
    .expect("合法阈值");
    let _ = breaker.call(&log, "contract.circuit", || {
        Err::<(), _>(ResiliencxError::transient("trip"))
    });
    assert_eq!(breaker.state(), CircuitState::Open);

    let invoked = std::cell::Cell::new(false);
    let error = breaker
        .call(&log, "contract.circuit", || {
            invoked.set(true);
            Ok::<(), ResiliencxError>(())
        })
        .expect_err("打开状态应短路拒绝");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(!invoked.get(), "短路时业务闭包不得被调用");
    assert_eq!(log.opens.lock().expect("读取熔断开启").len(), 1);
}

/// `RateLimiter::try_acquire`：令牌不足立即拒绝且不做部分扣减；`n == 0` 为空操作。
#[test]
fn rate_limiter_try_acquire_rejects_without_partial_deduction() {
    let mut limiter = RateLimiter::new(RateLimitConfig { capacity: 3 }).expect("合法容量");
    limiter.try_acquire(0).expect("获取 0 个是空操作");
    assert_eq!(limiter.available(), 3);
    limiter.try_acquire(1).expect("获取 1 个");
    let error = limiter.try_acquire(3).expect_err("令牌不足应拒绝");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(limiter.available(), 2, "不足时不得部分扣减");
}

/// `RateLimiter::refill`：显式补充且封顶于容量。
#[test]
fn rate_limiter_refill_caps_at_capacity() {
    let mut limiter = RateLimiter::new(RateLimitConfig { capacity: 2 }).expect("合法容量");
    limiter.try_acquire(2).expect("清空令牌");
    assert_eq!(limiter.available(), 0);
    limiter.refill(100);
    assert_eq!(limiter.available(), 2, "补充不得超过容量");
}

/// `Bulkhead::try_enter`：满载立即拒绝；RAII 许可 drop 后归还并发槽。
#[test]
fn bulkhead_try_enter_permit_raii_returns_slot() {
    let bulkhead = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("合法并发"));
    let permit = bulkhead.try_enter().expect("首次进入");
    assert_eq!(bulkhead.in_flight(), 1);
    let error = bulkhead.try_enter().expect_err("满载应立即拒绝");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    drop(permit);
    assert_eq!(bulkhead.in_flight(), 0);
    bulkhead.try_enter().expect("归还后可再次进入");
}

/// `NoopInstrumentation`：空操作实现可构造、可派生，且不改变重试语义。
#[test]
fn noop_instrumentation_is_a_valid_instrumentation() {
    let noop: &dyn Instrumentation = &NoopInstrumentation;
    noop.record_retry("contract.noop", 1);
    noop.record_circuit_open("contract.noop");
    noop.record_circuit_close("contract.noop");

    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        Err::<RetryValue, _>(ResiliencxError::transient("暂时失败"))
    };
    let error = retry_fn(
        &RetryConfig::fixed(2, 0),
        &NoopInstrumentation,
        "contract.noop",
        &mut operation,
    )
    .expect_err("空观测实现下仍应按配置重试一次");
    assert_eq!(calls, 2);
    assert_eq!(error.kind(), ErrorKind::Transient);
    assert!(format!("{NoopInstrumentation:?}").contains("NoopInstrumentation"));
}
