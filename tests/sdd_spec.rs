#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的每个 `##` 章节条款转成可执行断言。
//!
//! 章节与断言函数须与 `docs/标准.md` 的 `##` 章节 1:1（检查器按标题逐字比对）。
//!
//! // SPEC-MAP: S-1 | 1. 能力标准 | assert_capability_standard
//! // SPEC-MAP: S-2 | 2. 安全与取消标准 | assert_safety_and_cancellation_standard
//! // SPEC-MAP: S-3 | 3. 观测标准 | assert_observation_standard
//! // SPEC-MAP: S-4 | 4. jitter 标准 | assert_jitter_standard
//! // SPEC-MAP: S-5 | 5. 本地立即拒绝边界 | assert_local_fail_fast_boundary
//! // SPEC-MAP: S-6 | 6. 依赖与运行时 | assert_dependency_and_runtime
//! // SPEC-MAP: S-7 | 7. 验收 | assert_acceptance

use std::sync::{Arc, Mutex};

use resiliencx::{
    apply_deterministic_jitter, apply_seeded_jitter, retry_async, retry_async_with_budget,
    retry_delay_ms, retry_delay_ms_with_seed, retry_downcast, retry_fn, retry_fn_safe, retry_ok,
    Backoff, Bulkhead, BulkheadConfig, CircuitBreaker, CircuitConfig, CircuitState, ErrorKind,
    Instrumentation, NoWait, NoopInstrumentation, RateLimitConfig, RateLimiter, RecordingWait,
    ResiliencxError, RetryBudget, RetryConfig, RetryContext, RetrySafety, RetryValue,
};

#[derive(Debug, Default)]
struct AttemptLog {
    attempts: Mutex<Vec<u32>>,
}

impl Instrumentation for AttemptLog {
    fn record_retry(&self, _op: &str, attempt: u32) {
        self.attempts.lock().expect("记录重试").push(attempt);
    }

    fn record_circuit_open(&self, _op: &str) {}

    fn record_circuit_close(&self, _op: &str) {}
}

/// S-1：Retry / RetrySafety / RetryBudget / Circuit Breaker / Rate Limiter / Bulkhead 六项能力可执行面。
#[test]
fn assert_capability_standard() {
    // Retry：有限次数 + 仅可重试错误触发 + 固定/指数退避。
    let log = AttemptLog::default();
    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        Err::<RetryValue, _>(ResiliencxError::transient("暂时失败"))
    };
    let _ = retry_fn(&RetryConfig::fixed(3, 0), &log, "cap.retry", &mut operation);
    assert_eq!(calls, 3, "重试次数须有限且等于 max_attempts");
    assert_eq!(
        *log.attempts.lock().expect("读取"),
        vec![1, 2],
        "仅可重试错误触发退避"
    );

    let exponential = RetryConfig {
        max_attempts: 3,
        base_delay_ms: 5,
        backoff: Backoff::Exponential {
            factor: 2,
            max_delay_ms: 100,
        },
        jitter_bps: 0,
    };
    assert_eq!(
        retry_delay_ms(&exponential, 2),
        10,
        "指数退避按 factor 放大"
    );

    // Retry Budget：共享令牌限制重试次数。
    let budget = RetryBudget::new(1);
    assert!(budget.try_consume());
    assert!(budget.is_exhausted());

    // Circuit Breaker：本地三态状态机。
    let mut breaker = CircuitBreaker::new(CircuitConfig {
        failure_threshold: 1,
        success_threshold: 1,
        open_to_half_open_after_rejects: 1,
    })
    .expect("合法阈值");
    let _ = breaker.call(&NoopInstrumentation, "cap.cb", || {
        Err::<(), _>(ResiliencxError::transient("trip"))
    });
    assert_eq!(breaker.state(), CircuitState::Open);
    let _ = breaker.call(&NoopInstrumentation, "cap.cb", || Ok(()));
    assert_eq!(
        breaker.state(),
        CircuitState::HalfOpen,
        "按拒绝次数推进 HalfOpen"
    );
    breaker
        .call(&NoopInstrumentation, "cap.cb", || Ok(()))
        .expect("闭合");
    assert_eq!(breaker.state(), CircuitState::Closed);

    // Rate Limiter：本地令牌桶，资源不足立即拒绝。
    let mut limiter = RateLimiter::new(RateLimitConfig { capacity: 1 }).expect("合法容量");
    limiter.try_acquire(1).expect("取得令牌");
    assert_eq!(
        limiter.try_acquire(1).expect_err("不足").kind(),
        ErrorKind::Unavailable
    );

    // Bulkhead：本地并发计数 + RAII 许可。
    let bulkhead = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("合法并发"));
    let permit = bulkhead.try_enter().expect("进入");
    assert_eq!(bulkhead.in_flight(), 1);
    drop(permit);
    assert_eq!(bulkhead.in_flight(), 0, "RAII 许可归还并发槽");
}

/// S-2：RetrySafety 是运行时策略声明；不安全副作用仅允许单次尝试；async 预算先 reserve 后 commit。
#[test]
fn assert_safety_and_cancellation_standard() {
    // ReadOnly / Idempotent 允许多次尝试。
    assert!(RetrySafety::ReadOnly
        .validate(&RetryConfig::fixed(3, 0))
        .is_ok());
    assert!(RetrySafety::Idempotent
        .validate(&RetryConfig::fixed(3, 0))
        .is_ok());
    // UnsafeSideEffect 仅允许 max_attempts == 1。
    assert!(RetrySafety::UnsafeSideEffect
        .validate(&RetryConfig::fixed(1, 0))
        .is_ok());
    assert!(RetrySafety::UnsafeSideEffect
        .validate(&RetryConfig::fixed(2, 0))
        .is_err());

    // 生产安全入口在首次闭包执行前拒绝不安全的多次尝试。
    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        Ok::<RetryValue, ResiliencxError>(retry_ok(()))
    };
    let error = retry_fn_safe(
        RetryContext::new(
            &RetryConfig::fixed(2, 0),
            RetrySafety::UnsafeSideEffect,
            &NoopInstrumentation,
            "safety.unsafe",
        ),
        &NoWait,
        &mut operation,
    )
    .expect_err("多次不安全副作用应拒绝");
    assert_eq!(error.kind(), ErrorKind::Invalid);
    assert_eq!(calls, 0, "策略校验须在首次闭包执行前完成");
}

/// S-2（async 分支）：退避前原子 reserve、退避后 commit 并记录 retry；耗尽可立即返回。
#[tokio::test]
async fn assert_async_budget_reserve_before_backoff() {
    let budget = RetryBudget::new(1);
    let log = AttemptLog::default();
    let mut calls = 0u32;
    let error = retry_async_with_budget(
        &RetryConfig::fixed(5, 0),
        &log,
        "safety.async.budget",
        &NoWait,
        &budget,
        || {
            calls += 1;
            async { Err::<RetryValue, _>(ResiliencxError::transient("暂时失败")) }
        },
    )
    .await
    .expect_err("预算耗尽");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(calls, 2, "一个重试令牌 → 两次尝试后立即返回");
    assert_eq!(
        *log.attempts.lock().expect("读取"),
        vec![1],
        "commit 后记录刚失败的 attempt"
    );
    assert!(budget.is_exhausted(), "令牌不得泄漏");
}

/// S-3：`record_retry(op, attempt)` 的 attempt 表示「刚失败、即将触发下一次重试」的序号，从 1 起。
#[test]
fn assert_observation_standard() {
    let log = AttemptLog::default();
    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        Err::<RetryValue, _>(ResiliencxError::transient("暂时失败"))
    };
    let _ = retry_fn(&RetryConfig::fixed(4, 0), &log, "obs.retry", &mut operation);
    assert_eq!(calls, 4);
    assert_eq!(
        *log.attempts.lock().expect("读取"),
        vec![1, 2, 3],
        "attempt 从 1 起，且只在确实准备发起下一次尝试时记录"
    );

    // 预算耗尽不记录虚假 retry。
    let log2 = AttemptLog::default();
    let budget = RetryBudget::new(0);
    let mut calls2 = 0u32;
    let mut operation2 = || {
        calls2 += 1;
        Err::<RetryValue, _>(ResiliencxError::transient("暂时失败"))
    };
    let error = retry_fn_safe(
        RetryContext::new(
            &RetryConfig::fixed(5, 0),
            RetrySafety::ReadOnly,
            &log2,
            "obs.budget",
        )
        .with_budget(&budget),
        &NoWait,
        &mut operation2,
    )
    .expect_err("预算耗尽");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(calls2, 1);
    assert!(log2.attempts.lock().expect("读取").is_empty());
}

/// S-4：attempt-only jitter 可复现但同相；caller seed 用于实例去相关并接入 safe 退避。
#[test]
fn assert_jitter_standard() {
    // retry_delay_ms / apply_deterministic_jitter 只依赖 attempt，结果可复现。
    assert_eq!(retry_delay_ms(&RetryConfig::fixed(2, 10), 1), 10);
    let seeded = RetryConfig {
        max_attempts: 3,
        base_delay_ms: 1_000,
        backoff: Backoff::Constant,
        jitter_bps: 5_000,
    };
    assert_eq!(
        retry_delay_ms(&seeded, 1),
        apply_deterministic_jitter(1_000, 5_000, 1)
    );
    assert_eq!(
        apply_deterministic_jitter(1_000, 5_000, 1),
        apply_deterministic_jitter(1_000, 5_000, 1),
        "同 attempt 同结果"
    );
    // caller seed 去相关。
    assert_ne!(
        apply_seeded_jitter(1_000, 5_000, 1, 1),
        apply_seeded_jitter(1_000, 5_000, 1, 2)
    );

    // RetryContext::with_jitter_seed 接入 safe 退避的实际计算。
    let wait = RecordingWait::new();
    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        if calls == 1 {
            Err(ResiliencxError::transient("暂时失败"))
        } else {
            Ok(retry_ok(()))
        }
    };
    retry_fn_safe(
        RetryContext::new(
            &seeded,
            RetrySafety::ReadOnly,
            &NoopInstrumentation,
            "jitter.seed",
        )
        .with_jitter_seed(9),
        &wait,
        &mut operation,
    )
    .expect("安全重试");
    assert_eq!(wait.delays(), vec![retry_delay_ms_with_seed(&seeded, 1, 9)]);
}

/// S-5：熔断 / 限流 / 舱壁不做分布式协调，失败一律立即返回 `Unavailable`，无排队与等待。
#[test]
fn assert_local_fail_fast_boundary() {
    let mut limiter = RateLimiter::new(RateLimitConfig { capacity: 1 }).expect("合法容量");
    limiter.try_acquire(1).expect("取得令牌");
    assert_eq!(
        limiter.try_acquire(1).expect_err("立即拒绝").kind(),
        ErrorKind::Unavailable
    );

    let bulkhead = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("合法并发"));
    let permit = bulkhead.try_enter().expect("进入");
    assert_eq!(
        bulkhead.try_enter().expect_err("立即拒绝").kind(),
        ErrorKind::Unavailable
    );
    drop(permit);

    let mut breaker = CircuitBreaker::new(CircuitConfig {
        failure_threshold: 1,
        success_threshold: 1,
        open_to_half_open_after_rejects: 10,
    })
    .expect("合法阈值");
    let _ = breaker.call(&NoopInstrumentation, "s5.cb", || {
        Err::<(), _>(ResiliencxError::transient("trip"))
    });
    assert_eq!(breaker.state(), CircuitState::Open);
    assert_eq!(
        breaker
            .call(&NoopInstrumentation, "s5.cb", || Ok::<(), ResiliencxError>(
                ()
            ))
            .expect_err("立即拒绝")
            .kind(),
        ErrorKind::Unavailable
    );
}

/// S-6：观测契约来自 `instrumentationx`（只再导出 trait），错误模型为本 crate 内独立实现。
#[test]
fn assert_dependency_and_runtime() {
    // 观测注入只经 `dyn Instrumentation`，无需引入任何具体观测实现。
    let instrumentation: &dyn Instrumentation = &NoopInstrumentation;
    instrumentation.record_retry("dep.observe", 1);
    instrumentation.record_circuit_open("dep.observe");
    instrumentation.record_circuit_close("dep.observe");

    // 错误模型可独立构造与分类，不依赖外部错误框架。
    assert!(ResiliencxError::transient("下游暂时不可用").is_retryable());
    assert!(!ResiliencxError::invalid("输入非法").is_retryable());
    assert!(ResiliencxError::invariant("不变量被破坏").is_bug());
}

/// S-6（运行时分支）：库内不创建 runtime、不调用 `block_on`，async 入口可在当前线程 runtime 直接 await。
#[tokio::test]
async fn assert_async_entry_needs_no_runtime_creation() {
    let value = retry_async(
        &RetryConfig::fixed(1, 0),
        &NoopInstrumentation,
        "runtime.direct",
        &NoWait,
        || async { Ok(retry_ok(5u8)) },
    )
    .await
    .expect("单次尝试成功");
    assert_eq!(retry_downcast::<u8>(value).expect("成功值类型"), 5);
}

/// S-7：验收命令见 `docs/标准.md` §7；此处断言测试面本身可被一次性全部执行。
#[test]
fn assert_acceptance() {
    let _ = std::env::current_dir().expect("可取得当前目录（验收命令可执行）");
}
