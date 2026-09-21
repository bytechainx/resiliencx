#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! resiliencx 公开面补全：Wait 实现、jitter、bulkhead 查询、rate capacity。

use resiliencx::{
    apply_deterministic_jitter, apply_seeded_jitter, call_with_retry_budget_async,
    call_with_retry_budget_async_safe, call_with_retry_budget_safe, retry_delay_ms,
    retry_delay_ms_with_seed, retry_fn_safe, retry_fn_with_wait, retry_ok, Backoff, Bulkhead,
    BulkheadConfig, CircuitBreaker, CircuitConfig, CircuitState, NoWait, NoopInstrumentation,
    RateLimitConfig, RateLimiter, RecordingWait, RetryBudget, RetryConfig, RetryContext,
    RetrySafety, ThreadSleepWait, Wait,
};
use resiliencx::{ErrorKind, ResiliencxError};
use std::sync::Arc;

#[test]
fn wait_impls_and_delay_helpers() {
    Wait::wait_ms(&ThreadSleepWait, 0);
    Wait::wait_ms(&NoWait, 99_000);
    let rec = RecordingWait::new();
    rec.wait_ms(7);
    assert_eq!(rec.delays(), vec![7]);

    let fixed = RetryConfig::fixed(3, 10);
    assert_eq!(retry_delay_ms(&fixed, 1), 10);
    let exp = RetryConfig {
        max_attempts: 5,
        base_delay_ms: 10,
        backoff: Backoff::Exponential {
            factor: 2,
            max_delay_ms: 100,
        },
        jitter_bps: 0,
    };
    assert_eq!(retry_delay_ms(&exp, 3), 40);
    let j = apply_deterministic_jitter(100, 0, 1);
    assert_eq!(j, 100);
    let j2 = apply_deterministic_jitter(100, 10_000, 2);
    let _ = j2;
}

#[test]
fn bulkhead_and_rate_queries() {
    let b = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 2 }).unwrap());
    assert_eq!(b.max_concurrent(), 2);
    assert_eq!(b.in_flight(), 0);
    let p = b.try_enter().unwrap();
    assert_eq!(b.in_flight(), 1);
    drop(p);
    assert_eq!(b.in_flight(), 0);

    let mut lim = RateLimiter::new(RateLimitConfig { capacity: 3 }).unwrap();
    assert_eq!(lim.capacity(), 3);
    assert_eq!(lim.available(), 3);
    lim.try_acquire(2).unwrap();
    assert_eq!(lim.available(), 1);
    assert_eq!(
        lim.try_acquire(5).unwrap_err().kind(),
        ErrorKind::Unavailable
    );
}

#[test]
fn circuit_config_accessor_and_noop_instr() {
    let cfg = CircuitConfig {
        failure_threshold: 2,
        success_threshold: 1,
        open_to_half_open_after_rejects: 1,
    };
    let mut cb = CircuitBreaker::new(cfg).unwrap();
    assert_eq!(cb.config().failure_threshold, 2);
    assert_eq!(cb.state(), CircuitState::Closed);
    let instr = NoopInstrumentation;
    let _ = format!("{instr:?}");
    cb.call(&instr, "op", || Err::<(), _>(ResiliencxError::invalid("x")))
        .unwrap_err();
    cb.call(&instr, "op", || Err::<(), _>(ResiliencxError::invalid("x")))
        .unwrap_err();
    assert_eq!(cb.state(), CircuitState::Open);

    let rcfg = RetryConfig::fixed(1, 0);
    let mut op = || Ok(retry_ok(1u8));
    let out = retry_fn_with_wait(&rcfg, &instr, "n", &NoWait, &mut op).unwrap();
    let _ = out;
}

#[test]
fn caller_seed_can_decorrelate_jitter() {
    let cfg = RetryConfig {
        max_attempts: 3,
        base_delay_ms: 1_000,
        backoff: Backoff::Constant,
        jitter_bps: 5_000,
    };

    assert_ne!(
        retry_delay_ms_with_seed(&cfg, 1, 7),
        retry_delay_ms_with_seed(&cfg, 1, 8)
    );
    assert_ne!(apply_seeded_jitter(1_000, 5_000, 1, 7), 0);

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
            &cfg,
            RetrySafety::ReadOnly,
            &NoopInstrumentation,
            "seeded.read",
        )
        .with_jitter_seed(7),
        &wait,
        &mut operation,
    )
    .expect("seeded 安全重试");
    assert_eq!(wait.delays(), vec![retry_delay_ms_with_seed(&cfg, 1, 7)]);
}

#[tokio::test]
async fn generic_adapter_budget_safe_surface() {
    let budget = RetryBudget::new(1);
    assert_eq!(
        call_with_retry_budget_safe(
            &budget,
            1,
            RetrySafety::UnsafeSideEffect,
            "adapter.once",
            &NoopInstrumentation,
            || Ok(7u8),
        )
        .expect("单次不安全操作可执行"),
        7
    );

    assert_eq!(
        call_with_retry_budget_async_safe(
            &budget,
            2,
            RetrySafety::ReadOnly,
            "adapter.read",
            &NoopInstrumentation,
            || async { Ok(9u8) },
        )
        .await
        .expect("只读异步操作可执行"),
        9
    );

    assert_eq!(
        call_with_retry_budget_async(
            &budget,
            1,
            "adapter.unchecked.once",
            &NoopInstrumentation,
            || async { Ok(11u8) },
        )
        .await
        .expect("unchecked 兼容面保持 generic 返回值"),
        11
    );
}
