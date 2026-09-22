#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! resiliencx 内部生产消费者 example（Q-2=D · 非 dry-run 最低证据路径）。
//!
//! 覆盖能力面：retry（safe）· budget · circuit · rate_limit · bulkhead。
//!
//! ```bash
//! cargo run -p resiliencx --example resiliencx_basic
//! ```

use resiliencx::ResiliencxError;
use resiliencx::{
    call_with_retry_budget_safe, retry_fn_safe, retry_ok, Backoff, Bulkhead, BulkheadConfig,
    CircuitBreaker, CircuitConfig, CircuitState, NoWait, NoopInstrumentation, RateLimitConfig,
    RateLimiter, RetryBudget, RetryConfig, RetryContext, RetrySafety,
};
use std::sync::Arc;

fn main() {
    let profile = std::env::var("RESILIENCX_LIVE_PROFILE").unwrap_or_else(|_| "development".into());
    let instr = NoopInstrumentation;
    let wait = NoWait;

    // retry（safe · ReadOnly）
    let cfg = RetryConfig {
        max_attempts: 3,
        base_delay_ms: 0,
        backoff: Backoff::Constant,
        jitter_bps: 0,
    };
    let mut attempts = 0u32;
    let boxed = retry_fn_safe(
        RetryContext::new(&cfg, RetrySafety::ReadOnly, &instr, "prod.retry"),
        &wait,
        &mut || {
            attempts += 1;
            if attempts < 2 {
                return Err(ResiliencxError::transient("transient"));
            }
            Ok(retry_ok(attempts))
        },
    )
    .expect("retry safe");
    assert_eq!(attempts, 2);
    let _ = boxed;

    // budget（safe adapter 单次）
    let budget = RetryBudget::new(1);
    let out = call_with_retry_budget_safe(
        &budget,
        1,
        RetrySafety::ReadOnly,
        "prod.budget",
        &instr,
        || Ok(42u8),
    )
    .expect("budget safe");
    assert_eq!(out, 42);

    // circuit（Closed → Open）
    let mut cb = CircuitBreaker::new(CircuitConfig {
        failure_threshold: 1,
        success_threshold: 1,
        open_to_half_open_after_rejects: 1,
    })
    .expect("circuit");
    assert_eq!(cb.state(), CircuitState::Closed);
    let _ = cb
        .call(&instr, "prod.circuit", || {
            Err::<(), _>(ResiliencxError::transient("trip"))
        })
        .unwrap_err();
    assert_eq!(cb.state(), CircuitState::Open);

    // rate_limit（令牌桶 acquire + refill）
    let mut lim = RateLimiter::new(RateLimitConfig { capacity: 2 }).expect("rate");
    lim.try_acquire(1).expect("acquire");
    lim.refill(1);
    assert_eq!(lim.available(), 2);

    // bulkhead（并发上限 + RAII permit）
    let bh = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("bulkhead"));
    let permit = bh.try_enter().expect("enter");
    assert_eq!(bh.in_flight(), 1);
    drop(permit);
    assert_eq!(bh.in_flight(), 0);

    println!("resiliencx-consumer: ok profile={profile} retry={attempts} budget={out}");
}
