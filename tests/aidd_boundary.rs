#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//!
//! // AIDD: max_attempts 取 0 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §1 重试次数须有限 | 结论=保留
//! // AIDD: jitter_bps 取上限 10000 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 抖动只减不增 | 结论=保留
//! // AIDD: RetryBudget::new(0) | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 预算耗尽立即返回 | 结论=保留
//! // AIDD: 空桶上 acquire 0 与 refill 0 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §1 令牌桶边界 | 结论=保留
//! // AIDD: 熔断阈值取 0 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §1 阈值须 ≥ 1 | 结论=保留
//! // AIDD: 舱壁满载时并发争抢 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §1 满载立即拒绝 | 结论=保留
//! // AIDD: 指数退避 attempt 取 u32::MAX | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §1 退避须封顶 | 结论=保留
//! // AIDD: 非 ASCII 与超长 op 字符串 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 观测不得 panic | 结论=保留

use std::sync::{Arc, Mutex};

use resiliencx::{
    apply_seeded_jitter, retry_delay_ms, retry_fn, retry_ok, Backoff, Bulkhead, BulkheadConfig,
    CircuitBreaker, CircuitConfig, ErrorKind, Instrumentation, NoopInstrumentation,
    RateLimitConfig, RateLimiter, ResiliencxError, RetryBudget, RetryConfig, RetryValue,
};

/// 计数型观测替身（含长文本透传）。
#[derive(Debug, Default)]
struct CountingSink {
    retries: Mutex<Vec<(String, u32)>>,
}

impl Instrumentation for CountingSink {
    fn record_retry(&self, op: &str, attempt: u32) {
        self.retries
            .lock()
            .expect("记录重试")
            .push((op.to_owned(), attempt));
    }

    fn record_circuit_open(&self, _op: &str) {}

    fn record_circuit_close(&self, _op: &str) {}
}

/// 边界：`max_attempts == 0` 必须 fail-closed，且不执行任何一次闭包。
#[test]
fn zero_max_attempts_fails_closed() {
    let mut calls = 0u32;
    let mut operation = || {
        calls += 1;
        Ok::<RetryValue, ResiliencxError>(retry_ok(()))
    };
    let error = retry_fn(
        &RetryConfig::fixed(0, 0),
        &NoopInstrumentation,
        "aidd.zero",
        &mut operation,
    )
    .expect_err("零次尝试必须拒绝");
    assert_eq!(error.kind(), ErrorKind::Invalid);
    assert_eq!(calls, 0);
}

/// 边界：`jitter_bps` 上限 10000 —— 抖动只减不增，且同一 attempt 可复现。
#[test]
fn jitter_at_upper_bound_only_reduces() {
    let full = apply_seeded_jitter(1_000, 10_000, 3, 42);
    assert!(full <= 1_000, "抖动不得放大延迟");
    assert_eq!(full, apply_seeded_jitter(1_000, 10_000, 3, 42));
    // 超出 basis points 定义的取值被夹到 10000，不 panic。
    let clamped = apply_seeded_jitter(1_000, u32::MAX, 3, 42);
    assert!(clamped <= 1_000);
}

/// 边界：容量为 0 的预算——构造即耗尽，归还不得凭空造出令牌。
#[test]
fn zero_capacity_budget_stays_exhausted() {
    let budget = RetryBudget::new(0);
    assert!(budget.is_exhausted());
    assert!(!budget.try_consume());
    budget.refund();
    assert_eq!(budget.remaining(), 0, "容量 0 时归还是空操作");
}

/// 边界：空桶上的 `try_acquire(0)` 与 `refill(0)` 均为空操作。
#[test]
fn zero_token_acquire_and_refill_are_noops() {
    let mut limiter = RateLimiter::new(RateLimitConfig { capacity: 1 }).expect("合法容量");
    limiter.try_acquire(1).expect("清空");
    limiter.try_acquire(0).expect("获取 0 个是空操作");
    assert_eq!(limiter.available(), 0);
    limiter.refill(0);
    assert_eq!(limiter.available(), 0);
}

/// 边界：熔断阈值为 0 必须 fail-closed。
#[test]
fn zero_circuit_threshold_is_rejected() {
    for config in [
        CircuitConfig {
            failure_threshold: 0,
            success_threshold: 1,
            open_to_half_open_after_rejects: 1,
        },
        CircuitConfig {
            failure_threshold: 1,
            success_threshold: 0,
            open_to_half_open_after_rejects: 1,
        },
        CircuitConfig {
            failure_threshold: 1,
            success_threshold: 1,
            open_to_half_open_after_rejects: 0,
        },
    ] {
        let error = CircuitBreaker::new(config).expect_err("阈值 0 必须拒绝");
        assert_eq!(error.kind(), ErrorKind::Invalid);
    }
}

/// 边界：舱壁满载时多线程争抢槽位，非持有者一律立即 `Unavailable`，无一阻塞。
#[test]
fn bulkhead_contended_slot_all_rejected() {
    let bulkhead = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("合法并发"));
    let held = bulkhead.try_enter().expect("持有唯一槽位");

    let mut workers = Vec::new();
    for _ in 0..3 {
        let shared = Arc::clone(&bulkhead);
        workers.push(std::thread::spawn(move || {
            shared.try_enter().expect_err("满载应拒绝").kind()
        }));
    }
    for worker in workers {
        assert_eq!(
            worker.join().expect("工作线程不应 panic"),
            ErrorKind::Unavailable
        );
    }

    drop(held);
    assert_eq!(bulkhead.in_flight(), 0);
}

/// 边界：指数退避在 `attempt == u32::MAX` 时饱和到上限，不溢出、不 panic。
#[test]
fn exponential_backoff_saturates_at_extreme_attempt() {
    let config = RetryConfig {
        max_attempts: 5,
        base_delay_ms: 10,
        backoff: Backoff::Exponential {
            factor: 2,
            max_delay_ms: 40,
        },
        jitter_bps: 0,
    };
    assert_eq!(
        retry_delay_ms(&config, u32::MAX),
        40,
        "须封顶于 max_delay_ms"
    );
}

/// 边界：非 ASCII 与超长 op 透传到观测实现，不 panic、不丢计数。
#[test]
fn non_ascii_and_huge_op_survive_observation() {
    let sink = CountingSink::default();
    let unicode = "数据库．查询\u{0301}🦀";
    let long = "x".repeat(100_000);
    sink.record_retry(unicode, 1);
    sink.record_retry(&long, u32::MAX);
    let recorded = sink.retries.lock().expect("读取重试");
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].0, unicode);
    assert_eq!(recorded[1].0.len(), 100_000);
}
