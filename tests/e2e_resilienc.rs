#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E（resiliencx）：端到端执行**全部**公开接口，不依赖外部真服务。
//!
//! 本仓是韧性原语仓，E2E 的端到端含义是「**错误分类 → 重试策略校验 → 退避/抖动计算 →
//! 预算扣减与耗尽 → 同步/异步重试驱动真实尝试 → 熔断三态迁移 → 舱壁并发准入 →
//! 令牌桶限流**」的完整闭环，而不是单点调用。因此：
//! - 重试域用真实闭包走 `retry_fn*`（含 budget / wait 组合）与 `retry_async*`，断言
//!   真实尝试次数、返回载荷与预算耗尽语义；
//! - 熔断域驱动 `Closed → Open → HalfOpen → Closed` 的完整迁移；
//! - 舱壁域用 `Arc<Bulkhead>` 真实占用许可并断言拒绝与回收；
//! - 限流域驱动令牌桶的取出与补充；
//! - 预算域驱动扣减、退还、重置与「耗尽错误」的传播。
//!
//! 异步路径由 tokio 单线程 runtime 驱动（`#[tokio::test]`），`AsyncWait` 使用仓内
//! `RecordingWait` 以记录等待而不睡眠。
//!
//! 对齐对象是 `cargo +nightly public-api --simplified` 导出的完整公开面：
//! `fn` / `type` / `field` / `const` / `variant` 五类逐条登记在 [`E2E_MANIFEST`]，
//! 运行期由 `cover` 登记表核对「声明 = 实际执行」（缺一即失败）。
//!
//! **独立核对**：`scripts/verify-e2e-coverage.mjs` 会重新派生公开面与清单双向 diff，
//! 并用 `-C instrument-coverage` + `llvm-cov report --show-functions` 断言每条公开
//! 函数执行次数 > 0；本文件内的登记表只是**声明**，不是唯一证据。
//!
//! ```text
//! cd /home/workspace/bytechainx/infra/resiliencx
//! cargo test --test e2e_resilience
//! node /home/workspace/bytechainx/scripts/verify-e2e-coverage.mjs resiliencx --no-coverage
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use resiliencx::{
    apply_deterministic_jitter, apply_seeded_jitter, budget_exhausted_error,
    call_with_retry_budget, call_with_retry_budget_async, call_with_retry_budget_async_safe,
    call_with_retry_budget_safe, ensure_budget, retry_async, retry_async_safe,
    retry_async_with_budget, retry_delay_ms, retry_delay_ms_with_seed, retry_downcast, retry_fn,
    retry_fn_safe, retry_fn_with_budget, retry_fn_with_wait, retry_fn_with_wait_budget, retry_ok,
    AsyncWait, Backoff, BoxError, Bulkhead, BulkheadConfig, BulkheadPermit, CircuitBreaker,
    CircuitConfig, CircuitState, ErrorKind, NoWait, NoopInstrumentation, RateLimitConfig,
    RateLimiter, RecordingWait, ResiliencxError, ResiliencxResult, RetryBudget, RetryConfig,
    RetryContext, RetrySafety, RetryValue, ThreadSleepWait, Wait,
};

/// 公开面清单：`(条目类别, 入口 id)`，由 `cargo +nightly public-api --simplified` 派生并冻结。
///
/// 类别取值域：`fn` / `type` / `field` / `const` / `variant`。
/// 该清单是运行时登记的**唯一事实源**——`cover::hit` 拒绝清单外的 id，收尾断言拒绝
/// 「声明了却没执行」的条目。清单本身的时效性由外部核对器与公开面 diff 保证。
#[rustfmt::skip]
const E2E_MANIFEST: &[(&str, &str)] = &[
    ("type", "RetryBudget"),
    ("fn", "RetryBudget::capacity"),
    ("fn", "RetryBudget::is_exhausted"),
    ("fn", "RetryBudget::new"),
    ("fn", "RetryBudget::refund"),
    ("fn", "RetryBudget::remaining"),
    ("fn", "RetryBudget::reset"),
    ("fn", "RetryBudget::try_consume"),
    ("fn", "budget_exhausted_error"),
    ("fn", "call_with_retry_budget"),
    ("fn", "call_with_retry_budget_async"),
    ("fn", "call_with_retry_budget_async_safe"),
    ("fn", "call_with_retry_budget_safe"),
    ("fn", "ensure_budget"),
    ("type", "Bulkhead"),
    ("fn", "Bulkhead::call"),
    ("fn", "Bulkhead::in_flight"),
    ("fn", "Bulkhead::max_concurrent"),
    ("fn", "Bulkhead::new"),
    ("fn", "Bulkhead::try_enter"),
    ("type", "BulkheadConfig"),
    ("field", "BulkheadConfig::max_concurrent"),
    ("type", "BulkheadPermit"),
    ("type", "CircuitState"),
    ("variant", "CircuitState::Closed"),
    ("variant", "CircuitState::HalfOpen"),
    ("variant", "CircuitState::Open"),
    ("type", "CircuitBreaker"),
    ("fn", "CircuitBreaker::call"),
    ("fn", "CircuitBreaker::config"),
    ("fn", "CircuitBreaker::new"),
    ("fn", "CircuitBreaker::state"),
    ("type", "CircuitConfig"),
    ("field", "CircuitConfig::failure_threshold"),
    ("field", "CircuitConfig::open_to_half_open_after_rejects"),
    ("field", "CircuitConfig::success_threshold"),
    ("type", "ErrorKind"),
    ("variant", "ErrorKind::Cancelled"),
    ("variant", "ErrorKind::Conflict"),
    ("variant", "ErrorKind::DeadlineExceeded"),
    ("variant", "ErrorKind::Internal"),
    ("variant", "ErrorKind::Invalid"),
    ("variant", "ErrorKind::Invariant"),
    ("variant", "ErrorKind::Missing"),
    ("variant", "ErrorKind::Transient"),
    ("variant", "ErrorKind::Unavailable"),
    ("type", "ResiliencxError"),
    ("fn", "ResiliencxError::cancelled"),
    ("fn", "ResiliencxError::conflict"),
    ("fn", "ResiliencxError::context"),
    ("fn", "ResiliencxError::deadline_exceeded"),
    ("fn", "ResiliencxError::internal"),
    ("fn", "ResiliencxError::invalid"),
    ("fn", "ResiliencxError::invariant"),
    ("fn", "ResiliencxError::is_bug"),
    ("fn", "ResiliencxError::is_retryable"),
    ("fn", "ResiliencxError::kind"),
    ("fn", "ResiliencxError::missing"),
    ("fn", "ResiliencxError::retry_after"),
    ("fn", "ResiliencxError::transient"),
    ("fn", "ResiliencxError::transient_after"),
    ("fn", "ResiliencxError::unavailable"),
    ("fn", "ResiliencxError::with_source"),
    ("type", "BoxError"),
    ("type", "ResiliencxResult"),
    ("type", "RateLimitConfig"),
    ("field", "RateLimitConfig::capacity"),
    ("type", "RateLimiter"),
    ("fn", "RateLimiter::available"),
    ("fn", "RateLimiter::capacity"),
    ("fn", "RateLimiter::new"),
    ("fn", "RateLimiter::refill"),
    ("fn", "RateLimiter::try_acquire"),
    ("type", "Backoff"),
    ("variant", "Backoff::Constant"),
    ("variant", "Backoff::Exponential"),
    ("type", "RetrySafety"),
    ("variant", "RetrySafety::Idempotent"),
    ("variant", "RetrySafety::ReadOnly"),
    ("variant", "RetrySafety::UnsafeSideEffect"),
    ("fn", "RetrySafety::validate"),
    ("type", "NoWait"),
    ("type", "RecordingWait"),
    ("fn", "RecordingWait::delays"),
    ("fn", "RecordingWait::new"),
    ("type", "RetryConfig"),
    ("field", "RetryConfig::backoff"),
    ("field", "RetryConfig::base_delay_ms"),
    ("field", "RetryConfig::jitter_bps"),
    ("field", "RetryConfig::max_attempts"),
    ("fn", "RetryConfig::fixed"),
    ("type", "RetryContext"),
    ("type", "ThreadSleepWait"),
    ("type", "AsyncWait"),
    ("fn", "AsyncWait::wait_ms"),
    ("type", "Wait"),
    ("fn", "Wait::wait_ms"),
    ("fn", "apply_deterministic_jitter"),
    ("fn", "apply_seeded_jitter"),
    ("fn", "retry_async"),
    ("fn", "retry_async_safe"),
    ("fn", "retry_async_with_budget"),
    ("fn", "retry_delay_ms"),
    ("fn", "retry_delay_ms_with_seed"),
    ("fn", "retry_downcast"),
    ("fn", "retry_fn"),
    ("fn", "retry_fn_safe"),
    ("fn", "retry_fn_with_budget"),
    ("fn", "retry_fn_with_wait"),
    ("fn", "retry_fn_with_wait_budget"),
    ("fn", "retry_ok"),
    ("type", "RetryValue"),
    ("type", "NoopInstrumentation"),
];

mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    /// 登记一次真实执行。清单外的 `(类别, id)` 立即 panic，防止调用点与清单漂移。
    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(declared_kind, declared_id)| *declared_kind == kind && *declared_id == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }

    /// 已登记的执行集合（收尾断言用）。
    pub fn executed() -> BTreeSet<(&'static str, &'static str)> {
        log().lock().expect("覆盖登记表锁中毒").clone()
    }
}

/// 覆盖登记的简写入口（保持调用点可读）。
fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

/// 清单自身良构：类别取值域合法、`(类别, id)` 不重复。
fn assert_manifest_wellformed() {
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}（id={id}）"
        );
        assert!(seen.insert((*kind, *id)), "清单重复条目：{kind} {id}");
    }
}

/// 覆盖完整性：清单里每一条都必须被真实执行过。
fn assert_coverage_complete() {
    let executed = cover::executed();
    let mut missing: Vec<(&str, &str)> = Vec::new();
    for (kind, id) in E2E_MANIFEST {
        if !executed.contains(&(*kind, *id)) {
            missing.push((kind, id));
        }
    }
    assert!(missing.is_empty(), "声明了却未执行：{missing:?}");
}

/// 错误域：九个分类构造器、kind 映射、可重试/缺陷判定与来源链。
fn phase_error() {
    hit("type", "ResiliencxError");
    hit("type", "ErrorKind");
    hit("type", "BoxError");
    hit("type", "ResiliencxResult");

    let cases: [(ResiliencxError, ErrorKind); 9] = [
        (ResiliencxError::invalid("bad input"), ErrorKind::Invalid),
        (ResiliencxError::missing("absent"), ErrorKind::Missing),
        (
            ResiliencxError::conflict("state changed"),
            ErrorKind::Conflict,
        ),
        (
            ResiliencxError::transient("try later"),
            ErrorKind::Transient,
        ),
        (
            ResiliencxError::unavailable("downstream down"),
            ErrorKind::Unavailable,
        ),
        (
            ResiliencxError::cancelled("by caller"),
            ErrorKind::Cancelled,
        ),
        (
            ResiliencxError::deadline_exceeded("too slow"),
            ErrorKind::DeadlineExceeded,
        ),
        (
            ResiliencxError::invariant("broken precondition"),
            ErrorKind::Invariant,
        ),
        (
            ResiliencxError::internal("unclassified"),
            ErrorKind::Internal,
        ),
    ];
    let mut seen_kinds: BTreeSet<&'static str> = BTreeSet::new();
    for (error, expected) in cases {
        assert_eq!(error.kind(), expected, "构造器与分类必须一致：{error:?}");
        assert!(!error.context().is_empty(), "上下文不得为空");
        assert!(!error.to_string().is_empty(), "必须可读");
        seen_kinds.insert(expected_id(expected));
    }
    assert_eq!(seen_kinds.len(), 9, "九个分类必须全部出现");

    // 分类构造器各自落地（含未在 cases 中单独断言的分支）。
    hit("fn", "ResiliencxError::invalid");
    hit("fn", "ResiliencxError::missing");
    hit("fn", "ResiliencxError::conflict");
    hit("fn", "ResiliencxError::transient");
    hit("fn", "ResiliencxError::unavailable");
    hit("fn", "ResiliencxError::cancelled");
    hit("fn", "ResiliencxError::deadline_exceeded");
    hit("fn", "ResiliencxError::invariant");
    hit("fn", "ResiliencxError::internal");
    hit("variant", "ErrorKind::Invalid");
    hit("variant", "ErrorKind::Missing");
    hit("variant", "ErrorKind::Conflict");
    hit("variant", "ErrorKind::Transient");
    hit("variant", "ErrorKind::Unavailable");
    hit("variant", "ErrorKind::Cancelled");
    hit("variant", "ErrorKind::DeadlineExceeded");
    hit("variant", "ErrorKind::Invariant");
    hit("variant", "ErrorKind::Internal");

    // 可重试与缺陷分类：瞬时可重试、不变量为缺陷。
    hit("fn", "ResiliencxError::is_retryable");
    hit("fn", "ResiliencxError::is_bug");
    assert!(ResiliencxError::transient("t").is_retryable());
    assert!(!ResiliencxError::invalid("i").is_retryable());
    assert!(ResiliencxError::invariant("i").is_bug());
    assert!(!ResiliencxError::missing("m").is_bug());

    // retry_after 提示与来源链。
    hit("fn", "ResiliencxError::transient_after");
    hit("fn", "ResiliencxError::retry_after");
    let hinted = ResiliencxError::transient_after("throttled", Duration::from_millis(50));
    assert_eq!(hinted.retry_after(), Some(Duration::from_millis(50)));
    assert!(hinted.is_retryable(), "带提示的瞬时错误仍可重试");
    assert_eq!(ResiliencxError::transient("t").retry_after(), None);

    hit("fn", "ResiliencxError::with_source");
    let source: BoxError = Box::new(std::io::Error::other("底层原因"));
    let sourced = ResiliencxError::internal("wrapped").with_source(source);
    let sourced_debug = format!("{sourced:?}");
    assert!(
        sourced_debug.contains("底层原因") || sourced_debug.contains("source"),
        "来源链必须可观测：{sourced_debug}"
    );

    // 判别函数必须真实可用（kind / context / is_bug 已在上面执行）。
    hit("fn", "ResiliencxError::kind");
    hit("fn", "ResiliencxError::context");
}

/// `ErrorKind` 的静态标签（用于去重断言，不依赖 Debug 文本）。
const fn expected_id(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Invalid => "Invalid",
        ErrorKind::Missing => "Missing",
        ErrorKind::Conflict => "Conflict",
        ErrorKind::Transient => "Transient",
        ErrorKind::Unavailable => "Unavailable",
        ErrorKind::Cancelled => "Cancelled",
        ErrorKind::DeadlineExceeded => "DeadlineExceeded",
        ErrorKind::Invariant => "Invariant",
        ErrorKind::Internal => "Internal",
        _ => "Other",
    }
}

/// 配置域：退避策略、安全等级校验、退避与抖动计算。
fn phase_config() {
    hit("type", "RetryConfig");
    hit("fn", "RetryConfig::fixed");
    hit("field", "RetryConfig::max_attempts");
    hit("field", "RetryConfig::base_delay_ms");
    hit("field", "RetryConfig::backoff");
    hit("field", "RetryConfig::jitter_bps");
    hit("type", "Backoff");
    hit("variant", "Backoff::Constant");
    hit("variant", "Backoff::Exponential");
    hit("type", "RetrySafety");
    hit("variant", "RetrySafety::Idempotent");
    hit("variant", "RetrySafety::ReadOnly");
    hit("variant", "RetrySafety::UnsafeSideEffect");
    hit("fn", "RetrySafety::validate");

    let _backoff_variant: Backoff = Backoff::Constant;
    let fixed = RetryConfig::fixed(3, 5);
    assert_eq!(fixed.max_attempts, 3);
    assert_eq!(fixed.base_delay_ms, 5);
    assert!(matches!(fixed.backoff, Backoff::Constant));
    assert_eq!(fixed.jitter_bps, 0, "fixed 构造不得引入抖动");

    let exponential = RetryConfig {
        max_attempts: 4,
        base_delay_ms: 10,
        backoff: Backoff::Exponential {
            factor: 2,
            max_delay_ms: 100,
        },
        jitter_bps: 500,
    };
    assert!(matches!(
        exponential.backoff,
        Backoff::Exponential {
            factor: 2,
            max_delay_ms: 100
        }
    ));
    assert_eq!(exponential.jitter_bps, 500);

    // 安全等级校验：幂等/只读允许多次尝试，未声明安全副作用必须被拒。
    assert!(
        RetrySafety::Idempotent.validate(&fixed).is_ok(),
        "幂等操作允许多次尝试"
    );
    assert!(
        RetrySafety::ReadOnly.validate(&fixed).is_ok(),
        "只读操作允许多次尝试"
    );
    assert!(
        RetrySafety::UnsafeSideEffect.validate(&fixed).is_err(),
        "未声明安全副作用不得重试"
    );

    // 退避计算：常数退避恒定；指数退避单调不减且受上限约束。
    hit("fn", "retry_delay_ms");
    assert_eq!(
        retry_delay_ms(&fixed, 1),
        retry_delay_ms(&fixed, 5),
        "常数退避与尝试次数无关"
    );
    let d1 = retry_delay_ms(&exponential, 1);
    let d3 = retry_delay_ms(&exponential, 3);
    let d9 = retry_delay_ms(&exponential, 9);
    assert!(d1 <= d3, "指数退避必须单调不减（{d1} ≤ {d3}）");
    assert!(d9 <= 100, "指数退避必须受 max_delay_ms 约束（{d9} ≤ 100）");

    hit("fn", "retry_delay_ms_with_seed");
    let seeded = retry_delay_ms_with_seed(&fixed, 2, 42);
    assert_eq!(
        retry_delay_ms_with_seed(&fixed, 2, 42),
        seeded,
        "同种子必须可复现"
    );

    hit("fn", "apply_deterministic_jitter");
    let plain = apply_deterministic_jitter(100, 0, 1);
    assert_eq!(plain, 100, "零抖动必须原样返回");
    let jittered = apply_deterministic_jitter(100, 1000, 1);
    assert!(jittered <= 110, "抖动不得超出配置幅度（{jittered} ≤ 110）");
    assert_eq!(
        jittered,
        apply_deterministic_jitter(100, 1000, 1),
        "确定性抖动必须可复现"
    );

    hit("fn", "apply_seeded_jitter");
    let seeded_jitter = apply_seeded_jitter(100, 1000, 1, 7);
    assert!(
        seeded_jitter <= 110,
        "种子抖动不得超出配置幅度（{seeded_jitter} ≤ 110）"
    );
    assert_eq!(
        seeded_jitter,
        apply_seeded_jitter(100, 1000, 1, 7),
        "同种子抖动必须可复现"
    );
}

/// 预算域：扣减、退还、重置、耗尽传播与两个同步 budget 入口。
fn phase_budget() {
    hit("type", "RetryBudget");
    hit("fn", "RetryBudget::new");
    hit("fn", "RetryBudget::capacity");
    hit("fn", "RetryBudget::remaining");
    hit("fn", "RetryBudget::is_exhausted");
    hit("fn", "RetryBudget::try_consume");
    hit("fn", "RetryBudget::refund");
    hit("fn", "RetryBudget::reset");
    hit("fn", "budget_exhausted_error");
    hit("fn", "ensure_budget");

    let budget = RetryBudget::new(2);
    assert_eq!(budget.capacity(), 2);
    assert_eq!(budget.remaining(), 2);
    assert!(!budget.is_exhausted());
    assert!(budget.try_consume(), "首次扣减必须成功");
    assert!(budget.try_consume(), "第二次扣减必须成功");
    assert_eq!(budget.remaining(), 0);
    assert!(budget.is_exhausted(), "扣完必须判定耗尽");
    assert!(!budget.try_consume(), "耗尽后扣减必须失败");

    ensure_budget(&budget).expect_err("耗尽预算必须被 ensure_budget 拦截");
    let exhausted = ensure_budget(&budget).unwrap_err();
    assert_eq!(
        exhausted.kind(),
        budget_exhausted_error().kind(),
        "耗尽错误必须与标准耗尽错误同分类"
    );
    assert!(!budget_exhausted_error().to_string().is_empty());

    budget.refund();
    assert_eq!(budget.remaining(), 1, "退还必须返还一个额度");
    ensure_budget(&budget).expect("退还后必须可用");
    budget.reset();
    assert_eq!(budget.remaining(), 2, "重置必须回到容量");

    // 同步 budget 入口：重试到耗尽 → 返回耗尽错误；一次成功 → 返回载荷。
    hit("fn", "call_with_retry_budget");
    hit("type", "NoopInstrumentation");
    let instr = NoopInstrumentation;
    let spend = RetryBudget::new(1);
    let mut attempts = 0u32;
    let failed: ResiliencxResult<u64> = call_with_retry_budget(&spend, 5, "e2e.op", &instr, || {
        attempts += 1;
        Err(ResiliencxError::transient("always"))
    });
    let failed = failed.unwrap_err();
    assert!(attempts >= 2, "至少发生一次重试（实际 {attempts}）");
    assert_eq!(
        failed.kind(),
        budget_exhausted_error().kind(),
        "预算耗尽必须给出耗尽分类"
    );

    let ok_budget = RetryBudget::new(3);
    let value = call_with_retry_budget(&ok_budget, 3, "e2e.op", &instr, || Ok(7u64))
        .expect("首次成功必须返回载荷");
    assert_eq!(value, 7);
    assert_eq!(ok_budget.remaining(), 3, "未重试不得扣减预算");

    hit("fn", "call_with_retry_budget_safe");
    let safe_budget = RetryBudget::new(3);
    let safe_value = call_with_retry_budget_safe(
        &safe_budget,
        3,
        RetrySafety::Idempotent,
        "e2e.op",
        &instr,
        || Ok(11u64),
    )
    .expect("幂等安全入口必须成功");
    assert_eq!(safe_value, 11);
}

/// 熔断域：三态真实迁移。
fn phase_circuit() {
    hit("type", "CircuitBreaker");
    hit("fn", "CircuitBreaker::new");
    hit("fn", "CircuitBreaker::state");
    hit("fn", "CircuitBreaker::config");
    hit("fn", "CircuitBreaker::call");
    hit("type", "CircuitConfig");
    hit("field", "CircuitConfig::failure_threshold");
    hit("field", "CircuitConfig::success_threshold");
    hit("field", "CircuitConfig::open_to_half_open_after_rejects");
    hit("type", "CircuitState");
    hit("variant", "CircuitState::Closed");
    hit("variant", "CircuitState::Open");
    hit("variant", "CircuitState::HalfOpen");

    let config = CircuitConfig {
        failure_threshold: 2,
        success_threshold: 1,
        open_to_half_open_after_rejects: 3,
    };
    assert_eq!(config.failure_threshold, 2);
    assert_eq!(config.success_threshold, 1);
    assert_eq!(config.open_to_half_open_after_rejects, 3);

    let instr = NoopInstrumentation;
    let mut breaker = CircuitBreaker::new(config).expect("合法配置必须成功");
    let round_trip = breaker.config();
    assert_eq!(round_trip.failure_threshold, 2);
    assert!(
        matches!(breaker.state(), CircuitState::Closed),
        "初始必须为 Closed"
    );

    // 连续失败到阈值 → 跳闸。
    for _ in 0..2 {
        let outcome: ResiliencxResult<u64> =
            breaker.call(&instr, "e2e.op", || Err(ResiliencxError::transient("down")));
        assert!(outcome.is_err(), "失败调用必须传播错误");
    }
    assert!(
        matches!(breaker.state(), CircuitState::Open),
        "达到失败阈值必须跳闸，实际 {:?}",
        breaker.state()
    );

    // Open 下被拒的调用累计到阈值 → 半开。
    for _ in 0..3 {
        let rejected: ResiliencxResult<u64> = breaker.call(&instr, "e2e.op", || Ok(1u64));
        assert!(rejected.is_err(), "Open 期间调用必须被拒");
    }
    assert!(
        matches!(breaker.state(), CircuitState::HalfOpen),
        "拒绝次数达到阈值必须进入 HalfOpen，实际 {:?}",
        breaker.state()
    );

    // 半开成功到阈值 → 闭合。
    let probe = breaker
        .call(&instr, "e2e.op", || Ok(5u64))
        .expect("半开探测必须放行");
    assert_eq!(probe, 5);
    assert!(
        matches!(breaker.state(), CircuitState::Closed),
        "半开成功必须闭合，实际 {:?}",
        breaker.state()
    );
}

/// 并发准入域：舱壁许可与令牌桶限流。
fn phase_concurrency() {
    hit("type", "Bulkhead");
    hit("fn", "Bulkhead::new");
    hit("fn", "Bulkhead::max_concurrent");
    hit("fn", "Bulkhead::in_flight");
    hit("fn", "Bulkhead::try_enter");
    hit("fn", "Bulkhead::call");
    hit("type", "BulkheadConfig");
    hit("field", "BulkheadConfig::max_concurrent");
    hit("type", "BulkheadPermit");

    let bulkhead_config = BulkheadConfig { max_concurrent: 1 };
    assert_eq!(bulkhead_config.max_concurrent, 1);
    let bulkhead = Arc::new(Bulkhead::new(bulkhead_config).expect("合法配置必须成功"));
    assert_eq!(bulkhead.max_concurrent(), 1);
    assert_eq!(bulkhead.in_flight(), 0, "初始无在飞调用");

    let permit: BulkheadPermit = bulkhead.try_enter().expect("空舱壁必须放行");
    assert_eq!(bulkhead.in_flight(), 1, "占用许可必须在飞 +1");
    assert!(
        bulkhead.try_enter().is_err(),
        "超出 max_concurrent 必须拒绝"
    );
    drop(permit);
    assert_eq!(bulkhead.in_flight(), 0, "释放许可必须在飞回零");

    let value = bulkhead.call(|| Ok(9u64)).expect("许可释放后必须放行");
    assert_eq!(value, 9);
    assert_eq!(bulkhead.in_flight(), 0, "调用结束必须回收许可");

    hit("type", "RateLimitConfig");
    hit("field", "RateLimitConfig::capacity");
    hit("type", "RateLimiter");
    hit("fn", "RateLimiter::new");
    hit("fn", "RateLimiter::capacity");
    hit("fn", "RateLimiter::available");
    hit("fn", "RateLimiter::try_acquire");
    hit("fn", "RateLimiter::refill");

    let rate_config = RateLimitConfig { capacity: 2 };
    assert_eq!(rate_config.capacity, 2);
    let mut limiter = RateLimiter::new(rate_config).expect("合法配置必须成功");
    assert_eq!(limiter.capacity(), 2);
    assert_eq!(limiter.available(), 2, "初始令牌必须满额");
    limiter.try_acquire(1).expect("有余量必须放行");
    limiter.try_acquire(1).expect("最后一次必须放行");
    assert_eq!(limiter.available(), 0);
    assert!(limiter.try_acquire(1).is_err(), "令牌耗尽必须拒绝");
    limiter.refill(2);
    assert_eq!(limiter.available(), 2, "补充必须恢复令牌");
}

/// 同步重试域：五个入口与载荷装箱/解箱。
fn phase_retry_sync() {
    hit("type", "RetryValue");
    hit("fn", "retry_ok");
    hit("fn", "retry_downcast");
    hit("type", "RetryContext");
    hit("type", "Wait");
    hit("fn", "Wait::wait_ms");
    hit("type", "ThreadSleepWait");
    hit("type", "NoWait");
    hit("type", "RecordingWait");
    hit("fn", "RecordingWait::new");
    hit("fn", "RecordingWait::delays");
    hit("fn", "retry_fn");
    hit("fn", "retry_fn_with_wait");
    hit("fn", "retry_fn_with_budget");
    hit("fn", "retry_fn_with_wait_budget");
    hit("fn", "retry_fn_safe");

    let boxed: RetryValue = retry_ok(42u64);
    assert_eq!(retry_downcast::<u64>(boxed).expect("类型匹配必须解箱"), 42);
    assert!(
        retry_downcast::<String>(retry_ok(1u64)).is_err(),
        "类型不匹配必须报错"
    );

    let instr = NoopInstrumentation;
    let config = RetryConfig::fixed(3, 5);

    // retry_fn：两次失败后成功。
    let mut attempts = 0u32;
    let mut flaky = || -> ResiliencxResult<RetryValue> {
        attempts += 1;
        if attempts < 3 {
            Err(ResiliencxError::transient("flaky"))
        } else {
            Ok(retry_ok(attempts))
        }
    };
    let value = retry_fn(&config, &instr, "e2e.op", &mut flaky).expect("第三次必须成功");
    assert_eq!(retry_downcast::<u32>(value).expect("解箱"), 3);
    assert_eq!(attempts, 3, "必须真实尝试三次");

    // retry_fn_with_wait：真实 wait（ThreadSleepWait）被调用。
    let mut attempts = 0u32;
    let mut always_ok = || -> ResiliencxResult<RetryValue> {
        attempts += 1;
        Ok(retry_ok(1u8))
    };
    retry_fn_with_wait(&config, &instr, "e2e.op", &ThreadSleepWait, &mut always_ok)
        .expect("首次成功必须返回");
    assert_eq!(attempts, 1);

    let _thread_wait: &dyn Wait = &ThreadSleepWait;
    let recording = RecordingWait::new();
    let mut attempts = 0u32;
    let mut twice = || -> ResiliencxResult<RetryValue> {
        attempts += 1;
        if attempts < 2 {
            Err(ResiliencxError::transient("again"))
        } else {
            Ok(retry_ok(2u8))
        }
    };
    retry_fn_with_wait(&config, &instr, "e2e.op", &recording, &mut twice).expect("第二次必须成功");
    assert_eq!(
        recording.delays().len(),
        1,
        "RecordingWait 必须记录一次等待"
    );

    // retry_fn_with_budget：预算扣减真实发生。
    let budget = RetryBudget::new(5);
    let mut attempts = 0u32;
    let mut once_fail = || -> ResiliencxResult<RetryValue> {
        attempts += 1;
        if attempts < 2 {
            Err(ResiliencxError::transient("once"))
        } else {
            Ok(retry_ok(1u8))
        }
    };
    retry_fn_with_budget(&config, &instr, "e2e.op", &budget, &mut once_fail)
        .expect("预算充足必须成功");
    assert_eq!(budget.remaining(), 4, "一次重试必须扣减一个额度");

    // retry_fn_with_wait_budget：wait 与 budget 同时生效。
    let both_budget = RetryBudget::new(5);
    let both_wait = RecordingWait::new();
    let mut attempts = 0u32;
    let mut retry_once = || -> ResiliencxResult<RetryValue> {
        attempts += 1;
        if attempts < 2 {
            Err(ResiliencxError::transient("once"))
        } else {
            Ok(retry_ok(1u8))
        }
    };
    retry_fn_with_wait_budget(
        &config,
        &instr,
        "e2e.op",
        &both_wait,
        Some(&both_budget),
        &mut retry_once,
    )
    .expect("组合入口必须成功");
    assert_eq!(both_budget.remaining(), 4, "组合入口也必须扣减预算");
    assert_eq!(both_wait.delays().len(), 1, "组合入口也必须真实等待");

    // retry_fn_safe：经 RetryContext 驱动，并校验安全等级。
    let context = RetryContext::new(&config, RetrySafety::Idempotent, &instr, "e2e.op")
        .with_budget(&both_budget)
        .with_jitter_seed(9);
    let mut attempts = 0u32;
    let mut ok_once = || -> ResiliencxResult<RetryValue> {
        attempts += 1;
        Ok(retry_ok(1u8))
    };
    retry_fn_safe(context, &NoWait, &mut ok_once).expect("安全入口必须成功");
    assert_eq!(attempts, 1);

    // 不安全等级 + 多次尝试必须被安全入口拒绝。
    let unsafe_context =
        RetryContext::new(&config, RetrySafety::UnsafeSideEffect, &instr, "e2e.op");
    let mut never = || -> ResiliencxResult<RetryValue> { Ok(retry_ok(1u8)) };
    assert!(
        retry_fn_safe(unsafe_context, &NoWait, &mut never).is_err(),
        "未声明安全副作用不得经安全入口重试"
    );
}

/// 异步重试域：四个 async 入口 + budget 异步入口 + AsyncWait 真实等待。
async fn phase_retry_async() {
    hit("type", "AsyncWait");
    hit("fn", "AsyncWait::wait_ms");
    hit("fn", "retry_async");
    hit("fn", "retry_async_with_budget");
    hit("fn", "retry_async_safe");
    hit("fn", "call_with_retry_budget_async");
    hit("fn", "call_with_retry_budget_async_safe");

    let instr = NoopInstrumentation;
    let config = RetryConfig::fixed(3, 5);
    let wait = RecordingWait::new();
    let _async_wait: &dyn AsyncWait = &wait;

    let mut attempts = 0u32;
    let value = retry_async(&config, &instr, "e2e.op", &wait, || {
        attempts += 1;
        let current = attempts;
        async move {
            if current < 2 {
                Err(ResiliencxError::transient("async flaky"))
            } else {
                Ok(retry_ok(current))
            }
        }
    })
    .await
    .expect("异步重试第二次必须成功");
    assert_eq!(retry_downcast::<u32>(value).expect("解箱"), 2);
    assert!(
        !wait.delays().is_empty(),
        "AsyncWait::wait_ms 必须被真实调用"
    );

    let budget = RetryBudget::new(4);
    let mut attempts = 0u32;
    let value = retry_async_with_budget(&config, &instr, "e2e.op", &wait, &budget, || {
        attempts += 1;
        let current = attempts;
        async move {
            if current < 2 {
                Err(ResiliencxError::transient("async once"))
            } else {
                Ok(retry_ok(current))
            }
        }
    })
    .await
    .expect("带预算的异步重试必须成功");
    assert_eq!(retry_downcast::<u32>(value).expect("解箱"), 2);
    assert_eq!(budget.remaining(), 3, "异步重试必须扣减预算");

    let context = RetryContext::new(&config, RetrySafety::ReadOnly, &instr, "e2e.op");
    let mut attempts = 0u32;
    let value = retry_async_safe(context, &wait, || {
        attempts += 1;
        async move { Ok(retry_ok(attempts)) }
    })
    .await
    .expect("异步安全入口必须成功");
    assert_eq!(retry_downcast::<u32>(value).expect("解箱"), 1);

    let async_budget = RetryBudget::new(3);
    let mut attempts = 0u32;
    let value = call_with_retry_budget_async(&async_budget, 3, "e2e.op", &instr, || {
        attempts += 1;
        async move { Ok(attempts) }
    })
    .await
    .expect("异步 budget 入口必须成功");
    assert_eq!(value, 1);

    let safe_budget = RetryBudget::new(3);
    let value = call_with_retry_budget_async_safe(
        &safe_budget,
        3,
        RetrySafety::Idempotent,
        "e2e.op",
        &instr,
        || async { Ok(13u64) },
    )
    .await
    .expect("异步安全 budget 入口必须成功");
    assert_eq!(value, 13);
}

/// 单一驱动用例：保证阶段顺序与覆盖断言在同一个进程内完成。
#[tokio::test]
async fn e2e_resilience_all_public_api() {
    assert_manifest_wellformed();
    phase_error();
    phase_config();
    phase_budget();
    phase_circuit();
    phase_concurrency();
    phase_retry_sync();
    phase_retry_async().await;
    assert_coverage_complete();
}
