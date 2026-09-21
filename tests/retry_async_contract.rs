#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! async 重试合同：驱动 shipped [`retry_async`] + [`AsyncWait`]。

use resiliencx::{
    budget_exhausted_error, retry_async, retry_async_safe, retry_async_with_budget, retry_downcast,
    retry_ok, AsyncWait, Backoff, Instrumentation, NoWait, NoopInstrumentation, RecordingWait,
    RetryBudget, RetryConfig, RetryContext, RetrySafety,
};
use resiliencx::{ErrorKind, ResiliencxError};
use std::sync::Mutex;

#[derive(Default)]
struct RetryEvents(Mutex<Vec<u32>>);

impl Instrumentation for RetryEvents {
    fn record_retry(&self, _op: &str, attempt: u32) {
        self.0.lock().expect("记录异步重试事件").push(attempt);
    }

    fn record_circuit_open(&self, _op: &str) {}

    fn record_circuit_close(&self, _op: &str) {}
}

#[cfg(feature = "tokio")]
struct PendingWait;

#[cfg(feature = "tokio")]
#[async_trait::async_trait]
impl AsyncWait for PendingWait {
    async fn wait_ms(&self, _ms: u64) {
        std::future::pending::<()>().await;
    }
}

#[tokio::test]
async fn retry_async_succeeds_after_transient_with_recording_wait() {
    let cfg = RetryConfig {
        max_attempts: 4,
        base_delay_ms: 7,
        backoff: Backoff::Constant,
        jitter_bps: 0,
    };
    let instr = NoopInstrumentation;
    let wait = RecordingWait::new();
    let mut n = 0u32;
    let boxed = retry_async(&cfg, &instr, "async.op", &wait, || {
        n += 1;
        let attempt = n;
        async move {
            if attempt < 3 {
                return Err(ResiliencxError::transient(format!("t{attempt}")));
            }
            Ok(retry_ok(attempt))
        }
    })
    .await
    .expect("ok");
    let got: u32 = retry_downcast(boxed).unwrap();
    assert_eq!(got, 3);
    assert_eq!(wait.delays(), vec![7, 7]);
}

#[tokio::test]
async fn retry_async_non_retryable_stops_immediately() {
    let cfg = RetryConfig::fixed(5, 10);
    let instr = NoopInstrumentation;
    let wait = RecordingWait::new();
    let err = retry_async(&cfg, &instr, "op", &wait, || async {
        Err(ResiliencxError::invalid("nope"))
    })
    .await
    .expect_err("invalid");
    assert_eq!(err.kind(), ErrorKind::Invalid);
    assert!(wait.delays().is_empty());
}

#[tokio::test]
async fn retry_async_zero_attempts_invalid() {
    let cfg = RetryConfig {
        max_attempts: 0,
        ..RetryConfig::default()
    };
    let err = retry_async(&cfg, &NoopInstrumentation, "op", &NoWait, || async {
        Ok(retry_ok(1u8))
    })
    .await
    .expect_err("zero");
    assert_eq!(err.kind(), ErrorKind::Invalid);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn retry_async_tokio_sleep_wait_short_delay() {
    use resiliencx::TokioSleepWait;
    let cfg = RetryConfig {
        max_attempts: 2,
        base_delay_ms: 1,
        backoff: Backoff::Constant,
        jitter_bps: 0,
    };
    let mut n = 0u32;
    let boxed = retry_async(
        &cfg,
        &NoopInstrumentation,
        "tokio.op",
        &TokioSleepWait,
        || {
            n += 1;
            let attempt = n;
            async move {
                if attempt == 1 {
                    return Err(ResiliencxError::transient("once"));
                }
                Ok(retry_ok(attempt))
            }
        },
    )
    .await
    .expect("ok");
    assert_eq!(retry_downcast::<u32>(boxed).unwrap(), 2);
}

#[tokio::test]
async fn async_wait_nowait_is_noop() {
    // 对象安全：&dyn AsyncWait
    let w: &dyn AsyncWait = &NoWait;
    w.wait_ms(1000).await;
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn tokio_sleep_wait_zero_is_noop_and_debug() {
    use resiliencx::TokioSleepWait;
    let w = TokioSleepWait;
    let _ = format!("{w:?}");
    AsyncWait::wait_ms(&w, 0).await;
}

#[tokio::test]
async fn retry_async_budget_exhaustion_returns_standard_error() {
    let cfg = RetryConfig::fixed(5, 0);
    let budget = RetryBudget::new(1);
    let events = RetryEvents::default();
    let mut calls = 0u32;

    let err = retry_async_with_budget(&cfg, &events, "async.budget", &NoWait, &budget, || {
        calls += 1;
        async { Err(ResiliencxError::transient("稍后重试")) }
    })
    .await
    .expect_err("预算应在第三次调用前耗尽");

    assert_eq!(calls, 2);
    assert_eq!(err.kind(), ErrorKind::Unavailable);
    assert_eq!(err.to_string(), budget_exhausted_error().to_string());
    assert_eq!(*events.0.lock().expect("读取异步重试事件"), vec![1]);
}

#[tokio::test]
async fn safe_async_retry_rejects_unsafe_side_effect_before_polling_operation() {
    let cfg = RetryConfig::fixed(3, 0);
    let mut calls = 0u32;

    let err = retry_async_safe(
        RetryContext::new(
            &cfg,
            RetrySafety::UnsafeSideEffect,
            &NoopInstrumentation,
            "payment",
        ),
        &NoWait,
        || {
            calls += 1;
            async { Ok(retry_ok(())) }
        },
    )
    .await
    .expect_err("不安全副作用不得异步重试");

    assert_eq!(err.kind(), ErrorKind::Invalid);
    assert_eq!(calls, 0);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn whole_retry_deadline_maps_to_deadline_exceeded() {
    use resiliencx::retry_async_with_deadline;
    use std::time::Duration;

    let cfg = RetryConfig::fixed(3, 0);
    let err = retry_async_with_deadline(
        RetryContext::new(
            &cfg,
            RetrySafety::ReadOnly,
            &NoopInstrumentation,
            "slow.read",
        ),
        &NoWait,
        Duration::from_millis(1),
        std::future::pending,
    )
    .await
    .expect_err("整次重试 deadline 应超时");

    assert_eq!(err.kind(), ErrorKind::DeadlineExceeded);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn whole_retry_deadline_returns_fast_success() {
    use resiliencx::retry_async_with_deadline;
    use std::time::Duration;

    let value = retry_async_with_deadline(
        RetryContext::new(
            &RetryConfig::fixed(2, 0),
            RetrySafety::Idempotent,
            &NoopInstrumentation,
            "fast.write",
        ),
        &NoWait,
        Duration::from_secs(1),
        || async { Ok(retry_ok(9u8)) },
    )
    .await
    .expect("deadline 内应成功");

    assert_eq!(retry_downcast::<u8>(value).expect("类型"), 9);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn deadline_during_backoff_does_not_consume_budget_or_record_retry() {
    use resiliencx::retry_async_with_deadline;
    use std::time::Duration;

    let budget = RetryBudget::new(1);
    let events = RetryEvents::default();
    let mut calls = 0u32;
    let err = retry_async_with_deadline(
        RetryContext::new(
            &RetryConfig::fixed(3, 10),
            RetrySafety::ReadOnly,
            &events,
            "slow.backoff",
        )
        .with_budget(&budget)
        .with_jitter_seed(17),
        &PendingWait,
        Duration::from_millis(1),
        || {
            calls += 1;
            async { Err(ResiliencxError::transient("等待后重试")) }
        },
    )
    .await
    .expect_err("退避期间应触发整次 deadline");

    assert_eq!(err.kind(), ErrorKind::DeadlineExceeded);
    assert_eq!(calls, 1);
    assert_eq!(budget.remaining(), 1);
    assert!(events.0.lock().expect("读取退避观测事件").is_empty());
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn exhausted_budget_returns_before_pending_backoff() {
    use std::time::Duration;

    let budget = RetryBudget::new(1);
    assert!(budget.try_consume());
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        retry_async_with_budget(
            &RetryConfig::fixed(2, 10),
            &NoopInstrumentation,
            "exhausted.before.wait",
            &PendingWait,
            &budget,
            || async { Err(ResiliencxError::transient("重试")) },
        ),
    )
    .await
    .expect("预算耗尽必须在进入永久 wait 前返回");
    let err = result.expect_err("预算耗尽");

    assert_eq!(err.to_string(), budget_exhausted_error().to_string());
    assert_eq!(budget.remaining(), 0, "失败的 async reserve 不得复活令牌");
}

#[tokio::test]
async fn successful_async_retry_commits_exactly_one_budget_token() {
    let budget = RetryBudget::new(2);
    let events = RetryEvents::default();
    let mut calls = 0u32;
    let value = retry_async_with_budget(
        &RetryConfig::fixed(2, 0),
        &events,
        "normal.budget",
        &NoWait,
        &budget,
        || {
            calls += 1;
            let attempt = calls;
            async move {
                if attempt == 1 {
                    Err(ResiliencxError::transient("重试"))
                } else {
                    Ok(retry_ok(attempt))
                }
            }
        },
    )
    .await
    .expect("第二次成功");

    assert_eq!(retry_downcast::<u32>(value).expect("类型"), 2);
    assert_eq!(budget.remaining(), 1);
    assert_eq!(*events.0.lock().expect("读取正常预算事件"), vec![1]);
}
