#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! async 重试：使用 [`retry_async`] + 非阻塞 wait。
//!
//! ```bash
//! cargo run -p resiliencx --example retry_async_demo
//! cargo run -p resiliencx --example retry_async_demo --features tokio
//! ```
//!
//! # 生产红线
//! - **禁止** 在 async 任务里直接 `retry_fn`（默认 `ThreadSleepWait` 阻塞 runtime）。
//! - async 路径用 `retry_async` + `AsyncWait`（feature `tokio` → `TokioSleepWait`）。

use resiliencx::ResiliencxError;
use resiliencx::{
    retry_async, retry_downcast, retry_ok, Backoff, NoopInstrumentation, RetryConfig,
};

#[tokio::main]
async fn main() {
    let cfg = RetryConfig {
        max_attempts: 3,
        base_delay_ms: 1,
        backoff: Backoff::Constant,
        jitter_bps: 0,
    };

    #[cfg(not(feature = "tokio"))]
    let wait = resiliencx::NoWait;
    #[cfg(feature = "tokio")]
    let wait = resiliencx::TokioSleepWait;

    let mut n = 0u32;
    let boxed = retry_async(&cfg, &NoopInstrumentation, "demo.async", &wait, || {
        n += 1;
        let attempt = n;
        async move {
            if attempt < 2 {
                return Err(ResiliencxError::transient("retry me"));
            }
            Ok(retry_ok(attempt))
        }
    })
    .await
    .expect("retry_async");

    let attempts: u32 = retry_downcast(boxed).unwrap();
    println!("retry_async_ok attempts={attempts}");
}
