#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 同步重试最小路径：可注入 Wait，避免 async 路径误用默认 `ThreadSleepWait` 阻塞线程。
//!
//! ```bash
//! cargo run -p resiliencx --example retry_sync
//! ```
//!
//! # 生产红线
//! - `retry_fn` 默认使用 `ThreadSleepWait`（阻塞当前线程）。
//! - async 服务中请用 `retry_fn_with_wait` + 非阻塞 Wait（或外层编排）；请改用 `retry_async` + `AsyncWait`（feature `tokio`）。
//! - 熔断无墙钟冷却；限流需显式 `refill`。

use resiliencx::ResiliencxError;
use resiliencx::{
    retry_downcast, retry_fn_with_wait, retry_ok, Backoff, NoWait, NoopInstrumentation, RetryConfig,
};

fn main() {
    let cfg = RetryConfig {
        max_attempts: 3,
        base_delay_ms: 5,
        backoff: Backoff::Constant,
        jitter_bps: 0,
    };
    let instr = NoopInstrumentation;
    // 示例用 NoWait：可复现且不睡眠。生产默认可换 ThreadSleepWait（会阻塞线程）。
    let wait = NoWait;

    let mut n = 0u32;
    let boxed = retry_fn_with_wait(&cfg, &instr, "demo.op", &wait, &mut || {
        n += 1;
        if n < 3 {
            return Err(ResiliencxError::transient(format!("attempt {n} transient")));
        }
        Ok(retry_ok(n))
    })
    .expect("retry should succeed on 3rd attempt");

    let attempts: u32 = retry_downcast(boxed).expect("type");
    assert_eq!(attempts, 3);
    println!("retry_sync_ok attempts={attempts} wait=NoWait");
}
