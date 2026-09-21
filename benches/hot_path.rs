#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! resiliencx 热路径：retry_fn + RateLimiter。
use std::hint::black_box;
use std::time::Instant;

use resiliencx::ResiliencxError;
use resiliencx::{
    retry_downcast, retry_fn, retry_ok, NoopInstrumentation, RateLimitConfig, RateLimiter,
    RetryConfig,
};

fn iters() -> u32 {
    if std::env::args().any(|a| a == "--quick") {
        1_000
    } else {
        50_000
    }
}

fn main() {
    let n = iters();
    let instr = NoopInstrumentation;
    let cfg = RetryConfig::fixed(2, 0);
    let mut lim = RateLimiter::new(RateLimitConfig { capacity: n }).expect("lim");
    for _ in 0..n.min(20) {
        let mut op = || Ok(retry_ok(1u32));
        let _ = retry_fn(&cfg, &instr, "w", &mut op);
    }
    let start = Instant::now();
    let mut acc = 0u64;
    for i in 0..n {
        let mut hits = 0u8;
        let mut op = || {
            hits = hits.wrapping_add(1);
            if hits == 1 && i % 17 == 0 {
                Err(ResiliencxError::transient("t"))
            } else {
                Ok(retry_ok(i))
            }
        };
        let out = retry_fn(&cfg, &instr, "bench", &mut op).expect("ok");
        acc = acc.wrapping_add(u64::from(retry_downcast::<u32>(out).expect("ty")));
        lim.try_acquire(1).expect("tok");
        black_box(i);
    }
    let elapsed = start.elapsed();
    println!(
        "bench_resiliencx_retry: iters={n} total={elapsed:?} per_iter={:?} acc={}",
        elapsed / n,
        black_box(acc)
    );
}
