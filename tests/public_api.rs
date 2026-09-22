#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 库外消费者视角：公开 API 可引用。

use resiliencx::{
    retry_downcast, retry_fn, retry_fn_with_wait, retry_ok, Backoff, Bulkhead, BulkheadConfig,
    CircuitBreaker, CircuitConfig, CircuitState, Instrumentation, NoopInstrumentation,
    RateLimitConfig, RateLimiter, RecordingWait, RetryConfig,
};
use resiliencx::{ErrorKind, ResiliencxError, ResiliencxResult};
use std::sync::{Arc, Mutex};

struct CountingInstr {
    n: Mutex<u32>,
    open: Mutex<u32>,
    close: Mutex<u32>,
}

impl Instrumentation for CountingInstr {
    fn record_retry(&self, _op: &str, _attempt: u32) {
        *self.n.lock().expect("lock") += 1;
    }
    fn record_circuit_open(&self, _op: &str) {
        *self.open.lock().expect("o") += 1;
    }
    fn record_circuit_close(&self, _op: &str) {
        *self.close.lock().expect("c") += 1;
    }
}

#[test]
fn consumer_retry_fn_drives_shipped_surface() {
    let instr = CountingInstr {
        n: Mutex::new(0),
        open: Mutex::new(0),
        close: Mutex::new(0),
    };
    let cfg = RetryConfig::fixed(3, 0);
    let hits = Arc::new(Mutex::new(0u32));
    let h = hits.clone();
    let mut op = move || {
        let mut g = h.lock().expect("hits");
        *g += 1;
        if *g == 1 {
            Err(ResiliencxError::transient("try again"))
        } else {
            Ok(retry_ok("done"))
        }
    };
    let out: ResiliencxResult<_> = retry_fn(&cfg, &instr, "consumer-op", &mut op);
    let s = retry_downcast::<&'static str>(out.expect("ok")).expect("ty");
    assert_eq!(s, "done");
    assert_eq!(*instr.n.lock().expect("n"), 1);
    assert_eq!(*hits.lock().expect("hits"), 2);
}

#[test]
fn consumer_non_retryable_stops_immediately() {
    let instr = NoopInstrumentation;
    let cfg = RetryConfig::default();
    let mut op = || Err(ResiliencxError::invalid("nope"));
    let result = retry_fn(&cfg, &instr, "bad", &mut op);
    assert_eq!(result.expect_err("err").kind(), ErrorKind::Invalid);
}

#[test]
fn consumer_default_config() {
    let cfg = RetryConfig::default();
    assert_eq!(cfg.max_attempts, 3);
    assert_eq!(cfg.base_delay_ms, 0);
}

#[test]
fn consumer_circuit_and_rate_limit_surface() {
    let instr = CountingInstr {
        n: Mutex::new(0),
        open: Mutex::new(0),
        close: Mutex::new(0),
    };
    let mut cb = CircuitBreaker::new(CircuitConfig {
        failure_threshold: 1,
        success_threshold: 1,
        open_to_half_open_after_rejects: 1,
    })
    .expect("cb");
    let _ = cb.call(&instr, "c", || {
        Err::<(), _>(ResiliencxError::transient("trip"))
    });
    assert_eq!(cb.state(), CircuitState::Open);
    assert_eq!(*instr.open.lock().expect("o"), 1);
    // open reject → half-open
    let _ = cb.call(&instr, "c", || Ok(())).expect_err("rej");
    assert_eq!(cb.state(), CircuitState::HalfOpen);
    cb.call(&instr, "c", || Ok(())).expect("close");
    assert_eq!(cb.state(), CircuitState::Closed);
    assert_eq!(*instr.close.lock().expect("c"), 1);

    let mut lim = RateLimiter::new(RateLimitConfig { capacity: 1 }).expect("lim");
    lim.try_acquire(1).expect("tok");
    assert_eq!(
        lim.try_acquire(1).expect_err("rl").kind(),
        ErrorKind::Unavailable
    );
    lim.refill(1);
    lim.try_acquire(1).expect("after refill");
}

#[test]
fn consumer_bulkhead_surface() {
    let b = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("bh"));
    let p = b.try_enter().expect("enter");
    assert_eq!(
        b.call(|| Ok(())).expect_err("full").kind(),
        ErrorKind::Unavailable
    );
    drop(p);
    assert_eq!(b.call(|| Ok(7)).expect("ok"), 7);
}

#[test]
fn consumer_backoff_with_recording_wait() {
    let instr = CountingInstr {
        n: Mutex::new(0),
        open: Mutex::new(0),
        close: Mutex::new(0),
    };
    let cfg = RetryConfig {
        max_attempts: 3,
        base_delay_ms: 4,
        backoff: Backoff::Exponential {
            factor: 2,
            max_delay_ms: 100,
        },
        jitter_bps: 0,
    };
    let wait = RecordingWait::new();
    let hits = Arc::new(Mutex::new(0u32));
    let h = hits.clone();
    let mut op = move || {
        let mut g = h.lock().expect("h");
        *g += 1;
        if *g < 3 {
            Err(ResiliencxError::transient("t"))
        } else {
            Ok(retry_ok(9u8))
        }
    };
    let out = retry_fn_with_wait(&cfg, &instr, "pub", &wait, &mut op).expect("ok");
    assert_eq!(retry_downcast::<u8>(out).expect("ty"), 9);
    assert_eq!(wait.delays(), vec![4, 8]);
}
