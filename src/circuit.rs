//! 熔断器（三态；**无墙钟**——Open→HalfOpen 由拒绝次数推进，便于确定性测试）。
//!
//! 状态机：
//! - **Closed**：正常执行；连续失败达 `failure_threshold` → Open，并 `record_circuit_open`
//! - **Open**：直接拒绝（`Unavailable`）；拒绝次数达 `open_to_half_open_after_rejects` → HalfOpen
//! - **HalfOpen**：试探调用；连续成功达 `success_threshold` → Closed + `record_circuit_close`；
//!   任一次失败 → Open + `record_circuit_open`

use crate::error::{ResiliencxError, ResiliencxResult};
use crate::Instrumentation;

/// 熔断器状态。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// 正常放行。
    Closed,
    /// 短路拒绝。
    Open,
    /// 试探放行。
    HalfOpen,
}

/// 熔断配置（阈值均须 ≥ 1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitConfig {
    /// Closed 下连续失败次数达到后跳闸。
    pub failure_threshold: u32,
    /// HalfOpen 下连续成功次数达到后闭合。
    pub success_threshold: u32,
    /// Open 下累计拒绝次数达到后进入 HalfOpen。
    pub open_to_half_open_after_rejects: u32,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            open_to_half_open_after_rejects: 3,
        }
    }
}

/// 三态熔断器。
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    config: CircuitConfig,
    state: CircuitState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    rejects_in_open: u32,
}

impl CircuitBreaker {
    /// 校验配置并构造 Closed 状态实例。
    pub fn new(config: CircuitConfig) -> ResiliencxResult<Self> {
        if config.failure_threshold == 0
            || config.success_threshold == 0
            || config.open_to_half_open_after_rejects == 0
        {
            return Err(ResiliencxError::invalid(
                "circuit thresholds must be >= 1 (failure/success/open_to_half_open)",
            ));
        }
        Ok(Self {
            config,
            state: CircuitState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            rejects_in_open: 0,
        })
    }

    /// 当前状态。
    #[must_use]
    pub fn state(&self) -> CircuitState {
        self.state
    }

    /// 配置快照。
    #[must_use]
    pub fn config(&self) -> CircuitConfig {
        self.config
    }

    /// 经熔断保护执行 `f`。
    ///
    /// Open 时不调用 `f`，返回 `ResiliencxError::unavailable("熔断器已打开")`。
    pub fn call<R>(
        &mut self,
        instrumentation: &dyn Instrumentation,
        op: &str,
        f: impl FnOnce() -> ResiliencxResult<R>,
    ) -> ResiliencxResult<R> {
        match self.state {
            CircuitState::Open => {
                self.rejects_in_open = self.rejects_in_open.saturating_add(1);
                if self.rejects_in_open >= self.config.open_to_half_open_after_rejects {
                    self.state = CircuitState::HalfOpen;
                    self.rejects_in_open = 0;
                    self.consecutive_successes = 0;
                    self.consecutive_failures = 0;
                }
                return Err(ResiliencxError::unavailable("熔断器已打开"));
            }
            CircuitState::Closed | CircuitState::HalfOpen => {}
        }

        match f() {
            Ok(v) => {
                self.on_success(instrumentation, op);
                Ok(v)
            }
            Err(e) => {
                // 仅临时性（可重试）错误计入熔断计数；永久错误（配置/参数类错误）不计入，
                // 避免把调用方自身错误误判为下游故障导致断路器误跳闸。
                if e.is_retryable() {
                    self.on_failure(instrumentation, op);
                }
                Err(e)
            }
        }
    }

    fn on_success(&mut self, instrumentation: &dyn Instrumentation, op: &str) {
        self.consecutive_failures = 0;
        // call() 仅在 Closed|HalfOpen 下执行 f
        if self.state == CircuitState::HalfOpen {
            self.consecutive_successes = self.consecutive_successes.saturating_add(1);
            if self.consecutive_successes >= self.config.success_threshold {
                self.state = CircuitState::Closed;
                self.consecutive_successes = 0;
                self.rejects_in_open = 0;
                instrumentation.record_circuit_close(op);
            }
        } else {
            self.consecutive_successes = 0;
        }
    }

    fn on_failure(&mut self, instrumentation: &dyn Instrumentation, op: &str) {
        self.consecutive_successes = 0;
        if self.state == CircuitState::HalfOpen {
            self.trip_open(instrumentation, op);
        } else {
            // Closed
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            if self.consecutive_failures >= self.config.failure_threshold {
                self.trip_open(instrumentation, op);
            }
        }
    }

    fn trip_open(&mut self, instrumentation: &dyn Instrumentation, op: &str) {
        self.state = CircuitState::Open;
        self.consecutive_failures = 0;
        self.consecutive_successes = 0;
        self.rejects_in_open = 0;
        instrumentation.record_circuit_open(op);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopInstrumentation;
    use std::sync::Mutex;

    struct CountingInstr {
        open: Mutex<u32>,
        close: Mutex<u32>,
    }

    impl Instrumentation for CountingInstr {
        fn record_retry(&self, _: &str, _: u32) {}
        fn record_circuit_open(&self, _: &str) {
            *self.open.lock().expect("o") += 1;
        }
        fn record_circuit_close(&self, _: &str) {
            *self.close.lock().expect("c") += 1;
        }
    }

    fn cfg() -> CircuitConfig {
        CircuitConfig {
            failure_threshold: 2,
            success_threshold: 2,
            open_to_half_open_after_rejects: 2,
        }
    }

    #[test]
    fn rejects_zero_thresholds() {
        for c in [
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
            assert!(CircuitBreaker::new(c).is_err());
        }
    }

    #[test]
    fn closed_success_resets_and_stays_closed() {
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = NoopInstrumentation;
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.config().failure_threshold, 2);
        let v = cb
            .call(&instr, "op", || Ok::<_, ResiliencxError>(7))
            .expect("ok");
        assert_eq!(v, 7);
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn trips_open_after_failure_threshold() {
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = CountingInstr {
            open: Mutex::new(0),
            close: Mutex::new(0),
        };
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("1"))
        });
        assert_eq!(cb.state(), CircuitState::Closed);
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("2"))
        });
        assert_eq!(cb.state(), CircuitState::Open);
        assert_eq!(*instr.open.lock().expect("o"), 1);
    }

    #[test]
    fn open_rejects_then_half_open_then_close() {
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = CountingInstr {
            open: Mutex::new(0),
            close: Mutex::new(0),
        };
        // trip（使用 transient 错误触发跳闸）
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("a"))
        });
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("b"))
        });
        assert_eq!(cb.state(), CircuitState::Open);

        // first reject stays Open; second reject transitions to HalfOpen after returning
        let e1 = cb.call(&instr, "x", || Ok(())).expect_err("reject1");
        assert_eq!(e1.kind(), crate::error::ErrorKind::Unavailable);
        // after 1 reject still Open (threshold 2)
        assert_eq!(cb.state(), CircuitState::Open);
        let _ = cb.call(&instr, "x", || Ok(())).expect_err("reject2");
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // half-open successes
        cb.call(&instr, "x", || Ok(())).expect("s1");
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        cb.call(&instr, "x", || Ok(())).expect("s2");
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(*instr.close.lock().expect("c"), 1);
    }

    #[test]
    fn half_open_failure_reopens() {
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = CountingInstr {
            open: Mutex::new(0),
            close: Mutex::new(0),
        };
        // 使用 transient 错误触发跳闸
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("a"))
        });
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("b"))
        });
        let _ = cb.call(&instr, "x", || Ok(()));
        let _ = cb.call(&instr, "x", || Ok(()));
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        let opens_before = *instr.open.lock().expect("o");
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("boom"))
        });
        assert_eq!(cb.state(), CircuitState::Open);
        assert_eq!(*instr.open.lock().expect("o"), opens_before + 1);
    }

    #[test]
    fn default_config_constructible() {
        let cb = CircuitBreaker::new(CircuitConfig::default()).expect("def");
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.config().failure_threshold, 5);
        // 覆盖 CountingInstr::record_retry 空实现（本模块 mock 完整 Instrumentation）
        let instr = CountingInstr {
            open: Mutex::new(0),
            close: Mutex::new(0),
        };
        instr.record_retry("probe", 1);
    }

    // ── 回归：熔断仅对临时性错误计数 ──────────────────────────────────

    #[test]
    fn permanent_errors_do_not_trip_circuit() {
        // 永久错误（如 Invalid）属调用方自身问题，不应计入熔断计数。
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = NoopInstrumentation;
        for _ in 0..(cfg().failure_threshold + 5) {
            let _ = cb.call(&instr, "x", || {
                Err::<(), _>(ResiliencxError::invalid("永久错误"))
            });
            assert_eq!(
                cb.state(),
                CircuitState::Closed,
                "永久错误不应改变断路器状态"
            );
        }
    }

    #[test]
    fn transient_errors_still_trip_circuit() {
        // 临时性错误达到阈值后仍应触发 Open。
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = NoopInstrumentation;
        let thresh = cfg().failure_threshold;
        for _ in 0..thresh - 1 {
            let _ = cb.call(&instr, "x", || {
                Err::<(), _>(ResiliencxError::transient("临时"))
            });
            assert_eq!(cb.state(), CircuitState::Closed);
        }
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("最后一次"))
        });
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn mixed_errors_only_transient_counts() {
        // 永久错误穿插在临时错误之间，不重置也不累加计数器。
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = NoopInstrumentation;
        // 永久错误：不计入
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::invalid("永久"))
        });
        assert_eq!(cb.state(), CircuitState::Closed);
        // 临时错误 1
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("t1"))
        });
        assert_eq!(cb.state(), CircuitState::Closed);
        // 永久错误：不计入，不应重置 transient 计数
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::invalid("永久2"))
        });
        assert_eq!(cb.state(), CircuitState::Closed);
        // 临时错误 2 → 达到阈值
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::transient("t2"))
        });
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn half_open_permanent_error_does_not_reopen() {
        // HalfOpen 下永久错误不表示下游故障，不应重新跳闸。
        let mut cb = CircuitBreaker::new(cfg()).expect("cb");
        let instr = NoopInstrumentation;
        // 先通过 transient 错误进入 Open
        for _ in 0..cfg().failure_threshold {
            let _ = cb.call(&instr, "x", || {
                Err::<(), _>(ResiliencxError::transient("t"))
            });
        }
        assert_eq!(cb.state(), CircuitState::Open);
        // 拒绝达阈值 → HalfOpen
        for _ in 0..cfg().open_to_half_open_after_rejects {
            let _ = cb.call(&instr, "x", || Ok(()));
        }
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        // HalfOpen 下收到永久错误：不重新打开
        let _ = cb.call(&instr, "x", || {
            Err::<(), _>(ResiliencxError::invalid("永久"))
        });
        assert_eq!(
            cb.state(),
            CircuitState::HalfOpen,
            "永久错误在 HalfOpen 下不应导致重新跳闸"
        );
        // 后续成功仍可闭合
        for _ in 0..cfg().success_threshold {
            cb.call(&instr, "x", || Ok(())).expect("成功");
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }
}
