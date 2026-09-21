//! 并发舱壁（bulkhead）：限制同时进入保护段的调用数。
//!
//! **无墙钟、无排队超时**——满载时立即 `Unavailable`；许可用 RAII 归还（含 panic unwind）。

use crate::error::{ResiliencxError, ResiliencxResult};
use std::sync::{Arc, Mutex, MutexGuard};

/// 舱壁配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkheadConfig {
    /// 最大并发（须 ≥ 1）。
    pub max_concurrent: u32,
}

impl Default for BulkheadConfig {
    fn default() -> Self {
        Self { max_concurrent: 10 }
    }
}

/// 并发舱壁。
///
/// 共享方式：`Arc<Bulkhead>` + [`Bulkhead::try_enter`]。
#[derive(Debug)]
pub struct Bulkhead {
    max_concurrent: u32,
    state: Mutex<u32>,
}

impl Bulkhead {
    fn lock_state(&self) -> MutexGuard<'_, u32> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                self.state.clear_poison();
                poisoned.into_inner()
            }
        }
    }

    /// 构造舱壁；`max_concurrent == 0` → Invalid。
    pub fn new(config: BulkheadConfig) -> ResiliencxResult<Self> {
        if config.max_concurrent == 0 {
            return Err(ResiliencxError::invalid(
                "舱壁 max_concurrent 必须大于或等于 1",
            ));
        }
        Ok(Self {
            max_concurrent: config.max_concurrent,
            state: Mutex::new(0),
        })
    }

    /// 最大并发。
    #[must_use]
    pub fn max_concurrent(&self) -> u32 {
        self.max_concurrent
    }

    /// 当前在途数（观测/测试）。
    #[must_use]
    pub fn in_flight(&self) -> u32 {
        *self.lock_state()
    }

    /// 尝试获取许可；满载 → `Unavailable("bulkhead full")`。
    pub fn try_enter(self: &Arc<Self>) -> ResiliencxResult<BulkheadPermit> {
        let mut g = self.lock_state();
        if *g >= self.max_concurrent {
            return Err(ResiliencxError::unavailable("舱壁容量已满"));
        }
        *g = g.saturating_add(1);
        drop(g);
        Ok(BulkheadPermit {
            owner: Arc::clone(self),
        })
    }

    /// 在舱壁保护下执行 `f`（获取许可 → 调用 → 归还）。
    pub fn call<R>(
        self: &Arc<Self>,
        f: impl FnOnce() -> ResiliencxResult<R>,
    ) -> ResiliencxResult<R> {
        let _permit = self.try_enter()?;
        f()
    }
}

/// 舱壁许可；drop 时归还并发槽。
#[derive(Debug)]
pub struct BulkheadPermit {
    owner: Arc<Bulkhead>,
}

impl Drop for BulkheadPermit {
    fn drop(&mut self) {
        let mut g = self.owner.lock_state();
        *g = g.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use std::sync::Arc;

    #[test]
    fn rejects_zero_max() {
        assert!(Bulkhead::new(BulkheadConfig { max_concurrent: 0 }).is_err());
    }

    #[test]
    fn default_config_and_call() {
        let b = Arc::new(Bulkhead::new(BulkheadConfig::default()).expect("bh"));
        assert_eq!(b.max_concurrent(), 10);
        assert_eq!(b.in_flight(), 0);
        let v = b.call(|| Ok::<_, ResiliencxError>(42)).expect("ok");
        assert_eq!(v, 42);
        assert_eq!(b.in_flight(), 0);
    }

    #[test]
    fn full_rejects_then_release_allows() {
        let b = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("bh"));
        let p = b.try_enter().expect("enter");
        assert_eq!(b.in_flight(), 1);
        let e = b.try_enter().expect_err("full");
        assert_eq!(e.kind(), ErrorKind::Unavailable);
        let e2 = b.call(|| Ok(())).expect_err("call full");
        assert_eq!(e2.kind(), ErrorKind::Unavailable);
        drop(p);
        assert_eq!(b.in_flight(), 0);
        b.call(|| Ok(())).expect("after release");
    }

    #[test]
    fn permit_drop_on_err_path() {
        let b = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("bh"));
        let err = b
            .call(|| Err::<(), _>(ResiliencxError::invalid("boom")))
            .expect_err("e");
        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert_eq!(b.in_flight(), 0);
        b.try_enter().expect("slot free");
    }

    #[test]
    fn concurrent_slots_two() {
        let b = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 2 }).expect("bh"));
        let p1 = b.try_enter().expect("1");
        let p2 = b.try_enter().expect("2");
        assert_eq!(b.in_flight(), 2);
        assert!(b.try_enter().is_err());
        drop(p1);
        assert_eq!(b.in_flight(), 1);
        let p3 = b.try_enter().expect("3");
        drop(p2);
        drop(p3);
        assert_eq!(b.in_flight(), 0);
    }

    #[test]
    fn poisoned_state_does_not_leak_permit_capacity() {
        let b = Arc::new(Bulkhead::new(BulkheadConfig { max_concurrent: 1 }).expect("舱壁"));
        let permit = b.try_enter().expect("获取许可");

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = b.state.lock().expect("获取内部状态锁");
            panic!("注入舱壁状态毒锁");
        }));

        drop(permit);
        assert_eq!(b.in_flight(), 0);
        let recovered = b.try_enter().expect("毒锁恢复后容量应可再次使用");
        drop(recovered);
    }
}
