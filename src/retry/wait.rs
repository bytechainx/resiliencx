//! 退避策略与等待抽象。
//!
//! 自 `src/retry.rs` 下沉而来：`Backoff`、`Wait`（阻塞）/ `AsyncWait`（对象安全）两个 trait
//! 与三个实现（`ThreadSleepWait` / `NoWait` / `RecordingWait` / `TokioSleepWait`）。
//! 公开项经门面 `pub use` 转出，故 `resiliencx::retry::{…}` 与 crate 根的 re-export 路径不变。

use std::thread;
use std::time::Duration;

use async_trait::async_trait;

/// 退避策略（相对 [`RetryConfig::base_delay_ms`]）。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Backoff {
    /// 每次重试使用固定 `base_delay_ms`（历史默认）。
    #[default]
    Constant,
    /// 指数：`min(base * factor^(attempt-1), max_delay_ms)`；`attempt` 为已完成失败次数（≥1）。
    Exponential {
        /// 倍数，须 ≥ 1；`1` 等价常数。
        factor: u32,
        /// 上限毫秒。
        max_delay_ms: u64,
    },
}

/// 等待策略（对象安全）。生产默认 [`ThreadSleepWait`]；测试可用 [`NoWait`] / [`RecordingWait`]。
pub trait Wait: Send + Sync {
    /// 等待 `ms` 毫秒（`0` 应为空操作）。
    fn wait_ms(&self, ms: u64);
}

/// 使用 [`thread::sleep`] 的默认 wait（会阻塞调用线程）。
#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadSleepWait;

impl Wait for ThreadSleepWait {
    fn wait_ms(&self, ms: u64) {
        if ms > 0 {
            thread::sleep(Duration::from_millis(ms));
        }
    }
}

/// 空 wait（不睡眠；配合 delay 计算测试）。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoWait;

impl Wait for NoWait {
    fn wait_ms(&self, _ms: u64) {}
}

/// 记录每次请求延迟的 wait（测试用；不睡眠）。
#[derive(Debug, Default)]
pub struct RecordingWait {
    delays: std::sync::Mutex<Vec<u64>>,
}

/// 取得延迟记录锁；锁中毒（此前持锁线程 panic）时恢复内层数据，避免诊断辅助路径 panic。
#[track_caller]
fn lock_delays_or_recover(
    delays: &std::sync::Mutex<Vec<u64>>,
) -> std::sync::MutexGuard<'_, Vec<u64>> {
    match delays.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(
                lock = "resiliencx::retry::RecordingWait::delays",
                at = %std::panic::Location::caller(),
                "锁中毒已恢复：持锁线程此前 panic，返回已持有数据"
            );
            poisoned.into_inner()
        }
    }
}

impl RecordingWait {
    /// 构造空记录器。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 已记录的延迟序列（毫秒）。
    #[must_use]
    pub fn delays(&self) -> Vec<u64> {
        lock_delays_or_recover(&self.delays).clone()
    }
}

impl Wait for RecordingWait {
    fn wait_ms(&self, ms: u64) {
        lock_delays_or_recover(&self.delays).push(ms);
    }
}

// ── AsyncWait / 非阻塞重试 ─────────────────────────────────────────────────

/// 异步等待策略（对象安全）。
///
/// async 服务路径应使用本 trait + [`retry_async`]，**禁止**在 async 任务中直接调用
/// 默认阻塞的 [`retry_fn`] / [`ThreadSleepWait`]。
#[async_trait]
pub trait AsyncWait: Send + Sync {
    /// 异步等待 `ms` 毫秒（`0` 应为空操作）。
    async fn wait_ms(&self, ms: u64);
}

#[async_trait]
impl AsyncWait for NoWait {
    async fn wait_ms(&self, _ms: u64) {}
}

#[async_trait]
impl AsyncWait for RecordingWait {
    async fn wait_ms(&self, ms: u64) {
        lock_delays_or_recover(&self.delays).push(ms);
    }
}

/// 基于 `tokio::time::sleep` 的非阻塞 wait（feature `tokio`）。
///
/// 在 async runtime 内 `await` 等待，**不**占用阻塞线程。
#[cfg(feature = "tokio")]
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioSleepWait;

#[cfg(feature = "tokio")]
#[async_trait]
impl AsyncWait for TokioSleepWait {
    async fn wait_ms(&self, ms: u64) {
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }
}
