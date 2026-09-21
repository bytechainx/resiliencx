//! 令牌桶限流（**无墙钟**——调用方显式 [`RateLimiter::refill`]，便于确定性测试）。
//!
//! - `try_acquire(n)`：有足够令牌则扣减；否则返回 `Unavailable`
//! - `refill(n)`：补充令牌，不超过 `capacity`

use crate::error::{ResiliencxError, ResiliencxResult};

/// 限流配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitConfig {
    /// 桶容量（最大令牌数，须 ≥ 1）。
    pub capacity: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self { capacity: 10 }
    }
}

/// 令牌桶限流器（满桶起步）。
#[derive(Debug, Clone)]
pub struct RateLimiter {
    capacity: u32,
    tokens: u32,
}

impl RateLimiter {
    /// 构造满桶实例；`capacity == 0` → Invalid。
    pub fn new(config: RateLimitConfig) -> ResiliencxResult<Self> {
        if config.capacity == 0 {
            return Err(ResiliencxError::invalid("限流器 capacity 必须大于或等于 1"));
        }
        Ok(Self {
            capacity: config.capacity,
            tokens: config.capacity,
        })
    }

    /// 当前可用令牌。
    #[must_use]
    pub fn available(&self) -> u32 {
        self.tokens
    }

    /// 桶容量。
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// 尝试获取 `n` 个令牌。
    ///
    /// - `n == 0`：成功、不改动
    /// - 令牌不足：`Unavailable`
    pub fn try_acquire(&mut self, n: u32) -> ResiliencxResult<()> {
        if n == 0 {
            return Ok(());
        }
        if self.tokens < n {
            return Err(ResiliencxError::unavailable("请求已被限流"));
        }
        self.tokens -= n;
        Ok(())
    }

    /// 补充最多 `n` 个令牌（不超过 capacity）。
    pub fn refill(&mut self, n: u32) {
        if n == 0 {
            return;
        }
        let room = self.capacity.saturating_sub(self.tokens);
        self.tokens = self.tokens.saturating_add(n.min(room));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn rejects_zero_capacity() {
        assert!(RateLimiter::new(RateLimitConfig { capacity: 0 }).is_err());
    }

    #[test]
    fn full_bucket_acquire_and_exhaust() {
        let mut lim = RateLimiter::new(RateLimitConfig { capacity: 2 }).expect("lim");
        assert_eq!(lim.capacity(), 2);
        assert_eq!(lim.available(), 2);
        lim.try_acquire(1).expect("1");
        assert_eq!(lim.available(), 1);
        lim.try_acquire(1).expect("2");
        assert_eq!(lim.available(), 0);
        let e = lim.try_acquire(1).expect_err("limited");
        assert_eq!(e.kind(), ErrorKind::Unavailable);
    }

    #[test]
    fn acquire_zero_is_noop() {
        let mut lim = RateLimiter::new(RateLimitConfig { capacity: 1 }).expect("lim");
        lim.try_acquire(0).expect("zero");
        assert_eq!(lim.available(), 1);
    }

    #[test]
    fn refill_caps_at_capacity() {
        let mut lim = RateLimiter::new(RateLimitConfig { capacity: 3 }).expect("lim");
        lim.try_acquire(3).expect("drain");
        lim.refill(0); // noop
        assert_eq!(lim.available(), 0);
        lim.refill(2);
        assert_eq!(lim.available(), 2);
        lim.refill(100);
        assert_eq!(lim.available(), 3);
    }

    #[test]
    fn default_config() {
        let lim = RateLimiter::new(RateLimitConfig::default()).expect("def");
        assert_eq!(lim.capacity(), 10);
        assert_eq!(lim.available(), 10);
    }

    #[test]
    fn acquire_more_than_available_fails_without_partial() {
        let mut lim = RateLimiter::new(RateLimitConfig { capacity: 3 }).expect("lim");
        lim.try_acquire(1).expect("one");
        let e = lim.try_acquire(3).expect_err("need 3 have 2");
        assert_eq!(e.kind(), ErrorKind::Unavailable);
        assert_eq!(lim.available(), 2); // no partial deduct
    }
}
