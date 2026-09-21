//! 弹性原语的错误模型：语义分类 [`ErrorKind`] 与统一错误类型 [`ResiliencxError`]。
//!
//! 本 crate 的重试 / 预算 / 熔断 / 限流 / 舱壁原语需要一个**可判定的**错误语义：
//! 调用方必须能回答「这个错误值不值得重试」，而不是靠字符串匹配或类型断言。
//!
//! - [`ErrorKind`] 是语义分类，按「调用方应如何反应」划分；
//! - [`ResiliencxError`] 携带分类、人类可读上下文与可选的 `retry_after` 提示；
//! - [`ResiliencxError::is_retryable`] **仅**对 [`ErrorKind::Transient`] 返回 `true`。
//!
//! 本模块是 crate 内独立实现，不依赖任何外部错误框架：使用本 crate 的调用方需要把
//! 自己的错误映射为 [`ResiliencxError`]，再由原语按分类决定重试策略。
//!
//! # 示例
//!
//! ```
//! use resiliencx::{ErrorKind, ResiliencxError};
//!
//! let transient = ResiliencxError::transient("下游暂时不可用");
//! assert_eq!(transient.kind(), ErrorKind::Transient);
//! assert!(transient.is_retryable());
//!
//! let invalid = ResiliencxError::invalid("max_attempts 必须大于或等于 1");
//! assert_eq!(invalid.kind(), ErrorKind::Invalid);
//! assert!(!invalid.is_retryable());
//! ```

use std::borrow::Cow;
use std::fmt;

/// 可跨线程传递的装箱错误类型别名。
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// crate 专用 `Result` 别名。
pub type ResiliencxResult<T> = Result<T, ResiliencxError>;

/// 错误的语义分类，按「调用方应如何反应」划分。
///
/// 禁止通过字符串匹配或类型断言替代对本枚举的匹配。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// 请求、参数或输入本身非法。
    ///
    /// 反应：不自动重试；修正输入后可重新提交；不表示系统故障。
    Invalid,
    /// 请求的实体、资源或已声明依赖不存在。
    ///
    /// 反应：不立即自动重试；调用方可选择 fallback。
    Missing,
    /// 输入本身合法，但与当前状态冲突。
    ///
    /// 反应：不按瞬时故障自动重试；只有状态变化后重试才有意义。
    Conflict,
    /// 暂时性失败，保持相同语义的重试可能成功。
    ///
    /// 反应：可使用退避和抖动重试；`retry_after` 仅为提示。
    Transient,
    /// 下层依赖或必要基础能力不可用。
    ///
    /// 反应：默认传播；由组合根决定降级或 fail-fast。
    Unavailable,
    /// 操作被调用方或系统取消。
    ///
    /// 反应：不自动重试；不记录为内部故障；上层可将其视为正常终止路径。
    Cancelled,
    /// 操作未在调用方给定的 deadline 内完成。
    ///
    /// 反应：本次操作终止；是否重试由上层策略裁定。
    DeadlineExceeded,
    /// 内部不变量、前置条件或不可发生状态被破坏。
    ///
    /// 反应：不重试；视为 bug；必须进入错误预算、告警或受控 fail-fast。
    Invariant,
    /// 暂时无法归入以上类别的内部错误。
    ///
    /// 反应：不自动重试；应逐步收敛到更精确的分类。
    Internal,
}

/// 弹性原语的统一错误类型。
///
/// 所有字段均为私有，调用方只能通过构造器与查询方法使用错误语义。
pub struct ResiliencxError {
    kind: ErrorKind,
    context: Cow<'static, str>,
    retry_after: Option<std::time::Duration>,
    source: Option<BoxError>,
}

impl ResiliencxError {
    // -- 构造器 ------------------------------------------------------------

    /// 构造一个 [`ErrorKind::Invalid`] 错误。
    pub fn invalid(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, context, None)
    }

    /// 构造一个 [`ErrorKind::Missing`] 错误。
    pub fn missing(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Missing, context, None)
    }

    /// 构造一个 [`ErrorKind::Conflict`] 错误。
    pub fn conflict(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, context, None)
    }

    /// 构造一个 [`ErrorKind::Transient`] 错误，无 `retry_after`。
    pub fn transient(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, context, None)
    }

    /// 构造一个 [`ErrorKind::Transient`] 错误，附带 `retry_after` 提示。
    pub fn transient_after(context: impl Into<String>, retry_after: std::time::Duration) -> Self {
        Self::new(ErrorKind::Transient, context, Some(retry_after))
    }

    /// 构造一个 [`ErrorKind::Unavailable`] 错误。
    pub fn unavailable(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, context, None)
    }

    /// 构造一个 [`ErrorKind::Cancelled`] 错误。
    pub fn cancelled(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, context, None)
    }

    /// 构造一个 [`ErrorKind::DeadlineExceeded`] 错误。
    pub fn deadline_exceeded(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::DeadlineExceeded, context, None)
    }

    /// 构造一个 [`ErrorKind::Invariant`] 错误。
    pub fn invariant(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invariant, context, None)
    }

    /// 构造一个 [`ErrorKind::Internal`] 错误。
    ///
    /// 非紧急情况下应优先选择更精确的 `ErrorKind`。
    pub fn internal(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, context, None)
    }

    fn new(
        kind: ErrorKind,
        context: impl Into<String>,
        retry_after: Option<std::time::Duration>,
    ) -> Self {
        Self {
            kind,
            context: Cow::Owned(context.into()),
            retry_after,
            source: None,
        }
    }

    // -- 构建器 ------------------------------------------------------------

    /// 附加底层 error source，保持原有 [`ErrorKind`] 不变。
    #[must_use]
    pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.source = Some(source.into());
        self
    }

    // -- 查询方法 ----------------------------------------------------------

    /// 返回错误的语义分类。
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// 返回人类可读的上下文描述。
    #[must_use]
    pub fn context(&self) -> &str {
        &self.context
    }

    /// 返回建议的重试等待时间（仅 [`ErrorKind::Transient`] 可能非 `None`）。
    #[must_use]
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        self.retry_after
    }

    /// 仅当 [`ErrorKind::Transient`] 时返回 `true`。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.kind == ErrorKind::Transient
    }

    /// 仅当 [`ErrorKind::Invariant`] 时返回 `true`。
    #[must_use]
    pub fn is_bug(&self) -> bool {
        self.kind == ErrorKind::Invariant
    }
}

impl fmt::Display for ResiliencxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.context)
    }
}

impl fmt::Debug for ResiliencxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Debug 与 Display 一致，不展开 source 细节，避免把底层错误内容带进日志。
        f.debug_struct("ResiliencxError")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("retry_after", &self.retry_after)
            .field("source", &self.source.as_ref().map(|_| "..."))
            .finish()
    }
}

impl std::error::Error for ResiliencxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::{ErrorKind, ResiliencxError};
    use std::error::Error;
    use std::time::Duration;

    #[test]
    fn every_constructor_sets_the_matching_kind() {
        let cases: [(ResiliencxError, ErrorKind); 10] = [
            (ResiliencxError::invalid("i"), ErrorKind::Invalid),
            (ResiliencxError::missing("m"), ErrorKind::Missing),
            (ResiliencxError::conflict("c"), ErrorKind::Conflict),
            (ResiliencxError::transient("t"), ErrorKind::Transient),
            (
                ResiliencxError::transient_after("t", Duration::from_secs(5)),
                ErrorKind::Transient,
            ),
            (ResiliencxError::unavailable("u"), ErrorKind::Unavailable),
            (ResiliencxError::cancelled("ca"), ErrorKind::Cancelled),
            (
                ResiliencxError::deadline_exceeded("d"),
                ErrorKind::DeadlineExceeded,
            ),
            (ResiliencxError::invariant("inv"), ErrorKind::Invariant),
            (ResiliencxError::internal("int"), ErrorKind::Internal),
        ];
        for (error, expected) in cases {
            assert_eq!(error.kind(), expected);
            assert!(!error.context().is_empty());
            assert!(!error.to_string().is_empty());
        }
    }

    #[test]
    fn only_transient_is_retryable() {
        let kinds = [
            ErrorKind::Invalid,
            ErrorKind::Missing,
            ErrorKind::Conflict,
            ErrorKind::Unavailable,
            ErrorKind::Cancelled,
            ErrorKind::DeadlineExceeded,
            ErrorKind::Invariant,
            ErrorKind::Internal,
        ];
        for kind in kinds {
            assert!(
                !ResiliencxError::new(kind, "x", None).is_retryable(),
                "{kind:?} 不应可重试"
            );
        }
        assert!(ResiliencxError::transient("t").is_retryable());
        assert!(ResiliencxError::transient_after("t", Duration::from_secs(1)).is_retryable());
    }

    #[test]
    fn only_invariant_is_a_bug() {
        let kinds = [
            ErrorKind::Invalid,
            ErrorKind::Missing,
            ErrorKind::Conflict,
            ErrorKind::Transient,
            ErrorKind::Unavailable,
            ErrorKind::Cancelled,
            ErrorKind::DeadlineExceeded,
            ErrorKind::Internal,
        ];
        for kind in kinds {
            assert!(
                !ResiliencxError::new(kind, "x", None).is_bug(),
                "{kind:?} 不应是 bug"
            );
        }
        assert!(ResiliencxError::invariant("inv").is_bug());
    }

    #[test]
    fn retry_after_is_only_carried_by_transient_after() {
        let delay = Duration::from_millis(250);
        assert_eq!(
            ResiliencxError::transient_after("t", delay).retry_after(),
            Some(delay)
        );
        assert_eq!(ResiliencxError::transient("t").retry_after(), None);
    }

    #[test]
    fn display_carries_kind_and_context_but_not_source_details() {
        let error = ResiliencxError::internal("包装").with_source(std::io::Error::other("secret"));
        let display = error.to_string();
        assert!(display.starts_with("Internal:"));
        assert!(display.contains("包装"));
        assert!(!display.contains("secret"));
        assert!(!format!("{error:?}").contains("secret"));
    }

    #[test]
    fn with_source_preserves_kind_and_exposes_the_chain() {
        let source = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let error = ResiliencxError::transient("下游 I/O").with_source(source);
        assert_eq!(error.kind(), ErrorKind::Transient);
        assert!(error.is_retryable());
        let source = error.source().expect("source 应存在");
        assert!(source.to_string().contains("file missing"));
    }
}
