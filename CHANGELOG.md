# Changelog — resiliencx

本文件记录 `resiliencx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/infra/resiliencx` 抽取而来（抽取时点为 `0.1.5`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

## [0.1.0] - 2026-09-21

### 新增

- 从 `xhyper.rs` 抽取为独立 crate，移除对内部 crate `kernel` 与 `contracts` 的依赖。
- 新增 crate 内错误模型 `src/error.rs`：`ErrorKind`（9 个语义分类）、`ResiliencxError`
  （10 个构造器 + `kind` / `context` / `retry_after` / `is_retryable` / `is_bug` / `with_source`）
  与别名 `ResiliencxResult`；`Display` 形如 `Transient: <上下文>`，`Debug` 不展开 source。
- 观测注入改用共享契约 `instrumentationx::Instrumentation`；不再直接再导出 `contracts`。
- 重试：`RetryConfig` / `Backoff` / `Wait` / `AsyncWait` / `NoWait` / `ThreadSleepWait` /
  `RecordingWait` / `TokioSleepWait`、确定性 jitter 与 caller seed 去相关。
- 安全入口：`RetryContext` + `RetrySafety::{ReadOnly, Idempotent, UnsafeSideEffect}` +
  `retry_fn_safe` / `retry_async_safe` / `call_with_retry_budget_safe` /
  `call_with_retry_budget_async_safe`。
- 重试预算：`RetryBudget` / `budget_exhausted_error` / `ensure_budget`；async 路径使用
  reservation + RAII refund。
- 整次 deadline：`retry_async_with_deadline`（feature `tokio`），cooperative cancellation。
- 熔断 `CircuitBreaker` / `CircuitState`、限流 `RateLimiter` / `RateLimitConfig`、
  舱壁 `Bulkhead` / `BulkheadPermit`：全部为本地、无墙钟原语。
- `NoopInstrumentation`：空操作观测实现，便于测试与默认接线。

### 变更

- `retry_fn_with_wait_budget_inner` 中的 let-chain 改写为等价的嵌套 `if let`，
  使代码可在 edition 2021 下编译。

### 说明

不宣称 package stable、分布式协调、自动墙钟补充/冷却，也不撤销已发生的外部副作用。
