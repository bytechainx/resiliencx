# Changelog — resiliencx

本文件记录 `resiliencx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/infra/resiliencx` 抽取而来（抽取时点为 `0.1.5`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

## [0.1.1] - 2026-09-22

### 新增

- 特性 002：`tests/tdd_contracts.rs`（覆盖公开接口契约全部 12 个入口的行为契约 +
  TDD-PROBE 变异探测表）、`tests/sdd_spec.rs`（`docs/标准.md` 全部 7 个章节条款的
  可执行断言）、`tests/aidd_boundary.rs`（8 条经复核的 AI 生成对抗 / 边界用例）。

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，把
  `src/retry.rs` 的两块职责下沉为子模块 —— 退避策略与等待抽象（`Backoff`、`Wait` / `AsyncWait`
  两个 trait 与四个实现）→ `src/retry/wait.rs`；退避延迟与抖动的纯函数（`retry_delay_ms[_with_seed]`、
  `apply_deterministic_jitter` / `apply_seeded_jitter`）→ `src/retry/jitter.rs`。门面 `src/retry.rs`
  保留模块文档、`RetryConfig` / `RetrySafety` / `RetryContext` / `RetryValue`、`retry_fn*` 与
  `retry_async*` 全家族、`retry_ok` / `retry_downcast`，以及**原有内联测试**。
  搬走的公开项经门面 `pub use` 转出（含 `#[cfg(feature = "tokio")]` 门控的 `TokioSleepWait`），
  故 `resiliencx::retry::{…}` 与 crate 根 `pub use retry::{…}` 的公开路径**一字未改**。
  `src/retry.rs` 生产段由 **649 → 445** 行。
  动机：`module-rules` 是元仓库必需检查，且它审计各仓**默认分支**，故当 `retry.rs` 生产段距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 151 行时，任一仓的任意改动都可能卡住元仓库的全部 PR。
  属**纯搬移**（行多重集比对确认零代码行丢失），全部测试与 doctest 结果不变
  （默认 feature 115 项 / `--all-features` 122 项）。

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
