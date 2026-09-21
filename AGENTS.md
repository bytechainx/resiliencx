# resiliencx Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

弹性原语：安全重试 + 重试预算 + 熔断 + 限流 + 舱壁，无墙钟依赖、可注入观测。

## 技术栈

- Rust edition 2021, rust-version 1.77
- 关键依赖: `instrumentationx`（观测契约）、`async-trait`、`tracing`；可选 `tokio`（feature `tokio`，仅 `time`）
- `instrumentationx` 为 path 依赖（`../instrumentationx`），是不发布到 crates.io 的共享契约 crate
- 可观测性通过 `instrumentationx::Instrumentation` 注入；**禁止**直接依赖 `observex`，以免把其实现细节拖进本 crate 依赖图
- 零业务耦合，不依赖 kernel/contracts 等私有 crate
- crate 级 lint：`unwrap_used` / `expect_used` / `panic` / `unreachable` / `todo` / `unimplemented` 全部 `deny`（测试代码经 `cfg_attr(test)` 豁免）

## 代码结构

```text
src/
├── lib.rs        # 模块声明 + 受控 re-export（RetrySafety / RetryContext / RetryConfig 等）
├── retry.rs      # 安全重试：retry_fn_safe / retry_async_safe / Backoff / Wait（NoWait / ThreadSleepWait / RecordingWait）
├── budget.rs     # 令牌式重试预算：RetryBudget / call_with_retry_budget[_async][_safe]
├── circuit.rs    # 三态熔断：CircuitBreaker（无墙钟，拒绝计数推进 HalfOpen）
├── rate_limit.rs # 令牌桶限流：RateLimiter（无墙钟，显式 refill）
├── bulkhead.rs   # 舱壁：Bulkhead / BulkheadPermit（RAII 许可，满载立即拒绝）
└── error.rs      # ErrorKind / ResiliencxError / ResiliencxResult
```

- feature `tokio`：启用 `TokioSleepWait` 与 `retry_async_with_deadline`（cooperative cancellation）
- `#![forbid(unsafe_code)]`、`#![deny(missing_docs)]`、`#![deny(unreachable_pub)]`

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误：thiserror 风格枚举 + `#[non_exhaustive]` + Result 别名；错误分类（`ErrorKind`）决定「值不值得重试」
- 禁止裸 `unwrap()`（库代码；lint 已 deny）
- 保持**无墙钟**设计：时间推进由调用方显式传入（Wait / refill / tick），不得引入 `SystemTime::now` / `Instant::now` 作为正确性条件
- 重试入口须区分 safe（校验 `RetrySafety`）与 unchecked 兼容面，不得静默改变语义

## 门禁三件套（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
# feature 覆盖：
cargo test --all-targets --features tokio
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`
