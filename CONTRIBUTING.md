# CONTRIBUTING.md — 贡献指南（resiliencx）

本文件面向贡献者，汇总本地门禁与提交约定。
AI Agent 的工作约定另见 [`AGENTS.md`](./AGENTS.md)；术语与领域语言见 [`CONTEXT.md`](./CONTEXT.md)。

## 开发流程

- 本仓库是**独立的单 crate 仓库**，不依赖 `xhyper.rs` 主工程及其内部 crate（`kernel` / `contracts` 等）。
  唯一的 crate 间依赖是共享契约 `instrumentationx`，声明为 path 依赖
  （`instrumentationx = { version = "0.1.0", path = "../instrumentationx" }`）；
  本地开发需把 `instrumentationx` 与 `resiliencx` clone 到同级目录。
- substantial 变更走 feature branch → PR → review → merge，**禁止直接 push `main`**。
- `main` 已启用分支保护：要求 PR + 必需检查 `fmt / clippy / test`，
  `required_approving_review_count = 0`（单人也能合并），禁止强推与删除。
- 合并方式固定为 **create a merge commit**。注意仓库设置是
  `merge_commit_title = MERGE_MESSAGE` + `merge_commit_message = PR_TITLE`，因此
  `gh pr merge` 必须显式传 `--subject` 与 `--body`，否则会产出通用
  `Merge pull request #N from …` 标题。
- 提交信息遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `ci:` / `chore:` / `refactor:`），
  描述用简体中文。

## 本地门禁（P0 三件套）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

本 crate 有可选 feature `tokio`（启用 `TokioSleepWait` 与 `retry_async_with_deadline`），
因此 clippy 与 test 必须带 `--all-features`，否则该 feature 的代码路径不会被编译与检查。

元数据完整性门禁（**不发布 crates.io**，此命令只校验打包元数据）：

```bash
cargo package --no-verify --allow-dirty --offline \
  --config 'patch.crates-io.instrumentationx.path="../instrumentationx"'
```

`instrumentationx` 不发布到 crates.io，`cargo package` 的默认依赖解析会失败，
必须用 `--offline` 加 `patch.crates-io` 指向同级 checkout。该覆盖是**长期约定**，
不随版本演进移除。

## 复用口径（不发布 crates.io）

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
- 文档与元数据中不得出现「可独立发布」「可直接 `cargo publish`」等表述，
  也不得放置 crates.io / docs.rs 徽章与外链。
- `Cargo.toml` 的 `documentation` 指向 `https://github.com/bytechainx/resiliencx#readme`。
- 消费方引入方式（README「安装」小节为准）：

  ```toml
  [dependencies]
  resiliencx = { git = "https://github.com/bytechainx/resiliencx" }
  ```

## 开发约定

- 注释、文档、错误消息使用**简体中文**；标识符保持英文。
- MSRV 为 Rust 1.77、edition 2021；新增依赖或 API 不得抬高该下界而不说明。
- 错误模型：crate 内独立的 `ResiliencxError` + `#[non_exhaustive]` 的 `ErrorKind` +
  `ResiliencxResult` 别名，不依赖任何外部错误框架。
- 不在库代码里裸 `unwrap()`（`[lints.clippy]` 已 `deny` `unwrap_used` / `expect_used` / `panic`）。
- 所有 `pub` 项必须有中文 `///` 文档（`missing_docs` 已 `deny`）。
- 集成测试**必须离线运行**，不触碰真实网络。
- 保持**无墙钟**设计：时间推进由调用方显式传入（`Wait` / `refill` / 拒绝计数），
  不得引入 `SystemTime::now` / `Instant::now` 作为正确性条件。
- 重试入口须区分 `_safe` 与 unchecked 兼容面：不得静默改变既有入口的语义，
  也不得把兼容面写成生产安全路径。
- async 任务内不得调用默认阻塞的 `retry_fn` / `ThreadSleepWait`，应使用 `retry_async` + `AsyncWait`。
- 可观测性统一经 `instrumentationx::Instrumentation` 注入；**禁止**直接依赖 `observex`。

## 提交前自检清单

- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` 通过
- [ ] `cargo test --all-targets --all-features` 通过
- [ ] `cargo package --no-verify --allow-dirty --offline --config 'patch.crates-io.instrumentationx.path="../instrumentationx"'` 通过
- [ ] 新增 `pub` 项都有中文 `///` 文档
- [ ] 文档中无「可独立发布」/ crates.io / docs.rs 表述
- [ ] 新增代码未引入墙钟依赖，`RetrySafety` 语义未被静默改变
