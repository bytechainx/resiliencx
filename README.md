# resiliencx

`resiliencx` 是一组**进程内弹性原语**：安全重试、重试预算、熔断、限流与舱壁。

- 零外部可靠性框架依赖：错误分类、退避、预算、熔断状态机全部为 crate 内独立实现
- **无墙钟**：熔断按拒绝计数推进、限流需显式 `refill`、调度时间由调用方传入，因此可确定性测试
- 可注入观测：通过共享契约 `instrumentationx::Instrumentation` 注入，不把 `tracing` 拖进依赖图
- 统一错误模型：`ErrorKind` 语义分类 + `ResiliencxError` / `ResiliencxResult`

## 安装

本 crate **不发布到 crates.io**；它与 `instrumentationx` 之间是 path 依赖，需把两个仓库
clone 到同级目录后以 path 引入：

```toml
[dependencies]
resiliencx = { path = "../resiliencx" }
instrumentationx = { path = "../instrumentationx" }
```

启用非阻塞 `tokio` 等待与整次 deadline：

```toml
resiliencx = { path = "../resiliencx", features = ["tokio"] }
```

## 能力矩阵

| 能力 | 生产入口 / 类型 | 边界 |
| --- | --- | --- |
| 安全重试 | `RetryContext` + `RetrySafety` + `retry_fn_safe` / `retry_async_safe` | 多次尝试前显式声明只读或幂等；不安全副作用被拒绝 |
| 安全 Adapter budget | `call_with_retry_budget_safe` / `call_with_retry_budget_async_safe` | generic 返回值；首次 operation 前校验 safety |
| 整次 deadline | `retry_async_with_deadline`（feature `tokio`） | 覆盖尝试与退避；cooperative cancellation，不撤销已发生副作用 |
| 重试预算 | `RetryBudget` + safe retry / Adapter 入口 | 每次真正 retry 消耗令牌；耗尽统一返回标准 budget 错误 |
| 退避 | `Backoff::{Constant, Exponential}` | `Wait` / `AsyncWait` 可注入 |
| jitter | `retry_delay_ms_with_seed` / `apply_seeded_jitter` | seed 由调用方注入；非加密 RNG |
| 熔断 | `CircuitBreaker` 三态 | 本地、无墙钟；Open 按拒绝次数推进 |
| 限流 | `RateLimiter` 令牌桶 | 本地、无墙钟；仅显式 `refill` |
| 舱壁 | `Bulkhead` / `BulkheadPermit` | 本地并发上限；满载立即拒绝，无排队/等待 |
| 错误分类 | `ErrorKind` / `ResiliencxError` | 语义分类决定「值不值得重试」 |

## 生产安全重试

```rust
use resiliencx::{
    NoWait, NoopInstrumentation, RetryConfig, RetryContext, RetrySafety, ResiliencxResult,
    retry_downcast, retry_fn_safe, retry_ok,
};

fn main() -> ResiliencxResult<()> {
    let config = RetryConfig::fixed(3, 0);
    let mut operation = || Ok(retry_ok(42u8));
    let value = retry_fn_safe(
        RetryContext::new(&config, RetrySafety::ReadOnly, &NoopInstrumentation, "read-profile")
            .with_jitter_seed(7),
        &NoWait,
        &mut operation,
    )?;
    assert_eq!(retry_downcast::<u8>(value)?, 42);
    Ok(())
}
```

`RetryContext` 聚合 config、safety、instrumentation、op、可选 budget 与 caller seed。
`RetrySafety` 是**调用方声明**，不是对闭包的静态证明。`max_attempts > 1` 时，
`UnsafeSideEffect` 会在首次调用前返回 `Invalid`；`max_attempts == 1` 仍允许单次执行。

`call_with_retry_budget`、`call_with_retry_budget_async`、`retry_fn`、`retry_fn_with_wait`、
`retry_fn_with_budget`、`retry_fn_with_wait_budget`、`retry_async`、`retry_async_with_budget` 为
**unchecked compatibility** 入口，不会替调用方校验副作用安全性。新生产接线必须使用对应的
`*_safe` 入口。

## 错误分类

重试原语只依据 `ResiliencxError::is_retryable()` 决策，即**仅** `ErrorKind::Transient` 会被重试；
其余分类立即返回原错误。调用方需要把自己的错误映射为 `ResiliencxError`：

| `ErrorKind` | 反应 |
| --- | --- |
| `Invalid` / `Missing` / `Conflict` | 不自动重试；修正输入或等待状态变化 |
| `Transient` | 可使用退避与抖动重试；`retry_after()` 仅为提示 |
| `Unavailable` | 默认传播；由组合根决定降级或 fail-fast |
| `Cancelled` | 不自动重试；视为正常终止路径 |
| `DeadlineExceeded` | 本次终止；是否重试由上层策略裁定 |
| `Invariant` | 视为 bug；不重试（`is_bug()` 为 `true`） |
| `Internal` | 不自动重试；应逐步收敛到更精确的分类 |

## async deadline 与取消

feature `tokio` 下，`retry_async_with_deadline` 用 `tokio::time::timeout` 包裹整次安全重试，
包括每次 operation future 与退避等待。超时统一映射为 `ResiliencxError::deadline_exceeded`。

取消是 **cooperative cancellation**：超时时待执行 future 停止被轮询，但已经完成的网络写入、
数据库提交或其他外部副作用**不会**被自动撤销；operation 自行派生的后台任务也可能继续运行。
因此 deadline 不能替代幂等键、事务或补偿机制。

async budget 在退避前原子 reserve；deadline 在退避期取消时，未 commit 的预留由 RAII 自动 refund，
且不会产生 `record_retry`。预算初始耗尽时在进入 wait 前立即返回标准 budget 错误。

## jitter 去相关

兼容入口 `retry_delay_ms` / `apply_deterministic_jitter` 只依赖 attempt；相同配置的实例会得到
同一序列，**不具备抗群聚保证**。需要实例去相关时，调用方使用
`RetryContext::with_jitter_seed` 把不同 seed 接入 safe sync / async / deadline 的实际退避；
纯计算场景可使用 `retry_delay_ms_with_seed` / `apply_seeded_jitter`。

## 本地立即拒绝边界

熔断、限流、舱壁均是**单进程内**原语，不做跨进程协调。`RateLimiter::try_acquire` 与
`Bulkhead::try_enter` 在资源不足时立即返回 `Unavailable`，没有公平队列、排队 deadline 或自动 refill。
这些行为不能被描述为分布式限流、排队舱壁或按时间冷却熔断。

## 非目标

不宣称 package stable、分布式弹性平台、自动墙钟策略，也不会撤销已经发生的外部副作用。

## 门禁

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
# instrumentationx 不发布到 crates.io，该覆盖是长期约定；打包时始终显式指向同级的本地 checkout。
cargo package --no-verify --offline \
  --config 'patch.crates-io.instrumentationx.path="../instrumentationx"'
```

## 许可

MIT OR Apache-2.0
