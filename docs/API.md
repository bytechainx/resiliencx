# resiliencx 公开 API

本文对应 `resiliencx 0.1.0` 的进程内公开消费面。

## 公开消费面

| 能力 | API |
|------|-----|
| 生产安全同步重试 | `RetryContext` / `RetrySafety` / `retry_fn_safe` |
| 生产安全异步重试 | `RetryContext` / `retry_async_safe` / `AsyncWait` |
| 生产安全 Adapter budget | `call_with_retry_budget_safe` / `call_with_retry_budget_async_safe` |
| 整次 deadline | `retry_async_with_deadline`（feature `tokio`） |
| 重试预算 | `RetryBudget` / `budget_exhausted_error` |
| Unchecked 兼容面 | `call_with_retry_budget` / `call_with_retry_budget_async` / `retry_fn` / `retry_fn_with_wait` / `retry_fn_with_budget` / `retry_fn_with_wait_budget` / `retry_async` / `retry_async_with_budget` |
| 退避与 jitter | `RetryConfig` / `Backoff` / `retry_delay_ms_with_seed` / `apply_seeded_jitter` |
| 等待 | `Wait` / `AsyncWait` / `NoWait` / `RecordingWait` / `ThreadSleepWait` / `TokioSleepWait` |
| 熔断 | `CircuitBreaker` / `CircuitConfig` / `CircuitState` |
| 限流 | `RateLimiter` / `RateLimitConfig` |
| 舱壁 | `Bulkhead` / `BulkheadConfig` / `BulkheadPermit` |
| 观测 | `NoopInstrumentation` / re-export `Instrumentation` |

## 选择规则

1. 新的装箱生产接线使用 `retry_fn_safe` / `retry_async_safe`；generic Adapter 使用
   `call_with_retry_budget_safe` / `call_with_retry_budget_async_safe`。
2. `max_attempts > 1` 时只能声明 `ReadOnly` 或 `Idempotent`；`UnsafeSideEffect` 会在首次调用前拒绝。
3. `RetrySafety` 由调用方保证，不能替代幂等键、事务或领域级重复检测。
4. 使用 `RetryContext::with_budget` 与 `with_jitter_seed` 组合可选策略；caller seed 进入实际退避。
5. feature `tokio` 下需要总 deadline 时使用 `retry_async_with_deadline`；超时只取消 future 的继续轮询，
   不保证撤销已经发生的副作用。
6. 上表完整列出的 unchecked compatibility 入口不执行副作用安全校验；只能用于已在更高层完成
   safety 验证的兼容组合，不能称为安全重试。
7. budget 在每次真正 retry 前消耗；耗尽统一返回 `budget_exhausted_error()`。
8. attempt-only jitter 不抗群聚；多实例生产接线使用调用方 seed 入口。

async budget 使用 reservation：退避前 reserve，完成后 commit；future 在退避中被 drop 时 RAII refund。
无 wait 的 generic Adapter async 入口在 retry operation future 已构造/轮询后取消，视为 attempt 已发起，
已消费预算不退还。

## 消费方 Safety 分类建议

`RetrySafety` 由调用方声明，`resiliencx` 无法从闭包内容静态证明副作用语义。常见分类参考：

- **键值 / 缓存类客户端**：`GET` / `EXISTS` / `PTTL` / `MGET` = `ReadOnly`；
  无 TTL 的 `SET` / `MSET` = `Idempotent`；`DEL` / `PEXPIRE` / 相对 TTL `SET` = `UnsafeSideEffect`。
- **关系型数据库**：连接池本身不做 budget 自动接线。safe wrapper 要求调用方显式声明 safety；
  任意 SQL 默认**不能**证明只读或幂等，应保守使用 `UnsafeSideEffect`，多次尝试会因此在首次 future
  构造前被拒绝。
- 历史上带 `with_budget*` / `with_retry*` 的旧 wrapper 属于 unchecked compatibility，
  不要把它们当作安全入口。

## 本地原语边界

`CircuitBreaker`、`RateLimiter`、`Bulkhead` 都只维护本进程状态。
`try_acquire` / `try_enter` 是立即成功或立即 `Unavailable` 的接口，不提供排队、公平性、
跨进程配额、墙钟 refill 或自动超时。
