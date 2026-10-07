# latency-profiling-guardrails (Python / FastAPI)

在一个最基础的 FastAPI 服务上补齐三件事，并保持原有对外行为不变：

1. **端到端耗时观测**：按路由统计请求量、成功/失败数与滚动窗口耗时分位数；
2. **分段耗时**：把一次请求拆成自身处理 / 等待本地慢依赖 / 排队等待；
3. **过载保护 + 结果缓存**：昂贵接口的并发上限、可区分原因的拒绝，以及可开关、
   可失效、防击穿的结果缓存。

原有 `GET /` 的入参与返回（`{"Hello": "World"}`）保持不变。

---

## 1. 快速开始

```bash
uv sync --group dev        # 安装运行与测试依赖

# 启动服务（默认 127.0.0.1:8000，可用 PORT 覆盖）
uv run python -m app.main
# 或沿用旧入口
uv run python main.py
```

一键本地验证（启动服务 → 打各类请求 → 打印指标快照）：

```bash
PORT=8000 bash scripts/demo.sh
```

跑单测：

```bash
uv run pytest
```

测试输出会为每个用例打印 `PASSED/FAILED + 覆盖的业务点`，结尾附
"业务点覆盖汇总"。当前共 28 个用例，覆盖并发统计、过载拒绝、缓存命中/失效、
异常链路等。

---

## 2. 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 原有接口，行为不变 |
| GET | `/health` | 健康检查 |
| GET | `/expensive` | 昂贵接口（并发闸门 + 可选缓存 + 模拟慢依赖） |
| GET | `/metrics` | 当前滚动窗口指标快照（JSON） |
| POST | `/admin/cache/invalidate` | 显式失效缓存（`?key=` 指定 key，不传清空全部） |
| POST | `/admin/cache/enabled` | 缓存开关（`?enabled=true|false`） |

`/expensive` 参数：

- `delay`：模拟慢依赖耗时（秒，0~30）
- `timeout`：>0 时作为慢依赖超时阈值，`delay` 超过它返回 **504**
- `fail=true`：模拟依赖自身报错，返回 **500**
- `use_cache=false`：本次绕过缓存直接回源

返回体额外带 `cached: true|false` 标识本次是否命中缓存。

### 过载拒绝的两种可区分原因（HTTP 503）

```jsonc
// 在途（执行中+排队中）已达上限，连队都不让排：
{"error": "overloaded", "reason": "limit_reached", ...}
// 允许排队，但在 queue_timeout 内没等到执行位：
{"error": "overloaded", "reason": "queue_timeout", ...}
```

依赖超时是另一条链路：HTTP **504** `{"error":"dependency_timeout", ...}`。

---

## 3. 耗时与分位数口径

### 3.1 分段定义（单位：秒，基于 `time.monotonic`）

```
total      = 请求端到端总耗时（进入采集中间件 → 响应起始帧发出）
queue      = 排队等待耗时（等待并发闸门令牌）
dependency = 等待本地慢依赖的累计耗时（支持多次调用，逐段累加）
self       = 自身处理耗时 = total - queue - dependency
```

恒等式：**total = queue + dependency + self**（实现里对每段做了非负钳制）。
路由通过 `request.state.timing` 标记依赖等待区间；排队区间由并发闸门自动标记。

### 3.2 结果分类（outcome）

| outcome | 判定 | HTTP |
| --- | --- | --- |
| `success` | 2xx / 3xx | 2xx/3xx |
| `rejected` | 过载保护拒绝 | 503 |
| `timeout` | 本地慢依赖超时 | 504 |
| `error` | 其余失败（4xx/5xx、未处理异常、连接中断） | 4xx/500 等 |

**失败样本不会进入成功分位数**；异常不被吞掉——未处理异常仍打印完整堆栈并
返回 500，不会把本该失败的结果变成成功。

### 3.3 分位数

- 窗口：**60 秒 wall-clock 滚动窗口**（`RollingWindowMetrics(window_seconds=60)`），
  窗口外旧样本在每次写入/快照时惰性删除，不依赖后台线程，旧数据自然过期。
- 算法：最近序（线性插值，与 numpy 默认 `linear` 一致），默认输出
  **p50 / p95 / p99**。
- `/metrics` 中每路由给出三套分位数：
  - `total_success_seconds`：**仅成功请求**的端到端分位数（SLO 口径）；
  - `total_all_seconds`：全部请求（含失败/超时/拒绝）的端到端分位数；
  - `phase_seconds`：`total / self / dependency / queue` 四段各自的分位数
    （全样本，用于定位瓶颈是排队还是慢依赖）。
- 并发安全：每条路由一把锁，写入与快照都在锁内拷贝样本，
  并发请求统计不串、不丢（有 1000 样本多线程压测用例守护）。

`/metrics` 片段示例：

```json
{
  "route": "/expensive",
  "sample_count": 12,
  "counts": {"success": 5, "error": 1, "timeout": 1, "rejected": 5},
  "quantiles": {
    "total_success_seconds": {"p50": 0.400199, "p95": 0.400977, "p99": 0.40107},
    "phase_seconds": {
      "dependency": {"p50": 0.005, "p95": 0.4002, "p99": 0.4003},
      "queue":      {"p50": 0.00002, "p95": 0.2001, "p99": 0.2001}
    }
  }
}
```

---

## 4. 并发与过载策略

`ConcurrencyGuard(limit, queue_timeout, max_queue)`：

- `limit`（默认 3）：昂贵接口**同时执行**的请求上限，基于 `asyncio.Semaphore`。
- 信号量占满时，新请求进入排队，最多等 `queue_timeout`（默认 0.2s）；
  等到则正常执行，排队耗时计入 `queue` 段；等不到返回
  **503 `queue_timeout`**，不会无限堆积。
- 若"在途 = 执行中 + 排队中"已达 `limit + max_queue`（默认 3+20），直接返回
  **503 `limit_reached`**，连排队都拒绝，避免队列把内存/连接拖垮。
- 令牌在业务抛异常时也会正确释放（有对应用例）。

> 缓存命中的请求不进入并发闸门、不占执行位；只有回源请求受闸门约束。

---

## 5. 缓存策略

`SingleFlightCache(ttl_seconds=5.0)`：

- **TTL 自然过期**：读时严格比对墙钟时间，到期瞬间即视为失效，绝不返回旧值。
- **显式失效**：`POST /admin/cache/invalidate`（按 key 或清空全部）。
- **可开关**：`POST /admin/cache/enabled?enabled=false|true`；关闭后一律回源
  且不写缓存，重新打开后旧的成功缓存仍可见。
- **防击穿（single-flight）**：同一 key 的并发请求只执行一次回源，其余等待并
  共享同一个结果（20 并发用例验证 loader 只被调用 1 次）。
- **失败不缓存**：回源抛异常时该 key 不留下任何条目，后续请求正常重试；
  异常原样透传给所有并发等待者，不会变成成功结果。
- **结果隔离**：命中时返回缓存值的深拷贝，调用方篡改响应对象不污染缓存。
- 缓存 key 由业务参数构成（如 `expensive:delay=..:fail=..:timeout=..`），
  不同参数互不影响；**失败参数（fail/timeout）也参与 key**，因此失败不会
  顶替成功结果。

---

## 6. 代码结构

```
app/
  timing.py       # RequestTiming：分阶段计时（total/queue/dependency/self）
  metrics.py      # RollingWindowMetrics / Sample / RouteStats / 分位数
  concurrency.py  # ConcurrencyGuard + RejectedError / DependencyTimeoutError
  cache.py        # SingleFlightCache：TTL 缓存 + single-flight
  deps.py         # slow_local_call：模拟本地慢依赖（延迟/失败/超时）
  middleware.py   # ASGI 中间件：端到端计时、按路由与状态码记录指标
  routes.py       # /、/health、/expensive、/metrics、缓存管理接口
  main.py         # 组装 app + 异常→HTTP 映射（503/504/500）
tests/            # 28 个单测/集成测试
scripts/demo.sh   # 本地一键验证
```

## 7. 本地如何验证

1. `uv run pytest`：看每个用例的业务点与结果、结尾覆盖汇总；
2. `PORT=8000 bash scripts/demo.sh`：观察
   - 首次 `cached=false`、二次 `cached=true`；
   - 8 并发下 200 与 503（`queue_timeout`）混合出现；
   - `/metrics` 中 `/expensive` 的 `queue`/`dependency` 分位数随负载变化；
   - 504（超时）、500（依赖失败）互不混淆，且失败不计入成功分位数；
3. 想直接看到 `limit_reached`：把并发数提到 `limit + max_queue` 以上
   （默认 ≥24 个并发）即可。
