# latency-profiling-guardrails

在一个最小 FastAPI 服务上补齐三块能力，且**不改变原有对外行为**（`GET /` 依旧返回 `{"Hello": "World"}`）：

1. **端到端耗时观测**：按路由采集请求量、成功/失败数、滑动窗口内耗时分位数；
2. **耗时分段与失败分类**：把一次请求拆成排队、慢依赖、自身处理三段；超时/报错/过载明确区分，失败样本不混入成功统计，异常不留痕式吞掉；
3. **过载保护 + 可开关结果缓存**：对开销大的接口做并发上限与过载拒绝（拒绝原因可区分），结果缓存支持 TTL 与显式失效，并发不击穿、不返回过期/错误结果。

## 目录结构

```
app/
  main.py                 # 应用工厂，组装各组件（参数可调，默认值偏小便于本地压测）
  metrics.py              # 滑动窗口统计：分位数、计数、线程安全、窗口过期
  timing.py               # TimingRecorder：queue / dependency / self 分段计时
  overload.py             # ConcurrencyGuard：并发上限、有界队列、排队超时（async）
  cache.py                # ResultCache：TTL、single-flight、世代失效、可开关
  dependencies.py         # 本地慢依赖模拟（延迟/失败/超时预算，纯进程内）
  metrics_middleware.py   # ASGI 中间件：每请求计时、结果分类、异常留痕转 JSON
  routes.py               # /expensive、/metrics、/health 与 /admin/cache*
main.py                   # uvicorn 入口：app = create_app()
demo_local.py             # 本地一键真实 HTTP 验证脚本
tests/                    # 43 个单测 + 业务点覆盖报告插件 (tests/report.py)
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 原服务，行为不变 |
| GET | `/health` | 存活探针 |
| GET | `/expensive` | 受并发保护、可缓存的开销接口。参数：`q`(缓存键)、`delay`(自身处理秒)、`dep_timeout`(慢依赖预算秒)；请求头 `x-cache-control: no-cache` 强制重算；响应头 `x-cache: HIT/MISS/SHARED/BYPASS` |
| GET | `/metrics` | 各路由滑动窗口统计 |
| GET | `/admin/cache` | 缓存状态（条目数、世代、hit/miss/bypass） |
| POST | `/admin/cache/enabled` | body `{"enabled": true|false}` 开关缓存 |
| DELETE | `/admin/cache/{key}` | 失效单个键（键形如 `expensive:<q>`） |
| DELETE | `/admin/cache?all=true` | 失效全部条目 |

启动服务：

```bash
uv sync                       # 安装依赖（含 dev）
uv run uvicorn main:app --port 8000
# 或 uv run python main.py（端口由 uvicorn 分配）
```

## 一、耗时与分位数口径

### 分段定义（单位毫秒，单调时钟 `time.perf_counter`）

- **total_ms**：端到端总耗时，中间件从请求进入到响应/异常结束；
- **queue_ms**：在并发保护中**等待槽位**的时间（仅 `/expensive` 有排队语义；缓存 HIT 在入队前直接返回，不计排队）；
- **dependency_ms**：阻塞在**本地慢依赖**上的时间，多次调用累加；
- **self_ms**：自身处理时间，`total − queue − dependency`（不为负）。

三段满足 `total ≈ queue + dependency + self`。

### 窗口与分位数

- 统计基于**滑动时间窗口**（默认 60 秒，`create_app(window_seconds=...)` 可调）。样本按墙钟时间戳保存在每路由 deque 中，**读取和写入都会惰性驱逐**早于窗口左边界的样本——窗口滚动后旧数据自然过期，不需要定时清理。
- 分位数为 **p50 / p90 / p95 / p99**，采用**近秩（nearest-rank）**口径：`rank = ceil(q·N)`，对成功样本的 total_ms 升序取值；无样本返回 `null`。
- `/metrics` 中：
  - `success_latency_ms`：**只统计成功样本**的分位数；
  - `breakdown_ms`：成功样本各分段的均值；
  - `total_latency_by_outcome_ms`：按结果分别给出的 total 分位数（排查用，失败延迟也可观测，但绝不污染成功口径）；
  - `counts`：`total/success/error/timeout/rejected`；`reject_reasons`：拒绝原因分桶。

### 结果分类（一个请求只会归一类）

| 结果 | HTTP | 含义 |
| --- | --- | --- |
| `success` | 2xx/4xx（非 5xx） | 正常处理完成 |
| `rejected` | 503 | 过载保护在处理前拒绝，带 `x-overload-reason` 头 |
| `timeout` | 504 | 慢依赖超过预算（`DependencyTimeoutError`） |
| `error` | 其他 5xx | 未预期异常 |

**关键保证：** 超时/错误/拒绝样本只计入对应桶，绝不进入成功分位数；异常在中间件被捕获时用 `logger.exception` 输出**完整堆栈**后再转成 JSON 错误响应，因此既不破坏对外 JSON 一致性，也不会把失败伪装成成功。

### 并发正确性

- 统计存储用一把锁保护所有路由的样本 deque，asyncio 任务与线程池端点并发写入都不会丢样本、不会串路由（`tests/test_metrics.py` 用 16 线程 × 500 样本与 8 路由并发验证）。
- `TimingRecorder` 每请求一个实例，经 `request.state.timing` 与 contextvar 传递，天然不共享；依赖段重叠嵌套会直接报错，防止计时串段。

## 二、并发与过载策略

`ConcurrencyGuard`（`app/overload.py`，async 上下文管理器）是**快速失败 + 有界可选队列**：

1. 在飞数 `< max_inflight`：立即放行；
2. 否则若未配置队列（`max_queue=0`）：立即 503，原因 **`concurrency_limit_reached`**；
3. 否则若等待数已达 `max_queue`：立即 503，原因 **`queue_full`**（不无限排队，不拖垮服务）；
4. 否则入队等待，`queue_timeout` 秒内拿不到槽位：503，原因 **`queue_timeout`**；等待期间被取消（如客户端断开）则异常原样传播、队列干净回收。

默认配置很小（`max_inflight=2, max_queue=1, queue_timeout=0.05s`），本地几个并发请求就能观测到拒绝。拒绝请求不进入处理函数，只作为 `rejected` 样本落库。

## 三、缓存策略

`ResultCache`（`app/cache.py`）默认**关闭**，通过管理接口或 `create_app(cache_enabled=True)` 开启。

- **失效方式（明确、可预期）**：
  - TTL 到期（单调时钟）；过期条目一律视为不存在，**绝不返回过期值**；
  - 显式失效：`DELETE /admin/cache/{key}`、`?all=true`；
  - 关闭缓存即清空。
- **防击穿（single-flight）**：同一个键并发 miss 时，只有一个请求执行 producer，其余请求 `await` 同一个结果，响应头标记 `SHARED`；慢依赖因此只被调用一次。
- **缓存 HIT 不占并发槽位**：命中在进入并发保护之前返回，所以即便后端满载，缓存命中仍可服务。
- **错误不缓存**：producer 抛异常时所有等待者收到同一异常，不写入任何条目，下次请求会重新尝试，不会把一次失败记成"成功结果"。
- **世代（generation）防复活**：失效/关闭会递增世代号；在途 producer 若在失效后才完成，其结果属于旧世代，只交给当时的等待者、**不落库**，避免"刚失效又被旧计算填回"。

## 本地验证

### 1) 单元测试（推荐入口）

```bash
uv sync --group dev
uv run pytest
```

日志会逐用例打印业务点与结果，并在结尾输出「业务点覆盖清单」，例如：

```
[coverage] PASSED   test_queue_timeout_reason :: 排队等待超时：拿不到槽位时 queue_timeout 拒绝…
...
========================= 业务点覆盖清单 (business coverage) =========================
 1. [PASSED ] 并发安全：多线程并发写入不丢样本、计数精确  (tests/test_metrics.py::...)
...
```

覆盖重点（43 个用例）：

- `tests/test_metrics.py`：分位数口径、按路由隔离、**并发不丢不串**、窗口过期、失败样本不污染成功分位数、拒绝原因计数；
- `tests/test_overload.py`：三种拒绝原因、队列放行、在飞数不超上限、排队中取消干净回收、HTTP 503；
- `tests/test_cache.py`：HIT/MISS、TTL 过期、显式失效、**single-flight 防击穿**、producer 异常传播且不缓存、开关、在途失效防复活；
- `tests/test_timing.py`：三段拆分求和、依赖段累加、重叠拒绝；
- `tests/test_api.py`：原行为不变、端到端 /metrics、504/500 异常链路与堆栈日志、排队耗时可见、缓存 HIT/失效/开关/no-cache、过载后恢复、缓存命中绕过过载。

### 2) 真实 HTTP 一键演示

```bash
uv run python demo_local.py
```

脚本会在 `127.0.0.1:8765` 启动真实 uvicorn，顺序演示：原接口 → 20 次正常请求 → 一次依赖超时(504) → 8 并发冲击（可看到 `queue_full`/`queue_timeout` 两种 503 原因）→ 缓存 MISS/HIT/失效后 MISS → 打印 `/metrics` 汇总。

### 3) 手动 curl

```bash
uv run uvicorn main:app --port 8000 &
curl -s localhost:8000/metrics | python -m json.tool
# 制造超时
curl -s 'localhost:8000/expensive?dep_timeout=0.001'
# 开缓存、命中、失效
curl -s -X POST localhost:8000/admin/cache/enabled -d '{"enabled":true}' -H 'content-type: application/json'
curl -si 'localhost:8000/expensive?q=a' | grep -i x-cache
curl -s -X DELETE localhost:8000/admin/cache/expensive:a
```

## 设计取舍备注

- 慢依赖是**进程内模拟**（`SlowDependency`），延迟/失败/超时全部参数化，保证无需外部服务即可确定性验证；接真实依赖时把 `routes.produce()` 中的调用替换即可，计时/分类/保护逻辑不变。
- 统计保留原始样本以支持任意分位数，窗口内每路由样本量与请求量同阶；如需超大规模可把 deque 换成分桶直方图，接口（`record/snapshot`）不用变。
