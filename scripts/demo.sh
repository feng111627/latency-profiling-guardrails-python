#!/usr/bin/env bash
# 本地验证脚本：启动服务后依次演示
#   1) 原有接口兼容性  2) 缓存命中  3) 过载拒绝（两种原因）
#   4) 超时与失败链路  5) 缓存失效/开关  6) 指标快照
# 用法: bash scripts/demo.sh        （可用 PORT=8123 覆盖端口）
set -u
PORT="${PORT:-8000}"
BASE="http://127.0.0.1:${PORT}"

echo "==> 启动服务于 ${BASE}"
PORT="$PORT" uv run python -m app.main >/tmp/demo_server.log 2>&1 &
SERVER_PID=$!
trap 'kill ${SERVER_PID} 2>/dev/null' EXIT

# 等待就绪
for _ in $(seq 1 100); do
  if curl -s "${BASE}/health" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

say() { echo; echo "----- $* -----"; }

say "1. 原有接口行为不变"
curl -s "${BASE}/" ; echo

say "2. 昂贵接口首次调用（回源，观察 cached=false）"
curl -s "${BASE}/expensive?delay=0.2"; echo
say "2b. 立即再次调用（缓存命中 cached=true）"
curl -s "${BASE}/expensive?delay=0.2"; echo

say "3. 8 并发打爆（limit=3），观察状态码与拒绝原因"
pids=()
for i in $(seq 1 8); do
  curl -s -w "  -> [%{http_code}]\n" \
    "${BASE}/expensive?delay=0.4&use_cache=false" &
  pids+=($!)
done
wait "${pids[@]}"

say "4. 慢依赖超时 -> 504"
curl -s -w "  -> [%{http_code}]\n" \
  "${BASE}/expensive?delay=0.3&timeout=0.05&use_cache=false"
say "4b. 慢依赖报错 -> 500（异常不被吞）"
curl -s -w "  -> [%{http_code}]\n" \
  "${BASE}/expensive?delay=0.01&fail=true&use_cache=false"

say "5. 缓存显式失效 / 关闭 / 重新打开"
curl -s -X POST "${BASE}/admin/cache/invalidate"; echo
curl -s -X POST "${BASE}/admin/cache/enabled?enabled=false"; echo
curl -s -X POST "${BASE}/admin/cache/enabled?enabled=true"; echo

say "6. 指标快照（按路由计数 + 分位数 + 分段耗时）"
curl -s "${BASE}/metrics" | python3 -m json.tool
