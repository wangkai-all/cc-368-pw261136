#!/usr/bin/env bash
# 启动三个独立进程：提交服务(8637) / 共享计算进程 / 报表发布端
set -euo pipefail

export RECALC_HOME="${RECALC_HOME:-/workspace/.recalc}"
export RECALC_CONFIG="${RECALC_CONFIG:-/workspace/config/reports.json}"
export RECALC_PORT="${RECALC_PORT:-8637}"
export PYTHONPATH="/workspace/src${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONUNBUFFERED=1

mkdir -p "$RECALC_HOME/logs"
PIDDIR="$RECALC_HOME/pids"
mkdir -p "$PIDDIR"

start_one() {
  local name="$1"
  local pidfile="$PIDDIR/$name.pid"
  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "$name 已在运行 (pid $(cat "$pidfile"))"; return
  fi
  nohup python3 -I "/workspace/src/run_${name}.py" \
    >"$RECALC_HOME/logs/$name.out.log" 2>&1 &
  echo "$!" > "$pidfile"
  echo "启动 $name (pid $!)"
}

start_one submit_service
start_one worker
start_one publisher

sleep 1
echo "--- 进程状态 ---"
for f in "$PIDDIR"/*.pid; do
  pid="$(cat "$f")"; name="$(basename "$f" .pid)"
  if kill -0 "$pid" 2>/dev/null; then echo "OK  $name pid=$pid"; else echo "DEAD $name"; fi
done
echo "控制台: http://127.0.0.1:$RECALC_PORT/"
