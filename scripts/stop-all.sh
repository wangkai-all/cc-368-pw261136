#!/usr/bin/env bash
# 优雅停止三个进程（SIGTERM，worker/publisher 会完成在途阶段的收口与 GC）
set -euo pipefail
export RECALC_HOME="${RECALC_HOME:-/workspace/.recalc}"
PIDDIR="$RECALC_HOME/pids"

for name in publisher worker submit_service; do
  pidfile="$PIDDIR/$name.pid"
  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    pid="$(cat "$pidfile")"
    kill -TERM "$pid"
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.2
    done
    if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid"; fi
    echo "已停止 $name (pid $pid)"
  else
    echo "$name 未在运行"
  fi
  rm -f "$pidfile"
done
