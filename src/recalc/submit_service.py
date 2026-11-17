"""提交服务：HTTP API + 网页（独立进程）。

提交动作一次性把复用判定的三要素固化到作业记录：
输入版（快照内容哈希）、处理规则哈希、字段权限哈希。之后任何阶段崩溃，
恢复都基于这份不可变记录，与任务名称无关。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from . import config, engine, jobs, keys, manifest, paths, registry, serviceutil
from .serviceutil import setup_logging

log = setup_logging("submit")


# ---------------------------------------------------------------- 提交
def submit_recompute(
    report_id: str,
    rule_override: dict | None = None,
    faults: dict | None = None,
) -> dict:
    report = config.get_report(report_id)
    rule = config.effective_rule(report, rule_override)
    permission = report["permission"]

    job_id = manifest.new_id("job")
    snapshot_id = manifest.new_id("snap")

    # 1) 固化输入版：复制原始数据，之后原文件变化也不影响本作业
    raw_path = _resolve_raw(report["raw_path"])
    snapshot_meta = engine.snapshot_input(raw_path, snapshot_id)
    registry.snapshot_add_ref(snapshot_id, job_id)

    # 2) 三段式复用键（输入版 + 规则 + 字段权限，绝不只比任务名）
    reuse = keys.compute_keys(snapshot_meta, rule, permission)

    job = {
        "job_id": job_id,
        "report_id": report_id,
        "report_name": report["name"],
        "state": "PENDING",
        "rule": rule,
        "rule_version": report.get("rule_version", 1),
        "permission": permission,
        "snapshot_id": snapshot_id,
        "snapshot_hash": snapshot_meta["content_hash"],
        "source_path": str(raw_path),
        "faults": faults or {},
        "created_at": manifest.utc_now_iso(),
        "updated_at": manifest.utc_now_iso(),
        **reuse,
    }
    jobs.create(job)

    # 3) 建立引用（仅占位，不构建）。取消时释放的就是这些引用
    for artifact_id, kind, key in (
        (job["clean_artifact_id"], "clean", job["clean_key"]),
        (job["partition_artifact_id"], "partition", job["partition_key"]),
    ):
        registry.acquire_or_create(
            artifact_id,
            kind=kind,
            key=key,
            job_id=job_id,
            boot_id=f"submitter_{serviceutil.boot_id()}",
            detail={"placeholder": True},
            hold_lease=False,
        )
    log.info(
        "提交重算 report=%s job=%s clean=%s partition=%s",
        report_id,
        job_id,
        job["clean_artifact_id"],
        job["partition_artifact_id"],
    )
    return job


def _resolve_raw(rel_or_abs: str):
    from pathlib import Path

    p = Path(rel_or_abs)
    if p.is_absolute():
        return p
    return Path("/workspace") / p


def cancel(job_id: str) -> dict:
    job = jobs.request_cancel(job_id, reason="用户在网页取消")
    if job is None:
        raise KeyError(job_id)
    log.info("取消作业 %s -> CANCELED", job_id)
    return job


# ---------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "RecalcSubmit/1.0"

    def log_message(self, fmt, *args):  # 安静点，详细日志走 logger
        log.debug("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, payload, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/":
                self._serve_html()
            elif path == "/api/state":
                self._send_json(self._state())
            elif path.startswith("/api/report/"):
                report_id = path.rsplit("/", 1)[-1]
                self._send_json({"report": config.get_report(report_id)})
            elif path.startswith("/api/output/"):
                job_id = path.rsplit("/", 1)[-1]
                self._serve_output(job_id)
            else:
                self._send_json({"error": "not found"}, 404)
        except KeyError as exc:
            self._send_json({"error": f"不存在: {exc}"}, 404)
        except Exception as exc:  # noqa: BLE001
            log.exception("GET %s 失败", path)
            self._send_json({"error": str(exc)}, 500)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            payload = self._read_json()
            if path == "/api/recompute":
                job = submit_recompute(
                    payload["report_id"],
                    rule_override=payload.get("rule_override"),
                    faults=payload.get("faults"),
                )
                self._send_json({"job": job}, 201)
            elif path == "/api/cancel":
                self._send_json({"job": cancel(payload["job_id"])})
            elif path == "/api/rules":
                report = config.update_report_rules(payload["report_id"], payload.get("rule_override", {}))
                self._send_json({"report": report})
            else:
                self._send_json({"error": "not found"}, 404)
        except KeyError as exc:
            self._send_json({"error": f"不存在: {exc}"}, 404)
        except (ValueError, TypeError) as exc:
            self._send_json({"error": f"请求参数错误: {exc}"}, 400)
        except Exception as exc:  # noqa: BLE001
            log.exception("POST %s 失败", path)
            self._send_json({"error": str(exc)}, 500)

    def _state(self) -> dict:
        artifacts = registry.list_all()
        return {
            "reports": config.list_reports(),
            "jobs": jobs.list_all(),
            "artifacts": artifacts,
            "active_artifact_refs": {
                a["artifact_id"]: [
                    r for r in a["refs"]
                    if (jobs.get(r) or {}).get("state") in jobs.ACTIVE_STATES
                ]
                for a in artifacts
            },
        }

    def _serve_output(self, job_id: str) -> None:
        job = jobs.get(job_id)
        if not job or job["state"] != "PUBLISHED":
            self._send_json({"error": "报表尚未发布"}, 404)
            return
        from pathlib import Path

        csv_path = Path(job["output"]["path"])
        body = csv_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{job["report_id"]}.csv"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_html(self) -> None:
        from . import webui

        body = webui.INDEX_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve(host: str = "127.0.0.1", port: int = 8637) -> None:
    serviceutil.install_signal_handlers()
    paths.ensure_dirs()
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    log.info("提交服务监听 http://%s:%s （boot_id=%s）", host, port, serviceutil.boot_id())
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.3}, daemon=True)
    thread.start()
    while not serviceutil.shutdown_requested():
        serviceutil.interruptible_sleep(0.5)
    httpd.shutdown()
    log.info("提交服务已退出")


def main() -> None:
    import os

    serve(port=int(os.environ.get("RECALC_PORT", "8637")))


if __name__ == "__main__":
    main()
