"""报表发布端（publisher，独立进程）。

计算进程把作业推进到 PUBLISHING 后，由本进程轮询认领并执行原子发布。
发布前崩溃（含测试用的硬退出）只可能留下 staging 临时目录，作业仍是
PUBLISHING，重启后重新发布——发布按作业 ID 覆盖，天然幂等。发布确认
后才释放中间产物引用，并保留快照引用支撑血缘回溯。
"""
from __future__ import annotations

import os
import shutil
import time

from . import engine, jobs, paths, registry, serviceutil
from .serviceutil import setup_logging

POLL_INTERVAL = 0.4
log = setup_logging("publisher")


def run() -> None:
    serviceutil.install_signal_handlers()
    paths.ensure_dirs()
    log.info("publisher 启动 boot_id=%s", serviceutil.boot_id())
    try:
        _main_loop()
    finally:
        log.info("publisher 已退出")


def _main_loop() -> None:
    while not serviceutil.shutdown_requested():
        for job in jobs.list_all():
            if serviceutil.shutdown_requested():
                break
            if job["state"] != "PUBLISHING":
                continue
            _publish_one(job)
        serviceutil.interruptible_sleep(POLL_INTERVAL)


def _publish_one(job: dict) -> None:
    job_id = job["job_id"]
    try:
        # 清理上一次崩溃可能残留的 staging 目录
        staging = paths.published_dir() / job["report_id"] / f".staging-{job_id}"
        if staging.exists():
            shutil.rmtree(staging)

        latest = jobs.get(job_id)
        if latest is None or latest["state"] != "PUBLISHING":
            return

        faults = latest.get("faults") or {}
        if faults.get("crash_before_publish"):
            def consume(j: dict) -> None:
                j.setdefault("faults", {})["crash_before_publish"] = False
            jobs.update(job_id, consume)
            jobs.append_event(job_id, "crash_injected", {"point": "before_publish_rename"})
            log.warning("故障注入：作业 %s 在发布确认前硬退出进程", job_id)
            # 不做任何清理：模拟真实崩溃，验证恢复路径
            os._exit(42)

        partition_dir = paths.artifact_path(latest["partition_artifact_id"])
        if not (partition_dir / "partition.csv").exists():
            # 计算进程在发布前整体崩溃的极端情况：等恢复后的计算重建
            log.info("作业 %s 的分区产物尚不可用，等待计算进程恢复", job_id)
            return

        result = engine.publish(latest, partition_dir)
        jobs.append_event(job_id, "published", result)

        def mark_published(j: dict) -> None:
            if j["state"] == "PUBLISHING":
                j["state"] = "PUBLISHED"
                j["published_at"] = serviceutil.stamp()
                j["output"] = result

        jobs.update(job_id, mark_published)
        # 发布已确认：释放中间分区/清洗引用，快照保留用于追溯
        jobs.release_on_published(job_id)
        log.info("作业 %s 发布成功 -> %s", job_id, result["path"])
    except Exception as exc:  # 发布失败：保持 PUBLISHING，下一轮重新认领
        log.exception("作业 %s 发布失败，稍后重试", job_id)
        jobs.append_event(job_id, "publish_failed", {"error": f"{type(exc).__name__}: {exc}"})
        time.sleep(1.0)


def main() -> None:
    run()


if __name__ == "__main__":
    main()
