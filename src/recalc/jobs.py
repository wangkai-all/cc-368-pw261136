"""作业记录与状态流转。

作业记录是可恢复性的核心：每个阶段用了哪个快照、哪个产物、规则与权限
哈希是什么，全部落在 ``jobs/<job_id>/job.json``，并追加只增不改的
``events.log``。进程重启后扫描该目录即可把在途作业接续下去。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from . import manifest, paths, registry

# 非终态
ACTIVE_STATES = {"PENDING", "COMPUTING", "PUBLISHING"}
# 终态
TERMINAL_STATES = {"PUBLISHED", "CANCELED", "FAILED"}

# 终态 -> 是否已产出最终报表
_PUBLISHED_STATES = {"PUBLISHED"}


def job_dir(job_id: str) -> Path:
    return paths.jobs_dir() / job_id


def job_file(job_id: str) -> Path:
    return job_dir(job_id) / "job.json"


def _lock(job_id: str) -> Path:
    return job_dir(job_id) / ".lock"


def create(job: dict[str, Any]) -> dict[str, Any]:
    job_dir(job["job_id"]).mkdir(parents=True, exist_ok=False)
    manifest.atomic_write_json(job_file(job["job_id"]), job)
    append_event(job["job_id"], "created", {"state": job["state"]})
    return job


def get(job_id: str) -> dict[str, Any] | None:
    path = job_file(job_id)
    if not path.exists():
        return None
    return manifest.read_json(path)


def list_all() -> list[dict[str, Any]]:
    jobs = []
    for path in sorted(paths.jobs_dir().glob("*/job.json")):
        try:
            jobs.append(manifest.read_json(path))
        except (ValueError, OSError):
            continue
    jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
    return jobs


def active_jobs() -> list[dict[str, Any]]:
    return [j for j in list_all() if j["state"] in ACTIVE_STATES]


def append_event(job_id: str, event: str, detail: dict[str, Any] | None = None) -> None:
    line = manifest.canonical_json(
        {"at": manifest.utc_now_iso(), "event": event, "detail": detail or {}}
    )
    with manifest.file_lock(job_dir(job_id) / ".events.lock"):
        with (job_dir(job_id) / "events.log").open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def update(job_id: str, mutate) -> dict[str, Any] | None:
    """在作业锁内做 read-modify-write，``mutate`` 原地修改记录。"""
    with manifest.file_lock(_lock(job_id)):
        job = get(job_id)
        if job is None:
            return None
        before = job["state"]
        mutate(job)
        job["updated_at"] = manifest.utc_now_iso()
        manifest.atomic_write_json(job_file(job_id), job)
        if job["state"] != before:
            append_event(job_id, "state_changed", {"from": before, "to": job["state"]})
        return job


def request_cancel(job_id: str, reason: str) -> dict[str, Any] | None:
    """提交服务处理取消请求。

    已终态：幂等返回；非终态：置 CANCELED 并立即释放本作业持有的全部
    引用（快照 + 两个产物）。正在构建产物的计算进程轮询到该状态后会
    中断构建；若它正好完成，mark_ready 仍安全，产物会在 refs 为空时
    由 GC 回收。
    """
    with manifest.file_lock(_lock(job_id)):
        job = get(job_id)
        if job is None or job["state"] in TERMINAL_STATES:
            return job
        job["state"] = "CANCELED"
        job["cancel_reason"] = reason
        job["updated_at"] = manifest.utc_now_iso()
        manifest.atomic_write_json(job_file(job_id), job)
        append_event(job_id, "state_changed", {"from": "PENDING/ACTIVE", "to": "CANCELED"})
        _release_refs(job)
        return job


def fail(job_id: str, stage: str, error: str) -> None:
    """共享计算失败且重试耗尽时收口：置 FAILED 并释放引用。

    重试责任在计算进程（产物级 attempts 计数，见 registry）；发布失败
    由发布端在下个轮询周期重新认领，作业记录是认领依据。
    """

    def mutate(job: dict[str, Any]) -> None:
        if job["state"] in TERMINAL_STATES:
            return
        job["state"] = "FAILED"
        job["error"] = {"stage": stage, "message": error, "at": manifest.utc_now_iso()}
        _release_refs(job)

    update(job_id, mutate)


def release_on_published(job_id: str) -> None:
    """发布确认后释放中间产物引用；快照引用保留，供血缘回溯读取输入版。"""
    job = get(job_id)
    if job is None:
        return
    registry.remove_ref(job["clean_artifact_id"], job_id)
    registry.remove_ref(job["partition_artifact_id"], job_id)
    append_event(job_id, "refs_released", {"scope": "intermediate", "snapshot_kept": True})


def _release_refs(job: dict[str, Any]) -> None:
    """释放作业持有的全部引用（取消/失败路径）。"""
    registry.remove_ref(job["clean_artifact_id"], job["job_id"])
    registry.remove_ref(job["partition_artifact_id"], job["job_id"])
    registry.snapshot_remove_ref(job["snapshot_id"], job["job_id"])
