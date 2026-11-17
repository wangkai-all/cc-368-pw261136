"""中间产物与输入快照的注册表。

每个内容寻址产物一条 JSON 记录，记录它的数据目录、状态、引用它的作业
集合以及构建租约。取消某份报表只是移除它自己的引用；引用计数归零且无
租约时，GC 才删除数据文件与注册表——因此一份报表取消不会删掉其他报表
仍在用的中间数据。

状态：creating -> ready / failed。崩溃恢复的依据是租约：每条 creating
记录都带 owner 与心跳时间，owner 重启（新 boot_id）或心跳超时后可接管。
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any, Iterable

from . import manifest, paths

LEASE_TIMEOUT_SECONDS = 60.0


# ---------------------------------------------------------------- 基础读写
def _record_path(artifact_id: str) -> Path:
    return paths.artifacts_dir() / f"{artifact_id}.json"


def _lock_path(artifact_id: str) -> Path:
    return paths.artifacts_dir() / "locks" / f"{artifact_id}.lock"


def _exists(artifact_id: str) -> bool:
    return _record_path(artifact_id).exists()


def get(artifact_id: str) -> dict[str, Any] | None:
    path = _record_path(artifact_id)
    if not path.exists():
        return None
    return manifest.read_json(path)


def list_all() -> list[dict[str, Any]]:
    records = []
    for path in sorted(paths.artifacts_dir().glob("*.json")):
        try:
            records.append(manifest.read_json(path))
        except (ValueError, OSError):
            continue
    return records


# ---------------------------------------------------------------- 占位 / 引用
def acquire_or_create(
    artifact_id: str,
    *,
    kind: str,
    key: str,
    job_id: str,
    boot_id: str,
    detail: dict[str, Any],
    hold_lease: bool = True,
) -> tuple[dict[str, Any], bool]:
    """返回 (注册表记录, 本次调用是否成为构建者)。

    已 ready -> 直接复用，仅追加引用；
    creating 且租约有效 -> 搭车，不成为构建者，等待心跳者完成；
    creating 但租约失效 -> 接管，自己成为构建者；
    failed 且无有效租约 -> 重置为 creating 并重试。

    ``hold_lease=False`` 用于提交服务的占位：只登记引用，不持有租约，
    否则计算进程会把占位者误当成正在构建的人而死等。
    """
    now = manifest.utc_now_iso()
    with manifest.file_lock(_lock_path(artifact_id)):
        record = get(artifact_id)
        if record is None:
            record = {
                "artifact_id": artifact_id,
                "kind": kind,
                "key": key,
                "status": "creating",
                "refs": [job_id],
                "lease": (
                    {"owner_boot": boot_id, "heartbeat": now, "job_id": job_id}
                    if hold_lease
                    else None
                ),
                "detail": detail,
                "created_at": now,
                "updated_at": now,
                "attempts": 1 if hold_lease else 0,
                "last_error": None,
            }
            manifest.atomic_write_json(_record_path(artifact_id), record)
            return record, hold_lease

        if job_id not in record["refs"]:
            record["refs"].append(job_id)
        if record["status"] == "ready":
            record["updated_at"] = now
            manifest.atomic_write_json(_record_path(artifact_id), record)
            return record, False
        if record["status"] == "creating" and not _lease_expired(record):
            record["updated_at"] = now
            manifest.atomic_write_json(_record_path(artifact_id), record)
            return record, False
        # 租约失效（owner 崩溃）或上次失败：接管重试。
        # hold_lease=False（提交服务占位/追加引用）只登记引用，绝不能
        # 抢占一个新租约——否则会把无人构建的占位伪装成“正在构建”，
        # 让真正的计算进程误判而死等。
        if not hold_lease:
            record["status"] = "creating"
            record["lease"] = None
            record["updated_at"] = now
            manifest.atomic_write_json(_record_path(artifact_id), record)
            return record, False
        record["status"] = "creating"
        record["lease"] = {"owner_boot": boot_id, "heartbeat": now, "job_id": job_id}
        record["attempts"] = int(record.get("attempts", 0)) + 1
        record["last_error"] = None
        record["updated_at"] = now
        manifest.atomic_write_json(_record_path(artifact_id), record)
        return record, True


def heartbeat(artifact_id: str, boot_id: str) -> None:
    with manifest.file_lock(_lock_path(artifact_id)):
        record = get(artifact_id)
        if record is None:
            return
        lease = record.get("lease") or {}
        if lease.get("owner_boot") == boot_id and record["status"] == "creating":
            lease["heartbeat"] = manifest.utc_now_iso()
            record["lease"] = lease
            record["updated_at"] = manifest.utc_now_iso()
            manifest.atomic_write_json(_record_path(artifact_id), record)


def mark_ready(artifact_id: str, boot_id: str, detail: dict[str, Any]) -> None:
    with manifest.file_lock(_lock_path(artifact_id)):
        record = get(artifact_id)
        if record is None:
            return
        if record["status"] != "creating" or record.get("lease", {}).get("owner_boot") != boot_id:
            return
        record["status"] = "ready"
        record["lease"] = None
        record["detail"] = {**record.get("detail", {}), **detail}
        record["updated_at"] = manifest.utc_now_iso()
        manifest.atomic_write_json(_record_path(artifact_id), record)


def mark_failed(artifact_id: str, boot_id: str, error: str) -> None:
    with manifest.file_lock(_lock_path(artifact_id)):
        record = get(artifact_id)
        if record is None:
            return
        lease = record.get("lease") or {}
        if lease.get("owner_boot") != boot_id:
            return
        record["status"] = "failed"
        record["lease"] = None
        record["last_error"] = error
        record["updated_at"] = manifest.utc_now_iso()
        manifest.atomic_write_json(_record_path(artifact_id), record)


def remove_ref(artifact_id: str, job_id: str) -> None:
    """取消/失败时释放一份引用。不会动其他作业的引用。"""
    with manifest.file_lock(_lock_path(artifact_id)):
        record = get(artifact_id)
        if record is None:
            return
        if job_id in record["refs"]:
            record["refs"].remove(job_id)
        record["updated_at"] = manifest.utc_now_iso()
        manifest.atomic_write_json(_record_path(artifact_id), record)


def _lease_expired(record: dict[str, Any]) -> bool:
    lease = record.get("lease")
    if not lease:
        return True
    heartbeat_ts = lease.get("heartbeat")
    if not heartbeat_ts:
        return True
    age = time.time() - _parse_iso_epoch(heartbeat_ts)
    return age > LEASE_TIMEOUT_SECONDS


def _parse_iso_epoch(ts: str) -> float:
    from datetime import datetime, timezone

    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()


def reap_stale_leases(current_boot_id: str) -> list[str]:
    """计算进程启动时调用：接管上一世代遗留的全部 creating 租约。

    本架构只有一个共享计算进程：新进程启动意味着旧进程已不在（崩溃或
    重启），因此所有 creating 租约的 owner 都是死亡的旧 boot，无论心跳
    是否还在 60s 超时窗口内都应立即接管——否则新进程会把死进程刚续约
    的租约误判为“正在构建”而干等。不直接删数据，真正删除只走 GC 的
    引用归零路径。属于本 boot 的租约（理论上启动瞬间不存在）保持不动。
    """
    reaped = []
    for record in list_all():
        if record["status"] != "creating":
            continue
        lease = record.get("lease")
        if lease and lease.get("owner_boot") == current_boot_id:
            continue
        with manifest.file_lock(_lock_path(record["artifact_id"])):
            latest = get(record["artifact_id"])
            if not latest or latest["status"] != "creating":
                continue
            latest_lease = latest.get("lease")
            if latest_lease and latest_lease.get("owner_boot") == current_boot_id:
                continue
            latest["lease"] = None
            latest["updated_at"] = manifest.utc_now_iso()
            manifest.atomic_write_json(_record_path(latest["artifact_id"]), latest)
            reaped.append(latest["artifact_id"])
    return reaped


def garbage_collect(live_job_ids: Iterable[str]) -> list[str]:
    """删除无引用且无有效租约的产物数据与注册表，返回被清理的 ID。

    ``live_job_ids`` 是仍在非终态的作业：哪怕注册表 refs 暂时缺记录，
    只要作业还活着就不删，等作业走到终态再次释放后才回收。
    """
    live = set(live_job_ids)
    removed = []
    for record in list_all():
        referenced = any(ref in live for ref in record["refs"])
        if referenced:
            continue
        if record["status"] == "creating" and not _lease_expired(record):
            continue
        data_dir = paths.artifact_path(record["artifact_id"])
        if data_dir.exists():
            shutil.rmtree(data_dir)
        _record_path(record["artifact_id"]).unlink(missing_ok=True)
        lock = _lock_path(record["artifact_id"])
        lock.unlink(missing_ok=True)
        removed.append(record["artifact_id"])
    return removed


# ---------------------------------------------------------------- 输入快照
def snapshot_record_path(snapshot_id: str) -> Path:
    return paths.snapshots_dir() / snapshot_id / "snapshot.json"


def snapshot_add_ref(snapshot_id: str, job_id: str) -> None:
    lock = paths.snapshots_dir() / "locks" / f"{snapshot_id}.lock"
    with manifest.file_lock(lock):
        path = snapshot_record_path(snapshot_id)
        record = manifest.read_json(path)
        refs = set(record.get("refs", []))
        refs.add(job_id)
        record["refs"] = sorted(refs)
        manifest.atomic_write_json(path, record)


def snapshot_remove_ref(snapshot_id: str, job_id: str) -> None:
    lock = paths.snapshots_dir() / "locks" / f"{snapshot_id}.lock"
    with manifest.file_lock(lock):
        path = snapshot_record_path(snapshot_id)
        if not path.exists():
            return
        record = manifest.read_json(path)
        refs = [r for r in record.get("refs", []) if r != job_id]
        record["refs"] = refs
        if not refs:
            shutil.rmtree(path.parent, ignore_errors=True)
            lock.unlink(missing_ok=True)
            return
        manifest.atomic_write_json(path, record)
