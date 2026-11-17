"""原子写、规范 JSON、内容哈希与跨进程文件锁。

所有作业记录 / 注册表文件的更新都走 ``atomic_write_json``：先写临时文件，
fsync 后 rename，保证任何时刻读到的都是完整的一版，不会出现半截 JSON。
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator


def canonical_json(data: Any) -> str:
    """键排序、无多余空白的规范 JSON，用于计算稳定的复用哈希。"""
    return json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_file(path: Path) -> str:
    """流式哈希，避免把大文件整体读进内存。"""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-", suffix=".part")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)
        raise


def atomic_write_json(path: Path, data: Any) -> None:
    atomic_write_bytes(path, canonical_json(data).encode("utf-8"))


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


@contextlib.contextmanager
def file_lock(lock_path: Path) -> Iterator[None]:
    """跨进程互斥锁。锁文件单独存放，避免锁产物数据文件本身。

    计算进程只有一个，但提交服务与 GC 也会改注册表，因此仍用文件锁
    保证 read-modify-write 原子；锁内只做快速元数据更新，不做重活。
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_id(prefix: str) -> str:
    import uuid

    return f"{prefix}_{uuid.uuid4().hex[:12]}"
