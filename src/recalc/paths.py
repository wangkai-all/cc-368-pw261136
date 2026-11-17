"""运行期目录约定。所有持久化状态都位于一个根目录下，便于检查与备份。"""
from __future__ import annotations

import os
from pathlib import Path


def root() -> Path:
    """运行时数据根目录，由环境变量 RECALC_HOME 指定（测试可隔离）。"""
    return Path(os.environ.get("RECALC_HOME", "/workspace/.recalc"))


def config_path() -> Path:
    return Path(os.environ.get("RECALC_CONFIG", "/workspace/config/reports.json"))


def jobs_dir() -> Path:
    return root() / "jobs"


def artifacts_dir() -> Path:
    return root() / "artifacts"


def artifact_path(artifact_id: str) -> Path:
    """同一产物的所有文件落在固定子目录，GC 时整目录删除。"""
    return artifacts_dir() / artifact_id


def snapshots_dir() -> Path:
    return root() / "snapshots"


def snapshot_path(snapshot_id: str) -> Path:
    return snapshots_dir() / snapshot_id / "input.csv"


def logs_dir() -> Path:
    return root() / "logs"


def published_dir() -> Path:
    return root() / "published"


def ensure_dirs() -> None:
    for d in (
        jobs_dir(),
        artifacts_dir(),
        snapshots_dir(),
        logs_dir(),
        published_dir(),
    ):
        d.mkdir(parents=True, exist_ok=True)
