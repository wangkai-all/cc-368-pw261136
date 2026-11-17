"""实际的本地计算：快照固化 -> 清洗 -> 聚合 -> 发布。

不是状态机演示：每一步都真的读写 CSV 并产出数据文件。中间产物按
“输入版 + 规则 + 字段权限”内容寻址，因此同键重算直接复用，无需重跑。
"""
from __future__ import annotations

import csv
import shutil
from pathlib import Path
from typing import Any

from . import manifest, paths

# 可被处理规则引用的指标列，金额类列统一按数值解析
_NUMERIC_COLUMNS = {"amount", "cost"}


# ---------------------------------------------------------------- 输入快照
def snapshot_input(raw_path: Path, snapshot_id: str) -> dict[str, Any]:
    """提交时把原始输入复制到不可变快照，作为本次作业的“输入版”。

    之后原始文件再被修改也不影响在途作业；快照哈希写入作业记录，
    作为复用键的第一段，保证输入变化时绝不会误复用旧结果。
    """
    dest = paths.snapshot_path(snapshot_id)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(raw_path, dest)
    content_hash = manifest.digest_file(dest)
    meta = {
        "snapshot_id": snapshot_id,
        "source_path": str(raw_path),
        "content_hash": content_hash,
        "bytes": dest.stat().st_size,
        "created_at": manifest.utc_now_iso(),
    }
    manifest.atomic_write_json(dest.parent / "snapshot.json", meta)
    return meta


# ---------------------------------------------------------------- 清洗
def clean(
    snapshot_id: str,
    permission: dict[str, Any],
    rule: dict[str, Any],
    artifact_id: str,
    work_dir: Path,
) -> dict[str, Any]:
    """按字段权限裁剪列、按规则过滤脏数据，写出清洗后的 CSV。

    输出物理上只包含该角色被授权的字段——权限隔离在数据层落实，
    而不是仅靠查询时不展示。
    """
    src = paths.snapshot_path(snapshot_id)
    out_dir = work_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    allowed = list(permission["fields"])

    kept = 0
    dropped_missing = 0
    dropped_filter = 0
    metric = rule["metrics"][0]

    with src.open("r", encoding="utf-8", newline="") as fh_in, (
        out_dir / "clean.csv"
    ).open("w", encoding="utf-8", newline="") as fh_out:
        reader = csv.DictReader(fh_in)
        # 只保留授权且实际存在的列，表头顺序以权限声明为准
        columns = [c for c in allowed if c in (reader.fieldnames or [])]
        writer = csv.DictWriter(fh_out, fieldnames=columns)
        writer.writeheader()
        for row in reader:
            if any(not str(row.get(c, "")).strip() for c in columns):
                dropped_missing += 1
                continue
            value = _as_number(row.get(metric))
            if value is None:
                dropped_missing += 1
                continue
            if rule.get("drop_negatives") and value < 0:
                dropped_filter += 1
                continue
            if rule.get("min_amount") is not None and value < rule["min_amount"]:
                dropped_filter += 1
                continue
            writer.writerow({c: row[c] for c in columns})
            kept += 1

    result = {
        "rows_in": kept + dropped_missing + dropped_filter,
        "rows_kept": kept,
        "dropped_missing": dropped_missing,
        "dropped_filtered": dropped_filter,
        "columns": columns,
    }
    manifest.atomic_write_json(out_dir / "clean_stats.json", result)
    return result


# ---------------------------------------------------------------- 聚合
def aggregate(rule: dict[str, Any], work_dir: Path, clean_dir: Path) -> dict[str, Any]:
    """按 group_by 分组，对 metrics 做求和，输出分区结果 CSV。

    输入 ``clean.csv`` 位于清洗产物目录（可被多个分区作业共享，只读），
    本阶段只往自己的分区产物目录写 ``partition.csv``。
    """
    clean_path = clean_dir / "clean.csv"
    group_by = list(rule["group_by"])
    metric = rule["metrics"][0]

    totals: dict[tuple, float] = {}
    counts: dict[tuple, int] = {}
    with clean_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = tuple(row[g] for g in group_by)
            value = _as_number(row[metric]) or 0.0
            totals[key] = totals.get(key, 0.0) + value
            counts[key] = counts.get(key, 0) + 1

    out = work_dir / "partition.csv"
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(group_by + [f"{metric}_sum", "row_count"])
        for key in sorted(totals):
            total = totals[key]
            writer.writerow([*key, _format_number(total), counts[key]])

    return {
        "groups": len(totals),
        "group_by": group_by,
        "metric": metric,
    }


# ---------------------------------------------------------------- 发布
def publish(job: dict[str, Any], work_dir: Path) -> dict[str, Any]:
    """把分区结果原子发布为最终报表，并写血缘清单。

    先写临时目录再整体 rename，发布端在 rename 前退出不会留下
    半成品报表；重新执行时按作业 ID 幂等覆盖。
    """
    report_id = job["report_id"]
    final_dir = paths.published_dir() / report_id / job["job_id"]
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = final_dir.parent / f".staging-{job['job_id']}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    shutil.copyfile(work_dir / "partition.csv", staging / "report.csv")

    lineage = {
        "job_id": job["job_id"],
        "report_id": report_id,
        "published_at": manifest.utc_now_iso(),
        "input": {
            "snapshot_id": job["snapshot_id"],
            "snapshot_hash": job["snapshot_hash"],
            "source_path": job["source_path"],
        },
        "reuse_key": {
            "input_version": job["input_version_hash"],
            "rule_hash": job["rule_hash"],
            "permission_hash": job["permission_hash"],
            "clean_key": job["clean_key"],
            "partition_key": job["partition_key"],
        },
        "artifacts": {
            "clean_artifact_id": job["clean_artifact_id"],
            "partition_artifact_id": job["partition_artifact_id"],
        },
        "rule": job["rule"],
        "permission": job["permission"],
        "rule_version": job.get("rule_version", 1),
    }
    manifest.atomic_write_json(staging / "lineage.json", lineage)
    os_replace_dir(staging, final_dir)

    latest = paths.published_dir() / report_id / "LATEST"
    manifest.atomic_write_bytes(latest, job["job_id"].encode("utf-8"))
    return {"path": str(final_dir / "report.csv"), "lineage": str(final_dir / "lineage.json")}


def os_replace_dir(staging: Path, final_dir: Path) -> None:
    """原子替换目录：rename 到目标名，旧目录先改名再删除，避免长期半态。"""
    import os

    if final_dir.exists():
        backup = final_dir.parent / f".old-{final_dir.name}"
        if backup.exists():
            shutil.rmtree(backup)
        os.rename(final_dir, backup)
        os.rename(staging, final_dir)
        shutil.rmtree(backup)
    else:
        os.rename(staging, final_dir)


def _as_number(raw: Any) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _format_number(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return f"{value:.2f}"
