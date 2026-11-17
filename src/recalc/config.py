"""报表与字段权限配置。

复用判定的三个输入之一是字段权限：同一份原始数据，不同角色能看到的
字段集合不同，清洗产物里物理上就不允许出现越权字段，因此权限不同时
清洗结果也不能共享（由 :mod:`recalc.registry` 的产物 ID 天然隔离）。
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from . import manifest, paths

# 规则中影响处理结果的字段。故障注入参数等运行期选项不在此列，
# 否则一次“失败重试”会产生新的产物 ID，破坏复用。
RULE_FIELDS = ("drop_negatives", "min_amount", "group_by", "metrics")


def load_config() -> dict[str, Any]:
    return manifest.read_json(paths.config_path())


def list_reports() -> list[dict[str, Any]]:
    return sorted(load_config()["reports"], key=lambda r: r["id"])


def get_report(report_id: str) -> dict[str, Any]:
    for report in load_config()["reports"]:
        if report["id"] == report_id:
            return report
    raise KeyError(report_id)


def effective_rule(report: dict[str, Any], rule_override: dict[str, Any] | None) -> dict[str, Any]:
    """合并报表默认规则与本次提交的规则覆盖。"""
    rule = {field: report["rules"].get(field) for field in RULE_FIELDS}
    if rule_override:
        for field in RULE_FIELDS:
            if field in rule_override:
                rule[field] = rule_override[field]
    return rule


def rule_hash(rule: dict[str, Any]) -> str:
    """处理规则的稳定哈希：只取真正影响结果的字段。"""
    return manifest.digest_text(manifest.canonical_json({k: rule.get(k) for k in RULE_FIELDS}))


def permission_hash(permission: dict[str, Any]) -> str:
    """字段权限的稳定哈希（角色 + 允许字段的有序集合）。"""
    return manifest.digest_text(
        manifest.canonical_json(
            {
                "role": permission["role"],
                "fields": sorted(permission["fields"]),
            }
        )
    )


def update_report_rules(report_id: str, rule_override: dict[str, Any]) -> dict[str, Any]:
    """网页“修改规则再提交”使用：把规则覆盖写回配置，后续提交默认采用。"""
    cfg = load_config()
    for report in cfg["reports"]:
        if report["id"] == report_id:
            for field in RULE_FIELDS:
                if field in rule_override:
                    report["rules"][field] = rule_override[field]
            manifest.atomic_write_json(paths.config_path(), cfg)
            return deepcopy(report)
    raise KeyError(report_id)
