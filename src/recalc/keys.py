"""复用键计算。

复用判定的三段输入（缺一不可，绝不只比任务名称）：

1. 输入版 ``input_version_hash`` —— 快照内容哈希，输入一变键就变；
2. 处理规则哈希 —— 清洗相关规则与完整规则分别参与两层键；
3. 字段权限哈希 —— 权限不同物理隔离，连清洗结果都不共享。

两级共享：
* ``clean_key``     = 输入版 + 清洗规则 + 字段权限，相同则共享清洗后的输入；
* ``partition_key`` = clean_key + 完整规则（含分组/指标），相同才共享整个中间分区。
"""
from __future__ import annotations

from typing import Any

from . import config, manifest

# 只影响清洗阶段的规则字段；group_by 只影响聚合，不影响清洗结果
CLEAN_RULE_FIELDS = ("drop_negatives", "min_amount", "metrics")


def _clean_rule_hash(rule: dict[str, Any]) -> str:
    return manifest.digest_text(
        manifest.canonical_json({k: rule.get(k) for k in CLEAN_RULE_FIELDS})
    )


def compute_keys(
    snapshot_meta: dict[str, Any], rule: dict[str, Any], permission: dict[str, Any]
) -> dict[str, str]:
    input_version_hash = snapshot_meta["content_hash"]
    full_rule_hash = config.rule_hash(rule)
    clean_rule_hash = _clean_rule_hash(rule)
    perm_hash = config.permission_hash(permission)

    clean_key = manifest.digest_text(
        manifest.canonical_json(
            {
                "input_version": input_version_hash,
                "clean_rule": clean_rule_hash,
                "permission": perm_hash,
            }
        )
    )
    partition_key = manifest.digest_text(
        manifest.canonical_json(
            {
                "clean_key": clean_key,
                "full_rule": full_rule_hash,
            }
        )
    )
    return {
        "input_version_hash": input_version_hash,
        "rule_hash": full_rule_hash,
        "permission_hash": perm_hash,
        "clean_key": clean_key,
        "partition_key": partition_key,
        "clean_artifact_id": f"clean_{clean_key[:16]}",
        "partition_artifact_id": f"part_{partition_key[:16]}",
    }
