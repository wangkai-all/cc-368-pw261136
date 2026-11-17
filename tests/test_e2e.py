"""端到端测试：三个真实进程 + 真实 CSV 计算，覆盖全部需求场景。

直接运行：python3 -I tests/test_e2e.py
（仅用标准库，不依赖 pytest；每个用例独立临时 RECALC_HOME。）
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path("/workspace")
SRC = ROOT / "src"
CONFIG_TEMPLATE = ROOT / "config" / "reports.json"

PASS = 0
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAILURES.append(f"{name} {detail}")
        print(f"  ✗ {name} {detail}")


class Harness:
    def __init__(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="recalc-test-"))
        self.home = self.tmp / "home"
        self.cfgdir = self.tmp / "config"
        self.datadir = self.tmp / "data"
        self.home.mkdir()
        self.cfgdir.mkdir()
        self.datadir.mkdir()
        # 复制配置与原始数据到隔离目录，并把 raw_path 改写成临时绝对路径
        cfg = json.loads(CONFIG_TEMPLATE.read_text(encoding="utf-8"))
        for report in cfg["reports"]:
            src_data = ROOT / report["raw_path"]
            dest = self.datadir / Path(report["raw_path"]).name
            shutil.copyfile(src_data, dest)
            report["raw_path"] = str(dest)
        self.config = self.cfgdir / "reports.json"
        self.config.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        self.port = 8700 + (uuid.uuid4().int % 200)
        self.procs: dict[str, subprocess.Popen] = {}
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "RECALC_HOME": str(self.home),
            "RECALC_CONFIG": str(self.config),
            "RECALC_PORT": str(self.port),
            "PYTHONPATH": str(SRC),
            "PYTHONUNBUFFERED": "1",
        }

    def start(self, name: str) -> None:
        entry = ROOT / "src" / f"run_{name}.py"
        # 输出落文件而不是 PIPE：PIPE 无人读取会写满缓冲区并阻塞子进程
        log_fh = (self.tmp / f"{name}.out.log").open("ab")
        self._log_handles = getattr(self, "_log_handles", [])
        self._log_handles.append(log_fh)
        proc = subprocess.Popen(
            [sys.executable, "-I", str(entry)],
            cwd=str(ROOT),
            env=self.env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
        )
        self.procs[name] = proc

    def start_all(self) -> None:
        self.start("submit_service")
        self._wait_http()
        self.start("worker")
        self.start("publisher")

    def _wait_http(self, timeout: float = 10.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.api("GET", "/api/state")
                return
            except OSError:
                time.sleep(0.15)
        log_text = (self.tmp / "submit_service.out.log").read_text(
            encoding="utf-8", errors="replace") if (self.tmp / "submit_service.out.log").exists() else ""
        raise RuntimeError(f"提交服务未在预期时间内监听。子进程日志:\n{log_text}")

    def stop(self, name: str, sig: int = signal.SIGTERM) -> None:
        proc = self.procs.get(name)
        if proc and proc.poll() is None:
            proc.send_signal(sig)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def stop_all(self) -> None:
        for name in ("publisher", "worker", "submit_service"):
            self.stop(name)

    def cleanup(self) -> None:
        self.stop_all()
        for fh in getattr(self, "_log_handles", []):
            fh.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def api(self, method: str, path: str, payload: dict | None = None) -> dict:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise AssertionError(f"HTTP {exc.code}: {exc.read().decode('utf-8')}") from exc

    def recompute(self, report_id: str, **kw) -> dict:
        body = {"report_id": report_id}
        body.update(kw)
        return self.api("POST", "/api/recompute", body)["job"]

    def state(self) -> dict:
        return self.api("GET", "/api/state")

    def wait_state(self, job_id: str, target: str, timeout: float = 60.0) -> dict:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            for job in self.state()["jobs"]:
                if job["job_id"] == job_id:
                    last = job
                    if job["state"] == target:
                        return job
            time.sleep(0.25)
        raise AssertionError(f"作业 {job_id} 未在 {timeout}s 内到达 {target}，最后状态 {last and last['state']}")

    def wait_terminal(self, job_id: str, timeout: float = 60.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for job in self.state()["jobs"]:
                if job["job_id"] == job_id and job["state"] in (
                    "PUBLISHED", "CANCELED", "FAILED"
                ):
                    return job
            time.sleep(0.25)
        raise AssertionError(f"作业 {job_id} 未在 {timeout}s 内进入终态")

    def artifact(self, artifact_id: str) -> dict | None:
        for art in self.state()["artifacts"]:
            if art["artifact_id"] == artifact_id:
                return art
        return None

    def wait_artifact(self, artifact_id: str, status: str, timeout: float = 30.0) -> dict:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            art = self.artifact(artifact_id)
            if art is not None:
                last = art
                if art["status"] == status:
                    return art
            time.sleep(0.2)
        raise AssertionError(f"产物 {artifact_id} 未在 {timeout}s 内到达 {status}，最后 {last}")

    def wait_artifact_gone(self, artifact_id: str, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.artifact(artifact_id) is None:
                return
            time.sleep(0.25)
        raise AssertionError(f"产物 {artifact_id} 在 {timeout}s 后仍未被 GC")

    def report_csv(self, job_id: str) -> str:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.port}/api/output/{job_id}", timeout=10
        ) as resp:
            return resp.read().decode("utf-8")


# ---------------------------------------------------------------- 用例
def case_basic_publish(h: Harness) -> None:
    print("[1] 基础重算：真实清洗/聚合/发布，输出可下载，血缘完整")
    job = h.recompute("sales_summary")
    done = h.wait_state(job["job_id"], "PUBLISHED")
    check("作业发布成功", done["state"] == "PUBLISHED")
    csv_text = h.report_csv(job["job_id"])
    rows = [r for r in csv_text.strip().splitlines() if r.strip()]
    check("输出含表头+4个区域分组", len(rows) == 5, f"实际 {len(rows)} 行")
    check("负数/坏行被清洗（east 合计=6950）", "6950" in csv_text, csv_text)
    lineage = json.loads(
        (Path(done["output"]["lineage"])).read_text(encoding="utf-8")
    )
    check("血缘含输入快照哈希", lineage["input"]["snapshot_hash"] == job["snapshot_hash"])
    check("血缘含三段复用键", set(lineage["reuse_key"]) == {
        "input_version", "rule_hash", "permission_hash", "clean_key", "partition_key"
    })
    # 快照物理保留，可回溯输入版
    snap = h.home / "snapshots" / job["snapshot_id"] / "input.csv"
    check("发布后快照保留用于追溯", snap.exists())


def case_parallel_share_clean(h: Harness) -> None:
    print("[2] 并行两份重算：同输入/权限/清洗规则 -> 共享清洗结果，分区各自构建")
    delay = {"delay_seconds": 1.5}
    j1 = h.recompute("sales_summary", faults=delay)
    j2 = h.recompute("sales_channel", faults=delay)
    # 在作业发布（释放引用）之前采样：此时共享清洗产物应同时被两个作业引用
    shared = h.wait_artifact(j1["clean_artifact_id"], "ready")
    check("共享清洗产物在计算期间持有2个引用", len(shared["refs"]) == 2, str(shared["refs"]))
    h.wait_state(j1["job_id"], "PUBLISHED")
    h.wait_state(j2["job_id"], "PUBLISHED")
    check("两个作业复用同一清洗产物", j1["clean_artifact_id"] == j2["clean_artifact_id"])
    check("分组不同 -> 分区产物不同", j1["partition_artifact_id"] != j2["partition_artifact_id"])
    clean_data = h.home / "artifacts" / j1["clean_artifact_id"] / "clean.csv"
    check("清洗产物物理存在且只含授权列", clean_data.exists())
    header = clean_data.read_text(encoding="utf-8").splitlines()[0]
    check("analyst 视角无越权字段", set(header.split(",")).issubset(
        {"order_id", "region", "product", "amount", "channel"}))


def case_full_partition_share(h: Harness) -> None:
    print("[3] 规则相同的两份报表：整个中间分区（含清洗）完全共享")
    j1 = h.recompute("sales_summary")
    h.wait_state(j1["job_id"], "PUBLISHED")
    first_part = h.home / "artifacts" / j1["partition_artifact_id"] / "partition.csv"
    first_inode = first_part.stat().st_ino
    # sales_channel 改成与 sales_summary 完全一致的规则再提交
    j2 = h.recompute(
        "sales_channel",
        rule_override={"group_by": ["region"], "metrics": ["amount"],
                       "drop_negatives": True, "min_amount": 0},
    )
    # 第二作业在发布前，分区产物应已 ready 且只被它引用（j1 已发布释放）
    part = h.wait_artifact(j2["partition_artifact_id"], "ready")
    check("第二作业只持有自己这一份引用", part["refs"] == [j2["job_id"]], str(part["refs"]))
    h.wait_state(j2["job_id"], "PUBLISHED")
    check("clean_key 相同", j1["clean_key"] == j2["clean_key"])
    check("partition_key 相同 -> 共享整个中间分区",
          j1["partition_artifact_id"] == j2["partition_artifact_id"])
    # 同一 inode：第二作业没有重新构建，直接复用了第一作业的物理文件
    check("分区物理文件零重建复用（inode 相同）",
          first_part.exists() and first_part.stat().st_ino == first_inode)
    # 第二作业没有产生任何 partition 构建事件
    events2 = (h.home / "jobs" / j2["job_id"] / "events.log").read_text(encoding="utf-8")
    check("复用作业未触发分区构建", "partition_done" not in events2, events2)


def case_cancel_keeps_shared(h: Harness) -> None:
    print("[4] 取消其中一份：不能删除另一份仍在用的中间数据")
    delay = {"delay_seconds": 3}
    j1 = h.recompute("sales_summary", faults=delay)
    j2 = h.recompute("sales_channel", faults=delay)
    time.sleep(0.8)  # 等两个 worker 线程都进入构建
    h.api("POST", "/api/cancel", {"job_id": j2["job_id"]})
    canceled = h.wait_terminal(j2["job_id"])
    check("取消作业进入 CANCELED", canceled["state"] == "CANCELED")
    done = h.wait_state(j1["job_id"], "PUBLISHED", timeout=40)
    check("另一份报表照常发布", done["state"] == "PUBLISHED")
    # 共享清洗产物在保留作业存活期间必须始终存在（核心安全性质）
    shared = h.artifact(j1["clean_artifact_id"])
    check("共享清洗产物未被连带删除", shared is not None and shared["status"] == "ready")
    check("取消作业的引用已移除", j2["job_id"] not in (shared or {}).get("refs", []))
    # 被取消作业独占的分区产物：refs 归零且无租约，由周期 GC 回收（轮询等待）
    h.wait_artifact_gone(j2["partition_artifact_id"], timeout=30)
    check("取消者独占的分区产物已被 GC", h.artifact(j2["partition_artifact_id"]) is None)
    # 保留作业发布后其引用释放，共享产物最终也应被 GC 回收（无任何使用者）
    h.wait_artifact_gone(j1["clean_artifact_id"], timeout=30)
    check("无引用后共享产物最终回收", h.artifact(j1["clean_artifact_id"]) is None)


def case_permission_boundary(h: Harness) -> None:
    print("[5] 字段权限边界：同输入不同权限，清洗结果物理隔离不共享")
    j_analyst = h.recompute("sales_summary")
    j_finance = h.recompute("finance_revenue")
    h.wait_state(j_analyst["job_id"], "PUBLISHED")
    h.wait_state(j_finance["job_id"], "PUBLISHED")
    check("权限哈希不同", j_analyst["permission_hash"] != j_finance["permission_hash"])
    check("清洗产物不共享", j_analyst["clean_artifact_id"] != j_finance["clean_artifact_id"])
    finance_clean = h.home / "artifacts" / j_finance["clean_artifact_id"] / "clean.csv"
    header = finance_clean.read_text(encoding="utf-8").splitlines()[0]
    check("finance 产物物理上不含 product 字段", "product" not in header.split(","), header)


def case_input_change(h: Harness) -> None:
    print("[6] 输入变化：快照固化 + 内容哈希改变 -> 绝不复用旧中间数据")
    j1 = h.recompute("sales_summary")
    h.wait_state(j1["job_id"], "PUBLISHED")
    # 修改原始输入后再提交
    raw = Path(j1["source_path"])
    with raw.open("a", encoding="utf-8") as fh:
        fh.write("SO-9001,east,widget,5000,online\n")
    j2 = h.recompute("sales_summary")
    check("输入哈希随内容变化", j1["input_version_hash"] != j2["input_version_hash"])
    check("clean_key 随输入变化", j1["clean_key"] != j2["clean_key"])
    check("分区产物不共享", j1["partition_artifact_id"] != j2["partition_artifact_id"])
    done2 = h.wait_state(j2["job_id"], "PUBLISHED")
    csv_text = h.report_csv(done2["job_id"])
    check("新结果包含追加数据（east=11950）", "11950" in csv_text, csv_text)
    # 旧快照与旧产物仍在，旧报表结果可追溯
    old_snap = h.home / "snapshots" / j1["snapshot_id"] / "input.csv"
    check("旧快照不受原文件修改影响", old_snap.exists() and
          "SO-9001" not in old_snap.read_text(encoding="utf-8"))


def case_transient_failure_retry(h: Harness) -> None:
    print("[7] 共享计算瞬时失败：worker 自动重试，成功后产物仍可复用")
    j1 = h.recompute("sales_summary", faults={
        "fail_stage": "clean", "fail_times_remaining": 2, "delay_seconds": 0})
    done = h.wait_state(j1["job_id"], "PUBLISHED", timeout=40)
    check("重试后发布成功", done["state"] == "PUBLISHED")
    art = h.artifact(j1["clean_artifact_id"])
    check("产物 attempts=3（1 次初始 + 2 次重试）", art is not None and art["attempts"] == 3,
          str(art and art["attempts"]))
    events = (h.home / "jobs" / j1["job_id"] / "events.log").read_text(encoding="utf-8")
    check("事件流水记录了失败与重试", events.count("build_failed") == 2, events)
    # 后续同键作业直接复用，不再失败
    j2 = h.recompute("sales_summary")
    done2 = h.wait_state(j2["job_id"], "PUBLISHED", timeout=20)
    art2 = h.artifact(j2["clean_artifact_id"])
    check("后续作业复用已恢复产物", j2["clean_artifact_id"] == j1["clean_artifact_id"]
          and art2["attempts"] == 3)
    check("复用作业输出一致", h.report_csv(done["job_id"]) == h.report_csv(done2["job_id"]))


def case_persistent_failure(h: Harness) -> None:
    print("[8] 重试耗尽：作业 FAILED 并释放引用，GC 回收，其他作业不受影响")
    j_bad = h.recompute("cost_detail", faults={
        "fail_stage": "clean", "fail_times_remaining": 99, "delay_seconds": 0})
    j_ok = h.recompute("sales_summary")
    failed = h.wait_terminal(j_bad["job_id"], timeout=60)
    check("持续失败作业进入 FAILED", failed["state"] == "FAILED")
    check("记录失败阶段与原因", failed["error"]["stage"] == "clean")
    # 无关报表不受失败影响：正常发布且输出正确（其产物发布后被 GC 属预期）
    ok = h.wait_state(j_ok["job_id"], "PUBLISHED")
    check("无关报表正常发布", ok["state"] == "PUBLISHED")
    check("无关报表输出正确", "6950" in h.report_csv(j_ok["job_id"]))
    check("失败与成功作业的产物彼此隔离",
          j_bad["clean_artifact_id"] != j_ok["clean_artifact_id"])
    # 失败作业的产物 refs 已释放、无租约，由周期 GC 回收（轮询等待）
    h.wait_artifact_gone(j_bad["clean_artifact_id"], timeout=30)
    check("失败产物已被 GC", h.artifact(j_bad["clean_artifact_id"]) is None)


def case_crash_before_publish(h: Harness) -> None:
    print("[9] 中间结果写完、发布未确认时发布端硬退出：重启后恢复并可追溯")
    job = h.recompute("sales_summary", faults={"crash_before_publish": True})
    # publisher 会 os._exit；等它死掉，作业停在 PUBLISHING
    deadline = time.time() + 40
    while time.time() < deadline:
        proc = h.procs["publisher"]
        if proc.poll() is not None:
            break
        time.sleep(0.2)
    check("发布端进程确实硬退出", h.procs["publisher"].poll() is not None)
    current = next(j for j in h.state()["jobs"] if j["job_id"] == job["job_id"])
    check("作业停在 PUBLISHING（中间分区已就绪）", current["state"] == "PUBLISHING")
    part = h.home / "artifacts" / job["partition_artifact_id"] / "partition.csv"
    check("中间分区已落盘", part.exists())
    # 重启发布端，依据作业记录恢复
    h.start("publisher")
    done = h.wait_state(job["job_id"], "PUBLISHED", timeout=30)
    check("恢复后发布成功", done["state"] == "PUBLISHED")
    lineage = json.loads(Path(done["output"]["lineage"]).read_text(encoding="utf-8"))
    check("恢复后血缘仍可追溯输入版", lineage["input"]["snapshot_hash"] == job["snapshot_hash"])
    check("发布结果可下载且内容正确", "6950" in h.report_csv(job["job_id"]))


def case_worker_crash_recovery(h: Harness) -> None:
    print("[10] 计算进程构建中被 kill：租约失效后新世代接管，作业完成")
    job = h.recompute("sales_summary", faults={"delay_seconds": 6})
    time.sleep(1.5)
    h.stop("worker", sig=signal.SIGKILL)
    check("worker 已被强杀", h.procs["worker"].poll() is not None)
    mid = next(j for j in h.state()["jobs"] if j["job_id"] == job["job_id"])
    check("作业仍处于非终态可恢复", mid["state"] in ("PENDING", "COMPUTING"))
    h.start("worker")
    done = h.wait_state(job["job_id"], "PUBLISHED", timeout=45)
    check("新 worker 接管租约并完成", done["state"] == "PUBLISHED")
    check("输出内容正确", "6950" in h.report_csv(job["job_id"]))


def case_rule_edit_then_submit(h: Harness) -> None:
    print("[11] 修改规则再提交：规则哈希变化产生新产物，旧作业结果保持可追溯")
    j1 = h.recompute("sales_summary")
    h.wait_state(j1["job_id"], "PUBLISHED")
    h.api("POST", "/api/rules", {
        "report_id": "sales_summary",
        "rule_override": {"min_amount": 1000},
    })
    j2 = h.recompute("sales_summary")
    check("规则哈希变化", j1["rule_hash"] != j2["rule_hash"])
    check("清洗产物因清洗规则变化而不同", j1["clean_artifact_id"] != j2["clean_artifact_id"])
    done2 = h.wait_state(j2["job_id"], "PUBLISHED")
    csv2 = h.report_csv(done2["job_id"])
    # min_amount=1000 后，低于 1000 的分组行被过滤，east 仅保留 1200+2100+1500=4800
    check("新规则结果生效（east=4800）", "4800" in csv2, csv2)
    check("旧作业输出仍可下载", "6950" in h.report_csv(j1["job_id"]))
    cfg = json.loads(h.config.read_text(encoding="utf-8"))
    saved = next(r for r in cfg["reports"] if r["id"] == "sales_summary")
    check("规则已持久化到配置", saved["rules"]["min_amount"] == 1000)


def main() -> int:
    cases = [
        case_basic_publish,
        case_parallel_share_clean,
        case_full_partition_share,
        case_cancel_keeps_shared,
        case_permission_boundary,
        case_input_change,
        case_transient_failure_retry,
        case_persistent_failure,
        case_crash_before_publish,
        case_worker_crash_recovery,
        case_rule_edit_then_submit,
    ]
    for case in cases:
        h = Harness()
        try:
            h.start_all()
            case(h)
        except Exception as exc:  # noqa: BLE001
            FAILURES.append(f"{case.__name__} 抛异常: {type(exc).__name__}: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            h.cleanup()
    print("\n================ 结果 ================")
    print(f"通过断言 {PASS} 条；失败 {len(FAILURES)} 条")
    for f in FAILURES:
        print("  -", f)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
