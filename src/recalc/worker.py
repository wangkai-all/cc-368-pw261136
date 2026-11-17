"""共享计算进程（worker）。

职责：
* 轮询非终态作业，按 clean -> partition 两阶段构建内容寻址产物；
* 同一产物全进程只构建一次，其他线程在内存事件上搭车，跨进程则靠
  注册表租约与状态接力（崩溃后新世代接管失效租约）；
* 失败按产物级 attempts 重试，重试耗尽才让依赖它的作业失败并释放引用；
* 周期性 GC：引用归零且无有效租约的产物数据才删除；
* 任意时刻被 SIGTERM/os._exit 杀掉，重启后依据注册表与作业记录恢复。
"""
from __future__ import annotations

import concurrent.futures
import threading
import time

from . import engine, jobs, paths, registry, serviceutil
from .serviceutil import setup_logging

MAX_ATTEMPTS = 3
BACKOFF_SECONDS = [0.0, 0.5, 1.5]
POLL_INTERVAL = 0.4
GC_INTERVAL = 20.0
HEARTBEAT_INTERVAL = 8.0
# 搭车线程两次回查注册表之间的等待上限；事件通常会在产物就绪时立即唤醒，
# 这只是兜底（例如唤醒事件对象尚未被等待线程注册的极端窗口）。
LEASE_WAIT_SECONDS = 5.0

log = setup_logging("worker")


class Worker:
    def __init__(self) -> None:
        self.boot = serviceutil.boot_id()
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="calc")
        self.futures: dict[str, concurrent.futures.Future] = {}
        # artifact_id -> 唤醒事件，仅用于进程内线程在产物就绪/失败时被及时唤醒。
        # “谁是构建者”完全以注册表租约为准，事件不承担任何状态判定，
        # 因此不会与磁盘状态产生双层不一致。
        self.signals: dict[str, threading.Event] = {}
        self.signals_lock = threading.Lock()
        self.last_gc = 0.0

    # ------------------------------------------------------------ 生命周期
    def run(self) -> None:
        serviceutil.install_signal_handlers()
        paths.ensure_dirs()
        log.info("worker 启动 boot_id=%s", self.boot)
        reaped = registry.reap_stale_leases(self.boot)
        if reaped:
            log.info("启动恢复：接管失效租约产物 %s", reaped)
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        try:
            self._main_loop()
        finally:
            log.info("worker 停止，等待在途计算线程结束")
            serviceutil._shutdown.set()
            self.pool.shutdown(wait=True, cancel_futures=False)
            removed = registry.garbage_collect(j["job_id"] for j in jobs.active_jobs())
            if removed:
                log.info("退出前 GC 回收 %s", removed)
            log.info("worker 已退出")

    def _main_loop(self) -> None:
        while not serviceutil.shutdown_requested():
            try:
                self._prune_futures()
                capacity = 2 - len(self.futures)
                if capacity > 0:
                    for job in jobs.active_jobs():
                        if capacity <= 0:
                            break
                        if job["job_id"] in self.futures:
                            continue
                        if job["state"] == "PUBLISHING":
                            continue  # 等发布端认领
                        future = self.pool.submit(self._run_job_safely, job["job_id"])
                        self.futures[job["job_id"]] = future
                        capacity -= 1
                if time.time() - self.last_gc > GC_INTERVAL:
                    self.last_gc = time.time()
                    removed = registry.garbage_collect(j["job_id"] for j in jobs.active_jobs())
                    if removed:
                        log.info("周期 GC 回收 %s", removed)
            except Exception:  # 单次轮询出错不能拖垮整个计算进程
                log.exception("主循环轮询异常，下一轮继续")
            serviceutil.interruptible_sleep(POLL_INTERVAL)

    def _prune_futures(self) -> None:
        done = [jid for jid, fut in self.futures.items() if fut.done()]
        for jid in done:
            fut = self.futures.pop(jid)
            exc = fut.exception()
            if exc:
                log.error("作业 %s 线程异常: %r", jid, exc)

    def _heartbeat_loop(self) -> None:
        while not serviceutil.shutdown_requested():
            for record in registry.list_all():
                if record["status"] == "creating":
                    lease = record.get("lease") or {}
                    if lease.get("owner_boot") == self.boot:
                        registry.heartbeat(record["artifact_id"], self.boot)
            if serviceutil.interruptible_sleep(HEARTBEAT_INTERVAL):
                return

    # ------------------------------------------------------------ 作业执行
    def _run_job_safely(self, job_id: str) -> None:
        try:
            self._run_job(job_id)
        except Exception:  # 兜底：绝不让线程静默死掉
            log.exception("作业 %s 未捕获异常", job_id)
            jobs.fail(job_id, "worker", "未捕获异常，详见 worker 日志")

    def _run_job(self, job_id: str) -> None:
        job = jobs.get(job_id)
        if job is None or job["state"] in jobs.TERMINAL_STATES:
            return

        jobs.update(job_id, lambda j: j.update(state="COMPUTING") if j["state"] == "PENDING" else None)

        if not self._ensure_artifact(job_id, "clean"):
            return
        if serviceutil.shutdown_requested() or self._is_canceled(job_id):
            return
        if not self._ensure_artifact(job_id, "partition"):
            return
        if self._is_canceled(job_id):
            return

        def to_publishing(j: dict) -> None:
            if j["state"] == "COMPUTING":
                j["state"] = "PUBLISHING"

        jobs.update(job_id, to_publishing)
        log.info("作业 %s 计算完成，等待发布端确认", job_id)

    def _is_canceled(self, job_id: str) -> bool:
        job = jobs.get(job_id)
        return job is None or job["state"] == "CANCELED"

    def _is_terminal(self, job_id: str) -> bool:
        job = jobs.get(job_id)
        return job is None or job["state"] in jobs.TERMINAL_STATES

    # ---------------------------------------------------- 产物构建与复用
    def _signal(self, artifact_id: str) -> threading.Event:
        with self.signals_lock:
            event = self.signals.get(artifact_id)
            if event is None:
                event = threading.Event()
                self.signals[artifact_id] = event
            return event

    def _notify(self, artifact_id: str) -> None:
        self._signal(artifact_id).set()

    def _ensure_artifact(self, job_id: str, kind: str) -> bool:
        """保证 job 依赖的产物 ready。返回 False 表示作业终止（取消/失败）。

        协调的唯一事实来源是注册表租约：每一轮都重新尝试在注册表锁内认领，
        认领到就构建一次，认领到别人的有效租约就等待其落盘状态变化。
        失败后也回到这里重新认领，attempts 由注册表统一递增，重试上限因此
        不会因循环位置不同而失效。进程内事件只负责及时唤醒，不参与
        “谁构建”的判定，因此不存在双层不一致。
        """
        job = jobs.get(job_id)
        artifact_id = job[f"{kind}_artifact_id"]
        key = job[f"{kind}_key"] if kind == "partition" else job["clean_key"]
        event = self._signal(artifact_id)
        backoff_until = 0.0

        while True:
            if serviceutil.shutdown_requested() or self._is_terminal(job_id):
                return False
            if time.time() < backoff_until:
                if serviceutil.interruptible_sleep(backoff_until - time.time()):
                    return False

            record, became_builder = registry.acquire_or_create(
                artifact_id,
                kind=kind,
                key=key,
                job_id=job_id,
                boot_id=self.boot,
                detail={"kind": kind, "job_id": job_id},
            )
            if record["status"] == "ready":
                return True
            if not became_builder:
                # 已有有效构建者（同进程其他线程或另一进程），等其落盘后重查
                log.info("作业 %s 等待共享产物 %s（构建者 %s）",
                         job_id, artifact_id, (record.get("lease") or {}).get("owner_boot"))
                event.wait(timeout=LEASE_WAIT_SECONDS)
                continue

            # 成为构建者：清掉上一世代遗留的事件位，确保本次搭车者真正阻塞，
            # 直到本轮结束时 _notify 再唤醒。
            event.clear()
            if serviceutil.shutdown_requested() or self._is_canceled(job_id):
                # 立即释放租约并置 failed：活跃的搭车者下一轮即可接管，
                # 无活跃引用时 failed+无租约+refs 归零由 GC 回收数据。
                registry.mark_failed(artifact_id, self.boot, "构建者作业取消或进程退出")
                self._notify(artifact_id)
                return False

            try:
                self._execute_stage(job_id, kind, artifact_id)
            except Exception as exc:  # 共享计算失败：worker 负责重试
                error = f"{type(exc).__name__}: {exc}"
                # 取消优先于失败处理：构建者自己被取消，只释放租约退出，
                # 绝不连累其他活跃引用者——它们下一轮会接管重建。
                if serviceutil.shutdown_requested() or self._is_canceled(job_id):
                    registry.mark_failed(artifact_id, self.boot, "构建者作业取消或进程退出")
                    self._notify(artifact_id)
                    return False
                registry.mark_failed(artifact_id, self.boot, error)
                self._notify(artifact_id)
                latest = registry.get(artifact_id)
                attempts = int(latest.get("attempts", 1)) if latest else MAX_ATTEMPTS
                log.warning("产物 %s 第 %d 次构建失败: %s", artifact_id, attempts, error)
                jobs.append_event(
                    job_id, "build_failed",
                    {"artifact": artifact_id, "attempt": attempts, "error": error},
                )
                if attempts >= MAX_ATTEMPTS:
                    # 真正的计算重试耗尽：此时所有依赖者都无法得到该产物，
                    # 才统一收口为 FAILED 并释放引用。
                    self._fail_dependents(artifact_id, kind, error)
                    return False
                # 退避后回到循环顶部重新认领（failed 且无租约 -> 接管，attempts+1）
                backoff_until = time.time() + BACKOFF_SECONDS[
                    min(attempts, len(BACKOFF_SECONDS) - 1)
                ]
                continue

            registry.mark_ready(
                artifact_id,
                self.boot,
                {"ready_at": serviceutil.stamp(),
                 "data_dir": str(paths.artifact_path(artifact_id))},
            )
            self._notify(artifact_id)
            log.info("产物 %s 构建完成（%s），refs=%s",
                     artifact_id, kind, registry.get(artifact_id)["refs"])
            return True

    def _execute_stage(self, job_id: str, kind: str, artifact_id: str) -> None:
        job = jobs.get(job_id)
        work_dir = paths.artifact_path(artifact_id)
        work_dir.mkdir(parents=True, exist_ok=True)
        self._apply_faults(job_id, kind)

        if kind == "clean":
            stats = engine.clean(
                job["snapshot_id"], job["permission"], job["rule"], artifact_id, work_dir
            )
            jobs.append_event(job_id, "clean_done", {"artifact": artifact_id, **stats})
        else:
            # 清洗产物是独立的内容寻址目录，分区阶段只读复用它
            clean_dir = paths.artifact_path(job["clean_artifact_id"])
            stats = engine.aggregate(job["rule"], work_dir, clean_dir)
            jobs.append_event(job_id, "partition_done", {"artifact": artifact_id, **stats})

    def _apply_faults(self, job_id: str, kind: str) -> None:
        """测试故障注入：阶段延迟（便于制造并行/取消竞态）与瞬时失败。

        故障参数不属于复用键（见 config.RULE_FIELDS），重试成功后产物
        与正常路径完全一致，仍然可被后续作业复用。
        """
        job = jobs.get(job_id)
        faults = job.get("faults") or {}
        delay = float(faults.get("delay_seconds", 0) or 0)
        if delay > 0:
            log.info("故障注入：作业 %s 在 %s 阶段等待 %.1fs", job_id, kind, delay)
            if serviceutil.interruptible_sleep(delay):
                raise RuntimeError("阶段等待中进程退出")
            if self._is_canceled(job_id):
                raise RuntimeError("作业已取消，中断阶段执行")

        if faults.get("fail_stage") == kind:
            remaining = int(faults.get("fail_times_remaining", 0) or 0)
            if remaining > 0:
                def consume(j: dict) -> None:
                    j.setdefault("faults", {})["fail_times_remaining"] = remaining - 1
                jobs.update(job_id, consume)
                raise RuntimeError(f"注入失败：{kind} 阶段瞬时错误（剩余 {remaining - 1} 次）")

    # ------------------------------------------------------------ 收口
    def _fail_dependents(self, artifact_id: str, kind: str, error: str) -> None:
        record = registry.get(artifact_id)
        if not record:
            return
        for ref in list(record["refs"]):
            job = jobs.get(ref)
            if job and job["state"] in jobs.ACTIVE_STATES:
                jobs.fail(ref, kind, f"共享产物 {artifact_id} 重试耗尽：{error}")
                log.error("作业 %s 因共享产物失败而终止", ref)


def main() -> None:
    Worker().run()


if __name__ == "__main__":
    main()
