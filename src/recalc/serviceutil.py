"""三个进程共用的运行工具：日志、boot_id、优雅退出与可中断等待。"""
from __future__ import annotations

import logging
import signal
import threading
import time
import uuid
from pathlib import Path

from . import manifest, paths

_BOOT_ID = f"boot_{uuid.uuid4().hex[:10]}"
_shutdown = threading.Event()


def boot_id() -> str:
    """每次进程启动一个新 ID，用于产物租约区分新旧世代。"""
    return _BOOT_ID


def setup_logging(name: str) -> logging.Logger:
    paths.logs_dir().mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    file_handler = logging.FileHandler(paths.logs_dir() / f"{name}.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def install_signal_handlers() -> None:
    def _handle(signum, _frame):  # noqa: ANN001
        _shutdown.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def shutdown_requested() -> bool:
    return _shutdown.is_set()


def interruptible_sleep(seconds: float) -> bool:
    """等待期间也能响应退出信号，返回是否被要求退出。"""
    return _shutdown.wait(timeout=seconds)


def wait_until(condition, timeout: float = 300.0, interval: float = 0.25) -> bool:
    """轮询等待某个条件（如别的线程构建完共享产物），可被退出/超时打断。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _shutdown.is_set():
            return False
        if condition():
            return True
        time.sleep(interval)
    return condition()


def stamp() -> str:
    return manifest.utc_now_iso()
