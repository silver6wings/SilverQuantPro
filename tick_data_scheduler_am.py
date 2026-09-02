"""
Amazing 行情生产调度器：常驻运行，管理 NATS 与 tick producer 子进程。

 - 启动时：拉起 data/job_nats_service.py
- 每个交易日在 PRODUCER_WINDOWS 时段内运行 data/job_tick_amazing.py（午休停推以节约资源）
- 调度器启动时若已在窗口内，会立即拉起 producer
- 非交易日不启动 producer
- 子进程 stdout/stderr 带前缀打印到本进程控制台

用法
----
    PYTHONPATH=. python tick_data_scheduler_am.py

Ctrl+C 停止调度器，并依次终止 producer 与 nats 子进程。
"""

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path

from data.nats.nats_service import find_pids_on_port, is_nats_server_pid
from framework.tick_subscriber import DEFAULT_QUOTE_WINDOWS, in_quote_windows
from tools.utils_remote_am import AmazingSecurityType, check_is_open_day

PROJECT_ROOT = Path(__file__).resolve().parent
NATS_SCRIPT = PROJECT_ROOT / "data" / "job_nats_service.py"
PRODUCER_SCRIPT = PROJECT_ROOT / "data" / "job_tick_amazing.py"
NATS_PORT = 4222

PRODUCER_WINDOWS = DEFAULT_QUOTE_WINDOWS

PRODUCER_SECURITY_TYPES = (
    AmazingSecurityType.SZ_STOCK,
    AmazingSecurityType.SH_STOCK,
    # AmazingSecurityType.SZ_INDEX,
    # AmazingSecurityType.SH_INDEX,
    # AmazingSecurityType.HS_ETF,
)

STOP_TIMEOUT_SEC = 30.0
TICK_SEC = 1.0


def _format_producer_windows() -> str:
    return ", ".join(f"[{start}, {stop})" for start, stop in PRODUCER_WINDOWS)


def _log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[am-scheduler {stamp}] {message}", flush=True)


def _is_trading_day(day: date) -> bool:
    return check_is_open_day(day.strftime("%Y-%m-%d"))


def _is_nats_listener(pids: list[int]) -> bool:
    return bool(pids) and all(is_nats_server_pid(pid) for pid in pids)


def _should_start_managed_nats(port: int = NATS_PORT) -> bool:
    pids = find_pids_on_port(port)
    if not pids:
        _log(f"no process listening on port {port}")
        return True

    if _is_nats_listener(pids):
        _log(f"using existing nats-server on port {port}, pid(s): {pids}")
        return False

    raise RuntimeError(
        f"port {port} is already in use by non-NATS pid(s) {pids}; "
        f"release the port before starting scheduler"
    )


class ManagedProcess:
    def __init__(
        self,
        name: str,
        script: Path,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.script = script
        self._extra_env = extra_env or {}
        self._proc: subprocess.Popen[str] | None = None
        self._relay_thread: threading.Thread | None = None

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def start(self) -> None:
        if self.is_running():
            _log(f"{self.name} already running (pid={self._proc.pid})")
            return
        if not self.script.is_file():
            raise FileNotFoundError(f"script not found: {self.script}")

        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT)
        env.update(self._extra_env)

        cmd = [sys.executable, str(self.script)]
        _log(f"starting {self.name}: {' '.join(cmd)}")
        self._proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._relay_thread = threading.Thread(
            target=self._relay_output,
            name=f"{self.name}-output",
            daemon=True,
        )
        self._relay_thread.start()
        _log(f"{self.name} started (pid={self._proc.pid})")

    def stop(self, timeout: float = STOP_TIMEOUT_SEC) -> None:
        proc = self._proc
        if proc is None or proc.poll() is not None:
            self._proc = None
            return

        pid = proc.pid
        _log(f"stopping {self.name} (pid={pid})")
        proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _log(f"{self.name} did not exit in {timeout}s, killing")
            proc.kill()
            proc.wait()
        self._proc = None
        _log(f"{self.name} stopped (pid={pid})")

    def _relay_output(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        prefix = f"[{self.name}] "
        try:
            for line in proc.stdout:
                print(f"{prefix}{line}", end="", flush=True)
        except Exception:
            pass


class ProducerScheduler:
    def __init__(self) -> None:
        self.nats = ManagedProcess("nats", NATS_SCRIPT)
        self.producer = ManagedProcess(
            "producer",
            PRODUCER_SCRIPT,
            extra_env={
                "AM_SUBSCRIBE_SECURITY_TYPES": ",".join(PRODUCER_SECURITY_TYPES),
            },
        )
        self._trading_day_checked: date | None = None
        self._trading_day_open = False
        self._using_external_nats = False
        self._running = True
        self._stop_requested = False

    def _is_trading_day_cached(self, day: date) -> bool:
        if self._trading_day_checked != day:
            self._trading_day_checked = day
            self._trading_day_open = _is_trading_day(day)
            _log(
                f"{day} is "
                f"{'a trading day' if self._trading_day_open else 'not a trading day'}"
            )
        return self._trading_day_open

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        self._using_external_nats = not _should_start_managed_nats()
        if not self._using_external_nats:
            self.nats.start()
            time.sleep(2.0)

        _log(
            f"ready; producer windows {_format_producer_windows()}, "
            f"security_types={','.join(PRODUCER_SECURITY_TYPES)}"
        )
        self._sync_producer(datetime.now())

        try:
            while self._running:
                self._tick()
                time.sleep(TICK_SEC)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        if self._stop_requested:
            return
        self._stop_requested = True
        self._running = False
        _log("shutting down")
        self.producer.stop()
        self.nats.stop()
        _log("exit")

    def _on_signal(self, signum: int, _frame: object) -> None:
        _log(f"received signal {signum}")
        self._running = False

    def _sync_producer(self, now: datetime) -> None:
        trading = self._is_trading_day_cached(now.date())
        in_window = trading and in_quote_windows(now)

        if in_window and not self.producer.is_running():
            _log("entering producer window, starting producer")
            self.producer.start()
        elif not in_window and self.producer.is_running():
            reason = "outside producer window" if trading else "not a trading day"
            _log(f"{reason}, stopping producer")
            self.producer.stop()

    def _tick(self) -> None:
        if self._using_external_nats:
            pids = find_pids_on_port(NATS_PORT)
            if _is_nats_listener(pids):
                pass
            elif not pids:
                _log("external nats no longer listening, starting managed nats")
                self._using_external_nats = False
                self.nats.start()
                time.sleep(2.0)
            else:
                raise RuntimeError(
                    f"port {NATS_PORT} is now held by non-NATS pid(s) {pids}"
                )
        elif not self.nats.is_running():
            _log("nats not running, restarting")
            self.nats.start()
            time.sleep(2.0)

        self._sync_producer(datetime.now())


def main() -> None:
    ProducerScheduler().run()


if __name__ == "__main__":
    main()
