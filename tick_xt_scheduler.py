"""
Xtquant 行情生产调度器：常驻运行，管理 NATS 与 tick producer 子进程。

 - 启动时：拉起 data/job_nats_service.py
- 每个交易日 09:01：启动 data/job_tick_xtquant.py（调度器若在 09:01~09:46 之间启动，会立即拉起 producer）
- 每个交易日 09:46：停止 producer（SIGTERM）
- 非交易日不启动 producer
- 子进程 stdout/stderr 带前缀打印到本进程控制台

用法
----
    PYTHONPATH=. python tick_xt_scheduler.py

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
from tools.utils_remote_am import check_is_open_day
from tools.utils_remote_xt import XtSectorType

PROJECT_ROOT = Path(__file__).resolve().parent
NATS_SCRIPT = PROJECT_ROOT / "data" / "job_nats_service.py"
PRODUCER_SCRIPT = PROJECT_ROOT / "data" / "job_tick_xtquant.py"
NATS_PORT = 4222

PRODUCER_START_HOUR = 9
PRODUCER_START_MINUTE = 14
PRODUCER_STOP_HOUR = 15
PRODUCER_STOP_MINUTE = 1

PRODUCER_SECTORS = (
    XtSectorType.SZ_STOCK,
    XtSectorType.SH_STOCK,
)

STOP_TIMEOUT_SEC = 30.0
TICK_SEC = 1.0


def _minutes_of_day(hour: int, minute: int) -> int:
    return hour * 60 + minute


PRODUCER_WINDOW_START = _minutes_of_day(PRODUCER_START_HOUR, PRODUCER_START_MINUTE)
PRODUCER_WINDOW_STOP = _minutes_of_day(PRODUCER_STOP_HOUR, PRODUCER_STOP_MINUTE)


def _now_minutes(now: datetime) -> int:
    return _minutes_of_day(now.hour, now.minute)


def _in_producer_window(now: datetime) -> bool:
    current = _now_minutes(now)
    return PRODUCER_WINDOW_START <= current < PRODUCER_WINDOW_STOP


def _log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[xt-scheduler {stamp}] {message}", flush=True)


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
                "XT_SUBSCRIBE_SECTORS": ",".join(PRODUCER_SECTORS),
            },
        )
        self._last_start_date: date | None = None
        self._last_stop_date: date | None = None
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
            f"ready; producer schedule "
            f"{PRODUCER_START_HOUR:02d}:{PRODUCER_START_MINUTE:02d} start, "
            f"{PRODUCER_STOP_HOUR:02d}:{PRODUCER_STOP_MINUTE:02d} stop, "
            f"sectors={','.join(PRODUCER_SECTORS)}"
        )
        self._maybe_start_producer_on_startup()

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

    def _maybe_start_producer_on_startup(self) -> None:
        now = datetime.now()
        if not _in_producer_window(now):
            return
        if not self._is_trading_day_cached(now.date()):
            _log("within producer window but not a trading day, producer not started")
            return
        if self.producer.is_running():
            return
        _log("within producer window on trading day, starting producer on startup")
        self.producer.start()
        self._last_start_date = now.date()

    def _try_start_producer(self, now: datetime) -> None:
        today = now.date()
        scheduled = (
            now.hour == PRODUCER_START_HOUR
            and now.minute == PRODUCER_START_MINUTE
            and self._last_start_date != today
        )
        if not scheduled:
            return
        self._last_start_date = today
        if not self._is_trading_day_cached(today):
            _log("producer start skipped, not a trading day")
            return
        if not self.producer.is_running():
            self.producer.start()
        else:
            _log("producer start skipped, already running")

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

        now = datetime.now()
        today = now.date()

        self._try_start_producer(now)

        if (
            now.hour == PRODUCER_STOP_HOUR
            and now.minute == PRODUCER_STOP_MINUTE
            and self._last_stop_date != today
        ):
            self._last_stop_date = today
            if self.producer.is_running():
                self.producer.stop()
            else:
                _log("producer stop skipped, not running")


def main() -> None:
    ProducerScheduler().run()


if __name__ == "__main__":
    main()
