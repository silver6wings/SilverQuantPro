"""
根据操作系统与 CPU 架构，从 _service 目录选择 nats-server 二进制并管理进程。
"""
from __future__ import annotations

import atexit
import logging
import os
import platform
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SERVICE_DIR = PROJECT_ROOT / "_service"
DEFAULT_NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")

_PLATFORM_ALIASES: dict[str, str] = {
    "darwin": "darwin",
    "windows": "windows",
}

_ARCH_ALIASES: dict[str, str] = {
    "arm64": "arm64",
    "aarch64": "arm64",
    "amd64": "amd64",
    "x86_64": "amd64",
    "x64": "amd64",
}


@dataclass(frozen=True)
class RuntimeInfo:
    system: str
    machine: str
    platform_key: str
    arch_key: str

    @property
    def label(self) -> str:
        return f"{self.platform_key}-{self.arch_key}"


@dataclass(frozen=True)
class NatsServiceConfig:
    service_dir: Path = DEFAULT_SERVICE_DIR
    nats_url: str = DEFAULT_NATS_URL
    startup_timeout: float = 5.0
    stop_timeout: float = 5.0


class NatsServiceManager:
    def __init__(self, config: NatsServiceConfig | None = None) -> None:
        self.config = config or NatsServiceConfig()
        self._process: subprocess.Popen[bytes] | None = None
        self._binary_path: Path | None = None
        atexit.register(self.stop)

    @property
    def binary_path(self) -> Path | None:
        return self._binary_path

    @property
    def pid(self) -> int | None:
        process = self._process
        if process is None or process.poll() is not None:
            return None
        return process.pid

    @property
    def port(self) -> int:
        return parse_nats_port(self.config.nats_url)

    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def start(self, extra_args: list[str] | None = None) -> Path:
        if self.is_running():
            assert self._binary_path is not None
            logger.info("nats-server already running on port %d (%s)", self.port, self._binary_path.name)
            return self._binary_path

        existing_pids = find_pids_on_port(self.port)
        if existing_pids:
            raise RuntimeError(
                f"port {self.port} already in use by pid(s) {existing_pids}; "
                f"run `python demo/demo_nats_service.py kill --port {self.port}` first"
            )

        runtime = detect_runtime()
        binary = resolve_nats_binary(runtime, self.config.service_dir)
        self._ensure_executable(binary)

        cmd = [str(binary), "-p", str(self.port)]
        if extra_args:
            cmd.extend(extra_args)

        logger.info(
            "starting nats-server: platform=%s, binary=%s, port=%d",
            runtime.label,
            binary.name,
            self.port,
        )

        popen_kwargs: dict[str, Any] = {
            "cwd": str(self.config.service_dir),
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }
        if platform.system().lower() != "windows":
            popen_kwargs["start_new_session"] = True

        # SECURITY-REVIEW: subprocess without shell; binary path validated under service_dir.
        self._process = subprocess.Popen(cmd, **popen_kwargs)
        self._binary_path = binary

        if not self._wait_until_ready():
            stderr = self._read_process_stderr()
            self.stop()
            detail = f": {stderr}" if stderr else ""
            raise RuntimeError(f"nats-server failed to start on port {self.port}{detail}")

        logger.info("nats-server started on port %d (pid=%s)", self.port, self.pid)
        return binary

    def stop(self) -> None:
        process = self._process
        if process is None:
            return

        pid = process.pid
        if process.poll() is None:
            _terminate_process(process, timeout=self.config.stop_timeout)

        try:
            _, stderr = process.communicate(timeout=self.config.stop_timeout)
        except subprocess.TimeoutExpired:
            _force_kill_process(process)
            _, stderr = process.communicate()

        if process.returncode not in (0, None, -signal.SIGTERM, -signal.SIGINT):
            if platform.system().lower() == "windows" and process.returncode == 1:
                pass
            else:
                logger.warning("nats-server exited with code %s", process.returncode)
        if stderr:
            logger.debug("nats-server stderr: %s", stderr.decode("utf-8", errors="replace").strip())

        self._process = None
        self._binary_path = None

        if pid in find_pids_on_port(self.port):
            logger.warning("port %d still held by pid %s after stop, force killing", self.port, pid)
            kill_pid(pid, timeout=self.config.stop_timeout)

        logger.info("nats-server stopped")

    def __enter__(self) -> NatsServiceManager:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    def _wait_until_ready(self) -> bool:
        deadline = time.monotonic() + self.config.startup_timeout
        while time.monotonic() < deadline:
            process = self._process
            if process is None or process.poll() is not None:
                return False

            listener_pids = find_pids_on_port(self.port)
            if process.pid in listener_pids:
                return True
            time.sleep(0.1)

        process = self._process
        if process is None or process.poll() is not None:
            return False
        listener_pids = find_pids_on_port(self.port)
        return process.pid in listener_pids

    def _read_process_stderr(self) -> str:
        process = self._process
        if process is None or process.stderr is None:
            return ""
        try:
            return process.stderr.read().decode("utf-8", errors="replace").strip()
        except Exception:
            return ""

    @staticmethod
    def _ensure_executable(binary: Path) -> None:
        if platform.system().lower() == "windows":
            return
        if not os.access(binary, os.X_OK):
            binary.chmod(binary.stat().st_mode | 0o111)


def find_pids_on_port(port: int) -> list[int]:
    system = platform.system().lower()
    if system in {"darwin", "linux"}:
        return _find_pids_on_port_unix(port)
    if system == "windows":
        return _find_pids_on_port_windows(port)
    return []


def kill_processes_on_port(port: int, timeout: float = 5.0) -> list[int]:
    killed: list[int] = []
    for pid in find_pids_on_port(port):
        if kill_pid(pid, timeout=timeout):
            killed.append(pid)
    return killed


def kill_pid(pid: int, timeout: float = 5.0) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False

    if platform.system().lower() == "windows":
        result = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            check=False,
        )
        return result.returncode == 0

    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except ProcessLookupError:
        return True
    except PermissionError:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        os.kill(pid, signal.SIGTERM)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except OSError:
            return True

    try:
        os.killpg(os.getpgid(pid), signal.SIGKILL)
    except (ProcessLookupError, OSError):
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            return True
    return True


def detect_runtime() -> RuntimeInfo:
    system = platform.system().lower()
    machine = platform.machine().lower()

    platform_key = _PLATFORM_ALIASES.get(system)
    if platform_key is None:
        raise RuntimeError(f"unsupported operating system: {system}")

    arch_key = _ARCH_ALIASES.get(machine)
    if arch_key is None:
        raise RuntimeError(f"unsupported cpu architecture: {machine}")

    return RuntimeInfo(
        system=system,
        machine=machine,
        platform_key=platform_key,
        arch_key=arch_key,
    )


def resolve_nats_binary(runtime: RuntimeInfo, service_dir: Path | None = None) -> Path:
    root = (service_dir or DEFAULT_SERVICE_DIR).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"service directory not found: {root}")

    candidates = _match_nats_binaries(root, runtime)
    if not candidates:
        available = ", ".join(sorted(p.name for p in root.glob("nats-server*"))) or "(none)"
        raise FileNotFoundError(
            f"no nats-server binary for {runtime.label} in {root}; available: {available}"
        )

    if len(candidates) > 1:
        logger.warning(
            "multiple nats-server binaries matched %s, using %s",
            runtime.label,
            candidates[0].name,
        )

    binary = candidates[0].resolve()
    _validate_binary_path(binary, root)
    return binary


def _terminate_process(process: subprocess.Popen[bytes], timeout: float) -> None:
    if platform.system().lower() == "windows":
        process.terminate()
        return

    try:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except (ProcessLookupError, OSError):
        process.send_signal(signal.SIGTERM)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.1)

    _force_kill_process(process)


def _force_kill_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return

    if platform.system().lower() == "windows":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            check=False,
        )
        return

    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except (ProcessLookupError, OSError):
        process.kill()


def _find_pids_on_port_unix(port: int) -> list[int]:
    result = subprocess.run(
        ["lsof", "-ti", f":{port}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []

    pids: list[int] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pids.append(int(line))
        except ValueError:
            continue
    return sorted(set(pids))


def _find_pids_on_port_windows(port: int) -> list[int]:
    result = subprocess.run(
        ["netstat", "-ano"],
        capture_output=True,
        text=True,
        check=False,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        return []

    pids: list[int] = []
    suffix = f":{port}"
    for line in result.stdout.splitlines():
        if "LISTENING" not in line or suffix not in line:
            continue
        parts = line.split()
        if not parts:
            continue
        try:
            pids.append(int(parts[-1]))
        except ValueError:
            continue
    return sorted(set(pids))


def _match_nats_binaries(service_dir: Path, runtime: RuntimeInfo) -> list[Path]:
    token = f"{runtime.platform_key}-{runtime.arch_key}".lower()
    matched: list[Path] = []

    for path in sorted(service_dir.glob("nats-server*")):
        if not path.is_file():
            continue

        name = path.name.lower()
        if token not in name:
            continue
        if runtime.platform_key == "windows" and path.suffix.lower() != ".exe":
            continue
        if runtime.platform_key == "darwin" and path.suffix.lower() == ".exe":
            continue

        matched.append(path)

    return matched


def _validate_binary_path(binary: Path, service_dir: Path) -> None:
    service_root = service_dir.resolve()
    resolved = binary.resolve()
    if service_root not in resolved.parents:
        raise ValueError(f"binary path escapes service directory: {resolved}")
    if not resolved.is_file():
        raise FileNotFoundError(f"nats-server binary not found: {resolved}")


def parse_nats_port(nats_url: str) -> int:
    parsed = urlparse(nats_url)
    if parsed.port is not None:
        return parsed.port
    return 4222
