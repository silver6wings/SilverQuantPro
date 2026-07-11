"""
启动 / 停止本地 nats-server（自动按系统与芯片选择 _service 下的二进制）。

用法
----
    PYTHONPATH=. python data/job_nats_service.py
    PYTHONPATH=. python data/job_nats_service.py kill
    PYTHONPATH=. python data/job_nats_service.py kill --port 4222
"""
import argparse
import logging
import time

from data.nats.nats_service import (
    DEFAULT_NATS_URL,
    NatsService,
    detect_runtime,
    find_pids_on_port,
    kill_processes_on_port,
    parse_nats_port,
    resolve_nats_binary,
)


def run_start() -> None:
    runtime = detect_runtime()
    binary = resolve_nats_binary(runtime)
    print(f"runtime={runtime.label}")
    print(f"binary={binary}")

    try:
        with NatsService() as service:
            print(f"nats-server running on port {service.port} (pid={service.pid}), press Ctrl+C to stop")
            try:
                while service.is_active():
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
    except RuntimeError as exc:
        port = parse_nats_port(DEFAULT_NATS_URL)
        raise SystemExit(
            f"{exc}\nrun `python data/job_nats_service.py kill --port {port}` first"
        ) from exc


def run_kill(port: int) -> None:
    pids = find_pids_on_port(port)
    if not pids:
        print(f"no process listening on port {port}")
        return

    print(f"killing pid(s) on port {port}: {pids}")
    killed = kill_processes_on_port(port)
    remaining = find_pids_on_port(port)
    if remaining:
        raise SystemExit(f"failed to release port {port}, remaining pid(s): {remaining}")
    print(f"port {port} released, killed pid(s): {killed}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Manage bundled nats-server process")
    subparsers = parser.add_subparsers(dest="command")

    kill_parser = subparsers.add_parser("kill", help="kill process listening on NATS port")
    kill_parser.add_argument("--port", type=int, default=parse_nats_port(DEFAULT_NATS_URL))

    args = parser.parse_args()
    if args.command == "kill":
        run_kill(args.port)
    else:
        run_start()


if __name__ == "__main__":
    main()
