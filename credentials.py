from pathlib import Path
import os


def _load_dotenv() -> None:
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")

        if key and key not in os.environ:
            os.environ[key] = value


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value

    raise RuntimeError(f"Missing required environment variable: {name}")


_load_dotenv()

AMAZING_USERNAME = _required_env("AMAZING_USERNAME")
AMAZING_PASSWORD = _required_env("AMAZING_PASSWORD")
AMAZING_HOST = _required_env("AMAZING_HOST")
AMAZING_PORT = int(os.getenv("AMAZING_PORT", "8600"))
