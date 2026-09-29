"""与时间相关的通用工具（与 tick / kline 业务无关）。"""

from __future__ import annotations

from datetime import datetime


def now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)
