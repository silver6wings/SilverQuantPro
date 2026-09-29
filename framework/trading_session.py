"""A 股交易时段窗口：DEFAULT 供 tick；AM_KLINE 供 Amazing K 线 producer 与 NATS consumer。"""

from __future__ import annotations

from datetime import datetime

from data.tick.tick_quote import parse_hms

TradingSessionWindows = list[list[str]] | tuple[tuple[str, str], ...] | None

DEFAULT_TRADING_SESSION_WINDOWS: tuple[tuple[str, str], ...] = (
    ("09:14:30", "11:30:30"),
    ("12:59:30", "15:00:30"),
)

# Amazing K 线：producer 推送与 NATS consumer（lc5 writer 等）共用；左闭右开 [start, stop)
# 下午 stop 延后，等待 5 分钟收盘 bar 尾包（实测部分代码 ~15:06+ 才到）
AM_KLINE_TRADING_SESSION_WINDOWS: tuple[tuple[str, str], ...] = (
    ("09:14:30", "11:30:30"),
    ("12:59:30", "15:45:30"),
)


def _seconds(text: str) -> int:
    hour, minute, second = parse_hms(text)
    return hour * 3600 + minute * 60 + second


def has_trading_session_windows(windows: TradingSessionWindows) -> bool:
    return bool(windows)


def in_trading_session(now: datetime, windows: TradingSessionWindows = None) -> bool:
    current = now.hour * 3600 + now.minute * 60 + now.second
    active = windows if windows is not None else DEFAULT_TRADING_SESSION_WINDOWS
    return any(_seconds(start) <= current < _seconds(stop) for start, stop in active)
