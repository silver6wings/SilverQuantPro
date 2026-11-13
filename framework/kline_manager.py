"""K 线数据总入口：parquet 读写、保留策略、自动落盘。"""
from __future__ import annotations

import logging
import shutil
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from data.kline.kline_store import (
    copy_kline_rows,
    parquet_dataframe_to_rows,
    rows_to_parquet_dataframe,
)
from data.tick.tick_quote import clock_matches

logger = logging.getLogger(__name__)

# 晚于 AM_KLINE 下午窗口 15:45:30，等收盘 bar 尾包到齐后再落盘
SAVE_KLINE_HISTORY_AT = "15:50"
DEFAULT_KLINE_HISTORY_DIR = "_cache/kline_history"  # parquet 根目录：{dir}/{day}/{code}.parquet
DEFAULT_KLINE_HISTORY_RETENTION_DAYS = 30


def day_int(value: date | datetime) -> int:
    if isinstance(value, datetime):
        value = value.date()
    return value.year * 10000 + value.month * 100 + value.day


def code_parquet_path(root: Path, day: int, code: str) -> Path:
    return root / str(day) / f"{code}.parquet"


def save_code_klines(root: Path, day: int, code: str, rows: list[list[Any]]) -> bool:
    if not rows:
        return False
    path = code_parquet_path(root, day, code)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        rows_to_parquet_dataframe(rows).to_parquet(path, index=False)
    except Exception:
        logger.exception("failed to save kline history day=%s code=%s", day, code)
        return False
    return True


def load_code_klines(root: Path, day: int, code: str) -> list[list[Any]] | None:
    path = code_parquet_path(root, day, code)
    if not path.is_file():
        return None
    try:
        frame = pd.read_parquet(path)
    except Exception:
        logger.exception("failed to load kline history day=%s code=%s", day, code)
        return None
    return parquet_dataframe_to_rows(frame, day)


def prune_old_days(root: Path, retention_days: int, today: date) -> int:
    if retention_days <= 0 or not root.is_dir():
        return 0
    cutoff = day_int(today - timedelta(days=retention_days))
    removed = 0
    for entry in root.iterdir():
        if not entry.is_dir() or not entry.name.isdigit():
            continue
        if int(entry.name) < cutoff:
            shutil.rmtree(entry)
            removed += 1
            logger.info("removed old kline history dir %s", entry)
    return removed


@dataclass
class KlineManager:
    """K 线数据总入口：历史查询、按日落盘、过期清理。"""

    root: str = DEFAULT_KLINE_HISTORY_DIR
    retention_days: int = DEFAULT_KLINE_HISTORY_RETENTION_DAYS
    save_at: str = SAVE_KLINE_HISTORY_AT

    _cache: dict[tuple[int, str], list[list[Any]]] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _save_fired: date | None = field(default=None, init=False, repr=False)

    @property
    def root_path(self) -> Path:
        return Path(self.root)

    def history_klines(self, day: int, code: str) -> list[list[Any]]:
        """懒加载历史 K 线（仅读 parquet，不读 today 内存）。"""
        if day > day_int(date.today()):
            return []

        cache_key = (day, code)
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return copy_kline_rows(cached)

        rows = load_code_klines(self.root_path, day, code)
        if rows is None:
            return []

        with self._lock:
            self._cache[cache_key] = rows
        return copy_kline_rows(rows)

    def save_day(
        self,
        save_date: date,
        snapshot: dict[str, list[list[Any]]],
        *,
        log: Callable[[str], None] | None = None,
    ) -> int:
        """将 snapshot 写入 parquet 并清理过期目录；返回成功写入的 code 数。"""
        day = day_int(save_date)
        root = self.root_path
        filtered = {code: rows for code, rows in snapshot.items() if rows}

        if not filtered:
            message = f"kline history save skipped (no data) day={day}"
            if log is not None:
                log(message)
            else:
                logger.info(message)
            saved = 0
        else:
            saved = sum(1 for code, rows in filtered.items() if save_code_klines(root, day, code, rows))
            message = f"kline history saved day={day} codes={saved}"
            if log is not None:
                log(message)
            else:
                logger.info(message)

        removed = prune_old_days(root, self.retention_days, save_date)
        if removed:
            message = f"kline history pruned {removed} day dir(s)"
            if log is not None:
                log(message)
            else:
                logger.info(message)
        return saved

    def maybe_auto_save(
        self,
        now: datetime,
        today: date,
        snapshot: dict[str, list[list[Any]]],
        *,
        log: Callable[[str], None] | None = None,
    ) -> None:
        """到达 save_at 时刻后自动落盘一次（同一自然日只触发一次）。"""
        if not clock_matches(self.save_at, now):
            return
        if self._save_fired == today:
            return
        self._save_fired = today
        self.save_day(today, snapshot, log=log)
