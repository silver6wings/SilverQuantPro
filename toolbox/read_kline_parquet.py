"""
读取 kline_manager_demo 落盘的单个 parquet 文件。

用法：改下面 DAY / CODE，然后
    PYTHONPATH=. python toolbox/read_kline_parquet.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from framework.kline_manager import DEFAULT_KLINE_HISTORY_DIR, code_parquet_path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

DAY = 20260904
CODE = "000001.SZ"


def main() -> None:
    path = code_parquet_path(_PROJECT_ROOT / DEFAULT_KLINE_HISTORY_DIR, DAY, CODE)
    if not path.is_file():
        print(f"parquet not found: {path}")
        return

    df = pd.read_parquet(path)
    print(f"{path}: {len(df)} rows")
    if df.empty:
        return

    print(df[[
        "local", "time", "period",
        "open", "high", "low", "close", "volume", "amount",
    ]])


if __name__ == "__main__":
    from tools.utils_basic import pd_show_all

    pd_show_all()
    main()
