"""
读取 tick_manager_demo 落盘的单个 parquet 文件。

用法：改下面 DAY / CODE，然后
    PYTHONPATH=. python toolbox/read_parquet.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from framework.tick_manager import DEFAULT_TICK_HISTORY_DIR, code_parquet_path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

DAY = 20260818
CODE = "000001.SZ"


def main() -> None:
    path = code_parquet_path(_PROJECT_ROOT / DEFAULT_TICK_HISTORY_DIR, DAY, CODE)
    if not path.is_file():
        print(f"parquet not found: {path}")
        return

    df = pd.read_parquet(path)
    print(f"{path}: {len(df)} rows")
    if df.empty:
        return

    print(df[[
        'local', 'time',
        'lastPrice', 'high', 'low', 'lastClose', 'volume', 'amount',
        'askPrice1', 'askVol1', 'bidPrice1', 'bidVol1',
        'askPrice2', 'askVol2', 'bidPrice2', 'bidVol2',
    ]])

    # df = df[df['time'] > '09:30:00']
    # print(df.head(30))


if __name__ == "__main__":
    from tools.utils_basic import pd_show_all

    pd_show_all()
    main()
