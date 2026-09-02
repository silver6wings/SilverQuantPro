"""
Amazing 沪深 ETF 流动性筛选：拉取近 N 个交易日日线，按成交额排序并输出 CSV。

前置条件
--------
- 本机 .venv 已安装 AmazingData / tgw（见 _wheel/）
- 已配置 credentials.py

用法
----
    PYTHONPATH=. .venv/Scripts/python demo/demo_amazing_etf_liquidity.py
    PYTHONPATH=. .venv/Scripts/python demo/demo_amazing_etf_liquidity.py --days 10 --min-avg-amount 50000000
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import AmazingData as ad
import pandas as pd

from tools.utils_remote_am import AmazingSecurityType, am_login, am_logout, get_am_data

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_OUTPUT = _PROJECT_ROOT / "_cache" / "etf_liquidity.csv"
_DEFAULT_BATCH_SIZE = 80
_DEFAULT_DAYS = 5
_DEFAULT_MIN_AVG_AMOUNT = 30_000_000.0


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
        force=True,
    )


def _parse_yyyymmdd(value: int | str) -> date:
    text = str(value)
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _date_to_int(day: date) -> int:
    return int(day.strftime("%Y%m%d"))


def _recent_trade_dates(calendar: list[int | str], days: int, end_day: date | None = None) -> list[int]:
    """取最近 days 个交易日（含 end_day 当日若为交易日）。"""
    end_key = _date_to_int(end_day or date.today())
    trade_days = sorted(int(day) for day in calendar if int(day) <= end_key)
    if not trade_days:
        raise RuntimeError("calendar is empty")
    if days <= 0:
        raise ValueError("days must be positive")
    return trade_days[-days:]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


_DAY_PERIOD = ad.constant.Period.day.value


def _normalize_kline_result(result: object) -> dict[str, pd.DataFrame]:
    """兼容 AmazingData 不同版本的 query_kline 返回结构。"""
    if result is None:
        return {}

    payload = result
    if isinstance(result, tuple):
        if len(result) == 2:
            payload, err = result
            if err:
                logging.getLogger(__name__).warning("query_kline err=%s", err)
        else:
            payload = result[0]

    if not isinstance(payload, dict) or not payload:
        return {}

    first_value = next(iter(payload.values()))
    if isinstance(first_value, pd.DataFrame):
        return {
            str(code): frame
            for code, frame in payload.items()
            if isinstance(frame, pd.DataFrame) and not frame.empty
        }

    merged: dict[str, list[pd.DataFrame]] = defaultdict(list)
    for code_map in payload.values():
        if not isinstance(code_map, dict):
            continue
        for code, frame in code_map.items():
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                merged[str(code)].append(frame)

    out: dict[str, pd.DataFrame] = {}
    for code, frames in merged.items():
        df = pd.concat(frames, ignore_index=True)
        if "kline_time" in df.columns:
            df = df.drop_duplicates(subset=["kline_time"]).sort_values("kline_time")
        out[code] = df.reset_index(drop=True)
    return out


def _merge_batch_frames(
    merged: dict[str, pd.DataFrame],
    batch_frames: dict[str, pd.DataFrame],
) -> None:
    for code, frame in batch_frames.items():
        if code in merged:
            combined = pd.concat([merged[code], frame], ignore_index=True)
            if "kline_time" in combined.columns:
                combined = combined.drop_duplicates(subset=["kline_time"]).sort_values("kline_time")
            merged[code] = combined.reset_index(drop=True)
        else:
            merged[code] = frame.reset_index(drop=True)


def _fetch_daily_klines(
    market_data: ad.MarketData,
    codes: list[str],
    begin_date: int,
    end_date: int,
    batch_size: int,
) -> dict[str, pd.DataFrame]:
    logger = logging.getLogger(__name__)
    merged: dict[str, pd.DataFrame] = {}
    batches = _chunks(codes, batch_size)
    for index, batch in enumerate(batches, start=1):
        t0 = time.perf_counter()
        result = market_data.query_kline(
            code_list=batch,
            begin_date=begin_date,
            end_date=end_date,
            period=_DAY_PERIOD,
        )
        elapsed = time.perf_counter() - t0
        batch_frames = _normalize_kline_result(result)
        if not batch_frames:
            logger.warning("batch %d/%d empty result", index, len(batches))
            continue
        _merge_batch_frames(merged, batch_frames)
        logger.info(
            "batch %d/%d codes=%d got=%d elapsed=%.1fs",
            index,
            len(batches),
            len(batch),
            len(batch_frames),
            elapsed,
        )
    return merged


def _load_code_names(base_data: ad.BaseData) -> dict[str, str]:
    try:
        info = base_data.get_code_info(security_type=AmazingSecurityType.HS_ETF)
    except Exception as exc:
        logging.getLogger(__name__).warning("get_code_info failed: %s", exc)
        return {}

    if not isinstance(info, pd.DataFrame) or info.empty:
        return {}

    names: dict[str, str] = {}
    if "symbol" in info.columns:
        for code, row in info.iterrows():
            code_text = str(code).strip()
            if code_text:
                names[code_text] = str(row["symbol"]).strip()
        return names

    code_col = next((col for col in ("code", "security_code", "i_code") if col in info.columns), None)
    name_col = next(
        (col for col in ("security_name", "name", "sec_name", "symbol") if col in info.columns),
        None,
    )
    if code_col is None or name_col is None:
        return {}

    for _, row in info.iterrows():
        code = str(row[code_col]).strip()
        name = str(row[name_col]).strip()
        if code:
            names[code] = name
    return names


def _summarize_liquidity(
    klines: dict[str, pd.DataFrame],
    code_names: dict[str, str],
    min_avg_amount: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for code, df in klines.items():
        if df.empty or "amount" not in df.columns:
            continue

        amounts = pd.to_numeric(df["amount"], errors="coerce").dropna()
        if amounts.empty:
            continue

        volumes = (
            pd.to_numeric(df["volume"], errors="coerce").dropna()
            if "volume" in df.columns
            else pd.Series(dtype=float)
        )
        closes = (
            pd.to_numeric(df["close"], errors="coerce").dropna()
            if "close" in df.columns
            else pd.Series(dtype=float)
        )

        avg_amount = float(amounts.mean())
        if avg_amount < min_avg_amount:
            continue

        rows.append(
            {
                "code": code,
                "name": code_names.get(code, ""),
                "bar_count": int(len(amounts)),
                "avg_volume": float(volumes.mean()) if not volumes.empty else 0.0,
                "avg_amount": avg_amount,
                "total_amount": float(amounts.sum()),
                "latest_amount": float(amounts.iloc[-1]),
                "latest_volume": float(volumes.iloc[-1]) if not volumes.empty else 0.0,
                "latest_close": float(closes.iloc[-1]) if not closes.empty else 0.0,
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "code",
                "name",
                "bar_count",
                "avg_volume",
                "avg_amount",
                "total_amount",
                "latest_amount",
                "latest_volume",
                "latest_close",
            ]
        )

    out = pd.DataFrame(rows).sort_values(["avg_amount", "total_amount"], ascending=False)
    out.insert(0, "rank", range(1, len(out) + 1))
    return out.reset_index(drop=True)


def run_screen(
    *,
    days: int,
    min_avg_amount: float,
    batch_size: int,
    end_day: date | None,
    output: Path,
) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    am_login()
    try:
        base_data = get_am_data()
        calendar = base_data.get_calendar(data_type="str", market="SH", date=_date_to_int(end_day or date.today()))
        trade_dates = _recent_trade_dates(calendar, days, end_day=end_day)
        begin_date = trade_dates[0]
        end_date = trade_dates[-1]
        logger.info(
            "screen window: %s ~ %s (%d trading days)",
            begin_date,
            end_date,
            len(trade_dates),
        )

        codes = base_data.get_code_list(security_type=AmazingSecurityType.HS_ETF)
        logger.info("loaded %d ETF codes", len(codes))

        code_names = _load_code_names(base_data)
        market_data = ad.MarketData(calendar)
        klines = _fetch_daily_klines(
            market_data,
            codes,
            begin_date=begin_date,
            end_date=end_date,
            batch_size=batch_size,
        )
        logger.info("received kline for %d / %d codes", len(klines), len(codes))

        ranked = _summarize_liquidity(klines, code_names, min_avg_amount=min_avg_amount)
        output.parent.mkdir(parents=True, exist_ok=True)
        ranked.to_csv(output, index=False, encoding="utf-8-sig")
        logger.info("saved %d rows to %s", len(ranked), output)
        return ranked
    finally:
        am_logout()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Screen HS ETF liquidity by average daily amount (Amazing).")
    parser.add_argument("--days", type=int, default=_DEFAULT_DAYS, help="recent trading days to average")
    parser.add_argument(
        "--min-avg-amount",
        type=float,
        default=_DEFAULT_MIN_AVG_AMOUNT,
        help="minimum average daily amount (CNY) to include",
    )
    parser.add_argument("--batch-size", type=int, default=_DEFAULT_BATCH_SIZE, help="codes per query_kline request")
    parser.add_argument(
        "--end-date",
        type=str,
        default="",
        help="window end date YYYYMMDD, default today",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help=f"output CSV path (default: {_DEFAULT_OUTPUT})",
    )
    parser.add_argument("--top", type=int, default=30, help="print top N rows")
    parser.add_argument("--verbose", action="store_true", help="debug logging")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    setup_logging(args.verbose)

    end_day = _parse_yyyymmdd(args.end_date) if args.end_date else None
    ranked = run_screen(
        days=args.days,
        min_avg_amount=args.min_avg_amount,
        batch_size=args.batch_size,
        end_day=end_day,
        output=args.output,
    )

    if ranked.empty:
        print("no ETF matched filters")
        return 0

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(ranked.head(args.top).to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
