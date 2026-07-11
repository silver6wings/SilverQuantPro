"""QMT xtdata quote → 统一 tick quote 格式。

xtdata subscribe_whole_quote 回调的五档字段固定为 list，本 adapter 只处理该格式。
"""

from typing import Any

from data.tick.tick_quote import TickPayload, TickQuoteDict, build_tick_quote

_LEVEL_COUNT = 5
_ZERO_FLOATS = (0.0, 0.0, 0.0, 0.0, 0.0)
_ZERO_INTS = (0, 0, 0, 0, 0)


def quote_to_tick_payload(raw: dict[str, Any]) -> TickQuoteDict | None:
    """将单条 QMT xtdata quote 归一化为 tick quote。"""
    timestamp = raw.get("time")
    if timestamp is None:
        return None

    ask_p = _levels_float(raw.get("askPrice"))
    bid_p = _levels_float(raw.get("bidPrice"))
    ask_v = _levels_int(raw.get("askVol"))
    bid_v = _levels_int(raw.get("bidVol"))

    return build_tick_quote(
        timestamp=int(timestamp),
        last_close=_scalar_float(raw.get("lastClose")),
        open=_scalar_float(raw.get("open")),
        high=_scalar_float(raw.get("high")),
        low=_scalar_float(raw.get("low")),
        last_price=_scalar_float(raw.get("lastPrice")),
        volume=_scalar_int(raw.get("volume")),
        amount=_scalar_float(raw.get("amount")),
        ask_price1=ask_p[0],
        ask_price2=ask_p[1],
        ask_price3=ask_p[2],
        ask_price4=ask_p[3],
        ask_price5=ask_p[4],
        bid_price1=bid_p[0],
        bid_price2=bid_p[1],
        bid_price3=bid_p[2],
        bid_price4=bid_p[3],
        bid_price5=bid_p[4],
        ask_vol1=ask_v[0],
        ask_vol2=ask_v[1],
        ask_vol3=ask_v[2],
        ask_vol4=ask_v[3],
        ask_vol5=ask_v[4],
        bid_vol1=bid_v[0],
        bid_vol2=bid_v[1],
        bid_vol3=bid_v[2],
        bid_vol4=bid_v[3],
        bid_vol5=bid_v[4],
    )


def quotes_to_tick_payload(quotes: dict[str, Any]) -> TickPayload | None:
    """批量归一化 {code: qmt_quote}；无效 code/quote 会被跳过。"""
    if not quotes:
        return None

    normalized: TickPayload = {}
    for code, quote in quotes.items():
        if not isinstance(code, str) or not isinstance(quote, dict):
            continue
        tick = quote_to_tick_payload(quote)
        if tick is not None:
            normalized[code] = tick

    return normalized or None


def _scalar_float(value: Any) -> float:
    return 0.0 if value is None else value


def _scalar_int(value: Any) -> int:
    return 0 if value is None else value


def _levels_float(items: Any) -> tuple[float, float, float, float, float]:
    if not isinstance(items, list):
        return _ZERO_FLOATS
    size = len(items)
    return tuple(0.0 if index >= size or items[index] is None else items[index] for index in range(_LEVEL_COUNT))


def _levels_int(items: Any) -> tuple[int, int, int, int, int]:
    if not isinstance(items, list):
        return _ZERO_INTS
    size = len(items)
    return tuple(0 if index >= size or items[index] is None else items[index] for index in range(_LEVEL_COUNT))
