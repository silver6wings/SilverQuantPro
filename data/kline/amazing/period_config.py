"""AmazingData 订阅周期与配置名映射。"""

from __future__ import annotations

from typing import Any

import AmazingData as ad

from data.kline.kline_entity import KlinePeriodName, resolve_period_seconds

# period 秒数 → AmazingData Period 枚举成员名（见 SDK constant.Period）
_PERIOD_SECONDS_TO_AMAZING_ATTR: dict[int, str] = {
    60: "min1",
    180: "min3",
    300: "min5",
    600: "min10",
    900: "min15",
    1800: "min30",
    3600: "min60",
    7200: "min120",
}


def amazing_period_value(period_seconds: int) -> Any:
    attr = _PERIOD_SECONDS_TO_AMAZING_ATTR.get(period_seconds)
    if attr is None:
        raise ValueError(f"no AmazingData period mapping for {period_seconds}s")
    period_enum = ad.constant.Period
    if not hasattr(period_enum, attr):
        raise ValueError(
            f"AmazingData SDK has no Period.{attr} (seconds={period_seconds}); upgrade SDK or adjust mapping"
        )
    return getattr(period_enum, attr).value


def resolve_amazing_kline_period(period_name: str | KlinePeriodName) -> tuple[int, Any]:
    """配置名 → (period_seconds, ad.constant.Period.*.value)。"""
    seconds = resolve_period_seconds(period_name)
    return seconds, amazing_period_value(seconds)
