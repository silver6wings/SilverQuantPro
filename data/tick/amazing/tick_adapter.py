"""AmazingData snapshot → 统一 tick quote 格式。"""

import AmazingData as ad

from data.tick.tick_quote import TickPayload, build_tick_quote

Snapshot = (
    ad.constant.Snapshot
    | ad.constant.SnapshotIndex
    | ad.constant.SnapshotFuture
    | ad.constant.SnapshotOption
    | ad.constant.SnapshotHKT
)

_LOT_SIZE = 100

_Snapshot = ad.constant.Snapshot
_SnapshotIndex = ad.constant.SnapshotIndex
_SnapshotFuture = ad.constant.SnapshotFuture
_SnapshotOption = ad.constant.SnapshotOption
_SnapshotHKT = ad.constant.SnapshotHKT


def snapshot_to_tick_payload(data: Snapshot) -> TickPayload:
    """Convert AmazingData snapshot to {code: tick_quote}."""
    if isinstance(data, _Snapshot):
        return _snapshot_to_tick_payload(data)
    if isinstance(data, _SnapshotIndex):
        return _index_to_tick_payload(data)
    if isinstance(data, _SnapshotFuture):
        return _future_to_tick_payload(data)
    if isinstance(data, _SnapshotOption):
        return _option_to_tick_payload(data)
    if isinstance(data, _SnapshotHKT):
        return _hkt_to_tick_payload(data)
    raise TypeError(f"unsupported snapshot type: {type(data).__name__}")


def _vol_lots(volume: int | float | None) -> int:
    """A 股成交量/盘口量：股 → 手。"""
    return int(volume or 0) // _LOT_SIZE


def _vol_raw(volume: int | float | None) -> int:
    """指数/期货/期权/港股通：已是业务单位，直接取整。"""
    return int(volume or 0)


def _snapshot_to_tick_payload(data: _Snapshot) -> TickPayload:
    return {
        data.code: build_tick_quote(
            timestamp=int(data.trade_time.timestamp() * 1000),
            last_close=data.pre_close,
            open=data.open,
            high=data.high,
            low=data.low,
            last_price=data.last,
            volume=_vol_lots(data.volume),
            amount=data.amount,
            ask_price1=data.ask_price1,
            ask_price2=data.ask_price2,
            ask_price3=data.ask_price3,
            ask_price4=data.ask_price4,
            ask_price5=data.ask_price5,
            bid_price1=data.bid_price1,
            bid_price2=data.bid_price2,
            bid_price3=data.bid_price3,
            bid_price4=data.bid_price4,
            bid_price5=data.bid_price5,
            ask_vol1=_vol_lots(data.ask_volume1),
            ask_vol2=_vol_lots(data.ask_volume2),
            ask_vol3=_vol_lots(data.ask_volume3),
            ask_vol4=_vol_lots(data.ask_volume4),
            ask_vol5=_vol_lots(data.ask_volume5),
            bid_vol1=_vol_lots(data.bid_volume1),
            bid_vol2=_vol_lots(data.bid_volume2),
            bid_vol3=_vol_lots(data.bid_volume3),
            bid_vol4=_vol_lots(data.bid_volume4),
            bid_vol5=_vol_lots(data.bid_volume5),
        )
    }


def _index_to_tick_payload(data: _SnapshotIndex) -> TickPayload:
    return {
        data.code: build_tick_quote(
            timestamp=int(data.trade_time.timestamp() * 1000),
            last_close=data.pre_close,
            open=data.open,
            high=data.high,
            low=data.low,
            last_price=data.last,
            volume=_vol_raw(data.volume),
            amount=data.amount,
        )
    }


def _deriv_to_tick_payload(
    data: _SnapshotFuture | _SnapshotOption | _SnapshotHKT,
    *,
    open: float,
) -> TickPayload:
    return {
        data.code: build_tick_quote(
            timestamp=int(data.trade_time.timestamp() * 1000),
            last_close=data.pre_close,
            open=open,
            high=data.high,
            low=data.low,
            last_price=data.last,
            volume=_vol_raw(data.volume),
            amount=data.amount,
            ask_price1=data.ask_price1,
            ask_price2=data.ask_price2,
            ask_price3=data.ask_price3,
            ask_price4=data.ask_price4,
            ask_price5=data.ask_price5,
            bid_price1=data.bid_price1,
            bid_price2=data.bid_price2,
            bid_price3=data.bid_price3,
            bid_price4=data.bid_price4,
            bid_price5=data.bid_price5,
            ask_vol1=_vol_raw(data.ask_volume1),
            ask_vol2=_vol_raw(data.ask_volume2),
            ask_vol3=_vol_raw(data.ask_volume3),
            ask_vol4=_vol_raw(data.ask_volume4),
            ask_vol5=_vol_raw(data.ask_volume5),
            bid_vol1=_vol_raw(data.bid_volume1),
            bid_vol2=_vol_raw(data.bid_volume2),
            bid_vol3=_vol_raw(data.bid_volume3),
            bid_vol4=_vol_raw(data.bid_volume4),
            bid_vol5=_vol_raw(data.bid_volume5),
        )
    }


def _future_to_tick_payload(data: _SnapshotFuture) -> TickPayload:
    return _deriv_to_tick_payload(data, open=data.open)


def _option_to_tick_payload(data: _SnapshotOption) -> TickPayload:
    return _deriv_to_tick_payload(data, open=data.open)


def _hkt_to_tick_payload(data: _SnapshotHKT) -> TickPayload:
    return _deriv_to_tick_payload(data, open=0.0)
