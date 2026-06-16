"""
Convert AmazingData snapshots to QMT xtdata quote format: {code: quote}.
字段定义见开发手册附录 4.2。
"""

from typing import Any

import AmazingData as ad

Quote = dict[str, Any]
Snapshot = (
    ad.constant.Snapshot
    | ad.constant.SnapshotIndex
    | ad.constant.SnapshotFuture
    | ad.constant.SnapshotOption
    | ad.constant.SnapshotHKT
)

_LOT_SIZE = 100
_EMPTY_5F: tuple[float, ...] = (0.0, 0.0, 0.0, 0.0, 0.0)
_EMPTY_5I: tuple[int, ...] = (0, 0, 0, 0, 0)

_Snapshot = ad.constant.Snapshot
_SnapshotIndex = ad.constant.SnapshotIndex
_SnapshotFuture = ad.constant.SnapshotFuture
_SnapshotOption = ad.constant.SnapshotOption
_SnapshotHKT = ad.constant.SnapshotHKT


def _trade_time_ms(trade_time: Any) -> int:
    return int(trade_time.timestamp() * 1000)


def _stock_volume_fields(volume: int | float | None) -> tuple[int, int]:
    """Snapshot 成交总量为股，QMT volume 为手、pvolume 为股。"""
    raw = int(volume or 0)
    return raw // _LOT_SIZE, raw


def _raw_volume_fields(volume: int | float | None) -> tuple[int, int]:
    """指数/期货/期权/港股通成交量已是业务单位，不再除以 100。"""
    raw = int(volume or 0)
    return raw, raw


def _pack_order_book_shares(data: _Snapshot) -> tuple[list[float], list[float], list[int], list[int]]:
    """Snapshot 盘口量为股，QMT askVol/bidVol 为手。"""
    ls = _LOT_SIZE
    return (
        [data.ask_price1, data.ask_price2, data.ask_price3, data.ask_price4, data.ask_price5],
        [data.bid_price1, data.bid_price2, data.bid_price3, data.bid_price4, data.bid_price5],
        [
            (data.ask_volume1 or 0) // ls,
            (data.ask_volume2 or 0) // ls,
            (data.ask_volume3 or 0) // ls,
            (data.ask_volume4 or 0) // ls,
            (data.ask_volume5 or 0) // ls,
        ],
        [
            (data.bid_volume1 or 0) // ls,
            (data.bid_volume2 or 0) // ls,
            (data.bid_volume3 or 0) // ls,
            (data.bid_volume4 or 0) // ls,
            (data.bid_volume5 or 0) // ls,
        ],
    )


def _pack_order_book_lots(
    data: _SnapshotFuture | _SnapshotOption | _SnapshotHKT,
) -> tuple[list[float], list[float], list[int], list[int]]:
    """期货/期权/港股通盘口量已是手或张，直接透传。"""
    return (
        [data.ask_price1, data.ask_price2, data.ask_price3, data.ask_price4, data.ask_price5],
        [data.bid_price1, data.bid_price2, data.bid_price3, data.bid_price4, data.bid_price5],
        [
            data.ask_volume1 or 0,
            data.ask_volume2 or 0,
            data.ask_volume3 or 0,
            data.ask_volume4 or 0,
            data.ask_volume5 or 0,
        ],
        [
            data.bid_volume1 or 0,
            data.bid_volume2 or 0,
            data.bid_volume3 or 0,
            data.bid_volume4 or 0,
            data.bid_volume5 or 0,
        ],
    )


def _snapshot_to_qmt_quote(data: _Snapshot) -> Quote:
    code = data.code
    vol_lots, pvolume = _stock_volume_fields(data.volume)
    ask_price, bid_price, ask_vol, bid_vol = _pack_order_book_shares(data)
    return {
        code: {
            "time": _trade_time_ms(data.trade_time),
            "lastClose": data.pre_close,
            "open": data.open,
            "high": data.high,
            "low": data.low,
            "lastPrice": data.last,
            "volume": vol_lots,
            "pvolume": pvolume,
            "amount": data.amount,
            "askPrice": ask_price,
            "bidPrice": bid_price,
            "askVol": ask_vol,
            "bidVol": bid_vol,
            "transactionNum": data.num_trades or 0,
            "stockStatus": data.trading_phase_code,
            "openInt": 0,
            "lastSettlementPrice": 0.0,
            "settlementPrice": 0.0,
            "pe": 0.0,
            "volRatio": 0.0,
            "speed1Min": 0.0,
            "speed5Min": 0.0,
        }
    }


def _index_to_qmt_quote(data: _SnapshotIndex) -> Quote:
    code = data.code
    volume, pvolume = _raw_volume_fields(data.volume)
    return {
        code: {
            "time": _trade_time_ms(data.trade_time),
            "lastClose": data.pre_close,
            "open": data.open,
            "high": data.high,
            "low": data.low,
            "lastPrice": data.last,
            "volume": volume,
            "pvolume": pvolume,
            "amount": data.amount,
            "askPrice": _EMPTY_5F,
            "bidPrice": _EMPTY_5F,
            "askVol": _EMPTY_5I,
            "bidVol": _EMPTY_5I,
            "transactionNum": 0,
            "stockStatus": 0,
            "openInt": 0,
            "lastSettlementPrice": 0.0,
            "settlementPrice": 0.0,
            "pe": 0.0,
            "volRatio": 0.0,
            "speed1Min": 0.0,
            "speed5Min": 0.0,
        }
    }


def _future_to_qmt_quote(data: _SnapshotFuture) -> Quote:
    code = data.code
    volume, pvolume = _raw_volume_fields(data.volume)
    ask_price, bid_price, ask_vol, bid_vol = _pack_order_book_lots(data)
    return {
        code: {
            "time": _trade_time_ms(data.trade_time),
            "lastClose": data.pre_close,
            "open": data.open,
            "high": data.high,
            "low": data.low,
            "lastPrice": data.last,
            "volume": volume,
            "pvolume": pvolume,
            "amount": data.amount,
            "askPrice": ask_price,
            "bidPrice": bid_price,
            "askVol": ask_vol,
            "bidVol": bid_vol,
            "transactionNum": 0,
            "stockStatus": 0,
            "openInt": data.open_interest or 0,
            "lastSettlementPrice": data.pre_settle or 0.0,
            "settlementPrice": data.settle or 0.0,
            "pe": 0.0,
            "volRatio": 0.0,
            "speed1Min": 0.0,
            "speed5Min": 0.0,
        }
    }


def _option_to_qmt_quote(data: _SnapshotOption) -> Quote:
    code = data.code
    volume, pvolume = _raw_volume_fields(data.volume)
    ask_price, bid_price, ask_vol, bid_vol = _pack_order_book_lots(data)
    return {
        code: {
            "time": _trade_time_ms(data.trade_time),
            "lastClose": data.pre_close,
            "open": data.open,
            "high": data.high,
            "low": data.low,
            "lastPrice": data.last,
            "volume": volume,
            "pvolume": pvolume,
            "amount": data.amount,
            "askPrice": ask_price,
            "bidPrice": bid_price,
            "askVol": ask_vol,
            "bidVol": bid_vol,
            "transactionNum": 0,
            "stockStatus": data.trading_phase_code,
            "openInt": data.total_long_position or 0,
            "lastSettlementPrice": data.pre_settle or 0.0,
            "settlementPrice": data.settle or 0.0,
            "pe": 0.0,
            "volRatio": 0.0,
            "speed1Min": 0.0,
            "speed5Min": 0.0,
        }
    }


def _hkt_to_qmt_quote(data: _SnapshotHKT) -> Quote:
    code = data.code
    volume, pvolume = _raw_volume_fields(data.volume)
    ask_price, bid_price, ask_vol, bid_vol = _pack_order_book_lots(data)
    return {
        code: {
            "time": _trade_time_ms(data.trade_time),
            "lastClose": data.pre_close,
            "open": 0.0,
            "high": data.high,
            "low": data.low,
            "lastPrice": data.last,
            "volume": volume,
            "pvolume": pvolume,
            "amount": data.amount,
            "askPrice": ask_price,
            "bidPrice": bid_price,
            "askVol": ask_vol,
            "bidVol": bid_vol,
            "transactionNum": 0,
            "stockStatus": data.trading_phase_code,
            "openInt": 0,
            "lastSettlementPrice": 0.0,
            "settlementPrice": 0.0,
            "pe": 0.0,
            "volRatio": 0.0,
            "speed1Min": 0.0,
            "speed5Min": 0.0,
        }
    }


def snapshot_to_qmt_quote(data: Snapshot) -> Quote:
    """Convert AmazingData snapshot to QMT xtdata quote format."""
    if isinstance(data, _Snapshot):
        return _snapshot_to_qmt_quote(data)
    if isinstance(data, _SnapshotIndex):
        return _index_to_qmt_quote(data)
    if isinstance(data, _SnapshotFuture):
        return _future_to_qmt_quote(data)
    if isinstance(data, _SnapshotOption):
        return _option_to_qmt_quote(data)
    if isinstance(data, _SnapshotHKT):
        return _hkt_to_qmt_quote(data)
    raise TypeError(f"unsupported snapshot type: {type(data).__name__}")
