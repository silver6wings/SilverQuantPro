"""
Amazing K 线 NATS 生产端 demo。

用法
----
    PYTHONPATH=. python demo/demo_kline_am_producer.py

在 main 上方改 KLINE_PERIOD / CODE_LIST / SECURITY_TYPES 即可。
"""
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

from data.kline.amazing.nats_producer import AmazingKlineNatsProducer
from data.kline.kline_entity import KlinePayload, KlinePeriodName
from delegate.amazing_delegate import AmazingDelegate
from tools.utils_remote_am import AmazingSecurityType

_LOG_PATH = Path("_cache/demo_kline_producer.log")

CODE_LIST = [
    "000001.SZ",
    "300001.SZ",
    "600000.SH",
    "688001.SH",
    "920000.BJ",
    "000001.SH",
    "399001.SZ",
    "159159.SZ",
    "510510.SH",
    "123268.SZ",
    "113066.SH",
]

# CODE_LIST = None

SECURITY_TYPES: list[str] = [
    AmazingSecurityType.HS_STOCK,
]

KLINE_PERIOD = KlinePeriodName.MIN5

# 统一格式里 timestamp 为 bar 开始时刻(ms)；结束时刻 = timestamp + period(秒)
_PRINT_STATS_EVERY_N_PAYLOADS = 50


class DemoKlineProducerWithTimeStats(AmazingKlineNatsProducer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._start_time_counts: Counter[str] = Counter()
        self._end_time_counts: Counter[str] = Counter()
        self._payload_count = 0
        self._seen_end_times: set[str] = set()

    def on_kline(self, payload: KlinePayload) -> None:
        for code, bar in payload.items():
            start_ms = int(bar["timestamp"])
            period_sec = int(bar["period"])
            start_str = datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M")
            end_str = datetime.fromtimestamp((start_ms + period_sec * 1000) / 1000).strftime(
                "%Y-%m-%d %H:%M"
            )
            self._start_time_counts[start_str] += 1
            self._end_time_counts[end_str] += 1
            if end_str not in self._seen_end_times:
                self._seen_end_times.add(end_str)
                print(
                    f"[kline-time] 新结束时刻 {end_str} "
                    f"(对应开始 {start_str}, {code}, period={period_sec}s)"
                )
                if end_str.endswith(" 15:00"):
                    print(
                        f"[kline-time] *** 收盘 bar: 结束 15:00 "
                        f"(累计该结束时刻推送计数={self._end_time_counts[end_str]}) ***"
                    )

        super().on_kline(payload)

        self._payload_count += 1
        if self._payload_count % _PRINT_STATS_EVERY_N_PAYLOADS == 0:
            self._print_stats()

    def stop(self) -> None:
        self._print_stats(final=True)
        super().stop()

    def _print_stats(self, *, final: bool = False) -> None:
        tag = "最终" if final else "累计"
        print(
            f"[kline-stats {tag}] payload={self._payload_count} | "
            f"开始时刻种类={len(self._start_time_counts)} "
            f"结束时刻种类={len(self._end_time_counts)} | "
            f"说明: bar['timestamp']=开始, 结束=timestamp+period"
        )
        if self._end_time_counts:
            latest_end, latest_n = self._end_time_counts.most_common(1)[0]
            print(f"[kline-stats {tag}] 当前最多条的结束时刻: {latest_end} (共 {latest_n} 条推送计数)")


def setup_logging() -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(_LOG_PATH, encoding="utf-8"),
        ],
        force=True,
    )


def _load_code_list() -> list[str]:
    if CODE_LIST:
        return list(CODE_LIST)

    code_list: list[str] = []
    seen: set[str] = set()
    for security_type in SECURITY_TYPES:
        codes = AmazingDelegate.get_codes(security_type)
        print(f"{security_type}: {len(codes)}")
        for code in codes:
            if code not in seen:
                seen.add(code)
                code_list.append(code)
    print(f"Total codes={len(code_list)}")
    return code_list


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    code_list = _load_code_list()
    logger.info("subscribing %d codes, period=%s", len(code_list), KLINE_PERIOD)
    print(
        "[kline-time] 统计说明: NATS bar 的 timestamp 是 K 线开始时刻; "
        "结束时刻 = timestamp + period(秒); lc5 写入用的是结束时刻"
    )

    producer = DemoKlineProducerWithTimeStats(period_name=KLINE_PERIOD)
    producer.set_code_list(code_list)
    producer.run()


if __name__ == "__main__":
    main()
