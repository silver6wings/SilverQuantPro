"""
生产方订阅 K 线，通过 NATS 推送统一 kline 格式。
"""
import logging

from credentials import (
    NATS_AM_KLINE_SUBJECT,
    NATS_BATCH_SIZE,
    NATS_FLUSH_INTERVAL,
    NATS_MAX_QUEUE_SIZE,
    NATS_PRODUCER_URL,
)
from data.kline.amazing.period_config import resolve_amazing_kline_period
from data.kline.kline_entity import DEFAULT_KLINE_PERIOD, KlinePayload, KlinePeriodName
from data.nats.nats_producer import NatsThreadedProducer
from delegate.amazing_delegate import AmazingDelegate, AmazingKlineSubscriber

logger = logging.getLogger(__name__)

_DEFAULT_CODE_LIST = ["000001.SZ", "600000.SH"]
_DEFAULT_PUSH_STATS_LOG_STEP = 5000


class AmazingKlineNatsProducer:
    def __init__(
        self,
        nats_url: str = NATS_PRODUCER_URL,
        nats_subject: str = NATS_AM_KLINE_SUBJECT,
        batch_size: int = NATS_BATCH_SIZE,
        flush_interval: float = NATS_FLUSH_INTERVAL,
        max_queue_size: int = NATS_MAX_QUEUE_SIZE,
        push_stats_log_step: int = _DEFAULT_PUSH_STATS_LOG_STEP,
        period_name: str | KlinePeriodName = DEFAULT_KLINE_PERIOD,
    ) -> None:
        self.period_seconds, self._amazing_period = resolve_amazing_kline_period(period_name)
        self.nats_producer = NatsThreadedProducer(
            nats_url=nats_url,
            nats_subject=nats_subject,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_queue_size=max_queue_size,
        )
        self.amazing_subscriber: AmazingKlineSubscriber | None = None
        self.push_stats_log_step = push_stats_log_step
        self.pushed_count = 0
        self.dropped_count = 0
        self._push_stats_log_milestone = 0
        self.code_list: list[str] = list(_DEFAULT_CODE_LIST)
        self._running = False

    def set_period(self, period_name: str | KlinePeriodName) -> None:
        self.period_seconds, self._amazing_period = resolve_amazing_kline_period(period_name)

    def set_code_list(self, code_list: list[str]) -> None:
        self.code_list = list(code_list)

    def set_code_list_all(self) -> None:
        self.code_list = AmazingDelegate.get_hs_stock_codes()
        logger.info("loaded all codes, count=%d", len(self.code_list))

    def start(self) -> None:
        if self._running:
            return

        logger.info(
            "subscribing %d codes, period=%ds",
            len(self.code_list),
            self.period_seconds,
        )
        logger.info("publishing klines to %s", self.nats_producer.nats_subject)

        self.amazing_subscriber = AmazingKlineSubscriber(period_value=self._amazing_period)
        self.amazing_subscriber.set_period_seconds(self.period_seconds)
        self.amazing_subscriber.set_sub_code_list(self.code_list)

        self.nats_producer.start()
        self.amazing_subscriber.start_sub(callback=self.on_kline)
        self._running = True

    def stop(self) -> None:
        if not self._running:
            return

        subscriber = self.amazing_subscriber
        if subscriber is not None:
            subscriber.stop_sub()
        self.amazing_subscriber = None
        self.nats_producer.close()
        self._running = False
        self._log_push_stats(final=True)

    def run(self) -> None:
        self.start()
        try:
            if self.amazing_subscriber is not None:
                self.amazing_subscriber.wait()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def on_kline(self, payload: KlinePayload) -> None:
        logger.debug(payload)
        if self.nats_producer.push(payload):
            self.pushed_count += 1
        else:
            self.dropped_count += 1

        self._log_push_stats()

    def _log_push_stats(self, *, final: bool = False) -> None:
        total_count = self.pushed_count + self.dropped_count
        if not total_count:
            return

        milestone = total_count // self.push_stats_log_step
        if final:
            if total_count > self._push_stats_log_milestone * self.push_stats_log_step:
                logger.info("nats pushed=%d, dropped=%d", self.pushed_count, self.dropped_count)
            return

        if milestone > self._push_stats_log_milestone:
            self._push_stats_log_milestone = milestone
            logger.info("nats pushed=%d, dropped=%d", self.pushed_count, self.dropped_count)
