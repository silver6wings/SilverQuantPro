"""
生产方订阅 xtquant 全推行情，归一化为统一 tick quote 后批量推送到 NATS。

xt 一次 callback 可能带回数千 quote；逐条 enqueue 会拖慢 callback 线程。
按 NATS_XT_QUOTES_PER_MESSAGE（默认 1000）拆包成多 code 的 dict 再 push。
"""
import logging
import time
from typing import Any

from xtquant import xtdata

from credentials import (
    NATS_BATCH_SIZE,
    NATS_FLUSH_INTERVAL,
    NATS_MAX_QUEUE_SIZE,
    NATS_PRODUCER_URL,
    NATS_XT_QUOTES_PER_MESSAGE,
    NATS_XT_SUBJECT,
)
from data.nats.nats_producer import NatsThreadedProducer
from data.tick.tick_quote import TickPayload
from data.tick.xtquant.tick_adapter import quote_to_tick_payload

logger = logging.getLogger(__name__)

_DEFAULT_CODE_LIST = ["000001.SZ", "600000.SH"]
_DEFAULT_PUSH_STATS_LOG_STEP = 5000


class XtquantNatsProducer:
    def __init__(
        self,
        nats_url: str = NATS_PRODUCER_URL,
        nats_subject: str = NATS_XT_SUBJECT,
        batch_size: int = NATS_BATCH_SIZE,
        flush_interval: float = NATS_FLUSH_INTERVAL,
        max_queue_size: int = NATS_MAX_QUEUE_SIZE,
        quotes_per_message: int = NATS_XT_QUOTES_PER_MESSAGE,
        push_stats_log_step: int = _DEFAULT_PUSH_STATS_LOG_STEP,
    ) -> None:
        self.nats_producer = NatsThreadedProducer(
            nats_url=nats_url,
            nats_subject=nats_subject,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_queue_size=max_queue_size,
        )
        self.quotes_per_message = quotes_per_message
        self.push_stats_log_step = push_stats_log_step
        self.sub_sequence: int | None = None
        self.pushed_count = 0
        self.dropped_count = 0
        self._push_stats_log_milestone = 0
        self.code_list: list[str] = list(_DEFAULT_CODE_LIST)
        self._running = False

    def set_code_list(self, code_list: list[str]) -> None:
        self.code_list = list(code_list)

    def start(self) -> None:
        if self._running:
            return

        logger.info("subscribing %d codes", len(self.code_list))
        logger.info(
            "publishing quotes to %s, up to %d codes per message",
            self.nats_producer.nats_subject,
            self.quotes_per_message,
        )

        xtdata.enable_hello = False
        self.nats_producer.start()
        self.sub_sequence = xtdata.subscribe_whole_quote(self.code_list, callback=self.on_quotes)
        self._running = True
        logger.info("xt subscribe_whole_quote sequence=%s", self.sub_sequence)

    def stop(self) -> None:
        if not self._running:
            return

        if self.sub_sequence is not None:
            xtdata.unsubscribe_quote(self.sub_sequence)
            self.sub_sequence = None

        self.nats_producer.close()
        self._running = False
        self._log_push_stats(final=True)

    def run(self) -> None:
        self.start()
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def on_quotes(self, quotes: dict[str, dict[str, Any]]) -> None:
        if not quotes:
            return

        batch_size = self.quotes_per_message
        batch: TickPayload = {}

        try:
            for code, raw in quotes.items():
                if not isinstance(code, str) or not isinstance(raw, dict):
                    continue

                tick = quote_to_tick_payload(raw)
                if tick is None:
                    continue

                batch[code] = tick
                if len(batch) >= batch_size:
                    self._push_batch(batch)
                    batch = {}

            if batch:
                self._push_batch(batch)

            self._log_push_stats()
        except Exception:
            logger.exception("on_quotes failed")

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

    def _push_batch(self, batch: TickPayload) -> None:
        if self.nats_producer.push(batch):
            self.pushed_count += len(batch)
        else:
            self.dropped_count += len(batch)
