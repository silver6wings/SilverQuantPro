"""
生产方订阅行情数据，通过 NATS 推送统一 tick quote 格式。
"""
import logging

from credentials import NATS_AM_SUBJECT, NATS_BATCH_SIZE, NATS_FLUSH_INTERVAL, NATS_MAX_QUEUE_SIZE, NATS_PRODUCER_URL
from data.nats.nats_producer import NatsThreadedProducer
from data.tick.tick_quote import TickPayload
from delegate.amazing_delegate import AmazingDelegate
from delegate.amazing_subscriber import AmazingSubscriber

logger = logging.getLogger(__name__)

_DEFAULT_CODE_LIST = ["000001.SZ", "600000.SH"]


class AmazingNatsProducer:
    def __init__(
        self,
        nats_url: str = NATS_PRODUCER_URL,
        nats_subject: str = NATS_AM_SUBJECT,
        batch_size: int = NATS_BATCH_SIZE,
        flush_interval: float = NATS_FLUSH_INTERVAL,
        max_queue_size: int = NATS_MAX_QUEUE_SIZE,
    ) -> None:
        self.nats_producer = NatsThreadedProducer(
            nats_url=nats_url,
            nats_subject=nats_subject,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_queue_size=max_queue_size,
        )
        self.amazing_subscriber = None
        self.pushed_count = 0
        self.dropped_count = 0
        self.code_list: list[str] = list(_DEFAULT_CODE_LIST)
        self._running = False

    def set_code_list(self, code_list: list[str]) -> None:
        self.code_list = list(code_list)

    def set_code_list_all(self) -> None:
        self.code_list = AmazingDelegate.get_hs_stock_codes()
        logger.info("loaded all codes, count=%d", len(self.code_list))

    def start(self) -> None:
        if self._running:
            return

        logger.info("subscribing %d codes", len(self.code_list))
        logger.info("publishing snapshots to %s", self.nats_producer.nats_subject)

        self.amazing_subscriber = AmazingSubscriber()
        self.amazing_subscriber.set_sub_code_list(self.code_list)

        self.nats_producer.start()
        self.amazing_subscriber.start_sub(callback=self.on_tick)
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

    def run(self) -> None:
        self.start()
        try:
            self.amazing_subscriber.wait()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def on_tick(self, payload: TickPayload) -> None:
        logger.debug(payload)
        if self.nats_producer.push(payload):
            self.pushed_count += 1
        else:
            self.dropped_count += 1

        total_count = self.pushed_count + self.dropped_count
        if total_count % 5000 == 0:
            logger.info("nats pushed=%d, dropped=%d", self.pushed_count, self.dropped_count)
