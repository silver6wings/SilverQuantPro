"""
生产方订阅行情数据，通过 NATS 推送 QMT quotes 格式。
"""
import logging
import os

from data.nats.nats_producer import NatsThreadedProducer
from delegate.amazing_delegate import AmazingDelegate, Quote

logger = logging.getLogger(__name__)

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "market.tick.amazing")

NATS_BATCH_SIZE = int(os.getenv("NATS_BATCH_SIZE", "1000"))
NATS_FLUSH_INTERVAL = float(os.getenv("NATS_FLUSH_INTERVAL", "0.02"))
NATS_MAX_QUEUE_SIZE = int(os.getenv("NATS_MAX_QUEUE_SIZE", "100000"))


_DEFAULT_CODE_LIST = ["000001.SZ"]


class AmazingNatsProducer:
    def __init__(
        self,
        nats_url: str = NATS_URL,
        subject: str = NATS_SUBJECT,
        batch_size: int = NATS_BATCH_SIZE,
        flush_interval: float = NATS_FLUSH_INTERVAL,
        max_queue_size: int = NATS_MAX_QUEUE_SIZE,
    ) -> None:
        self.nats_producer = NatsThreadedProducer(
            nats_url=nats_url,
            subject=subject,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_queue_size=max_queue_size,
        )
        self.amazing_delegate = None
        self.pushed_count = 0
        self.dropped_count = 0
        self.code_list: list[str] = list(_DEFAULT_CODE_LIST)
        self._running = False

    def set_code_list(self, code_list: list[str]) -> None:
        self.code_list = list(code_list)

    def set_code_list_all(self) -> None:
        self.code_list = AmazingDelegate.get_all_stock_codes()
        logger.info("loaded all codes, count=%d", len(self.code_list))

    def start(self) -> None:
        if self._running:
            return

        logger.info("subscribing %d codes", len(self.code_list))
        logger.info("publishing snapshots to %s", self.nats_producer.subject)

        self.amazing_delegate = AmazingDelegate()
        self.amazing_delegate.set_code_list(self.code_list)

        self.nats_producer.start()
        self.amazing_delegate.start_sub(callback=self.on_tick)
        self._running = True

    def stop(self) -> None:
        if not self._running:
            return

        delegate = self.amazing_delegate
        if delegate is not None:
            delegate.stop_sub()
        self.amazing_delegate = None
        self.nats_producer.close()
        self._running = False

    def run(self) -> None:
        self.start()
        try:
            self.amazing_delegate.wait()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def on_tick(self, payload: Quote) -> None:
        if self.nats_producer.push(payload):
            self.pushed_count += 1
        else:
            self.dropped_count += 1

        total_count = self.pushed_count + self.dropped_count
        if total_count % 5000 == 0:
            logger.info("nats pushed=%d, dropped=%d", self.pushed_count, self.dropped_count)
