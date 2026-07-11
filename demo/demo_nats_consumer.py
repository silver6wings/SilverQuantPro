from typing import Any

from credentials import NATS_AM_SUBJECT
from data.base.base_consumer import BaseConsumer
from data.nats.nats_consumer import NatsConsumer


def main() -> None:
    consumer: BaseConsumer = NatsConsumer()

    def handle_data(payload: dict[str, Any] | str | bytes) -> None:
        print(f"received from {NATS_AM_SUBJECT}: {payload}")

    try:
        consumer.listen(NATS_AM_SUBJECT, handle_data)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
