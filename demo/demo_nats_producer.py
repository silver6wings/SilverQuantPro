import time

from credentials import NATS_AM_SUBJECT
from data.base.base_producer import BaseProducer
from data.nats.nats_producer import NatsProducer
from data.tick.tick_quote import TickPayload, build_tick_quote

_DEMO_CODE = "000001.SZ"


def main() -> None:
    producer: BaseProducer = NatsProducer()
    index = 0

    try:
        while True:
            last_price = round(11.07 + index * 0.01, 2)
            payload: TickPayload = {
                _DEMO_CODE: build_tick_quote(
                    timestamp=int(time.time() * 1000),
                    last_close=11.06,
                    open=11.07,
                    high=last_price,
                    low=11.06,
                    last_price=last_price,
                    volume=1000 + index,
                    amount=round(last_price * (1000 + index), 2),
                ),
            }
            producer.push(NATS_AM_SUBJECT, payload)
            print(f"published to {NATS_AM_SUBJECT}: {payload}")

            index += 1
            time.sleep(1)
    finally:
        producer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
