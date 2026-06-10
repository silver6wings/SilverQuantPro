import asyncio
import time

from data.nats.nats_producer import NatsProducer, NatsThreadedProducer


async def main() -> None:
    producer = NatsProducer()

    payload = {
        "ts_event": time.time_ns(),
        "source": "demo",
        "symbol": "000001.SZ",
        "price": 11.07,
        "volume": 1000,
    }

    await producer.push(payload, flush=True)
    print(f"published to {producer.subject}: {payload}")

    await producer.close()


def demo_nats_producer():
    asyncio.run(main())


def demo_nats_threaded_producer() -> None:
    producer = NatsThreadedProducer()
    producer.start()

    payload = {
        "ts_event": time.time_ns(),
        "source": "demo",
        "symbol": "000001.SZ",
        "price": 11.07,
        "volume": 1000,
    }

    ok = producer.push(payload, block=True, timeout=1.0)
    if not ok:
        print("queue full, failed to enqueue demo payload")
        producer.close()
        return

    print(f"enqueued for {producer.subject}: {payload}")
    producer.close()
    print(f"published to {producer.subject}: {payload}")


if __name__ == "__main__":
    demo_nats_producer()
    demo_nats_threaded_producer()
