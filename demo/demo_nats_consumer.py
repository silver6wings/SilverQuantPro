import asyncio

from typing import Any
from data.nats.nats_consumer import NatsConsumer


async def main() -> None:
    consumer = NatsConsumer()

    def handle_data(data: dict[str, Any] | str) -> None:
        print(f"received: {data}")

    await consumer.run(handle_data)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
