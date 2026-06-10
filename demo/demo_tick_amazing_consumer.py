import logging
from pathlib import Path

from data.tick.amazing_nats_consumer import AmazingNatsConsumer

_LOG_LEVEL = logging.INFO
_LOG_PATH = Path("_cache/demo_consumer.log")


def setup_logging() -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=_LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(_LOG_PATH, encoding="utf-8"),
        ],
        force=True,
    )


def main() -> None:
    setup_logging()

    consumer = AmazingNatsConsumer(interval=1.0)
    code_list = ["000001.SZ", "000002.SZ"]

    sub_sequence = consumer.subscribe_whole_quote(code_list, callback=callback_sub_whole)
    try:
        consumer.wait()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.unsubscribe_quote(sub_sequence)


def callback_sub_whole(quotes: dict) -> None:
    print(quotes)

    latest = quotes.get("000001.SZ")
    if latest is not None:
        print(latest)

    latest = quotes.get("000002.SZ")
    if latest is not None:
        print(latest)


if __name__ == "__main__":
    main()
