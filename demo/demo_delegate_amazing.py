import datetime

from data.tick.tick_quote import TickPayload
from delegate.amazing_delegate import AmazingDelegate
from delegate.amazing_subscriber import AmazingSubscriber


def callback(payload: TickPayload) -> None:
    print(f"{datetime.datetime.now()} {payload}\n", end="")


def demo() -> None:
    delegate = AmazingDelegate()
    subscriber = AmazingSubscriber(delegate)
    subscriber.start_sub(callback=callback)
    try:
        subscriber.wait()
    except KeyboardInterrupt:
        pass
    finally:
        subscriber.stop_sub()


if __name__ == "__main__":
    demo()
