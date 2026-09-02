import datetime

from data.tick.tick_quote import TickPayload
from delegate.amazing_delegate import AmazingSubscriber


def callback(payload: TickPayload) -> None:
    print(f"{datetime.datetime.now()} {payload}\n", end="")


def demo() -> None:
    subscriber = AmazingSubscriber()
    subscriber.set_sub_code_list(code_list=[
        '000001.SZ',    # stock
        '300001.SZ',    # stock
        '600000.SH',    # stock
        '688001.SH',    # stock
        '920000.BJ',    # stock
        '000001.SH',    # index
        '399001.SZ',    # index
        '159159.SZ',    # etf
        '510510.SH',    # etf
        '123268.SZ',    # kzz
        '113066.SH',    # kzz
    ])

    subscriber.start_sub(callback=callback)
    try:
        subscriber.wait()
    except KeyboardInterrupt:
        pass
    finally:
        subscriber.stop_sub()


if __name__ == "__main__":
    demo()
