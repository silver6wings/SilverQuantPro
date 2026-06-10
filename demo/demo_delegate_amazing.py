import datetime
from delegate.amazing_delegate import AmazingDelegate, Quote


def callback(payload: Quote) -> None:
    print(f"{datetime.datetime.now()} {payload}\n", end="")


def demo() -> None:
    delegate = AmazingDelegate()
    delegate.start_sub(callback=callback)
    try:
        delegate.wait()
    except KeyboardInterrupt:
        pass
    finally:
        delegate.stop_sub()


if __name__ == "__main__":
    demo()
