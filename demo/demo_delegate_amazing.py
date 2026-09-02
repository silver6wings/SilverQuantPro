import datetime

from data.tick.tick_quote import TickPayload
from delegate.amazing_delegate import AmazingDelegate
from tools.utils_remote_am import AmazingSecurityType


def callback(payload: TickPayload) -> None:
    print(f"{datetime.datetime.now()} {payload}\n", end="")


def demo() -> None:
    security_type = AmazingSecurityType.HS_ETF
    codes = AmazingDelegate.get_codes(security_type)
    print(f"{security_type}: {len(codes)}")


if __name__ == "__main__":
    demo()
