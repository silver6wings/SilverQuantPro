import logging
from pathlib import Path

from delegate.amazing_delegate import AmazingDelegate, AmazingSecurityType
from data.tick.amazing_nats_producer import AmazingNatsProducer

_LOG_PATH = Path("_cache/prod_producer.log")


def setup_logging() -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(_LOG_PATH, encoding="utf-8"),
        ],
        force=True,
    )


def run() -> None:
    delegate = AmazingDelegate()

    stock_code_list = delegate.get_codes(AmazingSecurityType.HSA_STOCK)
    print(len(stock_code_list), stock_code_list)

    index_code_list = delegate.get_codes(AmazingSecurityType.HSA_INDEX)
    print(len(index_code_list), index_code_list)

    etf_code_list = delegate.get_codes(AmazingSecurityType.HS_ETF)
    print(len(etf_code_list), etf_code_list)

    kzz_code_list = delegate.get_codes(AmazingSecurityType.HS_KZZ)
    print(len(kzz_code_list), kzz_code_list)

    producer = AmazingNatsProducer()
    producer.set_code_list(
        stock_code_list
        + index_code_list
        + etf_code_list
        + kzz_code_list
    )
    producer.run()


def main() -> None:
    setup_logging()
    run()


# NOTE:
# 深交所：17:00 结束
# 上交所: 16:30 结束
# 北交所: 15:35 结束

if __name__ == "__main__":
    main()
