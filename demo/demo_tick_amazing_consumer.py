"""
Amazing NATS 消费端 demo。

常见问题与原因
--------------

1. callback 触发几次后长时间没输出
   - callback 执行太慢（print 整包 quotes、同步 IO、复杂计算），上一轮未结束时
     后续 interval 会跳过 dispatch，表现为「卡住」。
   - callback 死循环或永久阻塞，之后不会再触发 dispatch，但 quote_count 可能仍在涨。

2. quote_count 在涨，但 callback 很少触发
   - 属正常：收消息与触发 callback 是两条路径；慢 callback 会让多轮 tick 攒在一批里
     一次性回调。
   - interval 内没有匹配 tick 时按设计不回调（不会传空 dict）。

3. 几乎收不到数据
   - consumer 的 code_list 与 producer 不一致，tick 被过滤。
   - NATS 未启动、subject 不一致、producer 未运行或队列满丢包。

4. subscribe 返回 -1
   - 已有订阅未 unsubscribe，须先 unsubscribe_quote() 再 subscribe。

callback 编写原则
------------------

1. 必须快
   - callback 在线程池执行，但上一轮未完成时本轮 dispatch 会跳过。
   - 目标：耗时远小于 interval（如 interval=1s，callback 建议 <100ms）。

2. 不要做重 IO / 大打印
   - 禁止 print(quotes) 整包；只打 len(quotes) 或少量 sample code。
   - 落盘、网络请求、数据库写放到 callback 外的队列，由独立 worker 异步处理。

3. 不要阻塞、不要死循环
   - 禁止 time.sleep、无限等待、join 其他慢线程。
   - quotes 是已 swap 出来的副本，可安全读写，但不要依赖「下一轮很快再来」。

4. 按批处理，按 code 聚合
   - 每次回调的 quotes 是 {code: quote_dict}，同一 code 在批内只有最新一条。
   - 遍历 quotes.items() 处理即可，不要假设固定有哪些 code。

5. 容错
   - callback 内异常不会搞垮 consumer，但该批数据处理会中断；关键逻辑自行 try/except。

推荐写法
--------
    def callback_sub_whole(quotes: dict) -> None:
        # 只统计、抽样，或写入内存/队列
        for code, quote in quotes.items():
            ...
"""

import logging
from pathlib import Path

from delegate.amazing_delegate import AmazingDelegate
from data.tick.amazing_nats_consumer import AmazingNatsConsumer

_LOG_LEVEL = logging.DEBUG
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

    code_list = ['000001.SZ', '000002.SZ', '600000.SH', '300001.SZ']
    # code_list = ["000001.SH"]

    # delegate = AmazingDelegate()
    # code_list = delegate.get_hs_index_codes()
    # print(len(code_list), code_list)

    consumer = AmazingNatsConsumer(interval=1.0)
    if consumer.subscribe_whole_quote(code_list, callback=callback_sub_whole) != 0:
        raise RuntimeError("subscribe failed")
    try:
        consumer.wait()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.unsubscribe_quote()


def callback_sub_whole(quotes: dict) -> None:
    sample_codes = list(quotes.keys())[:5]
    print(datetime.datetime.now(), f"quotes={len(quotes)}", f"sample={sample_codes}")


if __name__ == "__main__":
    import datetime
    main()
