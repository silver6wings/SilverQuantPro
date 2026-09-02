# 复制本文件为 credentials.py 并填写真实值（本文件仅为模板，运行时不会使用）

# 星耀数智服务账号密码
AMAZING_USERNAME = "********"
AMAZING_PASSWORD = "********"
AMAZING_HOST = "0.0.0.0"
AMAZING_PORT = 0

# NATS
NATS_PRODUCER_URL = "nats://127.0.0.1:4222"       # 生产方连接地址（tick producer 推送用）
NATS_CONSUMER_URL = "nats://127.0.0.1:4222"       # 消费方连接地址（tick consumer 订阅用）
NATS_AM_SUBJECT = "market.tick.amazing"           # Amazing 行情 subject
NATS_XT_SUBJECT = "market.tick.xtquant"           # xtquant 行情 subject

# xt producer：一条 NATS 消息里最多几个 code 的 quote（控制单包大小，避免 max_payload）
NATS_XT_QUOTES_PER_MESSAGE = 1000

# NatsThreadedProducer 发送侧参数（amazing / xt 共用）
NATS_BATCH_SIZE = 1000                            # 后台线程每轮最多连续 publish 几条「NATS 消息」再 flush；不是 xt 每包 code 数
NATS_FLUSH_INTERVAL = 0.02                        # 队列空时，发送线程休眠秒数（降低空转 CPU）
NATS_MAX_QUEUE_SIZE = 100000                      # 待发送队列上限；满则 push 失败并计 dropped

# nats-server 监听地址（data/job_nats_service.py）
NATS_BIND_ADDR = "0.0.0.0"
