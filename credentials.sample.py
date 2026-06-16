# 复制本文件为 credentials.py 并填写真实值

# 星耀数智服务账号密码
AMAZING_USERNAME = "2116000xxxxx"
AMAZING_PASSWORD = "2116000xxxxx@xxxx"
AMAZING_HOST = "xxx.xxx.xxx.xxx"
AMAZING_PORT = 1234

# NATS 推送（生产方）
NATS_PRODUCER_URL = "nats://127.0.0.1:4222"
NATS_PRODUCER_SUBJECT = "market.tick.amazing"
NATS_BATCH_SIZE = 1000
NATS_FLUSH_INTERVAL = 0.02
NATS_MAX_QUEUE_SIZE = 100000

# NATS 接收（消费方）
NATS_CONSUMER_URL = "nats://127.0.0.1:4222"
NATS_CONSUMER_SUBJECT = "market.tick.amazing"

# NATS 服务
NATS_BIND_ADDR = "0.0.0.0"
