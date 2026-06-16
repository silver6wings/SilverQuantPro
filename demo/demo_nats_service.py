"""
Demo：调用 data/nats_service.py 启停本地 nats-server。

用法
----
    PYTHONPATH=. python demo/demo_nats_service.py
    PYTHONPATH=. python demo/demo_nats_service.py kill
    PYTHONPATH=. python demo/demo_nats_service.py kill --port 4222
"""
from data.nats_service import main

if __name__ == "__main__":
    main()
