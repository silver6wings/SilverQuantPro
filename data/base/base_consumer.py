from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

Payload = dict[str, Any] | str | bytes
MessageHandler = Callable[[Payload], None]


class BaseConsumer(ABC):
    """消息消费端的最小公共接口。"""

    @abstractmethod
    def connect(self) -> None:
        """建立到底层消息系统的连接。"""

    @abstractmethod
    def close(self) -> None:
        """关闭连接并释放资源。"""

    @abstractmethod
    def is_connected(self) -> bool:
        """返回连接当前是否可用。"""

    @abstractmethod
    def listen(self, destination: str, handler: MessageHandler) -> None:
        """监听一个目标通道并分发消息；通常阻塞直到连接关闭或被中断。"""
