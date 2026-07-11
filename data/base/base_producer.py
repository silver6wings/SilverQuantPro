from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

Payload = dict[str, Any] | str | bytes


class BaseProducer(ABC):
    """消息生产端的最小公共接口。"""

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
    def push(self, destination: str, payload: Payload) -> None:
        """发送一条消息。"""
