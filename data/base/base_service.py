from __future__ import annotations

from abc import ABC, abstractmethod


class BaseService(ABC):
    """本地中间件服务的生命周期抽象。"""

    @abstractmethod
    def start(self) -> None:
        """启动本地服务。"""

    @abstractmethod
    def stop(self) -> None:
        """停止本地服务。"""

    @abstractmethod
    def is_active(self) -> bool:
        """返回服务当前是否可用。"""
