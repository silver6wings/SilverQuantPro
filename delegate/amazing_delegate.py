"""
Amazing Documents
https://cloud.chinastock.com.cn/p/DSG36jYQx2IY_Y8CIAA
"""
import threading

from tools.utils_remote_am import AmazingSecurityType, am_login, am_logout, get_am_data


class AmazingDelegate:
    _instance: "AmazingDelegate | None" = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls) -> "AmazingDelegate":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if AmazingDelegate._initialized:
            return
        with AmazingDelegate._lock:
            if AmazingDelegate._initialized:
                return
            am_login()
            self.amazing_data = get_am_data()
            AmazingDelegate._initialized = True

    def login(self) -> None:
        try:
            am_login()
        except Exception:
            pass

    def logout(self) -> None:
        try:
            am_logout()
        except Exception:
            pass

    def __del__(self) -> None:
        if AmazingDelegate._instance is not self:
            return
        self.logout()

    @classmethod
    def get_codes(cls, security_type: str) -> list[str]:
        code_list = cls().amazing_data.get_code_list(security_type=security_type)
        return list(code_list)

    @classmethod
    def get_hs_stock_codes(cls) -> list[str]:
        return cls.get_codes(AmazingSecurityType.HS_STOCK)

    @classmethod
    def get_hs_index_codes(cls) -> list[str]:
        return cls.get_codes(AmazingSecurityType.HS_INDEX)

    @classmethod
    def get_hs_etf_codes(cls) -> list[str]:
        return cls.get_codes(AmazingSecurityType.HS_ETF)
