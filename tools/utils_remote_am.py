"""
https://cloud.chinastock.com.cn/p/DSG36jYQx2IY_Y8CIAA
"""
import os
import pandas as pd
import AmazingData as ad

from credentials import AMAZING_HOST, AMAZING_PORT, AMAZING_USERNAME, AMAZING_PASSWORD


AM_TRADE_DAY_CACHE_PATH = "./_cache/_am_trade_days.csv"

_am_trade_day_cache: dict[str, bool | str] = {}
_am_trade_max_year_key = "max_year"


class AmazingSecurityType:
    HSA_STOCK = "EXTRA_STOCK_A"         # 上交所A股、深交所A股和北交所的股票列表
    HS_STOCK = "EXTRA_STOCK_A_SH_SZ"    # 上交所A股和深交所A股的股票列表
    SH_STOCK = "SH_A"                   # 上交所A股的股票列表
    SZ_STOCK = "SZ_A"                   # 深交所A股的股票列表
    BJ_STOCK = "BJ_A"                   # 北交所的股票列表

    HSA_INDEX = "EXTRA_INDEX_A"         # 上交所、深交所和北交所的指数列表
    HS_INDEX = "EXTRA_INDEX_A_SH_SZ"    # 上交所和深交所指数列表
    SH_INDEX = "SH_INDEX"               # 上交所指数列表
    SZ_INDEX = "SZ_INDEX"               # 深交所指数列表
    BJ_INDEX = "BJ_INDEX"               # 北交所的指数列表

    HS_ETF = "EXTRA_ETF"    # 上交所、深交所的 ETF 列表
    SH_ETF = "SH_ETF"       # 上交所的 ETF 列表
    SZ_ETF = "SZ_ETF"       # 深交所的 ETF 列表

    HS_KZZ = "EXTRA_KZZ"    # 上交所、深交所的可转债列表
    SH_KZZ = "SH_KZZ"       # 上交所的可转债列表
    SZ_KZZ = "SZ_KZZ"       # 深交所的可转债列表

    HS_HKT = "EXTRA_HKT"    # 沪深港通
    SH_HKT = "SH_HKT"       # 沪港通
    SZ_HKT = "SZ_HKT"       # 深港通

    HS_GLRA = "EXTRA_GLRA"  # 沪深逆回购
    SH_GLRA = "SH_GLRA"     # 上交所逆回购
    SZ_GLRA = "SZ_GLRA"     # 深交所逆回购


def am_login():
    ad.login(username=AMAZING_USERNAME, password=AMAZING_PASSWORD, host=AMAZING_HOST, port=AMAZING_PORT)


def am_logout():
    ad.logout(username=AMAZING_USERNAME)


def get_am_data():
    return ad.BaseData()


def _am_cal_int_to_date_str(day: int) -> str:
    s = str(day)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _get_am_disk_trade_day_list_and_update_max_year() -> list[str]:
    df = pd.read_csv(AM_TRADE_DAY_CACHE_PATH)
    trade_dates = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d").tolist()
    _am_trade_day_cache[_am_trade_max_year_key] = trade_dates[-1][:4]
    return trade_dates


def _fetch_am_trade_day_list(curr_year: str) -> bool:
    end_date = (int(curr_year) + 1) * 10000 + 1231
    try:
        am_login()
        calendar = get_am_data().get_calendar(
            data_type="str",
            market="SH",
            date=end_date,
        )
    except Exception as exc:
        print(f"[Amazing网络缓存] 交易日历拉取失败: {exc}")
        return False

    if calendar is None or len(calendar) == 0:
        print("[Amazing网络缓存] 交易日历拉取失败: 空数据")
        return False

    trade_dates = [_am_cal_int_to_date_str(int(day)) for day in calendar]
    os.makedirs(os.path.dirname(AM_TRADE_DAY_CACHE_PATH), exist_ok=True)
    pd.DataFrame({"trade_date": trade_dates}).to_csv(AM_TRADE_DAY_CACHE_PATH, index=False)
    return True


def check_is_open_day(curr_date: str) -> bool:
    """
    curr_date example: '2024-12-31'
    """
    curr_year = curr_date[:4]

    if curr_date in _am_trade_day_cache:
        if curr_year <= str(_am_trade_day_cache[_am_trade_max_year_key]):
            return bool(_am_trade_day_cache[curr_date])

    if os.path.exists(AM_TRADE_DAY_CACHE_PATH):
        trade_day_list = _get_am_disk_trade_day_list_and_update_max_year()
        if curr_year <= str(_am_trade_day_cache[_am_trade_max_year_key]):
            ans = curr_date in trade_day_list
            _am_trade_day_cache[curr_date] = ans
            print(f'[Amazing文件缓存] {curr_date} 为 {"" if ans else "非"}交易日')
            return ans

    if not _fetch_am_trade_day_list(curr_year):
        print("[Amazing网络缓存] 交易日历拉取失败且无可用缓存")
        return True
    print(
        f"[Amazing网络缓存] 更新交易日历 {curr_year} - {int(curr_year) + 1} "
        f"已存入 {AM_TRADE_DAY_CACHE_PATH}."
    )

    trade_day_list = _get_am_disk_trade_day_list_and_update_max_year()
    if curr_year <= str(_am_trade_day_cache[_am_trade_max_year_key]):
        ans = curr_date in trade_day_list
        _am_trade_day_cache[curr_date] = ans
        print(f'[Amazing网络缓存] {curr_date} 为 {"" if ans else "非"}交易日')
        return ans

    print(f"[DO NOT KNOW {curr_date}, default to True trade day]")
    return True
