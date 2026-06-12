"""
https://cloud.chinastock.com.cn/p/DSG36jYQx2IY_Y8CIAA
"""
import AmazingData as ad

from credentials import AMAZING_HOST, AMAZING_PORT, AMAZING_USERNAME, AMAZING_PASSWORD


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
