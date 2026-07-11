"""
QMT xtdata 板块名称常量，对应 xtdata.get_stock_list_in_sector(sector_name)。

名称与 QMT 客户端板块树 / xtdata.get_sector_list() 一致。
数据访问见 delegate.xtdata_delegate.XtdataDelegate。
"""


class XtSectorType:
    HSA_STOCK = "沪深京A股"   # 上交所A股、深交所A股和北交所的股票列表
    HS_STOCK = "沪深A股"      # 上交所A股和深交所A股的股票列表
    SH_STOCK = "上证A股"      # 上交所A股的股票列表
    SZ_STOCK = "深证A股"      # 深交所A股的股票列表
    BJ_STOCK = "京市A股"      # 北交所的股票列表

    HS_INDEX = "沪深指数"     # 上交所和深交所指数列表
    SH_INDEX = "沪市指数"     # 上交所指数列表
    SZ_INDEX = "深市指数"     # 深交所指数列表

    HS_ETF = "沪深ETF"        # 上交所、深交所的 ETF 列表
    SH_ETF = "沪市ETF"        # 上交所的 ETF 列表
    SZ_ETF = "深市ETF"        # 深交所的 ETF 列表

    HS_KZZ = "沪深转债"       # 上交所、深交所的可转债列表
    SH_KZZ = "上证转债"       # 上交所的可转债列表
    SZ_KZZ = "深证转债"       # 深交所的可转债列表

    HS_HKT = "香港联交所股票"  # 沪深港通标的（QMT 以联交所股票板块聚合）

    HS_GLRA = "沪深债券"      # 沪深逆回购等债券品种
    SH_GLRA = "沪市债券"      # 上交所逆回购
    SZ_GLRA = "深市债券"      # 深交所逆回购


class XtDividendType:
    NONE = "none"                 # 不复权
    FRONT = "front"               # 前复权
    BACK = "back"                 # 后复权
    FRONT_RATIO = "front_ratio"   # 等比前复权
    BACK_RATIO = "back_ratio"     # 等比后复权


def parse_xt_sectors(raw: str, default: tuple[str, ...]) -> list[str]:
    """解析逗号分隔的板块；支持 XtSectorType 成员名或板块名字符串。"""
    sectors: list[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        name = item.strip()
        if not name:
            continue
        sector = getattr(XtSectorType, name, name)
        if sector not in seen:
            seen.add(sector)
            sectors.append(sector)
    return sectors or list(default)
