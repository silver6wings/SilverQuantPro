from tools.utils_remote_xt import XtSectorType
from delegate.xtdata_delegate import XtdataDelegate


def demo_xt_code_list():
    delegate = XtdataDelegate()
    sectors = [XtSectorType.HS_STOCK]
    print(sectors)
    code_list = delegate.get_code_list(sectors)
    print(code_list)


if __name__ == '__main__':
    demo_xt_code_list()
