"""
定义各种文件和文件夹的路径,实际运行中需要把目录改成自己的目录
"""
import sys
import os

# 系统根目录, 实际运行中需要把目录改成自己的目录
BASE = os.path.dirname(os.path.abspath(__file__))

# 文件夹路径
data_package_path = os.path.join(BASE, 'data-package')
finance_report_path = os.path.join(BASE, 'finance-report')
sql_path = os.path.join(BASE, 'sql')
trade_record_path = os.path.join(BASE, 'trade-record')
stock_list_path = os.path.join(BASE, 'stock-list')
TMP_FILE_PATH = os.path.join(BASE, 'tmp-file')
BACKUP_FILE_PATH = os.path.join(BASE, 'backup-file')

# sqlite3数据库路径
INDICATOR_SQLITE3 = os.path.join(data_package_path, 'indicator.sqlite3')
DIVIDEND_RATE_SQLITE3 = os.path.join(data_package_path, 'dividend-rate.sqlite3')
HISTORY_PB_SQLITE3 = os.path.join(data_package_path, 'history-pb.sqlite3')
CASHFLOW_PROFIT_SQLITE3 = os.path.join(data_package_path, 'cashflow-profit.sqlite3')
CURVE_SQLITE3 = os.path.join(data_package_path, 'curve.sqlite3')
PE_PB_SQLITE3 = os.path.join(data_package_path, 'pe-pb.sqlite3')
SALARY_SQLITE3 = os.path.join(data_package_path, 'salary.sqlite3')
TVALUE_SQLITE3 = os.path.join(data_package_path, 'total-value.sqlite3')
INDICATOR_ROE_FROM_1991 = os.path.join(data_package_path, 'indicator-roe-from-1991.sqlite3')

# tmp backup file path
ALL_PB_PE_SQLITE3 = os.path.join(TMP_FILE_PATH, 'all-pb-pe-indicator.sqlite3')
COM_RANKS_SQLITE3 = os.path.join(TMP_FILE_PATH, 'stock-comprehensive-ranks.sqlite3')
TEST_CONDITION_SQLITE3 = os.path.join(TMP_FILE_PATH, 'test-condition.sqlite3')

# xlsx 文件路径
SW_STOCK_LIST = os.path.join(stock_list_path, 'sw-stock-list.xlsx')
CNINFO_STOCK_LIST = os.path.join(stock_list_path, 'cninfo_stock_list.xlsx')

# header file info 
header_xueqiu = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.62',   
}

headers_sina = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.24',
}

headers_chinabond = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15'
}
headers_cninfo = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.62',
    'cookie': 'JSESSIONID=0664CA318B535CEF06BFC64057EE7830; insert_cookie=45380249; routeId=.uc1; _sp_ses.2141=*; SID=6dfece46-7e46-4da8-9513-25fee0fc6839; cninfo_user_browse=600600,gssh0600600,%E9%9D%92%E5%B2%9B%E5%95%A4%E9%85%92|002142,9900003281,%E5%AE%81%E6%B3%A2%E9%93%B6%E8%A1%8C|603288,9900023228,%E6%B5%B7%E5%A4%A9%E5%91%B3%E4%B8%9A; _sp_id.2141=2f3d65d6-9fc8-4d26-91d1-3ffa965cd59c.1655331806.6.1669820830.1669815713.b0e7bfca-d0ae-451b-90df-bf9b61ebb710'
}
headers_163 = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.62',
    'cookie': '_antanalysis_s_id=1669993730392; _ntes_stock_recent_=0600600; _ntes_stock_recent_=0600600; _ntes_stock_recent_=0600600; ne_analysis_trace_id=1669994086442; s_n_f_l_n3=f3d14b207651653b1669994086444; pgr_n_f_l_n3=f3d14b207651653b16699943954914534; vinfo_n_f_l_n3=f3d14b207651653b.1.0.1669994086444.0.1669994404663',
}

headers_10jqka = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36',
}

