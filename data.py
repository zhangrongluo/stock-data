import random
import os
import re
import sys
import sqlite3
import datetime
import time
import pandas as pd
from pandas import DataFrame
import numpy as np
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple, Union
import pdfplumber
from concurrent.futures import ThreadPoolExecutor

from path import (
    INDICATOR_SQLITE3, DIVIDEND_RATE_SQLITE3, HISTORY_PB_SQLITE3, CASHFLOW_PROFIT_SQLITE3, PE_PB_SQLITE3, 
    PE_PB_SQLITE3, SALARY_SQLITE3, CURVE_SQLITE3, TVALUE_SQLITE3, INDICATOR_ROE_FROM_1991, data_package_path, 
    finance_report_path, sql_path, trade_record_path, header_xueqiu, headers_163, headers_chinabond, headers_cninfo, 
    headers_sina, headers_10jqka, SW_STOCK_LIST, CNINFO_STOCK_LIST
)
    

class StockData:
    """
    - 用于管理股票代码、下载数据、计算指标、初始化数据库和更新数据库.
    - 获取数据的来源:申万研究所、巨潮资讯、雪球网、新浪财经、同花顺、网易财经、国债信息网等网站.(2023-04-25)

    - update_...系列函数用于更新数据库,原思路是全部数据库独立下载,这样涉及到大量重复动作,浪费了网络下载资源.
    - 为了提高更新效率,前期已经把indicator.sqlite3和indicator-roe-from-1991.sqlite3数据库的更新工作合二为一.
    - 对于其他的数据库,也采用这种方式,即首先更新CSV交易记录文件,然后其他数据库再从CSV文件中读取数据,更新数据库.
    - 按照这个方法,每日需要执行下载更新的数据库为:indicator.sqlite3 curve.sqlite3 和 CSV文件.
    - 其他需要重复下载的update_...方法更名为update_...copy_from_CSV,原update_...系列函数全部保留.(2023-04-28)

    - TODO: 检查当申万股票池清单发生变化时,如何更新数据库的资料.(2023-06-02)
    """


    def __init__(self, stock_list_path: str = SW_STOCK_LIST):
        """ stock_list_path 为绝对路径  """

        self.__sw_stock_list: DataFrame = pd.read_excel(io=stock_list_path)  # 申万股票清单pandas df格式
        self.__cninfo_stock_list: DataFrame = pd.read_excel(io=CNINFO_STOCK_LIST, dtype={'code': str})  # 巨潮资讯网股票清单pandas
        self.__data_package_path = data_package_path
        self.__finance_report_path = finance_report_path
        self.__sql_path = sql_path
        self.__trade_record_path = trade_record_path

        # 标志cookies状态
        self.__xueqiu_session = requests.Session()
        self.__sina_session = requests.Session()
        self.__cninfo_session = requests.Session()
        self.__10jqka_session = requests.Session()
        
        self.__xueqiu_cookie_existed = False
        self.__sina_cookie_existed =False
        self.__cninfo_cookie_existed = False
        self.__chinabond_cookie_existed = False
        self.__163_cookie_existed = False
        self.__10jqka_cookie_existed = False

        # 选取EDGE浏览器数据即可
        self.__headers_xueqiu = header_xueqiu
        self.__headers_sina = headers_sina
        self.__headers_chinabond = headers_chinabond
        self.__headers_cninfo = headers_cninfo
        self.__headers_163 = headers_163
        self.__headers_10jqka = headers_10jqka

        # 用于微信消息推送，在http://www.pushplus.plus/push1.html获取
        self.__pushplus_token = '8306d0b7249b412ba10cf1109ff62cd7'  

        # 用于设置初始选股条件,为7年ROE值
        self.__init_roe_condition_value = [20]*7


    def calculate_average_salary(self, code: str) -> List:
        """
        - 获取公司全部员工的平均收入
        - 该函数取决于一下三个函数:search_yearly_total_employee_from_xueqiu
        download_cashflow_statement_from_xueqiu 和 search_yearly_report_total_employee

        - 返回一个列表，包含员工总数 薪酬总额 人均薪酬
        """

        result = []  # 定义返回值

        employee = self.search_yearly_total_employee_from_xueqiu(code=code)  # 雪球下载更准确
        result.append(employee)

        # 其次获取支付给职工的薪酬数据
        cashflow_result = self.download_cashflow_statement_from_xueqiu(code=code, count=1, type='Q4')
        cash_paid_to_employee_etc = cashflow_result['data']['list'][0]['cash_paid_to_employee_etc'][0]
        result.append(cash_paid_to_employee_etc)

        # 计算人均薪酬
        if employee:
            print(f'正在计算{code}人均薪酬......'+'\r', end='', flush=True)
            average_salary = cash_paid_to_employee_etc/employee
            result.append(round(average_salary, 2))
        else:
            result.append(0.00)

        return result


    def calculate_MAX_MIN_MEAN_pb(self, code: str) -> Tuple:
        """ 根据历史交易记录和财务数据,计算股票最大、最小和平均PB """

        # 打开文件
        industry_class = self.get_name_and_class_by_code(code=code)[1]
        trade_record_file = os.path.join(self.__trade_record_path, f'{industry_class}/{code}.csv')
        trade_record_df = pd.read_csv(filepath_or_buffer=trade_record_file)

        max_pb, min_pb, mean_pb = trade_record_df['PB'].max(), trade_record_df['PB'].min(), trade_record_df['PB'].mean()

        return round(max_pb, 2), round(min_pb, 2), round(mean_pb, 2)


    def calculate_5_years_cashflow_to_profit(self, code: str) -> float:
        """ 计算从上年起最近5年现金净流量之和/净利润之和 """

        cashflow_sum, profit_sum = 0.00, 0.00
        result = 0.00
        cashflow_result = self.download_cashflow_statement_from_xueqiu(code=code, count=5, type='Q4')
        profit_result = self.download_financial_indicator_from_xueqiu(code=code, count=5, type='Q4')

        for item in cashflow_result['data']['list']:
            try:
                cashflow_sum += (item['ncf_from_oa'][0] + item['ncf_from_ia'][0])  # 经营活动-投资活动
            except:
                ...

        for item in profit_result['data']['list']:
            profit_sum += item['net_profit_atsopc'][0]  # 净利润

        if profit_sum != 0:
            result = cashflow_sum/profit_sum

        return round(result, 4)


    def calculate_stock_mos(self, code:str, period:int) -> float:
        """ 
        - 计算股票的安全边际, code是股票代码, period指定跨度,从最近一个完整年度起算 
        - 如果数据库包括了最新的半年报数据,在计算时会包括该数据
        - 出错代码:88888.88和99999.99
        """
        mos = 0.00

        # 跨度最长为10年, 超过则返回99999.99
        if period > 10 or period <= 0:
            return 99999.99
        
        # 获取最新股票市净率
        pb = self.get_stock_pb_from_xueqiu(code=code)
        
        # 获取最近非0的国债到期收益率
        yield_value = 0  # TODO最终可能为0, 出错
        yesterday = datetime.date.today() + datetime.timedelta(days=-1)
        begin = yesterday + datetime.timedelta(days=-30)
        date_list = pd.date_range(begin, yesterday)
        date_str = [str(date)[0:10] for date in date_list]  # 生成日期序列

        con = sqlite3.connect(CURVE_SQLITE3)
        with con:
            for date in date_str[::-1]:
                sql = """ SELECT value1 FROM 'yield-curve' WHERE date1=? """
                tmp = con.execute(sql, (date,))
                try:
                    yield_value =  tmp.fetchone()[0]
                    if yield_value:
                        break
                except TypeError:  # 出现了表中没有的日期
                    ...

        con = sqlite3.connect(INDICATOR_SQLITE3)
        with con:
            sql = """ PRAGMA table_info('roe-all-stocks') """
            result = con.execute(sql).fetchall()

            # 生成搜索字符串
            roe_fields = []
            for tup in result:
                if ('stock' not in tup[1]) and ('Q2' not in tup[1]):  # 只选取年度字段
                    roe_fields.append(tup[1])
            roe_fields = sorted(roe_fields, reverse=True)[0:period]  # 降序后取指定的字段数目

            # 判断是否包括最新的半年roe数据
            half_exist = False
            last_year_filed = int(roe_fields[0][1:5])
            last_half_year_filed = 'Y' + str(last_year_filed + 1) + 'Q2'
            for filed in result:
                if last_half_year_filed == filed[1]:
                    roe_fields.insert(0, last_half_year_filed)
                    half_exist = True
                    break

            pre_scode = """"""
            for idx, filed in enumerate(roe_fields):
                if idx == len(roe_fields) - 1:  # 最后一项
                    pre_scode += filed
                else:
                    pre_scode += filed+', '
            sql = f"""SELECT {pre_scode} FROM 'roe-all-stocks' WHERE stockcode=?  """

            code = code + '.SH' if code.startswith('6') else code +'.SZ'
            tmp = con.execute(sql, (code,)).fetchone()
            try:
                if half_exist:
                    aver_roe = sum(tmp)/(period+0.5)
                else:
                    aver_roe = sum(tmp)/period
            except TypeError:
                return 88888.88

        # 计算股票安全边际MOS
        inner_pb = aver_roe/yield_value
        mos = round((1 - pb/inner_pb), 2)

        return mos


    def calculate_comprehensive_information(self, code: str) -> Dict:
        """ 
        - 计算并返回股票综合信息,包括:代码、名称、行业、价格、cur_PE、cur_PB、max_pb
        mean_pb、min_pb、MOS、期间涨跌幅、平均薪酬、5年cashflow/profit等
        - 主要用于前端显示股票综合信息.(2023-04-25)
        """

        result_dict = {}
        result_dict['stock_code'] = code+'.SH' if code.startswith('6') else code+'.SZ'
        result_dict['stock_name'] = self.get_name_and_class_by_code(code=code)[0]
        result_dict['stock_class'] = self.get_name_and_class_by_code(code=code)[1]

        # 涨跌幅
        today = datetime.date.today()
        pre_today_1800 = today + datetime.timedelta(days=-365*5)

        tmp = self.download_period_statistic_value_from_xueqiu(code=code, begin=str(pre_today_1800), end=str(today))

        value_items = tmp['data']['item']  # 当日涨跌幅
        result_dict['cur_price'] = value_items[-1][5]
        result_dict['cur_rising'] = value_items[-1][7]

        first_date_this_year = datetime.datetime.strptime(str(today.year)+'-01-01', '%Y-%m-%d').date()  # 当年涨跌幅
        camp_timestamp = self.date_to_timestamp(date=str(first_date_this_year))
        position = self.get_closest_date_position(camp_timestamp=camp_timestamp, target_list=value_items)
        result_dict['this_year_rising'] = value_items[-1][5] / value_items[position][5] - 1

        pre_day_365 = today + datetime.timedelta(days=-365)  # 1年涨跌幅
        camp_timestamp = self.date_to_timestamp(date=str(pre_day_365))
        position = self.get_closest_date_position(camp_timestamp=camp_timestamp, target_list=value_items)
        result_dict['1_year_rising'] = value_items[-1][5] / value_items[position][5] - 1

        # pe pb信息
        result_dict['cur_pe'] = self.get_stock_PE_from_sina(code=code)
        result_dict['cur_pb'] = self.get_stock_pb_from_xueqiu(code=code)
        con = sqlite3.connect(HISTORY_PB_SQLITE3)
        with con:
            sql = """ SELECT maxPB, minPB, meanPB FROM 'history-pb' WHERE stockcode=? """
            tmp = con.execute(sql, (result_dict['stock_code'],)).fetchone()
            if not tmp:
                result_dict['max_pb'] = '未能获取'
                result_dict['min_pb'] = '未能获取'
                result_dict['mean_pb'] = '未能获取'
            else:
                result_dict['max_pb'] = tmp[0]
                result_dict['min_pb'] = tmp[1]
                result_dict['mean_pb'] = tmp[2]

        # mos信息
        result_dict['3_mos'] = self.calculate_stock_mos(code=code, period=3)
        result_dict['5_mos'] = self.calculate_stock_mos(code=code, period=5)
        result_dict['7_mos'] = self.calculate_stock_mos(code=code, period=7)

        # salary信息
        con = sqlite3.connect(SALARY_SQLITE3)
        table_name = 'salary-' + str(datetime.datetime.now().year - 1)
        with con:
            sql = f""" SELECT average_salary FROM '{table_name}' WHERE stockcode=? """
            tmp = con.execute(sql, (result_dict['stock_code'],)).fetchone()
            if tmp:
                result_dict['salary'] = tmp[0]
            else:
                result_dict['salary'] = '无法获取'

        # 股息率信息
        result_dict['dividend_rate'] = self.get_stock_dividend_rate_from_xueqiu(code=code)

        # cashflow-profit ratio 信息
        con = sqlite3.connect(CASHFLOW_PROFIT_SQLITE3)
        last_year = datetime.datetime.now().year - 1
        table_name = str(last_year - 4) + '-' + str(last_year)
        with con:
            sql = f"""SELECT cash_to_profit FROM '{table_name}' WHERE stockcode=? """
            tmp = con.execute(sql, (result_dict['stock_code'], )).fetchone()
            if tmp:
                result_dict['cash_to_profit'] = tmp[0]
            else:
                result_dict['cash_to_profit'] = '无法获取'

        return result_dict


    def calculate_period_rising_value(self, args: List) -> float:
        """ 
        - 计算股票在指定期间的涨幅, args包含三项元素: 股票代码(不含后缀)、开始日期和结束日期 
        开始日期和结束日期均为yyyy-mm-dd型字符串,这个函数不是很稳定,可能还是有BUG. 
        """

        rising_value = 0.00

        code: str = args[0]
        start_date_str = args[1]
        end_date_str = args[2]

        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

        start_date_timestamp = self.date_to_timestamp(start_date_str)
        end_date_timestamp = self.date_to_timestamp(end_date_str)

        start_date_str_download = str(start_date + datetime.timedelta(days=-30))  # 前推30天

        try:
            tmp = self.download_period_statistic_value_from_xueqiu(code=code, begin=start_date_str_download, end=end_date_str)
            value_items = tmp['data']['item']

            start_date_position = self.get_closest_date_position(start_date_timestamp, value_items)
            end_date_position = self.get_closest_date_position(end_date_timestamp, value_items)

            start_price = value_items[start_date_position][5]
            end_price = value_items[end_date_position][5]

            rising_value = end_price/start_price -1
        except:
            ...

        return round(rising_value, 4)


    def adjust_trade_record_csv(self, code: str):
        """ 
        - 调整不符合规范的trade_record csv 文件: 删除包含空白的行,提取需要的列保存(2023-04-01)
        - 在init_trade_record_from_1991函数增加了保留需要的列功能后,这个函数可以废弃了.(2023-04-07)
        """
        
        # 打开现有的CSV文件,仅包括total-value数据
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        trade_csv_path = os.path.join(self.__trade_record_path, f'{stock_class}')
        trade_csv_file = os.path.join(trade_csv_path, f'{code}.csv')
        trade_record_df = pd.read_csv(trade_csv_file)

        # 删除空行
        has_null = trade_record_df.isnull().any(axis=1)
        # print(trade_record_df[has_null])
        trade_record_df.dropna(inplace=True)

        # 取出需要的内容后保存目标文件
        standard_columns = ['日期', '股票代码', '名称', '总市值', 'PB', 'PE', 'PS', 'PC']
        ndf = trade_record_df[standard_columns]
        ndf.to_csv(trade_csv_file, index=False)

        print(f'{code}.csv调整完成')


    def check_trade_record_csv(self, code: str) -> Union[str, Tuple]:
        """ 
        - 检查历史交易记录csv文件的格式.
        - 包括文件列名是否相符,总市值\pb\pe\ps\pc\dividend是否含有空值.
        - 每一列的数据格式是否符合规范:日期列符合yyyy-mm-dd型格式,股票代码6为代码,不含后缀.
        - PE PB PS PC DIVIDEND格式均为float型.

        - 无误返回OK,其他则返回错误提示.当日期和代码列出错时,返回错误提示和行号.(2023-04-07)

        - 今日增加了DIVIDEND信息检查内容.(2023-04-23)
        """

        standard_columns = ['日期', '股票代码', '名称', '总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND']
        # 打开现有的CSV文件
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        trade_csv_path = os.path.join(self.__trade_record_path, f'{stock_class}')
        trade_csv_file = os.path.join(trade_csv_path, f'{code}.csv')
        trade_record_df = pd.read_csv(trade_csv_file)

        if list(trade_record_df.columns) != standard_columns:
            return 'columns-error'
        elif trade_record_df['总市值'].isnull().any():
            return 'tvalue-empty'
        elif trade_record_df['PE'].isnull().any():
            return 'pe-empty'
        elif trade_record_df['PB'].isnull().any():
            return 'pb-empty'
        elif trade_record_df['PS'].isnull().any():
            return 'ps- empty'
        elif trade_record_df['PC'].isnull().any():
            return 'pc-empty'
        elif trade_record_df['DIVIDEND'].isnull().any():
            return 'dividend-empty'

        date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        code_regex = re.compile(r"^\'\d{6}$")
        for index, row in trade_record_df.iterrows():
            if not date_regex.match(row['日期'].strip(' ')):
                return 'date-format-error', index + 2
            if not code_regex.match(row['股票代码'].strip(' ')):
                return 'code-format-error', index + 2
        
        if 'float' not in str(trade_record_df['PB'].dtype):
            return 'pb-dtype-error'
        if 'float' not in str(trade_record_df['PE'].dtype):
            return 'pe-dtype-error'
        if 'float' not in str(trade_record_df['PS'].dtype):
            return 'ps-dtype-error'
        if 'float' not in str(trade_record_df['PC'].dtype):
            return 'pc-dtype-error'
        if 'float' not in str(trade_record_df['DIVIDEND'].dtype):
            return 'dividend-dtype-error'

        return 'ok'


    @staticmethod
    def date_to_timestamp(date: str) -> int:
        """ 将yyyy-mm-dd型字符创转化为13位时间戳 """

        struct_time = time.strptime(date, "%Y-%m-%d")
        time_stamp = time.mktime(struct_time)*1000

        return int(time_stamp)


    def download_financial_indicator_from_xueqiu(self, code: str, count: int, type: str) -> Dict:
        """
        - 下载股票的财务指标信息.
        - code:股票代码;count:数据期数
        - type:财务指标类型Q1、Q2、Q3、Q4、all,分别代表第一二三四季度及全部期间

        - 返回一个字典,包含财务指标信息.
        """

        print(f'正在下载{code} 主要财务指标信息......'+'\r', end='', flush=True)
        url = 'https://stock.xueqiu.com/v5/stock/finance/cn/indicator.json'
        params = {
            'symbol': f'sh{code}' if code.startswith('6') else f'sz{code}',
            'type': f'{type}',
            'is_detail': 'true',
            'count': f'{count}',
            'timestamp': ''
        }
        
        if not self.__xueqiu_cookie_existed:
            self.__xueqiu_session.get(url='https://xueqiu.com/', headers=self.__headers_xueqiu)
            self.__xueqiu_cookie_existed = True
        response = self.__xueqiu_session.get(url=url, headers=self.__headers_xueqiu, params=params)
        return response.json()


    def download_balance_sheet_from_xueqiu(self, code: str, count: int, type: str) -> Dict:
        """
        - 下载资产负债表
        - code:股票代码;count:数据期数
        - type:财务指标类型Q1、Q2、Q3、Q4、all,分别代表第一二三四季度及全部期间

        - 返回一个字典,包含资产负债表信息.
        """

        url = 'https://stock.xueqiu.com/v5/stock/finance/cn/balance.json'
        params = {
            'symbol': f'sh{code}' if code.startswith('6') else f'sz{code}',
            'type': f'{type}',
            'is_detail': 'true',
            'count': f'{count}',
            'timestamp': ''
        }

        if not self.__xueqiu_cookie_existed:
            self.__xueqiu_session.get(url='https://xueqiu.com/', headers=self.__headers_xueqiu)
            self.__xueqiu_cookie_existed = True
        response = self.__xueqiu_session.get(url=url, params=params, headers=self.__headers_xueqiu)
        print(f'正在下载{code} 资产负债表信息......'+'\r', end='', flush=True)

        return response.json()


    def download_cashflow_statement_from_xueqiu(self, code: str, count: int, type: str) -> Dict:
        """
        - 下载现金流量表
        - code:股票代码;count:数据期数
        - type:财务指标类型Q1、Q2、Q3、Q4、all,分别代表第一二三四季度及全部期间

        - 返回一个字典,包含现金流量表信息.
        """

        url = 'https://stock.xueqiu.com/v5/stock/finance/cn/cash_flow.json'
        params = {
            'symbol': f'sh{code}' if code.startswith('6') else f'sz{code}',
            'type': f'{type}',
            'is_detail': 'true',
            'count': f'{count}',
            'timestamp': ''
        }

        if not self.__xueqiu_cookie_existed:
            self.__xueqiu_session.get(url='https://xueqiu.com/', headers=self.__headers_xueqiu)
            self.__xueqiu_cookie_existed = True
        response = self.__xueqiu_session.get(url=url, params=params, headers=self.__headers_xueqiu)

        print(f'正在下载{code} 现金流量表信息......'+'\r', end='', flush=True)

        return response.json()


    def download_history_dividend_record_from_10jqka(self, code: str) -> Dict:
        """
        - 从同花顺下载股票的全部历史分红记录.这个方法写于2023-04-23日,是为了在CSV文件中增加分红信息.
        现有CSV文件包含8列内容:['日期', '股票代码', '名称', '总市值', 'PB', 'PE', 'PS', 'PC'],
        但是,这些数据并不包含分红信息,所以需要从同花顺下载分红信息,并将其合并到CSV文件中.

        :param code: 股票代码,不含后缀.

        :return: 返回一个字典,结构为{'2022-07-08': ['10派15.22元(含税)', '4.17%'], ...},
        键为时间组,记录分红实施日期,值为列表,第一项为分红方案,第二项为税前分红比例.
        税前分红比例应为百分比型的字符串,但是有些返回值为'--',需要特别处理.(2023-04-23)
        """

        url = f'http://basic.10jqka.com.cn/{code}/bonus.html'

        if not self.__10jqka_cookie_existed:
            self.__10jqka_session.get(url='http://basic.10jqka.com.cn/', headers=self.__headers_10jqka)
            self.__10jqka_cookie_existed = True
        response = self.__10jqka_session.get(url=url, headers=self.__headers_10jqka)
        response.encoding = 'gbk'
        df = pd.read_html(response.text)[0]

        result = {}  # 定义返回值
        pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        for index, row in df.iterrows():
            if pattern.match(row['实施公告日']):
                result[row['实施公告日']] = [row['分红方案说明'], row['税前分红率']]
        
        return result


    def add_dividend_rate_to_CSV(self, code: str, dividend: Dict) -> None:
        """
        - 将分红信息添加到CSV文件中.这个方法写于2023-04-23日,是为了在CSV文件中增加分红信息.
        按照如下程序,将分红信息添加到CSV文件中:
        - 打开CSV文件,如果没有分红信息 DIVIDEND 列,则添加该列.
        - 遍历参数dividend, 先把第一项分红率插入到对应的日期,然后逐日向上根据市值的变化折算对应分红率插入.
        再把第二项插入对应的日期,然后逐日向上根据市值的变化折算对应分红率插入,直到前一项最后的日期行为止,依次类推.
        - 如果没有对应的日期,则插入到最接近的日期所在的行.
        - 如果某项分红率为'--',则视为0.00.
        - 本方法是通过折算的方式间接获取分红信息,在CSV文件已经添加了DIVIDEND列以后,其余的分红信息通过update_...更新会更准确.
        - 向上折算填充分红信息时,最多不超过一年,如果超过一年,则不填充.

        - TODO: 一年内多次分红的情况没有专门的处理程序.

        :param code: 股票代码,不含后缀.
        :param dividend: 分红信息,self.download_history_dividend_record_from_10jqka()方法的返回值.

        :return: None(2023-04-23)
        """
        # 打开CSV文件
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        trade_csv_path = os.path.join(self.__trade_record_path, f'{stock_class}')
        trade_csv_file = os.path.join(trade_csv_path, f'{code}.csv')
        trade_record_df = pd.read_csv(trade_csv_file)

        # 如果没有分红信息 DIVIDEND 列,则添加该列,否则返回.
        if 'DIVIDEND' not in trade_record_df.columns:
            trade_record_df['DIVIDEND'] = 0.00
        # 如果屏蔽以下两行,则每次调用该函数都会全部重新计算一次.
        else:
            return
        
        # 定义一个函数计算两个yyyy-mm-dd型字符串日期之间的天数
        def days_between(date1: str, date2: str) -> int:
            date1 = datetime.datetime.strptime(date1, "%Y-%m-%d")
            date2 = datetime.datetime.strptime(date2, "%Y-%m-%d")
            return (date2 - date1).days

        # 遍历参数dividend
        date_list = trade_record_df['日期'].tolist()
        all_row_index = [0]  # 记录全部日期行的索引
        for date, value in dividend.items():
            # 获取date所在行的索引或者最接近的日期所在的行的索引
            if date in date_list:  # 如果有对应的日期,则插入到对应的日期所在的行.
                index = date_list.index(date)
            else:  # 如果没有对应的日期,则插入到最接近的日期所在的行.
                index = date_list.index(min(date_list, key=lambda x: abs(days_between(date, x))))
            
            # 获取index所在行的市值
            market_value = trade_record_df.loc[index, '总市值']
            # 提取分红率后插入index行的DIVIDEND列
            if '%' not in value[1]:  # 如果某项分红率为'--',则视为0.00.
                trade_record_df.loc[index, 'DIVIDEND'] = 0.00
            else:
                trade_record_df.loc[index, 'DIVIDEND'] = float(value[1].strip('%'))
            
            # 逐日向上根据市值的变化折算对应分红率插入
            all_row_index.append(index)  # 记录全部日期行的索引,倒数第二行为上组最后一项的日期行的索引
            pre_pos = all_row_index[-2]  # 上组最后一项的日期行的索引

            # 如果index-pre_pos大于365,则调整pre_pos的值,使得index-pre_pos不大于365
            if index - pre_pos > 365:
                pre_pos = index - 365

            for i in range(pre_pos, index):
                dividend_value = round(trade_record_df.loc[index, 'DIVIDEND'] * ( trade_record_df.loc[i, '总市值']/market_value), 2)
                trade_record_df.loc[i, 'DIVIDEND'] = dividend_value

        # 保存CSV文件
        trade_record_df.to_csv(trade_csv_file, index=False)


    def download_period_statistic_value_from_xueqiu(self, code: str, begin: str, end: str):
        """ 
        - 从雪球下载指定期间的股票涨跌幅统计数据, 类型为向前复权.code为不含后缀的代码.
        - begin和end为yyyy-mm-dd日期型字符串.(2023-02-...)
        - 如果需要获取指数的区间数据,和一般股票相比,地址是统一的,参数需要小的调整.
        我最常用的指数是沪深300(SH000300),创业板(SZ399006)和中证500(SH000905)三个指数.(2023-04-11)
        """

        url = "https://stock.xueqiu.com/v5/stock/chart/kline.json"

        # 补齐代码前缀
        index_list = ['000300', '399006', '000905']  # 三个常用指数列表
        if code in index_list:
            symbol = f'SH{code}' if code.startswith('0') else f'SZ{code}'  # 三个指数
        else:
            symbol = f'SH{code}' if code.startswith('6') else f'SZ{code}'  # 一般股票
        
        # 参数
        begin_stamp = self.date_to_timestamp(begin)
        end_stamp = self.date_to_timestamp(end)
        params = {
            'symbol': f'{symbol}',
            'begin': str(begin_stamp),
            'end': str(end_stamp),
            'period': 'day',
            'type': 'before',
            'indicator': 'kline'
        }

        if not self.__xueqiu_cookie_existed:
            self.__xueqiu_session.get(url='https://xueqiu.com/', headers=self.__headers_xueqiu)
            self.__xueqiu_cookie_existed = True
        return self.__xueqiu_session.get(url=url, headers=self.__headers_xueqiu, params=params).json()


    def download_trade_record_from_163(self, code:str):
        """ 
        从网易财经下载股票历史交易记录,仅包含总市值数据 
        包括从上市日到最近一个交易日的所有记录
        注:163似乎已经关闭了这个下载通道(2023-04-02)
        """

        # 准备下载目录
        industry_class = self.get_name_and_class_by_code(code=code)[1]
        download_path = os.path.join(self.__trade_record_path, industry_class)
        if not os.path.exists(download_path):
            os.mkdir(download_path)
        file_name = os.path.join(download_path, f'{code}.csv')

        # 准备下载参数
        url = f'http://quotes.money.163.com/f10/gszl_{code}.html'
        response = requests.get(url=url, headers=self.__headers_163)
        table_list = pd.read_html(response.text)
        start_date = table_list[4].iloc[1, 1].replace('-', '')
        end_date = str(datetime.date.today()).replace('-', '')

        # 下载历史交易数据,该网址在文件下载后出现在网络调试面板中,很难找到
        url = 'http://quotes.money.163.com/service/chddata.html'
        params = {
            'code': f'0{code}' if code.startswith('6') else f'1{code}',
            'start': start_date,
            'end': end_date,
            'fields': 'TCAP',  # 总市值参数
        }
        response = requests.get(url=url, params=params, headers=self.__headers_163)
        response.encoding = 'gbk'  # 解码公司简称中文字符
        with open(f'{file_name}', 'w') as file:
            file.write(response.text)
        print(f'{code}.csv历史记录已经下载完毕.', end='', flush=True)


    def download_year_PDF_report_from_cninfo(self, code:str, year:int):
        """ 从巨潮资讯网页面下载企业财务报表, year为下载的年份, code为股票代码 """

        # 检查目录中是否已经存在相应的报表文件
        file_name = os.path.join(self.__finance_report_path, f'{code}-{year}.PDF')
        if os.path.exists(file_name):
            print(f'{code} - {year} 年报文件已经存在,无需重新下载')
            return

        # 获取年报文件信息JSON数据
        url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
        df = self.__cninfo_stock_list
        orgId = df.loc[df['code'] == code].iloc[0, 2]
        data = {
            'stock': f"{code},{orgId}",
            'tabName': 'fulltext',
            'pageSize': '10',  # 年报份数
            'pageNum': '1',
            'column': 'sse' if code.startswith('6') else 'szse',
            'category': 'category_ndbg_szsh',
            'plate': 'sh' if code.startswith('6') else 'sz',
            'seDate': '', 
            'searchkey': '', 
            'secid': '', 
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'
        }
        response = requests.post(url=url, data=data, headers=self.__headers_cninfo)
        result = response.json()['announcements']  # 保存了年报文件信息列表

        # 获取年度报告文件url
        pdf_url = None
        for item in result:
            if (str(year) in item['announcementTitle']) and ('英文' not in item['announcementTitle']):  # 剔除英文版
                pdf_url = 'http://static.cninfo.com.cn/' + item['adjunctUrl']
                break
        
        if pdf_url is None:
            print('未发现指定年份财务报告......')
            return
        
        # 下载并保存年度报告
        print(f'正在下载 {code} - {year} 年报......')
        content = requests.get(url=pdf_url).content
        with open(file_name, 'wb') as file:
            file.write(content)
    

    def download_year_PDF_report_from_sina(self, code: str, year: int) -> None:
        """从新浪财经页面上下载企业财务报表,year为下载的年份,code为股票代码"""

        # 1 检查目录中是否已经存在相应的报表文件
        file_name = os.path.join(self.__finance_report_path, f'{code}-{year}.PDF')
        if os.path.exists(file_name):
            print(f'{code} - {year} 年报文件已经存在,无需重新下载')
            return

        # 2 如果不存在相应的年报文件,首先获取年报标题列表
        url = f'https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_Bulletin/stockid/{code}/page_type/ndbg.phtml'
        response = requests.get(url=url)
        soup = BeautifulSoup(response.text, 'html.parser')
        a_list = soup.select(selector='#con02-7 > table:nth-child(3) ul a')  # 各年年报链接标签

        # 3 获取指定年份年报页面地址
        url_tmp = None
        for a in a_list:
            if (str(year) in a.text) and ('英文' not in a.text):  # 剔除英文版
                url_tmp = 'https://vip.stock.finance.sina.com.cn/' + a['href']  # 指定年份年报地址
                break
        if not url_tmp:
            print('未找到对应年份年报地址......')
            return

        # 4 获取指定年份的PDF文档地址
        if not self.__sina_cookie_existed:
            self.__sina_session.get(url='https://finance.sina.com.cn')  # 获取cookie
            self.__sina_cookie_existed = True
        response = self.__sina_session.get(url=url_tmp, headers=self.__headers_sina)
        soup = BeautifulSoup(response.text, 'html.parser')
        pdf_a = soup.select(selector='#allbulletin > thead > tr > th > font a')[0]

        # 5 下载pdf文件至指定目录
        print(f'正在下载{code} - {year} 年报......')
        url = pdf_a['href']
        content = requests.get(url=url).content
        with open(file_name, 'wb') as f:
            f.write(content)


    def get_all_stocks_rising_value_ranks(self, start_date_str: str, end_date_str: str):
        """  
        返回全部股票指定期间内涨幅排名(按照降序排名),返回值包括股票代码和期间涨幅。
        这个函数是对全部股票进行搜索计算,消耗较大, 一般不要使用。
        """

        result = []

        stock_classes = self.get_stock_classes()
        for clas in stock_classes:
            tmp = self.get_stocks_of_specific_class(clas)
            code_list = [item[0][:6] for item in tmp]

            args_list = []  # 准备参数列表
            for item in code_list:
                args = [item, start_date_str, end_date_str]
                args_list.append(args)
            
            with ThreadPoolExecutor() as pool:
                value = pool.map(self.calculate_period_rising_value, args_list)

            for item in zip(code_list, value):  # 组合代码和涨跌幅
                result.append(item)
        
        result = sorted(result, key=lambda x: x[1], reverse=True)  # 降序排列

        return result


    @staticmethod
    def get_closest_date_position(camp_timestamp: int, target_list: list) -> int:
        """ 
        从包含时间戳的列表项目找出和目标时间戳最接近的位置
        本函数中, 时间戳在每一个列表项的第一个位置
        第一个参数为要寻找的时间戳, 13位. 第二个参数为在其中寻找的列表.
        """

        tmp_list = list(map(lambda x: abs(x[0] - camp_timestamp), target_list))

        return tmp_list.index(min(tmp_list))


    def get_stock_classes(self) -> List:
        """获取申万行业分类清单"""

        return self.__sw_stock_list['新版一级行业'].unique().tolist()
        

    def get_stock_list_from_cninfo(self) -> DataFrame:
        """ 
        从巨潮资讯网获取全部取票code pinyin categary orgId 和 zwjc 
        用于下载股票年报用
        """

        url = "http://www.cninfo.com.cn/new/data/szse_stock.json"
        response = requests.get(url=url)
        stock_json = response.json()
        df = pd.DataFrame(data=stock_json['stockList'])

        return df


    def get_latest_record_date(self):
        """ 从历史交易记录文件查找最新的日期,查找的位置为self.__trade_record_path下第一个文件夹中的第一个文件 """
        target_path = os.path.join(self.__trade_record_path, self.get_stock_classes()[0])
        target_file = os.path.join(target_path, os.listdir(target_path)[0])
        tmp_df = pd.read_csv(target_file, usecols=['日期'])

        return tmp_df.loc[0, '日期']


    def get_init_roe_condition_value(self) -> List:
        """ 获取初始选股条件 """

        return self.__init_roe_condition_value


    def get_name_and_class_by_code(self, code: str) -> List:
        """ 通过股票代码获取公司简称及行业分类 """

        df = self.__sw_stock_list
        code = code + '.SH' if code.startswith('6') else code + '.SZ'
        tmp = df.loc[df['股票代码'] == code]  # 选出股票所在的行
        result = ['错误', '错误'] if tmp.empty else tmp.loc[tmp.index[0], ['公司简称', '新版一级行业']].values.tolist()

        return result


    def get_pushing_message(self) -> Dict:
        """ 
        获取推送的股票消息, 返回的内容推送给微信
        通过ROE选出合适的股票, 在其中随机抽取3个股票, 提取股票的估值信息
        """

        message = {
            'condition': None,
            'result': None,
        }
        conditon_list = [ [8,] * 7, [12, ] * 7, [15, ] * 7 ]
        message['condition'] = random.choice(conditon_list)

        con = sqlite3.connect(INDICATOR_SQLITE3)
        with con:
            # 获取数据库数字型字段
            sql = """ PRAGMA TABLE_INFO('roe-all-stocks') """
            fileds = con.execute(sql).fetchall()

            roe_fields = []
            for filed in fileds:
                if ('stock' not in filed[1]) and ('Q2' not in filed[1]):  # 只选取年度字段
                    roe_fields.append(filed[1])
            roe_fields = sorted(roe_fields, reverse=True)
            
            # 生成筛选语句
            sql = """ SELECT stockcode, stockname, stockclass FROM 'roe-all-stocks' WHERE """
            for index, filed in enumerate(roe_fields[0:7]):
                if index == 6:
                    sql += f""" {filed}>=? """
                else:
                    sql += f""" {filed}>=? and """

            # 查询结果
            tmp = con.execute(sql, message['condition']).fetchall()
            message['result'] = random.sample(tmp, 3)

        return message


    def get_stock_dividend_rate_from_xueqiu(self, code: str):
        """ 从雪球网获取股票分红率数据 """

        dividend_rate = 0.00
        if code.startswith('6'):
            url = f"https://xueqiu.com/S/SH{code}"
        else:
            url = f"https://xueqiu.com/S/SZ{code}"   
        
        if not self.__xueqiu_cookie_existed:
            self.__xueqiu_session.get(url='https://xueqiu.com/', headers=self.__headers_xueqiu)
            self.__xueqiu_cookie_existed = True
        response = self.__xueqiu_session.get(url=url, headers=self.__headers_xueqiu)

        df_list = pd.read_html(response.text)
        info_df: DataFrame = df_list[0]
        for index, row in info_df.iterrows():
            for item in row:
                if isinstance(item, str) and ('股息率' in item):
                    pattern = r'\d*\.?\d+'
                    didivend_list = re.findall(pattern=pattern, string=item)
                    try:
                        if didivend_list:
                            dividend_rate = float(didivend_list[0])
                    except ValueError:
                        ...

        return dividend_rate


    def get_stocks_of_specific_class(self, stock_class: str) -> List:
        """获取stock_class指定的行业下上交所和深交所股票代码 公司简称 行业分类"""

        df = self.__sw_stock_list
        tmp = df.loc[df['新版一级行业'] == stock_class]  # 选出类所在的若干行
        # 剔除上交所深交所以外的股票
        criterion = tmp['股票代码'].map(lambda x: ('.SZ' in x) or ('.SH' in x))  
        result = tmp[criterion][['股票代码', '公司简称', '新版一级行业']].values.tolist()

        return result

    def get_stock_pb_from_xueqiu(self, code: str) -> float:
        """ 从雪球网获取股票PB数据 """

        stock_pb = 0.00

        if code.startswith('6'):
            url = f"https://xueqiu.com/S/SH{code}"
        else:
            url = f"https://xueqiu.com/S/SZ{code}"  

        if not self.__xueqiu_cookie_existed:
            self.__xueqiu_session.get(url='https://xueqiu.com/', headers=self.__headers_xueqiu)
            self.__xueqiu_cookie_existed = True
        response = self.__xueqiu_session.get(url=url, headers=self.__headers_xueqiu)

        # pd.read_html 方法
        df_list = pd.read_html(response.text)
        info_df: DataFrame = df_list[0]
        for index, row in info_df.iterrows():
            for item in row:
                if isinstance(item, str) and ('市净率' in item):
                    pattern = r'\d*\.?\d+'
                    pb_list = re.findall(pattern=pattern, string=item)
                    try:
                        if pb_list:
                            stock_pb = float(pb_list[0])
                    except ValueError:
                        stock_pb = 0

        return stock_pb

    
    def get_stock_total_value_from_sina(self, code: str) -> float:
        """ 使用新浪财经接口获取股票总市值 """

        total_value = 0.00

        if code.startswith('6'):
            url = f'http://qt.gtimg.cn/q=sh{code}'
        else:
            url = f'http://qt.gtimg.cn/q=sz{code}'
        
        if not self.__sina_cookie_existed:
            self.__sina_session.get(url='https://finance.sina.com.cn', headers=self.__headers_sina)
            self.__sina_cookie_existed = True
        response = self.__sina_session.get(url=url, headers=self.__headers_sina)

        try:
            total_value = float(response.text[11:].split('~')[45])*10e7
        except:
            ...
        
        return total_value


    def get_stock_PB_from_sina(self, code: str) -> float:
        """ 使用新浪财经接口获取股票市净率 """

        pb = 0.00

        if code.startswith('6'):
            url = f'http://qt.gtimg.cn/q=sh{code}'
        else:
            url = f'http://qt.gtimg.cn/q=sz{code}'
        
        if not self.__sina_cookie_existed:
            self.__sina_session.get(url='https://finance.sina.com.cn', headers=self.__headers_sina)
            self.__sina_cookie_existed = True
        response = self.__sina_session.get(url=url, headers=self.__headers_sina)

        try:
            pb = float(response.text[11:].split('~')[46])
        except:
            ...
        
        return pb


    def get_stock_PC_from_xueqiu_and_sina(self, code: str):
        """
        通过雪球和新浪获取经营现金净流量和总市值,间接获取股票市现率.
        PC(市现率) = 总市值/经营活动净流量(上一年度).金融行业的该指标如何运用?(2023-04-05)
        """

        pc = 0.00

        # 获取总市值和经营活动净流量noc
        tvalue = self.get_stock_total_value_from_sina(code=code)
        tmp = self.download_cashflow_statement_from_xueqiu(code=code, count=1, type='Q4')
        noc = tmp['data']['list'][0]['ncf_from_oa'][0]

        if noc:
            pc = round(tvalue/noc, 4)

        return pc


    def get_stock_PS_from_xueqiu_and_sina(self, code: str):
        """ 
        通过雪球获取市销率数据,无法直接获取。从新浪获取总市值,从雪球获取上年度总营收,
        通过市销率计算公司总市值/总营收间接获取.(2023-04-05)
        """
        ps = 0.00

        # 获取总市值和总营收
        tvalue = self.get_stock_total_value_from_sina(code=code)
        tmp = self.download_financial_indicator_from_xueqiu(code=code, count=1, type='Q4')
        total_revenue = tmp['data']['list'][0]['total_revenue'][0]

        if total_revenue:
            ps = round(tvalue/total_revenue, 4)

        return ps


    def get_stock_PE_from_sina(self, code: str) -> float:
        """ 使用新浪财经接口获取股票静态市盈率 """

        pe = 0.00

        if code.startswith('6'):
            url = f'http://qt.gtimg.cn/q=sh{code}'
        else:
            url = f'http://qt.gtimg.cn/q=sz{code}'
        
        if not self.__sina_cookie_existed:
            self.__sina_session.get(url='https://finance.sina.com.cn', headers=self.__headers_sina)
            self.__sina_cookie_existed = True
        response = self.__sina_session.get(url=url, headers=self.__headers_sina)

        try:
            pe = float(response.text[11:].split('~')[53])
        except:
            ...
        
        return pe


    def get_yield_data_from_china_bond(self, date_str: str) -> float:
        """ 从chinabond中债信息网获取指定日期10年期国债到期收益率表格,参数date_str格式为yyyy-mm-dd """
        
        curve_value = 0

        url = "https://yield.chinabond.com.cn/cbweb-cbrc-web/cbrc/queryGjqxInfo"
        data = {
            'workTime': date_str,
            'locale': 'cn_ZH',
        }
        time.sleep(0.05)
        response = requests.post(url=url, headers=self.__headers_chinabond, data=data)

        try:
            df_list = pd.read_html(io=response.text)
            curve_value = float(df_list[0].loc[0, '10年'])
        except KeyError:
            ...
        
        return curve_value


    def init_5_years_cashflow_to_profit_table(self, code_year_args: Tuple[str, int]):
        """ 
        获取上去年开始最近5年现金流量净利润比率并插入数据表.

        :param code_year_args: 股票代码(不含后缀)和年份(上一年的年份数字)元组,创建的表名称为year-4-year的表.
        :return: None
        """

        code = code_year_args[0]
        year = code_year_args[1]
        table_name = f'{year-4}-{year}'

        tmp = self.get_name_and_class_by_code(code=code)
        stock_name = tmp[0]
        stock_class = tmp[1]
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = [stock_code, stock_name, stock_class]

        con = sqlite3.connect(CASHFLOW_PROFIT_SQLITE3)
        with con:
            # 创建5年表的sql语句
            sql = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}' (
            stockcode TEXT NOT NULL PRIMARY KEY,
            stockname TEXT NOT NULL,
            stockclass TEXT NOT NULL,
            cash_to_profit REAL DEFAULT 0
            );
            """
            con.execute(sql)  # 创建5年表

            result = self.calculate_5_years_cashflow_to_profit(code=code)
            update_list.append(result)
            sql = f""" INSERT INTO '{table_name}' VALUES (?,?,?,?)  """
            try:
                print(f'正在插入{stock_code}-{stock_name}-{stock_class}现金流量/净利润比率数据'+'\r', end='', flush=True)
                con.execute(sql, tuple(update_list))
            except sqlite3.IntegrityError:  # 如果重复插入，略过
                ...


    def init_roe_table(self, code: str):
        """ 
        初始化股票10期ROE年度数据和2022年半年ROE数据并插入roe-all-stocks表
        这个函数是在2022年11月份建立数据库时使用的,在其他时间段使用需要做一定的代码调整.(2023-04-07)
        """

        tmp = self.get_name_and_class_by_code(code=code)
        stock_name = tmp[0]
        stock_class = tmp[1]
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = [stock_code, stock_name, stock_class]

        sql_file = os.path.join(self.__sql_path, 'roe.sql')
        con = sqlite3.connect(INDICATOR_SQLITE3)
        with con:
            with open(sql_file, 'r') as f:
                script = f.read()
                con.executescript(script)  # 创建ROE表

            # 获取2022年半年ROE数据，因本函数是2022年11月份完成的
            result = self.download_financial_indicator_from_xueqiu(code=code, count=1, type='Q2')
            for content in result['data']['list']:
                update_list.append(content['avg_roe'][0])

            # 获取10期ROE数据
            result = self.download_financial_indicator_from_xueqiu(code=code, count=10, type='Q4')
            for content in result['data']['list']:  # 遍历获取每年roe数据
                update_list.append(content['avg_roe'][0])
                
            if len(update_list) < 14:
                for index in range(14-len(update_list)):
                    update_list.append(0)  # 如果不足14列数据，补齐至14列

            sql = """ INSERT INTO 'roe-all-stocks' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)  """
            try:
                print(f'正在插入 {stock_code} - {stock_name} - {stock_class} ROE 数据......')
                con.execute(sql, tuple(update_list))
            except sqlite3.IntegrityError:  # 如果重复插入，略过
                ...


    def init_roe_table_from_1991(self, code: str): 
        """ 
        初始化从1991年以来的年度ROE数据库,半年更新一次.
        这个函数从2022年至1991年年度roe数据,在2023年3月份建立该数据库.
        如果是其他时间段再来重建数据库,这个函数不能简单的套用,需要做一定的调整.(2023-04-07s)
        """

        # 准备前3列
        tmp = self.get_name_and_class_by_code(code=code)
        stock_name = tmp[0]
        stock_class = tmp[1]
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = [stock_code, stock_name, stock_class]

        # 计算从1991-01-01至今天需要下载的期数
        today = datetime.datetime.now().today()
        start = datetime.datetime.strptime('1991-01-01', '%Y-%m-%d')
        days = (today-start).days
        count_season = int(days/365*4)
        count_year = int(days/365)
        
        # 打开数据库
        sql_file = os.path.join(self.__sql_path, 'roe-from-1991.sql')
        con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
        with con:
            # 创建ROE表
            with open(sql_file, 'r') as f:
                script = f.read()
                con.executescript(script)  

            # 获取期间ROE数据,如果没有公布2022年度roe数据,则以0填充
            result = self.download_financial_indicator_from_xueqiu(code=code, count=count_year, type='Q4')  # 覆盖全部
            if result['data']['last_report_name'] != '2022年报':
                update_list.append(0.00)
            for content in result['data']['list']:  # 遍历获取每年roe数据
                update_list.append(content['avg_roe'][0])

            if len(update_list) > 35:
                update_list = update_list[0:35]

            if len(update_list) < 35:  # 数据库初始表的列数
                for index in range(35-len(update_list)):
                    update_list.append(0)  # 如果不足35列数据，补齐至35列

            sql = """ INSERT INTO 'roe-all-stocks-from-1991' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  """
            try:
                print(f'正在插入 {stock_code} - {stock_name} - {stock_class} ROE 数据......')
                con.execute(sql, tuple(update_list))
            except sqlite3.IntegrityError:  # 如果重复插入，略过
                ...


    def init_curve_value_table(self, days: int):
        """
        获取10年期国债到期收益率插入yield-curve表中;
        插入的期间从昨天起向前推days天数.
        数据从2006-03-01开始.(2023-04-08)
        """

        yesterday = datetime.date.today() + datetime.timedelta(days=-1)
        begin = yesterday + datetime.timedelta(days=-days)
        date_list = pd.date_range(begin, yesterday)
        date_str = [str(date)[0:10] for date in date_list]  # 生成日期序列

        with ThreadPoolExecutor() as pool:
            value_list = pool.map(self.get_yield_data_from_china_bond, date_str)

        con = sqlite3.connect(CURVE_SQLITE3)
        with con:
            with open(os.path.join(self.__sql_path, 'yield-curve.sql'), 'r') as f:
                script = f.read()
                con.executescript(script)  # 创建yield-curve表格

            sql = """select * from 'yield-curve' """
            df = pd.read_sql_query(sql, con)

            for date, value in zip(date_str, value_list):  # 在第一行插入日期和值，遇重复日期，略过
                df.loc[-1] = [date, value]
                df.index += 1
                df = df.sort_index()

            # 删除重复行后保存数据
            df.drop_duplicates(subset=['date1'], keep='last', inplace=True)
            df.to_sql(name='yield-curve', con=con, index=False, if_exists='replace')


    def init_dividend_rate_table(self, code: str):
        """ 初始化股票现金股息率表 """

        # 准备插入的数据
        insert_list = []
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        insert_list.append(stock_code)
        tmp = self.get_name_and_class_by_code(code=code)
        for item in tmp:
            insert_list.append(item)

        dividend_rate = self.get_stock_dividend_rate_from_xueqiu(code=code)
        insert_list.append(dividend_rate)

        # 打开数据库，创建dividend-rate表，插入已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'dividend-rate.sql')
        con = sqlite3.connect(DIVIDEND_RATE_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建dividend-rate 表

            sql = """INSERT INTO 'dividend-rate' VALUES (?,?,?,?)"""
            try:
                con.execute(sql, tuple(insert_list))
                print(f'{stock_code}现金分红率已经初始化完成')
            except sqlite3.IntegrityError:
                ...

        
    def init_history_PB_table(self, code: str):
        """ 初始化股票历史PB表,包括最大最小和平均PB """

        # 准备插入的数据
        insert_list = []
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        insert_list.append(stock_code)
        tmp = self.get_name_and_class_by_code(code=code)
        for item in tmp:
            insert_list.append(item)
        
        pb_tup = self.calculate_MAX_MIN_MEAN_pb(code=code)
        for item in pb_tup:
            insert_list.append(item)
        
        # 打开数据库, 创建history-pb表格，插入已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'history-pb.sql')
        con = sqlite3.connect(HISTORY_PB_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建history-pb表

            sql = """ INSERT INTO 'history-pb' VALUES (?,?,?,?,?,?) """
            try:
                con.execute(sql, tuple(insert_list))
                print(f'{stock_code}历史PB初始化完成')
            except sqlite3.IntegrityError:
                ...


    def init_PE_PB_table(self, code: str):
        """ 初始化股票PE\PB表 """

        # 准备插入的数据
        insert_list = []
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        insert_list.append(stock_code)
        tmp = self.get_name_and_class_by_code(code=code)
        for item in tmp:
            insert_list.append(item)
        pe = self.get_stock_PE_from_sina(code=code)
        insert_list.append(pe)
        random.uniform(0.01, 0.15)
        pb = self.get_stock_pb_from_xueqiu(code=code)
        insert_list.append(pb)

        # 打开数据库, 创建pe-pb表格,插入已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'price-indicator.sql')
        con = sqlite3.connect(PE_PB_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建pe-pb表格
            sql = """ INSERT INTO 'pe-pb' VALUES (?, ?, ?, ?, ?) """
            try:
                con.execute(sql, tuple(insert_list))
                print(f'{stock_code}PE-PB初始化完成')
            except sqlite3.IntegrityError:
                ...


    def init_stock_total_value(self, code: str):
        """ 初始化股票总市值表, 每日更新 """

        # 准备插入的数据
        insert_list = []
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        insert_list.append(stock_code)
        tmp = self.get_name_and_class_by_code(code=code)
        for item in tmp:
            insert_list.append(item)

        random.uniform(0.01, 0.15)
        total_value = self.get_stock_total_value_from_sina(code=code)
        insert_list.append(total_value)

        # 打开数据库，创建total-value表，插入已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'total-value.sql')
        con = sqlite3.connect(TVALUE_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建total_value 表

            sql = """INSERT INTO 'total-value' VALUES (?,?,?,?)"""
            try:
                con.execute(sql, tuple(insert_list))
                print(f'{stock_code}总市值已经初始化完成')
            except sqlite3.IntegrityError:
                ...


    def init_trade_record_form_IPO(self, code: str) -> Union[str, None]:
        """ 
        - 在现有csv文件的基础上, 抓取股票自ipo以来的total-value\PE\PB\PS\PC...... csv格式文件
        这个函数是一次性的,用完了就不需要了. (2023-03-30)
        - 这个函数不是一次性,经常需要用到.还需要增加其他的指标:EV/EBITDA...,函数可能会显得过长了一点.(2023-04-04)
        - 终于在写创建PS数据列的时候,发现这个函数其实是有问题的.函数中要求逐行解析日期,当上年营收年度数据已经公布,则在本行填入上年营收数,
        否者填入前年总营收数据.这个地方就是有问题的,不应该在行中填入前年的营收数据,而应该等待年度数据全部发布后,再填入上年营收数.
        所以这个方法要在每年的4月30日以后执行才是可以的.这个错误在计算创建PE数据列的时候已经存在了,影响范围是当时尚未公布2022年度
        报告的公司数据,范围无法确定.不过也不能说完全错误,只是不太准确,影响100多行的数据吧.(2023-04-04)
        - 补写了update_trade_csv_at_date_row函数,可以按照具体日期更新数据,来打个补丁.(2023-04-07)

        - 今天在现CSV文件列的基础上,增加了DIVIDEND列,通过download_history_dividend_record_from_10jqka和add_dividend_rate_to_csv实现.(2023-04-23)
        """

        # 打开现有的CSV文件
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        trade_csv_path = os.path.join(self.__trade_record_path, f'{stock_class}')
        trade_csv_file = os.path.join(trade_csv_path, f'{code}.csv')
        trade_record_df = pd.read_csv(trade_csv_file)

        # 预处理 删除空行 格式转换
        trade_record_df = trade_record_df.dropna()
        if trade_record_df['总市值'].dtype == 'object':
            trade_record_df['总市值'].replace(to_replace='None', value='0', inplace=True)
            trade_record_df['总市值'] = trade_record_df['总市值'].astype('float64')

        # 计算从1991-01-01至今天需要下载的期数
        today = datetime.datetime.now().today()
        start = datetime.datetime.strptime('1991-01-01', '%Y-%m-%d')
        days = (today-start).days
        count_season = int(days/365*4)
        count_year = int(days/365)

        # 如果trade_record_df中还没有PB数据,则创建PB列数据
        if 'PB' not in trade_record_df.columns:
            ### 计算历史PB
            tmp = self.download_balance_sheet_from_xueqiu(code=code, count=count_season, type='all')
            net_assets = {}  # 储存年报名和净资产值
            for item in tmp['data']['list']:
                net_assets[item['report_name']] = item['total_quity_atsopc'][0]  # 获取年报名和净资产值键值对

            # 插入净资产列
            trade_record_df['净资产'] = 0.00
            for index, row in trade_record_df.iterrows():
                year_month_day_list = row['日期'].split('-')
                year = year_month_day_list[0]
                month = year_month_day_list[1]
                try:
                    if (int(month) in [1, 2, 3, 4]):
                        trade_record_df.loc[index, '净资产'] = net_assets[f'{int(year)-1}三季报']
                    elif (int(month) in [5, 6, 7, 8]):
                        trade_record_df.loc[index, '净资产'] = net_assets[f'{year}一季报']
                    elif (int(month) in [9, 10]):
                        trade_record_df.loc[index, '净资产'] = net_assets[f'{year}中报']
                    elif (int(month) in [11, 12]):
                        trade_record_df.loc[index, '净资产'] = net_assets[f'{year}三季报']
                except KeyError:
                    ...

            # 计算PB值
            trade_record_df = trade_record_df[trade_record_df['净资产']>0]  # 删除净资产小于等于0的行
            trade_record_df['净资产'] = trade_record_df['净资产'].astype('float64')
            trade_record_df['PB'] = trade_record_df['总市值'] / trade_record_df['净资产']

        # 如果trade_record_df中还没有PE数据,则创建PE列数据
        if 'PE' not in trade_record_df.columns:
            ### 计算历史PE
            tmp = self.download_financial_indicator_from_xueqiu(code=code, count=count_year, type='Q4')
            net_profit = {}  # 储存年报名和净资产值
            for item in tmp['data']['list']:
                net_profit[item['report_name']] = item['net_profit_atsopc'][0]  # 获取年报名和净资产值键值对

            # 插入净利润列
            trade_record_df['净利润'] = 0.00  
            for index, row in trade_record_df.iterrows():
                year_month_day_list = row['日期'].split('-')
                year = year_month_day_list[0]
                try:
                    if str(int(year)-1)+'年报' in net_profit.keys():
                        trade_record_df.loc[index, '净利润'] = net_profit[str(int(year)-1)+'年报']
                    else:
                        trade_record_df.loc[index, '净利润'] = net_profit[str(int(year)-2)+'年报']
                except KeyError:
                    ...

            # 计算PE值
            trade_record_df['净利润'] = trade_record_df['净利润'].astype('float64')
            trade_record_df['PE'] = trade_record_df['总市值'] / trade_record_df['净利润']

        # 如果trade_record_df中还没有PS数据,则创建PS列数据
        if 'PS' not in trade_record_df.columns:
            tmp = self.download_financial_indicator_from_xueqiu(code=code, count=count_year, type='Q4')
            total_revenue = {item['report_name']: item['total_revenue'][0] for item in tmp['data']['list']}  # 历史总营收

            # 插入总营收数据
            trade_record_df['总营收'] = 0.00
            for index, row in trade_record_df.iterrows():
                year_month_day_list = row['日期'].split('-')
                year = year_month_day_list[0]
                try:
                    if str(int(year)-1)+'年报' in total_revenue.keys():
                        trade_record_df.loc[index, '总营收'] = total_revenue[str(int(year)-1)+'年报']
                    else:
                        trade_record_df.loc[index, '总营收'] = total_revenue[str(int(year)-2)+'年报']
                except KeyError:
                    ...

            # 计算PS值
            trade_record_df['总营收'] = trade_record_df['总营收'].astype('float64')
            trade_record_df['PS'] = trade_record_df['总市值'] / trade_record_df['总营收']

        # 如果trade_record_df中还没有PC数据,则创建PC列数据
        if 'PC' not in trade_record_df.columns:
            tmp = self.download_cashflow_statement_from_xueqiu(code=code, count=count_year, type='Q4')
            noc_list = {item['report_name']: item['ncf_from_oa'][0] for item in tmp['data']['list']}  # 经营活动净流量

            # 插入总营收数据
            trade_record_df['经营活动净流量'] = 0.00
            for index, row in trade_record_df.iterrows():
                year_month_day_list = row['日期'].split('-')
                year = year_month_day_list[0]
                try:
                    if str(int(year)-1)+'年报' in noc_list.keys():
                        trade_record_df.loc[index, '经营活动净流量'] = noc_list[str(int(year)-1)+'年报']
                    else:
                        trade_record_df.loc[index, '经营活动净流量'] = noc_list[str(int(year)-2)+'年报']
                except KeyError:
                    ...

            # 计算PC值
            trade_record_df['经营活动净流量'] = trade_record_df['经营活动净流量'].astype('float64')
            trade_record_df['PC'] = trade_record_df['总市值'] / trade_record_df['经营活动净流量']

        # 添加DIVIDEND列
        dividend_dict = self.download_history_dividend_record_from_10jqka(code=code)
        self.add_dividend_rate_to_CSV(code=code, dividend=dividend_dict)

        # 保留需要的列,其余删除
        standard_columns = ['日期', '股票代码', '名称', '总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND']
        ndf = trade_record_df[standard_columns]

        # ['总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND'] 数字列保留两位小数
        ndf[['总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND']] = ndf[['总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND']].round(2)

        # 保存文件
        ndf.to_csv(trade_csv_file, index=False)


    def init_average_salary_to_table(self, code_year_args: Tuple[str, int]) -> None:
        """ 
        获取薪酬水平并加入到年份表中,参数code_year_args列表内容为code(不含后缀)和year(整数型)组成的元组.
        每年的薪酬水平创建一个表来保存,表名为salary-年份,如salary-2021表示2021年的薪酬水平表.(2023-05-11)

        :param code_year_args: 代码和年份组成的元组
        :return: None
        """

        # 分解参数
        code = code_year_args[0]
        year = code_year_args[1]
        table_name = 'salary-' + str(year)  # 执行前需在sql文件中增加创建年度表的语句

        # 获取股票名称和类别
        tmp = self.get_name_and_class_by_code(code=code)
        stock_name = tmp[0]
        stock_class = tmp[1]

        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        insert_list = [stock_code, stock_name, stock_class]

        con = sqlite3.connect(SALARY_SQLITE3)
        with con:
            # 创建年度表,如果已经存在则不创建
            sql = f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                stockcode TEXT NOT NULL PRIMARY KEY,
                stockname TEXT NOT NULL,
                stockclass TEXT NOT NULL,
                employee REAL NOT NULL,
                paid_salary REAL NOT NULL DEFAULT 0,
                average_salary REAL NOT NULL DEFAULT 0
            )        
            """
            con.execute(sql)  # 创建年度表

            salary_list = self.calculate_average_salary(code=code)
            for item in salary_list:
                insert_list.append(item)

            sql = f""" INSERT INTO "{table_name}" VALUES (?,?,?,?,?,?)  """
            try:
                print(f'正在插入 {stock_code} - {stock_name} - {stock_class} 薪酬数据......'+'\r', end='', flush=True)
                con.execute(sql, tuple(insert_list))
            except sqlite3.IntegrityError:  # 如果重复插入，略过
                ...


    def search_IPO_date_from_sina(self, code: str) -> str:
        """ 从新浪获取公司上市日期, 返回yyyy-mm-dd型字符串 """

        ipo_date = ''
        url = f"""http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpInfo/stockid/{code}.phtml"""

        if not self.__sina_cookie_existed:
            self.__sina_session.get(url='http://vip.stock.finance.sina.com.cn/', headers=self.__headers_sina)
            self.__sina_cookie_existed = True
        response = self.__sina_session.get(url=url, headers=self.__headers_sina)

        tables = pd.read_html(response.text)
        
        if tables[3].iloc[2, 1] == 'CDR':
            ipo_date = tables[3].iloc[3, 3]
            return ipo_date
        
        ipo_date = tables[3].iloc[2, 3]

        return ipo_date


    def search_yearly_total_employee_from_xueqiu(self, code: str) -> float:
        """ 从雪球获取最新职工人数 """

        employee = 0

        url = 'https://stock.xueqiu.com/v5/stock/f10/cn/company.json'

        params = {
            'symbol': f'SH{code}' if code.startswith('6') else f'SZ{code}'
        }

        if not self.__xueqiu_cookie_existed:
            self.__xueqiu_session.get(url='https://xueqiu.com/', headers=self.__headers_xueqiu)
            self.__xueqiu_cookie_existed = True
        response = self.__xueqiu_session.get(url=url, params=params, headers=self.__headers_xueqiu)

        try:
            employee = float(response.json()['data']['company']['staff_num'])
        except:
            ...

        return employee


    def send_msg_to_wechat_by_pushplus(self, title: str, content: str, template: str = 'html'):
        """ 
        通过pushplus将信息推送给微信, 用于每日定时发送股票的基本信息 
        title: 发送信息的标题, content: 发送信息的内容
        """

        url = f"https://www.pushplus.plus/send?token={self.__pushplus_token}&title={title}&content={content}&template={template}"
        requests.get(url=url)


    def set_cookies_status_to_FALSE(self):
        """ 置各cookies标志为FALSE状态 """
        self.__xueqiu_cookie_existed = False
        self.__sina_cookie_existed = False
        self.__163_cookie_existed = False
        self.__cninfo_cookie_existed = False
        self.__chinabond_cookie_existed = False


    def set_init_roe_condition_value(self, roe: float):
        """ 以roe值设置7年ROE选股条件 """
        self.__init_roe_condition_value = [roe]*7
    

    @staticmethod
    def timestamp_to_date(timestamp: int) -> str:
        """ 将13位时间戳转化为yyyy-mm-dd型字符串 """

        timestamp_10 = float(timestamp/1000)
        struct_time = time.localtime(timestamp_10)
        date = time.strftime("%Y-%m-%d", struct_time)

        return date


    def update_5_years_cashflow_to_profit_table(self, code_year_args: Tuple[str, int]):
        """
        更新5年现金流量表数据.初始化年度表以后,会发生原始数据未更新的情况,此时需要用此函数再次更新.

        :param code_year_args: 一个元组, 第一个元素为股票代码(不含后缀), 第二个元素为年份(int型).
        :return: None
        """

        # 分解参数
        code = code_year_args[0]
        year = code_year_args[1]
        last_year = datetime.datetime.now().year - 1
        table_name = f"{last_year - 4}-{last_year}"

        # 准备更新数据
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        cashflow_data = self.calculate_5_years_cashflow_to_profit(code=code)
        update_data = (cashflow_data, stock_code)
        
        # 更新数据
        con = sqlite3.connect(CASHFLOW_PROFIT_SQLITE3)
        with con:
            sql = f""" UPDATE "{table_name}" SET cashflow_to_profit = ? WHERE stock_code = ? """
            con.execute(sql, update_data)
            print(f"更新{stock_code}的现金流量/利润数据成功"+'\r', end='', flush=True)

    def update_average_salary_table(self, code_year_args: Tuple[str, int]):
        """ 
        更新人均工资表数据
        参数code_year_args: 一个元组, 第一个元素为股票代码(不含后缀), 第二个元素为年份(int型)

        初始化年度薪酬表的时候,会发生原始数据未更新的情况,此时需要用此函数再次更新.
        """

        # 分解参数
        code = code_year_args[0]
        year = code_year_args[1]
        table_name = f'salary-{year}'

        # 准备更新数据
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_data = [stock_code]
        slalry_data = self.calculate_average_salary(code=code)
        update_data = slalry_data + update_data

        #  打开数据库准备更新
        con = sqlite3.connect(SALARY_SQLITE3)
        with con:
            sql = f""" UPDATE '{table_name}' SET employee=?, paid_salary=?, average_salary=? WHERE stockcode=? """
            con.execute(sql, tuple(update_data))
            print(f'{stock_code} 人均工资已经更新完成'+ '\r', end='', flush=True)


    def update_curve_value_table(self):
        """ 刷新国债收率表至昨日数据, 须每日定期执行, 避免在计算MOS时出错 """

        # 打开国债收益率表获取表中最后的date及value
        con = sqlite3.connect(CURVE_SQLITE3)
        with con:
            sql = """ SELECT date1, value1 FROM 'yield-curve' ORDER BY date1 DESC """
            latest_value = con.execute(sql).fetchone()

        # 计算yesterday和latest_day之间的天数
        yesterday = datetime.date.today() + datetime.timedelta(days=-1)
        latest_day = datetime.datetime.strptime(latest_value[0], '%Y-%m-%d').date()
        delta_day = (yesterday - latest_day).days

        #  补齐空缺的value
        self.init_curve_value_table(days=delta_day)


    def update_dividend_rate_table(self, code: str):
        """ 更新最新的现金分红率表 """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        dividend_rate = self.get_stock_dividend_rate_from_xueqiu(code=code)
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = (dividend_rate, stock_code)

        # 打开数据库,更新已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'dividend-rate.sql')
        con = sqlite3.connect(DIVIDEND_RATE_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建dividend-rate表

            sql = """ UPDATE 'dividend-rate' SET rate=? WHERE stockcode=? """
            con.execute(sql, update_list)


    def update_dividend_rate_table_copy_from_CSV(self, code: str):
        """ 
        为了减少重复下载,节约网络资源,从CSV文件中拷贝更新dividend-rate表.
        每日定期执行,从CSV文件中获取最新(昨日)DIVIDEND数据.
        在执行该函数之前,首先应把CSV文件更新至最新数据(至昨日).

        :param code: 股票代码, 不含后缀.(2023-04-28)
        """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        # 打开CSV文件,获取最新的PE PB数据(第一行昨日数据)
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        csv_file = os.path.join(self.__trade_record_path, stock_class, f'{code}.csv')
        csv_df = pd.read_csv(csv_file)

        dividend = csv_df.iloc[0]['DIVIDEND']

        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = (dividend, stock_code)

        # 打开数据库,更新已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'dividend-rate.sql')
        con = sqlite3.connect(DIVIDEND_RATE_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建dividend-rate表

            sql = """ UPDATE 'dividend-rate' SET rate=? WHERE stockcode=? """
            con.execute(sql, update_list)


    def update_history_PB_table(self, code: str):
        """ 更新至最新的历史PB数据 """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        # 准备更新的数据
        pb_tup = self.calculate_MAX_MIN_MEAN_pb(code=code)
        update_list = [item for item in pb_tup]
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list.append(stock_code)
        
        # 打开数据库,更新已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'history-pb.sql')
        con = sqlite3.connect(HISTORY_PB_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建history-pb表

            sql = """ UPDATE 'history-pb' SET maxPB=?, minPB=?, meanPB=? WHERE stockcode=? """
            con.execute(sql, tuple(update_list))


    def update_PE_PB_table(self, code: str):
        """ 更新至昨日pe和pb """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        # 准备更新的数据
        update_list = []
        pe = self.get_stock_PE_from_sina(code=code)
        update_list.append(pe)
        random.uniform(0.01, 0.015)
        pb = self.get_stock_PB_from_sina(code=code)
        update_list.append(pb)
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list.append(stock_code)

        # 打开数据库, 更新已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'price-indicator.sql')
        con = sqlite3.connect(PE_PB_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                sql_script = file.read()
            con.executescript(sql_script)  # 创建pe-pb 表

            sql = "UPDATE 'pe-pb' SET pe=?, pb=? WHERE stockcode=?"
            con.execute(sql, tuple(update_list))


    def update_PE_PB_table_copy_from_CSV(self, code: str):
        """ 
        为了减少重复下载,节约网络资源,从CSV文件中拷贝更新pe-pb表.
        每日定期执行,从CSV文件中获取最新(昨日)PE PB数据.
        在执行该函数之前,首先应把CSV文件更新至最新数据(至昨日).(2023-04-28)
        """
        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        # 打开CSV文件,获取最新的PE PB数据(第一行昨日数据)
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        csv_file = os.path.join(self.__trade_record_path, stock_class, f'{code}.csv')
        csv_df = pd.read_csv(csv_file)

        pe = csv_df.iloc[0]['PE']
        pb = csv_df.iloc[0]['PB']

        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = (pe, pb, stock_code)

        # 打开数据库, 更新已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'price-indicator.sql')
        con = sqlite3.connect(PE_PB_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                sql_script = file.read()
            con.executescript(sql_script)  # 创建pe-pb 表

            sql = "UPDATE 'pe-pb' SET pe=?, pb=? WHERE stockcode=?"
            con.execute(sql, tuple(update_list))


    def update_roe_table(self, code: str):
        """ 
        将最新的年度或者半年ROE数据插入roe_all_stocks表,三个月检查更新一次

        在执行的过程中,在判断最新的字段是否需要插入数据库的时候,出现了bug.
        需要区别年度数据和半年度数据,对比最新报表名称和字段名的年份,判断是否需要插入.(2023-04-24)
        """

        # 获取雪球网最新数据的时间,确定需要插入的字段名
        tmp = self.download_financial_indicator_from_xueqiu(code=code, count=1, type='Q4')
        last_report_name = tmp['data']['last_report_name']

        if '一季报' in last_report_name:
            last_filed = 'Y'+str(int(last_report_name[0:4])-1)
        elif '年报' in last_report_name:
            last_filed = 'Y'+last_report_name[:4]
        elif '三季报' in last_report_name or '中报' in last_report_name:
            last_filed = 'Y'+last_report_name[:4]+'Q2'
        
        # 获取roe-all-stocks 表字段中最新的时间
        con = sqlite3.connect(INDICATOR_SQLITE3)
        with con:
            sql = """ SELECT * FROM 'roe-all-stocks' """
            df = pd.read_sql_query(sql, con)

            # 取出columns分为Q2和Q4两部分,按照升序排列(2023-04-24)
            q2_col = [item for item in df.columns[3:] if 'Q2' in item]
            q4_col = [item for item in df.columns[3:] if 'Q2' not in item]
            q2_col.sort(reverse=True)
            q4_col.sort(reverse=True)

            # 比较数据库字段和最新的报表时间的年份,判断是否需要插入新的字段(2023-04-24)
            if 'Q2' in last_filed:
                contain_last_report = False if (int(last_filed[1:5]) > int(q2_col[0][1:5])) else True
            else:
                contain_last_report = False if (int(last_filed[1:5]) > int(q4_col[0][1:5])) else True

            # 插入新的字段
            if not contain_last_report:
                df.insert(loc=3, column=last_filed, value=0.00)
                df.to_sql(name='roe-all-stocks', con=con, if_exists='replace', index=False)

            # 下载更新数据
            if 'Q2' in last_filed:
                tmp = self.download_financial_indicator_from_xueqiu(code=code, count=1, type='Q2')
            else:  # 'Q2' not in last_filed 年度数据上面已经下载过了
                ...
                # tmp = self.download_financial_indicator_from_xueqiu(code=code, count=1, type='Q4')
            last_roe = tmp['data']['list'][0]['avg_roe'][0]
            stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
            sql = f""" UPDATE 'roe-all-stocks' SET {last_filed}=? WHERE stockcode=? """
            try:
                con.execute(sql, (last_roe, stock_code))
            except sqlite3.IntegrityError:
                ...


    def update_roe_table_from_1991(self, code: str):
        """ 更新最新的年度ROE至indicator_roe_from_1991数据库.该文件收集1991以来的年度ROE数据.(2023-04-03) """
        
        # 获取雪球网最新数据的时间,确定需要插入的字段名
        tmp = self.download_financial_indicator_from_xueqiu(code=code, count=1, type='Q4')
        last_report_name = tmp['data']['last_report_name']

        if '一季报' in last_report_name or '中报' in last_report_name or '三季报' in last_report_name:
            last_filed = 'Y'+str(int(last_report_name[0:4])-1)
        else:  # '年报' in last_report_name:
            last_filed = 'Y'+last_report_name[:4]

        # 获取roe-all-stocks-from-1991 表字段中最新的时间
        con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
        with con:
            sql = """ SELECT * FROM 'roe-all-stocks-from-1991' """
            df = pd.read_sql_query(sql, con)

            # 如果roe-all-stocks-from-1991 表中未包含最近一期的年度数据,则插入新列
            contain_last_report = False if (last_filed not in df.columns) else True
            if not contain_last_report:
                df.insert(loc=3, column=last_filed, value=0.00)  # 在第四列插入
                df.to_sql(name='roe-all-stocks-from-1991', con=con, if_exists='replace', index=False)

            # 更新数据
            last_roe = tmp['data']['list'][0]['avg_roe'][0]
            stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
            sql = f""" UPDATE 'roe-all-stocks-from-1991' SET {last_filed}=? WHERE stockcode=? """
            try:
                con.execute(sql, (last_roe, stock_code))
            except sqlite3.IntegrityError:
                ...

    @staticmethod
    def update_roe_table_1991_copy_from_2012() -> Union[None, str]:
        """
        - 从indicator数据库中复制最新的ROE值到indicator_roe_from_1991数据库中.
        indicator数据库中的ROE值是从2012年开始的,indicator_roe_from_1991数据库中的ROE值是从1991年开始的,
        故本函数取名为update_roe_table_1991_copy_from_2012.不同于update_roe_table_from_1991方法,
        这种思路是为了节约下载资源,直接从indicator数据库中整体复制数据,快的飞起.

        - 本函数按照以下步骤更新indicator_roe_from_1991数据库:
        - 检查indicator_roe_from_1991数据库中是否已经包含最新的年度数据(上年数),如果没有,则插入新列.
        - 检查indicator数据库是否bao含最新的年度数据(上年数),如果没有,则报错退出.
        - 从indicator数据库中逐条复制最新的ROE值到indicator_roe_from_1991数据库对应的股票中.
        - 保存更新后的indicator_roe_from_1991数据库.

        :return: None or str, None表示更新成功, str表示更新失败(indicator数据库没有最新年度数据).(2023-04-26)
        """

        # 获取indicator_roe_from_1991数据库中最新的年度数据
        last_year_int = datetime.datetime.now().year - 1  # 上年数
        con_1991 = sqlite3.connect(INDICATOR_ROE_FROM_1991)
        with con_1991:
            sql = """ SELECT * FROM 'roe-all-stocks-from-1991' """
            df_1991 = pd.read_sql_query(sql, con_1991)
            last_year_in_1991 = [item for item in df_1991.columns if 'stock' not in item][0]  # 最新的年度列名,如'Y2022'

            # 如果roe-all-stocks-from-1991 表中未包含最近一期的年度数据,则插入新列
            contain_last_year_in_1991 = False if last_year_int != int(last_year_in_1991[1:]) else True
            if not contain_last_year_in_1991:
                df_1991.insert(loc=3, column='Y'+str(last_year_int), value=0.00)  # 在第四列插入

        # 获取indicator数据库中最新的年度数据
        con_2012 = sqlite3.connect(INDICATOR_SQLITE3)
        with con_2012:
            sql = """ SELECT * FROM 'roe-all-stocks' """
            df_2012 = pd.read_sql_query(sql, con_2012)
            last_year_in_2012 = [item for item in df_2012.columns if 'stock' not in item and 'Q2' not in item][0]  # 最新的年度列名,如'Y2022'

            # 如果roe-all-stocks 表中未包含最近一期的年度数据,则报错返回
            contain_last_year_in_2012 = False if last_year_int != int(last_year_in_2012[1:]) else True
            if not contain_last_year_in_2012:
                return 'indicator数据库中未包含最新的年度数据{last_year_int},请先更新indicator数据库.'

        # 遍历df_1991,以stock_code为纽带,从df_2012中复制最新的ROE值到df_1991中
        for index, stock_code in enumerate(df_1991['stockcode']):
            last_roe = df_2012[df_2012['stockcode'] == stock_code][last_year_in_2012].values[0]
            df_1991.loc[index, last_year_in_1991] = last_roe

        # 保存更新后的indicator_roe_from_1991数据库
        with con_1991:
            df_1991.to_sql(name='roe-all-stocks-from-1991', con=con_1991, if_exists='replace', index=False)

        
    def update_trade_record_cvs(self, code: str):
        """
        添加股票总市值 PE PB PS PC至昨日数据, 原可从163财经网整体下载
        现网站改版无法下载,采用在原下载交易文件基础上每日更新的方式

        今日增加了DIVIDEND信息更新内容.(2023-04-23)
        """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        # 准备插入信息
        yestoday_date = datetime.date.today() + datetime.timedelta(days=-1)
        yestoday_str = yestoday_date.strftime('%Y-%m-%d')
        stock_name = self.get_name_and_class_by_code(code=code)[0]
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        total_value = self.get_stock_total_value_from_sina(code=code)
        pb = self.get_stock_PB_from_sina(code=code)
        pe = self.get_stock_PE_from_sina(code=code)
        ps = self.get_stock_PS_from_xueqiu_and_sina(code)
        pc = self.get_stock_PC_from_xueqiu_and_sina(code)
        dividend = self.get_stock_dividend_rate_from_xueqiu(code=code)
        insert_value = [yestoday_str, f"'{code}", stock_name, total_value, pb, pe, ps, pc, dividend]

        # 打开原交易数据文件
        trade_csv_path = os.path.join(self.__trade_record_path, f'{stock_class}')
        trade_csv_file = os.path.join(trade_csv_path, f'{code}.csv')
        df = pd.read_csv(trade_csv_file)
        df = df.dropna()  # 删除含空的行
        columns = df.columns

        # 插入最新数据
        if yestoday_str != df.iloc[0, 0]:
            df = pd.DataFrame(np.insert(df.values, 0, values=insert_value, axis=0)) # 第一行插入
            df.columns = columns
            df.to_csv(path_or_buf=trade_csv_file, index=False)

    def update_trade_record_cvs_at_date_row(self, code: str, date: str):
        """ 
        更新指定日期date所在行的 总市值 PE PB PS PC......(2023-04-04)
        +++++++++++++++++++++++++++++++++++++++++++++++++++++
        今日增加了DIVIDEND信息更新内容.(2023-04-23)
        """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 上一天为周六和七则停止
            return
        # 检查参数
        date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        if not date_regex.match(date):
            return

        # 打开原交易数据文件定位到行
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        trade_csv_file = os.path.join(self.__trade_record_path, stock_class, f'{code}.csv')
        df = pd.read_csv(trade_csv_file)

        if date not in df['日期'].values.tolist():
            return
        
        condition = (df['日期'] == date)
        row_index = df[condition].index[0]
        
        # 更新所在行数据
        df.loc[row_index, '总市值'] = self.get_stock_total_value_from_sina(code)
        df.loc[row_index, 'PB'] = self.get_stock_PB_from_sina(code)
        df.loc[row_index, 'PE'] = self.get_stock_PE_from_sina(code)
        df.loc[row_index, 'PS'] = self.get_stock_PS_from_xueqiu_and_sina(code)
        df.loc[row_index, 'PC'] = self.get_stock_PC_from_xueqiu_and_sina(code)
        df.loc[row_index, 'DIVIDEND'] = self.get_stock_dividend_rate_from_xueqiu(code)
        
        df.to_csv(path_or_buf=trade_csv_file, index=False)


    def update_total_value(self, code: str):
        """ 更新股票总市值至昨日最新数据 """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        random.uniform(0.01, 0.15)
        total_value = self.get_stock_total_value_from_sina(code=code)
        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = (total_value, stock_code)

        # 打开数据库,更新已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'total-value.sql')
        con = sqlite3.connect(TVALUE_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建total-value表

            sql = """ UPDATE 'total-value' SET tvalue=? WHERE stockcode=? """
            con.execute(sql, update_list)


    def update_total_value_copy_from_CSV(self, code: str):
        """ 
        为了减少重复下载,节约网络资源,从CSV文件中拷贝更新total-value表.
        每日定期执行,从CSV文件中获取最新(昨日)总市值数据.
        在执行该函数之前,首先应把CSV文件更新至最新数据(至昨日).

        :param code: 股票代码, 不含后缀.(2023-04-28)
        """

        if datetime.datetime.now().isoweekday() in [1, 7]:  # 遇周六和七停止
            return

        # 打开CSV文件,获取最新的PE PB数据(第一行昨日数据)
        stock_class = self.get_name_and_class_by_code(code=code)[1]
        csv_file = os.path.join(self.__trade_record_path, stock_class, f'{code}.csv')
        csv_df = pd.read_csv(csv_file)

        total_value = csv_df.iloc[0]['总市值']

        stock_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        update_list = (total_value, stock_code)

        # 打开数据库,更新已经准备好的数据
        sql_file = os.path.join(self.__sql_path, 'total-value.sql')
        con = sqlite3.connect(TVALUE_SQLITE3)
        with con:
            with open(sql_file, 'r') as file:
                script = file.read()
            con.executescript(script)  # 创建total-value表

            sql = """ UPDATE 'total-value' SET tvalue=? WHERE stockcode=? """
            con.execute(sql, update_list)


if __name__ == "__main__":
    case = StockData()

    all_stock_list = []
    class_list = case.get_stock_classes()
    for clas in class_list:
        res = case.get_stocks_of_specific_class(clas)
        stock_list = [item[0][0:6] for item in res]
        for code in stock_list:
            all_stock_list.append(code)

    while True:

        print('-------------------------操作提示---------------------------' )
        print('Init-Trade-CSV     Update-PE-PB        Update-Dividend-Rate' )
        print('Update-TValue      Update-ROE-Table    Update-ROE-Table-1991')
        print('Update-Curve       Update-History-PB   Update-Trade-CSV'     )
        print('Check-Fix-CSV                          Quit'                 )
        print('-----------------------------------------------------------' )

        msg = input('>>>> 请选择操作提示 >>>>  ')

        if msg.upper() == 'INIT-TRADE-CSV':  # 初始化然后调整全部CSV文件
            error_code = []
            print('正在初始化历史交易记录文件......')
            classes = case.get_stock_classes()
            for clas in classes:
                code_name_class = case.get_stocks_of_specific_class(clas)
                for code in code_name_class:
                    try:
                        case.init_trade_record_form_IPO(code[0][0:6])
                        time.sleep(random.uniform(0.05, 0.1))
                    except:
                        error_code.append(code[0][0:6])
            print(f'历史交易记录文件已经初始化完成,错误代码为{error_code}')
        
        elif msg.upper() == 'UPDATE-TRADE-CSV':
            print('正在更新历史交易记录文件......')
            stock_classes = case.get_stock_classes()
            for clas in stock_classes:
                tmp = case.get_stocks_of_specific_class(clas)
                code_list = [item[0][:6] for item in tmp]
                with ThreadPoolExecutor() as pool:
                    pool.map(case.update_trade_record_cvs, code_list)
            print(f'历史交易记录文件已经更新完成.')

        elif msg.upper() == 'UPDATE-PE-PB':
            print('正在从CSV历史交易记录文件中copy update PE PB 表......')
            stock_classes = case.get_stock_classes()
            for clas in stock_classes:
                tmp = case.get_stocks_of_specific_class(clas)
                code_list = [item[0][:6] for item in tmp]
                with ThreadPoolExecutor() as pool:
                    pool.map(case.update_PE_PB_table_copy_from_CSV, code_list)
            print(f'PE PB 表已经更新完成.')

        elif msg.upper() == 'UPDATE-DIVIDEND-RATE':
            print('正在从CSV历史交易记录文件中copy update DIVIDEND RATE 表......')
            stock_classes = case.get_stock_classes()
            for clas in stock_classes:
                tmp = case.get_stocks_of_specific_class(clas)
                code_list = [item[0][:6] for item in tmp]
                with ThreadPoolExecutor() as pool:
                    pool.map(case.update_dividend_rate_table_copy_from_CSV, code_list)
            print(f'分红率表已经更新完成.')

        elif msg.upper() == 'UPDATE-TVALUE':
            print('从CSV历史交易记录文件中copy update总市值表......')
            stock_classes = case.get_stock_classes()
            for clas in stock_classes:
                tmp = case.get_stocks_of_specific_class(clas)
                code_list = [item[0][:6] for item in tmp]
                with ThreadPoolExecutor() as pool:
                    pool.map(case.update_total_value_copy_from_CSV, code_list)
            print(f'总市值表已经更新完成.')
            
        elif msg.upper() == 'UPDATE-ROE-TABLE':
            print('正在更新2012以来年度ROE数据库,请稍等......')
            con = sqlite3.connect(INDICATOR_SQLITE3)
            with con:
                # 读取数据
                df = pd.read_sql('select * from "roe-all-stocks"', con)
                df.fillna(0, inplace=True)
                df_col = [col for col in df.columns if 'stock' not in col]
                df_col.sort(reverse=True)  # 降序排列,排序后年份越大越靠前,且Y2022Q2在Y2022前面
                # 获取最新的字段
                if df_col[1] in df_col[0]:  
                    last_filed = df_col[1]  # 如果Y2022Q2在Y2022前面,则Y2022Q2为最新字段
                else:
                    last_filed = df_col[0]  # 否则Y2022为最新字段
                # 查询数据库中last_filed字段为0的股票代码,并下载数据.
                code_list_tmp = df[df[last_filed] == 0]['stockcode'].tolist()  # 含有后缀的股票代码
                code_list = [item[0:6] for item in code_list_tmp]
                print(f'需要更新的股票代码个数为{len(code_list)}')
                with ThreadPoolExecutor() as pool:
                    pool.map(case.update_roe_table, code_list)
                # 清洗数据,将null值替换为0后保存到数据库中
                df = pd.read_sql('select * from "roe-all-stocks"', con)
                df.fillna(0, inplace=True)
                df.to_sql('roe-all-stocks', con, if_exists='replace', index=False)
                print(f'更新完成.')

        elif msg.upper() == 'UPDATE-ROE-TABLE-1991':
            print('正在从indicator.sqlite3数据库复制最新年度ROE数据,请稍等......')
            case.update_roe_table_1991_copy_from_2012()
            print(f'更新完成.')
        
        elif msg.upper() == 'UPDATE-HISTORY-PB':
            print('正在更新history-pb数据库,请稍等......')
            stock_classes = case.get_stock_classes()
            for clas in stock_classes:
                tmp = case.get_stocks_of_specific_class(clas)
                code_list = [item[0][:6] for item in tmp]
                with ThreadPoolExecutor() as pool:
                    pool.map(case.update_history_PB_table, code_list)
            print(f'更新完成.')

        elif msg.upper() == 'UPDATE-CURVE':
            print('正在更新国债收益率数据库,请稍等......')
            case.update_curve_value_table()
            print(f'更新完成.')
            
        elif msg.upper() == 'CHECK-FIX-CSV':
            print('正在检查交易记录文件格式,请稍等......')
            error_code = []
            other_code = []
            for code in all_stock_list:
                try:
                    if case.check_trade_record_csv(code) != 'ok':
                        error_code.append(code)
                except:
                    other_code.append(code)
            if error_code + other_code:
                print('格式错误的代码集合为:', error_code)
                print('其他错误的代码集合为:', other_code)
                print('正在尝试修复错误......')
                no_fix_code = []
                for code in error_code + other_code:
                    try:
                        case.init_trade_record_form_IPO(code)
                    except:
                        no_fix_code.append(code)
                print('修复完成,以下代码修复失败,请手动修复:', no_fix_code)
            else:
                print('交易记录文件格式正确.')

        elif msg.upper() == 'QUIT':
            break

        else:
            continue

