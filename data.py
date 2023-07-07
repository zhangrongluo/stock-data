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
from concurrent.futures import ThreadPoolExecutor

from path import (
    trade_record_path, headers_cninfo, headers_10jqka, SW_STOCK_LIST, CNINFO_STOCK_LIST
)
    

class TradeRecordData:
    """
    从预测者网站下载历史日线初始数据后进一步处理, 生成WIN-STOCK系统所需格式交易记录CSV文件。
    原始数据下载之后,首先创建TradeRecordData类的实例.执行move_raw_data_to_target_path方法,将原始数据按照申万行业分类后移动到目标目录下。
    然后执行init_trade_record_from_IPO方法,补齐交易记录CSV文件中的缺失数据。最后执行check_trade_record_csv方法,检查文件数据格式.
    """


    def __init__(self, stock_list_path: str = SW_STOCK_LIST):
        """ stock_list_path 为绝对路径  """

        self.__sw_stock_list: DataFrame = pd.read_excel(io=stock_list_path)  # 申万股票清单pandas df格式
        self.__trade_record_path = trade_record_path

        # 标志cookies状态
        self.__10jqka_session = requests.Session()
        self.__10jqka_cookie_existed = False

        # 选取EDGE浏览器数据即可
        self.__headers_10jqka = headers_10jqka


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
            return 'ps-empty'
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


    def add_dividend_rate_to_CSV(self, trade_record_df:pd.DataFrame, dividend: Dict):
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

        :param trade_record_df: CSV文件的DataFrame对象.
        :param dividend: 分红信息,self.download_history_dividend_record_from_10jqka()方法的返回值.

        (2023-04-23)
        """

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

        return trade_record_df


    def get_stock_classes(self) -> List:
        """获取申万行业分类清单"""

        return self.__sw_stock_list['新版一级行业'].unique().tolist()
        

    def get_latest_record_date(self):
        """ 从历史交易记录文件查找最新的日期,查找的位置为self.__trade_record_path下第一个文件夹中的第一个文件 """
        target_path = os.path.join(self.__trade_record_path, self.get_stock_classes()[0])
        target_file = os.path.join(target_path, os.listdir(target_path)[0])
        tmp_df = pd.read_csv(target_file, usecols=['日期'])

        return tmp_df.loc[0, '日期']


    def get_name_and_class_by_code(self, code: str) -> List:
        """ 通过股票代码获取公司简称及行业分类 """

        df = self.__sw_stock_list
        code = code + '.SH' if code.startswith('6') else code + '.SZ'
        tmp = df.loc[df['股票代码'] == code]  # 选出股票所在的行
        result = ['错误', '错误'] if tmp.empty else tmp.loc[tmp.index[0], ['公司简称', '新版一级行业']].values.tolist()

        return result


    def get_stocks_of_specific_class(self, stock_class: str) -> List:
        """获取stock_class指定的行业下上交所和深交所股票代码 公司简称 行业分类"""

        df = self.__sw_stock_list
        tmp = df.loc[df['新版一级行业'] == stock_class]  # 选出类所在的若干行
        # 剔除上交所深交所以外的股票
        criterion = tmp['股票代码'].map(lambda x: ('.SZ' in x) or ('.SH' in x))  
        result = tmp[criterion][['股票代码', '公司简称', '新版一级行业']].values.tolist()

        return result


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

        # 添加DIVIDEND列
        try:
            dividend_dict = self.download_history_dividend_record_from_10jqka(code=code)
            trade_record_df = self.add_dividend_rate_to_CSV(trade_record_df=trade_record_df, dividend=dividend_dict)
        except:
            trade_record_df['DIVIDEND'] = 0.00

        # 保留需要的列,其余删除
        standard_columns = ['日期', '股票代码', '名称', '总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND']
        ndf = trade_record_df[standard_columns]

        # ['总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND'] 数字列保留两位小数
        ndf[['总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND']] = ndf[['总市值', 'PB', 'PE', 'PS', 'PC', 'DIVIDEND']].round(2)

        # 保存文件
        ndf.to_csv(trade_csv_file, index=False)


    def move_raw_data_to_target_path(self, raw_data: str, target_path: str) -> None:
        """
        使用申万行业股票清单,将下载的原始数据移动到目标文件夹.移动过程中完成股票行业分类和数据格式转换和清洗.

        下载的原始数据包含以下columns:code、date、open、high、low、close、change、volume、money、traded_market_value、
        market_value、turnover、adjust_price、report_type、report_date、PE_TTM、PS_TTM、PC_TTM、PB。
        交易记录CSV文件包含以下columns:日期,股票代码,名称,总市值,PB,PE,PS,PC,DIVIDEND。
        原始数据需要保留的columns为code、date、market_value、PE_TTM、PS_TTM、PC_TTM、PB,需要增加的columns为名称、DIVIDEND。

        原始数据下载之后,首先创建StockData类的实例.执行本方法,将原始数据按照申万行业分类后移动到目标目录下。
        然后执行init_trade_record_from_IPO方法,补齐交易记录CSV文件中的缺失数据。

        :param raw_data: 原始数据文件夹路径,为绝对路径.
        :param target_path: 目标文件夹路径,为绝对路径.
        :return: None
        """

        # 检查参数
        if not os.path.exists(raw_data):
            raise ValueError(f'原始数据文件夹路径{raw_data}不存在.')

        # 建立目标文件夹
        if not os.path.exists(target_path):
            os.mkdir(target_path)

        # 获取申万行业股票分类清单,获取每个行业的全部股票代码,将原始数据移动到目标文件夹.
        classes = self.get_stock_classes()
        for class_ in classes:
            # 获取每个行业的全部股票代码
            res = self.get_stocks_of_specific_class(class_)
            res_code = [item[0][:6] for item in res]

            # 建立每个行业的文件夹
            class_path = os.path.join(target_path, class_)
            if not os.path.exists(class_path):
                os.mkdir(class_path)

            # 遍历原始数据文件夹,将每个行业的股票数据移动到对应的行业文件夹
            for file in os.listdir(raw_data):
                for code in res_code:
                    if code in file:
                        os.system(f'cp {raw_data}/{file} {class_path}')
                        break
                    else:
                        continue

            # 对目标文件夹中的文件进行更名,去掉股票代码前缀sh或sz字符
            for file in os.listdir(class_path):
                os.rename(f'{class_path}/{file}', f'{class_path}/{file[2:]}')
            
            """以下完成对目标文件数据清洗工作"""
            for file in os.listdir(class_path):
                # pandas读取文件
                df = pd.read_csv(f'{class_path}/{file}')

                # 保留code、date、market_value、PE_TTM、PS_TTM、PC_TTM、PB列
                ndf = df[['date', 'code', 'market_value', 'PB', 'PE_TTM', 'PS_TTM', 'PC_TTM']]

                # 替换columns为日期,股票代码,总市值,PB,PE,PS,PC
                ndf.columns = ['日期', '股票代码', '总市值', 'PB', 'PE', 'PS', 'PC']

                # 将股票代码列前两个字符sh或sz去掉,在股票代码前加上“'“
                for index, item in ndf['股票代码'].items():
                    ndf.loc[index, '股票代码'] = "'" + item[2:]

                # 获取股票名称后插入到股票代码列后面
                stock_name = self.get_name_and_class_by_code(code=file[:6])[0]
                ndf.insert(2, '名称', stock_name)

                # 将日期列转换成字符串格式
                for index, item in ndf['日期'].items():
                    ndf.loc[index, '日期'] = str(item)

                # 按照日期降序排列
                ndf = ndf.sort_values(by='日期', ascending=False)

                # 保存文件
                ndf.to_csv(f'{class_path}/{file}', index=False)


            print(f'{class_}行业数据移动完成.')
    


if __name__ == "__main__":
    case = TradeRecordData()

    # 数据迁移
    raw_data = '/Users/zhangrongluo/Downloads/trading-data/stockdata'
    target_path = '/Users/zhangrongluo/Desktop/target'
    case.move_raw_data_to_target_path(raw_data=raw_data, target_path=target_path)

    classes = case.get_stock_classes()

    print('正在初始化历史交易记录文件......')
    for clas in classes:
        code_name_class = case.get_stocks_of_specific_class(clas)
        with ThreadPoolExecutor() as pool:
            pool.map(case.init_trade_record_form_IPO, [code[0][0:6] for code in code_name_class])

    print('正在检查历史交易记录文件......')
    for clas in classes:
        code_name_class = case.get_stocks_of_specific_class(clas)
        for code in code_name_class:
            result = case.check_trade_record_csv(code=code[0][0:6])
            if result != 'ok':
                print(f'{code[0]} {code[1]} {result}')

    print('初始化历史交易记录文件完成.')

