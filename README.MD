1. 从预测者网站下载历史日线初始数据后进一步处理，生成WIN-STOCK系统所需格式交易记录CSV文件。
下载地址为：https://www.yucezhe.com/product/data/trading 该下载是需要付费的。
2. 原始数据下载之后，首先创建TradeRecordData类的实例，执行move_raw_data_to_target_path方法,将原始数据按照申万行业分类后移动到目标目录下。
然后执行init_trade_record_from_IPO方法，补齐交易记录CSV文件中的缺失数据。最后执行check_trade_record_csv方法，检查文件数据格式。
无法修补的需要手动修补。
3. 实际使用时，需要在path.py文件中设定系统根目录BASE_DIR至实际目录。