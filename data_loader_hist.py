import akshare as ak
import pandas as pd
import threading
from sqlalchemy import create_engine
from datetime import datetime, timedelta

db_username = 'root'
db_password = 'qwertyui'
db_host = 'localhost'
db_port = '3306'
db_name = 'schemas'
table_name = 'stock_info'

# 创建数据库引擎
engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

sql = f"SELECT * FROM {table_name}"


lock = threading.Lock()
monitor = 0

# sql = """SELECT *
# FROM stock_info si
# WHERE NOT EXISTS (
#     SELECT 1
#     FROM stock_hist sh
#     WHERE si.stock_id = sh.stock_id
# );"""

stock_df = pd.read_sql(sql, engine)

print(stock_df)


quater = len(stock_df) // 4
code_list_1 = stock_df[:quater]['stock_id']
code_list_2 = stock_df[quater:2*quater]['stock_id']
code_list_3 = stock_df[2*quater:3*quater]['stock_id']
code_list_4 = stock_df[3*quater:]['stock_id']


# 初始化 DataFrame
df = pd.DataFrame(columns=[
    'date', 'open_price',
    'close_price',
    'high_price',
    'low_price',
    'volume',
    'turnover',
    'amplitude',
    'price_change_pct',
    'price_change_amt',
    'turnover_rate',
    'stock_id'
])
df_list = []  # 用于存储线程结果的列表


def get_info(codes, result_list,start_date,end_date,adjust="",period="daily"):
    global monitor
    temp_df = pd.DataFrame(columns=[
                'date', 'open_price',
                'close_price',
                'high_price',
                'low_price',
                'volume',
                'turnover',
                'amplitude',
                'price_change_pct',
                'price_change_amt',
                'turnover_rate',
                'stock_id'
            ])
    for code in codes:
        data = ak.stock_zh_a_hist(symbol=code,period=period,start_date=start_date,end_date=end_date,adjust=adjust)
        data = data.rename(columns={
            '日期': 'date',
            '开盘': 'open_price',
            '收盘': 'close_price',
            '最高': 'high_price',
            '最低': 'low_price',
            '成交量': 'volume',
            '成交额': 'turnover',
            '振幅': 'amplitude',
            '涨跌幅': 'price_change_pct',
            '涨跌额': 'price_change_amt',
            '换手率': 'turnover_rate'
        })
        data['stock_id']=code
        temp_df = pd.concat([temp_df, data], ignore_index=True)
        data.to_sql("stock_hist", engine, if_exists='append', index=False)
        with lock:
            monitor += 1
            print(f"Code {code} retrieved, total retrieved: {monitor}")
    result_list.append(temp_df)


# 创建线程
end_date = datetime.now().strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
thread1 = threading.Thread(target=get_info, args=(code_list_1, df_list,start_date,end_date))
thread2 = threading.Thread(target=get_info, args=(code_list_2, df_list,start_date,end_date))
thread3 = threading.Thread(target=get_info, args=(code_list_3, df_list,start_date,end_date))
thread4 = threading.Thread(target=get_info, args=(code_list_4, df_list,start_date,end_date))
# 启动线程
thread1.start()
thread2.start()
thread3.start()
thread4.start()
# 等待线程结束
thread1.join()
thread2.join()
thread3.join()
thread4.join()
# 合并结果
for temp_df in df_list:
    df = pd.concat([df, temp_df], ignore_index=True)

df.to_excel('output.xlsx')

# 关闭数据库连接
engine.dispose()
