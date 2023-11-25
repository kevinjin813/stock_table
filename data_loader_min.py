import akshare as ak
import pandas as pd
import threading
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pymysql
import db


def get_min_data(codes,start_date,end_date,adjust="",period="1",):
    global monitor
    for code in codes:
        data = ak.stock_zh_a_hist_min_em(symbol=code,period=period,start_date=start_date,end_date=end_date,adjust=adjust)
        data = data.rename(columns={
            '时间': 'date',
            '开盘': 'open_price',
            '收盘': 'close_price',
            '最高': 'high_price',
            '最低': 'low_price',
            '成交量': 'volume',
            '成交额': 'turnover',
            '最新价': 'now_price'
        })
        data['stock_id'] = code
        data.to_sql("stock_min_hist", engine, if_exists='append', index=False)
        with lock:
            monitor += 1
            print(f"Code {code} retrieved, total retrieved: {monitor}")
    engine.dispose()

engine = db.get_engine()
sql = f"SELECT * FROM stock_info"
stock_df = pd.read_sql(sql, engine)
print(stock_df)
lock = threading.Lock()
monitor = 0
quater = len(stock_df) // 4
code_list_1 = stock_df[:quater]['stock_id']
code_list_2 = stock_df[quater:2 * quater]['stock_id']
code_list_3 = stock_df[2 * quater:3 * quater]['stock_id']
code_list_4 = stock_df[3 * quater:]['stock_id']
end_date = datetime.now().strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=1))
thread1 = threading.Thread(target=get_min_data, args=(code_list_1, start_date, end_date))
thread2 = threading.Thread(target=get_min_data, args=(code_list_2, start_date, end_date))
thread3 = threading.Thread(target=get_min_data, args=(code_list_3, start_date, end_date))
thread4 = threading.Thread(target=get_min_data, args=(code_list_4, start_date, end_date))
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

engine.dispose()

