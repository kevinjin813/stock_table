import akshare as ak
import pandas as pd
import threading
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pymysql


def get_connection():
    return pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='qwertyui',
    database='schemas',
    charset='utf8'
    )


def get_engine():
    db_username = 'root'
    db_password = 'qwertyui'
    db_host = 'localhost'
    db_port = '3306'
    db_name = 'schemas'
    # 创建数据库引擎
    engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
    return engine


def query_data(sql):
    conn = get_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


def pd2sql(df,table_name):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists='append', index=False)
    engine.dispose()



def fetch_and_save_data_min(stock_id):
    # 获取当前时间并格式化
    now = datetime.now()
    end_time = now.strftime('%Y-%m-%d %H:%M:%S')
    start_time = (now - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')

    # 使用 Akshare API 获取数据
    data = ak.stock_zh_a_hist_min_em(symbol=stock_id, period='1', start_date=start_time, end_date=end_time, adjust="")
    data.rename(columns={
        '时间': 'date',
        '开盘': 'open_price',
        '收盘': 'close_price',
        '最高': 'high_price',
        '最低': 'low_price',
        '成交量': 'volume',
        '成交额': 'turnover',
        '最新价': 'now_price'
    }, inplace=True)
    data['stock_id'] = stock_id
    pd2sql(data,"stock_min_hist")





def fetch_data(stock_id,period):
    today = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
    if period == 'intraday':
        sql = f"""
                    SELECT sh.*, si.stock_name
                    FROM stock_min_hist sh
                    LEFT JOIN stock_info si ON sh.stock_id = si.stock_id
                    WHERE si.stock_id = '{stock_id}' and  date(sh.date) = '{today}'
                    Order by sh.date ASC;
                  """
        result = pd.DataFrame(query_data(sql))
        result['date'] = pd.to_datetime(result['date']).dt.time
        result['avg'] = result['turnover'] / (result['volume']+ 1e-9)
        result.loc[result['avg'] == 0, 'avg'] = result['now_price']
        stock_name = result['stock_name'][0].strip()
        return result

    elif period == 'daily':
        sql = f"""
                    SELECT sh.*, si.stock_name
                    FROM stock_min_hist sh
                    LEFT JOIN stock_info si ON sh.stock_id = si.stock_id
                    WHERE si.stock_id = '{stock_id}' 
                    Order by sh.date ASC;
                  """
        result = pd.DataFrame(query_data(sql))
        result['date'] = pd.to_datetime(result['date']).dt.time
        stock_name = result['stock_name'][0].strip()
        return result


if __name__ == '__main__':
    sql = "insert stock_bid (stock_id,bid5,bid5_vol) values('000003',1789.49,100.00)"
    insert_or_update_data(sql)
    sql = "select * from stock_bid"
    data = query_data(sql)
    import pprint
    pprint.pprint(data)


