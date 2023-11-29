import pymysql
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
import pandas as pd
import io
from flask import Flask, render_template, send_file, jsonify
import db
from flask import redirect, url_for, request
import redis
from datetime import datetime,timedelta
from flask_apscheduler import APScheduler
import akshare as ak
import time
import json
from threading import Thread
app = Flask(__name__)

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
scheduler.daemonic = False


redis_db = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
redis_client = redis.Redis(host='localhost', port=6379, db=0)
STREAM_NAME = 'stock_stream'

TRADING_START_1 = "09:30"
TRADING_END_1 = "11:30"
TRADING_START_2 = "13:00"
TRADING_END_2 = "15:00"


most_used_stocks = {stock['stock_id'] for stock in db.query_data("SELECT stock_id FROM most_use_stock")}



def is_trading_time():
    now = datetime.now()
    current_time = now.strftime('%H:%M')
    day_of_week = now.weekday()

    if day_of_week >= 5:
        return False

    if TRADING_START_1 <= current_time <= TRADING_END_1 or TRADING_START_2 <= current_time <= TRADING_END_2:
        return True

    return False

def fetch_and_save_spot_data_to_stream():

    if not is_trading_time():
        print("Not trading time!!")
        return
    print(f"Fetching data at {datetime.now()}")
    data = ak.stock_zh_a_spot_em()
    data = data.rename(columns={
        '代码': 'stock_id',
        '名称': 'stock_name',
        '最新价': 'now_price',
        '今开': 'open_price',
        '最高': 'high_price',
        '最低': 'low_price',
        '成交量': 'volume',
        '成交额': 'turnover',
        '振幅': 'amplitude',
        '涨跌幅': 'price_change_pct',
        '涨跌额': 'price_change_amt',
        '换手率': 'turnover_rate'
    })
    data = data.filter([
        'stock_name',
        'stock_id',
        'open_price',
        'now_price',
        'high_price',
        'low_price',
        'volume',
        'turnover',
        'amplitude',
        'price_change_pct',
        'price_change_amt',
        'turnover_rate'
    ])
    now = datetime.now()
    date_str = now.strftime('%Y%m%d')
    time_str = now.strftime('%H:%M:%S')
    data['date'] = date_str
    data['time'] = time_str
    data['unix_time'] = now.timestamp()
    json_data = data.to_json(orient='records')
    # 发送到 Redis Stream
    redis_client.xadd(STREAM_NAME, {'data': json_data})

    print("Data published to Redis Stream")


def consume_stream():
    """
    从 Redis Stream 中读取数据并保存到数据库的函数，
    同时更新 Redis 中的常用股票数据。
    """
    last_id = '0-0'  # 初始 ID 设置为 Stream 开始处

    while True:
        # 读取 Stream 中的新消息
        messages = redis_client.xread({STREAM_NAME: last_id}, count=5, block=1000)

        for stream, message_list in messages:
            for message_id, message_fields in message_list:
                try:
                    json_data = message_fields[b'data']  # 提取 JSON 字符串
                    data = json.loads(json_data)

                    # 将数据转换为 DataFrame 并保存到数据库
                    data_df = pd.DataFrame(data)
                    db.pd2sql(data_df, "stock_realtime")
                    print("Data pushed to mysql")
                    redis_client.xdel(stream, message_id)
                    # 更新常用股票数据
                    update_most_used_stocks(data_df)

                except Exception as e:
                    print(f"Error occurred: {e}")
                    continue  # 跳过当前循环的剩余部分，继续下一个迭代

                # 更新最后读取的消息 ID
                last_id = message_id

        # 简短休眠以减少循环频率，防止 CPU 使用过高
        time.sleep(1)

def update_most_used_stocks(data_df):
    """更新 Redis 中的常用股票数据"""
    combined_data = pd.DataFrame()
    for stock_id in most_used_stocks:
        new_stock_data = data_df[data_df['stock_id'] == stock_id]
        if not new_stock_data.empty:
            combined_data = pd.concat([combined_data, new_stock_data])

    # 批量更新 Redis，减少 Redis 交互次数
    if not combined_data.empty:
        redis_client.mset({f"stock_data:{row['stock_id']}": json.dumps(row.to_dict())
                           for index, row in combined_data.iterrows()})


Thread(target=consume_stream).start()


# @scheduler.task('cron', id='fetch_spot_data', minute='*')

def scheduled_task():
    fetch_and_save_spot_data_to_stream()

if is_trading_time():
    scheduled_task()

scheduler.add_job(func=scheduled_task, trigger='cron', minute='*', id='fetch_spot_data')

def clear_redis_data():
    # 获取所有与股票数据相关的 Redis 键
    stock_data_keys = redis_db.keys('stock_data:*')
    if stock_data_keys:
        redis_db.delete(*stock_data_keys)

scheduler.add_job(func=clear_redis_data, trigger='cron', day_of_week='mon-fri', hour='8', minute='55', id='clear_redis')

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # 处理表单提交
        stock_id = request.form['stock_id']
        return redirect(url_for('kline_chart_id', stock_id=stock_id))
    else:
        # 显示默认内容
        sql = """
            SELECT *
            FROM stock_info
            ORDER BY stock_id;
        """
        result = pd.DataFrame(db.query_data(sql))
        return render_template('index.html', data=result.to_dict(orient='records'))


@app.route('/kline_chart')
def kline_chart():
    return redirect(url_for('kline_chart_id', stock_id='000001'))

@app.route('/kline_chart/<stock_id>')
def kline_chart_id(stock_id):
    sql = f"""
                            SELECT sh.*, si.stock_name
                            FROM stock_hist sh
                            LEFT JOIN stock_info si ON sh.stock_id = si.stock_id
                            WHERE si.stock_id = '{stock_id}'
                            Order by sh.date ASC;
                          """
    data = pd.DataFrame(db.query_data(sql))
    print(data['date'])
    data['date'] = pd.to_datetime(data['date']).dt.date
    stock_name = data['stock_name'][0].strip()
    latest_sql = f"""
                              SELECT *
                              FROM stock_realtime
                              WHERE stock_id = '{stock_id}'
                              ORDER BY date DESC, time DESC
                              LIMIT 1;
                             """
    today_data = pd.DataFrame(db.query_data(latest_sql))

    if not today_data.empty:
        today_data['date'] = pd.to_datetime(today_data['date'], format='%Y%m%d').dt.date
        today_data = today_data.rename(columns={'now_price': 'close_price'})
        today_data = today_data.drop(columns=['time','unix_time'])
        today_data = today_data.reset_index(drop=True)
        data = pd.concat([data, today_data], axis=0, ignore_index=True)
    return render_template('kline_chart.html', data=data.to_dict(orient='records'),stock_name=stock_name)


@app.route('/get_bid_data')
def get_bid_data():
    stock_id = request.args.get('stock_id')
    data = ak.stock_bid_ask_em(symbol=stock_id)
    data_transposed = data.set_index('item').T.reset_index()
    data_transposed['stock_id'] = stock_id
    data_transposed['time'] = datetime.now().timestamp()
    data_transposed.drop(columns='index', inplace=True)
    return jsonify(data_transposed.to_dict(orient='records'))

@app.route('/get_stock_data')
def get_stock_data():
    now = datetime.now()
    formatted_now = now.strftime('%Y-%m-%d %H:%M')
    stock_id = request.args.get('stock_id')
    period = request.args.get('period')
    if period == "intraday":
        if stock_id in most_used_stocks:
            redis_data = redis_db.get(f"stock_data:{stock_id}")
            if redis_data:
                print("from redis intra")
                data = pd.read_json(redis_data, dtype={'stock_id': str})
            else:
                print("from mysql intra")
                sql_query = f"SELECT * FROM stock_realtime WHERE stock_id = '{stock_id}' AND date = '{now.strftime('%Y%m%d')}'AND time >= '09:30:00'"
                data = pd.DataFrame(db.query_data(sql_query))
                if data.empty:
                    print("No data found in MySQL for intraday of stock_id:", stock_id)
        else:
            print("from mysql intra")
            sql_query = f"SELECT * FROM stock_realtime WHERE stock_id = '{stock_id}' AND date = '{now.strftime('%Y%m%d')}'AND time >= '09:30:00'"
            data = pd.DataFrame(db.query_data(sql_query))
            if data.empty:
                print("No data found in MySQL for intraday of stock_id:", stock_id)

    elif period == "daily":
        print("from mysql daily")
        sql = f"""
                        SELECT sh.*, si.stock_name
                        FROM stock_hist sh
                        LEFT JOIN stock_info si ON sh.stock_id = si.stock_id
                        WHERE si.stock_id = '{stock_id}'
                        Order by sh.date ASC;
                      """
        data = pd.DataFrame(db.query_data(sql))
        data['date'] = pd.to_datetime(data['date']).dt.date

        latest_sql = f"""
                          SELECT *
                          FROM stock_realtime
                          WHERE stock_id = '{stock_id}'
                          ORDER BY date DESC, time DESC
                          LIMIT 1;
                         """
        today_data = pd.DataFrame(db.query_data(latest_sql))
        print(today_data)
        if not today_data.empty:
            today_data['date'] = pd.to_datetime(today_data['date'], format='%Y%m%d').dt.date
            today_data = today_data.rename(columns={'now_price': 'close_price'})
            today_data = today_data.drop(columns=['time','unix_time'])
            today_data = today_data.reset_index(drop=True)
            data = pd.concat([data, today_data], axis=0, ignore_index=True)
    else:
        print("from mysql")
        data = db.fetch_data(stock_id, period)
        data['date'] = data['date'].apply(lambda t: t.strftime('%Y%M%D') if pd.notnull(t) else None)
        redis_db.set(f"stock_data_{stock_id}_intraday", data.to_json(orient='records'))
    return jsonify(data.to_dict(orient='records'))


def delete_keys_with_prefix(prefix):
    # 获取所有匹配前缀的键
    keys_to_delete = redis_db.keys(f"{prefix}*")

    # 如果找到匹配的键，则删除它们
    if keys_to_delete:
        redis_db.delete(*keys_to_delete)


def fetch_today(stock_id):
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df.rename(columns={
        '名称': 'stock_name',
        '代码': 'stock_id',
        '今开': 'open_price',
        '最新价': 'close_price',
        '最高': 'high_price',
        '最低': 'low_price',
        '成交量': 'volume',
        '成交额': 'turnover',
        '振幅': 'amplitude',
        '涨跌幅': 'price_change_pct',
        '涨跌额': 'price_change_amt',
        '换手率': 'turnover_rate'
    })
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df.filter([
        'stock_name',
        'stock_id',
        'open_price',
        'close_price',
        'high_price',
        'low_price',
        'volume',
        'turnover',
        'amplitude',
        'price_change_pct',
        'price_change_amt',
        'turnover_rate'
    ])
    return stock_zh_a_spot_em_df[stock_zh_a_spot_em_df['stock_id']==stock_id]

# def job1():
#     print("hey 5 second")
# scheduler.add_job(func = job1,
#         trigger = 'interval',
#         seconds=5,
#         id='job1')

if __name__ == '__main__':
    # scheduler.add_job(func = job1,
    #     trigger = 'interval',
    #     second='5',
    #     id='job1')

    app.run(host='0.0.0.0',debug=True)
