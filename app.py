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



app = Flask(__name__)

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
scheduler.daemonic = False


redis_db = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

TRADING_START = "09:30"
TRADING_END = "15:00"

def is_trading_time():
    now = datetime.now()
    current_time = now.strftime('%H:%M')
    day_of_week = now.weekday()
    if day_of_week >= 5:  # 周六和周日不交易
        return False
    if TRADING_START <= current_time <= TRADING_END:
        return True
    return False

def fetch_and_save_spot_data():
    print(f"Fetching data at {datetime.now()}")
    # if not is_trading_time():
    #     return

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
    date_str = now.strftime('%Y%m%d')  # 日期，格式为“年月日”
    time_str = now.strftime('%H:%M')  # 时间，精确到分钟
    data['date'] = date_str
    data['time'] = time_str
    db.pd2sql(data, "stock_realtime")

    most_used_stocks = db.query_data("SELECT stock_id FROM most_use_stock")
    for stock in most_used_stocks:
        stock_id = stock['stock_id']
        previous_data = redis_db.get(f"stock_data:{stock_id}")
        if previous_data:
            previous_data_df = pd.read_json(previous_data, orient='records')
            new_stock_data = data[data['stock_id'] == stock_id]
            if not new_stock_data.empty:
                # 将新数据与旧数据合并
                combined_data = pd.concat([previous_data_df, new_stock_data])
                # 将合并后的数据存回 Redis
                redis_db.set(f"stock_data:{stock_id}", combined_data.to_json(orient='records'))
        else:
            # 如果 Redis 中没有旧数据，只存储新数据
            new_stock_data = data[data['stock_id'] == stock_id]
            if not new_stock_data.empty:
                redis_db.set(f"stock_data:{stock_id}", new_stock_data.to_json(orient='records'))




# @scheduler.task('cron', id='fetch_spot_data', minute='*')

def scheduled_task():
    print("start scheduler")
    fetch_and_save_spot_data()

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
    print("from mysql id")
    sql = f"""
                            SELECT sh.*, si.stock_name
                            FROM stock_hist sh
                            LEFT JOIN stock_info si ON sh.stock_id = si.stock_id
                            WHERE si.stock_id = '{stock_id}'
                            Order by sh.date ASC;
                          """
    data = pd.DataFrame(db.query_data(sql))
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
        today_data = today_data.drop(columns=['time'])
        today_data = today_data.reset_index(drop=True)
        data = pd.concat([data, today_data], axis=0, ignore_index=True)
    return render_template('kline_chart.html', data=data.to_dict(orient='records'),stock_name=stock_name)




@app.route('/get_stock_data')
def get_stock_data():
    now = datetime.now()
    formatted_now = now.strftime('%Y-%m-%d %H:%M')
    stock_id = request.args.get('stock_id')
    period = request.args.get('period')
    if period == "intraday":
        if stock_id in [stock['stock_id'] for stock in db.query_data("SELECT stock_id FROM most_use_stock")]:
            redis_data = redis_db.get(f"stock_data:{stock_id}")
            if redis_data:
                print("from redis intra")
                data = pd.read_json(redis_data, dtype={'stock_id': str})
            else:
                print("No data found in Redis for intraday of stock_id:", stock_id)
        else:
            print("from mysql")
            sql_query = f"SELECT * FROM stock_realtime WHERE stock_id = '{stock_id}' AND date = '{now.strftime('%Y%m%d')}'"
            data = pd.DataFrame(db.query_data(sql_query))
            data['date']=data['time']
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
        if not today_data.empty:
            today_data['date'] = pd.to_datetime(today_data['date'], format='%Y%m%d').dt.date
            today_data = today_data.rename(columns={'now_price': 'close_price'})
            today_data = today_data.drop(columns=[ 'time'])
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
    scheduler.add_job(func = job1,
        trigger = 'interval',
        second='5',
        id='job1')

    app.run(host='0.0.0.0',debug=True)
