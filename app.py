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
from datetime import datetime
from flask_apscheduler import APScheduler



app = Flask(__name__)

redis_db = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

TRADING_START = "09:30"
TRADING_END = "15:00"

def is_trading_time():
    now = datetime.now().strftime('%H:%M')
    return TRADING_START <= now <= TRADING_END

@scheduler.task('cron', id='fetch_data', minute='*')
def fetch_data():
    if not is_trading_time():
        return

    today = datetime.now().strftime('%Y-%m-%d')
    stock_ids = ['000001', '000002']  # 示例股票代码列表

    for stock_id in stock_ids:
        data = ak.stock_zh_a_hist_min_em(symbol=stock_id, period='1', start_date=today, adjust="")
        # 对 data 进行处理
        redis_client.set(f'stock:{stock_id}', data.to_json(orient='records'))


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
    # 查询数据库以获取数据
    sql = """
            SELECT sh.*, si.stock_name
            FROM stock_hist sh
            LEFT JOIN stock_info si ON sh.stock_id = si.stock_id
            WHERE si.stock_id = '000001'
            Order by sh.date ASC;
          """
    result = pd.DataFrame(db.query_data(sql))
    print(result)
    result['date'] = pd.to_datetime(result['date']).dt.date
    print(result['date'])
    return render_template('kline_chart.html', data=result.to_dict(orient='records'))

@app.route('/kline_chart/<stock_id>')
def kline_chart_id(stock_id):
    redis_data = redis_db.get(f"stock_data_{stock_id}_daily")

    if redis_data:
        # 如果 Redis 中存在数据，直接使用这些数据
        result = pd.read_json(redis_data,dtype={'stock_id':str})
        stock_name = result['stock_name'][0].replace(' ', '')
    else:
    # 查询数据库以获取数据
        sql = f"""
                SELECT sh.*, si.stock_name
                FROM stock_hist sh
                LEFT JOIN stock_info si ON sh.stock_id = si.stock_id
                WHERE si.stock_id = '{stock_id}'
                Order by sh.date ASC;
              """
        result = pd.DataFrame(db.query_data(sql))
        result['date'] = pd.to_datetime(result['date']).dt.date
        stock_name = result['stock_name'][0].replace(' ','')
        redis_db.set(f"stock_data_{stock_id}_daily", result.to_json(orient='records'))
    return render_template('kline_chart.html', data=result.to_dict(orient='records'),stock_name=stock_name)




@app.route('/get_stock_data')
def get_stock_data():
    stock_id = request.args.get('stock_id')
    period = request.args.get('period')
    redis_data = redis_db.get(f"stock_data_{stock_id}_{period}")

    if redis_data:
        # 如果 Redis 中存在数据，直接使用这些数据
        print("from redis")
        data = pd.read_json(redis_data, dtype={'stock_id': str})
        stock_name = data['stock_name'][0].replace(' ', '')
    else:
        print("from mysql")
        # 根据不同的 period 获取数据
        data = db.fetch_data(stock_id,period)
        if period == "intraday":
            data['date'] = data['date'].apply(lambda t: t.strftime('%H:%M:%S') if pd.notnull(t) else None)
            redis_db.set(f"stock_data_{stock_id}_intraday", data.to_json(orient='records'))
        else:
            data['date'] = data['date'].apply(lambda t: t.strftime('%Y%M%D') if pd.notnull(t) else None)
            redis_db.set(f"stock_data_{stock_id}_intraday", data.to_json(orient='records'))

    return jsonify(data.to_dict(orient='records'))




if __name__ == '__main__':
    app.run(host='0.0.0.0')
