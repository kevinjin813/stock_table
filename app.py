import pymysql
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
import pandas as pd
import io
from flask import Flask, render_template, send_file, jsonify
import db
from flask import redirect, url_for, request

app = Flask(__name__)

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
    return render_template('kline_chart.html', data=result.to_dict(orient='records'),stock_name=stock_name)




@app.route('/get_stock_data')
def get_stock_data():
    stock_id = request.args.get('stock_id')
    period = request.args.get('period')
    # 根据不同的 period 获取数据
    data = db.fetch_data(stock_id,period)
    if period == "intraday":
        data['date'] = data['date'].apply(lambda t: t.strftime('%H:%M:%S') if pd.notnull(t) else None)
    else:
        data['date'] = data['date'].apply(lambda t: t.strftime('%Y%M%D') if pd.notnull(t) else None)
    return jsonify(data.to_dict(orient='records'))




if __name__ == '__main__':
    app.run(host='0.0.0.0')
