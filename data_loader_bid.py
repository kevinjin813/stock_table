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

stock_df = pd.read_sql(sql, engine)

quater = len(stock_df) // 4
code_list_1 = stock_df[:quater]['stock_id']
code_list_2 = stock_df[quater:2*quater]['stock_id']
code_list_3 = stock_df[2*quater:3*quater]['stock_id']
code_list_4 = stock_df[3*quater:]['stock_id']



def get_info(codes, result_list,start_date,end_date,adjust="",period="daily",engine):
    global monitor
    for code in codes:
        data = ak.stock_bid_ask_em(symbol=code)
        data_transposed = data.set_index('item').T.reset_index()
        data_transposed['stock_id'] =code
        data_transposed['time'] = datetime.now().timestamp()
        data_transposed.drop(columns='index', inplace=True)

        delete_sql = f"DELETE FROM {table_name} WHERE stock_id = :stock_id"
        conn.execute(delete_sql, {'stock_id': row['stock_id']})

        # 插入新数据
        row_dict = row.to_dict()
        columns = ', '.join(row_dict.keys())
        placeholders = ', '.join([f":{key}" for key in row_dict])
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        conn.execute(insert_sql, row_dict)

        with engine.connect() as conn:
            for index, row in df.iterrows():
                # 删除现有数据
                delete_sql = f"DELETE FROM {table_name} WHERE stock_id = :stock_id"
                conn.execute(delete_sql, {'stock_id': row['stock_id']})

                # 插入新数据
                row_dict = row.to_dict()
                columns = ', '.join(row_dict.keys())
                placeholders = ', '.join([f":{key}" for key in row_dict])
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                conn.execute(insert_sql, row_dict)


        with lock:
            monitor += 1
            print(f"Code {code} retrieved, total retrieved: {monitor}")
    result_list.append(temp_df)


# 创建线程
end_date = datetime.now().strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=4)).strftime("%Y%m%d")
# thread1 = threading.Thread(target=get_info, args=(code_list_1, df_list,start_date,end_date))
# thread2 = threading.Thread(target=get_info, args=(code_list_2, df_list,start_date,end_date))
# thread3 = threading.Thread(target=get_info, args=(code_list_3, df_list,start_date,end_date))
thread4 = threading.Thread(target=get_info, args=(code_list_4, df_list,start_date,end_date))
# 启动线程
# thread1.start()
# thread2.start()
# thread3.start()
thread4.start()
# 等待线程结束
# thread1.join()
# thread2.join()
# thread3.join()
thread4.join()
# 合并结果
for temp_df in df_list:
    df = pd.concat([df, temp_df], ignore_index=True)



