import akshare as ak
import pandas as pd
import threading
from sqlalchemy import create_engine
# 获取股票代码列表
code_list = ak.stock_info_a_code_name()

code_list = code_list.rename(columns={
    'code': 'stock_id',
    'name': 'stock_name'
})

db_username = 'root'
db_password = 'qwertyui'
db_host = 'localhost'  # 或者其他主机地址
db_port = '3306'
db_name = 'schemas'
table_name = 'stock_info'  # 要创建或写入的表的名称

# 创建数据库引擎
engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# 将数据写入 MySQL
code_list.to_sql(table_name, engine, if_exists='replace', index=False)

# 关闭数据库连接
engine.dispose()
