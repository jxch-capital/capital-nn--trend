import db.raw_k_db as db
import utils.socket_util as socket_util
import datetime

socket_util.set_proxy()
#
start = datetime.datetime.now()
db.raw_k2db()
db.table_index()
print(f'耗时：{(datetime.datetime.now() - start).seconds} s')

# start = datetime.datetime.now()
# df = db.find_all_by_code(['AAPL','TSM'])
# print(f'查询耗时：{(datetime.datetime.now() - start).seconds} s')
# print(df)

# db.table_index()