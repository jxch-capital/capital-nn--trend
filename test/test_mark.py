import preprocessing.mark_strategy_1 as process
import datetime

start = datetime.datetime.now()
process.process()
print(f'{(datetime.datetime.now() - start).seconds} s')
