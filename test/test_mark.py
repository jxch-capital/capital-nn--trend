import preprocessing.mark_strategy_1 as process
import datetime
import strategy.simple_single_signal_k_pl_bs.mark as mark

# start = datetime.datetime.now()
# process.process()
# print(f'{(datetime.datetime.now() - start).seconds} s')

start = datetime.datetime.now()
mark.mark()
print(f'{(datetime.datetime.now() - start).seconds} s')
