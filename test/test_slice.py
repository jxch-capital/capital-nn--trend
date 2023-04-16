import preprocessing.slice as slice
import datetime

start = datetime.datetime.now()
slice.process()
print(f'{(datetime.datetime.now() - start).seconds} s')
