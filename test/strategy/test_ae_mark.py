import tensorflow as tf
import utils.socket_util as su
import yfinance as yf
import cufflinks as cf
import db.raw_k_db as k
import indicators.atr as atr
import indicators.advantage_excursion as ae
import db.codes_db as cdb
import datetime
import random
import numpy as np

su.set_proxy()
cf.set_config_file(offline=True)

# qqq = k.find_by_code_and_interval('QQQ', '1d')
# qqq = atr.calculate(qqq)
# qqq = ae.calculate(qqq)
# qqq = ae.mark(qqq)
#
# samples = ae.get_training_samples(qqq)
# k_hots = ae.training_samples_by(samples, 'label_bull_hot')
#
# print(k_hots)

# codes = cdb.find_all_code()
# random.shuffle(codes)
# sp = int(len(codes)*0.7)
#
# t_codes = codes[0:sp]
# v_codes = codes[sp:len(codes)]
#
# samples = None
# i = 0
# for code in t_codes:
#     print(f'{i} / {len(t_codes)} -> {code}')
#     i = i + 1
#     code_k = k.find_by_code_and_interval(code, '1d')
#     code_k = code_k.loc[code_k.query('low > 1')['low'].idxmin():code_k.index[-1]]
#     code_k = atr.calculate(code_k)
#     code_k = ae.calculate(code_k)
#     code_k = ae.mark(code_k)
#     samples_k = ae.get_training_samples(code_k)
#     if samples is None:
#         samples = samples_k
#     else:
#         samples.append(samples_k)
# print(samples)

xlp = k.find_by_code_and_interval('XLP', '1d')
xlp = xlp[~xlp.index.duplicated(keep='first')]
xlp = xlp.loc[xlp.query('low > 2')['low'].idxmin():xlp.index[-1]] if xlp.loc[xlp.index[0]]['low'] < 2 else xlp
xlp = atr.calculate(xlp)
xlp = xlp.drop(xlp[xlp['atr']==0].index)
xlp = ae.calculate(xlp)
xlp = ae.mark(xlp)

samples = ae.get_training_samples(xlp)
k_hots = ae.training_samples_by(samples, 'k')

print(k_hots)