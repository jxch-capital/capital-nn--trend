import datetime
from concurrent.futures import wait, ALL_COMPLETED

import numpy as np
import db.raw_k_db as k
import indicators.atr as atr
from config import config


def calculate(df, length=120):
    df.reset_index(inplace=True)
    df['bull_ae'] = np.NAN
    df['bear_ae'] = np.NAN

    for index, row in df.iloc[0:df.index[-1] - length].iterrows():
        high = df.iloc[df.iloc[index:index + length]['high'].idxmax()]['high']
        low = df.iloc[df.iloc[index:index + length]['low'].idxmin()]['low']

        bull_mfe = (high - row['high']) / row['atr']
        bull_mae = (row['high'] - low) / row['atr']
        bear_mfe = (row['low'] - low) / row['atr']
        bear_mae = (high - row['low']) / row['atr']

        df.loc[index, 'bull_ae'] = bull_mfe / bull_mae if bull_mae != 0 else 99
        df.loc[index, 'bear_ae'] = bear_mfe / bear_mae if bear_mae != 0 else 99

        if row['atr'] == 0:
            print(index)

    df.set_index('date', inplace=True)
    return df


def get_mark(ae):
    if ae == np.NAN or ae == np.inf:
        return np.NAN
    if 0 <= ae <= 1:
        return 0
    if 1 <= ae <= 2:
        return 1
    if 2 <= ae <= 4:
        return 2
    if 4 <= ae <= 7:
        return 3
    if 7 <= ae <= 10:
        return 4
    if 10 <= ae <= 15:
        return 5
    if 15 <= ae <= 20:
        return 6
    if 20 <= ae <= 30:
        return 7
    if 30 <= ae:
        return 8


def mark(df):
    df['bull_ae_mark'] = df['bull_ae'].apply(lambda x: get_mark(x))
    df['bear_ae_mark'] = df['bear_ae'].apply(lambda x: get_mark(x))
    df['total_ae_bull_mark'] = df['bull_ae_mark'] - df['bear_ae_mark']
    return df


def hot(label, count=9):
    return list(map(lambda x: int(x), list(str(bin(int(str(1 << label), 10))).replace('0b', '').zfill(count))))


def get_training_samples(df, sliding=120):
    samples = []
    training_df = df.dropna(axis=0, how='any').reset_index()[
        ['close', 'open', 'high', 'low', 'bull_ae_mark', 'bear_ae_mark', 'total_ae_bull_mark']]
    for index, row in training_df.iloc[0:training_df.index[-1] - sliding].iterrows():
        if row['open'] > 0:
            k = training_df.iloc[index:index + sliding]
            samples.append({
                'k': np.asarray(k[['close', 'open', 'high', 'low']].values),
                'label_bull': k.iloc[-1]['bull_ae_mark'],
                'label_bull_hot': np.array(hot(int(k.iloc[-1]['bull_ae_mark']))),
                'label_bear': k.iloc[-1]['bear_ae_mark'],
                'label_bear_hot': np.array(hot(int(k.iloc[-1]['bear_ae_mark']))),
                'label_total': k.iloc[-1]['total_ae_bull_mark'],
                'label_total_hot': np.array(hot(9 - int(k.iloc[-1]['total_ae_bull_mark']), 18)),
            })

    return samples


def training_samples_by(samples, by):
    return np.asarray(list(map(lambda x: x[by], samples)))


def build_code_samples(code, samples):
    print(f'--> {code}')
    code_k = k.find_by_code_and_interval(code, '1d')
    code_k = code_k[~code_k.index.duplicated(keep='first')]
    code_k = code_k.loc[code_k.query('low > 2')['low'].idxmin():code_k.index[-1]].copy() if code_k.loc[code_k.index[0]][
                                                                                                'low'] < 2 else code_k
    code_k = atr.calculate(code_k)
    code_k = code_k.drop(code_k[code_k['atr'] == 0].index)
    code_k = calculate(code_k)
    code_k = mark(code_k)
    samples_k = get_training_samples(code_k)

    samples.extend(samples_k)


def build_samples(codes):
    start = datetime.datetime.now()
    pool = config.get_thread_pool()
    samples = []
    tasks = [pool.submit(build_code_samples, code, samples) for code in codes]
    wait(tasks, return_when=ALL_COMPLETED)
    print(f'耗时：{(datetime.datetime.now() - start).seconds} s')
    return samples


def sample_normalization(sample_k, normalization_samples_k):
    open_price = sample_k[0][1]
    normalization_samples_k.append(sample_k / open_price)


def normalization(samples_k):
    start = datetime.datetime.now()
    pool = config.get_thread_pool()
    normalization_samples_k = []
    tasks = [pool.submit(sample_normalization, samples_k[i], normalization_samples_k) for i in
             range(0, samples_k.shape[0])]
    wait(tasks, return_when=ALL_COMPLETED)
    print(f'耗时：{(datetime.datetime.now() - start).seconds} s')
    return np.asarray(normalization_samples_k)


def decoding_total_mark(b_arr):
    return 9 - (18 - b_arr.index(1))


def total_mark_3(mark_num):
    if -2 <= mark_num <= 2:
        return 0
    if mark_num > 2:
        return 1
    if mark_num < -2:
        return 2


def total_mark_2(mark_num):
    if mark_num >= 0:
        return 0
    else:
        return 1


def re_code_total_mark3(mark_np_arr):
    new_code = []
    for i in range(0, len(mark_np_arr)):
        new_code.append(np.array(hot(total_mark_3(decoding_total_mark(mark_np_arr[i].tolist())), 3)))
        if len(new_code[-1]) == 4:
            print(i)
    return np.array(new_code)


def re_code_total_mark2_hot(mark_np_arr):
    new_code = []
    for i in range(0, len(mark_np_arr)):
        new_code.append(np.array(hot(total_mark_2(decoding_total_mark(mark_np_arr[i].tolist())), 2)))
    return np.array(new_code)


def re_code_total_mark2(mark_np_arr):
    new_code = []
    for i in range(0, len(mark_np_arr)):
        new_code.append(np.array(total_mark_2(decoding_total_mark(mark_np_arr[i].tolist()))))
    return np.array(new_code)
