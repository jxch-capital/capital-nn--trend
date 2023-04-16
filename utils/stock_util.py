import stockstats
import numpy as np
import math


def neighbor_normalized(old_df):
    ss = stockstats.StockDataFrame.retype(old_df)
    df = ss[['open', 'close', 'high', 'low', 'volume',
             'open_-1_d', 'close_-1_d', 'high_-1_d', 'low_-1_d', 'volume_-1_d', 'mark']]

    df['rate_open'] = df['open'] / (df['open'] - df['open_-1_d'])
    df['rate_close'] = df['close'] / (df['close'] - df['close_-1_d'])
    df['rate_high'] = df['high'] / (df['high'] - df['high_-1_d'])
    df['rate_low'] = df['low'] / (df['low'] - df['low_-1_d'])
    df['rate_volume'] = df['volume'] / (df['volume'] - df['volume_-1_d'])

    return df[['rate_open', 'rate_close', 'rate_high', 'rate_low', 'rate_volume', 'mark']]


def percentage_normalized(old_df):
    f_open = old_df.head(1)['open'].values[0]

    old_df['open'] = (old_df['open'] - f_open) / f_open * 100
    old_df['close'] = (old_df['close'] - f_open) / f_open * 100
    old_df['high'] = (old_df['high'] - f_open) / f_open * 100
    old_df['low'] = (old_df['low'] - f_open) / f_open * 100

    if old_df['open'].values[0] == np.inf:
        print(old_df)

    return old_df


def max_min_normalized(old_df):
    df_min = np.min(old_df.min())
    df_max = np.max(old_df.max())
    old_df['open'] = old_df.loc[:, ['open']].apply(lambda x: (x - df_min) / (df_max - df_min))['open']
    old_df['close'] = old_df.loc[:, ['close']].apply(lambda x: (x - df_min) / (df_max - df_min))['close']
    old_df['high'] = old_df.loc[:, ['high']].apply(lambda x: (x - df_min) / (df_max - df_min))['high']
    old_df['low'] = old_df.loc[:, ['low']].apply(lambda x: (x - df_min) / (df_max - df_min))['low']
    return old_df


def z_score_scaler(x):
    return (x - np.mean(x)) / (np.std(x))


def z_score_normalized(old_df):

    old_df['open'] = old_df[['open']].apply(z_score_scaler)['open']
    old_df['close'] = old_df[['close']].apply(z_score_scaler)['close']
    old_df['high'] = old_df[['high']].apply(z_score_scaler)['high']
    old_df['low'] = old_df[['low']].apply(z_score_scaler)['low']

    if old_df['open'].values[0] == np.inf:
        print(old_df)

    return old_df
