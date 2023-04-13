import stockstats


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
