import stockstats


def calculate(df):
    ss = stockstats.StockDataFrame.retype(df.copy())
    df['atr'] = ss[['open', 'close', 'high', 'low', 'volume', 'atr']]['atr']
    return df
