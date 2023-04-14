

def to1_overall(df):
    arr = []
    for index, row in df.iterrows():
        arr.append(row['close'])
        arr.append(row['high'])
        arr.append(row['low'])
        arr.append(row['open'])
        arr.append(row['volume'])
    return arr


def to1_neighbor(df):
    arr = []
    for index, row in df.iterrows():
        arr.append(row['rate_close'])
        arr.append(row['rate_high'])
        arr.append(row['rate_low'])
        arr.append(row['rate_open'])
        arr.append(row['rate_volume'])
    return arr
