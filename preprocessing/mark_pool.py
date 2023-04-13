from queue import Queue

mark_label = 'mark'
trend_marks = {
    'wait': 0,
    'buy': 1,
    'sell': 2,
}


class MarkPool(object):
    def __init__(self, df, min_pl_ratio=2):
        self.buy_pool = []
        self.sell_pool = []
        self.df = df.copy()
        self.df[mark_label] = trend_marks['wait']
        self.average_daily_range_5_queue = Queue(maxsize=5)
        self.average_daily_range_5 = None
        self.min_pl_ratio = min_pl_ratio

    def is_little_bar(self, row):
        return self.average_daily_range_5 is not None and self.average_daily_range_5 / (row['high'] - row['low']) > 3

    @staticmethod
    def is_cross_bar(row):
        return abs(row['open'] - row['close']) == 0 or (row['high'] - row['low']) / abs(row['open'] - row['close']) > 5

    @staticmethod
    def is_bull_bar(row):
        return row['close'] > row['open']

    @staticmethod
    def is_bear_bar(row):
        return row['close'] < row['open']

    def mark_buy(self, row, index):
        self.buy_pool.append({
            'mark': trend_marks['buy'],
            'row': row,
            'stop': row['low'],
            'in': row['high'],
            'index': index,
        })

    def mark_sell(self, row, index):
        self.sell_pool.append({
            'mark': trend_marks['sell'],
            'row': row,
            'stop': row['high'],
            'in': row['low'],
            'index': index,
        })

    def mark_write(self, success_list):
        for item in success_list:
            self.df.loc[item['index'], mark_label] = item['mark']

    def buy_success(self, row, item):
        return (row['high'] - item['in']) / (item['in'] - item['stop']) >= self.min_pl_ratio

    def sell_success(self, row, item):
        return (row['low'] - item['in']) / (item['in'] - item['stop']) >= self.min_pl_ratio

    def update_mark(self, row):
        # 只保留未被止损的入场点
        self.buy_pool = list(filter(lambda item: item['stop'] < row['low'], self.buy_pool))
        self.sell_pool = list(filter(lambda item: item['stop'] > row['high'], self.sell_pool))
        # 达到目标盈亏比
        self.mark_write(list(filter(lambda item: self.buy_success(row, item), self.buy_pool)))
        self.buy_pool = list(filter(lambda item: not self.buy_success(row, item), self.buy_pool))
        self.mark_write(list(filter(lambda item: self.sell_success(row, item), self.sell_pool)))
        self.sell_pool = list(filter(lambda item: not self.sell_success(row, item), self.sell_pool))

    def update_dynamic(self, row):
        if self.average_daily_range_5_queue.qsize() == 5:
            self.average_daily_range_5_queue.get()
        self.average_daily_range_5_queue.put(row['high'] - row['low'])
        self.average_daily_range_5 = sum(
            self.average_daily_range_5_queue.queue) / self.average_daily_range_5_queue.qsize()

    def mark(self):
        for index, row in self.df.iterrows():
            print(f'{index}', end='\r', flush=True)
            self.update_dynamic(row)
            self.update_mark(row)
            if not self.is_cross_bar(row) and not self.is_little_bar(row):
                if self.is_bull_bar(row):
                    self.mark_buy(row, index)
                elif self.is_bear_bar(row):
                    self.mark_sell(row, index)
        return self.df[mark_label]
