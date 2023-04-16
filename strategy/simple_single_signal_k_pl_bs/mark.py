from queue import Queue
import db.raw_k_db as raw_k_db
import db.codes_db as codes_db
from datadownload import intervals_d
import db.strategy.simple_single_signal_k_pl_bs_mark as mark_db
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
from config import config

mark_label = 'mark'

# 不可改，generator 耦合为数组坐标了
trend_marks = {
    'sell': 0,
    'buy': 1,
    'wait': 2,
}


# 基于单信号K线的盈亏比入场点（单标的）
class MarkPool(object):
    def __init__(self, raw_df, min_pl_ratio=2, average_daily_range_d=5, little_bar_rate=3, cross_bar_rate=5):
        self.buy_pool = []
        self.sell_pool = []
        self.raw_df = raw_df.copy()
        self.raw_df[mark_label] = trend_marks['wait']
        self.min_pl_ratio = min_pl_ratio
        self.little_bar_rate = little_bar_rate
        self.cross_bar_rate = cross_bar_rate
        self.average_daily_range = None
        self.average_daily_range_d = average_daily_range_d
        self.average_daily_range_queue = Queue(maxsize=self.average_daily_range_d)

    def is_little_bar(self, row):
        return self.average_daily_range is not None and self.average_daily_range / (
                row['high'] - row['low']) > self.little_bar_rate

    def is_cross_bar(self, row):
        return abs(row['open'] - row['close']) == 0 or (row['high'] - row['low']) / abs(
            row['open'] - row['close']) > self.cross_bar_rate

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
            self.raw_df.loc[item['index'], mark_label] = item['mark']

    def buy_success(self, row, item):
        return (row['high'] - item['in']) / (item['in'] - item['stop']) >= self.min_pl_ratio

    def sell_success(self, row, item):
        return (row['low'] - item['in']) / (item['in'] - item['stop']) >= self.min_pl_ratio

    def update_mark(self, row):
        self.buy_pool = list(filter(lambda item: item['stop'] < row['low'], self.buy_pool))
        self.sell_pool = list(filter(lambda item: item['stop'] > row['high'], self.sell_pool))
        self.mark_write(list(filter(lambda item: self.buy_success(row, item), self.buy_pool)))
        self.buy_pool = list(filter(lambda item: not self.buy_success(row, item), self.buy_pool))
        self.mark_write(list(filter(lambda item: self.sell_success(row, item), self.sell_pool)))
        self.sell_pool = list(filter(lambda item: not self.sell_success(row, item), self.sell_pool))

    def update_dynamic(self, row):
        if self.average_daily_range_queue.qsize() == self.average_daily_range_d:
            self.average_daily_range_queue.get()
        self.average_daily_range_queue.put(row['high'] - row['low'])
        self.average_daily_range = sum(
            self.average_daily_range_queue.queue) / self.average_daily_range_queue.qsize()

    def mark(self):
        for index, row in self.raw_df.iterrows():
            self.update_dynamic(row)
            self.update_mark(row)
            if not self.is_cross_bar(row) and not self.is_little_bar(row):
                if self.is_bull_bar(row):
                    self.mark_buy(row, index)
                elif self.is_bear_bar(row):
                    self.mark_sell(row, index)
        return self.raw_df[mark_label]


def do_mark(code, interval):
    df = raw_k_db.find_by_code_and_interval(code, interval)
    df[mark_label] = MarkPool(df).mark()
    mark_db.save_batch(df)
    print(f'---->> {code}-{interval}')


def mark():
    tasks = []
    for codes in codes_db.find_all()['codes'].values:
        for code in codes.split(','):
            for interval in intervals_d:
                tasks.append(config.get_thread_pool().submit(do_mark, code, interval))
    wait(tasks, return_when=ALL_COMPLETED)
