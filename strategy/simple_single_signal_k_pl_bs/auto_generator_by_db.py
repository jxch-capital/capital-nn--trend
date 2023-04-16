import db.raw_k_slice_db as raw_k_slice_db
import db.strategy.simple_single_signal_k_pl_bs_mark as mark_db
from queue import Queue
import atomics
from concurrent.futures import wait, ALL_COMPLETED
from config import config
import random
import numpy as np
import datetime
import utils.stock_util as stock_util
import traceback


class Generator(object):
    def __init__(self, sbw_rate=None, slide=100, tv_rate=.8, maxsize=None, normalized=stock_util.percentage_normalized):
        if sbw_rate is None:
            sbw_rate = [.3, .3, .4]
        self.sbw_rate = sbw_rate
        self.slide = slide
        self.tv_rate = tv_rate
        self.training_queue = Queue() if maxsize is None else Queue(maxsize=maxsize)
        self.validation_queue = Queue() if maxsize is None else Queue(maxsize=maxsize)
        self.current_t_counter = 0
        self.current_v_counter = 0
        self.training = []
        self.validation = []
        self.normalized = normalized

    def y_arr(self, mark):
        arr = [0, 0, 0]
        arr[mark] = 1
        return arr

    def __slice_queue(self, x_queue, y_queue, use_index, the_queue):
        item = the_queue[random.randint(0, len(the_queue) - 1)]
        index = item[use_index][random.randint(0, len(item[use_index]) - 1)]
        item_df = item['mark_df'][index:index + self.slide]
        y_queue.put(item_df.tail(1)['mark'].values[0] + 1)
        item_df = item_df.loc[:, ['open', 'close', 'high', 'low']]
        item_df = self.normalized(item_df)
        x_queue.put(item_df.values)
        item['used_index'].append(index)
        # todo 乱序，否则模型无法训练

    def get_random_arr(self, size, the_queue):
        start = datetime.datetime.now()
        s_size = int(size * self.sbw_rate[0])
        b_size = int(size * self.sbw_rate[1])
        w_size = int(size * self.sbw_rate[2])

        x_queue = Queue(maxsize=size)
        y_queue = Queue(maxsize=size)
        s_tasks = [config.get_thread_pool().submit(self.__slice_queue, x_queue, y_queue, 's_index', the_queue)
                   for _ in range(0, s_size)]
        b_tasks = [config.get_thread_pool().submit(self.__slice_queue, x_queue, y_queue, 'b_index', the_queue)
                   for _ in range(0, b_size)]
        w_tasks = [config.get_thread_pool().submit(self.__slice_queue, x_queue, y_queue, 'w_index', the_queue)
                   for _ in range(0, w_size)]

        wait(s_tasks + b_tasks + w_tasks, return_when=ALL_COMPLETED)
        print(f'dataset success: {(datetime.datetime.now() - start).seconds} s')
        return {
            'x': np.asarray(list(x_queue.queue)),
            'y': np.asarray(list(y_queue.queue)),
        }

    def __slice_list(self, x_list, y_list, use_index, the_list):
        follow = ['open', 'close', 'high', 'low']
        item = the_list[random.randint(0, len(the_list) - 1)]
        index = item[use_index][random.randint(0, len(item[use_index]) - 1)]
        item_df = item['mark_df'].loc[index: index + self.slide - 1]
        y_data = item_df.tail(1)['mark'].values[0]
        item_df = item_df.loc[:, follow]
        item_df = self.normalized(item_df)
        x_data = item_df.values
        if x_data.shape[0] == self.slide and x_data.shape[1] == len(follow):
            is_invalid = False
            for row in x_data:
                if np.inf in row:
                    print(f'{item["mark_df"]["code"].values[0]}-{item["mark_df"]["interval"].values[0]}-{index}')
                    is_invalid = True
            if not is_invalid:
                x_inx = random.randint(0, len(x_list) - 1) if len(x_list) > 0 else 0
                x_list.insert(x_inx, x_data)
                y_list.insert(x_inx, y_data)
                item['used_index'].append(index)
        else:
            print(
                f'请检查数据问题{item["mark_df"]["code"].values[0]}-{item["mark_df"]["interval"].values[0]}-{index}-{index + self.slide + 1}--shape: {item_df.shape}')
            self.__slice_list(x_list, y_list, use_index, the_list)

    def get_random_arr_single(self, size, the_list):
        start = datetime.datetime.now()
        s_size = int(size * self.sbw_rate[0])
        b_size = int(size * self.sbw_rate[1])
        w_size = int(size * self.sbw_rate[2])

        x_list = []
        y_list = []
        for _ in range(0, s_size):
            self.__slice_list(x_list, y_list, 's_index', the_list)
        for _ in range(0, b_size):
            self.__slice_list(x_list, y_list, 'b_index', the_list)
        for _ in range(0, w_size):
            self.__slice_list(x_list, y_list, 'w_index', the_list)

        print(f'dataset success: {(datetime.datetime.now() - start).seconds} s')
        return {
            'x': np.asarray(x_list),
            'y': np.asarray(y_list),
        }

    def get_training_random_single(self, size=10000):
        return self.get_random_arr_single(size, self.training)

    def get_validation_random_single(self, size=10000):
        return self.get_random_arr_single(size, self.validation)

    def get_training_random(self, size=10000):
        return self.get_random_arr(size, self.training)

    def get_validation_random(self, size=10000):
        return self.get_random_arr(size, self.validation)

    def __build_index(self, mark_df, mark):
        return list(map(lambda x: x - self.slide + 1,
                        list(filter(lambda x: x > self.slide, mark_df[mark_df['mark'] == mark].index.values))))

    def __mark_df_obj(self, mark_df):
        return {
            'mark_df': mark_df,
            'a_index': list(range(0, mark_df.shape[0] - self.slide + 1)),
            's_index': self.__build_index(mark_df, 0),
            'b_index': self.__build_index(mark_df, 1),
            'w_index': self.__build_index(mark_df, 2),
            'used_index': [],
        }

    def __is_put_t(self):
        return self.current_t_counter == 0 or \
            self.current_t_counter / (
                    self.current_t_counter + self.current_v_counter) < self.tv_rate

    def __put(self, mark_df):
        if self.__is_put_t():
            self.training_queue.put(self.__mark_df_obj(mark_df))
            self.current_t_counter = self.current_t_counter + 1
        else:
            self.validation_queue.put(self.__mark_df_obj(mark_df))
            self.current_v_counter = self.current_v_counter + 1

    def __do_build(self, row):
        mark_df = mark_db.find_by_id_between(row['raw_k_start_id'], row['raw_k_end_id'])
        if mark_df is not None and not mark_df.empty:
            if mark_df['mark'][::-1].lt(2).idxmax() + 1 < (mark_df.shape[0] - 1):
                mark_df.drop(range(mark_df['mark'][::-1].lt(2).idxmax() + 1, mark_df.shape[0]), inplace=True)
            if mark_df['open'][::-1].le(0).idxmax() < (mark_df.shape[0] - 1):
                mark_df.drop(range(mark_df.head(1).index.values[0], mark_df['open'][::-1].le(0).idxmax() + 1),
                             inplace=True)
            if mark_df.head(1)['open'].values[0] < 2:
                mark_df.drop(range(mark_df.head(1).index.values[0], mark_df['open'].gt(2).idxmax()), inplace=True)
            if mark_df.head(1)['low'].values[0] < 2:
                mark_df.drop(range(mark_df.head(1).index.values[0], mark_df['low'].gt(2).idxmax()), inplace=True)
            mark_df['hl_diff'] = mark_df['high'] - mark_df['low']
            if mark_df['hl_diff'].gt(0.01).idxmax() > 0:
                mark_df.drop(range(mark_df.head(1).index.values[0], mark_df['hl_diff'].gt(0.01).idxmax()),
                             inplace=True)
            if mark_df.shape[0] > self.slide * 2:
                mark_df.reset_index(inplace=True, drop=True)
                self.__put(mark_df)

    def build(self):
        start = datetime.datetime.now()
        df = raw_k_slice_db.find_all().query(f'raw_k_end_id - raw_k_start_id > {self.slide * 2}')
        tasks = [config.get_thread_pool().submit(self.__do_build, row) for index, row in df.iterrows()]
        wait(tasks, return_when=ALL_COMPLETED)
        self.training = list(self.training_queue.queue)
        self.validation = list(self.validation_queue.queue)
        print(f'build success: {(datetime.datetime.now() - start).seconds} s')

    def build_debug(self):
        start = datetime.datetime.now()
        df = raw_k_slice_db.find_all().query(f'raw_k_end_id - raw_k_start_id > {self.slide * 2}')
        for index, row in df.iterrows():
            self.__do_build(row)
        self.training = list(self.training_queue.queue)
        self.validation = list(self.validation_queue.queue)
        print(f'build success: {(datetime.datetime.now() - start).seconds} s')
