from queue import Queue
import pandas as pd


class SlicePool(object):
    def __init__(self, excel_name_path, maxsize=20):
        self.excel_name_path = excel_name_path
        self.maxsize = maxsize
        self.file_count = 0
        self.queue = Queue(maxsize=maxsize)

    def write_queue(self):
        with pd.ExcelWriter(f'{self.excel_name_path}{self.file_count}.xlsx') as writer:
            for i in range(self.queue.qsize()):
                self.queue.get().to_excel(excel_writer=writer, sheet_name=f'{i}')
        self.file_count = self.file_count + 1

    def submit(self, df):
        if self.queue.qsize() == self.maxsize:
            self.write_queue()
        self.queue.put(df)
