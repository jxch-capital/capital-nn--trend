from utils.file_util import file_paths
import pandas as pd
from preprocessing.slice_pool import SlicePool

mark_path = '../res/mark_strategy_1/'
slice_path = '../res/slice/'


def process():
    slice_pool = SlicePool(slice_path)
    all_file_paths = file_paths(mark_path)
    for path in all_file_paths:
        sheets = pd.read_excel(path, sheet_name=None)
        for sheet_name, df in sheets.items():
            df.drop(['Unnamed: 0'], axis=1, inplace=True)
            if df.shape[0] > 1200:
                df.drop(range(df['mark'][::-1].gt(0).idxmax() + 1, df['mark'][::-1].size), inplace=True)
                df.reset_index(inplace=True, drop=True)
                df.drop(range(0, df['low'].gt(2).idxmax()), inplace=True)
                df.reset_index(inplace=True, drop=True)
                if df.shape[0] > 1200:
                    slice_pool.submit(df)
        print(path)
    slice_pool.write_queue()
