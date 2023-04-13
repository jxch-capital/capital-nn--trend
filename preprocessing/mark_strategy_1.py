import numpy as np

from utils.file_util import file_paths
import pandas as pd
from datadownload.yahoo import intervals_d
from preprocessing.mark_pool import MarkPool, mark_label

raw_path = '../res/raw/'
mark_path = '../res/mark_strategy_1'
column_labels = {
    'adj_close': {'suffix': ''},
    'close': {'suffix': '.1'},
    'high': {'suffix': '.2'},
    'low': {'suffix': '.3'},
    'open': {'suffix': '.4'},
    'volume': {'suffix': '.5'},
}


def column_names_by_code(df_columns):
    column_names = {}
    for i in range(0, len(df_columns) // len(column_labels)):
        code = df_columns[i]
        code_label_names = {}
        for label, val in column_labels.items():
            code_label_names[label] = code + val['suffix']
        column_names[code] = code_label_names
    return column_names


def df_by_code(df):
    dfs_code = {}
    for code, label_names in column_names_by_code(df.columns).items():
        new_df = pd.DataFrame([])
        for label, name in label_names.items():
            new_df[label] = df[name]
        new_df['invalid'] = new_df.apply(lambda x: np.NAN if round(x['high'], 2) == round(x['low'], 2) else 0, axis=1)
        new_df.dropna(axis='index', inplace=True)
        new_df.drop(['invalid'], axis=1, inplace=True)
        new_df.reset_index(inplace=True, drop=True)
        new_df.drop(new_df.head(100).index, inplace=True)
        new_df.reset_index(inplace=True, drop=True)
        dfs_code[code] = new_df
    return dfs_code


def pruning(dfs_code):
    for code, df in dfs_code.items():
        df.drop(['adj_close'], axis=1, inplace=True)
    return dfs_code


def mark(dfs_code):
    for code, df in dfs_code.items():
        print(f'mark:{code}', end="\r", flush=True)
        df[mark_label] = MarkPool(df).mark()
    return dfs_code


def process():
    all_file_paths = file_paths(raw_path)
    for path in all_file_paths:
        filepath = f'{mark_path}/{path.replace(raw_path, "")}'
        with pd.ExcelWriter(filepath) as writer:
            for interval in intervals_d:
                df = pd.read_excel(path, sheet_name=interval, header=1)
                df.drop(['Unnamed: 0'], axis=1, inplace=True)
                df.drop(index=[0], inplace=True)
                dfs_code = df_by_code(df)
                pruning(dfs_code)
                mark(dfs_code)

                for code, item_df in dfs_code.items():
                    item_df.to_excel(excel_writer=writer, sheet_name=f'{code}-{interval}')
            print(f'\nsuccess. {path}')
    print(f'success.')

