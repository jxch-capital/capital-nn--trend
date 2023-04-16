from config import config
import pandas as pd

table_name = 'simple_single_signal_k_pl_bs_mark'


def save_batch(df):
    df.to_sql(table_name, config.pd2db_conn(), schema=config.db_schema, if_exists='append', index=False)


def table_index():
    with config.db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f'create index the_table_code_interval_k_index_date_index on {config.db_schema}.{table_name} (code, interval, k_index, date)')
        cur.execute(f'create index simple_single_signal_k_pl_bs_mark_id_index on {config.db_schema}.{table_name} (id)')
        conn.commit()


def find_by_id_between(start, end):
    return pd.read_sql(f'select * from {config.db_schema}.{table_name} where id between {start} and {end}',
                       con=config.pd2db_conn())
