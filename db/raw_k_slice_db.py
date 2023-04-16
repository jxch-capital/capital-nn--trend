from config import config
import pandas as pd
import db.raw_k_db as raw_k_db
import db.codes_db as codes_db
from datadownload import intervals_d

table_name = 'raw_k_slice'


def slice2db():
    slice_df = pd.DataFrame([], columns=['code', 'interval', 'raw_k_start_id', 'raw_k_end_id'])
    for codes in codes_db.find_all()['codes'].values:
        for code in codes.split(','):
            for interval in intervals_d:
                df = raw_k_db.find_by_code_and_interval(code, interval)
                if not df.empty:
                    slice_df = pd.concat([slice_df, pd.DataFrame([{
                        'code': df.head(1)['code'].values[0],
                        'interval': df.head(1)['interval'].values[0],
                        'raw_k_start_id': df.head(1)['id'].values[0],
                        'raw_k_end_id': df.tail(1)['id'].values[0],
                    }])])
                print(f'---->> {code}-{interval}')
    slice_df.to_sql(table_name, config.pd2db_conn(), schema=config.db_schema,
                    if_exists='replace', index=False)


def table_index():
    with config.db_conn() as conn:
        cur = conn.cursor()
        cur.execute(f'alter table {config.db_schema}.{table_name} add id bigserial')
        cur.execute(f'alter table {config.db_schema}.{table_name} add primary key(id)')
        cur.execute(
            f'create index raw_k_slice_code_interval_index on {config.db_schema}.{table_name} (code, interval)')
        conn.commit()


def find_all():
    return pd.read_sql(f'select * from {config.db_schema}.{table_name}', con=config.pd2db_conn())

