import datadownload.yahoo as yahoo
from config import config
import pandas as pd

table_name = 'download_codes'


def codes2db():
    yahoo.codes_df().to_sql(table_name, config.pd2db_conn(), schema=config.db_schema,
                            if_exists='replace', index=False)


def find_all():
    return pd.read_sql(f'select * from {config.db_schema}.{table_name}', con=config.pd2db_conn())


def table_index():
    with config.db_conn() as conn:
        cur = conn.cursor()
        cur.execute(f'alter table {config.db_schema}.{table_name} add id bigserial')
        cur.execute(f'alter table {config.db_schema}.{table_name} add primary key(id)')
        cur.execute(
            f'create index download_codes_type_engine_index on {config.db_schema}.{table_name} (type, engine)')
        conn.commit()
