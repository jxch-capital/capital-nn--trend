import datadownload.yahoo as yahoo
from config import config


def codes2db():
    yahoo.codes_df().to_sql('download_codes', config.pd2db_conn(), schema=config.db_schema,
                            if_exists='replace', index=False)



