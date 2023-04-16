import db.raw_k_slice_db as raw_k_slice_db


# df = raw_k_slice_db.find_all()
# df['len'] = df['raw_k_end_id'] - df['raw_k_start_id']
# print(df)


raw_k_slice_db.slice2db()