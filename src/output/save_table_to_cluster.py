import duckdb
import sys
import time

def _move_table_to_shared_folder(table_read_dir: str, table_write_path: str):
    con = duckdb.connect(database=':memory:')
    q = f"""
    SELECT *
    FROM read_parquet('{table_read_dir}/*.parquet')
    """
    con.sql(q).write_parquet(table_write_path)

def move_table_to_shared_folder(file_id):
    time.sleep(10)
    print(f'Moving submissions table for file {file_id}')
    read_dir = '/cluster/scratch/anmusso/reddit/spark/warehouse/submissions_with_keywords/'
    write_path = f'/cluster/work/gess/coss/users/anmusso/keyword_output/submissions_{file_id}.parquet'
    _move_table_to_shared_folder(table_read_dir=read_dir, table_write_path=write_path)
    print(f'Moving comments table for file {file_id}')
    read_dir = '/cluster/scratch/anmusso/reddit/spark/warehouse/comments_and_submissions_with_keywords/'
    write_path = f'/cluster/work/gess/coss/users/anmusso/keyword_output/comments_{file_id}.parquet'
    _move_table_to_shared_folder(table_read_dir=read_dir, table_write_path=write_path)
    print(f'Done')

if __name__ == '__main__':
    args = sys.argv[1:]
    move_table_to_shared_folder(file_id=args[0])