from to_parquet import zst_submissions_to_parquet, zst_comments_to_parquet
import sys

if __name__ == '__main__':
    zst_file_path = sys.argv[1]
    parquet_folder_path = sys.argv[2]

    if zst_file_path.split('/')[-1].startswith('RS'):
        zst_submissions_to_parquet(
            zst_file_path=zst_file_path,
            parquet_folder_path=parquet_folder_path
        )
    elif zst_file_path.split('/')[-1].startswith('RC'):
        zst_comments_to_parquet(
            zst_file_path=zst_file_path,
            parquet_folder_path=parquet_folder_path
        )
    else:
        raise ValueError(f'Invalid zst file path: {zst_file_path}')