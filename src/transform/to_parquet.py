from typing import Any, Dict, List, Union, Callable

import logging
import os
from pydantic import BaseModel, ValidationError
import pyarrow as pa
import pyarrow.parquet as pq
import gc
from datetime import datetime, timezone
import shutil

from zst_io import read_lines_zst

logger = logging.getLogger('to_parquet')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


class RedditSubmission(BaseModel):
    author: str | None
    subreddit: str
    score: int
    created_utc: int
    title: str
    id: str
    num_comments: int | None
    selftext: str | None
    media: Any | None


def _extract_submissions_line_data(line_str: str) -> Dict[str, Any]:
    reddit_submission = RedditSubmission.model_validate_json(line_str)
    dt = datetime.fromtimestamp(reddit_submission.created_utc, tz=timezone.utc)
    line_parse = {
        'author': reddit_submission.author,
        'subreddit': reddit_submission.subreddit,
        'score': reddit_submission.score,
        'created_utc': reddit_submission.created_utc,
        'title': reddit_submission.title,
        'id': f't3_{reddit_submission.id}', # t3 is the prefix for submissions. Reddit uses this everywhere else (e.g. in the link_id in the comments, but not in this id, so we need to add it here so joins work)
        'num_comments': reddit_submission.num_comments,
        'selftext': reddit_submission.selftext,
        'media': False if reddit_submission.media is None else True,
        'year': dt.year,
        'month': dt.month
    }
    return line_parse

def _save_submissions_batch_to_dataset(lines: List[Dict[str, Any]], dataset_path: str):
    schema = pa.schema([
        ('author', pa.string()),
        ('subreddit', pa.string()),
        ('score', pa.int64()),
        ('created_utc', pa.int64()),
        ('title', pa.string()),
        ('id', pa.string()),
        ('num_comments', pa.int64()),
        ('selftext', pa.string()),
        ('media', pa.bool_()),
        ('year', pa.int32()),
        ('month', pa.int32())
    ])

    table = pa.Table.from_pylist(lines, schema=schema)
    pq.write_to_dataset(table, root_path=dataset_path, partition_cols=['year', 'month'], compression='snappy', existing_data_behavior='overwrite_or_ignore')


def zst_submissions_to_parquet(zst_file_path: str, parquet_folder_path: str):
    _zst_to_parquet(
        zst_file_path=zst_file_path,
        parquet_folder_path=parquet_folder_path,
        _extract_line_data=_extract_submissions_line_data,
        _save_batch_to_dataset=_save_submissions_batch_to_dataset
    )


class RedditComment(BaseModel):
    author: str
    subreddit: str
    subreddit_id: str
    score: int
    created_utc: int
    body: str
    id: str
    link_id: str
    parent_id: str

def _extract_comments_line_data(line_str: str) -> Dict[str, Any]:
    reddit_comment = RedditComment.model_validate_json(line_str)
    dt = datetime.fromtimestamp(reddit_comment.created_utc, tz=timezone.utc)
    line_parse = {
        'author': reddit_comment.author,
        'subreddit': reddit_comment.subreddit,
        'subreddit_id': reddit_comment.subreddit_id,
        'score': reddit_comment.score,
        'created_utc': reddit_comment.created_utc,
        'body': reddit_comment.body,
        'id': f't1_{reddit_comment.id}', # t1 is the prefix for comments. Reddit uses this everywhere else (e.g. in the parent_id, but not in this id, so we need to add it here so joins work)
        'link_id': reddit_comment.link_id,
        'parent_id': reddit_comment.parent_id,
        'year': dt.year,
        'month': dt.month
    }
    return line_parse

def _save_comments_batch_to_dataset(lines: List[Dict[str, Any]], dataset_path: str):
    schema = pa.schema([
        ('author', pa.string()),
        ('subreddit', pa.string()),
        ('subreddit_id', pa.string()),
        ('score', pa.int64()),
        ('created_utc', pa.int64()),
        ('body', pa.string()),
        ('id', pa.string()),
        ('link_id', pa.string()),
        ('parent_id', pa.string()),
        ('year', pa.int32()),
        ('month', pa.int32())
    ])
    table = pa.Table.from_pylist(lines, schema=schema)
    pq.write_to_dataset(table, root_path=dataset_path, partition_cols=['year', 'month'], compression='snappy', existing_data_behavior='overwrite_or_ignore')

def zst_comments_to_parquet(zst_file_path: str, parquet_folder_path: str):
    _zst_to_parquet(
        zst_file_path=zst_file_path,
        parquet_folder_path=parquet_folder_path,
        _extract_line_data=_extract_comments_line_data,
        _save_batch_to_dataset=_save_comments_batch_to_dataset
    )

def _zst_to_parquet(zst_file_path: str, parquet_folder_path: str, _extract_line_data: Callable[[str], Dict[str, Any]], _save_batch_to_dataset: Callable[[List[Dict[str, Any]]], None]):
    logger.info(f'Converting {zst_file_path} to parquet files in {parquet_folder_path}')

    file_size_zst_bytes = os.stat(zst_file_path).st_size
    max_lines_parquet_file = 1_500_000

    bad_line_count = 0
    file_lines = 0
    batch_id = 0

    lines = []
    for line, file_bytes_processed in read_lines_zst(file_name=zst_file_path):
        try:
            lines.append(_extract_line_data(line_str=line))
        except ValidationError as err:
            bad_line_count += 1

        file_lines += 1

        # Log progress
        if file_lines % 100_000 == 0:
            logger.info(f'FILE LINES: {file_lines} -- PERCENTAGE: {(file_bytes_processed / file_size_zst_bytes) * 100:.0f}% -- BAD LINES SHARE {bad_line_count / file_lines:.2f}, MEGABYTES: {file_bytes_processed / 1_000_000:.2f}')

        # Save the parquet file
        if file_lines % max_lines_parquet_file == 0:
            logger.info(f'SAVING BATCH: {batch_id}')
            _save_batch_to_dataset(lines=lines, dataset_path=parquet_folder_path)
            batch_id += 1
            del lines
            gc.collect()
            lines = []

    # Save the last parquet file
    logger.info(f'SAVING BATCH: {batch_id}')
    _save_batch_to_dataset(lines=lines, dataset_path=parquet_folder_path)
    logger.info(f'Finished converting {zst_file_path} to parquet files in {parquet_folder_path}')


if __name__ == "__main__":
    directory_path_submissions = '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/data_trial/raw_submissions'
    shutil.rmtree(os.path.join(directory_path_submissions, 'parquet'))
    for file in os.listdir(directory_path_submissions):
        if file.endswith('.zst'):
            zst_submissions_to_parquet(
                zst_file_path=os.path.join(directory_path_submissions, file),
                parquet_folder_path=os.path.join(directory_path_submissions, 'parquet')
            )

    directory_path_comments = '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/data_trial/raw_comments'
    shutil.rmtree(os.path.join(directory_path_comments, 'parquet'))
    for file in os.listdir(directory_path_comments):
        if file.endswith('.zst'):
            zst_comments_to_parquet(
                zst_file_path=os.path.join(directory_path_comments, file),
                parquet_folder_path=os.path.join(directory_path_comments, 'parquet')
            )
