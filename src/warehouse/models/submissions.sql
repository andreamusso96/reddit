{{ config(materialized='table', file_format='parquet') }}

SELECT  author, 
        subreddit, 
        score, 
        created_utc, 
        title, 
        id, 
        num_comments, 
        selftext, 
        media, 
        year, 
        month
FROM parquet.`{{ env_var("RAW_SUBMISSIONS_PARQUET_DIR") }}`