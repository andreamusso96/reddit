{{ config(materialized='view') }}

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
FROM {{ source('raw', 'submissions') }}