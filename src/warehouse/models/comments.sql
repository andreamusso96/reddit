{{ config(materialized='view') }}

SELECT  author, 
        subreddit, 
        subreddit_id, 
        score, 
        created_utc, 
        body, 
        id,
        link_id, 
        parent_id, 
        year, 
        month
FROM {{ source('raw', 'comments') }}