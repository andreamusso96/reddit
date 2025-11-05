{{ config(materialized='table', file_format='parquet') }}

WITH submission_ids AS (
    SELECT id
    FROM {{ ref('submissions_with_keywords')}}
),
comments_and_submissions AS (
    SELECT  /*+ BROADCAST(submission_ids) */
            s.id AS id_submission,
            c.id AS id_comment,
            c.author AS author_comment,
            c.body AS body_comment,
            c.parent_id AS parent_id_comment,
            c.score AS score_comment,
            c.created_utc AS created_utc_comment
    FROM submission_ids s
    JOIN {{ ref('comments')}} c
    ON s.id = c.link_id
)
SELECT /*+ REPARTITION(32) */  *
FROM comments_and_submissions