{{ config(materialized='table') }}

SELECT  s.author AS author_submission,
        s.subreddit,
        s.title AS title_submission,
        s.selftext AS selftext_submission,
        s.id AS id_submission,
        s.created_utc AS created_utc_submission,
        s.score AS score_submission,
        c.id AS id_comment,
        c.author AS author_comment,
        c.body AS body_comment,
        c.parent_id AS parent_id_comment,
        c.score AS score_comment,
        c.created_utc AS created_utc_comment
FROM {{ ref('submissions_with_keywords')}} s
JOIN {{ ref('comments')}} c
ON s.id = c.link_id