{{ config(materialized='table') }}

SELECT *
FROM {{ ref('submissions') }}
WHERE LOWER(COALESCE(title, '')) LIKE '%theater%'
OR LOWER(COALESCE(selftext, '')) LIKE '%theater%'