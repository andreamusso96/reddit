{{ config(materialized='table') }}

SELECT *
FROM {{ ref('submissions') }}
WHERE LOWER(COALESCE(title, '')) LIKE '%internet%'
OR LOWER(COALESCE(selftext, '')) LIKE '%internet%'
OR LOWER(COALESCE(title, '')) LIKE '%computer%'
OR LOWER(COALESCE(selftext, '')) LIKE '%computer%'