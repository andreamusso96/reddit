{{ config(materialized='table', file_format='parquet') }}

{% set keywords = ['same-sex marriage', 'marriage equality', 'LGBT', 'gender identity', 'sexual orientation'] %}

WITH filtered_submissions AS (
    SELECT *
    FROM {{ ref('submissions') }}
    WHERE
    {% for keyword in keywords %}
        {% if not loop.first %}
        OR
        {% endif %}
        LOWER(COALESCE(title, '')) LIKE '%{{ keyword }}%'
        OR LOWER(COALESCE(selftext, '')) LIKE '%{{ keyword }}%'
    {% endfor %}
)
SELECT /*+ REPARTITION(8) */  *
FROM filtered_submissions