{{ config(materialized='table', file_format='parquet') }}

{% set keywords = ['pro-life', 'pro-choice', 'reproductive rights', 'pregnancy termination'] %}

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