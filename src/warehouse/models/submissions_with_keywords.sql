{{ config(materialized='table', file_format='parquet', location_root='file:/cluster/scratch/anmusso/reddit/dbt_external') }}

{% set keywords = ['theater', 'movie'] %}

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