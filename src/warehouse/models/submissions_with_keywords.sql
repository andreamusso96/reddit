{{ config(materialized='table', file_format='parquet') }}

{% set keywords = ['plantar fasciitis', 'plantar fascitis', 'heel pain', 'plantar fasciosis'] %}

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