{{ config(materialized='table', file_format='parquet') }}

{% set like_keywords = ['gpt', 'artificial intelligence', 'gemini', 'claude', 'llm', 'large language model', 'deepseek', 'bard'] %}
{% set full_word_keywords = ['ai', 'bot']}

WITH normalized_comments AS (
    SELECT *, LOWER(COALESCE(body, '')) AS body_lc
    FROM {{ ref('comments') }}
)
WITH filtered_comments AS (
    SELECT *
    FROM normalized_comments
    WHERE (
        {% for keyword in like_keywords %}
            {% if not loop.first %} OR {% endif %}
            body_lc LIKE '%{{ keyword }}%'
        {% endfor %}
    )
    OR (
        {% for keyword in full_word_keywords %}
            {% if not loop.first %} OR {% endif %}
            body_lc RLIKE '\\b{{ keyword }}\\b'
        {% endfor %}
    )
)
SELECT /*+ REPARTITION(32) */  *
FROM filtered_comments