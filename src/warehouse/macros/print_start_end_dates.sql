{% macro print_rowcount(model) %}
  {# Resolve to the built relation (schema.table) #}
  {% set rel = ref(model) %}
  {% set res = run_query("select MAX(created_utc) as max_created_utc, MIN(created_utc) as min_created_utc from " ~ rel) %}
  {% if execute and res is not none %}
    {% set max_created_utc = res.rows[0][0] %}
    {% set min_created_utc = res.rows[0][1] %}
    {{ log("Max created UTC for " ~ rel ~ ": " ~ max_created_utc|string, info=True) }}
    {{ log("Min created UTC for " ~ rel ~ ": " ~ min_created_utc|string, info=True) }}
  {% endif %}
{% endmacro %}