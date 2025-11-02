{% macro print_rowcount(model) %}
  {% set rel = ref(model) %}
  {% set res = run_query("select count(*) as cnt from " ~ rel) %}
  {% if execute and res is not none %}
    {% set cnt = res.rows[0][0] %}
    {{ log("Rowcount for " ~ rel ~ ": " ~ cnt|string, info=True) }}
  {% endif %}
{% endmacro %}
