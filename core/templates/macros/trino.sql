{#- Trino: the historical (Iceberg) tier. Overrides of macros/default.sql. -#}

{# Trino has no exact percentile function; this one is approximate. #}
{% macro percentile(expr, fraction) %}approx_percentile({{ expr }}, {{ fraction }}){% endmacro %}
