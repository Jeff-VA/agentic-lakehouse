{#- Trino: the historical (Iceberg) tier. Overrides of macros/default.sql. -#}

{# Trino has no exact percentile function; this one is approximate. It returns
   the input's type, so the cast keeps an integer column's p99 from being truncated. #}
{% macro percentile(expr, fraction) %}approx_percentile(CAST({{ expr }} AS DOUBLE), {{ fraction }}){% endmacro %}
