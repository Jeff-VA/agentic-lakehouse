{#- Apache Pinot: the real-time tier. Overrides of macros/default.sql. -#}

{# Pinot takes the percentile on a 0-100 scale. T-digest is approximate. #}
{% macro percentile(expr, fraction) %}PERCENTILETDIGEST({{ expr }}, {{ "%g" | format(fraction * 100) }}){% endmacro %}

{# Pinot stores TIMESTAMP columns as epoch milliseconds. #}
{% macro timestamp(ts) %}{{ ts | epoch_millis }}{% endmacro %}

{% macro time_bucket(expr, grain) %}DATETRUNC('{{ grain | upper }}', {{ expr }}){% endmacro %}

{# Pinot returns only 10 rows when a query has no LIMIT. #}
{% macro limit(n) %}LIMIT {{ n or 1000000 }}{% endmacro %}
