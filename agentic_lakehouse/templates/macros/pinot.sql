{#-
  Apache Pinot: the real-time tier. Overrides of macros/default.sql.

  Written for Pinot's multi-stage query engine, which agentic_lakehouse/engines.py always
  enables. The single-stage engine rejects an output alias that matches a
  column name, and every time-bucketed query has one ("event_ts").
-#}

{# Pinot takes the percentile on a 0-100 scale. T-digest is approximate. #}
{% macro percentile(expr, fraction) %}PERCENTILETDIGEST({{ expr }}, {{ "%g" | format(fraction * 100) }}){% endmacro %}

{# Pinot stores TIMESTAMP columns as epoch milliseconds. #}
{% macro timestamp(ts) %}{{ ts | epoch_millis }}{% endmacro %}

{# DATETRUNC returns epoch milliseconds; the cast makes the result a TIMESTAMP like the other engines'. #}
{% macro time_bucket(expr, grain) %}CAST(DATETRUNC('{{ grain | upper }}', {{ expr }}) AS TIMESTAMP){% endmacro %}

{# Pinot returns only 10 rows when a query has no LIMIT. #}
{% macro limit(n) %}LIMIT {{ n or 1000000 }}{% endmacro %}
