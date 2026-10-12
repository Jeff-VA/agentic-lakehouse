{#-
  Standard SQL for every construct the compiler emits. An engine's own macro
  file overrides only the macros it does differently (dbt's adapter.dispatch
  pattern). Macros can call each other as adapter.<name>, which also looks in
  the engine's file first.

  Arguments are SQL already rendered for the target engine, except:
    ts        a UTC datetime
    grain     minute | hour | day | week | month
    fraction  percentile as a fraction, e.g. 0.99
    n         row limit, or none
-#}

{# Output column names. Always quoted, so a name like "date" is never read as a keyword. #}
{% macro quote(name) %}"{{ name }}"{% endmacro %}

{% macro timestamp(ts) %}TIMESTAMP '{{ ts | sql_timestamp }}'{% endmacro %}

{# Start of the time bucket containing expr. #}
{% macro time_bucket(expr, grain) %}date_trunc('{{ grain }}', {{ expr }}){% endmacro %}

{# expr is none for COUNT(*). #}
{% macro count(expr) %}COUNT({{ expr or "*" }}){% endmacro %}

{% macro count_distinct(expr) %}COUNT(DISTINCT {{ expr }}){% endmacro %}

{% macro sum(expr) %}SUM({{ expr }}){% endmacro %}

{% macro avg(expr) %}AVG({{ expr }}){% endmacro %}

{% macro min(expr) %}MIN({{ expr }}){% endmacro %}

{% macro max(expr) %}MAX({{ expr }}){% endmacro %}

{% macro percentile(expr, fraction) %}PERCENTILE_CONT({{ fraction }}) WITHIN GROUP (ORDER BY {{ expr }}){% endmacro %}

{# The aggregate over only the rows that match predicate. #}
{% macro filtered(aggregate, predicate) %}{{ aggregate }} FILTER (WHERE {{ predicate }}){% endmacro %}

{# Floating-point division; NULL instead of an error when the denominator is 0. #}
{% macro ratio(numerator, denominator) %}CAST({{ numerator }} AS DOUBLE) / NULLIF({{ denominator }}, 0){% endmacro %}

{% macro limit(n) %}{% if n %}LIMIT {{ n }}{% endif %}{% endmacro %}
