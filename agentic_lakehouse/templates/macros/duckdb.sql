{#- DuckDB: the embedded engine. Overrides of macros/default.sql. -#}

{# Exact percentile, interpolated between the two nearest values. #}
{% macro percentile(expr, fraction) %}quantile_cont({{ expr }}, {{ fraction }}){% endmacro %}

{% macro quantile_grid(expr, fractions) %}quantile_cont({{ expr }}, [{{ fractions | join(', ') }}]){% endmacro %}
