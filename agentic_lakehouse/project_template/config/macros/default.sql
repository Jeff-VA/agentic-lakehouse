{#-
  This project's SQL macros: business definitions named once, and SQL that
  differs by engine. The ontology's SQL (a dimension's or metric's `sql`, a
  metric's `filters`), views and steps call them by name, for example
  filters: ["{{ large_event() }}"]. The comment directly above a macro, with
  no blank line between, describes it to people and agents.

  default.sql holds macros for every engine; pinot.sql, trino.sql and
  duckdb.sql, all optional, hold an engine's own versions. For each engine, a
  macro is looked up in this folder's <engine>.sql, the engine's built-in
  <engine>.sql, this folder's default.sql, then the built-in default.sql
  (agentic_lakehouse/templates/macros/). So an engine's own version beats a
  generic one, and at the same level the project's beats the built-in. That
  is how a project overrides a built-in such as `percentile`; the engine's
  README lists what each built-in must keep true.
-#}

{# A large event: an amount of 500 or more. #}
{% macro large_event() %}amount >= 500{% endmacro %}
