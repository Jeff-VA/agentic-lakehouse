{#-
  wiki-pulse's SQL macros: business definitions written once and used in
  ontology.yaml, views and steps. A macro here is for every engine;
  pinot.sql, trino.sql or duckdb.sql hold an engine's own version, which wins
  over this one. The comment directly above a macro, with no blank line
  between, describes it to people and agents (GET /catalog, MCP list_metrics).
-#}

{# Who made an edit, broadly: a bot, someone anonymous (an IP address or a temporary account), or a registered editor. #}
{% macro editor_type() %}CASE WHEN is_bot = true THEN 'bot' WHEN is_anonymous = true THEN 'anonymous' ELSE 'registered' END{% endmacro %}

{# An edit by a person with an account: not a bot, not anonymous. #}
{% macro human_edit() %}is_bot = false AND is_anonymous = false{% endmacro %}

{# The site a URL is on, e.g. en.wikipedia.org. #}
{% macro url_host(column) %}regexp_extract({{ column }}, '^https?://([^/]+)', 1){% endmacro %}
