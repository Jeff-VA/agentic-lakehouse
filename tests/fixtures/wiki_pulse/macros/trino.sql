{#- Trino's own versions of wiki-pulse's macros, used instead of default.sql's on Trino. -#}

{# The site a URL is on, e.g. en.wikipedia.org, with Trino's URL function. #}
{% macro url_host(column) %}url_extract_host({{ column }}){% endmacro %}
