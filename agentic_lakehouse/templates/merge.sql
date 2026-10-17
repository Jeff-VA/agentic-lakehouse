{#-
  Combines rows that several tiers returned for one query (DuckDB only).
  `relation` is their union; with group_by set, partial aggregates are merged
  per group, otherwise the rows are already final and only get ordered.
-#}
SELECT
{% for column in columns %}
  {{ column.sql }} AS {{ adapter.quote(column.name) }}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ relation }}
{% if group_by %}
GROUP BY {{ group_by | join(", ") }}
{% endif %}
{% if order_by %}
ORDER BY
{% for key in order_by %}
  {{ adapter.quote(key.field) }}{{ " DESC" if key.descending else "" }} NULLS LAST{{ "," if not loop.last else "" }}
{% endfor %}
{% endif %}
{{ adapter.limit(limit) }}
