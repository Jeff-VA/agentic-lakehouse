{#-
  One semantic query against one engine. Everything engine-specific comes from
  `adapter` macros (macros/<engine>.sql, falling back to macros/default.sql), so
  this skeleton is shared by every engine.
-#}
SELECT
{% for column in columns %}
  {{ column.sql }} AS {{ adapter.quote(column.name) }}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ table }}
WHERE {{ time_expr }} >= {{ adapter.timestamp(start) }}
  AND {{ time_expr }} < {{ adapter.timestamp(end) }}
{% for predicate in filters %}
  AND {{ predicate }}
{% endfor %}
{% if group_by %}
GROUP BY
{% for expr in group_by %}
  {{ expr }}{{ "," if not loop.last else "" }}
{% endfor %}
{% endif %}
{% if order_by %}
{#- NULLS LAST on every engine: left to itself, Pinot sorts NULLs first when descending. -#}
ORDER BY
{% for key in order_by %}
  {{ adapter.quote(key.field) }}{{ " DESC" if key.descending else "" }} NULLS LAST{{ "," if not loop.last else "" }}
{% endfor %}
{% endif %}
{{ adapter.limit(limit) }}
