SELECT
  CAST(DATETRUNC('HOUR', "event_ts") AS TIMESTAMP) AS "event_ts",
  "client_isp" AS "isp_provider",
  CASE WHEN "client_device" IN ('ios', 'android') THEN TRUE ELSE FALSE END AS "is_mobile_device",
  PERCENTILETDIGEST("request_latency_ms", 99) FILTER (WHERE "stream_start_event" = TRUE) AS "p99_ttfb",
  CAST(COUNT(*) FILTER (WHERE "buffer_duration_ms" > 200) AS DOUBLE) / NULLIF(COUNT(*), 0) AS "rebuffer_incidence_rate",
  COUNT(DISTINCT "session_id") AS "unique_sessions",
  AVG("bitrate_kbps") AS "avg_bitrate_kbps"
FROM telemetry
WHERE "event_ts" >= 1790812800000
  AND "event_ts" < 1790899200000
  AND "client_isp" IN ('comcast', 'verizon')
  AND CASE WHEN "client_device" IN ('ios', 'android') THEN TRUE ELSE FALSE END = TRUE
GROUP BY
  CAST(DATETRUNC('HOUR', "event_ts") AS TIMESTAMP),
  "client_isp",
  CASE WHEN "client_device" IN ('ios', 'android') THEN TRUE ELSE FALSE END
ORDER BY
  "event_ts",
  "isp_provider",
  "is_mobile_device"
LIMIT 1000000
