SELECT
  date_trunc('hour', event_ts) AS "event_ts",
  client_isp AS "isp_provider",
  CASE WHEN client_device IN ('ios', 'android') THEN TRUE ELSE FALSE END AS "is_mobile_device",
  approx_percentile(CAST(request_latency_ms AS DOUBLE), 0.99) FILTER (WHERE stream_start_event = TRUE) AS "p99_ttfb",
  CAST(COUNT(*) FILTER (WHERE buffer_duration_ms > 200) AS DOUBLE) / NULLIF(COUNT(*), 0) AS "rebuffer_incidence_rate",
  COUNT(DISTINCT session_id) AS "unique_sessions",
  AVG(bitrate_kbps) AS "avg_bitrate_kbps"
FROM iceberg.raw.telemetry
WHERE event_ts >= TIMESTAMP '2026-10-01 00:00:00'
  AND event_ts < TIMESTAMP '2026-10-02 00:00:00'
  AND client_isp IN ('comcast', 'verizon')
  AND CASE WHEN client_device IN ('ios', 'android') THEN TRUE ELSE FALSE END = TRUE
GROUP BY
  date_trunc('hour', event_ts),
  client_isp,
  CASE WHEN client_device IN ('ios', 'android') THEN TRUE ELSE FALSE END
ORDER BY
  "event_ts" NULLS LAST,
  "isp_provider" NULLS LAST,
  "is_mobile_device" NULLS LAST
