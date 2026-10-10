/*
description: >
  Hourly playback quality per ISP over the last `hours`, from the gold rollup:
  complete hours only, each published after passing its quality checks.
params:
  hours: {type: int, default: 48, min: 1, max: 2160, description: How far back to look.}
*/
SELECT event_ts AS hour, isp_provider, rebuffer_incidence_rate, p99_ttfb, unique_sessions, avg_bitrate_kbps
FROM gold.hourly_isp_quality
WHERE event_ts >= now() - INTERVAL :hours HOUR
ORDER BY hour, isp_provider
