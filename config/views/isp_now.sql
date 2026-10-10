/*
description: >
  Playback quality per ISP over the last hour (or `minutes`), worst rebuffer
  rate first. Answered from the real-time tier: what viewers get right now.
params:
  minutes: {type: int, default: 60, min: 5, max: 1440, description: How far back to look.}
*/
SELECT isp_provider, rebuffer_incidence_rate, p99_ttfb, unique_sessions
FROM playback_telemetry
WHERE event_ts >= now() - INTERVAL :minutes MINUTE
ORDER BY rebuffer_incidence_rate DESC
