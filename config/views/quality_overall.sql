/*
description: >
  Playback quality across all ISPs in a window measured back from now, such as
  the last hour (the defaults) or the hour before it (60 and 120): headline
  numbers, and what to compare them with.
params:
  from_minutes_ago: {type: int, default: 60, min: 1, max: 10080, description: Where the window starts.}
  to_minutes_ago: {type: int, default: 0, min: 0, max: 10080, description: Where it ends; 0 is now.}
*/
SELECT rebuffer_incidence_rate, p99_ttfb, unique_sessions
FROM playback_telemetry
WHERE event_ts >= now() - INTERVAL :from_minutes_ago MINUTE
  AND event_ts < now() - INTERVAL :to_minutes_ago MINUTE
