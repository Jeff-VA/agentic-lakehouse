/*
description: Activity per category over the last `minutes`, busiest first. Live from the real-time tier.
params:
  minutes: {type: int, default: 60, min: 5, max: 1440, description: How far back to look.}
*/
SELECT category, event_count, total_amount, avg_amount
FROM activity
WHERE event_ts >= now() - INTERVAL :minutes MINUTE
ORDER BY event_count DESC
