/*
description: >
  Totals across all categories in a window measured back from now: the last
  hour by default, or the hour before it (60 and 120) to compare with.
params:
  from_minutes_ago: {type: int, default: 60, min: 1, max: 10080, description: Where the window starts.}
  to_minutes_ago: {type: int, default: 0, min: 0, max: 10080, description: Where it ends; 0 is now.}
*/
SELECT event_count, total_amount, avg_amount
FROM activity
WHERE event_ts >= now() - INTERVAL :from_minutes_ago MINUTE
  AND event_ts < now() - INTERVAL :to_minutes_ago MINUTE
