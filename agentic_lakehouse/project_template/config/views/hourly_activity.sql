/*
description: Hourly activity per category over the last `hours`, from the gold rollup (complete hours that passed their checks).
params:
  hours: {type: int, default: 24, min: 1, max: 720, description: How far back to look.}
*/
SELECT event_ts AS hour, category, event_count, total_amount, avg_amount
FROM gold.hourly_activity
WHERE event_ts >= now() - INTERVAL :hours HOUR
ORDER BY hour, category
