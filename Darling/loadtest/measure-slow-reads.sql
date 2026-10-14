/*
#5794 load test, step (a), part 1: every read since :since (UTC) that took 5 s or more, or ended in a timeout or an
error. The service writes these rows within seconds, so this step does not wait for the hour to close.
*/
SELECT
    s.read_time,
    s.surface,
    s.route,
    s.outcome,
    s.total_ms,
    s.row_count,
    s.in_flight,
    s.wait_ms,
    s.window_end - s.window_start AS window_width
FROM collect.slow_reads AS s
WHERE s.read_time >= :'since'::timestamp
ORDER BY s.read_time;
