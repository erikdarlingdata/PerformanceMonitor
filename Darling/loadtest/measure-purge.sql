/*
#5794 load test, step (b): the daily retention purge. One row per purge run in the last 3 days of the collection log,
newest first. The purge drops the expired Query Store day partitions and deletes the expired rows of every other
table, so its time grows with the rows that age out each day. It runs once a day, so the newest run may be from before
the inflate.
*/
SELECT
    l.collection_time,
    l.status,
    l.duration_ms,
    l.rows_collected,
    l.error_message
FROM collection_log AS l
WHERE l.collector_name = 'data_retention'
AND   l.collection_time >= (now() AT TIME ZONE 'UTC') - interval '3 days'
ORDER BY l.collection_time DESC;
