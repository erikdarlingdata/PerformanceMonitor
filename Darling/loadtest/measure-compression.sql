/*
#5794 load test, step (d): the compression candidate (section 3, row 2 of the issue). Copies one closed day of
collect.query_store_interval_wide into a TimescaleDB hypertable in the loadtest schema, compresses it, and prints
the size of each form and the time of the same one-day scan on each. The scan is the shape of a one-day Custom
Views panel (#5582): every server's rows for the day, grouped by query_hash. Each scan runs twice; the second run
reads from a warm cache.

:day is the UTC day to copy (default: yesterday). Needs TimescaleDB in the store. The copy is dropped and made again
on each run, so a second run measures again. teardown.sql drops it.
*/
\if :{?day}
\else
    SELECT ((now() AT TIME ZONE 'UTC')::date - 1)::text AS day \gset
\endif
SELECT set_config('loadtest.day', :'day', false) \gset

CREATE SCHEMA IF NOT EXISTS loadtest;
DROP TABLE IF EXISTS loadtest.qs_wide_day_compressed;
CREATE TABLE loadtest.qs_wide_day_compressed (LIKE collect.query_store_interval_wide);
SELECT create_hypertable
(
    'loadtest.qs_wide_day_compressed',
    'first_execution_time',
    chunk_time_interval => interval '1 day'
) \gset
INSERT INTO loadtest.qs_wide_day_compressed
SELECT *
FROM collect.query_store_interval_wide AS w
WHERE w.first_execution_time >= :'day'::date
AND   w.first_execution_time < :'day'::date + 1;
ANALYZE loadtest.qs_wide_day_compressed;

DROP TABLE IF EXISTS pg_temp.compression_result;
CREATE TEMP TABLE compression_result
(
    form text,
    relation text,
    row_count bigint,
    total_bytes bigint,
    scan_1_ms numeric,
    scan_2_ms numeric
);

DO $$
DECLARE
    d date := current_setting('loadtest.day')::date;
    leaf text;
    n bigint;
    t0 timestamptz;
    ms1 numeric;
    ms2 numeric;
    scan text := 'SELECT count(*) FROM (SELECT w.query_hash, sum(w.execution_count), '
                 || 'sum(w.avg_duration_us * w.execution_count), sum(w.avg_cpu_time_us * w.execution_count) '
                 || 'FROM %s AS w WHERE w.first_execution_time >= $1 AND w.first_execution_time < $1 + 1 '
                 || 'GROUP BY w.query_hash) AS x';
BEGIN
    /* The partition that holds the day. Before the background step splits the table into day partitions it is one
       legacy partition, and its size is then the whole table, not one day. */
    SELECT w.tableoid::regclass::text
    INTO leaf
    FROM collect.query_store_interval_wide AS w
    WHERE w.first_execution_time >= d
    AND   w.first_execution_time < d + 1
    LIMIT 1;

    IF leaf IS NULL THEN
        RAISE EXCEPTION 'collect.query_store_interval_wide holds no rows for %', d;
    END IF;

    /* The plain day partition, as the store holds it now. */
    EXECUTE format('SELECT count(*) FROM ONLY %s WHERE first_execution_time >= $1 AND first_execution_time < $1 + 1', leaf)
    INTO n USING d;
    t0 := clock_timestamp();
    EXECUTE format(scan, 'collect.query_store_interval_wide') USING d;
    ms1 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    t0 := clock_timestamp();
    EXECUTE format(scan, 'collect.query_store_interval_wide') USING d;
    ms2 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    INSERT INTO compression_result VALUES ('plain day partition', leaf, n, pg_total_relation_size(leaf::regclass), ms1, ms2);

    /* The same rows in a hypertable, before compression. */
    INSERT INTO compression_result
    SELECT 'hypertable, uncompressed', 'loadtest.qs_wide_day_compressed', count(*),
           hypertable_size('loadtest.qs_wide_day_compressed'), NULL, NULL
    FROM loadtest.qs_wide_day_compressed;

    /* Compressed: one segment per server and database, ordered by time, the shape of the store's own policies. */
    ALTER TABLE loadtest.qs_wide_day_compressed SET
    (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'server_id, database_name',
        timescaledb.compress_orderby = 'first_execution_time'
    );
    PERFORM compress_chunk(c) FROM show_chunks('loadtest.qs_wide_day_compressed') AS c;

    t0 := clock_timestamp();
    EXECUTE format(scan, 'loadtest.qs_wide_day_compressed') USING d;
    ms1 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    t0 := clock_timestamp();
    EXECUTE format(scan, 'loadtest.qs_wide_day_compressed') USING d;
    ms2 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    SELECT count(*) INTO n FROM loadtest.qs_wide_day_compressed;
    INSERT INTO compression_result VALUES ('hypertable, compressed', 'loadtest.qs_wide_day_compressed', n,
        hypertable_size('loadtest.qs_wide_day_compressed'), ms1, ms2);
END
$$;

SELECT
    form,
    relation,
    row_count,
    pg_size_pretty(total_bytes) AS size,
    total_bytes,
    scan_1_ms,
    scan_2_ms
FROM compression_result
ORDER BY CASE form WHEN 'plain day partition' THEN 1 WHEN 'hypertable, uncompressed' THEN 2 ELSE 3 END;
