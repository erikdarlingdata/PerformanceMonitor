/*
#5794 load test, step (d): the compression candidate (section 3, row 2 of the issue). Copies one closed day of
collect.query_store_interval_wide into a TimescaleDB hypertable in the loadtest schema, compresses it in two shapes,
and prints the size of each form and the time of the same one-day scan on each. The scan is the shape of a one-day
Custom Views panel (#5582): every server's rows for the day, grouped by query_hash. Each scan runs twice; the second
run reads from a warm cache.

The two shapes (#5794 review round 3), measured one after the other on the same copied rows:
- store shape: compress_segmentby = 'server_id' and no compress_orderby, so TimescaleDB orders by the time column
  descending. This is what the store sets on every collector hypertable except collection_log and perfmon_stats
  (TimescaleSupport.EnableCompressionSql and CompressionSegmentByColumn).
- candidate: compress_segmentby = 'server_id, database_name' and compress_orderby = 'first_execution_time'. The store
  uses this shape nowhere. It is here because the wide table's reads filter on database_name.

Like with like (#5794 review round 3): the copy carries the parent's indexes (LIKE ... INCLUDING INDEXES) and the day
partition's fillfactor, and every row reports its data bytes (heap and TOAST) and its index bytes apart, then their
sum. The plain row is sized with pg_table_size and pg_indexes_size on the partition, the hypertable rows with
hypertable_detailed_size, which counts the chunks and, once they are compressed, the compressed chunks.

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
CREATE TABLE loadtest.qs_wide_day_compressed
(LIKE collect.query_store_interval_wide INCLUDING DEFAULTS INCLUDING INDEXES);

DROP TABLE IF EXISTS pg_temp.compression_result;
CREATE TEMP TABLE compression_result
(
    sort_order integer,
    form text,
    relation text,
    row_count bigint,
    data_bytes bigint,
    index_bytes bigint,
    scan_1_ms numeric,
    scan_2_ms numeric
);

DO $$
DECLARE
    d date := current_setting('loadtest.day')::date;
    copy_table constant text := 'loadtest.qs_wide_day_compressed';
    leaf text;
    leaf_options text[];
    n bigint;
    t0 timestamptz;
    ms1 numeric;
    ms2 numeric;
    sz record;
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

    /* The copy gets the partition's storage options (fillfactor 50, QueryStoreIntervalPartitions.CreateDaySql)
       before it becomes a hypertable, so its chunk is made with them. */
    SELECT c.reloptions INTO leaf_options FROM pg_class AS c WHERE c.oid = leaf::regclass;
    IF leaf_options IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s SET (%s)', copy_table, array_to_string(leaf_options, ', '));
    END IF;
    PERFORM create_hypertable(copy_table::regclass, 'first_execution_time', chunk_time_interval => interval '1 day');
    EXECUTE format('INSERT INTO %s SELECT * FROM collect.query_store_interval_wide AS w '
                   || 'WHERE w.first_execution_time >= $1 AND w.first_execution_time < $1 + 1', copy_table) USING d;
    EXECUTE format('ANALYZE %s', copy_table);

    /* 1. The plain day partition, as the store holds it now. */
    EXECUTE format('SELECT count(*) FROM ONLY %s WHERE first_execution_time >= $1 AND first_execution_time < $1 + 1', leaf)
    INTO n USING d;
    t0 := clock_timestamp();
    EXECUTE format(scan, 'collect.query_store_interval_wide') USING d;
    ms1 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    t0 := clock_timestamp();
    EXECUTE format(scan, 'collect.query_store_interval_wide') USING d;
    ms2 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    INSERT INTO compression_result
    VALUES (1, 'plain day partition', leaf, n, pg_table_size(leaf::regclass), pg_indexes_size(leaf::regclass), ms1, ms2);

    /* 2. The same rows in the hypertable, before compression. */
    t0 := clock_timestamp();
    EXECUTE format(scan, copy_table) USING d;
    ms1 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    t0 := clock_timestamp();
    EXECUTE format(scan, copy_table) USING d;
    ms2 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    EXECUTE format('SELECT count(*) FROM %s', copy_table) INTO n;
    SELECT * INTO sz FROM hypertable_detailed_size(copy_table::regclass);
    INSERT INTO compression_result
    VALUES (2, 'hypertable, uncompressed', copy_table, n, sz.table_bytes + sz.toast_bytes, sz.index_bytes, ms1, ms2);

    /* 3. The store's shape: segment by server_id, no orderby (TimescaleSupport.EnableCompressionSql). */
    EXECUTE format('ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = %L)',
                   copy_table, 'server_id');
    PERFORM compress_chunk(c) FROM show_chunks(copy_table::regclass) AS c;
    t0 := clock_timestamp();
    EXECUTE format(scan, copy_table) USING d;
    ms1 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    t0 := clock_timestamp();
    EXECUTE format(scan, copy_table) USING d;
    ms2 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    EXECUTE format('SELECT count(*) FROM %s', copy_table) INTO n;
    SELECT * INTO sz FROM hypertable_detailed_size(copy_table::regclass);
    INSERT INTO compression_result
    VALUES (3, 'compressed, store shape: segmentby server_id, default orderby (first_execution_time DESC)',
            copy_table, n, sz.table_bytes + sz.toast_bytes, sz.index_bytes, ms1, ms2);

    /* 4. The candidate: the same chunk, decompressed and compressed again with the other settings. Older TimescaleDB
       versions refuse a settings change while a chunk is compressed, and newer ones apply it only to the next
       compression, so the chunk is decompressed first on both. */
    PERFORM decompress_chunk(c) FROM show_chunks(copy_table::regclass) AS c;
    EXECUTE format('ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = %L, '
                   || 'timescaledb.compress_orderby = %L)',
                   copy_table, 'server_id, database_name', 'first_execution_time');
    PERFORM compress_chunk(c) FROM show_chunks(copy_table::regclass) AS c;
    t0 := clock_timestamp();
    EXECUTE format(scan, copy_table) USING d;
    ms1 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    t0 := clock_timestamp();
    EXECUTE format(scan, copy_table) USING d;
    ms2 := round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1);
    EXECUTE format('SELECT count(*) FROM %s', copy_table) INTO n;
    SELECT * INTO sz FROM hypertable_detailed_size(copy_table::regclass);
    INSERT INTO compression_result
    VALUES (4, 'compressed, candidate: segmentby server_id, database_name; orderby first_execution_time',
            copy_table, n, sz.table_bytes + sz.toast_bytes, sz.index_bytes, ms1, ms2);
END
$$;

/* Every size counts indexes on both sides: the data and index columns are apart, and the total is their sum. */
SELECT
    form,
    relation,
    row_count,
    pg_size_pretty(data_bytes) AS "data (heap + TOAST)",
    pg_size_pretty(index_bytes) AS "indexes",
    pg_size_pretty(data_bytes + index_bytes) AS "total (data + indexes)",
    data_bytes + index_bytes AS total_bytes_with_indexes,
    scan_1_ms,
    scan_2_ms
FROM compression_result
ORDER BY sort_order;
