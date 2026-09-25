\set ON_ERROR_STOP on
\timing on
SET search_path = collect, public;

\set wend '''2026-09-25 17:05:06'''
\set w24s '''2026-09-24 17:05:06'''
\set k7 '''{server_id,database_name,runtime_stats_interval_id,plan_id,query_id,replica_role,first_execution_time}'''
\set k8 '''{server_id,database_name,runtime_stats_interval_id,plan_id,query_id,replica_role,first_execution_time,execution_type_desc}'''
\set c17 '''{server_id,database_name,query_id,plan_id,replica_role,runtime_stats_interval_id,first_execution_time,collection_time,query_plan_hash,query_hash,execution_count,avg_cpu_time_us,avg_duration_us,last_execution_time,is_forced_plan,force_failure_count,query_text}'''

CREATE OR REPLACE FUNCTION pg_temp.allcols() RETURNS text[] LANGUAGE sql AS $$
    SELECT array_agg(column_name::text ORDER BY ordinal_position)
    FROM information_schema.columns
    WHERE table_schema = 'collect' AND table_name = 'query_store_stats'
    AND column_name NOT IN ('collection_id', 'server_name', 'query_plan_text') $$;

CREATE OR REPLACE FUNCTION pg_temp.build(tbl text, cols text[], keycols text[], regular_only boolean, before_ts timestamp)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE n bigint; k text; sel text;
BEGIN
    cols := COALESCE(cols, pg_temp.allcols());
    k := (SELECT string_agg('s.' || c, ', ') FROM unnest(keycols) AS c WHERE c <> 'server_id');
    sel := (SELECT string_agg('s.' || c, ', ') FROM unnest(cols) AS c);
    EXECUTE format('DROP TABLE IF EXISTS %s', tbl);
    EXECUTE format('CREATE TABLE %s WITH (fillfactor = 50) AS SELECT DISTINCT ON (%s) %s FROM collect.query_store_stats AS s WHERE s.server_id = 1 AND s.collection_time < %L %s ORDER BY %s, s.collection_time DESC, s.execution_count DESC',
        tbl, k, sel, before_ts,
        CASE WHEN regular_only THEN 'AND s.execution_type_desc = ''Regular'' AND s.first_execution_time IS NOT NULL' ELSE '' END, k);
    GET DIAGNOSTICS n = ROW_COUNT;
    EXECUTE format('CREATE UNIQUE INDEX ON %s (%s) NULLS NOT DISTINCT', tbl, array_to_string(keycols, ', '));
    RETURN n;
END $$;

CREATE OR REPLACE FUNCTION pg_temp.apply_batches(tbl text, cols text[], keycols text[], regular_only boolean, from_ts timestamp, to_ts timestamp)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE ct timestamp; n bigint := 0; rc bigint; tot bigint := 0; k text; sel text; setl text; q text; t0 timestamptz := clock_timestamp();
BEGIN
    cols := COALESCE(cols, pg_temp.allcols());
    k := (SELECT string_agg('s.' || c, ', ') FROM unnest(keycols) AS c WHERE c <> 'server_id');
    sel := (SELECT string_agg('s.' || c, ', ') FROM unnest(cols) AS c);
    setl := (SELECT string_agg(c || ' = EXCLUDED.' || c, ', ') FROM unnest(cols) AS c WHERE c <> ALL (keycols));
    q := format('INSERT INTO %s AS t (%s) SELECT DISTINCT ON (%s) %s FROM collect.query_store_stats AS s WHERE s.server_id = 1 AND s.collection_time = $1 %s ORDER BY %s, s.execution_count DESC ON CONFLICT (%s) DO UPDATE SET %s WHERE (EXCLUDED.collection_time, EXCLUDED.execution_count) > (t.collection_time, t.execution_count)',
        tbl, array_to_string(cols, ', '), k, sel,
        CASE WHEN regular_only THEN 'AND s.execution_type_desc = ''Regular'' AND s.first_execution_time IS NOT NULL' ELSE '' END,
        k, array_to_string(keycols, ', '), setl);
    FOR ct IN SELECT DISTINCT collection_time FROM collect.query_store_stats WHERE server_id = 1 AND collection_time >= from_ts AND collection_time <= to_ts ORDER BY 1 LOOP
        EXECUTE q USING ct;
        GET DIAGNOSTICS rc = ROW_COUNT;
        n := n + 1;
        tot := tot + rc;
    END LOOP;
    RETURN format('%s batches, %s rows upserted, %s ms', n, tot, round(extract(epoch FROM clock_timestamp() - t0) * 1000));
END $$;

\qecho === BUILD v143 (baseline / shape c untouched) as of w24s
SELECT 'built v143 ' || pg_temp.build('collect.proto_v143', :c17::text[], :k7::text[], true, :w24s);
CHECKPOINT;
SELECT pg_current_wal_lsn() AS l0 \gset v143_
SELECT 'apply v143: ' || pg_temp.apply_batches('collect.proto_v143', :c17::text[], :k7::text[], true, :w24s, :wend);
SELECT 'wal v143 bytes: ' || pg_wal_lsn_diff(pg_current_wal_lsn(), :'v143_l0');
ANALYZE collect.proto_v143;

\qecho === BUILD a (wide, all outcomes) as of w24s, index (server_id, collection_time)
SELECT 'built a ' || pg_temp.build('collect.proto_a', NULL, :k8::text[], false, :w24s);
CREATE INDEX ix_a_ct ON collect.proto_a (server_id, collection_time);
CHECKPOINT;
SELECT pg_current_wal_lsn() AS l0 \gset a_
SELECT 'apply a: ' || pg_temp.apply_batches('collect.proto_a', NULL, :k8::text[], false, :w24s, :wend);
SELECT 'wal a bytes: ' || pg_wal_lsn_diff(pg_current_wal_lsn(), :'a_l0');
ANALYZE collect.proto_a;

\qecho === BUILD b (wide, all outcomes) as of w24s, index (server_id, first_execution_time)
SELECT 'built b ' || pg_temp.build('collect.proto_b', NULL, :k8::text[], false, :w24s);
CREATE INDEX ix_b_fet ON collect.proto_b (server_id, first_execution_time);
CHECKPOINT;
SELECT pg_current_wal_lsn() AS l0 \gset b_
SELECT 'apply b: ' || pg_temp.apply_batches('collect.proto_b', NULL, :k8::text[], false, :w24s, :wend);
SELECT 'wal b bytes: ' || pg_wal_lsn_diff(pg_current_wal_lsn(), :'b_l0');
ANALYZE collect.proto_b;

\qecho === BUILD c8 (wide, 8-day retention) from proto_a, no secondary index
DROP TABLE IF EXISTS collect.proto_c8;
CREATE TABLE collect.proto_c8 WITH (fillfactor = 50) AS
SELECT * FROM collect.proto_a WHERE collection_time >= (:wend::timestamp - interval '8 days');
CREATE UNIQUE INDEX ON collect.proto_c8 (server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc) NULLS NOT DISTINCT;
CHECKPOINT;
SELECT pg_current_wal_lsn() AS l0 \gset c8_
SELECT 'apply c8 (steady-state, rows pre-exist): ' || pg_temp.apply_batches('collect.proto_c8', NULL, :k8::text[], false, :w24s, :wend);
SELECT 'wal c8 bytes: ' || pg_wal_lsn_diff(pg_current_wal_lsn(), :'c8_l0');
ANALYZE collect.proto_c8;

\qecho === SIZES
SELECT 'size ' || c.relname || ': rows=' || c.reltuples::bigint || ' heap=' || pg_relation_size(c.oid) || ' total=' || pg_total_relation_size(c.oid)
FROM pg_class AS c WHERE c.relname IN ('proto_v143', 'proto_a', 'proto_b', 'proto_c8') ORDER BY c.relname;

\qecho === HOT SHARE
SELECT 'stats ' || relname || ' ins=' || n_tup_ins || ' upd=' || n_tup_upd || ' hot=' || n_tup_hot_upd
    || ' hot_pct=' || round(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 1)
FROM pg_stat_user_tables WHERE relname IN ('proto_v143', 'proto_a', 'proto_b', 'proto_c8') ORDER BY relname;

\qecho === PURGE TESTS (rolled back, do not mutate)
SELECT min(collection_time) AS mn FROM collect.proto_a \gset a_
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) DELETE FROM collect.proto_a WHERE collection_time < (:'a_mn'::timestamp + interval '1 day');
ROLLBACK;

SELECT min(collection_time) AS mn FROM collect.proto_b \gset b_
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) DELETE FROM collect.proto_b WHERE collection_time < (:'b_mn'::timestamp + interval '1 day');
ROLLBACK;

BEGIN;
EXPLAIN (ANALYZE, BUFFERS) DELETE FROM collect.proto_c8 WHERE collection_time < (:wend::timestamp - interval '7 days');
ROLLBACK;

\qecho === DONE BUILD
