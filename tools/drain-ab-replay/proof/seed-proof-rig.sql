-- Seeds a SMALL, freshly migrated Darling store so Invoke-DrainAbReplay.ps1 has something to measure.
-- For the throwaway rig only (this file inserts directly; the replay tool itself never does that).
--
--   * 3 servers, all disabled (the replay tool's safety check passes without -IKnowThisIsACopy).
--   * About 25 hours of recent collector rows in 7 tables, so the tool can measure a per-table rate.
--   * A backlog of old rows (12 days) in 3 tables for Start-FakeRetention.ps1 to delete.
--
-- Usage: psql -d <db> -f seed-proof-rig.sql

INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (1, 'replay-a', false), (2, 'replay-b', false), (3, 'replay-c', false)
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION pg_temp.seed_value(col text, typ text, ts timestamp, sid int, g bigint) RETURNS text
LANGUAGE sql AS $$
    SELECT CASE
        WHEN col = 'collection_time' THEN format('%L::timestamp', ts)
        WHEN col = 'server_id' THEN sid::text
        WHEN col = 'server_name' THEN format('%L', 'replay-' || chr(96 + sid))
        WHEN col IN ('collection_id', 'log_id') THEN format('((extract(epoch FROM clock_timestamp()) * 1000000)::bigint * 4096 + gs * 7 + %s)', sid)
        WHEN typ LIKE 'timestamp%' THEN format('%L::timestamp', ts - interval '5 minutes')
        WHEN typ = 'boolean' THEN 'false'
        WHEN typ = 'smallint' THEN '3'
        WHEN typ = 'integer' THEN (g % 1000)::text
        WHEN typ = 'bigint' THEN (g * 13 % 100000)::text
        WHEN typ IN ('double precision', 'real') THEN (g % 500)::text
        WHEN typ LIKE 'numeric(%' THEN '1.5'
        WHEN typ = 'numeric' THEN '1.5'
        WHEN typ IN ('text') OR typ LIKE 'character%' THEN format('%L', 'seed-' || (g % 50))
        WHEN typ = 'date' THEN 'current_date'
        WHEN typ = 'bytea' THEN 'NULL'
        ELSE 'NULL'
    END
$$;

DO $$
DECLARE
    spec record;
    cols text;
    vals text;
    c record;
    sid int;
    h int;
    g int;
    ts timestamp;
BEGIN
    -- (table, rows per collection, collections per server per hour, backlog rows)
    FOR spec IN
        SELECT * FROM (VALUES
            ('wait_stats', 40, 2, 20000),
            ('perfmon_stats', 60, 2, 20000),
            ('file_io_stats', 20, 2, 20000),
            ('query_stats', 15, 2, 0),
            ('query_store_stats', 12, 1, 0),
            ('cpu_utilization_stats', 1, 12, 0),
            ('collection_log', 1, 12, 0)
        ) AS v(tbl, per_batch, per_hour, backlog)
    LOOP
        FOR sid IN 1..3 LOOP
            FOR h IN 0..24 LOOP
                FOR g IN 1..spec.per_hour LOOP
                    ts := (now() AT TIME ZONE 'UTC') - make_interval(hours => h, mins => (g * 7) % 50);
                    cols := ''; vals := '';
                    FOR c IN
                        SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS typ
                        FROM pg_attribute a
                        WHERE a.attrelid = ('collect.' || spec.tbl)::regclass AND a.attnum > 0 AND NOT a.attisdropped
                        AND NOT (a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> '')
                        ORDER BY a.attnum
                    LOOP
                        cols := cols || CASE WHEN cols = '' THEN '' ELSE ', ' END || quote_ident(c.name);
                        vals := vals || CASE WHEN vals = '' THEN '' ELSE ', ' END || pg_temp.seed_value(c.name, c.typ, ts, sid, 0) ;
                    END LOOP;
                    EXECUTE format('INSERT INTO collect.%I (%s) SELECT %s FROM generate_series(1, %s) AS gs',
                                   spec.tbl, cols, vals, spec.per_batch);
                END LOOP;
            END LOOP;
        END LOOP;

        IF spec.backlog > 0 THEN
            cols := ''; vals := '';
            FOR c IN
                SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS typ
                FROM pg_attribute a
                WHERE a.attrelid = ('collect.' || spec.tbl)::regclass AND a.attnum > 0 AND NOT a.attisdropped
                AND NOT (a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> '')
                ORDER BY a.attnum
            LOOP
                cols := cols || CASE WHEN cols = '' THEN '' ELSE ', ' END || quote_ident(c.name);
                IF c.name = 'collection_time' THEN
                    vals := vals || CASE WHEN vals = '' THEN '' ELSE ', ' END
                        || '(now() AT TIME ZONE ''UTC'') - interval ''12 days'' - (gs % 600) * interval ''1 minute''';
                ELSIF c.name IN ('collection_id', 'log_id') THEN
                    vals := vals || CASE WHEN vals = '' THEN '' ELSE ', ' END || '(900000000000 + gs)::bigint * 1000 + ' || length(spec.tbl);
                ELSIF c.name = 'server_id' THEN
                    vals := vals || CASE WHEN vals = '' THEN '' ELSE ', ' END || '(gs % 3 + 1)';
                ELSE
                    vals := vals || CASE WHEN vals = '' THEN '' ELSE ', ' END || pg_temp.seed_value(c.name, c.typ, now() AT TIME ZONE 'UTC' - interval '12 days', 1, 0);
                END IF;
            END LOOP;
            EXECUTE format('INSERT INTO collect.%I (%s) SELECT %s FROM generate_series(1, %s) AS gs',
                           spec.tbl, cols, vals, spec.backlog);
        END IF;
    END LOOP;
END
$$;

ANALYZE;
