/*
Load-test harness for #5794: inflate the Query Store interval tables of a RESTORED COPY of a Darling store.

Never run this on a store that a service uses for real monitoring. It adds synthetic servers to collect.servers.

Run with psql, connected to the copy as its owner role:
    psql -X -v ON_ERROR_STOP=1 -v n=10 -d <copy> -f inflate-query-store.sql

n (default 10) is the multiplier. Every server that has Query Store interval rows is cloned into n - 1 synthetic
servers, so the tables end with about n times their rows. Each clone keeps the source row's timestamps, so the
clones land in the same day partitions as their source rows.

The tables (every Query Store interval table the store migrations create; QueryStoreLoadTestHarnessTests pins
that this list stays complete):
    collect.query_store_interval_wide
    collect.query_store_interval_wide_coverage
    collect.query_store_interval_wide_pending
    collect.query_store_interval_latest
    collect.query_store_interval_latest_coverage
    collect.query_store_interval_latest_pending

The registry: the readers join collect.servers on server_id, so each synthetic server gets a collect.servers row.
It gets no config.config_monitored_servers row, because the service would then try to connect to it. On its next
config reload the service marks a registry row with no config row as disabled (DisableOrphanedServersSql).

Idempotent: loadtest.inflate_done records each (table, partition, copy) when its transaction commits, and every
insert also says ON CONFLICT DO NOTHING. A second run with the same n adds no rows. A later run with a larger n
adds only the new copies.

Teardown: teardown.sql, or drop the copy.
*/

\if :{?n}
\else
    \set n 10
\endif

SET client_min_messages = notice;

CREATE SCHEMA IF NOT EXISTS loadtest;

CREATE TABLE IF NOT EXISTS loadtest.server_map
(
    source_server_id integer NOT NULL,
    copy_no integer NOT NULL,
    synthetic_server_id integer NOT NULL,
    synthetic_server_name text NOT NULL,
    CONSTRAINT pk_server_map PRIMARY KEY (source_server_id, copy_no),
    CONSTRAINT uq_server_map_synthetic UNIQUE (synthetic_server_id)
);

CREATE TABLE IF NOT EXISTS loadtest.inflate_done
(
    table_name text NOT NULL,
    partition_name text NOT NULL,
    copy_no integer NOT NULL,
    rows_added bigint NOT NULL,
    done_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT pk_inflate_done PRIMARY KEY (table_name, partition_name, copy_no)
);

/* The six tables, in one place for the procedure and the report. */
CREATE OR REPLACE FUNCTION loadtest.query_store_tables()
RETURNS text[]
LANGUAGE sql
IMMUTABLE
AS $f$
    SELECT ARRAY
    [
        'collect.query_store_interval_wide',
        'collect.query_store_interval_wide_coverage',
        'collect.query_store_interval_wide_pending',
        'collect.query_store_interval_latest',
        'collect.query_store_interval_latest_coverage',
        'collect.query_store_interval_latest_pending'
    ]::text[];
$f$;

/* The leaf relations of a table: its day partitions when it is partitioned, the table itself when it is not. */
CREATE OR REPLACE FUNCTION loadtest.leaves(p_table text)
RETURNS TABLE (leaf regclass)
LANGUAGE sql
STABLE
AS $f$
    SELECT pt.relid::regclass
    FROM pg_partition_tree(p_table::regclass) AS pt
    WHERE pt.isleaf
    AND   EXISTS (SELECT 1 FROM pg_class AS c WHERE c.oid = p_table::regclass AND c.relkind = 'p')
    UNION ALL
    SELECT p_table::regclass
    WHERE EXISTS (SELECT 1 FROM pg_class AS c WHERE c.oid = p_table::regclass AND c.relkind = 'r')
    ORDER BY 1;
$f$;

/* Exact row count and total bytes for every leaf of the six tables. count(*) reads every row: on a large copy this
   takes minutes, which is part of the cost of the test, not of the product. */
CREATE OR REPLACE FUNCTION loadtest.partition_report()
RETURNS TABLE (table_name text, partition_name text, row_count bigint, total_bytes bigint)
LANGUAGE plpgsql
AS $f$
DECLARE
    t text;
    l regclass;
BEGIN
    FOREACH t IN ARRAY loadtest.query_store_tables()
    LOOP
        FOR l IN SELECT x.leaf FROM loadtest.leaves(t) AS x
        LOOP
            table_name := t;
            partition_name := l::text;
            EXECUTE format('SELECT count(*) FROM ONLY %s', l) INTO row_count;
            total_bytes := pg_total_relation_size(l);
            RETURN NEXT;
        END LOOP;
    END LOOP;
END
$f$;

CREATE OR REPLACE PROCEDURE loadtest.inflate(p_n integer)
LANGUAGE plpgsql
AS $p$
DECLARE
    src record;
    k integer;
    new_id integer;
    new_name text;
    server_columns text;
    server_values text;
    t text;
    l regclass;
    insert_columns text;
    select_columns text;
    added bigint;
BEGIN
    IF p_n IS NULL OR p_n < 1 THEN
        RAISE EXCEPTION 'n must be 1 or more, not %', p_n;
    END IF;

    /* 1. The synthetic servers. The sources are the servers with Query Store interval rows: the coverage tables hold
       one row per such server and are small, so this does not scan the large tables. */
    SELECT
        string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum),
        string_agg
        (
            CASE a.attname
                WHEN 'server_id' THEN '$1'
                WHEN 'server_name' THEN '$2'
                WHEN 'display_name' THEN 'CASE WHEN s.display_name IS NULL THEN NULL ELSE s.display_name || $3 END'
                ELSE 's.' || quote_ident(a.attname)
            END,
            ', ' ORDER BY a.attnum
        )
    INTO server_columns, server_values
    FROM pg_attribute AS a
    WHERE a.attrelid = 'collect.servers'::regclass
    AND   a.attnum > 0
    AND   NOT a.attisdropped
    AND   a.attgenerated = ''
    AND   a.attidentity = '';

    FOR src IN
        SELECT s.server_id, s.server_name
        FROM collect.servers AS s
        WHERE s.server_id IN
              (
                  SELECT c.server_id FROM collect.query_store_interval_wide_coverage AS c
                  UNION
                  SELECT c.server_id FROM collect.query_store_interval_latest_coverage AS c
              )
        AND   NOT EXISTS (SELECT 1 FROM loadtest.server_map AS m WHERE m.synthetic_server_id = s.server_id)
        ORDER BY s.server_id
    LOOP
        FOR k IN 1 .. p_n - 1
        LOOP
            CONTINUE WHEN EXISTS
            (
                SELECT 1 FROM loadtest.server_map AS m WHERE m.source_server_id = src.server_id AND m.copy_no = k
            );

            new_name := src.server_name || ' (loadtest ' || k || ')';
            new_id := hashtext('loadtest:' || src.server_id || ':' || k);
            /* Never 0 (the fleet sentinel) and never an id the registry already holds. */
            WHILE new_id = 0 OR EXISTS (SELECT 1 FROM collect.servers AS s WHERE s.server_id = new_id)
            LOOP
                new_id := new_id + 1;
            END LOOP;

            EXECUTE format
            (
                'INSERT INTO collect.servers (%s) SELECT %s FROM collect.servers AS s WHERE s.server_id = $4',
                server_columns,
                server_values
            )
            USING new_id, new_name, ' (loadtest ' || k || ')', src.server_id;

            INSERT INTO loadtest.server_map (source_server_id, copy_no, synthetic_server_id, synthetic_server_name)
            VALUES (src.server_id, k, new_id, new_name);
        END LOOP;
    END LOOP;

    COMMIT;

    /* 2. The rows, one leaf and one copy per transaction, so a long run can stop and resume. */
    FOREACH t IN ARRAY loadtest.query_store_tables()
    LOOP
        SELECT
            string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum),
            string_agg
            (
                CASE a.attname WHEN 'server_id' THEN 'm.synthetic_server_id' ELSE 'r.' || quote_ident(a.attname) END,
                ', ' ORDER BY a.attnum
            )
        INTO insert_columns, select_columns
        FROM pg_attribute AS a
        WHERE a.attrelid = t::regclass
        AND   a.attnum > 0
        AND   NOT a.attisdropped
        AND   a.attgenerated = ''
        AND   a.attidentity = '';

        FOR l IN SELECT x.leaf FROM loadtest.leaves(t) AS x
        LOOP
            FOR k IN 1 .. p_n - 1
            LOOP
                CONTINUE WHEN EXISTS
                (
                    SELECT 1
                    FROM loadtest.inflate_done AS d
                    WHERE d.table_name = t
                    AND   d.partition_name = l::text
                    AND   d.copy_no = k
                );

                /* Insert through the parent, so each clone is routed to the partition of its own timestamp. */
                EXECUTE format
                (
                    'INSERT INTO %s (%s) SELECT %s FROM ONLY %s AS r '
                    || 'JOIN loadtest.server_map AS m ON m.source_server_id = r.server_id AND m.copy_no = $1 '
                    || 'ON CONFLICT DO NOTHING',
                    t,
                    insert_columns,
                    select_columns,
                    l
                )
                USING k;
                GET DIAGNOSTICS added = ROW_COUNT;

                INSERT INTO loadtest.inflate_done (table_name, partition_name, copy_no, rows_added)
                VALUES (t, l::text, k, added);

                RAISE NOTICE '% copy %: % rows', l, k, added;
                COMMIT;
            END LOOP;
        END LOOP;
    END LOOP;
END
$p$;

\echo 'Before:'
DROP TABLE IF EXISTS pg_temp.inflate_before;
CREATE TEMP TABLE inflate_before AS SELECT * FROM loadtest.partition_report();

CALL loadtest.inflate(:n);

DROP TABLE IF EXISTS pg_temp.inflate_after;
CREATE TEMP TABLE inflate_after AS SELECT * FROM loadtest.partition_report();

\echo ''
\echo 'Rows and bytes per partition, before and after:'
SELECT
    coalesce(a.table_name, b.table_name) AS table_name,
    coalesce(a.partition_name, b.partition_name) AS partition_name,
    coalesce(b.row_count, 0) AS rows_before,
    coalesce(a.row_count, 0) AS rows_after,
    pg_size_pretty(coalesce(b.total_bytes, 0)) AS bytes_before,
    pg_size_pretty(coalesce(a.total_bytes, 0)) AS bytes_after
FROM inflate_after AS a
FULL JOIN inflate_before AS b
  ON  b.table_name = a.table_name
  AND b.partition_name = a.partition_name
ORDER BY 1, 2;

\echo ''
\echo 'Totals per table:'
SELECT
    a.table_name,
    sum(coalesce(b.row_count, 0)) AS rows_before,
    sum(a.row_count) AS rows_after,
    pg_size_pretty(sum(coalesce(b.total_bytes, 0))::bigint) AS bytes_before,
    pg_size_pretty(sum(a.total_bytes)::bigint) AS bytes_after
FROM inflate_after AS a
LEFT JOIN inflate_before AS b
  ON  b.table_name = a.table_name
  AND b.partition_name = a.partition_name
GROUP BY a.table_name
ORDER BY a.table_name;

\echo ''
\echo 'Synthetic servers:'
SELECT count(*) AS synthetic_servers, count(DISTINCT source_server_id) AS source_servers, max(copy_no) + 1 AS n
FROM loadtest.server_map;
