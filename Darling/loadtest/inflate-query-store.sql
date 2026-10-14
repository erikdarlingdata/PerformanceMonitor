/*
Load-test harness for #5794: inflate the Query Store interval tables of a RESTORED COPY of a Darling store.

Never run this on a store that a service uses for real monitoring. It adds synthetic servers to both server registries.

Run with psql, connected to the copy as its owner role, while the copy's service is stopped:
    psql -X -v ON_ERROR_STOP=1 -v n=10 -d <copy> -f inflate-query-store.sql

n (default 10) is the multiplier. Every server that has Query Store interval rows is cloned into n - 1 synthetic
servers, so the tables end with about n times their rows. Each clone keeps the source row's timestamps, so the
clones land in the same day partitions as their source rows. host (default loadtest.invalid) is the connection target
of every synthetic server: a name under the reserved .invalid domain never resolves, so the service's connection to it
fails at once.

The tables (every Query Store interval table the store migrations create; QueryStoreLoadTestHarnessTests pins
that this list stays complete):
    collect.query_store_interval_wide
    collect.query_store_interval_wide_coverage
    collect.query_store_interval_wide_pending
    collect.query_store_interval_latest
    collect.query_store_interval_latest_coverage
    collect.query_store_interval_latest_pending

The registries: each synthetic server gets an ENABLED row in collect.servers AND in config.config_monitored_servers,
with the host above. The MCP resolver and the three hourly Query Store builders read only enabled servers, and the
builders' cleanup deletes the built rows of a server that is not enabled. On its config reload the service disables a
collect.servers row with no config row (DisableOrphanedServersSql), so a synthetic server with no config row would drop
out of every read and every build (#5794 review round 1).

The row triggers: the late-row triggers on the wide and latest parents fire once per inserted row, and the wide one
keeps a list of the (server, hour) pairs it has marked in one transaction-local string that grows with every pair, so a
clone of a multi-day partition would run for hours. The script saves the state of each user trigger of the two parents
and their partitions in loadtest.trigger_state, then disables them, in one transaction. At the end, also when the
inflate fails, loadtest.restore_triggers() sets each trigger back to its saved state by name and empties the table, in
one transaction. If psql is stopped before the end (Ctrl-C, or a killed session), run the recovery statement before you
start the service:
    CALL loadtest.restore_triggers();
A run that starts while saved states remain keeps them: they are the states from before the first run.

The script then does in one statement what the wide table's trigger would have done: it marks every (synthetic
server, hour) that the compose rollup has already built as late, so the rollup rebuilds those hours with the clones in
them. The mark is owed from the first commit that adds wide rows (loadtest.compose_mark_owed holds the flag) until the
mark commits, so a run that stops after its last wide clone and runs again still marks (#5794 review round 2). The
plan regression builder needs no mark: it builds every (server, day) that has no built row, and the synthetic servers
have none.

Idempotent: loadtest.inflate_done records each (table, partition, copy) when its transaction commits, and every
insert also says ON CONFLICT DO NOTHING. A second run with the same n adds no rows. A later run with a larger n
adds only the new copies. Each copy reads the source partition again (n - 1 reads of each partition): that is the
price of a run that can stop and continue from the last partition it completed.

Teardown: teardown.sql, or drop the copy.
*/

\if :{?n}
\else
    \set n 10
\endif
\if :{?host}
\else
    \set host loadtest.invalid
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

/* Each user trigger of the two parents and their partitions as it was before the first run, by name, so the end of the
   run sets each one back to that state, not to enabled (#5794 review round 2). Empty when no run is in progress. */
CREATE TABLE IF NOT EXISTS loadtest.trigger_state
(
    relation_name text NOT NULL,
    trigger_name text NOT NULL,
    is_parent boolean NOT NULL,
    tgenabled "char" NOT NULL,
    CONSTRAINT pk_trigger_state PRIMARY KEY (relation_name, trigger_name)
);

/* One row while the compose rollup mark is owed: from the commit of a wide clone to the commit of the mark. */
CREATE TABLE IF NOT EXISTS loadtest.compose_mark_owed
(
    id smallint NOT NULL DEFAULT 1,
    owed_since timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT pk_compose_mark_owed PRIMARY KEY (id),
    CONSTRAINT ck_compose_mark_owed_one_row CHECK (id = 1)
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

/* The user triggers of the two parents and of their partitions: the ones whose late-row work the script does itself. */
CREATE OR REPLACE FUNCTION loadtest.trigger_relations()
RETURNS TABLE (rel regclass)
LANGUAGE sql
STABLE
AS $f$
    SELECT x.leaf FROM loadtest.leaves('collect.query_store_interval_wide') AS x
    UNION
    SELECT x.leaf FROM loadtest.leaves('collect.query_store_interval_latest') AS x
    UNION
    SELECT 'collect.query_store_interval_wide'::regclass
    UNION
    SELECT 'collect.query_store_interval_latest'::regclass;
$f$;

/* Saves the state of each user trigger, unless states from an earlier run are still saved, and disables them. One
   CALL is one transaction, so the save and both ALTERs apply together or not at all. */
CREATE OR REPLACE PROCEDURE loadtest.disable_triggers()
LANGUAGE plpgsql
AS $p$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM loadtest.trigger_state) THEN
        INSERT INTO loadtest.trigger_state (relation_name, trigger_name, is_parent, tgenabled)
        SELECT format('%I.%I', n.nspname, c.relname), tg.tgname, c.relkind = 'p', tg.tgenabled
        FROM pg_trigger AS tg
        JOIN pg_class AS c ON c.oid = tg.tgrelid
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE NOT tg.tgisinternal
        AND   tg.tgrelid IN (SELECT r.rel FROM loadtest.trigger_relations() AS r);
    ELSE
        RAISE NOTICE 'trigger states from an earlier run are still saved: they are kept';
    END IF;

    ALTER TABLE collect.query_store_interval_wide DISABLE TRIGGER USER;
    ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER USER;
END
$p$;

/* Sets each saved trigger back to its saved state by name, parents first (an ALTER on a parent also reaches its
   partitions), then empties the saved states. One CALL is one transaction. It is also the recovery statement after an
   interrupted run. */
CREATE OR REPLACE PROCEDURE loadtest.restore_triggers()
LANGUAGE plpgsql
AS $p$
DECLARE
    r record;
    restored integer := 0;
BEGIN
    FOR r IN
        SELECT s.relation_name, s.trigger_name, s.tgenabled
        FROM loadtest.trigger_state AS s
        ORDER BY s.is_parent DESC, s.relation_name, s.trigger_name
    LOOP
        CONTINUE WHEN NOT EXISTS
        (
            SELECT 1
            FROM pg_trigger AS tg
            WHERE tg.tgrelid = to_regclass(r.relation_name)
            AND   tg.tgname = r.trigger_name
        );
        EXECUTE format
        (
            'ALTER TABLE %s %s TRIGGER %I',
            r.relation_name,
            CASE r.tgenabled WHEN 'D' THEN 'DISABLE' WHEN 'R' THEN 'ENABLE REPLICA' WHEN 'A' THEN 'ENABLE ALWAYS' ELSE 'ENABLE' END,
            r.trigger_name
        );
        restored := restored + 1;
    END LOOP;

    DELETE FROM loadtest.trigger_state;
    RAISE NOTICE 'user triggers set back to their saved states: %', restored;
END
$p$;

/* The estimated rows (the planner's reltuples, NULL for a leaf never analyzed) and the total bytes of every leaf of the
   six tables. Both come from the catalog and the file sizes, so the report reads no table rows: a count(*) of every leaf
   would read the whole store twice per run, even a run that adds nothing. The exact rows a run added come from
   loadtest.inflate_done. */
CREATE OR REPLACE FUNCTION loadtest.partition_report()
RETURNS TABLE (table_name text, partition_name text, estimated_rows bigint, total_bytes bigint)
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
            SELECT CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END
            INTO estimated_rows
            FROM pg_class AS c
            WHERE c.oid = l;
            total_bytes := pg_total_relation_size(l);
            RETURN NEXT;
        END LOOP;
    END LOOP;
END
$f$;

CREATE OR REPLACE PROCEDURE loadtest.inflate(p_n integer, p_host text)
LANGUAGE plpgsql
AS $p$
DECLARE
    src record;
    syn record;
    k integer;
    new_id integer;
    new_name text;
    server_columns text;
    server_values text;
    config_columns text;
    config_values text;
    t text;
    l regclass;
    insert_columns text;
    select_columns text;
    added bigint;
    marked bigint;
BEGIN
    IF p_n IS NULL OR p_n < 1 THEN
        RAISE EXCEPTION 'n must be 1 or more, not %', p_n;
    END IF;

    IF p_host IS NULL OR p_host = '' THEN
        RAISE EXCEPTION 'host must name the connection target of the synthetic servers';
    END IF;

    /* The proof that the late-row triggers are off for the run: user triggers on the two parents and on their leaves
       that are still enabled (tgenabled other than D). */
    RAISE NOTICE 'user triggers still enabled on the two tables and their partitions: %',
    (
        SELECT count(*)
        FROM pg_trigger AS tg
        WHERE NOT tg.tgisinternal
        AND   tg.tgenabled <> 'D'
        AND   tg.tgrelid IN (SELECT r.rel FROM loadtest.trigger_relations() AS r)
    );

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
                WHEN 'is_enabled' THEN 'TRUE'
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
            /* Never 0 (the fleet sentinel) and never an id either registry already holds. */
            WHILE new_id = 0
                  OR EXISTS (SELECT 1 FROM collect.servers AS s WHERE s.server_id = new_id)
                  OR EXISTS (SELECT 1 FROM config.config_monitored_servers AS c WHERE c.server_id = new_id)
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

    /* 1b. The config registry, for every synthetic server, also the ones an earlier run made. A clone of the source's
       config row, with the unroutable host and enabled; a minimal row when the source has none. */
    SELECT
        string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum),
        string_agg
        (
            CASE a.attname
                WHEN 'server_id' THEN '$1'
                WHEN 'name' THEN '$2'
                WHEN 'host' THEN '$3'
                WHEN 'is_enabled' THEN 'TRUE'
                WHEN 'created_at' THEN '(now() AT TIME ZONE ''UTC'')'
                WHEN 'modified_at' THEN '(now() AT TIME ZONE ''UTC'')'
                ELSE 'c.' || quote_ident(a.attname)
            END,
            ', ' ORDER BY a.attnum
        )
    INTO config_columns, config_values
    FROM pg_attribute AS a
    WHERE a.attrelid = 'config.config_monitored_servers'::regclass
    AND   a.attnum > 0
    AND   NOT a.attisdropped
    AND   a.attgenerated = ''
    AND   a.attidentity = '';

    FOR syn IN SELECT sm.source_server_id, sm.synthetic_server_id, sm.synthetic_server_name FROM loadtest.server_map AS sm ORDER BY 1, 2
    LOOP
        IF EXISTS (SELECT 1 FROM config.config_monitored_servers AS c WHERE c.server_id = syn.synthetic_server_id) THEN
            UPDATE config.config_monitored_servers AS c
            SET is_enabled = TRUE,
                host = p_host,
                modified_at = (now() AT TIME ZONE 'UTC')
            WHERE c.server_id = syn.synthetic_server_id
            AND   (NOT c.is_enabled OR c.host IS DISTINCT FROM p_host);
        ELSIF EXISTS (SELECT 1 FROM config.config_monitored_servers AS c WHERE c.server_id = syn.source_server_id) THEN
            EXECUTE format
            (
                'INSERT INTO config.config_monitored_servers (%s) SELECT %s FROM config.config_monitored_servers AS c WHERE c.server_id = $4',
                config_columns,
                config_values
            )
            USING syn.synthetic_server_id, syn.synthetic_server_name, p_host, syn.source_server_id;
        ELSE
            INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled)
            VALUES (syn.synthetic_server_id, syn.synthetic_server_name, p_host, TRUE);
        END IF;
    END LOOP;

    UPDATE collect.servers AS s
    SET is_enabled = TRUE
    FROM loadtest.server_map AS sm
    WHERE s.server_id = sm.synthetic_server_id
    AND   NOT s.is_enabled;

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

                /* Insert through the parent, so each clone is routed to the partition of its own timestamp, which is
                   the source row's partition. */
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

                /* The mark is owed from this commit on, in the same transaction as the rows. */
                IF t = 'collect.query_store_interval_wide' AND added > 0 THEN
                    INSERT INTO loadtest.compose_mark_owed DEFAULT VALUES ON CONFLICT DO NOTHING;
                END IF;

                INSERT INTO loadtest.inflate_done (table_name, partition_name, copy_no, rows_added)
                VALUES (t, l::text, k, added);

                RAISE NOTICE '% copy %: % rows', l, k, added;
                COMMIT;
            END LOOP;
        END LOOP;
    END LOOP;

    /* 3. What the wide table's late-row trigger would have done, in one statement on the small built table: every
       (synthetic server, hour) whose source pair the compose rollup has built becomes a late pair (late_seq above
       built_seq), so the rollup's next ticks rebuild those hours with the clones in them. Hours the rollup has not
       built yet need no mark: their first build reads every server. The flag, not this run's row count, decides: a run
       that added every wide clone and stopped before the mark adds no wide rows when it runs again. A store without the
       built table has built no hour, so it needs no mark and the flag is cleared. */
    IF EXISTS (SELECT 1 FROM loadtest.compose_mark_owed) THEN
        IF to_regclass('collect.query_store_compose_stamp_built') IS NOT NULL THEN
            INSERT INTO collect.query_store_compose_stamp_built AS b (server_id, hour, late_seq)
            SELECT sm.synthetic_server_id, s.hour, 1
            FROM collect.query_store_compose_stamp_built AS s
            JOIN loadtest.server_map AS sm
              ON sm.source_server_id = s.server_id
            ON CONFLICT (server_id, hour) DO UPDATE SET late_seq = b.late_seq + 1;
            GET DIAGNOSTICS marked = ROW_COUNT;
            RAISE NOTICE 'compose rollup: % (synthetic server, hour) pairs marked late', marked;
        END IF;
        DELETE FROM loadtest.compose_mark_owed;
        COMMIT;
    END IF;
END
$p$;

DROP TABLE IF EXISTS pg_temp.inflate_before;
CREATE TEMP TABLE inflate_before AS SELECT * FROM loadtest.partition_report();
DROP TABLE IF EXISTS pg_temp.inflate_done_before;
CREATE TEMP TABLE inflate_done_before AS SELECT d.table_name, d.partition_name, d.copy_no FROM loadtest.inflate_done AS d;

/* The service must be stopped: the ALTERs wait for every open writer of the two tables. Restored below, also when the
   inflate fails. If psql itself is stopped before then, run CALL loadtest.restore_triggers(); by hand. */
CALL loadtest.disable_triggers();

\set ON_ERROR_STOP 0
CALL loadtest.inflate(:n, :'host');
\set inflate_failed :ERROR
\set ON_ERROR_STOP 1

CALL loadtest.restore_triggers();

\if :inflate_failed
    DO $$
    BEGIN
        RAISE EXCEPTION 'The inflate failed: the error is above. The triggers are back in their earlier states. Run the script again with the same n: it continues from the last partition it completed.';
    END
    $$;
\endif

DROP TABLE IF EXISTS pg_temp.inflate_after;
CREATE TEMP TABLE inflate_after AS SELECT * FROM loadtest.partition_report();

DROP TABLE IF EXISTS pg_temp.inflate_added;
CREATE TEMP TABLE inflate_added AS
SELECT
    d.table_name,
    d.partition_name,
    sum(d.rows_added) FILTER
    (
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM inflate_done_before AS b
            WHERE b.table_name = d.table_name
            AND   b.partition_name = d.partition_name
            AND   b.copy_no = d.copy_no
        )
    ) AS rows_added_this_run,
    sum(d.rows_added) AS rows_added_all_runs
FROM loadtest.inflate_done AS d
GROUP BY d.table_name, d.partition_name;

\echo ''
\echo 'Rows and bytes per partition (estimated rows from the catalog, added rows exact):'
SELECT
    coalesce(a.table_name, b.table_name) AS table_name,
    coalesce(a.partition_name, b.partition_name) AS partition_name,
    b.estimated_rows AS estimated_rows_before,
    coalesce(x.rows_added_this_run, 0) AS rows_added_this_run,
    coalesce(x.rows_added_all_runs, 0) AS rows_added_all_runs,
    pg_size_pretty(coalesce(b.total_bytes, 0)) AS bytes_before,
    pg_size_pretty(coalesce(a.total_bytes, 0)) AS bytes_after
FROM inflate_after AS a
FULL JOIN inflate_before AS b
  ON  b.table_name = a.table_name
  AND b.partition_name = a.partition_name
LEFT JOIN inflate_added AS x
  ON  x.table_name = coalesce(a.table_name, b.table_name)
  AND x.partition_name = coalesce(a.partition_name, b.partition_name)
ORDER BY 1, 2;

\echo ''
\echo 'Totals per table:'
SELECT
    a.table_name,
    sum(b.estimated_rows) AS estimated_rows_before,
    sum(coalesce(x.rows_added_this_run, 0)) AS rows_added_this_run,
    sum(coalesce(x.rows_added_all_runs, 0)) AS rows_added_all_runs,
    pg_size_pretty(sum(coalesce(b.total_bytes, 0))::bigint) AS bytes_before,
    pg_size_pretty(sum(a.total_bytes)::bigint) AS bytes_after
FROM inflate_after AS a
LEFT JOIN inflate_before AS b
  ON  b.table_name = a.table_name
  AND b.partition_name = a.partition_name
LEFT JOIN inflate_added AS x
  ON  x.table_name = a.table_name
  AND x.partition_name = a.partition_name
GROUP BY a.table_name
ORDER BY a.table_name;

\echo ''
\echo 'Synthetic servers (source_servers is the server count to use in the runbook formula):'
SELECT count(*) AS synthetic_servers, count(DISTINCT source_server_id) AS source_servers, max(copy_no) + 1 AS n
FROM loadtest.server_map;
