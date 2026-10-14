/*
#5794 load test teardown, for a copy that you keep after the test. The simplest teardown is to drop the copy.
Run it with psql while the copy's service is stopped:
    psql -X -v ON_ERROR_STOP=1 -d <copy> -f teardown.sql

This removes every synthetic row that inflate-query-store.sql added, the rows the service's Query Store builders wrote
for the synthetic servers (the plan regression daily totals, the Query Store top daily summary, the compose rollup and
their built tables), the rows the service wrote about the synthetic servers (their failed collections in the collection
log, their alerts in the alert log and their alert state, their slow reads), the synthetic servers from both registries,
and the loadtest schema.

What stays: collect.read_latency has no server column, so the read times of the test stay in it until its retention
removes them. The production servers of the copy keep their rows too, also their failed collections and alerts.

It commits after each partition, as the inflate does, so a run that stops can run again and continue: a partition it
has emptied has nothing left to delete. The deletes read every partition of the large tables, so this can take as long
as the inflate run did.
*/

SET client_min_messages = notice;

/* A stopped inflate can leave the late-row triggers disabled: set each one back to the state the inflate saved, by name
   (#5794 review round 2). Nothing is saved when no inflate run is in progress, and then nothing changes. */
DO $$
BEGIN
    IF to_regprocedure('loadtest.restore_triggers()') IS NOT NULL THEN
        CALL loadtest.restore_triggers();
    END IF;
END
$$;

DO $$
BEGIN
    IF to_regclass('loadtest.server_map') IS NULL THEN
        RAISE NOTICE 'Nothing to remove: loadtest.server_map does not exist.';
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS loadtest;

CREATE OR REPLACE PROCEDURE loadtest.teardown()
LANGUAGE plpgsql
AS $p$
DECLARE
    t text;
    l regclass;
    removed bigint;
BEGIN
    IF to_regclass('loadtest.server_map') IS NULL THEN
        RETURN;
    END IF;

    /* 1. The synthetic interval rows, one leaf per transaction. */
    FOREACH t IN ARRAY loadtest.query_store_tables()
    LOOP
        FOR l IN SELECT x.leaf FROM loadtest.leaves(t) AS x
        LOOP
            EXECUTE format
            (
                'DELETE FROM ONLY %s AS r USING loadtest.server_map AS m WHERE r.server_id = m.synthetic_server_id',
                l
            );
            GET DIAGNOSTICS removed = ROW_COUNT;
            RAISE NOTICE '%: % rows removed', l, removed;
            COMMIT;
        END LOOP;
    END LOOP;

    /* 2. What the service's builders wrote for the synthetic servers: their results and their built rows. Each table
       on its own, and only when the store has it. */
    FOREACH t IN ARRAY ARRAY
    [
        'collect.plan_regression_daily',
        'collect.plan_regression_daily_built',
        'collect.query_store_top_daily',
        'collect.query_store_top_daily_built',
        'collect.query_store_compose_stamp',
        'collect.query_store_compose_stamp_built'
    ]
    LOOP
        CONTINUE WHEN to_regclass(t) IS NULL;
        EXECUTE format
        (
            'DELETE FROM %s AS r USING loadtest.server_map AS m WHERE r.server_id = m.synthetic_server_id',
            t
        );
        GET DIAGNOSTICS removed = ROW_COUNT;
        RAISE NOTICE '%: % rows removed', t, removed;
        COMMIT;
    END LOOP;

    /* 3. The rows the service wrote about the synthetic servers, on each failed cycle: each table that has a server_id
       column, and only when the store has it. collect.read_latency has no server column and keeps its rows. */
    FOREACH t IN ARRAY ARRAY
    [
        'collect.collection_log',
        'collect.slow_reads',
        'collect.collector_state',
        'config.config_alert_log',
        'config.config_edge_trigger_watermarks',
        'config.alert_persistence_state'
    ]
    LOOP
        CONTINUE WHEN to_regclass(t) IS NULL;
        CONTINUE WHEN NOT EXISTS
        (
            SELECT 1
            FROM pg_attribute AS a
            WHERE a.attrelid = to_regclass(t)
            AND   a.attname = 'server_id'
            AND   NOT a.attisdropped
        );
        EXECUTE format
        (
            'DELETE FROM %s AS r USING loadtest.server_map AS m WHERE r.server_id = m.synthetic_server_id',
            t
        );
        GET DIAGNOSTICS removed = ROW_COUNT;
        RAISE NOTICE '%: % rows removed', t, removed;
        COMMIT;
    END LOOP;

    /* 4. The synthetic servers, from both registries. */
    DELETE FROM config.config_monitored_servers AS c
    USING loadtest.server_map AS m
    WHERE c.server_id = m.synthetic_server_id;

    DELETE FROM collect.servers AS s
    USING loadtest.server_map AS m
    WHERE s.server_id = m.synthetic_server_id;
    COMMIT;
END
$p$;

CALL loadtest.teardown();

DROP SCHEMA IF EXISTS loadtest CASCADE;
