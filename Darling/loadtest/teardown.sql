/*
#5794 load test teardown, for a copy that you keep after the test. The simplest teardown is to drop the copy.
Run it with psql while the copy's service is stopped:
    psql -X -v ON_ERROR_STOP=1 -d <copy> -f teardown.sql

This removes every synthetic row that inflate-query-store.sql added, the rows the service's Query Store builders wrote
for the synthetic servers (the plan regression daily totals, the Query Store top daily summary, the compose rollup and
their built tables), the synthetic servers from both registries, and the loadtest schema.

It commits after each partition, as the inflate does, so a run that stops can run again and continue: a partition it
has emptied has nothing left to delete. The deletes read every partition of the large tables, so this can take as long
as the inflate run did.
*/

SET client_min_messages = notice;

/* A stopped inflate can leave the late-row triggers disabled; enable them, whatever state they are in. */
ALTER TABLE collect.query_store_interval_wide ENABLE TRIGGER USER;
ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER USER;

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

    /* 3. The synthetic servers, from both registries. */
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
