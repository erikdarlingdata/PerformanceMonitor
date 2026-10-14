/*
#5794 load test teardown, for a copy that you keep after the test. The simplest teardown is to drop the copy.
This removes every synthetic row that inflate-query-store.sql added, the synthetic servers and the loadtest schema.
The deletes read every partition of the large tables, so this can take as long as the inflate run did.
*/
DO $$
DECLARE
    t text;
BEGIN
    IF to_regclass('loadtest.server_map') IS NULL THEN
        RAISE NOTICE 'Nothing to remove: loadtest.server_map does not exist.';
        RETURN;
    END IF;

    FOREACH t IN ARRAY loadtest.query_store_tables()
    LOOP
        EXECUTE format('DELETE FROM %s AS r USING loadtest.server_map AS m WHERE r.server_id = m.synthetic_server_id', t);
    END LOOP;

    DELETE FROM collect.servers AS s
    USING loadtest.server_map AS m
    WHERE s.server_id = m.synthetic_server_id;
END
$$;

DROP SCHEMA IF EXISTS loadtest CASCADE;
