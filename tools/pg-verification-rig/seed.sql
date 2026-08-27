/*
 * Enough schema and data to clear the collectors' size floors.
 *
 * This is not a benchmark and the shape of the data does not matter. What matters is the SCALE, because
 * several collectors deliberately ignore small objects and below their floors a healthy collector and a
 * broken one are indistinguishable:
 *
 *   pg_column_stats     relpages >= 128   (a ~1 MB floor; below it, no rows and no way to tell why)
 *   pg_index_bloat      measures the largest indexes per cycle, budgeted
 *   pg_buffer_usage     reports what is resident, which needs something to have been read
 *   pg_predicate_stats  needs predicates actually executed
 *
 * Four defects were found by running the real service against this and reading what it stored.
 */

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_wait_sampling;
CREATE EXTENSION IF NOT EXISTS pg_stat_kcache;
CREATE EXTENSION IF NOT EXISTS pg_qualstats;
CREATE EXTENSION IF NOT EXISTS pgstattuple;
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
CREATE EXTENSION IF NOT EXISTS hypopg;

DO $seed$
DECLARE
    i int;
BEGIN
    FOR i IN 1..40 LOOP
        EXECUTE format($fmt$
            CREATE TABLE IF NOT EXISTS t%1$s (
                id      bigserial PRIMARY KEY,
                ref     integer,
                status  text,
                amount  numeric(12,2),
                note    text,
                created timestamptz DEFAULT now()
            )$fmt$, i);

        /* ~20k rows puts every table comfortably past the 128-page floor. */
        EXECUTE format($fmt$
            INSERT INTO t%1$s (ref, status, amount, note)
            SELECT
                (random() * 1000)::int,
                (ARRAY['new','open','closed','void'])[1 + (random() * 3)::int],
                (random() * 10000)::numeric(12,2),
                repeat('x', 40)
            FROM generate_series(1, 20000)
            ON CONFLICT DO NOTHING$fmt$, i);

        /* Three secondary indexes each: 40 tables x (1 pk + 3) = 160 indexes. */
        EXECUTE format('CREATE INDEX IF NOT EXISTS t%1$s_ref ON t%1$s (ref)', i);
        EXECUTE format('CREATE INDEX IF NOT EXISTS t%1$s_status ON t%1$s (status)', i);
        EXECUTE format('CREATE INDEX IF NOT EXISTS t%1$s_created ON t%1$s (created)', i);

        /* ANALYZE, or pg_stats has nothing to report and pg_column_stats looks broken. */
        EXECUTE format('ANALYZE t%1$s', i);
    END LOOP;
END
$seed$;

/* Some executed predicates, so pg_qualstats and pg_stat_statements have something to have seen.
   Deliberately includes an UNINDEXED column (amount) so there is a real index candidate for
   test_hypothetical_index to find. */
DO $work$
DECLARE
    i int;
    n bigint;
BEGIN
    FOR i IN 1..40 LOOP
        EXECUTE format('SELECT count(*) FROM t%1$s WHERE status = ''open''', i) INTO n;
        EXECUTE format('SELECT count(*) FROM t%1$s WHERE amount > 5000', i) INTO n;
        EXECUTE format('SELECT count(*) FROM t%1$s WHERE ref BETWEEN 100 AND 200', i) INTO n;
    END LOOP;
END
$work$;

/* expr AS alias, not the T-SQL alias = expr this codebase uses everywhere else: PostgreSQL parses the
   latter as a comparison and fails on the unknown column. */
SELECT
    (SELECT count(*) FROM pg_stat_user_tables)  AS tables,
    (SELECT count(*) FROM pg_stat_user_indexes) AS indexes,
    (SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relpages >= 128) AS tables_over_the_size_floor;
