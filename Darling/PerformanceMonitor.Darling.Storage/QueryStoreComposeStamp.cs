/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The exact stamp-grain rollup of <c>collect.query_store_interval_wide</c> (#5582, V173): per (collection_time, server,
/// database, module, query_hash), the PARTIALS a Query Store panel combines (counts, sums, minima, maxima and the
/// execution-weighted products), so a fleet-day panel reads about a sixth of the bytes per wide row it replaces
/// (the field numbers are in the PR for #5582). The grain is the exact stamp, not the hour, so a bucketed panel gets the
/// same minute buckets the wide read gives. The compiler combines these partials with the same expressions it combines
/// the single-scan base CTE's partials with, so the answer is the wide table's answer.
///
/// <para><b>Validity is per (server, hour).</b> <c>collect.query_store_compose_stamp_built</c> keeps one row per pair with
/// <c>late_seq</c> (the number of transactions that landed a late row in the pair, bumped by the row triggers on the wide
/// table, once per pair per transaction) and <c>built_seq</c> (the value the builder read BEFORE it
/// aggregated). A pair is valid only while the two are equal. <c>collect.query_store_compose_stamp_hours</c> records which
/// hours the builder has done. The keys are per server for V168's reason: each server's apply is its own transaction, and a
/// fleet-wide hour row would queue 43 appliers on one tuple.</para>
///
/// <para><b>The build, one transaction per hour</b> (<see cref="BuildHourAsync"/>): (0) a transaction advisory lock on the hour,
/// so two services never build one hour at once; (0b) the margin guard (<see cref="OpenWriterSql"/>), which defers the hour while
/// a writing transaction that could hold an unmarked row of it is open; (1) a plain <c>SELECT</c> of each of the hour's pairs'
/// <c>late_seq</c>, BEFORE the aggregation (that order is what keeps the rollup exact: see <see cref="ReadPairsSql"/>); (2) delete the hour's rollup rows; (3) insert them from the
/// wide table; (4) upsert every pair of the hour with <c>built_seq</c> = the value read in (1), 0 for a pair that did not
/// exist; (5) upsert the hour. A writer whose bump is not committed at (1), or whose pair appears after (1), leaves the pair
/// stale (its <c>late_seq</c> ends above the <c>built_seq</c> written in (4), or its row is inserted with <c>built_seq</c>
/// NULL), which is safe: the next tick rebuilds the hour. A writer that bumps a pair the build has already upserted waits for the
/// build's commit on that row, from (4) on.</para>
///
/// <para><b>Order:</b> stale hours first (a stale hour is one the compiler can no longer serve from the rollup), then the hours
/// the builder has never done, newest first, so the default one-day window is servable after about 21 builds rather than after
/// the whole backlog. An hour is built once it is <see cref="BuildLag"/> old: a row leaves its hour at most about 59.5 minutes
/// plus one collection cycle after its stamp, about 2 h 25 min after the hour starts, so 3 h holds with about 35 minutes
/// to spare, and the trigger covers whatever is later.</para>
///
/// <para><b>The partial columns come from one table</b> (<see cref="PartialColumnMap"/>): the CREATE TABLE in the V173 rung is a
/// literal (a shipped migration is never regenerated), a test pins it equal to the map, and the builder's expressions and the
/// compiler's lookups are generated from the map. They are never <c>double precision</c>.</para>
///
/// <para><b>No Lite twin:</b> Lite has no PostgreSQL store.</para>
/// </summary>
public static class QueryStoreComposeStamp
{
    /// <summary>The store schema version of the rung that creates the tables and the triggers (V173).</summary>
    public const int RungVersion = 173;

    public const string Table = "collect.query_store_compose_stamp";
    public const string BuiltTable = "collect.query_store_compose_stamp_built";
    public const string HoursTable = "collect.query_store_compose_stamp_hours";
    public const string TriggerFunction = "collect.query_store_compose_stamp_mark_late";
    public const string InsertTrigger = "trg_query_store_compose_stamp_late_ins";
    public const string UpdateTrigger = "trg_query_store_compose_stamp_late_upd";

    /// <summary>How old an hour must be before the builder does it: build hour H once now is at least H + this.</summary>
    public static readonly TimeSpan BuildLag = TimeSpan.FromHours(3);

    /// <summary>The trigger marks a row whose hour is more than this before the hour of the writer's clock (the
    /// <c>WHEN</c> offset). One hour, against <see cref="BuildLag"/> of three: a row is marked from H + 2 h on, and the
    /// builder reads H from H + 3 h on, so a writer transaction would have to stay open over an hour between its write
    /// and its commit to land an unmarked row after the build. The offset is a literal in the rung (a shipped migration);
    /// <see cref="WhenOffsetSql"/> is the text a test compares it to.</summary>
    public static readonly TimeSpan WhenOffset = TimeSpan.FromHours(1);

    public const string WhenOffsetSql = "interval '1 hour'";

    /// <summary>The most hours one tick builds (about 2.6 s each cold, 400 K wide rows at 6.5 us).</summary>
    public const int MaxBuildsPerTick = 6;

    /// <summary>The wall-clock budget of one tick: no new build starts once this has passed.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(10);

    /// <summary>The server-side <c>statement_timeout</c> each build's transaction runs under.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>The alias the compiler gives the fact relation in a partial's text (<c>ComposeCompiler.FactAlias</c>). The
    /// map's <see cref="PartialColumn.FactExpression"/> is written over it, verbatim the text <c>TryBuildPartialValueExpr</c>
    /// adds to its <c>PartialColumns</c>, and the builder reads the wide table under the same alias.</summary>
    public const string FactAlias = "f";

    /// <summary>One partial a Query Store panel can ask for, and the rollup column that holds it.</summary>
    /// <param name="Column">The rollup column.</param>
    /// <param name="SqlType">The column's type: <c>numeric</c> for a sum (<c>SUM(bigint)</c> is numeric), <c>bigint</c> for
    /// a count, a minimum or a maximum. Never <c>double precision</c>.</param>
    /// <param name="FactExpression">The aggregate over one group of wide rows, over <see cref="FactAlias"/>, as the
    /// compiler writes it. The compiler finds its partial in the map by this text.</param>
    public sealed record PartialColumn(string Column, string SqlType, string FactExpression);

    /// <summary>
    /// The one table that says which rollup column holds which partial (#5582). Every partial that
    /// <c>ComposeCompiler.TryBuildPartialValueExpr</c> can ask for over a Query Store measure and a valid aggregate is here
    /// (a test walks the compiler and fails on one that is not): the cumulative <c>execution_count</c> (Sum, Avg, Min, Max;
    /// Count is <c>wide_rows</c>), the gauges <c>max_duration_us</c> and <c>max_cpu_time_us</c> (Avg, Min, Max), and the
    /// execution-weighted products <c>avg_duration_us * execution_count</c> and <c>avg_cpu_time_us * execution_count</c>,
    /// whose weight is <c>SUM(execution_count)</c>, which is <c>ec_sum</c>. An average partial is the sum and the count of
    /// NON-NULL values, so a NULL is not averaged. Order is the column order of the table: the 8-byte columns first.
    /// </summary>
    public static IReadOnlyList<PartialColumn> PartialColumnMap { get; } = new[]
    {
        new PartialColumn("wide_rows", "bigint", "COUNT(*)"),
        new PartialColumn("ec_count", "bigint", "COUNT(f.execution_count)"),
        new PartialColumn("ec_min", "bigint", "MIN(f.execution_count)"),
        new PartialColumn("ec_max", "bigint", "MAX(f.execution_count)"),
        new PartialColumn("maxdur_count", "bigint", "COUNT(f.max_duration_us)"),
        new PartialColumn("maxdur_min", "bigint", "MIN(f.max_duration_us)"),
        new PartialColumn("maxdur_max", "bigint", "MAX(f.max_duration_us)"),
        new PartialColumn("maxcpu_count", "bigint", "COUNT(f.max_cpu_time_us)"),
        new PartialColumn("maxcpu_min", "bigint", "MIN(f.max_cpu_time_us)"),
        new PartialColumn("maxcpu_max", "bigint", "MAX(f.max_cpu_time_us)"),
        new PartialColumn("ec_sum", "numeric", "SUM(f.execution_count)"),
        new PartialColumn("maxdur_sum", "numeric", "SUM(f.max_duration_us)"),
        new PartialColumn("maxcpu_sum", "numeric", "SUM(f.max_cpu_time_us)"),
        new PartialColumn("dur_wsum", "numeric", "SUM(f.avg_duration_us * f.execution_count)"),
        new PartialColumn("cpu_wsum", "numeric", "SUM(f.avg_cpu_time_us * f.execution_count)"),
    };

    /// <summary>The key columns of the grain, after <c>collection_time</c>.</summary>
    public static readonly IReadOnlyList<string> DimensionColumns = new[] { "server_id", "database_name", "module_name", "query_hash" };

    /// <summary>The rollup column that holds <paramref name="factExpression"/> (the compiler's text of a partial), or null.</summary>
    public static PartialColumn? ColumnFor(string factExpression) =>
        PartialColumnMap.FirstOrDefault(c => string.Equals(c.FactExpression, factExpression, StringComparison.Ordinal));

    /// <summary>
    /// The three tables, the index, the trigger function and the two row triggers, as the V173 rung runs them. Idempotent,
    /// catalog-only: the tables are created empty and the triggers are created on the partitioned PARENT, which clones them
    /// onto every leaf that exists and onto every leaf created or attached later. The rung writes no GRANT (the <c>collect</c>
    /// schema's blanket SELECT covers a new table). A shipped migration is never edited: change the rollup in a new rung.
    /// </summary>
    public const string CreateSql = """
/* V173 (#5582): the exact stamp-grain rollup of query_store_interval_wide, its per-(server, hour) validity record, the
   hours the builder has done, and the row triggers that mark a pair stale when a row lands in (or leaves) an hour that
   the builder may already have done. Naive UTC throughout. The service builds the rollup; the function writes only the
   validity table, the rung's own bookkeeping. */
CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp
(
    collection_time timestamp NOT NULL,
    wide_rows bigint NOT NULL,
    ec_count bigint,
    ec_min bigint,
    ec_max bigint,
    maxdur_count bigint,
    maxdur_min bigint,
    maxdur_max bigint,
    maxcpu_count bigint,
    maxcpu_min bigint,
    maxcpu_max bigint,
    server_id integer NOT NULL,
    database_name text,
    module_name text,
    query_hash text,
    ec_sum numeric,
    maxdur_sum numeric,
    maxcpu_sum numeric,
    dur_wsum numeric,
    cpu_wsum numeric
);

/* No unique index: a one-day range read scans the index leaves in range, and a five-column unique key is about a third
   more cold I/O than the read it would guard. The grain is held by the builder's GROUP BY and its one-transaction
   DELETE and INSERT under the validity rows' locks. The index is B-tree-deduplicated (about 2,600 distinct keys a day). */
CREATE INDEX IF NOT EXISTS ix_query_store_compose_stamp_time_server
ON collect.query_store_compose_stamp (collection_time, server_id);

CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp_built
(
    server_id integer NOT NULL,
    hour timestamp NOT NULL,
    late_seq bigint NOT NULL DEFAULT 0,
    built_seq bigint,
    built_at timestamp,
    source_rows bigint,
    stamp_rows bigint,
    CONSTRAINT pk_query_store_compose_stamp_built PRIMARY KEY (server_id, hour)
);

CREATE TABLE IF NOT EXISTS collect.query_store_compose_stamp_hours
(
    hour timestamp NOT NULL,
    built_at timestamp NOT NULL,
    CONSTRAINT pk_query_store_compose_stamp_hours PRIMARY KEY (hour)
);

CREATE OR REPLACE FUNCTION collect.query_store_compose_stamp_mark_late() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $f$
/* A (server, hour) is bumped at most once per transaction, for V168's reason: an apply is one statement, so tens of
   thousands of late rows would otherwise update the same validity row again and again, each update leaving another
   version of the tuple inside the one transaction, and the batch grows quadratically. One bump is enough: the builder
   reads late_seq before it aggregates, and the batch's rows commit together with the bump. The pairs
   already bumped are kept in a transaction-local setting (set_config(.., true)), which a rolled-back savepoint undoes
   together with the bump it recorded. Each key is server_id:hour-number between commas (hour-number is whole hours since
   1970-01-01), so one lookup is a substring test. After a commit the setting reads back as an empty string, not NULL.
   An UPDATE marks the hour it moves a row OUT of (OLD) as well as the hour it lands in (NEW): the open interval's row is
   refreshed with a newer collection_time, so a built hour loses a row. The threshold is the writer's own clock, not the
   transaction start: a writer transaction open longer than the build lag could otherwise land a row in a built hour
   unmarked. */
DECLARE
    threshold timestamp := date_trunc('hour', clock_timestamp() AT TIME ZONE 'UTC') - interval '1 hour';
    marked text := coalesce(nullif(current_setting('darling.query_store_compose_stamp_marked', true), ''), ',');
    sn integer;
    hn timestamp;
    kn text;
    so integer;
    ho timestamp;
    ko text;
BEGIN
    IF NEW.collection_time < threshold THEN
        sn := NEW.server_id;
        hn := date_trunc('hour', NEW.collection_time);
        kn := ',' || sn || ':' || (extract(epoch FROM hn)::bigint / 3600) || ',';
        IF position(kn IN marked) > 0 THEN
            kn := NULL;
        END IF;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF OLD.collection_time < threshold THEN
            so := OLD.server_id;
            ho := date_trunc('hour', OLD.collection_time);
            ko := ',' || so || ':' || (extract(epoch FROM ho)::bigint / 3600) || ',';
            IF position(ko IN marked) > 0 OR ko IS NOT DISTINCT FROM kn THEN
                ko := NULL;
            END IF;
        END IF;
    END IF;

    IF kn IS NULL AND ko IS NULL THEN
        RETURN NULL;
    END IF;

    INSERT INTO collect.query_store_compose_stamp_built AS b (server_id, hour, late_seq)
    SELECT v.s, v.h, 1
    FROM (VALUES (sn, hn, kn), (so, ho, ko)) AS v (s, h, k)
    WHERE v.k IS NOT NULL
    ON CONFLICT (server_id, hour) DO UPDATE SET late_seq = b.late_seq + 1;

    PERFORM set_config('darling.query_store_compose_stamp_marked',
        marked || CASE WHEN kn IS NOT NULL THEN substr(kn, 2) ELSE '' END || CASE WHEN ko IS NOT NULL THEN substr(ko, 2) ELSE '' END, true);
    RETURN NULL;
END
$f$;

/* On the partitioned PARENT, as V172 does for the latest table: a row trigger on a partitioned table fires for rows written
   through it, and is cloned onto every existing leaf (the legacy table, DEFAULT, each day) and onto every leaf created or
   attached later. WHEN reads the writer's clock (clock_timestamp), not the transaction start. No DELETE trigger: retention
   removes only hours that no eligible window reads, and a partition move does not change the table's content. */
DROP TRIGGER IF EXISTS trg_query_store_compose_stamp_late_ins ON collect.query_store_interval_wide;
CREATE TRIGGER trg_query_store_compose_stamp_late_ins
    AFTER INSERT ON collect.query_store_interval_wide
    FOR EACH ROW
    WHEN (NEW.collection_time < date_trunc('hour', clock_timestamp() AT TIME ZONE 'UTC') - interval '1 hour')
    EXECUTE FUNCTION collect.query_store_compose_stamp_mark_late();

DROP TRIGGER IF EXISTS trg_query_store_compose_stamp_late_upd ON collect.query_store_interval_wide;
CREATE TRIGGER trg_query_store_compose_stamp_late_upd
    AFTER UPDATE ON collect.query_store_interval_wide
    FOR EACH ROW
    WHEN (OLD.collection_time < date_trunc('hour', clock_timestamp() AT TIME ZONE 'UTC') - interval '1 hour'
          OR NEW.collection_time < date_trunc('hour', clock_timestamp() AT TIME ZONE 'UTC') - interval '1 hour')
    EXECUTE FUNCTION collect.query_store_compose_stamp_mark_late();
""";

    /// <summary>The insert column list: <c>collection_time</c>, the partials in map order, then the dimensions.</summary>
    private static string InsertColumns() =>
        "collection_time, " + string.Join(", ", PartialColumnMap.Select(c => c.Column)) + ", " + string.Join(", ", DimensionColumns);

    /// <summary>
    /// Takes the hour's transaction advisory lock: $1 the hour, as whole hours since 1970-01-01 (<see cref="HourNumber"/>).
    /// Two services (or a manual run) building one hour would otherwise both delete nothing and both insert, which doubles
    /// the hour's rows when the hour has no validity rows to lock.
    /// </summary>
    public const string HourLockSql = "SELECT pg_advisory_xact_lock(5582173, $1::integer);";

    /// <summary>
    /// The margin guard (#5582 review round 1): true while a WRITING transaction that could hold an unmarked row of the hour is
    /// still open: $1 the hour. The row trigger skips a row whose <c>collection_time</c> is within one hour of the hour of the
    /// writer's clock (<see cref="WhenOffset"/>), so a row of hour H can land unmarked only while the store's clock is before
    /// H + 2 h, and the writer's transaction began before that. If such a transaction is still open when the build starts, its
    /// rows may commit after the build read them, and nothing would ever mark the pair: the hour would read as current with the
    /// rows missing. So the build waits (it is deferred to the next tick) until no such transaction is open. After the check passes,
    /// every writer of an unmarked row of H has finished, and a writer that starts later writes marked rows, because the store's
    /// clock is past H + 3 h when the plan picks the hour.
    ///
    /// <para><c>backend_xid IS NOT NULL</c> is "has written" (a read-only transaction holds no xid, so a long report query does
    /// not defer anything). <c>datname = current_database()</c> keeps another database of the instance out of it, and
    /// <c>pid &lt;&gt; pg_backend_pid()</c> the build itself. <c>xact_start</c> of ANOTHER role's session reads NULL without
    /// <c>pg_read_all_stats</c>, and NULL never satisfies the comparison, so such a session is ignored: the only writer of the wide
    /// table is the service's own role, whose sessions the build's role can see. <c>pg_stat_activity</c> is read once per
    /// transaction, and this is the build's first read of it, after the advisory lock.</para>
    /// </summary>
    public const string OpenWriterSql = @"
SELECT EXISTS
(
    SELECT 1
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND backend_xid IS NOT NULL
      AND pid <> pg_backend_pid()
      AND (xact_start AT TIME ZONE 'UTC') < $1 + interval '2 hours'
);";

    /// <summary>
    /// Reads each of the hour's pairs' <c>late_seq</c>: $1 the hour. A plain <c>SELECT</c>, with no row lock: the hour's advisory
    /// lock already serializes builders, and what keeps the rollup exact is the ORDER, this read before the aggregation's
    /// snapshot. A bump committed before this read has its rows committed with it, so the aggregation sees them. A bump not
    /// committed at this read leaves <c>late_seq</c> above the <c>built_seq</c> the build writes (the upsert waits for that
    /// writer's row lock, then updates <c>built_seq</c> only), so the pair ends stale and the next tick rebuilds it. A
    /// <c>FOR UPDATE</c> here would save only that one rebuild, and would make every late writer that has to bump one of the
    /// hour's pairs wait for the whole build, under the collector's 5 s lock timeout (#5582 review round 1).
    /// </summary>
    public const string ReadPairsSql = @"
SELECT server_id, late_seq
FROM collect.query_store_compose_stamp_built
WHERE hour = $1
ORDER BY server_id;";

    /// <summary>Clears the hour's rollup rows before they are rebuilt: $1 the hour. Served by the (collection_time, server_id) index.</summary>
    public const string DeleteHourSql = @"
DELETE FROM collect.query_store_compose_stamp
WHERE collection_time >= $1
  AND collection_time < $1 + interval '1 hour';";

    /// <summary>
    /// Rolls up one hour of the wide table and returns the number of wide rows it read: $1 the hour. The partial expressions are
    /// the map's <see cref="PartialColumn.FactExpression"/> verbatim (the compiler's text), so a rollup column is exactly the
    /// aggregate the single-scan read computes over the same rows. <c>ORDER BY collection_time, server_id</c> lays the
    /// rows down in index order.
    ///
    /// <para>The <c>first_execution_time</c> floor is the one every per-server read of the wide table carries
    /// (<see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/>): with the table partitioned by day on that column it
    /// lets the planner skip the leaves that cannot hold a row of the hour (the legacy table above all), and it drops no
    /// row, because every stored row has <c>first_execution_time</c> within <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>
    /// before its <c>collection_time</c> (the argument is on that member). There is no upper bound: the compiler's wide read has
    /// none either, and the rollup must hold what the wide read holds.</para>
    /// </summary>
    public static readonly string BuildHourInsertSql = $@"
INSERT INTO collect.query_store_compose_stamp ({InsertColumns()})
SELECT {FactAlias}.collection_time, {string.Join(", ", PartialColumnMap.Select(c => c.FactExpression))}, {string.Join(", ", DimensionColumns.Select(d => FactAlias + "." + d))}
FROM collect.query_store_interval_wide AS {FactAlias}
WHERE {FactAlias}.collection_time >= $1
  AND {FactAlias}.collection_time < $1 + interval '1 hour'
  AND {FactAlias}.first_execution_time >= $1 - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
GROUP BY {FactAlias}.collection_time, {string.Join(", ", DimensionColumns.Select(d => FactAlias + "." + d))}
ORDER BY {FactAlias}.collection_time, {FactAlias}.server_id;";

    /// <summary>The wide rows the hour read, as the sum of the rollup's <c>wide_rows</c>: $1 the hour. A separate statement from
    /// the insert, on the rollup rows the transaction just wrote (a few thousand).</summary>
    public const string SourceRowsSql = @"
SELECT COALESCE(sum(wide_rows), 0)::bigint
FROM collect.query_store_compose_stamp
WHERE collection_time >= $1
  AND collection_time < $1 + interval '1 hour';";

    /// <summary>
    /// Records the hour's validity rows: $1 the hour, $2 built_at, $3 the servers read in step 1, $4 their <c>late_seq</c>.
    /// Every server of step 1 and every server in the new rollup rows gets a row. <c>built_seq</c> is the value read in step
    /// 1, 0 for a server that had no row, and <c>late_seq</c> is not touched: a row a writer created or bumped after step 1
    /// therefore stays stale.
    /// </summary>
    public const string UpsertBuiltSql = @"
INSERT INTO collect.query_store_compose_stamp_built AS b (server_id, hour, late_seq, built_seq, built_at, source_rows, stamp_rows)
SELECT x.server_id, $1, 0, COALESCE(l.late_seq, 0), $2, COALESCE(r.source_rows, 0), COALESCE(r.stamp_rows, 0)
FROM
(
    SELECT u.server_id FROM unnest($3::integer[]) AS u (server_id)
    UNION
    SELECT s.server_id
    FROM collect.query_store_compose_stamp AS s
    WHERE s.collection_time >= $1
      AND s.collection_time < $1 + interval '1 hour'
) AS x
LEFT JOIN unnest($3::integer[], $4::bigint[]) AS l (server_id, late_seq) ON l.server_id = x.server_id
LEFT JOIN
(
    SELECT s.server_id, sum(s.wide_rows) AS source_rows, count(*) AS stamp_rows
    FROM collect.query_store_compose_stamp AS s
    WHERE s.collection_time >= $1
      AND s.collection_time < $1 + interval '1 hour'
    GROUP BY s.server_id
) AS r ON r.server_id = x.server_id
ON CONFLICT (server_id, hour) DO UPDATE
SET built_seq = EXCLUDED.built_seq,
    built_at = EXCLUDED.built_at,
    source_rows = EXCLUDED.source_rows,
    stamp_rows = EXCLUDED.stamp_rows;";

    /// <summary>Records the hour: $1 the hour, $2 built_at.</summary>
    public const string UpsertHourSql = @"
INSERT INTO collect.query_store_compose_stamp_hours (hour, built_at)
VALUES ($1, $2)
ON CONFLICT (hour) DO UPDATE SET built_at = EXCLUDED.built_at;";

    /// <summary>
    /// The hours to build, as (hour, stale): $1 the floor (<see cref="FloorHour"/>), $2 the cap PER KIND, so at most that many
    /// stale hours and at most that many never-built hours; <see cref="ChooseBuilds"/> splits one tick's builds between the two.
    /// First the stale hours (an hour the builder has done that has a pair with <c>built_seq IS DISTINCT FROM late_seq</c>),
    /// newest first; then the hours the builder has never done that are at least <see cref="BuildLag"/> old, newest first. Neither goes
    /// below the floor or below the hour of the earliest wide-table coverage claim (<c>filled_since</c>) of an enabled
    /// server, since the rollup cannot speak for earlier hours. A pair of an hour that is not in the hours table is not
    /// stale: it was created by a late writer before the builder reached the hour, and the hour's first build covers it.
    ///
    /// <para>"Now" is the STORE's clock (<c>now()</c>), not the service's: the trigger's <c>WHEN</c> reads the store's clock too, so
    /// the trigger and this plan share one clock and the skew between the service and the store drops out of the margin. A service
    /// clock that ran ahead used to shrink the margin by the skew (#5582 review round 1).</para>
    /// </summary>
    public const string PlanSql = @"
WITH bounds AS
(
    SELECT greatest($1::timestamp, date_trunc('hour', min(c.filled_since))) AS floor_hour
    FROM collect.query_store_interval_wide_coverage AS c
    JOIN collect.servers AS sv ON sv.server_id = c.server_id
    WHERE sv.is_enabled
    HAVING min(c.filled_since) IS NOT NULL
),
stale AS
(
    SELECT DISTINCT b.hour, true AS stale
    FROM collect.query_store_compose_stamp_built AS b
    JOIN collect.query_store_compose_stamp_hours AS h ON h.hour = b.hour
    CROSS JOIN bounds
    WHERE b.built_seq IS DISTINCT FROM b.late_seq
      AND b.hour >= bounds.floor_hour
),
missing AS
(
    SELECT g AS hour, false AS stale
    FROM bounds
    CROSS JOIN LATERAL generate_series(bounds.floor_hour, date_trunc('hour', (now() AT TIME ZONE 'UTC') - interval '3 hours'), interval '1 hour') AS g
    WHERE NOT EXISTS (SELECT 1 FROM collect.query_store_compose_stamp_hours AS h WHERE h.hour = g)
)
SELECT p.hour, p.stale
FROM
(
    SELECT u.hour, u.stale, row_number() OVER (PARTITION BY u.stale ORDER BY u.hour DESC) AS rn
    FROM (SELECT * FROM stale UNION ALL SELECT * FROM missing) AS u
) AS p
WHERE p.rn <= $2
ORDER BY p.stale DESC, p.hour DESC;";

    /// <summary>
    /// The first step of the cleanup: removes the hours below the floor from the hours table and returns them: $1 the floor.
    /// The second step removes each returned hour's rollup rows by the (collection_time, server_id) index
    /// (<see cref="GcRollupSql"/>), so the cleanup is driven from the small tables (the #5507 shape, as
    /// <c>PlanRegressionDaily</c> and <c>QueryStoreTopDaily</c>): a delete on the rollup with a bare
    /// <c>collection_time &lt; floor</c> predicate would read an index range even when nothing is due.
    /// </summary>
    public const string GcHoursSql = @"
DELETE FROM collect.query_store_compose_stamp_hours
WHERE hour < $1
RETURNING hour;";

    /// <summary>Removes the validity rows below the floor: $1 the floor. The table is small (a pair per server and hour).</summary>
    public const string GcBuiltSql = @"
DELETE FROM collect.query_store_compose_stamp_built
WHERE hour < $1;";

    /// <summary>Removes one expired hour of rollup rows: $1 the hour.</summary>
    public const string GcRollupSql = DeleteHourSql;

    /// <summary>One planned build.</summary>
    public readonly record struct Build(DateTime Hour, bool Stale);

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int BuiltStale, int BuiltMissing, int Failed, long HoursRemoved, int Deferred = 0, int DeferredForWriters = 0)
    {
        public int Built => BuiltStale + BuiltMissing;
    }

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>Whole hours since 1970-01-01 of <paramref name="hour"/>: the same number the trigger puts in its key and the
    /// build uses for its advisory lock.</summary>
    public static int HourNumber(DateTime hour) =>
        checked((int)((DateTime.SpecifyKind(hour, DateTimeKind.Utc) - DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerHour));

    /// <summary>
    /// The earliest hour the rollup holds at <paramref name="nowUtc"/>: the retention horizon of the wide table (which purges
    /// on <c>first_execution_time</c>) moved forward by <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>, rounded up to
    /// the hour. A row collected from that hour on started after the horizon, so retention has not touched any row of
    /// the hour, and the hour's rollup is whole. Hours below it are not built and are removed by <see cref="GcAsync"/>.
    /// </summary>
    public static DateTime FloorHour(DateTime nowUtc, int retentionDays)
    {
        var edge = Unspecified(nowUtc).AddDays(-retentionDays) + QueryStoreIntervalWide.PurgeEdgeMargin;
        var hour = new DateTime(edge.Year, edge.Month, edge.Day, edge.Hour, 0, 0, DateTimeKind.Unspecified);
        return hour < edge ? hour.AddHours(1) : hour;
    }

    /// <summary>
    /// One tick's builds out of the planned candidates (<see cref="PlanSql"/>): stale hours first, newest first, then never-built hours
    /// newest first, at most <paramref name="max"/> in all. While never-built hours are waiting, stale hours take at most half of the
    /// tick's builds, so a storm of late writes (a backfill, a pending replay) that keeps marking built hours stale cannot starve the
    /// never-built backlog (a new store's first ~190 hours); with none waiting, stale hours may take every build. The reserved
    /// half goes to the never-built hours and no further (#5582 review round 1).
    /// </summary>
    public static IReadOnlyList<Build> ChooseBuilds(IReadOnlyList<Build> candidates, int max)
    {
        ArgumentNullException.ThrowIfNull(candidates);

        var stale = candidates.Where(b => b.Stale).OrderByDescending(b => b.Hour).ToList();
        var missing = candidates.Where(b => !b.Stale).OrderByDescending(b => b.Hour).ToList();
        var staleCap = missing.Count > 0 ? max / 2 : max;
        var chosen = stale.Take(staleCap).ToList();
        chosen.AddRange(missing.Take(Math.Max(0, max - chosen.Count)));
        return chosen;
    }

    /// <summary>The hours to build at <paramref name="nowUtc"/>: <see cref="ChooseBuilds"/> over <see cref="PlanSql"/>, at most <see cref="MaxBuildsPerTick"/>.</summary>
    public static async Task<IReadOnlyList<Build>> PlanBuildsAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        /* nowUtc only sets the retention floor; the build-lag bound is the store's clock, in the SQL. */
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = FloorHour(nowUtc, retentionDays) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetDateTime(0), reader.GetBoolean(1)));
        }

        return ChooseBuilds(builds, MaxBuildsPerTick);
    }

    /// <summary>
    /// Removes the hours below <see cref="FloorHour"/> from all three tables in ONE transaction and returns how many hours
    /// went. Step one deletes the hours and the validity rows (small tables) and returns the hours; step two removes each
    /// returned hour's rollup rows by the index. A rollup row is always written in the same transaction as its hour's row,
    /// so no rollup row is left without one.
    /// </summary>
    public static async Task<long> GcAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var floor = FloorHour(nowUtc, retentionDays);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var expired = new List<DateTime>();
        await using (var hours = new NpgsqlCommand(GcHoursSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            hours.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
            await using var reader = await hours.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                expired.Add(reader.GetDateTime(0));
            }
        }

        await using (var built = new NpgsqlCommand(GcBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
            await built.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var hour in expired)
        {
            await using var rollup = new NpgsqlCommand(GcRollupSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            rollup.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hour });
            await rollup.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return expired.Count;
    }

    /// <summary>
    /// Builds one hour in one transaction (the five steps in the type's summary). Returns the wide rows read, or null when the hour
    /// was DEFERRED because a writing transaction that could hold an unmarked row of it is still open (<see cref="OpenWriterSql"/>);
    /// a deferred hour changes nothing and is planned again next tick. An hour with no wide rows still gets its row in the hours table.
    /// </summary>
    public static async Task<long?> BuildHourAsync(
        NpgsqlConnection connection, DateTime hour, DateTime nowUtc, CancellationToken cancellationToken, Func<Task>? beforeCommit = null)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var hourValue = Unspecified(hour);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var advisory = new NpgsqlCommand(HourLockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            advisory.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = HourNumber(hourValue) });
            await advisory.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var writers = new NpgsqlCommand(OpenWriterSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            writers.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hourValue });
            if (Convert.ToBoolean(await writers.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture))
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        var servers = new List<int>();
        var lateSeqs = new List<long>();
        await using (var readPairs = new NpgsqlCommand(ReadPairsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            readPairs.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hourValue });
            await using var reader = await readPairs.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add(reader.GetInt32(0));
                lateSeqs.Add(reader.GetInt64(1));
            }
        }

        await using (var delete = new NpgsqlCommand(DeleteHourSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hourValue });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var insert = new NpgsqlCommand(BuildHourInsertSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hourValue });
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        long sourceRows;
        await using (var count = new NpgsqlCommand(SourceRowsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            count.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hourValue });
            sourceRows = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken));
        }

        await using (var built = new NpgsqlCommand(UpsertBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hourValue });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = servers.ToArray() });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = lateSeqs.ToArray() });
            await built.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var hours = new NpgsqlCommand(UpsertHourSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            hours.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hourValue });
            hours.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            await hours.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Test seam: runs with the build's row locks still held and nothing committed (a late write that bumps a pair the build has
           already upserted waits here for the commit). */
        if (beforeCommit is not null)
        {
            await beforeCommit();
        }

        await transaction.CommitAsync(cancellationToken);
        return sourceRows;
    }

    /// <summary>
    /// One maintenance tick: remove the expired hours, plan, then build each hour in its own transaction. A build that fails is
    /// logged (naming the hour) and the loop goes on; the next tick plans it again. Once <see cref="MaxTickDuration"/> has passed
    /// no further build starts, and the planned hours left are reported as deferred.
    /// </summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, retentionDays, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, int, ILogger, CancellationToken)"/> with the elapsed time
    /// since the tick began supplied by <paramref name="elapsed"/>, read before each build starts.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int builtStale = 0, builtMissing = 0, failed = 0, started = 0, deferredForWriters = 0;
        DateTime? firstDeferredHour = null;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var removed = await GcAsync(connection, nowUtc, retentionDays, cancellationToken);
        var plan = await PlanBuildsAsync(connection, nowUtc, retentionDays, cancellationToken);

        foreach (var item in plan)
        {
            if (elapsed() >= MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                if (await BuildHourAsync(connection, item.Hour, nowUtc, cancellationToken) is null)
                {
                    deferredForWriters++;
                    firstDeferredHour ??= item.Hour;
                    continue;
                }

                if (item.Stale)
                {
                    builtStale++;
                }
                else
                {
                    builtMissing++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "Query Store compose rollup: building hour {Hour:yyyy-MM-dd HH}:00 (stale {Stale}) failed and is retried next tick: {Message}",
                    item.Hour, item.Stale, ex.Message);
            }
        }

        if (deferredForWriters > 0)
        {
            /* Once per tick, however many hours: the cause is one open writing transaction, and the next tick retries. */
            logger.LogInformation("Query Store compose rollup: deferred {Count} hour(s), the first {Hour:yyyy-MM-dd HH}:00, because a writing transaction that began before the hour's trigger margin ended is still open; retried next tick",
                deferredForWriters, firstDeferredHour);
        }

        var deferred = plan.Count - started + deferredForWriters;
        var result = new TickResult(builtStale, builtMissing, failed, removed, deferred, deferredForWriters);
        logger.LogInformation("Query Store compose rollup: built {Built} hour(s) (stale {Stale}, missing {Missing}), failed {Failed}, deferred {Deferred}, removed {Removed} hour(s)",
            result.Built, builtStale, builtMissing, failed, deferred, removed);
        return result;
    }
}
