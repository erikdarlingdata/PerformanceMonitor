/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What is resident in shared buffers, by relation — <c>pg_buffercache</c> (#2544, the buffers slice).
///
/// <para>Answers the question every other cache metric only implies: a hit ratio says how often the pool
/// worked, and this says WHAT IS IN IT — whether the working set fits, and which relation is occupying the
/// memory that something else needs.</para>
///
/// <para><b>Requires the <c>pg_buffercache</c> extension</b>, so on a server without it the query fails with
/// 42P01, which <c>PostgresTargetProvider</c> already maps to
/// <see cref="CollectorTargetFault.ObjectMissing"/> and the worker records as a non-fatal
/// EXTENSION_MISSING skip naming the extension (#3240) — the declaration below is what routes it there.
/// That is the correct behaviour rather than a workaround: the extension cannot be referenced
/// conditionally in plain SQL (the parse fails before any guard could run), and the miss is now actionable —
/// <c>pg_extension_availability</c> (#2545) reports whether the extension is <c>available</c> and one
/// <c>CREATE EXTENSION</c> away. Hourly cadence keeps that outcome from being per-minute noise on a fleet
/// that mostly lacks it.</para>
///
/// <para><b>Three correctness traps, all measured rather than reasoned:</b></para>
///
/// <list type="number">
/// <item><b><c>relfilenode</c> is NOT <c>oid</c>.</b> The join every published example writes,
/// <c>pg_class.oid = pg_buffercache.relfilenode</c>, silently loses every table that has ever been rewritten
/// — <c>VACUUM FULL</c>, <c>CLUSTER</c>, <c>TRUNCATE</c>. Measured: after one <c>VACUUM FULL</c> the naive
/// join reported <b>0</b> buffers for a table that actually held <b>6,667</b>. <c>pg_relation_filenode(oid)</c>
/// is the correct key.</item>
/// <item><b>Mapped catalogs carry <c>relfilenode = 0</c></b> in <c>pg_class</c> — measured on
/// <c>pg_class</c> and <c>pg_proc</c> — so joining the raw column drops them too.
/// <c>pg_relation_filenode()</c> resolves those correctly as well, which is why it is used rather than
/// <c>coalesce</c> over the column.</item>
/// <item><b>The pool is CLUSTER-wide and <c>pg_class</c> is per-database</b> — the fourth catalog in this
/// effort whose scope does not match its name. Worse here than elsewhere: a relfilenode from another
/// database can collide with a local OID and resolve to the WRONG NAME. Measured — buffers belonging to
/// another database resolved to plausible local catalog names. So the name is resolved ONLY when the buffer
/// belongs to the connected database, and buffers from elsewhere keep their database name with a NULL
/// relation.</item>
/// </list>
///
/// <para>Readable by <c>pg_monitor</c>. The full view is a scan of every buffer, at about 1 microsecond per
/// buffer as measured on managed targets (57 ms for 32,768 buffers, 543 ms for 507,904), which scales
/// linearly: about 9 s for a 64 GB pool, plus a temp-file spill for the function scan's own store of the
/// pool (#9508). That is affordable hourly on a small pool and not on a large one, so the per-relation
/// read is skipped above <see cref="PoolCapBuffers"/> and the pool totals come from
/// <c>pg_buffercache_summary()</c> instead, which has no per-buffer row set.</para>
///
/// <para><b>Three shapes of run (#9508), decided on every run from the pool size in <c>pg_settings</c>:</b>
/// at or under the cap, the per-relation rows as always; over the cap, no <c>pg_buffercache</c> scan and one
/// totals-only row (<c>buffers</c> NULL, no relation columns) that every reader tells apart from a relation
/// row by that NULL; and the pool totals from <c>pg_buffercache_summary()</c> whenever <c>pg_proc</c> shows
/// the function on this connection, from a window over the scan otherwise. A target whose extension was
/// created before PostgreSQL 16 and never updated has no summary function: under the cap it keeps today's
/// read, over the cap the totals-only row has the pool size and no used count, and the reader states the
/// precondition <c>ALTER EXTENSION pg_buffercache UPDATE</c>. The collector never runs that statement.</para>
/// </summary>
public sealed class PgBufferUsageCollector : PostgresCollectorDefinitionBase<PgBufferUsageCollector.Row>
{
    public static PgBufferUsageCollector Instance { get; } = new();

    private PgBufferUsageCollector()
    {
    }

    /// <summary>
    /// The pool size, in buffers, above which the per-relation scan is skipped (#9508): 16 GB of 8 kB buffers. The scan
    /// costs about 1 microsecond per buffer on the monitored server, so a pool this size is about 2 s every hour, and
    /// it grows without bound with <c>shared_buffers</c> (a 64 GB pool is about 9 s, plus a temp spill). The decision
    /// is made on each run from <c>pg_settings</c>, so a pool that crosses the cap in either direction (a serverless
    /// target moves <c>shared_buffers</c> with capacity) switches on the next run.
    /// </summary>
    public const long PoolCapBuffers = 2_097_152;

    /// <summary>The <c>pg_buffercache</c> function that returns the pool totals without a per-buffer row set (extension 1.4, PostgreSQL 16).</summary>
    public const string SummaryFunction = "pg_buffercache_summary";

    /// <summary>Measurement labels (counts, never verdicts) a skipped per-relation scan leaves on the run's <c>collection_log</c> note.</summary>
    public const string ScanSkippedMeasurement = "relation_scan_skipped";
    public const string PoolBuffersMeasurement = "pool_buffers";
    public const string CapBuffersMeasurement = "cap_buffers";
    public const string SummaryMissingMeasurement = "summary_missing";

    /// <param name="RelationName">NULL when the buffer belongs to a different database — deliberately, see
    /// the type header. NOT an unnamed relation.</param>
    /// <param name="AvgUsageCount">Mean clock-sweep usage count, 0–5. Buffers sitting at 5 are being reused
    /// constantly; a relation whose buffers are all at 0 or 1 is occupying memory it is not earning.</param>
    /// <param name="Buffers">NULL only on the totals-only row of a run that skipped the per-relation scan
    /// (#9508). A relation row always has a count.</param>
    /// <param name="DirtyBuffers">On a totals-only row, the dirty buffers of the whole pool (NULL when the
    /// summary function is missing).</param>
    /// <param name="PoolBuffersTotal">Total buffers in the pool, repeated on every row so a caller can
    /// compute a share without a second read.</param>
    /// <param name="PoolBuffersUsed">NULL only on a totals-only row whose pool used count is unknown because
    /// the target has no <c>pg_buffercache_summary()</c>.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? RelationName,
        string? RelationKind,
        long? Buffers,
        long? DirtyBuffers,
        double? AvgUsageCount,
        long PoolBuffersTotal,
        long? PoolBuffersUsed)
    {
        /// <summary>True for the single row of a run that skipped the per-relation scan.</summary>
        public bool IsTotalsOnly => Buffers is null;
    }

    /* The filenode join is pg_relation_filenode(c.oid), never c.oid or c.relfilenode - see the type header
       for what each of those loses.

       The relation join is additionally gated on the buffer belonging to THIS database, because a filenode
       from another database can collide with a local OID and produce a confidently wrong name. Buffers from
       elsewhere are kept with their database named and the relation NULL: dropping them would understate
       how full the pool is, which is the one number this exists to report.

       Without pg_buffercache_summary(), the pool totals come from a window over the same single scan rather
       than a second query: two scans of a moving pool would disagree, and the disagreement would land in the
       percentage a reader computes. With it (#9508, SummaryCtes) the totals are a separate reading of the
       summary function, one row with no per-buffer row set, and the rows and the totals each describe the
       pool as of their own moment.

       #9062: the buffers are grouped by (reldatabase, relfilenode) FIRST, and the window runs over those few
       hundred grouped rows. The window used to run over every buffer row, and a 10 GB pool is 1.3 million of
       them, which the WindowAgg stored (86 MB of temp file per read, measured on a real target) before the
       GROUP BY reduced them to about 100. Everything below the grouping is a sum of the sums: buffers and
       dirty buffers add, and the average usage count is the usage sum over the count of buffers that HAVE a
       usage count, which is what avg() ignores NULLs to do (NULLIF keeps a group with none NULL, as avg() does,
       not a division error). The pool totals are sums of the grouped counts, so unused buffers (a NULL
       relfilenode) are in the total and out of the used count exactly as before. The remaining spill is the
       function scan's own store of the pool, which still spilled at 64 MB of work_mem; that is what the cap
       (#9508) is for.

       #9508: the scan sits under a WHERE that reads only the pool size (cfg, from pg_settings, where shared_buffers is
       already a count of block_size buffers, so the setting IS the buffer count on any build). A qual with no
       column of the scanned relation is planned as a one-time filter above the scan, so over the cap the
       pg_buffercache function is never called. The cap is a literal in the text, decided on each run.

       A floor of 8 buffers (64 KB) keeps the long tail of one-block catalog relations out; on a large pool
       that tail is thousands of rows that never inform anything. */
    private const string QueryText = @"
WITH cfg AS (
    SELECT s.setting::bigint AS pool_buffers
    FROM pg_catalog.pg_settings AS s
    WHERE s.name = 'shared_buffers'
),
@@SUMMARY@@per_rel AS (
    SELECT
        b.reldatabase,
        b.relfilenode,
        count(*)                                                AS buffers,
        count(*) FILTER (WHERE b.isdirty)                       AS dirty_buffers,
        sum(b.usagecount)                                       AS usage_sum,
        count(b.usagecount)                                     AS usage_n
    FROM public.pg_buffercache AS b
    WHERE (SELECT c.pool_buffers FROM cfg AS c) <= @@CAP@@
    GROUP BY b.reldatabase, b.relfilenode
),
pool AS (
    SELECT
        r.reldatabase,
        r.relfilenode,
        r.buffers,
        r.dirty_buffers,
        r.usage_sum,
        r.usage_n,
@@POOLTOTALS@@
    FROM per_rel AS r@@POOLJOIN@@
)
SELECT
    NULL::text                                                  AS database_name,
    NULL::text                                                  AS relation_name,
    NULL::text                                                  AS relation_kind,
    NULL::bigint                                                AS buffers,
@@TOTALSROW@@
FROM cfg AS g@@TOTALSJOIN@@
WHERE g.pool_buffers > @@CAP@@
UNION ALL
SELECT
    d.datname::text                                             AS database_name,
    c.relname::text                                             AS relation_name,
    c.relkind::text                                             AS relation_kind,
    sum(p.buffers)::bigint                                      AS buffers,
    sum(p.dirty_buffers)::bigint                                AS dirty_buffers,
    (sum(p.usage_sum) / nullif(sum(p.usage_n), 0))::double precision AS avg_usage_count,
    max(p.pool_total)::bigint                                   AS pool_buffers_total,
    max(p.pool_used)::bigint                                    AS pool_buffers_used
FROM pool AS p
LEFT JOIN pg_catalog.pg_database AS d
  ON d.oid = p.reldatabase
LEFT JOIN pg_catalog.pg_class AS c
  ON  p.relfilenode = pg_catalog.pg_relation_filenode(c.oid)
  AND p.reldatabase = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
WHERE p.relfilenode IS NOT NULL
GROUP BY d.datname, c.relname, c.relkind
HAVING sum(p.buffers) >= 8
ORDER BY buffers DESC NULLS LAST";

    /* #9508: the pieces that differ when pg_buffercache_summary() is there. summ is one row of the pool's own
       counters; the totals come from it and not from a window over the scan. */
    private const string SummaryCtes = @"summ AS (
    SELECT
        m.buffers_used::bigint                                  AS used,
        m.buffers_unused::bigint                                AS unused,
        m.buffers_dirty::bigint                                 AS dirty,
        m.usagecount_avg::double precision                      AS usage_avg
    FROM @@SCHEMA@@.pg_buffercache_summary() AS m
),
";

    private const string WindowPoolTotals = @"        sum(r.buffers) OVER ()                                  AS pool_total,
        coalesce(sum(r.buffers) FILTER (WHERE r.relfilenode IS NOT NULL) OVER (), 0) AS pool_used";

    private const string SummaryPoolTotals = @"        m.used + m.unused                                       AS pool_total,
        m.used                                                  AS pool_used";

    private const string SummaryTotalsRow = @"    m.dirty                                                     AS dirty_buffers,
    m.usage_avg                                                 AS avg_usage_count,
    m.used + m.unused                                           AS pool_buffers_total,
    m.used                                                      AS pool_buffers_used";

    private const string NoSummaryTotalsRow = @"    NULL::bigint                                                AS dirty_buffers,
    NULL::double precision                                      AS avg_usage_count,
    g.pool_buffers                                              AS pool_buffers_total,
    NULL::bigint                                                AS pool_buffers_used";

    public override string Name => "pg_buffer_usage";

    public override string TargetTable => "pg_buffer_usage";

    /// <summary>
    /// Every PostgreSQL target. The extension gate cannot be expressed here — <c>AppliesTo</c> sees the
    /// target's version and hosting, not which extensions a database has created, and that state changes
    /// without a reconnect. A server without the extension fails into <c>ObjectMissing</c>, which is the
    /// vocabulary's answer for exactly this and is now paired with a panel that says how to fix it.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>
    /// A plain <c>CREATE EXTENSION</c> in the connect database, with no restart — which is what
    /// makes the <c>ObjectMissing</c> degradation above a setup step rather than a wall.
    /// </summary>
    public override IReadOnlyList<PgExtensionDependency> RequiredPgExtensions { get; } = new[]
    {
        new PgExtensionDependency("pg_buffercache", PgExtensionInstallKind.CreateExtension),
    };

    /// <summary>
    /// Two minutes, because the COST here is the input scan and no LIMIT can reduce it.
    ///
    /// <para><c>pg_buffercache</c> materialises one row per shared buffer and takes buffer partition locks
    /// while it does — on a large instance that is millions of rows, and the aggregate needs all of them,
    /// so the usual trick of capping output does nothing. The output IS bounded (grouped per relation,
    /// with an eight-buffer floor); the read is not.</para>
    ///
    /// <para>Without an override a slow scan ends as <c>Exception while reading from stream</c>, an
    /// unclassified Npgsql failure — which is exactly how #2617 presented and why it went unnoticed for a
    /// collector that had never once succeeded. A classified timeout says which collector was too slow.</para>
    /// </summary>
    public override int? CommandTimeoutSecondsOverride => 120;

    /// <summary>The cap this run applies, in buffers: the context's override when a caller set one, else <see cref="PoolCapBuffers"/>.</summary>
    internal static long CapFor(CollectorContext context) => context.PgBufferUsagePoolCapBuffers ?? PoolCapBuffers;

    /* #6143: the view named by the schema pg_buffercache was created in; the monitoring connection's search_path is
       pg_catalog, and the extension need not be in public.

       #9508: the summary function is named only when the host's catalog read found it owned by the extension on this
       connection. A query that names a missing function fails at parse, so the text is chosen here, not in SQL. */
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var hasSummary = context.PgExtensionHasFunction(SummaryFunction);
        var schema = context.PgExtensionSchema("pg_buffercache");
        var text = QueryText
            .Replace("@@SUMMARY@@", hasSummary ? SummaryCtes : string.Empty, StringComparison.Ordinal)
            .Replace("@@POOLTOTALS@@", hasSummary ? SummaryPoolTotals : WindowPoolTotals, StringComparison.Ordinal)
            .Replace("@@POOLJOIN@@", hasSummary ? "\nCROSS JOIN summ AS m" : string.Empty, StringComparison.Ordinal)
            .Replace("@@TOTALSROW@@", hasSummary ? SummaryTotalsRow : NoSummaryTotalsRow, StringComparison.Ordinal)
            .Replace("@@TOTALSJOIN@@", hasSummary ? "\nCROSS JOIN summ AS m" : string.Empty, StringComparison.Ordinal)
            .Replace("@@CAP@@", CapFor(context).ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("FROM public.pg_buffercache AS b", "FROM " + schema + ".pg_buffercache AS b", StringComparison.Ordinal)
            .Replace("@@SCHEMA@@", schema, StringComparison.Ordinal);
        return new(text);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("relation_name", CollectorColumnType.Varchar),
        new CollectorColumn("relation_kind", CollectorColumnType.Varchar),
        new CollectorColumn("buffers", CollectorColumnType.BigInt),
        /* Dirty buffers are what a checkpoint has to write. A relation holding a large dirty share is what
           makes a checkpoint expensive, which is the write-side collector's subject - the two panels are
           the same story from opposite ends. */
        new CollectorColumn("dirty_buffers", CollectorColumnType.BigInt),
        new CollectorColumn("avg_usage_count", CollectorColumnType.Double),
        /* Repeated on every row so a share is computable without a second read against a pool that has
           moved on since. */
        new CollectorColumn("pool_buffers_total", CollectorColumnType.BigInt),
        new CollectorColumn("pool_buffers_used", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                RelationName: reader.IsDBNull(1) ? null : reader.GetString(1),
                RelationKind: reader.IsDBNull(2) ? null : reader.GetString(2),
                Buffers: reader.IsDBNull(3) ? null : reader.GetInt64(3),
                DirtyBuffers: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                AvgUsageCount: reader.IsDBNull(5) ? null : reader.GetDouble(5),
                PoolBuffersTotal: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                PoolBuffersUsed: reader.IsDBNull(7) ? null : reader.GetInt64(7)));
        }

        /* #9508: a skipped scan is a note on a run that succeeded, as counts the reads word. The pool size and the cap
           travel with it, so a reader can see how far over the cap the pool was. */
        foreach (var row in rows)
        {
            if (!row.IsTotalsOnly)
            {
                continue;
            }

            context.Measure(ScanSkippedMeasurement, 1);
            context.Measure(PoolBuffersMeasurement, row.PoolBuffersTotal);
            context.Measure(CapBuffersMeasurement, CapFor(context));
            if (row.PoolBuffersUsed is null)
            {
                context.Measure(SummaryMissingMeasurement, 1);
            }
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. This is a snapshot of what is resident; the history is what shows a relation being
           evicted and re-read repeatedly, which is invisible in any single sample. */
        writer
            .Value(row.DatabaseName)
            .Value(row.RelationName)
            .Value(row.RelationKind)
            .Value(row.Buffers)
            .Value(row.DirtyBuffers)
            .Value(row.AvgUsageCount)
            .Value(row.PoolBuffersTotal)
            .Value(row.PoolBuffersUsed);
    }
}
