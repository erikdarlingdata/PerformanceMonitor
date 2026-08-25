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
/// <see cref="CollectorTargetFault.ObjectMissing"/> and the worker records as a non-fatal PERMISSIONS
/// outcome. That is the correct behaviour rather than a workaround: the extension cannot be referenced
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
/// <para>Readable by <c>pg_monitor</c>. The full view is a scan of every buffer: measured at 6.1 ms for a
/// 512 MB pool, which scales linearly, so roughly 780 ms for 64 GB of <c>shared_buffers</c> — affordable
/// hourly and not affordable per minute, which is the other reason for the cadence.</para>
/// </summary>
public sealed class PgBufferUsageCollector : PostgresCollectorDefinitionBase<PgBufferUsageCollector.Row>
{
    public static PgBufferUsageCollector Instance { get; } = new();

    private PgBufferUsageCollector()
    {
    }

    /// <param name="RelationName">NULL when the buffer belongs to a different database — deliberately, see
    /// the type header. NOT an unnamed relation.</param>
    /// <param name="AvgUsageCount">Mean clock-sweep usage count, 0–5. Buffers sitting at 5 are being reused
    /// constantly; a relation whose buffers are all at 0 or 1 is occupying memory it is not earning.</param>
    /// <param name="PoolBuffersTotal">Total buffers in the pool, repeated on every row so a caller can
    /// compute a share without a second read.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? RelationName,
        string? RelationKind,
        long Buffers,
        long DirtyBuffers,
        double? AvgUsageCount,
        long PoolBuffersTotal,
        long PoolBuffersUsed);

    /* The filenode join is pg_relation_filenode(c.oid), never c.oid or c.relfilenode - see the type header
       for what each of those loses.

       The relation join is additionally gated on the buffer belonging to THIS database, because a filenode
       from another database can collide with a local OID and produce a confidently wrong name. Buffers from
       elsewhere are kept with their database named and the relation NULL: dropping them would understate
       how full the pool is, which is the one number this exists to report.

       Pool totals come from a window over the same single scan rather than a second query or
       pg_buffercache_summary() - two scans of a moving pool would disagree, and the disagreement would land
       in the percentage a reader computes.

       A floor of 8 buffers (64 KB) keeps the long tail of one-block catalog relations out; on a large pool
       that tail is thousands of rows that never inform anything. */
    private const string QueryText = @"
WITH pool AS (
    SELECT
        b.reldatabase,
        b.relfilenode,
        b.isdirty,
        b.usagecount,
        count(*) OVER ()                                        AS pool_total,
        count(*) FILTER (WHERE b.relfilenode IS NOT NULL) OVER () AS pool_used
    FROM public.pg_buffercache AS b
)
SELECT
    d.datname::text                                             AS database_name,
    c.relname::text                                             AS relation_name,
    c.relkind::text                                             AS relation_kind,
    count(*)::bigint                                            AS buffers,
    count(*) FILTER (WHERE p.isdirty)::bigint                   AS dirty_buffers,
    avg(p.usagecount)::double precision                         AS avg_usage_count,
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
HAVING count(*) >= 8
ORDER BY count(*) DESC";

    public override string Name => "pg_buffer_usage";

    public override string TargetTable => "pg_buffer_usage";

    /// <summary>
    /// Every PostgreSQL target. The extension gate cannot be expressed here — <c>AppliesTo</c> sees the
    /// target's version and hosting, not which extensions a database has created, and that state changes
    /// without a reconnect. A server without the extension fails into <c>ObjectMissing</c>, which is the
    /// vocabulary's answer for exactly this and is now paired with a panel that says how to fix it.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

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
                Buffers: reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DirtyBuffers: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                AvgUsageCount: reader.IsDBNull(5) ? null : reader.GetDouble(5),
                PoolBuffersTotal: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                PoolBuffersUsed: reader.IsDBNull(7) ? 0 : reader.GetInt64(7)));
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
