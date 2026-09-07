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
/// B-tree index bloat, MEASURED rather than estimated — <c>pgstatindex</c> from pgstattuple (#2561).
///
/// <para><b>Why exact and not an estimator.</b> #2561 proposed porting the ioguix btree estimator, which
/// derives an expected page count from <c>pg_stats</c> column widths. Measured, that route does not work for
/// the role this product runs as: <c>pg_stats</c> returns ZERO rows to a <c>pg_monitor</c>-only login,
/// because the view filters on <c>has_column_privilege</c> — the same trap that produced #2542's 88.59%
/// reported against a true 0.50%. Meanwhile <c>pgstatindex</c> DOES run for <c>pg_monitor</c> (verified),
/// because pgstattuple grants EXECUTE to <c>pg_stat_scan_tables</c>, which <c>pg_monitor</c> includes. So
/// under exactly the permissions we have, the exact function works and the estimator is blind.</para>
///
/// <para><b>The density is stored raw and NEVER converted to a bloat percentage.</b>
/// <c>100 - avg_leaf_density</c> is about 10% on a PERFECT index: measured across seven freshly-built
/// indexes, density landed between 89.98 and 91.48, and after <c>REINDEX</c> between 87.07 and 90.81. There
/// is no constant to subtract, so any stored "bloat %" would bake in a false floor that varies per index.
/// The server's own numbers are kept and the interpretation is left to the read, which is the same rule the
/// plan-readiness collector follows for GUC text.</para>
///
/// <para><b>Non-btree indexes must be excluded before the function is called, not filtered afterwards.</b>
/// <c>pgstatindex</c> RAISES on anything else — verified on GIN, BRIN and hash, all
/// <c>relation "x" is not a btree index</c> — so a single GIN index would take the whole collection down
/// every cycle. The btree filter sits behind an <c>OFFSET 0</c> optimisation fence: in testing the planner
/// applied a plain <c>WHERE</c> first and the naive form worked, but correctness here should not depend on
/// plan shape when the failure is total.</para>
///
/// <para><b>Every bound on work selects what the function is applied TO.</b> Same category as the btree
/// filter above, and the reason the whole query is shaped the way it is. A bound written as a qual on a
/// <c>LEFT JOIN LATERAL</c> to the function does not bound anything: the planner cannot skip an inner side
/// it has not evaluated, so the function runs once per candidate row and the qual only chooses whether to
/// keep the answer. The result is a statement that reads the entire instance while every
/// <c>skipped_reason</c> in its output is correct, which is indistinguishable from a working collector in
/// anything except whether it ever returns. So the size ceiling and both work budgets select the
/// <c>in_budget</c> relation, and the measurements are joined back afterwards.</para>
///
/// <para><b>Nothing is silently skipped.</b> The cost of this function is a full read of the index —
/// measured at exactly <c>relpages</c> blocks, 62,840 for a 491 MB index — so very large indexes are passed
/// over rather than read daily. They still get a ROW, with the measurements NULL and
/// <c>skipped_reason</c> populated, because a size cap that made indexes disappear would read as "no bloat
/// here" on precisely the biggest ones.</para>
///
/// <para>Primaries only. A standby's index files are byte-identical to the primary's by replication, so
/// measuring both spends the same full-index read twice for one answer.</para>
/// </summary>
public sealed class PgIndexBloatCollector : PostgresCollectorDefinitionBase<PgIndexBloatCollector.Row>
{
    public static PgIndexBloatCollector Instance { get; } = new();

    private PgIndexBloatCollector()
    {
    }

    /// <summary>
    /// Indexes at or above this many bytes are recorded but not measured. The read is proportional to
    /// index size, so this bounds what any single index can cost.
    ///
    /// <para>It moves in lockstep with <see cref="CycleMeasureBudgetBytes"/>, because the cycle budget may
    /// never sit below it: the band between the two would be indexes that are legitimate candidates,
    /// earn no "too large" reason, and yet exceed the whole cycle on their own first row every run.
    /// Lowering one without the other manufactures exactly that band.</para>
    /// </summary>
    public const long MeasureCeilingBytes = 2L * 1024 * 1024 * 1024;

    /// <summary>
    /// PostgreSQL's block size. <c>pgstatindex</c> is charged per BLOCK rather than per byte, so this is
    /// the unit the budget's cost argument is actually stated in.
    ///
    /// <para>Compile-time on the server (<c>BLCKSZ</c>) and 8 KB on every build this runs against,
    /// including Aurora. A server built with a different value would make the arithmetic below optimistic
    /// by that ratio, which is one more reason the rate it is multiplied by is a pessimistic one.</para>
    /// </summary>
    public const int BlockSizeBytes = 8192;

    /// <summary>
    /// The block rate <see cref="CycleMeasureBudgetBytes"/> is sized against, and the reason the budget is
    /// a small number rather than a large one.
    ///
    /// <para><b>Why a block rate and not a byte throughput.</b> <c>pgstatindex</c> walks the index one
    /// block at a time through the buffer manager with no prefetch, so its cost is a count of
    /// potentially-synchronous single-block reads rather than a bulk transfer. On network-attached storage
    /// each miss is a round trip, so per-block LATENCY sets the rate and a sequential-throughput figure
    /// overstates it by orders of magnitude.</para>
    ///
    /// <para>This value is an assumption, not a measurement — no SUCCESS row has ever supplied a duration
    /// for this collector — so it is deliberately pessimistic, and
    /// <c>TheCycleBudget_FitsTheDeadline_AtThePessimisticBlockRate</c> pins the budget, the deadline and
    /// this rate together so that raising any one of them has to argue against the other two.</para>
    /// </summary>
    public const int PessimisticBlocksPerSecond = 2_000;

    /// <summary>
    /// How many bytes of index ONE ATTEMPT will measure in total, largest first. Independent of
    /// <see cref="MeasureCeilingBytes"/> in what it measures: that one bounds a single index, this one
    /// bounds the statement.
    ///
    /// <para><b>Per ATTEMPT, which on this collector is per DATABASE.</b>
    /// <see cref="RunsPerDatabase"/> is true and each database gets its own statement under its own
    /// command deadline, so this is the bound the deadline is actually compared against. A sweep over N
    /// databases can therefore measure up to N times this figure in total, spread across N statements.
    /// The deadline is per statement, so that is the right granularity for the bound, but it is not a
    /// bound on a sweep.</para>
    ///
    /// <para><b>Why 2 GB.</b> Not from a throughput measurement — none exists for this collector, and a
    /// deadline-cut run bounds only its own duration, never a rate. It is derived instead from the cost
    /// model in <see cref="PessimisticBlocksPerSecond"/>: 2 GB is 262,144 blocks, which at 2,000
    /// blocks/s is about 131 seconds, inside half of
    /// <see cref="CommandTimeoutSecondsOverride"/>. The remaining headroom pays for the catalog scan,
    /// connection setup, and the tail index admitted while the running total was still just under
    /// budget.</para>
    ///
    /// <para><b>What would justify raising it.</b> A SUCCESS row's own
    /// <c>collection_log.sql_duration_ms</c> (#2997), which is a measured rate for this instance and
    /// this index population rather than the assumed one above. Until such a row exists, every value
    /// here is an argument rather than a measurement, and the small end of the argument is the one that
    /// produces the row.</para>
    ///
    /// <para><b>It must never drop BELOW <see cref="MeasureCeilingBytes"/>, and that it equals it is a
    /// floor rather than a coincidence.</b> A cycle budget under the per-index ceiling opens a band
    /// between them in which an index can never be measured at all: it is under the ceiling, so it is a
    /// legitimate candidate and gets no "too large" reason, yet it alone exceeds the whole cycle, so it
    /// is over budget on its own first row every single run. It would be labelled
    /// <c>not measured this cycle</c> forever, which is precisely the "deferred" reading that this
    /// collector refuses to let stand for "never". Equal is the tightest value with no such band, and
    /// <c>TheCycleBudget_IsNeverBelowThePerIndexCeiling</c> pins it.</para>
    ///
    /// <para><b>Accepted consequence.</b> An index at or above the ceiling is reported at its size with
    /// the ceiling's own reason and is never measured — on a large target that includes the biggest and
    /// most reclaimable indexes on the instance. That is a real gap, and it is stated in the row rather
    /// than hidden: the alternative on offer is not "measure them" but "measure nothing", which is what
    /// an unbounded statement delivers. Coverage of the tail additionally depends on the measured set
    /// rotating, which it does not do.</para>
    /// </summary>
    public const long CycleMeasureBudgetBytes = 2L * 1024 * 1024 * 1024;

    /// <param name="AvgLeafDensity">The server's own figure, 0–100. NOT a bloat percentage — a healthy
    /// index sits near 90, so subtracting from 100 invents roughly 10 points of bloat that is not there.</param>
    /// <param name="LeafFragmentation">Share of leaf pages out of physical order. Zero on a freshly built
    /// index; near 50 on the churned one measured while designing this.</param>
    /// <param name="EmptyPages">Pages holding nothing. Directly reclaimable by <c>REINDEX</c> and the most
    /// concrete number here.</param>
    /// <param name="DeletedPages">Pages marked deleted and awaiting reuse.</param>
    /// <param name="SkippedReason">Null when measured. Populated when the index was too large to read, so a
    /// cap can never masquerade as an absence of bloat.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? IndexName,
        long IndexBytes,
        int? TreeLevel,
        long? InternalPages,
        long? LeafPages,
        long? EmptyPages,
        long? DeletedPages,
        double? AvgLeafDensity,
        double? LeafFragmentation,
        string? SkippedReason);

    /* The candidate set is fenced with OFFSET 0 so the btree filter is applied BEFORE pgstatindex is
       called on anything. See the type header for why that matters more than usual here.

       Every gate that bounds WORK selects the function's INPUT relation (in_budget), and the function's
       output is joined back onto the full candidate set afterwards. A gate written as a qual on a LEFT
       JOIN LATERAL instead reads identically and bounds nothing: the planner has no way to skip an inner
       side it has not evaluated, so the function runs for every candidate row and the qual only decides
       whether to keep the answer. Measured on PostgreSQL 17.11 with a call-counting stand-in and ten
       candidates of which one was in budget: the ON-clause form reported `Rows Removed by Join Filter: 9`
       over a `Function Scan ... loops=10`, the fenced form `loops=1`, and the two returned byte-identical
       rows. Correct labels over an unbounded read is the shape that produces them.

       So skipped indexes reappear through the outer LEFT JOIN to `measured` rather than through a LEFT
       JOIN LATERAL on the function itself. An index that is not measured must still be RETURNED carrying
       its skipped_reason - dropping it would read as an index that does not exist - and this way it is
       returned without being read.

       System schemas are excluded - their indexes are not something an operator reindexes on our advice.

       pgstatindex is qualified public., NOT pg_catalog. - it is an EXTENSION function and lives wherever
       pgstattuple was created, which is public by convention. Qualifying it pg_catalog. simply does not
       resolve (verified: "function pg_catalog.pgstatindex(oid) does not exist"), and leaving it unqualified
       would let an object in an earlier search_path schema shadow it. An install that put pgstattuple
       somewhere else fails into ObjectMissing, which is the honest outcome and is exactly what
       pg_extension_availability (#2545) reports on. Same convention as PgBufferUsageCollector's
       public.pg_buffercache.

       The oid is cast to regclass because that is the parameter type; an unqualified oid does not match. */
    private const string QueryText = @"
WITH candidates AS (
    SELECT
        n.nspname                       AS schema_name,
        t.relname                       AS table_name,
        c.relname                       AS index_name,
        c.oid                           AS index_oid,
        pg_catalog.pg_relation_size(c.oid) AS index_bytes
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_am AS am
      ON am.oid = c.relam
    JOIN pg_catalog.pg_index AS x
      ON x.indexrelid = c.oid
    JOIN pg_catalog.pg_class AS t
      ON t.oid = x.indrelid
    JOIN pg_catalog.pg_namespace AS n
      ON n.oid = c.relnamespace
    WHERE c.relkind = 'i'
    AND   am.amname = 'btree'
    AND   x.indisvalid
    AND   x.indisready
    AND   n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    OFFSET 0
),
/* THE ACCOUNTING for the work budget (#2617), and only the accounting - in_budget below is what
   enforces it. pgstatindex reads every page it is pointed at, so an unbounded statement reads the
   whole instance: measured on a live Aurora target, 1,517 indexes totalling 461 GB in a single
   statement, which never finished and dropped the connection mid-read.

   Ranked by size and measured largest-first, because bloat that matters is concentrated in big
   indexes - a small index at 40% density is worth kilobytes. Everything past the budget is still
   RETURNED, with a reason, so the read never mistakes unmeasured for healthy.

   TWO figures, and the BYTE one is what bounds the work (#2997). A count bounds pages only where
   count correlates with bytes; on the first production target it did not, and the 200 largest
   sub-ceiling indexes there admitted 286 GB. The count survives because it is legible - an operator
   can predict the biggest N in a way they cannot predict a byte figure - not because it bounds
   anything: see CycleMeasureBudgetBytes for which of the two is load-bearing. */
ranked AS (
    SELECT
        k.*,
        row_number() OVER (ORDER BY k.index_bytes DESC, k.index_name) AS size_rank,
        /* The running total of what will actually be READ, so the cycle can stop at a byte figure
           rather than a row count. FILTERed to sub-ceiling indexes because an over-ceiling one is
           never handed to pgstatindex and therefore costs no pages; charging it to the budget would
           spend the whole allowance on indexes nobody reads. Since the over-ceiling indexes sort
           first and can total more than the budget between them, the unfiltered form would exhaust
           the allowance before the first measurable index and measure nothing at all.

           coalesce because a FILTERed window sum is NULL until its frame contains a matching row,
           and the over-ceiling indexes sort FIRST under size DESC. NULL <= budget is NULL rather
           than false, so the raw form would leave the gate neither open nor closed. */
        coalesce(
            sum(k.index_bytes) FILTER (WHERE k.index_bytes < " + CeilingLiteral + @")
                OVER (ORDER BY k.index_bytes DESC, k.index_name
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            0)::bigint                      AS measured_bytes_through_here
    FROM candidates AS k
),
/* THE GATE, and the only place the three bounds are enforced. Selecting the relation pgstatindex is
   applied to is what makes them bounds; a qual on the function's join makes them labels. Fenced with
   OFFSET 0 on the same argument as the candidate set - when the failure mode is that the collector
   returns nothing at all, the bound should not rest on the planner choosing to push a filter down. */
in_budget AS (
    SELECT
        k.index_oid                     AS index_oid
    FROM ranked AS k
    WHERE k.index_bytes < " + CeilingLiteral + @"
    AND   k.size_rank  <= " + BudgetLiteral + @"
    AND   k.measured_bytes_through_here <= " + CycleByteBudgetLiteral + @"
    OFFSET 0
),
/* CROSS JOIN LATERAL here, because every row of in_budget is by construction one this statement has
   decided to read. Nothing is dropped by the cross join: an index absent from in_budget still reaches
   the result through the outer LEFT JOIN below, with its measurements NULL and a reason. */
measured AS (
    SELECT
        b.index_oid                     AS index_oid,
        s.tree_level                    AS tree_level,
        s.internal_pages                AS internal_pages,
        s.leaf_pages                    AS leaf_pages,
        s.empty_pages                   AS empty_pages,
        s.deleted_pages                 AS deleted_pages,
        s.avg_leaf_density              AS avg_leaf_density,
        s.leaf_fragmentation            AS leaf_fragmentation
    FROM in_budget AS b
    CROSS JOIN LATERAL public.pgstatindex(b.index_oid::regclass) AS s
)
SELECT
    current_database()::text            AS database_name,
    k.schema_name::text                 AS schema_name,
    k.table_name::text                  AS table_name,
    k.index_name::text                  AS index_name,
    k.index_bytes::bigint               AS index_bytes,
    m.tree_level                        AS tree_level,
    m.internal_pages::bigint            AS internal_pages,
    m.leaf_pages::bigint                AS leaf_pages,
    m.empty_pages::bigint               AS empty_pages,
    m.deleted_pages::bigint             AS deleted_pages,
    m.avg_leaf_density::double precision   AS avg_leaf_density,
    m.leaf_fragmentation::double precision AS leaf_fragmentation,
    CASE
        WHEN k.index_bytes >= " + CeilingLiteral + @"
            THEN 'index is larger than the measurement ceiling; pgstatindex reads every page, so it is '
                 || 'recorded but not measured'
        /* The BYTE budget is reported ahead of the count one because it is the bound that actually
           binds on a large-index target, and an index past both is past this one first. */
        WHEN k.measured_bytes_through_here > " + CycleByteBudgetLiteral + @"
            THEN 'not measured this cycle (work budget): pgstatindex reads every page, so a run '
                 || 'stops once it has measured ' || pg_catalog.pg_size_pretty(" + CycleByteBudgetLiteral + @"::bigint)
                 || ' of index, largest first. This one is recorded at its size so it is never '
                 || 'mistaken for healthy.'
        WHEN k.size_rank > " + BudgetLiteral + @"
            THEN 'not measured this cycle (work budget): pgstatindex reads every page, so only the '
                 || 'largest ' || " + BudgetLiteral + @" || ' indexes are measured per run. This one is '
                 || 'recorded at its size so it is never mistaken for healthy.'
    END::text                           AS skipped_reason
FROM ranked AS k
/* Plain LEFT JOIN on the already-computed measurements, NOT a join to the function. Every candidate
   appears exactly once; the ones in_budget excluded arrive here with their measurement columns NULL,
   which is what the skipped_reason arms above describe. */
LEFT JOIN measured AS m
  ON m.index_oid = k.index_oid
ORDER BY k.index_bytes DESC";

    private const string CeilingLiteral = "2147483648";

    /* The upper bound on how many indexes one attempt will MEASURE, largest first: 200 indexes OR
       CycleMeasureBudgetBytes, whichever comes first. The byte figure is the one that binds on any
       target large enough to matter; see the ranked CTE for why the count is kept anyway. */
    private const string BudgetLiteral = "200";

    /* Kept in the C# type system as well as in the SQL literal so a reader has one authoritative
       figure and TheBudgetLiterals_AgreeWithTheirConstants can pin that the two agree. */
    private const string CycleByteBudgetLiteral = "2147483648";

    /// <summary>
    /// Five minutes, because even a bounded cycle of pages is real work and a slow single index
    /// should yield a CLASSIFIED timeout rather than a dropped connection. index_object_stats takes
    /// the same override for the same reason (#1135).
    ///
    /// <para>This is a BACKSTOP, not the bound. The deadline cannot make an over-large statement
    /// finish - it only decides how long the sweep waits before giving up, and a statement that does
    /// not fit spends the whole five minutes and then reports nothing. What bounds the work is
    /// <see cref="CycleMeasureBudgetBytes"/>, which is sized to fit inside HALF of this figure; a run
    /// that needs the rest of the deadline has already been mis-budgeted.</para>
    ///
    /// <para>Npgsql's CommandTimeout is a socket READ timeout that every backend message restarts,
    /// so it bounds backend SILENCE rather than total elapsed time. <c>pgstatindex</c> sends nothing
    /// while it scans, so it gets no reprieve from that and the deadline does fire - which is why
    /// the failures arrive as <c>Exception while reading from stream</c>, the transport's own words
    /// for a read that ran out of time.</para>
    /// </summary>
    public override int? CommandTimeoutSecondsOverride => 300;

    public override string Name => "pg_index_bloat";

    public override string TargetTable => "pg_index_bloat";

    /// <summary>
    /// Primaries only. A standby's index files are byte-identical to the primary's, so the answer is the
    /// same and the cost — a full read of every index — would be paid twice. Matches
    /// <see cref="PgIndexUsageStatsCollector"/>'s gate, which the two share a cadence with.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsInRecovery;

    /// <summary>Per-database: indexes and their catalogs are per-database.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* This collector runs once per database and pgstatindex measures the connected database only, so
           without this the same index name in two databases is one indistinguishable row (#2599). */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_bytes", CollectorColumnType.BigInt),
        /* Tree depth. A level that climbs on an index whose row count did not is a shape worth seeing, and
           it costs nothing to keep since the function already returned it. */
        new CollectorColumn("tree_level", CollectorColumnType.Integer),
        new CollectorColumn("internal_pages", CollectorColumnType.BigInt),
        new CollectorColumn("leaf_pages", CollectorColumnType.BigInt),
        /* The two concrete reclaimable numbers, and the only ones here that need no interpretation. */
        new CollectorColumn("empty_pages", CollectorColumnType.BigInt),
        new CollectorColumn("deleted_pages", CollectorColumnType.BigInt),
        /* Stored RAW. See the type header: a healthy index reads near 90, so this is not 100-minus-bloat. */
        new CollectorColumn("avg_leaf_density", CollectorColumnType.Double),
        new CollectorColumn("leaf_fragmentation", CollectorColumnType.Double),
        new CollectorColumn("skipped_reason", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                IndexName: reader.IsDBNull(3) ? null : reader.GetString(3),
                IndexBytes: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                /* Every measurement is nullable because a skipped index has none of them, and NULL is the
                   honest representation of "not measured" where 0 would read as a measured emptiness. */
                TreeLevel: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                InternalPages: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                LeafPages: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                EmptyPages: reader.IsDBNull(8) ? null : reader.GetInt64(8),
                DeletedPages: reader.IsDBNull(9) ? null : reader.GetInt64(9),
                AvgLeafDensity: reader.IsDBNull(10) ? null : reader.GetDouble(10),
                LeafFragmentation: reader.IsDBNull(11) ? null : reader.GetDouble(11),
                SkippedReason: reader.IsDBNull(12) ? null : reader.GetString(12)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Bloat is a level, and the history is what distinguishes an index that has been bloated
           since it was built from one that got that way this month. */
        writer
            .Value(row.DatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.IndexName)
            .Value(row.IndexBytes)
            .Value(row.TreeLevel)
            .Value(row.InternalPages)
            .Value(row.LeafPages)
            .Value(row.EmptyPages)
            .Value(row.DeletedPages)
            .Value(row.AvgLeafDensity)
            .Value(row.LeafFragmentation)
            .Value(row.SkippedReason);
    }
}
