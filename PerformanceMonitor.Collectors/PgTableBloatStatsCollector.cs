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
/// Per-table bloat — the DAMAGE, next to the cause chain we already collect (#2542).
/// <para><see cref="PgAutovacuumStatsCollector"/>, <see cref="PgWraparoundStatsCollector"/> and
/// <see cref="PgXminHorizonCollector"/> between them collect the entire CAUSE chain for bloat and nothing
/// that measures what it cost. This is that measurement — or rather, deliberately, an ESTIMATE of it, which
/// is the substance of the collector and is argued below rather than assumed.</para>
///
/// <para><b>The measurement decision.</b> Three options, and the cheap one wins on a measured margin rather
/// than on reasoning:</para>
/// <list type="bullet">
/// <item><b><c>pgstattuple</c></b> gives exact live/dead/free byte counts and is the canonical answer. It
/// reads the WHOLE relation. Measured on a 54 MB table entirely in shared buffers: 11.8 ms warm — about
/// 4.6 GB/s, i.e. bounded by memory bandwidth in the best case and by storage bandwidth in the normal one.
/// A 1 TB table therefore costs minutes, per table, per cycle, even fully cached. It is also an extension,
/// so it is absent unless somebody installed it. Not viable on a cadence; the collector records whether it
/// is installed so the read can name it as the escalation for one table.</item>
/// <item><b><c>pgstattuple_approx</c></b> skips all-visible pages using the visibility map — 0.58 ms on the
/// same table. That number is its BEST case and is not representative: the pages it must still read are
/// exactly the recently-written ones, so on the churning table you actually care about the visibility map
/// is cold and it degrades toward the full scan. Still an extension, so it shares the availability problem
/// without solving the cost one.</item>
/// <item><b>The statistics-based estimate</b> — what ships. It reads <c>pg_class</c> and <c>pg_stats</c>
/// only and never touches the relation, so its cost is independent of table SIZE and scales with table
/// COUNT: measured at 44 ms for a 2,001-table / 854 MB database, against 860 ms for <c>pgstattuple</c> over
/// the same tables. That 19x is a FLOOR on the ratio, not a typical value, because those tables were tiny
/// and fully cached — the gap widens with every gigabyte that is not.</item>
/// </list>
///
/// <para><b>How wrong the estimate can be, measured rather than hedged.</b> Against <c>pgstattuple</c> on
/// the same tables, with current statistics, the error was under 2.1 percentage points on every table
/// tested (churn-bloated 75.14 vs 74.82, delete-bloated 60.73 vs 59.52, clean 0.58 vs 0.50). That is the
/// good case, and it is not the case that matters. The failure modes, each reproduced:</para>
/// <list type="number">
/// <item><b>Stale column-width statistics — the dangerous one.</b> Two byte-identical 8,998-page tables
/// with identical true bloat of 10.93%: the one whose <c>pg_stats.avg_width</c> predated a widening UPDATE
/// estimated <b>92.64%</b>, the one analyzed afterwards estimated <b>11.01%</b>. 81.7 percentage points, on
/// a difference invisible in the arithmetic. <c>mods_since_analyze</c> is carried for exactly this: the
/// stale table showed 300,000 modifications against 150,000 live rows, the fresh one showed 0, and width
/// statistics can only go stale THROUGH modifications, which is what makes the ratio a sound proxy rather
/// than a coincidence.</item>
/// <item><b>A never-analyzed table</b> has no <c>pg_stats</c> rows at all and estimated 92.68% against a
/// true 48.86%. Caught by <c>estimate_unavailable</c>.</item>
/// <item><b>A non-default <c>fillfactor</c> is a definitional disagreement, not an error.</b> A
/// fillfactor-70 table estimated 3.79% where <c>pgstattuple</c> reported 32.52%, because
/// <c>pgstattuple</c>'s <c>free_space</c> counts the space fillfactor RESERVES ON PURPOSE for HOT updates.
/// The estimate is the more useful of the two readings there; <c>fillfactor</c> is carried so a read can
/// say which question it answered.</item>
/// </list>
///
/// <para><b>The permissions trap, which would have shipped a tool that tells everyone to
/// <c>VACUUM FULL</c> everything.</b> <c>pg_stats</c> is filtered by <c>has_column_privilege(...'select')</c>,
/// and <c>pg_monitor</c> — the one grant the PostgreSQL runbook says Darling needs — does NOT confer SELECT
/// on user tables. Measured against a <c>pg_monitor</c>-only role on a live target: <b>zero</b>
/// <c>pg_stats</c> rows visible, and the estimator did not fail. It returned a confident 88.59% for a table
/// whose true bloat is 0.50%, 95.03% for one that is really 74.82%, and 22.57% for one that is really
/// 0.46% — every row an argument for rewriting a production table. Adding <c>pg_read_all_data</c>
/// (PostgreSQL 14+; absent on 13, where explicit <c>GRANT SELECT</c> is the only route — both verified)
/// restored byte-identical agreement with the superuser's numbers.
/// <b>So <c>estimate_unavailable</c> is not advisory.</b> It is TRUE in exactly that state, and the read
/// must suppress the number rather than caption it — a footnote under a large red percentage is not a
/// safeguard.</para>
///
/// <para><b>What is always available, whatever the grants.</b> <c>live_tuples</c> and <c>dead_tuples</c>
/// come from the server's own counters and need no width model, so a dead-tuple fraction is computable from
/// this row even when the estimate is suppressed. It is a different and narrower quantity — it counts dead
/// tuples, not the free space a completed vacuum left behind and did not return to the OS — but it is the
/// server's own number rather than our arithmetic, and it is what autovacuum itself acts on. The row
/// carries both so the read can fall back to the honest narrow measure instead of to nothing.</para>
///
/// <para><b>Heap only; TOAST reported beside it, not folded in.</b> The classic estimator adds the TOAST
/// relation's pages to the total and models its tuple count as <c>toasttuples / 4</c>, which is a guess
/// standing next to an arithmetic estimate, and <c>pg_class.relpages</c> on a TOAST relation is 0 until
/// that relation is itself vacuumed — so the fold-in is both crude and frequently zero. <c>toast_bytes</c>
/// is instead a MEASURED <c>pg_relation_size</c> beside the heap estimate. Keeping the bloat arithmetic to
/// the heap is also what makes the accuracy figures above meaningful: <c>pgstattuple</c> on a relation
/// measures its heap and not its TOAST, so the comparison is like for like.</para>
///
/// <para>Runs once per database and hourly, matching <see cref="PgAutovacuumStatsCollector"/> deliberately
/// rather than by copying: the point of this collector is to be read ALONGSIDE the autovacuum lag that
/// caused the bloat, and correlating a cause with its damage requires a common grain. Two fan-outs on the
/// same cadence also share one connection budget decision instead of splitting it.</para>
/// </summary>
public sealed class PgTableBloatStatsCollector : PostgresCollectorDefinitionBase<PgTableBloatStatsCollector.Row>
{
    public static PgTableBloatStatsCollector Instance { get; } = new();

    private PgTableBloatStatsCollector()
    {
    }

    /// <summary>
    /// Tables at or above this heap size are collected. Bloat is only ever actionable as an absolute amount
    /// of space or an amount of scan work, and under a megabyte neither is: a 90%-bloated 500 KB table is a
    /// finding worth 450 KB. The floor is the fan-out cost control — measured on a 2,001-table database it
    /// removed 2,000 of them — and it is stated in BYTES from <c>pg_relation_size</c> rather than in
    /// <c>relpages</c>, because <c>relpages</c> is only refreshed by VACUUM or ANALYZE and would let a table
    /// that has grown since its last maintenance fall through the filter it most needs to pass.
    /// </summary>
    internal const long MinimumHeapBytes = 1048576;

    public readonly record struct Row(
        string SchemaName,
        string TableName,
        long HeapBytes,
        long HeapPages,
        long ToastBytes,
        long IndexBytes,
        long LiveTuples,
        long DeadTuples,
        long ModsSinceAnalyze,
        DateTime? LastAnalyzed,
        double EstimatedTupleBytes,
        long EstimatedHeapPages,
        int FillFactor,
        long BloatBytesEstimate,
        decimal BloatPctEstimate,
        bool EstimateUnavailable,
        int AlignmentBytes,
        bool PgstattupleAvailable);

    /* VERSION FLOORS: none, and that is a measured result rather than an omission. Every catalog column
       this query reads was confirmed present by listing the live catalogs on PostgreSQL 13, 14, 15, 16, 17
       and 18, and the whole query was then executed on all six and returned the same shape and the same
       numbers for the same fixture (3.8-15.9 ms). So AppliesTo carries no version gate.

       Checked present on 13-18: pg_class.reltuples/relpages/reltoastrelid/relkind/relnamespace/reloptions,
       pg_namespace.nspname, pg_attribute.attnum/attisdropped/atttypid, pg_stats.schemaname/tablename/
       attname/avg_width/null_frac/inherited, pg_stat_user_tables.n_dead_tup/n_mod_since_analyze/
       last_analyze/last_autoanalyze, pg_relation_size(), pg_indexes_size(), current_setting('block_size').

       reltuples is -1, not 0, on a table never analyzed since PG14 - the deliberate "unknown" distinct from
       "empty". GREATEST(...,0) keeps that from producing a negative page estimate, and the -1 is ALSO fed
       into estimate_unavailable so the unknown stays visible instead of being quietly floored to zero.

       MAXALIGN cannot be read from a GUC and is detected from version(), the way every published bloat
       estimator does it. The alternation carries aarch64|arm64 explicitly: Graviton RDS and Aurora
       instances are aarch64, and while their version() string does also contain "64-bit", relying on that
       one token to hold across every vendor's build is a bet with no upside. The value actually used is
       stored in alignment_bytes rather than kept in the query, so a platform where the detection is wrong
       is visible in the data instead of silently skewing every estimate on it.

       last_analyze / last_autoanalyze are `timestamp with time zone`; AT TIME ZONE 'UTC' rather than
       ::timestamp, because the cast renders in the SESSION's TimeZone and the store contract is naive UTC.

       pg_relation_size is evaluated in the inner query so it is computed once per table rather than
       re-evaluated by the size filter; the outer WHERE reads the computed column. */
    internal static readonly string QueryText = @"
SELECT
    s.schema_name,
    s.table_name,
    s.heap_bytes,
    s.heap_pages,
    s.toast_bytes,
    s.index_bytes,
    s.live_tuples,
    s.dead_tuples,
    s.mods_since_analyze,
    s.last_analyzed,
    s.estimated_tuple_bytes,
    s.estimated_heap_pages,
    s.fillfactor,
    CASE WHEN s.heap_pages > s.estimated_heap_pages
         THEN ((s.heap_pages - s.estimated_heap_pages) * s.block_size)::bigint
         ELSE 0::bigint END                                              AS bloat_bytes_estimate,
    CASE WHEN s.heap_pages > 0 AND s.heap_pages > s.estimated_heap_pages
         THEN round(100 * (s.heap_pages - s.estimated_heap_pages) / s.heap_pages::numeric, 2)
         ELSE 0::numeric END                                             AS bloat_pct_estimate,
    /* A FOURTH way the estimate has no basis, and it has to be OR-ed in HERE rather than in the inner
       query because that is where estimated_heap_pages first exists. When every column of a table has a
       zero avg_width the tuple size computes to 0, the NULLIF guards fire, estimated_heap_pages comes back
       NULL, and the two CASE expressions above resolve to a confident 0%% bloat - an estimate presented
       as a measurement, which is the one thing this collector must never do. Probably unreachable behind
       the 1 MB size floor, but probably-unreachable is not the standard the flag is held to. */
    (s.estimate_unavailable OR s.estimated_heap_pages IS NULL)      AS estimate_unavailable,
    s.alignment_bytes,
    s.pgstattuple_available
FROM
(
    SELECT
        a.schema_name,
        a.table_name,
        a.heap_bytes,
        a.heap_pages,
        a.toast_bytes,
        a.index_bytes,
        a.live_tuples,
        a.dead_tuples,
        a.mods_since_analyze,
        a.last_analyzed,
        a.fillfactor,
        a.block_size,
        a.alignment_bytes,
        a.estimate_unavailable,
        a.pgstattuple_available,
        a.estimated_tuple_bytes,
        /* Expected pages if every live tuple were packed to the table's own fillfactor. NULLIF guards a
           zero tuple size, which a table whose columns all have zero avg_width really can produce - and
           a division by zero would fail the entire collection for the whole database, not just this row. */
        ceil(
            GREATEST(a.live_tuples, 0)::numeric
            / NULLIF((a.block_size - 24) * a.fillfactor
                     / NULLIF(a.estimated_tuple_bytes * 100, 0), 0)
        )::bigint                                                        AS estimated_heap_pages
    FROM
    (
        SELECT
            b.schema_name,
            b.table_name,
            b.heap_bytes,
            b.heap_pages,
            b.toast_bytes,
            b.index_bytes,
            b.live_tuples,
            b.dead_tuples,
            b.mods_since_analyze,
            b.last_analyzed,
            b.fillfactor,
            b.block_size,
            b.alignment_bytes,
            b.estimate_unavailable,
            b.pgstattuple_available,
            /* The tuple the server would write: header, then the data, each padded up to MAXALIGN, plus
               the 4-byte item pointer in the page's line pointer array. */
            ( 4 + b.tuple_header_bytes + b.tuple_data_bytes + (2 * b.alignment_bytes)
              - CASE WHEN b.tuple_header_bytes % b.alignment_bytes = 0
                     THEN b.alignment_bytes ELSE b.tuple_header_bytes % b.alignment_bytes END
              - CASE WHEN ceil(b.tuple_data_bytes)::int % b.alignment_bytes = 0
                     THEN b.alignment_bytes ELSE ceil(b.tuple_data_bytes)::int % b.alignment_bytes END
            )::numeric                                                   AS estimated_tuple_bytes
        FROM
        (
            SELECT
                ns.nspname                                               AS schema_name,
                tbl.relname                                              AS table_name,
                pg_relation_size(tbl.oid)::bigint                        AS heap_bytes,
                tbl.relpages::bigint                                     AS heap_pages,
                coalesce(pg_relation_size(tbl.reltoastrelid), 0)::bigint AS toast_bytes,
                pg_indexes_size(tbl.oid)::bigint                         AS index_bytes,
                MAX(tbl.reltuples)::bigint                               AS live_tuples,
                coalesce(MAX(st.n_dead_tup), 0)::bigint                  AS dead_tuples,
                coalesce(MAX(st.n_mod_since_analyze), 0)::bigint         AS mods_since_analyze,
                /* GREATEST ignores NULLs, which is what is wanted here rather than a hazard: whichever of
                   the manual and the automatic analyze actually happened is the later non-NULL one, and a
                   table that has only ever been autoanalyzed must not report ""never analyzed"". */
                (GREATEST(MAX(st.last_analyze), MAX(st.last_autoanalyze)) AT TIME ZONE 'UTC') AS last_analyzed,
                coalesce(substring(array_to_string(tbl.reloptions, ' ')
                                   FROM 'fillfactor=([0-9]+)')::smallint, 100)::int AS fillfactor,
                current_setting('block_size')::int                       AS block_size,
                CASE WHEN version() ~ '64-bit|x86_64|ppc64|ia64|amd64|aarch64|arm64'
                     THEN 8 ELSE 4 END                                   AS alignment_bytes,
                (23 + CASE WHEN MAX(coalesce(sts.null_frac, 0)) > 0
                           THEN (7 + count(sts.attname)) / 8 ELSE 0 END)::int AS tuple_header_bytes,
                sum((1 - coalesce(sts.null_frac, 0)) * coalesce(sts.avg_width, 0))::numeric AS tuple_data_bytes,
                /* Three separate ways the estimate has no basis, collapsed into one flag the read must
                   honour: a name-typed column (pg_stats reports its width as the padded 64 bytes rather
                   than the stored text), any column with no pg_stats row at all (never analyzed, or - far
                   more likely in production - the login lacks SELECT and pg_stats filtered every row out),
                   and reltuples < 0, PG14's explicit ""never analyzed"" marker. */
                (   bool_or(att.atttypid = 'pg_catalog.name'::regtype)
                 OR count(sts.attname) <> count(att.attname)
                 OR MAX(tbl.reltuples) < 0 )                             AS estimate_unavailable,
                /* A fourth condition - estimated_heap_pages IS NULL - is OR-ed into this column in the
                   OUTERMOST select, where that value first exists. */
                EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
                        WHERE e.extname = 'pgstattuple')                 AS pgstattuple_available
            FROM pg_catalog.pg_attribute AS att
            JOIN pg_catalog.pg_class AS tbl
              ON tbl.oid = att.attrelid
            JOIN pg_catalog.pg_namespace AS ns
              ON ns.oid = tbl.relnamespace
            LEFT JOIN pg_catalog.pg_stat_user_tables AS st
              ON st.relid = tbl.oid
            LEFT JOIN pg_catalog.pg_stats AS sts
              ON  sts.schemaname = ns.nspname
              AND sts.tablename = tbl.relname
              AND sts.inherited = false
              AND sts.attname = att.attname
            WHERE att.attnum > 0
              AND NOT att.attisdropped
              AND tbl.relkind IN ('r', 'm')
              AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
              AND ns.nspname !~ '^pg_toast'
              AND ns.nspname !~ '^pg_temp'
            GROUP BY ns.nspname, tbl.oid, tbl.relname, tbl.relpages, tbl.reltoastrelid, tbl.reloptions
        ) AS b
    ) AS a
) AS s
WHERE s.heap_bytes >= " + MinimumHeapBytes + @"
ORDER BY (CASE WHEN s.heap_pages > s.estimated_heap_pages
               THEN (s.heap_pages - s.estimated_heap_pages) * s.block_size
               ELSE 0 END) DESC";

    public override string Name => "pg_table_bloat_stats";

    /// <summary>
    /// <c>pg_table_bloat_stats</c> shadows no <c>pg_catalog</c> object — checked rather than assumed,
    /// because <c>pg_catalog</c> is searched implicitly and FIRST and a store table named after a catalog
    /// object breaks <c>CREATE INDEX</c> with 42809 and makes unqualified reads resolve to the MONITORING
    /// store's own copy. <c>CollectorTableCatalogShadowTests</c> asserts it against a live catalog.
    /// </summary>
    public override string TargetTable => "pg_table_bloat_stats";

    /// <summary>
    /// Writers only, for two independent reasons either of which would be sufficient.
    /// <para><b>The trust signals are zero on a standby.</b> <c>pg_stat_user_tables</c> reports the
    /// STANDBY's counters, and they are not the writer's — measured on Aurora 17.7 for
    /// <see cref="PgAutovacuumStatsCollector"/>, same cluster and same tables, writer 13.6M dead tuples
    /// against reader 0. The bloat ARITHMETIC would still be right on a replica, because it reads
    /// replicated catalog rows; what would be wrong is <c>mods_since_analyze</c> and <c>last_analyzed</c>,
    /// which would both read as "statistics are perfectly fresh" on a replica that has never analyzed
    /// anything. Given the stale-statistics failure documented above is the one that produces an 81-point
    /// error, silently zeroing the signal that detects it is the worst possible way to be wrong here.</para>
    /// <para><b>And it would be duplicate data anyway.</b> Bloat is a property of the shared heap. The
    /// writer's answer IS the replica's answer, so collecting both stores the same measurement twice under
    /// two server ids and invites a reader to average them.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsInRecovery;

    /// <summary>
    /// <c>pg_stats</c> and <c>pg_stat_user_tables</c> are scoped to the connected database and PostgreSQL
    /// has no cross-database read, so this is necessarily a fan-out — sharing
    /// <see cref="PgAutovacuumStatsCollector"/>'s hourly cadence so cause and damage line up sample for
    /// sample.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* Not read from the result set: the per-database loop sets CurrentDatabaseName and the connection's
           database IS the row's database. Same reasoning as PgAutovacuumStatsCollector. */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),

        /* MEASURED sizes, all three - pg_relation_size and pg_indexes_size ask the filesystem. These are
           the columns a reader can trust unconditionally, and they are deliberately adjacent to the
           estimated ones so a read can show what is known beside what is inferred. */
        new CollectorColumn("heap_bytes", CollectorColumnType.BigInt),
        /* pg_class.relpages, the catalog's page count, which is what the estimate is computed AGAINST. It
           is only refreshed by VACUUM or ANALYZE, so heap_bytes and heap_pages * block_size disagreeing is
           itself a signal that the catalog is behind - a second, independent staleness check that costs
           nothing because both columns are already here. */
        new CollectorColumn("heap_pages", CollectorColumnType.BigInt),
        /* Beside the estimate, never folded into it - see the class remarks. A table whose heap is clean
           and whose TOAST is enormous is a real situation and one this collector must not hide. */
        new CollectorColumn("toast_bytes", CollectorColumnType.BigInt),
        /* What a rewrite would also have to rebuild. VACUUM FULL and pg_repack rebuild every index on the
           table, so the index footprint is part of the cost of the remedy, not background information. */
        new CollectorColumn("index_bytes", CollectorColumnType.BigInt),

        /* The counter-based pair, which needs NO width model and so survives the permissions failure that
           suppresses the estimate. dead_tuples is the narrow honest measure to fall back to; live_tuples
           is its denominator and is also the estimate's input, so a reader can see the number the
           arithmetic was fed. -1 in live_tuples is PG14+'s "never analyzed", preserved rather than floored. */
        new CollectorColumn("live_tuples", CollectorColumnType.BigInt),
        new CollectorColumn("dead_tuples", CollectorColumnType.BigInt),
        /* The trust signal for the estimate, and the reason the 81-percentage-point failure is detectable:
           width statistics can only go stale THROUGH modifications, and this counts them. Read as a ratio
           against live_tuples, not as an absolute. */
        new CollectorColumn("mods_since_analyze", CollectorColumnType.BigInt),
        /* NULL means never analyzed by either route, which is a stronger statement than a stale timestamp
           and must not be rendered as "unknown". */
        new CollectorColumn("last_analyzed", CollectorColumnType.Timestamp),

        /* The estimator's own inputs, stored rather than discarded, so the number can be argued with
           instead of only believed. estimated_tuple_bytes against heap_bytes/live_tuples is the whole
           estimate in two numbers, and it is what a human checks first when the answer looks wrong. */
        new CollectorColumn("estimated_tuple_bytes", CollectorColumnType.Double),
        new CollectorColumn("estimated_heap_pages", CollectorColumnType.BigInt),
        /* Not decoration: a non-default fillfactor is why the estimate and pgstattuple legitimately
           disagree (3.79% against 32.52% on a fillfactor-70 table), because the reserved space is doing
           its job rather than being wasted. A read that cannot see this cannot explain the difference. */
        new CollectorColumn("fillfactor", CollectorColumnType.Integer),

        /* THE ESTIMATE. Named *_estimate in the payload, not in the read's prose, so the qualifier cannot
           be lost between the store and the screen. */
        new CollectorColumn("bloat_bytes_estimate", CollectorColumnType.BigInt),
        new CollectorColumn("bloat_pct_estimate", CollectorColumnType.Decimal, 5, 2),
        /* Load-bearing, not advisory. TRUE means the estimate has no basis - most often because the login
           lacks SELECT on the table and pg_stats returned nothing, the state in which the estimator
           confidently reported 88.59% for a table that is really 0.50% bloated. The read must SUPPRESS the
           number when this is set, not caption it. */
        new CollectorColumn("estimate_unavailable", CollectorColumnType.Boolean),
        /* The MAXALIGN the estimate actually used, stored so a platform where the version() detection is
           wrong shows up in the data rather than skewing every row silently. */
        new CollectorColumn("alignment_bytes", CollectorColumnType.Integer),
        /* Whether the exact answer is reachable on this database at all, so the read can name the
           escalation accurately instead of suggesting a command that will fail with "function does not
           exist". Constant per database, carried per row for the same reason stats_reset is in
           pg_index_usage_stats: it makes the row self-contained rather than dependent on a join. */
        new CollectorColumn("pgstattuple_available", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                SchemaName: reader.GetString(0),
                TableName: reader.GetString(1),
                HeapBytes: Long(reader, 2),
                HeapPages: Long(reader, 3),
                ToastBytes: Long(reader, 4),
                IndexBytes: Long(reader, 5),
                /* -1 rather than 0 when absent: PG14+ reports reltuples = -1 for a table that has never
                   been analyzed, and that "unknown" is exactly what estimate_unavailable keys on. Folding
                   it to 0 would claim the table is empty. */
                LiveTuples: reader.IsDBNull(6) ? -1 : reader.GetInt64(6),
                DeadTuples: Long(reader, 7),
                ModsSinceAnalyze: Long(reader, 8),
                LastAnalyzed: reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                EstimatedTupleBytes: reader.IsDBNull(10) ? 0d : Convert.ToDouble(reader.GetValue(10)),
                /* estimated_heap_pages is NULL when the NULLIF guards fired - a zero tuple size. That is
                   "could not be computed", and -1 says so without claiming the table should occupy no
                   pages, which is what 0 would mean here. estimate_unavailable is already true in every
                   case that produces it. */
                EstimatedHeapPages: reader.IsDBNull(11) ? -1 : reader.GetInt64(11),
                FillFactor: reader.IsDBNull(12) ? 100 : reader.GetInt32(12),
                BloatBytesEstimate: Long(reader, 13),
                BloatPctEstimate: reader.IsDBNull(14) ? 0m : reader.GetDecimal(14),
                EstimateUnavailable: !reader.IsDBNull(15) && reader.GetBoolean(15),
                AlignmentBytes: reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                PgstattupleAvailable: !reader.IsDBNull(17) && reader.GetBoolean(17)));
        }

        return rows;

        static long Long(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? 0 : r.GetInt64(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Every column here is a LEVEL - how big the table is now, how much of it the estimate
           thinks is waste now. The interesting reading over time is the trend across stored samples, which
           the read computes from the raw levels; differencing at collection time would throw away the
           absolute number, which is the one somebody acts on. */
        writer
            .Value(context.CurrentDatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.HeapBytes)
            .Value(row.HeapPages)
            .Value(row.ToastBytes)
            .Value(row.IndexBytes)
            .Value(row.LiveTuples)
            .Value(row.DeadTuples)
            .Value(row.ModsSinceAnalyze)
            .Value(row.LastAnalyzed)
            .Value(row.EstimatedTupleBytes)
            .Value(row.EstimatedHeapPages)
            .Value(row.FillFactor)
            .Value(row.BloatBytesEstimate)
            .Value(row.BloatPctEstimate)
            .Value(row.EstimateUnavailable)
            .Value(row.AlignmentBytes)
            .Value(row.PgstattupleAvailable);
    }
}
