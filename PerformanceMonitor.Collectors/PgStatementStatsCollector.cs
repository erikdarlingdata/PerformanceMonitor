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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-query-shape execution statistics for an Amazon Aurora PostgreSQL target — the Postgres
/// counterpart of <see cref="QueryStatsCollector"/>. Reads <c>aurora_stat_statements()</c>, which is
/// <c>pg_stat_statements</c> plus columns only Aurora has.
/// <para>Two things Aurora adds that are worth the dependency. It decomposes the opaque
/// <c>shared_blks_read</c> into <b>where the block actually came from</b> —
/// <c>storage_blks_read</c> (the distributed storage volume), <c>orcache_blks_hit</c> (the local NVMe
/// Optimized Reads tier), and local — which means a cache-hit ratio computed the community way is
/// arithmetically misleading on Aurora, because a "read" may have been a fast local hit. And it
/// reports <b>peak memory per statement</b>, which is the closest thing PostgreSQL has to SQL Server's
/// memory-grant data; core PostgreSQL has no grant concept at all.</para>
/// <para>#2625: it reads BOTH sources. On Aurora it reads <c>aurora_stat_statements(false)</c>; on any
/// other PostgreSQL it reads the vanilla <c>pg_stat_statements</c> view and reports the Aurora-only
/// columns as NULL. One collector, one table, chosen at query-build time — because the Aurora function
/// is <c>pg_stat_statements</c> plus columns, so the vanilla read is a strict subset rather than a
/// different measurement, and splitting it in two would have meant two tables, two readers, two panels
/// and two MCP tools for one question.</para>
/// <para>It was Aurora-only until a self-hosted PostgreSQL target existed to notice. The cost of that
/// gate was not a missing column — it was that "which queries cost the most", the question a database
/// monitor exists to answer, had NO answer at all on stock PostgreSQL, while the collectors either side
/// of it happily gathered OS CPU and predicate selectivity keyed by the very queryids it was not
/// identifying.</para>
/// <para><b>NULL is load-bearing here.</b> The Aurora-only columns are written NULL rather than zero on
/// the vanilla path: zero would say Aurora measured no storage reads for this statement, which is a
/// claim about the server, not about the source. Every consumer of those columns already tolerates NULL
/// because Aurora itself returns NULL for a statement with no such activity.</para>
/// <para><b>A row that had zero calls since the last cycle is not shipped.</b> <c>pg_stat_statements</c>
/// has no server-side WHERE for "changed since I last looked" — the query already reads the whole live
/// catalog, and at a 50-cluster fleet running the 1-minute cadence <see cref="CollectorScheduleDefaults"/>
/// gives every PostgreSQL collector, that meant re-writing every tracked statement shape every minute
/// whether or not anyone ran it: <c>pg_statement_stats</c> reached 176.9 GB of a 180 GB store within 36
/// hours of the fleet's onboarding, growing 126 GB in a single day. The fix is the same shape as the
/// SQL Server side's rank-then-render split (#1959), aimed at a different cost: there is no plan XML to
/// defer here, so the lever is ROW COUNT, not row content — most statements in any given minute are idle,
/// and an idle repeat carries no information a reader does not already have.</para>
/// <para>The delta on CALLS decides it, computed once in <see cref="ReadAsync"/> rather than
/// <c>WritePayload</c> because the write loop starts the binary-import row before <c>WritePayload</c>
/// runs (see <c>DarlingCollectorRunner</c>) — by the time a collector could refuse to write, the row
/// already exists. Skipping has to happen earlier, in the list <see cref="ReadAsync"/> returns.</para>
/// <para>The distinction the skip decision rests on is the one <see cref="CollectorDeltaCalculator"/>
/// already draws between an UNKNOWN zero and a CONFIRMED one: a first sighting and a counter reset both
/// report interval 0 alongside delta 0, and both are genuinely new information — a shape appearing for
/// the first time, or one whose plan/counters were just evicted and re-entered — so both still ship.
/// Only delta 0 paired with a REAL interval (interval &gt; 0) means the statement demonstrably ran zero
/// times since the last look, which is the only case with nothing left to say. Calls is sufficient to
/// decide this alone: <c>pg_stat_statements</c> only advances total_exec_time, rows, or any other counter
/// here when calls also advances, so a confirmed zero-calls interval means every other counter on the row
/// is unchanged too.</para>
/// <para>The other two delta series (total_exec_time, rows) are still computed for every row the query
/// returns, including ones this ultimately skips — not merely for symmetry, but because
/// <see cref="CollectorDeltaCalculator"/> refreshes a series' cached timestamp only when it is called.
/// Skipping that call during an idle stretch would leave those two series' baselines stale, so the next
/// GENUINELY active cycle could see a gap exceeding <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/>
/// against a baseline that is, in fact, still current — reporting a false counter-reset (a real accrual
/// wrongly zeroed) on a statement whose calls delta correctly showed the activity. Calling all three
/// unconditionally, and gating only the OUTPUT on the calls delta, keeps every series' baseline exactly as
/// fresh as it is today.</para>
/// <para>No TOP-N cap layers on top of this. The measured driver of the growth was idle repeats, not a
/// large ACTIVE statement population — a cluster whose entire tracked catalog turns over every cycle
/// would need one, but that is a distinct, not-yet-observed failure mode with its own tuning (mirroring
/// the SQL Server compile-churn pattern already found on the use2 fleet), and adding a cap without
/// evidence it is needed would just be another number to defend later.</para>
/// <para><b>Also the statements-epoch carrier (#3653 A5).</b> Every delta above subtracts from a baseline
/// keyed by the statement's identity, and until #3653 nothing asked whether the counters it subtracted from
/// were the same counters: <c>pg_stat_statements_reset()</c>, a crash that lost the saved statistics, or an
/// endpoint that now reaches another instance all restart the counters without changing a key. The query
/// carries <c>pg_stat_statements_info.stats_reset</c> as its last column, and <see cref="ReadAsync"/> hands it
/// to <see cref="ServerEpoch.ObserveStatements"/> off the first row, BEFORE that row's own subtraction, so an
/// epoch forgets this family's baselines (and only this family's - the epoch says nothing about any other
/// counter on the server) on the very pass that sees it. The reset branch already reported the rows whose
/// counters FELL as (0, 0); what this adds is the rows whose counters rose past a stale baseline, the
/// persisted record of when the counters restarted, and the note on the run's collection_log row.</para>
/// </summary>
public sealed class PgStatementStatsCollector : PostgresCollectorDefinitionBase<PgStatementStatsCollector.Row>
{
    public static PgStatementStatsCollector Instance { get; } = new();

    private PgStatementStatsCollector()
    {
    }

    public readonly record struct Row(
        long QueryId,
        long DatabaseId,
        long UserId,
        bool TopLevel,
        long Calls,
        double TotalExecTimeMs,
        double MinExecTimeMs,
        double MaxExecTimeMs,
        double MeanExecTimeMs,
        long RowsReturned,
        long SharedBlocksHit,
        long SharedBlocksRead,
        long SharedBlocksDirtied,
        long SharedBlocksWritten,
        long TempBlocksRead,
        long TempBlocksWritten,
        double BlockReadTimeMs,
        double BlockWriteTimeMs,
        /* Nullable from #2625 on: these six exist only in aurora_stat_statements(). NULL means "this
           source does not report it", which is not the same statement as zero. */
        long? StorageBlocksRead,
        long? OrcacheBlocksHit,
        double? StorageBlockReadTimeMs,
        double? OrcacheBlockReadTimeMs,
        long WalRecords,
        long WalFpi,
        long WalBytes,
        long? TotalExecPeakMemBytes,
        long? MaxExecPeakMemBytes,
        /* Computed in ReadAsync, not WritePayload — see the class remarks on why the delta decides
           whether this row ships at all, which means it must exist before the row is (or is not)
           added to the list WritePayload will later be called once per. */
        long DeltaCalls,
        long DeltaTotalExecTimeMs,
        long DeltaRows,
        /* #3540 (Darling V128): the measured seconds the three deltas accrued over — the minimum over the
           row's groups, so 0 means "no delta in this row is knowable". Computed beside the deltas in
           ReadAsync because that is where the calculator is called. */
        int SampleIntervalSeconds);

    /* Column names DIFFER between PostgreSQL 16 and 17 and our fleet spans both, so the query is
       built per major rather than SELECT *-ed. Verified against live 16.11 and 17.7:

         16.11 : blk_read_time,        blk_write_time
         17.7  : shared_blk_read_time, shared_blk_write_time

       PG17 also adds jit_deform_time/_count, stats_since, and minmax_stats_since, which this
       definition does not read. A SELECT * here would not error on either version — it would silently
       shift every ordinal, which is how monitoring tools shipped broken PG17 collectors.

       Explicit casts pin the reader's types: wal_bytes is numeric in PostgreSQL (bigint cannot hold
       its declared 10^20 range, though no real statement approaches it), and Npgsql's type checking
       is strict enough that reading numeric with GetInt64 throws. */
    private static string BuildQueryText(int postgresMajorVersion, bool isAurora)
    {
        var readTime = postgresMajorVersion >= 17 ? "shared_blk_read_time" : "blk_read_time";
        var writeTime = postgresMajorVersion >= 17 ? "shared_blk_write_time" : "blk_write_time";

        /* #2625, the vanilla path. The ORDINALS are identical to Aurora's on purpose: the six columns
           only Aurora reports are selected as typed NULL literals rather than omitted, so ReadAsync,
           PayloadColumns and WritePayload stay one implementation with one ordering. A shorter SELECT
           here would mean a second reader whose ordinals could drift from this one, which is the exact
           failure the per-major column naming above already documents.

           toplevel arrived in pg_stat_statements 1.9 (PostgreSQL 14). Before that every row IS a top
           level statement - nested tracking is what 1.9 added - so `true` is the correct value on an
           older server, not a fallback.

           The SERVER major is a PROXY here, and #3818 is the record of what the proxy misses: the column
           belongs to the extension's VIEW, whose shape is the extension's catalog version, not the
           engine's. A 14+ engine whose pg_stat_statements was created before the upgrade keeps the 1.8
           view until someone runs ALTER EXTENSION pg_stat_statements UPDATE, and there `toplevel` does
           not exist - measured on PostgreSQL 14.24 with the extension created at VERSION '1.8': 42703.
           A column reference is resolved at parse analysis, so no SQL on the vanilla path can read a
           column that may not be there without a probe; the proxy stays because every 14+ cluster whose
           extension was created ON 14+ has the column, and the failing case is loud (42703 is
           Unclassified -> ERROR), not silently wrong. Aurora's aurora_stat_statements() carries the
           column regardless of the extension's catalog version, which is why the Aurora flavor below
           reads it unguarded. */
        var topLevel = postgresMajorVersion >= 14 ? "toplevel" : "true";

        /* The statements epoch (#3653 A5), ordinal 27 on both flavors: StatementsEpochSql, declared at class level
           with the measurement behind its shape (#3818). */
        var statsReset = StatementsEpochSql;

        /* #4428: stats_since arrived in pg_stat_statements 1.11, bundled with PostgreSQL 17 — the moment
           THIS entry was (re-)created in the extension's hashtable, the same series-age signal #2235
           already uses for query_stats' compile_age, reported as a timestamp rather than an age. It is a
           COLUMN of the base view, exactly like toplevel above, so it takes the SAME proxy toplevel
           already takes and for the same accepted reason (#3818's remarks on toplevel): the server major
           is a stand-in for the extension's catalog version, which is usually right and loudly wrong
           (42703, unclassified) on the cluster it misses — a 17+ engine whose extension predates the
           in-place major upgrade. aurora_stat_statements() is documented to carry every pg_stat_statements
           column, stats_since included, wherever the function itself exists, so the Aurora flavor reads
           it unguarded the same way it already reads toplevel unguarded. */
        var statsSince = postgresMajorVersion >= 17 ? "stats_since" : "NULL::timestamp with time zone";

        /* #4428: the TARGET's own clock, captured in the SAME read as stats_since, once per row (cheap —
           PostgreSQL evaluates now() once per statement, not per row, so this is one clock read per pass
           either way). The restart placement below compares stats_since ONLY against this column's value
           from the PREVIOUS pass, never against the collector host's own clock — the two clocks can be
           minutes apart (#4428's skew pin), and the collector's clock is not even the same MACHINE. */
        var targetNow = "now()";

        if (!isAurora)
        {
            return $@"
SELECT
    queryid::bigint                    AS queryid,
    dbid::bigint                       AS dbid,
    userid::bigint                     AS userid,
    {topLevel}                         AS toplevel,
    calls::bigint                      AS calls,
    total_exec_time                    AS total_exec_time,
    min_exec_time                      AS min_exec_time,
    max_exec_time                      AS max_exec_time,
    mean_exec_time                     AS mean_exec_time,
    rows::bigint                       AS rows_returned,
    shared_blks_hit::bigint            AS shared_blks_hit,
    shared_blks_read::bigint           AS shared_blks_read,
    shared_blks_dirtied::bigint        AS shared_blks_dirtied,
    shared_blks_written::bigint        AS shared_blks_written,
    temp_blks_read::bigint             AS temp_blks_read,
    temp_blks_written::bigint          AS temp_blks_written,
    {readTime}                         AS blk_read_time,
    {writeTime}                        AS blk_write_time,
    NULL::bigint                       AS storage_blks_read,
    NULL::bigint                       AS orcache_blks_hit,
    NULL::double precision             AS storage_blk_read_time,
    NULL::double precision             AS orcache_blk_read_time,
    wal_records::bigint                AS wal_records,
    wal_fpi::bigint                    AS wal_fpi,
    wal_bytes::bigint                  AS wal_bytes,
    NULL::bigint                       AS total_exec_peakmem,
    NULL::bigint                       AS max_exec_peakmem,
    {statsReset}                       AS statements_stats_reset,
    {statsSince}                       AS stats_since,
    {targetNow}                        AS target_now
FROM public.pg_stat_statements
WHERE calls > 0";
        }

        return $@"
SELECT
    queryid::bigint                    AS queryid,
    dbid::bigint                       AS dbid,
    userid::bigint                     AS userid,
    toplevel                           AS toplevel,
    calls::bigint                      AS calls,
    total_exec_time                    AS total_exec_time,
    min_exec_time                      AS min_exec_time,
    max_exec_time                      AS max_exec_time,
    mean_exec_time                     AS mean_exec_time,
    rows::bigint                       AS rows_returned,
    shared_blks_hit::bigint            AS shared_blks_hit,
    shared_blks_read::bigint           AS shared_blks_read,
    shared_blks_dirtied::bigint        AS shared_blks_dirtied,
    shared_blks_written::bigint        AS shared_blks_written,
    temp_blks_read::bigint             AS temp_blks_read,
    temp_blks_written::bigint          AS temp_blks_written,
    {readTime}                         AS blk_read_time,
    {writeTime}                        AS blk_write_time,
    storage_blks_read::bigint          AS storage_blks_read,
    orcache_blks_hit::bigint           AS orcache_blks_hit,
    storage_blk_read_time              AS storage_blk_read_time,
    orcache_blk_read_time              AS orcache_blk_read_time,
    wal_records::bigint                AS wal_records,
    wal_fpi::bigint                    AS wal_fpi,
    wal_bytes::bigint                  AS wal_bytes,
    total_exec_peakmem::bigint         AS total_exec_peakmem,
    max_exec_peakmem::bigint           AS max_exec_peakmem,
    {statsReset}                       AS statements_stats_reset,
    {statsSince}                       AS stats_since,
    {targetNow}                        AS target_now
FROM aurora_stat_statements(false)
WHERE calls > 0";
    }

    /* #3653 A5: the statements EPOCH, appended at ordinal 27 on both flavors. pg_stat_statements_info is
       one row holding stats_reset - the moment the statistics last started from zero. It moves on
       pg_stat_statements_reset(), when a crash left the saved statistics unreadable, and when the
       endpoint now reaches an instance with its own history; it does NOT move on a clean restart with
       pg_stat_statements.save = on, which is exactly right because the counters survive that restart and
       a subtraction across it is honest. Where the view is absent the column is a typed NULL, which
       ServerEpoch reads as "unknown" rather than as a change.

       #3818: gated on the RELATION'S EXISTENCE at query time, never on a version. The first cut of this
       read was `postgresMajorVersion >= 14 ? "(SELECT i.stats_reset FROM public.pg_stat_statements_info AS i)"`
       on the reasoning that the view arrived in extension 1.9 alongside toplevel, "so the same major
       guards both". That conflates the ENGINE major with the EXTENSION'S catalog version. The view is
       created by the extension's 1.8->1.9 update script, and a 14+ engine whose database was upgraded
       in place or restored keeps pg_stat_statements at 1.8 until ALTER EXTENSION ... UPDATE is run by
       hand - RDS and Aurora do not run it. On 23 of 50 clusters in one fleet the whole collector failed
       42P01 on this one column for 25 hours with its base view readable the entire time; the store-side
       read of pg_extension_availability later showed those clusters had no pg_stat_statements row in any
       database at all (#3830), which is the OTHER way the column can be absent and the one they were in.
       Either way the gate is the same, which is the point of gating on the relation rather than a version.
       The hard-coded `public.` was the second failure mode: a relocatable extension lives wherever
       CREATE EXTENSION ... SCHEMA put it.

       Why this shape and not the obvious one - MEASURED, on postgres:13/14/16 containers with the
       extension at 1.8 (PG13's ceiling; PG14 via CREATE EXTENSION ... VERSION '1.8'), 1.9 (PG14 after
       ALTER EXTENSION UPDATE) and 1.10 (PG16, created in a schema off the search_path):

         (SELECT i.stats_reset FROM pg_stat_statements_info AS i WHERE to_regclass(...) IS NOT NULL)
           -> 42P01 at 1.8. A FROM item is resolved at parse analysis, before any WHERE runs.
         CASE WHEN to_regclass(...) IS NULL THEN NULL ELSE (SELECT stats_reset FROM pg_stat_statements_info) END
           -> 42P01 at 1.8. The sub-SELECT's FROM is resolved at parse analysis too; CASE short-circuits
              EVALUATION, not name resolution.

       Only a read whose relation name is resolved at EXECUTION can be conditional on the relation
       existing, and core PostgreSQL has exactly one function that runs SQL text and returns its rows:
       query_to_xml. So: the extension's own schema from pg_extension (exact for a relocatable
       extension, and independent of the monitoring login's search_path - the issue's unqualified
       to_regclass('pg_stat_statements_info') read NULL on the PG16 rig with the extension in a schema
       off the path, which would have left the epoch dark there for a different reason), to_regclass on
       the schema-qualified name as the existence test, and query_to_xml of the one-row read only when
       it passed. CASE evaluates the ELSE branch only when reached, and the whole thing is an
       uncorrelated scalar subquery - EXPLAIN shows it as InitPlan 1, evaluated once per statement, not
       per row. Results: 1.8 -> NULL with no error on PG13 and PG14; after UPDATE to 1.9 -> filled; 1.10
       in schema `ext` -> filled; extension not created at all -> NULL (the outer read fails first on
       its base object, which is the fault the EXTENSION_MISSING sentence is for).

       query_to_xml renders a timestamptz as ISO 8601 with a `T` and a numeric offset
       (2026-09-20T17:54:26.803822+00:00), which the timestamptz input routine accepts; the substring
       takes the element's text and the cast restores the type, so ordinal 27 stays `timestamp with
       time zone` on every path and ReadAsync's GetDateTime is unchanged. With nulls=true a NULL
       stats_reset renders as an xsi:nil element with no text, so the pattern does not match and the
       column is NULL, which is the right answer. The inner statement is a nested statement to
       pg_stat_statements itself - tracked only where pg_stat_statements.track = all, one row per
       cycle, toplevel = false.

       The same form on both flavors: Aurora's aurora_stat_statements() is the extension's data plus
       columns, and the extension's own info view sits beside it wherever the extension was created. */
    public const string StatementsEpochSql = @"(SELECT CASE
            WHEN to_regclass(format('%I.pg_stat_statements_info', n.nspname)) IS NULL THEN NULL::timestamp with time zone
            ELSE substring(
                     query_to_xml(format('SELECT stats_reset FROM %I.pg_stat_statements_info', n.nspname), true, false, '')::text
                     FROM '<stats_reset>([^<]+)</stats_reset>')::timestamp with time zone
        END
     FROM pg_catalog.pg_extension AS e
     JOIN pg_catalog.pg_namespace AS n
       ON n.oid = e.extnamespace
    WHERE e.extname = 'pg_stat_statements')";

    public override string Name => "pg_statement_stats";

    public override string TargetTable => "pg_statement_stats";

    /// <summary>
    /// Every PostgreSQL target (#2625). The SOURCE differs by flavor - Aurora's extended function or the
    /// vanilla view - and that is a <see cref="BuildQuery"/> decision, not an applicability one.
    /// <para><c>AppliesTo</c> means permanent incapability, and stock PostgreSQL is perfectly capable of
    /// reporting per-statement execution statistics. Returning false here told every operator of a
    /// non-Aurora target that this server "does not collect per-query-shape execution statistics, and
    /// never will" - a sentence composed by the capability machinery precisely so that a real gap reads
    /// as final and is not chased. It was not a real gap.</para>
    /// <para><c>pg_stat_statements</c> is not installed by default, so a target without it fails the
    /// read with SQLSTATE 42P01 and lands in the ObjectMissing vocabulary - "the source object does not
    /// exist on this target" - which is the correct, actionable answer (CREATE EXTENSION) and is exactly
    /// how <c>pg_wait_sampling</c> and <c>pg_kernel_stats</c> already report the same situation. An
    /// uninstalled extension is a target-configuration fact, not an incapability.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>
    /// The view this reads is the extension's, and the module has to be preloaded before a
    /// <c>CREATE EXTENSION</c> for it does anything.
    /// <para>#3818: <c>pg_stat_statements_info</c> is declared as a COMPANION the extension gains at 1.9, so
    /// a 42P01 naming it is recorded as what it is - the extension present below 1.9, remedy
    /// <c>ALTER EXTENSION pg_stat_statements UPDATE</c> - and not as the extension missing. The query reads
    /// the companion in the select list and the base object in FROM, which is the order the declaration's
    /// inference depends on (see <see cref="PgExtensionDependency.Companions"/>). Since #3818 the read is
    /// gated on the relation's existence and cannot raise that 42P01 itself; the declaration stands for the
    /// stored sentence's sake and for the next companion.</para>
    /// <para>#3830: <c>AuroraNativeAlternative</c> is what makes the remedy correct on the population that
    /// motivated #3818. Those 23 clusters have no <c>pg_extension</c> row for <c>pg_stat_statements</c> at
    /// all - the extension was never created in any database - and this collector was productive on every
    /// one of them the whole time, because on Aurora it reads <c>aurora_stat_statements()</c>, which needs
    /// no extension. What their missing row costs is the companion and nothing else, so the remedy there is
    /// an OPTIONAL <c>CREATE EXTENSION</c> rather than the <c>ALTER EXTENSION ... UPDATE</c> that has
    /// nothing to update.</para>
    /// </summary>
    public override IReadOnlyList<PgExtensionDependency> RequiredPgExtensions { get; } = new[]
    {
        new PgExtensionDependency("pg_stat_statements", PgExtensionInstallKind.SharedPreloadLibraries)
        {
            Companions = new[] { new PgExtensionCompanionObject("pg_stat_statements_info", "1.9") },
            AuroraNativeAlternative = "aurora_stat_statements()",
        },
    };

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion, context.Target.IsAurora));

    /// <summary>
    /// The statements epoch this collector persists per server (#3653 A5): <c>pg_stat_statements_info.stats_reset</c>
    /// as last observed, and the value the latest change replaced. Declared so both hosts read the prior
    /// before the run and write the observed one after it (#1962's generic wiring). One row per server per
    /// key; the previous-value key is written only on an epoch.
    /// </summary>
    public override IReadOnlyList<string> StateKeys { get; } = new[]
    {
        ServerEpoch.StatementsStateKey,
        ServerEpoch.StatementsPreviousStateKey,
    };

    /// <summary>
    /// The delta groups this collector subtracts under, spelled once for the epoch forget below. The three
    /// <c>CalculateDeltaWithInterval</c> calls keep their literals on purpose: <c>DeltaFamilySeedingCensusTests</c>
    /// reads the groups off those call sites, and <c>ServerEpochTests.PgStatementStats_ForgetsItsOwnGroupsBeforeItsFirstSubtraction</c>
    /// drives one read and asserts the groups the forget named are exactly the groups the pass then subtracted under.
    /// </summary>
    internal static readonly string[] DeltaGroups = { "pg_statement_stats_calls", "pg_statement_stats_time", "pg_statement_stats_rows" };

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("queryid", CollectorColumnType.BigInt),
        new CollectorColumn("database_id", CollectorColumnType.BigInt),
        new CollectorColumn("user_id", CollectorColumnType.BigInt),
        new CollectorColumn("toplevel", CollectorColumnType.Boolean),
        new CollectorColumn("calls", CollectorColumnType.BigInt),
        new CollectorColumn("total_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("min_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("max_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("mean_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("rows_returned", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_hit", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_read", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_dirtied", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_written", CollectorColumnType.BigInt),
        new CollectorColumn("temp_blks_read", CollectorColumnType.BigInt),
        new CollectorColumn("temp_blks_written", CollectorColumnType.BigInt),
        new CollectorColumn("blk_read_time_ms", CollectorColumnType.Double),
        new CollectorColumn("blk_write_time_ms", CollectorColumnType.Double),
        /* Aurora-only I/O source split. Without these, blks_read is opaque: it may have been a
           network round trip to the storage volume or a hit in the local NVMe tier. */
        new CollectorColumn("storage_blks_read", CollectorColumnType.BigInt),
        new CollectorColumn("orcache_blks_hit", CollectorColumnType.BigInt),
        new CollectorColumn("storage_blk_read_time_ms", CollectorColumnType.Double),
        new CollectorColumn("orcache_blk_read_time_ms", CollectorColumnType.Double),
        new CollectorColumn("wal_records", CollectorColumnType.BigInt),
        new CollectorColumn("wal_fpi", CollectorColumnType.BigInt),
        new CollectorColumn("wal_bytes", CollectorColumnType.BigInt),
        /* The memory-grant analog. No SQL Server DMV gives per-query WAL bytes either, so both of
           these are signals the SQL Server side cannot offer. */
        new CollectorColumn("total_exec_peakmem_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("max_exec_peakmem_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("delta_calls", CollectorColumnType.BigInt),
        new CollectorColumn("delta_total_exec_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_rows", CollectorColumnType.BigInt),
        /* Appended (Darling V128, #3540): the measured seconds the row's three deltas accrued over, or 0
           when no delta was knowable. Appended at the END because the COPY writer is positional — the
           same rule GoldenCollectorSchema's header states for every column a numbered migration adds by
           ALTER TABLE. Until V128 this collector asked the calculator for the interval only to decide
           the idle-row skip and stored nothing, so every row it DID ship with interval 0 (first sighting,
           reset, gap re-baseline — the three cases the skip deliberately lets through) carried delta 0s
           that the per-statement duration trend divided by a LAG-derived span into 0.00 calls/sec. */
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var epochObserved = false;

        while (await reader.ReadAsync(cancellationToken))
        {
            /* #3653 A5: the epoch first, off the FIRST row and BEFORE that row's own subtraction. stats_reset
               is the same on every row (an uncorrelated scalar), so one read is the observation; and it has
               to precede the delta calls because the forget it may trigger is what makes THIS pass's rows
               honest - forgotten after the loop, every row would already have been subtracted from the
               dead baseline and stored. A pass that returns no rows (an idle server right after a reset,
               with `calls > 0` matching nothing) observes nothing and catches up on the first pass with a
               row, where the reset branch would have marked those rows (0, 0) anyway. */
            if (!epochObserved)
            {
                epochObserved = true;
                ServerEpoch.ObserveStatements(
                    context,
                    reader.IsDBNull(27) ? null : reader.GetDateTime(27),
                    DeltaGroups);
            }

            var queryId = reader.GetInt64(0);
            var databaseId = reader.GetInt64(1);
            var userId = reader.GetInt64(2);
            var topLevel = !reader.IsDBNull(3) && reader.GetBoolean(3);
            var calls = reader.GetInt64(4);
            var totalExecTimeMs = Dbl(reader, 5);
            var rowsReturned = reader.GetInt64(9);

            /* Delta key is (queryid, dbid, userid, toplevel) — the full pg_stat_statements identity, not
               queryid alone. The same normalized statement run by a different user or against a different
               database is a DIFFERENT entry with its own counters, so keying on queryid alone would
               interleave several series into one and produce nonsense deltas.

               queryid itself is not stable across major versions (it is derived from a post-parse-analysis
               tree, including internal object identifiers), so a mass reset after an upgrade is expected
               behaviour rather than an anomaly — the delta calculator's counter-regression handling covers
               it the same way it covers a restart. */
            var key = string.Create(CultureInfo.InvariantCulture,
                $"{queryId}|{databaseId}|{userId}|{(topLevel ? 1 : 0)}");

            /* #4428: stats_since (ordinal 28, NULL below PostgreSQL 17 or when the column does not exist
               on this cluster's extension catalog) and target_now (ordinal 29, always present — now() never
               returns NULL) are read TOGETHER, in this same row, before any delta call below can touch a
               baseline. Both travel on the TARGET's own clock; neither is ever compared against anything
               measured on the collector host's clock — see the class remarks and #4428's skew pin. */
            var statsSince = reader.IsDBNull(28) ? (DateTime?)null : reader.GetDateTime(28);
            var targetNow = reader.GetDateTime(29);

            /* #4428: peeked BEFORE any of the three per-family calls below mutate a baseline — the same
               ordering DecideRow's own doc comment requires and QueryStatsCollector already follows for
               query_stats. seriesAgeSeconds is passed null deliberately: DecideRow's OWN gap-placement
               branch (reached only when seriesAgeSeconds.HasValue) measures the gap on collectionTime,
               the COLLECTOR's clock, which this definition must never use for placement. Passing null
               skips that branch entirely and leaves this call a pure AnyReset peek; placement is decided
               below, on the target's own clock, using stats_since and the target-clock pass window. */
            var rowReset = context.Deltas.DecideRow(
                context.ServerId,
                new (string Family, long Current)[]
                {
                    ("pg_statement_stats_calls", calls),
                    ("pg_statement_stats_time", (long)totalExecTimeMs),
                    ("pg_statement_stats_rows", rowsReturned),
                },
                key,
                seriesAgeSeconds: null,
                collectionTime: context.CollectionTime,
                maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

            /* #4428: the TARGET-clock pass window, tracked under a group name no ordinary delta call ever
               passes as its own collectorName (see ICollectorDeltaCalculator.PreviousPass's contract), so
               it never collides with the collector-clock windows the three CalculateDeltaWithInterval
               calls below keep for themselves. Rolled every pass — target_now moves forward on every
               genuine pass, so this always advances — and returns the PREVIOUS pass's target_now, the
               only clock a restart may be placed against. */
            var previousTargetNow = context.Deltas.PreviousPass(context.ServerId, "pg_statement_stats_target_clock", targetNow);

            var deltaCalls = context.Deltas.CalculateDeltaWithInterval(
                context.ServerId, "pg_statement_stats_calls", key, calls, out var callsIntervalSeconds,
                collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
            /* Time is stored as double milliseconds but the delta machinery is integral, so the delta is
               taken on whole milliseconds. Sub-millisecond drift per interval is immaterial against the
               totals this feeds, and keeping one delta calculator for both engines is worth more.

               Computed unconditionally, even on a row this pass goes on to skip: CalculateDelta is also
               what refreshes a series' cached timestamp, and skipping the call on an idle row would leave
               THIS series stale while the calls series (called for every row, always) stayed fresh —
               so the next genuinely active cycle could see a gap here that calls never sees, and report a
               false counter-reset on a real accrual. See the class remarks. */
            var deltaTotalTime = context.Deltas.CalculateDeltaWithInterval(
                context.ServerId, "pg_statement_stats_time", key, (long)totalExecTimeMs, out var timeIntervalSeconds,
                collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
            var deltaRows = context.Deltas.CalculateDeltaWithInterval(
                context.ServerId, "pg_statement_stats_rows", key, rowsReturned, out var rowsIntervalSeconds,
                collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

            /* #4428: the row-coherent decision, applied AFTER every per-family call above has already run
               and stored its own new baseline — exactly QueryStatsCollector's ordering, so the row is
               ready for an ordinary delta on the NEXT pass regardless of which branch this row takes now.
               Any family decreasing makes the WHOLE row a restart (rowReset.AnyReset, decided above on the
               unmutated baseline). Placement: stats_since strictly AFTER the previous pass's target-clock
               now() means the restart happened inside the gap, so every counter's delta becomes its
               CURRENT value over the real target-clock gap; anything else — stats_since absent (rule 3),
               previousTargetNow unknown (first pass), or stats_since at or before the previous target_now
               — makes the whole row unknowable, (0, 0), same as an ordinary single-family reset already
               reports. NEVER compared against context.CollectionTime — the collector host's own clock —
               which is the whole point of carrying target_now beside stats_since in the same read. */
            if (rowReset.AnyReset)
            {
                var gapSeconds = previousTargetNow.HasValue
                    ? (int)(targetNow - previousTargetNow.Value).TotalSeconds
                    : 0;

                var creditedInGap = statsSince.HasValue && previousTargetNow.HasValue
                    && statsSince.Value > previousTargetNow.Value
                    && gapSeconds > 0;

                if (creditedInGap)
                {
                    deltaCalls = calls;
                    deltaTotalTime = (long)totalExecTimeMs;
                    deltaRows = rowsReturned;
                    callsIntervalSeconds = gapSeconds;
                    timeIntervalSeconds = gapSeconds;
                    rowsIntervalSeconds = gapSeconds;
                }
                else
                {
                    deltaCalls = 0;
                    deltaTotalTime = 0;
                    deltaRows = 0;
                    callsIntervalSeconds = 0;
                    timeIntervalSeconds = 0;
                    rowsIntervalSeconds = 0;
                }
            }

            /* #3540 (V128): the stored interval is the MINIMUM over the row's three groups — the V127 rule
               (WaitStatsCollector). The groups share a key and a collection time, so they agree in every
               case but an independent single-counter reset, and pg_stat_statements resets an entry's
               counters together; the minimum makes the stored pair mean "every delta in this row is
               knowable", so a reader never divides one group's reset 0 by a sibling's real span. After the
               #4428 override above the three intervals already agree, so this is a no-op for a restarted
               row and unchanged behaviour for every other one. */
            var sampleIntervalSeconds = Math.Min(callsIntervalSeconds, Math.Min(timeIntervalSeconds, rowsIntervalSeconds));

            /* The skip: a REAL interval (this is not a first sighting, a counter reset, or a gap this
               pass just re-baselined) with zero new calls means the statement demonstrably did not run,
               and pg_stat_statements only advances any OTHER counter on a call — so nothing on this row
               changed and there is nothing new to write. interval == 0 covers first-sight/reset/gap-reset
               together, and all three are genuinely new information, so all three still ship. See the
               class remarks for the full reasoning and why this check is calls-only. */
            if (callsIntervalSeconds > 0 && deltaCalls == 0)
            {
                continue;
            }

            rows.Add(new Row(
                QueryId: queryId,
                DatabaseId: databaseId,
                UserId: userId,
                TopLevel: topLevel,
                Calls: calls,
                TotalExecTimeMs: totalExecTimeMs,
                MinExecTimeMs: Dbl(reader, 6),
                MaxExecTimeMs: Dbl(reader, 7),
                MeanExecTimeMs: Dbl(reader, 8),
                RowsReturned: rowsReturned,
                SharedBlocksHit: reader.GetInt64(10),
                SharedBlocksRead: reader.GetInt64(11),
                SharedBlocksDirtied: reader.GetInt64(12),
                SharedBlocksWritten: reader.GetInt64(13),
                TempBlocksRead: reader.GetInt64(14),
                TempBlocksWritten: reader.GetInt64(15),
                BlockReadTimeMs: Dbl(reader, 16),
                BlockWriteTimeMs: Dbl(reader, 17),
                /* NullableLong/NullableDbl, not Dbl: on the vanilla path these six ARRIVE null, and
                   coalescing them to zero here would erase the distinction the whole change rests on. */
                StorageBlocksRead: NullableLong(reader, 18),
                OrcacheBlocksHit: NullableLong(reader, 19),
                StorageBlockReadTimeMs: NullableDbl(reader, 20),
                OrcacheBlockReadTimeMs: NullableDbl(reader, 21),
                WalRecords: reader.GetInt64(22),
                WalFpi: reader.GetInt64(23),
                WalBytes: reader.GetInt64(24),
                TotalExecPeakMemBytes: NullableLong(reader, 25),
                MaxExecPeakMemBytes: NullableLong(reader, 26),
                DeltaCalls: deltaCalls,
                DeltaTotalExecTimeMs: deltaTotalTime,
                DeltaRows: deltaRows,
                SampleIntervalSeconds: sampleIntervalSeconds));
        }

        return rows;

        static double Dbl(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? 0 : r.GetDouble(ordinal);

        static long? NullableLong(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetInt64(ordinal);

        static double? NullableDbl(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDouble(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.QueryId)
            .Value(row.DatabaseId)
            .Value(row.UserId)
            .Value(row.TopLevel)
            .Value(row.Calls)
            .Value(row.TotalExecTimeMs)
            .Value(row.MinExecTimeMs)
            .Value(row.MaxExecTimeMs)
            .Value(row.MeanExecTimeMs)
            .Value(row.RowsReturned)
            .Value(row.SharedBlocksHit)
            .Value(row.SharedBlocksRead)
            .Value(row.SharedBlocksDirtied)
            .Value(row.SharedBlocksWritten)
            .Value(row.TempBlocksRead)
            .Value(row.TempBlocksWritten)
            .Value(row.BlockReadTimeMs)
            .Value(row.BlockWriteTimeMs)
            .Value(row.StorageBlocksRead)
            .Value(row.OrcacheBlocksHit)
            .Value(row.StorageBlockReadTimeMs)
            .Value(row.OrcacheBlockReadTimeMs)
            .Value(row.WalRecords)
            .Value(row.WalFpi)
            .Value(row.WalBytes)
            .Value(row.TotalExecPeakMemBytes)
            .Value(row.MaxExecPeakMemBytes)
            .Value(row.DeltaCalls)
            .Value(row.DeltaTotalExecTimeMs)
            .Value(row.DeltaRows)
            .Value(row.SampleIntervalSeconds);  /* sample_interval_seconds INTEGER — measured, 0 = unknowable */
    }
}
