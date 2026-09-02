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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Query Store runtime stats from every database with Query Store actually enabled. Extracted
/// verbatim from Lite's RemoteCollectorService.QueryStore.cs: the enumeration cursor probes each
/// database's sys.database_query_store_options.actual_state (NOT sys.databases.is_query_store_on,
/// which can be out of sync on Azure SQL DB; on-prem additionally filters to non-AG or
/// primary-replica databases). actual_state is matched against the explicit usable set
/// IN (1, 2, 4) — READ_ONLY, READ_WRITE, READ_CAPTURE_SECONDARY — rather than the looser
/// "> 0", which also admits 3 = ERROR: an errored Query Store is not readable and must not
/// pass the "is QS usable" gate (0 = OFF). The per-item [db].sys.sp_executesql query is incremental on the
/// last_execution_time watermark (fallback: 60 minutes back), and the 2017+/2022+ column gates
/// are decided by a live PRODUCTVERSION probe each cycle (default 13 when the probe fails) —
/// deliberately probed rather than trusting cached connection status, which can be
/// version-unknown.
///
/// <para>TWO execution shapes, ONE payload (#1836). Everything above describes the on-prem/RDS/MI
/// path: enumerate, then run the payload per database through <c>[db].sys.sp_executesql</c>.
/// Azure SQL DB cannot do that at all — a three-part cross-database reference is rejected for EVERY
/// database, from master and from a user database alike — so there the host connects per database
/// (<see cref="RunsPerDatabase"/>) and <see cref="BuildQuery"/> runs the eligibility gate and the
/// payload directly against the connected database. Before #1836 the Azure enumeration probed with
/// exactly that rejected three-part reference inside an empty CATCH, so every database failed
/// silently, the item list came back empty, and the collector logged SUCCESS with zero rows forever.
/// Both shapes are built from the SINGLE body <see cref="BuildPayloadBody"/> returns — the on-prem
/// wrapper only quote-doubles it for nesting — because two hand-maintained copies of a 55-column
/// SELECT is precisely the drift this collector cannot survive. (55 selected columns; the stored row
/// is 56 — <c>database_name</c> is supplied client-side, from the enumerated item or the connected
/// database.)</para>
///
/// <para>ONE ROW PER INTERVAL (#1907). <c>sys.query_store_runtime_stats</c> hands back the flushed and
/// the still-in-memory slice of one <c>runtime_stats_interval_id</c> as separate, ADDITIVE rows, so the
/// payload groups on the view's natural key and combines them before emitting. Without it both slices
/// were stored, shared the whole read-side dedup key AND <c>collection_time</c>, and the survivor was
/// whichever the engine emitted first — a grid could show an in-memory sliver in place of the interval's
/// total. The emitted row SHAPE is untouched by that change; only the row COUNT per interval is.</para>
/// </summary>
public sealed class QueryStoreCollector : CollectorDefinitionBase<QueryStoreCollector.Row>
{
    public static QueryStoreCollector Instance { get; } = new();

    private QueryStoreCollector()
    {
    }

    public sealed class Row
    {
        public string DatabaseName { get; set; } = "";
        public long QueryId { get; set; }
        public long PlanId { get; set; }
        public string? ExecutionTypeDesc { get; set; }
        public DateTime? FirstExecutionTime { get; set; }
        public DateTime? LastExecutionTime { get; set; }
        public string? ModuleName { get; set; }
        public string? QueryText { get; set; }
        public string? QueryHash { get; set; }
        public long ExecutionCount { get; set; }
        public long AvgDurationUs { get; set; }
        public long MinDurationUs { get; set; }
        public long MaxDurationUs { get; set; }
        public long AvgCpuTimeUs { get; set; }
        public long MinCpuTimeUs { get; set; }
        public long MaxCpuTimeUs { get; set; }
        public long AvgLogicalIoReads { get; set; }
        public long MinLogicalIoReads { get; set; }
        public long MaxLogicalIoReads { get; set; }
        public long AvgLogicalIoWrites { get; set; }
        public long MinLogicalIoWrites { get; set; }
        public long MaxLogicalIoWrites { get; set; }
        public long AvgPhysicalIoReads { get; set; }
        public long MinPhysicalIoReads { get; set; }
        public long MaxPhysicalIoReads { get; set; }
        public long AvgClrTimeUs { get; set; }
        public long MinClrTimeUs { get; set; }
        public long MaxClrTimeUs { get; set; }
        public long MinDop { get; set; }
        public long MaxDop { get; set; }
        public long AvgQueryMaxUsedMemory { get; set; }
        public long MinQueryMaxUsedMemory { get; set; }
        public long MaxQueryMaxUsedMemory { get; set; }
        public long AvgRowcount { get; set; }
        public long MinRowcount { get; set; }
        public long MaxRowcount { get; set; }
        public long AvgNumPhysicalIoReads { get; set; }
        public long MinNumPhysicalIoReads { get; set; }
        public long MaxNumPhysicalIoReads { get; set; }
        public long AvgLogBytesUsed { get; set; }
        public long MinLogBytesUsed { get; set; }
        public long MaxLogBytesUsed { get; set; }
        public long AvgTempdbSpaceUsed { get; set; }
        public long MinTempdbSpaceUsed { get; set; }
        public long MaxTempdbSpaceUsed { get; set; }
        public string? PlanType { get; set; }
        public string? PlanForcingType { get; set; }
        public bool IsForcedPlan { get; set; }
        public long ForceFailureCount { get; set; }
        public string? LastForceFailureReason { get; set; }
        public int CompatibilityLevel { get; set; }
        public string? QueryPlanText { get; set; }
        public string? QueryPlanHash { get; set; }

        /// <summary>
        /// The replica role sys.query_store_replicas attributed this runtime-stats row to ('Primary',
        /// 'Secondary', 'Geo Secondary', 'Geo HA Secondary'). NULL = the server did not attribute it
        /// (pre-2022, a 2022 standalone whose sys.query_store_replicas is empty, or Managed Instance,
        /// which keeps the pure version gate) — deliberately not coalesced. Azure SQL DB DOES attribute
        /// as of #1872, having been gated off for bind safety until the catalog was proven live on both
        /// General Purpose and Hyperscale. See hasReplicaAttribution in <see cref="BuildPayloadBody"/>.
        /// </summary>
        public string? ReplicaRole { get; set; }

        /// <summary>
        /// The runtime-stats interval this row is a snapshot OF — <c>sys.query_store_runtime_stats.
        /// runtime_stats_interval_id</c>, the real interval identity (#1841 tier 2). Query Store rows are
        /// CUMULATIVE per-interval snapshots and the collector re-fetches the OPEN interval every cycle, so
        /// every aggregate read must collapse an interval to its latest snapshot before summing; before this
        /// column the only identity available was the <c>first_execution_time</c> proxy. Per DATABASE, not
        /// per server — each database has its own Query Store and its own interval sequence — so a dedup key
        /// carrying it must also carry <c>database_name</c>. NULL only on rows collected by a pre-tier-2
        /// build.
        /// </summary>
        public long? RuntimeStatsIntervalId { get; set; }

        /// <summary>
        /// When the interval STARTED, in UTC — <c>sys.query_store_runtime_stats_interval.start_time</c>,
        /// converted at collection (#1841 tier 2). This is the honest x-coordinate for "when the work ran":
        /// bucketing on <c>collection_time</c> attributes an interval to the cycle that last FETCHED it,
        /// which on Query Store's default 60-minute interval is reliably one bucket late.
        /// </summary>
        public DateTime? IntervalStartTimeUtc { get; set; }
    }

    private const string OnPremDatabaseListQueryText = @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @result TABLE (name sysname);

/* #1837: every probe failure below used to die in an empty CATCH, so a login that could not enter a
   single database enumerated 0 items and logged one indistinguishable SUCCESS row. These rows come back
   as the enumeration's SECOND result set — the driver's probe-failure contract — because the FIRST one
   is the item list the runners collect from. */
DECLARE
    @probe_failures TABLE (name sysname, error_text nvarchar(4000));

DECLARE
    @db sysname,
    @sql NVARCHAR(500),
    @exec_sp nvarchar(256);

DECLARE db_check CURSOR LOCAL FAST_FORWARD FOR
    SELECT /* PerformanceMonitorLite */
        d.name
    FROM sys.databases AS d
    LEFT JOIN sys.dm_hadr_database_replica_states AS drs
        ON d.database_id = drs.database_id
        AND drs.is_local = 1
    WHERE d.database_id > 4
    AND   d.database_id < 32761
    AND   d.state_desc = N'ONLINE'
    AND   d.name <> N'PerformanceMonitor'
    AND   HAS_DBACCESS(d.name) = 1 /*#1823's screen, which this collector never got: without it a least-privilege login probes every database it cannot enter and takes a 916 per database per cycle. Harmless while those failures were swallowed; with #1837 recording them they would be a permanent probe-failure note and a warning burst every cycle for a permission posture that is not changing. Its three siblings (database_scoped_config, index_object_stats, database_size_stats) already filter this way. On-prem only - from master on Azure SQL DB this returns 0 for every user database, and Azure does not use this enumeration.*/
    /* Default screen (#1565): vendor/system-adjacent databases with no customer workload. First group =
       vendor-controlled names (cannot collide with real customer data): cloud-provider management dbs,
       SSRS catalogs, PolyBase/DW artifacts, plus the id>4 system four as a name-based belt for clarity.
       Second group = common DBA-convention tooling names, screened by operator decision; the inverse
       case (exclude a real workload db) is what excludedDatabases handles. */
    AND   d.name NOT IN
          (
              N'master', N'model', N'msdb', N'tempdb',
              N'rdsadmin', N'gcloud_cloudsqladmin',
              N'ReportServer', N'ReportServerTempDB',
              N'DWConfiguration', N'DWDiagnostics', N'DWQueue',
              N'DBAUtil', N'DBAUtils', N'Utility'
          )
    AND
    (
        drs.database_id IS NULL          /*not in any AG*/
        OR drs.is_primary_replica = 1    /*primary replica*/
    )
    /*EXCLUSION_FILTER*/
    OPTION(RECOMPILE);

OPEN db_check;

FETCH NEXT
FROM db_check
INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        /* actual_state IN (1,2,4) = READ_ONLY / READ_WRITE / READ_CAPTURE_SECONDARY (#1546: > 0 also
           admitted 3 = ERROR). readonly_reason & 8 = 0 (#1558): bit 8 is the engine saying read-only
           BECAUSE this database is a readable secondary replica — its Query Store content is the
           PRIMARY's persisted QS tables arriving via replication, not local activity, so collecting it
           is duplicate, lagged primary data (caught live: 24 RDS read replicas each re-reading the same
           primary's QS — pathological volume for zero information). The HADR join in the cursor above
           only catches Always On AG secondaries; reason bit 8 is the engine's mechanism-agnostic flag
           (AG, RDS read replicas, geo-secondaries alike). An operator-set read-only QS on a PRIMARY has
           reason <> 8 and still collects; a 2025 READ_CAPTURE_SECONDARY (state 4, reason 0) captures
           REAL local secondary workload and still collects. */
        SET @sql = N'
            SELECT ' + QUOTENAME(@db, '''') + N'
            WHERE EXISTS
            (
                SELECT
                    1
                FROM sys.database_query_store_options
                WHERE actual_state IN (1, 2, 4)
                AND   readonly_reason & 8 = 0
            );';

        SET @exec_sp = QUOTENAME(@db) + N'.sys.sp_executesql';

        INSERT @result (name)
        EXECUTE @exec_sp @sql;
    END TRY
    BEGIN CATCH
        /* The failure modes this catches are ordinary and per-database (mid-restore, an AG failover
           mid-cursor, a login without access, a database that went offline between the cursor and the
           probe), so the cursor keeps going — but the database is now MISSING from a collection that
           still reports SUCCESS, which is exactly the hole #1837 closes. */
        INSERT @probe_failures (name, error_text)
        VALUES (@db, ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT
    FROM db_check
    INTO @db;
END;

CLOSE db_check;
DEALLOCATE db_check;

SELECT
    name
FROM @result
ORDER BY
    name;

/* Second result set = the driver's probe-failure contract (EnumeratedCollectorDriver.ReadEnumerationAsync).
   Always returned, normally empty; the driver reads zero rows and attaches no note. */
SELECT
    name,
    error_text
FROM @probe_failures
ORDER BY
    name;";

    /// <summary>
    /// The eligibility gate the Azure per-database path runs against the CURRENT database before the
    /// payload (#1836) — the same two predicates the on-prem enumeration cursor probes with, evaluated
    /// locally instead of through a cross-database <c>[db].sys.sp_executesql</c> reference that Azure
    /// SQL DB rejects outright. Ineligible databases (Query Store OFF, ERRORed, or a readable
    /// secondary) return NO result set at all, which the reader sees as zero rows — the cheapest
    /// possible answer, and it never touches the Query Store catalog views.
    /// </summary>
    private const string AzureEligibilityGateText = @"
IF NOT EXISTS
(
    SELECT
        1
    FROM sys.database_query_store_options
    WHERE actual_state IN (1, 2, 4)
    AND   readonly_reason & 8 = 0
)
BEGIN
    RETURN;
END;

";

    /// <summary>The live version probe deciding the 2017+/2022+ column gates (see class remarks).</summary>
    public const string ProductVersionProbeText =
        "SELECT CONVERT(integer, PARSENAME(CONVERT(sysname, SERVERPROPERTY('PRODUCTVERSION')), 4))";

    /// <summary>PRODUCTVERSION assumed when the probe fails or returns NULL (SQL Server 2016).</summary>
    public const int DefaultProductVersion = 13;

    /// <summary>
    /// Server-side per-database row backstop (#1556): a coarse ceiling — ~10× a healthy per-cycle
    /// volume — spliced as <c>TOP ... WITH TIES</c> so the SQL engine bounds its own work and the wire
    /// transfer before the client byte budget ever engages. Deliberately NOT the siblings' curation caps
    /// (query_stats TOP 200, procedure_stats TOP 150): those curate the "top N"; this only bounds a
    /// pathological cycle, and since #1960 (oldest-first shipping) the overflow DEFERS to the next
    /// cycle's resume from the shipped boundary rather than being dropped.
    /// It bounds COUNT, not BYTES — <see cref="MaxTextBytesPerDatabase"/> is the primary memory bound —
    /// and is the same const the host warns on (<see cref="PerItemRowCountWarnThreshold"/>).
    ///
    /// <para>Since #1907 the <c>TOP</c> sits OUTSIDE the slice aggregation, so it caps INTERVALS rather
    /// than the raw slices an interval decomposes into. That is the more useful unit — a cap that fell
    /// mid-interval would have truncated one interval's slices and emitted a partial sum, which is worse
    /// than not emitting the interval at all — and it is strictly more generous, since one interval is
    /// one row where it used to be several.</para>
    /// </summary>
    public const int MaxRowsPerDatabase = 50_000;

    /// <summary>
    /// The PRIMARY memory bound (#1556): the cumulative per-database TEXT byte budget the client stops
    /// reading at, enforced in the shared read loop both paths use — the field incident (0→13GB) was
    /// exactly this un-bounded: 50k rows × ~40KB plan XML is ~2GB for ONE database, with the
    /// row cap firing no defense (it caps rows, not bytes). Kept alongside the SQL <c>TOP</c> backstop.
    ///
    /// <para>64 MB rather than the original 256 MB (#1960): now that rows ship oldest-first and a
    /// budget cut RESUMES from the shipped boundary next cycle instead of abandoning the remainder,
    /// a smaller budget costs catch-up latency, never data — the July field catalog (~1.33 GB fresh)
    /// converges in ~21 bounded cycles instead of one unbounded pull. Composes with the #1553 4-wide
    /// sweep to a bounded peak of ≈ 4 × 64 MB ≈ 256 MB transient.</para>
    /// </summary>
    public const int MaxTextBytesPerDatabase = 64 * 1024 * 1024;

    /// <summary>
    /// The WALL-CLOCK ceiling for one database's pass (#2150), and the bound of last resort: the row cap
    /// bounds ROWS, the byte budget bounds BYTES, and neither bounds TIME.
    ///
    /// <para><b>The field report it exists for.</b> Two Azure SQL DB elastic-pool databases, same day,
    /// across the 3.3.0 → 3.4.0 upgrade: 198 passes at a median of <b>4.8 s</b> before, then six passes of
    /// 0.1, 37.6, 46.1, 82.1, 0.1 and <b>99.8 minutes</b> after. Because a host's live collectors run one
    /// after another, a single 100-minute pass starves every other collector on that server — which is the
    /// actual mechanism behind #2148's "all collection stopped".</para>
    ///
    /// <para><b>Why nothing already caught it.</b> The <c>CommandTimeout</c> was 30 s the whole time. It
    /// bounds the wait for a network read and SqlClient resets it on each read that arrives, so a result
    /// set that trickles rows never trips it — see <see cref="PerItemWallClockBudget"/>.</para>
    ///
    /// <para><b>Why ten minutes.</b> It has to sit far above every healthy observation and far below every
    /// pathological one. Healthy: 4.8 s median and 31 s max across 198 field passes; 375–524 ms for the
    /// staged query measured on a 212k-row Query Store; 6 s for the two fast post-upgrade passes.
    /// Pathological: 37.6 minutes at the low end. Ten minutes is ~19× the worst healthy pass, ~10× the
    /// Darling command-timeout default, and ~3.7× under the smallest pass this is meant to stop.</para>
    ///
    /// <para><b>It converges rather than repeating.</b> A cut pass ships nothing, so the watermark does not
    /// advance and the range is re-read — but the failure also feeds #2111's consecutive-failure count, so
    /// the catch-up window halves per failure toward 15 minutes until a pass fits. A success resets it and
    /// the database returns to full width. Without that this would be a bound that fires forever on the same
    /// impossible width; with it, a database that cannot finish narrows until it can.</para>
    /// </summary>
    public static readonly TimeSpan PerDatabaseWallClockBudget = TimeSpan.FromMinutes(10);

    /// <inheritdoc />
    public override TimeSpan? PerItemWallClockBudget => PerDatabaseWallClockBudget;

    /// <summary>
    /// The per-command budget for this collector, on BOTH halves of a fetch (#2776) — the SQL Server read
    /// via <see cref="DarlingCollectorRunner"/>'s <c>itemTimeout</c>, and the store-side probe/write commands
    /// the runner hands the same value to. One number, because a fetch is one logical operation and a cancel
    /// on either half has the identical consequence: the ids read as still-missing, the carry-over keeps them,
    /// and the target re-decompresses the same plans next cycle.
    ///
    /// <para><b>500 s is an empirical over-shoot pending measurement, not settled tuning.</b> Erik chose it
    /// deliberately against an observed <c>max_sql_ms</c> of 361 s, to see where the failures stop rather than
    /// to sit just above the worst known case. Evidence it replaces: the store side inherited Npgsql's 30 s
    /// default (nobody chose it — the path simply never set one) and the SQL side sat at the 60 s runner
    /// default, while measured <c>plan_fetch</c> phases reach 91,526 ms. Baseline on use1 over 14.9 h: 125
    /// plan-fetch failures, 16 text-fetch, 2,965 candidate-cap clamps.</para>
    ///
    /// <para><b>The tradeoff, stated because it may show up as a regression.</b> A fetch that previously failed
    /// fast now HOLDS its slot for up to 500 s. Fleet concurrency is 4 and the sweep budget is 60 s, so
    /// <c>skipping relaunch</c> and BODY_OVERRUN can get WORSE even if the failure count improves. Both
    /// directions have to be read together or a win on one number hides a loss on the other. Sits under
    /// <see cref="PerDatabaseWallClockBudget"/> (600 s), which remains the outer bound of last resort.</para>
    /// </summary>
    public override int? CommandTimeoutSecondsOverride => 500;

    /// <summary>
    /// The self-identification marker every collector query carries in its leading comment. Self rows
    /// are excluded CLIENT-SIDE in the shared read loop both paths use (#1565) — the old SQL-side
    /// NOT LIKE predicate was 75% of the read's elapsed time (a full nvarchar(max) scan per row on a
    /// column no index can serve), and the text is already materialized here anyway.
    /// </summary>
    public const string SelfQueryMarker = "PerformanceMonitorLite";

    public override string Name => "query_store";

    public override string TargetTable => "query_store_stats";

    /// <summary>
    /// Query Store first shipped in SQL Server 2016 (v13), so on-prem/RDS require v13+; a pre-2016 box has no
    /// Query Store at all. Azure SQL DB / Managed Instance report a low ProductMajorVersion yet ship Query
    /// Store, so they are never version-gated, and an unknown version (0) is assumed newest — the exact
    /// condition Lite used in IsCollectorSupported. Gated here in the shared AppliesTo so Lite and Darling
    /// skip identically on a pre-2016 target. (The per-cycle PRODUCTVERSION probe still refines which
    /// version-gated columns are selected; this gate decides whether the collector runs at all.)
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target.SqlMajorVersion == 0 || target.SqlMajorVersion >= 13 || target.IsAzureSqlDb || target.IsAzureManagedInstance;

    /// <summary>Incremental: only intervals with newer last_execution_time are fetched per cycle.</summary>
    public override string? WatermarkColumn => "last_execution_time";

    /// <summary>
    /// Per-database watermark (#1556): query_store enumerates databases and now FLUSHES each database's
    /// rows before reading the next, so its cutoff must be per-database — otherwise any mid-run abort
    /// (shutdown, OOM, a store-write failure) would strand the un-flushed databases' intervals behind the
    /// committed databases' advanced server-wide watermark. Keyed on the already-collected
    /// <c>database_name</c> column, the same precedent the deadlocks / blocked_process_report XE
    /// collectors set. With this, each database's commit advances only its own watermark and an abort
    /// loses nothing. This also relocates query_store's cutoff computation into the per-item loop, which
    /// is exactly where the catch-up clamp (<see cref="WatermarkPolicy"/>) must be applied — and
    /// since #1836 the clamp is applied by <see cref="BuildCutoffParameters"/> itself, so it holds on the
    /// Azure per-database path too, where the host reads this same per-database watermark but does not
    /// clamp (that branch is shared with the XE collectors, which must not be clamped).
    /// </summary>
    public override string? PerDatabaseWatermarkColumn => "database_name";

    /// <summary>Host warns when a per-database read hits the row backstop (see <see cref="MaxRowsPerDatabase"/>).</summary>
    public override int? PerItemRowCountWarnThreshold => MaxRowsPerDatabase;

    /// <summary>The client-side per-database text byte budget the shared read loop enforces (see <see cref="MaxTextBytesPerDatabase"/>).</summary>
    public override int? PerItemTextByteBudget => MaxTextBytesPerDatabase;

    /// <summary>
    /// Azure SQL DB only (#1836): the host opens one connection per database and drives
    /// <see cref="BuildQuery"/>. Every other target — box SQL Server, RDS, and Managed Instance, all
    /// of which honor the cross-database <c>[db].sys.sp_executesql</c> reference — keeps the single
    /// connection and the enumeration/per-item pair. Same override, same reasoning, as
    /// <c>ProcedureStatsCollector</c> (#1833) and the five database-scoped siblings before it.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>
    /// The Azure SQL DB per-database query (#1836): the eligibility gate, then the shared payload,
    /// both against the CURRENT database — the one the host's per-database connection is attached to.
    /// Never reached on any other target: there the enumeration/per-item pair drives the cycle, and
    /// this throws to say so rather than silently collecting the connection's own catalog (master,
    /// when the server entry leaves its Database field blank) as if it were the whole instance.
    /// </summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException("query_store enumerates databases on this target; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(
            AzureEligibilityGateText + BuildPayloadBody(context),
            BuildCutoffParameters(context));
    }

    /// <summary>
    /// The Azure SQL DB backfill slice (#2058, the Azure arm of #2022): the same eligibility gate and
    /// the same backfill-shaped payload as <see cref="BuildBackfillPerItemQuery"/>, executed verbatim
    /// on the host's per-database connection — Azure SQL DB rejects the on-prem
    /// <c>[db].sys.sp_executesql</c> nesting (#1836), so the window travels as command parameters
    /// instead. Same exclusive (floor, ceiling) window contract; the caller reads through
    /// <see cref="ReadAsync"/> with <see cref="CollectorContext.CurrentDatabaseName"/> set, exactly
    /// like the live Azure path. Throws off-Azure for the same reason <see cref="BuildQuery"/> does:
    /// silently collecting the connection's own catalog is worse than a loud wrong-path error.
    /// </summary>
    public CollectorQuery BuildBackfillQuery(CollectorContext context, DateTime floorUtc, DateTime ceilingUtc)
    {
        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException("query_store backfills per enumerated database on this target; BuildBackfillPerItemQuery drives the slice.");
        }

        return new CollectorQuery(
            AzureEligibilityGateText + BuildPayloadBody(context, backfill: true),
            new List<CollectorParameter>
            {
                new("@floor_time", floorUtc, CollectorParameterType.DateTime2),
                new("@ceiling_time", ceilingUtc, CollectorParameterType.DateTime2),
            });
    }

    /// <summary>
    /// On-prem / RDS / Managed Instance only: list the databases whose Query Store is usable, then
    /// collect each through <see cref="BuildPerItemQuery"/>. Null on Azure SQL DB — enumeration is not
    /// how that target is collected (<see cref="RunsPerDatabase"/>), and the Azure cursor this replaced
    /// could never return an item anyway: it probed each candidate through
    /// <c>QUOTENAME(@db) + N'.sys.sp_executesql'</c>, the cross-database reference Azure SQL DB rejects
    /// for every database, into an empty CATCH (#1836). Deleted rather than left unreachable, so it
    /// cannot be revived by a future edit that flips the RunsPerDatabase gate.
    /// </summary>
    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        var text = OnPremDatabaseListQueryText
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override CollectorQuery? BuildEnumerationProbe(CollectorContext context)
        => new(ProductVersionProbeText);

    /// <summary>
    /// The per-database Query Store payload — the ONE body both execution paths run (#1836). It is
    /// written as ordinary single-quoted T-SQL because that is what Azure SQL DB's per-database
    /// connection executes verbatim; <see cref="BuildPerItemQuery"/> quote-doubles the same string to
    /// nest it inside <c>[db].sys.sp_executesql N'...'</c> for on-prem. Both forms therefore select
    /// the identical 55 reader ordinals in the identical order, which is the whole point: this method
    /// is what stops the two paths from drifting into two column sets that one shared
    /// <see cref="ReadItemAsync"/> then mis-reads.
    ///
    /// <para>References <c>@cutoff_time</c>, supplied by <see cref="BuildCutoffParameters"/> — as an
    /// sp_executesql parameter on-prem, as a command parameter on Azure. Contains no double quotes and
    /// no braces, so it survives both the interpolation here and the escaping there unchanged.</para>
    ///
    /// <para>With <paramref name="backfill"/> true (#2022 phase 2), the SAME body flips its window and
    /// direction: the one-sided live cutoff (<c>&gt; @cutoff_time</c>) becomes the two-sided
    /// <c>&gt; @floor_time AND &lt; @ceiling_time</c> (interval pre-filter and HAVING both), and the ship
    /// order becomes <c>DESC</c> — newest history first, walking DOWN from the live path's floor. Strict
    /// <c>&lt;</c> on the ceiling is correct for the same reason the live path's strict <c>&gt;</c> is:
    /// both bounded cuts (TOP ... WITH TIES and the client byte budget) complete the boundary tie group,
    /// so nothing sharing a shipped boundary last_execution_time is ever left stranded behind the strict
    /// comparison — the #1960 invariant, mirror-imaged. Everything else (columns, slice aggregation,
    /// version gates) is byte-identical, so the reader contract cannot drift between live and backfill.</para>
    /// </summary>
    internal static string BuildPayloadBody(CollectorContext context, bool backfill = false, string? databaseName = null)
    {
        /* Detect server version for version-gated columns.
           isNew = true for SQL Server 2017+ (product version > 13) or Azure SQL DB/MI.
           Controls: avg_num_physical_io_reads, avg_log_bytes_used, avg_tempdb_space_used, plan_forcing_type_desc.
           hasPlanType = true for SQL Server 2022+ (product version >= 16), and on Azure SQL DB/MI.
           Controls: plan_type_desc. */
        var productVersion = context.EnumerationProbeResult is null
            ? DefaultProductVersion
            : Convert.ToInt32(context.EnumerationProbeResult, CultureInfo.InvariantCulture);
        bool isNew = productVersion > 13 || context.Target.IsAzureSqlDb || context.Target.IsAzureManagedInstance;

        /* plan_type_desc: version-gated on box SQL Server, but ALWAYS on for Azure SQL DB, which the
           version probe cannot speak for — it reports PRODUCTVERSION major 12 (the same reason isNew
           overrides above), while the engine underneath is evergreen and never older than 2022. The
           column's own catalog-view page lists Azure SQL Database in its Applies-to banner and names
           exactly one platform where referencing it errors — Azure Synapse Analytics, engine edition 6,
           which is never IsAzureSqlDb (edition 5).

           Managed Instance is ON as of #1886, on the same live-evidence basis Azure SQL DB was flipped on
           and NOT by pattern-matching the Azure change — see the replica-attribution comment below for
           the full probe, which answered both gates in one session. The short form: plan_type_desc binds
           on MI (COL_LENGTH = 120), measured on an instance following the CONSERVATIVE update policy. */
        bool hasPlanType = productVersion >= 16 || context.Target.IsAzureSqlDb || context.Target.IsAzureManagedInstance;

        /* Replica attribution — SQL Server 2022+ (product version >= 16). Controls: replica_role.

           "Query Store for secondary replicas" (2022+) gives an AG ONE shared Query Store that lives
           on the PRIMARY: secondary-replica workload is streamed to the primary and persisted in the
           primary's QS tables, distinguishable only by replica_group_id. We collect from primaries
           only (is_primary_replica = 1, in the enumeration cursor) — which is correct and stays — but
           without this attribution the primary's "Top Queries by CPU" silently BLENDS secondary
           workload into the primary's own numbers, with no way for a reader to tell. (Microsoft's own
           Query Performance Insight has this exact bug.)

           Gated on >= 16, NOT the docs' claimed 2025+: sys.query_store_replicas and
           sys.query_store_runtime_stats.replica_group_id both verified present on SQL 2022
           (16.0.4255.1) and SQL 2025 (17.0.4045.5).

           LEFT JOIN, deliberately: on a 2022 standalone (non-AG) server sys.query_store_replicas has
           ZERO rows, yet real runtime-stats rows still carry replica_group_id = 1. An INNER JOIN — or
           a WHERE replica_name = 'Primary' filter — would match nothing and silently delete ALL Query
           Store collection on every 2022 standalone server. Do not "tighten" this.

           replica_name is read DIRECTLY rather than mapped from role_type: contrary to the docs, it IS
           populated on box SQL Server (observed: Primary, Secondary, Geo Secondary, Geo HA Secondary),
           and the docs' sample CASEs replica_group_id as though it were role_type — it is not, it is a
           replica SET number that accumulates per role across failovers.

           Resulting replica_role: NULL on a 2022 standalone, 'Primary' on a 2025 standalone, the actual
           role on an AG with the feature enabled. NULL honestly means "the server did not attribute
           this row" and is deliberately NOT coalesced to an invented value.

           Azure SQL DB is ON, riding the same "Azure means newest" rule plan_type_desc gets above —
           but only because the live probe the previous version of this comment demanded was actually
           run. It was gated OFF from #1836 until then, and that carve-out was about bind SAFETY, not
           taste: a missing column is not a NULL, it fails the whole SELECT for that database, and on
           Azure this collector's per-database loop would then fail in EVERY database — a worse outcome
           than declining the attribution. The docs still do not settle it (replica_group_id's
           applies-to note names only "SQL Server (Starting with SQL Server 2022 (16.x))", and Query
           Store for secondary replicas is documented as unavailable on the Hyperscale service tier,
           silent on whether the view and column still BIND there), so this is flipped on live evidence
           instead — gathered on both service tiers the old comment worried about, 2026-07-31 UTC, both
           running Microsoft SQL Azure (RTM) 12.0.2000.8, EngineEdition 5:

             - The exact probe the old comment specified, answered on both: General Purpose
               (GP_S_Gen5_1, #1848) and Hyperscale (HS_S_Gen5_2, #1872) each returned
               OBJECT_ID('sys.query_store_replicas') = -660 and
               COL_LENGTH('sys.query_store_runtime_stats', 'replica_group_id') = 8.
             - sys.query_store_replicas binds on both and holds the same 4 role rows box SQL Server has
               (Primary, Secondary, Geo Secondary, Geo HA Secondary) — a static enumeration of ROLES,
               not instances, which is why a database with 0 HA replicas does not empty it. Every
               runtime-stats row carries replica_group_id = 1 and LEFT JOINs cleanly to 'Primary'.
             - Decisive (#1872): the full 55-column payload composed with hasReplicaAttribution = true —
               the exact text this method emits — executed on Hyperscale. 55 of 55 columns bound, exit
               0, and replica_role came back 'Primary' on every row.

           A per-tier gate was considered and rejected: Hyperscale reports EngineEdition 5, the same as
           General Purpose, so the collector cannot tell the tiers apart at query-build time. It does
           not need to — both bind identically.

           Managed Instance is ON as of #1886 — the probe the previous version of this comment demanded
           was run, so nobody needs to provision an MI to re-answer this. MI was held back through #1844
           and #1872 for a reason that does NOT apply to Azure SQL DB and had to be retired on its own
           terms: MI is not evergreen. Its feature set follows a per-instance UPDATE POLICY, so "Azure
           means 2022+" is a claim about the fleet that does not transfer to a specific instance, and an
           MI on an older policy genuinely might not have the catalog. The bar #1886 set for the simple
           edition gate was therefore stricter than Azure's: the catalog must be present on the OLDEST
           update policy still in support, not merely on whatever instance was to hand.

           Measured 2026-07-31 on a GPv2 Gen5 4-vCore Managed Instance, westus3, provisioned for the run
           and torn down after — reporting ProductVersion 12.0.2000.8, EngineEdition 8, and crucially
           SERVERPROPERTY('ProductUpdateType') = 'CU', i.e. the CONSERVATIVE (SQL Server 2022) update
           policy rather than Always-up-to-date. That is exactly the "oldest policy still in support"
           case, so the bar is met without an AlwaysUpToDate instance: catalog presence is a 2022-surface
           fact, not an evergreen one.

             - OBJECT_ID('sys.query_store_replicas') = -660 and
               COL_LENGTH('sys.query_store_runtime_stats', 'replica_group_id') = 8 — the same two-value
               probe #1848/#1872 ran on Azure SQL DB, non-NULL on both counts.
             - Answered THROUGH THE COLLECTOR'S OWN MECHANISM, not just in master: the same two values
               came back from a user-database context via [db].sys.sp_executesql (msdb standing in, since
               these catalog views are per-database). MI takes the on-prem enumeration path, so binding in
               master would not have settled it — that path is what the issue said MI lacked.
             - COL_LENGTH('sys.query_store_plan', 'plan_type_desc') = 120, which is what flips the
               hasPlanType carve-out above in the same session rather than provisioning MI twice.

           A standalone MI's sys.query_store_replicas is an EMPTY enumeration — zero rows — unlike Azure
           SQL DB's, which is a static 4-row roles table even with no replicas present. That difference
           is worth stating because it looks alarming and is not: BINDING is the collection-safety
           question and the answer is yes, while an empty enumeration only means a GP instance with no
           read replicas has nothing to attribute. The LEFT JOIN below is what makes that harmless — it
           is the same shape that keeps a 2022 standalone (whose view is also empty) collecting, and
           tightening it would break both. replica_role simply reads NULL there, which is the honest
           state rather than an invented one. */
        bool hasReplicaAttribution = productVersion >= 16 || context.Target.IsAzureSqlDb || context.Target.IsAzureManagedInstance;

        /* Build version-conditional column fragments for the Query Store query.
           None of these contain a single quote, so they splice into the body identically whether the
           body stays as written (Azure) or gets quote-doubled for sp_executesql nesting (on-prem).

           Each version-gated family now needs TWO fragments (#1907): one INSIDE the slice-aggregating
           derived table, which must vanish entirely when the columns do not exist (referencing an
           unbound column inside an aggregate fails the whole SELECT exactly as it would outside one),
           and one in the OUTER projection, which keeps emitting the typed NULL placeholder at the same
           ordinal so the 55-column reader contract never moves. The inner fragments carry a LEADING
           comma and sit at the END of the inner select list precisely because they can be empty; the
           outer ones keep their original trailing-comma form because they are never empty. */
        string numPhysIoReadsAgg = isNew
            ? $",\n    {WeightedAverage("avg_num_physical_io_reads")},\n    min_num_physical_io_reads = MIN(qsrs.min_num_physical_io_reads),\n    max_num_physical_io_reads = MAX(qsrs.max_num_physical_io_reads)"
            : "";

        string logBytesAgg = isNew
            ? $",\n    {WeightedAverage("avg_log_bytes_used")},\n    min_log_bytes_used = MIN(qsrs.min_log_bytes_used),\n    max_log_bytes_used = MAX(qsrs.max_log_bytes_used)"
            : "";

        string tempdbAgg = isNew
            ? $",\n    {WeightedAverage("avg_tempdb_space_used")},\n    min_tempdb_space_used = MIN(qsrs.min_tempdb_space_used),\n    max_tempdb_space_used = MAX(qsrs.max_tempdb_space_used)"
            : "";

        string numPhysIoReadsCols = isNew
            ? "qsrs.avg_num_physical_io_reads, qsrs.min_num_physical_io_reads, qsrs.max_num_physical_io_reads,"
            : "avg_num_physical_io_reads = NULL, min_num_physical_io_reads = NULL, max_num_physical_io_reads = NULL,";

        string logBytesCols = isNew
            ? "avg_log_bytes_used = qsrs.avg_log_bytes_used, min_log_bytes_used = qsrs.min_log_bytes_used, max_log_bytes_used = qsrs.max_log_bytes_used,"
            : "avg_log_bytes_used = NULL, min_log_bytes_used = NULL, max_log_bytes_used = NULL,";

        string tempdbCols = isNew
            ? "avg_tempdb_space_used = qsrs.avg_tempdb_space_used, min_tempdb_space_used = qsrs.min_tempdb_space_used, max_tempdb_space_used = qsrs.max_tempdb_space_used,"
            : "avg_tempdb_space_used = NULL, min_tempdb_space_used = NULL, max_tempdb_space_used = NULL,";

        string planForcingCol = isNew
            ? "plan_forcing_type = qsp.plan_forcing_type_desc,"
            : "plan_forcing_type = NULL,";

        string planTypeCol = hasPlanType
            ? "plan_type = qsp.plan_type_desc,"
            : "plan_type = NULL,";

        /* Execution-plan capture — mirrors the full Dashboard's @collect_plan path in
           install/09_collect_query_store.sql: CONVERT(nvarchar(max), qsp.query_plan) from
           sys.query_store_plan, no size guard. On only when the host sets CapturePlanXml (Darling);
           off = the nvarchar(1) NULL placeholder (Lite), byte-identical to the no-plan form. No
           single quotes, so it splices straight into the sp_executesql body.

           #1556 plan-text dedupe (ON branch only): a plan is landed ONCE per plan_id per cycle — on its
           newest runtime-stats interval in the window (rn = 1) — and NULL on the older intervals, instead
           of repeating the full plan XML on every interval row of the same plan. The partition ORDER BY
           stays DESC even though the outer sort is now ASC (#1960): under oldest-first shipping a plan's
           rn = 1 row sorts LAST among its rows, so a bounded cycle can cut before it and ship that plan's
           intervals without XML — harmless, because rn is recomputed over the NEXT cycle's window, whose
           newest-in-window row carries the XML then; steady-state cycles see one interval per plan and are
           unaffected. The consumers tolerate the per-row NULL — Lite selects NULL for the grid and fetches
           plans live, and Darling's stored-plan readers all guard `query_plan_text IS NOT NULL`. Not
           mirrored into the Dashboard proc: its "Download Plan" reads by exact collection_id, where
           per-row NULLs would break a real reader. */
        /* #2312: this runtime-stats query no longer carries plan XML at all — the #2164 ROW_NUMBER
           gate and its in-stream watermark predicate were DELETED by #2210, and #2312 then retired
           the watermark itself: the #2164 KNOWN GAP's "exact fix is a store-DERIVED watermark" is
           what the activity-driven fetch now IS. BuildPlanFetchByIdsQuery is the only thing that
           reads plan XML: the host probes its own store for the plans this cycle's rows reference
           and fetches exactly the missing ones, so each plan lands ONCE per database lifetime, a
           dormant plan resuming execution is fetched the cycle it resumes (no refresh horizon to
           wait out), and a caught-up database issues no fetch at all. The shape #2210 replaced
           re-shipped every plan on every pass forever — measured at 5.0x redundancy (871,196
           plan-XML rows against 175,328 distinct database/plan pairs in a day, on a 33 GB table).
           Both branches below emit the same placeholder, so the payload is byte-identical to Lite's
           regardless of the flag, and CapturePlanXml gates the separate by-ids fetch rather than
           this query. Existing inline rows are NOT migrated and stay readable via the reader's
           NULL-guarded fallback; backfill plan XML stays on its own rows for the same reason it
           always did — those intervals' plans are never in the live cycle's reference set. */
        const string planTextCol = "query_plan_text = CONVERT(nvarchar(1), NULL),";

        /* #2150: the LAST nvarchar(max) in this projection, and now the whole remaining cost of it. The cap
           and ship order sit above these joins, so a Top-N Sort carries the text through the sort and reads
           all of its input before emitting row one — choosing 50,000 rows materialized text for the entire
           qualifying set. Measured with #2210's plan XML already gone and this column as the only
           difference: time-to-first-row 4.67s vs 0.45s at 1,505 rows, 5.02s vs 0.57s at 4,037. Neither knob
           bounds it (TOP (500) == TOP (50000); wall time flat from a 4 MB to a 256 MB client budget).

           Gated rather than removed, because Lite stores this text inline in DuckDB and reads it from
           there — nulling it unconditionally would blind Lite, which is why this is a host flag and not a
           deletion. The ORDINAL is identical either way, the same discipline the version-gated columns
           above follow, so a host that has not built text storage is byte-compatible.

           The query_text JOIN is now DROPPED for Darling (FetchQueryTextSeparately): the column is nulled
           and text is pulled by-ids in BuildTextFetchByIdsQuery, so this join fed nothing. Measured on a
           40,388-plan / 3-interval catalog (SQL 2025, warm): the final SELECT fell 398ms -> 351ms (-12%)
           with the join removed, and it is provably non-filtering (qsq.query_text_id keys exactly one qst
           row), so the row set is unchanged. Lite keeps the join because it reads qst.query_sql_text inline. */
        string queryTextCol = context.FetchQueryTextSeparately
            ? "query_sql_text = CONVERT(nvarchar(1), NULL),"
            : "query_sql_text = qst.query_sql_text,";

        /* Gated on the SAME flag as the column above: the qst join is present only when Lite consumes
           qst.query_sql_text inline; Darling drops it (text arrives via the separate by-ids fetch). */
        string queryTextJoin = context.FetchQueryTextSeparately
            ? ""
            : "JOIN sys.query_store_query_text AS qst\n  ON qst.query_text_id = qsq.query_text_id\n";

        /* The replica-attribution column + its join (see hasReplicaAttribution above). Selected after every
           version-gated column, so pre-2022 targets read the nvarchar(1) NULL placeholder at the same
           ordinal — byte-identical shape to the attributed form. The interval-identity pair (#1841 tier 2)
           follows it and is NOT version-gated, so this fragment stays comma-free and the template supplies
           the separator. */
        string replicaRoleCol = hasReplicaAttribution
            ? "replica_role = qsr.replica_name"
            : "replica_role = CONVERT(nvarchar(1), NULL)";

        string replicaJoin = hasReplicaAttribution
            ? "LEFT JOIN sys.query_store_replicas AS qsr\n  ON qsr.replica_group_id = qsrs.replica_group_id"
            : "";

        /* replica_group_id is part of sys.query_store_runtime_stats' natural key, so it belongs in the
           slice-aggregation grouping (#1907) — two replicas' rows for one interval are DIFFERENT work,
           not slices of the same work, and summing them together would blend a secondary's executions
           into the primary's, re-creating by hand the exact bug replica attribution exists to prevent.
           It carries the SAME 2022+/Azure gate as the attribution column above, and for the same
           bind-safety reason: the column does not exist on older servers, and naming it in a GROUP BY
           fails the whole SELECT just as naming it in a select list would. When the gate is off there is
           only ever one replica group to begin with, so dropping it from the key changes no grouping.
           Leading comma: it splices into both the inner select list and the GROUP BY, and is empty on
           targets without the column. */
        string replicaGroupKey = hasReplicaAttribution
            ? ",\n    qsrs.replica_group_id"
            : "";

        /* There is deliberately NO self-exclusion predicate in this query (#1565, actual-plan evidence
           from a 103k-row burst). The old form (query_sql_text NOT LIKE N'%marker%') was 75% of the
           query's total elapsed time — a per-row substring scan over full nvarchar(max) text (11.2s of
           a 14.9s read; the field A/B measured 4.3x faster without it) — and no predicate shape fixes
           that server-side: the QS internal text table has no index on the column, so every variant is
           a residual scan whose cost is the bytes it reads. The exclusion instead happens in the read
           loop, where the query text is ALREADY materialized for every row — a client-side Contains at
           zero SQL cost, identical semantics. Self rows cross the wire (~2% of a busy database's rows)
           and are dropped before they enter the batch (never stored, never counted against the byte
           budget). The query still CONTAINS the marker, in its own leading comment. */

        /* Interval identity (#1841 tier 2), the last two SELECT items. Not version-gated: both
           sys.query_store_runtime_stats.runtime_stats_interval_id and the
           sys.query_store_runtime_stats_interval catalog view are original Query Store surface, verified
           present on SQL Server 2016 SP3 (13.0.6300.2) — the collector's own AppliesTo floor — so there is
           no target this collector runs on that lacks them.

           LEFT JOIN, not JOIN, for the same reason the replica join above is one: an INNER JOIN here would
           make every runtime-stats row's survival depend on its interval row resolving, and a Query Store
           that trimmed an interval row out from under us would silently delete real collection rather than
           lose one column. The id comes off qsrs directly and is unaffected either way; only
           interval_start_time_utc goes NULL if the join misses.

           start_time is datetimeoffset. AT TIME ZONE 'UTC' re-expresses it at +00:00 and the CONVERT drops
           the offset, so the stored value is naive UTC — the same clock as collection_time and as
           first_execution_time (which ReadRowsAsync already normalizes via DateTimeOffset.UtcDateTime).
           That is what makes it safe to bucket on: it is NOT the monitored server's local wall clock.
           AT TIME ZONE is SQL Server 2016+, matching the floor above, and the expression contains no
           single quote... except the timezone literal, which quote-doubles cleanly for the sp_executesql
           nesting exactly like the rest of the body. */

        /* Slice aggregation (#1907) — the derived table below, and the reason this query has one.

           sys.query_store_runtime_stats returns the FLUSHED slice and the still-IN-MEMORY slice of one
           runtime_stats_interval_id as SEPARATE ROWS, and they are ADDITIVE members of one interval, not
           competing snapshots of it. Verified on box SQL Server 2022 (16.0.4255.1) as well as the Azure
           SQL Database where it was found: 100 executions flushed + 25 executions in memory came back as
           two rows, and sys.dm_exec_procedure_stats — an entirely separate source, same instant —
           reported 125. SUM matches; the larger slice alone (100) does not. With a 900s default flush
           against a 3600s default interval, ONE interval can hold several flushed slices, so the count
           is not bounded at two.

           Selecting them straight through stored both, and they then shared every column of the
           read-side dedup key (#1841/#1845/#1853) AND collection_time, so the ROW_NUMBER survivor and the
           CAGGs' last() were decided by whichever row the engine happened to emit first — a grid could
           show the in-memory sliver (8) where the interval's truth was 94. The dedup itself is correct
           and stays: it exists to collapse RE-COLLECTIONS of one interval across cycles. It just cannot
           also be asked to ADD two slices within one cycle, and no read-side rule can express both.

           So the slices are combined HERE, where the identity is unambiguous, keyed on exactly the
           natural key of the view — (plan_id, runtime_stats_interval_id, execution_type, replica_group) —
           and one interval now yields at most one row per cycle. The EMITTED ROW SHAPE is unchanged: the
           same 55 columns in the same order, only fewer rows, so the positional writers and every
           downstream reader are untouched.

           How each column combines:
             - count_executions      SUM — the additive counter itself.
             - avg_*                 the count-WEIGHTED mean, SUM(avg * count) / SUM(count). Query Store
                                     stores avg and count, never a total, so avg * count reconstructs
                                     each slice's total exactly and the quotient is the interval's true
                                     average. A plain AVG() of the slice averages would weight a 25-
                                     execution sliver equally with a 100-execution flush. NULLIF guards
                                     the divide-by-zero rather than letting a zero-execution row (which
                                     should not exist, and would still not be worth failing a whole
                                     database's collection over) raise 8134.
             - min_* / max_*         MIN / MAX — extremes over a union of slices are the extremes of the
                                     slice extremes. Includes min_dop / max_dop, which have no avg.
             - first_execution_time  MIN, last_execution_time MAX — the interval's own span. Both slices
                                     of a pair share first_execution_time in practice, which is exactly
                                     why the tier-1 proxy key could not tell them apart either.

           The incremental filter moves from WHERE to HAVING, and that is load-bearing rather than
           cosmetic. A per-slice WHERE would break the SUM within one cycle of the fix: the flushed slice
           is STATIC, so once the growing in-memory slice pushes the watermark past the flushed slice's
           last_execution_time, the flushed slice stops qualifying and the "sum" becomes the sliver alone
           — the original bug with extra steps. HAVING MAX(last_execution_time) > @cutoff_time asks the
           question at interval grain: has this interval seen new activity, and if so give me ALL of it.
           It is strictly more permissive than the old per-slice predicate, so nothing that used to be
           collected stops being collected.

           The IN (...) pre-filter is a performance prune, not a semantic one — the HAVING already gives
           the exact answer, and the pre-filter's interval list is by construction a superset of the
           intervals the HAVING can keep, so it can never subtract a row. It is here because without it
           the aggregate has to run over the database's ENTIRE retained Query Store every cycle, which is
           the one shape that made this materially slower. Measured on a real 212k-row Query Store
           (SQL 2025), full 55-column payload, warm, three runs: pre-fix 453/485/516 ms for 510 rows;
           post-fix 375/422/438 ms for 262 rows; post-fix WITHOUT the pre-filter 1203/1203/1235 ms. The
           fixed query is FASTER than the one it replaces despite the added aggregate, because half the
           rows means half the nvarchar(max) query text and plan XML to materialize and ship. */
        /* Oldest-first + WITH TIES (#1960): rows ship FORWARD from the watermark, so a bounded cycle
           (byte budget or this TOP) leaves the derived watermark — MAX(last_execution_time) over the
           rows actually stored — sitting exactly at the shipped boundary, and the next cycle's strict
           `> @cutoff_time` resumes there with no hole. WITH TIES is load-bearing for that invariant:
           a bare TOP could split a group of rows sharing the boundary last_execution_time, stranding
           the unshipped half behind the strict comparison forever. The client byte budget completes
           boundary groups the same way (see ReadRowsAsync). */
        /* Backfill (#2022) is the mirror image: newest-first DESC inside (floor, ceiling), where the
           ceiling is the DERIVED backfill boundary — MIN(last_execution_time) over the rows already
           stored for the database — so each bounded slice leaves the next ceiling sitting exactly at
           its oldest shipped row, and the next slice's strict `< @ceiling_time` resumes with no hole
           or re-ship. Same TIES, same budget, same tie-group completion; only the window and the
           direction differ. */
        /* The interval pre-filter resolves candidate interval ids from the INTERVAL CATALOG
           (sys.query_store_runtime_stats_interval, ~one row per interval of retained history — hundreds
           of rows) rather than from runtime_stats itself (#2133; measured on the field store: 20 ms vs
           426 ms for the identical id set). end_time/start_time are datetimeoffset; the datetime2
           parameters promote with a zero offset, i.e. as the UTC instants they are — the same implicit
           promotion the HAVING's last_execution_time comparison has always relied on. The catalog bound
           is a SUPERSET (an interval can end after the cutoff while all its rows are older); the HAVING
           below stays the exact row-level filter, so shipped semantics are unchanged. */
        /* #2312: the live path has two forms. Most cycles ship CLOSED intervals only — immutable, so
           final on first collection — and skip the OPEN interval's cumulative snapshot, which is the
           whole re-read bill on a big multi-tenant primary (40–110 s per run measured, every one of
           those snapshots but the latest discarded by the read side's rn = 1). The host opts a cycle
           back in via context.IncludeOpenInterval (default true = today's exact form) on the
           QueryStoreOpenIntervalState cadence. SYSUTCDATETIME() promotes to datetimeoffset with a zero
           offset against end_time — the same implicit UTC-instant promotion the @cutoff_time comparison
           has always relied on — and being server-evaluated it adds no parameter, so the
           single-parameter sp_executesql contract is unchanged. Correctness of the skip leans on the
           cumulative-snapshot contract: a closed interval whose final content differs from our last
           open-snapshot must carry executions newer than the watermark, so the standing HAVING readmits
           it; one whose content did not change IS our last snapshot. */
        var intervalPreFilter = backfill
            ? @"i.end_time > @floor_time
    AND   i.start_time < @ceiling_time"
            : context.IncludeOpenInterval
                ? "i.end_time > @cutoff_time"
                : @"i.end_time > @cutoff_time
    AND   i.end_time <= SYSUTCDATETIME()";
        var intervalHaving = backfill
            ? @"MAX(qsrs.last_execution_time) > @floor_time
    AND MAX(qsrs.last_execution_time) < @ceiling_time"
            : "MAX(qsrs.last_execution_time) > @cutoff_time";
        var shipOrder = backfill ? "DESC" : "ASC";

        /* STAGED, not monolithic (#2133). Joining the slice aggregate straight into the
           query_store_plan/query/text TVFs handed the optimizer nothing but fixed-guess cardinalities,
           and the shape it picked re-materialized a TVF per probe — a fixed cost no window width could
           reduce. Field bisection on an 82k-plan catalog (echo, SQL 2022): the aggregate alone ran in
           81 ms and each TVF scanned bare in ~300 ms, yet aggregate-JOIN-qsp could not finish in 30 s,
           hinted or not; staged through the temp the same work totaled 524 ms (56 stage + 409 join).
           That fixed cost is what wedged the big-catalog databases at EVERY catch-up width and made
           #2125's shrink floor-pin instead of converge. The temp gives the final join REAL row counts —
           and for that reason the old LOOP JOIN hint must NOT return: looping from the temp into the
           TVFs is the same per-probe re-materialization by another name; the 524 ms join is unhinted,
           chosen by the optimizer from true cardinalities. sp_QuickieStore stages for the same reason.

           Batch mechanics: SELECT INTO emits no result set, so the batch still returns exactly ONE
           result set (the reader/byte-budget contract). Inside the on-prem [db].sys.sp_executesql
           nesting the temp's scope dies with the invocation; on Azure's direct per-database path the
           leading DROP TABLE IF EXISTS covers pooled-connection reuse. TOP ... WITH TIES, the ship
           order, and the derived-watermark semantics live on the final SELECT, unchanged.

           BOTH statements carry OPTION(RECOMPILE) (review catch): split out on its own, the staging
           statement would otherwise be cached via sp_executesql's parameterized text and sniffed
           across live vs backfill windows of wildly different selectivity — the same fixed-guess
           failure mode this rewrite removes, reintroduced one statement earlier. */
        return $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DROP TABLE IF EXISTS #pm_qs_slice;

SELECT /* PerformanceMonitorLite */
    qsrs.plan_id,
    qsrs.runtime_stats_interval_id,
    qsrs.execution_type_desc{replicaGroupKey},
    first_execution_time = MIN(qsrs.first_execution_time),
    last_execution_time = MAX(qsrs.last_execution_time),
    count_executions = SUM(qsrs.count_executions),
    {WeightedAverage("avg_duration")},
    min_duration = MIN(qsrs.min_duration),
    max_duration = MAX(qsrs.max_duration),
    {WeightedAverage("avg_cpu_time")},
    min_cpu_time = MIN(qsrs.min_cpu_time),
    max_cpu_time = MAX(qsrs.max_cpu_time),
    {WeightedAverage("avg_logical_io_reads")},
    min_logical_io_reads = MIN(qsrs.min_logical_io_reads),
    max_logical_io_reads = MAX(qsrs.max_logical_io_reads),
    {WeightedAverage("avg_logical_io_writes")},
    min_logical_io_writes = MIN(qsrs.min_logical_io_writes),
    max_logical_io_writes = MAX(qsrs.max_logical_io_writes),
    {WeightedAverage("avg_physical_io_reads")},
    min_physical_io_reads = MIN(qsrs.min_physical_io_reads),
    max_physical_io_reads = MAX(qsrs.max_physical_io_reads),
    {WeightedAverage("avg_clr_time")},
    min_clr_time = MIN(qsrs.min_clr_time),
    max_clr_time = MAX(qsrs.max_clr_time),
    min_dop = MIN(qsrs.min_dop),
    max_dop = MAX(qsrs.max_dop),
    {WeightedAverage("avg_query_max_used_memory")},
    min_query_max_used_memory = MIN(qsrs.min_query_max_used_memory),
    max_query_max_used_memory = MAX(qsrs.max_query_max_used_memory),
    {WeightedAverage("avg_rowcount")},
    min_rowcount = MIN(qsrs.min_rowcount),
    max_rowcount = MAX(qsrs.max_rowcount){numPhysIoReadsAgg}{logBytesAgg}{tempdbAgg}
INTO #pm_qs_slice
FROM sys.query_store_runtime_stats AS qsrs
WHERE qsrs.runtime_stats_interval_id IN
(
    SELECT
        i.runtime_stats_interval_id
    FROM sys.query_store_runtime_stats_interval AS i
    WHERE {intervalPreFilter}
)
GROUP BY
    qsrs.plan_id,
    qsrs.runtime_stats_interval_id,
    qsrs.execution_type_desc{replicaGroupKey}
HAVING
    {intervalHaving}
OPTION(RECOMPILE);

SELECT /* PerformanceMonitorLite */ TOP ({MaxRowsPerDatabase}) WITH TIES
    query_id = qsq.query_id,
    plan_id = qsp.plan_id,
    execution_type_desc = qsrs.execution_type_desc,
    first_execution_time = qsrs.first_execution_time,
    last_execution_time = qsrs.last_execution_time,
    module_name =
        CASE
            WHEN qsq.object_id = 0
            THEN N'Adhoc'
            ELSE COALESCE(
                OBJECT_SCHEMA_NAME(qsq.object_id) + N'.' + OBJECT_NAME(qsq.object_id),
                N'Unknown')
        END,
    {queryTextCol}
    query_hash = CONVERT(varchar(64), qsq.query_hash, 1),
    count_executions = qsrs.count_executions,
    avg_duration = qsrs.avg_duration,
    min_duration = qsrs.min_duration,
    max_duration = qsrs.max_duration,
    avg_cpu_time = qsrs.avg_cpu_time,
    min_cpu_time = qsrs.min_cpu_time,
    max_cpu_time = qsrs.max_cpu_time,
    avg_logical_io_reads = qsrs.avg_logical_io_reads,
    min_logical_io_reads = qsrs.min_logical_io_reads,
    max_logical_io_reads = qsrs.max_logical_io_reads,
    avg_logical_io_writes = qsrs.avg_logical_io_writes,
    min_logical_io_writes = qsrs.min_logical_io_writes,
    max_logical_io_writes = qsrs.max_logical_io_writes,
    avg_physical_io_reads = qsrs.avg_physical_io_reads,
    min_physical_io_reads = qsrs.min_physical_io_reads,
    max_physical_io_reads = qsrs.max_physical_io_reads,
    avg_clr_time = qsrs.avg_clr_time,
    min_clr_time = qsrs.min_clr_time,
    max_clr_time = qsrs.max_clr_time,
    min_dop = qsrs.min_dop,
    max_dop = qsrs.max_dop,
    avg_query_max_used_memory = qsrs.avg_query_max_used_memory,
    min_query_max_used_memory = qsrs.min_query_max_used_memory,
    max_query_max_used_memory = qsrs.max_query_max_used_memory,
    avg_rowcount = qsrs.avg_rowcount,
    min_rowcount = qsrs.min_rowcount,
    max_rowcount = qsrs.max_rowcount,
    {numPhysIoReadsCols}
    {logBytesCols}
    {tempdbCols}
    {planTypeCol}
    {planForcingCol}
    is_forced_plan = qsp.is_forced_plan,
    force_failure_count = qsp.force_failure_count,
    last_force_failure_reason = qsp.last_force_failure_reason_desc,
    compatibility_level = qsp.compatibility_level,
    {planTextCol}
    query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),
    {replicaRoleCol},
    runtime_stats_interval_id = qsrs.runtime_stats_interval_id,
    interval_start_time_utc = CONVERT(datetime2, qsrsi.start_time AT TIME ZONE 'UTC')
FROM #pm_qs_slice AS qsrs
JOIN sys.query_store_plan AS qsp
  ON qsp.plan_id = qsrs.plan_id
JOIN sys.query_store_query AS qsq
  ON qsq.query_id = qsp.query_id
{queryTextJoin}LEFT JOIN sys.query_store_runtime_stats_interval AS qsrsi
  ON qsrsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
{replicaJoin}
ORDER BY qsrs.last_execution_time {shipOrder}
OPTION(RECOMPILE);";
    }

    /// <summary>
    /// One slice-combining <c>avg_*</c> column for the aggregation in <see cref="BuildPayloadBody"/>
    /// (#1907): the count-WEIGHTED mean of the slices, aliased back to the column's own name so the
    /// outer projection's reference does not change.
    ///
    /// <para>Every <c>avg_*</c> column in the payload goes through here rather than being written out
    /// by hand, because the wrong form is not a compile error and not obviously wrong at a glance —
    /// a bare <c>AVG(qsrs.avg_x)</c> reads fine and silently weights a 25-execution sliver the same as a
    /// 100-execution flush. Query Store exposes an average and a count but never a total, so
    /// <c>avg * count</c> is how a slice's total is recovered, and the quotient of the summed totals
    /// over the summed counts is the interval's true average. Pinned by test: every <c>avg_</c> column
    /// the payload emits must match this shape.</para>
    /// </summary>
    private static string WeightedAverage(string column) =>
        $"{column} = SUM(qsrs.{column} * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0)";

    /// <summary>
    /// The incremental cutoff both paths bind as <c>@cutoff_time</c>: only runtime_stats intervals
    /// newer than what this database already has are fetched. Falls back to 60 minutes back when the
    /// database has nothing stored yet.
    ///
    /// <para>The catch-up clamp (<see cref="WatermarkPolicy"/>) is applied HERE, in the definition,
    /// rather than only in the host's enumeration loop (#1836). Query Store is the one
    /// unbounded-persisted source among the collectors — it retains ~30 days — so a service that was
    /// stopped for days must not come back and ask for the entire backlog in one cycle; that is the
    /// #1556 field incident. On Azure SQL DB the per-database cutoff is now computed on the host's
    /// generic per-database branch, which deliberately does NOT clamp (it also serves the XE
    /// ring-buffer collectors, where clamping would WRONGLY truncate legitimate catch-up), so binding
    /// the clamp to the collector instead of the path is what makes it hold on both. Applying it twice
    /// on the enumeration path is a no-op — clamping an already-clamped value returns it unchanged —
    /// and that path keeps its WARNING log, which is the operator-visible half.</para>
    ///
    /// <para>Expect a bursty cadence either way: Query Store buffers in memory and flushes to its
    /// persisted tables on DATA_FLUSH_INTERVAL_SECONDS (default 900s), so roughly every third 5-minute
    /// cycle returns a burst and the others return ~nothing. That is the SOURCE's behavior, not a
    /// watermark bug — do NOT "fix" it by narrowing the poll interval.</para>
    /// </summary>
    private static List<CollectorParameter> BuildCutoffParameters(CollectorContext context)
    {
        var clamped = WatermarkPolicy.ClampCatchup(context.Watermark, context.CollectionTime);

        /* Tell the host the clamp actually fired, so the hole it opens stays LOGGED rather than
           silent — the enumeration path logs its own clamp before we ever see the watermark, which is
           why this is false there (re-clamping an already-clamped value changes nothing) and true only
           on the Azure per-database path. Assigned unconditionally: the context is reused across every
           database in a cycle, so a stale true from the previous database would misreport this one. */
        context.CatchupClampApplied = context.Watermark.HasValue && clamped != context.Watermark;

        return new List<CollectorParameter>
        {
            new("@cutoff_time", clamped ?? context.CollectionTime.AddMinutes(-60), CollectorParameterType.DateTime2),
        };
    }

    /// <summary>
    /// On-prem / RDS / Managed Instance: the SAME body <see cref="BuildQuery"/> runs on Azure, only
    /// quote-doubled and nested inside <c>[db].sys.sp_executesql</c> so one connection can reach every
    /// database. The single <c>Replace</c> is the whole difference between the two paths' SQL — there
    /// is no second copy of the payload to keep in step (the IndexObjectStatsCollector precedent).
    /// </summary>
    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        /* Double single quotes so the body survives nesting inside [db].sys.sp_executesql N'...' */
        var escapedBody = BuildPayloadBody(context, databaseName: item).Replace("'", "''", StringComparison.Ordinal);
        var escapedDbName = item.Replace("]", "]]", StringComparison.Ordinal);

        var text = $@"
EXECUTE [{escapedDbName}].sys.sp_executesql
    N'{escapedBody}',
    N'@cutoff_time datetime2(7)',
    @cutoff_time;";

        return new CollectorQuery(text, BuildCutoffParameters(context));
    }

    /// <summary>
    /// The plan-XML fetch for one database (#2312 Finding 2): exactly the plans the caller names — the cycle's
    /// collected runtime rows whose XML the store does not already hold — in <c>plan_id</c> order, cut exactly
    /// by a running byte total. The STORE is the watermark: a caught-up database has an empty missing set and
    /// no fetch runs at all, which is the property the #2210 catalog walk lacked (measured 23s to discover
    /// "nothing new" on a warm catalog, every cycle, because the walk re-read the catalog to find out).
    ///
    /// <para>SEPARATE from the runtime-stats query on purpose, and that separation is still the fix rather than
    /// a refactor. The runtime query ships <c>ORDER BY qsrs.last_execution_time</c>, so a budget cut truncates
    /// it in TIME order — plan XML inline there re-shipped the same plans at 5.0x (measured, #2210). Here the
    /// caller hands an explicit id list, so a budget cut leaves ids that are simply STILL MISSING from the
    /// store, and the next cycle that references them (or the caller's own carry-over) re-selects them. No
    /// watermark, no suffix-safety argument, no out-of-order hazard.</para>
    ///
    /// <para>The candidate bound is the ID LIST ITSELF, which the caller caps via
    /// <see cref="QueryStorePlanXmlState.CandidatePlanCount"/> before building: the running total needs
    /// <c>DATALENGTH</c>, and <c>sys.query_store_plan.query_plan</c> is decompressed BY the view on access, so
    /// handing the whole missing set of a first-contact database in one statement would pay its entire
    /// decompression to enforce a budget meant to prevent exactly that. Chunking and capping are caller
    /// decisions; this builder's contract is only "the list you hand me is what I decompress".</para>
    ///
    /// <para><c>query_plan_hash</c> rides along in the SELECT — <c>CONVERT(varchar(64), ..., 1)</c>, the same
    /// rendering the runtime payload uses — because it reads WITHOUT decompressing the plan and the map stores
    /// it as the in-place-rewrite detector: a batch whose live hash differs from the stored one is the one case
    /// activity-driven fetch cannot see on its own.</para>
    ///
    /// <para>The budget test is <c>running_bytes - plan_bytes &lt; budget</c>, i.e. admit a plan when the total
    /// BEFORE it was still under. The obvious <c>running_bytes &lt;= budget</c> is a per-database STALL: a single
    /// plan larger than the whole budget has a running total that already exceeds it on its own row, so it is
    /// excluded, every later row is excluded too (the total is monotonic), the pass ships nothing, the plan
    /// stays missing, and the next pass re-selects it first — forever. One 13 MB plan against the 12 MB
    /// default is enough. Admitting the offender ships it alone and cuts after it; once landed it is never
    /// selected again.</para>
    ///
    /// <para>The honest cost of that: worst-case bytes for one pass are <c>budget + largest single plan</c>,
    /// not <c>budget</c>. The runtime-stats budget a few hundred lines up pays exactly the same price for the
    /// same reason (measured: 19.6 MB shipped against a 12 MB budget when one very large plan carried a pass
    /// past it), so "12 MB" is a floor on ship volume in both paths rather than a cap.</para>
    ///
    /// <para>The budget and the id list are inlined as parsed integers rather than parameters, and for the same
    /// reason as each other: the body nests inside <c>sp_executesql</c>, and the values are host-computed longs
    /// that never touch operator input.</para>
    ///
    /// <para>A NULL <c>query_plan</c> — a plan too large to persist, or certain forced-plan-failure paths —
    /// counts as ZERO bytes and STILL SHIPS, as a row with NULL text. Letting the NULL propagate through the
    /// arithmetic instead would make the budget predicate NULL and filter the row out, and the plan would be
    /// re-selected as missing forever. Shipping the row lets the writer record a content-less map row (the
    /// NULL-digest marker), which is what makes "the engine says this XML will never exist" a stored fact
    /// instead of a per-cycle rediscovery — the store's readers already guard
    /// <c>query_plan_text IS NOT NULL</c>, so absent content renders as absent either way.</para>
    ///
    /// <para>NEVER on the backfill path: backfill plan XML stays on its own rows, exactly as before — this
    /// fetch serves the live cycle's referenced plans and nothing else.</para>
    ///
    /// <para>The <c>CONVERT</c> happens ONCE, inside the candidate window, and the running total sums
    /// <c>DATALENGTH</c> of that converted text rather than of the view column. The alternative — measure with
    /// <c>DATALENGTH(qsp.query_plan)</c> in the window and join back to <c>sys.query_store_plan</c> for the text
    /// — decompresses every shipped plan TWICE, and that is measured, not reasoned: on a 73,163-plan production
    /// catalog, K=114 and a 12 MB budget, both shapes returned the same 114 rows and 1.7 MB, and the join-back
    /// form took 274ms cold / 262ms warm against 133ms for this one. Plan-id-only with no XML touched was 114ms,
    /// so this shape sits 19ms above the floor while the join-back form pays for the decompression twice.</para>
    ///
    /// <para>The obvious next idea — evaluate the byte budget against the COMPRESSED length first, so plans the
    /// budget will discard are never decompressed at all — is not available through this view, and that was
    /// measured rather than assumed (#2791). Over 512 candidate ids: selecting <c>plan_id</c> alone is 14ms,
    /// <c>DATALENGTH(qsp.query_plan)</c> is 321ms, and a full <c>CONVERT(nvarchar(max), ...)</c> is 331ms. The
    /// DATALENGTH form costs what the full decompression costs because the VIEW decompresses on any access to
    /// the column, so there is no cheap size to filter on; the compressed blob lives in the undocumented
    /// <c>sys.plan_persist_plan</c>, which is not a surface to ship against. The budget therefore bounds what
    /// is SHIPPED, not what is decompressed, and that is a property of the catalog rather than a shortcoming
    /// of this query.</para>
    /// </summary>
    public CollectorQuery BuildPlanFetchByIdsQuery(string item, CollectorContext context, IReadOnlyList<long> planIds, long budgetBytes)
    {
        /* The invariant the doc comment spends a paragraph on, actually enforced rather than left to the caller:
           this query exists only to fetch plan XML, so building it with plan capture off is a caller bug, not a
           no-op to swallow. Cheap, and it makes CapturePlanXml the single gate for the whole feature — the
           runtime query's plan-text CASE already reads the same flag. */
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (!context.CapturePlanXml)
        {
            throw new InvalidOperationException(
                "BuildPlanFetchByIdsQuery requires CapturePlanXml; a host that does not capture plan XML must not issue the plan fetch.");
        }

        /* A non-positive budget would make the predicate `running_bytes - plan_bytes < 0`, which excludes even
           the FIRST candidate (its running total before it is 0, and 0 < 0 is false) — the pass ships nothing,
           and because the ids stay missing from the store, the next pass re-selects the same plans forever.
           The oversized-plan stall, reached through the budget input rather than through cut ordering. */
        if (budgetBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(budgetBytes), budgetBytes, "The plan-fetch byte budget must be positive; a zero or negative budget ships nothing and the ids stay missing forever.");
        }

        /* An empty id list means the store already holds every plan this cycle referenced — the steady state
           whose whole point is that NO target query runs (#2312 Finding 2). Reaching this method with one is a
           caller bug, not a no-op to swallow: an `IN ()` is a syntax error anyway, and silently returning a
           no-op query would hide the caller's missing skip. */
        if (planIds is null || planIds.Count == 0)
        {
            throw new ArgumentException(
                "The plan id list must be non-empty; an empty missing set means no fetch should be issued at all.", nameof(planIds));
        }

        var escapedDbName = item.Replace("]", "]]", StringComparison.Ordinal);
        var budget = budgetBytes.ToString(System.Globalization.CultureInfo.InvariantCulture);
        /* Host-computed longs, inlined like the budget and for the same reason: the body nests inside
           sp_executesql, and none of these values ever touch operator input. */
        var idList = string.Join(", ", planIds.Select(id => id.ToString(System.Globalization.CultureInfo.InvariantCulture)));

        /* DECOMPRESS EACH PLAN ONCE (#2675). query_plan_text = CONVERT(nvarchar(max), qsp.query_plan)
           DECOMPRESSES the plan, and sys.query_store_plan.query_plan is decompressed BY the view on access.
           The prior single-statement form referenced that expression THREE times inside one CTE - plan_bytes,
           the running_bytes SUM, and the passthrough - and a CTE inlines rather than materialises, so SQL
           Server re-ran the CONVERT (re-decompressed every plan) for each reference. Measured on a SQL 2025
           rig at 1,500 plans x ~68 KB: 2,440 ms CPU that way vs ~540 ms decompressing once - ~4.5x, on the
           single most expensive operation in the heaviest collector. Materialising the decompressed text to a
           #temp first pins it to ONE decompression; DATALENGTH over the #temp column then reads a stored LOB
           length with no further decompression. The byte-budget cut is unchanged - it still runs over the
           SAME running-total-minus-own-bytes predicate, now against the materialised rows.

           ROWS UNBOUNDED PRECEDING, not the RANGE default: RANGE would tie-group peers and, more to the point,
           forces a spool. The frame is per-row precisely because the cut has to fall between two plans.

           SET NOCOUNT ON so the SELECT INTO emits no result set: the caller's reader takes the first result
           set as the shipped rows, and a stray done-count row would derail it. The #temp is created inside
           this sp_executesql scope and dropped at the end - explicit DROP for clarity; it would auto-drop on
           scope exit regardless.

           HASH JOIN on the fetch statement (#2791), and it is the whole fix rather than a tuning knob.
           sys.query_store_plan's view definition unions the on-disk table with the in-memory TVF
           QUERY_STORE_PLAN_IN_MEM, and the optimizer has NO statistics for that TVF - it uses a fixed guess.
           Measured on AYR the guess is 1,000 rows against 14,633 actual, 1,463% off, which is what makes
           Nested Loops look cheap: the TVF lands on the INNER side and is re-executed once per candidate
           plan_id, up to MaxCandidatePlans = 512 times, each execution scanning the whole in-memory Query
           Store before a single plan is decompressed. sp_QuickieStore put this statement at 55,000-61,000ms
           CPU, top of the entire instance. Under the hint it is a Hash Match reading the TVF ONCE: 0.508s.

           Query-level rather than per-join because the joins live inside the view definition and cannot be
           hinted individually. That means it also applies to the Clustered Index Seek into plan_persist_plan
           (91% of estimated cost), and forcing hash there could have turned a 512-row seek into a full scan
           of a large table - the real risk of this change. Measured, it does not: the hinted statement
           completes in half a second on an 85%-full Query Store, so the seek->scan trade never materialises.
           Recorded because the estimate says it should be a problem and the measurement says it is not.

           The SECOND statement below keeps plain OPTION(RECOMPILE): it reads only #plan_fetch and joins
           nothing, so there is no join strategy to force. Not an oversight - checked. */
        var body = $@"SET NOCOUNT ON;

SELECT
    plan_id = qsp.plan_id,
    query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),
    query_plan_text = CONVERT(nvarchar(max), qsp.query_plan)
INTO #plan_fetch
FROM sys.query_store_plan AS qsp
WHERE qsp.plan_id IN ({idList})
OPTION(RECOMPILE, HASH JOIN);

SELECT
    plan_id = b.plan_id,
    query_plan_hash = b.query_plan_hash,
    query_plan_text = b.query_plan_text
FROM
(
    SELECT
        plan_id = p.plan_id,
        query_plan_hash = p.query_plan_hash,
        query_plan_text = p.query_plan_text,
        plan_bytes = COALESCE(DATALENGTH(p.query_plan_text), 0),
        running_bytes = SUM(COALESCE(DATALENGTH(p.query_plan_text), 0)) OVER (ORDER BY p.plan_id ROWS UNBOUNDED PRECEDING)
    FROM #plan_fetch AS p
) AS b
WHERE b.running_bytes - b.plan_bytes < {budget}
ORDER BY b.plan_id
OPTION(RECOMPILE);

DROP TABLE #plan_fetch;";

        var escapedBody = body.Replace("'", "''", StringComparison.Ordinal);

        var text = $@"
EXECUTE [{escapedDbName}].sys.sp_executesql
    N'{escapedBody}';";

        return new CollectorQuery(text, new List<CollectorParameter>());
    }

    /// <summary>
    /// Statement text for one database (#2312 Finding 2, applying #2150's split): exactly the query_ids the
    /// caller names — the cycle's collected rows whose text the store does not already hold — cut by a byte
    /// budget. The sibling of <see cref="BuildPlanFetchByIdsQuery"/>, with the same store-as-watermark
    /// contract: an empty missing set means no fetch runs at all.
    ///
    /// <para><b>Simpler than the plan fetch on purpose.</b> There is no candidate-window estimator here
    /// because <c>DATALENGTH(query_sql_text)</c> is cheap: <c>sys.query_store_plan.query_plan</c> is
    /// decompressed BY the view on access, which is what forces the plan side to cap its id list, and
    /// <c>query_sql_text</c> has no such cost — the caller may hand the whole missing set (chunked only for
    /// statement-size sanity).</para>
    ///
    /// <para><c>query_hash</c> rides along — <c>CONVERT(varchar(64), ..., 1)</c>, the runtime payload's own
    /// rendering — because <c>query_id</c> is only unique until a Query Store reset renumbers it: a stored
    /// hash that differs from the batch's live one is how the store detects that id 5 is now a DIFFERENT
    /// statement and refetches, where the old design relied on a daily watermark expiry to eventually
    /// re-read everything.</para>
    ///
    /// <para><c>ROWS UNBOUNDED PRECEDING</c> rather than the <c>RANGE</c> default, for the same reason as
    /// the plan fetch: <c>RANGE</c> would tie-group peers and force a spool, and the frame has to be per-row
    /// because the cut falls between two rows.</para>
    /// </summary>
    public CollectorQuery BuildTextFetchByIdsQuery(string item, CollectorContext context, IReadOnlyList<long> queryIds, long budgetBytes)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        /* Same enforcement as the plan fetch's CapturePlanXml gate: this query exists only because the
           payload stopped carrying the text, so issuing it from a host that still ships the text inline is a
           caller bug rather than a harmless extra round trip — it would fetch and store text nobody reads. */
        if (!context.FetchQueryTextSeparately)
        {
            throw new InvalidOperationException(
                "BuildTextFetchByIdsQuery requires FetchQueryTextSeparately; a host that still ships query_sql_text inline must not issue the text fetch.");
        }

        /* A non-positive budget makes the predicate `running_bytes - text_bytes < 0` exclude even the FIRST
           candidate (its running total before it is 0, and 0 < 0 is false), so the pass ships nothing and the
           ids stay missing forever — a stall that looks like a quiet database. */
        if (budgetBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(budgetBytes), budgetBytes, "The text-fetch byte budget must be positive; a zero or negative budget ships nothing and the ids stay missing forever.");
        }

        /* Same contract as the plan fetch: empty means the caller should not have called. */
        if (queryIds is null || queryIds.Count == 0)
        {
            throw new ArgumentException(
                "The query id list must be non-empty; an empty missing set means no fetch should be issued at all.", nameof(queryIds));
        }

        var escapedDbName = item.Replace("]", "]]", StringComparison.Ordinal);
        var budget = budgetBytes.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var idList = string.Join(", ", queryIds.Select(id => id.ToString(System.Globalization.CultureInfo.InvariantCulture)));

        /* READ EACH TEXT ONCE (#2675), the same fix as the plan fetch. query_sql_text is nvarchar(max), and
           the prior single-statement CTE referenced it THREE times - text_bytes, the running_bytes SUM, and
           the passthrough - which, because a CTE inlines, re-read the LOB per reference. Text is not
           compressed so there is no decompression to repeat, but the repeated LOB reads still cost, and they
           scale with real query-text size (in the field text_fetch is the LARGER of the two by-ids phases).
           Materialising to a #temp pins it to one read; DATALENGTH then measures the stored length. The
           byte-budget cut is unchanged.

           SET NOCOUNT ON so the SELECT INTO emits no result set. #temp is scoped to this sp_executesql and
           dropped at the end. ROWS UNBOUNDED PRECEDING for the same per-row-cut reason as the plan fetch.

           HASH JOIN for the same reason as the plan fetch (#2791), and this one is a two-view join written
           out in the open: sys.query_store_query and sys.query_store_query_text are BOTH TVF-backed unions
           over their in-memory halves, driven by an IN list, which is exactly the shape that put the plan
           fetch on the inner side of a loop 512 times over. The plan fetch is the variant that carries the
           AYR measurement; this one is the same defect treated the same way, and that distinction is stated
           rather than blurred - the join-strategy change and the identical-rowset property are verified
           here, the 60s->0.5s number is not this statement's and is not claimed for it. */
        var body = $@"SET NOCOUNT ON;

SELECT
    query_id = qsq.query_id,
    query_hash = CONVERT(varchar(64), qsq.query_hash, 1),
    query_sql_text = qst.query_sql_text
INTO #text_fetch
FROM sys.query_store_query AS qsq
JOIN sys.query_store_query_text AS qst
  ON qst.query_text_id = qsq.query_text_id
WHERE qsq.query_id IN ({idList})
OPTION(RECOMPILE, HASH JOIN);

SELECT
    query_id = b.query_id,
    query_hash = b.query_hash,
    query_sql_text = b.query_sql_text
FROM
(
    SELECT
        query_id = t.query_id,
        query_hash = t.query_hash,
        query_sql_text = t.query_sql_text,
        text_bytes = COALESCE(DATALENGTH(t.query_sql_text), 0),
        running_bytes = SUM(COALESCE(DATALENGTH(t.query_sql_text), 0)) OVER (ORDER BY t.query_id ROWS UNBOUNDED PRECEDING)
    FROM #text_fetch AS t
) AS b
WHERE b.running_bytes - b.text_bytes < {budget}
ORDER BY b.query_id
OPTION(RECOMPILE);

DROP TABLE #text_fetch;";

        var escapedBody = body.Replace("'", "''", StringComparison.Ordinal);

        var text = $@"
EXECUTE [{escapedDbName}].sys.sp_executesql
    N'{escapedBody}';";

        return new CollectorQuery(text, new List<CollectorParameter>());
    }

    /// <summary>
    /// The #2022 phase-2 backfill slice for one on-prem/RDS/MI database: <see cref="BuildPayloadBody"/>
    /// in its backfill shape (newest-first DESC inside the two-sided window) nested in the same
    /// <c>[db].sys.sp_executesql</c> wrapper as <see cref="BuildPerItemQuery"/>. The window is
    /// (<paramref name="floorUtc"/>, <paramref name="ceilingUtc"/>) EXCLUSIVE on both ends: the ceiling
    /// is the derived backfill boundary (MIN(last_execution_time) already stored for this database —
    /// everything at or above it shipped complete, because both bounded cuts finish the boundary tie
    /// group), and the floor is the backfill horizon the worker refuses to dig below. The caller reads
    /// the result through the same <see cref="ReadItemAsync"/>/budget machinery as the live path;
    /// <see cref="CollectorContext.PerItemShippedBoundary"/> comes back as the slice's OLDEST shipped
    /// row — the next slice's ceiling.
    /// </summary>
    public CollectorQuery BuildBackfillPerItemQuery(string item, CollectorContext context, DateTime floorUtc, DateTime ceilingUtc)
    {
        var escapedBody = BuildPayloadBody(context, backfill: true).Replace("'", "''", StringComparison.Ordinal);
        var escapedDbName = item.Replace("]", "]]", StringComparison.Ordinal);

        var text = $@"
EXECUTE [{escapedDbName}].sys.sp_executesql
    N'{escapedBody}',
    N'@floor_time datetime2(7), @ceiling_time datetime2(7)',
    @floor_time,
    @ceiling_time;";

        return new CollectorQuery(text, new List<CollectorParameter>
        {
            new("@floor_time", floorUtc, CollectorParameterType.DateTime2),
            new("@ceiling_time", ceilingUtc, CollectorParameterType.DateTime2),
        });
    }

    /// <summary>
    /// On-prem / RDS / MI: the enumerated item IS the database the per-item query ran in.
    /// </summary>
    public override ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
        => new(ReadRowsAsync(item, reader, rows, context, cancellationToken));

    /// <summary>
    /// Azure SQL DB per-database path (#1836). The payload carries no database_name column — the
    /// on-prem path takes it from the enumerated item — so here it comes from
    /// <see cref="CollectorContext.CurrentDatabaseName"/>, the database the host's per-database loop
    /// connected to, which for a per-database connection IS the rows' database. Same reader contract,
    /// same budget, same self-row exclusion as the enumerated path: one loop serves both.
    ///
    /// <para>Throws rather than defaulting when the host left CurrentDatabaseName unset: an empty
    /// database_name is not a survivable fallback for this collector. It is the per-database watermark
    /// key (<see cref="PerDatabaseWatermarkColumn"/>), so those rows could never advance a watermark
    /// and would re-collect every cycle, and they would land in the grids under a blank database. A
    /// wiring mistake surfaces as one loud, classified failure instead.</para>
    /// </summary>
    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var databaseName = context.CurrentDatabaseName;
        if (string.IsNullOrEmpty(databaseName))
        {
            throw new InvalidOperationException(
                "query_store rows read on the per-database path need CollectorContext.CurrentDatabaseName; the host must set it before reading.");
        }

        var rows = new List<Row>();
        await ReadRowsAsync(databaseName, reader, rows, context, cancellationToken);
        return rows;
    }

    private static async Task ReadRowsAsync(string databaseName, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
    {
        /* Reset the per-item signals — the host reads them immediately after this returns. */
        context.PerItemTextBudgetExceeded = false;
        context.PerItemTextBytesShipped = 0;
        context.PerItemShippedBoundary = null;

        /* Client-side cumulative BYTE budget (#1556) — the PRIMARY memory bound. The server-side TOP caps
           the ROW COUNT, but a row carries two nvarchar(max) fields (query text + plan XML), so 50k rows
           can still be gigabytes. Accumulate the materialized text size and STOP reading at the budget,
           disposing the reader early, so one database can never balloon the process. */
        /* #2164: an operator budget override wins over the compile-time default (the host supplies it
           from the store knob; Lite passes null and keeps the const). Guarded to a positive value so a
           corrupt/zero setting can never mean "ship nothing" — the clamp lives at the store read, and
           this is the second line of defense. */
        var budget = (context.TextByteBudgetOverride is > 0 ? context.TextByteBudgetOverride : Instance.PerItemTextByteBudget)
            ?? int.MaxValue;
        long textBytes = 0;

        /* #1960 boundary-group completion: once the budget trips, rows TIED at the trip row's
           last_execution_time still ship (they are adjacent under the query's ASC order), and the first
           row past the tie ends the cycle. The derived watermark — MAX(last_execution_time) over stored
           rows — then sits exactly at the shipped boundary, and next cycle's strict `> @cutoff_time`
           resumes with no hole; a split tie group would strand its unshipped half behind that comparison
           forever. Mirrors the SQL's TOP ... WITH TIES, which guarantees the same at the row cap. */
        var budgetSpent = false;
        DateTime? cutBoundary = null;

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Row
            {
                DatabaseName = databaseName,
                QueryId = reader.GetInt64(0),
                PlanId = reader.GetInt64(1),
                ExecutionTypeDesc = reader.IsDBNull(2) ? null : reader.GetString(2),
                FirstExecutionTime = reader.IsDBNull(3) ? null : ((DateTimeOffset)reader.GetValue(3)).UtcDateTime,
                LastExecutionTime = reader.IsDBNull(4) ? null : ((DateTimeOffset)reader.GetValue(4)).UtcDateTime,
                ModuleName = reader.IsDBNull(5) ? null : reader.GetString(5),
                QueryText = reader.IsDBNull(6) ? null : reader.GetString(6),
                QueryHash = reader.IsDBNull(7) ? null : reader.GetString(7),
                ExecutionCount = reader.GetInt64(8),
                AvgDurationUs = ReadNullableInt64(reader, 9),
                MinDurationUs = ReadNullableInt64(reader, 10),
                MaxDurationUs = ReadNullableInt64(reader, 11),
                AvgCpuTimeUs = ReadNullableInt64(reader, 12),
                MinCpuTimeUs = ReadNullableInt64(reader, 13),
                MaxCpuTimeUs = ReadNullableInt64(reader, 14),
                AvgLogicalIoReads = ReadNullableInt64(reader, 15),
                MinLogicalIoReads = ReadNullableInt64(reader, 16),
                MaxLogicalIoReads = ReadNullableInt64(reader, 17),
                AvgLogicalIoWrites = ReadNullableInt64(reader, 18),
                MinLogicalIoWrites = ReadNullableInt64(reader, 19),
                MaxLogicalIoWrites = ReadNullableInt64(reader, 20),
                AvgPhysicalIoReads = ReadNullableInt64(reader, 21),
                MinPhysicalIoReads = ReadNullableInt64(reader, 22),
                MaxPhysicalIoReads = ReadNullableInt64(reader, 23),
                AvgClrTimeUs = ReadNullableInt64(reader, 24),
                MinClrTimeUs = ReadNullableInt64(reader, 25),
                MaxClrTimeUs = ReadNullableInt64(reader, 26),
                MinDop = ReadNullableInt64(reader, 27),
                MaxDop = ReadNullableInt64(reader, 28),
                AvgQueryMaxUsedMemory = ReadNullableInt64(reader, 29),
                MinQueryMaxUsedMemory = ReadNullableInt64(reader, 30),
                MaxQueryMaxUsedMemory = ReadNullableInt64(reader, 31),
                AvgRowcount = ReadNullableInt64(reader, 32),
                MinRowcount = ReadNullableInt64(reader, 33),
                MaxRowcount = ReadNullableInt64(reader, 34),
                AvgNumPhysicalIoReads = ReadNullableInt64(reader, 35),
                MinNumPhysicalIoReads = ReadNullableInt64(reader, 36),
                MaxNumPhysicalIoReads = ReadNullableInt64(reader, 37),
                AvgLogBytesUsed = ReadNullableInt64(reader, 38),
                MinLogBytesUsed = ReadNullableInt64(reader, 39),
                MaxLogBytesUsed = ReadNullableInt64(reader, 40),
                AvgTempdbSpaceUsed = ReadNullableInt64(reader, 41),
                MinTempdbSpaceUsed = ReadNullableInt64(reader, 42),
                MaxTempdbSpaceUsed = ReadNullableInt64(reader, 43),
                PlanType = reader.IsDBNull(44) ? null : reader.GetString(44),
                PlanForcingType = reader.IsDBNull(45) ? null : reader.GetString(45),
                IsForcedPlan = !reader.IsDBNull(46) && reader.GetBoolean(46),
                ForceFailureCount = reader.IsDBNull(47) ? 0L : reader.GetInt64(47),
                LastForceFailureReason = reader.IsDBNull(48) ? null : reader.GetString(48),
                CompatibilityLevel = reader.IsDBNull(49) ? 0 : Convert.ToInt32(reader.GetValue(49), CultureInfo.InvariantCulture),
                QueryPlanText = reader.IsDBNull(50) ? null : reader.GetString(50),
                QueryPlanHash = reader.IsDBNull(51) ? null : reader.GetString(51),
                ReplicaRole = reader.IsDBNull(52) ? null : reader.GetString(52),
                RuntimeStatsIntervalId = reader.IsDBNull(53) ? null : reader.GetInt64(53),
                /* Already datetime2 (the SELECT does the AT TIME ZONE conversion), so this reads a plain
                   DateTime — unlike first_execution_time/last_execution_time above, which arrive as
                   datetimeoffset and need the .UtcDateTime normalization. */
                IntervalStartTimeUtc = reader.IsDBNull(54) ? null : reader.GetDateTime(54),
            };

            /* Client-side self-exclusion (#1565): our own collector queries carry the marker in their
               leading comment. Skipped rows never enter the batch — not stored, not counted against the
               byte budget. This replaced the SQL-side NOT LIKE predicate, which was 75% of the read's
               elapsed time (full nvarchar(max) scan per row; the field A/B measured 4.3x without it).
               Reached on BOTH paths: on Azure our own per-database payload runs inside the very
               database whose Query Store we are reading, so without this it would collect itself. */
            if (row.QueryText?.Contains(SelfQueryMarker, StringComparison.Ordinal) == true)
            {
                continue;
            }

            /* Budget already spent: only the remainder of the boundary tie group still ships. A null
               boundary cannot tie (and a null-watermark row could never have reached the client past
               the strict cutoff anyway), so the first row after a null-boundary trip also ends here. */
            if (budgetSpent && (cutBoundary is null || row.LastExecutionTime != cutBoundary))
            {
                break;
            }

            rows.Add(row);

            /* char count × 2 for UTF-16. The plan XML is deduped server-side (NULL on all but the newest
               interval per plan_id within the window), but the query text repeats on every interval row,
               so this budget is what bounds that repetition too. At the budget, signal the bounded cycle
               and finish the boundary tie group — the host surfaces the WARNING. Rows are read
               OLDEST-first (#1960) and the per-database watermark derives from the newest STORED row, so
               everything past the cut stays ahead of the watermark and next cycle resumes exactly there:
               a bounded cycle costs latency, never data. */
            textBytes += ((long)(row.QueryText?.Length ?? 0) + (row.QueryPlanText?.Length ?? 0)) * 2L;

            if (!budgetSpent && textBytes >= budget)
            {
                budgetSpent = true;
                cutBoundary = row.LastExecutionTime;
                context.PerItemTextBudgetExceeded = true;
            }
        }

        context.PerItemTextBytesShipped = textBytes;
        context.PerItemShippedBoundary = rows.Count > 0 ? rows[^1].LastExecutionTime : null;

        /* #2312: no plan-XML watermark write-back any more. Inline-shipped plan XML (the backfill path)
           lands on its own rows; the LIVE fetch is activity-driven against the store's own map, so there
           is no resume point to persist here and nothing for a budget cut to corrupt. */
    }


    /// <summary>
    /// Reads a nullable int64, converting float/decimal Query Store values to long.
    /// Query Store runtime_stats columns are stored as float in the catalog but represent
    /// integer-scale values.
    /// </summary>
    private static long ReadNullableInt64(DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return 0L;
        var value = reader.GetValue(ordinal);
        return value switch
        {
            long l => l,
            int i => i,
            short s => s,
            decimal d => (long)d,
            double dbl => (long)dbl,
            float f => (long)f,
            _ => Convert.ToInt64(value, CultureInfo.InvariantCulture)
        };
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("plan_id", CollectorColumnType.BigInt),
        new CollectorColumn("execution_type_desc", CollectorColumnType.Varchar),
        new CollectorColumn("first_execution_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_execution_time", CollectorColumnType.Timestamp),
        new CollectorColumn("module_name", CollectorColumnType.Varchar),
        new CollectorColumn("query_text", CollectorColumnType.Varchar),
        new CollectorColumn("query_hash", CollectorColumnType.Varchar),
        new CollectorColumn("execution_count", CollectorColumnType.BigInt),
        new CollectorColumn("avg_duration_us", CollectorColumnType.BigInt),
        new CollectorColumn("min_duration_us", CollectorColumnType.BigInt),
        new CollectorColumn("max_duration_us", CollectorColumnType.BigInt),
        new CollectorColumn("avg_cpu_time_us", CollectorColumnType.BigInt),
        new CollectorColumn("min_cpu_time_us", CollectorColumnType.BigInt),
        new CollectorColumn("max_cpu_time_us", CollectorColumnType.BigInt),
        new CollectorColumn("avg_logical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("min_logical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("max_logical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("avg_logical_io_writes", CollectorColumnType.BigInt),
        new CollectorColumn("min_logical_io_writes", CollectorColumnType.BigInt),
        new CollectorColumn("max_logical_io_writes", CollectorColumnType.BigInt),
        new CollectorColumn("avg_physical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("min_physical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("max_physical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("avg_clr_time_us", CollectorColumnType.BigInt),
        new CollectorColumn("min_clr_time_us", CollectorColumnType.BigInt),
        new CollectorColumn("max_clr_time_us", CollectorColumnType.BigInt),
        new CollectorColumn("min_dop", CollectorColumnType.BigInt),
        new CollectorColumn("max_dop", CollectorColumnType.BigInt),
        new CollectorColumn("avg_query_max_used_memory", CollectorColumnType.BigInt),
        new CollectorColumn("min_query_max_used_memory", CollectorColumnType.BigInt),
        new CollectorColumn("max_query_max_used_memory", CollectorColumnType.BigInt),
        new CollectorColumn("avg_rowcount", CollectorColumnType.BigInt),
        new CollectorColumn("min_rowcount", CollectorColumnType.BigInt),
        new CollectorColumn("max_rowcount", CollectorColumnType.BigInt),
        new CollectorColumn("avg_num_physical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("min_num_physical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("max_num_physical_io_reads", CollectorColumnType.BigInt),
        new CollectorColumn("avg_log_bytes_used", CollectorColumnType.BigInt),
        new CollectorColumn("min_log_bytes_used", CollectorColumnType.BigInt),
        new CollectorColumn("max_log_bytes_used", CollectorColumnType.BigInt),
        new CollectorColumn("avg_tempdb_space_used", CollectorColumnType.BigInt),
        new CollectorColumn("min_tempdb_space_used", CollectorColumnType.BigInt),
        new CollectorColumn("max_tempdb_space_used", CollectorColumnType.BigInt),
        new CollectorColumn("plan_type", CollectorColumnType.Varchar),
        new CollectorColumn("plan_forcing_type", CollectorColumnType.Varchar),
        new CollectorColumn("is_forced_plan", CollectorColumnType.Boolean),
        new CollectorColumn("force_failure_count", CollectorColumnType.BigInt),
        new CollectorColumn("last_force_failure_reason", CollectorColumnType.Varchar),
        new CollectorColumn("compatibility_level", CollectorColumnType.Integer),
        new CollectorColumn("query_plan_text", CollectorColumnType.Varchar),
        new CollectorColumn("query_plan_hash", CollectorColumnType.Varchar),
        /* Appended at the END, not grouped with the identity columns: both hosts' bulk writers are
           POSITIONAL (Lite's DuckDB appender, Darling's Npgsql binary COPY), and an upgraded store gets
           this column from an ALTER TABLE ADD COLUMN, which can only append. A mid-list position would
           put it mid-table on a FRESH store (whose DDL is generated from this list) and last on an
           UPGRADED one — the same COPY then writing shifted values on one of them. Same reasoning as
           deadlocks.database_name (Darling V27 / Lite v46). */
        new CollectorColumn("replica_role", CollectorColumnType.Varchar),
        /* #1841 tier 2, appended for the same positional reason replica_role was: an upgraded store gets
           these from an ALTER TABLE ADD COLUMN, which can only append, while a fresh store's DDL is
           generated from this list — any other position would give the two stores different physical
           column orders and the positional bulk writers would then write shifted values on one of them.
           Both nullable: rows collected by a pre-tier-2 build have no interval identity, and every reader
           that keys on it must tolerate NULL rather than assume the upgrade backfilled anything (it
           cannot — the identity was never collected). */
        new CollectorColumn("runtime_stats_interval_id", CollectorColumnType.BigInt),
        new CollectorColumn("interval_start_time_utc", CollectorColumnType.Timestamp),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DatabaseName)
            .Value(row.QueryId)
            .Value(row.PlanId)
            .Value(row.ExecutionTypeDesc)
            .Value(row.FirstExecutionTime)
            .Value(row.LastExecutionTime)
            .Value(row.ModuleName)
            .Value(row.QueryText)
            .Value(row.QueryHash)
            .Value(row.ExecutionCount)
            .Value(row.AvgDurationUs)
            .Value(row.MinDurationUs)
            .Value(row.MaxDurationUs)
            .Value(row.AvgCpuTimeUs)
            .Value(row.MinCpuTimeUs)
            .Value(row.MaxCpuTimeUs)
            .Value(row.AvgLogicalIoReads)
            .Value(row.MinLogicalIoReads)
            .Value(row.MaxLogicalIoReads)
            .Value(row.AvgLogicalIoWrites)
            .Value(row.MinLogicalIoWrites)
            .Value(row.MaxLogicalIoWrites)
            .Value(row.AvgPhysicalIoReads)
            .Value(row.MinPhysicalIoReads)
            .Value(row.MaxPhysicalIoReads)
            .Value(row.AvgClrTimeUs)
            .Value(row.MinClrTimeUs)
            .Value(row.MaxClrTimeUs)
            .Value(row.MinDop)
            .Value(row.MaxDop)
            .Value(row.AvgQueryMaxUsedMemory)
            .Value(row.MinQueryMaxUsedMemory)
            .Value(row.MaxQueryMaxUsedMemory)
            .Value(row.AvgRowcount)
            .Value(row.MinRowcount)
            .Value(row.MaxRowcount)
            .Value(row.AvgNumPhysicalIoReads)
            .Value(row.MinNumPhysicalIoReads)
            .Value(row.MaxNumPhysicalIoReads)
            .Value(row.AvgLogBytesUsed)
            .Value(row.MinLogBytesUsed)
            .Value(row.MaxLogBytesUsed)
            .Value(row.AvgTempdbSpaceUsed)
            .Value(row.MinTempdbSpaceUsed)
            .Value(row.MaxTempdbSpaceUsed)
            .Value(row.PlanType)
            .Value(row.PlanForcingType)
            .Value(row.IsForcedPlan)
            .Value(row.ForceFailureCount)
            .Value(row.LastForceFailureReason)
            .Value(row.CompatibilityLevel)
            .Value(row.QueryPlanText)
            .Value(row.QueryPlanHash)
            .Value(row.ReplicaRole)
            .Value(row.RuntimeStatsIntervalId)
            .Value(row.IntervalStartTimeUtc);
    }
}
