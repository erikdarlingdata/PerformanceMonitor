/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store reads behind <c>get_query_store_clutter</c> (#3797) — four arms over rows the collectors already
/// write, and not one new query against a monitored server. The maintainer's design intent, verbatim spirit:
/// <i>some sort of view of Query Store clutter; not a new query — analyse what we've already collected.</i>
///
/// <para><b>Here rather than in the service's <c>Mcp/</c> folder</b>, beside the <c>DarlingPg*Reader</c>
/// family and for the reason that family moved (#2530): three surfaces answer this question — the MCP tool,
/// the web server tab that reads it, and the WPF Viewer, which has no route to the service and runs these
/// statements in process. The alternative is a second copy of four statements carrying a window floor, a
/// discrete-percentile definition that has to agree with the C# beside it, and a replica gate read off one
/// engine bit; the copy that drifts is never the one being read.</para>
///
/// <para><b>Why four separate statements rather than one composite.</b> Each arm has its own honest grain
/// and its own retention. Read cost is per DATABASE per collector RUN and lives in <c>collection_log</c>
/// (weeks of retention, small); plan churn is per DATABASE per plan and lives in <c>query_store_stats</c>
/// (the raw tier, dropped at four days when the rollups are armed, large); configuration is per DATABASE
/// per hourly capture in <c>query_store_health</c>; the overhead proxy is per SERVER and lives in
/// <c>wait_stats</c> (raw tier) and <c>memory_clerks</c>. A single statement would have to pick one grain
/// and one reach and would lie about the other three. The tool composes them and says, per arm, what span
/// was actually observed.</para>
///
/// <para><b>Every statement takes <c>server_id = ANY($1::int[])</c>, deliberately.</b> The same text serves
/// the one-server read (a one-element array) and the opt-in fleet read (every enabled SQL Server target), so
/// the fleet reference is computed by the SAME arithmetic as the number it is a reference for — a second
/// statement for the fleet would be a second definition of the statistic, and two definitions drift.
/// <c>percentile_disc</c> throughout, never <c>percentile_cont</c>: a discrete percentile is a value some
/// run really had, and it side-steps PostgreSQL's missing <c>round(double precision, integer)</c>
/// (<c>DarlingOversizedPlanBacklogReader</c>'s reasoning).</para>
///
/// <para><b>What the read-cost arm can and cannot say, restated from V80.</b> <c>collection_log</c> keeps
/// only the SLOWEST item of each fan-out run (<c>slowest_item</c> / <c>slowest_item_ms</c> /
/// <c>fanout_item_count</c>, <c>PgMigrations</c> V80, #2472): the per-database distribution was declined
/// as a hypertable at ~10% of <c>collection_log</c>'s volume forever. So this arm ranks databases by how
/// OFTEN and by how MUCH they were the slowest, not by a per-database series that does not exist. The
/// share arithmetic is <c>get_collection_health</c>'s (#3502): <c>slowest_item_ms / duration_ms</c> is the
/// slowest item's share of the whole pass, and it is the width-versus-concentration verdict; a share near
/// 100% is one database owning the pass. The cross-database ratio this reader adds — this database's median
/// slowest cost against the pooled median of every OTHER database's slowest cost on the same server — is a
/// different quantity from <c>get_collection_health</c>'s <c>dominance</c> (slowest × items / run), and the
/// tool names it so it cannot be read as that one.</para>
///
/// <para><b>Plan churn counts <c>plan_id</c>, not <c>query_plan_hash</c>.</b> Three reasons, each sufficient.
/// <c>plan_id</c> is the identity <c>MAX_PLANS_PER_QUERY</c> caps, so plans-per-query against that cap is
/// the same unit on both sides; it is the key <c>query_store_plan_map</c> holds one row per, so distinct plans
/// here is the number the plan dimension grows by; and it is in
/// <c>idx_query_store_stats_server_db_query_plan_time</c> (<c>PgTableTuning</c>), so the whole arm is an
/// index-only walk of one server's rows where a hash-keyed count would fetch every heap row in the window.
/// A shape hash collapses parameter-sniffed recompiles into one value and would UNDER-count exactly the
/// churn this arm exists to see (<c>PayloadDimensions</c>' first reason for not keying the dim on it).</para>
///
/// <para><b>Replicas are excluded by architecture, read off the engine's own bit.</b>
/// <c>readonly_reason &amp; 8</c> is SQL Server saying "read-only BECAUSE this database is a readable
/// secondary" — the mechanism-agnostic flag (AG, RDS read replica, geo-secondary alike) that
/// <c>QueryStoreCollector</c>'s enumeration already gates on (<c>readonly_reason &amp; 8 = 0</c>, #1558),
/// which is why a replica has no <c>query_store_stats</c> rows and no fan-out runs to begin with. The
/// health row still exists for it, and the tool reads the bit off that row rather than off
/// <c>server_properties.ag_replica_role</c>, which only knows Always On (<c>'Secondary'</c>) and reads
/// <c>'Standalone'</c> on an RDS read replica. The topology ruling (2026-09-20): a replica's Query Store is
/// its primary's, its READ_ONLY is by design and never a defect, and any QS-shaped finding excludes it
/// with the reason on the row.</para>
/// </summary>
public static class DarlingQueryStoreClutterReader
{
    /// <summary>The fan-out collector whose runs the read-cost arm reads. The clutter view is about Query
    /// Store's catalog cost, so the sibling fan-outs (<c>query_store_health</c>, <c>plan_correction</c>, …)
    /// are deliberately not blended in.</summary>
    public const string CollectorName = "query_store";

    /// <summary>The memory clerk that IS Query Store's in-memory footprint (<c>sys.dm_os_memory_clerks</c>
    /// type <c>MEMORYCLERK_QUERYDISKSTORE</c>).</summary>
    public const string QueryStoreMemoryClerk = "MEMORYCLERK_QUERYDISKSTORE";

    /// <summary>The engine's "readable secondary" bit in <c>readonly_reason</c> — decoded by
    /// <c>QueryStoreReadonlyReason</c> as "database is a secondary replica", gated on by the collector.</summary>
    public const int SecondaryReplicaReadonlyBit = 8;

    /* ─────────────────────────── rows ─────────────────────────── */

    /// <summary>One database's read-cost figures on one server: how often it was the slowest item of a
    /// <c>query_store</c> fan-out run, and how expensive it was when it was.</summary>
    /// <param name="RunsObserved">Fan-out runs on this SERVER in the window (the denominator, identical on every row of a server).</param>
    /// <param name="RunsSlowest">Runs on which THIS database was <c>slowest_item</c>.</param>
    /// <param name="SlowestItemMsP50">Discrete median of <c>slowest_item_ms</c> over those runs.</param>
    /// <param name="SlowestItemMsP95">Discrete 95th percentile of the same.</param>
    /// <param name="RunDurationMsP50">Discrete median of the whole run's <c>duration_ms</c> over those runs, for scale.</param>
    /// <param name="SlowestSharePctP50">Discrete median of <c>100 × slowest_item_ms / duration_ms</c> over those runs — the slowest item's share of the pass (#3502's verdict figure).</param>
    /// <param name="FanoutItemsMax">The widest fan-out among those runs.</param>
    /// <param name="LastSlowestAt">The newest run on which it was slowest.</param>
    /// <param name="OthersSlowestItemMsP50">Discrete median of <c>slowest_item_ms</c> pooled over every run on which a DIFFERENT database was slowest; null when no other database was ever slowest.</param>
    public sealed record ReadCostRow(
        int ServerId, string DatabaseName, int RunsObserved, int RunsSlowest,
        int SlowestItemMsP50, int SlowestItemMsP95, int RunDurationMsP50, double SlowestSharePctP50,
        int FanoutItemsMax, DateTime LastSlowestAt, int? OthersSlowestItemMsP50);

    /// <summary>One database's plan-churn figures on one server over the window's raw <c>query_store_stats</c>.</summary>
    /// <param name="CollectionsObserved">Distinct <c>collection_time</c> values that carried rows for this database.</param>
    /// <param name="PlansPerQueryP95">Discrete 95th percentile, over the database's queries, of distinct plans per query.</param>
    /// <param name="PlansSeenOnce">Plans whose rows appear under exactly ONE collection_time — the one-shot population.</param>
    /// <param name="PlansFirstSeenAfterFirstCollection">Plans whose earliest sighting is later than the database's first collection in the window — arrivals inside the window.</param>
    public sealed record PlanChurnRow(
        int ServerId, string DatabaseName, DateTime FirstCollection, DateTime LastCollection, int CollectionsObserved,
        int DistinctQueries, int DistinctPlans, int PlansPerQueryP95, int PlansPerQueryMax,
        int PlansSeenOnce, int PlansFirstSeenAfterFirstCollection);

    /// <summary>
    /// #9478: the one figure the fleet median reads from the plan-churn arm, per (server, database): the discrete 95th percentile of
    /// distinct plans per query. It is <see cref="PlanChurnRow.PlansPerQueryP95"/> from <see cref="GetPlanChurnP95Async"/>, which reads far less.
    /// </summary>
    public sealed record PlanChurnP95Row(int ServerId, string DatabaseName, int PlansPerQueryP95);

    /// <summary>The newest <c>query_store_health</c> capture per database inside the window, with the
    /// capture's own stamp so the tool can say how old "latest" is.</summary>
    /// <param name="QueryCaptureMode">The <c>query_capture_mode_desc</c> spelling verbatim (<c>ALL</c> /
    /// <c>AUTO</c> / <c>CUSTOM</c> / <c>NONE</c>), or null on a row captured before the V137 rung (#3796)
    /// created the column — null means NEVER ASKED, never <c>NONE</c>.</param>
    public sealed record ConfigRow(
        int ServerId, string DatabaseName, string ActualState, string DesiredState, int ReadonlyReason,
        long CurrentStorageMb, long MaxStorageMb, string SizeBasedCleanupMode,
        long StaleQueryThresholdDays, long MaxPlansPerQuery, long IntervalLengthMinutes, DateTime CapturedAt,
        string? QueryCaptureMode)
    {
        /// <summary>The engine's readable-secondary bit — the architectural exclusion.</summary>
        public bool IsSecondaryReplica => (ReadonlyReason & SecondaryReplicaReadonlyBit) != 0;
    }

    /// <summary>One <c>QDS_*</c> wait type's stored deltas summed over the window on one server.</summary>
    /// <param name="WaitMsTotal">Every row's <c>delta_wait_time_ms</c> summed — the window's total.</param>
    /// <param name="RatedWaitMs">The same sum over rows whose <c>sample_interval_seconds &gt; 0</c> — the rows a rate can be formed from.</param>
    /// <param name="MeasuredSeconds">Sum of <c>sample_interval_seconds</c> over those rated rows — the measured span the rate divides by.</param>
    /// <param name="RowsWithoutInterval">Pre-V127 rows (<c>sample_interval_seconds IS NULL</c>): their delta is in the total, not in the rate.</param>
    /// <param name="RowsUnknowable">Rows stored (0, 0) — a restart, a counter reset, a gap past the policy; counted, never rated.</param>
    public sealed record QdsWaitRow(
        int ServerId, string WaitType, long WaitMsTotal, long WaitingTasksTotal, long RatedWaitMs, long MeasuredSeconds,
        int RowsObserved, int RowsWithoutInterval, int RowsUnknowable, DateTime FirstObserved, DateTime LastObserved);

    /// <summary>The Query Store memory clerk over the window on one server, beside the fact that decides
    /// whether its absence means anything.</summary>
    /// <param name="LatestCapture">The newest <c>memory_clerks</c> capture of ANY clerk in the window — null when the collector wrote nothing.</param>
    /// <param name="Captures">Distinct captures in the window.</param>
    /// <param name="LatestMemoryMb">The clerk's <c>memory_mb</c> on its newest capture in the window, null when it never appeared.</param>
    /// <param name="LatestClerkCapture">When that newest appearance was; compare to <see cref="LatestCapture"/> — an older stamp means the clerk has since dropped out of the collector's top 25.</param>
    /// <param name="MaxMemoryMb">The clerk's largest value in the window.</param>
    /// <param name="ClerkSamples">Captures on which the clerk appeared.</param>
    public sealed record ClerkRow(
        DateTime? LatestCapture, int Captures, decimal? LatestMemoryMb, DateTime? LatestClerkCapture, decimal? MaxMemoryMb, int ClerkSamples);

    /* ─────────────────────────── (a) read cost ─────────────────────────── */

    /// <summary>
    /// Per (server, database): how often and by how much the database was the slowest item of a
    /// <c>query_store</c> fan-out run in the window. Runs with no fan-out (<c>fanout_item_count IS NULL</c> —
    /// a plain run, or an enumeration that yielded no items, which is every run on a replica) are not runs
    /// this arm can say anything about and are excluded from the denominator on purpose; a zero-duration
    /// run is excluded so the share has a denominator. The <c>others</c> CTE pools the OTHER databases'
    /// slowest costs on the same server so the tool can publish this database's median against theirs.
    /// $1 server_id array, $2/$3 window (naive UTC), $4 collector name, $5 the chosen databases (#5244: one text[], NULL
    /// for every database). The databases narrow the FINAL per-database rows only: <c>per_server</c> (the run denominator)
    /// and <c>others</c> (the pool the median is compared against) read every run, so a database's numbers do not move
    /// with the selection.
    /// </summary>
    public const string ReadCostSql = """
        WITH runs AS
        (
            SELECT server_id, collection_time, duration_ms, fanout_item_count, slowest_item, slowest_item_ms
            FROM v_collection_log
            WHERE server_id = ANY($1::int[])
            AND   collector_name = $4
            AND   collection_time > $2
            AND   collection_time <= $3
            AND   fanout_item_count IS NOT NULL
            AND   slowest_item IS NOT NULL
            AND   slowest_item_ms IS NOT NULL
            AND   duration_ms > 0
        ),
        per_server AS
        (
            SELECT server_id, COUNT(*) AS runs_observed
            FROM runs
            GROUP BY server_id
        ),
        per_database AS
        (
            SELECT
                server_id,
                slowest_item AS database_name,
                COUNT(*) AS runs_slowest,
                percentile_disc(0.5) WITHIN GROUP (ORDER BY slowest_item_ms) AS slowest_item_ms_p50,
                percentile_disc(0.95) WITHIN GROUP (ORDER BY slowest_item_ms) AS slowest_item_ms_p95,
                percentile_disc(0.5) WITHIN GROUP (ORDER BY duration_ms) AS run_duration_ms_p50,
                percentile_disc(0.5) WITHIN GROUP (ORDER BY 100.0 * slowest_item_ms / duration_ms) AS slowest_share_pct_p50,
                MAX(fanout_item_count) AS fanout_items_max,
                MAX(collection_time) AS last_slowest_at
            FROM runs
            GROUP BY server_id, slowest_item
        ),
        others AS
        (
            SELECT
                d.server_id,
                d.database_name,
                percentile_disc(0.5) WITHIN GROUP (ORDER BY r.slowest_item_ms) AS others_slowest_item_ms_p50
            FROM per_database AS d
            JOIN runs AS r
              ON  r.server_id = d.server_id
              AND r.slowest_item <> d.database_name
            GROUP BY d.server_id, d.database_name
        )
        SELECT
            d.server_id,
            d.database_name,
            s.runs_observed::int,
            d.runs_slowest::int,
            d.slowest_item_ms_p50,
            d.slowest_item_ms_p95,
            d.run_duration_ms_p50,
            d.slowest_share_pct_p50::double precision,
            d.fanout_items_max,
            d.last_slowest_at,
            o.others_slowest_item_ms_p50
        FROM per_database AS d
        JOIN per_server AS s
          ON s.server_id = d.server_id
        LEFT JOIN others AS o
          ON  o.server_id = d.server_id
          AND o.database_name = d.database_name
        WHERE ($5::text[] IS NULL OR d.database_name = ANY($5))
        ORDER BY d.server_id, d.runs_slowest DESC, d.slowest_item_ms_p50 DESC, d.database_name
        """;

    /// <param name="databases">#5244: the chosen databases (optional, so the Viewer's calls compile
    /// unchanged); <see cref="DatabaseFilter.All"/>, the default, is every database.</param>
    public static async Task<List<ReadCostRow>> GetReadCostAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, DatabaseFilter databases = default, DateTime? asOfUtc = null,
        ICollection<string>? omittedDatabases = null, ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<ReadCostRow>();
        await using var command = postgres.CreateCommand(ReadCostSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        command.Parameters.AddWithValue(CollectorName);
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ReadCostRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetInt32(3),
                reader.GetInt32(4),
                reader.GetInt32(5),
                reader.GetInt32(6),
                reader.GetDouble(7),
                reader.GetInt32(8),
                reader.GetDateTime(9),
                reader.IsDBNull(10) ? null : reader.GetInt32(10)));
        }

        /* #5633 item 4: a database the server no longer has (absent from the newest database_states snapshot) is left out when no
           database was chosen, so it cannot carry a verdict, a count or a recommendation. See CurrentDatabaseScope. */
        return await CurrentDatabaseScope.OmitDroppedAsync(postgres, rows, r => r.ServerId, r => r.DatabaseName, databases, r => r.LastSlowestAt, asOfUtc, omittedDatabases, logger, cancellationToken);
    }

    /* ─────────────────────────── (b) plan churn ─────────────────────────── */

    /// <summary>
    /// Per (server, database): plan cardinality and churn over the window's raw <c>query_store_stats</c> rows.
    /// Reads the base table, not <c>v_query_store_stats</c>, for the reason <c>get_query_store_top</c> does —
    /// the raw tier is the only tier that carries <c>plan_id</c>. The statement reads only the columns of
    /// <c>idx_query_store_stats_server_db_query_plan_time</c>, so the walk can be index-only. It aggregates straight from the
    /// table (#6808): <c>MIN</c>/<c>MAX</c> per plan and a <c>DISTINCT</c> over (server, database, collection_time), so no
    /// <c>COUNT(DISTINCT)</c> sorts a materialised copy of every raw row in the window.
    ///
    /// <para>The cumulative-interval mechanics make "seen once" mean what the tool says it means: the
    /// collector re-fetches an OPEN interval every cycle while its <c>last_execution_time</c> advances
    /// (#1841), so a plan that keeps running is seen under many <c>collection_time</c> values, and a plan
    /// seen under exactly one is one that ran inside a single collection cycle's span and never again in
    /// the window — the one-shot population an ad-hoc workload manufactures. "First seen after the first
    /// collection" is the arrivals: plans whose earliest sighting is later than the database's first
    /// collection in the window. The store keeps no first-seen stamp (<c>query_store_plan_map</c> holds
    /// <c>last_seen</c> only), so a plan idle before the window and executed again inside it counts as an
    /// arrival here; the tool says so.</para>
    /// $1 server_id array, $2/$3 window (naive UTC), $4 the chosen databases (#5244: one text[], NULL for every database;
    /// every figure here is per database, so the predicate narrows the source rows).
    /// </summary>
    public const string PlanChurnSql = RawPlanChurnHead + "\n" + PlanChurnTail;

    /// <summary>#9148: <see cref="PlanChurnSql"/> up to <c>per_plan</c>, read from raw alone.</summary>
    private const string RawPlanChurnHead = """
        WITH rows_in_window AS NOT MATERIALIZED
        (
            SELECT server_id, database_name, query_id, plan_id, collection_time
            FROM query_store_stats
            WHERE server_id = ANY($1::int[])
            AND   collection_time > $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        per_plan AS
        (
            SELECT
                server_id,
                database_name,
                query_id,
                plan_id,
                MIN(collection_time) AS first_seen,
                MAX(collection_time) AS last_seen
            FROM rows_in_window
            GROUP BY server_id, database_name, query_id, plan_id
        ),
        """;

    /// <summary>#9148: <see cref="PlanChurnSql"/> from <c>per_query</c> on, shared with <see cref="PlanChurnClosedDaysSql"/>.</summary>
    private const string PlanChurnTail = """
        per_query AS
        (
            SELECT server_id, database_name, query_id, COUNT(*) AS plans
            FROM per_plan
            GROUP BY server_id, database_name, query_id
        ),
        per_database AS
        (
            /* #8570: the first and last collection of a database are the earliest first sighting and the latest last
               sighting over its plans, so they come from per_plan and the window's rows are read once here. The
               number of distinct collection times (the part that needs the rows again) is PlanChurnCollectionsSql. */
            SELECT
                server_id,
                database_name,
                MIN(first_seen) AS first_collection,
                MAX(last_seen) AS last_collection
            FROM per_plan
            GROUP BY server_id, database_name
        ),
        plan_facts AS
        (
            SELECT
                p.server_id,
                p.database_name,
                COUNT(*) AS distinct_plans,
                COUNT(*) FILTER (WHERE p.first_seen = p.last_seen) AS plans_seen_once,
                COUNT(*) FILTER (WHERE p.first_seen > d.first_collection) AS plans_first_seen_after_first_collection
            FROM per_plan AS p
            JOIN per_database AS d
              ON  d.server_id = p.server_id
              AND d.database_name = p.database_name
            GROUP BY p.server_id, p.database_name
        ),
        query_facts AS
        (
            SELECT
                server_id,
                database_name,
                COUNT(*) AS distinct_queries,
                percentile_disc(0.95) WITHIN GROUP (ORDER BY plans) AS plans_per_query_p95,
                MAX(plans) AS plans_per_query_max
            FROM per_query
            GROUP BY server_id, database_name
        )
        SELECT
            d.server_id,
            d.database_name,
            d.first_collection,
            d.last_collection,
            q.distinct_queries::int,
            p.distinct_plans::int,
            q.plans_per_query_p95::int,
            q.plans_per_query_max::int,
            p.plans_seen_once::int,
            p.plans_first_seen_after_first_collection::int
        FROM per_database AS d
        JOIN plan_facts AS p
          ON  p.server_id = d.server_id
          AND p.database_name = d.database_name
        JOIN query_facts AS q
          ON  q.server_id = d.server_id
          AND q.database_name = d.database_name
        ORDER BY d.server_id, d.database_name
        """;

    /// <summary>
    /// #8570: the number of distinct <c>collection_time</c> values that carried rows, per (server, database), over the same window
    /// and database choice as <see cref="PlanChurnSql"/> ($1 to $4 mean the same). It was the second pass of one statement that read
    /// the window's index entries twice, one pass after the other. It is its own statement now so
    /// <see cref="GetPlanChurnAsync"/> can run it beside <see cref="PlanChurnSql"/> on a second connection: on a cold cache the two
    /// passes' page reads overlap and the call waits for the slower one, not for the sum. The values are the ones the single
    /// statement produced: <c>COUNT(*)</c> over <c>SELECT DISTINCT server_id, database_name, collection_time</c>.
    /// </summary>
    public const string PlanChurnCollectionsSql = """
        SELECT
            server_id,
            database_name,
            COUNT(*) AS collections_observed
        FROM
        (
            SELECT DISTINCT server_id, database_name, collection_time
            FROM query_store_stats
            WHERE server_id = ANY($1::int[])
            AND   collection_time > $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ) AS collections
        GROUP BY server_id, database_name
        """;

    /// <summary>
    /// #9148: the raw arms of the closed-day statements. $5/$6 are the whole UTC days <c>[$5, $6)</c> the clutter rollups serve. The raw
    /// table is read for the rest of the window only, as two ranges of <c>collection_time</c>, so the chunks of the served days are never walked.
    /// </summary>
    private static string RawOutsideServedDays(string columns, string rawBelow = "$5", string rawFrom = "$6") => $"""
            SELECT {columns}
            FROM query_store_stats
            WHERE server_id = ANY($1::int[])
            AND   collection_time > $2
            AND   collection_time <= $3
            AND   collection_time < {rawBelow}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            UNION ALL
            SELECT {columns}
            FROM query_store_stats
            WHERE server_id = ANY($1::int[])
            AND   collection_time > $2
            AND   collection_time <= $3
            AND   collection_time >= {rawFrom}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        """;

    /// <summary>
    /// #9148: <see cref="PlanChurnSql"/> with the whole UTC days <c>[$5, $6)</c> taken from <c>collect.query_store_clutter_plan_daily</c>
    /// and the rest of the window from raw. A plan's first sighting is the minimum of its daily first sightings and its raw rows, and its
    /// last sighting the maximum, so every figure from <c>per_plan</c> on is the raw statement's. $1 to $4 mean what they mean there.
    /// </summary>
    public static readonly string PlanChurnClosedDaysSql = $"""
        WITH sightings AS NOT MATERIALIZED
        (
        {RawOutsideServedDays("server_id, database_name, query_id, plan_id, collection_time AS first_seen, collection_time AS last_seen")}
            UNION ALL
            SELECT server_id, database_name, query_id, plan_id, first_seen, last_seen
            FROM collect.query_store_clutter_plan_daily
            WHERE server_id = ANY($1::int[])
            AND   day_utc >= $5::date
            AND   day_utc < $6::date
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        per_plan AS
        (
            SELECT
                server_id,
                database_name,
                query_id,
                plan_id,
                MIN(first_seen) AS first_seen,
                MAX(last_seen) AS last_seen
            FROM sightings
            GROUP BY server_id, database_name, query_id, plan_id
        ),
        """ + "\n" + PlanChurnTail;

    /// <summary>
    /// #9148: <see cref="PlanChurnCollectionsSql"/> with the whole UTC days <c>[$5, $6)</c> taken from
    /// <c>collect.query_store_clutter_collections_daily</c>. A collection belongs to one day and the raw ranges hold no served day, so the
    /// daily counts and the raw distinct count add up to the raw statement's count.
    /// </summary>
    public static readonly string PlanChurnCollectionsClosedDaysSql = $"""
        SELECT
            server_id,
            database_name,
            SUM(collections)::bigint AS collections_observed
        FROM
        (
            SELECT server_id, database_name, COUNT(*) AS collections
            FROM
            (
                SELECT DISTINCT server_id, database_name, collection_time
                FROM
                (
        {RawOutsideServedDays("server_id, database_name, collection_time")}
                ) AS raw_rows
            ) AS raw_collections
            GROUP BY server_id, database_name
            UNION ALL
            SELECT server_id, database_name, collections
            FROM collect.query_store_clutter_collections_daily
            WHERE server_id = ANY($1::int[])
            AND   day_utc >= $5::date
            AND   day_utc < $6::date
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ) AS parts
        GROUP BY server_id, database_name
        """;

    /// <summary>
    /// #9173: <see cref="PlanChurnClosedDaysSql"/> with whole hours as well. $5 to $8 are the served range: the hours <c>[$5, $6)</c> from
    /// <c>collect.query_store_clutter_plan_hourly</c>, the whole UTC days <c>[$6, $7)</c> from the daily table, and the hours <c>[$7, $8)</c>
    /// from the hourly table; raw is read below $5 and from $8 on. With no served day $6 = $7, and the hours are one range. Every figure
    /// from <c>per_plan</c> on is the raw statement's, because the first sighting is a minimum and the last a maximum over parts that do not
    /// overlap.
    /// </summary>
    public static readonly string PlanChurnClosedHoursSql = $"""
        WITH sightings AS NOT MATERIALIZED
        (
        {RawOutsideServedDays("server_id, database_name, query_id, plan_id, collection_time AS first_seen, collection_time AS last_seen", "$5", "$8")}
            UNION ALL
            SELECT server_id, database_name, query_id, plan_id, first_seen, last_seen
            FROM collect.query_store_clutter_plan_daily
            WHERE server_id = ANY($1::int[])
            AND   day_utc >= $6::date
            AND   day_utc < $7::date
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            UNION ALL
            SELECT server_id, database_name, query_id, plan_id, first_seen, last_seen
            FROM collect.query_store_clutter_plan_hourly
            WHERE server_id = ANY($1::int[])
            AND   hour_utc >= $5
            AND   hour_utc < $6
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            UNION ALL
            SELECT server_id, database_name, query_id, plan_id, first_seen, last_seen
            FROM collect.query_store_clutter_plan_hourly
            WHERE server_id = ANY($1::int[])
            AND   hour_utc >= $7
            AND   hour_utc < $8
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        per_plan AS
        (
            SELECT
                server_id,
                database_name,
                query_id,
                plan_id,
                MIN(first_seen) AS first_seen,
                MAX(last_seen) AS last_seen
            FROM sightings
            GROUP BY server_id, database_name, query_id, plan_id
        ),
        """ + "\n" + PlanChurnTail;

    /// <summary>
    /// #9173: <see cref="PlanChurnCollectionsClosedDaysSql"/> with whole hours as well ($5 to $8 as in <see cref="PlanChurnClosedHoursSql"/>). A
    /// collection belongs to one hour and one day and the raw ranges hold no served hour or day, so the hourly counts, the daily counts and the
    /// raw distinct count add up to the raw statement's count.
    /// </summary>
    public static readonly string PlanChurnCollectionsClosedHoursSql = $"""
        SELECT
            server_id,
            database_name,
            SUM(collections)::bigint AS collections_observed
        FROM
        (
            SELECT server_id, database_name, COUNT(*) AS collections
            FROM
            (
                SELECT DISTINCT server_id, database_name, collection_time
                FROM
                (
        {RawOutsideServedDays("server_id, database_name, collection_time", "$5", "$8")}
                ) AS raw_rows
            ) AS raw_collections
            GROUP BY server_id, database_name
            UNION ALL
            SELECT server_id, database_name, collections
            FROM collect.query_store_clutter_collections_daily
            WHERE server_id = ANY($1::int[])
            AND   day_utc >= $6::date
            AND   day_utc < $7::date
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            UNION ALL
            SELECT server_id, database_name, collections
            FROM collect.query_store_clutter_collections_hourly
            WHERE server_id = ANY($1::int[])
            AND   hour_utc >= $5
            AND   hour_utc < $6
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            UNION ALL
            SELECT server_id, database_name, collections
            FROM collect.query_store_clutter_collections_hourly
            WHERE server_id = ANY($1::int[])
            AND   hour_utc >= $7
            AND   hour_utc < $8
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ) AS parts
        GROUP BY server_id, database_name
        """;

    /// <summary>
    /// #9173: what a read takes from the clutter rollups. <c>[Start, DayStart)</c> and <c>[DayEnd, End)</c> are whole hours from the hourly
    /// rollup and <c>[DayStart, DayEnd)</c> whole UTC days from the daily one; raw is read outside <c>[Start, End)</c>. <see cref="Hours"/> is false
    /// when the hourly rollup serves nothing, and <c>Start = DayStart</c> and <c>DayEnd = End</c> then.
    /// </summary>
    internal readonly record struct ClutterRoute(DateTime Start, DateTime DayStart, DateTime DayEnd, DateTime End, bool Hours);

    /// <summary>
    /// #9148: the whole UTC days every server of the read can take from the clutter rollups, or null to read raw alone: a store below
    /// <see cref="QueryStoreClosedDayRollup.ClutterVersion"/>, a server with no served run in the window, or no run common to all of them.
    /// Each server's run is its served clutter days (built, not marked, heartbeat fresh), clipped to the days wholly inside retention
    /// on the store's clock (#9287, <see cref="QueryStoreClosedDayRollup.ServedClutterDaysSql"/>). It is not cut to the days raw still
    /// holds whole: a built day answers as collected, so a day whose raw rows the purge already dropped still comes from its rollup,
    /// until the day itself is past the retention edge. The same day read from raw alone (a missing rollup, or the fault fallback) can
    /// lack those rows. The window is <c>(start, end]</c>, so a start at midnight leaves its own day out.
    /// </summary>
    internal static async Task<QueryStoreClosedDayRollup.ServedDays?> ResolveClosedDaysAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        if (serverIds.Length == 0)
        {
            return null;
        }

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        bool hasFold;
        await using (var probe = new NpgsqlCommand(
            "SELECT to_regclass('collect.query_store_clutter_collections_daily') IS NOT NULL, to_regclass('collect.query_store_clutter_day_built') IS NOT NULL", connection)
            { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
        await using (var probed = await probe.ExecuteReaderAsync(cancellationToken))
        {
            await probed.ReadAsync(cancellationToken);
            if (!probed.GetBoolean(0))
            {
                return null;
            }

            hasFold = probed.GetBoolean(1);
        }

        var readStart = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified).AddTicks(1);
        var end = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified);
        DateTime commonStart = DateTime.MinValue, commonEnd = DateTime.MaxValue;
        foreach (var serverId in serverIds.Distinct())
        {
            var served = await QueryStoreClosedDayRollup.ResolveServedClutterDaysAsync(
                connection, null, serverId, readStart, end, StorageCommandDeadlines.McpReadSeconds, cancellationToken, hasFold);
            if (served is not { } run)
            {
                return null;
            }

            commonStart = run.Start > commonStart ? run.Start : commonStart;
            commonEnd = run.End < commonEnd ? run.End : commonEnd;
        }

        return commonStart < commonEnd ? new QueryStoreClosedDayRollup.ServedDays(commonStart, commonEnd) : null;
    }

    /// <summary>
    /// #9173: what every server of the read can take from the clutter rollups, or null to read raw alone. The whole days come from
    /// <see cref="ResolveClosedDaysAsync"/>'s rule (a run of built, unmarked days common to every server). Around them, each server's whole hours
    /// that are built and unmarked, in an unbroken run that joins the days at both ends, extend the served range, and the range is the one every
    /// server holds. With no common run of days the longest run of hours every server holds is the whole range. A server whose hour heartbeat is not
    /// fresh serves no hour. Days are clipped to retention and not cut to raw (#9287, see <see cref="ResolveClosedDaysAsync"/>). Hours are cut to
    /// what raw still holds whole: the hourly rollup has no edge-day exception.
    /// </summary>
    internal static async Task<ClutterRoute?> ResolveClutterRouteAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        if (serverIds.Length == 0)
        {
            return null;
        }

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        bool hasDaily, hasHourly, hasFold;
        await using (var probe = new NpgsqlCommand(
            "SELECT to_regclass('collect.query_store_clutter_collections_daily') IS NOT NULL, to_regclass('collect.query_store_clutter_collections_hourly') IS NOT NULL, to_regclass('collect.query_store_clutter_day_built') IS NOT NULL",
            connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
        await using (var probed = await probe.ExecuteReaderAsync(cancellationToken))
        {
            await probed.ReadAsync(cancellationToken);
            hasDaily = probed.GetBoolean(0);
            hasHourly = probed.GetBoolean(1);
            hasFold = probed.GetBoolean(2);
        }

        if (!hasDaily)
        {
            return null;
        }

        var readStart = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified).AddTicks(1);
        var end = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified);
        var firstHour = QueryStoreClutterHourly.CeilHour(readStart);
        var afterLastHour = QueryStoreClutterHourly.FloorHour(end);

        var servers = new List<(QueryStoreClosedDayRollup.ServedDays? Days, HashSet<DateTime> Hours, DateTime LowestHour)>();
        foreach (var serverId in serverIds.Distinct())
        {
            var days = await QueryStoreClosedDayRollup.ResolveServedClutterDaysAsync(
                connection, null, serverId, readStart, end, StorageCommandDeadlines.McpReadSeconds, cancellationToken, hasFold);

            await using var floorCommand = new NpgsqlCommand(RawWindowFloor.FloorSql(RawWindowFloor.Table.QueryStoreStats), connection)
            {
                CommandTimeout = StorageCommandDeadlines.McpReadSeconds,
            };
            floorCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            AddTimestamp(floorCommand, startUtc);
            AddTimestamp(floorCommand, endUtc);
            if (await floorCommand.ExecuteScalarAsync(cancellationToken) is not DateTime floor)
            {
                return null;
            }

            var lowestHour = QueryStoreClutterHourly.CeilHour(floor);
            if (lowestHour < firstHour)
            {
                lowestHour = firstHour;
            }

            var hours = hasHourly
                ? await QueryStoreClutterHourly.ResolveServedHoursAsync(
                    connection, null, serverId, lowestHour, afterLastHour, StorageCommandDeadlines.McpReadSeconds, cancellationToken) ?? []
                : [];
            servers.Add((days, hours, lowestHour));
        }

        if (servers.All(x => x.Days is not null))
        {
            var dayStart = servers.Max(x => x.Days!.Value.Start);
            var dayEnd = servers.Min(x => x.Days!.Value.End);
            if (dayStart < dayEnd)
            {
                var hourStart = dayStart;
                var hourEnd = dayEnd;
                if (hasHourly)
                {
                    hourStart = servers.Max(x => QueryStoreClutterHourly.ExtendBack(x.Hours, dayStart, x.LowestHour));
                    hourEnd = servers.Min(x => QueryStoreClutterHourly.ExtendForward(x.Hours, dayEnd, afterLastHour));
                }

                return new ClutterRoute(hourStart, dayStart, dayEnd, hourEnd, hourStart < dayStart || hourEnd > dayEnd);
            }
        }

        if (!hasHourly)
        {
            return null;
        }

        DateTime commonStart = DateTime.MinValue, commonEnd = DateTime.MaxValue;
        foreach (var server in servers)
        {
            if (QueryStoreClutterHourly.LongestRun(server.Hours) is not var (runStart, runEnd))
            {
                return null;
            }

            commonStart = runStart > commonStart ? runStart : commonStart;
            commonEnd = runEnd < commonEnd ? runEnd : commonEnd;
        }

        return commonStart < commonEnd ? new ClutterRoute(commonStart, commonStart, commonStart, commonEnd, true) : null;
    }

    /// <summary>
    /// #9478: the narrow plan-churn statements, for the fleet median alone. The median reads <c>plans_per_query_p95</c> per (server, database)
    /// and nothing else of the churn arm, so these carry the four key columns of each source and no first or last sighting, no per-database
    /// detail and no collections statement. With only the key columns every arm can be an index-only scan: the two rollups on their unique
    /// indexes, and raw query_store_stats on the non-unique tuning index idx_query_store_stats_server_db_query_plan_time.
    /// $1 to $4 and the served range in $5 to $8 mean what they mean in <see cref="PlanChurnSql"/>, <see cref="PlanChurnClosedDaysSql"/>
    /// and <see cref="PlanChurnClosedHoursSql"/>, and the count of distinct plans per (server, database, query) is the same, because a plan
    /// is one (server, database, query, plan) whichever source and however many sightings it has.
    /// </summary>
    private const string PlanChurnP95Tail = """
        per_query AS
        (
            SELECT server_id, database_name, query_id, COUNT(*) AS plans
            FROM pairs
            GROUP BY server_id, database_name, query_id
        )
        SELECT
            server_id,
            database_name,
            (percentile_disc(0.95) WITHIN GROUP (ORDER BY plans))::int AS plans_per_query_p95
        FROM per_query
        GROUP BY server_id, database_name
        ORDER BY server_id, database_name
        """;

    /// <summary>#9478: the narrow statement over the distinct plans of the window, read from raw alone.</summary>
    public static readonly string PlanChurnP95Sql = """
        WITH pairs AS NOT MATERIALIZED
        (
            SELECT DISTINCT server_id, database_name, query_id, plan_id
            FROM query_store_stats
            WHERE server_id = ANY($1::int[])
            AND   collection_time > $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        """ + "\n" + PlanChurnP95Tail;

    /// <summary>#9478: <see cref="PlanChurnP95Sql"/> with the whole UTC days <c>[$5, $6)</c> from <c>collect.query_store_clutter_plan_daily</c>, as <see cref="PlanChurnClosedDaysSql"/> does.</summary>
    public static readonly string PlanChurnP95ClosedDaysSql = $"""
        WITH pairs AS NOT MATERIALIZED
        (
            SELECT DISTINCT server_id, database_name, query_id, plan_id
            FROM
            (
        {RawOutsideServedDays("server_id, database_name, query_id, plan_id")}
                UNION ALL
                SELECT server_id, database_name, query_id, plan_id
                FROM collect.query_store_clutter_plan_daily
                WHERE server_id = ANY($1::int[])
                AND   day_utc >= $5::date
                AND   day_utc < $6::date
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
            ) AS sightings
        ),
        """ + "\n" + PlanChurnP95Tail;

    /// <summary>#9478: <see cref="PlanChurnP95Sql"/> with whole hours as well ($5 to $8 as in <see cref="PlanChurnClosedHoursSql"/>).</summary>
    public static readonly string PlanChurnP95ClosedHoursSql = $"""
        WITH pairs AS NOT MATERIALIZED
        (
            SELECT DISTINCT server_id, database_name, query_id, plan_id
            FROM
            (
        {RawOutsideServedDays("server_id, database_name, query_id, plan_id", "$5", "$8")}
                UNION ALL
                SELECT server_id, database_name, query_id, plan_id
                FROM collect.query_store_clutter_plan_daily
                WHERE server_id = ANY($1::int[])
                AND   day_utc >= $6::date
                AND   day_utc < $7::date
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
                UNION ALL
                SELECT server_id, database_name, query_id, plan_id
                FROM collect.query_store_clutter_plan_hourly
                WHERE server_id = ANY($1::int[])
                AND   hour_utc >= $5
                AND   hour_utc < $6
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
                UNION ALL
                SELECT server_id, database_name, query_id, plan_id
                FROM collect.query_store_clutter_plan_hourly
                WHERE server_id = ANY($1::int[])
                AND   hour_utc >= $7
                AND   hour_utc < $8
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
            ) AS sightings
        ),
        """ + "\n" + PlanChurnP95Tail;

    /// <param name="databases">#5244: the chosen databases (optional, so the Viewer's calls compile unchanged).</param>
    public static async Task<List<PlanChurnRow>> GetPlanChurnAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, DatabaseFilter databases = default, DateTime? asOfUtc = null,
        ICollection<string>? omittedDatabases = null, ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        /* #8570: the two statements read the window's index entries independently, so they run together on two pooled
           connections. A statement on a connection of its own cannot share a transaction with the other, and each one is a
           single read, so there is no snapshot to keep consistent between them beyond the window both name. */
        /* #9148: the whole UTC days the clutter rollups serve for every server of the read, read from the rollups; null reads raw alone,
           with the statements as they were. */
        var closedDays = await ResolveClutterRouteAsync(postgres, serverIds, startUtc, endUtc, cancellationToken);
        var collectionsTask = ReadChurnCollectionsAsync(postgres, serverIds, startUtc, endUtc, databases, closedDays, cancellationToken);
        var rows = new List<PlanChurnRow>();
        try
        {
            await using var command = postgres.CreateCommand(closedDays switch { null => PlanChurnSql, { Hours: true } => PlanChurnClosedHoursSql, _ => PlanChurnClosedDaysSql });
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            AddServers(command, serverIds);
            AddTimestamp(command, startUtc);
            AddTimestamp(command, endUtc);
            command.Parameters.Add(databases.Parameter());
            AddClosedDays(command, closedDays);
            var facts = new List<(int ServerId, string Database, DateTime First, DateTime Last, int Queries, int Plans, int P95, int Max, int Once, int AfterFirst)>();
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    facts.Add((reader.GetInt32(0), reader.GetString(1), reader.GetDateTime(2), reader.GetDateTime(3), reader.GetInt32(4),
                        reader.GetInt32(5), reader.GetInt32(6), reader.GetInt32(7), reader.GetInt32(8), reader.GetInt32(9)));
                }
            }

            var collections = await collectionsTask;
            foreach (var f in facts)
            {
                rows.Add(new PlanChurnRow(
                    f.ServerId, f.Database, f.First, f.Last, collections.GetValueOrDefault((f.ServerId, f.Database)),
                    f.Queries, f.Plans, f.P95, f.Max, f.Once, f.AfterFirst));
            }
        }
        catch
        {
            /* the second statement must not be left running (and unobserved) when the first one fails */
            try { await collectionsTask; } catch (Exception) { /* the first failure is the one reported */ }
            throw;
        }

        /* #5633 item 4: a database the server no longer has (absent from the newest database_states snapshot) is left out when no
           database was chosen, so it cannot carry a verdict, a count or a recommendation. See CurrentDatabaseScope. */
        return await CurrentDatabaseScope.OmitDroppedAsync(postgres, rows, r => r.ServerId, r => r.DatabaseName, databases, r => r.LastCollection, asOfUtc, omittedDatabases, logger, cancellationToken);
    }

    /// <summary>
    /// #9478: <see cref="GetPlanChurnAsync"/>'s <c>plans_per_query_p95</c> per (server, database) over every database, from the narrow
    /// statements above, for the fleet median. It takes the same route (<see cref="ResolveClutterRouteAsync"/>) and drops the same dropped
    /// databases, but reads only the key columns of each source and runs no collections statement, so on a large store it reads less and
    /// stays inside the caller's token. The narrow rows carry no last sighting, so a database created after the newest
    /// <c>database_states</c> snapshot is not kept by it here and joins the median at the next snapshot. The per-server read keeps that rule.
    /// </summary>
    public static async Task<List<PlanChurnP95Row>> GetPlanChurnP95Async(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, DateTime? asOfUtc = null,
        ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        var closedDays = await ResolveClutterRouteAsync(postgres, serverIds, startUtc, endUtc, cancellationToken);

        /* #9882: one statement per server on the route every server shares, FleetChurnParallelism at a time. The statement groups by
           server first, so the rows are the ones a single statement over the whole fleet returns. One server's distinct sort fits in
           memory where the fleet's spills to disk, and the statements read the index on several connections at once. */
        var ids = serverIds.Distinct().Order().ToArray();
        var perServer = new List<PlanChurnP95Row>[ids.Length];
        await Parallel.ForEachAsync(
            Enumerable.Range(0, ids.Length),
            new ParallelOptions { MaxDegreeOfParallelism = FleetChurnParallelism, CancellationToken = cancellationToken },
            async (i, token) => perServer[i] = await ReadPlanChurnP95StatementAsync(postgres, new[] { ids[i] }, startUtc, endUtc, closedDays, token));
        var rows = perServer.SelectMany(r => r).ToList();

        return await CurrentDatabaseScope.OmitDroppedAsync(postgres, rows, r => r.ServerId, r => r.DatabaseName, DatabaseFilter.All, null, asOfUtc, null, logger, cancellationToken);
    }

    /// <summary>#9882: how many per-server statements of <see cref="GetPlanChurnP95Async"/> run at once, each on its own pooled connection.</summary>
    internal const int FleetChurnParallelism = 4;

    /// <summary>Test seam (#9882): runs in each statement of <see cref="GetPlanChurnP95Async"/> once it holds its connection and is handed
    /// the statement's server ids and token, so a test can hold the statements in flight and count them. Unset in production.</summary>
    internal static readonly AsyncLocal<Func<int[], CancellationToken, Task>?> FleetChurnStatementSeam = new();

    /// <summary>#9882: one narrow plan-churn statement over <paramref name="serverIds"/> on a route already resolved
    /// (<see cref="ResolveClutterRouteAsync"/>), with no dropped-database filter. <see cref="GetPlanChurnP95Async"/> runs it once per server.</summary>
    internal static async Task<List<PlanChurnP95Row>> ReadPlanChurnP95StatementAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, ClutterRoute? closedDays, CancellationToken cancellationToken)
    {
        var rows = new List<PlanChurnP95Row>();
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        if (FleetChurnStatementSeam.Value is { } seam) await seam(serverIds, cancellationToken);
        await using var command = new NpgsqlCommand(closedDays switch { null => PlanChurnP95Sql, { Hours: true } => PlanChurnP95ClosedHoursSql, _ => PlanChurnP95ClosedDaysSql }, connection);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        AddClosedDays(command, closedDays);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PlanChurnP95Row(reader.GetInt32(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return rows;
    }

    /// <summary>
    /// #9382: where the plan-churn arm's coverage of a window starts, from what <see cref="GetPlanChurnAsync"/> reads for it: the clutter
    /// rollups from the start of the route (<see cref="ResolveClutterRouteAsync"/>) and raw <c>query_store_stats</c> outside it. So it is the
    /// earlier of the route's start and the oldest raw row (<see cref="RawWindowFloor.GetAsync"/>), and the oldest raw row alone when no
    /// rollup is served. A start before the window's own means the arm covers the whole window. Null when neither source holds the window.
    /// Several servers answer the latest of their starts: the least any one of them covers. <paramref name="rawFloorSeconds"/> is the deadline of
    /// the raw floor probe: the MCP read's by default, the Viewer's interactive one from the Viewer. The route read keeps the deadline
    /// <see cref="ResolveClutterRouteAsync"/> sets for every clutter read.
    /// </summary>
    public static async Task<DateTime?> GetPlanChurnCoverageStartAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc,
        int rawFloorSeconds = StorageCommandDeadlines.McpReadSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var route = await ResolveClutterRouteAsync(postgres, serverIds, startUtc, endUtc, cancellationToken);
        DateTime? start = null;
        foreach (var serverId in serverIds.Distinct())
        {
            var raw = await RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.QueryStoreStats, serverId, startUtc, endUtc, rawFloorSeconds, cancellationToken);
            DateTime? own = (raw, route) switch
            {
                (null, null) => null,
                ({ } r, null) => r,
                (null, { } t) => t.Start,
                ({ } r, { } t) => r < t.Start ? r : t.Start,
            };
            if (own is { } o && (start is null || o > start))
            {
                start = o;
            }
        }

        return start;
    }

    private static async Task<Dictionary<(int ServerId, string Database), int>> ReadChurnCollectionsAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, DatabaseFilter databases,
        ClutterRoute? closedDays, CancellationToken cancellationToken)
    {
        var counts = new Dictionary<(int ServerId, string Database), int>();
        await using var command = postgres.CreateCommand(closedDays switch { null => PlanChurnCollectionsSql, { Hours: true } => PlanChurnCollectionsClosedHoursSql, _ => PlanChurnCollectionsClosedDaysSql });
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        command.Parameters.Add(databases.Parameter());
        AddClosedDays(command, closedDays);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            counts[(reader.GetInt32(0), reader.GetString(1))] = (int)reader.GetInt64(2);
        }

        return counts;
    }

    /* ─────────────────────────── (c) configuration ─────────────────────────── */

    /// <summary>
    /// The newest <c>query_store_health</c> capture per (server, database) INSIDE the window — the
    /// configuration the churn arm is judged against, stamped with its own <c>capture_time</c>. Bounded by the
    /// window at both ends on purpose: an anchored read about a past incident must not be told today's
    /// configuration, and a database that left the server weeks ago must not keep appearing with a stale
    /// row. The collector is hourly, so the default 24-hour window always holds one; a one-hour window may
    /// not, and the tool says so rather than reaching outside it. <c>DISTINCT ON</c> rather than a
    /// <c>MAX(capture_time)</c> anchor because the newest capture is per DATABASE, not per server — a
    /// database the collector could not enter on the newest cycle still has its previous row.
    ///
    /// <para><c>query_capture_mode</c> is the last projected column, and its position is load-bearing: the
    /// reader maps it at ordinal 12, so anything inserted before it shifts every ordinal above. A NULL there
    /// is a row the collector wrote before the column existed — never asked, and never the engine's
    /// <c>NONE</c>.</para>
    /// $1 server_id array, $2/$3 window (naive UTC), $4 the chosen databases (#5244: one text[], NULL for every database).
    /// </summary>
    public const string ConfigSql = """
        SELECT DISTINCT ON (server_id, database_name)
            server_id,
            database_name,
            actual_state,
            desired_state,
            readonly_reason,
            current_storage_size_mb,
            max_storage_size_mb,
            size_based_cleanup_mode,
            stale_query_threshold_days,
            max_plans_per_query,
            interval_length_minutes,
            capture_time,
            query_capture_mode
        FROM v_query_store_health
        WHERE server_id = ANY($1::int[])
        AND   capture_time > $2
        AND   capture_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ORDER BY server_id, database_name, capture_time DESC
        """;

    /// <param name="databases">#5244: the chosen databases (optional, so the Viewer's calls compile unchanged).</param>
    public static async Task<List<ConfigRow>> GetConfigAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, DatabaseFilter databases = default, DateTime? asOfUtc = null,
        ICollection<string>? omittedDatabases = null, ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<ConfigRow>();
        await using var command = postgres.CreateCommand(ConfigSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* String fields coalesce DBNull to "" and numerics to 0 — DarlingConfigHistoryReader's
               QueryStoreHealthReadRow defaults, so the two reads over one table serialize the same. */
            rows.Add(new ConfigRow(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0L : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0L : reader.GetInt64(6),
                reader.IsDBNull(7) ? "" : reader.GetString(7),
                reader.IsDBNull(8) ? 0L : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0L : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0L : reader.GetInt64(10),
                reader.GetDateTime(11),
                /* NOT coalesced to "": an empty string would be a mode, and this column's null is the
                   absence of a capture, which the payload has a flag for. */
                reader.IsDBNull(12) ? null : reader.GetString(12)));
        }

        /* #5633 item 4: a database the server no longer has (absent from the newest database_states snapshot) is left out when no
           database was chosen, so it cannot carry a verdict, a count or a recommendation. See CurrentDatabaseScope. */
        return await CurrentDatabaseScope.OmitDroppedAsync(postgres, rows, r => r.ServerId, r => r.DatabaseName, databases, r => r.CapturedAt, asOfUtc, omittedDatabases, logger, cancellationToken);
    }

    /* ─────────────────────────── (d) QDS waits + the clerk ─────────────────────────── */

    /// <summary>
    /// Every <c>QDS_*</c> wait type's stored deltas summed over the window, per server. The deltas are the
    /// COLLECTOR's (<c>WaitStatsCollector</c>: <c>delta_wait_time_ms</c> / <c>delta_waiting_tasks</c>, with
    /// the epoch and counter-reset handling already applied — an unknowable delta is stored as (0, 0)), so
    /// no differencing happens here and a restart cannot manufacture a negative. The rate arm is formed only
    /// from rows whose <c>sample_interval_seconds &gt; 0</c> (#3540, the interval-honest rule every rated read
    /// follows): <c>rated_wait_ms</c> over <c>measured_seconds</c>, both sums over the same rows, so the
    /// tool's per-hour figure divides a measured quantity by the measured span it accrued over and never by
    /// a cadence. The three row counts say what was left out of the rate and why.
    ///
    /// <para><c>LIKE 'QDS\_%'</c> with the underscore escaped: the wait-type prefix is literal, and the four
    /// sleep waits <c>IgnoredWaitDefaults</c> drops at collection are filtered again by the tool (by name,
    /// against that list) so a store that predates the list, or a row that slipped past it, still lands in
    /// <c>excluded_wait_types</c> rather than in the proxy.</para>
    /// $1 server_id array, $2/$3 window (naive UTC).
    /// </summary>
    public const string QdsWaitsSql = """
        SELECT
            server_id,
            wait_type,
            CAST(SUM(delta_wait_time_ms) AS bigint) AS wait_ms_total,
            CAST(SUM(delta_waiting_tasks) AS bigint) AS waiting_tasks_total,
            CAST(COALESCE(SUM(delta_wait_time_ms) FILTER (WHERE sample_interval_seconds > 0), 0) AS bigint) AS rated_wait_ms,
            CAST(COALESCE(SUM(sample_interval_seconds) FILTER (WHERE sample_interval_seconds > 0), 0) AS bigint) AS measured_seconds,
            COUNT(*)::int AS rows_observed,
            (COUNT(*) FILTER (WHERE sample_interval_seconds IS NULL))::int AS rows_without_interval,
            (COUNT(*) FILTER (WHERE sample_interval_seconds = 0))::int AS rows_unknowable,
            MIN(collection_time) AS first_observed,
            MAX(collection_time) AS last_observed
        FROM v_wait_stats
        WHERE server_id = ANY($1::int[])
        AND   collection_time > $2
        AND   collection_time <= $3
        AND   wait_type LIKE 'QDS\_%'
        GROUP BY server_id, wait_type
        ORDER BY server_id, wait_ms_total DESC, wait_type
        """;

    public static async Task<List<QdsWaitRow>> GetQdsWaitsAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<QdsWaitRow>();
        await using var command = postgres.CreateCommand(QdsWaitsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new QdsWaitRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? 0L : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0L : reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt32(6),
                reader.GetInt32(7),
                reader.GetInt32(8),
                reader.GetDateTime(9),
                reader.GetDateTime(10)));
        }

        return rows;
    }

    /// <summary>
    /// The Query Store memory clerk over the window on ONE server, beside the newest capture of any clerk.
    /// The collector stores the top 25 clerks over 1 MB per cycle (<c>MemoryClerksCollector</c>), so the
    /// clerk's absence from a capture is a fact about its RANK, not a zero — which is why the read returns
    /// the newest capture of anything alongside the clerk's newest appearance: equal stamps mean the clerk is
    /// in the current top 25, an older clerk stamp means it has since fallen out, and a null with captures
    /// present means it never made the cut in the window. Never a fleet read: the clerk is a per-server
    /// figure with no fleet reference in this payload. $1 server_id, $2/$3 window (naive UTC), $4 clerk type.
    /// </summary>
    public const string QueryStoreClerkSql = """
        WITH captures AS
        (
            SELECT MAX(collection_time) AS latest_capture, COUNT(DISTINCT collection_time) AS captures
            FROM v_memory_clerks
            WHERE server_id = $1
            AND   collection_time > $2
            AND   collection_time <= $3
        ),
        clerk AS
        (
            SELECT memory_mb, collection_time
            FROM v_memory_clerks
            WHERE server_id = $1
            AND   clerk_type = $4
            AND   collection_time > $2
            AND   collection_time <= $3
        ),
        latest AS
        (
            SELECT memory_mb, collection_time
            FROM clerk
            ORDER BY collection_time DESC
            LIMIT 1
        ),
        span AS
        (
            SELECT MAX(memory_mb) AS max_memory_mb, COUNT(*) AS clerk_samples
            FROM clerk
        )
        SELECT
            c.latest_capture,
            c.captures::int,
            l.memory_mb,
            l.collection_time,
            s.max_memory_mb,
            s.clerk_samples::int
        FROM captures AS c
        CROSS JOIN span AS s
        LEFT JOIN latest AS l
          ON TRUE
        """;

    public static async Task<ClerkRow> GetQueryStoreClerkAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(QueryStoreClerkSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        command.Parameters.AddWithValue(QueryStoreMemoryClerk);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new ClerkRow(null, 0, null, null, null, 0);
        }

        return new ClerkRow(
            reader.IsDBNull(0) ? null : reader.GetDateTime(0),
            reader.GetInt32(1),
            reader.IsDBNull(2) ? null : reader.GetDecimal(2),
            reader.IsDBNull(3) ? null : reader.GetDateTime(3),
            reader.IsDBNull(4) ? null : reader.GetDecimal(4),
            reader.GetInt32(5));
    }

    /* ─────────────────────────── the fleet's SQL Server targets ─────────────────────────── */

    /// <summary>
    /// The enabled SQL Server targets the opt-in fleet reference is computed over. A registry row whose
    /// <c>engine_kind</c> is NULL (no connect has stamped it since V82) is admitted: it is a SQL Server row
    /// by every other column, and a PostgreSQL target has no <c>query_store</c> rows to contribute anyway.
    /// $1 the SQL Server engine-kind token.
    /// </summary>
    public const string EnabledSqlServerTargetsSql = """
        SELECT server_id
        FROM servers
        WHERE is_enabled
        AND   (engine_kind IS NULL OR engine_kind = $1)
        ORDER BY server_id
        """;

    public static async Task<int[]> GetEnabledSqlServerTargetsAsync(
        NpgsqlDataSource postgres, string sqlServerEngineKind, CancellationToken cancellationToken = default)
    {
        var ids = new List<int>();
        await using var command = postgres.CreateCommand(EnabledSqlServerTargetsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(sqlServerEngineKind);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids.ToArray();
    }

    /// <summary>Binds the server-id array every arm takes as <c>$1</c>.</summary>
    /// <summary>#9148: $5/$6, the served days, on a closed-day statement; nothing on a raw one.</summary>
    private static void AddClosedDays(NpgsqlCommand command, ClutterRoute? route)
    {
        if (route is not { } r)
        {
            return;
        }

        if (r.Hours)
        {
            AddTimestamp(command, r.Start);
            AddTimestamp(command, r.DayStart);
            AddTimestamp(command, r.DayEnd);
            AddTimestamp(command, r.End);
        }
        else
        {
            AddTimestamp(command, r.DayStart);
            AddTimestamp(command, r.DayEnd);
        }
    }

    private static void AddServers(NpgsqlCommand command, int[] serverIds) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });

    /// <summary>
    /// Binds a window edge as <c>Kind=Unspecified</c> — the naive-UTC discipline the whole
    /// <c>DarlingPg*Reader</c> family in this project binds by, and the one this file has to keep. A
    /// <c>Kind=Utc</c> value makes Npgsql infer <c>timestamptz</c>, and PostgreSQL then resolves the
    /// comparison against these naive <c>timestamp</c> columns by converting the COLUMNS at the store
    /// session's TimeZone: east of UTC every fresh row falls out of the window and the read returns
    /// nothing, silently, on a store whose host is not UTC.
    /// </summary>
    private static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });
}
