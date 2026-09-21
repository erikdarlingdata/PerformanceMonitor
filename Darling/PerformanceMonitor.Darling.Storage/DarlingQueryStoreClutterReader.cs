/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
    /// $1 server_id array, $2/$3 window (naive UTC), $4 collector name.
    /// </summary>
    public const string ReadCostSql = """
        WITH runs AS
        (
            SELECT server_id, collection_time, duration_ms, fanout_item_count, slowest_item, slowest_item_ms
            FROM v_collection_log
            WHERE server_id = ANY($1::int[])
            AND   collector_name = $4
            AND   collection_time >= $2
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
        ORDER BY d.server_id, d.runs_slowest DESC, d.slowest_item_ms_p50 DESC, d.database_name
        """;

    public static async Task<List<ReadCostRow>> GetReadCostAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<ReadCostRow>();
        await using var command = postgres.CreateCommand(ReadCostSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        command.Parameters.AddWithValue(CollectorName);
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

        return rows;
    }

    /* ─────────────────────────── (b) plan churn ─────────────────────────── */

    /// <summary>
    /// Per (server, database): plan cardinality and churn over the window's raw <c>query_store_stats</c> rows.
    /// Reads the base table, not <c>v_query_store_stats</c>, for the reason <c>get_query_store_top</c> does —
    /// the raw tier is the only tier that carries <c>plan_id</c>. The projection is exactly the columns of
    /// <c>idx_query_store_stats_server_db_query_plan_time</c>, so the walk can be index-only.
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
    /// $1 server_id array, $2/$3 window (naive UTC).
    /// </summary>
    public const string PlanChurnSql = """
        WITH rows_in_window AS
        (
            SELECT server_id, database_name, query_id, plan_id, collection_time
            FROM query_store_stats
            WHERE server_id = ANY($1::int[])
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        per_plan AS
        (
            SELECT
                server_id,
                database_name,
                query_id,
                plan_id,
                COUNT(DISTINCT collection_time) AS collections_seen,
                MIN(collection_time) AS first_seen
            FROM rows_in_window
            GROUP BY server_id, database_name, query_id, plan_id
        ),
        per_query AS
        (
            SELECT server_id, database_name, query_id, COUNT(*) AS plans
            FROM per_plan
            GROUP BY server_id, database_name, query_id
        ),
        per_database AS
        (
            SELECT
                server_id,
                database_name,
                MIN(collection_time) AS first_collection,
                MAX(collection_time) AS last_collection,
                COUNT(DISTINCT collection_time) AS collections_observed
            FROM rows_in_window
            GROUP BY server_id, database_name
        ),
        plan_facts AS
        (
            SELECT
                p.server_id,
                p.database_name,
                COUNT(*) AS distinct_plans,
                COUNT(*) FILTER (WHERE p.collections_seen = 1) AS plans_seen_once,
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
            d.collections_observed::int,
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

    public static async Task<List<PlanChurnRow>> GetPlanChurnAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<PlanChurnRow>();
        await using var command = postgres.CreateCommand(PlanChurnSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PlanChurnRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetDateTime(2),
                reader.GetDateTime(3),
                reader.GetInt32(4),
                reader.GetInt32(5),
                reader.GetInt32(6),
                reader.GetInt32(7),
                reader.GetInt32(8),
                reader.GetInt32(9),
                reader.GetInt32(10)));
        }

        return rows;
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
    /// $1 server_id array, $2/$3 window (naive UTC).
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
            /* V137 (#3796). NULL on every row captured before that rung and on a row a 2016 engine's
               collector wrote nothing into; the composition publishes the NULL as "never asked". */
            query_capture_mode
        FROM v_query_store_health
        WHERE server_id = ANY($1::int[])
        AND   capture_time >= $2
        AND   capture_time <= $3
        ORDER BY server_id, database_name, capture_time DESC
        """;

    public static async Task<List<ConfigRow>> GetConfigAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<ConfigRow>();
        await using var command = postgres.CreateCommand(ConfigSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServers(command, serverIds);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
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

        return rows;
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
        AND   collection_time >= $2
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
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        clerk AS
        (
            SELECT memory_mb, collection_time
            FROM v_memory_clerks
            WHERE server_id = $1
            AND   clerk_type = $4
            AND   collection_time >= $2
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
