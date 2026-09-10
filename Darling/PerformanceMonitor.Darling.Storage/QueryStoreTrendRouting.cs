/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Where the Query Store duration trend reads from (#2736): the routing decision and the rollup-routed SQL,
/// shared by the MCP reader (<c>DarlingTrendReader</c>) and the desktop viewer
/// (<c>ViewerDataService.QueryTrends</c>) so the two apps cannot drift apart about the same hour.
///
/// <para><b>The problem this exists for.</b> The trend's raw read does #1841's dedup — a
/// <c>ROW_NUMBER()</c> over <c>query_store_stats</c> at full interval-identity grain — per call, forever,
/// chunk-excluded only by a deliberately slack <c>collection_time</c> slab
/// (<c>-1 day / +30 days</c> around the window). On a store carrying millions of Query Store rows per hour
/// that fixed cost exceeds the mcp role's <c>statement_timeout</c> backstop at ANY window width, including
/// a 2-hour window anchored at now — the read that cannot run on exactly the store big enough to need it.</para>
///
/// <para><b>The route.</b> <c>query_store_stats_corrected_hourly</c> (#1849) already materializes this
/// series' ingredients — per-bucket <c>execution_count_sum</c> and <c>duration_us_weighted_sum</c>, deduped
/// at interval grain by L1's <c>last()</c> — so the window portion the rollup has materialized becomes a
/// rollup scan, and the raw rank runs only over the still-unmaterialized tail (bounded by the refresh
/// policy's <c>end_offset</c>, normally an hour or two). ComposeSourceRouter-style, the decision is gated on
/// what the store actually HAS (#1665: a plain-PostgreSQL store has no CAGGs, and naming an absent relation
/// fails at PARSE time — so the rollup SQL is only ever chosen after <see cref="RollupProbeSql"/> proved the
/// view present) and on what it has MATERIALIZED (#1759: a CAGG born <c>WITH NO DATA</c> serves only what a
/// refresh or backfill has reached; these CAGGs are materialized-only by design, so an un-materialized
/// window reads EMPTY off the view while raw still holds the rows).</para>
///
/// <para><b>What the rollup route trades, stated rather than hidden (#1849's boundary disclosure).</b> The
/// corrected hourly buckets on the COLLECTION hour, not the interval start the raw arm places at, so
/// rollup-served points sit at the hour the snapshots were collected (within the collection lag of the hour
/// the work ran), and an interval whose snapshots straddle an hour boundary contributes to both adjacent
/// buckets — the residual #1869 measured and left at this grain. Callers surface that in their payload
/// rather than presenting the two regions as one estimator. History below the rollup's materialized floor is
/// NOT served by falling back to ranking the raw slab — that is the exact timeout this routing removes — it
/// is disclosed, with <c>--backfill-rollups</c> as the remedy.</para>
/// </summary>
public static class QueryStoreTrendRouting
{
    /// <summary>
    /// Does the corrected hourly rollup EXIST here? <c>to_regclass</c> resolves through the database's
    /// search_path — the same resolution the trend SQL's own bare table names get — and returns NULL rather
    /// than erroring on a plain-PostgreSQL store, which is the availability-first discipline
    /// <see cref="TimescaleSupport.RollupCoverageProbeSql"/> documents: a relation named in a statement is
    /// resolved at parse time, so the rollup SQL may only be CHOSEN after this probe, never guarded
    /// in-statement.
    /// </summary>
    public const string RollupProbeSql =
        "SELECT to_regclass('" + TimescaleSupport.QueryStoreStatsCorrectedHourlyView + "') IS NOT NULL";

    /// <summary>
    /// The rollup's materialized span: oldest and newest bucket. Run ONLY after
    /// <see cref="RollupProbeSql"/> returned true (the relation is parse-time-resolved). Both are global
    /// rather than per-server, deliberately — <c>min(bucket)</c> is the same expression the #1759 coverage
    /// probe and the retention arming gate read, and it answers "how far has the rollup materialized", which
    /// is a property of the rollup, not of one server's data. A server with no rows in a materialized bucket
    /// is genuinely quiet there, and the read reports that honestly as an empty hour.
    /// </summary>
    public const string RollupBoundsSql =
        "SELECT min(bucket), max(bucket) FROM " + TimescaleSupport.QueryStoreStatsCorrectedHourlyView;

    /// <summary>
    /// The routing decision for one Query Store trend read. When <see cref="UseRollup"/> is false the read
    /// keeps its original raw-only SQL byte for byte — the fallback IS today's behavior, chosen for the
    /// stores where it is affordable (no TimescaleDB, or a rollup that has not materialized a single
    /// bucket). When true, <see cref="RawStartUtc"/> is the first instant the raw arms serve; everything
    /// before it comes from the rollup, and <see cref="RollupFloorUtc"/> carries the rollup's oldest
    /// materialized bucket so a caller can disclose a window head the rollup has not reached.
    /// </summary>
    public readonly record struct QueryStoreTrendRoute(bool UseRollup, DateTime RawStartUtc, DateTime? RollupFloorUtc)
    {
        /// <summary>The raw-only route: the pre-#2736 read, unchanged.</summary>
        public static QueryStoreTrendRoute RawOnly => new(false, default, null);
    }

    /// <summary>
    /// The pure decision, split from the probing so it is testable without a store: rollup absent or empty
    /// (no <paramref name="newestBucketUtc"/>) routes raw-only; otherwise the raw arms start at the bucket
    /// AFTER the newest materialized one — <c>newest + 1 hour</c> — so the two regions partition the window
    /// with no overlap: the rollup serves buckets strictly below that instant, raw serves points at or above
    /// it.
    /// </summary>
    public static QueryStoreTrendRoute Resolve(bool rollupExists, DateTime? oldestBucketUtc, DateTime? newestBucketUtc)
    {
        if (!rollupExists || newestBucketUtc is not DateTime newest)
        {
            return QueryStoreTrendRoute.RawOnly;
        }

        return new QueryStoreTrendRoute(true, newest.Add(TimescaleSupport.HourlyBucket), oldestBucketUtc);
    }

    /// <summary>
    /// Probes availability (<see cref="RollupProbeSql"/>) then coverage (<see cref="RollupBoundsSql"/>) and
    /// hands the result to <see cref="Resolve"/>. Two statements by necessity, not laziness: the bounds
    /// statement names the view, so it may only be issued once the probe proved the name resolves.
    /// A probe failure propagates rather than degrading to raw-only — silently falling back would
    /// reintroduce the #2736 timeout wearing a different error.
    /// </summary>
    public static async Task<QueryStoreTrendRoute> ResolveAsync(
        NpgsqlDataSource dataSource, CancellationToken cancellationToken = default)
    {
        if (dataSource is null)
        {
            throw new ArgumentNullException(nameof(dataSource));
        }

        await using (var probe = dataSource.CreateCommand(RollupProbeSql))
        {
            probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            if (await probe.ExecuteScalarAsync(cancellationToken) is not true)
            {
                return QueryStoreTrendRoute.RawOnly;
            }
        }

        await using var bounds = dataSource.CreateCommand(RollupBoundsSql);
        bounds.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await bounds.ExecuteReaderAsync(cancellationToken);
        DateTime? oldest = null;
        DateTime? newest = null;
        if (await reader.ReadAsync(cancellationToken))
        {
            oldest = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
            newest = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
        }

        return Resolve(rollupExists: true, oldest, newest);
    }

    /// <summary>
    /// The rollup-routed trend SQL. $1 server_id, $2/$3 window (naive UTC), $4 the raw boundary
    /// (<see cref="QueryStoreTrendRoute.RawStartUtc"/>); with <paramref name="withDatabaseFilter"/>, $5 is
    /// the viewer's guarded <c>text[]</c> database filter (#1319) on every arm — the corrected hourly
    /// carries <c>database_name</c>, so the filter survives the routing.
    ///
    /// <para><b>The partition seam.</b> The rollup arm takes buckets strictly BELOW $4; the raw arms take
    /// points at or ABOVE it. <c>bucket &lt; $4</c> is load-bearing rather than decorative: a refresh can
    /// materialize another bucket between the route probe and this read, and without the predicate that
    /// bucket would be served by BOTH regions. The raw interval arm keeps whole intervals on one side —
    /// an interval STARTING below $4 belongs to the rollup region even when its later snapshots land above
    /// the watermark. Those later snapshots are visible to NEITHER arm until the next refresh materializes
    /// their bucket, so a watermark-straddling interval is served at its last MATERIALIZED value and its
    /// accrual since then lags the read by the refresh cadence — a visibility LAG bounded by the refresh
    /// policy, never a loss (the rows sit in current-collection-hour buckets the invalidation engine will
    /// materialize, at the same collection-hour placement #1849 gives all history). The lag is the
    /// deliberate side of a trade: admitting those rows into the raw arm by <c>collection_time</c> instead
    /// would serve the same interval TWICE — its partial value inside a materialized bucket PLUS a raw
    /// point placed at an interval start inside the rollup region — and a double count cannot be disclosed
    /// away, while a bounded lag can be and is. With hourly Query Store intervals the lag touches only the
    /// intervals open across the watermark, for one refresh cycle; a database running
    /// <c>INTERVAL_LENGTH_MINUTES = 1440</c> keeps its one open interval lagged until its buckets
    /// materialize, which is the #1849 collection-hour semantics this route already discloses.</para>
    ///
    /// <para><b>The raw arms are bounded to the tail they actually serve — the ±slab is gone here.</b>
    /// The chunk-exclusion floor is <c>GREATEST($2, $4) - 1 hour</c> (a snapshot cannot precede its
    /// interval's start by more than clock skew, and the raw region starts at the later of the window start
    /// and the boundary — GREATEST so a stalled refresh widens the scan only to the stall, never to the
    /// window's full depth). The ceiling is <c>$3 + 1 day</c>: the engine's maximum
    /// INTERVAL_LENGTH_MINUTES is a day of closing-fetch margin, and the raw region sits at the recent
    /// edge by construction (at or above the rollup watermark, which trails now by the refresh policy's
    /// end_offset), so on a now-anchored window the ceiling reaches into hours that do not exist yet and
    /// excludes nothing. The old <c>+30 days</c> of collector-behind generosity is not needed at this
    /// edge: rows a catch-up sweep collects late land at their late COLLECTION time, which is inside this
    /// region's bounds, and history below the boundary is the rollup's to serve.</para>
    ///
    /// <para><b>Legacy rows</b> (pre-tier-2, <c>interval_start_time_utc IS NULL</c>) below $4 are served by
    /// the rollup too — L1 keys them on tier 1's <c>first_execution_time</c> proxy, so they arrive deduped
    /// per collection hour rather than one point per collection. That supersedes the pre-tier-2
    /// treatment for the rollup region only (such rows are older than any raw retention on a store with
    /// rollups; the raw tail keeps the legacy arm byte for byte).</para>
    /// </summary>
    public static string BuildRollupTrendSql(bool withDatabaseFilter)
    {
        var rollupFilter = withDatabaseFilter
            ? "\n    AND   ($5::text[] IS NULL OR database_name = ANY($5))"
            : "";
        var rawFilter = withDatabaseFilter
            ? "\n            AND   ($5::text[] IS NULL OR database_name = ANY($5))"
            : "";
        var legacyFilter = withDatabaseFilter
            ? "\n    AND   ($5::text[] IS NULL OR database_name = ANY($5))"
            : "";

        return $"""
WITH rollup_points AS
(
    /* The materialized region: one point per collection-hour bucket, already deduped at interval grain by
       L1's last() — the sum here is a rollup scan, not a rank. Weighted sums recompose the same
       execution_count * avg_duration_us total the raw arm computes per interval. */
    SELECT
        bucket AS point_time,
        SUM(duration_us_weighted_sum) / 1000.0 AS total_duration_ms,
        SUM(execution_count_sum) AS total_executions
    FROM {TimescaleSupport.QueryStoreStatsCorrectedHourlyView}
    WHERE server_id = $1
    AND   bucket >= $2
    AND   bucket <= $3
    AND   bucket < $4{rollupFilter}
    GROUP BY bucket
),
placed AS
(
    /* Arm 1 (#1841 tier 2), restricted to the unmaterialized tail: dedup each interval to its FINAL
       cumulative snapshot, placed at interval_start_time_utc. Whole intervals stay on one side of the $4
       seam — see the builder remarks. */
    SELECT
        interval_start_time_utc AS point_time,
        execution_count,
        avg_duration_us
    FROM
    (
        SELECT
            interval_start_time_utc,
            execution_count,
            avg_duration_us,
            ROW_NUMBER() OVER
            (
                PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                ORDER BY collection_time DESC, execution_count DESC
            ) AS rn
        FROM query_store_stats
        WHERE server_id = $1
        AND   interval_start_time_utc >= $2
        AND   interval_start_time_utc >= $4
        AND   interval_start_time_utc <= $3
        /* Tail-tight chunk exclusion — the #2736 fix. See the builder remarks for both margins. */
        AND   collection_time >= GREATEST($2, $4) - interval '1 hour'
        AND   collection_time <= $3 + interval '1 day'
        AND   interval_start_time_utc IS NOT NULL{rawFilter}
    ) AS identified
    WHERE rn = 1

    UNION ALL

    /* Arm 2 — pre-tier-2 rows in the tail, kept on their old un-deduped treatment. Practically empty on
       any store with rollups (legacy rows predate raw retention), kept so the arms still partition the
       tail's rows with no overlap and no gap. */
    SELECT
        collection_time AS point_time,
        execution_count,
        avg_duration_us
    FROM query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time >= $4
    AND   collection_time <= $3
    AND   interval_start_time_utc IS NULL{legacyFilter}
),
raw_points AS
(
    SELECT
        point_time,
        SUM(execution_count * avg_duration_us / 1000.0) AS total_duration_ms,
        SUM(execution_count) AS total_executions
    FROM placed
    GROUP BY point_time
),
united AS
(
    /* Rollup points sit strictly below $4 and raw points at or above it, so the union never carries the
       same instant twice. */
    SELECT point_time, total_duration_ms, total_executions FROM rollup_points
    UNION ALL
    SELECT point_time, total_duration_ms, total_executions FROM raw_points
),
rated AS
(
    SELECT
        point_time,
        total_duration_ms,
        total_executions,
        extract(epoch FROM (date_trunc('second', point_time) - date_trunc('second', LAG(point_time) OVER (ORDER BY point_time)))) AS interval_seconds
    FROM united
)
SELECT
    point_time AS collection_time,
    CASE WHEN interval_seconds > 0 THEN total_duration_ms / interval_seconds ELSE 0 END AS duration_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
FROM rated
ORDER BY point_time
""";
    }
}
