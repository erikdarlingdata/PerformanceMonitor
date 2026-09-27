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
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The #3893 arm-2 guard and composer for a 7-day fleet collection-health read, moved here from
/// <c>PerformanceMonitor.Darling.Service.Mcp.DarlingFleetReader</c> (#4226) so the service and the WPF viewer
/// read <c>collect.collection_health_hourly</c> through ONE implementation and cannot drift — the same #2530
/// precedent as the <c>DarlingPg*Reader</c> family and <c>DarlingQueryStoreClutterReader</c>: the viewer has no
/// route to the service's <c>Mcp/</c> folder and runs its statements in process.
///
/// <para><c>DarlingFleetReader</c> keeps its own field/property/method NAMES as thin wrappers over this class
/// (same values, same behavior) so its existing pins (<c>FreshStoreWatermarkTests</c>,
/// <c>CollectionHealthAggregateTests</c>, <c>FleetCollectionHealthMemoTests</c>) needed no rewrite — only
/// <c>FreshStoreWatermarkTests</c>' source-text scan for the guard's catch clause now points at THIS file,
/// since the catch itself moved here with the method.</para>
/// </summary>
public static class CollectionHealthRollupSupport
{
    /// <summary>Does <c>collect.collection_health_hourly</c> EXIST here? A relation named in a statement is
    /// resolved at parse time, so the composed SQL may only be CHOSEN after this probe (NULL on a
    /// plain-PostgreSQL store, never an error).</summary>
    public const string RollupProbeSql =
        "SELECT to_regclass('collect." + TimescaleSupport.CollectionHealthHourlyView + "') IS NOT NULL";

    /// <summary>The earliest watermark that can mean something was materialized (#3973): no Darling store holds
    /// a collection from before 2000, so no real refresh can leave the watermark below it. Anything earlier is
    /// the extension's "nothing materialized" sentinel, whichever one it uses.</summary>
    public static readonly DateTime MaterializedWatermarkFloor = new(2000, 1, 1);

    /// <summary>The aggregate's watermark — the instant below which it serves ONLY materialized buckets. NULL
    /// while nothing is materialized, when the whole window is served real-time from raw. Run only after
    /// <see cref="RollupProbeSql"/> said the aggregate exists (which implies TimescaleDB, so the catalog is
    /// there). "Nothing materialized" is not <c>-infinity</c> (#3973) — see
    /// <see cref="MaterializedWatermarkFloor"/>'s remarks; the extension's actual sentinel is the minimum FINITE
    /// timestamp, which <c>isfinite()</c> alone does not catch.</summary>
    public const string WatermarkSql = @"
SELECT CASE WHEN isfinite(w) AND w >= '2000-01-01'::timestamp THEN w END
FROM (SELECT _timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(mat_hypertable_id)) AS w
      FROM _timescaledb_catalog.continuous_agg
      WHERE user_view_schema = 'collect'
      AND   user_view_name = '" + TimescaleSupport.CollectionHealthHourlyView + @"') x";

    /// <summary>THE CONTINUITY GUARD (#3893's watermark hole, #4477's fix): every whole-hour boundary in
    /// [$1 head end, $2 watermark) that the aggregate has NO bucket for. Below the watermark only materialized
    /// buckets exist, so a missing hour there is a HOLE — rows that exist in <c>collection_log</c> and that
    /// the aggregate will never serve on its own. Returns the actual hour boundaries (not just a count, #4477)
    /// so the composer can read exactly those hours raw instead of falling every caller back to a 7-day raw
    /// scan for the whole window over one missing hour — the shape that sent the Viewer's fleet health read to
    /// the raw arm 819 times in three days on a store measured at 6.6 s (raw) against 184 ms (composed).
    /// Belt-and-braces for what the refresh policy's eight-day window cannot rule out: a refresh interrupted
    /// part-way, a hand-run narrow refresh, an altered policy.</summary>
    public const string MissingHoursSql = @"
SELECT gs
FROM generate_series($1::timestamp, $2::timestamp - INTERVAL '1 hour', INTERVAL '1 hour') AS gs
WHERE NOT EXISTS (SELECT 1 FROM collect." + TimescaleSupport.CollectionHealthHourlyView + @" h WHERE h.bucket = gs)
ORDER BY gs";

    /// <summary>The most hole hours the composed read will still serve individually (#4477). Above this, the
    /// per-hole raw slice this composer would add (two extra parameters and one extra UNION ALL branch per
    /// hole) grows large enough that reading the whole window raw is simpler and no slower in the case that
    /// matters — a fleet this broken is already answering from <c>collection_log</c>'s hot, uncompressed tail,
    /// not the 7-day cold scan a single missing hour used to force. Named so the guard's own doc comment and
    /// the tests that pin the too-many-holes fallback have one place to read it from.</summary>
    public const int MaxRepairableHoleHours = 48;

    /// <summary>The continuity guard's verdict: whether the composed read may be used at all, and — when it
    /// may — exactly which whole hours below the watermark have no bucket and must be read raw alongside the
    /// rollup instead of forcing the WHOLE window to raw (#4477). <see cref="Usable"/> converts implicitly to
    /// <c>bool</c> so the existing <c>Assert.True</c>/<c>Assert.False</c> pins and <c>if (composed)</c> call
    /// sites need no rewrite.</summary>
    public readonly record struct RollupPlan(bool Usable, IReadOnlyList<DateTime> HoleHours)
    {
        public static implicit operator bool(RollupPlan plan) => plan.Usable;

        /// <summary>The all-clear verdict every non-holed window reaches: usable, no holes to read raw.</summary>
        public static readonly RollupPlan Clean = new(true, Array.Empty<DateTime>());

        /// <summary>The verdict when the composed read must not be used at all — absent, too many holes, or a
        /// failed/unconvertible guard read. The caller falls back to the exact raw scan of the whole window.</summary>
        public static readonly RollupPlan Unusable = new(false, Array.Empty<DateTime>());
    }

    /// <summary>The first hour boundary at or after <paramref name="windowStart"/> — where whole buckets begin.</summary>
    public static DateTime CeilingHour(DateTime windowStart)
    {
        var floor = new DateTime(windowStart.Year, windowStart.Month, windowStart.Day, windowStart.Hour, 0, 0, DateTimeKind.Unspecified);
        return floor == windowStart ? floor : floor.AddHours(1);
    }

    /// <summary><paramref name="rawSql"/> (a <c>collection_time &gt;= $1</c>-bounded statement) with ONE bound
    /// inserted so it reads only the partial head hour [$1, $2) — derived by <c>Replace</c>, not restated, so
    /// the head slice can never drift from the raw read's own aggregates. Throws if the anchor is not found
    /// exactly once, so a raw statement reshaped without this composer noticing fails loudly instead of quietly
    /// reading the whole window as the "head".</summary>
    public static string InsertHeadBound(string rawSql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rawSql);

        var headSlice = rawSql.Replace(
            "WHERE collection_time >= $1",
            "WHERE collection_time >= $1\nAND   collection_time < $2",
            StringComparison.Ordinal);

        if (string.Equals(headSlice, rawSql, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "CollectionHealthRollupSupport.InsertHeadBound found no 'WHERE collection_time >= $1' to bound — the raw SQL has drifted from the shape this composer requires (#4226).",
                nameof(rawSql));
        }

        return headSlice;
    }

    /// <summary>
    /// Composes <paramref name="rawSql"/> with <c>collect.collection_health_hourly</c>: every WHOLE hour bucket
    /// from $2 (the ceiling hour) UNION ALL the raw head slice [$1, $2) (<see cref="InsertHeadBound"/>),
    /// re-aggregated per (server, collector) — COUNT by SUM, SUM by SUM, MAX by MAX, all lossless over hours.
    /// <paramref name="rawSql"/> must select exactly the thirteen columns <c>server_id, collector_name,
    /// total_runs, success_count, error_count, last_success_time, permission_denied_count, last_run_time,
    /// abandoned_count, extension_missing_count, last_non_skip_time, last_productive_time,
    /// last_zero_row_streak_break_time</c>, in that order, GROUPed BY <c>server_id, collector_name</c> — the
    /// shared shape both the service's and the viewer's raw statements produce. Same names, same types (the
    /// SUMs cast back to bigint), so a positional reader cannot tell which statement it ran. The aggregate is
    /// <c>materialized_only = false</c>: buckets above its watermark are computed real-time from raw, so the
    /// result is current to the second.
    /// </summary>
    public static string ComposeFleetSql(string rawSql) => ComposeFleetSql(rawSql, Array.Empty<DateTime>());

    /// <summary><paramref name="rawSql"/> (a <c>collection_time &gt;= $1</c>-bounded statement) with the SAME
    /// bound replaced with <c>holes.h</c> / <c>holes.h + INTERVAL '1 hour'</c> (#4477) — derived by
    /// <c>Replace</c>, not restated, for the same reason <see cref="InsertHeadBound"/> is: a raw statement
    /// reshaped without this composer noticing fails loudly instead of quietly reading the whole window.</summary>
    public static string InsertHoleBound(string rawSql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rawSql);

        var holeSlice = rawSql.Replace(
            "WHERE collection_time >= $1",
            "WHERE collection_time >= holes.h\nAND   collection_time < holes.h + INTERVAL '1 hour'",
            StringComparison.Ordinal);

        if (string.Equals(holeSlice, rawSql, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "CollectionHealthRollupSupport.InsertHoleBound found no 'WHERE collection_time >= $1' to bound — the raw SQL has drifted from the shape this composer requires (#4477).",
                nameof(rawSql));
        }

        return holeSlice;
    }

    /// <summary>
    /// Composes <paramref name="rawSql"/> with <c>collect.collection_health_hourly</c>: every WHOLE hour bucket
    /// from $2 (the ceiling hour) UNION ALL the raw head slice [$1, $2) (<see cref="InsertHeadBound"/>) UNION
    /// ALL, for every hour in <paramref name="holeHours"/> (#4477), the raw rows for that one hour
    /// (<see cref="InsertHoleBound"/>, driven off a <c>$3</c> array parameter via <c>unnest</c> so the SQL text
    /// stays the same shape whether there is one hole or <see cref="MaxRepairableHoleHours"/> of them) —
    /// re-aggregated per (server, collector), same as every other part. A hole hour with no raw rows at all (a
    /// real collection outage) contributes nothing from its branch, exactly what the raw arm would also have
    /// returned for that hour. <paramref name="rawSql"/> must select exactly the thirteen columns described on
    /// the overload above, in that order.
    /// </summary>
    public static string ComposeFleetSql(string rawSql, IReadOnlyList<DateTime> holeHours)
    {
        var headSlice = InsertHeadBound(rawSql);

        var holesSql = string.Empty;
        if (holeHours.Count > 0)
        {
            var holeSlice = InsertHoleBound(rawSql);
            holesSql = @"
    UNION ALL
    SELECT server_id, collector_name, total_runs, success_count, error_count, last_success_time,
           permission_denied_count, last_run_time, abandoned_count, extension_missing_count,
           last_non_skip_time, last_productive_time, last_zero_row_streak_break_time
    FROM unnest($3::timestamp[]) AS holes(h)
    CROSS JOIN LATERAL
    (
" + holeSlice + @"
    ) AS hole_rows";
        }

        return @"
WITH parts AS
(
    SELECT server_id, collector_name, total_runs, success_count, error_count, last_success_time,
           permission_denied_count, last_run_time, abandoned_count, extension_missing_count,
           last_non_skip_time, last_productive_time, last_zero_row_streak_break_time
    FROM collect." + TimescaleSupport.CollectionHealthHourlyView + @"
    WHERE bucket >= $2
    UNION ALL
" + headSlice + holesSql + @"
)
SELECT
    server_id,
    collector_name,
    CAST(SUM(total_runs) AS bigint) AS total_runs,
    CAST(SUM(success_count) AS bigint) AS success_count,
    CAST(SUM(error_count) AS bigint) AS error_count,
    MAX(last_success_time) AS last_success_time,
    CAST(SUM(permission_denied_count) AS bigint) AS permission_denied_count,
    MAX(last_run_time) AS last_run_time,
    CAST(SUM(abandoned_count) AS bigint) AS abandoned_count,
    CAST(SUM(extension_missing_count) AS bigint) AS extension_missing_count,
    MAX(last_non_skip_time) AS last_non_skip_time,
    MAX(last_productive_time) AS last_productive_time,
    MAX(last_zero_row_streak_break_time) AS last_zero_row_streak_break_time
FROM parts
GROUP BY server_id, collector_name";
    }

    /// <summary>
    /// Chooses the statement for the seven-day collection-health read: the composed one only when both guards
    /// pass, else the exact raw scan. (a) ABSENT: no aggregate (plain PostgreSQL, or not yet created) → raw.
    /// (b) CONTINUITY: the missing hours below the watermark (#4477: read raw alongside the rollup instead of
    /// forcing the WHOLE window to raw), unless there are more than <see cref="MaxRepairableHoleHours"/>, in
    /// which case → raw, unchanged from before #4477. A guard read that fails is treated as a failed guard,
    /// availability-first: the raw scan is always exact, only slower. That includes a value the client cannot
    /// convert (#3973): a guard read that produced one used to fail the whole overview instead.
    /// </summary>
    public static async Task<RollupPlan> RollupPlanAsync(
        NpgsqlDataSource postgres, DateTime headEnd, CancellationToken cancellationToken)
    {
        try
        {
            await using (var probe = postgres.CreateCommand(RollupProbeSql))
            {
                probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                if (await probe.ExecuteScalarAsync(cancellationToken) is not true)
                {
                    return RollupPlan.Unusable;
                }
            }

            DateTime? watermark;
            await using (var read = postgres.CreateCommand(WatermarkSql))
            {
                read.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                watermark = await read.ExecuteScalarAsync(cancellationToken) is DateTime w ? w : null;
            }

            if (watermark is not { } mark || mark <= headEnd)
            {
                return RollupPlan.Clean; // nothing whole below the watermark inside the window: all real-time, nothing to hole
            }

            var holes = new List<DateTime>();
            await using (var missing = postgres.CreateCommand(MissingHoursSql))
            {
                missing.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                AddTimestamp(missing, headEnd);
                AddTimestamp(missing, mark);
                await using var reader = await missing.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    holes.Add(DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Unspecified));
                }
            }

            return holes.Count switch
            {
                0 => RollupPlan.Clean,
                > MaxRepairableHoleHours => RollupPlan.Unusable, // too many holes to patch cheaply: raw is simpler and no slower
                _ => new RollupPlan(true, holes),
            };
        }
        catch (Exception ex) when (ex is PostgresException or InvalidCastException)
        {
            return RollupPlan.Unusable;
        }
    }

    /// <summary>Back-compat shape for callers that only need the usable/not verdict, not the hole hours
    /// (#4477's <see cref="RollupPlan"/> implicitly converts to <c>bool</c> too, but a caller that awaits this
    /// directly needs an explicit <c>Task&lt;bool&gt;</c>).</summary>
    public static async Task<bool> RollupUsableAsync(
        NpgsqlDataSource postgres, DateTime headEnd, CancellationToken cancellationToken) =>
        await RollupPlanAsync(postgres, headEnd, cancellationToken);

    private static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });
}
