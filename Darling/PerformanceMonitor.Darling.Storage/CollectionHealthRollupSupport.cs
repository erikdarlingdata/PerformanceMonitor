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

    /// <summary>THE CONTINUITY GUARD (#3893's watermark hole): how many distinct whole-hour buckets the
    /// aggregate holds in [$1 head end, $2 watermark). Below the watermark only materialized buckets exist, so
    /// a missing hour there is a HOLE — rows that exist in <c>collection_log</c> and that the aggregate will
    /// never serve. Belt-and-braces for what the refresh policy's eight-day window cannot rule out: a refresh
    /// interrupted part-way, a hand-run narrow refresh, an altered policy.</summary>
    public const string ContinuitySql =
        "SELECT count(DISTINCT bucket) FROM collect." + TimescaleSupport.CollectionHealthHourlyView + " WHERE bucket >= $1 AND bucket < $2";

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
    public static string ComposeFleetSql(string rawSql)
    {
        var headSlice = InsertHeadBound(rawSql);

        return @"
WITH parts AS
(
    SELECT server_id, collector_name, total_runs, success_count, error_count, last_success_time,
           permission_denied_count, last_run_time, abandoned_count, extension_missing_count,
           last_non_skip_time, last_productive_time, last_zero_row_streak_break_time
    FROM collect." + TimescaleSupport.CollectionHealthHourlyView + @"
    WHERE bucket >= $2
    UNION ALL
" + headSlice + @"
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
    /// (b) CONTINUITY: any whole hour in [head end, watermark) missing as a bucket → raw
    /// (<see cref="ContinuitySql"/> says why). A guard read that fails is treated as a failed guard,
    /// availability-first: the raw scan is always exact, only slower. That includes a value the client cannot
    /// convert (#3973): a guard read that produced one used to fail the whole overview instead.
    /// </summary>
    public static async Task<bool> RollupUsableAsync(
        NpgsqlDataSource postgres, DateTime headEnd, CancellationToken cancellationToken)
    {
        try
        {
            await using (var probe = postgres.CreateCommand(RollupProbeSql))
            {
                probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                if (await probe.ExecuteScalarAsync(cancellationToken) is not true)
                {
                    return false;
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
                return true; // nothing whole below the watermark inside the window: all real-time, nothing to hole
            }

            var expected = (long)Math.Floor((mark - headEnd).TotalHours);
            await using var count = postgres.CreateCommand(ContinuitySql);
            count.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            AddTimestamp(count, headEnd);
            AddTimestamp(count, headEnd.AddHours(expected));
            var present = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken));
            return present >= expected;
        }
        catch (Exception ex) when (ex is PostgresException or InvalidCastException)
        {
            return false;
        }
    }

    private static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });
}
