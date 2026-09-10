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

using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One point on a selected wait type's trend line, with both picker metrics computed IN SQL: the
/// per-second wait rate (this interval's <c>delta_wait_time_ms</c> divided by the seconds since the
/// previous collection via a per-type <c>LAG</c> window) and the average ms per wait (delta wait
/// time divided by delta waiting tasks). Signal-wait per second rides along for parity with Lite's
/// row shape though the picker's metric combo exposes only the other two. Copied from Lite's
/// <c>WaitStatsTrendPoint</c> (LocalDataService.WaitStats.cs).
/// </summary>
public sealed record WaitStatsTrendPoint(
    DateTime CollectionTime,
    double WaitTimeMsPerSecond,
    double SignalWaitTimeMsPerSecond,
    double AvgMsPerWait);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The wait-type picker's population read — Lite's <c>GetDistinctWaitTypesAsync</c> ported to
    /// Postgres verbatim (runs against the <c>v_wait_stats</c> passthrough view, the Darling-analysis
    /// convention): every wait_type collected for the server over the window, ranked by total delta
    /// wait time descending so the picker sees the heaviest types first (the checked-to-top order and
    /// the "Top Waits" fill both lean on it). Lite's per-user IgnoredWaitTypes exclusion clause is
    /// deliberately DROPPED — the viewer has no per-user ignore config to share.
    /// $1 server_id, $2 window start, $3 window end (all naive UTC).
    /// </summary>
    public const string DistinctWaitTypesSql = """
        SELECT
            wait_type,
            SUM(delta_wait_time_ms) AS total_delta
        FROM v_wait_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY wait_type
        ORDER BY SUM(delta_wait_time_ms) DESC
        """;

    /// <summary>
    /// Lite's batched <c>GetWaitStatsTrendsByTypesAsync</c> body, ported to Postgres verbatim: the
    /// per-second trend for ALL selected wait types in ONE query, grouped by type. The <c>LAG</c>
    /// window is partitioned by wait_type so each type's per-second rate is computed independently,
    /// and the interval seconds come from the truncate-then-diff epoch idiom already proven
    /// value-identical between DuckDB and Postgres. The <c>wait_type IN (...)</c> list is built
    /// dynamically from <c>$4</c> onward (server_id/start/end take <c>$1</c>/<c>$2</c>/<c>$3</c>),
    /// exactly like Lite; a caller passing <paramref name="waitTypeCount"/> = 0 is a bug the
    /// <see cref="GetWaitStatsTrendsByTypesAsync"/> guard prevents.
    /// </summary>
    public static string WaitTrendsSql(int waitTypeCount)
    {
        var typeParams = string.Join(", ", Enumerable.Range(0, waitTypeCount).Select(i => "$" + (i + 4)));
        return $$"""
            WITH raw AS
            (
                SELECT
                    wait_type,
                    collection_time,
                    delta_wait_time_ms,
                    delta_signal_wait_time_ms,
                    delta_waiting_tasks,
                    extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time)))) AS interval_seconds
                FROM v_wait_stats
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
                AND   wait_type IN ({{typeParams}})
            )
            SELECT
                wait_type,
                collection_time,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS signal_wait_time_ms_per_second,
                CASE WHEN delta_waiting_tasks > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / delta_waiting_tasks ELSE 0 END AS avg_ms_per_wait
            FROM raw
            ORDER BY wait_type, collection_time
            """;
    }

    /// <summary>
    /// The distinct wait types collected for one server in the window, ranked by total delta wait
    /// time descending — feeds the picker's population + default-selection.
    /// </summary>
    public async Task<List<string>> GetDistinctWaitTypesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<string>();

        await using var command = _dataSource.CreateCommand(DistinctWaitTypesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetString(0));
        }

        return items;
    }

    /// <summary>
    /// The per-second (and avg-per-wait) trend for every selected wait type in one query, grouped by
    /// type. Empty selection returns an empty map without touching the store.
    /// </summary>
    public async Task<Dictionary<string, List<WaitStatsTrendPoint>>> GetWaitStatsTrendsByTypesAsync(
        int serverId, List<string> waitTypes, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, List<WaitStatsTrendPoint>>();
        if (waitTypes.Count == 0)
        {
            return result;
        }

        await using var command = _dataSource.CreateCommand(WaitTrendsSql(waitTypes.Count));
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        foreach (var waitType in waitTypes)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = waitType });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var waitType = reader.GetString(0);
            if (!result.TryGetValue(waitType, out var list))
            {
                list = new List<WaitStatsTrendPoint>();
                result[waitType] = list;
            }

            list.Add(new WaitStatsTrendPoint(
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
        }

        return result;
    }
}
