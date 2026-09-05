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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One point on a selected perfmon counter's trend line: the raw counter value and the per-interval
/// delta, both summed across the counter's instances at each collection_time, plus the wall-clock
/// seconds that delta covers — MAX, not SUM, because the interval is one measured sweep gap repeated
/// per instance rather than a per-instance quantity (#2234; Transactions/sec carries a median of 12
/// instance rows). The picker does not derive a rate today, so the field rides along unplotted so that
/// whoever adds one has a real denominator instead of a fabricated cadence. Copied from Lite's
/// <c>PerfmonTrendPoint</c> (LocalDataService.Perfmon.cs). The picker plots <see cref="DeltaValue"/>;
/// <see cref="Value"/> rides along for parity with Lite's row shape.
/// </summary>
public sealed record PerfmonTrendPoint(
    DateTime CollectionTime,
    long Value,
    long DeltaValue,
    long SampleIntervalSeconds);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The perfmon picker's population read — Lite's <c>GetDistinctPerfmonCountersAsync</c> ported to
    /// Postgres verbatim (runs against the <c>v_perfmon_stats</c> passthrough view, the Darling-analysis
    /// convention): every counter_name collected for the server over the window, ordered by name so the
    /// picker + pack fills see a stable list.
    /// $1 server_id, $2 window start, $3 window end (all naive UTC).
    /// </summary>
    public const string DistinctPerfmonCountersSql = """
        SELECT DISTINCT counter_name
        FROM v_perfmon_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY counter_name
        """;

    /// <summary>
    /// Lite's batched <c>GetPerfmonTrendsByCountersAsync</c> body, ported to Postgres: the trend for
    /// ALL selected counters in ONE query, grouped by counter name and collection_time. The
    /// <c>counter_name IN (...)</c> list is built dynamically from <c>$4</c> onward
    /// (server_id/start/end take <c>$1</c>/<c>$2</c>/<c>$3</c>), exactly like Lite's numbering.
    /// Deviation from Lite: the two <c>SUM</c> aggregates are wrapped in <c>CAST(... AS bigint)</c>
    /// because Postgres' <c>SUM(bigint)</c> returns <c>numeric</c> (DuckDB kept it integral), and the
    /// typed <c>GetInt64</c> reader would throw against a numeric — the same typed-reader reason the
    /// tempdb/CPU trends CAST their aggregates. A caller passing <paramref name="counterCount"/> = 0 is
    /// a bug the <see cref="GetPerfmonTrendsByCountersAsync"/> guard prevents.
    /// </summary>
    public static string PerfmonTrendsSql(int counterCount)
    {
        var nameParams = string.Join(", ", Enumerable.Range(0, counterCount).Select(i => "$" + (i + 4)));
        return $"""
            SELECT
                counter_name,
                collection_time,
                CAST(SUM(cntr_value) AS bigint) AS cntr_value,
                CAST(SUM(delta_cntr_value) AS bigint) AS delta_cntr_value,
                CAST(MAX(sample_interval_seconds) AS bigint) AS sample_interval_seconds
            FROM v_perfmon_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   counter_name IN ({nameParams})
            GROUP BY counter_name, collection_time
            ORDER BY counter_name, collection_time
            """;
    }

    /// <summary>
    /// The distinct counter names collected for one server in the window, ordered by name — feeds the
    /// picker's population + pack fills.
    /// </summary>
    public async Task<List<string>> GetDistinctPerfmonCountersAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<string>();

        await using var command = _dataSource.CreateCommand(DistinctPerfmonCountersSql);
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
    /// The value + delta trend for every selected counter in one query, grouped by counter name.
    /// Empty selection returns an empty map without touching the store.
    /// </summary>
    public async Task<Dictionary<string, List<PerfmonTrendPoint>>> GetPerfmonTrendsByCountersAsync(
        int serverId, List<string> counterNames, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, List<PerfmonTrendPoint>>();
        if (counterNames.Count == 0)
        {
            return result;
        }

        await using var command = _dataSource.CreateCommand(PerfmonTrendsSql(counterNames.Count));
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
        foreach (var counterName in counterNames)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = counterName });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var counterName = reader.GetString(0);
            if (!result.TryGetValue(counterName, out var list))
            {
                list = new List<PerfmonTrendPoint>();
                result[counterName] = list;
            }

            list.Add(new PerfmonTrendPoint(
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4)));
        }

        return result;
    }
}
