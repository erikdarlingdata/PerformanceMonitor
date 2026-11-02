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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7242. The exact count of runs after a collector's newest streak break, for the fleet roll-ups that cannot rank the window
/// (the service's fleet card and the Viewer's Overview). Every run after the newest run that was not a zero-row SUCCESS is
/// itself a zero-row SUCCESS, so the count is the trailing zero-row run count the per-server read takes off its ranked subquery.
///
/// <para>Why it exists: the roll-ups estimate that count as elapsed time over the cadence
/// (<c>CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns</c>), which hands the wall clock back. The activity-sourced
/// quiet bar is measured in time the service observed, the run count times the interval, so a service that was stopped for a
/// weekend must not have those hours counted as quiet. Asked only for the activity-sourced rows whose last productive run is older
/// than the bar, one keyed range count per pair, so a healthy fleet runs nothing.</para>
/// </summary>
public static class CollectorObservedQuietRuns
{
    /// <summary>
    /// One row per requested (server, collector) pair with its run count. $1 server ids, $2 collector names and $3 the streak break
    /// times (parallel arrays), $4 the window start (naive UTC, the roll-up's own). One LATERAL per pair counts the runs strictly
    /// after the break through the (server, collector, collection_time) index.
    /// </summary>
    public const string FleetRunsSql = @"
SELECT p.server_id,
       p.collector_name,
       r.runs
FROM unnest($1::integer[], $2::text[], $3::timestamp[]) AS p(server_id, collector_name, break_time)
CROSS JOIN LATERAL
(
    SELECT COUNT(*) AS runs
    FROM v_collection_log v
    WHERE v.server_id = p.server_id
    AND   v.collector_name = p.collector_name
    AND   v.collection_time >= $4
    AND   v.collection_time > p.break_time
) r";

    /// <summary>
    /// Reads the run count for each request. Returns an empty map without touching the store when <paramref name="requests"/> is empty.
    /// </summary>
    public static async Task<Dictionary<(int ServerId, string CollectorName), long>> ReadAsync(
        NpgsqlDataSource dataSource,
        IReadOnlyList<(int ServerId, string CollectorName, DateTime BreakTime)> requests,
        DateTime windowStart,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        var counts = new Dictionary<(int ServerId, string CollectorName), long>();
        if (requests.Count == 0)
        {
            return counts;
        }

        var serverIds = new int[requests.Count];
        var collectors = new string[requests.Count];
        var breaks = new DateTime[requests.Count];
        for (var i = 0; i < requests.Count; i++)
        {
            serverIds[i] = requests[i].ServerId;
            collectors[i] = requests[i].CollectorName;
            breaks[i] = DateTime.SpecifyKind(requests[i].BreakTime, DateTimeKind.Unspecified);
        }

        await using var command = dataSource.CreateCommand(FleetRunsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = serverIds });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = collectors });
        command.Parameters.Add(new NpgsqlParameter<DateTime[]> { TypedValue = breaks });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified)
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            counts[(reader.GetInt32(0), reader.GetString(1))] =
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2));
        }

        return counts;
    }
}
