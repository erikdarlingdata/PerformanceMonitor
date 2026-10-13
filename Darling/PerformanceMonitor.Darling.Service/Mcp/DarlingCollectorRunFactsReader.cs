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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Reads <see cref="CollectorRunFacts"/> for one collector from <c>v_collection_log</c> (#5633 items 3 and 19): the
/// "did the collector run, and how often" evidence an empty read needs before it may say WHY it is empty. The words
/// live in <c>PerformanceMonitor.Common.CollectorRunFacts</c>, shared with Lite; this class only fetches the numbers.
///
/// <para>Asked only on an empty path, so it adds no cost to a read that has data. A read that fails is NOT turned into
/// a quiet "no runs": the exception goes to the tool's own catch, which answers <c>error</c>. The old text it replaces
/// ("fewer than two cycles") was a false claim on a healthy server, and an <c>error</c> is at least a true one.</para>
/// </summary>
internal static class DarlingCollectorRunFactsReader
{
    /// <summary>The newest <see cref="CollectorRunFacts.RunsCap"/> successful runs of one collector, newest first. Reads the
    /// <c>idx_collection_log_watermark</c> prefix and stops at the cap. $1 server_id, $2 collector_name, $3 the cap (bound, never a literal).</summary>
    public const string LatestSuccessSql = """
        SELECT collection_time, rows_collected
        FROM v_collection_log
        WHERE server_id = $1
        AND   collector_name = $2
        AND   status = 'SUCCESS'
        ORDER BY collection_time DESC
        LIMIT $3
        """;

    /// <summary><see cref="LatestSuccessSql"/> for a historical read (#5633 review): only runs AT OR BEFORE $4 (naive UTC), the read's
    /// anchor, so an empty page answers with the collector's state as of the anchor, not as of now. Twin of Lite's
    /// <c>CollectorLatestSuccessAtOrBeforeSql</c>.</summary>
    public const string LatestSuccessAtOrBeforeSql = """
        SELECT collection_time, rows_collected
        FROM v_collection_log
        WHERE server_id = $1
        AND   collector_name = $2
        AND   status = 'SUCCESS'
        AND   collection_time <= $4
        ORDER BY collection_time DESC
        LIMIT $3
        """;

    /// <summary>Successful runs of one collector inside a window. $1 server_id, $2 collector_name, $3/$4 window (naive UTC).</summary>
    public const string RunsInWindowSql = """
        SELECT COUNT(*)
        FROM v_collection_log
        WHERE server_id = $1
        AND   collector_name = $2
        AND   status = 'SUCCESS'
        AND   collection_time >= $3
        AND   collection_time <= $4
        """;

    /// <summary>The times of the trace_flags runs that found no flag (<see cref="DarlingRetentionHorizons.EmptyTraceFlagRunsSql"/>), for
    /// <c>ConfigChangeDiff.DiffTraceFlagChanges</c>.</summary>
    public static async Task<List<DateTime>> ReadEmptyTraceFlagRunsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime windowStartUtc, DateTime windowEndUtc, CancellationToken cancellationToken = default)
    {
        var times = new List<DateTime>();
        await using var command = postgres.CreateCommand(DarlingRetentionHorizons.EmptyTraceFlagRunsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, windowStartUtc);
        DarlingMcpReadParameters.AddTimestamp(command, windowEndUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            times.Add(reader.GetDateTime(0));
        }

        return times;
    }

    /// <summary>The run facts for <paramref name="collectorName"/>. Pass a window to also count the runs inside it; pass
    /// null for a read with no window question (the latest-snapshot trace-flag reads). A non-null <paramref name="windowEndUtc"/>
    /// (the read's anchor) also bounds the newest-run read, with or without a start (<see cref="LatestSuccessAtOrBeforeSql"/>).</summary>
    public static async Task<CollectorRunFacts> ReadAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string collectorName,
        DateTime? windowStartUtc,
        DateTime? windowEndUtc,
        CancellationToken cancellationToken = default)
    {
        long runs = 0;
        DateTime? last = null;
        long? lastRows = null;

        await using (var command = postgres.CreateCommand(windowEndUtc is null ? LatestSuccessSql : LatestSuccessAtOrBeforeSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, collectorName);
            DarlingMcpReadParameters.AddInt(command, CollectorRunFacts.RunsCap);
            if (windowEndUtc is { } ceiling)
            {
                DarlingMcpReadParameters.AddTimestamp(command, ceiling);
            }

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                runs++;
                if (runs == 1)
                {
                    last = reader.IsDBNull(0) ? null : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
                    lastRows = reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1));
                }
            }
        }

        long? inWindow = null;
        if (windowStartUtc is { } start && windowEndUtc is { } end)
        {
            await using var command = postgres.CreateCommand(RunsInWindowSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, collectorName);
            DarlingMcpReadParameters.AddTimestamp(command, start);
            DarlingMcpReadParameters.AddTimestamp(command, end);
            inWindow = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken) ?? 0L);
        }

        return new CollectorRunFacts(runs, inWindow, last, lastRows);
    }
}
