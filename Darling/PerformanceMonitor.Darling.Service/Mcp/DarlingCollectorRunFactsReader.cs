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

    /// <summary>
    /// The successful trace_flags runs that stored ZERO rows (#5633 item 19), the empty snapshots the trace-flag change diff
    /// needs: a run that finds no flag enabled stores nothing, so without these the last flag being turned off never shows.
    /// Bounded like the change read: the runs inside the window $2..$3 (naive UTC), plus the one newest run before $2, which is
    /// the baseline for a change on the window's first capture. SUCCESS only (a failed run did not look) and
    /// <c>rows_collected = 0</c> only (NULL is "not recorded", not "found nothing"). $1 server_id.
    /// </summary>
    public const string EmptyTraceFlagRunsSql = """
        (SELECT collection_time
         FROM v_collection_log
         WHERE server_id = $1
         AND   collector_name = 'trace_flags'
         AND   status = 'SUCCESS'
         AND   rows_collected = 0
         AND   collection_time >= $2
         AND   collection_time <= $3)
        UNION ALL
        (SELECT collection_time
         FROM v_collection_log
         WHERE server_id = $1
         AND   collector_name = 'trace_flags'
         AND   status = 'SUCCESS'
         AND   rows_collected = 0
         AND   collection_time < $2
         ORDER BY collection_time DESC
         LIMIT 1)
        """;

    /// <summary>The times of the trace_flags runs that found no flag (<see cref="EmptyTraceFlagRunsSql"/>), for
    /// <c>ConfigChangeDiff.DiffTraceFlagChanges</c>.</summary>
    public static async Task<List<DateTime>> ReadEmptyTraceFlagRunsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime windowStartUtc, DateTime windowEndUtc, CancellationToken cancellationToken = default)
    {
        var times = new List<DateTime>();
        await using var command = postgres.CreateCommand(EmptyTraceFlagRunsSql);
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
    /// null for a read with no window question (the latest-snapshot trace-flag reads).</summary>
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

        await using (var command = postgres.CreateCommand(LatestSuccessSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, collectorName);
            DarlingMcpReadParameters.AddInt(command, CollectorRunFacts.RunsCap);
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
