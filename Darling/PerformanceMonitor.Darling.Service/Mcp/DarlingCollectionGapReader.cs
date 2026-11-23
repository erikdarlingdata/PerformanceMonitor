/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9339: reads <see cref="CollectionGapFacts"/> for one snapshot collector: the server's skipped-slot rows
/// (<see cref="SkippedCycleReader"/>) and the collector's own runs that did not end SUCCESS, both inside one window. Asked
/// only on an empty answer for a server that is not offline.
/// </summary>
internal static class DarlingCollectionGapReader
{
    /// <summary>
    /// <see cref="EnumeratedCollectorDriver.FreshnessSuccessStatuses"/> (SUCCESS and SKIPPED) as a SQL list, built from the shared
    /// constant as <c>DarlingPgColumnStatsReader</c> does, so a run that ended SKIPPED is healthy here too and is not counted as a
    /// failed run beside the skipped-slot rows. Every element is a compile-time constant, never operator input.
    /// </summary>
    private static readonly string HealthyStatusList = string.Join(
        ", ",
        EnumeratedCollectorDriver.FreshnessSuccessStatuses.Select(status => "'" + status + "'"));

    /// <summary>The collector's runs in the window whose status is not in <see cref="EnumeratedCollectorDriver.FreshnessSuccessStatuses"/>
    /// (a WARNING run partly failed, so it still counts). $1 server_id, $2 collector_name, $3 and $4 the window (naive UTC, start left
    /// out, end kept). Reads the <c>idx_collection_log_watermark</c> prefix.</summary>
    public static readonly string FailedRunsSql = $"""
        SELECT COUNT(*), MIN(collection_time), MAX(collection_time)
        FROM v_collection_log
        WHERE server_id = $1
        AND   collector_name = $2
        AND   status NOT IN ({HealthyStatusList})
        AND   collection_time > $3
        AND   collection_time <= $4
        """;

    public static async Task<CollectionGapFacts> ReadAsync(
        NpgsqlDataSource postgres, int serverId, string collectorName, DateTime windowStartUtc, DateTime windowEndUtc,
        CancellationToken cancellationToken)
    {
        var skipped = await SkippedCycleReader.GetSkippedCycleSummaryAsync(
            postgres, serverId, windowEndUtc, McpCommandDeadlines.ReadSeconds, windowStartUtc, cancellationToken);

        long failed = 0;
        DateTime? first = null;
        DateTime? last = null;
        await using var command = postgres.CreateCommand(FailedRunsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, collectorName);
        DarlingMcpReadParameters.AddTimestamp(command, windowStartUtc);
        DarlingMcpReadParameters.AddTimestamp(command, windowEndUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            failed = Convert.ToInt64(reader.GetValue(0));
            first = reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
            last = reader.IsDBNull(2) ? null : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
        }

        return new CollectionGapFacts(
            skipped.Count, skipped.SlotsSkipped, skipped.FirstAtUtc, skipped.LastAtUtc, failed, first, last);
    }
}
