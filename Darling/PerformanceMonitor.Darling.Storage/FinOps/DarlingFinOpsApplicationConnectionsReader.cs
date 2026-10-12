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

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>
/// One application's connection counts and collected resource figures over the window. The times are naive UTC, as
/// stored; the caller converts them for display.
/// </summary>
public sealed record ApplicationConnectionUsage(
    string ApplicationName, int AvgConnections, int MaxConnections, int AvgRunning, int MaxRunning, int AvgSleeping,
    int MaxSleeping, int AvgDormant, int MaxDormant, long AvgCpuTimeMs, long MaxCpuTimeMs, long AvgReads, long MaxReads,
    long AvgWrites, long MaxWrites, long AvgLogicalReads, long MaxLogicalReads, long SampleCount,
    DateTime FirstSeenUtc, DateTime LastSeenUtc);

/// <summary>
/// The FinOps Application Connections read, shared by the viewer and the service so there is one copy of the SQL.
/// It returns naive-UTC records; the viewer keeps the clock and the display conversion.
/// </summary>
public static class DarlingFinOpsApplicationConnectionsReader
{
    /// <summary>
    /// Per-application connection counts plus the collected per-app resource + session-status metrics from
    /// session_stats (last 24h). AVG/MAX over the window for connection/running/sleeping/dormant counts and
    /// CPU/reads/writes/logical-reads (the resource columns are nullable, so AVG/MAX yield NULL until populated).
    /// $1 server_id, $2 cutoff.
    /// </summary>
    public const string ApplicationConnectionsSql = @"
SELECT
    program_name,
    CAST(AVG(connection_count) AS INTEGER) AS avg_connections,
    MAX(connection_count) AS max_connections,
    CAST(AVG(running_count) AS INTEGER) AS avg_running,
    MAX(running_count) AS max_running,
    CAST(AVG(sleeping_count) AS INTEGER) AS avg_sleeping,
    MAX(sleeping_count) AS max_sleeping,
    CAST(AVG(dormant_count) AS INTEGER) AS avg_dormant,
    MAX(dormant_count) AS max_dormant,
    CAST(AVG(total_cpu_time_ms) AS BIGINT) AS avg_cpu_time_ms,
    MAX(total_cpu_time_ms) AS max_cpu_time_ms,
    CAST(AVG(total_reads) AS BIGINT) AS avg_reads,
    MAX(total_reads) AS max_reads,
    CAST(AVG(total_writes) AS BIGINT) AS avg_writes,
    MAX(total_writes) AS max_writes,
    CAST(AVG(total_logical_reads) AS BIGINT) AS avg_logical_reads,
    MAX(total_logical_reads) AS max_logical_reads,
    COUNT(*) AS sample_count,
    MIN(collection_time) AS first_seen,
    MAX(collection_time) AS last_seen
FROM v_session_stats
WHERE server_id = $1
AND   collection_time >= $2
GROUP BY program_name
ORDER BY max_connections DESC";

    /// <summary>Reads the per-application connection figures since <paramref name="cutoffUtc"/> (naive UTC).</summary>
    public static async Task<List<ApplicationConnectionUsage>> GetApplicationConnectionsAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime cutoffUtc, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(ApplicationConnectionsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });

        var items = new List<ApplicationConnectionUsage>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ApplicationConnectionUsage(
                ApplicationName: reader.IsDBNull(0) ? "" : reader.GetString(0),
                AvgConnections: reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                MaxConnections: reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                AvgRunning: reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                MaxRunning: reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                AvgSleeping: reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                MaxSleeping: reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                AvgDormant: reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
                MaxDormant: reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8)),
                AvgCpuTimeMs: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                MaxCpuTimeMs: reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                AvgReads: reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                MaxReads: reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
                AvgWrites: reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)),
                MaxWrites: reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)),
                AvgLogicalReads: reader.IsDBNull(15) ? 0L : Convert.ToInt64(reader.GetValue(15)),
                MaxLogicalReads: reader.IsDBNull(16) ? 0L : Convert.ToInt64(reader.GetValue(16)),
                SampleCount: reader.IsDBNull(17) ? 0 : Convert.ToInt64(reader.GetValue(17)),
                FirstSeenUtc: reader.GetDateTime(18),
                LastSeenUtc: reader.GetDateTime(19)));
        }
        return items;
    }
}
