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

/// <summary>One (program_name) group at the latest session_stats snapshot.</summary>
public sealed record SessionStatRow(
    DateTime CollectionTime, string ProgramName, long ConnectionCount, int RunningCount, int SleepingCount,
    int DormantCount, long? TotalCpuTimeMs, long? TotalReads, long? TotalWrites, long? TotalLogicalReads);

/// <summary>
/// The per-application session snapshot read the web's <c>get_session_stats</c> endpoint (the MCP tool) and the Viewer's
/// Sessions by Application grid share (#6388), moved here from the Service project's <c>DarlingSessionReader</c>
/// unchanged except that the command deadline is passed in.
/// </summary>
public static class DarlingSessionStatsReader
{
    /// <summary>
    /// The latest per-application session_stats snapshot — Lite's <c>GetLatestSessionStatsAsync</c>: every
    /// program_name at the newest collection, most connections first. connection_count is bigint; the
    /// running/sleeping/dormant counts are integer; the cumulative totals are nullable bigint. $1 server_id.
    /// </summary>
    public const string LatestSql = """
        SELECT
            collection_time,
            program_name,
            connection_count,
            running_count,
            sleeping_count,
            dormant_count,
            total_cpu_time_ms,
            total_reads,
            total_writes,
            total_logical_reads
        FROM v_session_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_session_stats WHERE server_id = $1)
        ORDER BY connection_count DESC
        """;

    public static async Task<List<SessionStatRow>> GetLatestAsync(
        NpgsqlDataSource postgres, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = new List<SessionStatRow>();
        await using var command = postgres.CreateCommand(LatestSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SessionStatRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9)));
        }

        return rows;
    }
}
