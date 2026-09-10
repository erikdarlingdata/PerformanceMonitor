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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side read for the long-query completion MCP tool (<see cref="DarlingMcpLongQueryTools"/>) — the
/// SAME collected data the viewer's <c>ViewerDataService.LongQueries.cs</c> reads, adapted here so the MCP
/// host never references the WPF viewer project (#1496). A STORED read (no live monitored-server hit),
/// keyed by <c>server_id</c> and windowed on the naive-UTC <c>collection_time</c> prefix, over the BASE
/// <c>long_query_completions</c> table (a post-V14 collector has no <c>v_*</c> passthrough view). Ordered by
/// duration DESC (NULLS LAST so attentions, whose duration is NULL, follow the ranked completions). The SQL
/// is a public const so Darling.Tests can pin the dialect + columns without a live Postgres.
/// </summary>
internal static class DarlingLongQueryReader
{
    /// <summary>One long-query completion row — a completed rpc/batch (runtime, CPU, I/O, rows, result) or an
    /// attention (cancel/timeout; duration NULL).</summary>
    public sealed class LongQueryReadRow
    {
        public DateTime? EventTime { get; set; }
        public string? EventType { get; set; }
        public string? DatabaseName { get; set; }
        public int? SessionId { get; set; }
        public string? ClientAppName { get; set; }
        public int? ClientPid { get; set; }
        public string? NtUserName { get; set; }
        public string? ServerPrincipalName { get; set; }
        public string? QueryHash { get; set; }
        public long? EventSequence { get; set; }
        public long? DurationMicroseconds { get; set; }
        public long? CpuTimeMicroseconds { get; set; }
        public long? PhysicalReads { get; set; }
        public long? LogicalReads { get; set; }
        public long? Writes { get; set; }
        public long? RowCount { get; set; }
        public string? Result { get; set; }
        public string? StatementText { get; set; }
        public string? ObjectName { get; set; }
    }

    public const string LongQueryCompletionsSql = """
        SELECT
            event_time,
            event_type,
            database_name,
            session_id,
            client_app_name,
            client_pid,
            nt_username,
            server_principal_name,
            query_hash,
            event_sequence,
            duration_microseconds,
            cpu_time_microseconds,
            physical_reads,
            logical_reads,
            writes,
            row_count,
            result,
            statement_text,
            object_name
        FROM long_query_completions
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY duration_microseconds DESC NULLS LAST, event_time DESC
        LIMIT 200
        """;

    public static async Task<List<LongQueryReadRow>> GetRecentLongQueryCompletionsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<LongQueryReadRow>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(LongQueryCompletionsSql, connection);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new LongQueryReadRow
            {
                EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                EventType = reader.IsDBNull(1) ? null : reader.GetString(1),
                DatabaseName = reader.IsDBNull(2) ? null : reader.GetString(2),
                SessionId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                ClientAppName = reader.IsDBNull(4) ? null : reader.GetString(4),
                ClientPid = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                NtUserName = reader.IsDBNull(6) ? null : reader.GetString(6),
                ServerPrincipalName = reader.IsDBNull(7) ? null : reader.GetString(7),
                QueryHash = reader.IsDBNull(8) ? null : reader.GetString(8),
                EventSequence = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                DurationMicroseconds = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                CpuTimeMicroseconds = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                PhysicalReads = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                Writes = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                RowCount = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                Result = reader.IsDBNull(16) ? null : reader.GetString(16),
                StatementText = reader.IsDBNull(17) ? null : reader.GetString(17),
                ObjectName = reader.IsDBNull(18) ? null : reader.GetString(18),
            });
        }

        return rows;
    }
}
