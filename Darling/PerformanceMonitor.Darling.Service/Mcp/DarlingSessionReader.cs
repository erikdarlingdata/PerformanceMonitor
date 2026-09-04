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
/// Service-side reads for the session MCP tools (<see cref="DarlingMcpSessionTools"/>) — the SAME
/// collected data Lite's <c>McpSessionTools</c> / <c>McpWaitTools</c> and the viewer read, adapted here
/// so the MCP host never references the WPF viewer project. Each is a STORED read (no live
/// monitored-server hit) keyed by <c>server_id</c>.
///
/// <para>
/// get_session_stats reads the LATEST per-application snapshot from <c>v_session_stats</c> (Lite's
/// <c>GetLatestSessionStatsAsync</c> — the store-faithful per-<c>program_name</c> shape, NOT the
/// Dashboard's server-wide <c>session_summary_stats</c> summary; where the two diverge slices 1+2 follow
/// Lite). get_active_queries reads the <c>query_snapshots</c> base table over the window (the viewer's
/// <c>LatestQuerySnapshotsSql</c>). get_waiting_tasks reads the <c>waiting_tasks</c> BASE table (there is
/// no <c>v_waiting_tasks</c> passthrough view — the base table has existed since V1, and the viewer's
/// trend reads hit it directly the same way); <c>resource_description</c> is stored but the collector
/// writes it NULL ("no longer collected"). Every SQL string is a public const so Darling.Tests can pin the
/// dialect + columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingSessionReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One (program_name) group at the latest session_stats snapshot.</summary>
    public sealed record SessionStatRow(
        DateTime CollectionTime, string ProgramName, long ConnectionCount, int RunningCount, int SleepingCount,
        int DormantCount, long? TotalCpuTimeMs, long? TotalReads, long? TotalWrites, long? TotalLogicalReads);

    /// <summary>One captured query snapshot — the columns Lite's get_active_queries surfaces.</summary>
    public sealed record ActiveQueryRow(
        DateTime CollectionTime, int SessionId, string? DatabaseName, string? Status, long CpuTimeMs,
        long TotalElapsedTimeMs, string? ElapsedTimeFormatted, long LogicalReads, long Reads, long Writes,
        string? WaitType, long WaitTimeMs, int BlockingSessionId, int Dop, int ParallelWorkerCount,
        double GrantedQueryMemoryGb, string? TransactionIsolationLevel, int OpenTransactionCount,
        string? LoginName, string? HostName, string? ProgramName, string? QueryText);

    /// <summary>One waiting-task snapshot row.</summary>
    public sealed record WaitingTaskRow(
        DateTime CollectionTime, int SessionId, string? WaitType, long WaitDurationMs,
        int? BlockingSessionId, string? ResourceDescription, string? DatabaseName);

    /* ─────────────────────────── session stats (latest snapshot, per application) ─────────────────────────── */

    /// <summary>
    /// The latest per-application session_stats snapshot — Lite's <c>GetLatestSessionStatsAsync</c>: every
    /// program_name at the newest collection, most connections first. connection_count is bigint; the
    /// running/sleeping/dormant counts are integer; the cumulative totals are nullable bigint. $1 server_id.
    /// </summary>
    public const string LatestSessionStatsSql = """
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

    public static async Task<List<SessionStatRow>> GetLatestSessionStatsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<SessionStatRow>();
        await using var command = postgres.CreateCommand(LatestSessionStatsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
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

    /* ─────────────────────────── active queries (query snapshots over the window) ─────────────────────────── */

    /// <summary>
    /// The captured query snapshots over the window — the viewer's <c>LatestQuerySnapshotsSql</c> projected
    /// to the columns Lite's get_active_queries surfaces, from the base <c>query_snapshots</c> table (the
    /// viewer reads base here too). granted_query_memory_gb is <c>numeric(18,2)</c> → double precision. WAITFOR
    /// shells are trimmed. $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string ActiveQueriesSql = """
        SELECT
            collection_time,
            session_id,
            database_name,
            status,
            cpu_time_ms,
            total_elapsed_time_ms,
            elapsed_time_formatted,
            logical_reads,
            reads,
            writes,
            wait_type,
            wait_time_ms,
            blocking_session_id,
            dop,
            parallel_worker_count,
            CAST(granted_query_memory_gb AS double precision),
            transaction_isolation_level,
            open_transaction_count,
            login_name,
            host_name,
            program_name,
            query_text
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   query_text NOT LIKE 'WAITFOR%'
        ORDER BY collection_time DESC, cpu_time_ms DESC
        """;

    public static async Task<List<ActiveQueryRow>> GetActiveQueriesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<ActiveQueryRow>();
        await using var command = postgres.CreateCommand(ActiveQueriesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ActiveQueryRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                reader.IsDBNull(15) ? 0 : reader.GetDouble(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                reader.IsDBNull(18) ? null : reader.GetString(18),
                reader.IsDBNull(19) ? null : reader.GetString(19),
                reader.IsDBNull(20) ? null : reader.GetString(20),
                reader.IsDBNull(21) ? null : reader.GetString(21)));
        }

        return rows;
    }

    /* ─────────────────────────── waiting tasks (base table over the window) ─────────────────────────── */

    /// <summary>
    /// The recently-captured waiting tasks over the window — the base <c>waiting_tasks</c> table (there is no
    /// <c>v_waiting_tasks</c> view), newest first then longest wait. resource_description is stored but always
    /// NULL (the collector no longer collects it). $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string WaitingTasksSql = """
        SELECT
            collection_time,
            session_id,
            wait_type,
            wait_duration_ms,
            blocking_session_id,
            resource_description,
            database_name
        FROM waiting_tasks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   wait_type IS NOT NULL
        ORDER BY collection_time DESC, wait_duration_ms DESC
        LIMIT 500
        """;

    public static async Task<List<WaitingTaskRow>> GetWaitingTasksAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<WaitingTaskRow>();
        await using var command = postgres.CreateCommand(WaitingTasksSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new WaitingTaskRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return rows;
    }
}
