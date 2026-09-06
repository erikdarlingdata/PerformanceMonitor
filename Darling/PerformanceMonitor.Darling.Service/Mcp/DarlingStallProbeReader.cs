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
/// The read over <c>collect.collector_stall_probes</c> (#2880) — the out-of-band server-wide wait samples the
/// service takes while a collector is stalled mid-read.
///
/// <para>Newest first and unbanded, deliberately. A probe row is forensic evidence about ONE moment, not a
/// series with a healthy range: the whole reason the table exists is that the sequential sweep produces no
/// samples inside the four-minute window a stall occupies, so there is nothing to trend against and any band
/// would be inventing a threshold over a population of a few dozen rows a day.</para>
///
/// <para><b>Both reads carry the outcome census, not just the sampled rows.</b> A window returning three
/// samples out of eleven probes means eight probes could not get an answer, and that is the more interesting
/// finding — it is the first evidence anyone would have about whether a connection can be obtained mid-stall,
/// which #2880 lists as untested. A read that filtered to <c>SAMPLED</c> would hide exactly that.</para>
/// </summary>
internal static class DarlingStallProbeReader
{
    /// <summary>
    /// The samples themselves ($1 = since, naive UTC; $2 = an optional server_id, or NULL for the whole
    /// fleet; $3 = row limit), newest first.
    /// </summary>
    public const string ProbesSql = @"
SELECT
    p.probe_time,
    p.server_name,
    p.collector_name,
    p.outcome,
    p.budget_ms,
    p.trigger_elapsed_ms,
    p.trigger_rows_read,
    p.trigger_bytes_read,
    p.trigger_last_read_ms,
    p.connect_ms,
    p.query_ms,
    p.waiting_task_count,
    p.distinct_wait_types,
    p.top_wait_type,
    p.top_wait_total_ms,
    p.top_wait_max_ms,
    p.wait_summary,
    p.scheduler_count,
    p.runnable_tasks,
    p.work_queue_length,
    p.pending_disk_io,
    p.max_runnable_tasks,
    p.error_message
FROM collect.collector_stall_probes AS p
WHERE p.probe_time >= $1
AND   ($2::integer IS NULL OR p.server_id = $2)
ORDER BY p.probe_time DESC
LIMIT $3";

    /// <summary>
    /// The outcome census over the same window and scope ($1 = since, $2 = optional server_id). Its own read
    /// rather than derived from the limited row list above, so the census describes the WHOLE window even
    /// when the detail is capped.
    /// </summary>
    public const string OutcomeCensusSql = @"
SELECT
    p.outcome,
    count(*) AS probes,
    count(DISTINCT p.server_id) AS servers,
    max(p.connect_ms) AS max_connect_ms,
    max(p.query_ms) AS max_query_ms
FROM collect.collector_stall_probes AS p
WHERE p.probe_time >= $1
AND   ($2::integer IS NULL OR p.server_id = $2)
GROUP BY p.outcome
ORDER BY count(*) DESC";

    public sealed record StallProbeRow(
        DateTime ProbeTime,
        string ServerName,
        string CollectorName,
        string Outcome,
        int BudgetMs,
        int TriggerElapsedMs,
        long? TriggerRowsRead,
        long? TriggerBytesRead,
        int? TriggerLastReadMs,
        int? ConnectMs,
        int? QueryMs,
        long? WaitingTaskCount,
        int? DistinctWaitTypes,
        string? TopWaitType,
        long? TopWaitTotalMs,
        long? TopWaitMaxMs,
        string? WaitSummary,
        int? SchedulerCount,
        long? RunnableTasks,
        long? WorkQueueLength,
        long? PendingDiskIo,
        int? MaxRunnableTasks,
        string? ErrorMessage)
    {
        /// <summary>
        /// The delivered rate the probe fired on, in MB/s, or null when nothing had been delivered yet (the
        /// read was still inside <c>ExecuteReaderAsync</c>). Derived here rather than stored, so the stored
        /// row keeps only measurements and the reader owns the arithmetic.
        /// </summary>
        public double? TriggerMbPerSecond =>
            TriggerBytesRead is { } bytes && TriggerElapsedMs > 0
                ? Math.Round(bytes / 1024d / 1024d * 1000 / TriggerElapsedMs, 3)
                : null;

        /// <summary>
        /// <c>trigger_elapsed_ms - trigger_last_read_ms</c>: the time the reader had sat with nothing
        /// arriving, or null when no row had arrived. The V109 discriminator, computed at the probe's
        /// instant — 0-3 ms on every abandoned run measured, which is why the probe does not fire on it.
        /// </summary>
        public int? TerminalSilenceMs =>
            TriggerLastReadMs is { } lastRead ? Math.Max(0, TriggerElapsedMs - lastRead) : null;
    }

    public sealed record StallProbeOutcomeRow(
        string Outcome, long Probes, int Servers, int? MaxConnectMs, int? MaxQueryMs);

    public static async Task<List<StallProbeRow>> GetProbesAsync(
        NpgsqlDataSource postgres, DateTime since, int? serverId, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<StallProbeRow>();

        await using var command = postgres.CreateCommand(ProbesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(since, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId.HasValue ? serverId.Value : (object)DBNull.Value);
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StallProbeRow(
                reader.GetDateTime(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetInt32(4),
                reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetInt32(9),
                reader.IsDBNull(10) ? null : reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetInt64(11),
                reader.IsDBNull(12) ? null : reader.GetInt32(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetInt64(14),
                reader.IsDBNull(15) ? null : reader.GetInt64(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? null : reader.GetInt32(17),
                reader.IsDBNull(18) ? null : reader.GetInt64(18),
                reader.IsDBNull(19) ? null : reader.GetInt64(19),
                reader.IsDBNull(20) ? null : reader.GetInt64(20),
                reader.IsDBNull(21) ? null : reader.GetInt32(21),
                reader.IsDBNull(22) ? null : reader.GetString(22)));
        }

        return rows;
    }

    public static async Task<List<StallProbeOutcomeRow>> GetOutcomeCensusAsync(
        NpgsqlDataSource postgres, DateTime since, int? serverId,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<StallProbeOutcomeRow>();

        await using var command = postgres.CreateCommand(OutcomeCensusSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(since, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId.HasValue ? serverId.Value : (object)DBNull.Value);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StallProbeOutcomeRow(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4)));
        }

        return rows;
    }
}
