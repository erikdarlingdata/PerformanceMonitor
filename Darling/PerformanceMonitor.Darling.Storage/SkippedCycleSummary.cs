/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5735: one server's skipped-slot rows (<see cref="CollectionCycleRow"/>) over the trailing
/// <see cref="SkippedCycleReader.Window"/>. <see cref="Count"/> 0 means none: every other field is then null (and
/// <see cref="SlotsSkipped"/> 0). The twin of Lite's <c>SkippedCycleSummary</c> (#5643). On Darling a slot is one
/// scheduled run of one collector, summed across the server's collectors; on Lite it is a whole cycle.
/// </summary>
public sealed class SkippedCycleSummary
{
    /// <summary>Skipped-slot rows in the window (the writer coalesces them to one per server per five minutes).</summary>
    public long Count { get; init; }

    /// <summary>Slots those rows record, summed. A row that carries no count adds nothing.</summary>
    public long SlotsSkipped { get; init; }

    /// <summary>When the newest row was written (UTC, stored as a naive timestamp).</summary>
    public DateTime? LastAtUtc { get; init; }

    /// <summary>The newest row's cause (<c>slowest_item</c>).</summary>
    public string? LastSlowestStep { get; init; }

    /// <summary>That cause's length in milliseconds.</summary>
    public long? LastSlowestStepMs { get; init; }

    /// <summary>The newest row's own message.</summary>
    public string? LastMessage { get; init; }

    /// <summary>The newest row's cause length (<c>duration_ms</c>) in milliseconds.</summary>
    public long? LastDurationMs { get; init; }

    /// <summary>No rows in the window.</summary>
    public static SkippedCycleSummary None { get; } = new();
}

/// <summary>
/// #5735: the read behind the MCP <c>skipped_cycles</c> block, the web panel and the Viewer's Collection Health line.
/// One statement, one index seek: <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c>
/// serves the equality on the collector and the range on the time. It is never memoised: the block shows what the
/// loop just did, and the one-minute collector-health memo would show a row a minute late.
/// </summary>
public static class SkippedCycleReader
{
    /// <summary>The trailing window the block summarises.</summary>
    public static readonly TimeSpan Window = TimeSpan.FromHours(24);

    /// <summary>
    /// The statement. $1 server id, $2 collector name, $3 status, $4 window start, $5 window end. The window is
    /// inclusive at both ends. The count and the sum are window aggregates, evaluated before the LIMIT, so the one row
    /// that comes back is the newest (collection_time, then log_id) and carries the totals; no rows means none.
    /// </summary>
    public const string SummarySql = @"
SELECT
    COUNT(*) OVER ()                                          AS row_count,
    SUM(COALESCE(fanout_item_count, 0)) OVER ()               AS slots_skipped,
    collection_time,
    slowest_item,
    slowest_item_ms,
    error_message,
    duration_ms
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   status = $3
AND   collection_time >= $4
AND   collection_time <= $5
ORDER BY collection_time DESC, log_id DESC
LIMIT 1";

    /// <summary>
    /// This server's skipped-slot rows in the <see cref="Window"/> ending at <paramref name="asOfUtc"/> (default now).
    /// </summary>
    public static async Task<SkippedCycleSummary> GetSkippedCycleSummaryAsync(
        NpgsqlDataSource postgres, int serverId, DateTime? asOfUtc = null, int commandTimeoutSeconds = 30, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var end = asOfUtc ?? DateTime.UtcNow;
        var start = end - Window;

        await using var command = postgres.CreateCommand(SummarySql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = CollectionCycleRow.CollectorName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = CollectionCycleRow.Status });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return SkippedCycleSummary.None;
        }

        return new SkippedCycleSummary
        {
            Count = reader.GetInt64(0),
            SlotsSkipped = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
            LastAtUtc = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
            LastSlowestStep = reader.IsDBNull(3) ? null : reader.GetString(3),
            LastSlowestStepMs = reader.IsDBNull(4) ? null : reader.GetInt32(4),
            LastMessage = reader.IsDBNull(5) ? null : reader.GetString(5),
            LastDurationMs = reader.IsDBNull(6) ? null : Convert.ToInt64(reader.GetValue(6))
        };
    }
}
