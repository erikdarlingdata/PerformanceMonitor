/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #5643: the trailing window the tab and the MCP tool summarise the loop's late cycles over. The rows themselves are
/// <see cref="CollectionCycleSkip"/>'s: collector <see cref="CollectionCycleSkip.CollectorName"/>, status
/// <see cref="CollectionCycleSkip.Status"/>, <c>fanout_item_count</c> = the slots skipped, <c>slowest_item</c>/<c>slowest_item_ms</c>
/// = the cycle's slowest step, <c>duration_ms</c> = the cycle's total.
/// </summary>
internal static class SkippedCycleLog
{
    /// <summary>The trailing window the tab and the MCP tool summarise.</summary>
    internal static readonly TimeSpan Window = TimeSpan.FromHours(24);
}

/// <summary>
/// One server's late cycles over <see cref="SkippedCycleLog.Window"/>. <see cref="Count"/> 0 means none: every other
/// field is then null.
/// </summary>
public sealed class SkippedCycleSummary
{
    /// <summary>Late cycles in the window (one row each).</summary>
    public long Count { get; init; }

    /// <summary>Slots those cycles skipped, summed. A row that carries no count adds nothing.</summary>
    public long SlotsSkipped { get; init; }

    /// <summary>When the newest late cycle was logged (UTC).</summary>
    public DateTime? LastAtUtc { get; init; }

    /// <summary>The slowest step of the newest late cycle.</summary>
    public string? LastSlowestStep { get; init; }

    /// <summary>That step's cost in milliseconds.</summary>
    public long? LastSlowestStepMs { get; init; }

    /// <summary>The newest late cycle's own message.</summary>
    public string? LastMessage { get; init; }

    /// <summary>The newest late cycle's total duration in milliseconds.</summary>
    public long? LastDurationMs { get; init; }

    public static SkippedCycleSummary None { get; } = new();

    /// <summary>
    /// The one line the Collection Health tab prints: the all-clear when there is none, else the count, the slots, and the
    /// newest cycle's time and slowest step. <paramref name="clock"/> words the time on the server's own clock (#4766).
    /// </summary>
    public string SummaryLine(ServerClock? clock = null)
    {
        if (Count == 0 || LastAtUtc is null)
        {
            return "No skipped cycles in 24 h";
        }

        var step = LastSlowestStep is null
            ? "slowest step not recorded"
            : LastSlowestStepMs is { } ms
                ? $"slowest step {LastSlowestStep}, {FormatSeconds(ms)}"
                : $"slowest step {LastSlowestStep}";

        return string.Create(CultureInfo.InvariantCulture,
            $"Skipped cycles in 24 h: {Count:N0} late cycle(s), {SlotsSkipped:N0} slot(s) skipped. Last {CollectionHealthTime.Format(LastAtUtc.Value, clock)}: {step}.");
    }

    internal static string FormatSeconds(long ms) =>
        (ms / 1000.0).ToString("0.#", CultureInfo.InvariantCulture) + " s";
}

public partial class LocalDataService
{
    /// <summary>
    /// #5643: this server's late cycles in the 24 h ending at <paramref name="asOfUtc"/> (default now), from the
    /// <c>collection_cycle</c>/<c>SKIPPED</c> rows the collection loop writes. The window is inclusive at both ends. The
    /// newest row (ties broken by <c>log_id</c>) supplies the "last" fields.
    /// </summary>
    public async Task<SkippedCycleSummary> GetSkippedCycleSummaryAsync(int serverId, DateTime? asOfUtc = null, CancellationToken cancellationToken = default)
    {
        var end = asOfUtc ?? DateTime.UtcNow;
        var start = end - SkippedCycleLog.Window;

        using var connection = await OpenConnectionAsync(timer: null, cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    COUNT(*),
    SUM(COALESCE(fanout_item_count, 0)),
    MAX(collection_time),
    arg_max(slowest_item, (collection_time, log_id)),
    arg_max(slowest_item_ms, (collection_time, log_id)),
    arg_max(error_message, (collection_time, log_id)),
    arg_max(duration_ms, (collection_time, log_id))
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   status = $3
AND   collection_time >= $4
AND   collection_time <= $5";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = CollectionCycleSkip.CollectorName });
        command.Parameters.Add(new DuckDBParameter { Value = CollectionCycleSkip.Status });
        command.Parameters.Add(new DuckDBParameter { Value = start });
        command.Parameters.Add(new DuckDBParameter { Value = end });

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return SkippedCycleSummary.None;
        }

        var count = reader.IsDBNull(0) ? 0 : ToInt64(reader.GetValue(0));
        if (count == 0)
        {
            return SkippedCycleSummary.None;
        }

        return new SkippedCycleSummary
        {
            Count = count,
            SlotsSkipped = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
            LastAtUtc = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            LastSlowestStep = reader.IsDBNull(3) ? null : reader.GetString(3),
            LastSlowestStepMs = reader.IsDBNull(4) ? null : ToInt64(reader.GetValue(4)),
            LastMessage = reader.IsDBNull(5) ? null : reader.GetString(5),
            LastDurationMs = reader.IsDBNull(6) ? null : ToInt64(reader.GetValue(6))
        };
    }
}
