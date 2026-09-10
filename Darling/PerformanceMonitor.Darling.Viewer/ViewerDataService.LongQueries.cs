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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One long-query completion grid row (#1496): a completed rpc/batch over the duration threshold (with
/// runtime, CPU, I/O, row count, result) or an <c>attention</c> (a cancelled/timed-out query — duration
/// NULL, because the attention event's own duration is cancellation-handling time, not query runtime).
/// Mirrors Lite's <c>LongQueryCompletionRow</c>; event_time is stored naive-UTC, so
/// <see cref="EventTimeLocal"/> converts to viewer-local via <see cref="ViewerTimeHelper.ForDisplay"/>.
/// </summary>
public sealed class ViewerLongQueryRow
{
    public DateTime CollectionTime { get; set; }
    public DateTime? EventTime { get; set; }
    public string EventType { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public int? SessionId { get; set; }
    public string ClientAppName { get; set; } = "";
    public int? ClientPid { get; set; }
    public string NtUserName { get; set; } = "";
    public string ServerPrincipalName { get; set; } = "";
    public string QueryHash { get; set; } = "";
    public long? EventSequence { get; set; }
    public long? DurationMicroseconds { get; set; }
    public long? CpuTimeMicroseconds { get; set; }
    public long? PhysicalReads { get; set; }
    public long? LogicalReads { get; set; }
    public long? Writes { get; set; }
    public long? RowCountValue { get; set; }
    public string Result { get; set; } = "";
    public string StatementText { get; set; } = "";
    public string ObjectName { get; set; } = "";

    public string EventTimeLocal
        => EventTime is { } eventTime ? ViewerTimeHelper.ForDisplay(eventTime).ToString("yyyy-MM-dd HH:mm:ss") : "";

    public string DurationFormatted => FormatMicroseconds(DurationMicroseconds);
    public string CpuFormatted => FormatMicroseconds(CpuTimeMicroseconds);

    /// <summary>An attention row is a cancelled/timed-out query — no runtime, secondary evidence.</summary>
    public bool IsAttention => string.Equals(EventType, "attention", StringComparison.OrdinalIgnoreCase);

    /// <summary>A completed event aborted mid-flight (client cancel / query timeout) — the row tints.</summary>
    public bool IsAborted => string.Equals(Result, "Abort", StringComparison.OrdinalIgnoreCase);

    private static string FormatMicroseconds(long? microseconds) =>
        microseconds is not long us ? "" : us < 1_000_000 ? $"{us / 1000.0:F0} ms" : $"{us / 1_000_000.0:F1} sec";
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Recent long-running query completions (#1496) for one server, from the BASE
    /// <c>long_query_completions</c> table (a post-V14 collector has no <c>v_*</c> passthrough view). Same
    /// windowed shape as the blocking read (event_time DESC, LIMIT 200); the grid applies a view-only
    /// DESCENDING-by-duration sort. $1 server_id, $2 window start, $3 window end (naive UTC), $4 db filter.
    /// </summary>
    public const string LongQueryCompletionsSql = """
        SELECT
            collection_time,
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
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ORDER BY event_time DESC
        LIMIT 200
        """;

    public async Task<List<ViewerLongQueryRow>> GetRecentLongQueryCompletionsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerLongQueryRow>();

        await using var command = _dataSource.CreateCommand(LongQueryCompletionsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerLongQueryRow
            {
                CollectionTime = reader.GetDateTime(0),
                EventTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                EventType = reader.IsDBNull(2) ? "" : reader.GetString(2),
                DatabaseName = reader.IsDBNull(3) ? "" : reader.GetString(3),
                SessionId = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                ClientAppName = reader.IsDBNull(5) ? "" : reader.GetString(5),
                ClientPid = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                NtUserName = reader.IsDBNull(7) ? "" : reader.GetString(7),
                ServerPrincipalName = reader.IsDBNull(8) ? "" : reader.GetString(8),
                QueryHash = reader.IsDBNull(9) ? "" : reader.GetString(9),
                EventSequence = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                DurationMicroseconds = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                CpuTimeMicroseconds = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                PhysicalReads = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                LogicalReads = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                Writes = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                RowCountValue = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                Result = reader.IsDBNull(17) ? "" : reader.GetString(17),
                StatementText = reader.IsDBNull(18) ? "" : reader.GetString(18),
                ObjectName = reader.IsDBNull(19) ? "" : reader.GetString(19),
            });
        }

        return rows;
    }

    /// <summary>
    /// The effective enabled state of the opt-in <c>long_query_completions</c> collector for one server,
    /// resolved from <c>config_collector_schedules</c> (per-server override &gt; fleet override &gt; the
    /// shared default), so the Long Queries tab can show its "trace is OFF" empty-state banner. Mirrors the
    /// service's <c>StoreConfigProvider.ResolveSchedule</c> enabled precedence; the shared default for this
    /// collector is OFF (CollectorScheduleDefaults DefaultEnabled=false — the viewer doesn't reference the
    /// collector library, so the false default is inlined here rather than read from it).
    /// </summary>
    public async Task<bool> GetLongQueryTraceEnabledAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var overrides = await GetCollectorSchedulesAsync(cancellationToken);

        bool? perServer = null;
        bool? fleet = null;
        foreach (var o in overrides)
        {
            if (!string.Equals(o.CollectorName, "long_query_completions", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (o.ServerId == serverId)
            {
                perServer = o.Enabled;
            }
            else if (o.ServerId is null)
            {
                fleet = o.Enabled;
            }
        }

        return perServer ?? fleet ?? false;
    }
}
