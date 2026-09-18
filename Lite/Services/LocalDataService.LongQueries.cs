/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Recent long-running query completions (#1496) from <c>v_long_query_completions</c> (the archive
    /// union view), for the Long Queries grid. Reads the most recent completions in the window; the grid
    /// applies a view-only DESCENDING-by-duration sort, so the SQL keeps the chronological ORDER BY
    /// (mirrors the blocked-process reader).
    ///
    /// <para>The GRID's read, and no longer the MCP tool's (#3541 A3). <c>get_long_query_completions</c>
    /// used to take these 200 NEWEST rows and re-rank them by duration in C#, which is a different population
    /// from the window's slowest: on a busy trace the slowest run of the window could sit outside the newest
    /// 200 and be omitted entirely, while Darling's twin — ranked by duration in SQL — kept it. Same tool
    /// name, different truth per SKU. The tool now reads <see cref="GetSlowestLongQueryCompletionsAsync"/>.</para>
    /// </summary>
    public async Task<List<LongQueryCompletionRow>> GetRecentLongQueryCompletionsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = LongQueryCompletionsSelect + @"
FROM v_long_query_completions
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
ORDER BY event_time DESC
LIMIT 200";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        return await ReadLongQueryCompletionsAsync(command);
    }

    /// <summary>
    /// The <paramref name="limit"/> SLOWEST completions in the window — the MCP tool's read (#3541 A3),
    /// ranked in SQL by <c>duration_microseconds DESC NULLS LAST</c> so attentions (whose duration is NULL)
    /// follow the ranked completions, exactly as Darling's <c>DarlingLongQueryReader</c> orders it. Every row in
    /// the window is a candidate, so a page cut here still holds the window's slowest; the grid read above
    /// cannot promise that. Callers detecting truncation pass <c>limit + 1</c> and read the extra row as the
    /// signal. Windowed on the UTC helper (offset 0) because the caller is the MCP tool, whose window is UTC.
    /// </summary>
    public async Task<List<LongQueryCompletionRow>> GetSlowestLongQueryCompletionsAsync(int serverId, int hoursBack, DateTime? asOfUtc, int limit)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);

        command.CommandText = LongQueryCompletionsSelect + @"
FROM v_long_query_completions
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY duration_microseconds DESC NULLS LAST, event_time DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        return await ReadLongQueryCompletionsAsync(command);
    }

    /// <summary>The projection both long-query reads share, so the two cannot drift a column apart.</summary>
    private const string LongQueryCompletionsSelect = @"
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
    object_name";

    private static async Task<List<LongQueryCompletionRow>> ReadLongQueryCompletionsAsync(DuckDBCommand command)
    {
        var items = new List<LongQueryCompletionRow>();
        using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                items.Add(new LongQueryCompletionRow
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
        }

        return items;
    }
}

/// <summary>
/// One long-query completion grid row (#1496): the completed rpc/batch (with runtime, CPU, I/O,
/// row count, result) or an <c>attention</c> (a cancelled/timed-out query — duration NULL because the
/// attention event's own duration is cancellation-handling time, not query runtime).
/// </summary>
public class LongQueryCompletionRow
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

    public string EventTimeLocal => ServerTimeHelper.FormatServerTime(EventTime);
    public string DurationFormatted => FormatMicroseconds(DurationMicroseconds);
    public string CpuFormatted => FormatMicroseconds(CpuTimeMicroseconds);

    /// <summary>An attention row is a cancelled/timed-out query — no runtime, secondary evidence.</summary>
    public bool IsAttention => string.Equals(EventType, "attention", StringComparison.OrdinalIgnoreCase);

    /// <summary>A completed event aborted mid-flight (client cancel / query timeout) — the row tints.</summary>
    public bool IsAborted => string.Equals(Result, "Abort", StringComparison.OrdinalIgnoreCase);

    private static string FormatMicroseconds(long? microseconds)
    {
        if (microseconds is not long us)
        {
            return "";
        }

        return us < 1_000_000
            ? $"{us / 1000.0:F0} ms"
            : $"{us / 1_000_000.0:F1} sec";
    }
}
