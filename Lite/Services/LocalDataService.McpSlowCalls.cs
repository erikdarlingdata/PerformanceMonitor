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

/// <summary>
/// One MCP tool call that took over the slow-call threshold (#9423, #9424). Never carries an argument value: the only argument
/// kept is the window the call asked for.
/// </summary>
/// <param name="CallTimeUtc">When the call started, UTC.</param>
/// <param name="Tool">The tool name.</param>
/// <param name="Outcome">ok, error (the tool answered with an error envelope or flagged an error), exception (a throw reached the caller) or cancelled.</param>
/// <param name="TotalMs">Whole call time in milliseconds, from the call filter's entry to the tool's return.</param>
/// <param name="LockWaitMs">Time spent waiting for the store's read lock, added up over every connection the call opened.</param>
/// <param name="HotMs">Read time that is not an archive span read, inside the total. NULL when the tool keeps no phase split.</param>
/// <param name="ArchiveMs">Time spent reading archive spans, inside the total. NULL when the tool keeps no phase split.</param>
/// <param name="ArchiveFiles">Archive files those span reads opened. NULL when the tool keeps no phase split.</param>
/// <param name="ResponseBytes">UTF-8 bytes of the text the call returned.</param>
/// <param name="HoursBack">The hours_back (or hours) the call asked for, when it had one.</param>
/// <param name="ProcessPeakWorkingSetBytes">The highest working set of the whole Lite process while the call ran (the compaction history's figure, read the same way). Calls that overlap share it. Stored in the <c>peak_working_set_bytes</c> column.</param>
/// <param name="DuckDbPeakMemoryBytes">The highest duckdb_memory() total read while the call ran. NULL when the store lock was busy at every read. Calls that overlap share it. Stored in the <c>duckdb_memory_bytes</c> column.</param>
/// <param name="MemorySource">Where the two memory figures came from, in words, including what the sampling can miss.</param>
/// <param name="ServerName">The server the reply names, else the server_name the call carried. NULL when the call named none.</param>
/// <param name="RowsReturned">Rows the reply holds, read from its count or its one array. NULL when the reply carries neither.</param>
/// <param name="AnsweredFrom">Which read answered the call (#9885): <c>chain</c>, or <c>fold: </c> and its short reason, as <c>get_object_usage</c> says it in its reply. NULL for a tool that records none.</param>
/// <param name="PhaseSplit">The call's phases in words, the line <c>ReadPhaseSplit.Describe</c> writes (#9885). NULL for a tool that records none.</param>
public sealed record McpSlowCall(
    DateTime CallTimeUtc,
    string Tool,
    string Outcome,
    long TotalMs,
    long? LockWaitMs,
    long? HotMs,
    long? ArchiveMs,
    long? ArchiveFiles,
    long ResponseBytes,
    double? HoursBack,
    long? ProcessPeakWorkingSetBytes,
    long? DuckDbPeakMemoryBytes,
    string? MemorySource,
    string? ServerName = null,
    long? RowsReturned = null,
    string? AnsweredFrom = null,
    string? PhaseSplit = null);

public partial class LocalDataService
{
    /// <summary>Slow-call rows older than this many days are deleted by the next write (#9423).</summary>
    internal const int McpSlowCallRetentionDays = 7;

    /// <summary>The most slow-call rows kept: the newest ones, past this the oldest go (#9423).</summary>
    internal const int McpSlowCallMaxRows = 1000;

    private static DateTime UnspecifiedKind(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>Adds one slow-call row, then trims the table to its retention and row cap under the same write lock.</summary>
    internal async Task RecordMcpSlowCallAsync(McpSlowCall call)
    {
        using var connection = await OpenWriteConnectionAsync();
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = @"
INSERT INTO mcp_slow_calls
    (call_time, tool, outcome, total_ms, lock_wait_ms, hot_ms, archive_ms, archive_files, response_bytes, hours_back,
     peak_working_set_bytes, duckdb_memory_bytes, memory_source, server_name, rows_returned, answered_from, phase_split)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)";
            insert.Parameters.Add(new DuckDBParameter { Value = UnspecifiedKind(call.CallTimeUtc) });
            insert.Parameters.Add(new DuckDBParameter { Value = call.Tool });
            insert.Parameters.Add(new DuckDBParameter { Value = call.Outcome });
            insert.Parameters.Add(new DuckDBParameter { Value = call.TotalMs });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.LockWaitMs ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.HotMs ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.ArchiveMs ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.ArchiveFiles ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = call.ResponseBytes });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.HoursBack ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.ProcessPeakWorkingSetBytes ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.DuckDbPeakMemoryBytes ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.MemorySource ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.ServerName ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.RowsReturned ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.AnsweredFrom ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)call.PhaseSplit ?? DBNull.Value });
            await insert.ExecuteNonQueryAsync();
        }

        using var trim = connection.CreateCommand();
        trim.CommandText = @"
DELETE FROM mcp_slow_calls
WHERE call_time < $1
   OR rowid NOT IN (SELECT rowid FROM mcp_slow_calls ORDER BY call_time DESC, rowid DESC LIMIT $2)";
        trim.Parameters.Add(new DuckDBParameter { Value = UnspecifiedKind(DateTime.UtcNow.AddDays(-McpSlowCallRetentionDays)) });
        trim.Parameters.Add(new DuckDBParameter { Value = McpSlowCallMaxRows });
        await trim.ExecuteNonQueryAsync();
    }

    /// <summary>Slow-call rows from <paramref name="sinceUtc"/> on, newest first, at most <paramref name="limit"/>, optionally one tool.</summary>
    internal async Task<List<McpSlowCall>> GetMcpSlowCallsAsync(DateTime sinceUtc, int limit, string? tool)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT call_time, tool, outcome, total_ms, lock_wait_ms, hot_ms, archive_ms, archive_files, response_bytes, hours_back,
       peak_working_set_bytes, duckdb_memory_bytes, memory_source, server_name, rows_returned, answered_from, phase_split
FROM   mcp_slow_calls
WHERE  call_time >= $1
AND    ($2 IS NULL OR tool = $2)
ORDER  BY call_time DESC, rowid DESC
LIMIT  $3";
        command.Parameters.Add(new DuckDBParameter { Value = UnspecifiedKind(sinceUtc) });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)tool ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        var rows = new List<McpSlowCall>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new McpSlowCall(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetDouble(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetInt64(11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetInt64(14),
                reader.IsDBNull(15) ? null : reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16)));
        }

        return rows;
    }

    /// <summary>DuckDB's memory total now, or null when the store lock is busy (see <c>DuckDbInitializer.TryReadDuckDbMemoryBytes</c>).</summary>
    internal long? TryReadDuckDbMemoryBytes() => _duckDb.TryReadDuckDbMemoryBytes();
}
