/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// One statement of a cached batch as the store last saw it (#5632, step 10): the before image an eviction journals, and the shape
/// <c>get_compiles_after_evict</c> reads back. <c>creation_time</c> and <c>last_execution_time</c> are on the monitored server's own clock;
/// <c>collection_time</c> is the store's (UTC). The two clocks are never compared with each other.
/// </summary>
internal sealed record PlanEvictStatementImage(
    [property: JsonPropertyName("query_hash")] string? QueryHash,
    [property: JsonPropertyName("query_plan_hash")] string? QueryPlanHash,
    [property: JsonPropertyName("sql_handle")] string? SqlHandle,
    [property: JsonPropertyName("statement_start_offset")] int? StatementStartOffset,
    [property: JsonPropertyName("statement_end_offset")] int? StatementEndOffset,
    [property: JsonPropertyName("plan_handle")] string? PlanHandle,
    [property: JsonPropertyName("creation_time")] string? CreationTime,
    [property: JsonPropertyName("plan_generation_num")] long? PlanGenerationNum,
    [property: JsonPropertyName("execution_count")] long? ExecutionCount,
    [property: JsonPropertyName("last_execution_time")] string? LastExecutionTime,
    [property: JsonPropertyName("collection_time")] string? CollectionTime,
    [property: JsonPropertyName("avg_cpu_ms")] double? AvgCpuMs,
    [property: JsonPropertyName("avg_elapsed_ms")] double? AvgElapsedMs,
    [property: JsonPropertyName("avg_logical_reads")] double? AvgLogicalReads);

/// <summary>The store reads of the eviction tool and of <c>get_compiles_after_evict</c>. They touch no remediation type and never reach a monitored server.</summary>
internal static class DarlingPlanEvictStore
{
    /// <summary>The store window the before image looks back over.</summary>
    internal static readonly TimeSpan BeforeWindow = TimeSpan.FromHours(24);

    /// <summary>The window after an eviction that <c>get_compiles_after_evict</c> reads.</summary>
    internal static readonly TimeSpan AfterWindow = TimeSpan.FromDays(7);

    /// <summary>The most statements a before image holds.</summary>
    internal const int MaxStatements = 50;

    /// <summary>The most collected rows one compile read takes.</summary>
    internal const int MaxAfterRows = 5000;

    /// <summary>The timestamp text of the answer: round-trip, no zone, because the store's columns have none.</summary>
    internal static string? Time(DateTime? value) => value?.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture);

    internal const string StatementColumns = @"
       query_hash, query_plan_hash, sql_handle, statement_start_offset, statement_end_offset, plan_handle, creation_time,
       plan_generation_num, execution_count, last_execution_time, collection_time,
       total_worker_time, total_elapsed_time, total_logical_reads";

    /// <summary>The latest collected row of each statement of the handle, on this server, in the window. $1 server, $2 handle (upper case), $3 window start, $4 limit.</summary>
    internal const string BeforeSql = @"
SELECT DISTINCT ON (query_hash, sql_handle, statement_start_offset, statement_end_offset)" + StatementColumns + @"
FROM collect.query_stats
WHERE server_id = $1 AND upper(plan_handle) = $2 AND collection_time >= $3
ORDER BY query_hash, sql_handle, statement_start_offset, statement_end_offset, collection_time DESC
LIMIT $4";

    /// <summary>The procedure the handle belongs to, when it is a procedure's. $1 server, $2 handle (upper case), $3 window start.</summary>
    internal const string ProcedureSql = @"
SELECT database_name, schema_name, object_name
FROM collect.procedure_stats
WHERE server_id = $1 AND upper(plan_handle) = $2 AND collection_time >= $3
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// The latest row of each compile (each creation time under each handle) of the given statements, collected after the eviction and no more than
    /// the after window later. $1 server, $2 query hashes, $3 sql handles, $4 window start (exclusive), $5 window end, $6 limit.
    /// </summary>
    internal const string AfterSql = @"
SELECT DISTINCT ON (query_hash, sql_handle, statement_start_offset, statement_end_offset, creation_time, plan_handle)" + StatementColumns + @"
FROM collect.query_stats
WHERE server_id = $1 AND query_hash = ANY($2) AND sql_handle = ANY($3) AND collection_time > $4 AND collection_time <= $5
ORDER BY query_hash, sql_handle, statement_start_offset, statement_end_offset, creation_time, plan_handle, collection_time DESC
LIMIT $6";

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    internal static NpgsqlCommand Command(NpgsqlConnection connection, string sql) =>
        new(sql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };

    private static string? Text(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int? Int(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    private static long? Long(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);

    private static DateTime? Stamp(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);

    /// <summary>One collected row, with its time columns kept as times so the compile read compares them as times.</summary>
    internal sealed record Row(PlanEvictStatementImage Image, DateTime? CreationTime, DateTime? CollectionTime = null);

    private static Row ReadRow(NpgsqlDataReader reader)
    {
        var executions = Long(reader, 8);
        var cpu = Long(reader, 11);
        var elapsed = Long(reader, 12);
        var reads = Long(reader, 13);
        double? Per(long? total, double scale) => total is long t && executions is > 0 ? t / scale / executions.Value : null;
        var creation = Stamp(reader, 6);
        return new Row(new PlanEvictStatementImage(
            Text(reader, 0), Text(reader, 1), Text(reader, 2), Int(reader, 3), Int(reader, 4), Text(reader, 5), Time(creation),
            Long(reader, 7), executions, Time(Stamp(reader, 9)), Time(Stamp(reader, 10)),
            Per(cpu, 1000.0), Per(elapsed, 1000.0), Per(reads, 1.0)), creation, Stamp(reader, 10));
    }

    /// <summary>The statements Darling collected for the handle on the server in the last 24 hours, the latest row of each, at most <see cref="MaxStatements"/>.</summary>
    internal static async Task<(IReadOnlyList<PlanEvictStatementImage> Statements, string? ProcedureName)> ReadBeforeAsync(
        NpgsqlDataSource postgres, int serverId, string normalHandle, DateTime nowUtc, CancellationToken ct)
    {
        var handle = normalHandle.ToUpperInvariant();
        var since = Naive(nowUtc - BeforeWindow);
        var statements = new List<PlanEvictStatementImage>();
        string? procedure = null;
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await using (var command = Command(connection, BeforeSql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = handle, NpgsqlDbType = NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = since, NpgsqlDbType = NpgsqlDbType.Timestamp });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = MaxStatements, NpgsqlDbType = NpgsqlDbType.Integer });
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                statements.Add(ReadRow(reader).Image);
        }

        if (statements.Count > 0)
        {
            await using var command = Command(connection, ProcedureSql);
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = handle, NpgsqlDbType = NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = since, NpgsqlDbType = NpgsqlDbType.Timestamp });
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
                procedure = string.Join('.', new[] { Text(reader, 0), Text(reader, 1), Text(reader, 2) }.Where(part => !string.IsNullOrEmpty(part)));
        }

        return (statements, procedure);
    }

    /// <summary>Every compile of the given statements collected after <paramref name="evictedAt"/> (a store time), within the after window.</summary>
    internal static async Task<IReadOnlyList<Row>> ReadAfterAsync(
        NpgsqlDataSource postgres, int serverId, IReadOnlyCollection<string> queryHashes, IReadOnlyCollection<string> sqlHandles, DateTime evictedAt, CancellationToken ct)
    {
        var rows = new List<Row>();
        if (queryHashes.Count == 0 || sqlHandles.Count == 0)
            return rows;
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await using var command = Command(connection, AfterSql);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. queryHashes], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. sqlHandles], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = Naive(evictedAt), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = Naive(evictedAt + AfterWindow), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = MaxAfterRows, NpgsqlDbType = NpgsqlDbType.Integer });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(ReadRow(reader));
        return rows;
    }
}
