/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store read behind <c>get_slow_reads</c> (#5097): the newest recorded slow reads in a window and a count per
/// (surface, route, outcome). Both ride <c>idx_slow_reads_time</c> and carry the same filters.
/// </summary>
internal static class DarlingSlowReadReader
{
    internal sealed record SlowReadRow(
        long SlowReadId,
        DateTime ReadTime,
        string Surface,
        string Route,
        string Outcome,
        int TotalMs,
        int? ServerId,
        string? ServerName,
        DateTime? WindowStart,
        DateTime? WindowEnd,
        JsonNode? Arguments,
        bool ArgumentsTruncated,
        string? Source,
        string? SourceReason,
        JsonArray Statements,
        int StatementCount,
        bool StatementsTruncated,
        string? ErrorClass);

    internal sealed record SummaryRow(string Surface, string Route, string Outcome, long Count);

    internal sealed record Page(IReadOnlyList<SlowReadRow> Reads, IReadOnlyList<SummaryRow> Summary);

    private static string Where(int? serverId, string? surface, string? route)
    {
        var sql = "read_time >= $1";
        var next = 2;
        if (serverId is not null)
        {
            sql += $" AND r.server_id = ${next++}";
        }

        if (surface is not null)
        {
            sql += $" AND r.surface = ${next++}";
        }

        if (route is not null)
        {
            sql += $" AND r.route = ${next}";
        }

        return sql;
    }

    internal static async Task<Page> GetAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, int? serverId, string? surface, string? route, int limit,
        CancellationToken cancellationToken = default)
    {
        var normalizedSurface = string.IsNullOrWhiteSpace(surface) ? null : surface.Trim().ToLowerInvariant();
        var normalizedRoute = string.IsNullOrWhiteSpace(route) ? null : route.Trim();
        var where = Where(serverId, normalizedSurface, normalizedRoute);

        void Bind(NpgsqlCommand command)
        {
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified));
            if (serverId is not null)
            {
                command.Parameters.AddWithValue(serverId.Value);
            }

            if (normalizedSurface is not null)
            {
                command.Parameters.AddWithValue(normalizedSurface);
            }

            if (normalizedRoute is not null)
            {
                command.Parameters.AddWithValue(normalizedRoute);
            }
        }

        var summarySql = $@"
SELECT r.surface, r.route, r.outcome, count(*)::bigint
FROM collect.slow_reads AS r
WHERE {where}
GROUP BY r.surface, r.route, r.outcome
ORDER BY count(*) DESC, r.surface, r.route, r.outcome";

        var readsSql = $@"
SELECT r.slow_read_id, r.read_time, r.surface, r.route, r.outcome, r.total_ms, r.server_id, s.server_name,
       r.window_start, r.window_end, r.arguments::text, r.arguments_truncated, r.source, r.source_reason,
       r.statements::text, r.statement_count, r.statements_truncated, r.error_class
FROM collect.slow_reads AS r
LEFT JOIN servers AS s ON s.server_id = r.server_id
WHERE {where}
ORDER BY r.read_time DESC, r.slow_read_id DESC
LIMIT {limit.ToString(System.Globalization.CultureInfo.InvariantCulture)}";

        var summary = new List<SummaryRow>();
        await using (var command = postgres.CreateCommand(summarySql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            Bind(command);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                summary.Add(new SummaryRow(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt64(3)));
            }
        }

        var reads = new List<SlowReadRow>();
        await using (var command = postgres.CreateCommand(readsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            Bind(command);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                reads.Add(new SlowReadRow(
                    reader.GetInt64(0),
                    reader.GetDateTime(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetInt32(5),
                    reader.IsDBNull(6) ? null : reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                    reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                    JsonNode.Parse(reader.GetString(10)),
                    reader.GetBoolean(11),
                    reader.IsDBNull(12) ? null : reader.GetString(12),
                    reader.IsDBNull(13) ? null : reader.GetString(13),
                    JsonNode.Parse(reader.GetString(14)) as JsonArray ?? new JsonArray(),
                    reader.GetInt32(15),
                    reader.GetBoolean(16),
                    reader.IsDBNull(17) ? null : reader.GetString(17)));
            }
        }

        return new Page(reads, summary);
    }
}
