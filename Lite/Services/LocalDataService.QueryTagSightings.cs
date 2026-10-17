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

/// <summary>One row of <c>query_tag_sightings</c>, as <c>test_query_tag_rule</c> reads it.</summary>
public sealed record QueryTagSightingRow(
    int ServerId, string KeyKind, string Key, string? DatabaseName, string? ObjectName,
    string? ApplicationName, string? LoginName, string? HostName, string? TextSample, DateTime LastSeen);

public partial class LocalDataService
{
    /// <summary>
    /// The sightings seen within the last <paramref name="windowHours"/> hours (one server, or every server when
    /// <paramref name="serverId"/> is null), newest first, capped at <paramref name="maxRows"/>. Read-only; the sightings
    /// write itself belongs to the map pass (#5990).
    /// </summary>
    public async Task<List<QueryTagSightingRow>> GetQueryTagSightingsAsync(int? serverId, int windowHours, int maxRows)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, last_seen
FROM query_tag_sightings
WHERE last_seen >= now()::TIMESTAMP - to_hours($1::BIGINT)
AND   ($2 IS NULL OR server_id = $2)
ORDER BY last_seen DESC, server_id, key_kind, key
LIMIT $3";
        command.Parameters.Add(new DuckDBParameter { Value = (long)windowHours });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)serverId ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (long)maxRows });

        var rows = new List<QueryTagSightingRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            string? Text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
            rows.Add(new QueryTagSightingRow(
                reader.GetInt32(0), reader.GetString(1), reader.GetString(2),
                Text(3), Text(4), Text(5), Text(6), Text(7), Text(8), reader.GetDateTime(9)));
        }

        return rows;
    }
}
