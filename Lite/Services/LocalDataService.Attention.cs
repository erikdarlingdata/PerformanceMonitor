/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The Client Timeouts alert's window read (#5640), the DuckDB twin of <c>DarlingAlertReadAdapter.GetAttentionWindowAsync</c>:
    /// the same five statements from the one shared <see cref="AttentionWindowSql"/> text over <c>v_collection_log</c> and
    /// <c>v_attention_events</c> (the archive views, so a floor that reaches into parquet still reads). $1 server_id, $2 the
    /// naive-UTC floor, bound as a full-precision <see cref="DateTime"/> like the Runnable Queue read. Dumb by contract: no
    /// span, threshold or rule is applied here.
    /// </summary>
    public async Task<AttentionWindow> GetAttentionWindowAsync(int serverId, int lookbackMinutes)
    {
        using var connection = await OpenConnectionAsync();
        var floor = DateTime.UtcNow.AddMinutes(-lookbackMinutes);

        var runs = await QueryAttentionAsync(connection, AttentionWindowSql.Runs("v_collection_log"), serverId, floor,
            r => r.GetDateTime(0));
        var counts = await QueryAttentionAsync(connection, AttentionWindowSql.Counts("v_attention_events"), serverId, floor,
            r => new AttentionCountRow(r.GetDateTime(0), r.GetInt64(1)));
        var groups = await QueryAttentionAsync(connection, AttentionWindowSql.SampleGroups("v_attention_events"), serverId, floor,
            r => new AttentionSampleGroup(
                r.GetDateTime(0),
                r.IsDBNull(1) ? null : r.GetString(1),
                r.IsDBNull(2) ? null : r.GetString(2),
                r.GetInt64(3)));
        var statements = await QueryAttentionAsync(connection, AttentionWindowSql.TopStatements("v_attention_events"), serverId, floor,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1), r.IsDBNull(2) ? null : r.GetString(2)));
        var databases = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "database_name"), serverId, floor,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
        var applications = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "client_app_name"), serverId, floor,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));

        return new AttentionWindow(runs, counts, groups, statements, databases, applications);
    }

    private static async Task<List<T>> QueryAttentionAsync<T>(
        PerformanceMonitorLite.Database.LockedConnection connection, string sql, int serverId, DateTime floor, Func<DbDataReader, T> map)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = floor });

        var rows = new List<T>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(map(reader));
        }

        return rows;
    }
}
