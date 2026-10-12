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
using NpgsqlTypes;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store-only repro script behind get_query_repro_script (#5233): the same pure
/// <see cref="ReproScriptBuilder.BuildReproScript"/> call the Darling Viewer's "Copy Repro Script" makes
/// (<c>ViewerServerTab.BuildReproScriptForRow</c>), fed from the stored query text and plan. Nothing here
/// connects to the monitored server and nothing executes. Like the Viewer, it never passes
/// <c>isAzureSqlDb</c>, so the script is the SQL Server form.
/// </summary>
internal static class DarlingReproScript
{
    internal const string ProductName = "SQL Server Performance Monitor";

    internal const string KindQueryHash = "query_hash";
    internal const string KindQueryStore = "query_store";
    internal const string KindActiveSnapshot = "active_snapshot";

    /// <summary>The Viewer's Top Queries lookup (<c>ViewerDataService.QueryStats</c>), with the optional database
    /// guard the plan read by hash uses. $1 server_id, $2 query_hash, $3 database_name (NULL = any).</summary>
    internal const string QueryStatsTextSql = """
        SELECT query_text
        FROM v_query_stats
        WHERE server_id = $1
        AND   query_hash = $2
        AND   ($3::text IS NULL OR database_name = $3)
        AND   query_text IS NOT NULL
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>The Query Store text, collector table first and the newest inline text on the fact row second
    /// (the #2150 order the Query Store grid reads). $1 server_id, $2 database_name, $3 query_id.</summary>
    internal const string QueryStoreTextSql = """
        SELECT COALESCE(
                   (
                       SELECT x.query_sql_text
                       FROM query_store_text AS x
                       WHERE x.server_id = $1
                       AND   x.database_name = $2
                       AND   x.query_id = $3
                   ),
                   (
                       SELECT s.query_text
                       FROM query_store_stats AS s
                       WHERE s.server_id = $1
                       AND   s.database_name = $2
                       AND   s.query_id = $3
                       AND   s.query_text IS NOT NULL
                       ORDER BY s.collection_time DESC
                       LIMIT 1
                   )
               )
        """;

    /// <summary>One Active Queries snapshot row: text, isolation level and database, by the row's natural key.
    /// $1 server_id, $2 collection_time, $3 session_id, $4 request_id.</summary>
    internal const string SnapshotTextSql = """
        SELECT query_text, transaction_isolation_level, database_name
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time = $2
        AND   session_id = $3
        AND   COALESCE(request_id, 0) = $4
        LIMIT 1
        """;

    /// <summary>Builds the script for a kind from its stored fields; null when there is no query text. Only an
    /// Active Queries row carries an isolation level, as in the Viewer.</summary>
    internal static string? Build(string kind, string? queryText, string? db, string? planXml, string? isolation)
    {
        if (string.IsNullOrEmpty(queryText)) return null;
        return kind switch
        {
            KindQueryHash => ReproScriptBuilder.BuildReproScript(
                queryText, db, planXml, null, "Top Queries (dm_exec_query_stats)", productName: ProductName),
            KindQueryStore => ReproScriptBuilder.BuildReproScript(
                queryText, db, planXml, null, "Query Store", productName: ProductName),
            KindActiveSnapshot => ReproScriptBuilder.BuildReproScript(
                queryText, db, planXml, isolation, "Active Queries", productName: ProductName),
            _ => null,
        };
    }

    internal static async Task<string?> ReadQueryStatsTextAsync(
        NpgsqlDataSource postgres, int serverId, string queryHash, string? databaseName, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(QueryStatsTextSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queryHash });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)databaseName ?? DBNull.Value });
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    internal static async Task<string?> ReadQueryStoreTextAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, long queryId, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(QueryStoreTextSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId });
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>The snapshot's (query_text, isolation, database), or nulls when no such row is stored.
    /// <paramref name="collectionTimeUtc"/> is relabelled Unspecified so it binds as <c>timestamp</c> (#1969).</summary>
    internal static async Task<(string? Text, string? Isolation, string? Database)> ReadSnapshotTextAsync(
        NpgsqlDataSource postgres, int serverId, DateTime collectionTimeUtc, int sessionId, int requestId, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(SnapshotTextSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sessionId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = requestId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return (null, null, null);
        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2));
    }
}
