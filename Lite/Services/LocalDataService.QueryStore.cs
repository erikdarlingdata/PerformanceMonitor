/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest Query Store snapshot for a server, aggregated across all databases.
    /// Shows top queries by total duration (execution_count * avg_duration_ms).
    /// </summary>
    public async Task<List<QueryStoreRow>> GetQueryStoreTopQueriesAsync(int serverId, int hoursBack = 24, int top = 50, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    database_name,
    query_id,
    plan_id,
    query_hash,
    MAX(query_text) AS query_text,
    SUM(execution_count) AS total_executions,
    AVG(avg_duration_ms) AS avg_duration_ms,
    AVG(avg_cpu_time_ms) AS avg_cpu_time_ms,
    AVG(avg_logical_reads) AS avg_logical_reads,
    AVG(avg_logical_writes) AS avg_logical_writes,
    AVG(avg_physical_reads) AS avg_physical_reads,
    AVG(avg_rowcount) AS avg_rowcount,
    MAX(last_execution_time) AS last_execution_time,
    MAX(query_plan_hash) AS query_plan_hash
FROM v_query_store_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_text NOT LIKE 'WAITFOR%'
GROUP BY database_name, query_id, plan_id, query_hash
ORDER BY SUM(execution_count) * AVG(avg_duration_ms) DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = top });

        var items = new List<QueryStoreRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStoreRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                PlanId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                QueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
                QueryText = reader.IsDBNull(4) ? "" : reader.GetString(4),
                TotalExecutions = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                AvgDurationMs = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                AvgCpuTimeMs = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                AvgLogicalReads = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                AvgLogicalWrites = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                AvgPhysicalReads = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10)),
                AvgRowcount = reader.IsDBNull(11) ? 0 : ToDouble(reader.GetValue(11)),
                LastExecutionTime = reader.IsDBNull(12) ? (DateTime?)null : reader.GetDateTime(12),
                QueryPlanHash = reader.IsDBNull(13) ? "" : reader.GetString(13)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets collection-level history for a specific Query Store query (for drilldown).
    /// </summary>
    public async Task<List<QueryStoreHistoryRow>> GetQueryStoreHistoryAsync(int serverId, string databaseName, long queryId, long planId, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collection_time,
    execution_count,
    avg_duration_ms,
    avg_cpu_time_ms,
    avg_logical_reads,
    avg_logical_writes,
    avg_physical_reads,
    avg_rowcount,
    last_execution_time
FROM v_query_store_stats
WHERE server_id = $1
AND   database_name = $2
AND   query_id = $3
AND   plan_id = $4
AND   collection_time >= $5
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = queryId });
        command.Parameters.Add(new DuckDBParameter { Value = planId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hoursBack) });

        var items = new List<QueryStoreHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStoreHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                ExecutionCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                AvgDurationMs = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                AvgCpuTimeMs = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                AvgLogicalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                AvgLogicalWrites = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvgPhysicalReads = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                AvgRowcount = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                LastExecutionTime = reader.IsDBNull(8) ? (DateTime?)null : reader.GetDateTime(8)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets distinct databases that have Query Store data collected.
    /// </summary>
    public async Task<List<string>> GetQueryStoreDatabasesAsync(int serverId, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT DISTINCT database_name
FROM v_query_store_stats
WHERE server_id = $1
AND   collection_time >= $2
ORDER BY database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hoursBack) });

        var items = new List<string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(reader.GetString(0));
        }
        return items;
    }

    /// <summary>
    /// Fetches a query plan on-demand from Query Store by plan_id.
    /// Uses three-part naming with sp_executesql for Azure SQL DB compatibility.
    /// </summary>
    public static async Task<string?> FetchQueryStorePlanAsync(string connectionString, string databaseName, long planId)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        /*
        Validate the database name exists and get the QUOTENAME'd version
        to prevent SQL injection via malicious database names
        */
        var quotedDbName = await GetValidatedDatabaseNameAsync(connection, databaseName);
        if (quotedDbName == null)
        {
            return null;
        }

        /*
        Use three-part naming (database.sys.sp_executesql) instead of USE statement
        for Azure SQL DB compatibility
        */
        var query = $@"
EXECUTE {quotedDbName}.sys.sp_executesql
    N'
SELECT TOP (1)
    query_plan = CONVERT(nvarchar(max), qsp.query_plan)
FROM sys.query_store_plan AS qsp
WHERE qsp.plan_id = @plan_id
AND   qsp.query_plan IS NOT NULL
OPTION(RECOMPILE);',
    N'@plan_id bigint',
    @plan_id;";

        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }
    /// <summary>
    /// Gets Query Store duration trend — SUM(execution_count * avg_duration_ms) per collection snapshot.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetQueryStoreDurationTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(execution_count * avg_duration_ms) AS total_duration_ms,
        SUM(execution_count) AS total_executions,
        date_diff('second', LAG(collection_time) OVER (ORDER BY collection_time), collection_time) AS interval_seconds
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN total_duration_ms / interval_seconds ELSE 0 END AS duration_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE) / interval_seconds ELSE 0 END AS executions_per_second
FROM raw
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<QueryTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : (long)ToDouble(reader.GetValue(2))
            });
        }
        return items;
    }
}

public class QueryStoreRow
{
    public string DatabaseName { get; set; } = "";
    public long QueryId { get; set; }
    public long PlanId { get; set; }
    public string QueryHash { get; set; } = "";
    public string QueryText { get; set; } = "";
    public long TotalExecutions { get; set; }
    public double AvgDurationMs { get; set; }
    public double AvgCpuTimeMs { get; set; }
    public double AvgLogicalReads { get; set; }
    public double AvgLogicalWrites { get; set; }
    public double AvgPhysicalReads { get; set; }
    public double AvgRowcount { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public string LastExecutionTimeLocal => ServerTimeHelper.FormatServerTime(LastExecutionTime);
    public double TotalCpuMs => TotalExecutions * AvgCpuTimeMs;
    public double TotalDurationMs => TotalExecutions * AvgDurationMs;
}

public class QueryStoreHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long ExecutionCount { get; set; }
    public double AvgDurationMs { get; set; }
    public double AvgCpuTimeMs { get; set; }
    public double AvgLogicalReads { get; set; }
    public double AvgLogicalWrites { get; set; }
    public double AvgPhysicalReads { get; set; }
    public double AvgRowcount { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public double TotalDurationMs => ExecutionCount * AvgDurationMs;
    public double TotalCpuMs => ExecutionCount * AvgCpuTimeMs;
    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
    public string LastExecutionTimeLocal => ServerTimeHelper.FormatServerTime(LastExecutionTime);
}
