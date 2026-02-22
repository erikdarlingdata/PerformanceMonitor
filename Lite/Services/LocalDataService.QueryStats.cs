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
    /// Validates that a database name exists on the server and returns the properly quoted name.
    /// This prevents SQL injection via malicious database names.
    /// </summary>
    private static async Task<string?> GetValidatedDatabaseNameAsync(SqlConnection connection, string databaseName)
    {
        using var command = new SqlCommand(@"
SELECT
    quoted_name = QUOTENAME(d.name)
FROM sys.databases AS d
WHERE d.name = @database_name;", connection);
        command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Gets top queries by CPU for a server over a time period.
    /// </summary>
    public async Task<List<QueryStatsRow>> GetTopQueriesByCpuAsync(int serverId, int hoursBack = 24, int top = 50, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    database_name,
    query_hash,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_us,
    SUM(delta_elapsed_time) AS total_elapsed_us,
    SUM(delta_logical_reads) AS total_reads,
    SUM(delta_rows) AS total_rows,
    SUM(delta_logical_writes) AS total_writes,
    SUM(delta_physical_reads) AS total_physical_reads,
    SUM(delta_spills) AS total_spills,
    MIN(min_dop) AS min_dop,
    MAX(max_dop) AS max_dop,
    MIN(min_worker_time) AS min_worker_time,
    MAX(max_worker_time) AS max_worker_time,
    MIN(min_elapsed_time) AS min_elapsed_time,
    MAX(max_elapsed_time) AS max_elapsed_time,
    MAX(query_plan_hash) AS query_plan_hash,
    MAX(sql_handle) AS sql_handle,
    MAX(plan_handle) AS plan_handle,
    MAX(query_text) AS query_text,
    MAX(query_plan_xml) AS query_plan
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_text NOT LIKE 'WAITFOR%'
GROUP BY database_name, query_hash
HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
ORDER BY SUM(delta_elapsed_time) DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = top });

        var items = new List<QueryStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStatsRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalExecutions = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                TotalCpuUs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                TotalElapsedUs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TotalLogicalReads = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalRows = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                TotalLogicalWrites = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                TotalPhysicalReads = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                TotalSpills = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                MinDop = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                MaxDop = reader.IsDBNull(11) ? 0 : reader.GetInt32(11),
                MinCpuUs = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                MaxCpuUs = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                MinElapsedUs = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                MaxElapsedUs = reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                QueryPlanHash = reader.IsDBNull(16) ? "" : reader.GetString(16),
                SqlHandle = reader.IsDBNull(17) ? "" : reader.GetString(17),
                PlanHandle = reader.IsDBNull(18) ? "" : reader.GetString(18),
                QueryText = reader.IsDBNull(19) ? "" : reader.GetString(19),
                QueryPlan = reader.IsDBNull(20) ? null : reader.GetString(20)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets collection-level history for a specific query hash (for drilldown).
    /// </summary>
    public async Task<List<QueryStatsHistoryRow>> GetQueryStatsHistoryAsync(int serverId, string databaseName, string queryHash, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collection_time,
    delta_execution_count,
    delta_worker_time,
    delta_elapsed_time,
    delta_logical_reads,
    delta_logical_writes,
    delta_physical_reads,
    delta_rows,
    delta_spills,
    min_dop,
    max_dop,
    min_worker_time,
    max_worker_time,
    min_elapsed_time,
    max_elapsed_time,
    query_plan_xml,
    query_plan_hash
FROM v_query_stats
WHERE server_id = $1
AND   database_name = $2
AND   query_hash = $3
AND   collection_time >= $4
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = queryHash });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hoursBack) });

        var items = new List<QueryStatsHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStatsHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeltaExecutions = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                DeltaCpuUs = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                DeltaElapsedUs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DeltaLogicalReads = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                DeltaLogicalWrites = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DeltaPhysicalReads = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                DeltaRows = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                DeltaSpills = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                MinDop = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                MaxDop = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                MinCpuUs = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                MaxCpuUs = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                MinElapsedUs = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                MaxElapsedUs = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                QueryPlan = reader.IsDBNull(15) ? null : reader.GetString(15),
                QueryPlanHash = reader.IsDBNull(16) ? "" : reader.GetString(16)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets collection-level history for a specific procedure (for drilldown).
    /// </summary>
    public async Task<List<ProcedureStatsHistoryRow>> GetProcedureStatsHistoryAsync(int serverId, string databaseName, string schemaName, string objectName, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collection_time,
    delta_execution_count,
    delta_worker_time,
    delta_elapsed_time,
    delta_logical_reads,
    delta_logical_writes,
    delta_physical_reads
FROM v_procedure_stats
WHERE server_id = $1
AND   database_name = $2
AND   schema_name = $3
AND   object_name = $4
AND   collection_time >= $5
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = schemaName });
        command.Parameters.Add(new DuckDBParameter { Value = objectName });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hoursBack) });

        var items = new List<ProcedureStatsHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ProcedureStatsHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeltaExecutions = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                DeltaCpuUs = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                DeltaElapsedUs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DeltaLogicalReads = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                DeltaLogicalWrites = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DeltaPhysicalReads = reader.IsDBNull(6) ? 0 : reader.GetInt64(6)
            });
        }

        return items;
    }

    /// <summary>
    /// Looks up a cached query plan from DuckDB by server_id and query_hash.
    /// Returns the most recently collected plan XML, or null if not found.
    /// </summary>
    public async Task<string?> GetCachedQueryPlanAsync(int serverId, string queryHash)
    {
        const string query = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   query_hash = $2
AND   query_plan_xml IS NOT NULL
AND   query_plan_xml <> ''
ORDER BY collection_time DESC
LIMIT 1";

        using var connection = await OpenConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = queryHash });
        var result = await cmd.ExecuteScalarAsync();
        return result as string;
    }

    public async Task<string?> GetCachedProcedurePlanAsync(int serverId, string planHandle)
    {
        const string query = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   plan_handle = $2
AND   query_plan_xml IS NOT NULL
AND   query_plan_xml <> ''
ORDER BY collection_time DESC
LIMIT 1";

        using var connection = await OpenConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = planHandle });
        var result = await cmd.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Fetches a query plan on-demand from the remote server by query hash.
    /// </summary>
    public static async Task<string?> FetchQueryPlanOnDemandAsync(string connectionString, string queryHash)
    {
        const string query = @"
SELECT TOP (1)
    query_plan_text = tqp.query_plan
FROM sys.dm_exec_query_stats AS qs
OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) AS tqp
WHERE CONVERT(varchar(64), qs.query_hash, 1) = @query_hash
AND   tqp.query_plan IS NOT NULL
ORDER BY
    qs.total_elapsed_time DESC
OPTION(RECOMPILE);";

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@query_hash", SqlDbType.VarChar, 64) { Value = queryHash });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Fetches a procedure plan on-demand from the remote server by object name.
    /// Uses three-part naming with sp_executesql for Azure SQL DB compatibility.
    /// </summary>
    public static async Task<string?> FetchProcedurePlanOnDemandAsync(string connectionString, string databaseName, string schemaName, string objectName)
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
    query_plan_text = tqp.query_plan
FROM sys.dm_exec_procedure_stats AS ps
OUTER APPLY sys.dm_exec_text_query_plan(ps.plan_handle, 0, -1) AS tqp
WHERE ps.database_id = DB_ID()
AND   OBJECT_NAME(ps.object_id, ps.database_id) COLLATE DATABASE_DEFAULT = @object_name COLLATE DATABASE_DEFAULT
AND   OBJECT_SCHEMA_NAME(ps.object_id, ps.database_id) COLLATE DATABASE_DEFAULT = @schema_name COLLATE DATABASE_DEFAULT
AND   tqp.query_plan IS NOT NULL
ORDER BY
    ps.total_elapsed_time DESC
OPTION(RECOMPILE);',
    N'@object_name sysname, @schema_name sysname',
    @object_name,
    @schema_name;";

        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@object_name", SqlDbType.NVarChar, 128) { Value = objectName });
        command.Parameters.Add(new SqlParameter("@schema_name", SqlDbType.NVarChar, 128) { Value = schemaName });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Gets top procedures by CPU for a server.
    /// </summary>
    public async Task<List<ProcedureStatsRow>> GetTopProceduresByCpuAsync(int serverId, int hoursBack = 24, int top = 50, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    database_name,
    schema_name,
    object_name,
    object_type,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_us,
    SUM(delta_elapsed_time) AS total_elapsed_us,
    SUM(delta_logical_reads) AS total_reads,
    SUM(delta_logical_writes) AS total_writes,
    SUM(delta_physical_reads) AS total_physical_reads,
    MIN(min_worker_time) AS min_worker_time,
    MAX(max_worker_time) AS max_worker_time,
    MIN(min_elapsed_time) AS min_elapsed_time,
    MAX(max_elapsed_time) AS max_elapsed_time,
    SUM(total_spills) AS total_spills,
    MAX(sql_handle) AS sql_handle,
    MAX(plan_handle) AS plan_handle
FROM v_procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY database_name, schema_name, object_name, object_type
HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
ORDER BY SUM(delta_elapsed_time) DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = top });

        var items = new List<ProcedureStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ProcedureStatsRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ObjectName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ObjectType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TotalExecutions = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TotalCpuUs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalElapsedUs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                TotalLogicalReads = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                TotalLogicalWrites = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                TotalPhysicalReads = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                MinWorkerTimeUs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                MaxWorkerTimeUs = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                MinElapsedTimeUs = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                MaxElapsedTimeUs = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                TotalSpills = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                SqlHandle = reader.IsDBNull(15) ? "" : reader.GetString(15),
                PlanHandle = reader.IsDBNull(16) ? "" : reader.GetString(16)
            });
        }

        return items;
    }
    /// <summary>
    /// Gets query duration trend — total elapsed time per collection snapshot.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetQueryDurationTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
        SUM(delta_execution_count) AS total_executions,
        date_diff('second', LAG(collection_time) OVER (ORDER BY collection_time), collection_time) AS interval_seconds
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
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

    /// <summary>
    /// Gets procedure duration trend — elapsed time per second per collection snapshot.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetProcedureDurationTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
        SUM(delta_execution_count) AS total_executions,
        date_diff('second', LAG(collection_time) OVER (ORDER BY collection_time), collection_time) AS interval_seconds
    FROM v_procedure_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
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

    /// <summary>
    /// Gets execution count trend — executions per second per collection snapshot from query_stats.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetExecutionCountTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(delta_execution_count) AS total_executions,
        date_diff('second', LAG(collection_time) OVER (ORDER BY collection_time), collection_time) AS interval_seconds
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
)
SELECT
    collection_time,
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
                Value = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1))
            });
        }
        return items;
    }
}

public class QueryTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double Value { get; set; }
    public long ExecutionCount { get; set; }
}

public class QueryStatsRow
{
    public string DatabaseName { get; set; } = "";
    public string QueryHash { get; set; } = "";
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalRows { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long TotalSpills { get; set; }
    public int MinDop { get; set; }
    public int MaxDop { get; set; }
    public long MinCpuUs { get; set; }
    public long MaxCpuUs { get; set; }
    public long MinElapsedUs { get; set; }
    public long MaxElapsedUs { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string? QueryPlan { get; set; }
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlan);
    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public double AvgCpuMs => TotalExecutions > 0 ? TotalCpuMs / TotalExecutions : 0;
    public double AvgElapsedMs => TotalExecutions > 0 ? TotalElapsedMs / TotalExecutions : 0;
    public double AvgReads => TotalExecutions > 0 ? (double)TotalLogicalReads / TotalExecutions : 0;
    public double MinCpuMs => MinCpuUs / 1000.0;
    public double MaxCpuMs => MaxCpuUs / 1000.0;
    public double MinElapsedMs => MinElapsedUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedUs / 1000.0;
}

public class ProcedureStatsRow
{
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string ObjectName { get; set; } = "";
    public string ObjectType { get; set; } = "";
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long MinWorkerTimeUs { get; set; }
    public long MaxWorkerTimeUs { get; set; }
    public long MinElapsedTimeUs { get; set; }
    public long MaxElapsedTimeUs { get; set; }
    public long TotalSpills { get; set; }
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public string FullName => string.IsNullOrEmpty(SchemaName) ? ObjectName : $"{SchemaName}.{ObjectName}";
    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public double AvgCpuMs => TotalExecutions > 0 ? TotalCpuMs / TotalExecutions : 0;
    public double AvgElapsedMs => TotalExecutions > 0 ? TotalElapsedMs / TotalExecutions : 0;
    public double AvgReads => TotalExecutions > 0 ? (double)TotalLogicalReads / TotalExecutions : 0;
    public double MinCpuMs => MinWorkerTimeUs / 1000.0;
    public double MaxCpuMs => MaxWorkerTimeUs / 1000.0;
    public double MinElapsedMs => MinElapsedTimeUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedTimeUs / 1000.0;
}

public class QueryStatsHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long DeltaExecutions { get; set; }
    public long DeltaCpuUs { get; set; }
    public long DeltaElapsedUs { get; set; }
    public long DeltaLogicalReads { get; set; }
    public long DeltaLogicalWrites { get; set; }
    public long DeltaPhysicalReads { get; set; }
    public long DeltaRows { get; set; }
    public long DeltaSpills { get; set; }
    public int MinDop { get; set; }
    public int MaxDop { get; set; }
    public long MinCpuUs { get; set; }
    public long MaxCpuUs { get; set; }
    public long MinElapsedUs { get; set; }
    public long MaxElapsedUs { get; set; }
    public string? QueryPlan { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlan);
    public double DeltaCpuMs => DeltaCpuUs / 1000.0;
    public double DeltaElapsedMs => DeltaElapsedUs / 1000.0;
    public double AvgCpuMs => DeltaExecutions > 0 ? DeltaCpuMs / DeltaExecutions : 0;
    public double AvgElapsedMs => DeltaExecutions > 0 ? DeltaElapsedMs / DeltaExecutions : 0;
    public double AvgReads => DeltaExecutions > 0 ? (double)DeltaLogicalReads / DeltaExecutions : 0;
    public double MinCpuMs => MinCpuUs / 1000.0;
    public double MaxCpuMs => MaxCpuUs / 1000.0;
    public double MinElapsedMs => MinElapsedUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedUs / 1000.0;
    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
}

public class ProcedureStatsHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long DeltaExecutions { get; set; }
    public long DeltaCpuUs { get; set; }
    public long DeltaElapsedUs { get; set; }
    public long DeltaLogicalReads { get; set; }
    public long DeltaLogicalWrites { get; set; }
    public long DeltaPhysicalReads { get; set; }
    public double DeltaCpuMs => DeltaCpuUs / 1000.0;
    public double DeltaElapsedMs => DeltaElapsedUs / 1000.0;
    public double AvgCpuMs => DeltaExecutions > 0 ? DeltaCpuMs / DeltaExecutions : 0;
    public double AvgElapsedMs => DeltaExecutions > 0 ? DeltaElapsedMs / DeltaExecutions : 0;
    public double AvgReads => DeltaExecutions > 0 ? (double)DeltaLogicalReads / DeltaExecutions : 0;
    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
}
