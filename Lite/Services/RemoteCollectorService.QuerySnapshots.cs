/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    private const string StringConstantToFormat = """
        
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000;

SELECT /* PerformanceMonitorLite */
    der.session_id,
    database_name = DB_NAME(der.database_id),
    elapsed_time_formatted =
        CASE
            WHEN der.total_elapsed_time < 0
            THEN '00 00:00:00.000'
            ELSE RIGHT(REPLICATE('0', 2) + CONVERT(varchar(10), der.total_elapsed_time / 86400000), 2) +
                 ' ' + RIGHT(CONVERT(varchar(30), DATEADD(second, der.total_elapsed_time / 1000, 0), 120), 9) +
                 '.' + RIGHT('000' + CONVERT(varchar(3), der.total_elapsed_time % 1000), 3)
        END,
    query_text = SUBSTRING(dest.text, (der.statement_start_offset / 2) + 1,
        ((CASE der.statement_end_offset WHEN -1 THEN DATALENGTH(dest.text)
          ELSE der.statement_end_offset END - der.statement_start_offset) / 2) + 1),
    query_plan = TRY_CAST(deqp.query_plan AS nvarchar(max)),
    {0}
    der.status,
    der.blocking_session_id,
    der.wait_type,
    wait_time_ms = CONVERT(bigint, der.wait_time),
    der.wait_resource,
    cpu_time_ms = CONVERT(bigint, der.cpu_time),
    total_elapsed_time_ms = CONVERT(bigint, der.total_elapsed_time),
    der.reads,
    der.writes,
    der.logical_reads,
    granted_query_memory_gb = CONVERT(decimal(38, 2), (der.granted_query_memory / 128. / 1024.)),
    transaction_isolation_level =
        CASE der.transaction_isolation_level
            WHEN 0 THEN 'Unspecified'
            WHEN 1 THEN 'Read Uncommitted'
            WHEN 2 THEN 'Read Committed'
            WHEN 3 THEN 'Repeatable Read'
            WHEN 4 THEN 'Serializable'
            WHEN 5 THEN 'Snapshot'
            ELSE '???'
        END,
    der.dop,
    der.parallel_worker_count,
    des.login_name,
    des.host_name,
    des.program_name,
    des.open_transaction_count,
    der.percent_complete
FROM sys.dm_exec_requests AS der
JOIN sys.dm_exec_sessions AS des
    ON des.session_id = der.session_id
OUTER APPLY sys.dm_exec_sql_text(COALESCE(der.sql_handle, der.plan_handle)) AS dest
OUTER APPLY sys.dm_exec_text_query_plan(der.plan_handle, der.statement_start_offset, der.statement_end_offset) AS deqp
{1}
WHERE der.session_id <> @@SPID
AND   der.session_id >= 50
AND   dest.text IS NOT NULL
AND   der.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
ORDER BY der.cpu_time DESC, der.parallel_worker_count DESC
OPTION(MAXDOP 1, RECOMPILE);
""";
    private readonly static CompositeFormat QuerySnapshotsBase = CompositeFormat.Parse(StringConstantToFormat);

    /// <summary>
    /// Builds the query snapshots SQL with or without live query plan support.
    /// Used by both the collector and the live snapshot button.
    /// </summary>
    internal static string BuildQuerySnapshotsQuery(bool supportsLiveQueryPlan)
    {
        return supportsLiveQueryPlan
            ? string.Format(null, QuerySnapshotsBase, "live_query_plan = deqs.query_plan,", "OUTER APPLY sys.dm_exec_query_statistics_xml(der.session_id) AS deqs")
            : string.Format(null, QuerySnapshotsBase, "live_query_plan = CONVERT(xml, NULL),", "");
    }

    /// <summary>
    /// Collects currently running queries (point-in-time snapshot).
    /// </summary>
    private async Task<int> CollectQuerySnapshotsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        // dm_exec_query_statistics_xml requires SQL Server 2016 SP1+ (version 13)
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        var supportsLiveQueryPlan = serverStatus.SqlMajorVersion >= 13 || serverStatus.SqlMajorVersion == 0
            || serverStatus.SqlEngineEdition == 5 || serverStatus.SqlEngineEdition == 8;

        var query = BuildQuerySnapshotsQuery(supportsLiveQueryPlan);

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        sqlSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;

        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using var appender = duckConnection.CreateAppender("query_snapshots");
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = appender.CreateRow();
                row.AppendValue(GenerateCollectionId())
                   .AppendValue(collectionTime)
                   .AppendValue(serverId)
                   .AppendValue(server.ServerName)
                   .AppendValue(Convert.ToInt32(reader.GetValue(0)))                                       /* session_id */
                   .AppendValue(reader.IsDBNull(1) ? (string?)null : reader.GetString(1))                  /* database_name */
                   .AppendValue(reader.IsDBNull(2) ? (string?)null : reader.GetString(2))                  /* elapsed_time_formatted */
                   .AppendValue(reader.IsDBNull(3) ? (string?)null : reader.GetString(3))                  /* query_text */
                   .AppendValue(reader.IsDBNull(4) ? (string?)null : reader.GetString(4))                  /* query_plan */
                   .AppendValue(reader.IsDBNull(5) ? (string?)null : reader.GetValue(5)?.ToString())       /* live_query_plan (xml) */
                   .AppendValue(reader.IsDBNull(6) ? (string?)null : reader.GetString(6))                  /* status */
                   .AppendValue(reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)))              /* blocking_session_id */
                   .AppendValue(reader.IsDBNull(8) ? (string?)null : reader.GetString(8))                  /* wait_type */
                   .AppendValue(reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)))             /* wait_time_ms */
                   .AppendValue(reader.IsDBNull(10) ? (string?)null : reader.GetString(10))                /* wait_resource */
                   .AppendValue(reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)))           /* cpu_time_ms */
                   .AppendValue(reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)))           /* total_elapsed_time_ms */
                   .AppendValue(reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)))           /* reads */
                   .AppendValue(reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)))           /* writes */
                   .AppendValue(reader.IsDBNull(15) ? 0L : Convert.ToInt64(reader.GetValue(15)))           /* logical_reads */
                   .AppendValue(reader.IsDBNull(16) ? 0m : reader.GetDecimal(16))                            /* granted_query_memory_gb */
                   .AppendValue(reader.IsDBNull(17) ? (string?)null : reader.GetString(17))                /* transaction_isolation_level */
                   .AppendValue(reader.IsDBNull(18) ? 0 : Convert.ToInt32(reader.GetValue(18)))            /* dop */
                   .AppendValue(reader.IsDBNull(19) ? 0 : Convert.ToInt32(reader.GetValue(19)))            /* parallel_worker_count */
                   .AppendValue(reader.IsDBNull(20) ? (string?)null : reader.GetString(20))                /* login_name */
                   .AppendValue(reader.IsDBNull(21) ? (string?)null : reader.GetString(21))                /* host_name */
                   .AppendValue(reader.IsDBNull(22) ? (string?)null : reader.GetString(22))                /* program_name */
                   .AppendValue(reader.IsDBNull(23) ? 0 : Convert.ToInt32(reader.GetValue(23)))            /* open_transaction_count */
                   .AppendValue(reader.IsDBNull(24) ? 0m : Convert.ToDecimal(reader.GetValue(24)))        /* percent_complete */
                   .EndRow();

                rowsCollected++;
            }
        }

        duckSw.Stop();
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} query snapshots for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
