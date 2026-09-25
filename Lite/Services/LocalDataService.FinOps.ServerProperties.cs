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
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Inventory facts: edition, product version/level, storage, HADR/cluster flags, host OS, and AG
    /// replica role. Every value comes from SERVERPROPERTY scalars or OBJECT_ID-guarded dynamic SQL
    /// that needs NO special permission, so this succeeds for any connected login — even an Azure SQL
    /// DB login lacking VIEW DATABASE STATE. The five hardware columns (reader indices 4, 5, 6, 8, 9)
    /// are typed-NULL placeholders here, filled by a separate <see cref="HardwareQueryText"/> read;
    /// keeping them preserves the reader column order. sys.dm_os_sys_info is deliberately NOT
    /// referenced so a permission gap can never sink the whole inventory row (#1535).
    /// Columns: 0 edition, 1 product_version, 2 product_level, 3 product_update_level, 4 cpu_count,
    /// 5 physical_memory_mb, 6 sqlserver_start_time, 7 storage_gb, 8 socket_count, 9 cores_per_socket,
    /// 10 engine_edition, 11 is_hadr_enabled, 12 is_clustered, 13 host_os, 14 ag_replica_role.
    /// </summary>
    internal const string InventoryQueryText = @"
DECLARE
    @storage_sql nvarchar(max) =
        CASE
            WHEN CONVERT(integer, SERVERPROPERTY('EngineEdition')) = 5
            THEN N'SELECT @gb = SUM(CAST(size AS bigint)) * 8.0 / 1024.0 / 1024.0 FROM sys.database_files'
            ELSE N'SELECT @gb = SUM(CAST(size AS bigint)) * 8.0 / 1024.0 / 1024.0 FROM sys.master_files'
        END,
    @storage_gb decimal(19,2),
    @host_os nvarchar(256),
    @ag_role nvarchar(20) = N'Standalone';

EXEC sys.sp_executesql @storage_sql, N'@gb decimal(19,2) OUTPUT', @gb = @storage_gb OUTPUT;

IF OBJECT_ID(N'sys.dm_os_host_info', N'V') IS NOT NULL
    EXEC sys.sp_executesql N'SELECT @os = host_distribution FROM sys.dm_os_host_info',
        N'@os nvarchar(256) OUTPUT', @os = @host_os OUTPUT;

IF @host_os IS NULL
BEGIN
    DECLARE @ver nvarchar(4000) = @@VERSION;
    DECLARE @on_pos integer = CHARINDEX(N' on ', @ver);
    IF @on_pos > 0
        SET @host_os = LTRIM(SUBSTRING(@ver, @on_pos + 4, LEN(@ver)));
END;

/* Availability Group replica role. The DMV is referenced only inside
   OBJECT_ID-guarded dynamic SQL, so this batch still compiles on Azure SQL
   Database and non-AG instances — both leave @ag_role at 'Standalone' (#980). */
IF CONVERT(integer, ISNULL(SERVERPROPERTY('IsHadrEnabled'), 0)) = 1
   AND OBJECT_ID(N'sys.dm_hadr_availability_replica_states') IS NOT NULL
BEGIN
    DECLARE @ag_detected nvarchar(20);
    EXEC sys.sp_executesql N'
        SELECT @r = CASE
            WHEN MAX(CASE WHEN ars.role = 1 THEN 1 ELSE 0 END) = 1 THEN N''Primary''
            WHEN MAX(CASE WHEN ars.role = 2 THEN 1 ELSE 0 END) = 1 THEN N''Secondary''
            ELSE N''Standalone'' END
        FROM sys.dm_hadr_availability_replica_states AS ars
        WHERE ars.is_local = 1;',
        N'@r nvarchar(20) OUTPUT', @r = @ag_detected OUTPUT;
    SET @ag_role = ISNULL(@ag_detected, N'Standalone');
END;

SELECT
    /* Azure SQL DB reports the legacy 'SQL Azure' for SERVERPROPERTY('Edition');
       show the actual product name + service tier (e.g. 'Azure SQL Database
       (General Purpose)') instead. */
    CASE
        WHEN CONVERT(integer, SERVERPROPERTY('EngineEdition')) = 5
        THEN N'Azure SQL Database'
             + ISNULL(N' (' +
                 CASE CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Edition'))
                     WHEN N'GeneralPurpose'   THEN N'General Purpose'
                     WHEN N'BusinessCritical' THEN N'Business Critical'
                     ELSE CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Edition'))
                 END + N')', N'')
        ELSE CONVERT(nvarchar(256), SERVERPROPERTY('Edition'))
    END,
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')),
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductUpdateLevel')),
    CONVERT(integer, NULL),
    CONVERT(bigint, NULL),
    CONVERT(datetime, NULL),
    @storage_gb,
    CONVERT(integer, NULL),
    CONVERT(integer, NULL),
    CONVERT(integer, SERVERPROPERTY('EngineEdition')),
    CONVERT(integer, SERVERPROPERTY('IsHadrEnabled')),
    CONVERT(integer, SERVERPROPERTY('IsClustered')),
    @host_os,
    @ag_role;";

    /// <summary>
    /// Hardware facts from sys.dm_os_sys_info: CPU count, physical memory (MB), start time, and
    /// socket/core topology. That DMV needs VIEW SERVER STATE (VIEW DATABASE STATE on Azure SQL DB),
    /// so it runs in its OWN best-effort read — a permission gap loses only these fields, never the
    /// inventory row above (#1535). Columns: 0 cpu_count, 1 physical_memory_mb, 2 sqlserver_start_time,
    /// 3 socket_count, 4 cores_per_socket.
    /// </summary>
    internal const string HardwareQueryText =
        "SELECT cpu_count, physical_memory_kb / 1024, sqlserver_start_time, socket_count, cores_per_socket FROM sys.dm_os_sys_info;";

    /// <summary>
    /// Queries a SQL Server directly for its properties. Inventory facts (edition, version, storage)
    /// come from a permission-free query; hardware facts (CPU/memory/sockets) come from a separate
    /// best-effort read of sys.dm_os_sys_info, so a login without VIEW DATABASE STATE still gets the
    /// inventory row with <see cref="ServerPropertyRow.HardwareUnavailableReason"/> set.
    /// Works from any database context — no PerformanceMonitor DB required.
    /// </summary>
    public static async Task<ServerPropertyRow> GetServerPropertiesLiveAsync(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        ServerPropertyRow result;

        // Inventory read. Scoped closed in a using(...) block BEFORE the hardware read opens: the
        // connection has no MARS, so an overlapping second reader would throw (#1535/#1589).
        using (var command = new SqlCommand(InventoryQueryText, connection) { CommandTimeout = 30 })
        using (var reader = await command.ExecuteReaderAsync())
        {
            if (!await reader.ReadAsync())
                return new ServerPropertyRow();

            var version = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var level = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var updateLevel = reader.IsDBNull(3) ? null : reader.GetString(3);
            var versionDisplay = !string.IsNullOrEmpty(updateLevel)
                ? $"{version} - {updateLevel}"
                : $"{version} - {level}";

            result = new ServerPropertyRow
            {
                Edition = reader.IsDBNull(0) ? "" : reader.GetString(0),
                ProductVersion = versionDisplay,
                CpuCount = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                PhysicalMemoryMb = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                SqlServerStartTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                StorageTotalGb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                SocketCount = reader.IsDBNull(8) ? null : Convert.ToInt32(reader.GetValue(8)),
                CoresPerSocket = reader.IsDBNull(9) ? null : Convert.ToInt32(reader.GetValue(9)),
                EngineEdition = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)),
                IsHadrEnabled = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)) == 1,
                IsClustered = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12)) == 1,
                HostOsVersion = reader.IsDBNull(13) ? "" : reader.GetString(13),
                AgReplicaRole = reader.IsDBNull(14) ? "Standalone" : reader.GetString(14),
                LastUpdated = DateTime.Now
            };
        }

        // Hardware read: best-effort in its OWN try/catch. sys.dm_os_sys_info needs VIEW SERVER STATE
        // (VIEW DATABASE STATE on Azure SQL DB); a login without it loses only these fields, and the
        // inventory row above still renders edition/version/storage (#1535).
        try
        {
            using var hardwareCommand = new SqlCommand(HardwareQueryText, connection) { CommandTimeout = 30 };
            using var hardwareReader = await hardwareCommand.ExecuteReaderAsync();
            if (await hardwareReader.ReadAsync())
            {
                result.CpuCount = hardwareReader.IsDBNull(0) ? 0 : Convert.ToInt32(hardwareReader.GetValue(0));
                result.PhysicalMemoryMb = hardwareReader.IsDBNull(1) ? 0L : Convert.ToInt64(hardwareReader.GetValue(1));
                result.SqlServerStartTime = hardwareReader.IsDBNull(2) ? null : hardwareReader.GetDateTime(2);
                result.SocketCount = hardwareReader.IsDBNull(3) ? null : Convert.ToInt32(hardwareReader.GetValue(3));
                result.CoresPerSocket = hardwareReader.IsDBNull(4) ? null : Convert.ToInt32(hardwareReader.GetValue(4));
            }
        }
        catch (SqlException ex)
        {
            result.HardwareUnavailableReason =
                "Hardware inventory (CPU, memory, sockets) unavailable - the monitoring login likely lacks VIEW DATABASE STATE on this database.";
            AppLogger.Warn("FinOps", $"Hardware inventory unavailable for '{connection.DataSource}': {ex.Message}");
        }

        return result;
    }

    /// <summary>One fleet server's overlay metrics — <see cref="GetServerMetricsAsync"/>'s per-server result.</summary>
    public readonly record struct ServerMetricsRow(decimal? AvgCpuPct, decimal? StorageTotalGb, int? IdleDbCount, string? ProvisioningStatus);

    /// <summary>
    /// Collected metrics (CPU, storage, idle DBs, provisioning status) for EVERY server in the local DuckDB, in
    /// ONE round trip (#4227 Lite parity with Darling's fleet merge — see #4227's Darling PR for the production
    /// measurement this generalized from). A server with a row in <c>servers</c> but no rows in any of the five
    /// source tables yet still gets an entry, all fields null — matching the OLD per-server statement's anchor
    /// row, which always returned exactly one all-NULL row rather than no row at all.
    ///
    /// <para>Measured on a seeded DuckDB (50 servers): the old N-call loop (one <see cref="OpenConnectionAsync"/>
    /// and one read-lock acquisition per server, run in the pool-wide parallel fan-out <c>LoadServerInventoryAsync</c>
    /// already uses) averaged ~100-185ms wall-clock at 50 servers; this one statement averaged ~85-99ms —
    /// roughly half, and the gap widens with server count since the old shape's cost is one query plan and one
    /// read-lock acquisition PER server while this one pays that cost once for the whole fleet.</para>
    /// </summary>
    public async Task<Dictionary<int, ServerMetricsRow>> GetServerMetricsAsync()
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cpuCutoff = DateTime.UtcNow.AddHours(-24);
        var idleCutoff = DateTime.UtcNow.AddDays(-7);

        command.CommandText = @"
WITH cpu_24h AS (
    SELECT
        server_id,
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct
    FROM v_cpu_utilization_stats
    WHERE collection_time >= $1
    GROUP BY server_id
),
/* Only the worker counts are consumed now: memory_ratio used to feed this read's own CASE, and that
   CASE was the #2246 bug. The verdict comes from ProvisioningVerdict, so the division would be dead. */
mem_latest AS (
    SELECT
        s.server_id,
        latest.max_workers_count,
        latest.current_workers_count
    FROM servers s
    LEFT JOIN LATERAL (
        SELECT max_workers_count, current_workers_count
        FROM v_memory_stats
        WHERE server_id = s.server_id
        ORDER BY collection_time DESC
        LIMIT 1
    ) AS latest ON true
),
/* Same workspace-memory pressure signals as the drill-down read, so the INVENTORY GRID cannot classify a
   server by a rule the drill-down no longer uses (#2246 — this grid is the screen the field report was
   looking at). */
grants AS (
    SELECT
        server_id,
        MAX(waiter_count) AS max_grant_waiters,
        SUM(COALESCE(timeout_error_count_delta, 0)) AS grant_timeouts,
        SUM(COALESCE(forced_grant_count_delta, 0)) AS forced_grants,
        MAX(100.0 * granted_memory_mb / NULLIF(target_memory_mb, 0)) AS grant_utilization_pct
    FROM v_memory_grant_stats
    WHERE collection_time >= $1
    GROUP BY server_id
),
/* The latest size-snapshot TIME per server, shared by storage_totals (sums every database at that instant)
   and latest_dbs (the idle check's known-databases universe) so neither re-derives it. */
size_latest AS (
    SELECT
        s.server_id,
        latest_time.collection_time
    FROM servers s
    LEFT JOIN LATERAL (
        SELECT collection_time
        FROM v_database_size_stats
        WHERE server_id = s.server_id
        ORDER BY collection_time DESC
        LIMIT 1
    ) AS latest_time ON true
),
storage_totals AS (
    SELECT
        sl.server_id,
        SUM(vd.total_size_mb) / 1024.0 AS total_storage_gb
    FROM size_latest sl
    JOIN v_database_size_stats vd
      ON vd.server_id = sl.server_id
     AND vd.collection_time = sl.collection_time
    GROUP BY sl.server_id
),
latest_dbs AS (
    SELECT DISTINCT
        sl.server_id,
        vd.database_name
    FROM size_latest sl
    JOIN v_database_size_stats vd
      ON vd.server_id = sl.server_id
     AND vd.collection_time = sl.collection_time
    WHERE vd.database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
),
active_dbs AS (
    SELECT DISTINCT server_id, database_name
    FROM v_query_stats
    WHERE collection_time >= $2
    AND   delta_execution_count > 0
),
idle_dbs AS (
    SELECT server_id, COUNT(*) AS idle_db_count
    FROM (
        SELECT server_id, database_name FROM latest_dbs
        EXCEPT
        SELECT server_id, database_name FROM active_dbs
    ) AS idle
    GROUP BY server_id
)
SELECT
    s.server_id,
    c.avg_cpu_pct,
    st.total_storage_gb,
    id.idle_db_count,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    COALESCE(m.max_workers_count, 0),
    COALESCE(m.current_workers_count, 0),
    COALESCE(g.max_grant_waiters, 0),
    COALESCE(g.grant_timeouts, 0),
    COALESCE(g.forced_grants, 0),
    COALESCE(g.grant_utilization_pct, 0)
FROM servers s
LEFT JOIN cpu_24h c ON c.server_id = s.server_id
LEFT JOIN mem_latest m ON m.server_id = s.server_id
LEFT JOIN storage_totals st ON st.server_id = s.server_id
LEFT JOIN idle_dbs id ON id.server_id = s.server_id
LEFT JOIN grants g ON g.server_id = s.server_id";

        command.Parameters.Add(new DuckDBParameter { Value = cpuCutoff });
        command.Parameters.Add(new DuckDBParameter { Value = idleCutoff });

        var results = new Dictionary<int, ServerMetricsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var serverId = Convert.ToInt32(reader.GetValue(0));

            /* The verdict is computed HERE rather than as a SQL CASE, so this grid and the drill-down cannot
               disagree — they now call the same predicate. The old inline CASE was copies 5 and 6 of the
               ratio bug, on the screen the field report was actually looking at (#2246). */
            var status = ProvisioningVerdict.Evaluate(
                avgCpuPercent: reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                maxCpuPercent: reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                p95CpuPercent: reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                maxGrantWaiters: reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8)),
                grantTimeouts: reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9)),
                forcedGrants: reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10)),
                grantUtilizationPercent: reader.IsDBNull(11) ? 0m : Convert.ToDecimal(reader.GetValue(11)),
                maxWorkers: reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                currentWorkers: reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)));

            results[serverId] = new ServerMetricsRow(
                reader.IsDBNull(1) ? null : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                status);
        }

        return results;
    }
}
