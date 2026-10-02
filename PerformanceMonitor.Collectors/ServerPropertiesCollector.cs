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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Server edition, version, and CPU/memory hardware metadata for license audit and FinOps cost
/// attribution (on-load-only collector). Extracted verbatim from Lite's
/// RemoteCollectorService.ServerProperties.cs, including: the Azure SQL DB edition/service-tier
/// naming, the vCore parse from the service objective (dm_os_sys_info.cpu_count reports the
/// compute node's cores on Azure, not the per-database allocation), and the WS5 server-health
/// probe (LPIM / IFI / memory-dump count) as a best-effort SUPPLEMENTAL query — its failure can
/// never fail the properties row, mirroring install/53_collect_server_properties.sql.
///
/// <para><b>The zone beside the offset (Darling V134 / Lite v63, #3653 item 13, Q8).</b>
/// <c>utc_offset_minutes</c> is <c>DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())</c> — the offset IN FORCE at
/// collection, which is exact only for a stored instant on the same side of a DST transition as the
/// collection was. Every de-skew that subtracts it from a column that can outlive a transition
/// (<c>get_index_usage</c>'s <c>last_user_access</c>, the PVS cleaner stamps, #3231) is an hour wrong for
/// the far side, and an offset cannot say which side a given instant was on; a ZONE can. <c>time_zone_id</c>
/// is <c>CURRENT_TIMEZONE_ID()</c>, the engine's own zone name (a Windows zone id such as
/// <c>Eastern Standard Time</c>, the key <c>AT TIME ZONE</c> takes), stored verbatim beside the offset the
/// ruling keeps alongside it. NULL where the engine cannot say: the function exists on SQL Server 2022+
/// and on Azure SQL Database / Managed Instance only, and on an older engine it is not a missing OBJECT
/// but a missing built-in, so the batch that names it fails to COMPILE — an <c>OBJECT_ID</c> guard has
/// nothing to test and a <c>CASE</c> cannot help, because the whole statement is rejected before any
/// branch runs. The read therefore sits in its own <c>sp_executesql</c> batch, behind a version / edition
/// gate so a pre-2022 engine never even attempts it, and inside <c>TRY … CATCH</c> so an engine the gate
/// admits but that still lacks the function (or refuses it) leaves NULL rather than losing the row —
/// the same isolation the hardware read has for a missing grant (#1591). A NULL zone id means "pre-2022
/// engine: only the offset is known", and the MCP <c>get_server_properties</c> payload says so.</para>
/// </summary>
public sealed class ServerPropertiesCollector : CollectorDefinitionBase<ServerPropertiesCollector.Row>
{
    public static ServerPropertiesCollector Instance { get; } = new();

    private ServerPropertiesCollector()
    {
    }

    public readonly record struct Row(
        string Edition,
        string ProductVersion,
        string ProductLevel,
        string? ProductUpdateLevel,
        int EngineEdition,
        /* Nullable since #1591: these three come from sys.dm_os_sys_info, which needs VIEW SERVER
           STATE (VIEW DATABASE STATE on Azure SQL DB). Without it the hardware facts are unknown
           and the rest of the row is still collected, so "unknown" has to be representable. */
        int? CpuCount,
        int? HyperthreadRatio,
        long? PhysicalMemoryMb,
        int? SocketCount,
        int? CoresPerSocket,
        bool? IsHadrEnabled,
        bool? IsClustered,
        string? ServiceObjective,
        int? VcoreCount,
        bool? LockPagesInMemory,
        bool? InstantFileInitializationEnabled,
        int? MemoryDumpCount,
        DateTime? SqlServerStartTime,
        string? HostOsVersion,
        string? AgReplicaRole,
        int? UtcOffsetMinutes,
        /* #3653 item 13 (Q8): CURRENT_TIMEZONE_ID() on SQL Server 2022+ / Azure SQL DB / Managed Instance;
           NULL on every older engine, where only the offset is known. */
        string? TimeZoneId);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Host OS + AG replica role are resolved into locals first, each behind an OBJECT_ID guard so the
   batch still compiles + runs on Azure SQL Database and non-AG instances (the DMVs are absent there)
   — the deferred-object-ref pattern, NOT a version guard (#980). Both leave a benign default when
   unavailable: host OS falls back to the @@VERSION 'on ...' suffix; AG role stays 'Standalone'. */
DECLARE
    @host_os nvarchar(256),
    @ag_role nvarchar(20) = N'Standalone';

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

IF CONVERT(integer, ISNULL(SERVERPROPERTY(N'IsHadrEnabled'), 0)) = 1
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

/* Hardware facts are the ONLY part of this collector that needs a DMV, and sys.dm_os_sys_info
   requires VIEW SERVER STATE (VIEW DATABASE STATE on Azure SQL DB). Read them into variables
   inside TRY/CATCH -- exactly as the supplemental health query below does -- so a login without
   that grant loses only the hardware columns instead of the ENTIRE server_properties row.
   Everything else in the SELECT is a permission-free SERVERPROPERTY/DATABASEPROPERTYEX scalar. */
DECLARE
    @cpu_count integer = NULL,
    @hyperthread_ratio integer = NULL,
    @physical_memory_mb bigint = NULL,
    @socket_count integer = NULL,
    @cores_per_socket integer = NULL,
    @sqlserver_start_time datetime = NULL;

BEGIN TRY
    SELECT
        @cpu_count = osi.cpu_count,
        @hyperthread_ratio = osi.hyperthread_ratio,
        @physical_memory_mb = osi.physical_memory_kb / 1024,
        @socket_count = osi.socket_count,
        @cores_per_socket = osi.cores_per_socket,
        @sqlserver_start_time = osi.sqlserver_start_time
    FROM sys.dm_os_sys_info AS osi;
END TRY
BEGIN CATCH
    /* Permission denied (or the DMV is unavailable): leave every hardware value NULL. */
END CATCH;

/* #3653 item 13 (Q8): the engine's own time-zone id, beside the offset the projection below computes.
   CURRENT_TIMEZONE_ID() is a BUILT-IN, not an object: on SQL Server 2019 and earlier the function does not
   exist and a batch that names it fails to compile as a whole, so it cannot sit in the main SELECT behind
   a CASE and OBJECT_ID() has nothing to test — this is the one column here that needs a version gate,
   and the gate is stated as such rather than dressed up as the deferred-object-ref pattern the two guards
   above use. ProductMajorVersion 16 = SQL Server 2022, the first box release with the function; Azure SQL
   Database (EngineEdition 5) and Managed Instance (8) report a low ProductMajorVersion yet ship it, the
   same shape QueryStatsCollector's gate notes for its DMV. The dynamic batch compiles at EXEC time, one
   level down, so a compile error there IS catchable by this TRY/CATCH — belt and braces for an edition the
   gate admits that still refuses the call. NULL means the engine cannot say, never a guessed zone. */
DECLARE @time_zone_id nvarchar(128) = NULL;

IF CONVERT(integer, SERVERPROPERTY(N'ProductMajorVersion')) >= 16
OR CONVERT(integer, SERVERPROPERTY(N'EngineEdition')) IN (5, 8)
BEGIN
    BEGIN TRY
        EXEC sys.sp_executesql
            N'SELECT @tz = CURRENT_TIMEZONE_ID();',
            N'@tz nvarchar(128) OUTPUT', @tz = @time_zone_id OUTPUT;
    END TRY
    BEGIN CATCH
        SET @time_zone_id = NULL;
    END CATCH;
END;

SELECT
    server_name =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName')),
    edition =
        /* Azure SQL DB reports the legacy 'SQL Azure' for SERVERPROPERTY('Edition');
           store the actual product name + service tier instead. */
        CASE
            WHEN CONVERT(integer, SERVERPROPERTY(N'EngineEdition')) = 5
            THEN N'Azure SQL Database'
                 + ISNULL(N' (' +
                     CASE CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), N'Edition'))
                         WHEN N'GeneralPurpose'   THEN N'General Purpose'
                         WHEN N'BusinessCritical' THEN N'Business Critical'
                         ELSE CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), N'Edition'))
                     END + N')', N'')
            ELSE CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition'))
        END,
    product_version =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
    product_level =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductLevel')),
    product_update_level =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductUpdateLevel')),
    engine_edition =
        CONVERT(integer, SERVERPROPERTY(N'EngineEdition')),
    cpu_count =
        @cpu_count,
    hyperthread_ratio =
        @hyperthread_ratio,
    physical_memory_mb =
        @physical_memory_mb,
    socket_count =
        @socket_count,
    cores_per_socket =
        @cores_per_socket,
    is_hadr_enabled =
        CONVERT(bit, SERVERPROPERTY(N'IsHadrEnabled')),
    is_clustered =
        CONVERT(bit, SERVERPROPERTY(N'IsClustered')),
    service_objective =
        CASE
            WHEN CONVERT(integer, SERVERPROPERTY(N'EngineEdition')) = 5
            THEN CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), N'ServiceObjective'))
            ELSE NULL
        END,
    sqlserver_start_time =
        @sqlserver_start_time,
    host_os_version =
        @host_os,
    ag_replica_role =
        @ag_role,
    /* The monitored server's UTC offset in minutes — the same live derivation Lite computes at
       connect (ServerManager). Collected so the headless viewer, which cannot query the target
       live, can render timestamps in the server's own local time (Server-time display mode). This is
       the offset IN FORCE at collection — exact for an instant on the same side of a DST transition,
       an hour wrong for one on the other side (#3231); the zone id beside it is what can tell the two
       apart, and it stays alongside rather than replacing this because the zone is NULL pre-2022. */
    utc_offset_minutes =
        DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()),
    /* #3653 item 13 (Q8): the gated CURRENT_TIMEZONE_ID() read above; NULL where the engine cannot say. */
    time_zone_id =
        @time_zone_id
OPTION(RECOMPILE);";

    private const string ServerHealthQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @lpim bit = NULL,
    @ifi bit = NULL,
    @dumps integer = NULL;

BEGIN TRY
    SELECT
        @lpim =
            CASE
                WHEN osi.sql_memory_model IN (2, 3)
                THEN CONVERT(bit, 1)
                ELSE CONVERT(bit, 0)
            END
    FROM sys.dm_os_sys_info AS osi;
END TRY
BEGIN CATCH
    SET @lpim = NULL;
END CATCH;

IF OBJECT_ID(N'sys.dm_server_services', N'V') IS NOT NULL
AND EXISTS
(
    SELECT
        1/0
    FROM sys.system_columns AS sc
    WHERE sc.object_id = OBJECT_ID(N'sys.dm_server_services')
    AND   sc.name = N'instant_file_initialization_enabled'
)
BEGIN
    BEGIN TRY
        DECLARE
            @ifi_sql nvarchar(max) =
                N'
        SELECT TOP (1)
            @ifi_out =
                CASE
                    WHEN ss.instant_file_initialization_enabled = N''Y''
                    THEN CONVERT(bit, 1)
                    WHEN ss.instant_file_initialization_enabled = N''N''
                    THEN CONVERT(bit, 0)
                    ELSE NULL
                END
        FROM sys.dm_server_services AS ss
        WHERE ss.servicename LIKE N''SQL Server (%'';';

        EXECUTE sys.sp_executesql
            @ifi_sql,
          N'@ifi_out bit OUTPUT',
            @ifi_out = @ifi OUTPUT;
    END TRY
    BEGIN CATCH
        SET @ifi = NULL;
    END CATCH;
END;

IF OBJECT_ID(N'sys.dm_server_memory_dumps', N'V') IS NOT NULL
BEGIN
    BEGIN TRY
        SELECT
            @dumps = COUNT_BIG(*)
        FROM sys.dm_server_memory_dumps AS smd;
    END TRY
    BEGIN CATCH
        SET @dumps = NULL;
    END CATCH;
END;

SELECT
    lock_pages_in_memory = @lpim,
    instant_file_initialization_enabled = @ifi,
    memory_dump_count = @dumps;";

    public override string Name => "server_properties";

    public override string TargetTable => "server_properties";

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    /// <summary>WS5 health probe — meaningless/unavailable on Azure SQL DB, so skipped there.</summary>
    public override CollectorQuery? BuildSupplementalQuery(CollectorContext context)
        => context.Target.IsAzureSqlDb ? null : new CollectorQuery(ServerHealthQueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("edition", CollectorColumnType.Varchar),
        new CollectorColumn("product_version", CollectorColumnType.Varchar),
        new CollectorColumn("product_level", CollectorColumnType.Varchar),
        new CollectorColumn("product_update_level", CollectorColumnType.Varchar),
        new CollectorColumn("engine_edition", CollectorColumnType.Integer),
        new CollectorColumn("cpu_count", CollectorColumnType.Integer),
        new CollectorColumn("hyperthread_ratio", CollectorColumnType.Integer),
        new CollectorColumn("physical_memory_mb", CollectorColumnType.BigInt),
        new CollectorColumn("socket_count", CollectorColumnType.Integer),
        new CollectorColumn("cores_per_socket", CollectorColumnType.Integer),
        new CollectorColumn("is_hadr_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("is_clustered", CollectorColumnType.Boolean),
        new CollectorColumn("enterprise_features", CollectorColumnType.Varchar),
        new CollectorColumn("service_objective", CollectorColumnType.Varchar),
        new CollectorColumn("vcore_count", CollectorColumnType.Integer),
        new CollectorColumn("lock_pages_in_memory", CollectorColumnType.Boolean),
        new CollectorColumn("instant_file_initialization_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("memory_dump_count", CollectorColumnType.Integer),
        /* Appended (never inserted) so a fresh generated V1 store + an ALTER-migrated store keep an
           identical physical column order for the positional writers (Lite DuckDB appender / Darling
           binary COPY). Restores the three fields Lite's FinOps Server Inventory previously read from a
           LIVE query — now collected so the headless viewer surfaces them too. */
        new CollectorColumn("sqlserver_start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("host_os_version", CollectorColumnType.Varchar),
        new CollectorColumn("ag_replica_role", CollectorColumnType.Varchar),
        /* Appended (never inserted) so a fresh V1 store and an ALTER-migrated store keep an identical
           physical column order for the positional writers. The monitored server's UTC offset in
           minutes — the viewer's Server-time display mode reads it (UTC + offset = server local). */
        new CollectorColumn("utc_offset_minutes", CollectorColumnType.Integer),
        /* Appended (never inserted), Darling V134 / Lite v63 (#3653 item 13, Q8): the engine's time-zone
           id from CURRENT_TIMEZONE_ID(), beside the offset it disambiguates across DST; NULL on a pre-2022
           engine, where only the offset is known. */
        new CollectorColumn("time_zone_id", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        if (!await reader.ReadAsync(cancellationToken))
        {
            return rows;
        }

        var serviceObjective = reader.IsDBNull(13) ? null : reader.GetString(13);
        /* sqlserver_start_time (14) is the server's LOCAL wall clock (sys.dm_os_sys_info) — stored
           verbatim like every other DMV clock. host_os_version (15) / ag_replica_role (16) are the
           OBJECT_ID-guarded locals (host OS + AG role), NULL/'Standalone' when the DMVs are absent. */
        var sqlServerStartTime = reader.IsDBNull(14) ? (DateTime?)null : reader.GetDateTime(14);
        var hostOsVersion = reader.IsDBNull(15) ? null : reader.GetString(15);
        var agReplicaRole = reader.IsDBNull(16) ? null : reader.GetString(16);
        /* utc_offset_minutes (17): DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) — always non-null in
           practice, guarded anyway for a defensive read. time_zone_id (18): the gated
           CURRENT_TIMEZONE_ID() local — NULL is the expected value on a pre-2022 engine (#3653 item 13). */
        var utcOffsetMinutes = reader.IsDBNull(17) ? (int?)null : reader.GetInt32(17);
        var timeZoneId = reader.IsDBNull(18) ? null : reader.GetString(18);

        /* For Azure SQL DB, sys.dm_os_sys_info.cpu_count returns the compute node's total cores,
           not the per-database vCore allocation. Parse the actual vCore count from the service
           objective string (e.g. "HS_Gen5_14" → 14). */
        int? vcoreCount = null;
        if (context.Target.IsAzureSqlDb && !string.IsNullOrEmpty(serviceObjective))
        {
            vcoreCount = ParseVcoreFromServiceObjective(serviceObjective);
        }

        rows.Add(new Row(
            Edition: reader.GetString(1),
            ProductVersion: reader.GetString(2),
            ProductLevel: reader.GetString(3),
            ProductUpdateLevel: reader.IsDBNull(4) ? null : reader.GetString(4),
            EngineEdition: reader.GetInt32(5),
            CpuCount: reader.IsDBNull(6) ? null : reader.GetInt32(6),
            HyperthreadRatio: reader.IsDBNull(7) ? null : reader.GetInt32(7),
            PhysicalMemoryMb: reader.IsDBNull(8) ? null : reader.GetInt64(8),
            SocketCount: reader.IsDBNull(9) ? null : reader.GetInt32(9),
            CoresPerSocket: reader.IsDBNull(10) ? null : reader.GetInt32(10),
            IsHadrEnabled: reader.IsDBNull(11) ? null : reader.GetBoolean(11),
            IsClustered: reader.IsDBNull(12) ? null : reader.GetBoolean(12),
            ServiceObjective: serviceObjective,
            VcoreCount: vcoreCount,
            LockPagesInMemory: null,
            InstantFileInitializationEnabled: null,
            MemoryDumpCount: null,
            SqlServerStartTime: sqlServerStartTime,
            HostOsVersion: hostOsVersion,
            AgReplicaRole: agReplicaRole,
            UtcOffsetMinutes: utcOffsetMinutes,
            TimeZoneId: timeZoneId));

        return rows;
    }

    public override async ValueTask ApplySupplementalAsync(List<Row> rows, DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || !await reader.ReadAsync(cancellationToken))
        {
            return;
        }

        rows[0] = rows[0] with
        {
            LockPagesInMemory = reader.IsDBNull(0) ? null : reader.GetBoolean(0),
            InstantFileInitializationEnabled = reader.IsDBNull(1) ? null : reader.GetBoolean(1),
            MemoryDumpCount = reader.IsDBNull(2) ? null : reader.GetInt32(2),
        };
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.Edition)
            .Value(row.ProductVersion)
            .Value(row.ProductLevel)
            .Value(row.ProductUpdateLevel)
            .Value(row.EngineEdition)
            .Value(row.CpuCount)
            .Value(row.HyperthreadRatio)
            .Value(row.PhysicalMemoryMb)
            .Value(row.SocketCount)
            .Value(row.CoresPerSocket)
            .Value(row.IsHadrEnabled)
            .Value(row.IsClustered)
            .Value((string?)null)   /* enterprise_features — not collected in Lite (requires cross-database cursor) */
            .Value(row.ServiceObjective)
            .Value(row.VcoreCount)
            .Value(row.LockPagesInMemory)
            .Value(row.InstantFileInitializationEnabled)
            .Value(row.MemoryDumpCount)
            .Value(row.SqlServerStartTime)
            .Value(row.HostOsVersion)
            .Value(row.AgReplicaRole)
            .Value(row.UtcOffsetMinutes)
            .Value(row.TimeZoneId);      /* time_zone_id — NULL pre-2022, where only the offset is known (#3653 item 13) */
    }

    /// <summary>
    /// Parses the vCore count from an Azure SQL DB service objective string.
    /// vCore tiers follow the pattern {Tier}_{Gen}_{VcoreCount}
    /// (e.g. "HS_Gen5_14", "GP_Gen5_6", "BC_Gen5_8", "GP_S_Gen5_2").
    /// DTU tiers (e.g. "P1", "S0") and elastic pools return null.
    /// </summary>
    public static int? ParseVcoreFromServiceObjective(string serviceObjective)
    {
        var parts = serviceObjective.Split('_');
        if (parts.Length >= 3 && int.TryParse(parts[^1], out var vcores) && vcores > 0)
        {
            return vcores;
        }

        return null;
    }
}
