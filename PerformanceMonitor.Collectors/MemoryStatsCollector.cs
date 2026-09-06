/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Server memory statistics from sys.dm_os_sys_memory + performance counters, with an Azure SQL
/// DB variant (edition 5) that approximates from sys.dm_os_sys_info committed targets and reports
/// current_workers_count as NULL — elastic pools enforce VIEW SERVER PERFORMANCE STATE on
/// sys.dm_os_schedulers regardless of DB-scoped grants (#857). Azure MI (edition 8) behaves like
/// on-prem. Extracted verbatim from Lite's RemoteCollectorService.Memory.cs. Single row per cycle;
/// zero rows when the query returns none.
/// </summary>
public sealed class MemoryStatsCollector : CollectorDefinitionBase<MemoryStatsCollector.Row>
{
    public static MemoryStatsCollector Instance { get; } = new();

    private MemoryStatsCollector()
    {
    }

    public readonly record struct Row(
        decimal TotalPhysicalMemoryMb,
        decimal AvailablePhysicalMemoryMb,
        decimal TotalPageFileMb,
        decimal AvailablePageFileMb,
        string SystemMemoryState,
        string SqlMemoryModel,
        decimal TargetServerMemoryMb,
        decimal TotalServerMemoryMb,
        decimal BufferPoolMb,
        decimal PlanCacheMb,
        int MaxWorkersCount,
        int CurrentWorkersCount);

    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    total_physical_memory_mb = CONVERT(decimal(18,2), osi.committed_target_kb / 1024.0),
    available_physical_memory_mb = CONVERT(decimal(18,2), (osi.committed_target_kb - osi.committed_kb) / 1024.0),
    total_page_file_mb = CONVERT(decimal(18,2), 0),
    available_page_file_mb = CONVERT(decimal(18,2), 0),
    system_memory_state = N'Available',
    sql_memory_model = N'N/A',
    target_server_memory_mb = CONVERT(decimal(18,2), pc_target.cntr_value / 1024.0),
    total_server_memory_mb = CONVERT(decimal(18,2), pc_total.cntr_value / 1024.0),
    buffer_pool_mb = CONVERT(decimal(18,2), pc_buffer.cntr_value / 1024.0),
    plan_cache_mb = CONVERT(decimal(18,2), pc_plan.cntr_value * 8.0 / 1024.0),
    max_workers_count = osi.max_workers_count,
    current_workers_count = CONVERT(integer, NULL)
FROM sys.dm_os_sys_info AS osi
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Target Server Memory (KB)'
) AS pc_target
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Total Server Memory (KB)'
) AS pc_total
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Database Cache Memory (KB)'
) AS pc_buffer
CROSS JOIN
(
    SELECT cntr_value = SUM(cntr_value)
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Cache Pages'
      AND object_name LIKE N'%:Plan Cache%'
) AS pc_plan
OPTION(RECOMPILE);";

    private const string OnPremQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    total_physical_memory_mb = CONVERT(decimal(18,2), osm.total_physical_memory_kb / 1024.0),
    available_physical_memory_mb = CONVERT(decimal(18,2), osm.available_physical_memory_kb / 1024.0),
    total_page_file_mb = CONVERT(decimal(18,2), osm.total_page_file_kb / 1024.0),
    available_page_file_mb = CONVERT(decimal(18,2), osm.available_page_file_kb / 1024.0),
    system_memory_state = osm.system_memory_state_desc,
    sql_memory_model = osi.sql_memory_model_desc,
    target_server_memory_mb = CONVERT(decimal(18,2), pc_target.cntr_value / 1024.0),
    total_server_memory_mb = CONVERT(decimal(18,2), pc_total.cntr_value / 1024.0),
    buffer_pool_mb = CONVERT(decimal(18,2), pc_buffer.cntr_value / 1024.0),
    plan_cache_mb = CONVERT(decimal(18,2), pc_plan.cntr_value * 8.0 / 1024.0),
    max_workers_count = osi.max_workers_count,
    current_workers_count = w.current_workers
FROM sys.dm_os_sys_memory AS osm
CROSS JOIN sys.dm_os_sys_info AS osi
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Target Server Memory (KB)'
) AS pc_target
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Total Server Memory (KB)'
) AS pc_total
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Database Cache Memory (KB)'
) AS pc_buffer
CROSS JOIN
(
    SELECT cntr_value = SUM(cntr_value)
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Cache Pages'
      AND object_name LIKE N'%:Plan Cache%'
) AS pc_plan
CROSS JOIN
(
    SELECT current_workers = SUM(active_workers_count)
    FROM sys.dm_os_schedulers
    WHERE status = N'VISIBLE ONLINE'
) AS w
OPTION(RECOMPILE);";

    public override string Name => "memory_stats";

    public override string TargetTable => "memory_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(context.Target.IsAzureSqlDb ? AzureSqlDbQueryText : OnPremQueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("total_physical_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("available_physical_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("total_page_file_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("available_page_file_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("system_memory_state", CollectorColumnType.Varchar),
        new CollectorColumn("sql_memory_model", CollectorColumnType.Varchar),
        new CollectorColumn("target_server_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("total_server_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("buffer_pool_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("plan_cache_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("max_workers_count", CollectorColumnType.Integer),
        new CollectorColumn("current_workers_count", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        if (!await reader.ReadAsync(cancellationToken))
        {
            return rows;
        }

        rows.Add(new Row(
            reader.IsDBNull(0) ? 0m : reader.GetDecimal(0),
            reader.IsDBNull(1) ? 0m : reader.GetDecimal(1),
            reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
            reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
            reader.IsDBNull(4) ? "Unknown" : reader.GetString(4),
            reader.IsDBNull(5) ? "Unknown" : reader.GetString(5),
            reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
            reader.IsDBNull(7) ? 0m : reader.GetDecimal(7),
            reader.IsDBNull(8) ? 0m : reader.GetDecimal(8),
            reader.IsDBNull(9) ? 0m : reader.GetDecimal(9),
            reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
            reader.IsDBNull(11) ? 0 : reader.GetInt32(11)));

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.TotalPhysicalMemoryMb)
            .Value(row.AvailablePhysicalMemoryMb)
            .Value(row.TotalPageFileMb)
            .Value(row.AvailablePageFileMb)
            .Value(row.SystemMemoryState)
            .Value(row.SqlMemoryModel)
            .Value(row.TargetServerMemoryMb)
            .Value(row.TotalServerMemoryMb)
            .Value(row.BufferPoolMb)
            .Value(row.PlanCacheMb)
            .Value(row.MaxWorkersCount)
            .Value(row.CurrentWorkersCount);
    }
}
