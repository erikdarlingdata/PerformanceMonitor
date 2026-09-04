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
/// CPU utilization — ring buffer (on-prem, MI, RDS) or sys.dm_db_resource_stats (Azure SQL DB).
/// Extracted verbatim from Lite's RemoteCollectorService.Cpu.cs, including the platform rules
/// that are the parity contract: Linux stores NULL other-process CPU when SystemIdle reports
/// 0 there (#1048); incomplete SystemHealth ring-buffer records are skipped (#989);
/// Azure filters server-side on the watermark while the ring buffer dedups client-side
/// (its sample_time is computed and cannot be filtered in SQL).
/// </summary>
public sealed class CpuUtilizationCollector : CollectorDefinitionBase<CpuUtilizationCollector.Row>
{
    public static CpuUtilizationCollector Instance { get; } = new();

    private CpuUtilizationCollector()
    {
    }

    public readonly record struct Row(DateTime SampleTime, int SqlServerCpuUtilization, int? OtherProcessCpuUtilization);

    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (60)
    sample_time = drs.end_time,
    sqlserver_cpu_utilization = CONVERT(integer, drs.avg_cpu_percent),
    other_process_cpu_utilization = 0
FROM sys.dm_db_resource_stats AS drs
ORDER BY
    drs.end_time DESC
OPTION(RECOMPILE);";

    private const string AzureSqlDbWatermarkedQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    sample_time = drs.end_time,
    sqlserver_cpu_utilization = CONVERT(integer, drs.avg_cpu_percent),
    other_process_cpu_utilization = 0
FROM sys.dm_db_resource_stats AS drs
WHERE drs.end_time > @last_sample_time
ORDER BY
    drs.end_time DESC
OPTION(RECOMPILE);";

    private const string RingBufferQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @ms_ticks bigint,
    @start_time datetime2(7),
    @is_linux bit = 0;

SELECT
    @ms_ticks = dosi.ms_ticks,
    @start_time = dosi.sqlserver_start_time
FROM sys.dm_os_sys_info AS dosi;

/* Detect SQL Server on Linux. SystemIdle reports 0 in the SCHEDULER_MONITOR
   ring buffer on some Linux/SQL Server version combos, so 100 - SystemIdle - ProcessUtilization
   fabricates a host figure that pins total CPU at 100% (Issue #1048). The ring-buffer metrics
   fix shipped in SQL Server 2025 CU1 (KB5078298 fix 4796293), so real SystemIdle values start
   there, not at 2025 RTM. Prior to that no DMV exposes true host CPU when SystemIdle is 0, so
   other_process is stored as NULL. sys.dm_os_linux_cpu_stats (2025 CU1+) exposes real host CPU
   time but is a cumulative counter requiring a two-sample delta, not a point-in-time snapshot
   like SCHEDULER_MONITOR, so it isn't used here. sys.dm_os_host_info is 2017+; referenced via
   sp_executesql so SQL 2016 never binds it (@is_linux = 0). */
IF OBJECT_ID(N'sys.dm_os_host_info', N'V') IS NOT NULL
    EXEC sys.sp_executesql
        N'SELECT @linux = CASE WHEN hi.host_platform = N''Linux'' THEN 1 ELSE 0 END FROM sys.dm_os_host_info AS hi;',
        N'@linux bit OUTPUT', @linux = @is_linux OUTPUT;

SELECT TOP (60)
    /* MILLISECOND, not SECOND-truncated: dividing by 1000 before DATEADD(SECOND, ...) discards the
       sub-second remainder of the elapsed-ticks offset, and that remainder is different on every poll
       (SYSDATETIME() keeps moving while a given ring-buffer entry's own timestamp does not). Since this
       query re-reads the ring buffer's whole retained history every cycle and dedups client-side on this
       COMPUTED value (a physical entry has no stable key), a recomputed sample_time that drifts forward
       by a fraction of a second on a later poll can land just above the watermark and re-insert the same
       entry as a fresh row - a near-duplicate a moment after the original. Issue #2749: this duplicate
       pattern (two samples ~200ms-1s apart, then a real ~60s gap to the next) collapses
       TimeSeriesGaps.BreakAtGaps's median-derived threshold to sub-second, breaking the CPU chart's line
       at every genuine interval and leaving only the tiny intra-duplicate dash. Millisecond arithmetic
       keeps the full offset, so recomputing the same entry on a later poll reproduces the same instant.
       Issue #2755: passing the full elapsed-ticks offset straight to DATEADD(MILLISECOND, ...) overflows
       once it exceeds int range (~24.8 days of milliseconds) on server uptimes past that point - hit in
       production on a long-uptime box within minutes of #2749's fix shipping. Split into a SECOND-scale
       DATEADD (safely inside int range for realistic uptimes, ~68 years) plus a MILLISECOND-scale DATEADD
       for the 0-999 remainder: same result as the single-step version (verified bit-identical against a
       live SQL Server across the full delta range, including values past the int boundary), because no
       intermediate value ever leaves int range.

       SYSUTCDATETIME(), not SYSDATETIME(): this value is STORED, and every naive timestamp in the store
       is UTC. The local-time version put sample_time one UTC offset behind the collection_time written
       by the very same run -- measured at exactly 4h on 42 of 42 us-east-2 servers, whose collectors
       were reporting SUCCESS the whole time, so nothing surfaced it. It also disagreed with this
       collector's OWN Azure arm, which takes sys.dm_db_resource_stats.end_time and is already UTC: the
       same column arrived in two different frames depending on the target. Nothing downstream can
       recover the frame from the value, because the offset is the deployment's, not the row's, and it
       moves with DST. */
    sample_time = DATEADD(
        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),
        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSUTCDATETIME())),
    sqlserver_cpu_utilization = x.process_utilization,
    other_process_cpu_utilization =
        CASE
            WHEN @is_linux = 1 AND x.system_idle = 0
            THEN NULL
            WHEN (100 - x.system_idle - x.process_utilization) < 0
            THEN 0
            ELSE 100 - x.system_idle - x.process_utilization
        END
FROM
(
    SELECT
        dorb.timestamp,
        record = CONVERT(xml, dorb.record)
    FROM sys.dm_os_ring_buffers AS dorb
    WHERE dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
) AS t
CROSS APPLY
(
    SELECT
        process_utilization = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer'),
        system_idle = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer')
) AS x
/* Skip ring-buffer records lacking a complete SystemHealth block — their
   XML values extract as NULL and would store NULL samples (Issue #989). */
WHERE x.process_utilization IS NOT NULL
AND   x.system_idle IS NOT NULL
ORDER BY t.timestamp DESC
OPTION(RECOMPILE);";

    public override string Name => "cpu_utilization";

    public override string TargetTable => "cpu_utilization_stats";

    public override string? WatermarkColumn => "sample_time";

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("sample_time", CollectorColumnType.Timestamp),
        new CollectorColumn("sqlserver_cpu_utilization", CollectorColumnType.Integer),
        new CollectorColumn("other_process_cpu_utilization", CollectorColumnType.Integer),
    };

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        if (context.Target.IsAzureSqlDb && context.Watermark.HasValue)
        {
            /* Azure SQL DB: end_time is a real column, so the watermark filters server-side. */
            return new CollectorQuery(
                AzureSqlDbWatermarkedQueryText,
                new[] { new CollectorParameter("@last_sample_time", context.Watermark.Value, CollectorParameterType.DateTime2) });
        }

        return context.Target.IsAzureSqlDb
            ? new CollectorQuery(AzureSqlDbQueryText)
            : new CollectorQuery(RingBufferQueryText);
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var sampleTime = reader.GetDateTime(0);

            /* Client-side dedup for the ring buffer (computed sample_time can't be filtered in SQL). */
            if (!context.Target.IsAzureSqlDb && context.Watermark.HasValue && sampleTime <= context.Watermark.Value)
            {
                continue;
            }

            rows.Add(new Row(
                sampleTime,
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                /* NULL = host/other CPU not derivable (SystemIdle reported 0, Issue #1048) */
                reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.SampleTime)                  /* sample_time TIMESTAMP */
            .Value(row.SqlServerCpuUtilization)     /* sqlserver_cpu_utilization INTEGER */
            .Value(row.OtherProcessCpuUtilization); /* other_process_cpu_utilization INTEGER (nullable) */
    }
}
