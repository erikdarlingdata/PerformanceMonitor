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
/// Memory pressure notifications from RING_BUFFER_RESOURCE_MONITOR (the sp_pressuredetector
/// source): IndicatorsProcess/IndicatorsSystem (0-1 normal, 2 medium, 3+ severe) plus the
/// notification type. Does not apply to Azure SQL DB (sys.dm_os_ring_buffers is not exposed).
/// Client-side watermark dedup — the computed sample_time cannot be filtered server-side.
/// Extracted verbatim from Lite's RemoteCollectorService.Memory.cs.
/// </summary>
public sealed class MemoryPressureEventsCollector : CollectorDefinitionBase<MemoryPressureEventsCollector.Row>
{
    public static MemoryPressureEventsCollector Instance { get; } = new();

    private MemoryPressureEventsCollector()
    {
    }

    public readonly record struct Row(DateTime SampleTime, string MemoryNotification, int IndicatorsProcess, int IndicatorsSystem);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @ms_ticks bigint,
    /* SYSUTCDATETIME(), not SYSDATETIME(): @now is the base for a STORED sample_time, and both readers of
       memory_pressure_events.sample_time treat it as naive UTC -- Darling passes it to
       ViewerTimeHelper.ForDisplay, which takes naive-UTC input, and Lite windows it with the UTC-based
       GetTimeRange and adds UtcOffsetMinutes when plotting. A local clock here therefore put the whole
       memory-pressure series one UTC offset away from every other lane: measured at exactly 4h behind the
       collection_time written by the same collector run, on 42 of 42 us-east-2 servers, with the collector
       reporting SUCCESS throughout (#2932).

       Not the same answer as CpuUtilizationCollector, which shares this ring-buffer arithmetic and
       deliberately KEEPS SYSDATETIME(): its sample_time is documented as the monitored server's local wall
       clock and #1262 de-skews it in SQL per collection batch, so UTC there would break that correction and
       Lite's server-local window. The frame is a property of each column's readers, not of the store, and
       CollectorTimestampFrameTests pins both directions. */
    @now datetime2(7) = SYSUTCDATETIME();

SELECT @ms_ticks = dosi.ms_ticks FROM sys.dm_os_sys_info AS dosi;

SELECT
    /* MILLISECOND, not SECOND-truncated -- see CpuUtilizationCollector's identical fix (#2749) for the
       full explanation: dividing by 1000 before DATEADD(SECOND, ...) discards the sub-second remainder,
       which differs on every poll, so re-reading the same ring-buffer entry on a later cycle recomputes a
       slightly different sample_time and the client-side watermark dedup below can treat it as new.
       Split into SECOND + MILLISECOND-remainder DATEADD calls -- see CpuUtilizationCollector's identical
       fix (#2755) for the full explanation: a single DATEADD(MILLISECOND, ...) overflows once the offset
       exceeds int range (~24.8 days of ms), which this collector's wider ring-buffer retention window hit
       in production within minutes of the single-step version shipping. */
    sample_time = DATEADD(
        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),
        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), @now)),
    memory_notification = t.record.value('(/Record/ResourceMonitor/Notification)[1]', 'nvarchar(100)'),
    memory_indicators_process = t.record.value('(/Record/ResourceMonitor/IndicatorsProcess)[1]', 'integer'),
    memory_indicators_system = t.record.value('(/Record/ResourceMonitor/IndicatorsSystem)[1]', 'integer')
FROM
(
    SELECT
        dorb.timestamp,
        record = CONVERT(xml, dorb.record)
    FROM sys.dm_os_ring_buffers AS dorb
    WHERE dorb.ring_buffer_type = N'RING_BUFFER_RESOURCE_MONITOR'
) AS t
ORDER BY t.timestamp
OPTION(RECOMPILE);";

    public override string Name => "memory_pressure_events";

    public override string TargetTable => "memory_pressure_events";

    public override string? WatermarkColumn => "sample_time";

    /// <summary>Ring buffers are not exposed on Azure SQL DB — skip the cycle entirely.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("sample_time", CollectorColumnType.Timestamp),
        new CollectorColumn("memory_notification", CollectorColumnType.Varchar),
        new CollectorColumn("memory_indicators_process", CollectorColumnType.Integer),
        new CollectorColumn("memory_indicators_system", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var sampleTime = reader.IsDBNull(0) ? DateTime.MinValue : reader.GetDateTime(0);

            /* Client-side dedup: computed sample_time can't be filtered server-side. */
            if (context.Watermark.HasValue && sampleTime <= context.Watermark.Value)
            {
                continue;
            }

            rows.Add(new Row(
                sampleTime,
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.SampleTime)            /* sample_time TIMESTAMP */
            .Value(row.MemoryNotification)    /* memory_notification VARCHAR */
            .Value(row.IndicatorsProcess)     /* memory_indicators_process INTEGER */
            .Value(row.IndicatorsSystem);     /* memory_indicators_system INTEGER */
    }
}
