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

SELECT /* PerformanceMonitorLite */ @ms_ticks = dosi.ms_ticks FROM sys.dm_os_sys_info AS dosi;

SELECT /* PerformanceMonitorLite */
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

    /// <summary>#4197 part b: lets the host cache this collector's server-scoped watermark in memory.</summary>
    public override Func<Row, DateTime?>? WatermarkValueAccessor => static row => row.SampleTime;

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

    /// <summary>
    /// #7441: how far the record the watermark holds may sit from its stored stamp, as seen in one read, and
    /// still be recognised as that record. The query derives <c>sample_time</c> on every poll from
    /// <c>SYSUTCDATETIME()</c> minus <c>ms_ticks - timestamp</c>, two clocks that run slightly apart, so the
    /// newest stored record comes back above its stored stamp (1 to 18 ms per 5-minute poll, about 60 ppm) and
    /// a strict <c>&lt;= watermark</c> test stored it again on every poll. The store keeps no record id, so the
    /// watermark is the only memory, and a fixed tolerance only delays the repeat: a skipped row never moves
    /// the watermark, so its gap to the watermark is the skew since it was FIRST stored, and that grows on
    /// every poll (a 1 s tolerance is passed after about 56 polls at 18 ms).
    /// The skew does not matter within one read, where every row shares <c>@now</c> and <c>@ms_ticks</c> and the
    /// spacing between rows is exact. So <see cref="ReadAsync"/> finds the row nearest the watermark and treats
    /// it as the stored record, whatever the skew, as long as it is within this bound. 10 minutes is what 60 ppm
    /// builds up in about 116 days, longer than a quiet server's newest record sits in the ring buffer. A step
    /// past it (a suspended VM resumed, a clock set by hand) makes the held record look new once: it is stored
    /// again with its new stamp, the watermark follows it and the skew starts from zero, so that is one repeat,
    /// not a loop. The bound also stops the nearest row being taken for the held record after the held record
    /// has aged out of the ring buffer and only newer rows remain: those are then kept. The cost is one
    /// genuinely new record dropped when the held record aged out and the oldest remaining row is within 10
    /// minutes of it. <see cref="CpuUtilizationCollector"/> keeps its own fixed tolerance (#5644) because its
    /// records arrive every minute, so its watermark moves.
    /// </summary>
    internal static readonly TimeSpan HeldRecordMaxSkew = TimeSpan.FromMinutes(10);

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var all = new List<(Row Row, bool NullStamp)>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var nullStamp = reader.IsDBNull(0);
            all.Add((new Row(
                nullStamp ? DateTime.MinValue : reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3)), nullStamp));
        }

        if (!context.Watermark.HasValue)
        {
            return all.ConvertAll(static r => r.Row);
        }

        /* Client-side dedup: computed sample_time can't be filtered server-side. A stamp at or before the
           watermark is already stored. The record the watermark holds comes back with a skewed stamp (see
           HeldRecordMaxSkew), so also find the row nearest the watermark and skip it and every earlier row.
           The watermark and the stamps are both UTC (the column is stored from SYSUTCDATETIME(), see @now
           above). A skipped row is never stored, so the watermark the host caches (the max stored
           sample_time) does not move. A NULL stamp is skipped, as before. On a tie the earlier row is the held
           one: that keeps a possibly new record rather than risk dropping it. */
        var skipUpTo = context.Watermark.Value;
        var nearestGap = TimeSpan.MaxValue;
        var nearest = DateTime.MinValue;
        foreach (var (row, nullStamp) in all)
        {
            if (nullStamp)
            {
                continue;
            }

            var gap = (row.SampleTime - context.Watermark.Value).Duration();
            if (gap < nearestGap || (gap == nearestGap && row.SampleTime < nearest))
            {
                nearestGap = gap;
                nearest = row.SampleTime;
            }
        }

        if (nearestGap <= HeldRecordMaxSkew && nearest > skipUpTo)
        {
            skipUpTo = nearest;
        }

        var rows = new List<Row>();
        foreach (var (row, nullStamp) in all)
        {
            if (!nullStamp && row.SampleTime > skipUpTo)
            {
                rows.Add(row);
            }
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
