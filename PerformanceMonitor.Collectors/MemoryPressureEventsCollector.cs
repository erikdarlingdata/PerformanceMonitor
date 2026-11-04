/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Memory pressure notifications from RING_BUFFER_RESOURCE_MONITOR (the sp_pressuredetector
/// source): IndicatorsProcess/IndicatorsSystem (0-1 normal, 2 medium, 3+ severe) plus the
/// notification type. Does not apply to Azure SQL DB (sys.dm_os_ring_buffers is not exposed).
/// Client-side dedup — the computed sample_time cannot be filtered server-side, so ReadAsync
/// skips rows by the watermark and by the raw ring buffer timestamp it remembers per server (#7441).
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
    memory_indicators_system = t.record.value('(/Record/ResourceMonitor/IndicatorsSystem)[1]', 'integer'),
    /* The raw ring buffer timestamp (ms since the server started). It does not move with the clocks that
       skew sample_time, so ReadAsync uses it as the record's identity (#7441). */
    ring_timestamp = t.timestamp
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
    /// #7441: how far the record the watermark holds may sit from its stored stamp, as seen in one read, when
    /// this process has no memory of the server yet (the first read after a service start). The query derives
    /// <c>sample_time</c> on every poll from <c>SYSUTCDATETIME()</c> minus <c>ms_ticks - timestamp</c>, two clocks
    /// that run slightly apart, so the newest stored record comes back above its stored stamp (1 to 18 ms per
    /// 5-minute poll, about 60 ppm) and a strict <c>&lt;= watermark</c> test stored it again on every poll. The
    /// store keeps no record id and a fixed tolerance only delays the repeat: a skipped row never moves the
    /// watermark, so its gap to the watermark is the skew since it was first stored, and that grows on every poll.
    ///
    /// <para>Memory is the exact answer, and this bound is only the cold-start guess. The query also returns each
    /// row's raw ring buffer <c>timestamp</c>, which does not move with the clocks, and <see cref="ReadAsync"/>
    /// remembers the highest raw value it has seen stored per server (<see cref="ReadMemory"/>).</para>
    ///
    /// <para>With no memory the read can only guess, and the guess is the row nearest the watermark by absolute
    /// gap. That is NOT exact: shared skew moves every row in a read the same way, so it is right about which
    /// row is nearest only when no older record sits within twice the skew of the held one. An older record
    /// 1 ms before the held one, with 18 ms of skew, reads 17 ms from the watermark against 18 ms for the held
    /// record, so the older row is taken for the held one and the held record is stored once more. That
    /// repeat is the whole remaining cost: the stored record then carries the new stamp, memory is set from this
    /// read, and the next read skips by raw value, which skew cannot move. The bound also stops the nearest row
    /// being taken for the held record after the held record has aged out of the ring buffer and only newer rows
    /// remain; the cost there is one genuinely new record dropped when the oldest remaining row is within this
    /// bound of the watermark. 10 minutes is what 60 ppm builds up in about 116 days.
    /// <see cref="CpuUtilizationCollector"/> keeps its own fixed tolerance (#5644) because its records arrive
    /// every minute, so its watermark moves.</para>
    /// </summary>
    internal static readonly TimeSpan HeldRecordMaxSkew = TimeSpan.FromMinutes(10);

    /// <summary>
    /// How far the server's boot instant (the row's <c>sample_time</c> minus its raw <c>timestamp</c>) may move
    /// between two reads before the read memory is cleared as a restart. Boot drifts by the same 60 ppm skew
    /// (18 ms per 5-minute poll), so a minute is far past drift. A false restart (a suspended VM resumed) costs
    /// one cold-start guess, and a missed restart would skip new records, so this errs towards clearing.
    /// </summary>
    internal static readonly TimeSpan RestartTolerance = TimeSpan.FromMinutes(1);

    /// <summary>
    /// What the last read left for a server. <c>Boot</c> is that read's boot instant. <c>Confirmed</c> is the
    /// highest raw ring buffer timestamp known to be stored or dropped for good (every row at or below it is
    /// skipped). <c>Pending</c> is the highest raw timestamp the last read handed to the host to store: it
    /// becomes <c>Confirmed</c> only when the next read sees the watermark past <c>PendingBasis</c> (the
    /// watermark the last read ran under), because the host moves the watermark only after a write succeeds, and
    /// a read whose write failed must find its rows again.
    /// </summary>
    private sealed record ReadMemory(DateTime Boot, long? Confirmed, long? Pending, DateTime? PendingBasis);

    private readonly ConcurrentDictionary<int, ReadMemory> _memory = new();

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var all = new List<(Row Row, bool NullStamp, long Raw)>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var nullStamp = reader.IsDBNull(0);
            all.Add((new Row(
                nullStamp ? DateTime.MinValue : reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3)), nullStamp, reader.IsDBNull(4) ? 0 : reader.GetInt64(4)));
        }

        var watermark = context.Watermark;
        _memory.TryGetValue(context.ServerId, out var memory);

        /* The last read's rows count as stored only if the watermark has moved since: the host advances it after
           a successful write, so an unmoved watermark means the write failed (or nothing was handed over) and
           those rows have to come back. */
        if (memory?.Pending is { } pending)
        {
            var stored = watermark is { } w && (memory.PendingBasis is not { } basis || w > basis);
            memory = memory with
            {
                Confirmed = stored ? Math.Max(memory.Confirmed ?? long.MinValue, pending) : memory.Confirmed,
                Pending = null,
                PendingBasis = null,
            };
        }

        /* The boot instant is the same for every row in a read and moves only by drift, so a jump is a restart:
           the raw timestamps are a new series and the remembered ones mean nothing. */
        DateTime? readBoot = null;
        foreach (var (row, nullStamp, raw) in all)
        {
            if (!nullStamp)
            {
                readBoot = row.SampleTime.AddMilliseconds(-raw);
                break;
            }
        }

        var restarted = false;
        if (memory is not null && readBoot is { } boot && (boot - memory.Boot).Duration() > RestartTolerance)
        {
            memory = null;
            restarted = true;
        }

        var confirmed = memory?.Confirmed;
        var filtering = watermark.HasValue || confirmed.HasValue;
        var skipUpTo = watermark ?? DateTime.MinValue;

        /* Cold start: no memory of this server, so the held record can only be guessed. The row nearest the
           watermark is taken as the held record, with every earlier row (see HeldRecordMaxSkew for why this is
           a guess and what it costs). After a restart there is no held record in the ring buffer at all, so
           nothing is guessed. A NULL stamp is skipped, as before. On a tie the earlier row is the held one: that
           keeps a possibly new record rather than risk dropping it. The watermark and the stamps are both UTC
           (the column is stored from SYSUTCDATETIME(), see @now above). */
        if (confirmed is null && !restarted && watermark is { } cold)
        {
            var nearestGap = TimeSpan.MaxValue;
            var nearest = DateTime.MinValue;
            foreach (var (row, nullStamp, _) in all)
            {
                if (nullStamp)
                {
                    continue;
                }

                var gap = (row.SampleTime - cold).Duration();
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
        }

        /* With memory a row is skipped by raw timestamp, which skew cannot move. The stamp test stays as well: a
           row at or before the watermark is already stored whatever the memory says. Rows come in ring buffer
           order, so the skipped ones are always a prefix. */
        var rows = new List<Row>();
        long? skippedMax = null;
        long? keptMax = null;
        foreach (var (row, nullStamp, raw) in all)
        {
            if (!filtering)
            {
                rows.Add(row);
                if (!nullStamp)
                {
                    keptMax = keptMax is { } k1 && k1 > raw ? k1 : raw;
                }

                continue;
            }

            if (nullStamp)
            {
                continue;
            }

            if (row.SampleTime > skipUpTo && (confirmed is not { } c || raw > c))
            {
                rows.Add(row);
                keptMax = keptMax is { } k2 && k2 > raw ? k2 : raw;
            }
            else
            {
                skippedMax = skippedMax is { } s && s > raw ? s : raw;
            }
        }

        if (readBoot is { } newBoot)
        {
            var newConfirmed = confirmed;
            if (skippedMax is { } sm)
            {
                newConfirmed = confirmed is { } old ? Math.Max(old, sm) : sm;
            }

            _memory[context.ServerId] = new ReadMemory(newBoot, newConfirmed, keptMax, keptMax is null ? null : watermark);
        }
        else if (memory is not null)
        {
            _memory[context.ServerId] = memory;
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
