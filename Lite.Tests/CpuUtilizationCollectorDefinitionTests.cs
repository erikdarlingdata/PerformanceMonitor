/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted cpu_utilization definition: the three-way query
/// selection (ring buffer vs. Azure vs. Azure+watermark with its bound parameter), the
/// client-side ring-buffer dedup, and the Linux NULL other-process rule (#1048).
/// </summary>
public sealed class CpuUtilizationCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void BuildQuery_RingBuffer_ForNonAzure_EvenWithWatermark()
    {
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: DateTime.UtcNow));

        Assert.Contains("RING_BUFFER_SCHEDULER_MONITOR", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_os_host_info", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void BuildQuery_RingBuffer_SuppressesOtherCpu_OnlyWhenSystemIdleIsZero()
    {
        /* Some Linux/SQL Server version combos report a non-zero SystemIdle, so
           unconditionally nulling other-process CPU on @is_linux = 1 over-suppressed a
           derivable value. The guard must require BOTH @is_linux = 1 AND system_idle = 0. */
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false));

        Assert.Contains("WHEN @is_linux = 1 AND x.system_idle = 0", plan.Text, StringComparison.Ordinal);

        /* Normalize line endings first: on a CRLF checkout the template contains "= 1\r\n", which
           the "\n"-suffixed needle would never match, making this guard vacuously pass. */
        Assert.DoesNotContain("WHEN @is_linux = 1\n", plan.Text.Replace("\r\n", "\n"), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_RingBuffer_ComputesSampleTimeAtMillisecondPrecision_NotSecondTruncated()
    {
        /* #2749: DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), ...) discarded the sub-second
           remainder of the elapsed-ticks offset, and that remainder differs on every poll (SYSDATETIME()
           keeps moving; a given ring-buffer entry's own timestamp does not). Since this query re-reads
           the ring buffer's whole retained history every cycle and dedups client-side on this COMPUTED
           value, a later poll's recomputed sample_time for the SAME physical entry could drift forward by
           a fraction of a second, land just above the watermark, and re-insert a near-duplicate row a
           moment after the original -- which collapsed the CPU chart's TimeSeriesGaps median threshold to
           sub-second and broke the line at every genuine ~1-minute interval, leaving only dots. */
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false));

        Assert.Contains("MILLISECOND, -((@ms_ticks - t.timestamp) % 1000)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_RingBuffer_SplitsSampleTimeMath_ToAvoidMillisecondDateaddOverflow()
    {
        /* #2755: a single DATEADD(MILLISECOND, -(@ms_ticks - t.timestamp), ...) -- the #2749 fix's shape --
           overflows once the elapsed-ticks offset exceeds int range (~24.8 days of milliseconds), which a
           long-uptime box hit in production within minutes of that fix shipping ("Arithmetic overflow error
           converting expression to data type int"). Splitting into a SECOND-scale DATEADD (safely inside int
           range for realistic uptimes, ~68 years) plus a MILLISECOND-scale DATEADD for the 0-999 remainder
           preserves the exact same instant -- verified bit-identical against a live SQL Server across the
           full delta range, including negative deltas and values past the int boundary -- without ever
           passing an out-of-range value to any single DATEADD call. */
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false));

        Assert.Contains("DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSUTCDATETIME())", plan.Text, StringComparison.Ordinal);
        Assert.Contains(
            "DATEADD(\n        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),\n        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSUTCDATETIME())),",
            plan.Text.Replace("\r\n", "\n"),
            StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_NoWatermark_UsesTop60()
    {
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("sys.dm_db_resource_stats", plan.Text, StringComparison.Ordinal);
        Assert.Contains("TOP (60)", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void BuildQuery_Azure_WithWatermark_FiltersServerSide_AndBindsParameter()
    {
        var watermark = new DateTime(2026, 7, 1, 11, 55, 0, DateTimeKind.Utc);
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermark));

        Assert.Contains("WHERE drs.end_time > @last_sample_time", plan.Text, StringComparison.Ordinal);
        var parameter = Assert.Single(plan.Parameters);
        Assert.Equal("@last_sample_time", parameter.Name);
        Assert.Equal(watermark, parameter.Value);
        Assert.Equal(CollectorParameterType.DateTime2, parameter.Type);
    }

    [Fact]
    public void WatermarkColumn_IsSampleTime_AndNamesMatchDispatchAndSchema()
    {
        Assert.Equal("sample_time", CpuUtilizationCollector.Instance.WatermarkColumn);
        Assert.Equal("cpu_utilization", CpuUtilizationCollector.Instance.Name);
        Assert.Equal("cpu_utilization_stats", CpuUtilizationCollector.Instance.TargetTable);
        Assert.Equal(
            new[] { "sample_time", "sqlserver_cpu_utilization", "other_process_cpu_utilization" },
            CpuUtilizationCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task ReadAsync_RingBuffer_DedupsAtOrBelowWatermark()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark.AddSeconds(60), 50, 10 },
            new object[] { watermark, 40, 5 },
            new object[] { watermark.AddSeconds(-60), 30, 2 });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader,
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: watermark),
            CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(new CpuUtilizationCollector.Row(watermark.AddSeconds(60), 50, 10), row);
    }

    [Fact]
    public async Task ReadAsync_Azure_DoesNotDedupClientSide()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark, 40, 0 });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader,
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermark),
            CancellationToken.None);

        Assert.Single(rows);
    }

    [Fact]
    public async Task ReadAsync_NullOtherProcess_StaysNull_LinuxRule()
    {
        var sample = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { sample, 55, DBNull.Value });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.OtherProcessCpuUtilization);

        var writer = new RecordingCollectorRowWriter();
        CpuUtilizationCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { sample, 55, null }, writer.Values);
    }
}
