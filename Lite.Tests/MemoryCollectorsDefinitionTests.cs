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
/// Pins the parity contracts of the three memory collector definitions extracted from
/// RemoteCollectorService.Memory.cs (headless plan v5.1).
/// </summary>
public sealed class MemoryStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void BuildQuery_SelectsVariantByTarget()
    {
        var azure = MemoryStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)).Text;
        var onPrem = MemoryStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;

        /* Azure: committed-target approximation + NULL workers (#857 elastic-pool grant wall). */
        Assert.Contains("committed_target_kb", azure, StringComparison.Ordinal);
        Assert.Contains("current_workers_count = CONVERT(int, NULL)", azure, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.dm_os_schedulers", azure, StringComparison.Ordinal);

        /* On-prem/MI: real memory DMV + live worker count. */
        Assert.Contains("sys.dm_os_sys_memory", onPrem, StringComparison.Ordinal);
        Assert.Contains("sys.dm_os_schedulers", onPrem, StringComparison.Ordinal);
    }

    [Fact]
    public void NamesAndColumns_MatchDispatchAndSchema()
    {
        Assert.Equal("memory_stats", MemoryStatsCollector.Instance.Name);
        Assert.Equal("memory_stats", MemoryStatsCollector.Instance.TargetTable);
        Assert.Null(MemoryStatsCollector.Instance.WatermarkColumn);
        Assert.True(MemoryStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.Equal(
            new[]
            {
                "total_physical_memory_mb", "available_physical_memory_mb", "total_page_file_mb",
                "available_page_file_mb", "system_memory_state", "sql_memory_model",
                "target_server_memory_mb", "total_server_memory_mb", "buffer_pool_mb",
                "plan_cache_mb", "max_workers_count", "current_workers_count",
            },
            MemoryStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task ReadAsync_MapsSingleRow_WithNullDefaults()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { 64000m, 12000m, 8000m, 6000m, "Available", "Conventional", 48000m, 40000m, 30000m, 2000m, 704, DBNull.Value });

        var rows = await MemoryStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(0, row.CurrentWorkersCount);
        Assert.Equal("Available", row.SystemMemoryState);

        var writer = new RecordingCollectorRowWriter();
        MemoryStatsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(12, writer.Values.Count);
        Assert.Equal(64000m, writer.Values[0]);
        Assert.Equal(0, writer.Values[11]);
    }

    [Fact]
    public async Task ReadAsync_NoRow_YieldsNoRows()
    {
        using var reader = new FakeCollectorDataReader();

        var rows = await MemoryStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        Assert.Empty(rows);
    }
}

public sealed class MemoryClerksCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void NamesColumnsAndQuery_MatchSchema()
    {
        Assert.Equal("memory_clerks", MemoryClerksCollector.Instance.Name);
        Assert.Equal("memory_clerks", MemoryClerksCollector.Instance.TargetTable);
        var query = MemoryClerksCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;
        Assert.Contains("sys.dm_os_memory_clerks", query, StringComparison.Ordinal);
        Assert.Contains("TOP (25)", query, StringComparison.Ordinal);
        Assert.Equal(
            new[] { "clerk_type", "memory_mb" },
            MemoryClerksCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task ReadAndWrite_RoundTripInOrder()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "MEMORYCLERK_SQLBUFFERPOOL", 30000m },
            new object[] { "CACHESTORE_SQLCP", 1500m });

        var rows = await MemoryClerksCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        Assert.Equal(2, rows.Count);
        var writer = new RecordingCollectorRowWriter();
        MemoryClerksCollector.Instance.WritePayload(rows[0], writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { "MEMORYCLERK_SQLBUFFERPOOL", 30000m }, writer.Values);
    }
}

public sealed class MemoryPressureEventsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void AppliesTo_SkipsAzureSqlDb()
    {
        Assert.False(MemoryPressureEventsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(MemoryPressureEventsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = false }));
        Assert.Equal("sample_time", MemoryPressureEventsCollector.Instance.WatermarkColumn);
        Assert.Equal("memory_pressure_events", MemoryPressureEventsCollector.Instance.Name);
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder()
    {
        Assert.Equal(
            new[] { "sample_time", "memory_notification", "memory_indicators_process", "memory_indicators_system" },
            MemoryPressureEventsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public void BuildQuery_ComputesSampleTimeAtMillisecondPrecision_NotSecondTruncated()
    {
        /* #2749 sibling fix -- see CpuUtilizationCollectorDefinitionTests' identical pin for the full
           explanation. Same ring-buffer / client-side-watermark-dedup shape, same truncation bug. */
        var plan = MemoryPressureEventsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));

        Assert.Contains("DATEADD(MILLISECOND, -(@ms_ticks - t.timestamp), @now)", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/ 1000)", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("DATEADD(SECOND, -((@ms_ticks", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_DedupsAtWatermark_IncludingNullSampleTimes()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark.AddMinutes(-5), "RESOURCE_MEMPHYSICAL_LOW", 2, 0 },
            new object[] { DBNull.Value, "RESOURCE_MEMPHYSICAL_LOW", 3, 1 },
            new object[] { watermark.AddMinutes(5), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0 });

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark), CancellationToken.None);

        /* Older-than-watermark and NULL-sample_time (DateTime.MinValue) rows are both skipped. */
        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", row.MemoryNotification);

        var writer = new RecordingCollectorRowWriter();
        MemoryPressureEventsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { watermark.AddMinutes(5), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0 }, writer.Values);
    }

    [Fact]
    public async Task ReadAsync_NoWatermark_KeepsNullSampleTimeRows_AsMinValue()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { DBNull.Value, "RESOURCE_MEMPHYSICAL_LOW", 2, 0 });

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(DateTime.MinValue, row.SampleTime);
    }
}
