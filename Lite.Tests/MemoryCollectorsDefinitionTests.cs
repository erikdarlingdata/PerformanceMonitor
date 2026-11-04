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
        Assert.Contains("current_workers_count = CONVERT(integer, NULL)", azure, StringComparison.Ordinal);
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

    /// <summary>An Azure SQL Database's query returns NULL for the in-use worker count (it cannot read it). That NULL is stored as NULL,
    /// not as 0, so a reader can tell "not collected" from "no workers in use".</summary>
    [Fact]
    public async Task ReadAsync_KeepsANullInUseWorkerCountNull_NotZero()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { 64000m, 12000m, 8000m, 6000m, "Available", "Conventional", 48000m, 40000m, 30000m, 2000m, 704, DBNull.Value });

        var rows = await MemoryStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.CurrentWorkersCount);
        Assert.Equal(704, row.MaxWorkersCount);
        Assert.Equal("Available", row.SystemMemoryState);

        var writer = new RecordingCollectorRowWriter();
        MemoryStatsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(12, writer.Values.Count);
        Assert.Equal(64000m, writer.Values[0]);
        Assert.Equal(704, writer.Values[10]);
        Assert.Null(writer.Values[11]);
    }

    [Fact]
    public async Task ReadAsync_KeepsAnInUseWorkerCountAsRead_IncludingAGenuineZero()
    {
        foreach (var inUse in new[] { 0, 37 })
        {
            using var reader = new FakeCollectorDataReader(
                new object[] { 64000m, 12000m, 8000m, 6000m, "Available", "Conventional", 48000m, 40000m, 30000m, 2000m, 704, inUse });

            var rows = await MemoryStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

            var row = Assert.Single(rows);
            Assert.Equal(inUse, row.CurrentWorkersCount);

            var writer = new RecordingCollectorRowWriter();
            MemoryStatsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
            Assert.Equal(inUse, writer.Values[11]);
        }
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

        Assert.Contains("MILLISECOND, -((@ms_ticks - t.timestamp) % 1000)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_SplitsSampleTimeMath_ToAvoidMillisecondDateaddOverflow()
    {
        /* #2755 sibling fix -- see CpuUtilizationCollectorDefinitionTests' identical pin for the full
           explanation. This collector's own live production data was the one that actually overflowed:
           447/447 SUCCESS before the #2751 fix deployed, then 24/24 ERROR ("Arithmetic overflow error
           converting expression to data type int") within ~14 minutes after, on a real box. */
        var plan = MemoryPressureEventsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));

        Assert.Contains("DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), @now)", plan.Text, StringComparison.Ordinal);
        Assert.Contains(
            "DATEADD(\n        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),\n        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), @now)),",
            plan.Text.Replace("\r\n", "\n"),
            StringComparison.Ordinal);
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

    /// <summary>
    /// #7441: the ring buffer record that is already stored comes back on every poll with a stamp ABOVE the
    /// stored one (SYSUTCDATETIME() and ms_ticks drift apart). The host only stores kept rows, so the watermark
    /// stays at the record's FIRST stored stamp and the gap to it is the skew since then, which grows on every
    /// poll. The same record is replayed with a skew of 8 ms, 18 ms, 2 s, 30 s, 5 min and 9 min and is never
    /// stored again, so a fixed tolerance (the first version of this fix, 1 s) would have failed from the 2 s
    /// poll on.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(8)]
    [InlineData(18)]
    [InlineData(2_000)]
    [InlineData(30_000)]
    [InlineData(5 * 60_000)]
    [InlineData(9 * 60_000)]
    public async Task ReadAsync_HeldRecordReplayedWithGrowingSkew_IsNotStoredAgain(int skewMs)
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark.AddMilliseconds(skewMs), "RESOURCE_MEM_STEADY", 0, 0 });

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>
    /// #7441: the host's loop, replayed. Each poll re-reads the one record in the ring buffer 18 ms further from
    /// its first stored stamp, and the watermark is the newest stamp the earlier polls stored. 3,000 polls is
    /// 54 s of skew and 10 days of 5-minute polls, past both the 1 s and the 10 s a fixed tolerance would hold.
    /// The record is stored by the first poll only.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OneRecordPolledManyTimes_IsStoredOnce()
    {
        var firstStamp = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        DateTime? watermark = null;
        var stored = 0;
        for (var poll = 0; poll < 3_000; poll++)
        {
            using var reader = new FakeCollectorDataReader(
                new object[] { firstStamp.AddMilliseconds(18.0 * poll), "RESOURCE_MEM_STEADY", 0, 0 });

            var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
                reader, CollectorTestContext.Make(s_deltas, watermark: watermark), CancellationToken.None);

            stored += rows.Count;
            if (rows.Count > 0)
            {
                watermark = rows.Max(r => r.SampleTime);
            }
        }

        Assert.Equal(1, stored);
    }

    /// <summary>
    /// #7441: a record newer than the held one is kept whatever the skew on the held one, and the held record
    /// (older rows in the read) is not. The new record is 4 minutes after the held one, the read carries a
    /// 30 s skew.
    /// </summary>
    [Fact]
    public async Task ReadAsync_NewRecordAfterTheHeldOne_IsKeptUnderSkew()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var skew = TimeSpan.FromSeconds(30);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark.AddMinutes(-9) + skew, "RESOURCE_MEM_STEADY", 0, 0 },
            new object[] { watermark + skew, "RESOURCE_MEM_STEADY", 0, 0 },
            new object[] { watermark.AddMinutes(4) + skew, "RESOURCE_MEMPHYSICAL_LOW", 2, 0 });

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_LOW", row.MemoryNotification);
        Assert.Equal(watermark.AddMinutes(4) + skew, row.SampleTime);
    }

    /// <summary>
    /// #7441: two records 5 ms apart, the held one and a new one just after it, are told apart by identity.
    /// A time tolerance wide enough to cover the held record's skew would drop the new one.
    /// </summary>
    [Fact]
    public async Task ReadAsync_TwoCloseRecords_TheLaterOneNew_AreToldApart()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var skew = TimeSpan.FromSeconds(3);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark + skew, "RESOURCE_MEM_STEADY", 0, 0 },
            new object[] { watermark.AddMilliseconds(5) + skew, "RESOURCE_MEMPHYSICAL_LOW", 2, 0 });

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_LOW", row.MemoryNotification);
    }

    /// <summary>
    /// #7441: the held record has aged out of the ring buffer and only rows much newer than the watermark
    /// remain (all beyond the bound). None of them is the held record, so all are kept.
    /// </summary>
    [Fact]
    public async Task ReadAsync_HeldRecordAgedOut_NewerRowsAreKept()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark.AddHours(3), "RESOURCE_MEMPHYSICAL_LOW", 2, 0 },
            new object[] { watermark.AddHours(3).AddSeconds(40), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0 });

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark), CancellationToken.None);

        Assert.Equal(
            new[] { "RESOURCE_MEMPHYSICAL_LOW", "RESOURCE_MEMPHYSICAL_HIGH" },
            rows.Select(r => r.MemoryNotification).ToArray());
    }

    /// <summary>
    /// #7441: a skew past the bound (a suspended VM resumed) shows the held record as a new one once. It is
    /// stored again, and that is the one repeat the bound accepts. An older row than the watermark is still
    /// skipped, and so is a NULL stamp.
    /// </summary>
    [Fact]
    public async Task ReadAsync_SkewPastTheBound_StoresOnceAndOlderRowsAndNullStampStaySkipped()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { DBNull.Value, "RESOURCE_MEM_STEADY", 3, 1 },
            new object[] { watermark.AddMinutes(-30), "RESOURCE_MEM_STEADY", 0, 0 },
            new object[] { watermark.AddMinutes(11), "RESOURCE_MEMPHYSICAL_LOW", 2, 0 });

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_LOW", row.MemoryNotification);
    }
}
