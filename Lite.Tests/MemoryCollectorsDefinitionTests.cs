/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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

    private static int s_nextServer = 7_000;

    /* The collector remembers raw ring buffer timestamps per server, so each read-sequence gets its own server id. */
    private static int NewServer() => Interlocked.Increment(ref s_nextServer);

    private static readonly DateTime s_bootAnchor = new(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// One query row: sample_time, notification, the two indicators, the raw ring buffer timestamp, then the
    /// instance start time, then the memory node (NULL when none is given, #7691). Without an explicit raw value it is the stamp's offset from a fixed anchor, so rows in
    /// one read keep their real order and spacing (what the query returns, since both come from the same
    /// ms_ticks). Without an explicit start time it is <see cref="s_startTime"/>.
    /// </summary>
    private static object[] Rec(object stamp, string notification, int process, int system, long? raw = null, DateTime? startTime = null, int? node = null)
        => new object[]
        {
            stamp, notification, process, system,
            raw ?? (stamp is DateTime d ? (long)(d - s_bootAnchor).TotalMilliseconds : 0L),
            startTime ?? s_startTime,
            node is { } n ? n : DBNull.Value,
        };

    private static readonly DateTime s_startTime = new(2026, 5, 20, 8, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime s_held = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private const long HeldRaw = 5_000_000_000;

    /// <summary>
    /// A read of the ring buffer as the query returns it: the held record (raw <see cref="HeldRaw"/>, first stored at
    /// <see cref="s_held"/>) and an older record <paramref name="olderByMs"/> before it, both stamped
    /// <paramref name="skew"/> later than they were first stored, which is how the shared clock drift shows.
    /// </summary>
    private static FakeCollectorDataReader HeldAndOlderRead(int olderByMs, TimeSpan skew)
        => new(
            Rec(s_held.AddMilliseconds(-olderByMs) + skew, "RESOURCE_MEM_STEADY", 0, 0, HeldRaw - olderByMs),
            Rec(s_held + skew, "RESOURCE_MEMPHYSICAL_LOW", 2, 0, HeldRaw));

    private static async Task<List<MemoryPressureEventsCollector.Row>> PollAsync(FakeCollectorDataReader reader, int serverId, DateTime? watermark)
        => await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: serverId), CancellationToken.None);

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
            new[] { "sample_time", "memory_notification", "memory_indicators_process", "memory_indicators_system", "memory_node_id" },
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
            Rec(watermark.AddMinutes(-5), "RESOURCE_MEMPHYSICAL_LOW", 2, 0),
            Rec(DBNull.Value, "RESOURCE_MEMPHYSICAL_LOW", 3, 1),
            Rec(watermark.AddMinutes(5), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer()), CancellationToken.None);

        /* Older-than-watermark and NULL-sample_time (DateTime.MinValue) rows are both skipped. */
        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", row.MemoryNotification);

        var writer = new RecordingCollectorRowWriter();
        MemoryPressureEventsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { watermark.AddMinutes(5), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0, null }, writer.Values);
    }

    [Fact]
    public async Task ReadAsync_TwoRecordsAtOneTimestampOnTwoNodes_AreBothKept_AndBothWrittenWithTheirNode()
    {
        /* #7691: a server with several memory nodes writes one ring buffer record per node for the same event, to the 100 ns of sample_time.
           Both rows are real records, so both are kept, and the node is what tells them apart. */
        var stamp = new DateTime(2026, 7, 1, 19, 7, 58, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            Rec(stamp, "RESOURCE_MEMPHYSICAL_LOW", 2, 0, node: 0),
            Rec(stamp, "RESOURCE_MEMPHYSICAL_LOW", 2, 0, node: 1),
            Rec(stamp.AddMinutes(1), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, serverId: NewServer()), CancellationToken.None);

        Assert.Equal(3, rows.Count);
        Assert.Equal(new int?[] { 0, 1, null }, rows.Select(r => r.NodeId).ToArray());

        var written = rows.Select(row =>
        {
            var writer = new RecordingCollectorRowWriter();
            MemoryPressureEventsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
            return writer.Values[^1];
        }).ToArray();
        Assert.Equal(new object?[] { 0, 1, null }, written);
    }

    [Fact]
    public void BuildQuery_ReadsTheMemoryNodeFromTheRecord_LastSoTheOtherPositionsStay()
    {
        var plan = MemoryPressureEventsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));

        Assert.Contains("memory_node_id = t.record.value('(/Record/ResourceMonitor/NodeId)[1]', 'integer')", plan.Text, StringComparison.Ordinal);
        Assert.True(
            plan.Text.IndexOf("start_time = @start_time", StringComparison.Ordinal) < plan.Text.IndexOf("memory_node_id =", StringComparison.Ordinal),
            "the node is the last column, after the start time ReadAsync reads at position 5");
    }

    [Fact]
    public async Task ReadAsync_NoWatermark_KeepsNullSampleTimeRows_AsMinValue()
    {
        using var reader = new FakeCollectorDataReader(
            Rec(DBNull.Value, "RESOURCE_MEMPHYSICAL_LOW", 2, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, serverId: NewServer()), CancellationToken.None);

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
            Rec(watermark.AddMilliseconds(skewMs), "RESOURCE_MEM_STEADY", 0, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer()), CancellationToken.None);

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
                Rec(firstStamp.AddMilliseconds(18.0 * poll), "RESOURCE_MEM_STEADY", 0, 0, raw: 1_000_000));

            var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
                reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer()), CancellationToken.None);

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
            Rec(watermark.AddMinutes(-9) + skew, "RESOURCE_MEM_STEADY", 0, 0),
            Rec(watermark + skew, "RESOURCE_MEM_STEADY", 0, 0),
            Rec(watermark.AddMinutes(4) + skew, "RESOURCE_MEMPHYSICAL_LOW", 2, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer()), CancellationToken.None);

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
            Rec(watermark + skew, "RESOURCE_MEM_STEADY", 0, 0),
            Rec(watermark.AddMilliseconds(5) + skew, "RESOURCE_MEMPHYSICAL_LOW", 2, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer()), CancellationToken.None);

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
            Rec(watermark.AddHours(3), "RESOURCE_MEMPHYSICAL_LOW", 2, 0),
            Rec(watermark.AddHours(3).AddSeconds(40), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer()), CancellationToken.None);

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
            Rec(DBNull.Value, "RESOURCE_MEM_STEADY", 3, 1),
            Rec(watermark.AddMinutes(-30), "RESOURCE_MEM_STEADY", 0, 0),
            Rec(watermark.AddMinutes(11), "RESOURCE_MEMPHYSICAL_LOW", 2, 0));

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer()), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_LOW", row.MemoryNotification);
    }

    /// <summary>
    /// #7441: an older record 1 to 10 ms before the held one. Shared skew moves both rows, so the older row reads
    /// nearer the watermark than the held one and a nearest-row guess takes it for the held record. With the raw
    /// timestamp remembered, the held record is stored by the first poll and never again, however far the skew
    /// grows over 500 polls (9 s at 18 ms a poll).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task ReadAsync_OlderRecordJustBeforeTheHeldOne_HeldRecordIsStoredOnce(int olderByMs)
    {
        var server = NewServer();
        DateTime? watermark = null;
        var held = 0;
        var older = 0;
        for (var poll = 0; poll < 500; poll++)
        {
            using var reader = HeldAndOlderRead(olderByMs, TimeSpan.FromMilliseconds(18.0 * poll));
            var rows = await PollAsync(reader, server, watermark);

            held += rows.Count(r => r.MemoryNotification == "RESOURCE_MEMPHYSICAL_LOW");
            older += rows.Count(r => r.MemoryNotification == "RESOURCE_MEM_STEADY");
            if (rows.Count > 0)
            {
                watermark = rows.Max(r => r.SampleTime);
            }
        }

        Assert.Equal(1, held);
        Assert.Equal(1, older);
    }

    /// <summary>
    /// #7441: the cold start that memory cannot help with. The service has just started, the store holds the
    /// held record, and an older record sits 1 to 10 ms before it, so the first read takes the older row for the
    /// held one and stores the held record once more. That is the whole cost: from the second read on the raw
    /// timestamp is remembered and nothing is stored again over 500 polls. Before the fix the held record was
    /// stored on every one of them.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task ReadAsync_ColdStartWithAnOlderRecordClose_CostsOneRepeatThenNothing(int olderByMs)
    {
        var server = NewServer();
        var watermark = s_held;
        var held = 0;
        var older = 0;
        for (var poll = 1; poll <= 500; poll++)
        {
            using var reader = HeldAndOlderRead(olderByMs, TimeSpan.FromMilliseconds(18.0 * poll));
            var rows = await PollAsync(reader, server, watermark);

            held += rows.Count(r => r.MemoryNotification == "RESOURCE_MEMPHYSICAL_LOW");
            older += rows.Count(r => r.MemoryNotification == "RESOURCE_MEM_STEADY");
            if (rows.Count > 0)
            {
                watermark = rows.Max(r => r.SampleTime);
            }
        }

        Assert.Equal(1, held);
        Assert.Equal(0, older);
    }

    /// <summary>
    /// #7441: a read whose write failed leaves the watermark where it was, so the next read must hand the same
    /// rows back. The raw value is trusted only once the watermark has moved past the read that returned it.
    /// </summary>
    [Fact]
    public async Task ReadAsync_WriteFailed_RowsComeBackUntilTheWatermarkMoves()
    {
        var server = NewServer();

        var first = await PollAsync(HeldAndOlderRead(5, TimeSpan.Zero), server, watermark: null);
        var retry = await PollAsync(HeldAndOlderRead(5, TimeSpan.FromMilliseconds(18)), server, watermark: null);
        Assert.Equal(2, first.Count);
        Assert.Equal(2, retry.Count);

        var stored = retry.Max(r => r.SampleTime);
        var after = await PollAsync(HeldAndOlderRead(5, TimeSpan.FromMilliseconds(36)), server, stored);
        Assert.Empty(after);
    }

    /// <summary>
    /// #7441: a restart or reboot makes the raw timestamps a new, smaller series. Remembered values from the old
    /// series would skip the new records, so a changed instance start time clears them, and the nearest-row guess
    /// is not made either (no held record survives a restart): both new records, 3 and 4 minutes after the
    /// watermark, are kept. A reboot moves the computed boot instant by the whole old uptime, so the old
    /// boot-instant test caught this case as well: this one guards the new start-time test, it does not fail
    /// on the old code.
    /// </summary>
    [Fact]
    public async Task ReadAsync_ServerRestarted_EveryNewRowIsKept()
    {
        var server = NewServer();
        var first = await PollAsync(HeldAndOlderRead(5, TimeSpan.Zero), server, watermark: null);
        var watermark = first.Max(r => r.SampleTime);

        var restartedAt = s_startTime.AddDays(30);
        using var afterRestart = new FakeCollectorDataReader(
            Rec(watermark.AddMinutes(3), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0, raw: 1_000, startTime: restartedAt),
            Rec(watermark.AddMinutes(4), "RESOURCE_MEMPHYSICAL_LOW", 2, 0, raw: 2_000, startTime: restartedAt));
        var rows = await PollAsync(afterRestart, server, watermark);

        Assert.Equal(2, rows.Count);
    }

    /// <summary>
    /// #7441: a VM suspend or an NTP step moves SYSUTCDATETIME() forward, so every computed sample_time jumps by
    /// the same amount while the instance start time and the raw timestamps do not. The jump here is 5 minutes,
    /// past the watermark by more than the gap between the two records. Neither record is stored again. The
    /// earlier version read the moved boot instant as a restart, dropped its memory and stored both.
    /// </summary>
    [Fact]
    public async Task ReadAsync_ClockJumpsForwardWithSameStartTime_StoresNoRepeats()
    {
        var server = NewServer();
        var first = await PollAsync(HeldAndOlderRead(2_000, TimeSpan.Zero), server, watermark: null);
        Assert.Equal(2, first.Count);
        var watermark = first.Max(r => r.SampleTime);

        var jump = TimeSpan.FromMinutes(5);
        var afterJump = await PollAsync(HeldAndOlderRead(2_000, jump), server, watermark);
        Assert.Empty(afterJump);

        /* A genuinely new record is still kept after the jump. */
        using var withNew = new FakeCollectorDataReader(
            Rec(s_held.AddMilliseconds(-2_000) + jump, "RESOURCE_MEM_STEADY", 0, 0, HeldRaw - 2_000),
            Rec(s_held + jump, "RESOURCE_MEMPHYSICAL_LOW", 2, 0, HeldRaw),
            Rec(s_held.AddMinutes(1) + jump, "RESOURCE_MEMPHYSICAL_HIGH", 0, 0, HeldRaw + 60_000));
        var kept = Assert.Single(await PollAsync(withNew, server, watermark));
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", kept.MemoryNotification);
    }

    /// <summary>
    /// #7441: a quiet server (one record in the ring buffer, nothing new) polled 500 times by the host's loop,
    /// with the clock stepping forward 10 minutes at poll 100 and again at poll 300, stores that record once.
    /// </summary>
    [Fact]
    public async Task ReadAsync_QuietServerWithClockSteps_StoresNothingAfterTheFirstRead()
    {
        var server = NewServer();
        DateTime? watermark = null;
        var stored = 0;
        for (var poll = 0; poll < 500; poll++)
        {
            var step = (poll >= 100 ? TimeSpan.FromMinutes(10) : TimeSpan.Zero) + (poll >= 300 ? TimeSpan.FromMinutes(10) : TimeSpan.Zero);
            using var reader = new FakeCollectorDataReader(
                Rec(s_held + step + TimeSpan.FromMilliseconds(18.0 * poll), "RESOURCE_MEM_STEADY", 0, 0, HeldRaw));

            var rows = await PollAsync(reader, server, watermark);
            stored += rows.Count;
            if (rows.Count > 0)
            {
                watermark = rows.Max(r => r.SampleTime);
            }
        }

        Assert.Equal(1, stored);
    }

    /// <summary>
    /// #7441: with memory, a record newer than the held one is kept even when it sits within the skew of it,
    /// where a time test could not tell them apart.
    /// </summary>
    [Fact]
    public async Task ReadAsync_NewRecordRightAfterTheHeldOne_IsKeptByRawIdentity()
    {
        var server = NewServer();
        var first = await PollAsync(HeldAndOlderRead(5, TimeSpan.Zero), server, watermark: null);
        var watermark = first.Max(r => r.SampleTime);
        var skew = TimeSpan.FromSeconds(2);

        using var reader = new FakeCollectorDataReader(
            Rec(s_held.AddMilliseconds(-5) + skew, "RESOURCE_MEM_STEADY", 0, 0, HeldRaw - 5),
            Rec(s_held + skew, "RESOURCE_MEMPHYSICAL_LOW", 2, 0, HeldRaw),
            Rec(s_held.AddMilliseconds(3) + skew, "RESOURCE_MEMPHYSICAL_HIGH", 0, 0, HeldRaw + 3));
        var rows = await PollAsync(reader, server, watermark);

        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", row.MemoryNotification);
    }

    /* #8981: what the host does around a collector, in one place. A restart is a fresh server id (the memory of the
       collector is per process and keyed by it), the watermark is the newest stored stamp, and the state the previous
       process left comes back through the context. The host saves the state only after the rows are written. */
    private sealed class RestartingHost
    {
        public readonly List<MemoryPressureEventsCollector.Row> Stored = new();
        public readonly Dictionary<string, string> State = new();

        public async Task<List<MemoryPressureEventsCollector.Row>> RestartAndReadAsync(IEnumerable<object[]> ring)
        {
            var context = CollectorTestContext.Make(
                s_deltas,
                watermark: Stored.Count == 0 ? null : Stored.Max(r => r.SampleTime),
                serverId: NewServer(),
                state: new Dictionary<string, string>(State));
            using var reader = new FakeCollectorDataReader(ring.ToArray());
            var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
            Stored.AddRange(rows);
            foreach (var (key, value) in context.PendingState)
            {
                State[key] = value;
            }

            return rows;
        }
    }

    private static readonly DateTime s_boot = new(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// A ring buffer record as a read at <paramref name="readTime"/> returns it. The stamp is SYSUTCDATETIME() minus the ticks
    /// since the record, and the two clocks run about 60 ppm apart, so the same record reads higher the later after boot it
    /// is read: a second read 95 days after the first stamps it about 8 minutes higher.
    /// </summary>
    private static object[] RingRecord(DateTime eventTime, DateTime readTime, string notification = "RESOURCE_MEMPHYSICAL_LOW", int process = 2)
        => Rec(eventTime + TimeSpan.FromTicks((long)((readTime - s_boot).Ticks * 60e-6)), notification, process, 0, (long)(eventTime - s_boot).TotalMilliseconds);

    /// <summary>
    /// #8981: a quiet server whose last burst of memory pressure notifications (40, 15 s apart) was stored in August. After
    /// a service restart in November the ring buffer returns each of them about 8 minutes above the stamp it was stored
    /// with (60 ppm of 95 days). The watermark is the stamp of the newest one, so every record of the burst that sat within
    /// 8 minutes of the newest now reads ABOVE the watermark. The cold-start guess spares the one row nearest the watermark and
    /// stored the rest again, months-old records with a new sample_time. With the position persisted, none of them is stored
    /// again, and the two records that really are new, 10 s apart and right behind them in the buffer, are stored once.
    /// </summary>
    [Fact]
    public async Task ReadAsync_RestartMonthsAfterABurst_StoresNoneOfItAgain_AndTheNewRecordsOnce()
    {
        var host = new RestartingHost();
        var burstStart = new DateTime(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc);
        var burst = Enumerable.Range(0, 40).Select(i => burstStart.AddSeconds(15 * i)).ToArray();

        var firstRead = new DateTime(2026, 8, 19, 13, 0, 0, DateTimeKind.Utc);
        var stored = await host.RestartAndReadAsync(burst.Select(t => RingRecord(t, firstRead)));
        Assert.Equal(40, stored.Count);

        var secondRead = new DateTime(2026, 11, 14, 5, 35, 0, DateTimeKind.Utc);
        var again = await host.RestartAndReadAsync(burst.Select(t => RingRecord(t, secondRead)));
        Assert.Empty(again);

        var fresh = new[] { new DateTime(2026, 11, 14, 4, 0, 0, DateTimeKind.Utc), new DateTime(2026, 11, 14, 4, 0, 10, DateTimeKind.Utc) };
        var thirdRead = new DateTime(2026, 11, 15, 14, 17, 0, DateTimeKind.Utc);
        var kept = await host.RestartAndReadAsync(burst.Concat(fresh).Select(t => RingRecord(t, thirdRead)));
        Assert.Equal(2, kept.Count);

        var fourthRead = new DateTime(2026, 11, 17, 16, 22, 0, DateTimeKind.Utc);
        Assert.Empty(await host.RestartAndReadAsync(burst.Concat(fresh).Select(t => RingRecord(t, fourthRead))));
        Assert.Equal(42, host.Stored.Count);
    }

    /// <summary>
    /// #8981: one old record and a neighbour 150 ms before it, read after twelve service restarts a few hours apart. Each read
    /// stamps both 0.1 to 0.2 s higher than the one before (the copies of the October record in the field differ by that much).
    /// Each is stored once.
    /// </summary>
    [Fact]
    public async Task ReadAsync_TwelveRestartsOverOneOldRecord_StoreItOnce()
    {
        var host = new RestartingHost();
        var held = new DateTime(2026, 10, 14, 9, 30, 0, DateTimeKind.Utc);
        var neighbour = held.AddMilliseconds(-150);

        for (var restart = 0; restart < 13; restart++)
        {
            var read = new DateTime(2026, 10, 14, 10, 0, 0, DateTimeKind.Utc).AddHours(4 * restart);
            await host.RestartAndReadAsync(new[] { RingRecord(neighbour, read, "RESOURCE_MEM_STEADY", 0), RingRecord(held, read) });
        }

        Assert.Equal(2, host.Stored.Count);
    }

    /// <summary>
    /// #8981: a restart of the SQL Server instance makes the raw timestamps a new, smaller series. The position saved before it
    /// must not hide the new records, which are all kept.
    /// </summary>
    [Fact]
    public async Task ReadAsync_SqlServerRestartedBetweenServiceRestarts_KeepsEveryNewRecord()
    {
        var host = new RestartingHost();
        var event1 = new DateTime(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc);
        await host.RestartAndReadAsync(new[] { RingRecord(event1, new DateTime(2026, 8, 19, 13, 0, 0, DateTimeKind.Utc)) });

        var newStart = s_startTime.AddDays(60);
        var newest = host.Stored.Max(r => r.SampleTime);
        var rows = await host.RestartAndReadAsync(new[]
        {
            Rec(newest.AddMinutes(3), "RESOURCE_MEMPHYSICAL_HIGH", 0, 0, raw: 1_000, startTime: newStart),
            Rec(newest.AddMinutes(4), "RESOURCE_MEMPHYSICAL_LOW", 2, 0, raw: 2_000, startTime: newStart),
        });

        Assert.Equal(2, rows.Count);
    }

    /// <summary>
    /// #8981: a store that lost the rows the saved position stands for (no watermark) gets them back. The position counts as
    /// stored only when the watermark has moved past the read that returned it.
    /// </summary>
    [Fact]
    public async Task ReadAsync_SavedPositionButTheStoreHoldsNothing_ReturnsTheRowsAgain()
    {
        var host = new RestartingHost();
        var burstStart = new DateTime(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc);
        var ring = Enumerable.Range(0, 5).Select(i => RingRecord(burstStart.AddSeconds(15 * i), burstStart.AddHours(1))).ToArray();
        Assert.Equal(5, (await host.RestartAndReadAsync(ring)).Count);

        host.Stored.Clear();
        Assert.Equal(5, (await host.RestartAndReadAsync(ring)).Count);
    }

    /// <summary>#8981: a saved value that is not ours (an edited row, an older format) is no state. The read does not throw and dedupes as it did before the state existed.</summary>
    [Theory]
    [InlineData("")]
    [InlineData("garbage")]
    [InlineData("1|2|3")]
    [InlineData("x|1||")]
    [InlineData("99999999999999999999|1||")]
    public async Task ReadAsync_UnreadableSavedPosition_FallsBackToTheColdStartGuess(string saved)
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            Rec(watermark.AddMilliseconds(8), "RESOURCE_MEM_STEADY", 0, 0),
            Rec(watermark.AddMinutes(20), "RESOURCE_MEMPHYSICAL_LOW", 2, 0));
        var state = new Dictionary<string, string> { [MemoryPressureEventsCollector.MemoryStateKey] = saved };

        var rows = await MemoryPressureEventsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, watermark: watermark, serverId: NewServer(), state: state), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("RESOURCE_MEMPHYSICAL_LOW", row.MemoryNotification);
    }
}
