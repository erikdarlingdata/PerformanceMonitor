/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the cross-SKU parity contract of the shared latch_stats collector definition (the
/// Dashboard-parity port of install/32_collect_latch_stats.sql): verbatim query text, payload
/// column order, ReadAsync mapping, and the delta groups/keys/gap policy. An intentional change to
/// any of these must consciously update these tests — that is the point.
/// </summary>
public sealed class LatchStatsCollectorDefinitionTests
{
    private const string ExpectedQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    latch_class = ls.latch_class,
    waiting_requests_count = ls.waiting_requests_count,
    wait_time_ms = ls.wait_time_ms,
    max_wait_time_ms = ls.max_wait_time_ms
FROM sys.dm_os_latch_stats AS ls
WHERE ls.wait_time_ms > 0
OPTION(RECOMPILE);";

    [Fact]
    public void Query_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());
        Assert.Equal(ExpectedQuery, LatchStatsCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(LatchStatsCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(LatchStatsCollector.Instance.WatermarkColumn);
        Assert.Equal("latch_stats", LatchStatsCollector.Instance.Name);
        Assert.Equal("latch_stats", LatchStatsCollector.Instance.TargetTable);
    }

    [Fact]
    public void AppliesTo_Everywhere_IncludingAzure()
    {
        Assert.True(LatchStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(LatchStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(LatchStatsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_AreInAppendOrder()
    {
        var names = LatchStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "latch_class",
                "waiting_requests_count",
                "wait_time_ms",
                "max_wait_time_ms",
                "delta_waiting_requests_count",
                "delta_wait_time_ms",
                "delta_max_wait_time_ms",
            },
            names);
    }

    [Fact]
    public async Task ReadAsync_MapsColumns()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "ACCESS_METHODS_HOBT_VIRTUAL_ROOT", 7L, 300L, 20L },
            new object[] { "BUFFER", 12L, 900L, 50L });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await LatchStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new LatchStatsCollector.Row("ACCESS_METHODS_HOBT_VIRTUAL_ROOT", 7, 300, 20), rows[0]);
        Assert.Equal(new LatchStatsCollector.Row("BUFFER", 12, 900, 50), rows[1]);
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_AndPinsDeltaGroupsKeysAndGapPolicy()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();
        var row = new LatchStatsCollector.Row("BUFFER", 7, 300, 20);

        LatchStatsCollector.Instance.WritePayload(row, writer, context);

        /* Payload order: raw values then the three deltas (recording calculator returns value * 10). */
        Assert.Equal(new object?[] { "BUFFER", 7L, 300L, 20L, 70L, 3000L, 200L }, writer.Values);

        /* Delta contract: group names, key = latch_class, the host collection time, the shared gap policy. */
        Assert.Equal(3, deltas.Calls.Count);
        Assert.Equal(("latch_stats_waiting_requests", "BUFFER", 7L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[0]);
        Assert.Equal(("latch_stats_wait_time", "BUFFER", 300L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[1]);
        Assert.Equal(("latch_stats_max_wait", "BUFFER", 20L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[2]);
        Assert.All(deltas.Calls, _ => Assert.Equal(42, deltas.LastServerId));
    }
}
