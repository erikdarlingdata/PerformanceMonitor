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
/// Pins the cross-SKU parity contract of the extracted wait_stats collector definition
/// (headless plan v5.1): verbatim query text, ignored-wait filtering, delta groups/keys/gap
/// policy, and payload column order. An intentional change to any of these must consciously
/// update these tests — that is the point.
/// </summary>
public sealed class WaitStatsCollectorDefinitionTests
{
    private const string ExpectedQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    wait_type = ws.wait_type,
    waiting_tasks_count = ws.waiting_tasks_count,
    wait_time_ms = ws.wait_time_ms,
    signal_wait_time_ms = ws.signal_wait_time_ms
FROM sys.dm_os_wait_stats AS ws
WHERE ws.wait_time_ms > 0
OPTION(RECOMPILE);";

    /* #3653 A5: the instance identity, as the batch's SECOND result set after the parity payload — the CPU
       carrier's exact column names, read straight from the DMV. Not on Azure SQL DB (below). */
    private const string ExpectedIdentitySet = @"

SELECT
    server_start_time = dosi.sqlserver_start_time,
    server_name = @@SERVERNAME
FROM sys.dm_os_sys_info AS dosi;";

    /// <summary>
    /// The payload SELECT is the verbatim parity contract and comes FIRST, so the rows land on ordinals 0-3
    /// exactly as before; the identity set is appended after it (#3653 A5), so ReadAsync's NextResult lands
    /// on it once the rows are drained. The Azure SQL DB batch is the pre-#3653 text byte for byte: neither
    /// SQL Server carrier reads the pair there (#3694's ruling), so the two carriers observe under one rule.
    /// </summary>
    [Fact]
    public void Query_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());
        Assert.Equal(ExpectedQuery + ExpectedIdentitySet, WaitStatsCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(WaitStatsCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(WaitStatsCollector.Instance.WatermarkColumn);

        var azure = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator(), isAzureSqlDb: true);
        Assert.Equal(ExpectedQuery, WaitStatsCollector.Instance.BuildQuery(azure).Text);
        Assert.DoesNotContain("dm_os_sys_info", WaitStatsCollector.Instance.BuildQuery(azure).Text, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_AreInAppendOrder()
    {
        var names = WaitStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "wait_type",
                "waiting_tasks_count",
                "wait_time_ms",
                "signal_wait_time_ms",
                "delta_waiting_tasks",
                "delta_wait_time_ms",
                "delta_signal_wait_time_ms",
                "sample_interval_seconds",
            },
            names);

        /* #3540: the interval is the TRAILING column, in the same INTEGER type perfmon_stats and query_stats
           have always used, so the two stores' positional writers land it after every pre-existing column. */
        var interval = WaitStatsCollector.Instance.PayloadColumns[^1];
        Assert.Equal("sample_interval_seconds", interval.Name);
        Assert.Equal(CollectorColumnType.Integer, interval.Type);
        Assert.Equal(
            PerfmonStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "sample_interval_seconds").Type,
            interval.Type);
    }

    [Fact]
    public async Task ReadAsync_MapsColumns_AndFiltersIgnoredWaitTypes()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "SOS_SCHEDULER_YIELD", 10L, 200L, 150L },
            new object[] { "SLEEP_TASK", 5L, 100L, 50L },
            new object[] { "PAGEIOLATCH_SH", 7L, 300L, 20L });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator(), ignored: new[] { "SLEEP_TASK" });

        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new WaitStatsCollector.Row("SOS_SCHEDULER_YIELD", 10, 200, 150), rows[0]);
        Assert.Equal(new WaitStatsCollector.Row("PAGEIOLATCH_SH", 7, 300, 20), rows[1]);
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_AndPinsDeltaGroupsKeysAndGapPolicy()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();
        var row = new WaitStatsCollector.Row("PAGEIOLATCH_SH", 7, 300, 20);

        WaitStatsCollector.Instance.WritePayload(row, writer, context);

        /* Payload order: raw values, the three deltas (recording calculator returns value * 10), then the
           measured interval (#3540) — the fake reports 0, the calculator's "no delta knowable" marker. */
        Assert.Equal(new object?[] { "PAGEIOLATCH_SH", 7L, 300L, 20L, 70L, 3000L, 200L, 0 }, writer.Values);

        /* Delta contract: group names, key = wait_type, the host collection time, the shared gap policy. */
        Assert.Equal(3, deltas.Calls.Count);
        Assert.Equal(("wait_stats_tasks", "PAGEIOLATCH_SH", 7L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[0]);
        Assert.Equal(("wait_stats_time", "PAGEIOLATCH_SH", 300L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[1]);
        Assert.Equal(("wait_stats_signal", "PAGEIOLATCH_SH", 20L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[2]);
        Assert.All(deltas.Calls, _ => Assert.Equal(42, deltas.LastServerId));
    }

    /// <summary>
    /// #3540: the interval reaches the payload MEASURED, not as a constant. A distinctive value (neither 0
    /// nor a plausible cadence) so this can only pass if what the calculator reported is what was written.
    /// </summary>
    [Fact]
    public void WritePayload_WritesTheMeasuredInterval()
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 137 };
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();

        WaitStatsCollector.Instance.WritePayload(new WaitStatsCollector.Row("PAGEIOLATCH_SH", 7, 300, 20), writer, context);

        Assert.Equal(137, writer.Values[^1]);
    }

    /// <summary>
    /// #3540: one interval per ROW, the MINIMUM over the row's delta groups. If any one group's delta is
    /// unknowable (interval 0) the row is stored as (…, 0) so no reader divides a reset counter's 0 by a
    /// sibling's real interval and reads it as idle. The three groups share a key and a collection time, so
    /// they disagree only on an independent single-counter reset — which the minimum is for.
    /// </summary>
    [Fact]
    public void WritePayload_StoresTheMinimumIntervalAcrossTheRowsDeltaGroups()
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 300 };
        deltas.IntervalByGroup["wait_stats_signal"] = 0;
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();

        WaitStatsCollector.Instance.WritePayload(new WaitStatsCollector.Row("PAGEIOLATCH_SH", 7, 300, 20), writer, context);

        Assert.Equal(0, writer.Values[^1]);
        Assert.Equal(3, deltas.Calls.Count);
    }
}

/// <summary>Shared context factory for collector-definition tests.</summary>
internal static class CollectorTestContext
{
    public static CollectorContext Make(
        ICollectorDeltaCalculator deltas,
        IEnumerable<string>? ignored = null,
        bool isAzureSqlDb = false,
        DateTime? watermark = null,
        bool capturePlanXml = false)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            Watermark = watermark,
            IgnoredWaitTypes = new HashSet<string>(ignored ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase),
            CapturePlanXml = capturePlanXml,
        };
}
