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

/// <summary>Pins the parity contracts of the batch-12 definitions.</summary>
public sealed class PerfmonStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void DefaultCounterList_IsTheCuratedParityContract()
    {
        Assert.Equal(61, PerfmonStatsCollector.DefaultCounters.Count);
        Assert.Contains("Batch Requests/sec", PerfmonStatsCollector.DefaultCounters);
        Assert.Contains("Number of Deadlocks/sec", PerfmonStatsCollector.DefaultCounters);
        Assert.Contains("Wait for the worker", PerfmonStatsCollector.DefaultCounters);

        /* #991: the two SQLServer:Database Replica counters that carry the PRIMARY side of AG commit
           latency. Their ratio (delay per mirrored transaction) is the number sync-commit conversations
           are actually about, and neither name occurs on any other perfmon object, so the collector's
           counter_name-only filter is safe without an object_name predicate. */
        Assert.Contains("Transaction Delay", PerfmonStatsCollector.DefaultCounters);
        Assert.Contains("Mirrored Write Transactions/sec", PerfmonStatsCollector.DefaultCounters);
    }

    [Fact]
    public void BuildQuery_UsesDefaults_EscapedAsLiterals()
    {
        var text = PerfmonStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;

        Assert.Contains("N'Batch Requests/sec'", text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_os_performance_counters", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_OverrideWins_AndEscapesQuotes()
    {
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            PerfmonCounterOverride = new[] { "O'Brien/sec" },
        };

        var text = PerfmonStatsCollector.Instance.BuildQuery(context).Text;

        Assert.Contains("N'O''Brien/sec'", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Batch Requests/sec", text, StringComparison.Ordinal);
    }

    /// <summary>The query selects the DMV's <c>cntr_type</c> (V132 / v62) and the payload declares it LAST —
    /// both stores' writers are positional and an upgraded store receives the column by ALTER TABLE, which
    /// can only land at the end. Seven payload columns; the interval stays sixth.</summary>
    [Fact]
    public void BuildQuery_SelectsTheCounterType_AndThePayloadDeclaresItLast()
    {
        var text = PerfmonStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;
        Assert.Contains("cntr_type = pc.cntr_type", text, StringComparison.Ordinal);

        var columns = PerfmonStatsCollector.Instance.PayloadColumns;
        Assert.Equal(7, columns.Count);
        Assert.Equal(new[] { "object_name", "counter_name", "instance_name", "cntr_value", "delta_cntr_value", "sample_interval_seconds", "cntr_type" },
            columns.Select(c => c.Name));
        Assert.Equal(CollectorColumnType.Integer, columns[^1].Type);
        Assert.Equal(CollectorColumnType.BigInt, columns.Single(c => c.Name == "cntr_value").Type);
    }

    /// <summary>A RATE row (<c>PERF_COUNTER_BULK_COUNT</c>) keeps the pre-rung write — raw value, the calculator's
    /// delta, the MEASURED interval — plus the type as the seventh value. The interval is distinctive,
    /// deliberately neither 0 nor the 60 this collector used to hard-code, so the assertion can only pass if
    /// the MEASURED value is what gets written (#2234).</summary>
    [Fact]
    public async Task WritePayload_RateRow_DeltaAndMeasuredInterval_ThenTheType()
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 137 };
        var context = CollectorTestContext.Make(deltas);
        using var reader = new FakeCollectorDataReader(
            new object[] { "SQLServer:SQL Statistics", "Batch Requests/sec", "", 987654L, 272696576 });

        var rows = await PerfmonStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var writer = new RecordingCollectorRowWriter();
        PerfmonStatsCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(new object?[] { "SQLServer:SQL Statistics", "Batch Requests/sec", "", 987654L, 9876540L, 137, 272696576 }, writer.Values);
        var call = Assert.Single(deltas.Calls);
        Assert.Equal(("perfmon", "SQLServer:SQL Statistics|Batch Requests/sec|", 987654L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), call);
    }

    /// <summary>
    /// A GAUGE row (<c>PERF_COUNTER_LARGE_RAWCOUNT</c>, and the 32-bit <c>PERF_COUNTER_RAWCOUNT</c>) is written as its
    /// level: the raw value, NULL delta, NULL interval, the type — and the delta calculator is NOT called, so a
    /// falling level can never present to it as a counter reset (#3653 A7, #3540 "a falling gauge = fake counter
    /// reset"). NULL rather than (0, 0): 0 is the calculator's "no delta knowable" marker, a claim about a count,
    /// and a gauge has no delta to know. The measurement contract's rule 8, "gauges are never delta'd", made
    /// executable.
    /// </summary>
    [Theory]
    [InlineData(65792)]
    [InlineData(65536)]
    public async Task WritePayload_GaugeRow_WritesTheLevelWithNullDeltaAndNullInterval_AndNeverCallsTheCalculator(int gaugeType)
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 137 };
        var context = CollectorTestContext.Make(deltas);
        using var reader = new FakeCollectorDataReader(
            new object[] { "SQLServer:Memory Manager", "Total Server Memory (KB)", "", 8_388_608L, gaugeType });

        var rows = await PerfmonStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var writer = new RecordingCollectorRowWriter();
        PerfmonStatsCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(new object?[] { "SQLServer:Memory Manager", "Total Server Memory (KB)", "", 8_388_608L, null, null, gaugeType }, writer.Values);
        Assert.Empty(deltas.Calls);
        Assert.True(PerfmonStatsCollector.IsGauge(gaugeType));
    }

    /// <summary>The average/fraction/base family (<c>PERF_AVERAGE_BULK</c> here — the wait-statistics counters'
    /// <c>Average wait time (ms)</c> instance) is NOT a gauge and keeps the pre-rung write: its delta is a real
    /// per-interval change of the raw numerator, and the stored type is what tells a reader not to divide it
    /// into the average the instance name promises. Stated finding of the rung, not something it fixes.</summary>
    [Fact]
    public async Task WritePayload_AverageBulkRow_IsDifferencedLikeARate_AndCarriesItsType()
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 299 };
        var context = CollectorTestContext.Make(deltas);
        using var reader = new FakeCollectorDataReader(
            new object[] { "SQLServer:Wait Statistics", "Lock waits", "Average wait time (ms)", 4242L, 1073874176 });

        var rows = await PerfmonStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var writer = new RecordingCollectorRowWriter();
        PerfmonStatsCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(new object?[] { "SQLServer:Wait Statistics", "Lock waits", "Average wait time (ms)", 4242L, 42420L, 299, 1073874176 }, writer.Values);
        Assert.Single(deltas.Calls);
        Assert.False(PerfmonStatsCollector.IsGauge(1073874176));
    }
}

public sealed class DmvBlockingSnapshotCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void SyntheticMonitorLoop_IsNegativeSecondsSince2020()
    {
        var t = new DateTime(2020, 1, 1, 0, 0, 30, DateTimeKind.Utc);
        Assert.Equal(-30, DmvBlockingSnapshotCollector.SyntheticMonitorLoop(t));
        Assert.True(DmvBlockingSnapshotCollector.SyntheticMonitorLoop(new DateTime(2026, 7, 2, 0, 0, 0, DateTimeKind.Utc)) < 0);
    }

    [Fact]
    public void NameIsSingular_TableIsPlural_AndQueryPinsTheWaitFloors()
    {
        Assert.Equal("dmv_blocking_snapshot", DmvBlockingSnapshotCollector.Instance.Name);
        Assert.Equal("dmv_blocking_snapshots", DmvBlockingSnapshotCollector.Instance.TargetTable);

        var query = DmvBlockingSnapshotCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;
        Assert.Contains("WHEN wt.wait_type LIKE N'LCK[_]%'             THEN 2000", query, StringComparison.Ordinal);
        Assert.Contains("WHEN wt.wait_type LIKE N'PAGELATCH[_]%'       THEN 500", query, StringComparison.Ordinal);
        Assert.Contains("WHEN wt.wait_type LIKE N'RESOURCE_SEMAPHORE%' THEN 5000", query, StringComparison.Ordinal);
        Assert.Contains("sys.dm_os_waiting_tasks", query, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder_21Columns()
    {
        var names = DmvBlockingSnapshotCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.Equal(21, names.Length);
        Assert.Equal("monitor_loop", names[0]);
        Assert.Equal("event_time", names[1]);
        Assert.Equal("blocking_client_app", names[20]);
    }

    [Fact]
    public async Task WritePayload_EmitsSyntheticLoop_AndEventTime()
    {
        var context = CollectorTestContext.Make(s_deltas);
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "StackOverflow", 71, 0, DBNull.Value, 55, 0, DBNull.Value, 4500L,
                "X", "sleeping", "[dbo].[Orders]", "UPDATE ...", "BEGIN TRAN ...",
                "app_user", "web01", "MyApp", "admin", "jump01", "SSMS",
            });

        var rows = await DmvBlockingSnapshotCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var writer = new RecordingCollectorRowWriter();
        DmvBlockingSnapshotCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(21, writer.Values.Count);
        Assert.Equal(DmvBlockingSnapshotCollector.SyntheticMonitorLoop(context.CollectionTime), writer.Values[0]);
        Assert.Equal(context.CollectionTime, writer.Values[1]);
        Assert.Equal("[dbo].[Orders]", writer.Values[12]);
        Assert.Equal("SSMS", writer.Values[20]);
    }
}
