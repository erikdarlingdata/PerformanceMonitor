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
/// Pins the parity contract of the extracted memory_grant_stats definition: column mapping,
/// the composite "{pool}_{semaphore}" delta key, the two delta groups with the shared gap
/// policy, and the payload order matching the memory_grant_stats schema.
/// </summary>
public sealed class MemoryGrantsCollectorDefinitionTests
{
    [Fact]
    public void PayloadColumns_MatchSchemaOrder()
    {
        var names = MemoryGrantsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "resource_semaphore_id",
                "pool_id",
                "target_memory_mb",
                "max_target_memory_mb",
                "total_memory_mb",
                "available_memory_mb",
                "granted_memory_mb",
                "used_memory_mb",
                "grantee_count",
                "waiter_count",
                "timeout_error_count",
                "forced_grant_count",
                "timeout_error_count_delta",
                "forced_grant_count_delta",
                "sample_interval_seconds",
            },
            names);

        /* #3540 (v61): the interval is the TRAILING column, in the same INTEGER type perfmon_stats and
           query_stats have always used, so the two stores' positional writers land it after every
           pre-existing column and one NULLIF idiom reads every family. */
        var interval = MemoryGrantsCollector.Instance.PayloadColumns[^1];
        Assert.Equal("sample_interval_seconds", interval.Name);
        Assert.Equal(CollectorColumnType.Integer, interval.Type);
        Assert.Equal(
            PerfmonStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "sample_interval_seconds").Type,
            interval.Type);
    }

    [Fact]
    public void NameAndTable_MatchTheDispatchAndSchema()
    {
        Assert.Equal("memory_grant_stats", MemoryGrantsCollector.Instance.Name);
        Assert.Equal("memory_grant_stats", MemoryGrantsCollector.Instance.TargetTable);
        Assert.Contains("sys.dm_exec_query_resource_semaphores", MemoryGrantsCollector.Instance.BuildQuery(CollectorTestContext.Make(new RecordingCollectorDeltaCalculator())).Text, System.StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_MapsAllColumns()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { (short)0, 2, 100.5m, 200.5m, 90.25m, 80.75m, 10.5m, 8.25m, 3, 1, 5L, 2L });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await MemoryGrantsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(new MemoryGrantsCollector.Row(0, 2, 100.5m, 200.5m, 90.25m, 80.75m, 10.5m, 8.25m, 3, 1, 5L, 2L), row);
    }

    [Fact]
    public void WritePayload_EmitsSchemaOrder_AndPinsCompositeDeltaKey()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var writer = new RecordingCollectorRowWriter();
        var context = CollectorTestContext.Make(deltas);
        var row = new MemoryGrantsCollector.Row(1, 2, 100.5m, 200.5m, 90.25m, 80.75m, 10.5m, 8.25m, 3, 4, 5L, 6L);

        MemoryGrantsCollector.Instance.WritePayload(row, writer, context);

        /* Payload order: raw values, the two deltas (recording calculator returns value * 10), then the
           measured interval (#3540, v61) — the fake reports 0, the calculator's "no delta knowable" marker. */
        Assert.Equal(
            new object?[] { (short)1, 2, 100.5m, 200.5m, 90.25m, 80.75m, 10.5m, 8.25m, 3, 4, 5L, 6L, 50L, 60L, 0 },
            writer.Values);

        /* Delta contract: composite key "{pool}_{semaphore}", both groups, the shared gap policy. */
        Assert.Equal(2, deltas.Calls.Count);
        Assert.Equal(("memory_grants_timeouts", "2_1", 5L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[0]);
        Assert.Equal(("memory_grants_forced", "2_1", 6L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[1]);
    }

    /// <summary>
    /// #3540 (v61): the interval reaches the payload MEASURED, not as a constant. A distinctive value (neither
    /// 0 nor a plausible cadence) so this can only pass if what the calculator reported is what was written.
    /// </summary>
    [Fact]
    public void WritePayload_WritesTheMeasuredInterval()
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 137 };
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();

        MemoryGrantsCollector.Instance.WritePayload(new MemoryGrantsCollector.Row(1, 2, 100.5m, 200.5m, 90.25m, 80.75m, 10.5m, 8.25m, 3, 4, 5L, 6L), writer, context);

        Assert.Equal(137, writer.Values[^1]);
    }

    /// <summary>
    /// #3540: one interval per ROW, the MINIMUM over the row's two delta groups. If either group's delta is
    /// unknowable (interval 0) the row is stored as (…, 0), so no reader divides a reset counter's 0 by its
    /// sibling's real interval and reads it as a quiet semaphore.
    /// </summary>
    [Fact]
    public void WritePayload_StoresTheMinimumIntervalAcrossTheRowsDeltaGroups()
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 300 };
        deltas.IntervalByGroup["memory_grants_forced"] = 0;
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();

        MemoryGrantsCollector.Instance.WritePayload(new MemoryGrantsCollector.Row(1, 2, 100.5m, 200.5m, 90.25m, 80.75m, 10.5m, 8.25m, 3, 4, 5L, 6L), writer, context);

        Assert.Equal(0, writer.Values[^1]);
        Assert.Equal(2, deltas.Calls.Count);
    }
}
