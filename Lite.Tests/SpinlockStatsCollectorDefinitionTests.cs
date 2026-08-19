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
/// Pins the cross-SKU parity contract of the shared spinlock_stats collector definition (the
/// Dashboard-parity port of install/33_collect_spinlock_stats.sql): verbatim query text, payload
/// column order, ReadAsync mapping (spins_per_collision is a real/float read via GetFloat), and the
/// delta groups/keys/gap policy — spins_per_collision is a computed ratio and gets no delta.
/// </summary>
public sealed class SpinlockStatsCollectorDefinitionTests
{
    private const string ExpectedQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    spinlock_name = ss.name,
    collisions = ss.collisions,
    spins = ss.spins,
    spins_per_collision = ss.spins_per_collision,
    sleep_time = ss.sleep_time,
    /* backoffs is int in sys.dm_os_spinlock_stats on SQL 2016/2017 and was widened to bigint on
       2019+. CONVERT so the wire type is always bigint and ReadAsync's GetInt64 doesn't throw an
       Int32->Int64 InvalidCast on the older versions (collisions/spins/sleep_time are bigint on
       every supported version, so they need no cast). */
    backoffs = CONVERT(bigint, ss.backoffs)
FROM sys.dm_os_spinlock_stats AS ss
WHERE ss.collisions > 0
OR    ss.spins > 0
OPTION(RECOMPILE);";

    [Fact]
    public void Query_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());
        Assert.Equal(ExpectedQuery, SpinlockStatsCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(SpinlockStatsCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(SpinlockStatsCollector.Instance.WatermarkColumn);
        Assert.Equal("spinlock_stats", SpinlockStatsCollector.Instance.Name);
        Assert.Equal("spinlock_stats", SpinlockStatsCollector.Instance.TargetTable);
    }

    [Fact]
    public void AppliesTo_Everywhere_IncludingAzure()
    {
        Assert.True(SpinlockStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(SpinlockStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(SpinlockStatsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_AreInAppendOrder()
    {
        var names = SpinlockStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "spinlock_name",
                "collisions",
                "spins",
                "spins_per_collision",
                "sleep_time",
                "backoffs",
                "delta_collisions",
                "delta_spins",
                "delta_sleep_time",
                "delta_backoffs",
            },
            names);

        /* spins_per_collision is the DMV's real ratio — mapped to a double, not a delta. */
        var spinsPerCollision = SpinlockStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "spins_per_collision");
        Assert.Equal(CollectorColumnType.Double, spinsPerCollision.Type);
    }

    [Fact]
    public async Task ReadAsync_MapsColumns_IncludingRealSpinsPerCollision()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "SOS_SUSPEND_QUEUE", 100L, 5000L, 2.5f, 10L, 3L },
            new object[] { "LOCK_HASH", 40L, 800L, 20.0f, 0L, 1L });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await SpinlockStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new SpinlockStatsCollector.Row("SOS_SUSPEND_QUEUE", 100, 5000, 2.5, 10, 3), rows[0]);
        Assert.Equal(new SpinlockStatsCollector.Row("LOCK_HASH", 40, 800, 20.0, 0, 1), rows[1]);
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_AndPinsDeltaGroupsKeysAndGapPolicy()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();
        var row = new SpinlockStatsCollector.Row("SOS_SUSPEND_QUEUE", 100, 5000, 2.5, 10, 3);

        SpinlockStatsCollector.Instance.WritePayload(row, writer, context);

        /* Payload order: raw values (spins_per_collision passed through as a double) then the four
           deltas (recording calculator returns value * 10). */
        Assert.Equal(
            new object?[] { "SOS_SUSPEND_QUEUE", 100L, 5000L, 2.5, 10L, 3L, 1000L, 50000L, 100L, 30L },
            writer.Values);

        /* Delta contract: group names, key = spinlock_name, host collection time, the shared gap policy.
           No delta call for spins_per_collision — exactly four calls. */
        Assert.Equal(4, deltas.Calls.Count);
        Assert.Equal(("spinlock_stats_collisions", "SOS_SUSPEND_QUEUE", 100L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[0]);
        Assert.Equal(("spinlock_stats_spins", "SOS_SUSPEND_QUEUE", 5000L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[1]);
        Assert.Equal(("spinlock_stats_sleep_time", "SOS_SUSPEND_QUEUE", 10L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[2]);
        Assert.Equal(("spinlock_stats_backoffs", "SOS_SUSPEND_QUEUE", 3L, context.CollectionTime, CollectorDeltaCalculator.DefaultMaxGapSeconds), deltas.Calls[3]);
        Assert.All(deltas.Calls, _ => Assert.Equal(42, deltas.LastServerId));
    }
}
