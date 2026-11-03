/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7153: both sample accumulators drain before their first await. A write that fails or is cancelled after the drain
/// (a stop cancels the hourly sweep's linked token) must put the samples back, merged with anything recorded since, so the
/// stop-time flush still has them. A closed connection fails the INSERT after the drain without needing a store.
/// </summary>
public class SampleFlushFailureKeepsSamplesTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadLatency_AWriteThatFailsAfterTheDrain_LeavesTheSamplesForTheNextFlush(bool cancelled)
    {
        var acc = new ReadLatencyAccumulator();
        acc.Record(ReadSurface.Web, "plan", ReadOutcome.Ok, 40);
        acc.Record(ReadSurface.Web, "plan", ReadOutcome.Ok, 900);
        acc.Record(ReadSurface.Mcp, "waits", ReadOutcome.Timeout, 5_000);

        using var cts = new CancellationTokenSource();
        if (cancelled)
        {
            await cts.CancelAsync();
        }

        await using var closed = new NpgsqlConnection();
        await Assert.ThrowsAnyAsync<Exception>(() => acc.FlushAsync(closed, DateTime.UtcNow, null, cts.Token));

        Assert.True(acc.HasSamples);
        var rows = acc.Drain();
        Assert.Equal(2, rows.Count);
        var plan = rows.Single(r => r.Route == "plan");
        Assert.Equal(2, plan.Count);
        Assert.Equal(940, plan.TotalMs);
        Assert.Equal(900, plan.MaxMs);
        Assert.Equal(2, plan.BucketCounts.Sum());
        Assert.Equal(1, rows.Single(r => r.Route == "waits").Count);
        Assert.False(acc.HasSamples);
    }

    [Fact]
    public void ReadLatency_Restore_MergesWithSamplesRecordedSinceTheDrain_CountingEachOnce()
    {
        var acc = new ReadLatencyAccumulator();
        acc.Record(ReadSurface.Web, "plan", ReadOutcome.Ok, 40, ReadWindowBand.None, 3, 7, "a");
        acc.Record(ReadSurface.Web, "plan", ReadOutcome.Ok, 900, ReadWindowBand.None, 5, 11, "b");
        var drained = acc.Drain();

        /* A read for the same key lands between the drain and the failed write. */
        acc.Record(ReadSurface.Web, "plan", ReadOutcome.Ok, 2_000, ReadWindowBand.None, 4, 13, "c");
        acc.Restore(drained);

        var row = Assert.Single(acc.Drain());
        Assert.Equal(3, row.Count);
        Assert.Equal(2_940, row.TotalMs);
        Assert.Equal(2_000, row.MaxMs);
        Assert.Equal(3, row.BucketCounts.Sum());
        Assert.Equal(5, row.MaxInFlight);
        Assert.Equal(31, row.WaitTotalMs);
        Assert.Equal(2, row.DistinctCallers);
        Assert.Empty(acc.Drain());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CollectorCost_AWriteThatFailsAfterTheDrain_LeavesTheSamplesForTheNextFlush(bool cancelled)
    {
        var acc = new CollectorCostAccumulator();
        acc.Record(1, "query_store", rows: 100, sqlMs: 500, storageMs: 10);
        acc.Record(1, "query_store", rows: 200, sqlMs: 900, storageMs: 20);
        acc.Record(2, "deadlocks", rows: 3, sqlMs: 50, storageMs: 1);

        using var cts = new CancellationTokenSource();
        if (cancelled)
        {
            await cts.CancelAsync();
        }

        await using var closed = new NpgsqlConnection();
        await Assert.ThrowsAnyAsync<Exception>(() => acc.FlushAsync(closed, DateTime.UtcNow, null, cts.Token));

        Assert.True(acc.HasSamples);
        var rows = acc.Drain();
        Assert.Equal(2, rows.Count);
        var qs = rows.Single(r => r.CollectorName == "query_store");
        Assert.Equal(2, qs.RunCount);
        Assert.Equal(1_400, qs.TotalSqlMs);
        Assert.Equal(900, qs.MaxSqlMs);
        Assert.Equal(30, qs.TotalStorageMs);
        Assert.Equal(300, qs.TotalRows);
        Assert.False(acc.HasSamples);
    }

    [Fact]
    public void CollectorCost_Restore_MergesWithSamplesRecordedSinceTheDrain_CountingEachOnce()
    {
        var acc = new CollectorCostAccumulator();
        acc.Record(1, "query_store", 100, 500, 10);
        var drained = acc.Drain();

        acc.Record(1, "query_store", 50, 900, 5);
        acc.Restore(drained);

        var row = Assert.Single(acc.Drain());
        Assert.Equal(2, row.RunCount);
        Assert.Equal(1_400, row.TotalSqlMs);
        Assert.Equal(900, row.MaxSqlMs);
        Assert.Equal(15, row.TotalStorageMs);
        Assert.Equal(150, row.TotalRows);
        Assert.Empty(acc.Drain());
    }
}
