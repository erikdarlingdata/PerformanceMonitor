/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2674: the in-memory aggregation that feeds collect.collector_cost. The MAX assertion is the load-bearing
/// one — the tail is how a collector "sticks out" on a target, and an average would hide a single spike.
/// </summary>
public class CollectorCostAccumulatorTests
{
    [Fact]
    public void Record_AggregatesPerServerCollector_SummingAndKeepingTheMax()
    {
        var acc = new CollectorCostAccumulator();
        acc.Record(1, "query_store", rows: 100, sqlMs: 500, storageMs: 10);
        acc.Record(1, "query_store", rows: 200, sqlMs: 900, storageMs: 20); // higher sql_ms -> new max
        acc.Record(1, "query_store", rows: 50, sqlMs: 300, storageMs: 5);
        acc.Record(2, "deadlocks", rows: 3, sqlMs: 50, storageMs: 1);

        var drained = acc.Drain();
        Assert.Equal(2, drained.Count);

        var qs = drained.Single(d => d.ServerId == 1 && d.CollectorName == "query_store");
        Assert.Equal(3, qs.RunCount);
        Assert.Equal(1700, qs.TotalSqlMs);   // 500 + 900 + 300
        Assert.Equal(900, qs.MaxSqlMs);      // the tail, not the average
        Assert.Equal(35, qs.TotalStorageMs); // 10 + 20 + 5
        Assert.Equal(350, qs.TotalRows);     // 100 + 200 + 50

        var dl = drained.Single(d => d.ServerId == 2 && d.CollectorName == "deadlocks");
        Assert.Equal(1, dl.RunCount);
        Assert.Equal(50, dl.MaxSqlMs);
    }

    [Fact]
    public void Drain_ResetsTheWindow_SoTheNextDrainIsEmpty()
    {
        var acc = new CollectorCostAccumulator();
        acc.Record(1, "query_store", 100, 500, 10);

        Assert.Single(acc.Drain());
        Assert.Empty(acc.Drain());
    }

    [Fact]
    public void Record_IgnoresAnEmptyCollectorName()
    {
        var acc = new CollectorCostAccumulator();
        acc.Record(1, "", 100, 500, 10);

        Assert.Empty(acc.Drain());
    }
}
