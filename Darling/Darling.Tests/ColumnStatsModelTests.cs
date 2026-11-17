/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>#8798: selectivity arithmetic, density row mapping, labels and change decisions.</summary>
[Trait("Stage", "Guard")]
public class ColumnStatsModelTests
{
    private static StatCandidate Stat(string[] columns, string[] clustered) =>
        new(1, "dbo", "T", 2, "S", "index", new DateTime(2026, 1, 1), new DateTime(2026, 1, 1), columns, clustered, 1000);

    private static StoredPrefix Stored(long sampled, DateTime updated) =>
        new("S", "index", ["a"], false, 0.1, 1000, sampled, updated, null, updated);

    [Fact]
    public void SingleColumn_UniqueDensity_GivesValueOneAndOneRowPerValue()
    {
        var s = ColumnStatsModel.Compute(1.0 / 1564, 1564)!;

        Assert.Equal(1.0, s.Value, 9);
        Assert.Equal(1564, s.DistinctEstimate, 9);
        Assert.Equal(1.0, s.RowsPerValue, 9);
    }

    [Fact]
    public void LowSelectivity_SingleColumnWithManyRowsPerValue_IsLabelled()
    {
        var s = ColumnStatsModel.Compute(0.25, 8_000_000)!;

        Assert.Equal(2_000_000, s.RowsPerValue, 9);
        Assert.Equal(5e-7, s.Value, 12);
        Assert.Equal("low selectivity", ColumnStatsModel.Label(s, 1, false));
    }

    [Fact]
    public void LowSelectivity_BoundaryIsStrictlyMoreThanOneHundredThousandRowsPerValue()
    {
        Assert.Null(ColumnStatsModel.Label(ColumnStatsModel.Compute(0.1, 1_000_000)!, 1, false));
        Assert.Equal("low selectivity", ColumnStatsModel.Label(ColumnStatsModel.Compute(0.1, 1_000_010)!, 1, false));
    }

    [Fact]
    public void UniqueLabel_OnlyComesFromTheSchemaNeverFromTheDensity()
    {
        var s = ColumnStatsModel.Compute(1.0 / 100, 100)!;

        Assert.Null(ColumnStatsModel.Label(s, 1, false));
        Assert.Equal("unique", ColumnStatsModel.Label(s, 1, true));
    }

    [Fact]
    public void ZeroRowsOrDensity_IsNull()
    {
        Assert.Null(ColumnStatsModel.Compute(0.5, 0));
        Assert.Null(ColumnStatsModel.Compute(0, 100));
    }

    [Fact]
    public void MultiColumn_NeverGetsTheLowSelectivityLabel()
    {
        Assert.Null(ColumnStatsModel.Label(ColumnStatsModel.Compute(0.5, 1_000_000)!, 2, false));
    }

    [Fact]
    public void ColumnNameWithComma_ComesFromStatsColumnsNotText()
    {
        var prefixes = ColumnStatsModel.MapDensityRows(Stat(["a,b", "c"], []), [0.5, 0.1]);

        Assert.Equal(2, prefixes.Count);
        Assert.Equal(["a,b"], prefixes[0].Columns);
        Assert.Equal(["a,b", "c"], prefixes[1].Columns);
    }

    [Fact]
    public void TrailingClusteringKeyRows_AreMarked()
    {
        var prefixes = ColumnStatsModel.MapDensityRows(Stat(["CustomerId", "OrderType"], ["Id"]), [0.5, 0.2, 0.001]);

        Assert.Equal(3, prefixes.Count);
        Assert.Equal(["CustomerId", "OrderType", "Id"], prefixes[2].Columns);
        Assert.True(prefixes[2].IncludesClusteringKey);
        Assert.False(prefixes[0].IncludesClusteringKey);
        Assert.False(prefixes[1].IncludesClusteringKey);
        Assert.Equal([1, 2, 3], prefixes.Select(p => p.PrefixLength));
    }

    [Fact]
    public void ClusteringKeyAlreadyInKey_IsNotRepeated()
    {
        var prefixes = ColumnStatsModel.MapDensityRows(Stat(["Id", "X"], ["Id"]), [0.5, 0.1]);

        Assert.Equal(2, prefixes.Count);
        Assert.All(prefixes, p => Assert.False(p.IncludesClusteringKey));
    }

    [Fact]
    public void MoreDensityRowsThanNames_ExtraRowsDropped()
    {
        var prefixes = ColumnStatsModel.MapDensityRows(Stat(["a"], []), [0.5, 0.1, 0.01]);

        var only = Assert.Single(prefixes);
        Assert.Equal(["a"], only.Columns);
    }

    [Fact]
    public void PickBest_LargestSampleThenNewest()
    {
        var old = new DateTime(2026, 1, 1);
        var recent = new DateTime(2026, 2, 1);
        var bigOld = Stored(900, old);
        var smallRecent = Stored(100, recent);

        Assert.Same(bigOld, ColumnStatsModel.PickBest([smallRecent, bigOld]));

        var tieOld = Stored(500, old);
        var tieRecent = Stored(500, recent);
        Assert.Same(tieRecent, ColumnStatsModel.PickBest([tieOld, tieRecent]));
    }

    [Fact]
    public void OlderStatsDate_CountsAsChanged()
    {
        var t0 = new DateTime(2026, 1, 1);
        var t1 = t0.AddDays(1);

        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(t0, t1));
        Assert.Equal(StatChange.Unchanged, ColumnStatsModel.Decide(t0, t0));
        Assert.Equal(StatChange.New, ColumnStatsModel.Decide(t0, null));
    }

    [Fact]
    public void Rename_CountsAsChanged_WithTheSameStatsDate()
    {
        var current = Stat(["CustomerId", "OrderType"], ["Id"]);
        var same = new StoredStatistic(current.StatsDate, "dbo", "T", "S", ["CustomerId", "OrderType", "Id"]);

        Assert.Equal(StatChange.Unchanged, ColumnStatsModel.Decide(current, same));
        Assert.Equal(StatChange.New, ColumnStatsModel.Decide(current, null));
        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(current, same with { StatsDate = current.StatsDate.AddSeconds(1) }));
        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(current, same with { SchemaName = "old" }));
        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(current, same with { ObjectName = "OldTable" }));
        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(current, same with { StatsName = "OldStat" }));
        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(current, same with { Columns = ["CustomerId", "Type", "Id"] }));
        /* A clustering key column renamed, and a change of case only. */
        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(current, same with { Columns = ["CustomerId", "OrderType", "OldId"] }));
        Assert.Equal(StatChange.Changed, ColumnStatsModel.Decide(current, same with { Columns = ["customerid", "OrderType", "Id"] }));
        /* DBCC returned fewer rows than the column list: the stored prefix is still the leading part. */
        Assert.Equal(StatChange.Unchanged, ColumnStatsModel.Decide(current, same with { Columns = ["CustomerId", "OrderType"] }));
    }
}
