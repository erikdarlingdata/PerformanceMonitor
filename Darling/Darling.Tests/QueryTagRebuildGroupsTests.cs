/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="QueryTagRebuildGroups"/> (#9705): which servers of a pass a rebuild reads together, from the row estimates
/// the store's statistics give each server and a budget of rows. Pure: no store.
/// </summary>
public sealed class QueryTagRebuildGroupsTests
{
    private static string Shape(List<List<int>> groups) => string.Join(" / ", groups.Select(group => string.Join(",", group)));

    private static QueryTagRebuildGroups.ServerStatistics Statistics(double rows, double? distinct, int[]? servers = null, double[]? shares = null) =>
        new(rows, distinct, servers ?? Array.Empty<int>(), shares ?? Array.Empty<double>());

    [Fact]
    public void ServersFillAGroupUpToTheBudget_InTheirOrder_AndTheNextServerStartsANewGroup()
    {
        var estimates = new Dictionary<int, double> { [1] = 40, [2] = 40, [3] = 40, [4] = 10, [5] = 100, [6] = 1 };

        /* 40 + 40 fits 100 and the third 40 does not, 40 + 10 fits and 100 does not, and 100 is exactly the budget. */
        Assert.Equal("1,2 / 3,4 / 5 / 6", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3, 4, 5, 6 }, estimates, 100)));
        Assert.Equal("1,2,3,4,5,6", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3, 4, 5, 6 }, estimates, 231)));
        Assert.Equal("6,5 / 4,3,2 / 1", Shape(QueryTagRebuildGroups.Form(new[] { 6, 5, 4, 3, 2, 1 }, estimates, 101)));
        Assert.Empty(QueryTagRebuildGroups.Form(Array.Empty<int>(), estimates, 100));
    }

    [Fact]
    public void AServerOverTheBudget_IsAGroupOfOne_AndTheServersAroundItStillShareAGroup()
    {
        var estimates = new Dictionary<int, double> { [1] = 30, [2] = 500, [3] = 30, [4] = 30 };

        Assert.Equal("1,3,4 / 2", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3, 4 }, estimates, 100)));

        /* A budget under every server leaves each one alone. */
        Assert.Equal("1 / 2 / 3 / 4", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3, 4 }, estimates, 1)));
    }

    [Fact]
    public void WithNoEstimates_EveryServerIsAGroupOfOne_AndAServerWithNoEstimateIsAloneAmongOnesThatHaveOne()
    {
        Assert.Equal("1 / 2 / 3", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3 }, null, 100)));
        Assert.Equal("1 / 2 / 3", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3 }, new Dictionary<int, double>(), 100)));
        Assert.Equal("1,3 / 2", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3 }, new Dictionary<int, double> { [1] = 10, [3] = 10 }, 100)));
    }

    [Fact]
    public void ATableWithNoStatistics_GivesNoEstimate_AndATableAnalyzedEmptyGivesZero()
    {
        /* No row in pg_class, a table never analyzed (reltuples -1), and rows with no statistics row for the column. */
        Assert.Null(QueryTagRebuildGroups.EstimateRows(null, 7));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Statistics(-1, null), 7));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Statistics(-1, 3, new[] { 7 }, new[] { 1.0 }), 7));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Statistics(1000, null), 7));

        Assert.Equal(0, QueryTagRebuildGroups.EstimateRows(Statistics(0, null), 7)!.Value, 6);
    }

    [Fact]
    public void ACommonServer_GetsItsShareOfTheRows_AndAnyOtherServerAnEvenPartOfWhatTheCommonOnesLeave()
    {
        /* 1000 rows, 5 distinct servers, two of them common with 50 and 30 percent: the other three split the last 20 percent. */
        var statistics = Statistics(1000, 5, new[] { 7, 8 }, new[] { 0.5, 0.3 });
        Assert.Equal(500, QueryTagRebuildGroups.EstimateRows(statistics, 7)!.Value, 6);
        Assert.Equal(300, QueryTagRebuildGroups.EstimateRows(statistics, 8)!.Value, 6);
        Assert.Equal(200.0 / 3, QueryTagRebuildGroups.EstimateRows(statistics, 9)!.Value, 6);

        /* No common values: every server gets an even part of all the rows. */
        Assert.Equal(250, QueryTagRebuildGroups.EstimateRows(Statistics(1000, 4), 9)!.Value, 6);

        /* The common values are every distinct server and all the rows: another server gets nothing, and never a negative count. */
        Assert.Equal(0, QueryTagRebuildGroups.EstimateRows(Statistics(1000, 2, new[] { 7, 8 }, new[] { 0.7, 0.3 }), 9)!.Value, 6);
        Assert.Equal(0, QueryTagRebuildGroups.EstimateRows(Statistics(1000, 2, new[] { 7, 8 }, new[] { 0.7, 0.4 }), 9)!.Value, 6);
    }

    [Fact]
    public void ANegativeDistinctCount_IsAFractionOfTheRows()
    {
        /* -0.01 of 1000 rows is 10 distinct servers. Two are common with 60 percent, so the other eight split 40 percent. */
        var statistics = Statistics(1000, -0.01, new[] { 7, 8 }, new[] { 0.4, 0.2 });
        Assert.Equal(400, QueryTagRebuildGroups.EstimateRows(statistics, 7)!.Value, 6);
        Assert.Equal(50, QueryTagRebuildGroups.EstimateRows(statistics, 9)!.Value, 6);
    }
}
