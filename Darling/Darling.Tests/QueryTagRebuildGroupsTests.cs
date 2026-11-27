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

    /* A table that has the blocks it was counted with, so its rows now are the rows counted. */
    private static QueryTagRebuildGroups.ServerStatistics Statistics(double rows, double? distinct, int[]? servers = null, double[]? shares = null) =>
        new(rows, 10, 10, distinct, servers ?? Array.Empty<int>(), shares ?? Array.Empty<double>());

    /* A table counted with some rows in some blocks that has another number of blocks now. */
    private static QueryTagRebuildGroups.ServerStatistics Table(double tuples, double pages, double currentPages, double? distinct = 1) =>
        new(tuples, pages, currentPages, distinct, Array.Empty<int>(), Array.Empty<double>());

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
    public void ATableWithNoStatistics_GivesNoEstimate()
    {
        /* No row in pg_class, a table never analyzed (reltuples -1), and rows with no statistics row for the column. */
        Assert.Null(QueryTagRebuildGroups.EstimateRows(null, 7));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Statistics(-1, null), 7));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Statistics(-1, 3, new[] { 7 }, new[] { 1.0 }), 7));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Statistics(1000, null), 7));
    }

    [Fact]
    public void ATablesRows_AreTheRowsABlockHeldWhenItWasCounted_TimesTheBlocksItHasNow()
    {
        /* 1000 rows in 10 blocks is 100 a block: 30 blocks now are 3000 rows, 10 are the 1000 counted, and 5 are 500. */
        Assert.Equal(3000, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 30))!.Value, 6);
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 10))!.Value, 6);
        Assert.Equal(500, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 5))!.Value, 6);

        /* A server's rows are its share of the rows the table has now, not of the rows counted. */
        var grown = new QueryTagRebuildGroups.ServerStatistics(1000, 10, 30, 5, new[] { 7, 8 }, new[] { 0.5, 0.3 });
        Assert.Equal(1500, QueryTagRebuildGroups.EstimateRows(grown, 7)!.Value, 6);
        Assert.Equal(900, QueryTagRebuildGroups.EstimateRows(grown, 8)!.Value, 6);
        Assert.Equal(200, QueryTagRebuildGroups.EstimateRows(grown, 9)!.Value, 6);

        /* A negative distinct count is a fraction of the rows the table has now: -0.01 of 3000 rows is 30 servers. */
        Assert.Equal(100, QueryTagRebuildGroups.EstimateRows(Table(1000, 10, 30, distinct: -0.01), 9)!.Value, 6);
    }

    [Fact]
    public void ATableWithNoBlocksNow_HasNoRows_WhateverWasCounted()
    {
        /* Analyzed empty and still empty, never analyzed and empty, and counted with rows and emptied since. */
        Assert.Equal(0, QueryTagRebuildGroups.EstimateTableRows(Table(0, 0, 0))!.Value, 6);
        Assert.Equal(0, QueryTagRebuildGroups.EstimateTableRows(Table(-1, 0, 0))!.Value, 6);
        Assert.Equal(0, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 0))!.Value, 6);

        /* No rows needs no statistics for the column: every server has none of them. */
        Assert.Equal(0, QueryTagRebuildGroups.EstimateRows(Table(0, 0, 0, distinct: null), 7)!.Value, 6);
        Assert.Equal(0, QueryTagRebuildGroups.EstimateRows(Table(-1, 0, 0, distinct: null), 7)!.Value, 6);
    }

    [Fact]
    public void ATableWithBlocksNow_AndNoRowsABlockToScale_GivesNoEstimate_AndNeverZero()
    {
        /* Analyzed empty and filled since: no rows and no blocks were counted, and the table has blocks now. */
        Assert.Null(QueryTagRebuildGroups.EstimateTableRows(Table(0, 0, 500)));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Table(0, 0, 500), 7));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Table(0, 0, 500, distinct: null), 7));

        /* Never vacuumed or analyzed, with blocks now. */
        Assert.Null(QueryTagRebuildGroups.EstimateTableRows(Table(-1, 0, 500)));
        Assert.Null(QueryTagRebuildGroups.EstimateTableRows(Table(-1, 10, 500)));

        /* Rows counted in no blocks. */
        Assert.Null(QueryTagRebuildGroups.EstimateTableRows(Table(1000, 0, 500)));

        /* Counted when its blocks held no row: none a block says nothing of the rows those blocks hold now. */
        Assert.Null(QueryTagRebuildGroups.EstimateTableRows(Table(0, 10, 10)));
        Assert.Null(QueryTagRebuildGroups.EstimateRows(Table(0, 10, 500), 7));
    }

    [Fact]
    public void AServerOfATableThatGivesNoEstimate_IsReadAlone_WhileTheOthersShareAGroup()
    {
        /* Server 2 has no estimate, as when a table it has rows in gives none: it is not grouped as if it had no rows. */
        var estimates = new Dictionary<int, double> { [1] = 0, [3] = 0, [4] = 0 };
        Assert.Equal("1,3,4 / 2", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3, 4 }, estimates, 100)));
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
