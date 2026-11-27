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
        new(rows, 10, 10, distinct, servers ?? Array.Empty<int>(), shares ?? Array.Empty<double>(), 8192);

    /* A table counted with some rows in some blocks that has another number of blocks now. */
    private static QueryTagRebuildGroups.ServerStatistics Table(double tuples, double pages, double currentPages, double? distinct = 1) =>
        new(tuples, pages, currentPages, distinct, Array.Empty<int>(), Array.Empty<double>(), 8192);

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
    public void ATableTheStoreHasNoPlainTableFor_GivesNoEstimate_AndOneWithNoStatisticsForTheColumn_GivesEveryServerAllItsRows()
    {
        /* No row for the table: it is not there, or it is not a plain table. */
        Assert.Null(QueryTagRebuildGroups.EstimateRows(null, 7));

        /* Rows counted and no statistics row for the column (vacuumed, never analyzed): no server can be placed, so each is
           taken at all the table's rows. */
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateRows(Statistics(1000, null), 7)!.Value, 6);
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateRows(Statistics(1000, null), 9)!.Value, 6);

        /* Never vacuumed or analyzed (reltuples -1) and no statistics row: all of the most its ten blocks can hold. */
        Assert.Equal(2910, QueryTagRebuildGroups.EstimateRows(Statistics(-1, null), 7)!.Value, 6);
    }

    [Fact]
    public void ATablesRows_AreTheRowsABlockHeldWhenItWasCounted_TimesTheBlocksItHasNow()
    {
        /* 1000 rows in 10 blocks is 100 a block: 30 blocks now are 3000 rows, 10 are the 1000 counted, and 5 are 500. */
        Assert.Equal(3000, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 30)), 6);
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 10)), 6);
        Assert.Equal(500, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 5)), 6);

        /* A server's rows are its share of the rows the table has now, not of the rows counted. */
        var grown = new QueryTagRebuildGroups.ServerStatistics(1000, 10, 30, 5, new[] { 7, 8 }, new[] { 0.5, 0.3 }, 8192);
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
        Assert.Equal(0, QueryTagRebuildGroups.EstimateTableRows(Table(0, 0, 0)), 6);
        Assert.Equal(0, QueryTagRebuildGroups.EstimateTableRows(Table(-1, 0, 0)), 6);
        Assert.Equal(0, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 10, 0)), 6);

        /* No rows needs no statistics for the column: every server has none of them. */
        Assert.Equal(0, QueryTagRebuildGroups.EstimateRows(Table(0, 0, 0, distinct: null), 7)!.Value, 6);
        Assert.Equal(0, QueryTagRebuildGroups.EstimateRows(Table(-1, 0, 0, distinct: null), 7)!.Value, 6);
    }

    [Fact]
    public void ATableWithBlocksNow_AndNoRowsABlockToScale_IsTakenAtTheMostItsBlocksCanHold_AndNeverAtZero()
    {
        /* The most rows a heap block holds: 291 at the default 8 kB, and the same rule at the other block sizes. */
        Assert.Equal(291, QueryTagRebuildGroups.MaxHeapTuplesPerPage(8192));
        Assert.Equal(145, QueryTagRebuildGroups.MaxHeapTuplesPerPage(4096));
        Assert.Equal(584, QueryTagRebuildGroups.MaxHeapTuplesPerPage(16384));

        /* Analyzed empty and filled since: no rows and no blocks were counted, and the table has 500 blocks now. */
        Assert.Equal(145_500, QueryTagRebuildGroups.EstimateTableRows(Table(0, 0, 500)), 6);
        Assert.Equal(145_500, QueryTagRebuildGroups.EstimateRows(Table(0, 0, 500), 7)!.Value, 6);
        Assert.Equal(145_500, QueryTagRebuildGroups.EstimateRows(Table(0, 0, 500, distinct: null), 7)!.Value, 6);

        /* Never vacuumed or analyzed, with blocks now. */
        Assert.Equal(145_500, QueryTagRebuildGroups.EstimateTableRows(Table(-1, 0, 500)), 6);
        Assert.Equal(145_500, QueryTagRebuildGroups.EstimateTableRows(Table(-1, 10, 500)), 6);

        /* Rows counted in no blocks. */
        Assert.Equal(145_500, QueryTagRebuildGroups.EstimateTableRows(Table(1000, 0, 500)), 6);

        /* Counted when its blocks held no row: none a block says nothing of the rows those blocks hold now. */
        Assert.Equal(2910, QueryTagRebuildGroups.EstimateTableRows(Table(0, 10, 10)), 6);
        Assert.Equal(145_500, QueryTagRebuildGroups.EstimateRows(Table(0, 10, 500), 7)!.Value, 6);

        /* The store's own block size decides the ceiling. */
        var wide = new QueryTagRebuildGroups.ServerStatistics(-1, 0, 10, null, Array.Empty<int>(), Array.Empty<double>(), 16384);
        Assert.Equal(5840, QueryTagRebuildGroups.EstimateRows(wide, 7)!.Value, 6);
    }

    [Fact]
    public void ASmallTableNeverAnalyzed_GroupsItsServers_AndALargeOneReadsEachServerAlone()
    {
        /* Each server's estimate is all the ceiling of its table, and the budget is the one that ships. */
        Dictionary<int, double> Estimates(double blocks) => new[] { 1, 2, 3 }.ToDictionary(
            id => id,
            id => QueryTagRebuildGroups.EstimateRows(Table(-1, 0, blocks, distinct: null), id)!.Value);

        /* Two blocks, 582 rows at most, the table that stays under the autoanalyze threshold: one group. */
        Assert.Equal("1,2,3", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3 }, Estimates(2), QueryTagMapStore.RebuildGroupRows)));

        /* 100,000 blocks, 29.1 million rows at most: every server alone. */
        Assert.Equal("1 / 2 / 3", Shape(QueryTagRebuildGroups.Form(new[] { 1, 2, 3 }, Estimates(100_000), QueryTagMapStore.RebuildGroupRows)));
    }

    [Fact]
    public void AServerOfATableThatGivesNoEstimate_IsReadAlone_WhileTheOthersShareAGroup()
    {
        /* Server 2 has no estimate: it is not grouped as if it had no rows. */
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

    }

    [Fact]
    public void AServerTheStatisticsCannotPlace_IsEstimatedAtAllTheTablesRows_AndNeverAtNone()
    {
        /* The common values are every distinct server and all the rows, as for a server added since the last analyze: it is
           taken at all the table's rows. The common servers keep their shares. */
        var covered = Statistics(1000, 2, new[] { 7, 8 }, new[] { 0.7, 0.3 });
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateRows(covered, 9)!.Value, 6);
        Assert.Equal(700, QueryTagRebuildGroups.EstimateRows(covered, 7)!.Value, 6);
        Assert.Equal(300, QueryTagRebuildGroups.EstimateRows(covered, 8)!.Value, 6);

        /* The shares add up to more than all the rows, which sampled shares can. */
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateRows(Statistics(1000, 2, new[] { 7, 8 }, new[] { 0.7, 0.4 }), 9)!.Value, 6);

        /* A share is left and no distinct server to give it to, and a distinct server is left and no share. */
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateRows(Statistics(1000, 2, new[] { 7, 8 }, new[] { 0.6, 0.3 }), 9)!.Value, 6);
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateRows(Statistics(1000, 3, new[] { 7, 8 }, new[] { 0.7, 0.3 }), 9)!.Value, 6);

        /* A negative distinct count that comes to the two common servers and no other. */
        Assert.Equal(1000, QueryTagRebuildGroups.EstimateRows(Statistics(1000, -0.002, new[] { 7, 8 }, new[] { 0.5, 0.3 }), 9)!.Value, 6);

        /* On a table over the budget the server nothing places is read alone, and the common servers still share a group. */
        var large = new QueryTagRebuildGroups.ServerStatistics(100_000_000, 10, 10, 2, new[] { 7, 8 }, new[] { 0.02, 0.01 }, 8192);
        var estimates = new[] { 7, 8, 9 }.ToDictionary(id => id, id => QueryTagRebuildGroups.EstimateRows(large, id)!.Value);
        Assert.Equal("7,8 / 9", Shape(QueryTagRebuildGroups.Form(new[] { 7, 8, 9 }, estimates, QueryTagMapStore.RebuildGroupRows)));
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
