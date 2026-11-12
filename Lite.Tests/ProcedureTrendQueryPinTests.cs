/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5449: the procedure grain's duration trend moved to the run-based model (<see cref="LocalDataService.ProcedureCollectionsSql"/>),
/// and the query grain's did not. These pin the query grain's two statements byte for byte (SHA-256 of the text as it stood before
/// #5449, line endings normalised) so a change to the shared builders cannot move <c>v_query_stats</c>, and pin the procedure
/// statements' shape: both tables read from an hour before the window, and none of the removed span term.
/// #7161 changed the query grain's rating on purpose (a gap to the previous collection replaces MAX of the stored intervals) and
/// updated these four hashes with that reason.
/// #8073 is another: both query-grain statements gain a trailing <c>execution_count</c> column (the bucket's summed rated
/// executions), so the four hashes moved again on purpose.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ProcedureTrendQueryPinTests
{
    private static string Sha(string sql) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql.Replace("\r\n", "\n"))));

    private static string Clause(int firstIndex) =>
        LocalDataService.BuildDbInClause(new[] { "A", "B" }, "database_name", firstIndex, out _);

    [Fact]
    public void QueryStatsChartSql_IsByteIdenticalToItsTextBeforeTheProcedureChange()
    {
        Assert.Equal("15D7072335BC0123D2A5D646F346ED8D28367604CC4D866029613EF4CB8BE2A9", Sha(LocalDataService.DurationTrendChartSql("v_query_stats", "", 4)));
        Assert.Equal("0B43A65EC59C2DE3003465E13934D337C03B9DBCB3758391C0D8E5C86495F610", Sha(LocalDataService.DurationTrendChartSql("v_query_stats", Clause(4), 6)));
    }

    [Fact]
    public void QueryStatsBucketedSql_IsByteIdenticalToItsTextBeforeTheProcedureChange()
    {
        Assert.Equal("8020B8CE5EE7813BDFF96EC980D1112BBBE820E57CCA074AACA37C87C3808705", Sha(LocalDataService.BucketedDurationTrendSql("v_query_stats", "")));
        Assert.Equal("CA5B63CCD7B613BA0BB95EFD4CD5495670747691E3194987799EADB0AAB075DC", Sha(LocalDataService.BucketedDurationTrendSql("v_query_stats", Clause(5))));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ProcedureStatements_ShareOneCollectionsFragment_ReadBothTablesFromAnHourBack_AndHaveNoSpanTerm(bool withFilter)
    {
        var chart = LocalDataService.DurationTrendChartSql("v_procedure_stats", withFilter ? Clause(4) : "", 6);
        var bucketed = LocalDataService.BucketedDurationTrendSql("v_procedure_stats", withFilter ? Clause(5) : "");
        var fragment = LocalDataService.ProcedureCollectionsSql(withFilter ? Clause(4) : "");

        Assert.Contains(fragment.Replace("\r\n", "\n").Split("\nraw AS")[0], chart.Replace("\r\n", "\n"));
        Assert.StartsWith("stored AS", fragment);
        foreach (var sql in new[] { chart, bucketed })
        {
            /* Both the stored rows and the log's runs come from an hour before $2, and the points before $2 are dropped after the LAG. */
            Assert.Equal(2, CountOf(sql, "collection_time >= $2 - to_seconds(3600)"));
            Assert.Contains("WHERE collection_time >= $2", sql);
            Assert.Contains("v_collection_log", sql);
            Assert.DoesNotContain("series_first", sql);
            Assert.DoesNotContain("first_interval", sql);
            Assert.DoesNotContain("GREATEST(\n", sql.Replace("\r\n", "\n"));
        }
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }
        return count;
    }
}
