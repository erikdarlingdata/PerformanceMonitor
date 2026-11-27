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
/// The window's start is another: both statements read the collections in (start, end], so the collection stamped exactly on
/// the start is the baseline of the first one inside and not a point of its own, and the four hashes are of that text.
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
        Assert.Equal("FEE4CF0D04A802C144DBAC9272A8C0F9DE8524BE21E3C5E90DAD697E766C6275", Sha(LocalDataService.DurationTrendChartSql("v_query_stats", "", 4)));
        Assert.Equal("BC78243DFAE247499F4FC3EA90E28BF16C3D9C097BA5B9F735729F234D9BEF2B", Sha(LocalDataService.DurationTrendChartSql("v_query_stats", Clause(4), 6)));
    }

    [Fact]
    public void QueryStatsBucketedSql_IsByteIdenticalToItsTextBeforeTheProcedureChange()
    {
        Assert.Equal("3DEE866B4357869E6F3BF6931544312FD942F9345AF1E1951D0707C260A3A37F", Sha(LocalDataService.BucketedDurationTrendSql("v_query_stats", "")));
        Assert.Equal("4FE1063E18498B028A1C7863962978B97A80CADD97559AB82E190C475B54C5A5", Sha(LocalDataService.BucketedDurationTrendSql("v_query_stats", Clause(5))));
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
