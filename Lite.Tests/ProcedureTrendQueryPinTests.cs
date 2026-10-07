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
        Assert.Equal("DE5B7384FAEDD51B1BF8E3B4DAEBCF3C5DEAD100A7A8CAEFB89C5C08A1CA81C2", Sha(LocalDataService.DurationTrendChartSql("v_query_stats", "", 4)));
        Assert.Equal("EB6226CB8F98242253395FAD9E41E431A650630F1C79109778279C08FBF56423", Sha(LocalDataService.DurationTrendChartSql("v_query_stats", Clause(4), 6)));
    }

    [Fact]
    public void QueryStatsBucketedSql_IsByteIdenticalToItsTextBeforeTheProcedureChange()
    {
        Assert.Equal("03270AC3C869E7777050580B1F373911E3D3789B6A98054E894E872D894CC341", Sha(LocalDataService.BucketedDurationTrendSql("v_query_stats", "")));
        Assert.Equal("C7B500FC53A093AF9340583AF94C742019C61AFC8FCD7BB4BA53C7C5AAE075C2", Sha(LocalDataService.BucketedDurationTrendSql("v_query_stats", Clause(5))));
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
