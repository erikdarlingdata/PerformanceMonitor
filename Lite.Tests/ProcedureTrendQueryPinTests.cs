/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5449: the procedure grain's duration trend moved to the run-based model (<see cref="LocalDataService.ProcedureCollectionsSql"/>),
/// and the query grain's did not. These pin the procedure statements' shape: both tables read from an hour before the window, and
/// none of the removed span term. (The byte-for-byte pins of the query grain's two statements were removed in #7161, which changes
/// that grain on purpose; their rule was the spelling of the text, and the behaviour is covered by PerformanceTrendDurationBucketingTests.)
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ProcedureTrendQueryPinTests
{
    private static string Clause(int firstIndex) =>
        LocalDataService.BuildDbInClause(new[] { "A", "B" }, "database_name", firstIndex, out _);

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
