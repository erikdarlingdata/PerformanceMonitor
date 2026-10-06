/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5299 round 3 (O16): the Lite twin of Darling's <c>TheQueryStoreAndViewerTopStatements_RankOnATotalOrder</c>. Both top lists
/// rank on the metric, then the WHOLE group key, at all three places the order is spelled: the ranked CTE, the page_ord numbering and
/// the page's own ORDER BY. A tie at the candidate cut then picks the same keys in every refill round, so one read cannot return a
/// different page from the next. The statements are inline in the reader methods, so the pin reads the method text from the source.
/// </summary>
public sealed class TopListTotalOrderPinTests
{
    private const string QueryStatsKey = "database_name, query_hash, host_object_name";
    private const string QueryStoreKey = "database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role";

    [Fact]
    public void TheQueryStatsTopList_RanksOnATotalOrder()
    {
        var sql = MethodText("Lite/Services/LocalDataService.QueryStats.cs", "GetTopQueriesByCpuAsync");

        Assert.Contains("ORDER BY SUM(delta_worker_time) DESC, " + QueryStatsKey + "\n", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER (ORDER BY r.total_cpu_us DESC, r.database_name, r.query_hash, r.host_object_name) AS page_ord", sql, StringComparison.Ordinal);
        Assert.Contains("\nORDER BY r.total_cpu_us DESC, r.database_name, r.query_hash, r.host_object_name\nLIMIT $4", sql, StringComparison.Ordinal);
        AssertNoRankingOrderEndsOnTheMetric(sql, @"ORDER BY (?:SUM\(delta_worker_time\)|r\.total_cpu_us) DESC[^\n]*");

        /* The module pick reaches the page (the procedure a statement belongs to): a tie on the time takes the row collected last. */
        Assert.Contains("PARTITION BY sql_handle ORDER BY collection_time DESC, collection_id DESC) AS rn", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheQueryStoreTopList_RanksOnATotalOrder()
    {
        var sql = MethodText("Lite/Services/LocalDataService.QueryStore.cs", "GetQueryStoreTopQueriesAsync");

        Assert.Contains("AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) DESC, " + QueryStoreKey + "\n", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER (ORDER BY r.total_executions * r.avg_duration_ms DESC, r.database_name, r.query_id, r.plan_id, r.query_hash, r.execution_type_desc, r.replica_role) AS page_ord", sql, StringComparison.Ordinal);
        /* #5381: the limit moved to code, after the WAITFOR filter, so the statement now ends on the page order; the total order is pinned by the page_ord line above. */
        Assert.Contains("\nORDER BY p.page_ord", sql, StringComparison.Ordinal);
        AssertNoRankingOrderEndsOnTheMetric(sql, @"ORDER BY (?:SUM\(execution_count\) \*|r\.total_executions \*)[^\n]*");
    }

    private static void AssertNoRankingOrderEndsOnTheMetric(string sql, string rankingOrderPattern)
    {
        var orders = Regex.Matches(sql, rankingOrderPattern).Select(m => m.Value.TrimEnd()).ToList();
        Assert.NotEmpty(orders);
        Assert.All(orders, order => Assert.False(order.EndsWith("DESC", StringComparison.Ordinal),
            $"the ranking '{order}' ends on the metric alone, so a tie at the cut is broken by scan order"));
    }

    /// <summary>The source text of one reader method, from its declaration to the next member, with line endings normalized.</summary>
    private static string MethodText(string relative, string method)
    {
        var path = Path.Combine(RepoRoot(), relative);
        Assert.True(File.Exists(path), $"scan target not found: {path}");
        var text = File.ReadAllText(path).ReplaceLineEndings("\n");
        var start = text.IndexOf(" " + method + "(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{method} not found in {relative}");
        var next = Regex.Match(text[(start + 1)..], @"\n    (?:public|internal|private|protected)\b");
        return next.Success ? text.Substring(start, next.Index + 1) : text[start..];
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
