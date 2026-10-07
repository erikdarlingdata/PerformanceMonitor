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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5226 pins for the raw ranking statements' two-pass shape (check 3 of the review: identical results only under
/// four conditions), the stable-final-key and NULLS LAST rules on every deciding ORDER BY (F2, F8), and the single
/// rank anchor <c>TopRankings.Apply</c> swaps (F11). They read the consts' text only; the row-for-row proof that the
/// two passes answer what the single pass did is the live differential test. The anchor is spelled out here, not
/// read from <c>TopRankings</c>, so a rename of it fails these pins instead of following the code.
/// </summary>
public sealed class TopRankingTwoPassTests
{
    private const string Anchor = "$RANK$";

    private static string Sql(string name) => name switch
    {
        "TopQueriesSql" => DarlingDataReader.TopQueriesSql,
        "TopQueriesByHostObjectSql" => DarlingDataReader.TopQueriesByHostObjectSql,
        "TopProceduresSql" => DarlingDataReader.TopProceduresSql,
        "TopQueriesHourlySql" => DarlingDataReader.TopQueriesHourlySql,
        "TopProceduresHourlySql" => DarlingDataReader.TopProceduresHourlySql,
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    private static bool IsHourly(string name) => name.Contains("Hourly", StringComparison.Ordinal);

    private static IEnumerable<TopRanking> Rankings(string name) =>
        Enum.GetValues<TopRanking>().Where(r => !IsHourly(name) || TopRankings.HourlyCarries(r));

    public static IEnumerable<object[]> AllFive() =>
        new[] { "TopQueriesSql", "TopQueriesByHostObjectSql", "TopProceduresSql", "TopQueriesHourlySql", "TopProceduresHourlySql" }
            .Select(n => new object[] { n });

    public static IEnumerable<object[]> RawThree() =>
        new[] { "TopQueriesSql", "TopQueriesByHostObjectSql", "TopProceduresSql" }.Select(n => new object[] { n });

    // ------------------------------------------------------------------ F11: the anchor

    [Theory]
    [MemberData(nameof(AllFive))]
    public void EveryConst_CarriesTheRankAnchorExactlyOnce_InItsRankingSelectList(string name)
    {
        var sql = Sql(name);
        Assert.Single(Regex.Matches(sql, Regex.Escape(Anchor)));
        Assert.Contains(Anchor + " AS rank_metric", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, @"SUM\(\w+\) AS rank_cpu"));
    }

    [Theory]
    [MemberData(nameof(AllFive))]
    public void Apply_ThrowsWhenAnyAnchorIsMissing_OrAppearsTwice(string name)
    {
        var sql = Sql(name);
        foreach (var ranking in Rankings(name))
        {
            var hourly = IsHourly(name);
            var missing = sql.Replace(Anchor, "SUM(x)", StringComparison.Ordinal);
            var twice = sql + "\n" + Anchor;
            Assert.Throws<InvalidOperationException>(() => TopRankings.Apply(missing, ranking, hourly));
            Assert.Throws<InvalidOperationException>(() => TopRankings.Apply(twice, ranking, hourly));
            Assert.DoesNotContain(Anchor, TopRankings.Apply(sql, ranking, hourly), StringComparison.Ordinal);
        }
    }

    // ------------------------------------------------------------------ F2 + F8: NULLS LAST and a final key

    private static readonly Dictionary<string, string[]> KeyTails = new()
    {
        ["TopQueriesSql"] = new[] { "win_database_name, win_query_hash, win_host_object_name", "r.database_name, r.query_hash, r.host_object_name" },
        ["TopQueriesByHostObjectSql"] = new[] { "win_database_name, win_host_object_name, win_group_hash", "r.database_name, r.host_object_name, r.query_hash" },
        ["TopProceduresSql"] = new[] { "win_database_name, win_schema_name, win_object_name, win_object_type", "database_name, schema_name, object_name, object_type" },
        ["TopQueriesHourlySql"] = new[] { "database_name, query_hash", "r.database_name, r.query_hash" },
        ["TopProceduresHourlySql"] = new[] { "database_name, schema_name, object_name", "r.database_name, r.schema_name, r.object_name" },
    };

    /// <summary>Every ORDER BY that decides which groups survive or in what order (the latest-text lookup's is not one).</summary>
    private static List<string> RankingOrderBys(string sql) =>
        Regex.Matches(sql, @"(?m)^\s*ORDER BY ([^\r\n]+)").Select(m => m.Groups[1].Value.Trim())
            .Where(o => !o.StartsWith("collection_time", StringComparison.Ordinal) && !o.StartsWith("q.", StringComparison.Ordinal)
                && !o.StartsWith("p.page_ord", StringComparison.Ordinal)).ToList();   /* #5313: the count-row join re-sorts the page by its position */

    [Theory]
    [MemberData(nameof(AllFive))]
    public void EveryDecidingOrderBy_SortsDescSumsNullsLast_AndEndsInTheUniqueGroupKey(string name)
    {
        foreach (var ranking in Rankings(name))
        {
            var orders = RankingOrderBys(TopRankings.Apply(Sql(name), ranking, IsHourly(name)));
            Assert.Equal(2, orders.Count);
            for (var i = 0; i < orders.Count; i++)
            {
                var o = orders[i];
                Assert.Matches(@"^(MAX\()?([rw]\.)?rank_metric(\))? DESC NULLS LAST, (MAX\()?([rw]\.)?rank_cpu(\))? DESC NULLS LAST, ", o);
                Assert.EndsWith(", " + KeyTails[name][i], o, StringComparison.Ordinal);
                foreach (var item in o.Split(", "))
                {
                    Assert.True(!item.EndsWith(" DESC", StringComparison.Ordinal), $"{name}: '{item}' sorts DESC without NULLS LAST");
                }
            }
        }
    }

    // ------------------------------------------------------------------ check 3: the four conditions of the two passes

    [Theory]
    [MemberData(nameof(RawThree))]
    public void Raw_Pass1RanksNarrow_AndRepeatsTheHavingTheFloorAndTheKey(string name)
    {
        var sql = Sql(name);
        var p1 = sql[sql.IndexOf("winners AS MATERIALIZED (", StringComparison.Ordinal)..];
        var limitAt = p1.IndexOf(name == "TopProceduresSql" ? "LIMIT $4" : "LIMIT $7", StringComparison.Ordinal);
        p1 = p1[..p1.IndexOfAny(new[] { '\r', '\n' }, limitAt)];   /* through the end of pass 1's LIMIT line */
        Assert.DoesNotContain("COUNT(DISTINCT", p1, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(plan_handle)", p1, StringComparison.Ordinal);
        Assert.DoesNotContain("MIN(min_", p1, StringComparison.Ordinal);
        if (name == "TopProceduresSql")
        {
            Assert.Contains("GROUP BY database_name, schema_name, object_name, object_type", p1, StringComparison.Ordinal);
            Assert.Contains("HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0", p1, StringComparison.Ordinal);
            Assert.EndsWith("LIMIT $4", p1, StringComparison.Ordinal);
            Assert.DoesNotContain("+ 5", sql, StringComparison.Ordinal);   /* procedures has no over-fetch */
            return;
        }

        Assert.Contains("HAVING (SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0)", p1, StringComparison.Ordinal);
        Assert.Contains("AND COALESCE(MAX(max_dop), 0) >= $6", p1, StringComparison.Ordinal);
        Assert.Contains(name == "TopQueriesSql"
            ? "GROUP BY database_name, query_hash, host_object_name"
            : "CASE WHEN host_object_name IS NULL THEN query_hash END", p1, StringComparison.Ordinal);
        Assert.EndsWith("LIMIT $7", p1.TrimEnd(), StringComparison.Ordinal);   /* the candidate limit (#5313) stays in pass 1, and only there */
        Assert.Single(Regex.Matches(sql, Regex.Escape("LIMIT $7")));
        Assert.DoesNotContain("+ 5", sql, StringComparison.Ordinal);   /* no fixed over-fetch: TopFill picks the candidate limit */
    }

    [Theory]
    [MemberData(nameof(RawThree))]
    public void Raw_Pass2RescansTheSameWindow_AndMatchesTheKeyNullSafely(string name)
    {
        var sql = Sql(name);
        foreach (var filter in new[]
        {
            "server_id = $1", "collection_time >= $2", "collection_time <= $3",
            "$5::text[] IS NULL OR database_name = ANY($5)", "{TimescaleSupport.IntervalHonestSourceFilter}",
        })
        {
            var text = filter.Replace("{TimescaleSupport.IntervalHonestSourceFilter}", "sample_interval_seconds IS DISTINCT FROM 0", StringComparison.Ordinal);
            /* once per pass; the queries' one latest-text lookup (#5309) scopes by server and reads the window too */
            var lookup = name != "TopProceduresSql" && (filter == "server_id = $1" || filter.StartsWith("collection_time", StringComparison.Ordinal));
            Assert.Equal(lookup ? 3 : 2, Regex.Matches(sql, Regex.Escape(text)).Count);
        }

        var p2 = sql[sql.IndexOf("JOIN winners AS w", StringComparison.Ordinal)..];
        var keys = name switch
        {
            "TopQueriesSql" => new[] { "database_name", "query_hash", "host_object_name" },
            "TopQueriesByHostObjectSql" => new[] { "database_name", "host_object_name", "group_hash" },
            _ => new[] { "database_name", "schema_name", "object_name", "object_type" },
        };
        foreach (var key in keys)
        {
            Assert.Contains("IS NOT DISTINCT FROM w.win_" + key, p2, StringComparison.Ordinal);
        }

        Assert.DoesNotMatch(@"=\s*w\.win_", p2);   /* a plain equality drops the NULL-keyed groups */
        Assert.DoesNotMatch(@"\bIN \(SELECT", sql);
    }

    [Theory]
    [MemberData(nameof(RawThree))]
    public void Raw_TextLookupWaitforTrimAndFinalLimit_RunAfterPass2(string name)
    {
        var sql = Sql(name);
        var pass2 = sql.IndexOf("JOIN winners AS w", StringComparison.Ordinal);
        Assert.True(sql.IndexOf("winners AS MATERIALIZED", StringComparison.Ordinal) < pass2);
        if (name == "TopProceduresSql")
        {
            Assert.DoesNotContain("LATERAL", sql, StringComparison.Ordinal);
            return;
        }

        /* #5309: the text is ONE lookup over the winners (the latest_text CTE), joined to the ranked rows once. */
        Assert.DoesNotContain("LATERAL", sql, StringComparison.Ordinal);
        var lookup = sql.IndexOf("latest_text AS MATERIALIZED (", StringComparison.Ordinal);
        var lookupJoin = sql.IndexOf("LEFT JOIN latest_text AS t", StringComparison.Ordinal);
        var waitfor = sql.IndexOf("NOT LIKE 'WAITFOR%'", StringComparison.Ordinal);
        var finalLimit = sql.LastIndexOf("LIMIT $4", StringComparison.Ordinal);
        Assert.True(pass2 < lookup && lookup < lookupJoin && lookupJoin < waitfor && waitfor < finalLimit, "text lookup, WAITFOR trim and LIMIT $4 follow pass 2");
        Assert.Single(Regex.Matches(sql, @"LIMIT \$4\s*$", RegexOptions.Multiline));
    }
}
