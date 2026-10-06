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
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5309 and #5313 (inside #5299): every Top Queries statement resolves the latest text of all its ranked groups with
/// ONE lookup (no per-row <c>ORDER BY collection_time DESC LIMIT 1</c>, which scanned the raw table once per row on a
/// store without TimescaleDB), and fills the page past the WAITFOR trim through <see cref="TopFill"/>'s bounded
/// rounds instead of a fixed over-fetch of five. These are SQL-shape pins plus a unit test of the fill loop; the
/// plans and timings are measured on a live store, not here.
/// </summary>
public sealed class TopTextLookupFillTests
{
    private static string Sql(string name) => name switch
    {
        "TopQueriesSql" => DarlingDataReader.TopQueriesSql,
        "TopQueriesByHostObjectSql" => DarlingDataReader.TopQueriesByHostObjectSql,
        "TopQueriesHourlySql" => DarlingDataReader.TopQueriesHourlySql,
        "ViewerTopQueriesSql" => ViewerDataService.TopQueriesSql,
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    public static IEnumerable<object[]> Statements() =>
        new[] { "TopQueriesSql", "TopQueriesByHostObjectSql", "TopQueriesHourlySql", "ViewerTopQueriesSql" }
            .Select(n => new object[] { n });

    // ------------------------------------------------------------------ #5309: one lookup

    [Theory]
    [MemberData(nameof(Statements))]
    public void TheLatestTextIsOneLookupForAllRankedGroups_NotOnePerRow(string name)
    {
        var sql = Sql(name);
        Assert.DoesNotContain("LATERAL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 1", sql, StringComparison.Ordinal);   /* the per-row text lookup's shape */
        Assert.DoesNotMatch(@"(FROM|JOIN) v_query_stats", sql);
        /* Each text lookup picks the newest row per key in one pass: DISTINCT ON over (key, collection_time DESC). */
        Assert.Matches(@"SELECT DISTINCT ON \(q\.database_name, q\.(query_hash|host_object_name)", sql);
        Assert.Matches(@"ORDER BY q\.database_name, q\.(query_hash|host_object_name)[^\r\n]*q\.collection_time DESC", sql);
        /* The newest row's text resolves through the dimension for that one row, as v_query_stats' COALESCE does. */
        Assert.Contains("COALESCE(l.query_text, d.query_text) AS query_text", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN query_text_dim AS d ON d.digest = l.query_text_digest", sql, StringComparison.Ordinal);
        /* The lookup joins the winners (or ranked) CTE, which is read, not repeated. */
        Assert.Matches(@"JOIN (winners|ranked) AS (w|rk)\b[\s\S]*COALESCE\(q\.", sql);
    }

    [Theory]
    [MemberData(nameof(Statements))]
    public void TheLookupMatchesTheKeyNullSafely_AndOnlyTheOuterJoinReadsTheResult(string name)
    {
        var sql = Sql(name);
        Assert.Contains("q.database_name IS NOT DISTINCT FROM", sql, StringComparison.Ordinal);
        /* The one join to the ranked rows is NULL-safe on every key column; no plain "t.x = r.x" equality that would
           drop a NULL-keyed group's text. */
        Assert.DoesNotMatch(@"\bt\.\w+\s*=\s*r\.", sql);
        Assert.Single(Regex.Matches(sql, @"LEFT JOIN latest_text AS t|LEFT JOIN latest_in_window AS w"));
    }

    [Theory]
    [InlineData("TopQueriesSql")]
    [InlineData("TopQueriesByHostObjectSql")]
    [InlineData("ViewerTopQueriesSql")]
    public void TheRawLookupIsBoundedToTheWindow(string name)
    {
        var sql = Sql(name);
        var lookup = sql[sql.IndexOf("latest_text AS (", StringComparison.Ordinal)..];
        var end = lookup.IndexOf("LEFT JOIN query_text_dim AS d ON d.digest = l.query_text_digest", StringComparison.Ordinal);
        lookup = lookup[..end];
        Assert.Contains("q.collection_time >= $2", lookup, StringComparison.Ordinal);
        Assert.Contains("q.collection_time <= $3", lookup, StringComparison.Ordinal);
        Assert.Contains("q.server_id = $1", lookup, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHourlyLookup_ReadsTheWindowFirst_ThenOnlyTheKeysTheWindowHadNoTextFor()
    {
        var sql = DarlingDataReader.TopQueriesHourlySql;
        var window = sql.IndexOf("latest_in_window AS (", StringComparison.Ordinal);
        var missing = sql.IndexOf("missing AS MATERIALIZED (", StringComparison.Ordinal);
        var any = sql.IndexOf("latest_any AS (", StringComparison.Ordinal);
        var final = sql.IndexOf("COALESCE(w.query_text, a.query_text) AS query_text", StringComparison.Ordinal);
        Assert.True(window > 0 && window < missing && missing < any && any < final);
        var windowPass = sql[window..missing];
        Assert.Contains("q.collection_time >= $2", windowPass, StringComparison.Ordinal);
        Assert.Contains("q.collection_time < $3", windowPass, StringComparison.Ordinal);
        /* The fallback has no window bound (the rollup outlives raw), but it joins only the missing keys. */
        var anyPass = sql[any..final];
        Assert.DoesNotContain("collection_time >=", anyPass, StringComparison.Ordinal);
        Assert.Contains("JOIN missing AS m", anyPass, StringComparison.Ordinal);
        Assert.Contains("NOT EXISTS", sql[missing..any], StringComparison.Ordinal);
    }

    [Fact]
    public void TheHostObjectLookup_KeysOnTheGroupingKey_SoAProcGroupTakesAnyFragmentsText()
    {
        var sql = DarlingDataReader.TopQueriesByHostObjectSql;
        Assert.Contains("SELECT DISTINCT ON (q.database_name, q.host_object_name, CASE WHEN q.host_object_name IS NULL THEN q.query_hash END)", sql, StringComparison.Ordinal);
        Assert.Contains("AND t.group_hash IS NOT DISTINCT FROM CASE WHEN r.host_object_name IS NULL THEN r.query_hash END", sql, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------ #5313: the bounded fill

    [Theory]
    [MemberData(nameof(Statements))]
    public void TheCandidateLimitIsAParameter_NotAFixedFive_AndTheCountRidesLast(string name)
    {
        var sql = Sql(name);
        Assert.DoesNotContain("+ 5", sql, StringComparison.Ordinal);
        var limit = name is "TopQueriesHourlySql" or "ViewerTopQueriesSql" ? "LIMIT $6" : "LIMIT $7";
        Assert.Single(Regex.Matches(sql, Regex.Escape(limit) + @"\s*$", RegexOptions.Multiline));
        Assert.Single(Regex.Matches(sql, @"LIMIT \$4\s*$", RegexOptions.Multiline));   /* the final cap stays top */
        Assert.Matches(@"\(SELECT COUNT\(\*\) FROM (winners|ranked)\) AS candidate_count", sql);
        Assert.Single(Regex.Matches(sql, "candidate_count"));
    }

    [Fact]
    public void TheCallersRunTheStatementsThroughTopFill()
    {
        var reader = System.IO.File.ReadAllText(FindSource("PerformanceMonitor.Darling.Service/Mcp/DarlingDataReader.cs"));
        Assert.Equal(2, Regex.Matches(reader, @"TopFill\.RunAsync\(top,").Count);   /* the raw route and the hourly route */
        var viewer = System.IO.File.ReadAllText(FindSource("PerformanceMonitor.Darling.Viewer/ViewerDataService.QueryStats.cs"));
        Assert.Single(Regex.Matches(viewer, @"TopFill\.RunAsync\(top,"));
    }

    private static string FindSource(string relative)
    {
        var dir = new System.IO.DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var path = System.IO.Path.Combine(dir.FullName, "Darling", relative);
            if (System.IO.File.Exists(path))
            {
                return path;
            }

            path = System.IO.Path.Combine(dir.FullName, relative);
            if (System.IO.File.Exists(path))
            {
                return path;
            }

            dir = dir.Parent;
        }

        throw new System.IO.FileNotFoundException(relative);
    }

    [Fact]
    public void FirstRound_IsTopPlusFive_TheOldOverFetch()
    {
        Assert.Equal(30, TopFill.FirstCandidates(25));
        Assert.Equal(int.MaxValue, TopFill.FirstCandidates(int.MaxValue));
    }

    [Theory]
    [InlineData(25, 1, 30, 25, 30)]    /* a full page stops */
    [InlineData(25, 1, 30, 20, 12)]    /* short, but the candidates ran out (12 < 30): nothing more to fetch */
    [InlineData(25, 1, 30, 0, 0)]      /* no row, so no count: treated as exhausted */
    [InlineData(25, 3, 225, 10, 225)]  /* the round bound */
    public void NoRefill_WhenFull_Exhausted_OrAtTheBound(int top, int round, int candidates, int returned, int candidateCount)
    {
        Assert.Null(TopFill.NextCandidates(top, round, candidates, returned, candidateCount));
    }

    [Fact]
    public void Refill_GrowsFourfold_CappedAtTopPlus200()
    {
        Assert.Equal(120, TopFill.NextCandidates(25, 1, 30, 20, 30));
        Assert.Equal(225, TopFill.NextCandidates(25, 2, 120, 20, 120));   /* 480 would pass top + 200 */
        Assert.Null(TopFill.NextCandidates(25, 2, 225, 20, 225));         /* already at the cap */
    }

    [Fact]
    public async Task Run_StopsAtTheFirstFullPage_AfterOneRound()
    {
        var asked = new List<int>();
        var rows = await TopFill.RunAsync(25, c =>
        {
            asked.Add(c);
            return Task.FromResult((Enumerable.Range(0, 25).ToList(), c));
        });
        Assert.Equal(new[] { 30 }, asked);
        Assert.Equal(25, rows.Count);
    }

    [Fact]
    public async Task Run_FillsThePageWhenMoreThanFiveShellsAreTrimmed()
    {
        /* 40 WAITFOR shells lead the ranking: the page can only fill once the candidate limit passes 40 + 25. */
        var asked = new List<int>();
        var rows = await TopFill.RunAsync(25, c =>
        {
            asked.Add(c);
            var survivors = Math.Max(0, Math.Min(c, 1000) - 40);
            return Task.FromResult((Enumerable.Range(0, Math.Min(25, survivors)).ToList(), Math.Min(c, 1000)));
        });
        Assert.Equal(new[] { 30, 120 }, asked);
        Assert.Equal(25, rows.Count);
    }

    [Fact]
    public async Task Run_NeverRunsMoreThanThreeRounds_AndNeverPassesTheCandidateCap()
    {
        var asked = new List<int>();
        var rows = await TopFill.RunAsync(25, c =>
        {
            asked.Add(c);
            return Task.FromResult((new List<int>(), c));   /* every candidate trimmed, always more behind them */
        });
        Assert.Equal(new[] { 30, 120, 225 }, asked);
        Assert.Empty(rows);
        Assert.All(asked, c => Assert.True(c <= 25 + TopFill.MaxExtraCandidates));
    }

    [Fact]
    public async Task Run_StopsWhenTheCandidatesRunOut_WithAShortPage()
    {
        var asked = new List<int>();
        var rows = await TopFill.RunAsync(25, c =>
        {
            asked.Add(c);
            return Task.FromResult((Enumerable.Range(0, 3).ToList(), 12));   /* only 12 candidates exist, 9 of them shells */
        });
        Assert.Equal(new[] { 30 }, asked);
        Assert.Equal(3, rows.Count);
    }
}
