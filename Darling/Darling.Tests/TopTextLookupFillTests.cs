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

    /// <summary>The raw statements (and the Viewer's) read the text in ONE pass over the winners. The hourly statement is
    /// different on purpose and has its own test below.</summary>
    public static IEnumerable<object[]> RawStatements() =>
        new[] { "TopQueriesSql", "TopQueriesByHostObjectSql", "ViewerTopQueriesSql" }.Select(n => new object[] { n });

    [Theory]
    [MemberData(nameof(RawStatements))]
    public void TheLatestTextIsOneLookupForAllRankedGroups_NotOnePerRow(string name)
    {
        var sql = Sql(name);
        /* Materialized: the join to the lookup is NULL-safe, which PostgreSQL cannot hash, so a plain CTE is inlined into a
           nested loop and re-run per ranked row (#5309's per-row re-run in a new shape; TopTextLookupPlanLiveTests reads it
           from the plan). */
        Assert.Contains("latest_text AS MATERIALIZED (", sql, StringComparison.Ordinal);
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
        var lookup = sql[sql.IndexOf("latest_text AS MATERIALIZED (", StringComparison.Ordinal)..];
        var end = lookup.IndexOf("LEFT JOIN query_text_dim AS d ON d.digest = l.query_text_digest", StringComparison.Ordinal);
        lookup = lookup[..end];
        Assert.Contains("q.collection_time >= $2", lookup, StringComparison.Ordinal);
        Assert.Contains("q.collection_time <= $3", lookup, StringComparison.Ordinal);
        Assert.Contains("q.server_id = $1", lookup, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHourlyWindowLookup_ProbesTheIndexOncePerRankedKey_NotTheWholeWindow()
    {
        var sql = DarlingDataReader.TopQueriesHourlySql;
        var window = sql.IndexOf("latest_in_window AS (", StringComparison.Ordinal);
        var lookup = sql[window..sql.IndexOf("missing AS MATERIALIZED (", StringComparison.Ordinal)];
        /* One probe per ranked key: a lateral over ranked, each arm an ordered LIMIT 1 on the indexed hash equality
           (idx_query_stats_server_hash_time: server_id, query_hash, collection_time DESC). The old form read every raw row
           of the window and hash joined it to the ranked keys (1,439,400 rows at 168 hours to find 25 texts). */
        Assert.Contains("FROM ranked AS rk", lookup, StringComparison.Ordinal);
        Assert.Contains("CROSS JOIN LATERAL", lookup, StringComparison.Ordinal);
        Assert.DoesNotContain("DISTINCT ON", lookup, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN ranked", lookup, StringComparison.Ordinal);
        Assert.Contains("q.query_hash = rk.query_hash", lookup, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(lookup, @"ORDER BY q\.collection_time DESC, q\.collection_id DESC\s+LIMIT 1").Count);   /* N3: a tie on the time breaks on the row collected last */
        /* NULL-safe on every key column: the database by IS NOT DISTINCT FROM, the hash by its own NULL arm. */
        Assert.Contains("q.database_name IS NOT DISTINCT FROM rk.database_name", lookup, StringComparison.Ordinal);
        Assert.Contains("q.query_hash IS NULL", lookup, StringComparison.Ordinal);
        Assert.Contains("rk.query_hash IS NULL", lookup, StringComparison.Ordinal);
        /* One text lookup per statement still: the window pass, then the fallback for the keys it found nothing for. */
        Assert.Single(Regex.Matches(sql, @"LEFT JOIN latest_in_window AS w"));
        Assert.Single(Regex.Matches(sql, @"FROM latest_in_window"));
    }

    [Fact]
    public void TheHourlyLookup_ReadsTheWindowFirst_ThenOnlyTheKeysTheWindowHadNoTextFor()
    {
        var sql = DarlingDataReader.TopQueriesHourlySql;
        var window = sql.IndexOf("latest_in_window AS (", StringComparison.Ordinal);
        var missing = sql.IndexOf("missing AS MATERIALIZED (", StringComparison.Ordinal);
        var any = sql.IndexOf("latest_any AS MATERIALIZED (", StringComparison.Ordinal);
        var final = sql.IndexOf("COALESCE(w.query_text, a.query_text) AS query_text", StringComparison.Ordinal);
        Assert.True(window > 0 && window < missing && missing < any && any < final);
        var windowPass = sql[window..missing];
        Assert.Contains("q.collection_time >= $2", windowPass, StringComparison.Ordinal);
        Assert.Contains("q.collection_time < $3", windowPass, StringComparison.Ordinal);
        /* The fallback has no window bound (the rollup outlives raw), but it joins only the missing keys. */
        var anyPass = sql[any..final];
        Assert.DoesNotContain("collection_time >=", anyPass, StringComparison.Ordinal);
        /* #5299 round 2 (N1): a per-key lateral probe of the hash index (two arms, so a NULL-hash key still finds its text),
           not one DISTINCT ON over every raw row of the server joined to the missing keys. */
        Assert.Contains("FROM missing AS m", anyPass, StringComparison.Ordinal);
        Assert.Contains("CROSS JOIN LATERAL", anyPass, StringComparison.Ordinal);
        Assert.DoesNotContain("DISTINCT ON", anyPass, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(anyPass, @"LIMIT 1\s*$", RegexOptions.Multiline).Count);
        Assert.Contains("NOT EXISTS", sql[missing..any], StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------ #5299 round 2: N3 and N6, a total order

    [Fact]
    public void EveryNewestTextPick_BreaksATieOnTheTimeOnTheRowCollectedLast()
    {
        /* Two rows of one key at one collection_time (one shape hash, two plans, one collection) must give the same text every
           run, because the text also decides the WAITFOR trim. collection_id is unique per row, so it is a total tie-break. */
        foreach (var (name, sql) in new[]
        {
            ("TopQueriesSql", DarlingDataReader.TopQueriesSql),
            ("TopQueriesByHostObjectSql", DarlingDataReader.TopQueriesByHostObjectSql),
            ("TopQueriesHourlySql", DarlingDataReader.TopQueriesHourlySql),
            ("QueryStoreTopSql", DarlingDataReader.QueryStoreTopSql),
            ("QueryStoreTopTableSql", DarlingDataReader.QueryStoreTopTableSql),
            ("QueryStoreTopDailyTableSql", DarlingDataReader.QueryStoreTopDailyTableSql),
            ("ViewerTopQueriesSql", ViewerDataService.TopQueriesSql),
            ("ViewerQueryStoreTopSql", ViewerDataService.QueryStoreTopSql),
        })
        {
            var picks = Regex.Matches(sql, @"ORDER BY[^\n]*\b[qs]\.collection_time DESC[^\n]*").Select(m => m.Value).ToList();
            Assert.NotEmpty(picks);
            Assert.All(picks, pick => Assert.True(pick.Contains("collection_id DESC", StringComparison.Ordinal),
                $"{name}: the newest-text pick '{pick.Trim()}' has no tie-break after collection_time"));
        }
    }

    [Fact]
    public void TheQueryStoreAndViewerTopStatements_RankOnATotalOrder()
    {
        /* Ranking value, then the whole group key: an exact tie at the candidate boundary must not pick different members in a
           refill round. The page order uses the same columns the ranking did. */
        const string queryStoreKey = "database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role";
        foreach (var sql in new[] { DarlingDataReader.QueryStoreTopSql, DarlingDataReader.QueryStoreTopTableSql, DarlingDataReader.QueryStoreTopDailyTableSql, ViewerDataService.QueryStoreTopSql })
        {
            Assert.Contains("DESC, " + queryStoreKey + "\n", sql.ReplaceLineEndings("\n"), StringComparison.Ordinal);
            Assert.Contains("DESC, r.database_name, r.query_id, r.plan_id, r.query_hash, r.execution_type_desc, r.replica_role) AS page_ord", sql, StringComparison.Ordinal);
        }

        var viewer = ViewerDataService.TopQueriesSql.ReplaceLineEndings("\n");
        Assert.Contains("ORDER BY SUM(delta_elapsed_time) DESC, database_name, query_hash, host_object_name\n", viewer, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER (ORDER BY r.total_elapsed_us DESC, r.database_name, r.query_hash, r.host_object_name) AS page_ord", viewer, StringComparison.Ordinal);
        Assert.Contains("ORDER BY r.total_elapsed_us DESC, r.database_name, r.query_hash, r.host_object_name\n", viewer, StringComparison.Ordinal);
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
        /* The count rides on a row of its own, joined to the page, so a round whose candidates were ALL trimmed still
           reports it (an empty page used to read as exhausted). page_ord is the page's last column and is NULL on that row. */
        Assert.Matches(@"FROM \(SELECT COUNT\(\*\) AS candidate_count FROM (winners|ranked)\) AS c\s+LEFT JOIN page AS p ON TRUE", sql);
        Assert.Single(Regex.Matches(sql, @"SELECT p\.\*, c\.candidate_count"));
        Assert.Single(Regex.Matches(sql, @"AS page_ord"));
        Assert.Matches(@"ORDER BY p\.page_ord\s*$", sql);
    }

    [Fact]
    public void TheCallersRunTheStatementsThroughTopFill()
    {
        var reader = System.IO.File.ReadAllText(FindSource("PerformanceMonitor.Darling.Service/Mcp/DarlingDataReader.cs"));
        Assert.Equal(4, Regex.Matches(reader, @"TopFill\.RunAsync\(top,").Count);   /* raw and hourly Top Queries, then the Query Store raw read and its table read (daily shares the table call site) */
        var viewer = System.IO.File.ReadAllText(FindSource("PerformanceMonitor.Darling.Viewer/ViewerDataService.QueryStats.cs"));
        Assert.Single(Regex.Matches(viewer, @"TopFill\.RunAsync\(top,"));
        var viewerQs = System.IO.File.ReadAllText(FindSource("PerformanceMonitor.Darling.Viewer/ViewerDataService.QueryStore.cs"));
        Assert.Equal(2, Regex.Matches(viewerQs, @"TopFill\.RunAsync\(top,").Count);   /* raw and interval table */
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
    [InlineData(25, 1, 30, 0, 0)]      /* the round found no candidates at all: exhausted */
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
    public async Task Run_AnEmptyRoundThatSawAFullCandidateLimit_AsksAgain_UpToTheBound()
    {
        /* #5313: every candidate is a shell, so each round trims to nothing and returns no page row, but it still reports
           that it considered its whole candidate limit. The fill must go on: 30, 120, 225 candidates and then stop. */
        var asked = new List<int>();
        var rows = await TopFill.RunAsync(25, c =>
        {
            asked.Add(c);
            return Task.FromResult((new List<int>(), c));
        });
        Assert.Equal(new[] { 30, 120, 225 }, asked);   /* at most three rounds, never more than top + 200 candidates */
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Run_AnIdleWindow_CostsOneRound()
    {
        /* No candidates at all: the count row says 0, which is under the first limit, so there is nothing to refill. */
        var asked = new List<int>();
        var rows = await TopFill.RunAsync(25, c =>
        {
            asked.Add(c);
            return Task.FromResult((new List<int>(), 0));
        });
        Assert.Equal(new[] { 30 }, asked);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Run_AnEmptyFirstRound_ThenRealRows_FillsThePage()
    {
        var asked = new List<int>();
        var rows = await TopFill.RunAsync(3, c =>
        {
            asked.Add(c);
            return Task.FromResult(c < 30 ? (new List<int>(), c) : (new List<int> { 1, 2, 3 }, c));
        });
        Assert.Equal(new[] { 8, 32 }, asked);
        Assert.Equal(3, rows.Count);
    }

    // ------------------------------------------------------------------ the Query Store twin (#5313)

    public static IEnumerable<object[]> QueryStoreStatements() =>
        new[] { "McpRaw", "McpTable", "McpDaily", "ViewerRaw", "ViewerTable" }.Select(n => new object[] { n });

    private static (string Sql, string Limit) QueryStoreSql(string name) => name switch
    {
        "McpRaw" => (DarlingDataReader.QueryStoreTopSql, "LIMIT $8"),
        "McpTable" => (DarlingDataReader.QueryStoreTopTableSql, "LIMIT $8"),
        "McpDaily" => (DarlingDataReader.QueryStoreTopDailyTableSql, "LIMIT $10"),
        "ViewerRaw" => (ViewerDataService.QueryStoreTopSql, "LIMIT $6"),
        "ViewerTable" => (ViewerDataService.QueryStoreTopTableSql, "LIMIT $6"),
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    [Theory]
    [MemberData(nameof(QueryStoreStatements))]
    public void TheQueryStoreReads_TakeTheCandidateLimitAsAParameter_AndReportTheCountOnTheirOwnRow(string name)
    {
        var (sql, limit) = QueryStoreSql(name);
        Assert.DoesNotContain("+ 5", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, Regex.Escape(limit) + @"\s*$", RegexOptions.Multiline));
        Assert.Single(Regex.Matches(sql, @"LIMIT \$4\s*$", RegexOptions.Multiline));   /* the final cap stays top */
        Assert.Matches(@"FROM \(SELECT COUNT\(\*\) AS candidate_count FROM ranked\) AS c\s+LEFT JOIN page AS p ON TRUE", sql);
        Assert.Single(Regex.Matches(sql, @"SELECT p\.\*, c\.candidate_count"));
        Assert.Single(Regex.Matches(sql, @"AS page_ord"));
        Assert.Matches(@"ORDER BY p\.page_ord\s*$", sql);
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
