/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5226 source pins for the page half of the ranking choice (no database): the selector on the Top Queries and Top
/// Procedures cards, where its choice lives, what it sends, and that the card draws the reads ranking's retention
/// notice. The server half is pinned by <c>TopRankingTests</c> and the live classes.
/// </summary>
public sealed class TopRankingPagePinTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")
            .ReplaceLineEndings("\n");

    private static string Panels() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js")
            .ReplaceLineEndings("\n");

    /// <summary>The page offers exactly the server's whitelist, in the server's order, and the choice lives at module scope.</summary>
    [Fact]
    public void TheSelectorOffersTheServersWhitelist_AndTheChoiceLivesAtModuleScope()
    {
        var js = Tab();
        var list = Regex.Match(js, @"const TOP_RANKINGS = \[(?<body>[^\]]*)\];");
        Assert.True(list.Success, "TOP_RANKINGS must declare the choices.");
        var values = Regex.Matches(list.Groups["body"].Value, @"value: ""([a-z]+)"", label: ""([A-Za-z]+)""");
        Assert.Equal(
            new[] { "cpu=CPU", "duration=Duration", "reads=Reads", "executions=Executions" },
            values.Select(m => m.Groups[1].Value + "=" + m.Groups[2].Value).ToArray());

        /* Column zero of the file: a module-scope binding, so the 60 s rebuild of a tab (a new card each time) keeps the pick. */
        Assert.Matches(@"(?m)^const topRankingPick = \{ queries: ""cpu"", procedures: ""cpu"" \};", js);
        /* A pick stores the choice and draws the card again, so the title follows it. */
        Assert.Contains("topRankingPick[kind] = value;\n          draw();", js, StringComparison.Ordinal);
        Assert.Contains("\"Top Queries by \" + topRankingLabel(ranking)", js, StringComparison.Ordinal);
        Assert.Contains("\"Top Procedures by \" + topRankingLabel(ranking)", js, StringComparison.Ordinal);
    }

    /// <summary>The default read is unchanged: <c>order_by</c> is sent only for a pick that is not CPU.</summary>
    [Fact]
    public void OrderByIsSentOnlyForANonDefaultPick()
    {
        var js = Tab();
        Assert.Contains(
            "return ranking === \"cpu\" ? params : { ...params, order_by: ranking };",
            js, StringComparison.Ordinal);
        /* Every read of the two lists goes through it, so no call site can send order_by (or forget to) on its own. */
        Assert.DoesNotContain("order_by:", js.Replace("{ ...params, order_by: ranking }", ""), StringComparison.Ordinal);
    }

    /// <summary>
    /// A reads ranking over a window past what raw keeps carries the raw route's retention notice (<c>retention_notice</c>), and the page
    /// draws it ONCE (#5299): readTool puts it in the window note's place (util.js), so each card names <c>truncation_note</c> as its one
    /// note and holds no second key or strip for it. What each card draws is run by <c>TopRankingNoticeBehaviourTests</c>.
    /// </summary>
    [Fact]
    public void TheRetentionNotice_RidesTheWindowNote_SoNoCardDrawsItTwice()
    {
        var js = Tab();
        Assert.DoesNotContain("RANKING_NOTE_KEYS", js, StringComparison.Ordinal);
        /* The three plain grids (queries on the CPU tab, procedures on both tabs) name the window note and no further note key... */
        Assert.Equal(3, Regex.Matches(js, @"""truncation_note"",\s*null,\s*TOP_(QUERY|PROC)_GROUPS,\s*picker").Count);
        /* ...and the Queries tab's composite draws no strip of its own for the retention notice. */
        Assert.DoesNotContain("noticeStrip(res.data.retention_notice)", js, StringComparison.Ordinal);
        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js").ReplaceLineEndings("\n");
        Assert.Contains("const note = retentionNoticeOf(res.data) ?? windowNoteText(res.data);", util, StringComparison.Ordinal);
    }

    /// <summary>The selector sits under the card's title: panels.js draws an optional <c>control</c> before the body.</summary>
    [Fact]
    public void TheCardDrawsItsControlUnderTheTitle_BeforeTheBody()
    {
        Assert.Matches(@"desc\.control \|\| null,\s*body,", Panels());
        Assert.Contains("control,\n    body,", Tab(), StringComparison.Ordinal);
    }
}
