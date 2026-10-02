/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The server page's heading is the server's display name, not its registry key.
///
/// <para><b>The defect.</b> The sidebar, the fleet cards and every other page name a server by the fleet card's
/// <c>display_name</c>, but the server page printed the route's server KEY in its <c>&lt;h2&gt;</c>. For most
/// servers the two are the same word; for an Azure SQL Database the key is "host:database", so the page opened
/// under a heading no other screen used for that server.</para>
///
/// <para><b>The fix.</b> The heading starts as the key (what the route knows before any read returns, and what a
/// failed fleet read leaves behind) and <c>fillServerHead</c> - run for a remembered card at once and for a fresh
/// card when it lands - swaps in the card's <c>display_name</c> when it has one. Only the words on screen change:
/// the route, the sidebar links, the sub-tab links and every <c>/api/read</c> call keep the key.</para>
///
/// <para>This repository carries no JavaScript test runner, so these are source pins over the shipped module (the
/// <see cref="ChartWindowDomainTests"/> pattern). The browser tab title is the static "Darling Web" in
/// <c>index.html</c> and no script sets <c>document.title</c>, so there is no second place that printed the key.</para>
/// </summary>
public sealed class ServerPageTitlePinTests
{
    private static string ServerJs => ReadRepoFileLf(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server.js"));

    /// <summary>The heading is a node the card fill can reach - created holding the key, placed in the header
    /// as that node - and both places the header is filled from a card hand it over.</summary>
    [Fact]
    public void TheHeading_IsANodeTheCardFillCanRename_StartingAsTheKey()
    {
        var js = ServerJs;

        Assert.Contains("const title = el(\"h2\", { text: server });", js, StringComparison.Ordinal);
        Assert.Contains("el(\"span\", { class: \"server-title\" }, [dot, title]),", js, StringComparison.Ordinal);
        Assert.DoesNotContain("el(\"h2\", { text: server })]", js, StringComparison.Ordinal);

        /* The remembered-card paint and the fresh-card callback both fill the header, and both pass the heading. */
        Assert.Equal(2, Regex.Matches(js, @"^\s+" + Regex.Escape("fillServerHead(title, dot, badgeSlot, engineSlot, whySlot, "), RegexOptions.Multiline).Count);
    }

    /// <summary>The card's display name replaces the key inside <c>fillServerHead</c> itself, and only when the
    /// card has one - a card without a display name leaves the key that is already in the heading.</summary>
    [Fact]
    public void TheCardFill_PutsTheDisplayNameInTheHeading_WhenTheCardHasOne()
    {
        var js = ServerJs;

        var fill = Regex.Match(js, @"function fillServerHead\(title, dot, badgeSlot, engineSlot, whySlot, card, reason\) \{\n(?<body>.*?)\n\}\n", RegexOptions.Singleline);
        Assert.True(fill.Success, "fillServerHead(title, dot, ...) is no longer declared in server.js.");

        var body = fill.Groups["body"].Value;
        Assert.Contains("if (!card) return;", body, StringComparison.Ordinal);
        Assert.Contains("if (card.display_name) title.textContent = card.display_name;", body, StringComparison.Ordinal);

        /* The heading is written before the fill reads anything else off the card, so the name is on screen with
           the band badge rather than a render later. */
        Assert.True(
            body.IndexOf("title.textContent", StringComparison.Ordinal) < body.IndexOf("dot.className", StringComparison.Ordinal),
            "the heading is filled after the band dot");
    }

    /// <summary>The key stays the identity: the page still finds its card by key OR display name, builds its
    /// sub-tab links and reads from the key, and never reassigns it to the display name.</summary>
    [Fact]
    public void TheRouteKey_StaysTheIdentity_OnlyTheHeadingChanges()
    {
        var js = ServerJs;

        Assert.Contains("const matches = (c) => c.server_name === server || c.display_name === server;", js, StringComparison.Ordinal);
        Assert.Contains("current = { server, tab: null };", js, StringComparison.Ordinal);
        Assert.Contains("mount(main, [head, whySlot, tabsSlot, gridNode]);", js, StringComparison.Ordinal);
        Assert.DoesNotContain("server = card.display_name", js, StringComparison.Ordinal);
        Assert.DoesNotContain("current = { server: card.display_name", js, StringComparison.Ordinal);
    }
}
