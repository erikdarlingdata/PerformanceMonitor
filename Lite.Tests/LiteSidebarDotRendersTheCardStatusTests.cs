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
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2458: the sidebar row's status dot stops deriving its own copy of the four-word status ladder.
///
/// <para><b>What was wrong.</b> <c>ServerConnection.DotStatus</c> computed "Unknown"/"Online"/"Warning"/"Offline"
/// from its own <c>(IsOnline, HasCollectorErrors)</c> pair — the same ladder the Overview card derives, on a
/// different type, on a different surface, from a different instance of the same flags. The two could therefore
/// say different things about one server and nothing would notice. #2451 collapsed the card's five renderings onto
/// <see cref="ServerCardStatus"/> for exactly this reason and pinned the collapse — but the pin counted one literal
/// in one file, and this fourth copy was in a file that scan never opened.</para>
///
/// <para><b>What is pinned here.</b> That the two surfaces agree BY CONSTRUCTION rather than by observation — both
/// render <see cref="ServerCardStatusRules"/> — and that the dot now says what it means. The dot is the thing a
/// reader points at first, which is @ehaar's #2422 complaint one surface over from where it was reported.</para>
///
/// <para><b>What is deliberately NOT here.</b> Collection freshness. #2457 kept it out of the status word and gave
/// it its own banded row on the card, because folding it in recreates the #2429/#2422 conflation of a stale
/// collection with a failing one. <see cref="TheDot_CannotBandFreshnessEvenByAccident"/> holds that line at the
/// classifier's signature, and the tooltip tells the reader where the freshness answer actually lives.</para>
/// </summary>
public sealed class LiteSidebarDotRendersTheCardStatusTests
{
    private static ServerConnection Connection(bool? isOnline, bool? hasCollectorErrors) =>
        new() { ServerName = "srv", IsOnline = isOnline, HasCollectorErrors = hasCollectorErrors };

    private static ServerSummaryItem Card(bool? isOnline, bool hasCollectorErrors) =>
        new() { ServerName = "srv", IsOnline = isOnline, HasCollectorErrors = hasCollectorErrors };

    /// <summary>
    /// The defect itself: the sidebar and the card, handed the same flags, land on the same state and the same
    /// word. Sampled over every reachable combination rather than the happy one, because the pairs that drifted on
    /// #2429 and #2451 were both edge combinations nobody looked at.
    /// </summary>
    [Fact]
    public void TheDot_AndTheCard_RenderOneLadder()
    {
        foreach (var isOnline in new bool?[] { true, false, null })
        {
            foreach (var hasErrors in new[] { true, false })
            {
                var dot = Connection(isOnline, hasErrors);
                var card = Card(isOnline, hasErrors);

                Assert.Equal(card.CardStatus, dot.CardStatus);
                Assert.Equal(card.StatusDisplay, dot.DotStatus);
                Assert.StartsWith(dot.DotTooltip.Split('\n')[0], card.StatusTooltip, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// The four words are unchanged, so the sidebar's DataTriggers keep painting the dot they always did. This is
    /// the compatibility half of the collapse: <c>Word()</c> feeds a XAML <c>DataTrigger Value=</c> match, and a
    /// word that stopped matching would fall through to the muted default rather than fail anything.
    /// </summary>
    [Fact]
    public void TheDotWords_AreTheOnesTheSidebarPaints()
    {
        Assert.Equal("Online", Connection(true, false).DotStatus);
        Assert.Equal("Warning", Connection(true, true).DotStatus);
        Assert.Equal("Offline", Connection(false, false).DotStatus);
        Assert.Equal("Unknown", Connection(null, false).DotStatus);

        /* An offline server's collector-error marker must not turn its dot amber — the flags resolve in order. */
        Assert.Equal("Offline", Connection(false, true).DotStatus);

        /* HasCollectorErrors is nullable on this type only. Null means nobody has established collector health,
           which is not the same claim as "collectors are failing" — the string ladder folded it to Online and
           that reading is kept. */
        Assert.Equal("Online", Connection(true, null).DotStatus);

        var xaml = ReadRepoFile(Path.Combine("Lite", "MainWindow.xaml"));
        foreach (var word in new[] { "Online", "Offline", "Warning" })
        {
            Assert.Contains(
                "<DataTrigger Binding=\"{Binding Server.Connection.DotStatus}\" Value=\"" + word + "\">",
                xaml, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The dot says what it means, in the card's words. The first line is byte-for-byte the card's, because that
    /// is the whole argument for doing every surface: a reader moving between them meets one vocabulary rather
    /// than three levels of helpfulness.
    /// </summary>
    [Fact]
    public void TheDotTooltip_OpensOnTheSentenceTheCardOpensOn()
    {
        Assert.StartsWith("Online — the last connection check succeeded", Connection(true, false).DotTooltip, StringComparison.Ordinal);
        Assert.StartsWith("Warning — one or more collectors are failing on this server", Connection(true, true).DotTooltip, StringComparison.Ordinal);
        Assert.StartsWith("Offline — the last connection check failed", Connection(false, false).DotTooltip, StringComparison.Ordinal);
        Assert.StartsWith("Unknown — this server has not been connection-checked yet", Connection(null, false).DotTooltip, StringComparison.Ordinal);

        /* And it ends on the gesture THIS surface supports. The card's line names the card; naming a single click
           on either would be naming a no-op, since both handlers act only on a double-click. */
        Assert.EndsWith("Double-click the row to open this server's tab", Connection(true, false).DotTooltip, StringComparison.Ordinal);
        Assert.EndsWith("Double-click the card to open this server's tab", Card(true, false).StatusTooltip, StringComparison.Ordinal);

        var xaml = ReadRepoFile(Path.Combine("Lite", "MainWindow.xaml"));
        Assert.Contains("MouseDoubleClick=\"ServerListView_MouseDoubleClick\"", xaml, StringComparison.Ordinal);
    }

    /// <summary>
    /// The dot actually carries the tooltip. Removing the attribute compiles perfectly clean and silently returns
    /// the sidebar to a coloured circle that will not say what it means, which is why this reads XAML — no
    /// assertion about a C# object can reach into an element's attributes.
    /// </summary>
    [Fact]
    public void TheSidebarDot_IsBoundToItsTooltip()
    {
        var xaml = ReadRepoFile(Path.Combine("Lite", "MainWindow.xaml"));

        var at = xaml.IndexOf("{Binding Server.Connection.DotStatus}", StringComparison.Ordinal);
        Assert.True(at > 0, "the sidebar status dot is gone — find where it moved before editing this test");

        /* Walk back to the Ellipse that owns those triggers and assert the ToolTip is on the element itself. */
        var open = xaml.LastIndexOf("<Ellipse ", at, StringComparison.Ordinal);
        Assert.True(open > 0, "the sidebar status dot is no longer an Ellipse");

        var element = xaml[open..xaml.IndexOf(">", open, StringComparison.Ordinal)];
        Assert.Contains("ToolTip=\"{Binding Server.Connection.DotTooltip}\"", element, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2457 kept collection freshness out of the status word on purpose, and this dot is the surface where that
    /// separation is easiest to lose: <c>ServerConnection</c> carries no last-collection time, so a green dot here
    /// is a connection answer being read somewhere that offers no freshness answer at all.
    ///
    /// <para>Two things hold the line. The classifier takes exactly two arguments, so freshness cannot be folded in
    /// without changing a signature this test names — the failure mode being guarded is a plausible, well-meant
    /// edit, not a typo. And the tooltip tells the reader where the freshness answer lives instead of leaving them
    /// to infer it from a colour, which is the #2429/#2422 conflation in miniature.</para>
    /// </summary>
    [Fact]
    public void TheDot_CannotBandFreshnessEvenByAccident()
    {
        const string disclaimer =
            "It reports the connection check only, not collection freshness — the Overview card's Last Collect row bands that.";

        foreach (var isOnline in new bool?[] { true, false, null })
        {
            Assert.Contains(disclaimer, Connection(isOnline, false).DotTooltip, StringComparison.Ordinal);
        }

        var rules = ReadRepoFile(Path.Combine("Lite", "Services", "LocalDataService.Overview.cs"));
        Assert.Contains(
            "public static ServerCardStatus Classify(bool? isOnline, bool hasCollectorErrors) => isOnline switch",
            rules, StringComparison.Ordinal);

        /* And the sidebar has nothing to band it FROM, which is why unifying the ladder was the whole of #2458 and
           plumbing a collection time to this surface was split off. If that ever changes, this assertion is the
           place the decision gets made rather than discovered. */
        var sidebar = ReadRepoFile(Path.Combine("Lite", "Models", "ServerConnection.cs"));
        Assert.DoesNotContain("LastCollection", sidebar, StringComparison.Ordinal);
    }

    /// <summary>
    /// The pin that would actually have caught this one. #2451 counted the literal <c>"IsOnline switch"</c>
    /// at exactly one occurrence, in exactly one file — and the fourth copy it existed to forbid was already
    /// sitting in ANOTHER file, written as a chain of <c>if</c> statements rather than a switch. It evaded
    /// that pin on both axes at once, and went on evading it through #2451 and #2457.
    ///
    /// <para>So the invariant is not "one switch". It is that the four words are WRITTEN once. Scanning for
    /// them as string literals is syntax-agnostic: an <c>if</c> chain, a switch, a dictionary and a ternary
    /// all have to spell them, and none of them can spell them here any more. Comments are stripped first
    /// because <c>DotStatus</c>'s own doc comment legitimately names all four while the code writes none.</para>
    /// </summary>
    [Fact]
    public void TheFourWords_AreWrittenInExactlyOnePlace()
    {
        var sidebar = WithoutComments(ReadRepoFile(Path.Combine("Lite", "Models", "ServerConnection.cs")));

        foreach (var word in new[] { "Online", "Warning", "Offline", "Unknown" })
        {
            Assert.DoesNotContain("\"" + word + "\"", sidebar, StringComparison.Ordinal);
        }

        /* And they are written in the one function both surfaces render. */
        var rules = ReadRepoFile(Path.Combine("Lite", "Services", "LocalDataService.Overview.cs"));
        Assert.Contains(
            "public static string Word(this ServerCardStatus status) => status switch",
            rules, StringComparison.Ordinal);
    }

    /// <summary>Drops line comments so a literal scan is neither defeated nor falsely tripped by prose.
    /// <c>ServerConnection.cs</c> carries exactly one block comment — the licence header at the top — so
    /// dropping <c>//</c>-prefixed lines is sufficient and cannot eat a string literal.</summary>
    private static string WithoutComments(string source) =>
        string.Join("\n", source.Split('\n').Where(line => !line.TrimStart().StartsWith("//", StringComparison.Ordinal)));

    /// <summary>Locates a repo file by walking up from this file's compile-time path — the same helper
    /// <c>LiteOverviewCardExplainsItselfTests</c> uses, and for the same reason: the assertions above are about
    /// text that lives in files, not about objects.</summary>
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate).Replace("\r\n", "\n", StringComparison.Ordinal);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
