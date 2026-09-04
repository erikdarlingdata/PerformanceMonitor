/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2473: the sidebar row's status dot stops deriving its own copy of the collection-status ladder.
///
/// <para><b>What was wrong, and it was not merely duplication.</b> <c>DarlingServer.ApplyFreshness</c> and
/// <c>ServerSummaryItem.ApplyFreshness</c> start from the SAME
/// <see cref="ServerHealthClassifier.ClassifyFreshness"/> call and then each threw the discriminant away in
/// favour of flags — the card kept three of them, the sidebar row kept two. So for a registered-but-never-
/// collected server the card said amber "Awaiting first collection" and the dot beside it went grey
/// "Unknown", on the same screen, about the same server. The dot is the thing a reader points at first, and
/// it was silently giving the pre-#2429 answer.</para>
///
/// <para><b>What is pinned here.</b> That the two surfaces agree BY CONSTRUCTION rather than by observation —
/// both render <see cref="ServerCollectionStatusRules"/> — that the dot says what it means, that every state
/// the card paints has a dot colour to match, and that the words are written in exactly one place, scanned
/// without caring what syntax a copy is written in.</para>
///
/// <para><b>Why the scan is shaped the way it is.</b> #2470 landed the Lite half of this and its pin had to be
/// widened twice: the copy it existed to forbid sat in a DIFFERENT FILE and was written as <c>if</c>
/// statements, so a scan for one literal in one file missed it on both axes at once. The scan below walks
/// three source trees, strips comments with a real lexer rather than a line prefix, and forbids the words
/// themselves — an <c>if</c> chain, a switch, a dictionary and a ternary all have to spell them.</para>
/// </summary>
public sealed class ViewerSidebarDotRendersTheCardStatusTests
{
    private static readonly DateTime Now = new(2026, 8, 21, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The four freshness inputs a real refresh can produce, as (last collection, what it means).</summary>
    public static TheoryData<int> FreshnessAgesMinutes => new() { 0, 5, 30 };

    private static DarlingServer Dot(DateTime? lastCollectionUtc)
    {
        var server = new DarlingServer(1, "SQL2022", "Prod", true, 16);
        server.ApplyFreshness(lastCollectionUtc, Now);
        return server;
    }

    private static ServerSummaryItem Card(DateTime? lastCollectionUtc)
    {
        var card = new ServerSummaryItem { ServerName = "SQL2022", ServerId = 1, LastCollectionTime = lastCollectionUtc };
        card.ApplyFreshness(Now);
        return card;
    }

    /// <summary>
    /// THE DEFECT. Handed the same last-collection instant, the sidebar row and the Overview card land on the
    /// same state and show the same word. Swept over every freshness band rather than the happy one, because
    /// the band that drifted is the one nobody looks at during a steady-state day — a fleet only ever has
    /// never-collected servers while it is bootstrapping, which is exactly when someone is watching the
    /// sidebar to see whether the bootstrap is working.
    /// </summary>
    [Fact]
    public void TheDot_AndTheCard_RenderOneLadder()
    {
        var inputs = new DateTime?[]
        {
            Now,                      // Fresh
            Now.AddMinutes(-5),       // Stale
            Now.AddMinutes(-31),      // Offline — just past the shared 30-min collection-stopped window (#2794)
            null,                     // NeverCollected — the one that disagreed
        };

        var seen = new HashSet<ServerCollectionStatus>();

        foreach (var lastCollection in inputs)
        {
            var dot = Dot(lastCollection);
            var card = Card(lastCollection);
            var where = $"last collection {lastCollection?.ToString("O") ?? "never"}";

            Assert.Equal(card.CardStatus, dot.CardStatus);
            Assert.Equal(card.StatusDisplay, dot.DotStatus);

            /* And the flags themselves, because they are what WPF binds — the card border and the offline
               overlay read them directly, so two surfaces agreeing on the word while disagreeing on
               IsOnline would still paint differently. */
            Assert.Equal(card.IsOnline, dot.IsOnline);
            Assert.Equal(card.HasCollectorErrors, dot.HasCollectorErrors);
            Assert.Equal(card.AwaitingFirstCollection, dot.AwaitingFirstCollection);

            Assert.True(seen.Add(dot.CardStatus), $"two inputs produced the same state; {where}");
        }

        /* The sweep really did reach four distinct states, so a future change that collapses one of them
           cannot leave this passing over a smaller surface than it claims. */
        Assert.Equal(4, seen.Count);
        Assert.Contains(ServerCollectionStatus.AwaitingFirstCollection, seen);
    }

    /// <summary>
    /// The dot's words are the card's words, including the fifth one the dot never had. "Unknown" is
    /// unreachable through <c>ApplyFreshness</c> and stays a named state anyway: a row constructed but not yet
    /// refreshed is in it, which is what the viewer shows for the first second after a store read.
    /// </summary>
    [Fact]
    public void TheDotWords_AreTheCardsWords()
    {
        Assert.Equal("Online", Dot(Now).DotStatus);
        Assert.Equal("Warning", Dot(Now.AddMinutes(-5)).DotStatus);
        Assert.Equal("Offline", Dot(Now.AddMinutes(-31)).DotStatus);
        Assert.Equal("Awaiting first collection", Dot(null).DotStatus);
        Assert.Equal("Unknown", new DarlingServer(1, "SQL2022", "Prod", true, 16).DotStatus);
    }

    /// <summary>
    /// Every state the card paints has a dot colour that MATCHES it, keyed off the enum rather than a list
    /// someone remembered to extend. This is the assertion that would have caught the defect on its own: the
    /// dot had triggers for three of five states and the two without one fell through to the muted grey
    /// default, which paints a wrong colour and fails nothing.
    ///
    /// <para>The dot paints from the theme dictionaries and the card from its own dark-theme hexes, so the
    /// two cannot be compared as colours. They are compared as SEVERITY: the card's hex says which family the
    /// state belongs to, and the trigger must reach for that family's brush key. Amber is deliberately shared
    /// by two states here — that is the card's own choice and the reason the dot needed a tooltip.</para>
    ///
    /// <para><c>Unknown</c> is asserted to have NO trigger. Grey is the honest paint for a server whose
    /// freshness was never classified, and it is the style's default, so an arm would be redundant — but
    /// "redundant" and "forgotten" look identical in XAML, which is the whole story of this issue. Saying
    /// which one it is here makes the next reader's edit a decision.</para>
    /// </summary>
    [Fact]
    public void EveryStateTheCardPaints_HasADotColourToMatch()
    {
        /* The card's palette, read off the card rather than retyped, mapped to the brush family it means. */
        var family = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["#FF81C784"] = "SuccessBrush",
            ["#FFFFD54F"] = "WarningBrush",
            ["#FFE57373"] = "ErrorBrush",
            ["#FF888888"] = "ForegroundMutedBrush",
        };

        var triggers = SidebarDotTriggers();

        foreach (var status in Enum.GetValues<ServerCollectionStatus>())
        {
            var word = status.Word();
            var cardColour = CardWith(status).StatusBrush.Color.ToString(System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(family.ContainsKey(cardColour), $"the card paints {status} an unrecognised {cardColour}");
            var expected = family[cardColour];

            if (expected == "ForegroundMutedBrush")
            {
                Assert.False(triggers.ContainsKey(word),
                    $"'{word}' is the muted default and must not also have a trigger");
                continue;
            }

            Assert.True(triggers.ContainsKey(word),
                $"the sidebar dot has no DataTrigger for '{word}' — it will paint the muted default and fail nothing");
            Assert.Equal(expected, triggers[word]);
        }

        /* And no trigger for a word the enum cannot produce: a stale arm left behind after a rename paints
           nothing and reads as coverage. */
        var words = Enum.GetValues<ServerCollectionStatus>().Select(s => s.Word()).ToHashSet(StringComparer.Ordinal);
        foreach (var painted in triggers.Keys)
        {
            Assert.Contains(painted, words);
        }
    }

    /// <summary>
    /// The dot carries the tooltip, and it opens on the sentence the card's vocabulary uses. Removing the
    /// attribute compiles perfectly clean and silently returns the sidebar to a coloured circle nobody can
    /// interrogate, which is why this reads XAML — no assertion about a C# object can reach an element's
    /// attributes.
    /// </summary>
    [Fact]
    public void TheSidebarDot_IsBoundToItsTooltip()
    {
        var xaml = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));

        var at = xaml.IndexOf("{Binding Server.DotStatus}", StringComparison.Ordinal);
        Assert.True(at > 0, "the sidebar status dot is gone — find where it moved before editing this test");

        var open = xaml.LastIndexOf("<Ellipse ", at, StringComparison.Ordinal);
        Assert.True(open > 0, "the sidebar status dot is no longer an Ellipse");

        var element = xaml[open..xaml.IndexOf('>', open)];
        Assert.Contains("ToolTip=\"{Binding Server.DotTooltip}\"", element, StringComparison.Ordinal);
    }

    /// <summary>
    /// What the tooltip says. The first line is the shared headline, so a reader moving between the dot and
    /// the card meets one vocabulary; it opens on the same word the dot itself shows, so the tooltip can
    /// never explain a different state than the colour is painting.
    /// </summary>
    [Fact]
    public void TheDotTooltip_ExplainsTheStateItIsPainting()
    {
        foreach (var lastCollection in new DateTime?[] { Now, Now.AddMinutes(-5), Now.AddMinutes(-30), null })
        {
            var dot = Dot(lastCollection);
            var lines = dot.DotTooltip.Split('\n');

            Assert.Equal(dot.CardStatus.Headline(), lines[0]);
            Assert.StartsWith(dot.DotStatus, lines[0], StringComparison.Ordinal);

            /* And it ends on the gesture THIS surface supports. ServerList_MouseDoubleClick opens the tab;
               a single click only selects, so naming one would be naming a no-op. */
            Assert.Equal("Double-click the row to open this server's tab", lines[^1]);
        }

        var xaml = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));
        Assert.Contains("MouseDoubleClick=\"ServerList_MouseDoubleClick\"", xaml, StringComparison.Ordinal);
    }

    /// <summary>
    /// The separation #2429 spent four review rounds on, held at the classifier's signature. In Darling every
    /// one of these words is a COLLECTION answer — there is no live ping — and the conflation that issue
    /// untangled was one amber word standing for a stale collection AND for a metric breach with nothing
    /// telling them apart. So a server whose metrics are on fire but whose collection is current is
    /// <c>Online</c>, and its severity lives on the card's rows and border where a reader can see which axis
    /// they are looking at.
    ///
    /// <para><see cref="ServerCollectionStatusRules.Classify"/> takes three flags and no metric, so folding a
    /// severity back in cannot be done without changing a signature this test names — the failure mode being
    /// guarded is a plausible, well-meant edit, not a typo. Lite holds the mirror-image line: #2457 kept
    /// collection freshness out of a word that reports a connection check there.</para>
    /// </summary>
    [Fact]
    public void TheStatusWord_CannotBandMetricsEvenByAccident()
    {
        var onFire = new ServerSummaryItem
        {
            ServerName = "SQL2022",
            ServerId = 1,
            LastCollectionTime = Now,
            CpuPercent = 99,
            DeadlockCount = 12,
            FailedCollectorCount = 3,
        };
        onFire.ApplyFreshness(Now);

        Assert.Equal(ServerCollectionStatus.Online, onFire.CardStatus);
        Assert.Equal("Online", onFire.StatusDisplay);

        /* The severity is not lost — it is reported on the axis it belongs to. */
        Assert.Equal(HealthSeverity.Critical, onFire.OverallMetricSeverity);

        var rules = ReadRepoFile(Path.Combine("PerformanceMonitor.Common", "ServerHealthBands.cs"));
        Assert.Contains(
            "public static ServerCollectionStatus Classify(bool? isOnline, bool hasCollectorErrors, bool awaitingFirstCollection) =>",
            rules, StringComparison.Ordinal);

        /* The dot tells the reader which axis it is on, rather than leaving them to infer it from a colour —
           the #2429 / #2422 conflation in miniature. */
        Assert.Contains(
            "Darling has no live ping: this is how old the newest collection is, not a connection check.",
            Dot(Now).DotTooltip, StringComparison.Ordinal);
    }

    /// <summary>
    /// The pin that would actually have caught this one, in the shape #2470 had to arrive at the hard way.
    /// #2451 counted the literal <c>"IsOnline switch"</c> at exactly one occurrence in exactly one file, and
    /// the copy it existed to forbid was already sitting in another file written as <c>if</c> statements — it
    /// evaded that pin on both axes at once.
    ///
    /// <para>So the invariant is not "one switch". It is that the WORDS are written once, scanned across
    /// every tree that can hold a copy, with comments removed by a lexer rather than a line prefix — three of
    /// the four copies this issue found sit inside files whose doc comments legitimately quote all five
    /// words.</para>
    ///
    /// <para><c>"Warning"</c> is deliberately not in the forbidden set. It is shared with the alert-badge
    /// severity, the AG-health labels and the fleet band labels, none of which are this ladder, and a text
    /// scan cannot tell them apart. It costs nothing: no copy of THIS ladder can be written without also
    /// spelling at least "Online" and "Offline".</para>
    ///
    /// <para>XAML is out of scope on purpose — <c>MainWindow.xaml</c> must spell the words to match on them,
    /// and <see cref="EveryStateTheCardPaints_HasADotColourToMatch"/> is what holds those spellings to the
    /// enum instead.</para>
    /// </summary>
    [Fact]
    public void TheStatusWords_AreWrittenInExactlyOnePlace()
    {
        var rulesFile = Path.Combine(RepoRoot(), "PerformanceMonitor.Common", "ServerHealthBands.cs");
        var forbidden = new[] { "\"Online\"", "\"Offline\"", "\"Unknown\"", "\"Awaiting first collection\"", "\"AwaitingFirstCollection\"" };

        /* Part A: the phrase only this ladder spells, anywhere in the three trees. A copy with all five
           states has to write it; nothing else in the product has any reason to. */
        var spellingTheePhrase = ScannedSources()
            .Where(f => !PathsEqual(f.Key, rulesFile))
            .Where(f => f.Value.Contains("\"Awaiting first collection\"", StringComparison.Ordinal)
                     || f.Value.Contains("\"AwaitingFirstCollection\"", StringComparison.Ordinal))
            .Select(f => f.Key)
            .ToList();

        Assert.True(spellingTheePhrase.Count == 0,
            "these files spell the never-collected state themselves instead of rendering the one ladder: "
            + string.Join(", ", spellingTheePhrase));

        /* Part B: any file that turns a freshness band into anything is only allowed to do it through the
           rules. This is the half that catches a FOUR-state copy — one that never spells the fifth word and
           so slips past Part A, which is precisely what the sidebar dot was. */
        var classifiers = ScannedSources()
            .Where(f => f.Value.Contains("ClassifyFreshness(", StringComparison.Ordinal))
            .ToList();

        var expected = new[]
        {
            Path.Combine("PerformanceMonitor.Common", "ServerHealthBands.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Overview.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"),
        };

        /* The scan is looking at something. A rename that emptied it would otherwise make every assertion
           below vacuously true, which is the failure mode a coverage check has and a test must not. */
        foreach (var relative in expected)
        {
            Assert.Contains(classifiers, f => f.Key.EndsWith(relative, StringComparison.Ordinal));
        }

        var offenders = new List<string>();
        foreach (var (path, source) in classifiers)
        {
            if (PathsEqual(path, rulesFile))
            {
                continue;
            }

            foreach (var word in forbidden)
            {
                if (source.Contains(word, StringComparison.Ordinal))
                {
                    offenders.Add($"{path} spells {word}");
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "a status word is written somewhere that classifies freshness, so two surfaces can disagree again: "
            + string.Join("; ", offenders));

        /* And they are written in the one function every surface renders. */
        var rules = ReadRepoFile(Path.Combine("PerformanceMonitor.Common", "ServerHealthBands.cs"));
        Assert.Contains("public static string Word(this ServerCollectionStatus status) => status switch", rules, StringComparison.Ordinal);
        Assert.Contains("public static string McpToken(this ServerCollectionStatus status) => status switch", rules, StringComparison.Ordinal);
        Assert.Contains("public static string Headline(this ServerCollectionStatus status) => status switch", rules, StringComparison.Ordinal);
    }

    /// <summary>
    /// The MCP token vocabulary is published to clients, so it stays spelled the way it shipped — but it is a
    /// rendering of the same decision, not a second ladder with its own thresholds. <c>list_servers</c>
    /// carried its own 2-minute and 15-minute constants until #2473, which meant
    /// <see cref="ServerHealthThresholds"/> could move and the tool would go on answering with the old ones.
    /// </summary>
    [Fact]
    public void TheMcpTokens_RenderTheSameLadder_WithoutTheirOwnThresholds()
    {
        Assert.Equal("Online", ServerCollectionStatus.Online.McpToken());
        Assert.Equal("Warning", ServerCollectionStatus.Stale.McpToken());
        Assert.Equal("Offline", ServerCollectionStatus.Offline.McpToken());
        Assert.Equal("AwaitingFirstCollection", ServerCollectionStatus.AwaitingFirstCollection.McpToken());
        Assert.Equal("Unknown", ServerCollectionStatus.Unknown.McpToken());

        /* One arm differs from Word(), and only one. The rest being identical is what makes the difference
           legible as a decision rather than as drift. */
        var differing = Enum.GetValues<ServerCollectionStatus>()
            .Where(s => !string.Equals(s.Word(), s.McpToken(), StringComparison.Ordinal))
            .ToList();
        Assert.Equal(new[] { ServerCollectionStatus.AwaitingFirstCollection }, differing);

        var tools = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));
        Assert.DoesNotContain("TimeSpan.FromMinutes(2)", tools, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeSpan.FromMinutes(15)", tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every freshness band maps to exactly one state, and the flags a surface binds are the same flags that
    /// state was derived from. Composed rather than switched twice on purpose — #2470's own correction was
    /// that a second switch is a second ladder even when it returns the right type.
    /// </summary>
    [Fact]
    public void EveryFreshnessBand_MapsToOneStateAndOneFlagTriple()
    {
        var expected = new Dictionary<ServerFreshness, ServerCollectionStatus>
        {
            [ServerFreshness.Fresh] = ServerCollectionStatus.Online,
            [ServerFreshness.Stale] = ServerCollectionStatus.Stale,
            [ServerFreshness.Offline] = ServerCollectionStatus.Offline,
            [ServerFreshness.NeverCollected] = ServerCollectionStatus.AwaitingFirstCollection,
        };

        foreach (var band in Enum.GetValues<ServerFreshness>())
        {
            var flags = ServerCollectionStatusRules.FlagsFor(band);
            var viaFlags = ServerCollectionStatusRules.Classify(flags.IsOnline, flags.HasCollectorErrors, flags.AwaitingFirstCollection);

            Assert.Equal(expected[band], ServerCollectionStatusRules.FromFreshness(band));
            Assert.Equal(expected[band], viaFlags);
        }

        /* Unknown is not reachable from a band, which is why it is a named state rather than a fall-through:
           a row that has been constructed but never refreshed is in it. */
        Assert.DoesNotContain(ServerCollectionStatus.Unknown, expected.Values);
        Assert.Equal(ServerCollectionStatus.Unknown, ServerCollectionStatusRules.Classify(null, false, false));
    }

    /// <summary>
    /// The guard runs on the changes it guards. #2471 found a pin in Lite.Tests that CI skipped for three of
    /// the trees it scanned, which is a guard that has silently stopped guarding — the same failure one layer
    /// out. This suite reads <c>PerformanceMonitor.Common</c> as well as both Darling trees, so the step that
    /// runs it has to fire on <c>core</c> as well as <c>darling</c>.
    /// </summary>
    [Fact]
    public void TheGuard_RunsOnEveryTreeItScans()
    {
        var workflow = ReadRepoFile(Path.Combine(".github", "workflows", "build.yml"));

        var step = workflow.IndexOf("- name: Run Darling tests", StringComparison.Ordinal);
        Assert.True(step > 0, "the step that runs Darling.Tests was renamed — re-point this assertion before editing it");

        var gate = workflow[step..workflow.IndexOf("run:", step, StringComparison.Ordinal)];
        Assert.Contains("steps.filter.outputs.darling == 'true'", gate, StringComparison.Ordinal);
        Assert.Contains("steps.filter.outputs.core == 'true'", gate, StringComparison.Ordinal);

        /* And the two filters really do cover the trees this file walks. */
        Assert.Contains("- 'PerformanceMonitor.Common/**/!(*.md)'", workflow, StringComparison.Ordinal);
        Assert.Contains("- 'Darling/**/!(*.md)'", workflow, StringComparison.Ordinal);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────

    /// <summary>A card in a given state, built from the flags rather than from a clock, so the palette read
    /// off it covers the states <c>ApplyFreshness</c> cannot reach as well as the ones it can.</summary>
    private static ServerSummaryItem CardWith(ServerCollectionStatus status)
    {
        var flags = status switch
        {
            ServerCollectionStatus.Online => new ServerCollectionFlags(true, false, false),
            ServerCollectionStatus.Stale => new ServerCollectionFlags(true, true, false),
            ServerCollectionStatus.Offline => new ServerCollectionFlags(false, false, false),
            ServerCollectionStatus.AwaitingFirstCollection => new ServerCollectionFlags(null, false, true),
            _ => new ServerCollectionFlags(null, false, false),
        };

        var card = new ServerSummaryItem
        {
            ServerName = "SQL2022",
            ServerId = 1,
            IsOnline = flags.IsOnline,
            HasCollectorErrors = flags.HasCollectorErrors,
            AwaitingFirstCollection = flags.AwaitingFirstCollection,
        };

        Assert.Equal(status, card.CardStatus);
        return card;
    }

    /// <summary>The sidebar dot's DataTrigger values mapped to the brush key each one sets, read out of the
    /// Ellipse that actually owns them rather than out of the whole file (MainWindow.xaml has other dots).</summary>
    private static Dictionary<string, string> SidebarDotTriggers()
    {
        var xaml = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));

        var at = xaml.IndexOf("{Binding Server.DotStatus}", StringComparison.Ordinal);
        Assert.True(at > 0, "the sidebar status dot is gone — find where it moved before editing this test");

        var open = xaml.LastIndexOf("<Ellipse ", at, StringComparison.Ordinal);
        var close = xaml.IndexOf("</Ellipse>", open, StringComparison.Ordinal);
        Assert.True(open > 0 && close > open, "the sidebar status dot is no longer a closed Ellipse element");

        var element = xaml[open..close];
        var triggers = new Dictionary<string, string>(StringComparer.Ordinal);

        const string marker = "<DataTrigger Binding=\"{Binding Server.DotStatus}\" Value=\"";
        for (var i = element.IndexOf(marker, StringComparison.Ordinal); i >= 0; i = element.IndexOf(marker, i + 1, StringComparison.Ordinal))
        {
            var valueStart = i + marker.Length;
            var word = element[valueStart..element.IndexOf('"', valueStart)];

            const string setter = "Value=\"{DynamicResource ";
            var setterAt = element.IndexOf(setter, valueStart, StringComparison.Ordinal);
            Assert.True(setterAt > 0, $"the '{word}' trigger sets no brush");
            var keyStart = setterAt + setter.Length;
            triggers[word] = element[keyStart..element.IndexOf('}', keyStart)];
        }

        return triggers;
    }

    /// <summary>Every C# source file in the trees that can hold a copy of the ladder, with comments removed.</summary>
    private static IEnumerable<KeyValuePair<string, string>> ScannedSources()
    {
        var roots = new[]
        {
            Path.Combine(RepoRoot(), "PerformanceMonitor.Common"),
            Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer"),
            Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service"),
        };

        foreach (var root in roots)
        {
            Assert.True(Directory.Exists(root), $"{root} is gone — this scan is walking nothing");

            foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return new KeyValuePair<string, string>(file, StripComments(File.ReadAllText(file)));
            }
        }
    }

    private static bool PathsEqual(string a, string b) =>
        string.Equals(Path.GetFullPath(a), Path.GetFullPath(b), StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Drops comments while leaving every string literal intact. A line-prefix filter is not enough here and
    /// that is not hypothetical: the files this scan walks put block comments mid-file, put prose after code
    /// on the same line, and quote all five status words inside doc comments that are entirely legitimate.
    /// Handles regular, verbatim and raw string literals plus char literals, because a scan that mangles a
    /// literal is a scan that can miss the copy it exists to find.
    /// </summary>
    private static string StripComments(string source)
    {
        var kept = new StringBuilder(source.Length);
        var i = 0;

        while (i < source.Length)
        {
            var c = source[i];

            if (c == '/' && i + 1 < source.Length && source[i + 1] == '/')
            {
                while (i < source.Length && source[i] != '\n') i++;
                continue;
            }

            if (c == '/' && i + 1 < source.Length && source[i + 1] == '*')
            {
                i += 2;
                while (i + 1 < source.Length && !(source[i] == '*' && source[i + 1] == '/')) i++;
                i = Math.Min(source.Length, i + 2);
                kept.Append(' ');
                continue;
            }

            if (c == '@' && i + 1 < source.Length && source[i + 1] == '"')
            {
                kept.Append(source[i]).Append(source[i + 1]);
                i += 2;
                while (i < source.Length)
                {
                    if (source[i] == '"' && i + 1 < source.Length && source[i + 1] == '"')
                    {
                        kept.Append(source[i]).Append(source[i + 1]);
                        i += 2;
                        continue;
                    }

                    kept.Append(source[i]);
                    if (source[i++] == '"') break;
                }

                continue;
            }

            if (c == '"' && i + 2 < source.Length && source[i + 1] == '"' && source[i + 2] == '"')
            {
                var fence = 0;
                while (i + fence < source.Length && source[i + fence] == '"') fence++;
                kept.Append(source, i, fence);
                i += fence;

                while (i < source.Length)
                {
                    if (source[i] == '"')
                    {
                        var run = 0;
                        while (i + run < source.Length && source[i + run] == '"') run++;
                        kept.Append(source, i, run);
                        i += run;
                        if (run >= fence) break;
                        continue;
                    }

                    kept.Append(source[i++]);
                }

                continue;
            }

            if (c == '"' || c == '\'')
            {
                var quote = c;
                kept.Append(source[i++]);
                while (i < source.Length)
                {
                    if (source[i] == '\\' && i + 1 < source.Length)
                    {
                        kept.Append(source[i]).Append(source[i + 1]);
                        i += 2;
                        continue;
                    }

                    kept.Append(source[i]);
                    if (source[i++] == quote) break;
                }

                continue;
            }

            kept.Append(source[i++]);
        }

        return kept.ToString();
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Common")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"Could not locate the repo root walking up from {thisFile}");
    }

    private static string ReadRepoFile(string relative) =>
        File.ReadAllText(Path.Combine(RepoRoot(), relative)).Replace("\r\n", "\n", StringComparison.Ordinal);
}
