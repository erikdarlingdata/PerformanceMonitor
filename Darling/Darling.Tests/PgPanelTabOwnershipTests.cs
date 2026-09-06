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
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A PostgreSQL panel must be filled by the tab it renders on (#3048).
///
/// <para>The viewer loads a server tab's panels on demand: the inner-tab dispatcher in
/// <c>ViewerServerTab.xaml.cs</c> maps each <c>Pg…InnerTabIndex</c> to one <c>LoadPg…Async</c>, and that
/// loader — plus whatever it calls — assigns the panels. Nothing checks that the panels it assigns are the
/// panels declared inside that tab, and #3048 was a grid that rendered on Vacuum while only the Activity
/// tab's load path filled it. Visiting Vacuum without Activity left it empty, which is indistinguishable
/// from a server that never deadlocked.</para>
///
/// <para><b>It is invisible without this test.</b> The XAML compiles, BAML is produced, the build is green
/// and the panel appears; it is only ever empty at a moment nobody scripted. The comment above it named the
/// right tab for two years and the layout did not match, so prose is demonstrably not the check.</para>
///
/// <para>Ownership resolves to the OUTERMOST named <c>TabItem</c>, because Activity holds a sub-tab control
/// and a panel inside its Blocking sub-tab is still loaded by <c>LoadPgActivityAsync</c>.</para>
///
/// <para><b>How the C# is read, and why it is not a regex of this file's own.</b> Both <c>.cs</c> files go
/// through <see cref="CSharpSourceWalker"/> (#3052). The comment-stripping regex this file used to carry
/// removed <c>///</c> and <c>//</c> lines and left <c>/* … */</c> blocks entirely — and this repo puts no
/// asterisk on a block comment's continuation lines, so every continuation line was handed to the scanner as
/// code. A comment explaining why a grid is filled somewhere else read as an assignment filling it HERE.
/// The walk also makes the brace count that finds a loader body immune to a brace inside a string or char
/// literal, which decided whether a body was read whole.</para>
///
/// <para>Because the walk blanks literal TEXT as well as comments, a scan that needed a literal's contents
/// would have to read them out of the original source at the offset matched in the walked text (the
/// <c>PgRegistryPanelPlacementTests.LiteralAt</c> idiom, sound because the two are character-aligned).
/// Nothing here does: every token these regexes match — a control name, a member access, a method name, a
/// <c>case</c> label — is code, and code survives the walk untouched.
/// <see cref="EveryPgGridOnAPgTab_IsFilledBySomePgTabsLoadPath"/> is what goes red if that stops holding,
/// naming the grid whose assignment the read could no longer see.</para>
/// </summary>
public sealed class PgPanelTabOwnershipTests
{
    /// <summary>
    /// Panels whose load path is not their own tab's, each with the issue that tracks it. EMPTY, and the
    /// rule below is enforced with no exemption anywhere in the PostgreSQL tree.
    ///
    /// <para>An entry belongs here only alongside an open issue that says which way the pair will be
    /// resolved, and it is carried per PANEL rather than per tab so every other panel on that tab stays
    /// checked. The stale-exemption assertion at the end of the rule is what stops one outliving its
    /// fix.</para>
    /// </summary>
    private static readonly Dictionary<string, string> KnownOffTab = new(StringComparer.Ordinal);

    private static readonly Regex ControlAssignment = new(
        @"\b(?<name>Pg[A-Za-z]+(?:Grid|Note|Expander))\s*(?:\.\w+)?\s*=", RegexOptions.Compiled);

    private static readonly Regex LoaderCall = new(
        @"\b(?<name>LoadPg[A-Za-z]+Async)\b", RegexOptions.Compiled);

    private static readonly Regex DispatcherArm = new(
        @"case\s+Pg(?<tab>\w+)InnerTabIndex:\s*\r?\n\s*await\s+LoadPg(?<loader>\w+)Async\(\);",
        RegexOptions.Compiled);

    /// <summary>
    /// A loader declaration. A field rather than an inline literal so <see cref="Read"/> can count the
    /// declarations WITHOUT going through the brace scan — a scan that lost a method would otherwise be
    /// counted by the same code that lost it, and a dropped method is silent everywhere downstream: every
    /// panel it assigns simply stops being attributed to any tab.
    /// </summary>
    private static readonly Regex MethodDeclaration = new(
        @"private\s+async\s+Task\s+(?<name>LoadPg[A-Za-z]+Async)\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// A grid's <c>x:Name</c> in the raw XAML text, for the population cross-check in
    /// <see cref="EveryPgGridOnAPgTab_IsFilledBySomePgTabsLoadPath"/>. Deliberately a second reading of the
    /// same file that shares no code with the <see cref="XDocument"/> walk: the rule's success condition is
    /// an EMPTY offender list, and a walk that resolved no grids — or some of them — would satisfy it
    /// without checking anything.
    /// </summary>
    private static readonly Regex XamlGridName = new(
        @"x:Name=""(?<name>Pg[A-Za-z]+Grid)""", RegexOptions.Compiled);

    /// <summary>
    /// An XML comment, removed before <see cref="XamlGridName"/> reads the markup.
    ///
    /// <para>This file's whole subject one artifact over, and it was worth measuring rather than assuming:
    /// <c>XDocument.Descendants()</c> yields <c>XElement</c> only, so a commented-out declaration is
    /// invisible to the walk, while a regex over raw text reads it as live. The shipped markup carries 147
    /// XML comment nodes and none of them holds a grid name today; planted, one made the cross-check red
    /// with a bare collection diff on markup that was perfectly correct. A commented-out declaration is not
    /// a declaration on either side of the comparison.</para>
    /// </summary>
    private static readonly Regex XamlComment = new(@"<!--.*?-->", RegexOptions.Compiled | RegexOptions.Singleline);

    [Fact]
    public void EveryPanelAPgTabLoadPathFills_IsDeclaredInsideThatTab()
    {
        var (tabOf, loadPaths) = Read();

        /* Floors first. Every assertion below is over these two sets, so an empty one would make the whole
           test pass by having nothing to disagree about — which is the failure this test is about. */
        Assert.True(tabOf.Count >= 40, $"Only {tabOf.Count} named controls resolved to a Pg tab; the XAML walk is not finding the tree.");
        Assert.True(loadPaths.Count >= 6, $"Only {loadPaths.Count} dispatcher arms found; the tab-to-loader map is not being read.");

        var (offenders, exemptionsSeen) = OffTabPanels(tabOf, loadPaths);

        Assert.True(offenders.Count == 0,
            "A PostgreSQL panel is filled by one tab's load path and rendered on another, so it is empty "
            + "until the other tab is visited — and an empty panel reads as 'nothing to report' rather than "
            + "'nothing loaded this'. Either move the declaration into the tab that loads it, or move the "
            + "loader call into the tab that renders it, and say which evidence decided:\n  "
            + string.Join("\n  ", offenders));

        /* An exemption that has stopped being true is worse than no exemption: it documents debt that is
           already paid and licenses the defect to come back at that name unnoticed. */
        var stale = KnownOffTab.Keys.Where(k => !exemptionsSeen.Contains(k)).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.True(stale.Count == 0,
            "These panels are exempt but no longer off-tab, so the exemption is stale. Remove them from "
            + $"KnownOffTab and close the issue they cite:\n  {string.Join("\n  ", stale)}");
    }

    /// <summary>
    /// Every grid declared inside a PostgreSQL tab is filled by SOME PostgreSQL tab's load path.
    ///
    /// <para><b>This is what makes a partial loss loud.</b> The rule above is per-panel but it only ever
    /// looks at panels the read FOUND, so anything the read drops makes it quieter rather than red: it
    /// compares fewer pairs and reports a clean run. Its one floor — a tab's load path assigns at least one
    /// panel — trips only when a whole tab is zeroed. Measured on the shipped tree: a stray <c>}</c> in a
    /// literal at the top of <c>LoadPgIoAsync</c> takes the I/O tab from six panels to none and reds that
    /// floor, and the same <c>}</c> one line later takes it from six to ONE and is completely green, with
    /// four of the five lost panels belonging to the two loaders the truncation also stopped following.
    /// Asked from this direction the loss has nowhere to hide: a grid nothing fills is named, whether the
    /// assignment was dropped by a bad read or deleted from the source.</para>
    ///
    /// <para>Grids only, and the four exceptions are why. <c>PgOverviewNote</c>, <c>PgActivityNote</c>,
    /// <c>PgVacuumNote</c> and <c>PgStorageNote</c> are tab-level framing prose assigned once in
    /// <c>ApplyEngineTabSet</c> — tab setup, not a load path — so requiring a load path for every named
    /// control would need an exemption table, and an exemption table is what this rule exists without. A
    /// grid is the case with no legitimate exception: it holds collected rows, and rows arrive on a load
    /// path.</para>
    /// </summary>
    [Fact]
    public void EveryPgGridOnAPgTab_IsFilledBySomePgTabsLoadPath()
    {
        var (tabOf, loadPaths) = Read();

        var grids = tabOf.Keys.Where(n => n.EndsWith("Grid", StringComparison.Ordinal))
                              .ToHashSet(StringComparer.Ordinal);

        /* The population, read a second time out of the raw markup by a regex that shares no code with the
           XDocument walk above. The rule's success condition is an empty offender list, so a walk that
           resolved no grids would pass it vacuously; and a walk that resolved SOME of them would pass it
           just as quietly. Equality rather than a floor, because both readings answer the same question
           over the same file and a disagreement is a finding either way. */
        var markup = XamlComment.Replace(ViewerFile("ViewerServerTab.xaml"), string.Empty);
        var declaredInMarkup = XamlGridName.Matches(markup)
            .Select(m => m.Groups["name"].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(declaredInMarkup.Count >= 25,
            $"Only {declaredInMarkup.Count} Pg…Grid x:Name attributes were found in ViewerServerTab.xaml. "
            + "The PostgreSQL surface ships more grids than that, so the markup is not being read and every "
            + "assertion below would be over the remainder.");

        var walkMissed = declaredInMarkup.Except(grids, StringComparer.Ordinal)
                                         .OrderBy(g => g, StringComparer.Ordinal).ToList();
        var walkInvented = grids.Except(declaredInMarkup, StringComparer.Ordinal)
                                .OrderBy(g => g, StringComparer.Ordinal).ToList();

        Assert.True(walkMissed.Count == 0,
            "The markup declares these grids but the XDocument walk did not resolve any of them to a "
            + "PostgreSQL TabItem, so the rule below silently stops covering them. Either the walk is "
            + "broken, or the grid really is declared outside the PostgreSQL tabs — which is worth "
            + "deciding rather than absorbing, because a control named Pg…Grid somewhere else is confusing "
            + "on its own terms:\n  " + string.Join("\n  ", walkMissed));

        Assert.True(walkInvented.Count == 0,
            "The XDocument walk resolved these grid names but the raw markup does not declare them in the "
            + "form this cross-check reads (x:Name=\"…\"). The two readings answer the same question over "
            + "the same file, so a disagreement means one of them is no longer reading it:\n  "
            + string.Join("\n  ", walkInvented));

        var filled = loadPaths.Values.SelectMany(p => p).ToHashSet(StringComparer.Ordinal);
        var unfilled = grids.Where(g => !filled.Contains(g)).OrderBy(g => g, StringComparer.Ordinal).ToList();

        Assert.True(unfilled.Count == 0,
            "These grids are declared inside a PostgreSQL tab but no PostgreSQL tab's load path assigns "
            + "them, so they render empty forever and an empty grid reads as 'nothing to report'. Either "
            + "the assignment is gone, or it is in a method no dispatcher arm reaches, or the source read "
            + "above lost it — check the loader that used to fill it before changing this rule:\n  "
            + string.Join("\n  ", unfilled));
    }

    [Fact]
    public void TheDispatcher_NamesEachTabAndItsLoaderConsistently()
    {
        var shell = CSharpSourceWalker.StripCommentsAndStrings(ViewerFile("ViewerServerTab.xaml.cs"));
        var arms = DispatcherArm.Matches(shell);

        Assert.True(arms.Count >= 6, $"Found {arms.Count} PostgreSQL dispatcher arms; the pattern no longer matches the switch.");

        foreach (Match arm in arms)
        {
            var tab = arm.Groups["tab"].Value;
            var loader = arm.Groups["loader"].Value;
            Assert.True(tab == loader,
                $"The dispatcher routes Pg{tab}InnerTabIndex to LoadPg{loader}Async. The rest of this file "
                + "derives a tab's loader from its name, so a mismatch here would silently check the wrong "
                + "tab's panels against the wrong load path.");
        }
    }

    /// <summary>
    /// Plant an off-tab panel into the real tree and require the rule's OWN evaluation to report it.
    ///
    /// <para><b>It used to re-implement the predicate inline</b>, which made it a control that could not
    /// fail: measured on this tree, a single <c>continue;</c> at the top of the rule's inner loop — the rule
    /// reporting nothing at all, ever — left every test in this file green, this one included. A control
    /// that re-implements the thing it controls cannot distinguish "the rule fires" from "my copy of the
    /// rule fires", and it converts an open question into false confidence. It now calls
    /// <see cref="OffTabPanels"/>, which is the rule.</para>
    ///
    /// <para>The DIFFERENCE between the unplanted and planted evaluations is what is asserted, not that the
    /// planted one reports exactly one pair. Whether the shipped tree is clean is the rule's to say; making
    /// one misplaced panel red this test as well would cost a reader the attribution.</para>
    /// </summary>
    [Fact]
    public void TheDetector_FindsAnOffTabPanelPlantedIntoTheRealTree()
    {
        var (tabOf, loadPaths) = Read();

        var vacuumPanel = tabOf.Where(kv => kv.Value == "PgVacuumTab" && !KnownOffTab.ContainsKey(kv.Key))
                               .Select(kv => kv.Key)
                               .OrderBy(p => p, StringComparer.Ordinal)
                               .First();

        var planted = loadPaths.ToDictionary(
            p => p.Key,
            p => new HashSet<string>(p.Value, StringComparer.Ordinal),
            StringComparer.Ordinal);

        /* If the panel were already on Activity's load path, planting it would change nothing and this
           control would pass against the unplanted tree — the fixture would have no arranged fact in it. */
        Assert.True(planted["PgActivityTab"].Add(vacuumPanel),
            $"{vacuumPanel} is already assigned by PgActivityTab's load path, so planting it arranges "
            + "nothing. Pick a PgVacuumTab panel that Activity does not already fill.");

        var (before, _) = OffTabPanels(tabOf, loadPaths);
        var (after, _) = OffTabPanels(tabOf, planted);

        var added = after.Except(before, StringComparer.Ordinal).ToList();

        Assert.Single(added);
        Assert.StartsWith($"{vacuumPanel} is filled by PgActivityTab's load path", added[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// The control for the step every rule here stands on: <see cref="MethodRanges"/> counts braces, so a
    /// brace inside a string or char literal decides whether a loader's body is read whole. Arranged rather
    /// than measured on the shipped tree, because the shipped tree happens to balance its literal braces
    /// today — it holds no brace in any literal at all — and "safe today because they happen to balance" is
    /// the standing assumption this file exists to refuse.
    ///
    /// <para>Both directions are asserted, so the walk is shown to be what saves it rather than asserted to
    /// be. A <c>}</c> in a literal ends the raw count early and TRUNCATES the body, silently dropping every
    /// control assignment after it and every loader it would have followed. A <c>{</c> in a literal never
    /// lets the count return to zero: mid-file the scan OVER-EXTENDS into the methods below and attributes
    /// their panels to this tab, and at end of file it finds no closing brace at all and the method is
    /// DROPPED. None of the three shows up as an error downstream — a truncated body just yields fewer
    /// panels, and fewer panels is a quieter rule, not a failing one.</para>
    /// </summary>
    [Fact]
    public void TheMethodWalk_ReadsABodyWhoseLiteralsHoldUnbalancedBraces_AndTheRawCountDoesNot()
    {
        /* A closing brace inside a literal: the raw count reaches zero at it and stops there. */
        const string truncates = """
            private async Task LoadPgProbeAsync()
            {
                PgProbeNote.Text = "no autovacuum has run } yet";
                PgProbeGrid.ItemsSource = rows;
            }
            """;

        var walkedCode = CSharpSourceWalker.StripCommentsAndStrings(truncates);
        var walked = MethodRanges(walkedCode);
        var raw = MethodRanges(truncates);

        var walkedBody = truncates[walked["LoadPgProbeAsync"].Start..walked["LoadPgProbeAsync"].End];
        Assert.Equal(2, ControlAssignment.Matches(walkedBody).Count);
        Assert.Contains("PgProbeGrid", walkedBody, StringComparison.Ordinal);

        var rawBody = truncates[raw["LoadPgProbeAsync"].Start..raw["LoadPgProbeAsync"].End];
        Assert.Single(ControlAssignment.Matches(rawBody));
        Assert.DoesNotContain("PgProbeGrid", rawBody, StringComparison.Ordinal);

        /* And the diagnosis in Read() is what turns that truncation into a red rather than a quieter rule. */
        Assert.Null(BodyDiagnosis(walkedCode, walked["LoadPgProbeAsync"].Start, walked["LoadPgProbeAsync"].End));
        Assert.Equal("truncated", BodyDiagnosis(walkedCode, raw["LoadPgProbeAsync"].Start, raw["LoadPgProbeAsync"].End));

        /* An OPENING brace in a char literal is the other direction. Mid-file the raw count runs on into the
           method below and takes its panels with it, which is the over-extension the diagnosis names. */
        const string overExtends = """
            partial class ViewerServerTab
            {
                private async Task LoadPgProbeAsync()
                {
                    var opener = '{';
                    PgProbeGrid.ItemsSource = rows;
                }

                private async Task LoadPgOtherAsync()
                {
                    PgOtherGrid.ItemsSource = rows;
                }
            }
            """;

        var overCode = CSharpSourceWalker.StripCommentsAndStrings(overExtends);
        var overWalked = MethodRanges(overCode);
        var overRaw = MethodRanges(overExtends);

        Assert.DoesNotContain("PgOtherGrid",
            overExtends[overWalked["LoadPgProbeAsync"].Start..overWalked["LoadPgProbeAsync"].End],
            StringComparison.Ordinal);
        Assert.Contains("PgOtherGrid",
            overExtends[overRaw["LoadPgProbeAsync"].Start..overRaw["LoadPgProbeAsync"].End],
            StringComparison.Ordinal);

        Assert.Null(BodyDiagnosis(overCode, overWalked["LoadPgProbeAsync"].Start, overWalked["LoadPgProbeAsync"].End));
        Assert.Equal("over-extended", BodyDiagnosis(overCode, overRaw["LoadPgProbeAsync"].Start, overRaw["LoadPgProbeAsync"].End));

        /* At end of file the same brace finds no closing brace at all and the method is dropped outright,
           which no per-body diagnosis can name because there is no body to diagnose. That is what the
           declaration count in Read() is for, and MethodDeclaration is the field it counts with — it sees
           the declaration whether or not the scan resolved a body for it. */
        const string drops = """
            private async Task LoadPgProbeAsync()
            {
                var opener = '{';
                PgProbeGrid.ItemsSource = rows;
            }
            """;

        Assert.True(MethodRanges(CSharpSourceWalker.StripCommentsAndStrings(drops)).ContainsKey("LoadPgProbeAsync"));
        Assert.Empty(MethodRanges(drops));
        Assert.Single(MethodDeclaration.Matches(drops));
    }

    /// <summary>
    /// The other half of the same adoption, on an arranged body for the same reason: the shipped
    /// <c>ViewerServerTab.Postgres.cs</c> carries fourteen multi-line block comments spanning
    /// thirty-five continuation lines and none of them happens to name a control today, so nothing on the
    /// tree can show what the regex did with one that does.
    ///
    /// <para>This repo writes no asterisk on a continuation line, so the old regex — <c>///</c> lines and
    /// <c>//</c> to end of line — left every continuation line in the code stream. A comment saying a grid
    /// is filled on ANOTHER tab therefore read as an assignment filling it HERE, and the rule reported the
    /// panel it had just been told about. Both directions asserted, so the walk is shown to be what fixes
    /// it.</para>
    /// </summary>
    [Fact]
    public void TheSourceRead_IgnoresAControlNamedInABlockCommentsContinuationLine()
    {
        const string source = """
            private async Task LoadPgProbeAsync()
            {
                /* Wraparound is not filled from here even though the same reader carries it. Vacuum owns
                   it, and PgWraparoundGrid.ItemsSource = that tab's rows on that tab's load path. */
                PgProbeGrid.ItemsSource = rows;
            }
            """;

        var walked = CSharpSourceWalker.StripCommentsAndStrings(source);
        var walkedNames = ControlAssignment.Matches(walked).Select(m => m.Groups["name"].Value).ToList();
        Assert.Equal(new[] { "PgProbeGrid" }, walkedNames);

        /* The regex this file used to carry, inline so the comparison is against the shape that shipped
           rather than a description of it. The first line of the comment is not the problem — the
           continuation line is, and only a block-comment-aware read can tell them apart. */
        var lineCommentsOnly = Regex.Replace(
            Regex.Replace(source, @"^[ \t]*///.*$", string.Empty, RegexOptions.Multiline),
            @"//.*$", string.Empty, RegexOptions.Multiline);

        var regexNames = ControlAssignment.Matches(lineCommentsOnly).Select(m => m.Groups["name"].Value).ToList();
        Assert.Equal(new[] { "PgWraparoundGrid", "PgProbeGrid" }, regexNames);
    }

    /// <summary>
    /// The rule's evaluation, in one place: every (panel, tab) pair
    /// <see cref="EveryPanelAPgTabLoadPathFills_IsDeclaredInsideThatTab"/> would report, and the exemptions
    /// it consumed on the way.
    ///
    /// <para>A helper rather than the rule's own inline loop so that
    /// <see cref="TheDetector_FindsAnOffTabPanelPlantedIntoTheRealTree"/> runs THIS and not a second copy of
    /// it. The per-tab floor lives here too, because it is part of the same evaluation: a tab whose load
    /// path assigns nothing has no pairs to disagree about, and an empty pair set is indistinguishable from
    /// a clean one.</para>
    /// </summary>
    private static (List<string> Offenders, HashSet<string> ExemptionsSeen) OffTabPanels(
        IReadOnlyDictionary<string, string> tabOf,
        IReadOnlyDictionary<string, HashSet<string>> loadPaths)
    {
        var offenders = new List<string>();
        var exemptionsSeen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (tab, panels) in loadPaths.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            Assert.True(panels.Count > 0, $"{tab}'s load path assigns no panel at all, so nothing about it is verified.");

            foreach (var panel in panels.OrderBy(p => p, StringComparer.Ordinal))
            {
                if (!tabOf.TryGetValue(panel, out var declaredIn))
                {
                    continue; /* not declared in this XAML at all - another file's control, not this rule's business */
                }

                if (declaredIn == tab)
                {
                    continue;
                }

                if (KnownOffTab.ContainsKey(panel))
                {
                    exemptionsSeen.Add(panel);
                    continue;
                }

                offenders.Add($"{panel} is filled by {tab}'s load path but is declared inside {declaredIn}");
            }
        }

        return (offenders, exemptionsSeen);
    }

    private static string ViewerDirectory()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for "
            + "PerformanceMonitor.sln). This test reads the source tree, so it cannot run without it — fix "
            + "the walk-up rather than skipping, or the rule stops being enforced without anyone noticing.");
        return Path.Combine(root!, "Darling", "PerformanceMonitor.Darling.Viewer");
    }

    private static string ViewerFile(string name) => File.ReadAllText(Path.Combine(ViewerDirectory(), name));

    /// <summary>
    /// (control -> outermost named Pg tab, tab -> every panel its load path assigns).
    /// </summary>
    private static (Dictionary<string, string> TabOf, Dictionary<string, HashSet<string>> LoadPaths) Read()
    {
        var x = XName.Get("Name", "http://schemas.microsoft.com/winfx/2006/xaml");

        var doc = XDocument.Parse(ViewerFile("ViewerServerTab.xaml"));
        var tabOf = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var element in doc.Descendants())
        {
            var name = element.Attribute(x)?.Value;
            if (name is null)
            {
                continue;
            }

            /* Outermost, not nearest: a panel in Activity's Blocking sub-tab is loaded by the Activity arm. */
            var owner = element.Ancestors()
                .Where(a => a.Name.LocalName == "TabItem" && a.Attribute(x)?.Value is not null)
                .Select(a => a.Attribute(x)!.Value)
                .LastOrDefault(t => t.StartsWith("Pg", StringComparison.Ordinal));

            if (owner is not null)
            {
                tabOf[name] = owner;
            }
        }

        /* Two names, deliberately. The brace scan is handed the WALKED text, and every range it returns is
           re-checked against that same walked text below — but the re-check has to be able to disagree with
           the scan, so what the scan reads and what the diagnosis reads are separate reads of separate
           variables rather than one variable used twice. Point MethodRanges at loaderSource and the
           diagnosis still says truncated; that is the difference between a re-check and a restatement. */
        var loaderSource = ViewerFile("ViewerServerTab.Postgres.cs");
        var loaderCode = CSharpSourceWalker.StripCommentsAndStrings(loaderSource);
        var ranges = MethodRanges(loaderCode);

        /* The scan's own integrity, asserted before anything is derived from it, and against the walked
           source INDEPENDENTLY of what the scan was handed. Every failure below is silent downstream: a
           truncated or dropped body assigns fewer panels, and fewer panels makes the rules above quieter
           rather than red. Naming the method here is the difference between "this loader's body was not
           read whole" and a panel count nobody looks at. */
        var declarations = MethodDeclaration.Matches(loaderCode)
            .Select(m => m.Groups["name"].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(declarations.Count >= 15,
            $"Only {declarations.Count} LoadPg…Async declarations were found in ViewerServerTab.Postgres.cs. "
            + "There is one per dispatcher arm plus the helpers they call, so the declaration regex is no "
            + "longer matching the file — and every assertion below is over what it found, including the "
            + "ones whose success condition is an empty list.");

        var dropped = declarations.Where(d => !ranges.ContainsKey(d))
                                  .OrderBy(d => d, StringComparer.Ordinal)
                                  .ToList();

        Assert.True(dropped.Count == 0,
            "These loaders are declared in ViewerServerTab.Postgres.cs but the brace scan resolved no body "
            + "for them, so every panel they assign is attributed to no tab at all and both rules above go "
            + "quieter rather than red. The scan reads walked source, where a brace inside a literal or a "
            + "comment cannot count — so this is the scan failing, not the source:\n  "
            + string.Join("\n  ", dropped));

        var truncated = new List<string>();
        var overExtended = new List<string>();
        foreach (var (method, (start, end)) in ranges.OrderBy(r => r.Key, StringComparer.Ordinal))
        {
            /* Truncation and over-extension are named apart because they are different defects with
               different fixes and they lie in opposite directions: one drops this method's panels, the
               other steals the next method's. A single "malformed" list would leave the reader to work out
               which. */
            switch (BodyDiagnosis(loaderCode, start, end))
            {
                case "truncated":
                    truncated.Add($"{method} (line {LineOf(loaderCode, start)}, scan ended at line {LineOf(loaderCode, end - 1)})");
                    break;
                case "over-extended":
                    overExtended.Add($"{method} (line {LineOf(loaderCode, start)}, scan ran to line {LineOf(loaderCode, end - 1)})");
                    break;
            }
        }

        Assert.True(truncated.Count == 0,
            "The brace scan ended before these loaders' own closing brace, so their bodies are TRUNCATED: "
            + "every control assignment after the cut is dropped, and so is every loader they would have "
            + "called — which takes that loader's panels with it. The rules above then compare fewer pairs "
            + "and report a clean run:\n  " + string.Join("\n  ", truncated));

        Assert.True(overExtended.Count == 0,
            "The brace scan returned to depth zero before the end of these ranges, so it ran PAST the "
            + "loader it was reading and the methods below it are now part of its body. Their panels are "
            + "attributed to this tab, which invents off-tab pairs rather than hiding them:\n  "
            + string.Join("\n  ", overExtended));

        var loadPaths = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var shell = CSharpSourceWalker.StripCommentsAndStrings(ViewerFile("ViewerServerTab.xaml.cs"));
        foreach (Match arm in DispatcherArm.Matches(shell))
        {
            var tab = $"Pg{arm.Groups["tab"].Value}Tab";
            var entry = $"LoadPg{arm.Groups["loader"].Value}Async";

            Assert.Contains(entry, declarations);

            loadPaths[tab] = Transitive(entry, loaderCode, ranges, new HashSet<string>(StringComparer.Ordinal));
        }

        return (tabOf, loadPaths);
    }

    private static HashSet<string> Transitive(
        string method,
        string code,
        Dictionary<string, (int Start, int End)> ranges,
        HashSet<string> seen)
    {
        var found = new HashSet<string>(StringComparer.Ordinal);
        if (!seen.Add(method) || !ranges.TryGetValue(method, out var range))
        {
            return found;
        }

        var body = code[range.Start..range.End];

        foreach (Match m in ControlAssignment.Matches(body))
        {
            found.Add(m.Groups["name"].Value);
        }

        foreach (Match m in LoaderCall.Matches(body))
        {
            var callee = m.Groups["name"].Value;
            if (callee != method)
            {
                found.UnionWith(Transitive(callee, code, ranges, seen));
            }
        }

        return found;
    }

    /// <summary>
    /// Each <c>LoadPg…Async</c>'s body as a half-open range over the code it is given.
    ///
    /// <para>The brace count is only sound over WALKED text, and that is measured rather than assumed:
    /// <see cref="TheMethodWalk_ReadsABodyWhoseLiteralsHoldUnbalancedBraces_AndTheRawCountDoesNot"/> runs it
    /// both ways over the same arranged body. Ranges rather than substrings so
    /// <see cref="BodyDiagnosis"/> can re-check where the scan stopped, which a substring has already
    /// thrown away.</para>
    /// </summary>
    private static Dictionary<string, (int Start, int End)> MethodRanges(string code)
    {
        var ranges = new Dictionary<string, (int Start, int End)>(StringComparer.Ordinal);
        foreach (Match m in MethodDeclaration.Matches(code))
        {
            var depth = 0;
            var started = false;
            for (var i = m.Index; i < code.Length; i++)
            {
                if (code[i] == '{')
                {
                    depth++;
                    started = true;
                }
                else if (code[i] == '}')
                {
                    depth--;
                    if (started && depth == 0)
                    {
                        ranges[m.Groups["name"].Value] = (m.Index, i + 1);
                        break;
                    }
                }
            }
        }

        return ranges;
    }

    /// <summary>
    /// <c>null</c> when <c>code[start..end]</c> is one whole method body — it opens a brace, returns to
    /// depth zero exactly once, and does so on the last character. Otherwise the name of the failure:
    /// <c>"truncated"</c> when the range ends anywhere but its own closing brace, <c>"over-extended"</c>
    /// when depth returns to zero before the end and the scan therefore kept reading.
    ///
    /// <para>Checked over the walked text, so a brace inside a literal or a comment is not a brace here —
    /// which is the whole point: the diagnosis has to disagree with a scan that counted one.</para>
    /// </summary>
    private static string? BodyDiagnosis(string code, int start, int end)
    {
        if (end <= start || end > code.Length || code[end - 1] != '}')
        {
            return "truncated";
        }

        var depth = 0;
        var opened = false;

        for (var i = start; i < end; i++)
        {
            if (code[i] == '{')
            {
                depth++;
                opened = true;
            }
            else if (code[i] == '}')
            {
                depth--;
                if (depth < 0)
                {
                    return "over-extended";
                }

                if (depth == 0 && i != end - 1)
                {
                    return "over-extended";
                }
            }
        }

        return opened && depth == 0 ? null : "truncated";
    }

    /// <summary>
    /// The 1-based line <paramref name="offset"/> falls on. Sound over walked text because
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> preserves every newline, so the walked text
    /// is line-aligned with the file a reader will open.
    /// </summary>
    private static int LineOf(string code, int offset) =>
        code.AsSpan(0, Math.Clamp(offset, 0, code.Length)).Count('\n') + 1;

    /// <summary>Same walk-up idiom as <c>DocCommentHygieneTests.FindRepoRoot</c>.</summary>
    private static string? FindRepoRoot()
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 10 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
            {
                return dir;
            }

            dir = Path.GetDirectoryName(dir);
        }

        return null;
    }
}
