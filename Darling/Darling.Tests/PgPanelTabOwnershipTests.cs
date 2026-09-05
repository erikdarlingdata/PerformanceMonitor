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
/// </summary>
public sealed class PgPanelTabOwnershipTests
{
    /// <summary>
    /// Panels whose load path is not their own tab's, each with the issue that tracks it. Deliberately
    /// carrying the pair rather than muting the whole tab: any OTHER panel on those tabs is still checked.
    /// </summary>
    private static readonly Dictionary<string, string> KnownOffTab = new(StringComparer.Ordinal)
    {
        ["PgWaitSamplingGrid"] = "#3050",
        ["PgWaitSamplingNote"] = "#3050",
        ["PgPredicateStatsGrid"] = "#3050",
        ["PgPredicateStatsNote"] = "#3050",
    };

    private static readonly Regex ControlAssignment = new(
        @"\b(?<name>Pg[A-Za-z]+(?:Grid|Note|Expander))\s*(?:\.\w+)?\s*=", RegexOptions.Compiled);

    private static readonly Regex LoaderCall = new(
        @"\b(?<name>LoadPg[A-Za-z]+Async)\b", RegexOptions.Compiled);

    private static readonly Regex DispatcherArm = new(
        @"case\s+Pg(?<tab>\w+)InnerTabIndex:\s*\r?\n\s*await\s+LoadPg(?<loader>\w+)Async\(\);",
        RegexOptions.Compiled);

    [Fact]
    public void EveryPanelAPgTabLoadPathFills_IsDeclaredInsideThatTab()
    {
        var (tabOf, loadPaths) = Read();

        /* Floors first. Every assertion below is over these two sets, so an empty one would make the whole
           test pass by having nothing to disagree about — which is the failure this test is about. */
        Assert.True(tabOf.Count >= 40, $"Only {tabOf.Count} named controls resolved to a Pg tab; the XAML walk is not finding the tree.");
        Assert.True(loadPaths.Count >= 6, $"Only {loadPaths.Count} dispatcher arms found; the tab-to-loader map is not being read.");

        var offenders = new List<string>();
        var exemptionsSeen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (tab, panels) in loadPaths.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            Assert.True(panels.Count > 0, $"{tab}'s load path assigns no panel at all, so nothing about it is verified.");

            foreach (var panel in panels.OrderBy(p => p, StringComparer.Ordinal))
            {
                if (!tabOf.TryGetValue(panel, out var declaredIn))
                {
                    continue; /* not declared in this XAML at all — another file's control, not this rule's business */
                }

                if (declaredIn == tab)
                {
                    continue;
                }

                if (KnownOffTab.TryGetValue(panel, out var issue))
                {
                    exemptionsSeen.Add(panel);
                    continue;
                }

                offenders.Add($"{panel} is filled by {tab}'s load path but is declared inside {declaredIn}");
            }
        }

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

    [Fact]
    public void TheDispatcher_NamesEachTabAndItsLoaderConsistently()
    {
        var shell = File.ReadAllText(Path.Combine(ViewerDirectory(), "ViewerServerTab.xaml.cs"));
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

    [Fact]
    public void TheDetector_FindsAnOffTabPanelPlantedIntoTheRealTree()
    {
        /* Without this the rule above passes on any tree where the walk quietly returns nothing, and the
           exemptions would be the only evidence it ever fires. Plant a panel that IS declared on one tab
           and IS assigned by another tab's loader, and require it to be reported. */
        var (tabOf, loadPaths) = Read();

        var vacuumPanel = tabOf.First(kv => kv.Value == "PgVacuumTab" && !KnownOffTab.ContainsKey(kv.Key)).Key;
        var activityPanels = new HashSet<string>(loadPaths["PgActivityTab"], StringComparer.Ordinal) { vacuumPanel };

        var reported = activityPanels
            .Where(p => tabOf.TryGetValue(p, out var t) && t != "PgActivityTab" && !KnownOffTab.ContainsKey(p))
            .ToList();

        Assert.Contains(vacuumPanel, reported);
        Assert.Single(reported);
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

    /// <summary>
    /// (control -> outermost named Pg tab, tab -> every panel its load path assigns).
    /// </summary>
    private static (Dictionary<string, string> TabOf, Dictionary<string, HashSet<string>> LoadPaths) Read()
    {
        var dir = ViewerDirectory();
        var x = XName.Get("Name", "http://schemas.microsoft.com/winfx/2006/xaml");

        var doc = XDocument.Load(Path.Combine(dir, "ViewerServerTab.xaml"));
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

        var source = Strip(File.ReadAllText(Path.Combine(dir, "ViewerServerTab.Postgres.cs")));
        var bodies = MethodBodies(source);

        var loadPaths = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (Match arm in DispatcherArm.Matches(File.ReadAllText(Path.Combine(dir, "ViewerServerTab.xaml.cs"))))
        {
            var tab = $"Pg{arm.Groups["tab"].Value}Tab";
            var entry = $"LoadPg{arm.Groups["loader"].Value}Async";
            loadPaths[tab] = Transitive(entry, bodies, new HashSet<string>(StringComparer.Ordinal));
        }

        return (tabOf, loadPaths);
    }

    private static HashSet<string> Transitive(string method, Dictionary<string, string> bodies, HashSet<string> seen)
    {
        var found = new HashSet<string>(StringComparer.Ordinal);
        if (!seen.Add(method) || !bodies.TryGetValue(method, out var body))
        {
            return found;
        }

        foreach (Match m in ControlAssignment.Matches(body))
        {
            found.Add(m.Groups["name"].Value);
        }

        foreach (Match m in LoaderCall.Matches(body))
        {
            var callee = m.Groups["name"].Value;
            if (callee != method)
            {
                found.UnionWith(Transitive(callee, bodies, seen));
            }
        }

        return found;
    }

    /// <summary>Doc comments and line comments out, so prose naming a grid cannot read as an assignment.</summary>
    private static string Strip(string source) =>
        Regex.Replace(Regex.Replace(source, @"^[ \t]*///.*$", string.Empty, RegexOptions.Multiline),
                      @"//.*$", string.Empty, RegexOptions.Multiline);

    private static Dictionary<string, string> MethodBodies(string source)
    {
        var bodies = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(source, @"private\s+async\s+Task\s+(?<name>LoadPg[A-Za-z]+Async)\s*\("))
        {
            var depth = 0;
            var started = false;
            for (var i = m.Index; i < source.Length; i++)
            {
                if (source[i] == '{')
                {
                    depth++;
                    started = true;
                }
                else if (source[i] == '}')
                {
                    depth--;
                    if (started && depth == 0)
                    {
                        bodies[m.Groups["name"].Value] = source[m.Index..(i + 1)];
                        break;
                    }
                }
            }
        }

        return bodies;
    }

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
