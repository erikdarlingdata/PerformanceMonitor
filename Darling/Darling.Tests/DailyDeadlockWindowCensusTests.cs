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
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3525: the shared daily classifier bands deadlocks as a RATE over <see cref="DailyHealthSignals.Window"/>
/// — so every PRODUCTION signals bundle has to declare the window its counts cover, or the band silently
/// takes the unrateable arm and a genuinely storming day maxes out at Warning. This is the
/// <c>DeadlockRateBandRungTests.EveryProductionMetricBundleDeclaresTheWindowAndTheTiers</c> census, one
/// signals type over: same walker, same brace-matching, same whole-repo scope.
/// </summary>
public sealed class DailyDeadlockWindowCensusTests
{
    /// <summary>
    /// Every production <see cref="DailyHealthSignals"/> bundle that assigns <c>Deadlocks</c> also assigns
    /// <c>Window</c> — and, since #3539 A2, <c>CollectionRuns</c> and <c>PeakBlockWaitMs</c>.
    ///
    /// <para><b>Keyed on <c>Deadlocks</c> AND <c>HasData</c> assigned in the same initializer</b> — the
    /// rung census's own lesson about member names shared across types, met the way it met it: two other
    /// production types (<c>InactionFigures</c>, the PostgreSQL <c>DatabaseRow</c>) carry a
    /// <c>Deadlocks</c> member, and neither has a <c>HasData</c>, while a signals bundle without
    /// <c>HasData</c> is unbuildable in practice (it would band <c>NoData</c> unconditionally). A type-name
    /// scan is not an option for the reason the rung census states: three of the four sites are
    /// target-typed <c>=&gt; new() { ... }</c>. Prefixed members like <c>TotalDeadlocks</c> are excluded
    /// by the leading character class. Tests are excluded because a fixture deliberately omitting the
    /// window IS the unrateable-arm pin; <c>deprecated/</c> because the old Dashboard froze its own
    /// banding.</para>
    /// </summary>
    [Fact]
    public void EveryProductionDailySignalsBundle_DeclaresTheWindow()
    {
        var offenders = new List<string>();
        var found = 0;

        foreach (var file in ProductionCSharpFiles())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(System.IO.File.ReadAllText(file));

            foreach (var initializer in SignalsBundleInitializers(code))
            {
                found++;

                /* #3539 A2 widened the census to the two members that landed beside the window: the
                   collection-run denominator (without it the error share cannot form and every erroring
                   window is Warning on presence) and the peak block (without it the day's blocking band has
                   no wait arm and a 60-second block cannot redden a cell). */
                if (!AssignsMember(initializer, "Window")
                    || !AssignsMember(initializer, "CollectionRuns")
                    || !AssignsMember(initializer, "PeakBlockWaitMs"))
                {
                    offenders.Add(System.IO.Path.GetFileName(file));
                }
            }
        }

        /* The scan has to have FOUND the bundles, or "no offenders" is vacuous. Four production sites:
           DarlingHealthReader's ToSignals (the calendar/MCP day), the viewer's DailySummaryRow.ToSignals,
           Lite's DailySummaryRow.ToSignals, and the fleet sweep's span read. Pinned as an exact count — a
           FIFTH bundle is a new surface that has to be looked at, and a floor would let it in silently,
           while a count below four means the regex stopped matching, not that the code got better. */
        Assert.Equal(4, found);

        Assert.True(
            offenders.Count == 0,
            "these production DailyHealthSignals bundles omit a denominator or the peak block (Window, "
          + "CollectionRuns or PeakBlockWaitMs), so a rate or share cannot form and the band falls to its "
          + "presence arm: "
          + string.Join(", ", offenders.Distinct().OrderBy(f => f, StringComparer.Ordinal)));
    }

    /// <summary>
    /// The three CALENDAR-DAY projections declare the day's window through the ONE clamp helper — stated
    /// against the source because the census above can only see that A window was assigned, and a calendar
    /// day whose denominator drifted would band the same count differently across the three surfaces that
    /// answer the same question. The helper (not a bare 24h constant) is the pin because the still-forming
    /// day must clamp to its elapsed portion (#3525 review: an active storm banded over unelapsed hours
    /// reads Healthy mid-crisis), and a projection that hand-rolls FromDays(1) reintroduces that dilution.
    /// </summary>
    [Fact]
    public void EveryCalendarDayProjection_BandsThroughTheDayWindowClamp()
    {
        var surfaces = new (string What, string Text)[]
        {
            ("service daily read", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingHealthReader.cs")),
            ("viewer calendar row", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.DailySummary.cs")),
            ("Lite calendar row", RepoFile.ReadRepoFile(
                "Lite", "Services", "LocalDataService.DailySummary.cs")),
        };

        foreach (var (what, text) in surfaces)
        {
            Assert.False(string.IsNullOrWhiteSpace(text), $"{what}: read nothing, so this pin would assert nothing");
            Assert.Contains(
                "Window = DailyHealthBandCalculator.CalendarDayWindow(SummaryDate, ReferenceUtc)",
                text, StringComparison.Ordinal);
            Assert.DoesNotContain("Window = TimeSpan.FromDays(1)", text, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The fleet sweep's window is its OWN span, never a calendar day — the whole #3525 point for the
    /// sweep was that a 15-minute span and a 24-hour day must not band the same count the same way.
    /// </summary>
    [Fact]
    public void TheSweepWindow_IsTheSpan_NotACalendarDay()
    {
        var sweep = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "FleetSweepEngine.cs");

        Assert.Contains("Window = spanEndUtc - spanStartUtc", sweep, StringComparison.Ordinal);
        Assert.DoesNotContain("Window = TimeSpan.FromDays(1)", sweep, StringComparison.Ordinal);
        Assert.DoesNotContain("CalendarDayWindow", sweep, StringComparison.Ordinal);
    }

    /* ─────────────────────── helpers (the rung census's own, re-keyed) ─────────────────────── */

    private static readonly Regex AssignsDeadlocks =
        new(@"(?<![.\w])Deadlocks\s*=(?!=|>)", RegexOptions.Compiled);

    private static IEnumerable<string> ProductionCSharpFiles()
    {
        foreach (var file in System.IO.Directory.EnumerateFiles(
            RepoFile.Root, "*.cs", System.IO.SearchOption.AllDirectories))
        {
            var segments = file.Split(System.IO.Path.DirectorySeparatorChar, System.IO.Path.AltDirectorySeparatorChar);
            if (segments.Contains("bin") || segments.Contains("obj")
                || segments.Contains("deprecated")
                || segments.Any(s => s.EndsWith("Tests", StringComparison.Ordinal)))
            {
                continue;
            }

            yield return file;
        }
    }

    private static IEnumerable<string> SignalsBundleInitializers(string code)
    {
        foreach (var initializer in BraceMatchedInitializers(code))
        {
            if (AssignsMember(initializer, "HasData"))
            {
                yield return initializer;
            }
        }
    }

    private static IEnumerable<string> BraceMatchedInitializers(string code)
    {
        foreach (var match in AssignsDeadlocks.Matches(code).Cast<Match>())
        {
            var depth = 0;
            var open = -1;

            for (var i = match.Index; i >= 0; i--)
            {
                if (code[i] == '}')
                {
                    depth++;
                }
                else if (code[i] == '{' && depth-- == 0)
                {
                    open = i;
                    break;
                }
            }

            if (open < 0)
            {
                continue;
            }

            depth = 0;
            for (var i = open; i < code.Length; i++)
            {
                if (code[i] == '{')
                {
                    depth++;
                }
                else if (code[i] == '}' && --depth == 0)
                {
                    yield return code[open..(i + 1)];
                    break;
                }
            }
        }
    }

    private static bool AssignsMember(string initializer, string member) =>
        Regex.IsMatch(initializer, $@"(?<![.\w]){Regex.Escape(member)}\s*=(?!=|>)");
}
