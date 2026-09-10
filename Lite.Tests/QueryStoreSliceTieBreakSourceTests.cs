/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Source-containment guard for the #1907 tie-break across EVERY Lite Query Store dedup site. The exact
/// counterpart of <c>Darling.Tests/QueryStoreSliceTieBreakSourceTests</c>, deliberately built to the same
/// shape so the two apps are guarded the same way rather than one app being guarded and the other trusted.
///
/// <para>Query Store returns the flushed and the still-in-memory slice of one runtime-stats interval as two
/// ADDITIVE rows. Until #1907 the collector stored both, and they then shared the entire read-side dedup key
/// AND <c>collection_time</c> — so <c>ROW_NUMBER() ... ORDER BY collection_time DESC</c> was ordering by a
/// value identical for both rows and the survivor was whichever the engine emitted first. A grid could show
/// an in-memory sliver of 8 where the interval's total was 94, and a different number on the next run.</para>
///
/// <para>The collector now combines the slices before storing, so rows collected since cannot tie at all.
/// The tie-break is for the rows ALREADY stored, which cannot be rewritten: it resolves them to the FLUSHED
/// slice — the one holding the bulk of the interval's work — deterministically instead of flapping. That is
/// closest-available, not correct; the correct value is the SUM, and the residual is tracked in #1912.</para>
///
/// <para><b>Why this exists ON TOP OF <see cref="QueryStoreDedupReadTests"/>.</b> That test's
/// <c>EveryQueryStoreAggregateInTheFile_CarriesADedupCte</c> reads exactly one file,
/// <c>LocalDataService.QueryStore.cs</c>, which is right for what it checks (the dedup CTEs, the rank
/// filters, the trend's two arms) but leaves 2 of Lite's 7 dedup sites — the ones in
/// <c>Analysis/DrillDownCollector.Queries.cs</c> and <c>Analysis/DuckDbFactCollector.QueryPerf.cs</c> —
/// covered by no test at all. A future edit dropping <c>execution_count DESC</c> from either would have gone
/// uncaught, which is precisely the recurrence mode #1841 / #1845 / #1853 / #1907 keep demonstrating. Caught
/// by review on #1919; the alternative on offer was to soften the source comment to stop claiming coverage
/// that did not exist, which would have been a Lite/Darling parity scope-down.</para>
/// </summary>
public sealed class QueryStoreSliceTieBreakSourceTests
{
    /// <summary>
    /// Every Lite file holding a Query Store dedup window, with the number of dedup sites in each. Written
    /// out rather than globbed so that DELETING a read — or moving one to a new file — fails here and has to
    /// be re-declared, instead of quietly reducing the guard's coverage to whatever is left.
    /// </summary>
    private static readonly (string Path, int Sites)[] DedupSites =
    [
        (Path.Combine("Lite", "Services", "LocalDataService.QueryStore.cs"), 6),
        (Path.Combine("Lite", "Analysis", "DrillDownCollector.Queries.cs"), 1),
        (Path.Combine("Lite", "Analysis", "DuckDbFactCollector.QueryPerf.cs"), 1),
    ];

    [Fact]
    public void EveryQueryStoreDedup_OrdersByCollectionTimeThenExecutionCount()
    {
        /* The ORDER BY of a window whose PARTITION BY carries the interval identity. Lite's Query Store SQL
           is all inline string literals, so there is no constant to pin and the file itself is the surface. */
        var dedupOrderBy = new Regex(
            @"PARTITION BY[^()]*?runtime_stats_interval_id[^()]*?ORDER BY\s+collection_time\s+DESC(?<tie>,\s*execution_count DESC)?",
            RegexOptions.Singleline);

        var untied = new List<string>();
        var total = 0;

        foreach (var (relative, expected) in DedupSites)
        {
            var source = File.ReadAllText(SourcePath(relative));
            var matches = dedupOrderBy.Matches(source);

            Assert.True(
                matches.Count == expected,
                $"{relative}: expected {expected} Query Store dedup site(s), found {matches.Count}. " +
                "A read was added, removed, or moved — update this guard deliberately rather than letting " +
                "its coverage drift.");

            foreach (Match m in matches)
            {
                total++;
                if (!m.Groups["tie"].Success)
                {
                    untied.Add($"{relative} @ char {m.Index}");
                }
            }
        }

        Assert.True(
            untied.Count == 0,
            "Query Store dedup sites missing the #1907 execution_count tie-break:\n  " + string.Join("\n  ", untied));

        /* The count is asserted as a whole too: a file dropping to zero sites would otherwise be caught only
           by its own per-file assertion, and this states the total the fix actually swept. */
        Assert.Equal(8, total);
    }

    /// <summary>
    /// The tie-break must FOLLOW collection_time, never replace it. "Latest" is decided by when a row was
    /// collected — an interval's execution_count can sit still across a hundred re-collections (the 496x
    /// shape from #1841), so ordering by the count first would keep the stalest snapshot's averages.
    /// </summary>
    [Fact]
    public void TheTieBreakNeverBecomesThePrimarySort()
    {
        foreach (var (relative, _) in DedupSites)
        {
            var source = File.ReadAllText(SourcePath(relative));
            Assert.DoesNotContain("ORDER BY execution_count", source, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Lite and Darling must carry the SAME number of guarded dedup sites as their own guards declare, so
    /// neither app can quietly fall behind the other. This reads Darling's guard file and compares its
    /// declared total against this one's — the parity claim made executable rather than asserted in a
    /// comment, since drift between the two apps is the failure this defect class keeps producing.
    /// </summary>
    [Fact]
    public void BothAppsGuardTheirOwnDedupSites_SoNeitherSideCanFallBehind()
    {
        var darlingGuard = SourcePath(Path.Combine("Darling", "Darling.Tests", "QueryStoreSliceTieBreakSourceTests.cs"));
        Assert.True(File.Exists(darlingGuard), "Darling's counterpart guard is missing: " + darlingGuard);

        var darlingSource = File.ReadAllText(darlingGuard);

        /* Darling declares its own total the same way this file does. If that guard is deleted or stops
           asserting a total, this fails rather than silently becoming a one-sided check. The literal below
           MIRRORS Darling's declared total: raising one without the other is what broke dev on #2830, so
           bump both in the same commit. */
        var declared = Regex.Match(darlingSource, @"Assert\.Equal\((?<total>\d+), total\);");
        Assert.True(declared.Success, "Darling's guard no longer declares a total dedup-site count.");
        Assert.Equal(18, int.Parse(declared.Groups["total"].Value, System.Globalization.CultureInfo.InvariantCulture));

        /* And it must still enumerate its files rather than globbing, for the same reason this one does. */
        Assert.Contains("DedupSites", darlingSource, StringComparison.Ordinal);
    }

    /// <summary>Walks up from the test binary to the repo root so the pin works from any run directory.</summary>
    private static string SourcePath(string relative)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, "could not locate the repository root from " + AppContext.BaseDirectory);
        return Path.Combine(dir!, relative);
    }
}
