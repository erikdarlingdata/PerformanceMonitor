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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Source-containment guard for the #1907 tie-break across EVERY Darling Query Store dedup site.
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
/// closest-available, not correct; the correct value is the SUM, which no read-side rule can express, and
/// the residual is tracked in #1912.</para>
///
/// <para>This is a SOURCE guard rather than a set of per-query assertions because the failure it prevents is
/// omission. The existing per-read pins all assert <c>Contains("ORDER BY collection_time DESC")</c>, which a
/// site missing the tie-break still satisfies — so a thirteenth dedup site, or a "simplified" ORDER BY,
/// would ship silently. Counting every partition against every tie-break is what makes that impossible.
/// Lite has the same guard in <c>QueryStoreDedupReadTests</c>; the two together cover both apps, which is
/// the parity this defect class keeps breaking.</para>
/// </summary>
public sealed class QueryStoreSliceTieBreakSourceTests
{
    /// <summary>
    /// Every Darling file holding a Query Store dedup window, with the number of dedup sites in each.
    /// Written out rather than globbed so that DELETING a read — or moving one to a new file — fails here
    /// and has to be re-declared, instead of quietly reducing the guard's coverage to whatever is left.
    /// </summary>
    private static readonly (string Path, int Sites)[] DedupSites =
    [
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryStore.cs"), 4),
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryStoreRegressions.cs"), 2),
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryTrends.cs"), 1),
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.ItemTimeline.cs"), 1),
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs"), 1),
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Compose", "ComposeCompiler.cs"), 1),
        /* 7, not 1, since #2827: the plan-regression dedup is one logical site but expresses its ordering
           once per projected column, as GROUP BY + ordered array_agg rather than a single window. Each of
           those orderings is independently breakable — that is the whole reason the count is declared here
           rather than globbed — so all seven are held to the tie-break, not just the first. */
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.QueryPerf.cs"), 7),
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", "PgDrillDownCollector.Queries.cs"), 1),
    ];

    [Fact]
    public void EveryQueryStoreDedup_OrdersByCollectionTimeThenExecutionCount()
    {
        /* The ORDER BY of a window whose PARTITION BY carries the interval identity. ComposeCompiler builds
           its SQL by C# concatenation with an interpolated time column, so the time term is matched as
           either the literal or the placeholder.

           #2827 added a SECOND shape. A dedup does not have to be a window function: PgFactCollector's
           plan-regression query now expresses the same "keep the latest row per interval" as GROUP BY plus
           ordered array_agg, because the window form forced one global sort of the server's whole Query
           Store slice (23.4s to 9.6s in isolation on the busiest use1 server, byte-identical results). The
           TIE-BREAK CONTRACT this guard exists to protect is unchanged and just as breakable in the new
           form — more so, since each aggregate sorts independently and a single drifted ORDER BY would
           blend two collections into one row. So the pattern matches the ordering wherever it appears
           rather than only inside a PARTITION BY, and the site count below still holds each file to a
           declared number so a dedup cannot be deleted or moved without saying so. */
        var dedupOrderBy = new Regex(
            @"(?:PARTITION BY[^()]*?runtime_stats_interval_id[^()]*?|array_agg\([^()]*?)ORDER BY\s+(?<time>collection_time|\{timeColumn\})\s+DESC(?<tie>,\s*execution_count DESC)?",
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
           by its own per-file assertion, and this states the total the fix actually swept.
           12 -> 18 with #2827: the plan-regression dedup states its ordering once per projected column
           (7) where the window form stated it once, so the same logical site now contributes seven. */
        Assert.Equal(18, total);
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
