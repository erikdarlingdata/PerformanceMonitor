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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every fact read must carry an EXPLICIT command deadline (#2810).
///
/// <para>All thirty-one commands in <c>PgFactCollector</c> ran with no <c>CommandTimeout</c>, so
/// every one of them inherited Npgsql's undocumented 30 s default. Nobody chose 30 s; it was simply
/// what happened. Measured on the production store after retention was restored (#2809) and after
/// #2827's dedup rewrite, the plan-regression read still crossed that ceiling on a COLD run — 34.7 s
/// and 32.4 s on the two largest servers, against 10.6-21.2 s for the same servers warm. A few
/// seconds of overshoot, on the cold/large combination only, which is exactly why it presented as
/// intermittent and why two prior changes aimed elsewhere: #2827 made the query 2.4x faster and
/// #2826 made the failure visible, but neither touched the ceiling it was failing against.</para>
///
/// <para><b>Why this pin is shaped over the FAMILY rather than the one site.</b> The defect was
/// reported against <c>PlanRegressionSql</c>. Fixing only that command would have been
/// scenario-shaped, and this repo has already paid twice for exactly that — the #2344 enumerated pin,
/// and <c>PgStatementText</c>, where a broken sibling arm survived a green suite because the test
/// named the arm it was written for. Thirty-one commands shared one missing value; the assertion is
/// therefore structural: EVERY command constructed in this collector sets a deadline, so the next one
/// somebody adds cannot quietly reintroduce the inherited default.</para>
///
/// <para>The value itself is pinned separately and deliberately below. It is bounded ABOVE by the
/// 120 s analysis pass budget that thirty collect methods share, not just below by how long the
/// slowest query takes — a detail that is easy to lose and expensive to relearn, because a timeout
/// raised past the pass budget converts "one fact is missing" into "every fact is missing".</para>
/// </summary>
public sealed class FactCollectorCommandTimeoutTests
{
    /// <summary>
    /// A command construction in this collector. Matched by shape rather than by variable name — the
    /// sites use both <c>cmd</c> and <c>command</c>, and a pin keyed on either spelling would miss
    /// half of them.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new NpgsqlCommand\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryFactCollectorCommandSetsAnExplicitTimeout()
    {
        var missing = new List<string>();
        var covered = 0;

        foreach (var file in FactCollectorSources())
        {
            var lines = File.ReadAllLines(file);

            for (var i = 0; i < lines.Length; i++)
            {
                if (!s_commandCtor.IsMatch(lines[i]))
                {
                    continue;
                }

                /* The initializer may wrap, so the deadline counts if it appears on the construction
                   line or in the statement that follows it — both are in use across this codebase
                   (PgPlanFetcher uses the initializer, PgDrillDownCollector assigns afterwards). */
                var window = string.Join(
                    "\n",
                    lines.Skip(i).Take(Math.Min(3, lines.Length - i)));

                if (s_setsTimeout.IsMatch(window))
                {
                    covered++;
                }
                else
                {
                    missing.Add($"{Path.GetFileName(file)}:{i + 1}");
                }
            }
        }

        Assert.True(
            missing.Count == 0,
            "These fact-collector commands carry no explicit CommandTimeout, so they inherit Npgsql's "
            + "undocumented 30 s default — the defect #2810 fixed, where a read that needed 34.7 s cold "
            + "failed on every cold pass and nobody had chosen the ceiling it failed against:"
            + Environment.NewLine + string.Join(Environment.NewLine, missing));

        /* A floor, not an equality: the point is that nobody adds a command without a deadline, and a
           pin that also broke on every ADDED collector would be a tax on writing one. The floor still
           fails loudly if the sweep is partially reverted, which is the regression that matters. */
        Assert.True(
            covered >= 31,
            $"Expected at least the 31 swept command sites to set a timeout; found {covered}. "
            + "A drop means sites were removed or the timeout was refactored out from under this pin.");
    }

    /// <summary>
    /// The value is bounded on BOTH sides, and the upper bound is the one that is easy to get wrong.
    ///
    /// <para>Below: the measured cold worst case was 34.7 s, so anything at or under that reproduces
    /// #2810. Above: <c>DarlingWorker.s_analysisTimeout</c> gives the whole analysis pass 120 s and
    /// thirty collect methods share it, so a per-command deadline approaching that figure lets ONE
    /// stalled read consume the pass and cost the server its other twenty-nine facts — a strictly
    /// worse failure than the one being fixed. This asserts the value stays in the band where both
    /// arguments hold.</para>
    /// </summary>
    [Fact]
    public void TheTimeoutClearsTheMeasuredColdWorstCaseWithoutOwningThePassBudget()
    {
        const int MeasuredColdWorstCaseSeconds = 35;
        const int AnalysisPassBudgetSeconds = 120;

        Assert.True(
            PgFactCollector.FactCommandTimeoutSeconds > MeasuredColdWorstCaseSeconds,
            $"The deadline ({PgFactCollector.FactCommandTimeoutSeconds}s) must clear the measured cold "
            + $"worst case ({MeasuredColdWorstCaseSeconds}s) or #2810 reproduces: the plan-regression "
            + "read took 34.7s cold on the largest production server.");

        Assert.True(
            PgFactCollector.FactCommandTimeoutSeconds <= AnalysisPassBudgetSeconds / 2,
            $"The deadline ({PgFactCollector.FactCommandTimeoutSeconds}s) must leave at least half of "
            + $"the {AnalysisPassBudgetSeconds}s analysis pass for the other twenty-nine collect "
            + "methods. One read that stalls must not cost a server every other fact.");
    }

    private static IEnumerable<string> FactCollectorSources()
    {
        var darling = Directory.GetFiles(
            Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis"),
            "PgFactCollector.*.cs");

        Assert.NotEmpty(darling);

        return darling.OrderBy(f => f, StringComparer.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
