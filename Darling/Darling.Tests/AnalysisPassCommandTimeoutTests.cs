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
/// EVERY command in the analysis pass must carry an explicit deadline (#2871), not just the fact
/// collector's (#2810).
///
/// <para><b>What was still missing.</b> #2810 swept <c>PgFactCollector</c>'s thirty-one commands and
/// left the other thirty-two in the same pass inheriting Npgsql's undocumented 30 s default. One of
/// them was failing in production: on the dogfood box the <c>io_latency</c> baseline timed out
/// nineteen times over two days and EVERY sample landed between 30.1 s and 31.4 s. A fixed wall —
/// not a variable-duration fault, and not the 15 s connection timeout. The same read completes in
/// ~1.6 s normally, measured against the live store on the three busiest servers, so it only crosses
/// the ceiling when the store stalls. That is why it read as intermittent, and why #2820 (5.6x
/// faster) and #2826 (made the failure visible) both left it failing: neither touched the ceiling.</para>
///
/// <para><b>Why the pin is over the pass, not the file.</b> The reported site was one baseline query.
/// Pinning that alone would be scenario-shaped, and this repo has paid for that repeatedly — #2344's
/// enumerated pin, and <c>PgStatementText</c>, where a broken sibling arm survived a green suite
/// because the test named the arm it was written for. The 120 s budget is shared by the fact
/// collector, the anomaly detector, the baseline provider, the drill-down and the finding store, so
/// the invariant belongs to the pass: every command constructed anywhere in this assembly sets a
/// deadline, and the next one somebody adds cannot quietly reintroduce the inherited default.</para>
///
/// <para>This subsumes <c>FactCollectorCommandTimeoutTests</c>' structural half by construction;
/// that test is left in place because its value-band assertion pins a different constant.</para>
/// </summary>
public sealed class AnalysisPassCommandTimeoutTests
{
    /* Matched by SHAPE, not by variable name: the sites use cmd, command, peakCmd, rateCmd,
       contribCmd and queryCmd, and a pin keyed on any spelling would miss most of them. */
    private static readonly Regex s_commandCtor = new(
        @"new NpgsqlCommand\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryAnalysisPassCommandSetsAnExplicitTimeout()
    {
        var missing = new List<string>();
        var covered = 0;

        foreach (var file in AnalysisSources())
        {
            var lines = File.ReadAllLines(file);

            for (var i = 0; i < lines.Length; i++)
            {
                if (!s_commandCtor.IsMatch(lines[i]))
                {
                    continue;
                }

                /* The deadline may sit in the object initializer on the construction line or be
                   assigned in the following statement — both spellings are in use here. */
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
            "These analysis-pass commands carry no explicit CommandTimeout, so they inherit Npgsql's "
            + "undocumented 30 s default — the defect #2871 fixed, where the io_latency baseline hit "
            + "that ceiling nineteen times in two days while the query itself runs in ~1.6 s:"
            + Environment.NewLine + string.Join(Environment.NewLine, missing));

        /* A floor rather than an equality, so adding a collector is not taxed; it still fails loudly
           if the sweep is partially reverted, which is the regression that matters. */
        Assert.True(
            covered >= 71,
            $"Expected at least the 71 analysis-pass command sites to set a timeout; found {covered}. "
            + "A drop means sites were removed or the deadline was refactored out from under this pin.");
    }

    /// <summary>
    /// The value is bounded on both sides, and the upper bound is the one that binds.
    ///
    /// <para><b>Below.</b> Every observed failure sat at 30.1-31.4 s, so anything at or under 31 s
    /// reproduces #2871. That record is RIGHT-CENSORED — each run was killed at the ceiling, so it
    /// cannot say whether the read wanted 35 s or 300 s. The deadline is therefore chosen as twice
    /// what it had rather than as a fitted value.</para>
    ///
    /// <para><b>Above.</b> <c>DarlingWorker.s_analysisTimeout</c> gives the pass 120 s, shared by the
    /// fact collector's thirty-one reads, up to eleven baseline computations per server, the anomaly
    /// detector and the drill-down. A per-command deadline approaching that figure lets ONE stalled
    /// command consume the pass and cost the server every other fact — strictly worse than the
    /// failure being fixed, which loses one metric.</para>
    /// </summary>
    [Fact]
    public void TheAnalysisDeadlineClearsTheObservedCeilingWithoutOwningThePassBudget()
    {
        const int ObservedCeilingSeconds = 31;
        const int AnalysisPassBudgetSeconds = 120;

        Assert.True(
            DarlingAnalysisService.AnalysisCommandTimeoutSeconds > ObservedCeilingSeconds,
            $"The deadline ({DarlingAnalysisService.AnalysisCommandTimeoutSeconds}s) must clear the "
            + $"observed {ObservedCeilingSeconds}s ceiling or #2871 reproduces: nineteen io_latency "
            + "baseline failures all landed between 30.1s and 31.4s.");

        Assert.True(
            DarlingAnalysisService.AnalysisCommandTimeoutSeconds <= AnalysisPassBudgetSeconds / 2,
            $"The deadline ({DarlingAnalysisService.AnalysisCommandTimeoutSeconds}s) must leave at "
            + $"least half of the {AnalysisPassBudgetSeconds}s pass for everything else in it. One "
            + "stalled command must not cost a server every other fact and baseline.");
    }

    private static IEnumerable<string> AnalysisSources()
    {
        var files = Directory.GetFiles(
            Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis"),
            "*.cs",
            SearchOption.TopDirectoryOnly);

        Assert.NotEmpty(files);

        return files.OrderBy(f => f, StringComparer.Ordinal);
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
