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
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A fact collector that CANNOT run must be distinguishable from one that ran and found nothing
/// (#2826).
///
/// <para>Every collect method in both fact collectors ends in a catch that degrades to "no facts",
/// which is the right behaviour — one unavailable input must not cost a server its other
/// twenty-seven facts. The defect was that the catch body was EMPTY, so a <c>PlanRegressionSql</c>
/// cancelled by its inherited 30 s Npgsql deadline produced output byte-identical to the
/// <c>if (offenderCount == 0) return;</c> four lines above it. On the dogfood box that happened 325
/// times in one day, and plan-regression detection was in effect off for the servers where the query
/// reliably exceeded its deadline. Nothing downstream could tell, because the collected data is
/// unaffected either way — which is precisely why nothing noticed.</para>
///
/// <para><b>Why this pin is shaped over the FAMILY.</b> The bug was reported at one site. Fixing
/// that site would have been scenario-shaped, and this repo has now paid twice for exactly that: the
/// #2344 enumerated pin, and <c>PgStatementText</c>, where a broken sibling arm survived a green
/// suite because the test named the arm it was written for. So the assertion here is structural —
/// EVERY catch of the fact-collector swallow shape must report — and it is matched by SHAPE rather
/// than by the old <c>"Table may not exist or have no data"</c> comment. That matters: matching the
/// comment found 50 sites, matching the shape found 54. The four the comment missed were the same
/// defect wearing different wording (<c>"Columns may not exist yet (pre-migration)"</c>,
/// <c>"best-effort"</c>), and a comment-shaped pin would have left them silent forever.</para>
///
/// <para>Both collectors are scanned from ONE test file on purpose. They are a method-for-method
/// port of each other and the class doc says so; a blind spot repaired on one side only would
/// silently re-open on the other the next time someone ports a method across.</para>
/// </summary>
public sealed class FactCollectorFailureReportingTests
{
    /// <summary>
    /// The swallow shape, in both dialects — Darling's <c>AnalysisShutdown.IsExpectedAbandon</c> and
    /// Lite's narrower <c>AnalysisAbandon.IsExpected</c>. The <c>when</c> filter is the tell: it is
    /// what makes these catches "swallow everything that is not an abandonment", and there is no
    /// other catch in either file that wears it.
    /// </summary>
    private static readonly Regex s_swallowCatch = new(
        @"^\s*catch\s*\(Exception ex\)\s*when\s*\(!Analysis(?:Shutdown\.IsExpectedAbandon|Abandon\.IsExpected)\(ex,",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_reports = new(
        @"ReportCollectionFailure\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EverySwallowingFactCollectorCatchReportsWhyItSwallowed()
    {
        var unreported = new List<string>();
        var reported = 0;

        foreach (var file in FactCollectorSources())
        {
            var lines = File.ReadAllLines(file);

            for (var i = 0; i < lines.Length; i++)
            {
                if (!s_swallowCatch.IsMatch(lines[i]))
                {
                    continue;
                }

                var body = CatchBody(lines, i);

                if (s_reports.IsMatch(body))
                {
                    reported++;
                }
                else
                {
                    unreported.Add($"{Path.GetFileName(file)}:{i + 1}");
                }
            }
        }

        Assert.True(
            unreported.Count == 0,
            $"These fact-collector catches swallow a failure without reporting why, so a collector "
            + $"that could not run is indistinguishable from one that found nothing (#2826):"
            + Environment.NewLine + string.Join(Environment.NewLine, unreported));

        /* A floor, not an equality: the point is that nobody adds an unreporting catch, and a pin
           that also broke on every ADDED collector would be a tax on writing one. The floor still
           fails loudly if the sweep is partially reverted, which is the regression that matters. */
        Assert.True(
            reported >= 54,
            $"Expected at least the 54 swept catch sites to report; found {reported}. "
            + "A drop means sites were removed or the reporting call was refactored out from under this pin.");
    }

    /// <summary>
    /// The classifier has to separate a timeout from everything else STRUCTURALLY, because that
    /// distinction is the entire point of #2826 and because Npgsql renders its own client-side
    /// deadline as "Exception while reading from stream" — read literally, that says the network
    /// broke. Pinned here against the shared helper the reporter delegates to, so the reporter cannot
    /// quietly start message-matching instead.
    /// </summary>
    [Fact]
    public void TheReporterClassifiesTimeoutsStructurallyAndNotByMessage()
    {
        Assert.True(PgBaselineProvider.IsCommandTimeout(
            new PostgresException("cancelled", "ERROR", "ERROR", "57014")));
        Assert.True(PgBaselineProvider.IsCommandTimeout(new TimeoutException("timed out")));

        /* The ambiguous text alone is NOT a timeout — only the wrapped TimeoutException makes it one.
           A message-matching reporter would get this backwards and relabel real connection faults. */
        Assert.False(PgBaselineProvider.IsCommandTimeout(
            new NpgsqlException("Exception while reading from stream")));
        Assert.True(PgBaselineProvider.IsCommandTimeout(
            new NpgsqlException("Exception while reading from stream", new TimeoutException())));

        /* 42P01 is the case the old comment was written for, and it must NOT read as a timeout. */
        Assert.False(PgBaselineProvider.IsCommandTimeout(
            new PostgresException("no such table", "ERROR", "ERROR", "42P01")));

        /* Nor 42703. Review catch on this PR: the site whose comment reads "Columns may not exist
           yet (pre-migration)" selects engine_edition / product_version, which a later migration rung
           added — so a rolling deploy raises undefined_COLUMN there, not undefined_TABLE. Classifying
           only 42P01 would have logged an ERROR every pass for the whole migration window, for
           precisely the transient condition the quiet arm exists to hold. */
        Assert.False(PgBaselineProvider.IsCommandTimeout(
            new PostgresException("no such column", "ERROR", "ERROR", "42703")));
    }

    /// <summary>
    /// Both pre-migration SQLSTATEs stay in the quiet arm. Pinned on the SHIPPED source rather than a
    /// retyped copy of the condition, so a future edit that drops one goes red here.
    /// </summary>
    [Fact]
    public void TheQuietArmCoversMissingColumnsAndNotJustMissingTables()
    {
        var source = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.cs"));

        var arm = new Regex(
            @"ex is PostgresException \{ SqlState: (?<states>[^}]*) \}",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        var m = arm.Match(source);
        Assert.True(m.Success, "the SQLSTATE-classifying arm should still exist in ReportCollectionFailure");

        var states = m.Groups["states"].Value;
        Assert.Contains("42P01", states, StringComparison.Ordinal);
        Assert.Contains("42703", states, StringComparison.Ordinal);
    }

    private static IEnumerable<string> FactCollectorSources()
    {
        var root = RepoRoot();

        var darling = Directory.GetFiles(
            Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Analysis"),
            "PgFactCollector.*.cs");

        var lite = Directory.GetFiles(
            Path.Combine(root, "Lite", "Analysis"),
            "DuckDbFactCollector.*.cs");

        Assert.NotEmpty(darling);
        Assert.NotEmpty(lite);

        return darling.Concat(lite).OrderBy(f => f, StringComparer.Ordinal);
    }

    /// <summary>
    /// The catch's body text, from the brace after the catch line to its match. Brace counting is
    /// enough here — these bodies contain no string literal carrying an unbalanced brace, and the
    /// test asserts over what it found rather than trusting the count.
    /// </summary>
    private static string CatchBody(string[] lines, int catchLine)
    {
        var depth = 0;
        var body = new List<string>();

        for (var j = catchLine + 1; j < lines.Length; j++)
        {
            depth += lines[j].Count(c => c == '{') - lines[j].Count(c => c == '}');
            body.Add(lines[j]);

            if (depth <= 0 && lines[j].Contains('}'))
            {
                break;
            }
        }

        return string.Join(Environment.NewLine, body);
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
