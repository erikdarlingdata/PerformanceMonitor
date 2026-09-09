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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Query Store liveness touch guard is SINGLE-SOURCED (#3189). Three statements guard a
/// <c>last_seen</c> touch — <see cref="QueryStorePlanMap.TouchAndProbeSql"/>'s <c>touched</c> CTE, that
/// statement's <c>dim_touch</c>, and <see cref="QueryStoreTextStore.TouchAndProbeSql"/>'s <c>touched</c>
/// CTE — and they must all carry the SAME width, from
/// <see cref="QueryStoreLivenessTouchGuard.GuardInterval"/>.
///
/// <para><b>Why the property and not the value.</b> <see cref="QueryStorePlanMap.TouchAndProbeSql"/>'s own
/// documentation says the map row and the dimension row cannot age out at different times, and calls that
/// STRUCTURALLY impossible rather than carefully avoided. With a literal per site it is neither — it is
/// copies that happen to agree — and the failure mode is a partial migration: someone widens the two sites
/// in the file they are reading and leaves the third, in the other file, at the old width. A value pin
/// ("the width is six hours") passes a partial migration the moment its own expected value is updated in
/// the same commit, so the pin that has to hold is the single-sourcing.</para>
///
/// <para><b>The instrument is the SOURCE scan, and it is the one to attack.</b> Once all three sites
/// interpolate one constant they cannot disagree, so an assertion over the SHIPPED strings that they agree
/// is unfalsifiable by construction — true of the implementation, not evidence about it. What can still
/// happen is somebody typing an interval back into one site, and only reading the source can see that. So
/// <see cref="NoGuardSiteSpellsItsOwnWidth"/> is the control, the composed-SQL pin below is the corroborator
/// (it catches a site appearing or disappearing, which the source scan alone would not price), and the
/// derivation pin holds the width to the margin it is spent out of.</para>
/// </summary>
public sealed class QueryStoreTouchGuardSingleSourceTests
{
    /* The two files that carry a guarded last_seen touch. Named rather than globbed: a glob that stopped
       matching would shrink the scanned set to nothing and report a clean bill of health, and the whole
       point of this pin is that the SECOND file is the one an editor misses. */
    private static readonly string[] GuardedSourceFiles =
    [
        "QueryStorePlanMap.cs",
        "QueryStoreTextStore.cs",
    ];

    /// <summary>
    /// The three guarded touches: two in <see cref="QueryStorePlanMap"/> (the <c>touched</c> CTE and
    /// <c>dim_touch</c>) and one in <see cref="QueryStoreTextStore"/>. An exact count, not a floor — a floor
    /// passes when a site is deleted, and a deleted guard is an unguarded touch on every cycle.
    /// </summary>
    private const int GuardSiteCount = 3;

    /* Every guard site is this comparison against the batch stamp, immediately followed by the interval.
       Anchoring on the shape rather than on a table alias is what lets one expectation cover all three. */
    private const string GuardAnchor = "last_seen < $5::timestamp - ";

    /* The RAW reader, not the LF-normalising one, and deliberately: every anchor below is single-line, so
       nothing here can span a line break. The one place a newline could reach an anchor —
       GuardHours' initializer, which a reformat could wrap — collapses whitespace itself before matching,
       which is a stronger guarantee than normalising the newline and still depending on the rest of the
       spacing. See RepoFileAdoptionTests for why that choice is a decision rather than a default. */

    /* An interval literal with a NUMBER in it. The prune statements in both files build
       "INTERVAL '" + chunkIntervalDays + " days'" by concatenation, so no single literal there ever holds a
       digit inside the quotes — which is why this pattern discriminates a hard-coded guard from the
       legitimate composed intervals in the same files. */
    private static readonly Regex NumericIntervalLiteral =
        new(@"interval\s+'\s*\d", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /* Floors on the scan itself. Measured on dev at the time of writing: 24 string literals in
       QueryStorePlanMap.cs and 15 in QueryStoreTextStore.cs. Set low enough that ordinary growth never trips
       them and high enough that a desynchronised literal walk, or a file read that came back empty, fails
       instead of reporting that no site spells its own width. */
    private const int MinimumLiteralsPerFile = 5;

    /// <summary>
    /// The control. No guard site may spell its own interval: reading the two files' string literals, none
    /// of them contains an interval with a number in it, so every width in both statements comes from
    /// <see cref="QueryStoreLivenessTouchGuard.GuardInterval"/>.
    ///
    /// <para>This is what catches a PARTIAL migration — one site reverted to <c>interval '1 hour'</c> while
    /// the other two follow the constant — which is the divergence
    /// <see cref="QueryStorePlanMap.TouchAndProbeSql"/> declares impossible. It fails identically whichever
    /// of the three sites is reverted, and whichever of the two files it lives in.</para>
    /// </summary>
    [Fact]
    public void NoGuardSiteSpellsItsOwnWidth()
    {
        foreach (var file in GuardedSourceFiles)
        {
            var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", file);
            var literals = CSharpSourceWalker.StringLiteralBodies(source).ToList();

            Assert.True(
                literals.Count >= MinimumLiteralsPerFile,
                $"only {literals.Count} string literals were walked out of {file} (floor "
                + $"{MinimumLiteralsPerFile}) — the scan resolved nothing, so it cannot say whether a guard "
                + "site spells its own width.");

            foreach (var (start, text) in literals)
            {
                var hit = NumericIntervalLiteral.Match(text);

                Assert.False(
                    hit.Success,
                    $"{file} spells an interval width in a string literal at offset {start + hit.Index} "
                    + $"('{text.Substring(hit.Index, Math.Min(24, text.Length - hit.Index))}'). The Query "
                    + "Store liveness touch guard is written once, in "
                    + "QueryStoreLivenessTouchGuard.GuardInterval, because the map row and the dimension row "
                    + "must not be able to age out at different times — with a literal per site they are "
                    + "copies that happen to agree, and widening some of them is exactly the divergence "
                    + "QueryStorePlanMap.TouchAndProbeSql declares structurally impossible.");
            }
        }
    }

    /// <summary>
    /// Every guarded touch in the SHIPPED statements carries the one width, and there are exactly
    /// <see cref="GuardSiteCount"/> of them. The corroborator: it cannot see a hard-coded literal that
    /// happens to equal the constant, which is why
    /// <see cref="NoGuardSiteSpellsItsOwnWidth"/> is the control — but it does catch a guard site
    /// disappearing (an unguarded touch, one write per row per cycle) or a fourth one arriving at some other
    /// width.
    /// </summary>
    [Fact]
    public void EveryGuardSiteInTheShippedSqlCarriesTheOneWidth()
    {
        var statements = new[] { QueryStorePlanMap.TouchAndProbeSql, QueryStoreTextStore.TouchAndProbeSql };
        var widths = new List<string>();

        foreach (var sql in statements)
        {
            var at = sql.IndexOf(GuardAnchor, StringComparison.Ordinal);

            while (at >= 0)
            {
                var after = at + GuardAnchor.Length;
                var end = sql.IndexOf('\n', after);
                widths.Add((end < 0 ? sql[after..] : sql[after..end]).TrimEnd('\r'));
                at = sql.IndexOf(GuardAnchor, after, StringComparison.Ordinal);
            }
        }

        Assert.Equal(GuardSiteCount, widths.Count);
        Assert.All(widths, width => Assert.Equal(QueryStoreLivenessTouchGuard.GuardInterval, width));

        /* The two plan-side sites are in ONE statement, so a reader checking only the file they are editing
           still sees two of the three. Stated as its own assertion because that asymmetry is the reason the
           third site gets missed. */
        Assert.Equal(2, CountOf(QueryStorePlanMap.TouchAndProbeSql, GuardAnchor));
        Assert.Equal(1, CountOf(QueryStoreTextStore.TouchAndProbeSql, GuardAnchor));
    }

    /// <summary>
    /// The width is a stated share of the margin the prune cutoffs reserve for a trailing stamp, and it
    /// FOLLOWS that margin rather than restating a number that currently agrees with it.
    ///
    /// <para>The source assertion is what makes this non-vacuous. <c>GuardHours * MarginShareDivisor ==
    /// StampSkewMarginHours</c> holds just as well for a hard-coded <c>6</c> while the margin happens to be
    /// one day, so on its own it would only fail once somebody moved the margin — which is exactly when
    /// nobody is looking at this file. Asserting the initializer forbids the hard-coding outright.</para>
    /// </summary>
    [Fact]
    public void TheWidthIsAShareOfTheStampSkewMarginAndFollowsIt()
    {
        Assert.True(
            QueryStoreLivenessTouchGuard.GuardStaysInsideStampSkewMargin(),
            $"the guard is {QueryStoreLivenessTouchGuard.GuardHours}h taking "
            + $"1/{QueryStoreLivenessTouchGuard.MarginShareDivisor} of a "
            + $"{QueryStoreLivenessTouchGuard.StampSkewMarginHours}h margin — it must be at least an hour, "
            + "and the divisor must divide the margin exactly, or the share taken is not the share stated.");

        /* Sized on the TIGHTER of the two prune margins, so one shared width is safe on both sides. */
        Assert.Equal(
            Math.Min(QueryStorePlanMap.PruneMarginDays, QueryStoreTextStore.PruneMarginDays),
            QueryStoreLivenessTouchGuard.StampSkewMarginDays);

        Assert.Equal(
            QueryStoreLivenessTouchGuard.StampSkewMarginDays * 24,
            QueryStoreLivenessTouchGuard.GuardHours * QueryStoreLivenessTouchGuard.MarginShareDivisor);

        /* And the width is DERIVED in source, not restated. Read from the code with comments and literals
           blanked, so an initializer quoted in a doc comment cannot satisfy it. */
        var guardSource = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreLivenessTouchGuard.cs"));

        Assert.Contains(
            "GuardHours = StampSkewMarginHours / MarginShareDivisor",
            Regex.Replace(guardSource, @"\s+", " "),
            StringComparison.Ordinal);

        /* The interval is rendered from the derived hours rather than typed, for the same reason. */
        Assert.Equal(
            $"interval '{QueryStoreLivenessTouchGuard.GuardHours} hours'",
            QueryStoreLivenessTouchGuard.GuardInterval);
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var at = haystack.IndexOf(needle, StringComparison.Ordinal);

        while (at >= 0)
        {
            count++;
            at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }
}
