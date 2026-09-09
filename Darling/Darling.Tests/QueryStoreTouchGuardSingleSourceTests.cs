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
using System.Reflection;
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
       point of this pin is that the SECOND file is the one an editor misses.

       This is the DECLARED set, and TheGuardedStoreTypesAreDerivedFromTheGuardSites requires it to equal the
       set the project-wide scan finds. So it is a decision record rather than the scan's input: a new file
       carrying a guard site reds here until somebody decides it belongs, and a file listed here that has no
       guard site reds too. */
    private static readonly string[] GuardedSourceFiles =
    [
        "QueryStorePlanMap.cs",
        "QueryStoreTextStore.cs",
    ];

    /// <summary>
    /// The store type each guarded file declares. Convention, verified: the file name is the type name, and
    /// the type is resolved out of the built assembly so a rename cannot quietly drop a term.
    /// </summary>
    private static string StoreTypeOf(string sourceFile) =>
        Path.GetFileNameWithoutExtension(sourceFile);

    /// <summary>
    /// A file the scan must SEE and must NOT classify as guarded - the exclusion made measurable.
    ///
    /// <para><see cref="PgStatementText"/> declares a <c>PruneMarginDays</c> of 2 and has no guard site: its
    /// <c>UpsertSql</c> conflict arm advances <c>last_seen</c> on every conflict under a monotonicity guard,
    /// not a staleness one, and its margin exists so text outlives the statistics rows referencing it. Two
    /// is above <see cref="QueryStoreLivenessTouchGuard.StampSkewMarginDays"/>, so including it in the
    /// <c>min</c> could not move the value - which is precisely why the criterion needs a test rather than a
    /// sentence. Named here so the exclusion reds if the scan ever stops being able to see the file, which is
    /// the failure a set equality cannot report: an invisible file is absent from both sides and they
    /// agree.</para>
    /// </summary>
    private const string UnguardedMarginFile = "PgStatementText.cs";

    /* Floor on the project-wide sweep. The storage project holds dozens of .cs files; a glob that stopped
       matching, or a directory that moved, would otherwise derive an EMPTY guarded set and agree with an
       empty expectation. */
    private const int MinimumProjectFilesScanned = 20;

    /* How a guard site is recognised in CODE (comments and literals blanked): a reference to the one
       constant. Deliberately the code reference and not the SQL text - a site that spells its own interval is
       NoGuardSiteSpellsItsOwnWidth's business, and counting it as a guard site here would let a reverted site
       keep its membership. */
    private const string GuardReference = "QueryStoreLivenessTouchGuard.GuardInterval";

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

        /* Sized on the TIGHTEST margin among the guarded tables, so one shared width is safe on all of
           them. WHICH tables those are is TheGuardedStoreTypesAreDerivedFromTheGuardSites' business - this
           only checks the value the enumeration produces. */
        Assert.Equal(
            QueryStoreLivenessTouchGuard.StampSkewMarginDays,
            Math.Min(QueryStorePlanMap.PruneMarginDays, QueryStoreTextStore.PruneMarginDays));

        Assert.Equal(
            QueryStoreLivenessTouchGuard.GuardHours * QueryStoreLivenessTouchGuard.MarginShareDivisor,
            QueryStoreLivenessTouchGuard.StampSkewMarginDays * 24);

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

    /// <summary>
    /// <see cref="QueryStoreLivenessTouchGuard.StampSkewMarginDays"/> is a <c>min</c> over an enumeration, and
    /// the enumeration's MEMBERSHIP is derived from the guard sites rather than declared beside it.
    ///
    /// <para><b>Why this needs a test and not a sentence.</b> The criterion is "the prune margin of every
    /// table this guard's touch writes <c>last_seen</c> on". The tree holds three <c>PruneMarginDays</c>
    /// constants and only two of them qualify: <see cref="PgStatementText.PruneMarginDays"/> is 2 and belongs
    /// to a table with NO guard site, whose upsert advances <c>last_seen</c> on every conflict under a
    /// monotonicity guard and whose margin exists so text outlives the statistics rows referencing it.
    /// Because 2 is above the current minimum, adding it to the <c>min</c> would be a genuine category error
    /// that changes NO value - so no other assertion here, and no reviewer reading the result, would notice.
    /// A term that is wrong to include and cannot move the answer is the worst kind to leave
    /// unenforced.</para>
    ///
    /// <para>So the guarded store types are derived by sweeping the whole storage project for guard sites,
    /// and the set of <c>PruneMarginDays</c> terms in the initializer's own SOURCE must equal that set. A new
    /// store type that grows a guard joins the <c>min</c> or reds; one without a guard cannot be added to it
    /// by mistake.</para>
    /// </summary>
    [Fact]
    public void TheGuardedStoreTypesAreDerivedFromTheGuardSites()
    {
        var projectDirectory = RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Storage");
        var separator = Path.DirectorySeparatorChar;
        var projectFiles = Directory
            .EnumerateFiles(projectDirectory, "*.cs", SearchOption.AllDirectories)
            .Where(path => !path.Contains($"{separator}obj{separator}", StringComparison.Ordinal)
                        && !path.Contains($"{separator}bin{separator}", StringComparison.Ordinal))
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();

        /* The anti-vacuity floor. A moved directory or a broken filter derives an EMPTY guarded set, which
           agrees with an empty expectation and reports clean. */
        Assert.True(
            projectFiles.Count >= MinimumProjectFilesScanned,
            $"only {projectFiles.Count} .cs files were swept out of {projectDirectory} (floor "
            + $"{MinimumProjectFilesScanned}) - the sweep is not reading the storage project, so every set "
            + "equality below would hold for a reason unrelated to guard membership.");

        var scannedNames = projectFiles.Select(path => Path.GetFileName(path)!).ToList();

        /* The exclusion has to be a fact about a file the sweep CAN see. An invisible file is absent from the
           derived set and absent from the expectation, so the equality passes while the decision it looks
           like was never taken. */
        Assert.Contains(UnguardedMarginFile, scannedNames, StringComparer.Ordinal);

        var derived = projectFiles
            .Where(path => CSharpSourceWalker
                .StripCommentsAndStrings(File.ReadAllText(path))
                .Contains(GuardReference, StringComparison.Ordinal))
            .Select(path => Path.GetFileName(path)!)
            .Where(name => !string.Equals(name, "QueryStoreLivenessTouchGuard.cs", StringComparison.Ordinal))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(GuardedSourceFiles.OrderBy(f => f, StringComparer.Ordinal).ToArray(), derived);
        Assert.DoesNotContain(UnguardedMarginFile, derived, StringComparer.Ordinal);

        /* Every derived file's store type must exist in the built assembly and declare the constant, so a
           rename cannot drop a term while the file name still parses. */
        var expectedTerms = derived.Select(StoreTypeOf).OrderBy(t => t, StringComparer.Ordinal).ToArray();

        foreach (var storeType in expectedTerms)
        {
            var type = typeof(QueryStoreLivenessTouchGuard).Assembly
                .GetType("PerformanceMonitor.Darling.Storage." + storeType, throwOnError: false);

            Assert.NotNull(type);
            Assert.NotNull(type!.GetField("PruneMarginDays", BindingFlags.Public | BindingFlags.Static));
        }

        /* And the initializer's OWN terms, read out of source with comments and literals blanked, must be
           exactly those types. This is the assertion that reds on PgStatementText.PruneMarginDays being
           folded in, which every value assertion in this class passes unchanged. */
        var guardSource = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreLivenessTouchGuard.cs"));

        var declarationAt = guardSource.IndexOf("StampSkewMarginDays =", StringComparison.Ordinal);
        Assert.True(
            declarationAt >= 0,
            "StampSkewMarginDays' initializer was not found in the code, so its terms cannot be read and "
            + "this pin would assert nothing.");

        var initializer = CSharpSourceWalker.StatementSpanFrom(guardSource, declarationAt, 1);
        var actualTerms = Regex
            .Matches(initializer, @"([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*PruneMarginDays")
            .Select(match => match.Groups[1].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(term => term, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(expectedTerms, actualTerms);

        /* query_plan_dim is the third table the touch writes and contributes no term, because it has no
           PruneMarginDays: its room above the fact horizon is ChunkIntervalDays + 1 in
           DarlingRetention.ComputeDimensionCutoff. MarginOrderingHolds already pins the map's margin strictly
           below that, so guard <= map margin < dim margin follows from an invariant this repo already holds
           rather than from a second copy of the arithmetic. Asserted here so the chain reds if that ordering
           ever inverts, which would silently put the dim inside the guard's reach. */
        Assert.True(
            QueryStorePlanMap.MarginOrderingHolds(TimescaleSupport.ChunkIntervalDays),
            "the map's prune margin is no longer strictly inside the dimension's, so a guard sized on the "
            + "map's margin no longer clears the dimension GC and query_plan_dim needs a term of its own.");
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
