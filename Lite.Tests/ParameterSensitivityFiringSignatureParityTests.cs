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
/// Lite's half of the cross-SKU parity guard for the PARAMETER_SENSITIVITY firing signature. The exact
/// counterpart of <c>Darling.Tests.ParameterSensitivityFiringSignatureParityTests</c>, deliberately built
/// to the same shape so the two apps are guarded the same way rather than one being guarded and the other
/// trusted.
///
/// <para><b>The signature.</b> Three floors and a ratio, applied as one contiguous predicate block:
/// <c>min_worker_time &gt;= 10000</c>, <c>max_worker_time &gt;= 250000</c>,
/// <c>execution_count &gt;= 20</c>, the compiled-before-the-window test on the de-skewed
/// <c>creation_time_utc</c>, and a max-over-min worker time of at least 10. Lite writes it three times —
/// once in <c>DuckDbFactCollector</c>'s parameter-sensitivity read, once in
/// <c>DrillDownCollector</c>'s parameter-sensitivity drill-down, and once in the <c>psp_signature</c> CTE
/// of its regressed-queries read — and Darling writes the same three against its own store. The
/// <c>psp_signature</c> CTE's own comment states why they must agree: reusing the detector's thresholds is
/// what keeps the co-fired flag honest, so a query flagged there IS one the detector counts when it fires
/// and never a looser lookalike.</para>
///
/// <para><b>Why the pair, and not one scan.</b> Darling's twin does the cross-STORE comparison, because
/// <c>build.yml</c>'s <c>darling</c> path filter covers every Lite <c>.cs</c> file as well as the whole
/// Darling tree, so it runs on an edit to either analysis tree. This file owns Lite's own census and
/// meta-pins Darling's declared numbers, and the <c>lite</c> filter covers Darling's test tree — so an
/// edit that weakens Darling's guard runs this one. Raising a count or retuning a floor in one guard
/// without the other fails: <see cref="DarlingsTwinGuard_PinsTheSameCanonicalSignature_SoNeitherSideCanFallBehind"/>
/// reads Darling's guard as source and compares the two declarations, the technique
/// <c>QueryStoreSliceTieBreakSourceTests.BothAppsGuardTheirOwnDedupSites_SoNeitherSideCanFallBehind</c>
/// established after #2830 raised one side's total alone and dev went green on it.</para>
///
/// <para><b>Read from source, not from a constant.</b> Lite's analysis SQL is inline
/// <c>cmd.CommandText</c> where Darling's is a named constant reachable through <c>PgFactCollector.AllSql</c>
/// and <c>PgDrillDownCollector.AllSql</c>. The asymmetry does not matter here, because no test project
/// references both SKUs' assemblies — whichever suite hosts a cross-SKU claim has to read at least one
/// side out of the checked-out tree anyway (that is what <see cref="ParitySource"/> exists for). Reading
/// BOTH sides as source is what makes them comparable on the same terms, and it keeps the guard from
/// having to touch live analysis SQL to exist.</para>
///
/// <para><b>Matched in predicate context.</b> <c>10000</c> is not a searchable identity: it also appears
/// in <c>BaselineProvider</c>'s wait-stats restart exclusion, and in Darling's <c>PgBaselineProvider</c>
/// in both the <c>QUALIFY</c> shape and the <c>prior_total_wait_ms</c> shape the PostgreSQL rewrite
/// produces. <see cref="TheDiscriminator_MatchesEveryShippedForm_AndSkew_ButNotABareThreshold"/> holds
/// those exact corpus lines as negative controls and the shipped forms as positive ones, so a pattern
/// that has quietly stopped matching fails instead of reporting a clean bill of health.</para>
/// </summary>
public sealed class ParameterSensitivityFiringSignatureParityTests
{
    /// <summary>
    /// The firing signature, normalized: runs of whitespace collapsed to single spaces and the positional
    /// bind erased to <c>$?</c>. Byte-identical to the literal Darling's twin declares, and the meta-pin
    /// below compares the two.
    /// </summary>
    internal const string CanonicalFiringSignature =
        "AND min_worker_time >= 10000 " +
        "AND max_worker_time >= 250000 " +
        "AND execution_count >= 20 " +
        "AND creation_time_utc <= $? " +
        "AND max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10";

    /// <summary>
    /// Copies of the signature in Lite's analysis tree. Darling's twin declares this same number as its
    /// <c>LiteSignatureCopies</c> and the meta-pin below reads it back, so neither side can be raised
    /// alone.
    /// </summary>
    internal const int ExpectedSignatureCopies = 3;

    /// <summary>Darling's guard, read as source because no test project references both SKUs.</summary>
    private const string DarlingGuard = "Darling/Darling.Tests/ParameterSensitivityFiringSignatureParityTests.cs";

    /// <summary>This file, read as source so the meta-pin's parser can be self-validated.</summary>
    private const string ThisGuard = "Lite.Tests/ParameterSensitivityFiringSignatureParityTests.cs";

    /// <summary>
    /// The signature, per Lite file — a floor and a ceiling. Written out rather than globbed so that
    /// deleting a read, or moving one to a new file, fails here and has to be re-declared instead of
    /// quietly reducing this guard's coverage to whatever is left.
    /// </summary>
    private static readonly (string RelativePath, int Copies)[] ExpectedCopies =
    [
        ("Lite/Analysis/DuckDbFactCollector.QueryPerf.cs", 1),
        ("Lite/Analysis/DrillDownCollector.Queries.cs", 2),
    ];

    /// <summary>
    /// The signature as a shape: every token pinned literally EXCEPT the four numbers, which are captured
    /// so a skewed copy is still FOUND and reported as a wrong value rather than vanishing from the
    /// census. Adjacency is pinned with <c>[ \t]</c> rather than <c>\s</c> so a blank line or a reordering
    /// cannot slide past; the <c>psp_signature</c> copy sits four spaces deeper inside its subquery, which
    /// is why the indent is matched and not counted.
    /// </summary>
    private static readonly Regex FiringSignature = new(
        @"AND[ \t]+min_worker_time[ \t]*>=[ \t]*(?<minWorker>\d+)[ \t]*\r?\n"
        + @"[ \t]*AND[ \t]+max_worker_time[ \t]*>=[ \t]*(?<maxWorker>\d+)[ \t]*\r?\n"
        + @"[ \t]*AND[ \t]+execution_count[ \t]*>=[ \t]*(?<executions>\d+)[ \t]*\r?\n"
        + @"[ \t]*AND[ \t]+creation_time_utc[ \t]*<=[ \t]*\$\d+[ \t]*\r?\n"
        + @"[ \t]*AND[ \t]+max_worker_time::DOUBLE[ \t]+PRECISION[ \t]*/[ \t]*"
        + @"NULLIF\(min_worker_time,[ \t]*0\)[ \t]*>=[ \t]*(?<ratio>\d+)",
        RegexOptions.IgnoreCase);

    [Fact]
    public void EveryCopyOfTheFiringSignature_IsAccountedFor_InLitesAnalysisSql()
    {
        var total = 0;

        foreach (var (relativePath, expected) in ExpectedCopies)
        {
            var actual = FiringSignature.Matches(ReadNormalized(relativePath)).Count;
            total += actual;

            Assert.True(
                actual == expected,
                $"{relativePath} carries {actual} copy/copies of the PARAMETER_SENSITIVITY firing "
                + $"signature, expected {expected}. A read was added, removed or ported: if it decides "
                + "whether the finding fires it carries the signature verbatim, and if it does not, update "
                + "this guard AND Darling's in the same commit.");
        }

        Assert.Equal(ExpectedSignatureCopies, total);
    }

    [Fact]
    public void NoUndeclaredLiteAnalysisRead_CarriesItsOwnCopyOfTheFiringSignature()
    {
        var declared = ExpectedCopies
            .Select(e => RepoPath(e.RelativePath))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var files = LiteAnalysisSourceFiles();

        /* A floor, so a broken enumeration cannot report a clean bill of health. 24 files under
           Lite/Analysis when this was written; set well below that so ordinary growth never trips it, but
           high enough that an empty enumeration fails instead of passing. */
        Assert.True(files.Length >= 12, $"enumerated only {files.Length} files under Lite/Analysis");

        /* And the declared files must be part of what was enumerated, or the two halves of this guard are
           looking at different trees. */
        Assert.Equal(ExpectedCopies.Length, declared.Intersect(files, StringComparer.OrdinalIgnoreCase).Count());

        var strays = new List<string>();

        foreach (var path in files)
        {
            if (declared.Contains(path))
            {
                continue;
            }

            var count = FiringSignature.Matches(
                File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal)).Count;

            if (count > 0)
            {
                strays.Add($"{Path.GetFileName(path)}: {count} copy/copies");
            }
        }

        Assert.True(
            strays.Count == 0,
            "the firing signature appears in Lite analysis files this guard does not declare:"
            + Environment.NewLine + string.Join(Environment.NewLine, strays) + Environment.NewLine
            + "Add them to ExpectedCopies — and add the mirror copy to Darling's store — in the same commit.");
    }

    [Fact]
    public void EveryCopyOfTheFiringSignature_ReducesToTheCanonicalOne()
    {
        var skewed = new List<string>();
        var seen = 0;

        foreach (var (relativePath, _) in ExpectedCopies)
        {
            foreach (Match m in FiringSignature.Matches(ReadNormalized(relativePath)))
            {
                seen++;

                if (!string.Equals(Normalize(m.Value), CanonicalFiringSignature, StringComparison.Ordinal))
                {
                    skewed.Add(
                        $"{relativePath} @ char {m.Index}: min_worker_time={m.Groups["minWorker"].Value}, "
                        + $"max_worker_time={m.Groups["maxWorker"].Value}, "
                        + $"execution_count={m.Groups["executions"].Value}, "
                        + $"ratio={m.Groups["ratio"].Value}");
                }
            }
        }

        Assert.True(
            skewed.Count == 0,
            "PARAMETER_SENSITIVITY firing-signature copies disagree with the canonical one:"
            + Environment.NewLine + string.Join(Environment.NewLine, skewed) + Environment.NewLine
            + "canonical: " + CanonicalFiringSignature + Environment.NewLine
            + "The detector, the drill-down and the regressed-queries psp_signature CTE must all apply the "
            + "SAME floors, in Lite AND in Darling, or a query the co-fired flag reports is not one the "
            + "detector counts.");

        /* The census must have had something to look at: nothing found would otherwise pass this test by
           having nothing left to disagree with. */
        Assert.Equal(ExpectedSignatureCopies, seen);
    }

    /// <summary>
    /// The discriminator, pinned in BOTH directions against literals written for the purpose, and kept in
    /// this suite as well as Darling's so that each half can fail on its own rather than inheriting the
    /// other's confidence.
    /// </summary>
    [Fact]
    public void TheDiscriminator_MatchesEveryShippedForm_AndSkew_ButNotABareThreshold()
    {
        /* The two shipped layouts: the fact collector's and the drill-down's flush form on $2, and the
           psp_signature CTE's four-space-deeper form on $3, plus the LF-only spelling the committed blobs
           carry. All must MATCH and all must reduce to the canonical signature. */
        string[] shipped =
        [
            "AND   min_worker_time >= 10000\r\n"
            + "AND   max_worker_time >= 250000\r\n"
            + "AND   execution_count >= 20\r\n"
            + "AND   creation_time_utc <= $2\r\n"
            + "AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10\r\n",

            "    AND   min_worker_time >= 10000\r\n"
            + "    AND   max_worker_time >= 250000\r\n"
            + "    AND   execution_count >= 20\r\n"
            + "    AND   creation_time_utc <= $3\r\n"
            + "    AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10\r\n",

            "AND   min_worker_time >= 10000\n"
            + "AND   max_worker_time >= 250000\n"
            + "AND   execution_count >= 20\n"
            + "AND   creation_time_utc <= $2\n"
            + "AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10\n",
        ];

        foreach (var sql in shipped)
        {
            var m = FiringSignature.Match(sql);
            Assert.True(m.Success, "the discriminator missed a shipped signature form");
            Assert.Equal(CanonicalFiringSignature, Normalize(m.Value));
        }

        /* SKEW — the failure this guard exists for. Each must still MATCH, so it lands in the census and
           is reported as a wrong value, and must NOT reduce to the canonical signature. */
        (string Sql, string What)[] skews =
        [
            (shipped[0].Replace("250000", "500000", StringComparison.Ordinal), "max_worker_time raised"),
            (shipped[0].Replace(">= 10000", ">= 20000", StringComparison.Ordinal), "min_worker_time raised"),
            (shipped[0].Replace(">= 20\r\n", ">= 50\r\n", StringComparison.Ordinal), "execution_count raised"),
            (shipped[0].Replace(") >= 10", ") >= 5", StringComparison.Ordinal), "worker ratio lowered"),
        ];

        foreach (var (sql, what) in skews)
        {
            var m = FiringSignature.Match(sql);
            Assert.True(
                m.Success,
                $"the discriminator stopped matching a skewed copy ({what}) — it would be reported as a "
                + "missing copy rather than a skewed floor");
            Assert.NotEqual(CanonicalFiringSignature, Normalize(m.Value));
        }

        Assert.False(
            FiringSignature.IsMatch(
                shipped[0].Replace("AND   execution_count >= 20\r\n", "", StringComparison.Ordinal)),
            "the discriminator matched a signature with execution_count dropped");

        /* BARE NUMBERS — real lines from this corpus carrying the same values in unrelated predicates. */
        string[] benign =
        [
            "        AND COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) > 10000)",
            "    WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000)",
            "        AND COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) > 1000)",
            "    min_worker_time,\r\n    max_worker_time,\r\n    min_grant_kb,",
            "    max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) AS worker_ratio,",
            "HAVING SUM(execs) >= 25",
        ];

        foreach (var sql in benign)
        {
            Assert.False(FiringSignature.IsMatch(sql), $"the discriminator dragged in a benign form: {sql}");
        }
    }

    /// <summary>
    /// Darling's twin must exist, pin the SAME canonical signature, and declare the same Lite-side count
    /// this file declares for itself. Retuning a floor or moving a copy on one side of the SKU boundary
    /// without the other is what this fails on — and because the guards, not just the SQL, are compared,
    /// deleting or softening one while the other keeps claiming parity fails too.
    /// </summary>
    [Fact]
    public void DarlingsTwinGuard_PinsTheSameCanonicalSignature_SoNeitherSideCanFallBehind()
    {
        /* Self-validate the parser against THIS file first: a parser that reads nothing would otherwise
           "agree" with Darling by returning the same nothing from both sides. */
        Assert.Equal(CanonicalFiringSignature, ParseCanonicalFiringSignature(ReadNormalized(ThisGuard), ThisGuard));

        var darling = ReadNormalized(DarlingGuard);

        Assert.Equal(CanonicalFiringSignature, ParseCanonicalFiringSignature(darling, DarlingGuard));

        var liteSide = Regex.Match(darling, @"LiteSignatureCopies\s*=\s*(?<n>\d+)\s*;");
        Assert.True(liteSide.Success, $"{DarlingGuard} no longer declares LiteSignatureCopies.");
        Assert.Equal(
            ExpectedSignatureCopies,
            int.Parse(liteSide.Groups["n"].Value, System.Globalization.CultureInfo.InvariantCulture));

        /* And it must still declare its own side, and still enumerate its files rather than globbing, for
           the same reason this one does. */
        Assert.Matches(@"DarlingSignatureCopies\s*=\s*\d+\s*;", darling);
        Assert.Contains("ExpectedCopies", darling, StringComparison.Ordinal);
    }

    /// <summary>
    /// Reconstructs a <c>CanonicalFiringSignature</c> declaration from C# source, for the half of the pair
    /// that cannot reference the other SKU's assembly.
    /// </summary>
    private static string ParseCanonicalFiringSignature(string source, string label)
    {
        var start = source.IndexOf("CanonicalFiringSignature =", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{label} no longer declares CanonicalFiringSignature.");

        var end = source.IndexOf(';', start);
        Assert.True(end > start, $"{label}'s CanonicalFiringSignature declaration is unterminated.");

        var declaration = source[start..end];

        /* The literal is deliberately escape-free on both sides, so the segment pattern below is exact
           rather than approximate. A backslash appearing here means the declaration changed shape and the
           parser is no longer reading what it thinks it is. */
        Assert.DoesNotContain("\\", declaration, StringComparison.Ordinal);

        var segments = Regex.Matches(declaration, "\"(?<s>[^\"]*)\"")
            .Select(m => m.Groups["s"].Value)
            .ToList();

        Assert.True(segments.Count > 0, $"{label}'s CanonicalFiringSignature parsed to nothing.");

        return string.Concat(segments);
    }

    /// <summary>
    /// Collapses runs of whitespace to single spaces and erases the positional bind ordinal. The bind
    /// differs by read — <c>$2</c> where the window start is the second parameter, <c>$3</c> in the
    /// regressed-queries read where it is the third — and that is a parameter layout, not a floor.
    /// </summary>
    private static string Normalize(string block) =>
        Regex.Replace(Regex.Replace(block, @"\$\d+", _ => "$?"), @"\s+", " ").Trim();

    /// <summary>
    /// Reads a repo-relative source file with line endings collapsed to LF. <c>.gitattributes</c> checks
    /// the working tree out CRLF while the committed blobs are LF, and the signature is the same either
    /// way, so normalizing here keeps the regex from having to care which one it is looking at.
    /// </summary>
    private static string ReadNormalized(string relativePath) =>
        ParitySource.ReadFile(relativePath).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>Resolves a repo-relative path the same way <see cref="ParitySource.ReadFile"/> does.</summary>
    private static string RepoPath(string relativePath) =>
        Path.Combine(ParitySource.RepoRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>
    /// Every <c>.cs</c> file under Lite's analysis tree, RECURSIVELY.
    /// <see cref="ParitySource.EnumerateCsFiles"/> is top-directory-only, which would leave
    /// <c>Lite/Analysis/Recommendations</c> unscanned — and an unscanned subtree is exactly how a copy of
    /// the signature added to a new read escapes the census above.
    /// </summary>
    private static string[] LiteAnalysisSourceFiles()
    {
        var root = RepoPath("Lite/Analysis");

        return Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(p =>
                !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                && !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .ToArray();
    }
}
