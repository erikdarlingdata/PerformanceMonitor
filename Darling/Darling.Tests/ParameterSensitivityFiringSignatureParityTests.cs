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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Cross-SKU parity guard for the PARAMETER_SENSITIVITY firing signature: the four floors that decide
/// whether the finding fires, written out SIX times across two stores and otherwise held equal by
/// nothing.
///
/// <para><b>What the signature is.</b> Three floors and a ratio, applied as one contiguous predicate
/// block: <c>min_worker_time &gt;= 10000</c>, <c>max_worker_time &gt;= 250000</c>,
/// <c>execution_count &gt;= 20</c>, the compiled-before-the-window test on the de-skewed
/// <c>creation_time_utc</c>, and a max-over-min worker time of at least 10. It appears once in each
/// store's fact collector (the read that emits the fact), once in each store's parameter-sensitivity
/// drill-down, and once inside the <c>psp_signature</c> CTE of each store's regressed-queries read.
/// Three copies per SKU, six in total.</para>
///
/// <para><b>Why a guard.</b> <c>PgDrillDownCollector.RegressedQueriesSql</c>'s <c>psp_signature</c> CTE
/// states the contract in its own comment: reusing the detector's own thresholds is what keeps the
/// co-fired flag honest, so that a query flagged as parameter-sensitive there IS one the detector counts
/// when it fires and never a looser lookalike. That contract spans four files and two dialects, and
/// <c>PgFactCollectorTests.ImplementsTheSharedSeam_WithLitesCollectMethodSurface</c> and
/// <c>DarlingAnalysisPipelineTests.DrillDown_CarriesLitesCollectMethodSurface</c> pin METHOD NAMES only.
/// A floor raised on one side, or a copy ported to a new read and left at a different number, satisfies
/// every existing pin. The two SKUs are deliberate ports of one another and the whole corpus of analysis
/// SQL agrees on these four numbers; an invariant over the corpus is what keeps it that way.</para>
///
/// <para><b>Scope: floors, not text.</b> The two stores' SQL diverges legitimately and permanently, so a
/// text comparison of the reads would be useless. PostgreSQL spells the clock de-skew
/// <c>make_interval(mins =&gt; ...)</c> where DuckDB has no such function and must multiply by
/// <c>INTERVAL '1' MINUTE</c> (<c>AT TIME ZONE</c> there needs the ICU extension). Darling reads
/// <c>server_properties</c> directly because its store has no <c>v_</c> view, where Lite must read
/// <c>v_server_properties</c> — its <c>v_</c> views union the live tables with archived Parquet, and
/// <c>Lite.Tests.AnalysisDataSpanTests.AnalysisPipeline_NeverReadsAnArchivableTableRaw</c> positively
/// REQUIRES the <c>v_</c> form under Lite's analysis tree. <c>QUALIFY</c>, <c>read_parquet</c>,
/// <c>UNION ALL BY NAME</c>, <c>DISTINCT ON</c> and <c>any_value</c> are each available on one side only.
/// This guard therefore asserts on the firing signature alone, which is dialect-free.</para>
///
/// <para><b>Matched in predicate context, never as a bare number.</b> The threshold VALUES are not
/// searchable on their own: <c>10000</c> also appears in <c>PgBaselineProvider</c>'s restart-exclusion
/// predicate (<c>COALESCE(LAG(total_wait_ms) OVER (...), 0) &gt; 10000</c>, and again in the
/// <c>prior_total_wait_ms</c> form the PostgreSQL rewrite of Lite's <c>QUALIFY</c> produces) and in
/// Lite's <c>BaselineProvider</c> likewise. A guard keyed on the number would fail on the day it is
/// written. <see cref="TheDiscriminator_MatchesEveryShippedForm_AndSkew_ButNotABareThreshold"/> holds
/// those exact corpus lines as negative controls, and the shipped forms as positive ones.</para>
///
/// <para><b>Where the halves live.</b> This file does the cross-store work because <c>build.yml</c>'s
/// <c>darling</c> path filter covers the whole Darling tree AND every Lite <c>.cs</c> file, so it runs on
/// an edit to EITHER analysis tree. Lite's twin,
/// <c>Lite.Tests.ParameterSensitivityFiringSignatureParityTests</c>, declares Lite's own half and
/// meta-pins <see cref="LiteSignatureCopies"/> and <see cref="CanonicalFiringSignature"/> out of this
/// file, so raising a number in one guard without the other fails. Its filter, <c>lite</c>, covers
/// Darling's test tree — exactly the edits that could weaken this file.</para>
/// </summary>
public sealed class ParameterSensitivityFiringSignatureParityTests
{
    /// <summary>
    /// The firing signature, normalized: runs of whitespace collapsed to single spaces and the positional
    /// bind erased to <c>$?</c>. Every copy in either store reduces to exactly this, which is the parity
    /// claim in one line. Lite's twin declares the identical literal and its meta-pin compares the two, so
    /// the floors cannot be retuned on one side alone.
    ///
    /// <para>Written as a concatenation of plain string segments, none containing a quote or a backslash,
    /// because Lite's meta-pin reconstructs it by reading THIS FILE as source — no test project
    /// references both SKUs' assemblies. That parser is self-validated against an in-memory copy of the
    /// same literal on both sides, so a parser bug fails loudly instead of green-washing drift.</para>
    /// </summary>
    internal const string CanonicalFiringSignature =
        "AND min_worker_time >= 10000 " +
        "AND max_worker_time >= 250000 " +
        "AND execution_count >= 20 " +
        "AND creation_time_utc <= $? " +
        "AND max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10";

    /// <summary>Copies of the signature in Darling's analysis tree.</summary>
    internal const int DarlingSignatureCopies = 3;

    /// <summary>
    /// Copies of the signature in LITE's analysis tree. Lite's twin declares the same number for its own
    /// half and reads this constant back out of this file, so neither side can be raised alone.
    /// </summary>
    internal const int LiteSignatureCopies = 3;

    /// <summary>
    /// The signature, per file, across BOTH stores — a floor and a ceiling. Naming the count per file is
    /// what makes a copy silently dropped from one SKU fail: a bare total would let a Darling copy move to
    /// Lite and still add up.
    /// </summary>
    private static readonly (string RelativePath, int Copies)[] ExpectedCopies =
    [
        ("Darling/PerformanceMonitor.Darling.Analysis/PgFactCollector.QueryPerf.cs", 1),
        ("Darling/PerformanceMonitor.Darling.Analysis/PgDrillDownCollector.Queries.cs", 2),
        ("Lite/Analysis/DuckDbFactCollector.QueryPerf.cs", 1),
        ("Lite/Analysis/DrillDownCollector.Queries.cs", 2),
    ];

    /// <summary>
    /// The signature as a shape: every token pinned literally EXCEPT the four numbers, which are captured
    /// so a skewed copy is still FOUND and then reported as a wrong value rather than vanishing from the
    /// census. Adjacency is pinned too — one newline and leading horizontal whitespace between
    /// predicates, matched with <c>[ \t]</c> rather than <c>\s</c> so a blank line or a reordering cannot
    /// slide past. The <c>psp_signature</c> copy sits four spaces deeper inside its subquery, which is why
    /// the indent is matched and not counted.
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
    public void EveryCopyOfTheFiringSignature_IsAccountedFor_InBothStores()
    {
        foreach (var (relativePath, expected) in ExpectedCopies)
        {
            var path = RepoPath(relativePath);
            Assert.True(File.Exists(path), $"{relativePath} is gone — update this guard deliberately");

            var actual = FiringSignature.Matches(ReadNormalizedSource(path)).Count;

            Assert.True(
                actual == expected,
                $"{relativePath} carries {actual} copy/copies of the PARAMETER_SENSITIVITY firing "
                + $"signature, expected {expected}. A read was added, removed or ported: if it decides "
                + "whether the finding fires it carries the signature verbatim, and if it does not, "
                + "update this guard in the same commit. The two SKUs are ports of one another, so a "
                + "one-sided change is otherwise silent.");
        }
    }

    [Fact]
    public void NoUndeclaredAnalysisRead_CarriesItsOwnCopyOfTheFiringSignature()
    {
        var declared = ExpectedCopies
            .Select(e => RepoPath(e.RelativePath))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var scanned = 0;
        var strays = new List<string>();

        foreach (var path in AnalysisSourceFiles())
        {
            scanned++;

            if (declared.Contains(path))
            {
                continue;
            }

            var count = FiringSignature.Matches(ReadNormalizedSource(path)).Count;
            if (count > 0)
            {
                strays.Add($"{Path.GetFileName(path)}: {count} copy/copies");
            }
        }

        /* A floor, so a broken glob cannot report a clean bill of health. 44 files across the two
           analysis projects when this was written; set well below that so ordinary growth never trips it,
           but high enough that an empty enumeration fails instead of passing. */
        Assert.True(scanned >= 20, $"scanned only {scanned} analysis source files — check the globs below");

        /* And the declared files must be part of what was scanned, or the two halves of this guard are
           looking at different trees. */
        Assert.Equal(ExpectedCopies.Length, declared.Count);

        Assert.True(
            strays.Count == 0,
            "the firing signature appears in analysis files this guard does not declare:"
            + Environment.NewLine + string.Join(Environment.NewLine, strays) + Environment.NewLine
            + "Add them to ExpectedCopies — and add the mirror copy to the other store — in the same commit.");
    }

    [Fact]
    public void EveryCopyOfTheFiringSignature_ReducesToTheCanonicalOne_InBothStores()
    {
        var perStore = new Dictionary<string, int>(StringComparer.Ordinal) { ["Darling"] = 0, ["Lite"] = 0 };
        var skewed = new List<string>();

        foreach (var (relativePath, _) in ExpectedCopies)
        {
            var store = relativePath.StartsWith("Lite/", StringComparison.Ordinal) ? "Lite" : "Darling";

            foreach (Match m in FiringSignature.Matches(ReadNormalizedSource(RepoPath(relativePath))))
            {
                perStore[store]++;

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
            + "SAME floors in BOTH stores, or a query the co-fired flag reports is not one the detector "
            + "counts. Retuning them means editing all six copies and both guards' canonical literal in "
            + "one commit.");

        /* Both stores must actually have contributed. A store whose copies all disappeared would
           otherwise pass this test by having nothing left to disagree with. */
        Assert.Equal(DarlingSignatureCopies, perStore["Darling"]);
        Assert.Equal(LiteSignatureCopies, perStore["Lite"]);
    }

    /// <summary>
    /// The discriminator, pinned in BOTH directions against literals written for the purpose. A scan whose
    /// pattern has quietly stopped matching reports a clean bill of health, which is worse than no scan —
    /// so every negative claim above is backed by a positive control through the identical regex.
    /// </summary>
    [Fact]
    public void TheDiscriminator_MatchesEveryShippedForm_AndSkew_ButNotABareThreshold()
    {
        /* The two shipped layouts: the fact collector's and the drill-down's flush form on $2, and the
           psp_signature CTE's four-space-deeper form on $3. Both must MATCH and both must reduce to the
           canonical signature. */
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

            /* And the same, LF-only: the committed blobs are pure LF while the working tree is CRLF, so
               both endings have to reduce to one answer. */
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
           is reported as a wrong value, and must NOT reduce to the canonical signature. A pattern so
           tight that a skewed copy stopped matching would fail the COUNT instead, which names the wrong
           defect. */
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

        /* A predicate DROPPED from the middle breaks adjacency, so the copy leaves the census. That is the
           right answer for a signature that is no longer the signature. */
        Assert.False(
            FiringSignature.IsMatch(
                shipped[0].Replace("AND   execution_count >= 20\r\n", "", StringComparison.Ordinal)),
            "the discriminator matched a signature with execution_count dropped");

        /* BARE NUMBERS — real lines from this corpus carrying the same values in unrelated predicates.
           Matching a threshold as a number rather than in predicate context would flag every one. */
        string[] benign =
        [
            /* PgBaselineProvider and Lite's BaselineProvider, the wait-stats restart exclusion, in both
               the QUALIFY form and the WHERE form the PostgreSQL rewrite produces. */
            "        AND COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) > 10000)",
            "    WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000)",
            "        AND COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) > 1000)",
            /* The signature's own columns, projected rather than filtered. */
            "    min_worker_time,\r\n    max_worker_time,\r\n    min_grant_kb,",
            "    max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) AS worker_ratio,",
            /* A regressed-queries floor that is NOT part of the signature and must not be dragged in. */
            "HAVING SUM(execs) >= 25",
        ];

        foreach (var sql in benign)
        {
            Assert.False(FiringSignature.IsMatch(sql), $"the discriminator dragged in a benign form: {sql}");
        }
    }

    /// <summary>
    /// Lite's twin must exist and pin the SAME canonical signature and the same Lite-side count. Deleting
    /// or softening one guard while the other keeps claiming parity is the drift this pair exists to
    /// prevent, and <c>build.yml</c>'s <c>lite</c> filter covers Darling's test tree, so the mirror of
    /// this assertion runs on any edit to this file.
    /// </summary>
    [Fact]
    public void LitesTwinGuard_PinsTheSameCanonicalSignature_AndTheSameLiteSideCount()
    {
        const string twin = "Lite.Tests/ParameterSensitivityFiringSignatureParityTests.cs";
        var path = RepoPath(twin);
        Assert.True(File.Exists(path), $"Lite's counterpart guard is missing: {twin}");

        var source = ReadNormalizedSource(path);

        Assert.Equal(CanonicalFiringSignature, ParseCanonicalFiringSignature(source, twin));

        var declared = Regex.Match(source, @"ExpectedSignatureCopies\s*=\s*(?<n>\d+)\s*;");
        Assert.True(declared.Success, $"{twin} no longer declares ExpectedSignatureCopies.");
        Assert.Equal(
            LiteSignatureCopies,
            int.Parse(declared.Groups["n"].Value, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The source parser must read THIS file's own constant back correctly, or it proves nothing about
    /// Lite's. Lite's twin runs the same self-check against its own copy.
    /// </summary>
    [Fact]
    public void TheCanonicalSignatureParser_RoundTripsThisGuardsOwnDeclaration()
    {
        var thisFile = ThisFilePath();

        Assert.Equal(
            CanonicalFiringSignature,
            ParseCanonicalFiringSignature(ReadNormalizedSource(thisFile), thisFile));
    }

    /// <summary>
    /// Reconstructs a <c>CanonicalFiringSignature</c> declaration from C# source, for the half of the pair
    /// that cannot reference the other SKU's assembly.
    /// </summary>
    internal static string ParseCanonicalFiringSignature(string source, string label)
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
    /// Reads source with line endings collapsed to LF. <c>.gitattributes</c> checks the working tree out
    /// CRLF while the committed blobs are LF, and the signature is the same either way, so normalizing
    /// here keeps the regex from having to care which one it is looking at.
    /// </summary>
    private static string ReadNormalizedSource(string path) =>
        File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal);

    /* ── Source location. Resolved by walking up from this file, the way the sibling scans do. ── */

    private static string ThisFilePath([CallerFilePath] string thisFile = "") => thisFile;

    private static IEnumerable<string> AnalysisSourceFiles([CallerFilePath] string thisFile = "")
    {
        foreach (var root in new[]
        {
            RepoPath("Darling/PerformanceMonitor.Darling.Analysis", thisFile),
            RepoPath("Lite/Analysis", thisFile),
        })
        {
            foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return path;
            }
        }
    }

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "")
    {
        /* This file lives at <repo>/Darling/Darling.Tests/, so the repo root is two levels up. */
        var repoRoot = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
        return Path.GetFullPath(Path.Combine(repoRoot, relative.Replace('/', Path.DirectorySeparatorChar)));
    }
}
