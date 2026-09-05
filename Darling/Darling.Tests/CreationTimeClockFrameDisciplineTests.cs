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
/// #2991: no analysis SQL in either store may compare <c>query_stats.creation_time</c> against a window
/// bound without first de-skewing it by the collected <c>server_properties.utc_offset_minutes</c>.
///
/// <para><c>creation_time</c> is the monitored server's LOCAL wall clock and the window bound is naive
/// UTC, so the untranslated comparison asks a different question on every server. The behavioural pins
/// (<c>ParameterSensitivityClockFrameLiveTests</c> and Lite's
/// <c>ParameterSensitivityClockFrameTests</c>) prove the two DETECTORS and the two DRILL-DOWNS answer
/// correctly; this scan exists for the two sites they cannot cheaply reach — the
/// <c>psp_signature</c> CTE inside each SKU's regressed-queries read, which needs a whole Query Store
/// fixture before it returns a row — and for every site added after this was written.</para>
///
/// <para><b>Why a scan and not more fixtures.</b> The six sites were byte-comparable across the two
/// SKUs before this change, and the inventory that preceded it found NOTHING in either suite that
/// compares Darling's analysis SQL against Lite's: no parity guard, no shared constant, not even a
/// count. So the realistic regression is not someone breaking the arithmetic — it is someone porting a
/// read to one store and not the other, or adding a seventh site that reads correctly and skews
/// silently. That is a category, and the thing that catches a category is an invariant over the
/// corpus rather than another example.</para>
///
/// <para>Both directions are pinned. <see cref="TheDiscriminator_FlagsABareComparison_AndPassesADeSkewedOne"/>
/// runs the same two regexes over literals written for the purpose, because a scan whose discriminator
/// has quietly stopped matching reports a clean bill of health, and that is worse than no scan.</para>
/// </summary>
public sealed class CreationTimeClockFrameDisciplineTests
{
    /// <summary>
    /// A window comparison against a bare <c>creation_time</c> — the defect. Deliberately spelled
    /// against the positional bind (<c>$2</c> / <c>$3</c>) rather than any identifier, because that is
    /// what a window bound is in both dialects and it cannot be satisfied by renaming a column.
    /// </summary>
    private static readonly Regex BareComparison =
        new(@"(?<![\w.])creation_time\s*(?:<=|<|>=|>|BETWEEN)\s*\$\d", RegexOptions.IgnoreCase);

    /// <summary>
    /// The de-skew, in either dialect: PostgreSQL <c>make_interval(mins =&gt; ...)</c> or DuckDB
    /// <c>... * INTERVAL '1' MINUTE</c>. DuckDB has no <c>make_interval</c> and <c>AT TIME ZONE</c>
    /// would drag in ICU, so the two spellings are a real and permanent divergence — matching both here
    /// is what lets one guard cover both stores.
    /// </summary>
    private static readonly Regex DeSkew =
        new(@"creation_time\s*-\s*(?:make_interval\s*\(\s*mins\s*=>\s*\w+\.offset_minutes\s*\)"
            + @"|\w+\.offset_minutes\s*\*\s*INTERVAL\s*'1'\s*MINUTE)\s+AS\s+creation_time_utc",
            RegexOptions.IgnoreCase);

    /// <summary>
    /// The de-skew sites, per file, as of #2991 — a floor AND a ceiling. Naming the count per file is
    /// what makes a site silently dropped from one SKU fail here: a bare total would let a Darling site
    /// move to Lite and still add up.
    /// </summary>
    private static readonly (string RelativePath, int Sites)[] ExpectedSites =
    [
        ("Darling/PerformanceMonitor.Darling.Analysis/PgFactCollector.QueryPerf.cs", 1),
        ("Darling/PerformanceMonitor.Darling.Analysis/PgDrillDownCollector.Queries.cs", 2),
        ("Lite/Analysis/DuckDbFactCollector.QueryPerf.cs", 1),
        ("Lite/Analysis/DrillDownCollector.Queries.cs", 2),
    ];

    [Fact]
    public void NoAnalysisSql_ComparesABareCreationTime_AgainstAWindowBound()
    {
        var offenders = new List<string>();
        var scanned = 0;

        foreach (var path in AnalysisSourceFiles())
        {
            scanned++;
            var text = File.ReadAllText(path);

            foreach (Match m in BareComparison.Matches(text))
            {
                offenders.Add($"{Path.GetFileName(path)}: {m.Value.Trim()}");
            }
        }

        /* A floor, so a broken glob cannot report a clean bill of health. Measured at 40 files across
           the two analysis projects when this was written; set well below that so ordinary growth never
           trips it, but high enough that an empty enumeration fails instead of passing. */
        Assert.True(scanned >= 20, $"scanned only {scanned} analysis source files — check the globs below");

        Assert.True(
            offenders.Count == 0,
            "analysis SQL compares a bare creation_time against a window bound:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders) + Environment.NewLine
            + "creation_time is the monitored server's LOCAL wall clock and the bound is naive UTC. "
            + "De-skew it by the collected server_properties.utc_offset_minutes first — Postgres "
            + "make_interval(mins => svr.offset_minutes), DuckDB svr.offset_minutes * INTERVAL '1' "
            + "MINUTE — and compare the resulting creation_time_utc.");
    }

    [Fact]
    public void EveryKnownDeSkewSite_IsStillThere_InBothStores()
    {
        foreach (var (relativePath, expected) in ExpectedSites)
        {
            var path = RepoPath(relativePath);
            Assert.True(File.Exists(path), $"{relativePath} is gone — update this guard deliberately");

            var found = DeSkew.Matches(File.ReadAllText(path)).Count;

            Assert.True(
                found == expected,
                $"{relativePath} carries {found} creation_time de-skew site(s), expected {expected}. "
                + "A read was added, removed or ported: if it windows on creation_time it needs the "
                + "de-skew, and if it does not, update this guard in the same commit. The two SKUs are "
                + "ports of one another and nothing else in either suite compares their SQL, so a "
                + "one-sided change is otherwise silent.");
        }
    }

    /// <summary>
    /// The discriminator, pinned in BOTH directions against literals written for the purpose. Each
    /// hazard is a form that actually shipped in this repo; each benign form is one the corpus contains
    /// and must not be dragged in with them.
    /// </summary>
    [Fact]
    public void TheDiscriminator_FlagsABareComparison_AndPassesADeSkewedOne()
    {
        string[] hazards =
        [
            /* The two spellings that shipped, one per SKU's parameter layout. */
            "AND   creation_time <= $2",
            "    AND   creation_time <= $3",
            /* Qualified, and the other operators. */
            "AND creation_time >= $2",
            "AND creation_time BETWEEN $2 AND $3",
        ];

        foreach (var sql in hazards)
        {
            Assert.True(BareComparison.IsMatch(sql), $"discriminator missed the hazard: {sql}");
        }

        string[] benign =
        [
            /* The fix, in each dialect. */
            "AND   creation_time_utc <= $2",
            "creation_time - make_interval(mins => svr.offset_minutes) AS creation_time_utc,",
            "creation_time - svr.offset_minutes * INTERVAL '1' MINUTE AS creation_time_utc,",
            /* A projection, and a comparison against another COLUMN rather than a window bound. */
            "        creation_time,",
            "MAX(creation_time) AS creation_time,",
            "AND creation_time <= last_execution_time",
            /* The suffix guard: a different column that merely ends in creation_time. */
            "AND plan_creation_time <= $2",
        ];

        foreach (var sql in benign)
        {
            Assert.False(BareComparison.IsMatch(sql), $"discriminator dragged in a benign form: {sql}");
        }

        /* And the de-skew regex must actually recognise both dialects, or the site census above passes
           by finding nothing rather than by the sites being present. */
        Assert.True(DeSkew.IsMatch("creation_time - make_interval(mins => svr.offset_minutes) AS creation_time_utc,"));
        Assert.True(DeSkew.IsMatch("creation_time - svr.offset_minutes * INTERVAL '1' MINUTE AS creation_time_utc,"));
        Assert.False(DeSkew.IsMatch("        creation_time,"));
    }

    /* ── Source location. Resolved by walking up from this file, the way the sibling scans do. ── */

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
