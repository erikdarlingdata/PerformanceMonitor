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
/// The threshold-lineage rule for the PostgreSQL-target scorer (#3542 via #3605 ↔ #3538 A5/A7), made
/// executable before the first bar is written so it bites the content lanes rather than a later audit.
///
/// <para><b>The rule.</b> #3538 A5 recorded that the SQL Server scorer's thresholds are inherited constants
/// while <c>ServerHealthBands</c>, one assembly over, derives its deadlock tiers from 14 days of fleet data;
/// A7 that its absolute-millisecond gates do not scale with <c>hours_back</c>. The PostgreSQL detectors inherit
/// the METHOD and not the numbers: every numeric bar in a <c>PgTargetScorer*.cs</c> partial carries, on its own
/// line or within the six lines above it, exactly one of three lineage markers —</para>
/// <list type="bullet">
/// <item><description><b>measured</b>: "pNN of &lt;metric&gt; over &lt;N&gt; days × &lt;M&gt; servers of the
/// dogfood PostgreSQL fleet, &lt;date&gt;" — the V120 method;</description></item>
/// <item><description><b>engine-defined</b>: the bar IS the engine's own line (autovacuum's per-table
/// threshold, <c>max_connections − superuser_reserved_connections</c>, the alert evaluator's wraparound
/// fractions), named;</description></item>
/// <item><description><b>unmeasured</b>: "chosen, not measured — calibrate against &lt;table&gt; before the
/// next release", with the fact carrying <c>threshold_lineage = 0</c> so <c>get_analysis_facts</c> can show
/// it.</description></item>
/// </list>
/// <para>A bar is a numeric literal on a line that grades something: an <c>ApplyThresholdFormula(</c> call, a
/// comparison operator, or a <c>Boost =</c>. The trivial literals arithmetic needs (<c>0</c>, <c>1</c>, and
/// their <c>.0</c> forms) are not bars. And no PostgreSQL gate is an absolute-millisecond total: the
/// hour-in-milliseconds constant may not appear in a scorer partial unless the same line names a
/// <c>_per_hour</c> metadata key — PostgreSQL gates are rates or fractions of OBSERVED time.</para>
///
/// <para>Passes trivially on the plumbing lane's stubs (every partial returns <c>0.0</c> / <c>[]</c>), which is
/// the point: the rule exists on the tree before the first number does.</para>
/// </summary>
public sealed class PgTargetThresholdLineageTests
{
    /// <summary>A line that grades: the shared formula, a comparison, or an amplifier boost. A comparison is a
    /// space-delimited operator, so a generic type argument (<c>List&lt;Fact&gt;</c>) and a lambda arrow
    /// (<c>=&gt;</c>) are not read as one.</summary>
    private static readonly Regex s_thresholdSite = new(
        @"ApplyThresholdFormula\s*\(|\s[<>]=?\s|\bBoost\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A numeric literal that is a bar: anything but the trivial 0 / 1 (and 0.0 / 1.0 / 1.00 …).
    /// Digit separators are allowed so <c>3_600_000</c> is one literal, not three.</summary>
    private static readonly Regex s_numericLiteral = new(
        @"(?<![\w.])(\d[\d_]*(?:\.\d+)?)(?![\w.])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_lineageMarker = new(
        @"\b(measured|engine-defined|unmeasured)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private const int MarkerLookbackLines = 6;

    [Fact]
    public void EveryBarInThePgScorer_CarriesALineageMarker_WithinSixLines()
    {
        var files = ScorerPartials();
        Assert.True(files.Count >= 12, $"expected the PgTargetScorer family (dispatcher + eleven partials); found {files.Count}");

        var offenders = new List<string>();
        foreach (var file in files)
        {
            offenders.AddRange(BarsWithoutLineage(File.ReadAllText(file)).Select(line => $"{Path.GetFileName(file)}:{line.Line}  {line.Text}"));
        }

        Assert.True(
            offenders.Count == 0,
            "These PostgreSQL scorer bars carry no lineage marker (measured / engine-defined / unmeasured) within the "
            + "six lines above them. A PostgreSQL threshold inherits the SQL Server scorer's METHOD, never its "
            + "constants by value (#3538 A5); say where the number came from:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    [Fact]
    public void NoPgGate_IsAnAbsoluteMillisecondTotal()
    {
        foreach (var file in ScorerPartials())
        {
            var lines = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                var code = CSharpSourceWalker.StripCommentsAndStrings(lines[i]);
                if (!code.Contains("3_600_000", StringComparison.Ordinal) && !code.Contains("3600000", StringComparison.Ordinal))
                    continue;

                Assert.True(
                    lines[i].Contains("_per_hour", StringComparison.Ordinal),
                    $"{Path.GetFileName(file)}:{i + 1} divides or compares against an hour in milliseconds without a _per_hour key on the "
                    + "same line — PostgreSQL gates are rates or fractions of OBSERVED time, so they scale with hours_back (#3538 A7)");
            }
        }
    }

    /* ── the scanner, exercised on arranged inputs so the rule above is known to bite ── */

    [Fact]
    public void TheScanner_FlagsABareBar_AndAcceptsEachMarkerShape()
    {
        const string bare = """
            private static partial double ScoreWriteFact(Fact fact) =>
                ApplyThresholdFormula(fact.Value, 0.25, 0.5);
            """;
        Assert.Single(BarsWithoutLineage(bare));

        const string comparison = """
            if (fact.Metadata.GetValueOrDefault("ratio") >= 3.0)
                return 0.5;
            """;
        Assert.Single(BarsWithoutLineage(comparison));

        const string boost = """
            amplifiers.Add(new() { Description = "x", Boost = 0.3, Predicate = _ => true });
            """;
        Assert.Single(BarsWithoutLineage(boost));

        const string measured = """
            /* measured: p95 of requested-checkpoint share over 14 days x 9 servers of the dogfood PostgreSQL fleet, 2026-09-18 */
            return ApplyThresholdFormula(fact.Value, 0.25, 0.5);
            """;
        Assert.Empty(BarsWithoutLineage(measured));

        const string engineDefined = """
            /* engine-defined: the ceiling is max_connections - superuser_reserved_connections, read from the config facts */
            var ceiling = max - reserved;
            if (fact.Value / ceiling >= 0.9)
                return 1.0;
            """;
        Assert.Empty(BarsWithoutLineage(engineDefined));

        const string unmeasured = """
            /* unmeasured: chosen, not measured - calibrate against pg_database_stats before the next release;
               the fact carries threshold_lineage = 0 */
            fact.Metadata["threshold_lineage"] = 0;
            return ApplyThresholdFormula(fact.Value, 100, 1000);
            """;
        Assert.Empty(BarsWithoutLineage(unmeasured));
    }

    [Fact]
    public void TheScanner_IgnoresTrivialLiterals_ProseAndLiteralText_AndAMarkerTooFarAbove()
    {
        /* 0 / 1 and their decimal forms are arithmetic, not bars; a number inside a string or a comment is not code. */
        const string trivial = """
            if (fact.Value <= 0) return 0.0;
            if (ratio >= 1.0) return 1;
            /* the 80% bar lives in the amplifier arm */
            var label = "at least 3 samples";
            """;
        Assert.Empty(BarsWithoutLineage(trivial));

        /* Seven lines above is one too many: the marker must sit with the bar it explains. */
        const string tooFar = """
            /* measured: p95 over 14 days */
            //
            //
            //
            //
            //
            //
            return ApplyThresholdFormula(fact.Value, 0.25, 0.5);
            """;
        Assert.Single(BarsWithoutLineage(tooFar));

        const string sixAbove = """
            /* measured: p95 over 14 days */
            //
            //
            //
            //
            //
            return ApplyThresholdFormula(fact.Value, 0.25, 0.5);
            """;
        Assert.Empty(BarsWithoutLineage(sixAbove));

        /* A marker word used as ordinary prose somewhere else in the file does not license a bar below it. */
        const string proseElsewhere = """
            /* This family's facts are measured over the observed window. */
            var x = 1;
            var y = 2;
            var z = 3;
            var w = 4;
            var v = 5;
            var u = 6;
            var t = 7;
            return ApplyThresholdFormula(fact.Value, 0.25, 0.5);
            """;
        Assert.Single(BarsWithoutLineage(proseElsewhere));
    }

    /// <summary>
    /// Each bar-carrying line whose six-line neighbourhood (itself and the lines above) carries no marker.
    /// The bar test reads CODE (comments and literals stripped, line-aligned) so a number in prose is not a
    /// bar; the marker test reads the ORIGINAL lines, because the marker lives in a comment.
    /// </summary>
    private static List<(int Line, string Text)> BarsWithoutLineage(string source)
    {
        var normalised = source.Replace("\r\n", "\n", StringComparison.Ordinal);
        var lines = normalised.Split('\n');
        var code = CSharpSourceWalker.StripCommentsAndStrings(normalised).Split('\n');
        Assert.Equal(lines.Length, code.Length);

        var offenders = new List<(int, string)>();
        for (var i = 0; i < code.Length; i++)
        {
            if (!s_thresholdSite.IsMatch(code[i]))
                continue;
            if (!s_numericLiteral.Matches(code[i]).Any(m => !IsTrivial(m.Groups[1].Value)))
                continue;

            var from = Math.Max(0, i - MarkerLookbackLines);
            var neighbourhood = string.Join("\n", lines[from..(i + 1)]);
            if (s_lineageMarker.IsMatch(neighbourhood))
                continue;

            offenders.Add((i + 1, lines[i].Trim()));
        }

        return offenders;
    }

    private static bool IsTrivial(string literal)
    {
        var value = literal.Replace("_", string.Empty, StringComparison.Ordinal);
        return double.TryParse(value, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var d)
            && (d == 0.0 || d == 1.0);
    }

    private static List<string> ScorerPartials()
    {
        var directory = RepoFile.PathTo("PerformanceMonitor.Analysis");
        return Directory.GetFiles(directory, "PgTargetScorer*.cs").OrderBy(f => f, StringComparer.Ordinal).ToList();
    }
}
