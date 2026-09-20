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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #3691 lineage flips, pinned to their evidence: every PostgreSQL bar the fleet calibrations measured (the
/// 2026-09-19 read of levels and the 2026-09-20 read of hour-of-week ratios, per-event wait fractions and reads per
/// quarter-hour) now carries a <c>measured</c> comment that names ITS calibration's DATE and the POPULATION, the
/// bars the calibrations could not measure still say <c>unmeasured</c>, and each flipped scorer stamps the metadata
/// flag that agrees with its comments.
///
/// <para><b>Why a pin on prose.</b> <see cref="PgTargetThresholdLineageTests"/> accepts any of the three marker
/// words within six lines of a bar; it cannot tell a measured comment that cites its read from one that merely
/// says the word. The flip's whole value is the citation — a future change that moves a constant and keeps
/// "measured: … 2026-09-19" above it would be presenting a stale read as the number's lineage, exactly the lie
/// #3538 A5 names. So this pins, per constant, that the comment block immediately above it (a) is the measured
/// shape, (b) cites <c>2026-09-19</c>, and (c) names the population (<c>Aurora PostgreSQL clusters</c>). Moving
/// a bar then means re-reading the fleet or rewriting the comment as unmeasured, and either is honest.</para>
///
/// <para><b>What stays unmeasured, on purpose.</b> The sampling floor and the sampled profile's bar (population 0
/// on the fleet as of 2026-09-20 — no cluster runs <c>pg_wait_sampling</c>), the session-count floors (read as a
/// fraction of the ceiling, not as counts), and every co-fire boost. Those are pinned to still say so, because the
/// flag contract — 0 = at least one chosen bar decided, 1 = every bar that decided is measured or engine-defined —
/// depends on the unmeasured ones not quietly gaining the word. The round-2 flips (the wait standout bars, read per
/// event; the I/O admission floor, read per quarter-hour; the ratio multiple, read per family) cite 2026-09-20, and
/// a constant's entry below names the date its block must cite — a round-1 citation above a round-2 bar is a stale
/// read presented as the number's lineage.</para>
///
/// <para>Reads the LF-normalised source (<see cref="RepoFile.ReadRepoFileLf"/>) because a comment block spans
/// lines and is collapsed to one string before its citation is matched; declared in
/// <c>RepoFileAdoptionTests.s_lfReaders</c> for that reason.</para>
/// </summary>
public sealed class PgTargetMeasuredLineageTests
{
    private const string CalibrationDate = "2026-09-19";
    /// <summary>The second read (batch C): hour-of-week ratios, per-event wait fractions, reads per 15-minute bucket.</summary>
    private const string SecondCalibrationDate = "2026-09-20";
    private const string Population = "Aurora PostgreSQL clusters";

    private static readonly Regex s_constDeclaration = new(
        @"^\s*public\s+(?:const|static\s+readonly)\s+\w+\s+(\w+)\s*=", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The bars the calibrations measured, by file, constant and the DATE of the read that placed them — the
    /// flip lists of the two STEP briefs, verbatim. The wait rollup bars, the wait standout bars and the CPU bars
    /// are declared as pairs under one comment; the walk-up below skips a sibling declaration so each member of a
    /// pair reads the shared block.</summary>
    private static readonly (string File, string Date, string[] Constants)[] s_measured =
    {
        ("PgTargetScorer.Temp.cs", CalibrationDate, new[] { "TempSpillConcerningBytesPerSec", "TempSpillCriticalBytesPerSec" }),
        ("PgTargetScorer.Vacuum.cs", CalibrationDate, new[] { "BacklogPersistenceSamples", "BacklogCriticalRatio", "BacklogManyTables" }),
        ("PgTargetScorer.Database.cs", CalibrationDate, new[] { "DeadlockWarnPerHour", "DeadlockCriticalPerHour" }),
        ("PgTargetScorer.Queries.cs", CalibrationDate, new[] { "BadActorShareConcerning", "BadActorShareCritical", "BadActorBusyFloor" }),
        ("PgTargetScorer.Waits.cs", CalibrationDate, new[] { "WaitRollupConcerning", "WaitRollupCritical", "WaitIoConcerning", "WaitIoCritical" }),
        /* Round 2 (#3691, batch C §C3): the per-EVENT read placed the standout bars — ≈ p99.6 / p99.99 of non-IO event buckets. */
        ("PgTargetScorer.Waits.cs", SecondCalibrationDate, new[] { "WaitStandoutConcerning", "WaitStandoutCritical" }),
        ("PgTargetScorer.Cpu.cs", CalibrationDate, new[] { "CpuCapacityWarningPercent", "CpuCapacityCriticalPercent" }),
        /* Lane 14 (#3691) added the idle-in-transaction duration bars, measured as the §B4 empty interval. */
        ("PgTargetScorer.Sessions.cs", CalibrationDate, new[] { "ConnectionSaturationWarning", "ConnectionSaturationCritical", "IdleInTransactionShareBar", "IdleInTransactionWarningMs", "IdleInTransactionCriticalMs" }),
        /* Round 2 (§C5): the quarter-hour reads floor, measured as an ADMISSION floor (57 % of fleet buckets under it). */
        ("PgTargetScorer.Io.cs", SecondCalibrationDate, new[] { "IoBaselineBucketMinimumReads" }),
        ("Baselines/AnomalyThresholds.cs", CalibrationDate, new[] { "PgTpsFloor", "PgTpsFallback", "PgCpuFloorPct", "PgCpuFallbackPct", "PgWaitProfileFallbackMsPerSec", "PgDeadlockRateFloorPerHour" }),
        /* Round 2 (§C2): the ratio multiple, read per family — the verdict differs by family and the comment says each. */
        ("Baselines/AnomalyThresholds.cs", SecondCalibrationDate, new[] { "PgRatioAnomalyThreshold" }),
    };

    /// <summary>The bars the calibrations did NOT measure, which must still say so.</summary>
    private static readonly (string File, string[] Constants)[] s_stillUnmeasured =
    {
        ("PgTargetScorer.Waits.cs", new[] { "WaitSampledMinimumSamples", "WaitCoFireBoost" }),
        ("PgTargetScorer.Temp.cs", new[] { "TempCauseBoost" }),
        ("PgTargetScorer.Queries.cs", new[] { "BadActorCoFireBoost" }),
        /* Lane 18 (#3691) added the offered-vs-delivered co-fire boost — a boost, unmeasured like its siblings. */
        ("PgTargetScorer.Sessions.cs", new[] { "ConnectionSaturationCoFireBoost", "IdleInTransactionRecurrenceCaptures", "IdleInTransactionRecurrenceBoost", "OfferedVsDeliveredBoost" }),
        /* Lane 24 (#3691): the stock SAMPLED profile's bar — the calibration read Aurora's exact deltas, not pg_wait_sampling. */
        ("Baselines/AnomalyThresholds.cs", new[] { "PgSessionCountFloor", "PgSessionCountFallback", "PgSampledWaitProfileFallbackMsPerSec" }),
        /* The ratio families' ramp spans: the 2026-09-20 read placed the firing multiple, not where a ramp should top out. */
        ("PgTargetScorer.Anomaly.cs", new[] { "RatioAnomalySaturation", "WaitProfileModifiedZSpan" }),
    };

    /// <summary>The sampled-wait bars (#3765 / lane 24) must state the reason they are STILL unmeasured after round 2:
    /// the fleet has no <c>pg_wait_sampling</c> population to read (§C6), and the note keeps anyone from re-reading an
    /// empty table for them.</summary>
    private static readonly (string File, string Constant)[] s_sampledPopulationZero =
    {
        ("PgTargetScorer.Waits.cs", "WaitSampledMinimumSamples"),
        ("Baselines/AnomalyThresholds.cs", "PgSampledWaitProfileFallbackMsPerSec"),
    };

    [Fact]
    public void EveryMeasuredBar_CitesTheCalibrationDateAndPopulation_InTheCommentAboveIt()
    {
        var offenders = new List<string>();
        foreach (var (file, date, constants) in s_measured)
        {
            var lines = Source(file);
            foreach (var name in constants)
            {
                var block = CommentBlockAbove(lines, name, file);
                if (!block.Contains("measured", StringComparison.Ordinal) || block.Contains("unmeasured:", StringComparison.Ordinal))
                    offenders.Add($"{file}::{name} is not marked measured (or still carries the unmeasured marker)");
                if (!block.Contains(date, StringComparison.Ordinal))
                    offenders.Add($"{file}::{name} does not cite the calibration date {date}");
                if (!block.Contains(Population, StringComparison.Ordinal))
                    offenders.Add($"{file}::{name} does not name the measured population ({Population})");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "A bar a fleet calibration measured must say so above its declaration, with that read's date and "
            + "the population — a measured comment without its read is the lie #3538 A5 names, and a moved bar under "
            + "a stale citation is the same lie one release later:" + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    [Fact]
    public void TheBarsTheCalibrationCouldNotRead_StillSayUnmeasured()
    {
        var offenders = new List<string>();
        foreach (var (file, constants) in s_stillUnmeasured)
        {
            var lines = Source(file);
            foreach (var name in constants)
            {
                var block = CommentBlockAbove(lines, name, file);
                if (!block.Contains("unmeasured", StringComparison.OrdinalIgnoreCase))
                    offenders.Add($"{file}::{name} no longer says unmeasured — if it was read, add it to s_measured with its citation");
            }
        }

        Assert.True(offenders.Count == 0, string.Join(Environment.NewLine, offenders));

        foreach (var (file, name) in s_sampledPopulationZero)
        {
            var block = CommentBlockAbove(Source(file), name, file);
            Assert.Contains("Population 0 on the dogfood fleet as of " + SecondCalibrationDate, block, StringComparison.Ordinal);
            Assert.Contains("pg_wait_sampling", block, StringComparison.Ordinal);
        }
    }

    /// <summary>The flag agrees with the comments file by file: a scorer whose every graded bar is now measured or
    /// engine-defined stamps 1 and never 0; the wait scorer stamps by POPULATION (Aurora's measured deltas vs the
    /// stock sampled estimate) on both its exits, now that the standout bars are measured too, and never a bare 0;
    /// the checkpoint and buffer scorers (not flipped — not applicable on Aurora / not measured) and the ratio-anomaly
    /// scorer (its ramp spans are chosen) still stamp 0 and never 1.</summary>
    [Fact]
    public void TheMetadataFlag_AgreesWithTheCommentsFileByFile()
    {
        foreach (var file in new[] { "PgTargetScorer.Temp.cs", "PgTargetScorer.Vacuum.cs", "PgTargetScorer.Database.cs", "PgTargetScorer.Queries.cs", "PgTargetScorer.Cpu.cs", "PgTargetScorer.Sessions.cs" })
        {
            var source = string.Join("\n", Source(file));
            Assert.Contains("[\"threshold_lineage\"] = 1;", source, StringComparison.Ordinal);
            Assert.DoesNotContain("[\"threshold_lineage\"] = 0;", source, StringComparison.Ordinal);
        }

        var waits = string.Join("\n", Source("PgTargetScorer.Waits.cs"));
        /* Both exits — the yielded rollup and the fraction ramp — stamp the population verdict; a bare 0 on the yield
           path would be the round-1 "decided on the unmeasured standout bar" reading, which §C3 retired. */
        Assert.Equal(2, Regex.Matches(waits, Regex.Escape("[\"threshold_lineage\"] = isMeasuredPopulation ? 1 : 0;")).Count);
        Assert.DoesNotContain("[\"threshold_lineage\"] = 0;", waits, StringComparison.Ordinal);
        Assert.DoesNotContain("[\"threshold_lineage\"] = 1;", waits, StringComparison.Ordinal);

        foreach (var file in new[] { "PgTargetScorer.Write.cs", "PgTargetScorer.Buffer.cs", "PgTargetScorer.Anomaly.cs" })
        {
            var source = string.Join("\n", Source(file));
            Assert.Contains("[\"threshold_lineage\"] = 0;", source, StringComparison.Ordinal);
            Assert.DoesNotContain("[\"threshold_lineage\"] = 1;", source, StringComparison.Ordinal);
        }

        /* The anomaly detector's z-family stamp is the caller's verdict; TPS and CPU say measured at the call. */
        var detector = string.Join("\n", RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs").Split('\n'));
        Assert.Equal(2, Regex.Matches(detector, @"ZScoreMetadata\([^;]*barsMeasured:\s*true\)").Count);
        Assert.Contains("[\"threshold_lineage\"] = barsMeasured ? 1 : 0,", detector, StringComparison.Ordinal);
    }

    /* ── the walk-up, exercised on an arranged input so the pin is known to bite ── */

    [Fact]
    public void TheWalkUp_ReadsTheBlockAboveADeclaration_SharesItAcrossAPair_AndStopsAtABlankLineOrCode()
    {
        var lines = """
            /// <summary>A stale citation above an unrelated declaration.</summary>
            public const string Unrelated = "x";
            /* measured: p99 over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19;
               a second line ending in a semicolon; and a third. */
            public const double First = 0.2;
            public const double Second = 1.0;

            /// <summary>unmeasured: chosen, not measured.</summary>
            public const double Third = 0.15;
            var code = 1;
            /// <summary>measured: 2026-09-19, Aurora PostgreSQL clusters — but a line of code sits between.</summary>
            var moreCode = 2;
            public const double Fourth = 0.5;
            """.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');

        var first = CommentBlockAbove(lines, "First", "arranged");
        Assert.Contains(CalibrationDate, first, StringComparison.Ordinal);
        Assert.Contains("and a third.", first, StringComparison.Ordinal);
        Assert.DoesNotContain("stale citation", first, StringComparison.Ordinal);   /* a const ABOVE the comment is code, not a pair sibling */
        Assert.Contains(CalibrationDate, CommentBlockAbove(lines, "Second", "arranged"), StringComparison.Ordinal);
        var third = CommentBlockAbove(lines, "Third", "arranged");
        Assert.Contains("unmeasured", third, StringComparison.Ordinal);
        Assert.DoesNotContain(CalibrationDate, third, StringComparison.Ordinal);
        Assert.Equal(string.Empty, CommentBlockAbove(lines, "Fourth", "arranged"));
    }

    /// <summary>The comment lines immediately above <paramref name="constantName"/>'s declaration, collapsed to one
    /// space-joined string. Walks upward from the declaration through doc-comment lines (<c>///</c>) and block
    /// comments (entered at a line ending <c>*/</c>, left at the line carrying <c>/*</c>), skipping a sibling
    /// <c>public const</c> declaration so the members of a pair declared under one comment both read it. A blank
    /// line or a line of code ends the block — the constants in these files are blank-line separated, which is
    /// what makes "the block above" well-defined. Prose is never inspected for code shapes, so a citation line
    /// that happens to end in a semicolon does not truncate the block.</summary>
    private static string CommentBlockAbove(string[] lines, string constantName, string file)
    {
        var declaration = Array.FindIndex(lines, l =>
        {
            var m = s_constDeclaration.Match(l);
            return m.Success && m.Groups[1].Value == constantName;
        });
        Assert.True(declaration >= 0, $"{file}: no public const/static readonly declaration of {constantName}");

        var block = new List<string>();
        var inBlockComment = false;
        for (var i = declaration - 1; i >= 0; i--)
        {
            var trimmed = lines[i].Trim();
            if (trimmed.Length == 0) break;
            /* A sibling of a pair sits BETWEEN this declaration and the shared comment, so it is skipped only while
               no comment line has been collected yet; a declaration above the comment is code and ends the block. */
            if (s_constDeclaration.IsMatch(lines[i])) { if (block.Count == 0) continue; break; }
            if (inBlockComment)
            {
                block.Add(trimmed);
                if (trimmed.Contains("/*", StringComparison.Ordinal)) inBlockComment = false;
                continue;
            }
            if (trimmed.StartsWith("///", StringComparison.Ordinal))
            {
                /* The prefix is dropped so a phrase wrapped across two doc lines rejoins as prose. */
                block.Add(trimmed[3..].Trim());
                continue;
            }
            if (trimmed.EndsWith("*/", StringComparison.Ordinal))
            {
                block.Add(trimmed);
                inBlockComment = !trimmed.Contains("/*", StringComparison.Ordinal);
                continue;
            }
            break;   /* code: the block above the declaration has ended */
        }
        block.Reverse();
        return string.Join(" ", block);
    }

    private static string[] Source(string file) =>
        RepoFile.ReadRepoFileLf(("PerformanceMonitor.Analysis/" + file).Split('/')).Split('\n');
}
