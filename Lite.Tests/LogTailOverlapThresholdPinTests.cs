/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The two server-log collectors read the same file the same way — a fixed byte tail via
/// <c>pg_read_file</c>, no resume marker — and their doc comments explain truncation recovery in terms of
/// consecutive reads OVERLAPPING. That only happens while the log grows by less than one tail between
/// cycles, so the threshold is <c>TailBytes / interval</c> and it is a different number for each of them:
/// <c>pg_deadlocks</c> runs every five minutes and <c>pg_plan_capture</c> every sixty.
///
/// <para>Those numbers are written into four prose comments, which makes them exactly the thing that goes
/// stale when somebody changes a cadence or the tail and reads the comment as still true. So the threshold
/// is DERIVED here from <see cref="CollectorScheduleDefaults"/> and the collectors' own constants, and the
/// figure each comment states is parsed back out and compared. Changing either input fails this.</para>
///
/// <para>Every assertion is floored on the population it matched, because the failure mode being guarded
/// is a claim nobody checks — a regex that silently matches nothing would "pass" while proving nothing,
/// which is the same shape as the drift itself.</para>
/// </summary>
public sealed class LogTailOverlapThresholdPinTests
{
    private static string RepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 10 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} from {AppContext.BaseDirectory}");
    }

    private static string Collectors(string fileName) =>
        File.ReadAllText(RepoFile(Path.Combine("PerformanceMonitor.Collectors", fileName)));

    /* The literal spliced into each collector's SQL. Read from source rather than referenced, because
       both are private consts - and reading them is the point: the pin is that the two agree. */
    private static int TailBytesIn(string fileName)
    {
        var source = Collectors(fileName);
        var match = Regex.Match(source, @"TailBytesLiteral\s*=\s*""(\d+)""");
        Assert.True(match.Success, $"No TailBytesLiteral found in {fileName} — the tail is no longer spelled the way this pin reads it, so its arithmetic is unverified rather than satisfied.");
        return int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// KB/s of log growth at which consecutive tail reads stop overlapping: one tail spread over one
    /// interval.
    /// </summary>
    private static double ThresholdKbPerSecond(int tailBytes, int intervalMinutes) =>
        tailBytes / (intervalMinutes * 60.0) / 1024.0;

    [Fact]
    public void BothLogTailCollectors_ReadTheSameTail()
    {
        var deadlocks = TailBytesIn("PgDeadlocksCollector.cs");
        var plans = TailBytesIn("PgPlanCaptureCollector.cs");

        Assert.Equal(4 * 1024 * 1024, deadlocks);
        Assert.Equal(deadlocks, plans);
    }

    [Fact]
    public void TheTwoCadences_AreNotEqual_AndNoCommentClaimsTheyAre()
    {
        var deadlockMinutes = CollectorScheduleDefaults.All["pg_deadlocks"].FrequencyMinutes;
        var planMinutes = CollectorScheduleDefaults.All["pg_plan_capture"].FrequencyMinutes;

        /* Not an accident and not interchangeable: a deadlock is an event that has to still be inside the
           window when the read comes round, and the plan collector's hourly interval was chosen for the
           consume-once RDS transport that shares its schedule key. If these are ever equalised the prose
           below has to change with them, which is why this asserts the relationship rather than either
           number alone. */
        Assert.NotEqual(deadlockMinutes, planMinutes);
        Assert.True(deadlockMinutes < planMinutes, $"pg_deadlocks ({deadlockMinutes}m) is expected to run more often than pg_plan_capture ({planMinutes}m).");

        /* The specific false claim this pin exists to prevent recurring: the schedule table describing one
           of these cadences as matching the other. Anchored to the two collector names appearing in a
           claim of sameness rather than to one phrasing, and the control below proves it can fire. */
        var schedule = Collectors("CollectorScheduleDefaults.cs");
        foreach (var phrase in ClaimsOfParity(schedule))
        {
            Assert.Fail($"CollectorScheduleDefaults claims a cadence match between the two log-tail collectors, but they run {deadlockMinutes}m and {planMinutes}m apart: \"{phrase.Trim()}\"");
        }
    }

    private static IEnumerable<string> ClaimsOfParity(string source) =>
        Regex.Matches(source, @"[^.\r\n]*\b(?:matching|matches|same as|identical to)\s+pg_plan_capture\b[^.\r\n]*")
             .Select(m => m.Value)
             .Concat(Regex.Matches(source, @"[^.\r\n]*\b(?:matching|matches|same as|identical to)\s+pg_deadlocks\b[^.\r\n]*")
                          .Select(m => m.Value));

    [Fact]
    public void TheParityDetector_FiresOnTheClaimItGuardsAgainst()
    {
        /* Without this, the assertion above passes on any source that simply never says the word - and it
           would have passed on a file where the claim was reworded. Both directions, because the schedule
           table could name either collector as the one being matched. */
        Assert.Single(ClaimsOfParity("Every 5 minutes against a 4 MB tail, matching pg_plan_capture: the two read the same file."));
        Assert.Single(ClaimsOfParity("Hourly, same as pg_deadlocks, because both read a log."));
        Assert.Empty(ClaimsOfParity("Every 5 minutes against the same 4 MB tail pg_plan_capture reads, at a TWELFTH of its interval."));
    }

    /* One file legitimately states BOTH thresholds - the schedule table explains both cadences - so this
       cannot assert that every figure in a file matches one collector. It asserts two things instead: the
       collector's OWN threshold appears in the file that explains it, and every figure stated anywhere is
       one of the two the arithmetic produces. A third number, or a stale one after a cadence change, fails
       the second; a comment that quietly dropped its threshold fails the first. */
    [Theory]
    [InlineData("pg_deadlocks", "CollectorScheduleDefaults.cs")]
    [InlineData("pg_deadlocks", "PgDeadlockLogParser.cs")]
    [InlineData("pg_plan_capture", "CollectorScheduleDefaults.cs")]
    [InlineData("pg_plan_capture", "PgPlanCaptureCollector.cs")]
    [InlineData("pg_plan_capture", "PgPlanLogParser.cs")]
    public void EveryStatedOverlapThreshold_MatchesTheCadenceAndTailItIsDerivedFrom(string collectorName, string fileName)
    {
        var own = ThresholdFor(collectorName);
        var everyKnown = new[] { ThresholdFor("pg_deadlocks"), ThresholdFor("pg_plan_capture") };

        var stated = StatedThresholds(fileName);

        /* The floor. A comment that stopped stating its threshold would otherwise satisfy this by having
           nothing to check, which is the drift being guarded rather than a state to accept. */
        Assert.True(stated.Length > 0, $"{fileName} states no KB/s overlap threshold, so the cadence it explains is unverified. Expected roughly {own:0.0} KB/s for {collectorName}.");

        Assert.True(
            stated.Any(v => Math.Abs(v - own) <= Tolerance),
            $"{fileName} explains {collectorName} but states no figure near {own:0.00} KB/s (the {CollectorScheduleDefaults.All[collectorName].FrequencyMinutes}-minute cadence against its tail). Stated: {string.Join(", ", stated)}.");

        foreach (var value in stated)
        {
            Assert.True(
                everyKnown.Any(k => Math.Abs(value - k) <= Tolerance),
                $"{fileName} states {value} KB/s, which is neither collector's overlap threshold ({string.Join(" or ", everyKnown.Select(k => k.ToString("0.00", CultureInfo.InvariantCulture)))} KB/s). A cadence or tail change has to move the prose with it.");
        }
    }

    /* Rounded prose against derived arithmetic: wide enough for "about 14" and "1.2", far too narrow to
       survive a cadence change, which moves these by 12x. */
    private const double Tolerance = 0.5;

    private static double ThresholdFor(string collectorName)
    {
        var tail = TailBytesIn(collectorName == "pg_deadlocks" ? "PgDeadlocksCollector.cs" : "PgPlanCaptureCollector.cs");
        return ThresholdKbPerSecond(tail, CollectorScheduleDefaults.All[collectorName].FrequencyMinutes);
    }

    private static double[] StatedThresholds(string fileName) =>
        Regex.Matches(Collectors(fileName), @"(\d+(?:\.\d+)?)\s*KB/s")
             .Select(m => double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
             .ToArray();

    [Fact]
    public void TheThresholdArithmetic_IsTheOneTheCommentsDescribe()
    {
        /* Guards the helper itself rather than the prose: a tail spread over its interval, so halving the
           interval doubles the rate the window can absorb. Fixed numbers, so a refactor of the expression
           above cannot quietly redefine what "threshold" means. */
        Assert.Equal(13.65, ThresholdKbPerSecond(4 * 1024 * 1024, 5), 2);
        Assert.Equal(1.14, ThresholdKbPerSecond(4 * 1024 * 1024, 60), 2);
        Assert.Equal(2 * ThresholdKbPerSecond(4 * 1024 * 1024, 60), ThresholdKbPerSecond(4 * 1024 * 1024, 30), 6);
    }
}
