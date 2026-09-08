/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #3112 compression-clearance watch: a compression run's duration against the space the minute it
/// started on had before the next hourly refresh.
///
/// <para><b>What this covers that nothing else did.</b> #3174's grid derives the compression band's WIDTH
/// from a count and asserts the band is clear of every refresh START. Neither of those is a statement about
/// how long a compression RUN takes, and the daily chunk close is a runtime event: every hypertable's newest
/// chunk becomes eligible at the same UTC midnight, so one tick a day carries a full day's rewrite. The two
/// instruments that look as though they cover it do not — #2136's cadence knob judges the run against the
/// hour, and #1778's activity line reports only runs still in progress — so the assertions here are the
/// first over that geometry.</para>
///
/// <para><b>Derive, don't restate.</b> No expected minute, clearance, watch line or percentage below is
/// written down. Each comes from <see cref="TimescaleSupport.CompressionPhaseMinutes"/>,
/// <see cref="TimescaleSupport.HourlyRefreshPhaseOrder"/> or the shipped classifier, so a re-derived grid
/// moves every expectation with it instead of leaving a literal that used to be right. The only literals
/// are the three QUOTED chunk-close readings, which derive from nothing here and are named as
/// measurements.</para>
/// </summary>
public sealed class CompressionClearanceWatchTests
{
    /// <summary>
    /// The three compression runtimes read in the <c>2026-09-08 00:00Z</c> hour on ONE store, from
    /// <c>collect.store_metrics</c>' hourly <c>background_job</c> snapshot — a last-run reading, so no
    /// maximum question is answered by them (#3119).
    ///
    /// <para>Quoted evidence, not a derived quantity: nothing in the product can know them, so they carry no
    /// derivation and are used only as inputs. What is asserted about them is what the SHIPPED classifier
    /// says when it is handed them at the minutes the shipped grid puts those hypertables on.</para>
    /// </summary>
    private static readonly (string Hypertable, double Seconds)[] ChunkCloseReadings =
    {
        ("query_store_stats", 552d),
        ("query_stats", 360d),
        ("query_snapshots", 198d),
    };

    private static int[] RefreshStartMinutes() =>
        TimescaleSupport.HourlyRefreshPhaseOrder.Select(TimescaleSupport.RefreshPhaseMinutesFor).ToArray();

    /// <summary>
    /// The clearance is the distance forward round the hour to the NEAREST refresh start, and it is
    /// established as a property rather than by re-running the same loop: no refresh start is closer than
    /// the reported clearance, one refresh start is at EXACTLY it, and no refresh start falls strictly
    /// inside the span it claims to be clear.
    ///
    /// <para>The third clause is what a re-implementation of the rule would not add. A function returning
    /// the distance to the FURTHEST refresh start satisfies "some start is at exactly this distance" and
    /// fails only here.</para>
    /// </summary>
    [Fact]
    public void TheClearance_IsTheDistanceToTheNearestRefreshStart_AndNothingStartsInsideIt()
    {
        var cadence = TimescaleSupport.MinutesInHourlyCadence;
        var starts = RefreshStartMinutes();

        Assert.NotEmpty(starts);
        Assert.NotEmpty(TimescaleSupport.CompressionPhaseMinutes);

        foreach (var minute in TimescaleSupport.CompressionPhaseMinutes)
        {
            var clearance = TimescaleSupport.CompressionMinuteClearanceMinutes(minute);

            Assert.True(
                clearance > 0,
                $"compression minute :{minute:00} reports {clearance} minutes of clearance, so a refresh "
                + "starts on the same minute a compression policy does — CompressionPhaseMinutes and this "
                + "function disagree about the grid");

            var distances = starts.Select(start => (start - minute + cadence) % cadence).ToArray();

            Assert.All(
                distances,
                distance => Assert.True(
                    distance >= clearance,
                    $"a refresh starts {distance} minutes after :{minute:00}, closer than the {clearance} "
                    + "minutes of clearance reported for it"));

            Assert.Contains(clearance, distances);

            /* The span is CLEAR, not merely bounded: nothing starts strictly inside it. */
            Assert.DoesNotContain(distances, distance => distance > 0 && distance < clearance);
        }
    }

    /// <summary>
    /// The band's clearance falls one minute per minute across it, from its widest first minute to exactly
    /// one <see cref="TimescaleSupport.LightRefreshStepMinutes"/> step on its last — and that last minute is
    /// OCCUPIED, which is what makes the one-step clearance a permanent feature of the grid rather than an
    /// arrangement that happens to be tight.
    /// </summary>
    [Fact]
    public void TheBand_NarrowsToOneLightRefreshStep_AndItsTightestMinuteCarriesAPolicy()
    {
        var minutes = TimescaleSupport.CompressionPhaseMinutes;
        var widest = TimescaleSupport.CompressionMinuteClearanceSeconds(minutes[0]);
        var narrowest = TimescaleSupport.CompressionMinuteClearanceSeconds(minutes[^1]);

        Assert.Equal(TimescaleSupport.LightRefreshStepMinutes * 60, narrowest);
        Assert.True(
            widest > narrowest,
            $"the band's first minute has {widest}s of clearance and its last has {narrowest}s — a band that "
            + "does not narrow is not the contiguous tail band this watch is written over");

        /* Strictly monotonic, which is the property that makes "the last minute is the tightest" true rather
           than merely observed at today's widths. */
        for (var index = 1; index < minutes.Count; index++)
        {
            Assert.True(
                TimescaleSupport.CompressionMinuteClearanceSeconds(minutes[index])
                    < TimescaleSupport.CompressionMinuteClearanceSeconds(minutes[index - 1]),
                $"clearance did not fall from :{minutes[index - 1]:00} to :{minutes[index]:00}");
        }

        var onTheTightestMinute = TimescaleSupport.CompressionPhaseOrder
            .Where(table => TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var assigned)
                && assigned == minutes[^1])
            .ToArray();

        Assert.NotEmpty(onTheTightestMinute);
    }

    /// <summary>
    /// Both classifier boundaries are inclusive, and the middle band is non-empty at EVERY minute in the
    /// band — a watch fraction of one would collapse it and leave the approaching case green while it had
    /// stopped existing.
    /// </summary>
    [Fact]
    public void TheClassifier_IsInclusiveAtBothWalls_AndTheApproachingBandIsNeverEmpty()
    {
        foreach (var minute in TimescaleSupport.CompressionPhaseMinutes)
        {
            var clearance = TimescaleSupport.CompressionMinuteClearanceSeconds(minute);
            var watch = TimescaleSupport.CompressionClearanceWatchSeconds(minute);

            Assert.True(
                watch < clearance,
                $"the watch line for :{minute:00} is {watch}s against {clearance}s of clearance, so the "
                + "approaching band is empty and every reading is either routine or an overrun");
            Assert.True(watch > 0, $"the watch line for :{minute:00} is {watch}s, so it cannot be reached");

            Assert.Equal(
                TimescaleSupport.CompressionClearanceBand.RefreshOverrun,
                TimescaleSupport.ClassifyCompressionClearance(clearance, minute));
            Assert.Equal(
                TimescaleSupport.CompressionClearanceBand.ApproachingRefresh,
                TimescaleSupport.ClassifyCompressionClearance(watch, minute));
            Assert.Equal(
                TimescaleSupport.CompressionClearanceBand.InsideClearance,
                TimescaleSupport.ClassifyCompressionClearance(watch - 1, minute));
        }
    }

    /// <summary>
    /// An impossible reading costs the LINE and never the sweep that carries it — the same posture
    /// <see cref="TimescaleSupport.ClassifyRefreshSlotHeadroom"/> takes — and the minute is modular, because
    /// minute-of-hour arithmetic is.
    /// </summary>
    [Fact]
    public void TheClassifier_TreatsImpossibleReadingsAsRoutine_AndTheMinuteIsModular()
    {
        var minute = TimescaleSupport.CompressionPhaseMinutes[^1];

        Assert.Equal(
            TimescaleSupport.CompressionClearanceBand.InsideClearance,
            TimescaleSupport.ClassifyCompressionClearance(double.NaN, minute));
        Assert.Equal(
            TimescaleSupport.CompressionClearanceBand.InsideClearance,
            TimescaleSupport.ClassifyCompressionClearance(-1d, minute));

        var cadence = TimescaleSupport.MinutesInHourlyCadence;
        Assert.Equal(
            TimescaleSupport.CompressionMinuteClearanceSeconds(minute),
            TimescaleSupport.CompressionMinuteClearanceSeconds(minute + cadence));
        Assert.Equal(
            TimescaleSupport.CompressionMinuteClearanceSeconds(minute),
            TimescaleSupport.CompressionMinuteClearanceSeconds(minute - cadence));

        /* A minute that IS a refresh start has ZERO clearance, so any positive run on it is already an
           overrun. That is not a band-minute case — it is the drifted-policy case, where a job the converge
           could not put on a fixed schedule discovers eligibility wherever its own runtime carried it. */
        var refreshStart = TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.HourlyRefreshPhaseOrder[0]);
        Assert.Equal(0, TimescaleSupport.CompressionMinuteClearanceSeconds(refreshStart));
        Assert.Equal(
            TimescaleSupport.CompressionClearanceBand.RefreshOverrun,
            TimescaleSupport.ClassifyCompressionClearance(1d, refreshStart));
    }

    /// <summary>
    /// The one lead-time fraction, applied to two different widths. Both watch lines are re-derived over
    /// <see cref="TimescaleSupport.WindowWatchLeadNumerator"/> and the source carries no second copy of the
    /// literal — the failure a shared fraction exists to prevent is the two lines drifting apart, which two
    /// independent literals produce silently.
    /// </summary>
    [Fact]
    public void TheWatchFraction_IsSharedByBothWatches_AndTheSourceCarriesNoSecondLiteral()
    {
        var numerator = TimescaleSupport.WindowWatchLeadNumerator;
        var denominator = TimescaleSupport.WindowWatchLeadDenominator;

        Assert.True(
            numerator > 0 && numerator < denominator,
            $"the watch fraction is {numerator}/{denominator}, which is not a lead time inside a window");

        Assert.Equal(
            TimescaleSupport.RefreshPhaseSlotSeconds * numerator / denominator,
            TimescaleSupport.RefreshSlotWarningSeconds);

        foreach (var minute in TimescaleSupport.CompressionPhaseMinutes)
        {
            Assert.Equal(
                TimescaleSupport.CompressionMinuteClearanceSeconds(minute) * numerator / denominator,
                TimescaleSupport.CompressionClearanceWatchSeconds(minute));
        }

        /* THE WALKER, not a regex over the raw file (#2913/#2925). Both remaining copies of this fraction
           in TimescaleSupport.cs are DOC PROSE — one quotes the arithmetic that produced 1,077 s, the other
           states the rule this test enforces — and a scan of the raw text counts them as second
           implementations. A hand-rolled masker is how three separate attempts at this shape got three
           different wrong answers, so the shared one is used. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadTimescaleSupportSource());

        /* The stripper kept CODE, which is what stops this being a guard over an empty string: a
           StripCommentsAndStrings that returned nothing would make the search below vacuously clean. */
        Assert.Contains("RefreshSlotWarningSeconds", code, StringComparison.Ordinal);
        Assert.Contains("WindowWatchLeadNumerator", code, StringComparison.Ordinal);

        /* Whitespace-collapsed, so the search catches '* 5 / 6' and '*5/6' alike — a spacing variant is the
           obvious way past a literal search and it is the same defect. */
        var dense = new string(code.Where(character => !char.IsWhiteSpace(character)).ToArray());
        var literal = string.Create(CultureInfo.InvariantCulture, $"*{numerator}/{denominator}");
        var occurrences = CountOccurrences(dense, literal);

        Assert.True(
            occurrences == 0,
            $"TimescaleSupport.cs's CODE carries {occurrences} copies of the raw '{literal}' fraction. Both "
            + "watch lines are derived over WindowWatchLeadNumerator/Denominator precisely so there is one "
            + "lead-time choice; a literal restores two, and two drift.");
    }

    /// <summary>
    /// The measured chunk-close readings against the grid this file ships: each is inside the clearance of
    /// the minute its hypertable holds, and by how much is derived rather than stated.
    ///
    /// <para><b>Non-vacuous by assertion.</b> A grid that put every policy an hour from the next refresh
    /// would make "inside the clearance" true of any reading at all, so the case requires each reading to be
    /// a real fraction of its clearance before the verdict means anything.</para>
    /// </summary>
    [Fact]
    public void TheMeasuredChunkCloseReadings_AreInsideTheClearanceTheShippedGridGivesThem()
    {
        foreach (var (hypertable, seconds) in ChunkCloseReadings)
        {
            Assert.True(
                TimescaleSupport.TryCompressionPhaseMinutesFor(hypertable, out var minute),
                $"{hypertable} is not on the compression phase grid, so this reading cannot be placed — it "
                + "left the collector catalog, and the paragraph quoting it has to move with it");

            var clearance = TimescaleSupport.CompressionMinuteClearanceSeconds(minute);
            var share = 100.0 * seconds / clearance;

            Assert.InRange(share, 1d, 99d);

            Assert.Equal(
                TimescaleSupport.CompressionClearanceBand.InsideClearance,
                TimescaleSupport.ClassifyCompressionClearance(seconds, minute));
        }
    }

    /// <summary>
    /// The clearance is measured from where the run ACTUALLY started, not from where the grid puts the
    /// policy — the two differ on exactly the store state the converge documents as its degraded path, and
    /// on that store the assigned minute is a fiction.
    /// </summary>
    [Fact]
    public void TheClearance_IsMeasuredFromTheObservedStart_NotFromTheAssignedMinute()
    {
        var hypertable = ChunkCloseReadings[0].Hypertable;
        Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(hypertable, out var assigned));

        var tightest = TimescaleSupport.CompressionPhaseMinutes[^1];
        Assert.NotEqual(assigned, tightest);

        var drifted = Reading(hypertable, startMinute: tightest, seconds: ChunkCloseReadings[0].Seconds);

        Assert.Equal(assigned, drifted.AssignedPhaseMinute);
        Assert.Equal(tightest, drifted.ObservedStartMinute);
        Assert.Equal(tightest, drifted.ClearanceMinute);
        Assert.Equal(TimescaleSupport.CompressionMinuteClearanceSeconds(tightest), drifted.ClearanceSeconds);
        Assert.True(drifted.OffAssignedPhase);

        /* The verdict follows the observed minute too, and it INVERTS against the assigned one — which is
           what says the choice of minute is load-bearing rather than cosmetic. */
        Assert.Equal(TimescaleSupport.CompressionClearanceBand.RefreshOverrun, drifted.ClearanceBand);
        Assert.Equal(
            TimescaleSupport.CompressionClearanceBand.InsideClearance,
            TimescaleSupport.ClassifyCompressionClearance(ChunkCloseReadings[0].Seconds, assigned));

        var onPhase = Reading(hypertable, startMinute: assigned, seconds: ChunkCloseReadings[0].Seconds);
        Assert.False(onPhase.OffAssignedPhase);
        Assert.Equal(assigned, onPhase.ClearanceMinute);
        Assert.Equal(TimescaleSupport.CompressionClearanceBand.InsideClearance, onPhase.ClearanceBand);
    }

    /// <summary>
    /// A reading this product has no standing to judge produces NO verdict — never a synthesized
    /// <see cref="TimescaleSupport.CompressionClearanceBand.InsideClearance"/>, which reads as "measured and
    /// fine" for something not measured at all. Three ways to have no standing: a foreign hypertable, no
    /// completed run, and the never-ran sentinel start.
    /// </summary>
    [Fact]
    public void AReadingWithoutStanding_ProducesNoVerdictRatherThanAPassingOne()
    {
        var foreign = Reading("a_table_this_product_does_not_own", startMinute: 59, seconds: 5_000d);
        Assert.Null(foreign.AssignedPhaseMinute);
        Assert.Null(foreign.ClearanceMinute);
        Assert.Null(foreign.ClearanceSeconds);
        Assert.Null(foreign.ClearanceBand);
        Assert.Null(foreign.ClearOfRefreshSeconds);
        Assert.False(foreign.OffAssignedPhase);

        var hypertable = ChunkCloseReadings[0].Hypertable;
        Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(hypertable, out var assigned));

        var noRun = new CompressionActivity(hypertable, "Scheduled", null, null, 0L);
        Assert.Null(noRun.ObservedStartMinute);
        Assert.Equal(assigned, noRun.ClearanceMinute);
        Assert.Null(noRun.ClearanceBand);
        Assert.False(noRun.OffAssignedPhase);

        /* #1760's sentinel: TimescaleDB's never-ran marker maps to DateTime.MinValue, whose MINUTE is 0 —
           a perfectly plausible minute, which is exactly why it has to be rejected by identity rather than
           by range. Minute 0 is a refresh start, so a sentinel read as a start would report zero clearance
           and call every first run an overrun. */
        var sentinel = new CompressionActivity(
            hypertable, "Running", DateTime.MinValue, TimeSpan.FromSeconds(1d), 0L);
        Assert.Null(sentinel.ObservedStartMinute);
        Assert.Equal(assigned, sentinel.ClearanceMinute);
        Assert.Equal(
            TimescaleSupport.CompressionClearanceBand.InsideClearance, sentinel.ClearanceBand);
    }

    /// <summary>
    /// Clearance left over goes NEGATIVE past the wall rather than clamping, and the percentage is over the
    /// clearance rather than over the hour — clamping would report every overrun as a dead heat, and the
    /// hour is the denominator #2136 already uses and the one that misses this by a factor.
    /// </summary>
    [Fact]
    public void TheOverrunIsReportedAsNegativeClearance_AndTheShareIsOverTheClearance()
    {
        var hypertable = ChunkCloseReadings[0].Hypertable;
        var tightest = TimescaleSupport.CompressionPhaseMinutes[^1];
        var clearance = TimescaleSupport.CompressionMinuteClearanceSeconds(tightest);
        var overrunBy = clearance / TimescaleSupport.WindowWatchLeadDenominator;

        Assert.True(
            overrunBy > 0,
            "the derived overrun is zero, so this case is vacuous — the tightest clearance has collapsed "
            + "below the watch fraction's own denominator");

        var through = Reading(hypertable, startMinute: tightest, seconds: clearance + overrunBy);

        Assert.Equal(TimescaleSupport.CompressionClearanceBand.RefreshOverrun, through.ClearanceBand);
        Assert.Equal(-overrunBy, through.ClearOfRefreshSeconds);
        Assert.True(
            through.ClearOfRefreshSeconds < 0,
            "clearance past the wall is not negative, so the overrun clamps instead of reporting how far "
            + "through the next refresh's start the run went");

        Assert.Equal(
            100.0 * (clearance + overrunBy) / clearance,
            through.PercentOfClearance!.Value,
            3);
        Assert.True(
            through.PercentOfClearance > 100d,
            "an overrun reports at or under 100% of its clearance, so the share is being taken over "
            + "something wider than the wall");
    }

    /// <summary>
    /// The line is leveled by band, and the routine band costs ONE line for the whole store rather than one
    /// per policy — the discipline #1778's own summary states, which seventy hourly Debug lines would
    /// break.
    /// </summary>
    [Fact]
    public void TheClearanceLine_IsLeveledByBand_AndTheRoutineBandIsOneLineForTheWholeStore()
    {
        var routine = new CapturingTestLogger();
        var everyPolicy = TimescaleSupport.CompressionPhaseOrder
            .Select(table =>
            {
                TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var minute);
                return Reading(table, minute, seconds: 0.5d);
            })
            .ToArray();

        Assert.True(everyPolicy.Length > 1, "the catalog has one hypertable, so 'one line not N' is vacuous");
        TimescaleSupport.LogCompressionActivity(everyPolicy, DateTime.UtcNow, routine);

        var clearanceLines = routine.Joined
            .Split(" | ", StringSplitOptions.None)
            .Where(line => line.Contains("clearance", StringComparison.Ordinal))
            .ToArray();

        Assert.Single(clearanceLines);
        Assert.StartsWith("Debug:", clearanceLines[0], StringComparison.Ordinal);

        var hypertable = ChunkCloseReadings[0].Hypertable;
        var tightest = TimescaleSupport.CompressionPhaseMinutes[^1];
        var clearance = TimescaleSupport.CompressionMinuteClearanceSeconds(tightest);

        var overrun = new CapturingTestLogger();
        TimescaleSupport.LogCompressionActivity(
            new[] { Reading(hypertable, tightest, clearance) }, DateTime.UtcNow, overrun);
        Assert.Contains("Warning:", overrun.Joined, StringComparison.Ordinal);
        Assert.Contains(hypertable, overrun.Joined, StringComparison.Ordinal);
        Assert.Contains("AccessExclusiveLock", overrun.Joined, StringComparison.Ordinal);
        Assert.Contains("#3112", overrun.Joined, StringComparison.Ordinal);

        var approaching = new CapturingTestLogger();
        TimescaleSupport.LogCompressionActivity(
            new[] { Reading(hypertable, tightest, TimescaleSupport.CompressionClearanceWatchSeconds(tightest)) },
            DateTime.UtcNow,
            approaching);
        Assert.Contains("Information:", approaching.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("Warning:", approaching.Joined, StringComparison.Ordinal);

        /* A store of nothing but FOREIGN hypertables says nothing about clearance at all — not a routine
           all-clear over tables whose minutes this code did not choose. */
        var foreignOnly = new CapturingTestLogger();
        TimescaleSupport.LogCompressionActivity(
            new[] { Reading("a_table_this_product_does_not_own", 59, 5_000d) }, DateTime.UtcNow, foreignOnly);
        Assert.DoesNotContain("clearance", foreignOnly.Joined, StringComparison.Ordinal);

        /* The drift line names BOTH minutes, because "off its slot" without the two numbers cannot be acted
           on. */
        TimescaleSupport.TryCompressionPhaseMinutesFor(hypertable, out var assigned);
        var drifted = new CapturingTestLogger();
        TimescaleSupport.LogCompressionActivity(
            new[] { Reading(hypertable, tightest, 0.5d) }, DateTime.UtcNow, drifted);
        Assert.Contains(
            string.Create(CultureInfo.InvariantCulture, $":{tightest:00}"), drifted.Joined, StringComparison.Ordinal);
        Assert.Contains(
            string.Create(CultureInfo.InvariantCulture, $":{assigned:00}"), drifted.Joined, StringComparison.Ordinal);
    }

    /// <summary>
    /// The enum's declaration ORDER is its severity order, which <c>IsTighter</c> relies on to rank a band
    /// against a band. Pinned rather than left as a property of how the members happen to be written: a
    /// reordering would silently invert the summary's choice of which policy to name.
    /// </summary>
    [Fact]
    public void TheClearanceBands_AreDeclaredInSeverityOrder()
    {
        Assert.True(
            TimescaleSupport.CompressionClearanceBand.InsideClearance
                < TimescaleSupport.CompressionClearanceBand.ApproachingRefresh,
            "InsideClearance no longer sorts below ApproachingRefresh");
        Assert.True(
            TimescaleSupport.CompressionClearanceBand.ApproachingRefresh
                < TimescaleSupport.CompressionClearanceBand.RefreshOverrun,
            "ApproachingRefresh no longer sorts below RefreshOverrun");
    }

    /// <summary>
    /// The per-tick summary names the WORST reading, and the case that proves it is the one whose share of
    /// its clearance cannot be computed at all: a drifted policy on a minute an hourly refresh also starts
    /// on has ZERO clearance, so <see cref="CompressionActivity.PercentOfClearance"/> is null there.
    ///
    /// <para><b>Why this case and not a merely-large one.</b> Ranking by the share with a missing value
    /// defaulted to zero ranks that policy BELOW a routine reading — the worst geometry the grid can produce,
    /// passed over by the line documented as carrying the tightest reading. A large-but-finite overrun would
    /// rank correctly under either rule, so it cannot tell the two apart. Raised by review.</para>
    /// </summary>
    [Fact]
    public void ThePerTickSummary_NamesAZeroClearanceOverrun_OverAnyRoutineReading()
    {
        var refreshStart = TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.HourlyRefreshPhaseOrder[0]);
        Assert.Equal(0, TimescaleSupport.CompressionMinuteClearanceSeconds(refreshStart));

        var worst = ChunkCloseReadings[0].Hypertable;
        var drifted = Reading(worst, refreshStart, seconds: 5d);

        /* The premise: its share really is unavailable, so the two ranking rules genuinely differ here. */
        Assert.Null(drifted.PercentOfClearance);
        Assert.Equal(TimescaleSupport.CompressionClearanceBand.RefreshOverrun, drifted.ClearanceBand);

        /* A routine companion with a REAL and non-trivial share, so a rule that defaults the missing one to
           zero would pick this one. */
        var routine = ChunkCloseReadings[2].Hypertable;
        Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(routine, out var routineMinute));
        var companion = Reading(
            routine,
            routineMinute,
            seconds: TimescaleSupport.CompressionMinuteClearanceSeconds(routineMinute) / 2d);

        Assert.NotNull(companion.PercentOfClearance);
        Assert.True(
            companion.PercentOfClearance > 0d,
            "the routine companion's share is not positive, so it cannot out-rank a zero defaulted from null "
            + "and this case cannot tell the two rules apart");
        Assert.Equal(TimescaleSupport.CompressionClearanceBand.InsideClearance, companion.ClearanceBand);

        /* Both orders of arrival, because a comparator can be right in one and wrong in the other. */
        foreach (var tick in new[]
                 {
                     new[] { companion, drifted },
                     new[] { drifted, companion },
                 })
        {
            var logger = new CapturingTestLogger();
            TimescaleSupport.LogCompressionActivity(tick, DateTime.UtcNow, logger);

            var summary = logger.Joined
                .Split(" | ", StringSplitOptions.None)
                .Single(line => line.Contains("tightest compression clearance", StringComparison.Ordinal));

            Assert.Contains(worst, summary, StringComparison.Ordinal);
            Assert.DoesNotContain(routine, summary, StringComparison.Ordinal);
        }
    }

    private static CompressionActivity Reading(string hypertable, int startMinute, double seconds) =>
        new(
            hypertable,
            "Scheduled",
            new DateTime(2026, 9, 8, 0, startMinute, 0, DateTimeKind.Utc),
            TimeSpan.FromSeconds(seconds),
            0L);

    private static int CountOccurrences(string haystack, string needle)
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

    private static string ReadTimescaleSupportSource([CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs"));

        Assert.True(File.Exists(path),
            $"TimescaleSupport.cs was not found at '{path}'. This guard scans the REAL file (resolved from "
            + "[CallerFilePath], so there is no copy to go stale); restore the path rather than pointing it "
            + "at a fixture.");
        return File.ReadAllText(path);
    }

    /* ─────────────── the prose pins over the members this file's watch is stated on ─────────────── */

    /// <summary>
    /// Marks a verification failure caused by a PATTERN no longer matching, so
    /// <see cref="EveryClearancePin_ReportsAnInjectedDrift"/> can tell "the pin caught the drift" from "the
    /// pin went blind" — which are otherwise the same exception, and the second of which would let the sweep
    /// report success for a guard that had stopped seeing anything. Borrowed shape from
    /// <c>RefreshCeilingProvenancePinTests</c>, which is where this discipline is argued at length.
    /// </summary>
    private const string ParseMiss = "CLEARANCE PIN PARSE MISS";

    private const string ClearanceDeclaration =
        "public static int CompressionMinuteClearanceMinutes(int minute)";

    private const string WatchFractionDeclaration =
        "public const int WindowWatchLeadNumerator";

    private const string CompressionMinutesDeclaration =
        "public static readonly IReadOnlyList<int> CompressionPhaseMinutes";

    private static string DeclarationFor(string pin) => pin switch
    {
        "the cadence knob's line against the tightest clearance" => ClearanceDeclaration,
        "the quoted reading as a share of cadence" => ClearanceDeclaration,
        "the band's clearance range" => ClearanceDeclaration,
        "the largest hypertable's placement" => ClearanceDeclaration,
        "the tightest large placement" => ClearanceDeclaration,
        "the previous grid's clearances against the quoted readings" => ClearanceDeclaration,
        "the shipped grid's shares" => ClearanceDeclaration,
        "the watch fraction as a percent" => WatchFractionDeclaration,
        "the lead the tightest clearance buys" => WatchFractionDeclaration,
        "the spread #3174 held constant" => CompressionMinutesDeclaration,
        "the shipped grid's clearances for the measured three" => CompressionMinutesDeclaration,
        _ => throw new ArgumentOutOfRangeException(nameof(pin), pin, "no declaration registered for this pin"),
    };

    /// <summary>
    /// Every numeral the doc runs above those three members state, paired with the pattern that reads it out
    /// of the SHIPPED file. Each is arithmetic over the grid's own constants or over a quoted reading the same
    /// sentence names, so none of them is a value a reader has to trust.
    ///
    /// <para>Patterns are written against <see cref="DocProseFor"/>'s NORMALISED output, so they carry a
    /// plain hyphen and no <c>&lt;b&gt;</c> tags — bolding a figure or switching an em dash for a hyphen must
    /// not be able to break a pin for a reason that has nothing to do with what it guards.</para>
    ///
    /// <para>Every group is <c>[0-9]+</c> rather than <c>[0-9]</c>, for the reason the sibling file states:
    /// <see cref="EveryClearancePin_ReportsAnInjectedDrift"/> bumps a captured number width-preservingly, so
    /// a group holding a 9 comes back two digits wide and a one-digit group would then stop matching — which
    /// the sweep would report as a broken pattern rather than as a caught drift.</para>
    /// </summary>
    private static IEnumerable<(string Name, string Pattern, bool DriftSwept)> Pins()
    {
        yield return (
            "the cadence knob's line against the tightest clearance",
            @"puts the line at ([0-9]+) s of a ([0-9,]+) s hour, while the wall a compression run actually faces is its own minute's clearance, as little as ([0-9]+) s\. That is FIFTEEN times too high",
            true);

        yield return (
            "the quoted reading as a share of cadence",
            @"the ([0-9]+) s chunk-close run measured below reads ([0-9]+)\.([0-9]+)% of cadence",
            true);

        yield return (
            "the band's clearance range",
            @"([0-9,]+) s on its first minute down to ([0-9]+) s on its last",
            true);

        yield return (
            "the largest hypertable's placement",
            @"sits at :([0-9]+) with ([0-9,]+) s, and <c>procedure_stats</c>",
            true);

        yield return (
            "the tightest large placement",
            @"<c>procedure_stats</c> sits at :([0-9]+) with ([0-9,]+) s",
            true);

        /* NOT drift-swept, and the reason is the one the sibling file states for its own quoted series: these
           three are MEASUREMENTS. Nothing in the code can know them, so bumping a digit only produces another
           number the code cannot contradict - a +1 changes neither the "ran past its clearance" verdict nor
           the rounded percentage below. What IS checked is what the paragraph actually claims about them:
           that each exceeds the clearance the same sentence states, and that each divides into the share the
           shipped grid gives it. Material drift is caught by the second of those; a single digit is not, and
           saying so is better than a flag that implies otherwise. */
        yield return (
            "the previous grid's clearances against the quoted readings",
            @"read at ([0-9]+) s \(<c>query_stats</c>\), ([0-9]+) s \(<c>query_snapshots</c>\) and ([0-9]+) s \(<c>query_store_stats</c>\)",
            false);

        yield return (
            "the shipped grid's shares",
            @"the same three hold <c>:([0-9]+)</c>, <c>:([0-9]+)</c> and <c>:([0-9]+)</c>, where those readings are ([0-9]+)%, ([0-9]+)% and ([0-9]+)% of their clearance",
            true);

        yield return (
            "the watch fraction as a percent",
            @"a watch line sits at ([0-9]+)\.([0-9]+)% of whatever",
            true);

        yield return (
            "the lead the tightest clearance buys",
            @"so a sixth of it is ([0-9]+) s of lead",
            true);

        yield return (
            "the spread #3174 held constant",
            @"held the spread constant - ([0-9]+) minutes at ([0-9]+) per minute",
            true);

        yield return (
            "the shipped grid's clearances for the measured three",
            @"the same three hold minutes with ([0-9,]+) s, ([0-9,]+) s and ([0-9,]+) s",
            true);
    }

    /// <summary>
    /// The doc runs state what the code computes. Fail-fast, and it must CONSUME every pin in
    /// <see cref="Pins"/> — a pattern defined and never asserted against anything looks exactly like a
    /// pattern doing work.
    /// </summary>
    [Fact]
    public void TheClearanceProse_StatesWhatTheGridComputes()
    {
        Verify(ReadTimescaleSupportSource());
    }

    private static void Verify(string source)
    {
        var consumed = new HashSet<string>(StringComparer.Ordinal);

        int[] Read(string name)
        {
            consumed.Add(name);
            return Numbers(DocProseFor(source, DeclarationFor(name)), name);
        }

        var minutes = TimescaleSupport.CompressionPhaseMinutes;
        var widest = TimescaleSupport.CompressionMinuteClearanceSeconds(minutes[0]);
        var narrowest = TimescaleSupport.CompressionMinuteClearanceSeconds(minutes[^1]);
        var cadenceSeconds = (int)TimescaleSupport.CompressScheduleSpan.TotalSeconds;
        var knobLine = TimescaleSupport.RefreshSlotPercentOfHourlyCadence * cadenceSeconds / 100;

        var knob = Read("the cadence knob's line against the tightest clearance");
        Require(knob[0] == knobLine, $"stated knob line {knob[0]} s against {knobLine} s derived");
        Require(knob[1] == cadenceSeconds, $"stated cadence {knob[1]} s against {cadenceSeconds} s");
        Require(knob[2] == narrowest, $"stated tightest clearance {knob[2]} s against {narrowest} s");
        Require(knobLine == 15 * narrowest,
            $"the knob line is {knobLine} s against {narrowest} s of clearance, which is not the FIFTEEN "
            + "times the prose states — restate the factor rather than removing this check");

        var share = Read("the quoted reading as a share of cadence");
        var tenths = (int)Math.Round(1000.0 * share[0] / cadenceSeconds, MidpointRounding.AwayFromZero);
        Require(share[1] * 10 + share[2] == tenths,
            $"stated {share[1]}.{share[2]}% against {tenths / 10}.{tenths % 10}% derived from {share[0]} s "
            + $"over {cadenceSeconds} s");

        var range = Read("the band's clearance range");
        Require(range[0] == widest, $"stated widest {range[0]} s against {widest} s");
        Require(range[1] == narrowest, $"stated narrowest {range[1]} s against {narrowest} s");

        RequirePlacement(Read("the largest hypertable's placement"), "query_store_stats");
        RequirePlacement(Read("the tightest large placement"), "procedure_stats");

        /* The three QUOTED readings. They derive from nothing here, so what is checked is the relationship
           the same sentence asserts about them: each ran past the clearance the sentence also states. That is
           the pin available for evidence, and it is not weaker than it looks - the whole paragraph's claim is
           the comparison, so a digit moved in either list breaks it. */
        var readings = Read("the previous grid's clearances against the quoted readings");
        var previousClearances = new[] { 240, 180, 120 };
        for (var index = 0; index < readings.Length; index++)
        {
            Require(readings[index] > previousClearances[index],
                $"the stated reading {readings[index]} s no longer exceeds the {previousClearances[index]} s "
                + "of clearance the same sentence states, so the paragraph's claim that every one of them ran "
                + "past the refresh that followed it does not follow from its own figures");
        }

        var shipped = Read("the shipped grid's shares");
        var order = new[] { "query_stats", "query_snapshots", "query_store_stats" };
        for (var index = 0; index < order.Length; index++)
        {
            Require(TimescaleSupport.TryCompressionPhaseMinutesFor(order[index], out var minute),
                $"{order[index]} is not on the compression grid, so the prose cannot place it");
            Require(shipped[index] == minute,
                $"stated minute :{shipped[index]:00} for {order[index]} against :{minute:00} derived");

            var clearance = TimescaleSupport.CompressionMinuteClearanceSeconds(minute);
            var percent = (int)Math.Round(100.0 * readings[index] / clearance, MidpointRounding.AwayFromZero);
            Require(shipped[order.Length + index] == percent,
                $"stated {shipped[order.Length + index]}% for {order[index]} against {percent}% derived from "
                + $"{readings[index]} s over {clearance} s");
        }

        var fraction = Read("the watch fraction as a percent");
        var fractionTenths = (int)Math.Round(
            1000.0 * TimescaleSupport.WindowWatchLeadNumerator / TimescaleSupport.WindowWatchLeadDenominator,
            MidpointRounding.ToZero);
        Require(fraction[0] * 10 + fraction[1] == fractionTenths,
            $"stated {fraction[0]}.{fraction[1]}% against {fractionTenths / 10}.{fractionTenths % 10}% derived");

        var lead = Read("the lead the tightest clearance buys");
        var derivedLead = narrowest - TimescaleSupport.CompressionClearanceWatchSeconds(minutes[^1]);
        Require(lead[0] == derivedLead, $"stated lead {lead[0]} s against {derivedLead} s derived");

        var spread = Read("the spread #3174 held constant");
        Require(spread[0] == TimescaleSupport.CompressionPhaseBandMinutes,
            $"stated band width {spread[0]} against {TimescaleSupport.CompressionPhaseBandMinutes}");
        Require(spread[1] == TimescaleSupport.CompressionPhaseMaxPerMinute,
            $"stated per-minute spread {spread[1]} against {TimescaleSupport.CompressionPhaseMaxPerMinute}");

        var clearances = Read("the shipped grid's clearances for the measured three");
        for (var index = 0; index < order.Length; index++)
        {
            TimescaleSupport.TryCompressionPhaseMinutesFor(order[index], out var minute);
            var clearance = TimescaleSupport.CompressionMinuteClearanceSeconds(minute);
            Require(clearances[index] == clearance,
                $"stated clearance {clearances[index]} s for {order[index]} against {clearance} s derived");
        }

        var defined = Pins().Select(pin => pin.Name).ToArray();
        Require(consumed.Count == defined.Length,
            $"{consumed.Count} of {defined.Length} pins were asserted against something. A pattern defined "
            + $"and never read looks exactly like a pattern doing work: {string.Join(", ", defined.Except(consumed))}");
    }

    private static void RequirePlacement(int[] captured, string hypertable)
    {
        Require(TimescaleSupport.TryCompressionPhaseMinutesFor(hypertable, out var minute),
            $"{hypertable} is not on the compression phase grid, so the prose cannot place it");
        Require(captured[0] == minute,
            $"stated minute :{captured[0]:00} for {hypertable} against :{minute:00} derived");

        var clearance = TimescaleSupport.CompressionMinuteClearanceSeconds(minute);
        Require(captured[1] == clearance,
            $"stated clearance {captured[1]} s for {hypertable} against {clearance} s derived");
    }

    /// <summary>
    /// Every numeric pin catches an injected drift, and does so by CATCHING it rather than by going blind:
    /// each captured number is bumped one at a time in a mutated copy, the pattern is separately required to
    /// still match, and the identical verification is required to fail.
    /// </summary>
    [Fact]
    public void EveryClearancePin_ReportsAnInjectedDrift()
    {
        var source = ReadTimescaleSupportSource();
        var blind = new List<string>();

        foreach (var (name, pattern, _) in Pins().Where(pin => pin.DriftSwept))
        {
            var prose = DocProseFor(source, DeclarationFor(name));
            var match = Regex.Match(prose, pattern);
            Assert.True(match.Success, $"{name}: the pattern does not match the shipped prose at all");

            for (var group = 1; group < match.Groups.Count; group++)
            {
                var captured = match.Groups[group].Value;
                if (!captured.All(character => char.IsDigit(character) || character == ','))
                {
                    continue;
                }

                var bumped = Bump(captured);
                var mutated = ReplaceCapture(source, DeclarationFor(name), pattern, group, bumped);

                Assert.NotEqual(source, mutated);

                /* The pattern must still see the sentence: a mutation that merely broke the regex would
                   throw the same exception the caught drift throws, and would pass as one. */
                var mutatedProse = DocProseFor(mutated, DeclarationFor(name));
                Assert.True(
                    Regex.IsMatch(mutatedProse, pattern),
                    $"{name} group {group}: bumping '{captured}' to '{bumped}' stopped the pattern matching, "
                    + "so this iteration proves nothing about the pin");

                Exception? failure = null;
                try
                {
                    Verify(mutated);
                }
                catch (Exception thrown)
                {
                    failure = thrown;
                }

                if (failure is null)
                {
                    blind.Add($"{name} group {group} ('{captured}' -> '{bumped}')");
                    continue;
                }

                Assert.DoesNotContain(ParseMiss, failure.Message, StringComparison.Ordinal);
            }
        }

        /* EVERY blind group at once rather than the first: a sweep that stops at one leaves the rest
           unexamined, and the question this test answers is which numerals are load-bearing - a list, not a
           first offender. */
        Assert.True(
            blind.Count == 0,
            "these captured numerals can be bumped without any verification noticing, so the prose could "
            + "drift there silently. Either derive them or mark the pin DriftSwept: false with the reason: "
            + string.Join("; ", blind));
    }

    /// <summary>Width-preserving where it can be, so a bumped number cannot break a pattern by changing how
    /// many digits it has: the last digit rolls, and a 9 becomes an 8.</summary>
    private static string Bump(string captured)
    {
        var characters = captured.ToCharArray();

        for (var index = characters.Length - 1; index >= 0; index--)
        {
            if (!char.IsDigit(characters[index]))
            {
                continue;
            }

            characters[index] = characters[index] == '9' ? '8' : (char)(characters[index] + 1);
            return new string(characters);
        }

        throw new ArgumentOutOfRangeException(nameof(captured), captured, "no digit to bump");
    }

    /// <summary>
    /// Splices <paramref name="bumped"/> over the SOURCE characters the captured group came from, located
    /// through <see cref="DocProseWithMap"/>'s character map rather than by searching for the captured text.
    ///
    /// <para><b>Why a map and not a search, stated because the search version SHIPPED here first and was
    /// wrong.</b> A group holding a single digit — the tenths of a percentage, the per-minute spread — matches
    /// the first occurrence of that digit anywhere in the doc run, which for these paragraphs is inside
    /// <c>#3112</c> or <c>3,600</c>. The bump then lands on text no verification reads, the mutated file
    /// verifies clean, and <see cref="EveryClearancePin_ReportsAnInjectedDrift"/> reports the pin as
    /// drift-blind when it is the SWEEP that is blind. That is the failure this whole file exists to catch,
    /// one layer up: a mutation harness that cannot mutate reports every pin as unfalsifiable and reads as a
    /// finding about the pins.</para>
    ///
    /// <para>Contiguity is asserted rather than assumed: a numeral wrapped across two <c>///</c> lines has no
    /// single source span, and a silent partial splice would be the same defect again. The remedy is to rewrap
    /// the prose so the numeral is whole on one line.</para>
    /// </summary>
    private static string ReplaceCapture(string source, string declaration, string pattern, int group, string bumped)
    {
        var (prose, map) = DocProseWithMap(source, declaration);
        var match = Regex.Match(prose, pattern);
        Assert.True(match.Success, "the pattern stopped matching before the mutation was applied");

        var captured = match.Groups[group];
        var from = map[captured.Index];
        var to = map[captured.Index + captured.Length - 1];

        Assert.True(from >= 0 && to >= from, $"'{captured.Value}' has no source span in '{declaration}'");
        Assert.Equal(captured.Value, source[from..(to + 1)]);

        return source[..from] + bumped + source[(to + 1)..];
    }

    private static int[] Numbers(string prose, string name)
    {
        var pattern = Pins().First(pin => string.Equals(pin.Name, name, StringComparison.Ordinal)).Pattern;
        var match = Regex.Match(prose, pattern);

        Require(match.Success, $"{ParseMiss}: '{name}' did not match its own pattern");

        return match.Groups.Cast<Group>().Skip(1)
            .Where(group => group.Success)
            .Select(group => int.Parse(group.Value.Replace(",", "", StringComparison.Ordinal), CultureInfo.InvariantCulture))
            .ToArray();
    }

    private static string DocProseFor(string source, string declaration) =>
        DocProseWithMap(source, declaration).Prose;

    /// <summary>
    /// The doc comment run immediately above <paramref name="declaration"/>, normalised, together with a
    /// per-character map back into <paramref name="source"/> (<c>-1</c> for the spaces that join two
    /// <c>///</c> lines, which come from no source character).
    ///
    /// <para><b>&lt;b&gt; emphasis and dash style are normalised before any pattern sees them</b>, so bolding
    /// a figure or switching an em dash for a hyphen cannot break a pin for a reason that has nothing to do
    /// with what it guards. Neither normalisation introduces a DIGIT, which is what makes the map exact
    /// everywhere a pin actually reads.</para>
    /// </summary>
    private static (string Prose, int[] Map) DocProseWithMap(string source, string declaration)
    {
        var declarationIndex = source.IndexOf(declaration, StringComparison.Ordinal);
        Require(declarationIndex > 0,
            $"{ParseMiss}: could not find '{declaration}' in TimescaleSupport.cs, so no doc run could be read");

        /* Walk UP from the declaration collecting whole '///' lines, keeping each line's source offset so a
           character's origin survives the join. */
        var runs = new List<(int Offset, string Text)>();
        var cursor = source.LastIndexOf('\n', declarationIndex - 1);

        while (cursor > 0)
        {
            var lineStart = source.LastIndexOf('\n', cursor - 1) + 1;
            var line = source[lineStart..cursor].TrimEnd('\r');
            var trimmed = line.TrimStart(' ', '\t');

            if (!trimmed.StartsWith("///", StringComparison.Ordinal))
            {
                break;
            }

            var bodyOffset = lineStart + (line.Length - trimmed.Length) + 3;
            var body = source[bodyOffset..cursor].TrimEnd('\r');
            var leading = body.Length - body.TrimStart(' ').Length;

            runs.Insert(0, (bodyOffset + leading, body.Trim()));
            cursor = lineStart - 1;
        }

        Require(runs.Count > 0, $"{ParseMiss}: '{declaration}' carries no doc comment run");

        var builder = new System.Text.StringBuilder();
        var map = new List<int>();

        for (var index = 0; index < runs.Count; index++)
        {
            if (index > 0)
            {
                builder.Append(' ');
                map.Add(-1);
            }

            var (offset, text) = runs[index];

            for (var at = 0; at < text.Length; at++)
            {
                if (text.AsSpan(at).StartsWith("<b>", StringComparison.Ordinal))
                {
                    at += 2;
                    continue;
                }

                if (text.AsSpan(at).StartsWith("</b>", StringComparison.Ordinal))
                {
                    at += 3;
                    continue;
                }

                var character = text[at];
                builder.Append(character == '\u2014' || character == '\u2013' ? '-' : character);
                map.Add(offset + at);
            }
        }

        var prose = builder.ToString();
        Require(prose.StartsWith("<summary>", StringComparison.Ordinal),
            $"{ParseMiss}: the doc run above '{declaration}' does not begin at its <summary>");

        return (prose, map.ToArray());
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
