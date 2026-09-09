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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #3182 finding: a live reading that has overtaken a ceiling CONSTANT, reported separately from where
/// that reading sits against its slot.
///
/// <para><b>The defect these cases are written against.</b>
/// <see cref="TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds"/> is recorded as a PREFIX
/// MAXIMUM — the largest run its regime had been recorded to make when it was read, which a later run of
/// the same regime joins and can exceed (#3188). Runs above it happened, and every one of them classified
/// <see cref="TimescaleSupport.RefreshSlotHeadroom.InsideSlot"/> and logged at Debug, because a run can be
/// past the recorded ceiling and still a long way inside the window the grid gives it. So the product had a
/// signal for "the slot is exceeded" and no signal at all for "the number the grid was DERIVED from is
/// wrong" — and the second is what a stale sizing constant produces. The two are different facts with
/// different remedies and both can be true of one reading.</para>
///
/// <para><b>Every probe is DERIVED from the constants, never a literal.</b> The falsifying reading is the
/// recorded ceiling plus one second, which is the tightest falsifier that exists at any value the constant
/// can take; the band probes are the watch line and the slot width. One case quotes a measured reading, and
/// it says so and carries its own admissibility guard.</para>
///
/// <para><b>What is NOT claimed here.</b> Nothing in this file asserts that either constant IS or is NOT
/// currently stale — that is a measurement, it moves hourly, and a test that encoded tonight's answer would
/// be the same frozen-figure defect one level up. What is asserted is that a falsification is REPORTED when
/// it occurs, from any band, at a level that is not the level the defect hid at, and no more than once per
/// constant until a larger run arrives.</para>
/// </summary>
public sealed class RefreshCeilingStalenessTests
{
    private static int Ceiling => TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds;

    private static int WatchLine => TimescaleSupport.RefreshSlotWarningSeconds;

    private static int Slot => TimescaleSupport.RefreshPhaseSlotSeconds;

    private static string HeaviestConstant => TimescaleSupport.HeaviestRefreshCeilingConstantName;

    private static string LightConstant => TimescaleSupport.OtherRefreshCeilingConstantName;

    /// <summary>
    /// THE HEADLINE CASE, and the one the whole finding exists for: a reading above the recorded ceiling but
    /// below the watch line. The slot watch calls it routine and logs Debug — correctly, it IS routine
    /// against the slot — and the staleness finding still speaks.
    ///
    /// <para><b>The vacuity guard is the first assertion and it is not decoration.</b> This case only tests
    /// anything while a falsifying reading can EXIST inside the routine band, which needs
    /// <c>Ceiling + 1 &lt; RefreshSlotWarningSeconds</c>. If a re-derivation ever put the ceiling at or past
    /// the watch line, every falsifying reading would be a warning or a breach anyway, the finding's whole
    /// premise would have changed, and this case would go red rather than quietly stop covering the band it
    /// names.</para>
    /// </summary>
    [Fact]
    public void TheFinding_SpeaksFromInsideTheRoutineBand_WhereTheSlotWatchIsSilent()
    {
        Assert.True(
            Ceiling + 1 < WatchLine,
            $"the {Ceiling} s ceiling plus one second is at or past the {WatchLine} s watch line, so no "
            + "falsifying reading can land in the routine band and this case covers nothing. That is a real "
            + "change in the finding's premise — the constant would then be inside the band the slot watch "
            + "already speaks in — so re-read #3182 rather than adjusting this probe");

        var falsifying = Ceiling + 1;

        /* The slot watch's verdict on the same reading, from the shipped classifier: routine, and Debug. */
        Assert.Equal(
            TimescaleSupport.RefreshSlotHeadroom.InsideSlot,
            TimescaleSupport.ClassifyRefreshSlotHeadroom(falsifying));

        var slotWatch = new CapturingTestLogger();
        TimescaleSupport.LogHeaviestRefreshSlotHeadroom(
            new HeaviestRefreshSlotReading(TimescaleSupport.HeaviestHourlyRefreshView, falsifying), slotWatch);
        Assert.StartsWith("Debug:", slotWatch.Joined, StringComparison.Ordinal);

        /* And the finding, on the same reading: a Warning that names the CONSTANT. */
        Assert.Equal(
            TimescaleSupport.RefreshCeilingFreshness.CeilingFalsified,
            TimescaleSupport.ClassifyRefreshCeilingFreshness(falsifying, Ceiling));

        var finding = new CapturingTestLogger();
        TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, falsifying,
            new RefreshCeilingStalenessWatch(), finding);

        Assert.StartsWith("Warning:", finding.Joined, StringComparison.Ordinal);
        Assert.Contains(HeaviestConstant, finding.Joined, StringComparison.Ordinal);
        Assert.Contains(TimescaleSupport.HeaviestHourlyRefreshView, finding.Joined, StringComparison.Ordinal);
    }

    /// <summary>
    /// The finding is reachable from EVERY slot band, enumerated over the shipped enum rather than over a
    /// list written here — so a band added later has to be given a probe deliberately instead of silently
    /// falling outside the coverage this case claims.
    ///
    /// <para>Each probe is asserted to actually LAND in the band it is offered for, before the finding is
    /// asked anything. A probe that had drifted into a neighbouring band would otherwise report coverage of
    /// a band nothing tested.</para>
    /// </summary>
    [Fact]
    public void TheFinding_IsReachableFromEverySlotBand()
    {
        var probes = new Dictionary<TimescaleSupport.RefreshSlotHeadroom, double>
        {
            [TimescaleSupport.RefreshSlotHeadroom.InsideSlot] = Ceiling + 1,
            [TimescaleSupport.RefreshSlotHeadroom.ApproachingSlot] = WatchLine,
            [TimescaleSupport.RefreshSlotHeadroom.SlotExceeded] = Slot,
        };

        var bands = Enum.GetValues<TimescaleSupport.RefreshSlotHeadroom>();
        Assert.NotEmpty(bands);
        Assert.All(bands, band => Assert.True(
            probes.ContainsKey(band),
            $"the slot classifier has a {band} band with no probe here, so this case's claim to cover every "
            + "band is false. Add a falsifying reading that lands in it (#3182)"));

        foreach (var (band, reading) in probes)
        {
            Assert.Equal(band, TimescaleSupport.ClassifyRefreshSlotHeadroom(reading));

            Assert.True(
                reading > Ceiling,
                $"the {band} probe of {reading} s is not above the {Ceiling} s ceiling, so it cannot "
                + "falsify anything and this band's coverage is vacuous");

            var log = new CapturingTestLogger();
            TimescaleSupport.LogRefreshCeilingStaleness(
                HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, reading,
                new RefreshCeilingStalenessWatch(), log);

            Assert.StartsWith("Warning:", log.Joined, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The ceiling boundary EXCLUDES equality where the slot bands include it, and that opposition is the
    /// point rather than an inconsistency.
    ///
    /// <para>A run that took exactly its window was already colliding with its neighbour, so the slot wall is
    /// inclusive. A reading equal to a recorded maximum is the reading that maximum was taken from and
    /// CONFIRMS the constant. Inclusive here would report the grid's own sizing figure as a falsification of
    /// itself on the hour it was measured — which is the one reading guaranteed to arrive.</para>
    /// </summary>
    [Fact]
    public void TheCeilingBoundaryExcludesEquality_WhereTheSlotWallIncludesIt()
    {
        Assert.Equal(
            TimescaleSupport.RefreshCeilingFreshness.CeilingHolds,
            TimescaleSupport.ClassifyRefreshCeilingFreshness(Ceiling, Ceiling));

        Assert.Equal(
            TimescaleSupport.RefreshCeilingFreshness.CeilingFalsified,
            TimescaleSupport.ClassifyRefreshCeilingFreshness(Ceiling + 1, Ceiling));

        /* The contrast, from the shipped sibling: AT the slot width is already a breach. */
        Assert.Equal(
            TimescaleSupport.RefreshSlotHeadroom.SlotExceeded,
            TimescaleSupport.ClassifyRefreshSlotHeadroom(Slot));

        /* And the constant's own recorded value logs NOTHING, which is the consequence that matters: the
           sizing figure arrives once an hour on a store sitting at its recorded ceiling, and a finding that
           fired on it would be noise from the first tick. */
        var log = new CapturingTestLogger();
        TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, Ceiling,
            new RefreshCeilingStalenessWatch(), log);
        Assert.Equal("(no log lines captured)", log.Joined);
    }

    /// <summary>
    /// An impossible reading costs the LINE, never the sweep — the posture
    /// <see cref="TimescaleSupport.ClassifyRefreshSlotHeadroom"/> and
    /// <see cref="TimescaleSupport.ClassifyCompressionClearance"/> both take.
    ///
    /// <para>NaN is asserted explicitly even though <c>NaN &gt; x</c> is false anyway: the answer has to be a
    /// DECISION in the code, not a property of IEEE comparison that a later refactor could invert without
    /// anything noticing.</para>
    /// </summary>
    [Fact]
    public void AnImpossibleReading_HoldsTheCeiling_AndSaysNothing()
    {
        Assert.Equal(
            TimescaleSupport.RefreshCeilingFreshness.CeilingHolds,
            TimescaleSupport.ClassifyRefreshCeilingFreshness(double.NaN, Ceiling));
        Assert.Equal(
            TimescaleSupport.RefreshCeilingFreshness.CeilingHolds,
            TimescaleSupport.ClassifyRefreshCeilingFreshness(-1d, Ceiling));

        var log = new CapturingTestLogger();
        TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, double.NaN,
            new RefreshCeilingStalenessWatch(), log);
        TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, -1d,
            new RefreshCeilingStalenessWatch(), log);
        Assert.Equal("(no log lines captured)", log.Joined);

        /* AND THE LOAD-BEARING HALF, which is the limiter rather than the classifier. Every comparison
           against NaN is false, so a NaN allowed to become the high-water mark answers "report" for every
           later reading too — one impossible catalog reading would disable the rate limit for the life of
           the process. Asserted against ShouldReport DIRECTLY, because that is the surface where the value
           can arrive: the classifier's own comparison already answers CeilingHolds for a non-finite reading,
           so a guard there would be unreachable and a test on it would pass with the guard deleted. */
        var watch = new RefreshCeilingStalenessWatch();
        Assert.False(watch.ShouldReport(HeaviestConstant, double.NaN));
        Assert.False(watch.ShouldReport(HeaviestConstant, double.PositiveInfinity));
        Assert.Null(watch.ReportedHighWaterMark(HeaviestConstant));

        /* And a real falsification afterwards behaves as though the nonsense never arrived. */
        Assert.True(watch.ShouldReport(HeaviestConstant, Ceiling + 1));
        Assert.Equal(Ceiling + 1, watch.ReportedHighWaterMark(HeaviestConstant));
        Assert.False(watch.ShouldReport(HeaviestConstant, Ceiling + 1));
    }

    /// <summary>
    /// The rate limit: the first falsifying reading reports, and after that only one LARGER than the largest
    /// already reported.
    ///
    /// <para><b>Why a high-water mark rather than a latch or a time window.</b> What this finding asks for is
    /// a constant re-derived to at least the largest run on record, so a new record changes the answer and a
    /// repeat does not. A once-per-process latch would hide the reading that actually sizes the
    /// re-derivation behind whichever one happened to arrive first, and a time window would re-report a value
    /// already reported on a timer — which is how an hourly Warning becomes furniture.</para>
    ///
    /// <para>The mark is read directly rather than inferred from the log, because a test that could only
    /// count lines would be testing the message.</para>
    /// </summary>
    [Fact]
    public void TheRateLimit_ReportsTheFirstFalsification_ThenOnlyALargerRun()
    {
        var watch = new RefreshCeilingStalenessWatch();

        Assert.Null(watch.ReportedHighWaterMark(HeaviestConstant));

        Assert.True(watch.ShouldReport(HeaviestConstant, Ceiling + 1));
        Assert.Equal(Ceiling + 1, watch.ReportedHighWaterMark(HeaviestConstant));

        /* The same reading again says nothing. */
        Assert.False(watch.ShouldReport(HeaviestConstant, Ceiling + 1));

        /* A LARGER run does, because it changes what the constant has to be re-derived to. */
        Assert.True(watch.ShouldReport(HeaviestConstant, Ceiling + 5));
        Assert.Equal(Ceiling + 5, watch.ReportedHighWaterMark(HeaviestConstant));

        /* A smaller one afterwards does not, and the mark does NOT fall back to it. The mark is a record of
           the largest value already said out loud, not a measure of current load, so load falling cannot
           make the finding start repeating itself. */
        Assert.False(watch.ShouldReport(HeaviestConstant, Ceiling + 3));
        Assert.Equal(Ceiling + 5, watch.ReportedHighWaterMark(HeaviestConstant));

        Assert.True(watch.ShouldReport(HeaviestConstant, Ceiling + 9));
        Assert.Equal(Ceiling + 9, watch.ReportedHighWaterMark(HeaviestConstant));
    }

    /// <summary>
    /// The whole sequence again through the SHIPPED log path rather than through
    /// <see cref="RefreshCeilingStalenessWatch.ShouldReport"/> directly — because a limiter that works and a
    /// caller that never consults it produce identical unit results, and the wiring between them is where
    /// this kind of thing breaks.
    /// </summary>
    [Fact]
    public void TheLogPath_ConsultsTheRateLimit_RatherThanReportingEveryTick()
    {
        var watch = new RefreshCeilingStalenessWatch();
        var log = new CapturingTestLogger();

        void Report(double seconds) => TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, seconds, watch, log);

        Report(Ceiling + 1);
        Report(Ceiling + 1);
        Report(Ceiling + 1);

        Assert.Equal(1, CountLines(log));

        Report(Ceiling + 7);
        Assert.Equal(2, CountLines(log));

        Report(Ceiling + 2);
        Assert.Equal(2, CountLines(log));
    }

    /// <summary>
    /// The rate limit is keyed on the CONSTANT, not on the view — which matters only for the light ceiling
    /// and matters a lot there: one constant
    /// (<see cref="TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds"/>) covers the twelve other
    /// hourly views, so keying on the view would let each of them report the same constant's staleness
    /// independently and the limit would be twelve times looser than it reads.
    ///
    /// <para>The two constants are independent of each other, which is the other half: a falsification of
    /// one must not silence the other.</para>
    /// </summary>
    [Fact]
    public void TheRateLimit_IsKeyedOnTheConstant_NotOnTheView()
    {
        var lightCeiling = TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds;
        var lightViews = TimescaleSupport.HourlyRefreshPhaseOrder
            .Where(v => !string.Equals(v, TimescaleSupport.HeaviestHourlyRefreshView, StringComparison.Ordinal))
            .ToArray();

        Assert.True(
            lightViews.Length > 1,
            "there is at most one light hourly view, so a view-keyed limit would be indistinguishable from a "
            + "constant-keyed one and this case covers nothing");

        var watch = new RefreshCeilingStalenessWatch();
        var log = new CapturingTestLogger();

        foreach (var view in lightViews)
        {
            TimescaleSupport.LogRefreshCeilingStaleness(
                LightConstant, lightCeiling, view, lightCeiling + 1d, watch, log);
        }

        Assert.Equal(1, CountLines(log));

        /* And the heaviest constant is untouched by all of that. */
        Assert.Null(watch.ReportedHighWaterMark(HeaviestConstant));
        TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, Ceiling + 1, watch, log);
        Assert.Equal(2, CountLines(log));
    }

    /// <summary>
    /// The two findings cannot be read as each other. The staleness line asks for the CONSTANT to be
    /// re-derived and cites #3182; the slot breach line asks for the GRID to be re-derived and cites #3035.
    /// Neither remedy appears in the other's line.
    ///
    /// <para>This is the pin that stops the finding being "collapsed" back into the band watch by a later
    /// simplification — two log lines whose text is interchangeable are one signal with two spellings, and
    /// the whole of #3182 is that these are two facts.</para>
    /// </summary>
    [Fact]
    public void TheTwoFindings_NameDifferentRemedies()
    {
        var staleness = new CapturingTestLogger();
        TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView, Ceiling + 1,
            new RefreshCeilingStalenessWatch(), staleness);

        Assert.Contains("#3182", staleness.Joined, StringComparison.Ordinal);
        Assert.Contains(HeaviestConstant, staleness.Joined, StringComparison.Ordinal);

        var breach = new CapturingTestLogger();
        TimescaleSupport.LogHeaviestRefreshSlotHeadroom(
            new HeaviestRefreshSlotReading(TimescaleSupport.HeaviestHourlyRefreshView, Slot), breach);

        Assert.StartsWith("Error:", breach.Joined, StringComparison.Ordinal);
        Assert.Contains("#3035", breach.Joined, StringComparison.Ordinal);

        /* Each line names its own subject and NOT the other's, so an operator reading one is not sent to the
           other's repair. */
        Assert.DoesNotContain(HeaviestConstant, breach.Joined, StringComparison.Ordinal);

        /* And the levels differ, which is the load-bearing half: levelling the staleness finding Error too
           would make a stale constant indistinguishable from a violated precondition on every log surface
           there is. */
        Assert.DoesNotContain("Error:", staleness.Joined, StringComparison.Ordinal);
    }

    /// <summary>
    /// The light ceiling now HAS a live feed keyed on the view, and it is the same statement as the heaviest
    /// one with its view filter inverted.
    ///
    /// <para><b>Why the filter-inversion equality is the pin.</b> The measured OR-join is shared between the
    /// two statements, so asserting that both carry both of its arms would be an assertion about a shared
    /// constant and could not fail. What CAN fail is the two statements diverging — one of them re-spelled,
    /// re-joined, or given a different projection — and the inversion equality is red the moment they differ
    /// anywhere other than that one operator.</para>
    /// </summary>
    [Fact]
    public void TheLightRefreshRead_IsTheHeaviestReadWithItsViewFilterInverted()
    {
        var heaviest = TimescaleSupport.HeaviestRefreshRuntimeSql;
        var light = TimescaleSupport.OtherHourlyRefreshRuntimesSql;

        var include = $"ca.view_name = '{TimescaleSupport.HeaviestHourlyRefreshView}'";
        var exclude = $"ca.view_name <> '{TimescaleSupport.HeaviestHourlyRefreshView}'";

        Assert.Contains(include, heaviest, StringComparison.Ordinal);
        Assert.Contains(exclude, light, StringComparison.Ordinal);

        Assert.Equal(light, heaviest.Replace(include, exclude, StringComparison.Ordinal));

        /* Both keep the status filter, for the reason every sibling read has it. */
        Assert.Contains("js.last_run_status = 'Success'", light, StringComparison.Ordinal);

        /* And the projection is the VIEW name, which is the identity the grid is keyed on — a job id would
           name a different job on any other deployment. */
        Assert.Contains("ca.view_name,", light, StringComparison.Ordinal);
        Assert.DoesNotContain("job_id =", light, StringComparison.Ordinal);
    }

    /// <summary>
    /// The finding is WIRED, checked against the real service source. A classifier, a limiter and a log
    /// method that nothing calls are individually perfect and jointly inert — the failure shape this repo
    /// already parses <c>DarlingStoreUpgrade.cs</c> and the Kestrel hosts for.
    ///
    /// <para>Both constants have to be wired, and the light one needs its READ wired too, since it had no
    /// live reading anywhere in the product before #3182.</para>
    /// </summary>
    [Fact]
    public void BothCeilingsAreWiredIntoTheHourlySweep()
    {
        var worker = ReadDarlingWorkerSource();

        Assert.Contains(
            nameof(TimescaleSupport.LogRefreshCeilingStaleness), worker, StringComparison.Ordinal);
        Assert.Contains(
            nameof(TimescaleSupport.HeaviestRefreshCeilingConstantName), worker, StringComparison.Ordinal);
        Assert.Contains(
            nameof(TimescaleSupport.OtherRefreshCeilingConstantName), worker, StringComparison.Ordinal);
        Assert.Contains(
            nameof(TimescaleSupport.ReadOtherHourlyRefreshRuntimesAsync), worker, StringComparison.Ordinal);

        /* The limiter has to be a FIELD, not a local: one constructed inside the sweep is recreated every
           tick and limits nothing, which is the one wiring mistake here that leaves every unit case green
           while the log fills up hourly. A field declaration is the shape that cannot coexist with a
           per-sweep local, so its presence is the check. */
        Assert.Contains(
            $"private readonly {nameof(RefreshCeilingStalenessWatch)} ", worker, StringComparison.Ordinal);
        Assert.DoesNotContain(
            $"new {nameof(RefreshCeilingStalenessWatch)}()", ServiceSweepBody(worker),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The sweep's own comment does not describe <see cref="TimescaleSupport.CompressionPhaseGuardMinutes"/>
    /// as the light-refresh ceiling rounded up (#3188) — a PRESENT-TENSE claim about a derivation the code
    /// no longer has.
    ///
    /// <para><b>Why this is a test and why it lives here.</b> #3188 declared that width and swept every
    /// site in <c>TimescaleSupport.cs</c> to say the relationship in the past tense. It did not sweep the
    /// CALL SITE, which sat in another project and went on telling a reader that the ceiling was arithmetic
    /// INPUT to the width. That is the drift the whole change is about, one file over — and the prose pins
    /// in <c>RefreshCeilingProvenancePinTests</c> could not see it, because they read the doc runs of the
    /// declaration rather than the comments of its callers. This file already reads the real worker source
    /// for the wiring case above, so the check goes where the reader already is.</para>
    ///
    /// <para><b>A SHAPE with a stated bound, not a list of wordings.</b> The claim is matched as
    /// "<c>CompressionPhaseGuardMinutes</c> … <c>rounded up</c>" within 160 characters, and a match is
    /// allowed only when a past-tense marker sits between the two — so a rewording passes only if it is
    /// honest about tense. The window is bounded and the scan stops at the next mention of the member, so a
    /// match cannot run from one comment through unrelated code into another. <b>What it does NOT cover:</b>
    /// this reads the worker and nothing else. <c>TimescaleSupport.cs</c>' half is
    /// <see cref="RefreshCeilingProvenancePinTests.NoGuardWidth_IsDerivedFromAMeasurement"/>, which forbids
    /// the expression itself, and every other project is unchecked. The split is named rather than implied,
    /// because "the file I happened to sweep" is exactly how this defect got here.</para>
    ///
    /// <para>The positive clause is what stops a comment that DROPPED the relationship from passing: the
    /// sweep reads that ceiling because it is what the declared width is checked against, and a reader not
    /// told which way that goes will read the finding's remedy as "re-derive the guard".</para>
    /// </summary>
    [Fact]
    public void TheSweepsComment_DoesNotDescribeTheGuardWidthAsDerivedFromTheCeiling()
    {
        var worker = ReadDarlingWorkerSource();

        var stale = GuardDescribedAsRoundedUp.Matches(worker)
            .Where(match => !PastTenseMarkers.Any(
                marker => match.Groups["between"].Value.Contains(marker, StringComparison.OrdinalIgnoreCase)))
            .Select(match => match.Value)
            .ToArray();

        Assert.True(stale.Length == 0,
            "DarlingWorker.cs describes CompressionPhaseGuardMinutes as the light-refresh ceiling rounded up, "
            + "in the present tense. That width is DECLARED since #3188 and derives from no measurement, so "
            + "the comment states something false about the code beside it — and it points a reader at the "
            + "wrong remedy, because a ceiling past the width is now a scheduling decision rather than a "
            + "renumbering. Say it in the past tense or say what the relationship is now: "
            + string.Join(" | ", stale));

        Assert.Contains("DECLARED width is checked against", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// The derivation claim as a bounded shape. Non-greedy and stopped at the next mention of the member, so
    /// one match spans one comment rather than reaching across unrelated code to find the words it wants.
    /// </summary>
    private static readonly Regex GuardDescribedAsRoundedUp = new(
        @"CompressionPhaseGuardMinutes(?<between>(?:(?!CompressionPhaseGuardMinutes).){0,160}?)rounded up",
        RegexOptions.Compiled | RegexOptions.Singleline);

    /// <summary>
    /// What makes a "rounded up" mention honest: it is describing what the width USED TO BE. Matched
    /// case-insensitively, because the file emphasises with capitals and a case-sensitive allowance would
    /// flag correct prose for its typography.
    /// </summary>
    private static readonly string[] PastTenseMarkers = ["used to", "it used", "was ", "before #3188"];

    /// <summary>
    /// A MEASURED reading, quoted as evidence rather than derived: the clean 17:00Z run of 2026-09-08, whose
    /// existence is the concrete demonstration that the recorded ceiling was overtaken from inside the
    /// routine band (#3182).
    ///
    /// <para><b>This case is written to EXPIRE, and the expiry is the useful part.</b> Its guards require the
    /// quoted run to still be ABOVE the recorded ceiling and BELOW the watch line. A re-derivation that
    /// raised the ceiling past this run turns it red — correctly, because the reading would then no longer
    /// demonstrate anything and the evidence has to be re-read rather than the case silently continuing to
    /// pass on a premise that has gone. Nothing else in this file depends on the value.</para>
    /// </summary>
    [Fact]
    public void TheMeasuredRunThatDemonstratedTheDefect_StillFalsifiesTheRecordedCeiling()
    {
        const double MeasuredCleanRunSeconds = 952d;

        Assert.True(
            MeasuredCleanRunSeconds > Ceiling,
            $"the quoted {MeasuredCleanRunSeconds} s run is no longer above the {Ceiling} s recorded "
            + "ceiling, so it no longer demonstrates the defect this file guards. Either the constant was "
            + "re-derived over a population that contains this run — in which case quote a run that is "
            + "still outside it, or drop this case and keep the derived ones — or the reading is being "
            + "cited against a constant it was not measured against (#3182)");

        Assert.True(
            MeasuredCleanRunSeconds < WatchLine,
            $"the quoted {MeasuredCleanRunSeconds} s run is at or past the {WatchLine} s watch line, so it "
            + "no longer demonstrates a falsification from INSIDE the routine band, which is the specific "
            + "invisibility #3182 is about");

        Assert.Equal(
            TimescaleSupport.RefreshSlotHeadroom.InsideSlot,
            TimescaleSupport.ClassifyRefreshSlotHeadroom(MeasuredCleanRunSeconds));

        Assert.Equal(
            TimescaleSupport.RefreshCeilingFreshness.CeilingFalsified,
            TimescaleSupport.ClassifyRefreshCeilingFreshness(MeasuredCleanRunSeconds, Ceiling));

        var log = new CapturingTestLogger();
        TimescaleSupport.LogRefreshCeilingStaleness(
            HeaviestConstant, Ceiling, TimescaleSupport.HeaviestHourlyRefreshView,
            MeasuredCleanRunSeconds, new RefreshCeilingStalenessWatch(), log);
        Assert.StartsWith("Warning:", log.Joined, StringComparison.Ordinal);
    }

    /// <summary>
    /// The body of the hourly compression-job-health sweep — the method the finding is called from — so
    /// "the limiter is not constructed per sweep" can be asserted against the code that would construct it
    /// rather than against the whole file, where the field's own initializer is a false positive.
    /// </summary>
    private static string ServiceSweepBody(string worker)
    {
        const string Signature = "private async Task EvaluateCompressionJobHealthAsync(";
        var start = worker.IndexOf(Signature, StringComparison.Ordinal);
        Assert.True(start >= 0,
            $"'{Signature}' was not found in DarlingWorker.cs, so this guard is reading a sweep that no "
            + "longer exists under that name — find where the #3182 finding is called from and point it "
            + "there rather than dropping the assertion");

        var end = worker.IndexOf("\r\n    private ", start + Signature.Length, StringComparison.Ordinal);
        return end > start ? worker[start..end] : worker[start..];
    }

    /// <summary>
    /// The feasibility model AGREES WITH THE SHIPPED GRID at the live guard, which is the only thing that
    /// makes it worth consulting. A second, kinder model of the same arithmetic would answer "feasible"
    /// where the build answers "red", and a bound that disagrees with the thing it bounds is worse than no
    /// bound at all.
    /// </summary>
    [Fact]
    public void TheFeasibilityModel_ReproducesTheShippedGridAtTheLiveGuard()
    {
        Assert.Equal(
            TimescaleSupport.HeaviestRefreshWindowMinutes,
            TimescaleSupport.GuardAndWindowSharedMinutes - TimescaleSupport.CompressionPhaseGuardMinutes);

        Assert.Equal(
            WatchLine,
            TimescaleSupport.RefreshSlotWarningSecondsForGuardMinutes(
                TimescaleSupport.CompressionPhaseGuardMinutes));
    }

    /// <summary>
    /// The shipped guard is inside the bound the hour can carry — the check that turns "48 minutes of guard
    /// does not fit in 60" from a paragraph into a build failure (#3182).
    ///
    /// <para><b>What this goes red on, said plainly, because that is the whole reason it exists.</b> The
    /// guard is <see cref="TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds"/> rounded up to a whole
    /// minute; that ceiling is a maximum over a long-tailed series, so the derivation has no upper bound of
    /// its own while the hour does. Re-derive that constant above
    /// <see cref="TimescaleSupport.WidestFeasibleOtherRefreshCeilingSeconds"/> and this is red — which is
    /// the correct outcome and not an obstacle: at that point the guard the measurement asks for and the
    /// window the heaviest refresh needs cannot both be had, and that is a scheduling decision rather than a
    /// renumbering.</para>
    /// </summary>
    [Fact]
    public void TheShippedGuard_FitsInsideTheHourItSharesWithTheHeaviestWindow()
    {
        Assert.True(
            TimescaleSupport.WidestFeasibleCompressionPhaseGuardMinutes >= 0,
            $"no guard width at all leaves a window wide enough for the {Ceiling} s recorded ceiling, so the "
            + "hour cannot contain this grid at ANY guard. That is not a guard to narrow: the repair is a "
            + "cheaper refresh or a longer cadence for that one aggregate, and neither is a constant to "
            + "re-derive (#3035, #3182)");

        Assert.True(
            TimescaleSupport.CompressionPhaseGuardMinutes
                <= TimescaleSupport.WidestFeasibleCompressionPhaseGuardMinutes,
            $"the guard is {TimescaleSupport.CompressionPhaseGuardMinutes} minutes against a feasible "
            + $"maximum of {TimescaleSupport.WidestFeasibleCompressionPhaseGuardMinutes}, so the light "
            + "refreshes' guard and the heaviest refresh's window are both claiming minutes the hour does "
            + "not have. Do not widen the hour — there is none to take (#3182)");

        Assert.True(
            TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds
                <= TimescaleSupport.WidestFeasibleOtherRefreshCeilingSeconds,
            $"the light-refresh ceiling is {TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds} s "
            + $"against the {TimescaleSupport.WidestFeasibleOtherRefreshCeilingSeconds} s the grid can "
            + "absorb. A census maximum above that line is a statement that the two bands cannot both be "
            + "satisfied, not a constant to update (#3182)");
    }

    /// <summary>
    /// The bound is TIGHT, which is what stops it being a wider allowance than it reads: the widest feasible
    /// guard is feasible and one minute more is not.
    ///
    /// <para>An off-by-one in the search would leave the bound either one minute pessimistic — costing a
    /// minute of guard nobody chose to give up — or one minute optimistic, which is the direction that
    /// matters: a bound that admits an infeasible guard is a check that certifies the defect.</para>
    /// </summary>
    [Fact]
    public void TheFeasibleGuardBound_IsTight()
    {
        var widest = TimescaleSupport.WidestFeasibleCompressionPhaseGuardMinutes;
        Assert.True(widest >= 0);

        Assert.True(
            Ceiling < TimescaleSupport.RefreshSlotWarningSecondsForGuardMinutes(widest),
            $"the guard reported as widest-feasible ({widest} minutes) leaves a watch line of "
            + $"{TimescaleSupport.RefreshSlotWarningSecondsForGuardMinutes(widest)} s, which the {Ceiling} s "
            + "ceiling is already at or past — so the bound admits an infeasible guard");

        Assert.False(
            Ceiling < TimescaleSupport.RefreshSlotWarningSecondsForGuardMinutes(widest + 1),
            $"a guard of {widest + 1} minutes is also feasible, so the bound is pessimistic by at least a "
            + "minute and the grid is giving up guard width nobody decided to give up");
    }

    /// <summary>
    /// THE LIVE HALF OF #3185's MEMBERSHIP RULE: a light refresh that outran the gap its cost class was
    /// given is reported, whichever class the rule put it in.
    ///
    /// <para><b>The defect this exists for is one no build-time test can reach.</b>
    /// <see cref="TimescaleSupport.IsUnboundedCardinalityRefresh"/> decides how much of the light band a
    /// view gets by reading its GROUP BY, and a view can be slow for a reason its group key does not show.
    /// A hand-kept list of heavy views goes stale LOUDLY — on the next registration, where a reviewer sees
    /// it. A rule that has stopped predicting goes stale QUIETLY, forever, which is strictly worse and is
    /// why the rule needs a live falsifier rather than only a build-time one.</para>
    ///
    /// <para><b>And it is not covered by the ceiling finding above.</b> A deployment-bounded member at
    /// 100 s outran its 60 s step while sitting a long way under
    /// <see cref="TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds"/>, so
    /// <see cref="TimescaleSupport.LogRefreshCeilingStaleness"/> is silent on precisely the reading this is
    /// for. Both probes here are derived from
    /// <see cref="TimescaleSupport.LightRefreshSpacingMinutesFor"/> so they follow the shipped gap at any
    /// width it takes.</para>
    /// </summary>
    [Fact]
    public void ALightRefreshPastItsClassGap_IsReported_AndOneAtTheGapIsNot()
    {
        var bounded = TimescaleSupport.MemoryBaselineView;
        var unbounded = TimescaleSupport.QueryStoreStatsHourlyView;

        Assert.False(TimescaleSupport.IsUnboundedCardinalityRefresh(bounded));
        Assert.True(TimescaleSupport.IsUnboundedCardinalityRefresh(unbounded));

        var boundedGap = TimescaleSupport.LightRefreshSpacingMinutesFor(bounded) * 60;
        var unboundedGap = TimescaleSupport.LightRefreshSpacingMinutesFor(unbounded) * 60;

        /* The two classes get DIFFERENT gaps, or the finding cannot tell the two remedies apart and the
           layout it reports against is not the layout that shipped. */
        Assert.True(
            unboundedGap > boundedGap,
            "the two cost classes are given the same gap, so separating the unbounded members bought "
            + "nothing and every assertion below reduces to one case (#3185)");

        /* AT the gap says nothing: a run that took exactly its gap finished as the next one began. */
        Assert.False(TimescaleSupport.LightRefreshRunExceedsItsSpacing(bounded, boundedGap));
        var quiet = new CapturingTestLogger();
        TimescaleSupport.LogLightRefreshSpacingBreach(
            bounded, boundedGap, new RefreshCeilingStalenessWatch(), quiet);
        Assert.Equal("(no log lines captured)", quiet.Joined);

        /* One second past it reports, at Warning, naming the constant whose repair is the CLASSIFICATION. */
        Assert.True(TimescaleSupport.LightRefreshRunExceedsItsSpacing(bounded, boundedGap + 1));
        var boundedLog = new CapturingTestLogger();
        TimescaleSupport.LogLightRefreshSpacingBreach(
            bounded, boundedGap + 1, new RefreshCeilingStalenessWatch(), boundedLog);
        Assert.StartsWith("Warning:", boundedLog.Joined, StringComparison.Ordinal);
        Assert.Contains(bounded, boundedLog.Joined, StringComparison.Ordinal);
        Assert.Contains(
            nameof(TimescaleSupport.LightRefreshStepMinutes), boundedLog.Joined, StringComparison.Ordinal);

        /* AND THE READING THE CEILING FINDING CANNOT SEE, which is the whole reason this is a second
           finding: the same run is a long way under the recorded light ceiling, so that watch says nothing
           about it. */
        Assert.True(boundedGap + 1 < TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds);
        var ceilingSilent = new CapturingTestLogger();
        TimescaleSupport.LogRefreshCeilingStaleness(
            TimescaleSupport.OtherRefreshCeilingConstantName,
            TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds,
            bounded, boundedGap + 1, new RefreshCeilingStalenessWatch(), ceilingSilent);
        Assert.Equal("(no log lines captured)", ceilingSilent.Joined);

        /* An unbounded member's own gap is wider, so the bounded probe is silent for it — the classes are
           judged against their own room and not against a single line. */
        Assert.False(TimescaleSupport.LightRefreshRunExceedsItsSpacing(unbounded, boundedGap + 1));

        /* Past ITS gap it reports, naming the constant whose repair is the SEPARATION. */
        var unboundedLog = new CapturingTestLogger();
        TimescaleSupport.LogLightRefreshSpacingBreach(
            unbounded, unboundedGap + 1, new RefreshCeilingStalenessWatch(), unboundedLog);
        Assert.StartsWith("Warning:", unboundedLog.Joined, StringComparison.Ordinal);
        Assert.Contains(
            nameof(TimescaleSupport.UnboundedLightRefreshSeparationMinutes),
            unboundedLog.Joined,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The two cost classes are rate-limited under DIFFERENT keys, so the larger breach cannot suppress the
    /// smaller one.
    ///
    /// <para><b>This is the one thing a shared high-water mark must not do when the findings are not the
    /// same finding.</b> An unbounded member's breach is reported in hundreds of seconds and a bounded
    /// member's in tens, so one key would mean the first unbounded breach permanently silences every
    /// bounded one — and the bounded one is the finding that says the CLASSIFICATION is wrong, which is the
    /// half no build-time rule covers. Asserted in that order deliberately: the large reading first, so a
    /// single shared mark would already be above the small one when it arrives.</para>
    /// </summary>
    [Fact]
    public void TheTwoCostClasses_DoNotSuppressEachOther_UnderOneWatch()
    {
        var bounded = TimescaleSupport.MemoryBaselineView;
        var unbounded = TimescaleSupport.QueryStoreStatsHourlyView;

        Assert.NotEqual(
            TimescaleSupport.LightRefreshSpacingConstantNameFor(bounded),
            TimescaleSupport.LightRefreshSpacingConstantNameFor(unbounded));

        var watch = new RefreshCeilingStalenessWatch();
        var log = new CapturingTestLogger();

        var large = (TimescaleSupport.LightRefreshSpacingMinutesFor(unbounded) * 60) + 100;
        var small = (TimescaleSupport.LightRefreshSpacingMinutesFor(bounded) * 60) + 1;

        Assert.True(large > small, "the large probe is not larger, so a shared mark would not suppress");

        TimescaleSupport.LogLightRefreshSpacingBreach(unbounded, large, watch, log);
        TimescaleSupport.LogLightRefreshSpacingBreach(bounded, small, watch, log);

        Assert.Equal(2, CountLines(log));

        /* And WITHIN one class the mark still holds, so the split did not cost the rate limit. */
        var repeat = new CapturingTestLogger();
        var again = new RefreshCeilingStalenessWatch();
        TimescaleSupport.LogLightRefreshSpacingBreach(bounded, small + 10, again, repeat);
        TimescaleSupport.LogLightRefreshSpacingBreach(bounded, small, again, repeat);
        Assert.Equal(1, CountLines(repeat));
    }

    /// <summary>
    /// The service sweep CALLS the spacing finding on the same per-view feed it calls the ceiling finding
    /// on, and passes the same watch instance.
    ///
    /// <para>Parsed out of the real <c>DarlingWorker.cs</c> for the reason the sibling guards in this file
    /// are: a finding that exists and is never reached is the defect it was written to remove. Asserted
    /// inside the light-refresh loop rather than merely present in the file, because the finding is
    /// per-view and a call outside that loop would report one reading an hour.</para>
    /// </summary>
    [Fact]
    public void TheSpacingFinding_IsCalledPerLightRefresh_OnTheSameWatch()
    {
        var worker = ReadDarlingWorkerSource();

        var loop = worker.IndexOf(
            nameof(TimescaleSupport.ReadOtherHourlyRefreshRuntimesAsync), StringComparison.Ordinal);
        Assert.True(loop > 0, "the light-refresh runtime loop is gone from DarlingWorker");

        var call = worker.IndexOf(
            nameof(TimescaleSupport.LogLightRefreshSpacingBreach), StringComparison.Ordinal);
        Assert.True(
            call > loop,
            "TimescaleSupport.LogLightRefreshSpacingBreach is not called after the per-view light-refresh "
            + "read, so the spacing finding either does not run or runs on something other than a per-view "
            + "reading (#3185)");

        /* The tail of that loop, so "inside it" is a claim about the loop and not about the file. */
        var body = worker[loop..];
        var close = body.IndexOf("\r\n            }", StringComparison.Ordinal);
        Assert.True(close > 0, "could not find the end of the light-refresh loop");
        Assert.Contains(
            nameof(TimescaleSupport.LogLightRefreshSpacingBreach),
            body[..close],
            StringComparison.Ordinal);
    }

    private static int CountLines(CapturingTestLogger logger) =>
        logger.Joined == "(no log lines captured)"
            ? 0
            : logger.Joined.Split(" | ", StringSplitOptions.None).Length;

    private static string ReadDarlingWorkerSource([CallerFilePath] string thisFile = "") =>
        ReadSibling(thisFile, "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

    /// <summary>
    /// A sibling project's REAL source, resolved from <c>[CallerFilePath]</c> — no fixture copy to go stale,
    /// which is the whole point of parsing source in the first place.
    /// </summary>
    private static string ReadSibling(string thisFile, string project, string file)
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", project, file));

        Assert.True(File.Exists(path),
            $"{file} was not found at '{path}'. This guard parses the REAL file (resolved from "
            + "[CallerFilePath], so there is no copy to go stale); restore the path rather than pointing it "
            + "at a fixture.");
        return File.ReadAllText(path);
    }
}
