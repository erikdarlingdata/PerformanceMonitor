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
/// Drift guard between <see cref="TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds"/>'s recorded
/// DERIVATION and the arithmetic that derivation consists of (#3069, #3101).
///
/// <para><b>Why this exists.</b> The constant is the maximum of a published population, and the doc comment
/// states the population, the exclusion rule that produced it, and the summary figures that follow from it.
/// Every one of those relationships is checkable and none of them is checked by the compiler: a population
/// written in prose has no way to notice that its own summary no longer follows from it, and a summary is
/// exactly what a later reader will cite. So the estimator is re-computed here from the series the comment
/// publishes and the constant is required to equal it, which is the difference between a value someone can
/// re-derive and a value someone has to trust.</para>
///
/// <para><b>Derive, don't restate.</b> Nothing below hardcodes the ceiling, the slot, the margin or any
/// summary figure. Every expected value comes either from the constants
/// (<see cref="TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds"/>,
/// <see cref="TimescaleSupport.RefreshPhaseSlotSeconds"/>,
/// <see cref="TimescaleSupport.HeaviestRefreshWindowMinutes"/>,
/// <see cref="TimescaleSupport.RefreshSlotWarningSeconds"/>) or from the POPULATION THE COMMENT ITSELF
/// PUBLISHES — or, for the live envelope, from a QUOTED READING the same sentence states, with the
/// figures drawn against that reading derived rather than restated. A pin that restated the summary
/// would go stale by precisely the mechanism it exists to stop.</para>
///
/// <para><b>One pin was written to expire, and it has (#3166).</b> The comment's reason for taking the
/// maximum rather than a percentile was PARTLY that at sixteen readings the 95th percentile IS the maximum by
/// nearest rank; <see cref="Verify"/> checked that, so it went red once the census grew the population past
/// the point where it held, which is the moment the maximum-versus-percentile decision genuinely had to be
/// re-taken rather than inherited. It was re-taken, the estimator is still the maximum, and the surviving
/// reason — a scheduling exclusion may accept no exceedance over its own record — does not reference the
/// population's size. So the expiring clause is gone with the argument it guarded, and the two percentiles
/// are now pinned as readings that TRACK the population. There is no replacement expiry, because a reason
/// independent of <c>n</c> has nothing left to expire; what a growing population now moves is the ceiling
/// itself, which is checked directly.</para>
///
/// <para><b>Which numeral KIND these are, because the repo handles two differently and neither #3072 nor
/// #3073 says so out loud.</b> A numeral that restates an adjacent list gets DELETION offered in its
/// failure message — dropping the sentence is as correct an outcome as correcting the figure — while a
/// numeral that is program output gets "fix the pattern rather than the count", because a transcript has
/// to keep showing what the tool really prints. <c>ReadmeDerivedCountPinTests</c> implements both halves
/// and states neither, so the rule has to be inferred from the disagreement between two of its own
/// failure texts. Every pattern here is the first kind — derived arithmetic restated in prose — which is
/// why deletion is offered throughout; and this paragraph is NARRATION of an existing rule, not a counted
/// claim, so nothing checks it and nothing should. It states no count of patterns either, for the same
/// reason: a numeral over a list that grows is the defect this file is about, one level up.</para>
///
/// <para><b>What is deliberately NOT pinned, said plainly rather than left looking covered.</b> The
/// snapshot readings are quoted evidence, not derived quantities — nothing in the code can know them — so
/// they are held only by their MEMBERSHIP of the census population (see
/// <see cref="TheInadmissibleReadings_CannotBeFoldedIntoTheStatusVerifiedSeries"/>). That is the inverse of
/// the relationship the sixteen-run record held them to, and the inversion is the finding rather than a
/// weakening (#3166): disjointness was a property of a SAMPLE, and a census of the same job contains by
/// construction whatever a snapshot of its last run reported — so a value-level test could never have
/// encoded a rule about SOURCES, and the rule is checked against the shipped SQL of all three reads by
/// <see cref="TheInadmissibilityClaim_MatchesTheShippedSql"/> instead. The
/// pre-narrowing band and the 3.8x ratio drawn against it are likewise evidence — that band is #3012's
/// measurement and no constant here spells it — so they carry no pin. The excluded run's ratio to the
/// population maximum IS pinned, because both of its terms are stated here. The live envelope's measured
/// maximum is evidence on the same footing, and its day count with it; what IS pinned is every figure the
/// prose draws AGAINST that maximum — the slot margin, the share of the slot and the distance past the
/// watch line are all arithmetic over it and the grid's own constants, plus the band the shipped
/// classifier actually returns for it, so the paragraph cannot state a verdict the code does not
/// produce.</para>
///
/// <para><b>The guard is itself guarded, three ways.</b> Every extraction asserts its pattern MATCHED before
/// a value is compared. <see cref="EveryNumericPin_ReportsAnInjectedDrift"/> bumps each captured number ONE
/// AT A TIME in a mutated copy and requires the identical verification to fail — after separately asserting
/// the pattern still matches, so a mutation that merely broke a regex cannot pass as a caught drift. And
/// <see cref="Verify"/> requires that it CONSUMED every pin in <see cref="Pins"/>, because a pattern defined
/// in the list and never asserted against anything looks exactly like a pattern doing work.</para>
///
/// <para><b>And the SCOPING rule, held file-wide rather than only where it is stated (#3133).</b>
/// <see cref="Verify"/> requires the ceiling paragraph's live-envelope population to carry a CLOSED scope;
/// <see cref="NoOpenPopulationClaim_SurvivesOutsideTheSentenceThatRejectsIt"/> requires the rest of the
/// file not to break the same rule. A rule stated in one paragraph binds nothing in the paragraphs a
/// reader meets thousands of lines away, so where it is stated is not where it needs enforcing.</para>
/// </summary>
public sealed class RefreshCeilingProvenancePinTests
{
    /// <summary>
    /// Marks a verification failure caused by a pattern no longer matching, so
    /// <see cref="EveryNumericPin_ReportsAnInjectedDrift"/> can tell "the pin caught the drift" from "the pin
    /// stopped being able to see anything" — which are the same exception otherwise, and the second of which
    /// would let the sweep report success for a guard that had gone blind.
    /// </summary>
    private const string ParseMiss = "PIN PARSE MISS";

    private const string CeilingDeclaration =
        "public const int HeaviestHourlyRefreshObservedCeilingSeconds";

    private const string CompressionMinutesDeclaration =
        "public static readonly IReadOnlyList<int> CompressionPhaseMinutes";

    private const string WarningLineDeclaration =
        "public static int RefreshSlotWarningSeconds";

    private const string LightCeilingDeclaration =
        "public const double OtherHourlyRefreshObservedCeilingSeconds";

    private const string GuardBandDeclaration =
        "public static int CompressionPhaseGuardMinutes";

    private static int Ceiling => TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds;

    /// <summary>
    /// The light-refresh ceiling in TENTHS of a second, because that constant is a <see cref="double"/> and
    /// every figure stated against it is stated to one decimal. Integer tenths throughout, so no rounding
    /// mode can move a comparison — the same reason the live-envelope clauses work in tenths.
    /// </summary>
    private static int LightCeilingTenths =>
        (int)Math.Round(TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds * 10);

    private static int Slot => TimescaleSupport.RefreshPhaseSlotSeconds;

    private static int SlotMinutes => TimescaleSupport.HeaviestRefreshWindowMinutes;

    private static int WatchLine => TimescaleSupport.RefreshSlotWarningSeconds;

    /// <summary>
    /// The pinned sentences, and the ONLY place a pattern is written down — <see cref="Verify"/> looks them up
    /// by name, so the sweep and the assertions cannot end up testing different regexes.
    ///
    /// <para>The runs pinned here are the ones stating figures derived from the ceiling or from the grid
    /// step it is sized against, wherever they live. Which run a pin reads is carried in its own
    /// <c>Declaration</c> and its pattern names words from that run, so none of them can match a similar
    /// sentence elsewhere in a four-thousand-line file. That set is deliberately NOT enumerated here:
    /// <see cref="Pins"/> is the enumeration, a prose copy of it would be a frozen list beside a growing
    /// one, and this file exists to stop exactly that.</para>
    ///
    /// <para><b>ASCII-only patterns on purpose, and the reason is <see cref="DocProseFor"/> rather than
    /// anything in the patterns themselves.</b> It normalises <c>—</c> and <c>–</c> to <c>-</c>
    /// before any pattern runs, so not one of them has to match a dash variant — and not one does.
    /// Matching a dash through a source-encoding round trip is a way for a pattern to stop matching for a
    /// reason that has nothing to do with what it guards, and normalising at the extractor removes that at
    /// the source instead of working around it thirteen times. The single literal hyphen below, in
    /// <c>([0-9]+)-second slot</c>, is a plain ASCII hyphen in the prose rather than a normalised
    /// dash.</para>
    ///
    /// <para><b>Every group is <c>[0-9]+</c> and never <c>[0-9]</c>, including the ones that read a single
    /// decimal place.</b> <see cref="EveryNumericPin_ReportsAnInjectedDrift"/> bumps a captured number
    /// WIDTH-PRESERVING, so a group holding a 9 comes back as two digits — and a one-digit group then stops
    /// matching, which that sweep reports as a broken pattern rather than as a caught drift. Widening costs
    /// no strictness, because <see cref="Verify"/> compares each captured value against the derived one
    /// either way: prose stating a tenth as two digits is caught by the comparison instead of by the
    /// regex.</para>
    /// </summary>
    private static IEnumerable<(string Name, string Declaration, string Pattern, bool DriftSwept)> Pins()
    {
        yield return (
            "the boundary timestamp",
            CeilingDeclaration,
            @"narrowing boundary of <c>([0-9][0-9]):([0-9][0-9]):([0-9][0-9])</c>",
            true);

        yield return (
            "the excluded run's clock arithmetic",
            CeilingDeclaration,
            @"<c>([0-9][0-9]):([0-9][0-9]):([0-9][0-9])</c> to <c>([0-9][0-9]):([0-9][0-9]):([0-9][0-9])</c>",
            true);

        yield return (
            "the excluded run's duration",
            CeilingDeclaration,
            @"boundary day, ([0-9]+) s, excluded for starting before the boundary",
            true);

        yield return (
            "the excluded run's ratio to the population maximum",
            CeilingDeclaration,
            @"([0-9]+)\.([0-9]+)x of the largest reading",
            true);

        yield return (
            "the boundary day's tail",
            CeilingDeclaration,
            @"every run that day after the boundary: <c>([0-9]+(?:, [0-9]+)+)</c> seconds",
            true);

        yield return (
            "the days after the boundary",
            CeilingDeclaration,
            @"The days after it: <c>([0-9]+(?:, [0-9]+)+)</c> seconds",
            true);

        yield return (
            "the population summary",
            CeilingDeclaration,
            @"Together ([0-9]+) runs spanning ([0-9]+) s to ([0-9]+) s, totalling ([0-9]+) s, median ([0-9]+) s",
            true);

        /* A TOTAL rather than a mean (#3166). 27799 s over 57 runs has no exact one-decimal form, so a
           stated mean would be a rounded figure the comparison below could only accept by rounding too -
           and a pin that rounds is a pin with a tolerance nobody wrote down. The total is exact in the unit
           the population is listed in, and it keeps every individual reading load-bearing for the same
           reason the mean did: a single digit moved anywhere in the list changes it, which is what
           EveryNumericPin_ReportsAnInjectedDrift depends on for the middle-ranked readings that move
           neither the range nor the median. */
        yield return (
            "the percentile decision",
            CeilingDeclaration,
            @"by nearest rank over ([0-9]+) readings the 95th percentile is ([0-9]+) s and the 90th is ([0-9]+) s, ([0-9]+) s and ([0-9]+) s below the maximum",
            true);

        /* ARITY-FREE, and that is the point rather than a convenience. This list gains a reading every hour
           the job runs - it went from two to three while #3077 was in review - so a pattern shaped
           "(N) s and (N) s" would encode the count and go stale on the next snapshot. Encoding a count in a
           regex is the stale-enumeration defect with a test wrapped around it, which is the exact thing this
           file exists to prevent. One group holding the whole list, flattened by Numbers().

           NOT drift-swept either: these are quoted readings, so bumping a digit only produces another number
           the code cannot contradict. What IS checked is the rule the paragraph is actually about - see
           TheInadmissibleReadings_CannotBeFoldedIntoTheStatusVerifiedSeries and the as-at scope guard in
           Verify.

           THE RELATIONSHIP TO THE POPULATION INVERTED at #3166 and the check inverted with it. These three
           used to be held DISJOINT from the published population, which held only while that population was
           a sixteen-run SAMPLE of this job. Against a CENSUS of the same job disjointness cannot hold: the
           self-metrics snapshot reads that job's last run and the census contains every run, so all three
           readings are members. Verify now requires CONTAINMENT, which is the same evidence read the right
           way round - it is what demonstrates the two series read the same runs, and therefore why a
           value-level test could never have encoded a rule about SOURCES. */
        yield return (
            "the inadmissible snapshot readings",
            CeilingDeclaration,
            @"\(([0-9]+ s(?:, [0-9]+ s)*)\) says the series stayed flat",
            false);

        yield return (
            "the low the grid is deliberately not sized against",
            CeilingDeclaration,
            @"against the ([0-9]+) s low",
            true);

        yield return (
            "the margin sentence",
            CeilingDeclaration,
            @"At ([0-9]+) s against a ([0-9]+)-second slot the margin is ([0-9]+) seconds",
            true);

        /* THE LIVE ENVELOPE (#3119). The measured maximum itself is a quoted reading and derives from
           nothing here — but it is captured anyway, because every figure stated against it is derived
           from it and from the grid's constants, so a bump to the reading has to disagree with all of
           them rather than pass as a second opinion. Every group is a pair, since each figure is stated
           to one decimal and the verification works in tenths. */
        yield return (
            "the measured live envelope",
            CeilingDeclaration,
            @"this job's maximum was ([0-9]+)\.([0-9]+) s\. That leaves ([0-9]+)\.([0-9]+) s of the slot, ([0-9]+)\.([0-9]+)% of it, and sits ([0-9]+)\.([0-9]+) s BELOW",
            true);

        yield return (
            "the exclude-whole occupancy sentence",
            CompressionMinutesDeclaration,
            @"occupies ([0-9]+) of the ([0-9]+) seconds",
            true);

        yield return (
            "the guard-band arithmetic the exclusion rests on",
            CompressionMinutesDeclaration,
            @"band is ([0-9]+), so applying the ordinary band to this window would admit ([0-9]+) minutes that sit INSIDE the refresh",
            true);

        yield return (
            "the minutes left on the table",
            CompressionMinutesDeclaration,
            @"The other ([0-9]+) minutes of the window are past the refresh",
            true);

        /* The watch line's REJECTED alternative. The figure is derived arithmetic - the slot less one guard
           band - so it is pinned to that arithmetic here, and the PATTERN spans the ordering words the
           prose rejects it on, so a rejection restated as a tally of readings is a parse miss rather than
           a silent pass. Whether that ordering still HOLDS is asserted against the constants and the
           shipped classifier in TimescaleSupportTests, where it is reachable: Verify is a fail-fast chain
           and a constant moved far enough to flip the ordering trips an earlier clause first, so a copy of
           the comparison here would be a pin that cannot fail.

           An ordering rather than a share of the readings, and that is the pin's whole point (#3107). A
           share of a population is not a stable reason for a threshold: it changes as the population grows
           without the threshold or the decision changing at all, so a pin on it would go red for a reason
           that is not a defect - and a pin on it going GREEN says nothing about whether the decision still
           holds. The ordering has neither property. */
        yield return (
            "the rejected alternative watch line",
            WarningLineDeclaration,
            @"<see cref=""CompressionPhaseGuardMinutes""/> band, ([0-9]+) s, and it now sits ABOVE <see cref=""HeaviestHourlyRefreshObservedCeilingSeconds""/>",
            true);

        yield return (
            "the population size and range the exclusion defers to",
            CompressionMinutesDeclaration,
            @"population is ([0-9]+) readings and still moving \(([0-9]+) s to ([0-9]+) s within the clean regime\)",
            true);

        /* ---- the LIGHT refresh ceiling's own census (#3174) ---- */

        /* The estimator and the population's shape. The maximum is held to the constant, the run count to
           the exclusion clause below, and the view count to the product's own light-policy count - so all
           four captured numbers are load-bearing and the drift sweep can reach every one of them. */
        yield return (
            "the light-refresh census",
            LightCeilingDeclaration,
            @"the maximum is ([0-9]+)\.([0-9]+) s over ([0-9]+) runs of ([0-9]+) views",
            true);

        /* The exclusion CONTROL, which is what says the succeeded/finish filter is not selecting the
           population: both figures are the same number, and that number is the census count. A filter that
           had quietly removed a status-selected part would state two different ones. */
        yield return (
            "the light-refresh census exclusion count",
            LightCeilingDeclaration,
            @"the succeeded/finish filter \(([0-9]+) of ([0-9]+)\)",
            true);

        /* NOT drift-swept: quoted quantiles of the named read, which derive from nothing here, so bumping a
           digit only produces another reading the code cannot contradict - the same treatment the heaviest
           ceiling's snapshot series gets. What IS checked is the ORDERING they are quoted for: the estimator
           is the maximum precisely because the percentiles sit well below it. */
        yield return (
            "the light-refresh quantiles",
            LightCeilingDeclaration,
            @"95th percentile ([0-9]+)\.([0-9]+) s and median ([0-9]+)\.([0-9]+) s",
            false);

        /* The gap between them, which IS derived - from the two figures above - and is the sentence the
           estimator choice actually rests on. */
        yield return (
            "the light-refresh percentile gap",
            LightCeilingDeclaration,
            @"the 95th percentile - ([0-9]+)\.([0-9]+) s over a population",
            true);

        yield return (
            "the midnight maximum",
            LightCeilingDeclaration,
            @"([0-9]+)\.([0-9]+) s on <c>2026-09-08</c> against",
            true);

        /* NOT drift-swept, and the reason is arithmetic rather than a preference: the previous night's
           reading is a quoted figure, and a one-tenth bump to it leaves the truncated percentage unchanged
           (66.3 s over 160.5 s is still 41%), so a sweep case on it would report a caught drift the check
           cannot see. The percentage itself IS checked against the two readings, which is the claim the
           sentence makes. */
        yield return (
            "the midnight night-over-night growth",
            LightCeilingDeclaration,
            @"against ([0-9]+)\.([0-9]+) s on <c>2026-09-07</c>, ([0-9]+)% higher night over night",
            false);

        /* NOT drift-swept, for the same reason: a quoted reading whose only checkable property is its
           position in the order the sentence claims for it. */
        yield return (
            "the third-largest light run",
            LightCeilingDeclaration,
            @"The third-largest run is ([0-9]+)\.([0-9]+) s",
            false);

        /* The guard band the light ceiling now SIZES, rather than merely characterises, and the whole of the
           margin that sizing leaves. Both are derived, so both are swept. */
        yield return (
            "the guard band the light ceiling derives",
            GuardBandDeclaration,
            @"so ([0-9]+) minutes against a ([0-9]+)\.([0-9]+) s ceiling",
            true);

        yield return (
            "the guard band's rounding margin",
            GuardBandDeclaration,
            @"rounding is the whole margin, and it is ([0-9]+)\.([0-9]+) s",
            true);
    }

    [Fact]
    public void EveryDerivationClaim_FollowsFromTheConstantsAndThePublishedPopulation()
    {
        Verify(ReadTimescaleSupportSource());
    }

    /// <summary>
    /// The paragraph's whole job is to keep the unfiltered self-metrics readings OUT of the SOURCES a
    /// constant may be set from while stating what they corroborate, so the mutation is a reading with no
    /// run behind it rather than a digit bump: put a value the census does not contain in the snapshot
    /// list's place and require verification to fail.
    ///
    /// <para><b>The relationship checked here inverted at #3166, and the mutation inverted with it.</b> It
    /// used to be DISJOINTNESS, which held only while the published population was a sixteen-run sample of
    /// this job — so the mutation then was to inject a population member. Against a CENSUS of the same job
    /// disjointness is impossible: the snapshot reads that job's last run, and every run is in the census.
    /// What is checkable is CONTAINMENT, so the mutation is a NON-member, and the failure it forces is the
    /// one worth having: a quoted reading with no census run behind it means the two reads disagree about
    /// what the job did.</para>
    ///
    /// <para>Both populations are asserted NON-EMPTY first. Containment over an empty snapshot list is
    /// vacuously true and an empty list is also exactly what a pattern matching nothing produces — so
    /// emptiness has to be excluded before the result says anything at all.</para>
    /// </summary>
    [Fact]
    public void TheInadmissibleReadings_CannotBeFoldedIntoTheStatusVerifiedSeries()
    {
        var source = ReadTimescaleSupportSource();
        var prose = DocProseFor(source, CeilingDeclaration);

        var series = CleanPopulation(prose);
        var snapshot = Numbers(prose, "the inadmissible snapshot readings");

        Assert.NotEmpty(series);
        Assert.NotEmpty(snapshot);
        Assert.All(snapshot, reading => Assert.Contains(reading, series));

        /* Injected through the same ordinal rewrite the drift sweep uses, rather than by string-replacing
           the reading. A bare Replace would depend on the surrounding emphasis tags - the very coupling
           DocProseFor now normalises away - and would silently find nothing if someone unbolded the figure,
           which is a mutation that proves nothing wearing a caught drift's clothes.

           WIDTH-PRESERVING, so the replacement has to be a non-member of the same digit count as the
           reading it displaces: the rewrite pads to the original width and a longer value would leave the
           pattern matching a number the source does not contain. The census maximum plus one is a
           non-member by construction and is chosen over an arbitrary value for that reason. */
        var snapshotPattern = PatternFor("the inadmissible snapshot readings");
        var firstReading = OrdinalOfFirstNumberInGroups(prose, snapshotPattern);
        Assert.True(firstReading >= 0, "the snapshot pair's pattern captured no number, so its pin is vacuous.");

        var nonMember = series.Max() + 1;
        Assert.DoesNotContain(nonMember, series);

        var mutated = RewriteNumberInDocRun(
            source,
            CeilingDeclaration,
            snapshotPattern,
            firstReading,
            nonMember.ToString(CultureInfo.InvariantCulture));

        Assert.True(mutated is not null,
            "could not inject a non-member into the snapshot list, so nothing was proved.");
        Assert.NotEqual(source, mutated);

        var failure = Assert.ThrowsAny<Exception>(() => Verify(mutated!));
        Assert.DoesNotContain(ParseMiss, failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The doc says the snapshot readings are inadmissible BECAUSE that series carries no status column,
    /// while the two reads a constant may be set from filter to successful runs. That is a claim about
    /// shipped SQL, so it is checked against the shipped strings rather than restated in prose.
    ///
    /// <para><b>The predicate is red-proofed in both directions on synthetic copies.</b> A predicate that
    /// could only ever return <c>true</c> would satisfy the two positive lines and prove nothing whatever
    /// about the negative one — and the negative one is the load-bearing half, since it is the reason two
    /// real readings are excluded from the record.</para>
    /// </summary>
    [Fact]
    public void TheInadmissibilityClaim_MatchesTheShippedSql()
    {
        const string filter = "last_run_status = 'Success'";

        Assert.True(FiltersToSuccessfulRuns(TimescaleSupport.HeaviestRefreshRuntimeSql),
            "HeaviestRefreshRuntimeSql no longer filters to successful runs, so the ceiling's doc comment is "
            + "wrong about which reads a constant may be set from (#3069).");
        Assert.True(FiltersToSuccessfulRuns(TimescaleSupport.JobCadenceReadSql),
            "JobCadenceReadSql no longer filters to successful runs, so the ceiling's doc comment is wrong "
            + "about #2136's read (#3069).");

        Assert.False(FiltersToSuccessfulRuns(StoreSelfMetrics.BackgroundJobInsertSql),
            "the self-metrics background-job snapshot now filters on run status, so the two readings the "
            + "ceiling's doc comment excludes as unfiltered may in fact be admissible — re-read that "
            + "paragraph rather than deleting this assertion (#3069).");
        Assert.DoesNotContain("last_run_status", StoreSelfMetrics.BackgroundJobInsertSql, StringComparison.Ordinal);

        /* Red-proof, both directions, so neither result above can be an artefact of a one-sided predicate. */
        Assert.False(FiltersToSuccessfulRuns(
            TimescaleSupport.HeaviestRefreshRuntimeSql.Replace(filter, "1 = 1", StringComparison.Ordinal)));
        Assert.True(FiltersToSuccessfulRuns(
            StoreSelfMetrics.BackgroundJobInsertSql + "\nWHERE js." + filter));

        /* And the paragraph names all three reads, so a rewrite cannot quietly drop the one carrying the
           reasoning. Member names rather than prose, because these are the crefs the claim is anchored on. */
        var prose = DocProseFor(ReadTimescaleSupportSource(), CeilingDeclaration);
        Assert.Contains(nameof(TimescaleSupport.HeaviestRefreshRuntimeSql), prose, StringComparison.Ordinal);
        Assert.Contains(nameof(TimescaleSupport.JobCadenceReadSql), prose, StringComparison.Ordinal);
        Assert.Contains(nameof(StoreSelfMetrics.BackgroundJobInsertSql), prose, StringComparison.Ordinal);
    }

    /// <summary>
    /// Non-vacuity for every drift-swept pin, one captured number at a time: bump it in a COPY, assert the
    /// pattern STILL MATCHES, then require verification to fail with something other than a parse miss.
    ///
    /// <para>One number at a time rather than all at once is the point of the shape. It is what makes each
    /// individual reading in the published population load-bearing — a bump to a middle-ranked reading
    /// changes neither the range, the maximum nor the median, and is caught only because the mean is stated
    /// too. Bumping every group together would have hidden that.</para>
    /// </summary>
    [Fact]
    public void EveryNumericPin_ReportsAnInjectedDrift()
    {
        var source = ReadTimescaleSupportSource();
        var swept = 0;

        foreach (var (name, declaration, pattern, driftSwept) in Pins())
        {
            var prose = DocProseFor(source, declaration);
            var match = Regex.Match(prose, pattern);
            Assert.True(match.Success, $"{name}: pattern matched nothing, so its pin is vacuous.");

            if (!driftSwept)
            {
                continue;
            }

            var numbers = Regex.Matches(prose, "[0-9]+").Count;

            for (var ordinal = 0; ordinal < numbers; ordinal++)
            {
                var mutated = BumpNumberInsideGroups(source, declaration, pattern, ordinal);
                if (mutated is null)
                {
                    continue;
                }

                Assert.NotEqual(source, mutated);

                /* The mutation must leave the pattern matching. If it did not, the failure below would be a
                   parse miss wearing a caught drift's clothes. */
                Assert.True(Regex.IsMatch(DocProseFor(mutated, declaration), pattern),
                    $"{name}: bumping captured number #{ordinal} broke the pattern, so this case proves "
                    + "nothing. Narrow the pattern rather than dropping the case.");

                var failure = Assert.ThrowsAny<Exception>(() => Verify(mutated));
                Assert.DoesNotContain(ParseMiss, failure.Message, StringComparison.Ordinal);
                swept++;
            }
        }

        /* A sweep that enumerated nothing satisfies every assertion above. The floor is the count of captured
           numbers across the drift-swept pins, so it also fails if a pin's groups stop being reachable. */
        var expected = Pins().Where(p => p.DriftSwept).Sum(p => CapturedNumberCount(source, p.Declaration, p.Pattern));
        Assert.True(expected > 0, "no drift-swept pin captured any number, so the sweep is vacuous.");
        Assert.Equal(expected, swept);
    }

    /// <summary>
    /// The class summary claims that no pattern has to match a dash variant, because
    /// <see cref="DocProseFor"/> normalises <c>—</c> and <c>–</c> away before any pattern runs. That is a
    /// claim about the pattern list, so it is enforced here rather than left standing in prose.
    ///
    /// <para><b>Why bother, given this is a test file's own comment.</b> The review that produced this
    /// assertion found the previous version of that paragraph justifying the ASCII-only patterns by a
    /// <c>\S</c> dash stand-in that had been deleted three commits earlier — a comment claiming a mechanism
    /// the code no longer implemented, which is the exact defect this whole file exists to catch, one level
    /// up. The cheapest fix was to correct the sentence; the durable one is to make the sentence unable to
    /// go stale in that direction again.</para>
    /// </summary>
    [Fact]
    public void NoPattern_NeedsToMatchADashVariant_WhichIsWhatTheNormalisationBuys()
    {
        var pins = Pins().ToArray();
        Assert.NotEmpty(pins);

        Assert.All(pins, pin => Assert.True(
            pin.Pattern.All(character => character <= 127),
            $"{pin.Name}'s pattern carries a non-ASCII character. The class summary says none has to, "
            + "because DocProseFor normalises dash variants to a plain hyphen before any pattern runs — so "
            + "either the normalisation stopped covering this case or that paragraph is now wrong."));

        /* And the normalisation itself, on an arranged input rather than on the tree: without this, "the
           patterns can be ASCII-only because the extractor normalises" is a claim with nothing behind it. */
        Assert.DoesNotContain('\u2014', DashNormalisationProbe);
        Assert.DoesNotContain('\u2013', DashNormalisationProbe);
        Assert.Contains("a - b - c", DashNormalisationProbe, StringComparison.Ordinal);
    }

    /// <summary>
    /// The pure arithmetic, kept out of the parsing so it reads without a regex in the way — and so the two
    /// figures the prose states to one decimal are held to being exactly stateable to one decimal.
    /// </summary>
    [Fact]
    public void TheDerivedFiguresAreExactlyStateable_AndTheConstantIsThePopulationMaximum()
    {
        /* CompressionPhaseMinutes used to quote the ceiling in minutes to one decimal, which was only
           faithful while the ceiling was an exact tenth of a minute - 9.9 for a 9.91666 value is a rounded
           claim wearing an exact one's clothes. That divisibility requirement is gone with the sentence:
           896 is not a multiple of six seconds, and a check demanding that a MEASUREMENT be divisible makes
           an honest figure unstateable rather than catching a drift. The occupancy is stated in seconds
           against a slot in seconds instead, which is exact for every value the constant can take, and
           Verify pins it in that unit. */

        var population = CleanPopulation(DocProseFor(ReadTimescaleSupportSource(), CeilingDeclaration));
        Assert.NotEmpty(population);

        /* THE DERIVATION, as one assertion: this constant IS the maximum of the published population. Not
           "above it" - equal to it, because the comment says the estimator is the maximum and a value merely
           above the maximum is the unestablished-bound shape the derivation replaces. */
        Assert.Equal(population.Max(), Ceiling);

        /* The median is stated as a whole number, so it has to be exactly stateable in that shape. The mean
           is no longer stated at all - 27799 s over 57 readings has no exact one-decimal form - so the
           prose carries the exact TOTAL instead and Verify pins that. An exactness check on a figure the
           prose does not state would pass on any population and guard nothing.

           At an ODD population size this parity check is vacuous: both indices are the same element, so the
           sum is even by construction. It is kept because the population's size is a measurement and the
           next census may be even, and a check that is vacuous today is not the same as one that is
           wrong. */
        var sorted = population.OrderBy(v => v).ToArray();
        Assert.Equal(0, (sorted[(sorted.Length - 1) / 2] + sorted[sorted.Length / 2]) % 2);

        /* The relationship the grid depends on, over the WHOLE population rather than one true clause
           standing in for it: every clean run fits inside the window, and inside the routine band.

           THE SECOND CLAUSE AND THE ORDERING BELOW WENT RED on #3166's census, with both CONDITIONS left
           exactly as written, and #3174 answered them with geometry rather than with a re-typed band. At the
           750 s line a 15-minute slot produced, seven of the 57 census readings sat in the warning band and
           the sizing figure sat 146 s above the line; against the window the hour can spare the line is
           1,050 s and the whole population is below it. Only the messages moved, because the reason the old
           ones gave - "the doc comment claims otherwise" - is no longer why they matter: the comment states
           the crossings plainly, so what these clauses guard is the GRID, not the prose. */
        Assert.All(population, reading => Assert.True(reading < Slot,
            $"a clean post-boundary reading of {reading} s is at or past the {Slot} s refresh slot, so the "
            + "compression grid's stated precondition is false and #3035 has to be re-derived rather than "
            + "renumbered"));
        Assert.All(population, reading => Assert.True(reading < WatchLine,
            $"a clean post-boundary reading of {reading} s is at or past the {WatchLine} s watch line, so "
            + "the grid is no longer clear of the load it is sized against: the warning band is meant to "
            + "hold readings this record has no instance of, and it now holds several. A scheduling "
            + "decision (#3035, #3044, #3107), not a condition to renumber"));

        /* And the ordering the whole #3044 watch rests on: the line sits ABOVE the derived ceiling, which is
           what makes a crossing news rather than a restatement of the grid's own sizing figure. */
        Assert.True(Ceiling < WatchLine,
            $"the {Ceiling} s ceiling is at or above the {WatchLine} s watch line, so the figure the grid is "
            + "sized against classifies as a warning and the watch reports the sizing rather than anything "
            + "new. This is the outcome RefreshSlotWarningSeconds' own summary pre-registered; re-take the "
            + "slot or the fraction (#3035, #3044, #3107) rather than editing this assertion.");
    }

    /// <summary>
    /// The scoping rule <see cref="Verify"/> enforces for the ceiling's own paragraph, applied to the whole
    /// file (#3133). A scope has to CLOSE a population: <c>every reading</c> closes nothing, and
    /// <c>the readings so far</c> is relative to a reading time a doc comment does not have. The rule is
    /// stated in one paragraph of this file and breakable in every other one.
    ///
    /// <para><b>Matched against JOINED doc prose, which is why this is a test and not a grep.</b> A claim of
    /// this shape wraps across <c>///</c> lines as readily as it fits on one — "the one every / reading
    /// taken so far falls in" exists on no single line — so a line-oriented search returns a confident
    /// count short by however many wrapped. Runs are joined before any pattern runs, the same normalisation
    /// <see cref="DocProseFor"/> applies to a single declaration.</para>
    ///
    /// <para><b>The one permitted occurrence is ANCHORED to the sentence that rejects it, not exempted by
    /// location.</b> Exempting the doc run the rule lives in would let a fresh universal be added to that
    /// run and pass. Requiring the quotation instead means a reworded rule turns this red and the allowance
    /// gets re-decided rather than quietly widening — and it is what makes the scan non-vacuous, because a
    /// walk that read nothing fails that same assertion.</para>
    ///
    /// <para><b>What this does NOT hold, said plainly rather than left looking covered.</b> It guards two
    /// SHAPES — a quantifier over a population of observations, and the relative-scope form — not staleness
    /// itself. Nothing in the code can know the live series, so no assertion can recognise an open
    /// population claim in a construction it has never seen; a claim built out of neither shape passes this
    /// and still breaks the rule. What carries the rest is that the claim is ABSENT rather than reworded —
    /// the band is described by <see cref="TimescaleSupport.RefreshSlotWarningSeconds"/> and by what the
    /// level means, so there is no census for a later edit to keep current. The population that IS
    /// published carries its own assertion in
    /// <see cref="TheDerivedFiguresAreExactlyStateable_AndTheConstantIsThePopulationMaximum"/>.</para>
    /// </summary>
    [Fact]
    public void NoOpenPopulationClaim_SurvivesOutsideTheSentenceThatRejectsIt()
    {
        AssertNoOpenPopulationClaim(ReadTimescaleSupportSource());
    }

    /// <summary>
    /// Each ceiling constant's summary NAMES ITS OWN STATISTIC and names its own population, and neither
    /// treats a single closed day and the constant's population as the same population (#3182).
    ///
    /// <para><b>Why this is a test.</b> The defect #3182 records is a summary that said "the census
    /// maximum" while quoting one day's figure, with a sentence asserting that the two "describe the same
    /// population". Nothing could fail on that: a statistic and a population stated in prose cannot be
    /// derived from anything, so a constant whose summary does not name its own statistic can drift into
    /// being a different one with nothing going red. Which is what happened — and the rest of this file,
    /// which checks every FIGURE the prose draws, passed the whole time, because each figure was correct
    /// arithmetic over a population the sentence above it mislabelled.</para>
    ///
    /// <para>Checked as a SHAPE rather than as a wording: the summary must declare a statistic and must
    /// name a population, and the specific false identity claim must stay gone. A reworded but honest
    /// summary passes; one that drops either half does not. Lives HERE rather than beside the staleness
    /// finding's own cases because <see cref="DocProseFor"/> is the doc-run walker this file already
    /// carries a stated bound for, and a second hand-rolled one is the shape
    /// <c>CommentFilterAdoptionTests</c> exists to stop.</para>
    /// </summary>
    [Fact]
    public void EachCeilingSummary_NamesItsStatisticAndItsPopulation()
    {
        var source = ReadTimescaleSupportSource();

        foreach (var declaration in new[] { CeilingDeclaration, LightCeilingDeclaration })
        {
            var prose = DocProseFor(source, declaration);

            Assert.Contains("This is a MAXIMUM.", prose, StringComparison.Ordinal);
            Assert.Contains("Its population is", prose, StringComparison.Ordinal);
        }

        /* THE CLAIM THAT HAD TO GO, checked file-wide rather than in the paragraph that carried it: the
           substitution of a day for a population has as many wordings as it has sites, and the one wording
           that is known to have been made is the one worth being unable to restore. */
        Assert.DoesNotContain("describe the same population", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// Each shape the guard forbids, injected ONE AT A TIME into a copy of the source with the guard
    /// required to fail on it — a bundled mutation reports that something fired, not which clause did.
    ///
    /// <para>The first case goes back in WRAPPED across two <c>///</c> lines, so it proves the joining is
    /// load-bearing rather than tidy: unjoined, that mutation is invisible, and a guard that cannot see a
    /// wrapped claim reports a clean file for a broken one.</para>
    /// </summary>
    [Fact]
    public void TheOpenPopulationGuard_ReportsEachForbiddenShape()
    {
        var source = ReadTimescaleSupportSource();

        /* Clean first. Without this, every case below could be reporting a violation that was already
           there, which is indistinguishable from a caught mutation. */
        AssertNoOpenPopulationClaim(source);

        Assert.ThrowsAny<Exception>(() => AssertNoOpenPopulationClaim(
            InjectDocLines(source, InsideSlotMember, "/// and the one every", "/// reading taken so far falls in.")));

        Assert.ThrowsAny<Exception>(() => AssertNoOpenPopulationClaim(
            InjectDocLines(source, InsideSlotMember, "/// The readings so far are all inside it.")));

        /* A THIRD wording of the same claim, and the reason the pattern matches a shape rather than a list
           of phrasings: this claim has as many wordings as it has sites. */
        Assert.ThrowsAny<Exception>(() => AssertNoOpenPopulationClaim(
            InjectDocLines(source, InsideSlotMember, "/// Zero exceptions on every sample since.")));

        /* And the anchor: reword the rule's own quotation and the allowance stops being granted. */
        var unanchored = source.Replace(
            @"""the readings so far"" carries a scope",
            @"""a scope read against now"" carries a scope",
            StringComparison.Ordinal);
        Assert.NotEqual(source, unanchored);
        Assert.ThrowsAny<Exception>(() => AssertNoOpenPopulationClaim(unanchored));
    }

    /// <summary>
    /// Two doc runs cannot combine into a claim neither of them makes, checked on an arranged pair rather
    /// than on the tree — the same way <see cref="DashNormalisationProbe"/> checks the normalisation it
    /// depends on.
    ///
    /// <para><b>Why an arranged pair and not a hopeful comment.</b> <c>\s</c> matches <c>\n</c> in .NET, so
    /// a newline between runs is NOT a barrier: a run ending in a quantifier and the next one opening with
    /// a population noun would match as one claim across it. The only shape that can bridge the separator
    /// is the one built here, and the separator has to be something no whitespace class can absorb.</para>
    /// </summary>
    [Fact]
    public void TwoDocRuns_CannotCombineIntoAClaimNeitherOfThemMakes()
    {
        /* The first run ENDS in the quantifier and the second OPENS with the population noun. Neither run
           states a population claim; only their concatenation could. */
        var prose = JoinedDocProse(
            "    /// a band every\r\n    int first;\r\n\r\n    /// readings arrive hourly\r\n    int second;\r\n");

        /* Both halves present first, so a pass cannot come from the scan having read nothing. */
        Assert.Contains("every", prose, StringComparison.Ordinal);
        Assert.Contains("readings", prose, StringComparison.Ordinal);

        Assert.False(UniversalOverAnOpenSeries.IsMatch(prose),
            "two doc runs combined into a population claim neither of them makes, so the run separator is "
            + "being absorbed by a pattern's whitespace class. Keep the separator non-whitespace: \\s "
            + "matches \\n in .NET, so a newline cannot hold two runs apart.");
    }

    /* ─────────────────────────────── verification ─────────────────────────────── */

    /// <summary>
    /// Every claim, checked against the constants and against the series the comment publishes. Throws on the
    /// first disagreement; parse misses carry <see cref="ParseMiss"/> so the drift sweep can tell them apart.
    /// Finishes by requiring that it consumed every pin, so a pattern cannot sit in the list doing nothing.
    /// </summary>
    private static void Verify(string source)
    {
        var consumed = new HashSet<string>(StringComparer.Ordinal);
        var ceilingProse = DocProseFor(source, CeilingDeclaration);

        /* Which doc run a pin is read out of comes from the PIN'S OWN Declaration, never from its name — a
           renamed pin must not be able to change which prose its pattern is aimed at. */
        int[] Read(string name)
        {
            consumed.Add(name);
            return Numbers(DocProseFor(source, DeclarationFor(name)), name);
        }

        var tail = Read("the boundary day's tail");
        var after = Read("the days after the boundary");
        Require(tail.Length > 0, "the boundary day's tail parsed to nothing");
        Require(after.Length > 0, "the days after the boundary parsed to nothing");

        var population = tail.Concat(after).ToArray();
        var sorted = population.OrderBy(v => v).ToArray();
        var min = sorted[0];
        var max = sorted[^1];
        var total = population.Sum();
        var median = (sorted[(sorted.Length - 1) / 2] + sorted[sorted.Length / 2]) / 2;

        /* THE DERIVATION, and the one relationship everything else here is decoration on: the constant is
           the maximum of the population the comment publishes. */
        Require(max == Ceiling,
            $"the published population {Join(sorted)} has maximum {max} s, but this constant holds "
            + $"{Ceiling} s — the comment says the estimator is the maximum, so one of the two is wrong");

        Require((sorted[(sorted.Length - 1) / 2] + sorted[sorted.Length / 2]) % 2 == 0,
            "the population's median is no longer a whole number of seconds, so the prose cannot state it as "
            + "one — restate it rather than removing this check");

        var summary = Read("the population summary");
        Require(summary[0] == population.Length,
            $"stated count {summary[0]} against {population.Length} listed");
        Require(summary[1] == min, $"stated low {summary[1]} s against {min} s listed");
        Require(summary[2] == max, $"stated high {summary[2]} s against {max} s listed");

        /* A TOTAL rather than a mean (#3166): 27799 s over 57 readings has no exact one-decimal form, and a
           comparison that accepted a rounded mean would be a pin with an unwritten tolerance. The total is
           exact in the unit the readings are listed in and catches the same drift — a bump to a
           middle-ranked reading moves neither the range nor the median, and this is what sees it. */
        Require(summary[3] == total, $"stated total {summary[3]} s against {total} s summed from the list");
        Require(summary[4] == median, $"stated median {summary[4]} s against {median} s computed");

        /* THE ESTIMATOR CHOICE. This clause used to require the 95th percentile to EQUAL the maximum, which
           was half the comment's stated reason for taking the maximum and was written to expire once the
           population grew. It expired (#3166): at 57 readings the 95th sits below the maximum, the decision
           was re-taken, and the surviving reason — a scheduling exclusion may accept no exceedance over its
           own record — does not reference the population's size, so there is nothing left for an expiry to
           watch. What replaces it is stricter about what the prose may claim: both percentiles and both
           gaps, derived by nearest rank from the published list rather than restated. */
        var percentile = Read("the percentile decision");
        Require(percentile[0] == population.Length,
            $"stated sample size {percentile[0]} against {population.Length} listed");
        Require(percentile[1] == NearestRank(sorted, 95),
            $"stated 95th percentile {percentile[1]} s against {NearestRank(sorted, 95)} s computed");
        Require(percentile[2] == NearestRank(sorted, 90),
            $"stated 90th percentile {percentile[2]} s against {NearestRank(sorted, 90)} s computed");
        Require(percentile[3] == max - NearestRank(sorted, 95),
            $"stated 95th-percentile gap {percentile[3]} s against {max - NearestRank(sorted, 95)} s derived");
        Require(percentile[4] == max - NearestRank(sorted, 90),
            $"stated 90th-percentile gap {percentile[4]} s against {max - NearestRank(sorted, 90)} s derived");

        /* And the divergence the paragraph is ABOUT, as an inequality rather than as prose: if a high
           percentile ever coincided with the maximum again the sample-size argument would be available
           once more, and re-taking the decision on the cost argument alone would no longer be the whole
           story. Stated so a coincidence has to be noticed rather than sat on. */
        Require(NearestRank(sorted, 95) < max,
            $"the 95th percentile of {Join(sorted)} is back at the {max} s maximum, so the paragraph's "
            + "premise — that the two answers have diverged — is false and the sample-size half of the "
            + "estimator argument is live again. Re-read #3101's trade rather than editing this assertion");

        var low = Read("the low the grid is deliberately not sized against");
        Require(low[0] == min, $"stated low {low[0]} s against {min} s listed");

        var margin = Read("the margin sentence");
        Require(margin[0] == Ceiling, $"stated ceiling {margin[0]} s against {Ceiling} s");
        Require(margin[1] == Slot, $"stated slot {margin[1]} s against {Slot} s");
        Require(margin[2] == Slot - Ceiling, $"stated margin {margin[2]} s against {Slot - Ceiling} s derived");

        /* THE LIVE ENVELOPE (#3119), which is the margin sentence's counterpart: that one states the
           clearance the published population carries and this one states what a measured day leaves. In
           TENTHS throughout, because each figure is stated to one decimal and integer arithmetic keeps a
           rounding mode from being able to move an assertion. */
        var live = Read("the measured live envelope");
        var measured10 = live[0] * 10 + live[1];
        var statedMargin10 = live[2] * 10 + live[3];
        var statedShareTenths = live[4] * 10 + live[5];
        var statedBelowWatch10 = live[6] * 10 + live[7];

        /* The two now COINCIDE, because the constant is the maximum of a census that includes this day
           rather than of a sample that predated it (#3166). #3119 required the measured maximum to be
           strictly ABOVE the ceiling, which was the whole reason that paragraph existed; equality is what
           says the census has closed that gap. A measured day ABOVE the ceiling would mean the census has
           been overtaken and the constant is stale again; BELOW would mean the constant was not taken from
           this population at all. */
        Require(measured10 / 10 == Ceiling,
            $"the measured maximum {measured10 / 10}.{measured10 % 10} s no longer truncates to the "
            + $"{Ceiling} s recorded ceiling. Above it, the census this constant was derived from has been "
            + "overtaken and it needs re-deriving; below it, this paragraph is quoting a day the constant "
            + "was not taken from");

        /* THE VERDICT, from the SHIPPED classifier and ABOVE the arithmetic, because Verify is fail-fast:
           a maximum restated low enough to change its band trips the margin equality below on the way
           past, and a verdict clause underneath that one could not go red on its own. It is also the only
           clause here that reads the code rather than the constants - the two inequalities say where the
           reading sits, and only the classifier says what the shipped code does with it. */
        Require(
            TimescaleSupport.ClassifyRefreshSlotHeadroom(measured10 / 10.0)
                == TimescaleSupport.RefreshSlotHeadroom.InsideSlot,
            $"ClassifyRefreshSlotHeadroom bands {measured10 / 10}.{measured10 % 10} s as "
            + $"{TimescaleSupport.ClassifyRefreshSlotHeadroom(measured10 / 10.0)} rather than "
            + "InsideSlot, so the live envelope paragraph states a verdict the shipped classifier "
            + $"does not produce. Against a {Slot} s window the warning band opens at {WatchLine} s");

        Require(statedMargin10 == Slot * 10 - measured10,
            $"stated slot margin {statedMargin10 / 10}.{statedMargin10 % 10} s against "
            + $"{(Slot * 10 - measured10) / 10}.{(Slot * 10 - measured10) % 10} s derived from the "
            + $"{Slot} s slot less the measured maximum");

        /* The distance BELOW the watch line, which also carries the strict-below claim and INVERTED at
           #3174: while the slot was 15 minutes the measured maximum sat PAST this line, and the re-derived
           window put it under it. A maximum at or above the line makes this difference zero or negative, and
           no figure the pattern can hold equals that — so the direction is load-bearing rather than
           cosmetic, and a reading that climbed back past the line goes red here instead of leaving the word
           BELOW standing over a value that is not. */
        Require(statedBelowWatch10 == (WatchLine * 10) - measured10,
            $"stated distance below the watch line {statedBelowWatch10 / 10}.{statedBelowWatch10 % 10} s "
            + $"against {((WatchLine * 10) - measured10) / 10}.{((WatchLine * 10) - measured10) % 10} s "
            + $"derived from the {WatchLine} s line");

        Require(statedShareTenths == statedMargin10 * 1000 / (Slot * 10),
            $"stated {statedShareTenths / 10}.{statedShareTenths % 10}% of the slot against "
            + $"{statedMargin10 * 1000 / (Slot * 10) / 10}.{statedMargin10 * 1000 / (Slot * 10) % 10}% "
            + $"derived from {statedMargin10 / 10}.{statedMargin10 % 10} s over the {Slot} s slot, "
            + "truncated to one decimal as the prose states it");

        /* AND ITS CLOSING SCOPE, the same rule the snapshot enumeration below carries. An absolute day
           has ENDED and stays true; "recently", "the latest day" or a bare "currently" is read against a
           clock a doc comment does not have, so it rots while looking scoped. */
        Require(
            Regex.IsMatch(ceilingProse,
                @"Over <c>[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]</c> - one closed day"),
            "the live envelope's population is no longer CLOSED by an absolute day, so those figures read "
            + "as a claim about current load and are false as soon as the load moves. Restore a window "
            + "that has ended, or drop the figures and keep the mechanism, which is the timeless half");

        /* The EXCLUDED run. Its clock arithmetic has to reproduce its own stated duration, it has to end one
           second after the stated boundary, and its ratio to the population maximum has to be the ratio the
           prose draws — which is what keeps that paragraph about this population rather than free-floating. */
        var run = Read("the excluded run's clock arithmetic");
        var started = Seconds(run[0], run[1], run[2]);
        var ended = Seconds(run[3], run[4], run[5]);
        var excluded = Read("the excluded run's duration");
        Require(ended - started == excluded[0],
            $"the quoted run spans {ended - started} s, not the {excluded[0]} s stated for it");
        /* The excluded run now sits INSIDE the population's range, which is what the prose says and is the
           reason its duration-based disqualification was dropped (#3166). Held as an inequality rather than
           left as prose: if it rose back above the maximum, "inside the post-boundary range" would be false
           and the paragraph would need the earlier argument back. */
        Require(excluded[0] < Ceiling,
            $"the excluded run's {excluded[0]} s is at or above the {Ceiling} s population maximum again, so "
            + "the prose's \"sits INSIDE the post-boundary range\" is false");

        var boundary = Read("the boundary timestamp");
        Require(ended - Seconds(boundary[0], boundary[1], boundary[2]) == 1,
            "the quoted run no longer ends exactly one second after the quoted boundary, so the circularity "
            + "the open-question paragraph is about is not what the numbers say");

        var ratio = Read("the excluded run's ratio to the population maximum");
        Require(ratio[0] * 100 + ratio[1] == excluded[0] * 100 / Ceiling,
            $"stated ratio {ratio[0]}.{ratio[1]}x against {excluded[0] * 100 / Ceiling / 100}."
            + $"{excluded[0] * 100 / Ceiling % 100}x derived from {excluded[0]} s over {Ceiling} s");

        /* THE SNAPSHOT READINGS ARE MEMBERS of the census population, HOWEVER MANY of them there are, and
           that is the inversion of what this clause used to require (#3166). Disjointness was a property of
           the SAMPLE the population used to be, not of the rule: the self-metrics snapshot reads this job's
           last run, so a census of the job contains whatever it reported. Containment is the same evidence
           read the right way round, and it fails toward the more useful label - a quoted reading that is NOT
           in the census means the unfiltered series is reporting a run the status-verified census does not
           have, which is an anomaly in the reads rather than a tidiness problem in the prose. */
        var snapshot = Read("the inadmissible snapshot readings");
        Require(snapshot.Length > 0, "the inadmissible snapshot readings parsed to nothing");
        Require(snapshot.All(reading => population.Contains(reading)),
            $"a quoted snapshot reading is absent from the census population {Join(sorted)}. The snapshot "
            + "series reads this job's last run and the population is a census of every run since the "
            + "boundary, so a reading with no member to match either came from a different job, or the "
            + "population is not the census it says it is");

        /* THE CLOSING SCOPE, which is what stops that list reading as a complete enumeration of an open
           set. The series it quotes gains a reading every hour, so an unscoped list is a frozen enumeration
           wearing a complete one's clothes - #3072's defect exactly.

           And the scope has to CLOSE the population rather than merely date it. "up to <hh:mmZ>" names a
           window that has ended; "the readings so far" carries a scope and rots anyway, because a doc
           comment has no timestamp of its own for a relative phrase to be read against. So the pattern
           demands the closing preposition and an absolute stamp, not just any stamp.

           Checked for PRESENCE and not for value: the timestamp is evidence, and pinning it to a derived
           quantity would be inventing a bound this constant does not have. */
        Require(
            Regex.IsMatch(ceilingProse, @"complete set of snapshot readings up to <c>[0-9][0-9]:[0-9][0-9]Z</c> \("),
            "the snapshot readings are no longer bounded by a CLOSED window, so the list reads as a complete "
            + "enumeration of a series that gains a reading every hour this job runs. Restore a scope that "
            + "has ended (\"up to <hh:mmZ>\"), not one relative to whenever it is read - or drop the "
            + "enumeration and keep only the rule, which is the timeless part and the paragraph's real "
            + "subject");

        /* CompressionPhaseMinutes' exclude-whole reasoning: the ceiling against the slot, the guard band
           against it, and the minutes the exclusion declines to recover.

           IN SECONDS, not tenths of a minute (#3166). The old form stated the occupancy as a tenth of a
           minute, which is only faithful while the ceiling is a multiple of six seconds; 896 is not, and a
           check demanding that divisibility would make an honest measurement unstateable rather than
           catching anything. Seconds against seconds is exact for every value the constant can take, so
           this cannot expire for a reason that has nothing to do with the grid. */
        var occupancy = Read("the exclude-whole occupancy sentence");
        Require(occupancy[0] == Ceiling, $"stated occupancy {occupancy[0]} s against {Ceiling} s");
        Require(occupancy[1] == Slot, $"stated slot {occupancy[1]} s against {Slot} s");

        var minutesInsideTheRefresh = (Ceiling + 59) / 60;
        var band = Read("the guard-band arithmetic the exclusion rests on");
        Require(band[0] == TimescaleSupport.CompressionPhaseGuardMinutes,
            $"stated guard band {band[0]} minutes against {TimescaleSupport.CompressionPhaseGuardMinutes}");
        Require(band[1] == minutesInsideTheRefresh - TimescaleSupport.CompressionPhaseGuardMinutes,
            $"stated {band[1]} band minutes inside the refresh against "
            + $"{minutesInsideTheRefresh - TimescaleSupport.CompressionPhaseGuardMinutes} derived — if this "
            + "reached zero the band would clear the refresh and the exclude-whole reasoning would no longer "
            + "hold, so re-read #3035 rather than restating the figure");

        var onTheTable = Read("the minutes left on the table");
        Require(onTheTable[0] == SlotMinutes - minutesInsideTheRefresh,
            $"stated {onTheTable[0]} minutes past the refresh against "
            + $"{SlotMinutes - minutesInsideTheRefresh} derived");

        var deferred = Read("the population size and range the exclusion defers to");
        Require(deferred[0] == population.Length,
            $"stated population size {deferred[0]} against {population.Length} listed");
        Require(deferred[1] == min, $"stated low {deferred[1]} s against {min} s listed");
        Require(deferred[2] == max, $"stated high {deferred[2]} s against {max} s listed");

        /* THE REJECTED ALTERNATIVE watch line, held to the arithmetic the prose derives it as. Slot less one
           guard band, in seconds, so a moved grid step moves it and the sentence cannot keep quoting a
           figure the grid no longer produces. */
        var alternative = Read("the rejected alternative watch line");
        var derivedAlternative = Slot - TimescaleSupport.CompressionPhaseGuardMinutes * 60;
        Require(alternative[0] == derivedAlternative,
            $"the rejected alternative is stated as {alternative[0]} s against {derivedAlternative} s derived "
            + $"from the {Slot} s slot less one {TimescaleSupport.CompressionPhaseGuardMinutes}-minute guard "
            + "band");

        /* ---- the LIGHT refresh ceiling's own census (#3174) ---- */

        /* THE DERIVATION for this constant, in the same shape as the heaviest one's: the value is the
           maximum of the census the comment states, the population's own count is corroborated by the
           exclusion control, and the view count is the product's own light-policy count rather than a
           number someone typed. */
        var lightCensus = Read("the light-refresh census");
        var lightMax10 = (lightCensus[0] * 10) + lightCensus[1];
        var lightRuns = lightCensus[2];

        Require(lightMax10 == LightCeilingTenths,
            $"the stated census maximum {lightCensus[0]}.{lightCensus[1]} s is not this constant's "
            + $"{LightCeilingTenths / 10}.{LightCeilingTenths % 10} s - the comment says the estimator is "
            + "the maximum, so one of the two is wrong");
        Require(lightCensus[3] == TimescaleSupport.LightHourlyRefreshCount,
            $"the census states {lightCensus[3]} views against the "
            + $"{TimescaleSupport.LightHourlyRefreshCount} non-heaviest hourly policies the product "
            + "registers, so it is a census of a different set of jobs than the one this constant bounds");

        var lightKept = Read("the light-refresh census exclusion count");
        Require(lightKept[0] == lightKept[1],
            $"the succeeded/finish filter is stated as keeping {lightKept[0]} of {lightKept[1]}, so it DID "
            + "remove rows - the population is status-selected and the census is a part of the span rather "
            + "than the whole of it");
        Require(lightKept[0] == lightRuns,
            $"the filter clause counts {lightKept[0]} runs against the census's {lightRuns}, so the two "
            + "sentences are about different reads");

        /* The percentiles, and the GAP that is the estimator argument. A percentile here would discard the
           runs the bound exists for, and the gap is the size of what it would discard. */
        var lightQuantiles = Read("the light-refresh quantiles");
        var light95Tenths = (lightQuantiles[0] * 10) + lightQuantiles[1];
        var lightMedianTenths = (lightQuantiles[2] * 10) + lightQuantiles[3];

        Require(light95Tenths < lightMax10,
            $"the stated 95th percentile {lightQuantiles[0]}.{lightQuantiles[1]} s is at or above the "
            + "census maximum, so the sentence about a percentile discarding the runs this bound exists for "
            + "no longer describes this population");
        Require(lightMedianTenths <= light95Tenths,
            "the stated median is above the stated 95th percentile, so the two quantiles are transposed");

        var lightGap = Read("the light-refresh percentile gap");
        Require((lightGap[0] * 10) + lightGap[1] == lightMax10 - light95Tenths,
            $"the stated gap {lightGap[0]}.{lightGap[1]} s is not "
            + $"{(lightMax10 - light95Tenths) / 10}.{(lightMax10 - light95Tenths) % 10} s derived from the "
            + "census maximum less the stated 95th percentile");

        /* THE MIDNIGHT MECHANISM, which is the reason this constant deliberately does NOT exclude the
           regime that produced it. The maximum is the later of the two nights, and the growth is derived
           from both rather than restated. */
        var midnightMax = Read("the midnight maximum");
        Require((midnightMax[0] * 10) + midnightMax[1] == lightMax10,
            $"the later midnight run is stated as {midnightMax[0]}.{midnightMax[1]} s against the census "
            + $"maximum {lightMax10 / 10}.{lightMax10 % 10} s, so the paragraph is no longer quoting the run "
            + "this constant IS");

        var growth = Read("the midnight night-over-night growth");
        var previousNight10 = (growth[0] * 10) + growth[1];
        Require(previousNight10 < lightMax10,
            $"the earlier midnight run is stated as {growth[0]}.{growth[1]} s, at or above the later one, so "
            + "the night-over-night claim runs the wrong way");
        Require(growth[2] == (lightMax10 - previousNight10) * 100 / previousNight10,
            $"the stated growth {growth[2]}% is not "
            + $"{(lightMax10 - previousNight10) * 100 / previousNight10}% derived from the two readings the "
            + "same sentence states, truncated as the prose states it");

        var thirdLargest = Read("the third-largest light run");
        Require((thirdLargest[0] * 10) + thirdLargest[1] < previousNight10,
            $"the run called third-largest, {thirdLargest[0]}.{thirdLargest[1]} s, is at or above the "
            + "second-largest the same paragraph states, so the ordering the sentence claims is false");

        /* And the guard band this constant SIZES, which is the whole of what changed about it at #3174: it
           used to be characterised against a step-derived band, and now the band is derived from it. */
        var guardBand = Read("the guard band the light ceiling derives");
        Require(guardBand[0] == TimescaleSupport.CompressionPhaseGuardMinutes,
            $"the guard band is stated as {guardBand[0]} minutes against "
            + $"{TimescaleSupport.CompressionPhaseGuardMinutes} derived");
        Require((guardBand[1] * 10) + guardBand[2] == LightCeilingTenths,
            $"the ceiling the guard band is derived from is stated as {guardBand[1]}.{guardBand[2]} s "
            + $"against this constant's {LightCeilingTenths / 10}.{LightCeilingTenths % 10} s");

        var guardMargin = Read("the guard band's rounding margin");
        Require(
            (guardMargin[0] * 10) + guardMargin[1]
                == (TimescaleSupport.CompressionPhaseGuardMinutes * 600) - LightCeilingTenths,
            $"the rounding margin is stated as {guardMargin[0]}.{guardMargin[1]} s against "
            + $"{((TimescaleSupport.CompressionPhaseGuardMinutes * 600) - LightCeilingTenths) / 10}."
            + $"{((TimescaleSupport.CompressionPhaseGuardMinutes * 600) - LightCeilingTenths) % 10} s derived "
            + "from the guard band less the ceiling it covers - if this reached zero or below, a compression "
            + "policy could start while a light refresh still held AccessShareLock");

        /* And nothing in the pin list went unused: a pattern that is never asserted against reads, from the
           list, exactly like one that is. */
        var unused = Pins().Select(p => p.Name).Where(n => !consumed.Contains(n)).ToArray();
        Require(unused.Length == 0,
            "these pinned sentences are declared but never verified, so they guard nothing: "
            + string.Join(", ", unused));
    }

    /// <summary>
    /// The nearest-rank percentile of an ASCENDING population — <c>ceil(p/100 * n)</c>, 1-based, which is
    /// the definition the doc comment's percentile argument is stated in. Integer arithmetic throughout, so
    /// no rounding mode can move the rank.
    /// </summary>
    private static int NearestRank(int[] sortedAscending, int percentile)
    {
        Require(sortedAscending.Length > 0, "no population, so no percentile");
        var rank = (percentile * sortedAscending.Length + 99) / 100;
        return sortedAscending[Math.Clamp(rank, 1, sortedAscending.Length) - 1];
    }

    /// <summary>
    /// <see cref="DocProseFor"/>'s dash and emphasis normalisation, applied to an arranged doc run so the
    /// claim can be checked without depending on which dashes happen to be in the real comment today.
    /// </summary>
    private static string DashNormalisationProbe =>
        DocProseFor(
            "    /// <summary>\r\n    /// a \u2014 b \u2013 c <b>d</b>\r\n    /// </summary>\r\n    probe marker;\r\n",
            "probe marker");

    private static bool FiltersToSuccessfulRuns(string sql) =>
        Regex.IsMatch(sql, @"last_run_status\s*=\s*'Success'");

    /// <summary>
    /// The clean post-boundary population: both published lists, concatenated. Read from the two patterns
    /// rather than from one, because the comment publishes them separately — the boundary day's tail is one
    /// partial day and the days after it are whole ones, which is the split the per-day trend the comment
    /// states is read off. Both halves are now censuses (#3166); the sample-versus-census distinction the
    /// split originally carried is retired, and the split is kept because it is what makes the boundary
    /// day's nine runs identifiable as a tail rather than a day.
    /// </summary>
    private static int[] CleanPopulation(string prose) =>
        Numbers(prose, "the boundary day's tail")
            .Concat(Numbers(prose, "the days after the boundary"))
            .ToArray();

    private static int Seconds(int hours, int minutes, int seconds) => (hours * 60 + minutes) * 60 + seconds;

    private static string Join(IEnumerable<int> values) =>
        "[" + string.Join(", ", values.Select(v => v.ToString(CultureInfo.InvariantCulture))) + "]";

    /* ─────────────────────────────── parsing ─────────────────────────────── */

    /// <summary>
    /// The one pin with this name. Requires EXACTLY one, so a duplicated name cannot leave two patterns
    /// competing for the same assertions with only one of them ever read.
    /// </summary>
    private static (string Name, string Declaration, string Pattern, bool DriftSwept) PinFor(string name)
    {
        var pins = Pins().Where(p => string.Equals(p.Name, name, StringComparison.Ordinal)).ToArray();
        Require(pins.Length == 1,
            $"{ParseMiss}: '{name}' resolves to {pins.Length} pins rather than exactly one");
        return pins[0];
    }

    private static string PatternFor(string name) => PinFor(name).Pattern;

    private static string DeclarationFor(string name) => PinFor(name).Declaration;

    /// <summary>
    /// Every integer captured by the named pin, flattened — a group holding a comma-separated list
    /// contributes each of its members, so the series pattern and the single-figure patterns read the same
    /// way and the drift sweep can address any of them by ordinal.
    /// </summary>
    private static int[] Numbers(string prose, string name)
    {
        var pattern = PatternFor(name);
        var match = Regex.Match(prose, pattern);
        Require(match.Success,
            $"{ParseMiss}: {name} is no longer present in TimescaleSupport.cs in the pinned shape "
            + $"({pattern}). Three fixes are all correct and you should pick by INTENT, not by whichever is "
            + "quickest:\n"
            + "  (1) the wording moved but the figures still agree - UPDATE THIS PATTERN in "
            + "RefreshCeilingProvenancePinTests.Pins(). This is sanctioned rather than a loophole, because "
            + "EveryNumericPin_ReportsAnInjectedDrift re-derives every case from the pattern you write: "
            + "loosen it until it stops guarding and that test goes red, so a pattern cannot be widened into "
            + "a no-op.\n"
            + "  (2) a figure is now WRONG - fix the prose, not this pin. The comparison below names the "
            + "derived value.\n"
            + "  (3) the figure is no longer worth stating - DELETE IT FROM THE PROSE and drop this pin with "
            + "it. Equally acceptable; a pin must not become the reason to keep a sentence nobody wants "
            + "(#3072's rule, #3069's pin).\n"
            + "Emphasis tags and dash style are already normalised away by DocProseFor, so a pure typo or "
            + "reformatting fix that touches none of these words needs no change here.");

        return match.Groups.Cast<Group>().Skip(1)
            .SelectMany(g => Regex.Matches(g.Value, "[0-9]+").Select(m => m.Value))
            .Select(v => int.Parse(v, CultureInfo.InvariantCulture))
            .ToArray();
    }

    /// <summary>
    /// The normalisation every pattern in this file is written against: <c>&lt;b&gt;</c> emphasis removed
    /// and dash variants flattened to a plain hyphen, then runs of spaces and tabs collapsed to one. That
    /// is #3077's review complaint met at the extractor rather than argued with — bolding or unbolding a
    /// figure, and em versus en versus plain hyphen, are copy-edits that must not be able to break a pin.
    ///
    /// <para><b>One implementation because <see cref="DocProseFor"/> and <see cref="JoinedDocProse"/> feed
    /// the SAME patterns.</b> Two copies could drift into normalising differently, and a pattern would then
    /// match through one reader and not the other for a reason nothing in the file states. Sharing it makes
    /// them equivalent by construction rather than by a claim in a summary.</para>
    ///
    /// <para><b>Line breaks are deliberately left alone.</b> Collapsing them would erase
    /// <see cref="DocRunSeparator"/>, which is the only thing keeping two doc runs from combining into a
    /// claim neither of them makes. <see cref="DocProseFor"/> is unaffected either way, because it has
    /// already joined its own run to a single line before calling in.</para>
    /// </summary>
    private static string NormaliseDocProse(string prose) =>
        Regex.Replace(
            prose.Replace("<b>", string.Empty, StringComparison.Ordinal)
                .Replace("</b>", string.Empty, StringComparison.Ordinal)
                .Replace('—', '-')
                .Replace('–', '-'),
            "[ \t]+",
            " ").Trim();

    /// <summary>
    /// The doc-comment run immediately above <paramref name="declaration"/>, stripped of its <c>///</c>
    /// markers and collapsed to one line so a pattern can span the wrapping.
    ///
    /// <para><b>Anti-truncation.</b> The run is required to open with <c>&lt;summary&gt;</c> and close with
    /// <c>&lt;/summary&gt;</c>. A walk that stopped early would still hand back plausible prose, and every
    /// pattern that happened to sit in the part it read would still match — which is a guard reporting a
    /// clean tree from half a file.</para>
    /// </summary>
    private static string DocProseFor(string source, string declaration)
    {
        var declarationIndex = source.IndexOf(declaration, StringComparison.Ordinal);
        Require(declarationIndex > 0,
            $"{ParseMiss}: could not find '{declaration}' in TimescaleSupport.cs, so no doc run could be read");

        var lines = source[..declarationIndex].Split('\n');
        var run = new List<string>();

        /* The final element is the indentation on the declaration's own line, so the run starts above it. */
        for (var index = lines.Length - 2; index >= 0; index--)
        {
            var line = lines[index].Trim('\r', ' ', '\t');
            if (!line.StartsWith("///", StringComparison.Ordinal))
            {
                break;
            }

            run.Insert(0, line.Length > 3 ? line[3..].Trim() : string.Empty);
        }

        Require(run.Count > 0, $"{ParseMiss}: '{declaration}' carries no doc comment run");

        /* NORMALISED before any pattern sees it, so two whole classes of copy-edit cannot break a pin:
           <b> emphasis (bolding or unbolding a figure) and dash style (em, en or plain hyphen). That is
           #3077's review complaint met at the extractor rather than argued with. What a pattern still has
           to name is the handful of words that identify WHICH CLAIM a number belongs to, and that is not
           removable — see the class summary on why a claim-blind numeral bag cannot do this job. */
        var prose = NormaliseDocProse(string.Join(" ", run));
        Require(prose.StartsWith("<summary>", StringComparison.Ordinal),
            $"{ParseMiss}: the doc run above '{declaration}' does not begin at its <summary>, so the walk "
            + "reached the top of a truncated block");
        Require(prose.EndsWith("</summary>", StringComparison.Ordinal),
            $"{ParseMiss}: the doc run above '{declaration}' does not end at its </summary>, so the walk "
            + "stopped short of the declaration");

        return prose;
    }

    /// <summary>
    /// Where a declaration's doc run begins and ends in the raw source. The run's digits appear in the same
    /// order as the collapsed prose's — the only things stripped are <c>///</c> markers and whitespace, and
    /// no number is ever split across a wrap — which is what lets the sweep address a number by ordinal in
    /// one and rewrite it in the other.
    /// </summary>
    private static (int Start, int End) DocRunSpan(string source, string declaration)
    {
        var declarationIndex = source.IndexOf(declaration, StringComparison.Ordinal);
        Require(declarationIndex > 0, $"{ParseMiss}: could not find '{declaration}' in TimescaleSupport.cs");

        var start = source.LastIndexOf("/// <summary>", declarationIndex, StringComparison.Ordinal);
        Require(start > 0 && start < declarationIndex,
            $"{ParseMiss}: could not find the opening of '{declaration}''s doc run");
        return (start, declarationIndex);
    }

    /* ─────────────────────────────── mutation ─────────────────────────────── */

    private static int CapturedNumberCount(string source, string declaration, string pattern)
    {
        var prose = DocProseFor(source, declaration);
        var match = Regex.Match(prose, pattern);
        return !match.Success
            ? 0
            : match.Groups.Cast<Group>().Skip(1).Sum(g => Regex.Matches(g.Value, "[0-9]+").Count);
    }

    /// <summary>
    /// Adds one to the <paramref name="ordinal"/>-th integer of the declaration's doc run, in a copy of
    /// <paramref name="source"/>, but only when that integer is inside one of
    /// <paramref name="pattern"/>'s capture groups — otherwise null, so the sweep skips it.
    ///
    /// <para>Addressed by ORDINAL rather than by value, because a bare string replace of "36" would rewrite
    /// every 36 in the file and prove something other than what it was aimed at, and because repeated values
    /// (two 36s, two 306s, three 9s) have to stay individually addressable. The bump preserves width, so a
    /// zero-padded clock component cannot break the pattern it is being tested against.</para>
    /// </summary>
    private static string? BumpNumberInsideGroups(string source, string declaration, string pattern, int ordinal)
        => RewriteNumberInDocRun(source, declaration, pattern, ordinal, replacement: null);

    /// <summary>
    /// The doc-run-wide ordinal of the first integer that <paramref name="pattern"/> actually CAPTURES, or -1
    /// when it captures none. Ordinals are counted over the whole run because that is the coordinate
    /// <see cref="RewriteNumberInDocRun"/> maps back onto the source.
    /// </summary>
    private static int OrdinalOfFirstNumberInGroups(string prose, string pattern)
    {
        var match = Regex.Match(prose, pattern);
        if (!match.Success)
        {
            return -1;
        }

        var numbers = Regex.Matches(prose, "[0-9]+");
        for (var ordinal = 0; ordinal < numbers.Count; ordinal++)
        {
            var number = numbers[ordinal];
            if (match.Groups.Cast<Group>().Skip(1).Any(g =>
                    g.Success && number.Index >= g.Index && number.Index + number.Length <= g.Index + g.Length))
            {
                return ordinal;
            }
        }

        return -1;
    }

    /// <summary>
    /// Rewrites the <paramref name="ordinal"/>-th integer of the declaration's doc run in a COPY of
    /// <paramref name="source"/>, but only when that integer sits inside one of <paramref name="pattern"/>'s
    /// capture groups — otherwise null, so a caller can skip it.
    ///
    /// <para><paramref name="replacement"/> null means "add one", which is what the drift sweep wants; an
    /// explicit value is used by the population-merge mutation. Either way the bump preserves WIDTH, so a
    /// zero-padded clock component cannot break the pattern it is being tested against.</para>
    ///
    /// <para>Addressed by ORDINAL rather than by value, because a bare string replace of a figure would
    /// rewrite every copy of it in the file and prove something other than what it was aimed at — and
    /// because a doc run that states the same value in several places (a reading that is also the maximum,
    /// a population size quoted twice) has to keep each copy individually addressable.</para>
    /// </summary>
    private static string? RewriteNumberInDocRun(
        string source, string declaration, string pattern, int ordinal, string? replacement)
    {
        var prose = DocProseFor(source, declaration);
        var match = Regex.Match(prose, pattern);
        if (!match.Success)
        {
            return null;
        }

        var proseNumbers = Regex.Matches(prose, "[0-9]+");
        if (ordinal >= proseNumbers.Count)
        {
            return null;
        }

        var number = proseNumbers[ordinal];
        var inAGroup = match.Groups.Cast<Group>().Skip(1).Any(g =>
            g.Success && number.Index >= g.Index && number.Index + number.Length <= g.Index + g.Length);
        if (!inAGroup)
        {
            return null;
        }

        var (start, end) = DocRunSpan(source, declaration);
        var sourceNumbers = Regex.Matches(source[start..end], "[0-9]+");

        /* The two sequences must correspond exactly, or an ordinal in one does not name the same number in
           the other and the mutation would land somewhere unintended. */
        if (sourceNumbers.Count != proseNumbers.Count
            || !string.Equals(sourceNumbers[ordinal].Value, number.Value, StringComparison.Ordinal))
        {
            return null;
        }

        var bumped = (replacement
                ?? (int.Parse(number.Value, CultureInfo.InvariantCulture) + 1)
                    .ToString(CultureInfo.InvariantCulture))
            .PadLeft(number.Value.Length, '0');

        var offset = start + sourceNumbers[ordinal].Index;
        return source.Remove(offset, number.Value.Length).Insert(offset, bumped);
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new PinDriftException(message);
        }
    }

    private sealed class PinDriftException(string message) : Exception(message);

    private static string ReadTimescaleSupportSource([CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs"));

        Assert.True(File.Exists(path),
            $"TimescaleSupport.cs was not found at '{path}'. This guard parses the REAL file (resolved from "
            + "[CallerFilePath], so there is no copy to go stale); restore the path rather than pointing it "
            + "at a fixture.");
        return File.ReadAllText(path);
    }

    /* ─────────────────────────────── the scoping rule, file-wide ─────────────────────────────── */

    /// <summary>The member <see cref="TheOpenPopulationGuard_ReportsEachForbiddenShape"/> injects at, named
    /// by its own declaration line with the indentation included, so the injected lines land inside that
    /// member's doc run.</summary>
    private const string InsideSlotMember = "        InsideSlot,";

    /// <summary>
    /// What holds one doc run apart from the next in <see cref="JoinedDocProse"/>. Deliberately NOT
    /// whitespace: <c>\s</c> matches <c>\n</c> in .NET, so a newline separator is crossed by any pattern
    /// written with <c>\s+</c> — a run ending in "every" and an unrelated run opening with "readings" would
    /// then match as one claim and fail this guard over prose that states nothing of the kind. A
    /// non-whitespace sentinel cannot be absorbed by a whitespace class, so the property holds for every
    /// pattern here rather than only for the ones written carefully — and
    /// <see cref="TwoDocRuns_CannotCombineIntoAClaimNeitherOfThemMakes"/> is what says so, rather than this
    /// paragraph.
    ///
    /// <para>A <c>NUL</c> rather than a printable sentinel, because it cannot occur in source prose —
    /// so no comment can contain the thing that holds comments apart.</para>
    /// </summary>
    private const string DocRunSeparator = "\u0000";

    /// <summary>
    /// The universal-quantifier form: a quantifier over a population of OBSERVATIONS, which is a series
    /// that gains a member every hour this job runs.
    ///
    /// <para><b>The SHAPE rather than a list of phrasings (#3133).</b> One claim has as many wordings as
    /// it has sites — "every reading", "every reading taken so far", "every sample since" all say it — so a
    /// pattern enumerating wordings is a frozen list against a defect that rewords itself freely. The
    /// quantifier and the population noun are matched separately instead.</para>
    ///
    /// <para><b><c>run</c> is deliberately NOT a population noun here.</b> This file uses it for closed
    /// censuses ("every run that day after the boundary"), for the exclusion rule ("every run that started
    /// at or before the narrowing boundary") and for a statement about a SOURCE's completeness ("read as
    /// every run since the boundary rather than sampled") — none of which is the defect, and the third of
    /// which is the source-versus-reading distinction the rule paragraph itself draws. Including it would
    /// make this guard red on correct prose, which is how a guard gets edited away.</para>
    ///
    /// <para>A CLOSED claim in this shape ("every reading up to <c>04:20Z</c>") also matches, and that is
    /// intended rather than a false positive: a closed claim still needs an assertion, so it should arrive
    /// here to acquire one.</para>
    /// </summary>
    private static readonly Regex UniversalOverAnOpenSeries =
        new(@"\b(?:every|each|all)[ \t]+(?:reading|readings|sample|samples|snapshot|snapshots)\b",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// The relative-scope form. It looks scoped and is not: a doc comment has no timestamp of its own for
    /// "so far" to be read against.
    /// </summary>
    private static readonly Regex RelativeScope =
        new("readings so far", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// The rule sentence's own quotation of <see cref="RelativeScope"/>, which it names in order to reject
    /// it — the single permitted occurrence, and the anchor the permission is measured against.
    /// </summary>
    private static readonly Regex RelativeScopeRejected =
        new(@"""the readings so far"" carries a scope", RegexOptions.Compiled);

    /// <summary>
    /// The rule, checked against every doc-comment run in TimescaleSupport.cs at once.
    /// </summary>
    private static void AssertNoOpenPopulationClaim(string source)
    {
        var prose = JoinedDocProse(source);

        /* THE ANCHOR, first, because both clauses below are measured against it - and because a scan that
           read nothing produces zero here, which is what stops them passing vacuously. */
        var rejected = RelativeScopeRejected.Matches(prose).Count;
        Assert.True(rejected == 1,
            $"the sentence quoting \"the readings so far\" in order to REJECT it appears {rejected} times "
            + "rather than once, so this guard's single allowance is no longer anchored to it. If the rule "
            + "is being reworded, move this anchor with it deliberately; do not drop the assertion, because "
            + "zero here is also what a scan that read no doc comments at all looks like.");

        var universal = UniversalOverAnOpenSeries.Matches(prose).Count;
        Assert.True(universal == 0,
            $"{universal} doc comment(s) in TimescaleSupport.cs quantify over a population of readings "
            + "without closing it: that series gains a member every hour this job runs, so the claim is "
            + "false as soon as one reading falls outside it and nothing in the file can notice. Say what "
            + "the band IS - it is defined by its constant and needs no census. If a population claim is "
            + "genuinely wanted, CLOSE it (\"up to <hh:mmZ>\") and pin it here the way Verify pins the "
            + "ceiling paragraph's; a closed claim with no assertion rots just as fast.");

        var relative = RelativeScope.Matches(prose).Count;
        Assert.True(relative == rejected,
            $"\"readings so far\" appears {relative} times against the {rejected} the rule sentence quotes, "
            + "so a doc comment is now USING the form that sentence rejects. It reads as a scope and is "
            + "not one: a doc comment has no timestamp of its own for \"so far\" to be relative to.");
    }

    /// <summary>
    /// Every doc-comment run in the file, each joined to one line and put through
    /// <see cref="NormaliseDocProse"/> — so a phrase that wraps across <c>///</c> lines is one string, and
    /// neither emphasis nor dash style can hide a match.
    ///
    /// <para>Runs are held apart by <see cref="DocRunSeparator"/> rather than run together, so no two of
    /// them can manufacture a phrase that neither one contains.</para>
    /// </summary>
    private static string JoinedDocProse(string source)
    {
        var runs = new List<string>();
        var current = new List<string>();

        foreach (var raw in source.Split('\n'))
        {
            var line = raw.Trim('\r', ' ', '\t');
            if (line.StartsWith("///", StringComparison.Ordinal))
            {
                current.Add(line.Length > 3 ? line[3..].Trim() : string.Empty);
                continue;
            }

            if (current.Count > 0)
            {
                runs.Add(string.Join(" ", current));
                current.Clear();
            }
        }

        if (current.Count > 0)
        {
            runs.Add(string.Join(" ", current));
        }

        return NormaliseDocProse(string.Join(DocRunSeparator, runs));
    }

    /// <summary>
    /// Inserts <paramref name="docLines"/> immediately above <paramref name="member"/>'s declaration line in
    /// a COPY of <paramref name="source"/>, extending that member's doc run — the coordinate the whole-file
    /// scan reads, so a mutation lands where the guard looks. Indentation comes from the declaration itself
    /// rather than being written in, so a reindented file cannot produce a line the scan quietly skips.
    /// </summary>
    private static string InjectDocLines(string source, string member, params string[] docLines)
    {
        var index = source.IndexOf(member, StringComparison.Ordinal);
        Require(index > 0,
            $"{ParseMiss}: could not find '{member}' in TimescaleSupport.cs, so no mutation was injected and "
            + "the case that follows would pass on a file it never changed");
        Require(source.IndexOf(member, index + 1, StringComparison.Ordinal) < 0,
            $"{ParseMiss}: '{member}' appears more than once, so a mutation aimed at it is not aimed at one "
            + "known doc run");

        var indent = member[..(member.Length - member.TrimStart().Length)];
        var mutated = source.Insert(index, string.Concat(docLines.Select(line => indent + line + "\r\n")));
        Assert.NotEqual(source, mutated);
        return mutated;
    }
}
