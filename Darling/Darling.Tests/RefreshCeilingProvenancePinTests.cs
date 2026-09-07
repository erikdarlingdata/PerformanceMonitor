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
/// <see cref="TimescaleSupport.RefreshPhaseStepMinutes"/>,
/// <see cref="TimescaleSupport.RefreshSlotWarningSeconds"/>) or from the POPULATION THE COMMENT ITSELF
/// PUBLISHES. A pin that restated the summary would go stale by precisely the mechanism it exists to
/// stop.</para>
///
/// <para><b>One pin is written to expire, deliberately.</b> The comment's reason for taking the maximum
/// rather than a percentile is partly that at this sample size the 95th percentile IS the maximum by nearest
/// rank. <see cref="Verify"/> checks that, so the pin goes red once the population grows past the point where
/// it holds — which is the moment the maximum-versus-percentile decision genuinely has to be re-taken rather
/// than inherited. A check that simply kept passing would let the stated reason quietly stop being the real
/// one.</para>
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
/// they are held only by the DISJOINTNESS the paragraph is about (see
/// <see cref="TheInadmissibleReadings_CannotBeFoldedIntoTheStatusVerifiedSeries"/>), which is the failure
/// mode that matters: an unfiltered reading migrating into the population a constant may be set from. The
/// pre-narrowing band and the 3.8x ratio drawn against it are likewise evidence — that band is #3012's
/// measurement and no constant here spells it — so they carry no pin. The excluded run's 1.45x ratio to the
/// population maximum IS pinned, because both of its terms are stated here.</para>
///
/// <para><b>The guard is itself guarded, three ways.</b> Every extraction asserts its pattern MATCHED before
/// a value is compared. <see cref="EveryNumericPin_ReportsAnInjectedDrift"/> bumps each captured number ONE
/// AT A TIME in a mutated copy and requires the identical verification to fail — after separately asserting
/// the pattern still matches, so a mutation that merely broke a regex cannot pass as a caught drift. And
/// <see cref="Verify"/> requires that it CONSUMED every pin in <see cref="Pins"/>, because a pattern defined
/// in the list and never asserted against anything looks exactly like a pattern doing work.</para>
///
/// <para><b>And the SCOPING rule, held file-wide rather than only where it is stated (#3133).</b>
/// <see cref="Verify"/> requires the ceiling paragraph's own enumeration to carry a CLOSED scope;
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
        "public const int RefreshSlotWarningSeconds";

    private static int Ceiling => TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds;

    private static int Slot => TimescaleSupport.RefreshPhaseSlotSeconds;

    private static int SlotMinutes => TimescaleSupport.RefreshPhaseStepMinutes;

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
            @"([0-9]+)\.([0-9]+)x slower than the largest reading",
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
            @"Together ([0-9]+) runs spanning ([0-9]+) s to ([0-9]+) s, mean ([0-9]+)\.([0-9]+) s, median ([0-9]+) s",
            true);

        yield return (
            "the percentile decision",
            CeilingDeclaration,
            @"by nearest rank over ([0-9]+) readings the 95th percentile IS the largest of them, and the 90th is ([0-9]+) s, only ([0-9]+) s below it",
            true);

        /* ARITY-FREE, and that is the point rather than a convenience. This list gains a reading every hour
           the job runs - it went from two to three while #3077 was in review - so a pattern shaped
           "(N) s and (N) s" would encode the count and go stale on the next snapshot. Encoding a count in a
           regex is the stale-enumeration defect with a test wrapped around it, which is the exact thing this
           file exists to prevent. One group holding the whole list, flattened by Numbers().

           NOT drift-swept either: these are quoted readings, so bumping a digit only produces another number
           the code cannot contradict. What IS checked is the rule the paragraph is actually about - see
           TheInadmissibleReadings_CannotBeFoldedIntoTheStatusVerifiedSeries and the as-at scope guard in
           Verify. */
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

        yield return (
            "the exclude-whole occupancy sentence",
            CompressionMinutesDeclaration,
            @"occupies ([0-9]+)\.([0-9]+) of the ([0-9]+) minutes",
            true);

        yield return (
            "the guard-band arithmetic the exclusion rests on",
            CompressionMinutesDeclaration,
            @"band is ([0-9]+), so applying the ordinary band to this slot would admit ([0-9]+) minutes that sit INSIDE the refresh",
            true);

        yield return (
            "the minutes left on the table",
            CompressionMinutesDeclaration,
            @"The other ([0-9]+) minutes of the slot are past the refresh",
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
            @"<see cref=""CompressionPhaseGuardMinutes""/> band, ([0-9]+) s, and it sits BELOW <see cref=""HeaviestHourlyRefreshObservedCeilingSeconds""/>",
            true);

        yield return (
            "the population size and range the exclusion defers to",
            CompressionMinutesDeclaration,
            @"population is ([0-9]+) readings and still moving \(([0-9]+) s to ([0-9]+) s within the clean regime\)",
            true);
    }

    [Fact]
    public void EveryDerivationClaim_FollowsFromTheConstantsAndThePublishedPopulation()
    {
        Verify(ReadTimescaleSupportSource());
    }

    /// <summary>
    /// The paragraph's whole job is to keep the unfiltered self-metrics readings OUT of the population a
    /// constant may be set from, so the mutation is the merge itself rather than a digit bump: put a
    /// status-verified reading in the snapshot pair's place and require verification to fail.
    ///
    /// <para>Both populations are asserted NON-EMPTY first. Disjointness is a claim about an absence, and an
    /// absence is also exactly what a pattern matching nothing produces — so emptiness has to be excluded
    /// before the disjointness result says anything at all.</para>
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
        Assert.Empty(series.Intersect(snapshot));

        /* Injected through the same ordinal rewrite the drift sweep uses, rather than by string-replacing
           the reading. A bare Replace would depend on the surrounding emphasis tags - the very coupling
           DocProseFor now normalises away - and would silently find nothing if someone unbolded the figure,
           which is a mutation that proves nothing wearing a caught drift's clothes. */
        var snapshotPattern = PatternFor("the inadmissible snapshot readings");
        var firstReading = OrdinalOfFirstNumberInGroups(prose, snapshotPattern);
        Assert.True(firstReading >= 0, "the snapshot pair's pattern captured no number, so its pin is vacuous.");

        var mutated = RewriteNumberInDocRun(
            source,
            CeilingDeclaration,
            snapshotPattern,
            firstReading,
            series[^1].ToString(CultureInfo.InvariantCulture));

        Assert.True(mutated is not null,
            "could not inject a series reading into the snapshot pair, so nothing was proved.");
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
        /* CompressionPhaseMinutes quotes the ceiling in minutes to one decimal, which is only faithful while
           the ceiling is an exact tenth of a minute: 9.9 for a 9.91666 value is a rounded claim wearing an
           exact one's clothes. */
        Assert.Equal(0, Ceiling * 10 % 60);

        var population = CleanPopulation(DocProseFor(ReadTimescaleSupportSource(), CeilingDeclaration));
        Assert.NotEmpty(population);

        /* THE DERIVATION, as one assertion: this constant IS the maximum of the published population. Not
           "above it" - equal to it, because the comment says the estimator is the maximum and a value merely
           above the maximum is the unestablished-bound shape the derivation replaces. */
        Assert.Equal(population.Max(), Ceiling);

        /* The mean is stated to one decimal and the median as a whole number, so both have to be exactly
           stateable in those shapes. */
        Assert.Equal(0, population.Sum() * 10 % population.Length);
        var sorted = population.OrderBy(v => v).ToArray();
        Assert.Equal(0, (sorted[(sorted.Length - 1) / 2] + sorted[sorted.Length / 2]) % 2);

        /* The relationship the grid depends on, over the WHOLE population rather than one true clause
           standing in for it: every clean run fits inside the slot, and inside the routine band. */
        Assert.All(population, reading => Assert.True(reading < Slot,
            $"a clean post-boundary reading of {reading} s is at or past the {Slot} s refresh slot, so the "
            + "compression grid's stated precondition is false and #3035 has to be re-derived rather than "
            + "renumbered"));
        Assert.All(population, reading => Assert.True(reading < WatchLine,
            $"a clean post-boundary reading of {reading} s is at or past the {WatchLine} s watch line, so "
            + "the doc comment's claim that every reading in the population is below it is false"));

        /* And the contrast the prose draws: the watch line sits ABOVE the derived ceiling, which is what
           makes a crossing news rather than a restatement of the grid's own sizing figure. */
        Assert.True(Ceiling < WatchLine,
            "the ceiling now classifies as a warning, so the doc comment's claim that the watch line sits "
            + "above the observed range is stale.");
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
    /// the band is described by
    /// <see cref="TimescaleSupport.RefreshSlotWarningSeconds"/> and by what the level means, so there is no
    /// census for a later edit to keep current. The population that IS published carries its own assertion
    /// in <see cref="TheDerivedFiguresAreExactlyStateable_AndTheConstantIsThePopulationMaximum"/>.</para>
    /// </summary>
    [Fact]
    public void NoOpenPopulationClaim_SurvivesOutsideTheSentenceThatRejectsIt()
    {
        AssertNoOpenPopulationClaim(ReadTimescaleSupportSource());
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
        var mean10 = population.Sum() * 10 / population.Length;
        var median = (sorted[(sorted.Length - 1) / 2] + sorted[sorted.Length / 2]) / 2;

        /* THE DERIVATION, and the one relationship everything else here is decoration on: the constant is
           the maximum of the population the comment publishes. */
        Require(max == Ceiling,
            $"the published population {Join(sorted)} has maximum {max} s, but this constant holds "
            + $"{Ceiling} s — the comment says the estimator is the maximum, so one of the two is wrong");

        Require(population.Sum() * 10 % population.Length == 0,
            "the population's mean is no longer an exact tenth of a second, so the prose cannot state it to "
            + "one decimal — restate it rather than removing this check");
        Require((sorted[(sorted.Length - 1) / 2] + sorted[sorted.Length / 2]) % 2 == 0,
            "the population's median is no longer a whole number of seconds, so the prose cannot state it as "
            + "one — restate it rather than removing this check");

        var summary = Read("the population summary");
        Require(summary[0] == population.Length,
            $"stated count {summary[0]} against {population.Length} listed");
        Require(summary[1] == min, $"stated low {summary[1]} s against {min} s listed");
        Require(summary[2] == max, $"stated high {summary[2]} s against {max} s listed");
        Require(summary[3] * 10 + summary[4] == mean10,
            $"stated mean {summary[3]}.{summary[4]} s against {mean10 / 10}.{mean10 % 10} s computed");
        Require(summary[5] == median, $"stated median {summary[5]} s against {median} s computed");

        /* THE ESTIMATOR CHOICE, checked rather than asserted in prose — and written to EXPIRE. The comment
           takes the maximum partly because at this sample size the 95th percentile is the maximum by nearest
           rank; when the population grows past that, this goes red and the decision gets re-taken instead of
           inherited. */
        var percentile = Read("the percentile decision");
        Require(percentile[0] == population.Length,
            $"stated sample size {percentile[0]} against {population.Length} listed");
        Require(NearestRank(sorted, 95) == max,
            $"the 95th percentile of {Join(sorted)} is {NearestRank(sorted, 95)} s and no longer the "
            + $"{max} s maximum, so the comment's stated reason for taking the maximum rather than a "
            + "percentile has expired. RE-TAKE the decision (issue #3101 records the trade) rather than "
            + "editing this assertion — that is what this pin is for");
        Require(percentile[1] == NearestRank(sorted, 90),
            $"stated 90th percentile {percentile[1]} s against {NearestRank(sorted, 90)} s computed");
        Require(percentile[2] == max - NearestRank(sorted, 90),
            $"stated gap {percentile[2]} s against {max - NearestRank(sorted, 90)} s derived");

        var low = Read("the low the grid is deliberately not sized against");
        Require(low[0] == min, $"stated low {low[0]} s against {min} s listed");

        var margin = Read("the margin sentence");
        Require(margin[0] == Ceiling, $"stated ceiling {margin[0]} s against {Ceiling} s");
        Require(margin[1] == Slot, $"stated slot {margin[1]} s against {Slot} s");
        Require(margin[2] == Slot - Ceiling, $"stated margin {margin[2]} s against {Slot - Ceiling} s derived");

        /* The EXCLUDED run. Its clock arithmetic has to reproduce its own stated duration, it has to end one
           second after the stated boundary, and its ratio to the population maximum has to be the ratio the
           prose draws — which is what keeps that paragraph about this population rather than free-floating. */
        var run = Read("the excluded run's clock arithmetic");
        var started = Seconds(run[0], run[1], run[2]);
        var ended = Seconds(run[3], run[4], run[5]);
        var excluded = Read("the excluded run's duration");
        Require(ended - started == excluded[0],
            $"the quoted run spans {ended - started} s, not the {excluded[0]} s stated for it");
        Require(excluded[0] > Ceiling,
            $"the excluded run's {excluded[0]} s is no longer above the {Ceiling} s population maximum, so "
            + "the prose's \"slower than the largest reading\" is false");

        var boundary = Read("the boundary timestamp");
        Require(ended - Seconds(boundary[0], boundary[1], boundary[2]) == 1,
            "the quoted run no longer ends exactly one second after the quoted boundary, so the circularity "
            + "the open-question paragraph is about is not what the numbers say");

        var ratio = Read("the excluded run's ratio to the population maximum");
        Require(ratio[0] * 100 + ratio[1] == excluded[0] * 100 / Ceiling,
            $"stated ratio {ratio[0]}.{ratio[1]}x against {excluded[0] * 100 / Ceiling / 100}."
            + $"{excluded[0] * 100 / Ceiling % 100}x derived from {excluded[0]} s over {Ceiling} s");

        /* The snapshot readings stay out of the admissible population, HOWEVER MANY of them there are. */
        var snapshot = Read("the inadmissible snapshot readings");
        Require(snapshot.Length > 0, "the inadmissible snapshot readings parsed to nothing");
        Require(!snapshot.Intersect(population).Any(),
            $"an unfiltered snapshot reading appears in the status-verified population {Join(sorted)}, so "
            + "the two populations have been run together");

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

        /* CompressionPhaseMinutes' exclude-whole reasoning: the ceiling in minutes, the guard band against
           it, and the minutes the exclusion declines to recover. */
        Require(Ceiling * 10 % 60 == 0,
            "the ceiling is no longer an exact tenth of a minute, so the occupancy figure cannot be stated to "
            + "one decimal");
        var occupancy = Read("the exclude-whole occupancy sentence");
        Require(occupancy[0] == Ceiling / 60, $"stated whole minutes {occupancy[0]} against {Ceiling / 60}");
        Require(occupancy[1] == Ceiling * 10 / 60 % 10,
            $"stated tenth of a minute {occupancy[1]} against {Ceiling * 10 / 60 % 10}");
        Require(occupancy[2] == SlotMinutes, $"stated slot {occupancy[2]} minutes against {SlotMinutes}");

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
    /// rather than from one, because the comment publishes them separately — the boundary day's tail is a
    /// census of that day and the days after it are a sample, and collapsing the two into a single list in
    /// the prose would erase a distinction the estimator's stated limitation rests on.
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
        var prose = string.Join(" ", run)
            .Replace("<b>", string.Empty, StringComparison.Ordinal)
            .Replace("</b>", string.Empty, StringComparison.Ordinal)
            .Replace('\u2014', '-')
            .Replace('\u2013', '-');
        prose = Regex.Replace(prose, @"\s+", " ").Trim();
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
        new(@"\b(?:every|each|all)\s+(?:reading|readings|sample|samples|snapshot|snapshots)\b",
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
    /// Every doc-comment run in the file, each joined to one line and normalised the way
    /// <see cref="DocProseFor"/> normalises a single declaration's — so a phrase that wraps across
    /// <c>///</c> lines is one string, and neither emphasis nor dash style can hide a match.
    ///
    /// <para>Runs are kept newline-separated from each other rather than run together, so two adjacent
    /// runs cannot manufacture a phrase that neither of them contains.</para>
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

        var prose = string.Join("\n", runs)
            .Replace("<b>", string.Empty, StringComparison.Ordinal)
            .Replace("</b>", string.Empty, StringComparison.Ordinal)
            .Replace('—', '-')
            .Replace('–', '-');
        return Regex.Replace(prose, "[ \t]+", " ").Trim();
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
