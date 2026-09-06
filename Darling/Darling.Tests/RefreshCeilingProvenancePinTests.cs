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
/// provenance and the arithmetic that provenance rests on (#3069).
///
/// <para><b>Why this exists.</b> The constant's doc comment used to present 864 s as the highest figure
/// recorded under the narrowed window, alongside three companions "climbing with volume rather than
/// settling". A ten-run status-verified series then rose to 594 s and settled back, and the 864 s run turned
/// out to end one second AFTER the boundary the whole issue splits on — so its regime membership is
/// undetermined. The comment records the distribution now. A distribution written in prose has no way to
/// notice that its own summary no longer follows from it, which is how the superseded characterisation
/// survived, so every summary figure is derived here and the prose is required to agree.</para>
///
/// <para><b>Derive, don't restate.</b> Nothing below hardcodes 864, 900, 36, 306 or 338. Every expected value
/// comes either from the constants (<see cref="TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds"/>,
/// <see cref="TimescaleSupport.RefreshPhaseSlotSeconds"/>,
/// <see cref="TimescaleSupport.RefreshPhaseStepMinutes"/>,
/// <see cref="TimescaleSupport.RefreshSlotWarningSeconds"/>) or from the SERIES THE COMMENT ITSELF PUBLISHES.
/// A pin that restated the summary would go stale by precisely the mechanism it exists to stop.</para>
///
/// <para><b>Which numeral KIND these are, because the repo handles two differently and neither #3072 nor
/// #3073 says so out loud.</b> A numeral that restates an adjacent list gets DELETION offered in its
/// failure message — dropping the sentence is as correct an outcome as correcting the figure — while a
/// numeral that is program output gets "fix the pattern rather than the count", because a transcript has
/// to keep showing what the tool really prints. <c>ReadmeDerivedCountPinTests</c> implements both halves
/// and states neither, so the rule has to be inferred from the disagreement between two of its own
/// failure texts. All thirteen patterns here are the first kind — derived arithmetic restated in prose —
/// which is why deletion is offered throughout; and this paragraph is NARRATION of an existing rule, not
/// a counted claim, so nothing checks it and nothing should.</para>
///
/// <para><b>What is deliberately NOT pinned, said plainly rather than left looking covered.</b> The two
/// snapshot readings are quoted evidence, not derived quantities — nothing in the code can know them — so
/// they are held only by the DISJOINTNESS the paragraph is about (see
/// <see cref="TheInadmissibleReadings_CannotBeFoldedIntoTheStatusVerifiedSeries"/>), which is the failure
/// mode that matters: an unfiltered reading migrating into the population a constant may be set from. The
/// clock time of the unobserved run and the pre-narrowing band are likewise evidence and carry no pin.</para>
///
/// <para><b>The guard is itself guarded, three ways.</b> Every extraction asserts its pattern MATCHED before
/// a value is compared. <see cref="EveryNumericPin_ReportsAnInjectedDrift"/> bumps each captured number ONE
/// AT A TIME in a mutated copy and requires the identical verification to fail — after separately asserting
/// the pattern still matches, so a mutation that merely broke a regex cannot pass as a caught drift. And
/// <see cref="Verify"/> requires that it CONSUMED every pin in <see cref="Pins"/>, because a pattern defined
/// in the list and never asserted against anything looks exactly like a pattern doing work.</para>
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

    private static int Ceiling => TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds;

    private static int Slot => TimescaleSupport.RefreshPhaseSlotSeconds;

    private static int SlotMinutes => TimescaleSupport.RefreshPhaseStepMinutes;

    private static int WatchLine => TimescaleSupport.RefreshSlotWarningSeconds;

    /// <summary>
    /// The pinned sentences, and the ONLY place a pattern is written down — <see cref="Verify"/> looks them up
    /// by name, so the sweep and the assertions cannot end up testing different regexes.
    ///
    /// <para>Two members carry figures derived from the ceiling: the constant itself, and
    /// <see cref="TimescaleSupport.CompressionPhaseMinutes"/>, whose exclude-whole reasoning quotes it in
    /// MINUTES. Each pattern names the doc run it is read out of, so none of them can match a similar
    /// sentence elsewhere in a four-thousand-line file.</para>
    ///
    /// <para><b>ASCII-only patterns on purpose, and the reason is <see cref="DocProseFor"/> rather than
    /// anything in the patterns themselves.</b> It normalises <c>—</c> and <c>–</c> to <c>-</c>
    /// before any pattern runs, so not one of the thirteen has to match a dash variant — and not one does.
    /// Matching a dash through a source-encoding round trip is a way for a pattern to stop matching for a
    /// reason that has nothing to do with what it guards, and normalising at the extractor removes that at
    /// the source instead of working around it thirteen times. The single literal hyphen below, in
    /// <c>([0-9]+)-second slot</c>, is a plain ASCII hyphen in the prose rather than a normalised
    /// dash.</para>
    /// </summary>
    private static IEnumerable<(string Name, string Declaration, string Pattern, bool DriftSwept)> Pins()
    {
        yield return (
            "the transition run's clock arithmetic",
            CeilingDeclaration,
            @"<c>([0-9][0-9]):([0-9][0-9]):([0-9][0-9])</c> to <c>([0-9][0-9]):([0-9][0-9]):([0-9][0-9])</c>",
            true);

        yield return (
            "the boundary timestamp",
            CeilingDeclaration,
            @"<c>([0-9][0-9]):([0-9][0-9]):([0-9][0-9])</c>, one second BEFORE",
            true);

        yield return (
            "the run count",
            CeilingDeclaration,
            @"([0-9]+) consecutive runs",
            true);

        yield return (
            "the published series",
            CeilingDeclaration,
            @"<c>([0-9]+(?:, [0-9]+)+)</c> seconds",
            true);

        yield return (
            "the clean-series summary",
            CeilingDeclaration,
            @"remaining ([0-9]+) run from ([0-9]+) s to ([0-9]+) s, mean ([0-9]+) s, middle reading ([0-9]+) s",
            true);

        yield return (
            "the slot-clearance contrast",
            CeilingDeclaration,
            @"clears the slot by ([0-9]+) s, where this constant clears it by ([0-9]+) s",
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
            "the downward-edit consequence",
            CeilingDeclaration,
            @"toward the observed ([0-9]+) s would leave ([0-9]+) s of margin in place of ([0-9]+) s",
            true);

        yield return (
            "the low the grid is deliberately not sized against",
            CeilingDeclaration,
            @"not against the ([0-9]+) s low",
            true);

        yield return (
            "the margin sentence",
            CeilingDeclaration,
            @"At ([0-9]+) s against a ([0-9]+)-second slot the margin is already only ([0-9]+) seconds",
            true);

        yield return (
            "the exclude-whole occupancy sentence",
            CompressionMinutesDeclaration,
            @"occupies ([0-9]+)\.([0-9]) of the ([0-9]+) minutes",
            true);
    }

    [Fact]
    public void EveryProvenanceClaim_FollowsFromTheConstantsAndThePublishedSeries()
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

        var series = Numbers(prose, "the published series");
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
    /// individual reading in the published series load-bearing — a bump to a middle-ranked reading changes
    /// neither the range nor the middle, and is caught only because the mean is stated too. Bumping every
    /// group together would have hidden that.</para>
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
    public void TheDerivedFiguresAreExactlyStateable_AndTheCleanSeriesSitsWhereTheProseSaysItDoes()
    {
        /* CompressionPhaseMinutes quotes the ceiling in minutes to one decimal, which is only faithful while
           the ceiling is an exact tenth of a minute: 14.4 for a 14.41666 value is a rounded claim wearing an
           exact one's clothes. */
        Assert.Equal(0, Ceiling * 10 % 60);

        var clean = CleanSeries(DocProseFor(ReadTimescaleSupportSource(), CeilingDeclaration));
        Assert.NotEmpty(clean);

        /* The mean is stated as a whole number of seconds. */
        Assert.Equal(0, clean.Sum() % clean.Length);

        /* The relationship the whole resolution turns on, as an assertion over the WHOLE inventory rather
           than one true clause standing in for it: every cleanly post-boundary run came in under the value
           the grid is sized against, which is what makes that value safe without making it measured. */
        Assert.All(clean, reading => Assert.True(reading < Ceiling,
            $"a cleanly post-boundary reading of {reading} s is at or past the {Ceiling} s the compression "
            + "grid is sized against, so the ceiling is no longer above the observed range and #3069's "
            + "resolution — record the provenance, leave the value — no longer holds."));
        Assert.All(clean, reading => Assert.True(reading < WatchLine,
            $"a cleanly post-boundary reading of {reading} s is at or past the {WatchLine} s watch line, so "
            + "the doc comment's claim that every clean reading is below it is false."));

        /* The contrast the prose draws: the undetermined run is in the warning band, the clean ones are not. */
        Assert.True(Ceiling >= WatchLine,
            "the ceiling no longer classifies as a warning, so the doc comment's contrast between it and the "
            + "clean series is stale.");
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

        var series = Read("the published series");
        Require(series.Length > 0, "the published series parsed to nothing");

        /* THE LINK TO THE CONSTANT. The comment's first reading IS this constant — that is what makes the
           transition-run paragraph about this number rather than about some other run. */
        Require(series.Count(v => v == Ceiling) == 1,
            $"the published series {Join(series)} does not contain {Ceiling} exactly once, so the comment's "
            + "transition-run paragraph is no longer about the value of this constant");

        var clean = CleanSeries(ceilingProse);
        Require(clean.Length == series.Length - 1, "setting the transition run aside did not leave one fewer reading");
        Require(clean.Length > 0, "the clean series is empty, so every summary figure below would be vacuous");
        Require(clean.Sum() % clean.Length == 0,
            "the clean series' mean is no longer a whole number of seconds, so the prose cannot state it as "
            + "one — restate it rather than removing this check");

        var sorted = clean.OrderBy(v => v).ToArray();
        var min = sorted[0];
        var max = sorted[^1];
        var middle = sorted[sorted.Length / 2];
        var mean = clean.Sum() / clean.Length;

        var runCount = Read("the run count");
        Require(runCount[0] == series.Length,
            $"the comment says {runCount[0]} consecutive runs and then lists {series.Length}");

        var summary = Read("the clean-series summary");
        Require(summary[0] == clean.Length, $"stated clean count {summary[0]} against {clean.Length} listed");
        Require(summary[1] == min, $"stated low {summary[1]} s against {min} s listed");
        Require(summary[2] == max, $"stated high {summary[2]} s against {max} s listed");
        Require(summary[3] == mean, $"stated mean {summary[3]} s against {mean} s computed");
        Require(summary[4] == middle, $"stated middle {summary[4]} s against {middle} s computed");

        var clearance = Read("the slot-clearance contrast");
        Require(clearance[0] == Slot - max, $"stated clearance {clearance[0]} s against {Slot - max} s derived");
        Require(clearance[1] == Slot - Ceiling, $"stated margin {clearance[1]} s against {Slot - Ceiling} s derived");

        var downward = Read("the downward-edit consequence");
        Require(downward[0] == max, $"stated observed maximum {downward[0]} s against {max} s listed");
        Require(downward[1] == Slot - max, $"stated replacement margin {downward[1]} s against {Slot - max} s derived");
        Require(downward[2] == Slot - Ceiling, $"stated current margin {downward[2]} s against {Slot - Ceiling} s derived");

        var low = Read("the low the grid is deliberately not sized against");
        Require(low[0] == min, $"stated low {low[0]} s against {min} s listed");

        var margin = Read("the margin sentence");
        Require(margin[0] == Ceiling, $"stated ceiling {margin[0]} s against {Ceiling} s");
        Require(margin[1] == Slot, $"stated slot {margin[1]} s against {Slot} s");
        Require(margin[2] == Slot - Ceiling, $"stated margin {margin[2]} s against {Slot - Ceiling} s derived");

        /* Clock arithmetic. The transition-run finding IS this subtraction: the run ends one second after the
           boundary, which is why its regime membership cannot be settled from the data in hand. */
        var run = Read("the transition run's clock arithmetic");
        var started = Seconds(run[0], run[1], run[2]);
        var ended = Seconds(run[3], run[4], run[5]);
        Require(ended - started == Ceiling,
            $"the quoted run spans {ended - started} s, not the {Ceiling} s this constant holds");

        var boundary = Read("the boundary timestamp");
        Require(ended - Seconds(boundary[0], boundary[1], boundary[2]) == 1,
            "the quoted run no longer ends exactly one second after the quoted boundary, so the "
            + "transition-run reading the whole provenance paragraph rests on is not what the numbers say");

        /* The snapshot readings stay out of the admissible population, HOWEVER MANY of them there are. */
        var snapshot = Read("the inadmissible snapshot readings");
        Require(snapshot.Length > 0, "the inadmissible snapshot readings parsed to nothing");
        Require(!snapshot.Intersect(series).Any(),
            $"an unfiltered snapshot reading appears in the status-verified series {Join(series)}, so the two "
            + "populations have been run together");

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

        /* CompressionPhaseMinutes' exclude-whole reasoning, in minutes. */
        Require(Ceiling * 10 % 60 == 0,
            "the ceiling is no longer an exact tenth of a minute, so the occupancy figure cannot be stated to "
            + "one decimal");
        var occupancy = Read("the exclude-whole occupancy sentence");
        Require(occupancy[0] == Ceiling / 60, $"stated whole minutes {occupancy[0]} against {Ceiling / 60}");
        Require(occupancy[1] == Ceiling * 10 / 60 % 10,
            $"stated tenth of a minute {occupancy[1]} against {Ceiling * 10 / 60 % 10}");
        Require(occupancy[2] == SlotMinutes, $"stated slot {occupancy[2]} minutes against {SlotMinutes}");

        /* And nothing in the pin list went unused: a pattern that is never asserted against reads, from the
           list, exactly like one that is. */
        var unused = Pins().Select(p => p.Name).Where(n => !consumed.Contains(n)).ToArray();
        Require(unused.Length == 0,
            "these pinned sentences are declared but never verified, so they guard nothing: "
            + string.Join(", ", unused));
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
    /// The published series with the ONE undetermined-regime reading set aside — matched against this
    /// constant rather than taken by position, so a reordered list cannot silently set a different run aside.
    /// </summary>
    private static int[] CleanSeries(string prose)
    {
        var removed = false;
        var clean = new List<int>();
        foreach (var reading in Numbers(prose, "the published series"))
        {
            if (!removed && reading == Ceiling)
            {
                removed = true;
                continue;
            }

            clean.Add(reading);
        }

        Require(removed, $"the published series does not contain {Ceiling}, so nothing was set aside");
        return clean.ToArray();
    }

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
    /// <para>Addressed by ORDINAL rather than by value, because a bare string replace of "36" would rewrite
    /// every 36 in the file and prove something other than what it was aimed at — and because repeated
    /// values (36 appears four times in this doc run, 306 twice, 9 three times) have to stay individually
    /// addressable.</para>
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
}
