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
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgCappedRead"/>, and the two surfaces that have to keep asking it (#3424, #3425).
///
/// <para><b>What a guard on this has to do that a cap test normally does not.</b> A truncation pin is
/// trivially vacuous: a fixture smaller than the cap makes every arm agree, so the test passes whichever
/// classifier shipped and asserts nothing about truncation at all. Every case below therefore names a
/// population LARGER than the limit it is read at, the boundary cases sit at exactly the limit and one row
/// either side of it, and the two measured production shapes are carried as their own cases so a
/// re-derivation cannot quietly change what the tool does on the servers the issues were written about.</para>
///
/// <para><b>And the arms are asserted DISCRIMINATING, not merely present.</b> The value of this classifier
/// is entirely that <see cref="PgCappedReach.Unreachable"/> and <see cref="PgCappedReach.Partial"/> say
/// different things - one means raise the limit and the other means it will never help - so a pin that only
/// checked an arm was returned would survive the two being collapsed into one. The causes are compared
/// pairwise, and the cause is compared rather than the message because the figures always differ and
/// comparing them would report discrimination this class had stopped providing.</para>
/// </summary>
public sealed class PgCappedReadTests
{
    private const string Remedy = "USE THE FILTER.";

    /// <summary>
    /// The measured #3424 shape: 1,779 answerless indexes ahead of 726 answers covering 213.8 GB, read at
    /// the maximum permitted limit of 1,000.
    ///
    /// <para>The whole issue in one assertion. The leading block alone exceeds the cap, so the response is
    /// 1,000 answerless rows and no answer can appear in it - which is what the empirical check in the issue
    /// found, and it is arithmetic rather than luck. <c>ARaisedLimitWouldHelp</c> has to be FALSE here: it is
    /// the one figure that separates this from a limit somebody set too low.</para>
    /// </summary>
    [Fact]
    public void TheMeasuredIndexBloatShape_IsUnreachableAtTheMaximumLimit()
    {
        var verdict = PgCappedRead.Classify(
            rowsAhead: 1_779, wantedRows: 726, returnedRows: 1_000,
            limit: McpHelpers.MaxTop, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.Unreachable, verdict.Reach);
        Assert.Equal(0L, verdict.ReachableRows);
        Assert.Equal(0L, verdict.ReachableAtMaxRows);
        Assert.Equal(726L, verdict.WithheldRows);
        Assert.False(verdict.IsComplete);
        Assert.False(
            verdict.ARaisedLimitWouldHelp,
            "the 726 answers sit behind 1,779 answerless rows against a 1,000-row ceiling, so no permitted "
            + "limit reaches one of them - reporting that a larger limit would help is the advice #3278 "
            + "already rejected in its own opening sentence");
        Assert.Contains(Remedy, verdict.Cause, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same server with <c>answered_only</c>: nothing ahead of the answers, so a limit of 25 is the top
    /// 25 of 726 by reclaimable bytes.
    ///
    /// <para>The fix's own case, and it has to land on <see cref="PgCappedReach.RankedTail"/> rather than on
    /// <see cref="PgCappedReach.Complete"/> - 25 of 726 is a page, and a reader told it was complete would
    /// publish a 25-index ranking as the server's.</para>
    /// </summary>
    [Fact]
    public void TheSameShapeUnderAnsweredOnly_IsARankedTailAndNotComplete()
    {
        var verdict = PgCappedRead.Classify(
            rowsAhead: 0, wantedRows: 726, returnedRows: 25,
            limit: 25, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.RankedTail, verdict.Reach);
        Assert.False(verdict.IsComplete);
        Assert.Equal(25L, verdict.ReachableRows);
        Assert.Equal(701L, verdict.WithheldRows);
        Assert.True(
            verdict.ARaisedLimitWouldHelp,
            "with nothing ahead of them, 726 answers are entirely reachable at the 1,000-row maximum - this "
            + "is the arm where raising the limit IS the answer");
    }

    /// <summary>
    /// The measured #3425 shape at ten databases: 102 extension names per database, of which 2 are created,
    /// so 1,000 non-created rows sort ahead of 20 created ones against a 1,000-row ceiling.
    ///
    /// <para>Ten is the threshold the issue measured and this is why: the leading block reaches the cap
    /// exactly, so <c>plpgsql</c> - installed in every database of the server - cannot appear at any limit,
    /// and an install census built from the rows comes back looking complete. Nine databases is the control
    /// below.</para>
    /// </summary>
    [Fact]
    public void TheMeasuredExtensionShape_TipsToUnreachableAtTenDatabases()
    {
        const int PerDatabase = 102;
        const int CreatedPerDatabase = 2;

        var ten = PgCappedRead.Classify(
            rowsAhead: 10 * (PerDatabase - CreatedPerDatabase),
            wantedRows: 10 * CreatedPerDatabase,
            returnedRows: McpHelpers.MaxTop,
            limit: McpHelpers.MaxTop, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Grouped, remedy: Remedy);

        Assert.Equal(PgCappedReach.Unreachable, ten.Reach);
        Assert.Equal(1_000L, ten.RowsAhead);
        Assert.Equal(0L, ten.ReachableRows);
        Assert.Equal(20L, ten.WithheldRows);

        /* THE CONTROL, and it is the reason this case is not a coincidence: one database fewer and the whole
           product fits, so the same arithmetic answers Complete. A pin that only asserted the ten-database
           verdict would pass against a classifier that answered Unreachable for every input. */
        var nine = PgCappedRead.Classify(
            rowsAhead: 9 * (PerDatabase - CreatedPerDatabase),
            wantedRows: 9 * CreatedPerDatabase,
            returnedRows: 9 * PerDatabase,
            limit: McpHelpers.MaxTop, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Grouped, remedy: Remedy);

        Assert.Equal(PgCappedReach.Complete, nine.Reach);
        Assert.True(nine.IsComplete);
        Assert.Equal(18L, nine.ReachableRows);
        Assert.Equal(0L, nine.WithheldRows);
    }

    /// <summary>
    /// The boundary, one over it, and one under it - the three inputs a cap guard is written wrong at.
    ///
    /// <para>The leading block is compared against the CEILING with <c>&gt;=</c>, so a block of exactly
    /// <see cref="McpHelpers.MaxTop"/> rows is already unreachable: it fills the largest response the
    /// surface will produce and leaves nothing behind it. One row fewer leaves room for exactly one wanted
    /// row, which is <see cref="PgCappedReach.Partial"/> and not
    /// <see cref="PgCappedReach.Unreachable"/>.</para>
    /// </summary>
    [Theory]
    [InlineData(999, PgOrderSemantics.Ranked, PgCappedReach.Partial, 1)]
    [InlineData(1000, PgOrderSemantics.Ranked, PgCappedReach.Unreachable, 0)]
    [InlineData(1001, PgOrderSemantics.Ranked, PgCappedReach.Unreachable, 0)]
    [InlineData(999, PgOrderSemantics.Grouped, PgCappedReach.Partial, 1)]
    [InlineData(1000, PgOrderSemantics.Grouped, PgCappedReach.Unreachable, 0)]
    [InlineData(1001, PgOrderSemantics.Grouped, PgCappedReach.Unreachable, 0)]
    public void TheCeilingBoundary_IsInclusive(
        long rowsAhead, PgOrderSemantics order, PgCappedReach expected, long reachableAtMax)
    {
        var verdict = PgCappedRead.Classify(
            rowsAhead: rowsAhead, wantedRows: 50, returnedRows: McpHelpers.MaxTop,
            limit: McpHelpers.MaxTop, maxLimit: McpHelpers.MaxTop, order: order, remedy: Remedy);

        Assert.Equal(expected, verdict.Reach);
        Assert.Equal(reachableAtMax, verdict.ReachableAtMaxRows);
    }

    /// <summary>
    /// A response sitting EXACTLY on its limit is not complete, even when the population figures say every
    /// wanted row fits.
    ///
    /// <para>The pessimistic comparison, and the one case where the population arithmetic is not enough: at
    /// <c>returnedRows == limit</c> the read cannot tell "that was all of them" from "there was one more",
    /// and every other read in this repo reports <c>truncated</c> on exactly that test. One row fewer
    /// returned and the same figures answer <see cref="PgCappedReach.Complete"/>.</para>
    /// </summary>
    [Fact]
    public void AResponseSittingOnItsLimit_IsNotClaimedComplete()
    {
        var onTheLimit = PgCappedRead.Classify(
            rowsAhead: 0, wantedRows: 25, returnedRows: 25, limit: 25, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.RankedTail, onTheLimit.Reach);
        Assert.False(onTheLimit.IsComplete);

        /* AND THE SAME FIGURES OVER A GROUPING are not complete either, and are not a ranked tail: the
           limit cut a partition, so the reader holds some groups and not others. Carried here because this
           is the one boundary where the two orders part company - every other case in this class agrees. */
        var groupedOnTheLimit = PgCappedRead.Classify(
            rowsAhead: 0, wantedRows: 25, returnedRows: 25, limit: 25, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Grouped, remedy: Remedy);

        Assert.Equal(PgCappedReach.Partial, groupedOnTheLimit.Reach);
        Assert.False(groupedOnTheLimit.IsComplete);

        var shortOfIt = PgCappedRead.Classify(
            rowsAhead: 0, wantedRows: 24, returnedRows: 24, limit: 25, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.Complete, shortOfIt.Reach);
        Assert.True(shortOfIt.IsComplete);
    }

    /// <summary>
    /// ZERO wanted rows behind a leading block larger than the ceiling is still
    /// <see cref="PgCappedReach.Unreachable"/>, and ALL wanted rows with nothing ahead of them and room to
    /// spare is <see cref="PgCappedReach.Complete"/>. The two ends of the population axis.
    ///
    /// <para>Zero wanted is the state of a server whose monitoring login cannot read <c>pg_stats</c> -
    /// #3278's <c>NothingTrusted</c>, measured as the fleet's commonest arm. Nothing can be reached because
    /// there is nothing there, and the arm has to keep saying the rows ahead are what fills the response:
    /// answering <see cref="PgCappedReach.Complete"/> would be arithmetically defensible and would read as
    /// "this response holds every answer this server has", which is the sentence both issues are about.</para>
    /// </summary>
    [Fact]
    public void BothEndsOfThePopulationAxis_KeepTheirArm()
    {
        var nothingWanted = PgCappedRead.Classify(
            rowsAhead: 6_680, wantedRows: 0, returnedRows: McpHelpers.MaxTop,
            limit: McpHelpers.MaxTop, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.Unreachable, nothingWanted.Reach);
        Assert.False(
            nothingWanted.IsComplete,
            "a server with no trusted answer at all reported its response COMPLETE, which reads as "
            + "\"every answer is here\" on the arm where there are none");
        Assert.Equal(0L, nothingWanted.WithheldRows);

        var everythingWanted = PgCappedRead.Classify(
            rowsAhead: 0, wantedRows: 2_505, returnedRows: 2_505,
            limit: 4_000, maxLimit: 4_000, order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.Complete, everythingWanted.Reach);
        Assert.Equal(2_505L, everythingWanted.ReachableRows);
        Assert.Equal(0L, everythingWanted.WithheldRows);
        Assert.False(everythingWanted.ARaisedLimitWouldHelp);
    }

    /// <summary>
    /// Every arm's cause is distinct from every other arm's, and none of them is empty.
    ///
    /// <para>The discrimination guard. This classifier is worth having only because its arms carry different
    /// instructions - raise the limit, do not bother raising the limit, this is a top-N, this is everything -
    /// so two arms sharing a sentence is the failure that makes it decorative. Compared on CAUSE and not on
    /// MESSAGE: the message embeds the figures, which differ in every case, so a message comparison would
    /// pass over arms that had become indistinguishable.</para>
    /// </summary>
    [Fact]
    public void EveryArmsCauseIsDistinctFromEveryOthers()
    {
        var byArm = new Dictionary<PgCappedReach, string>();

        foreach (var verdict in EveryArm())
        {
            Assert.False(
                string.IsNullOrWhiteSpace(verdict.Cause),
                verdict.Reach + " returned an empty cause, so the arm carries no instruction");

            if (byArm.TryGetValue(verdict.Reach, out var seen))
            {
                Assert.Equal(seen, verdict.Cause);
                continue;
            }

            byArm[verdict.Reach] = verdict.Cause;
        }

        Assert.Equal(
            Enum.GetValues<PgCappedReach>().Length,
            byArm.Count);

        Assert.Equal(byArm.Count, byArm.Values.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// The two arms that need the caller to DO something carry the remedy; the two that do not, do not.
    ///
    /// <para>A remedy printed on <see cref="PgCappedReach.Complete"/> would tell a reader holding the whole
    /// population to go and filter it, which is how a helpful sentence becomes noise that gets ignored on
    /// the arm that needed it.</para>
    /// </summary>
    [Fact]
    public void TheRemedyAppearsOnExactlyTheArmsThatNeedOne()
    {
        var carried = 0;
        var withheld = 0;

        foreach (var verdict in EveryArm())
        {
            var needsOne = verdict.Reach is PgCappedReach.Partial or PgCappedReach.Unreachable;

            Assert.Equal(needsOne, verdict.Cause.Contains(Remedy, StringComparison.Ordinal));

            if (needsOne) { carried++; } else { withheld++; }
        }

        /* BOTH BRANCHES EXERCISED, because a foreach over an empty sequence asserts nothing and the loop
           above is the entire test. Measured: crippling EveryArm() to yield nothing left this green while
           it was the only assertion, so the tally is not decoration - it is what makes the pin a
           measurement. Counted rather than pinned to a literal, so adding an arm does not make this stale
           while still refusing a vacuous run. */
        Assert.True(carried > 0, "no arm needing a remedy was exercised, so the positive half asserts nothing");
        Assert.True(withheld > 0, "no arm without a remedy was exercised, so the negative half asserts nothing");
        Assert.Equal(EveryArm().Count(), carried + withheld);
    }

    /// <summary>
    /// Nonsense inputs fail toward the worse label rather than toward "you can see it all".
    ///
    /// <para>Negative counts are reachable: these figures come out of a store, and the subtraction behind
    /// <c>ReachableRows</c> would hand back MORE rows than the limit allows on a negative leading block -
    /// arithmetic failing in the one direction this class exists to refuse. Clamped rather than thrown on,
    /// following the coverage classifiers: this runs to describe a result the caller already has.</para>
    /// </summary>
    [Theory]
    [InlineData(-5_000L, 100L)]
    [InlineData(0L, -100L)]
    [InlineData(-1L, -1L)]
    public void NegativeFiguresCannotManufactureReach(long rowsAhead, long wantedRows)
    {
        var verdict = PgCappedRead.Classify(
            rowsAhead: rowsAhead, wantedRows: wantedRows, returnedRows: 10, limit: 10,
            maxLimit: McpHelpers.MaxTop, order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.True(verdict.RowsAhead >= 0);
        Assert.True(verdict.WantedRows >= 0);
        Assert.InRange(verdict.ReachableRows, 0, Math.Max(0, wantedRows));
        Assert.True(
            verdict.ReachableRows <= verdict.WantedRows,
            "a negative figure let the classifier report more reachable rows than the population has");
    }

    /// <summary>
    /// A zero maximum limit - the degenerate surface - is unreachable rather than complete.
    /// </summary>
    [Fact]
    public void AZeroCeilingReachesNothing()
    {
        var verdict = PgCappedRead.Classify(
            rowsAhead: 1, wantedRows: 1, returnedRows: 0, limit: 0, maxLimit: 0,
            order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.Unreachable, verdict.Reach);
        Assert.False(verdict.IsComplete);
    }

    /// <summary>
    /// The #3435 case: a GROUPED ordering with nothing ahead of the wanted population is
    /// <see cref="PgCappedReach.Partial"/>, and the identical figures over a RANKING are
    /// <see cref="PgCappedReach.RankedTail"/>.
    ///
    /// <para><b>The figures are the same in both halves, which is the point.</b> Everything a count-only
    /// classifier can see is held constant and only the declared meaning of the order changes, so this fails
    /// if the order is ignored AND fails if it is consulted the wrong way round. A classifier deciding from
    /// counts alone answers <see cref="PgCappedReach.RankedTail"/> on both, and that arm's sentence tells
    /// the caller the rows it lost rank below the rows it got - which over a grouping is not a weaker claim,
    /// it is a false one: the rows lost are a different group.</para>
    ///
    /// <para><b>And it is not an assertion about today's fleet.</b> "A grouped surface only reaches this
    /// shape when <c>rowsAhead</c> is zero, which the fleet does not produce" is a claim about a census, of
    /// the kind #3423 retired: <c>EXISTS(... collection_log ...)</c> was a correct proxy for "the collector
    /// runs here" right up to the moment the rows feeding it stopped being written. The figures below are
    /// simply handed to the classifier, so the arm is a property of the contract and not of a census.</para>
    /// </summary>
    [Fact]
    public void AGroupedOrderingWithNothingAhead_IsPartialWhereARankingIsARankedTail()
    {
        var grouped = PgCappedRead.Classify(
            rowsAhead: 0, wantedRows: 510, returnedRows: 50, limit: 50, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Grouped, remedy: Remedy);

        Assert.Equal(PgCappedReach.Partial, grouped.Reach);
        Assert.Equal(PgOrderSemantics.Grouped, grouped.Order);
        Assert.False(grouped.IsComplete);
        Assert.Contains(Remedy, grouped.Cause, StringComparison.Ordinal);

        var ranked = PgCappedRead.Classify(
            rowsAhead: 0, wantedRows: 510, returnedRows: 50, limit: 50, maxLimit: McpHelpers.MaxTop,
            order: PgOrderSemantics.Ranked, remedy: Remedy);

        Assert.Equal(PgCappedReach.RankedTail, ranked.Reach);
        Assert.Equal(PgOrderSemantics.Ranked, ranked.Order);

        /* THE FIGURES ARE IDENTICAL, asserted rather than assumed: if the two halves differed in anything
           but the order, the arms could be differing for that reason instead. */
        Assert.Equal(ranked.RowsAhead, grouped.RowsAhead);
        Assert.Equal(ranked.WantedRows, grouped.WantedRows);
        Assert.Equal(ranked.ReachableRows, grouped.ReachableRows);
        Assert.Equal(ranked.ReachableAtMaxRows, grouped.ReachableAtMaxRows);
        Assert.Equal(ranked.WithheldRows, grouped.WithheldRows);
        Assert.Equal(ranked.Limit, grouped.Limit);
        Assert.Equal(ranked.MaxLimit, grouped.MaxLimit);

        /* AND ONLY THE RANKED HALF CLAIMS A RANKING. The grouped cause must not carry the claim in any
           casing, because the defect was a caller reading that sentence and believing it. */
        Assert.Contains("RANKING", ranked.Cause, StringComparison.Ordinal);
        Assert.DoesNotContain("ranks below", grouped.Cause, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GROUPING", grouped.OrderNote, StringComparison.Ordinal);

        /* AND THE PARTIAL CAUSE DOES NOT ASSERT DISPLACEMENT AS A FACT, because this route has nothing
           ahead of the population at all: RowsAhead is zero and the limit did the cutting. The arm carries
           ONE sentence - which is what keeps the arms comparable - so that sentence has to be true on both
           routes into it, and a sentence naming displacement outright is not. Pinned as that phrasing's
           absence, since it is the natural way to word this arm and the way that makes it false here. */
        Assert.Equal(0L, grouped.RowsAhead);
        Assert.DoesNotContain("displaced part of it", grouped.Cause, StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="PgCappedReach.RankedTail"/> beside <see cref="PgOrderSemantics.Grouped"/> is not merely
    /// unreached, it is UNREPRESENTABLE - there is no verdict in which those two values sit together.
    ///
    /// <para><b>Why this is the acceptance test and a call-site scan is not.</b> "No caller does it" is a
    /// statement about today's callers, and a type that can represent a wrong answer eventually does. The
    /// claim here is about the type: <see cref="PgCappedReachVerdict.Reach"/> and
    /// <see cref="PgCappedReachVerdict.Cause"/> are DERIVED from the figures and the declared order rather
    /// than stored beside them, so there is no constructor parameter to pass the wrong arm to, no setter for
    /// an object initializer or a <c>with</c> expression to reach, and no backing field for reflection or
    /// <c>Unsafe</c> to overwrite. The reflective half below is what makes that a measurement instead of a
    /// reading of the source.</para>
    ///
    /// <para><b>The sweep is over the classifier's input space, not over the call sites.</b> Every
    /// combination of order, leading block, population, returned count, limit and ceiling from the grid
    /// below, including the boundaries, and its own non-vacuity is asserted: the grid has to actually produce
    /// the arm, and has to actually produce grouped verdicts with nothing ahead of them, or "no grouped
    /// RankedTail" would hold over a grid that generated neither.</para>
    /// </summary>
    [Fact]
    public void TheRankedTailArmIsUnrepresentableOverAGroupedOrdering()
    {
        var verdict = typeof(PgCappedReachVerdict);

        /* NO ARM TO HAND IN. A constructor parameter of the arm's own type is how the wrong pairing would be
           expressed, and there is none - on any constructor, public or not. */
        foreach (var constructor in verdict.GetConstructors(
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            Assert.DoesNotContain(typeof(PgCappedReach), constructor.GetParameters().Select(p => p.ParameterType));
            Assert.DoesNotContain(
                "cause",
                constructor.GetParameters().Select(p => p.Name ?? string.Empty),
                StringComparer.OrdinalIgnoreCase);
        }

        /* NO SETTER, so neither an object initializer nor a `with` expression reaches either member. A
           record's positional members get an `init` accessor, so a stored Reach WOULD be assignable this
           way and this assertion is what catches it coming back. */
        Assert.Null(verdict.GetProperty(nameof(PgCappedReachVerdict.Reach))!.SetMethod);
        Assert.Null(verdict.GetProperty(nameof(PgCappedReachVerdict.Cause))!.SetMethod);

        /* AND NO BACKING FIELD, which is what puts this beyond the reach of reflection rather than merely
           beyond the reach of C# syntax. */
        Assert.DoesNotContain(
            typeof(PgCappedReach),
            verdict.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Select(f => f.FieldType));

        /* DEFAULT IS THE WORST ARM, not the best. All-zero figures describe a surface that returns nothing,
           and the zero-valued order is Grouped, so an uninitialised verdict cannot claim a ranking either. */
        Assert.Equal(PgCappedReach.Unreachable, default(PgCappedReachVerdict).Reach);
        Assert.Equal(PgOrderSemantics.Grouped, default(PgCappedReachVerdict).Order);

        var rankedTails = 0;
        var groupedWithNothingAhead = 0;
        var comparedWithSomethingAhead = 0;
        var divergences = 0;

        foreach (var ahead in new long[] { 0, 1, 2, 25, 999, 1_000, 1_001 })
        foreach (var wanted in new long[] { 0, 1, 25, 726 })
        foreach (var returned in new[] { 0, 1, 25, 1_000 })
        foreach (var limit in new[] { 0, 1, 25, 1_000 })
        foreach (var ceiling in new[] { 0, 1, 25, 1_000 })
        {
            var asRanking = PgCappedRead.Classify(
                ahead, wanted, returned, limit, ceiling, PgOrderSemantics.Ranked, Remedy);
            var asGrouping = PgCappedRead.Classify(
                ahead, wanted, returned, limit, ceiling, PgOrderSemantics.Grouped, Remedy);

            Assert.NotEqual(PgCappedReach.RankedTail, asGrouping.Reach);

            if (asRanking.Reach == PgCappedReach.RankedTail) { rankedTails++; }
            if (ahead == 0) { groupedWithNothingAhead++; }

            /* THE BLAST RADIUS, measured: with something ahead of the population the arm does not consult
               the order at all, so the two must agree exactly - which is the statement that #3424's
               index-bloat surface is behaviourally untouched everywhere but the leading-block-empty route
               it deliberately takes under answered_only. */
            if (ahead > 0)
            {
                comparedWithSomethingAhead++;
                Assert.Equal(asRanking.Reach, asGrouping.Reach);
                Assert.Equal(asRanking.Cause, asGrouping.Cause);
            }
            else if (asRanking.Reach != asGrouping.Reach)
            {
                divergences++;
            }
        }

        /* NON-VACUITY, because "no grouped RankedTail" is satisfied by a grid that produces no RankedTail at
           all and by a grid that produces no grouped verdicts. Both halves are counted. */
        Assert.True(rankedTails > 0, "the sweep produced no RankedTail at all, so its central claim is vacuous");
        Assert.True(
            groupedWithNothingAhead > 0,
            "the sweep produced no grouped verdict with an empty leading block, which is the only shape the "
            + "two orders can disagree on");
        Assert.True(comparedWithSomethingAhead > 0, "the parity half of the sweep compared nothing");
        Assert.True(
            divergences > 0,
            "no combination of figures made the two orders answer differently, so this sweep would pass "
            + "against a classifier that ignored the order entirely");
    }

    /// <summary>
    /// The order's meaning reaches the reader on EVERY arm, and the two orders do not share a sentence.
    ///
    /// <para>Carried on <see cref="PgCappedReachVerdict.Census"/> rather than on
    /// <see cref="PgCappedReachVerdict.Cause"/> so the arms stay comparable on the cause alone - which is
    /// what <see cref="EveryArmsCauseIsDistinctFromEveryOthers"/> measures - while a human reading
    /// <c>message</c> still learns whether the page is the top of a ranking or a slice of a partition.</para>
    /// </summary>
    [Fact]
    public void TheOrderNoteReachesEveryArmsMessageAndDiscriminatesTheTwoOrders()
    {
        var notes = new HashSet<string>(StringComparer.Ordinal);
        var arms = new HashSet<PgCappedReach>();

        foreach (var verdict in EveryArm())
        {
            Assert.False(string.IsNullOrWhiteSpace(verdict.OrderNote));
            Assert.Contains(verdict.OrderNote, verdict.Census, StringComparison.Ordinal);
            Assert.Contains(verdict.OrderNote, verdict.Message, StringComparison.Ordinal);

            notes.Add(verdict.OrderNote);
            arms.Add(verdict.Reach);
        }

        /* ONE NOTE PER ORDER and both of them exercised, so a single shared sentence - the shape that would
           make this decoration - fails here. */
        Assert.Equal(Enum.GetValues<PgOrderSemantics>().Length, notes.Count);
        Assert.Equal(Enum.GetValues<PgCappedReach>().Length, arms.Count);
    }

    /// <summary>
    /// One verdict per arm, built from inputs that differ in the arm and not only in the figures - and over
    /// BOTH orderings, so a cause that only held for one of them is a failure here rather than a surprise on
    /// whichever surface was not thought about (#3435).
    /// </summary>
    private static IEnumerable<PgCappedReachVerdict> EveryArm()
    {
        const PgOrderSemantics Ranked = PgOrderSemantics.Ranked;
        const PgOrderSemantics Grouped = PgOrderSemantics.Grouped;

        /* Complete, reachable from either order. */
        yield return PgCappedRead.Classify(0, 5, 5, 25, McpHelpers.MaxTop, Ranked, Remedy);
        yield return PgCappedRead.Classify(10, 5, 15, 25, McpHelpers.MaxTop, Ranked, Remedy);
        yield return PgCappedRead.Classify(0, 5, 5, 25, McpHelpers.MaxTop, Grouped, Remedy);
        yield return PgCappedRead.Classify(10, 5, 15, 25, McpHelpers.MaxTop, Grouped, Remedy);

        /* RankedTail, reachable from ONE order - which is the whole of #3435. */
        yield return PgCappedRead.Classify(0, 900, 25, 25, McpHelpers.MaxTop, Ranked, Remedy);
        yield return PgCappedRead.Classify(0, 25, 25, 25, McpHelpers.MaxTop, Ranked, Remedy);

        /* Partial, from both orders AND from both of its routes: displaced by a leading block, and cut by
           the limit over a grouping with nothing ahead of it at all. The second is where a grouped read's
           RankedTail used to come from. */
        yield return PgCappedRead.Classify(40, 900, 25, 25, McpHelpers.MaxTop, Ranked, Remedy);
        yield return PgCappedRead.Classify(999, 50, 1_000, McpHelpers.MaxTop, McpHelpers.MaxTop, Ranked, Remedy);
        yield return PgCappedRead.Classify(40, 900, 25, 25, McpHelpers.MaxTop, Grouped, Remedy);
        yield return PgCappedRead.Classify(0, 900, 25, 25, McpHelpers.MaxTop, Grouped, Remedy);
        yield return PgCappedRead.Classify(0, 25, 25, 25, McpHelpers.MaxTop, Grouped, Remedy);

        /* Unreachable, reachable from either order. */
        yield return PgCappedRead.Classify(1_779, 726, 1_000, McpHelpers.MaxTop, McpHelpers.MaxTop, Ranked, Remedy);
        yield return PgCappedRead.Classify(1_000, 20, 1_000, McpHelpers.MaxTop, McpHelpers.MaxTop, Ranked, Remedy);
        yield return PgCappedRead.Classify(1_779, 726, 1_000, McpHelpers.MaxTop, McpHelpers.MaxTop, Grouped, Remedy);
        yield return PgCappedRead.Classify(1_000, 20, 1_000, McpHelpers.MaxTop, McpHelpers.MaxTop, Grouped, Remedy);
    }
}
