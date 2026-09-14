/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What a capped read's ordering MEANS, which decides whether a truncation can be described as the tail of a
/// ranking (#3435).
///
/// <para><b>Why the classifier has to be told.</b> <see cref="PgCappedReach.RankedTail"/> tells a caller the
/// rows it lost rank BELOW the rows it got. That holds when the order is a ranking and is false when the
/// order is a grouping: a cut in a grouping does not leave the top of anything, it leaves some groups and
/// not others. Decided from counts alone the two are indistinguishable, so the surface declares which it
/// has and the arm follows from the declaration.</para>
///
/// <para><b><see cref="Grouped"/> is first, so it is <c>default</c>.</b> Zero-valued and therefore what a
/// <c>default</c> verdict carries, which keeps the ranking claim from being the one a value nobody filled in
/// makes. Same direction as every other comparison here: toward the label that claims less.</para>
/// </summary>
public enum PgOrderSemantics
{
    /// <summary>
    /// The order partitions the rows rather than ranking them: rows sort into groups, and within the read's
    /// order one group is simply before another. <c>get_pg_extensions</c> is this - relevance band, then
    /// state, then database - so a cut removes whole databases rather than the bottom of a ranking, and an
    /// install census taken from the surviving rows reports the extension installed everywhere as installed
    /// nowhere.
    ///
    /// <para>A grouped read cannot reach <see cref="PgCappedReach.RankedTail"/>: there is no tail to be at
    /// the top of, so the classifier answers <see cref="PgCappedReach.Partial"/>, whose claim is only that
    /// some of the population is missing.</para>
    /// </summary>
    Grouped,

    /// <summary>
    /// The order ranks the wanted population by something worth acting on, so the first N rows are the N
    /// that matter most and everything cut off ranks below them. <c>get_pg_index_bloat</c> is this -
    /// reclaimable bytes descending over the answered rows - which is the whole value of
    /// <see cref="PgCappedReach.RankedTail"/> and the only order it may be claimed from.
    /// </summary>
    Ranked,
}

/// <summary>
/// Whether a capped read can show the reader the rows they came for, and if not, whether raising the limit
/// would help (#3424, #3425).
///
/// <para><b>The distinction this exists to make.</b> A read whose ordering deliberately places one
/// population ahead of another has a truncation that is not a random slice: the rows sorting behind the
/// leading block are absent, and when that block is larger than the maximum permitted limit they are absent
/// at EVERY limit. "Truncated" cannot say that. It is the same word for "you saw the top 25 of 726 ranked
/// answers" and for "you saw none of the 726 answers and no limit reaches them", and those two pages are
/// indistinguishable to the reader who most needs to tell them apart.</para>
///
/// <para><b>Measured, on the two surfaces this shipped for.</b> <c>get_pg_index_bloat</c> sorts answerless
/// rows first by design (#3278 - an unmeasured index is the likeliest big win and must not rank below
/// measured ones). On the fleet's largest census that is 1,779 answerless rows ahead of 726 answers covering
/// 213.8 GB against a cap of 1,000, so the answers are Unreachable and raising the limit is not a fix while
/// the leading block is still larger. <c>get_pg_extensions</c> sorts non-relevant rows last:
/// <c>plpgsql</c> is the ONLY non-relevant INSTALLED extension on the fleet, measured as one row per
/// database at positions 506-510 of a complete 510-row five-database response, so on a ten-database host its
/// first row sits past 1,000 and an install census comes back looking complete.</para>
///
/// <para><b>It fails toward the worse label, deliberately.</b> An unknown population is treated as not
/// reached rather than as nothing in the way, and a limit that exactly reaches the wanted population is only
/// <see cref="PgCappedReach.Complete"/> when the read also came back short of its limit - because a response
/// sitting exactly ON the limit cannot distinguish "that was all of them" from "there was one more". A
/// reader told <see cref="PgCappedReach.Unreachable"/> when the truth was <see cref="PgCappedReach.Partial"/>
/// asks for a filter they did not strictly need; a reader told <see cref="PgCappedReach.Complete"/> when it
/// was not publishes a census missing its most important row. Only one of those is recoverable.</para>
///
/// <para><b>Pure policy: no clock, no I/O, and no knowledge of either subject.</b> The caller supplies how
/// many rows its ordering places AHEAD of the wanted population, how big that population is, its own limit,
/// the surface's maximum, and what its order MEANS - so the arm selection is assertable without a store, and
/// one classifier serves both tools so they cannot disagree about what a capped page means. The one thing
/// counts cannot settle is whether an order ranks or groups, and that is a <see cref="PgOrderSemantics"/>
/// the surface declares rather than a caveat the reader is expected to apply (#3435).</para>
/// </summary>
public enum PgCappedReach
{
    /// <summary>
    /// The population the reader wants is entirely inside the response: nothing the ordering placed ahead of
    /// it displaced any of it, and the read came back short of its own limit.
    /// </summary>
    Complete,

    /// <summary>
    /// The response starts at the top of the wanted population and is cut at the limit, so the cut follows
    /// the read's own RANKING: the caller holds the first N of what they asked for and everything missing
    /// ranks below everything present.
    ///
    /// <para>The mildest truncation, and the only one where the page is a smaller answer rather than a
    /// different one: "the top 25 by reclaimable bytes" is the question the ranking exists to answer. Named
    /// all the same, because a reader still needs the denominator to know it is a top-N.</para>
    ///
    /// <para><b>Reachable only from <see cref="PgOrderSemantics.Ranked"/>, and that is the arm's whole
    /// integrity (#3435).</b> The claim "what you lost ranks below what you got" is false over an order that
    /// GROUPS - there the rows lost are a different group, not a lower-ranked one - so this arm is not
    /// selected for a grouped read and is not representable on one: <see cref="PgCappedReachVerdict.Reach"/>
    /// is derived from the verdict's own figures and its declared order rather than stored beside them, so
    /// there is no verdict in which this name sits next to
    /// <see cref="PgOrderSemantics.Grouped"/>. Before #3435 the arm carried a sentence disclaiming the
    /// ranking instead, which left both meanings under one name and relied on the reader noticing.</para>
    /// </summary>
    RankedTail,

    /// <summary>
    /// Part of the wanted population is outside the response: rows the ordering places ahead of it displaced
    /// some, or the caller's limit cut it, and a larger permitted limit would reach more - possibly all - of
    /// what is missing.
    ///
    /// <para>Reachable with ZERO rows actually shown: a limit of 25 behind a leading block of 40 shows none
    /// of the wanted population while the maximum of 1,000 would show all of it. That is a different
    /// situation from <see cref="Unreachable"/> with a different remedy, so the two do not share an arm.</para>
    ///
    /// <para><b>Where a GROUPED read's capped page lands (#3435), and it is the arm with no ranking claim
    /// rather than a softened <see cref="RankedTail"/>.</b> "Some of it is missing and a larger limit reaches
    /// more" is true of a grouped cut; "what is missing ranks lower" is not. Folding the grouped case in here
    /// removes a false claim rather than hiding one, which is why it is not the <c>undelivered</c> shape of
    /// #3430: the arm asserts strictly less, and <see cref="PgCappedReachVerdict.Order"/> carries what the
    /// order actually is for any caller that needs it.</para>
    /// </summary>
    Partial,

    /// <summary>
    /// The rows the ordering places ahead of the wanted population are at least as many as the MAXIMUM
    /// permitted limit, so not one row of that population can appear in any response this surface will
    /// produce. Raising the limit is not a fix and never becomes one; only a filter, a different order, or an
    /// aggregate reaches the data.
    ///
    /// <para>The arm both issues are about, and the one that has to band apart from <see cref="Partial"/> -
    /// the <c>EXTENSION_MISSING</c> precedent (#3240), where a refusal with its own cause and its own remedy
    /// gets its own name rather than being folded into the nearest existing one.</para>
    /// </summary>
    Unreachable,
}

/// <summary>
/// One <see cref="PgCappedReach"/> with every figure it was decided from and the sentence that says so, as
/// ONE value - so no surface can print the arm without its figures or the figures without the arm.
///
/// <para><b>The arm is DERIVED, not stored, and that is what makes a wrong answer inexpressible
/// (#3435).</b> Every member below that a reader acts on - <see cref="Reach"/>, <see cref="Cause"/>,
/// <see cref="ReachableRows"/>, <see cref="ReachableAtMaxRows"/> - is computed from the figures and the
/// declared <see cref="Order"/>. So there is no constructor call, object initializer, <c>with</c>
/// expression, reflective field write or <c>default</c> that produces a verdict naming
/// <see cref="PgCappedReach.RankedTail"/> over <see cref="PgOrderSemantics.Grouped"/>, or naming any arm its
/// own figures contradict: the pairing is not data that could be wrong, it is a reading of data.
/// <c>default</c> lands on <see cref="PgCappedReach.Unreachable"/> - zeroes mean a surface that returns
/// nothing, which is the pessimistic answer and not the flattering one.</para>
/// </summary>
/// <param name="Order">What this read's ordering MEANS. The one input counts cannot supply, declared by the
/// surface, and the gate on <see cref="PgCappedReach.RankedTail"/>.</param>
/// <param name="RowsAhead">How many rows the read's ordering places AHEAD of the wanted population. Zero
/// when the response starts at the top of it.</param>
/// <param name="WantedRows">How big the wanted population is, measured over the SERVER rather than over the
/// page - a page-derived figure would be the very quantity this class exists to stop being trusted.</param>
/// <param name="ReturnedRows">How many rows the read actually returned. Carried rather than consumed and
/// discarded, because it is one of the figures the arm is decided from: it is the only thing that tells a
/// response which stopped short of its limit from one sitting exactly on it.</param>
/// <param name="Limit">The caller's row limit, as supplied.</param>
/// <param name="MaxLimit">The surface's maximum permitted row limit.</param>
/// <param name="Remedy">What this read offers that does reach the data - the filter, order or aggregate to
/// use. Appended to <see cref="Cause"/> on the two arms where the caller has to do something, and ignored on
/// the arms where they do not.</param>
public readonly record struct PgCappedReachVerdict(
    PgOrderSemantics Order,
    long RowsAhead,
    long WantedRows,
    int ReturnedRows,
    int Limit,
    int MaxLimit,
    string Remedy)
{
    /// <summary>
    /// The arm, read off the figures and the declared order rather than carried beside them.
    ///
    /// <para>The branch order is the classification, and each test is the pessimistic one:</para>
    /// <list type="number">
    /// <item><description>SHORT OF THE LIMIT, not merely equal to it. A read that returned exactly its limit
    /// cannot distinguish "that was all of them" from "there was at least one more", so completeness is
    /// claimed only when the read stopped early - which is why <see cref="ReturnedRows"/> is a figure this
    /// verdict keeps. With the population figures agreeing AND the read short of its limit, nothing is behind
    /// the page.</description></item>
    /// <item><description>NOTHING AHEAD AND A RANKING means the cut follows that ranking, so the page is the
    /// top N of the population rather than an arbitrary slice of it. Checked before the two displaced arms
    /// because <see cref="ReachableRows"/> can equal <see cref="WantedRows"/> here while the read still sits
    /// on its limit - which is not completeness and is not displacement either. A GROUPED read fails this
    /// test whatever its figures say and falls through, which is #3435: nothing ahead of a grouping still
    /// leaves the reader holding some groups and not others.</description></item>
    /// <item><description>AT LEAST AS MANY AS THE CEILING, so the first wanted row sits at position
    /// <see cref="RowsAhead"/> + 1 beyond <see cref="MaxLimit"/> and no permitted limit reaches it.
    /// <c>&gt;=</c> rather than <c>&gt;</c>: a leading block of exactly <see cref="MaxLimit"/> rows fills the
    /// largest response this surface will produce, leaving nothing for the population behind
    /// it.</description></item>
    /// </list>
    /// </summary>
    public PgCappedReach Reach =>
        ReachableRows >= WantedRows && ReturnedRows < Limit ? PgCappedReach.Complete
        : RowsAhead == 0 && Order == PgOrderSemantics.Ranked ? PgCappedReach.RankedTail
        : RowsAhead >= MaxLimit ? PgCappedReach.Unreachable
        : PgCappedReach.Partial;

    /// <summary>
    /// How many of <see cref="WantedRows"/> this caller's <see cref="Limit"/> can show. Zero is a legitimate
    /// answer and is NOT the same as <see cref="WantedRows"/> being zero.
    /// </summary>
    public long ReachableRows => ReachableAt(Limit);

    /// <summary>
    /// How many of <see cref="WantedRows"/> the MAXIMUM permitted limit could show. The figure that separates
    /// "raise the limit" from "raising the limit cannot help", which is #3278's own opening sentence and the
    /// reason the last two arms are not one arm.
    /// </summary>
    public long ReachableAtMaxRows => ReachableAt(MaxLimit);

    /// <summary>
    /// What the arm MEANS and what closes the gap. Carries no figures: a guard that the arms stay
    /// distinguishable has to compare the cause, and comparing a composed message would pass on the figures
    /// differing, which they always do.
    ///
    /// <para>ONE sentence per arm, and it says only what that arm asserts on EVERY order it is reachable
    /// from. <see cref="PgCappedReach.RankedTail"/> can therefore claim the ranking outright, because the
    /// arm is unreachable without one; <see cref="PgCappedReach.Partial"/> claims only that some of the
    /// population is missing, because it is reachable from both.</para>
    /// </summary>
    public string Cause =>
        Reach switch
        {
            PgCappedReach.Complete =>
                "COMPLETE: every row of the population you asked for is in this response.",

            PgCappedReach.RankedTail =>
                "RANKED TAIL: this response starts at the top of the population you asked for and stops at "
                + "your row limit, so nothing displaced it - these are the FIRST rows of it in this read's "
                + "own RANKING, and every row you cannot see ranks below every row you can. This arm is "
                + "reachable only from a read that declares its order a ranking, so that is the type's "
                + "guarantee rather than this sentence's. It is still a page and not the population - read "
                + "the figures above before treating it as one.",

            PgCappedReach.Unreachable =>
                "UNREACHABLE at every permitted row limit: the rows this read's ordering places ahead of the "
                + "population you asked for already fill the largest response this surface will return, so "
                + "not one row of that population can appear in it. Raising the limit is not a fix and does "
                + "not become one as the server grows. " + Remedy,

            _ =>
                "PARTIAL: part of the population you asked for is outside this response, either displaced by "
                + "rows this read's ordering places ahead of it or cut off by your row limit. WHICH rows are "
                + "missing follows this read's own order, so read the order beside these figures before "
                + "assuming the ones you have are the ones that matter. A larger limit reaches more of it, "
                + "up to the surface maximum. " + Remedy,
        };

    /// <summary>
    /// What this read's order MEANS, in one sentence, true on every arm - including
    /// <see cref="PgCappedReach.Complete"/>, where nothing was cut and the reader still may want to know how
    /// the rows are arranged.
    ///
    /// <para>Separate from <see cref="Cause"/> so the arms stay comparable on the cause alone, and printed
    /// with the figures rather than with the arm because it is a property of the READ rather than of this
    /// particular response.</para>
    /// </summary>
    public string OrderNote =>
        Order == PgOrderSemantics.Ranked
            ? "This read's order is a RANKING, so a cut follows it and what it withholds ranks below what it "
              + "returns."
            : "This read's order is a GROUPING rather than a ranking, so a cut falls BETWEEN groups: what it "
              + "withholds is other groups and not the lower-ranked remainder of the rows you have.";

    /// <summary>
    /// Whether the response can be read as a complete statement about the wanted population. False on every
    /// arm but <see cref="PgCappedReach.Complete"/> - including <see cref="PgCappedReach.RankedTail"/>, where
    /// the page is a correct top-N and still not the population.
    ///
    /// <para>Exposed so a consumer branches on ONE property rather than re-deriving the arm set, and so a
    /// later arm cannot leave a caller silently treating it as complete: a new arm is incomplete unless
    /// somebody comes here and says otherwise.</para>
    /// </summary>
    public bool IsComplete => Reach == PgCappedReach.Complete;

    /// <summary>
    /// Whether a larger permitted limit would show more of the wanted population than this caller's limit
    /// does. False on <see cref="PgCappedReach.Unreachable"/>, which is the point of that arm, and false on
    /// <see cref="PgCappedReach.Complete"/>, which has nothing left to show.
    /// </summary>
    public bool ARaisedLimitWouldHelp => ReachableAtMaxRows > ReachableRows;

    /// <summary>How many rows of the wanted population this caller cannot see at all.</summary>
    public long WithheldRows => WantedRows - ReachableRows;

    /// <summary>The figures then the cause, for the surfaces that print one string.</summary>
    public string Message => Census + " " + Cause;

    /// <summary>
    /// The figures and what the order means, in one shape for every arm. Separate from <see cref="Cause"/>
    /// so the arms can be asserted distinguishable on the cause alone.
    /// </summary>
    public string Census =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"Rows the ordering places ahead of what you asked for: {RowsAhead:N0}. Rows in that population: "
            + $"{WantedRows:N0}. Of those, reachable at your limit of {Limit:N0}: {ReachableRows:N0}; "
            + $"reachable at the maximum limit of {MaxLimit:N0}: {ReachableAtMaxRows:N0}. {OrderNote}");

    /// <summary>
    /// How many of the wanted rows a given limit can show: the budget left after the leading block, capped by
    /// the population itself and floored at zero.
    ///
    /// <para>One expression serving both the caller's limit and the ceiling. Two copies would have to agree,
    /// and the failure of a disagreement is <see cref="ARaisedLimitWouldHelp"/> answering from two different
    /// arithmetics - a read that half works, for a reason no reader could see.</para>
    ///
    /// <para>Written as a floored minimum rather than a clamp because a clamp THROWS when its bounds cross,
    /// which a negative <see cref="WantedRows"/> would do - and a property getter that throws is a worse
    /// answer than zero on a type whose whole job is to describe a result the caller already holds.
    /// <see cref="PgCappedRead.Classify"/> clamps the figures on the way in, so this is identical to a clamp
    /// on every verdict it produces.</para>
    /// </summary>
    private long ReachableAt(long limit) => Math.Max(0, Math.Min(limit - RowsAhead, WantedRows));
}

/// <summary>
/// Classifies whether a capped read reaches the population its reader came for (#3424, #3425). See
/// <see cref="PgCappedReach"/> for the defect, the measurements, and why it fails toward the worse label, and
/// <see cref="PgOrderSemantics"/> for why the surface has to declare what its ordering means (#3435).
/// </summary>
public static class PgCappedRead
{
    /// <summary>
    /// The arm, its figures, and what closes the gap.
    /// </summary>
    /// <param name="rowsAhead">How many rows this read's ordering places AHEAD of the wanted population, over
    /// the SERVER. Negative is treated as unknown-and-therefore-large rather than as zero - see the class
    /// note on failing toward the worse label.</param>
    /// <param name="wantedRows">How big the wanted population is, over the server.</param>
    /// <param name="returnedRows">How many rows the read actually returned. Used only to tell a response that
    /// stopped short of its limit from one sitting exactly on it, which is the one thing the population
    /// figures cannot settle.</param>
    /// <param name="limit">The caller's row limit.</param>
    /// <param name="maxLimit">The surface's maximum permitted row limit - pass the shared constant, never a
    /// literal, or this classifier decides the difference between "raise the limit" and "raising the limit
    /// cannot help" against a number that is not the one the surface enforces.</param>
    /// <param name="order">Whether this read's ordering RANKS the wanted population or GROUPS it. The gate on
    /// <see cref="PgCappedReach.RankedTail"/>, and the one thing the counts cannot supply: pass what this
    /// read's own ORDER BY does, not what would read better.</param>
    /// <param name="remedy">What this read offers that does reach the data - the filter, order or aggregate
    /// to use. Appended to the cause on the two arms where the caller has to do something, and ignored on
    /// the arms where they do not.</param>
    public static PgCappedReachVerdict Classify(
        long rowsAhead,
        long wantedRows,
        int returnedRows,
        int limit,
        int maxLimit,
        PgOrderSemantics order,
        string remedy) =>
        /* CLAMPED, not trusted. Every one of these is a count read out of a store or handed in by a caller,
           and a negative rowsAhead would make the subtraction behind ReachableRows hand back MORE reachable
           rows than the limit allows - the arithmetic failing toward "you can see it all", which is the
           direction this whole class exists to refuse. A negative wantedRows would do the same via
           WithheldRows. Clamped HERE and only here, so the figures a verdict reports are the figures its arm
           was read from. */
        new PgCappedReachVerdict(
            order,
            Math.Max(0, rowsAhead),
            Math.Max(0, wantedRows),
            returnedRows,
            Math.Max(0, limit),
            Math.Max(0, maxLimit),
            remedy);
}
