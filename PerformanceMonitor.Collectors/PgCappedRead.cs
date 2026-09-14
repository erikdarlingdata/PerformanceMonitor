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
/// many rows its ordering places AHEAD of the wanted population, how big that population is, its own limit
/// and the surface's maximum - so the arm selection is assertable without a store, and one classifier serves
/// both tools so they cannot disagree about what a capped page means.</para>
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
    /// the read's OWN ranking and the caller holds the highest-ranked N of what they asked for.
    ///
    /// <para>The benign truncation, and the only one that is: "the top 25 by reclaimable bytes" is the
    /// question the ranking exists to answer, so a page of it is a smaller answer rather than a different
    /// one. Named all the same, because a reader still needs the denominator to know it is a top-N.</para>
    /// </summary>
    RankedTail,

    /// <summary>
    /// Rows the ordering places ahead of the wanted population displaced part of it, and a larger permitted
    /// limit would reach more - possibly all - of what is missing.
    ///
    /// <para>Reachable with ZERO rows actually shown: a limit of 25 behind a leading block of 40 shows none
    /// of the wanted population while the maximum of 1,000 would show all of it. That is a different
    /// situation from <see cref="Unreachable"/> with a different remedy, so the two do not share an arm.</para>
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
/// </summary>
/// <param name="Reach">The arm.</param>
/// <param name="RowsAhead">How many rows the read's ordering places AHEAD of the wanted population. Zero
/// when the response starts at the top of it.</param>
/// <param name="WantedRows">How big the wanted population is, measured over the SERVER rather than over the
/// page - a page-derived figure would be the very quantity this class exists to stop being trusted.</param>
/// <param name="ReachableRows">How many of <paramref name="WantedRows"/> this caller's
/// <paramref name="Limit"/> can show. Zero is a legitimate answer and is NOT the same as
/// <paramref name="WantedRows"/> being zero.</param>
/// <param name="ReachableAtMaxRows">How many of <paramref name="WantedRows"/> the MAXIMUM permitted limit
/// could show. The figure that separates "raise the limit" from "raising the limit cannot help", which is
/// #3278's own opening sentence and the reason the last two arms are not one arm.</param>
/// <param name="Limit">The caller's row limit, as supplied.</param>
/// <param name="MaxLimit">The surface's maximum permitted row limit.</param>
/// <param name="Cause">What the arm MEANS and what closes the gap. Carries no figures: a guard that the arms
/// stay distinguishable has to compare the cause, and comparing a composed message would pass on the figures
/// differing, which they always do.</param>
public readonly record struct PgCappedReachVerdict(
    PgCappedReach Reach,
    long RowsAhead,
    long WantedRows,
    long ReachableRows,
    long ReachableAtMaxRows,
    int Limit,
    int MaxLimit,
    string Cause)
{
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
    /// The figures, in one shape for every arm. Separate from <see cref="Cause"/> so the arms can be
    /// asserted distinguishable on the cause alone.
    /// </summary>
    public string Census =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"Rows the ordering places ahead of what you asked for: {RowsAhead:N0}. Rows in that population: "
            + $"{WantedRows:N0}. Of those, reachable at your limit of {Limit:N0}: {ReachableRows:N0}; "
            + $"reachable at the maximum limit of {MaxLimit:N0}: {ReachableAtMaxRows:N0}.");
}

/// <summary>
/// Classifies whether a capped read reaches the population its reader came for (#3424, #3425). See
/// <see cref="PgCappedReach"/> for the defect, the measurements, and why it fails toward the worse label.
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
    /// <param name="remedy">What this read offers that does reach the data - the filter, order or aggregate
    /// to use. Appended to the cause on the two arms where the caller has to do something, and ignored on
    /// the arms where they do not.</param>
    public static PgCappedReachVerdict Classify(
        long rowsAhead,
        long wantedRows,
        int returnedRows,
        int limit,
        int maxLimit,
        string remedy)
    {
        /* CLAMPED, not trusted. Every one of these is a count read out of a store or handed in by a caller,
           and a negative rowsAhead would make the subtraction below hand back MORE reachable rows than the
           limit allows - the arithmetic failing toward "you can see it all", which is the direction this
           whole class exists to refuse. A negative wantedRows would do the same via WithheldRows. */
        var ahead = Math.Max(0, rowsAhead);
        var wanted = Math.Max(0, wantedRows);
        var callerLimit = Math.Max(0, limit);
        var ceiling = Math.Max(0, maxLimit);

        var reachable = Reachable(ahead, wanted, callerLimit);
        var reachableAtMax = Reachable(ahead, wanted, ceiling);

        /* SHORT OF THE LIMIT, not merely equal to it. A read that returned exactly `limit` rows cannot
           distinguish "that was all of them" from "there was at least one more", so completeness is claimed
           only when the read stopped early - the pessimistic comparison, and the reason `returnedRows` is a
           parameter at all. With the population figures agreeing AND the read short of its limit, nothing is
           behind the page. */
        if (reachable >= wanted && returnedRows < callerLimit)
        {
            return new PgCappedReachVerdict(
                PgCappedReach.Complete,
                ahead,
                wanted,
                reachable,
                reachableAtMax,
                callerLimit,
                ceiling,
                "COMPLETE: every row of the population you asked for is in this response.");
        }

        /* NOTHING AHEAD means the cut follows this read's own ranking, so the page is the top N of the
           population rather than an arbitrary slice of it. Checked before the two displaced arms because
           `reachable` can equal `wanted` here while the read still sits on its limit - which is not
           completeness and is not displacement either. */
        if (ahead == 0)
        {
            return new PgCappedReachVerdict(
                PgCappedReach.RankedTail,
                ahead,
                wanted,
                reachable,
                reachableAtMax,
                callerLimit,
                ceiling,
                "RANKED TAIL: this response starts at the top of the population you asked for and stops at "
                + "your row limit, so it is the highest-ranked rows of it rather than an arbitrary slice. It "
                + "is still a page and not the population - read the figures above before treating it as "
                + "one.");
        }

        /* AT LEAST AS MANY AS THE CEILING, so the first wanted row sits at position ahead + 1 > maxLimit and
           no permitted limit reaches it. `>=` rather than `>`: a leading block of exactly maxLimit rows fills
           the largest response this surface will produce, leaving nothing for the population behind it. */
        if (ahead >= ceiling)
        {
            return new PgCappedReachVerdict(
                PgCappedReach.Unreachable,
                ahead,
                wanted,
                reachable,
                reachableAtMax,
                callerLimit,
                ceiling,
                "UNREACHABLE at every permitted row limit: the rows this read's ordering places ahead of the "
                + "population you asked for already fill the largest response this surface will return, so "
                + "not one row of that population can appear in it. Raising the limit is not a fix and does "
                + "not become one as the server grows. " + remedy);
        }

        return new PgCappedReachVerdict(
            PgCappedReach.Partial,
            ahead,
            wanted,
            reachable,
            reachableAtMax,
            callerLimit,
            ceiling,
            "PARTIAL: rows this read's ordering places ahead of the population you asked for displaced part "
            + "of it. A larger limit reaches more of it, up to the surface maximum. " + remedy);
    }

    /// <summary>
    /// How many of the wanted rows a given limit can show: the budget left after the leading block, capped by
    /// the population itself and floored at zero.
    ///
    /// <para>One expression serving both the caller's limit and the ceiling. Two copies would have to agree,
    /// and the failure of a disagreement is <c>ARaisedLimitWouldHelp</c> answering from two different
    /// arithmetics - a read that half works, for a reason no reader could see.</para>
    /// </summary>
    private static long Reachable(long rowsAhead, long wantedRows, long limit) =>
        Math.Clamp(limit - rowsAhead, 0, wantedRows);
}
