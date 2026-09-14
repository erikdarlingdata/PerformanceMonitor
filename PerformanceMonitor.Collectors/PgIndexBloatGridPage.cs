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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// One graphical grid of <c>pg_index_bloat</c> rows, composed so the answered indexes are REACHED without
/// the answerless ones losing the lead (#3434).
///
/// <para><b>The defect this closes.</b> A grid that fetches one capped page in the read's own order shows
/// 100% answerless rows whenever that population outnumbers the cap — structurally, not by chance, because
/// answerless rows sort FIRST by design (#3278: the index nobody can model is the likeliest big win and must
/// not rank below measured ones). #3431 gave the read an <c>answered_only</c> filter, so the answers now have
/// a route; a grid still asking for one page in the default order does not take it. Measured on the fleet at
/// 2026-09-14: the largest census is 2,505 candidate indexes, of which 1,779 are answerless and 726 carry a
/// trusted answer covering 213.8 GB, and a second target holds 268 answerless ahead of 79 answers covering
/// 191.0 GB. Both leading blocks exceed a 200-row grid, so both grids show no answer at all.</para>
///
/// <para><b>Reach, not order.</b> The composed grid keeps the read's order — answerless rows first, then the
/// answered ranking by reclaimable bytes — and truncates the LEADING block rather than the trailing one. A
/// fraction of the grid is reserved for the answered ranking, so what changes is which block the row cap is
/// spent on and never which block leads. Changing the sort would hide the gaps instead, which is #3278's
/// defect pointed the other way.</para>
///
/// <para><b>Disjoint by PREDICATE, not by key comparison.</b> The leading page contributes only the rows
/// <c>hasNoAnswer</c> accepts and the answered page only the rows it rejects, so no index can appear twice
/// however the two reads overlap — and a caller that forgot to pass <c>answered_only</c> loses rows from the
/// trailing block rather than duplicating them into it. Nothing here compares an index identity, which is
/// what keeps this pure.</para>
///
/// <para><b>Pure policy: no clock, no I/O, no knowledge of the row.</b> The caller supplies the two pages, a
/// predicate that says which rows have no answer, a projection for the figure the grid ranks by, and its own
/// row cap; everything else is arithmetic over counts. That is what lets the composition and the sentence
/// describing it be asserted together without a store or a UI thread — which matters here more than usual,
/// because the surface this ships for is WPF and cannot be rendered on the hosts this work happens on.</para>
/// </summary>
/// <param name="Rows">What the grid shows, in order: the answerless block, then the answered ranking.</param>
/// <param name="AnswerlessShown">How many rows of <paramref name="Rows"/> have no answer. They are its
/// leading <c>AnswerlessShown</c> entries.</param>
/// <param name="AnsweredShown">How many rows of <paramref name="Rows"/> carry an answer. They are its
/// trailing <c>AnsweredShown</c> entries.</param>
/// <param name="AnswerlessNotShown">Answerless rows THE READS RETURNED that the grid does not show. A page
/// figure and never a population one: both reads are themselves capped, so this is a floor on what is
/// behind the grid and the coverage census is what states the server's population.</param>
/// <param name="AnsweredNotShown">Answered rows THE READS RETURNED that the grid does not show, on the same
/// terms.</param>
/// <param name="ReclaimableBytesShown">The ranked figure summed over <paramref name="Rows"/> — carried here
/// rather than left to the caller so the grid and the sentence describing it cannot be summed over two
/// different row sets.</param>
/// <param name="GridLimit">The caller's row cap, as applied.</param>
public readonly record struct PgIndexBloatGridPage<TRow>(
    IReadOnlyList<TRow> Rows,
    int AnswerlessShown,
    int AnsweredShown,
    int AnswerlessNotShown,
    int AnsweredNotShown,
    long ReclaimableBytesShown,
    int GridLimit)
{
    /// <summary>
    /// Whether the grid reaches an answered index at all. The one property this composition exists to make
    /// true, exposed so a consumer branches on it rather than re-deriving it from the counts.
    /// </summary>
    public bool ReachesAnAnswer => AnsweredShown > 0;

    /// <summary>
    /// Whether the answered ranking cost the answerless block any of its rows — true only when the
    /// answerless population would otherwise have filled the grid on its own.
    /// </summary>
    public bool TheLeadWasTruncatedForTheAnswers => AnswerlessNotShown > 0 && AnsweredShown > 0;

    /// <summary>
    /// What THIS GRID is, in figures, for the panel note to print beside the coverage census.
    ///
    /// <para>Every number here is a count of <see cref="Rows"/> and of nothing else, which is the whole
    /// point of composing the sentence in the same value as the rows: a note describing a different
    /// population than the grid beside it is the defect #3434 is about, and a count computed at the call
    /// site is how it comes back one layer in.</para>
    ///
    /// <para>It says the counts are the GRID's and points at the census for the server's, because the two
    /// reads behind this are both capped — so nothing here can honestly be read as a population.</para>
    /// </summary>
    public string Composition =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"THIS GRID holds {AnswerlessShown:N0} index(es) with NO answer, listed first, and "
            + $"{AnsweredShown:N0} answered index(es) behind them ranked by reclaimable bytes. Those are "
            + $"counts of the grid and not of the server - the census above is the population, and this "
            + $"grid is two capped reads of it.")
        + (TheLeadWasTruncatedForTheAnswers
            ? string.Create(
                CultureInfo.InvariantCulture,
                $"  The answerless block is CUT at {AnswerlessShown:N0} of the "
                + $"{AnswerlessShown + AnswerlessNotShown:N0} this read returned, because "
                + $"{PgIndexBloatGridBudget.AnsweredReserveFor(GridLimit):N0} of the {GridLimit:N0} row(s) "
                + $"are reserved for the answered ranking. Without that reservation the answerless rows "
                + $"fill the grid at their own population size - they sort first by design - and no "
                + $"answer appears at any row cap.")
            : string.Empty)
        + (AnsweredNotShown > 0
            ? string.Create(
                CultureInfo.InvariantCulture,
                $"  {AnsweredNotShown:N0} further answered index(es) this read returned rank below the "
                + $"ones shown and are not in the grid.")
            : string.Empty);
}

/// <summary>
/// Composes a <c>pg_index_bloat</c> grid from the read's default page and its <c>answered_only</c> page
/// (#3434). See <see cref="PgIndexBloatGridPage{TRow}"/> for the defect, the measurements, and why the
/// leading block is what gets truncated.
/// </summary>
public static class PgIndexBloatGridBudget
{
    /// <summary>
    /// The share of a grid reserved for the answered ranking, as a divisor: one quarter.
    ///
    /// <para><b>Why a share and not a count.</b> The two surfaces that render this read cap at different
    /// sizes — 200 rows in the desktop viewer, 25 on the web dashboard — so a fixed reserve is either most
    /// of the smaller grid or a rounding error in the larger one. A share is the same promise at both.</para>
    ///
    /// <para><b>Why a quarter.</b> The answerless block keeps three quarters of the grid, so the gaps in a
    /// census still lead by a wide margin — which is what #3278 is for — while the answers get a page large
    /// enough to be a ranking rather than a token. On the fleet's largest census that is 150 answerless rows
    /// ahead of the top 50 of 726 answers.</para>
    /// </summary>
    public const int AnsweredReserveDivisor = 4;

    /// <summary>
    /// How many rows of a grid of <paramref name="gridLimit"/> are reserved for the answered ranking.
    ///
    /// <para>FLOORED AT ONE for any non-empty grid, and that floor is the guarantee rather than a rounding
    /// convenience: it is what makes "an answered index is reachable" a property of the arithmetic at every
    /// limit instead of a consequence of 200 being a large number. Without it a grid of three rows reserves
    /// nothing and reproduces the defect at a smaller cap.</para>
    ///
    /// <para>At a ONE-row grid the floor is the whole grid, so that single row is the top answer rather
    /// than a gap — the one cap where the two populations cannot both be represented and the tie has to go
    /// somewhere. Said here rather than left to be discovered: the shipped caps are 200 and 25, two orders
    /// of magnitude away, and no surface renders a grid that small.</para>
    /// </summary>
    public static int AnsweredReserveFor(int gridLimit) =>
        gridLimit <= 0 ? 0 : Math.Max(1, gridLimit / AnsweredReserveDivisor);

    /// <summary>
    /// The grid, its figures and the sentence describing them, as ONE value — so no surface can render the
    /// rows without the figures or the figures without the rows.
    /// </summary>
    /// <param name="leadingPage">The read's DEFAULT page, in #3278's answerless-first order. Only the rows
    /// with no answer are taken from it: its answered tail is a prefix of
    /// <paramref name="answeredPage"/>'s ranking over the same population, so taking it as well would be
    /// the same rows twice.</param>
    /// <param name="answeredPage">The same read under <c>answered_only</c> — the answered population ranked
    /// by reclaimable bytes, with nothing ahead of it.</param>
    /// <param name="hasNoAnswer">Whether a row carries a reason instead of an answer. The ONLY trust
    /// predicate on this collector is the absence of a stored reason; a caller keying on one of the
    /// <c>est_</c> intermediates would accept every row, because they are populated on 100% of the
    /// answerless ones (#3278).</param>
    /// <param name="reclaimableBytes">The figure the grid is ranked by, per row, for the total the note
    /// prints. Zero for a row that has none.</param>
    /// <param name="gridLimit">The grid's row cap. Bounded because a WPF DataGrid asked to realise a
    /// fleet-sized census is a hung UI thread rather than a slow one.</param>
    public static PgIndexBloatGridPage<TRow> Compose<TRow>(
        IReadOnlyList<TRow> leadingPage,
        IReadOnlyList<TRow> answeredPage,
        Func<TRow, bool> hasNoAnswer,
        Func<TRow, long> reclaimableBytes,
        int gridLimit)
    {
        ArgumentNullException.ThrowIfNull(leadingPage);
        ArgumentNullException.ThrowIfNull(answeredPage);
        ArgumentNullException.ThrowIfNull(hasNoAnswer);
        ArgumentNullException.ThrowIfNull(reclaimableBytes);

        var limit = Math.Max(0, gridLimit);

        /* FILTERED ON BOTH SIDES, so disjointness is a property of this function rather than of the
           parameters it was handed. The leading page keeps only what has no answer and the answered page
           only what has one, so an index cannot appear twice however the two reads overlap - and a caller
           that forgot answered_only loses rows from the trailing block instead of duplicating them. */
        var answerless = new List<TRow>(leadingPage.Count);
        foreach (var row in leadingPage)
        {
            if (hasNoAnswer(row))
            {
                answerless.Add(row);
            }
        }

        var answered = new List<TRow>(answeredPage.Count);
        foreach (var row in answeredPage)
        {
            if (!hasNoAnswer(row))
            {
                answered.Add(row);
            }
        }

        /* THE RESERVE IS SPENT ONLY ON ANSWERS THAT EXIST, so a server with none gives the whole grid to
           the answerless rows and the composition is the unmodified read. And the answered side then takes
           whatever the answerless block did not need, so a small answerless population still shows the
           same long ranking it does today - the reserve is a floor under the answers, never a ceiling. */
        var answerlessShown = Math.Min(answerless.Count, Math.Max(0, limit - Math.Min(answered.Count, AnsweredReserveFor(limit))));
        var answeredShown = Math.Min(answered.Count, Math.Max(0, limit - answerlessShown));

        var rows = new List<TRow>(answerlessShown + answeredShown);
        for (var i = 0; i < answerlessShown; i++)
        {
            rows.Add(answerless[i]);
        }

        for (var i = 0; i < answeredShown; i++)
        {
            rows.Add(answered[i]);
        }

        var bytes = 0L;
        foreach (var row in rows)
        {
            bytes += reclaimableBytes(row);
        }

        return new PgIndexBloatGridPage<TRow>(
            rows,
            answerlessShown,
            answeredShown,
            answerless.Count - answerlessShown,
            answered.Count - answeredShown,
            bytes,
            limit);
    }
}
