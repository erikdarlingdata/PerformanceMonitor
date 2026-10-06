/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5313: the bounded fill behind the Top Queries reads. Those statements rank candidate groups, look up each
/// one's latest text and drop the WAITFOR shells, then cap at <c>top</c>. A fixed over-fetch of five covers up
/// to five shells; a window whose top groups hold more of them (a WAITFOR runs long and costs almost no CPU, so
/// a duration ranking puts them high) returned a short page. This asks again with a larger candidate limit while
/// the page is short AND the last round filled its whole candidate limit (so more candidates may exist), up to a
/// hard bound.
/// <para><b>The bound.</b> At most <see cref="MaxRounds"/> rounds, and the candidate limit never passes
/// <c>top + </c><see cref="MaxExtraCandidates"/>. Round one is exactly the old statement (<c>top + 5</c>), so a
/// read that was already full costs what it did; the fill only adds rounds when a page came back short while the
/// candidates had not run out, and the worst case is three runs of the old shape.</para>
/// <para><b>Exhaustion.</b> Each round reports how many candidates pass 1 produced. Fewer than asked for means
/// the window has no more, so the short page is the whole answer. A round that returned no row at all carries no
/// count (the count rides on the rows), and is treated as exhausted.</para>
/// </summary>
public static class TopFill
{
    /// <summary>Round one's over-fetch, the value the statements used before the fill (#5313).</summary>
    public const int FirstExtra = 5;

    /// <summary>Most rounds one read may run, the first included.</summary>
    public const int MaxRounds = 3;

    /// <summary>Each later round asks for this many times the previous candidate limit.</summary>
    public const int Growth = 4;

    /// <summary>The candidate limit never passes <c>top + MaxExtraCandidates</c>.</summary>
    public const int MaxExtraCandidates = 200;

    /// <summary>The first round's candidate limit.</summary>
    public static int FirstCandidates(int top) => Clamp((long)top + FirstExtra);

    /// <summary>
    /// The next round's candidate limit, or null when the page is done: it holds <paramref name="top"/> rows,
    /// the last round did not fill its candidate limit (the candidates ran out), the round bound is reached, or
    /// the limit cannot grow any further.
    /// </summary>
    /// <param name="top">Rows the caller asked for.</param>
    /// <param name="round">The round just run, 1-based.</param>
    /// <param name="candidates">The candidate limit that round ran with.</param>
    /// <param name="returned">Rows that round returned after the WAITFOR trim.</param>
    /// <param name="candidateCount">How many candidates pass 1 produced; 0 when no row came back.</param>
    public static int? NextCandidates(int top, int round, int candidates, int returned, int candidateCount)
    {
        if (returned >= top || round >= MaxRounds || candidateCount < candidates)
        {
            return null;
        }

        var cap = Clamp((long)top + MaxExtraCandidates);
        var next = Math.Min(cap, Clamp((long)candidates * Growth));
        return next > candidates ? next : null;
    }

    /// <summary>
    /// Runs <paramref name="round"/> with each candidate limit in turn and returns the last round's rows. The
    /// delegate returns that round's rows and its candidate count (<see cref="NextCandidates"/>).
    /// </summary>
    public static async Task<List<T>> RunAsync<T>(int top, Func<int, Task<(List<T> Rows, int CandidateCount)>> round)
    {
        ArgumentNullException.ThrowIfNull(round);
        var candidates = FirstCandidates(top);
        for (var n = 1; ; n++)
        {
            var (rows, candidateCount) = await round(candidates).ConfigureAwait(false);
            var next = NextCandidates(top, n, candidates, rows.Count, candidateCount);
            if (next is null)
            {
                return rows;
            }

            candidates = next.Value;
        }
    }

    private static int Clamp(long value) => (int)Math.Min(value, int.MaxValue);
}
