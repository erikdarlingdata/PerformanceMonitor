/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Decision-table pins for the shared <see cref="QueryDopProvenance"/> (#3648) — the one sentence both
/// SKUs' <c>top_cpu_queries</c> / <c>bad_actor_query</c> drill-downs attach when a query-stats group's
/// cross-plan <c>max_dop</c> history disagrees with the newest plan's reading. This SAME table is pinned
/// identically in Darling.Tests (<c>QueryDopProvenanceTests</c>) so the two SKUs cannot drift; the shape is
/// <c>QueryStatExtremesTests</c>', the #2235 precedent for a lifetime-extreme annotation.
/// </summary>
public sealed class QueryDopProvenanceTests
{
    private static readonly DateTime Seen = new(2026, 9, 4, 13, 45, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// The live page (#3648): the newest plan is serial, an older plan ran at 16, three plans in the window.
    /// The exact format renderers print verbatim.
    /// </summary>
    [Fact]
    public void ParallelHistoryBehindASerialHeadline_IsSpelledOut()
    {
        Assert.Equal(
            "DOP 1 (a parallel plan ran at 16 until 2026-09-04; 3 plans in window)",
            QueryDopProvenance.Note(newestPlanMaxDop: 1, maxDopAnyPlan: 16, maxDopAnyPlanLastSeen: Seen, planCount: 3));
    }

    /// <summary>Row #3 on the same card: one serial plan, headline equals history, nothing to disclose.</summary>
    [Fact]
    public void HeadlineEqualToTheMaximum_CarriesNoNote()
    {
        Assert.Null(QueryDopProvenance.Note(1, 1, Seen, 1));
        Assert.Null(QueryDopProvenance.Note(8, 8, Seen, 2));
    }

    /// <summary>
    /// A cumulative per-plan maximum can never be BELOW the newest plan's own reading unless the group is
    /// inconsistent; the guard is <c>&lt;=</c>, so that case is also silent rather than inventing a history.
    /// </summary>
    [Fact]
    public void MaximumBelowTheHeadline_CarriesNoNote()
    {
        Assert.Null(QueryDopProvenance.Note(4, 2, Seen, 1));
    }

    /// <summary>No row in the group carried a reading: nothing to compare, nothing to say.</summary>
    [Fact]
    public void NoReadingAnywhere_CarriesNoNote()
    {
        Assert.Null(QueryDopProvenance.Note(null, null, null, 1));
    }

    /// <summary>
    /// Newest reading unknown, only serial plans on record: "unknown" is already the whole truth and there
    /// is no parallel plan to disclose.
    /// </summary>
    [Fact]
    public void UnknownHeadline_WithOnlySerialHistory_CarriesNoNote()
    {
        Assert.Null(QueryDopProvenance.Note(null, 1, Seen, 2));
    }

    /// <summary>
    /// Newest reading unknown but a parallel plan is on record — the case where a reader would otherwise
    /// reach for the folded maximum, so it is named as unknown and the history is disclosed.
    /// </summary>
    [Fact]
    public void UnknownHeadline_WithParallelHistory_SaysUnknownAndDiscloses()
    {
        Assert.Equal(
            "DOP unknown for the newest plan (a parallel plan ran at 16 until 2026-09-04; 2 plans in window)",
            QueryDopProvenance.Note(null, 16, Seen, 2));
    }

    /// <summary>
    /// One plan SHAPE (a recompile to the same query_plan_hash after MAXDOP was lowered resets the counter)
    /// still reads as a history; the count is singular and the "until" clause is dropped when the store
    /// could not say when the maximum was last seen.
    /// </summary>
    [Fact]
    public void SinglePlan_AndNoLastSeen_AreSpelledWithoutInvention()
    {
        Assert.Equal(
            "DOP 1 (a parallel plan ran at 16; 1 plan in window)",
            QueryDopProvenance.Note(1, 16, null, 1));
    }
}
