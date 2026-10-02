/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// #3648: the human sentence behind a query-stats drill-down row's DOP, composed once here so Lite and
/// Darling say the byte-identical thing. Sibling of <see cref="QueryStatExtremes"/>, which does the same
/// job for the lifetime CPU/elapsed extremes.
///
/// <para><b>What the number was.</b> <c>v_query_stats.max_dop</c> is <c>sys.dm_exec_query_stats.max_dop</c>:
/// a PER-PLAN high-water mark — the highest degree of parallelism that one cached plan has run at since it
/// entered the cache. The analysis drill-downs (<c>top_cpu_queries</c>, <c>bad_actor_query</c>) group query
/// stats by <c>(database_name, query_hash)</c>, and until #3648 projected <c>MAX(max_dop)</c> over that
/// group — which folds every plan the statement text ever had inside the window into one number and keeps
/// the largest. On a live card that number read 16 for a statement on an instance whose MAXDOP had been 1
/// for the whole 14-day config history, whose stored plan for that exact hash was serial
/// (<c>NonParallelPlanReason="MaxDOPSetToOne"</c>), and whose row #3 on the same card honestly said 1. The
/// 16 belonged to a plan compiled before the instance was pinned to 1, still sitting in the cache with its
/// old counter. A reader recommended a MAXDOP 1 Query Store hint from that field — the exact decision the
/// card exists to support — and retracted it after reading the plan the card should have summarized.</para>
///
/// <para><b>What the record says now.</b> <c>max_dop</c> is the NEWEST plan's reading — the row with the
/// latest <c>collection_time</c> among the rows that spent CPU in the window — because the card's question
/// is "what is this query doing to the CPU now". The cross-plan maximum survives as
/// <c>max_dop_any_plan</c>, with <c>max_dop_any_plan_last_seen</c> (the latest snapshot that carried it)
/// and <c>plan_count</c> (distinct <c>query_plan_hash</c> values in the group) beside it, so the maximum is
/// a HISTORY with provenance rather than a bare number. <see cref="Note"/> turns that history into the one
/// sentence a renderer can print verbatim when — and only when — the history disagrees with the headline.</para>
///
/// <para><b>0 is not a DOP.</b> A serial plan reports 1; the DMV never reports 0. The readers used to coerce
/// a NULL reading to 0 and the card rendered it as a number. Every DOP parameter here is nullable and NULL
/// means "unknown"; the note says so in words rather than inventing a value.</para>
/// </summary>
public static class QueryDopProvenance
{
    /// <summary>
    /// The conditional provenance sentence for one drill-down row, or null when the headline already tells
    /// the whole story. Fires only when a plan in the group ran at a HIGHER degree of parallelism than the
    /// newest plan reports — the case that produced the wrong recommendation — or when the newest plan's
    /// reading is unknown and a parallel plan is on record. A group whose every plan agrees with the
    /// headline gets no note: there is no history to disclose.
    /// </summary>
    /// <param name="newestPlanMaxDop">The newest plan's <c>max_dop</c>; null when the DMV row carried none.</param>
    /// <param name="maxDopAnyPlan">The cross-plan maximum over the window; null when no row carried a reading.</param>
    /// <param name="maxDopAnyPlanLastSeen">The latest <c>collection_time</c> at which a row reported that maximum.</param>
    /// <param name="planCount">Distinct <c>query_plan_hash</c> values in the group.</param>
    public static string? Note(int? newestPlanMaxDop, int? maxDopAnyPlan, DateTime? maxDopAnyPlanLastSeen, long planCount)
    {
        if (maxDopAnyPlan is null)
        {
            /* No row in the group carried a reading at all — nothing to compare, nothing to say. */
            return null;
        }

        if (newestPlanMaxDop is { } newest && maxDopAnyPlan <= newest)
        {
            /* The newest plan's reading IS the group's maximum: one honest number, no history behind it. */
            return null;
        }

        if (newestPlanMaxDop is null && maxDopAnyPlan <= 1)
        {
            /* Newest reading unknown, and the only plans on record were serial: "unknown" is already the
               whole truth, and there is no parallel plan to disclose. */
            return null;
        }

        var headline = newestPlanMaxDop is { } known
            ? $"DOP {known.ToString(CultureInfo.InvariantCulture)}"
            : "DOP unknown for the newest plan";

        var until = maxDopAnyPlanLastSeen is { } seen
            ? $" until {seen.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}"
            : string.Empty;

        var plans = planCount == 1
            ? "1 plan in window"
            : $"{planCount.ToString(CultureInfo.InvariantCulture)} plans in window";

        return $"{headline} (a parallel plan ran at {maxDopAnyPlan.Value.ToString(CultureInfo.InvariantCulture)}{until}; {plans})";
    }
}
