/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Bounds how far back an incremental collector will catch up after an outage (#1556). The field
/// incident was query_store: a service stopped for days came back, read its stored per-database
/// watermark, and issued a cutoff pointing DAYS into the past — the source retains ~30 days, so the
/// one cycle tried to pull the entire backlog at once and drove the 0→13GB commit-limit blowout.
///
/// <para>
/// <see cref="ClampCatchup"/> floors a stale watermark to <c>now - 1h</c>: a routine restart never
/// clamps (its watermark is minutes old), and anything longer survives as a deliberate, logged,
/// BOUNDED hole that the backfill worker (#2022/#2058) trickles in afterwards. The horizon was 24h
/// until the use1 migration wedge (#2102) proved a row cap is not a cost cap: the per-database query
/// aggregates and sorts the WHOLE window before TOP or the byte budget can bound anything, so its
/// cost grows with window width. A big database that missed one 60s cycle faced a wider window the
/// next cycle, which cost more and timed out again — a self-sustaining spiral the 24h clamp sat far
/// above and never interrupted. One hour is the envelope the fleet already proves every day (Query
/// Store's 900s flush cadence makes 15–60min effective windows the routine steady state), and the
/// clamp floor slides forward with <c>now</c>, so recovery is immediate no matter how stale the
/// watermark got. This is deliberately NOT applied to every timestamp watermark — for a ring-buffer
/// or rolling-trace source the clamp is a no-op at best and, on a quiet <c>default_trace</c> whose
/// 100MB ring can span days, a WRONG truncation of legitimate catch-up. It is therefore scoped to
/// exactly ONE collector: query_store's per-database cutoff (the only unbounded-persisted source among
/// the collectors). It lives here as a pure function purely so that placement is unit-testable in
/// isolation.
/// </para>
///
/// <para>
/// Two call sites, one collector. The hosts apply it in the enumeration loop's per-database watermark
/// refresh, where they also emit the operator-visible WARNING; and <c>QueryStoreCollector</c> applies it
/// again inside its own cutoff computation, which is what makes the bound hold on the Azure SQL DB
/// per-database path (#1836) — that host branch is shared with the XE ring-buffer collectors and so
/// deliberately does not clamp. Applying it twice is a no-op by construction: clamping an
/// already-clamped value returns it unchanged.
/// </para>
/// </summary>
public static class WatermarkPolicy
{
    /// <summary>
    /// The maximum catch-up horizon — the live path's one-query cost envelope, matched to
    /// <see cref="QueryStoreBackfillState.MaxSliceSpan"/> so no path ever windows wider than the
    /// steady state the fleet proves (#2102). Everything older is the backfill worker's job.
    /// Exposed so a test pins the boundary.
    ///
    /// <para><b>This is the only place the horizon's number is written down, and that is now asserted</b>
    /// (<c>TheCatchUpHorizon_IsWrittenDownInExactlyOnePlace</c>). #2102 moved it from 24 hours to one, and
    /// every runner that applies the clamp carried a comment calling it "the 24h catch-up clamp". None of
    /// them moved with it. Nothing misbehaved and nothing went red, because the operator-facing WARNINGs
    /// interpolate <see cref="MaxCatchup"/> rather than restating it — so the stale prose was invisible to
    /// every instrument the repo has, and it survived long enough for #2468 to be filed against a 24-hour
    /// lever that had not existed for months. Those comments now name the concept and leave the number
    /// here; the assertion is what stops the next move from doing this again.</para>
    /// </summary>
    public static readonly TimeSpan MaxCatchup = TimeSpan.FromHours(1);

    /// <summary>
    /// Floors a stale timestamp watermark to <c>now - <see cref="MaxCatchup"/></c>; a null watermark (nothing
    /// collected yet — the definition's documented first-run window applies) stays null, and a
    /// watermark within the horizon is returned unchanged. Compare the result to the input to tell
    /// whether a clamp fired (the runner logs a WARNING when it does).
    /// </summary>
    /// <param name="watermark">The stored watermark (newest already-collected timestamp), or null.</param>
    /// <param name="now">The current collection time (the cutoff is measured back from here).</param>
    public static DateTime? ClampCatchup(DateTime? watermark, DateTime now)
    {
        if (watermark is null)
        {
            return null;
        }

        var floor = now - MaxCatchup;
        return watermark.Value < floor ? floor : watermark;
    }

    /// <summary>
    /// Extra history the watermark READ may look at beyond <see cref="MaxCatchup"/> (#2344). Purely a
    /// safety margin for clock disagreement between a monitored server and the store — the correctness
    /// argument needs none of it, so it is generous rather than tuned.
    /// </summary>
    public static readonly TimeSpan ReadFloorMargin = TimeSpan.FromHours(2);

    /// <summary>
    /// The oldest <c>collection_time</c> a clamped watermark read has to consider (#2344), or null when
    /// <paramref name="now"/> is default — callers pass this straight through to the store read as an
    /// optional bound.
    ///
    /// <para><b>Why bounding the read changes no answer.</b> Every consumer of a clamped watermark ends
    /// up at <c>max(stored, now - MaxCatchup)</c>: <see cref="ClampCatchup"/> floors anything older, and a
    /// NULL result falls back to query_store's documented 60-minute first-run window — the same instant as
    /// the floor. So a row older than the horizon cannot move the result whether it is found or not, and
    /// the unbounded <c>MAX</c> that used to find it was paying to confirm a value the clamp would have
    /// produced anyway. Measured on the 106 GB use1 store: 25,766 buffer reads plus temp spill cold, 228 ms
    /// warm, against 29 ms bounded (five chunks excluded) — and the unbounded cost scales with STORE SIZE
    /// and cache residency rather than with anything the monitored server is doing, so it degrades exactly
    /// where an operator is weakest.</para>
    ///
    /// <para><b>Bound the PARTITIONING column, not the watermark column.</b> The hypertables partition on
    /// <c>collection_time</c>; a predicate on the watermark column alone prunes nothing. This is safe
    /// because a row's watermark value can never exceed its own <c>collection_time</c> — an execution
    /// cannot be collected before it happens — so no qualifying row hides behind the bound.</para>
    ///
    /// <para><b>Only for readers whose value is clamped.</b> The clamp is scoped to query_store (see the
    /// class remarks); a ring-buffer collector whose legitimate catch-up spans days must keep reading its
    /// full history, and handing it this floor would silently truncate that. A future definition wanting
    /// the bound has to adopt <see cref="ClampCatchup"/> first — the two travel together.</para>
    /// </summary>
    public static DateTime? ReadFloor(DateTime now) =>
        now == default ? null : now - MaxCatchup - ReadFloorMargin;
}
