/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// Pins the #2022 backfill worker's pure contracts: the hole codec (round-trip, malformed-input
/// conservatism, the merge-widens rule), the horizon DERIVATION (raw tier minus route margin —
/// the #1937 no-second-hand-maintained-number rule), and the state identity that keeps the
/// worker's collector_state rows off the query_store definition (whose "declares NO StateKeys"
/// contract is pinned by CollectorStateContractTests and must survive this feature).
/// </summary>
public sealed class QueryStoreBackfillTests
{
    [Fact]
    public void HoleCodec_RoundTrips_AndRejectsMalformedOrInverted()
    {
        var from = new DateTime(2026, 7, 1, 3, 15, 30, DateTimeKind.Utc).AddTicks(1234567);
        var to = new DateTime(2026, 7, 2, 3, 15, 30, DateTimeKind.Utc);

        var encoded = QueryStoreBackfillState.EncodeHole(from, to);
        Assert.True(QueryStoreBackfillState.TryDecodeHole(encoded, out var decodedFrom, out var decodedTo));
        Assert.Equal(from, decodedFrom);
        Assert.Equal(to, decodedTo);

        /* Malformed values decode false — the scan treats that as "no hole recorded", the
           conservative direction (the tail logic still runs; nothing throws mid-loop). */
        Assert.False(QueryStoreBackfillState.TryDecodeHole("", out _, out _));
        Assert.False(QueryStoreBackfillState.TryDecodeHole("not|dates", out _, out _));
        Assert.False(QueryStoreBackfillState.TryDecodeHole(from.ToString("o"), out _, out _));
        /* An inverted or empty range is malformed too: from must be strictly before to. */
        Assert.False(QueryStoreBackfillState.TryDecodeHole(QueryStoreBackfillState.EncodeHole(to, from), out _, out _));
        Assert.False(QueryStoreBackfillState.TryDecodeHole(QueryStoreBackfillState.EncodeHole(from, from), out _, out _));
    }

    [Fact]
    public void MergeHole_WidensOverExisting_AndStartsFreshOverGarbage()
    {
        var from = new DateTime(2026, 7, 2, 0, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 7, 3, 0, 0, 0, DateTimeKind.Utc);

        /* No existing record: the new clamp IS the hole. */
        Assert.Equal((from, to), QueryStoreBackfillState.MergeHole(null, from, to));

        /* A repeat outage WIDENS the pending hole in both directions — overwriting would lose the
           unserviced earlier range, a silent hole in a design whose premise is that holes are
           recorded. */
        var earlierWider = QueryStoreBackfillState.EncodeHole(from.AddDays(-1), to.AddHours(-12));
        Assert.Equal((from.AddDays(-1), to), QueryStoreBackfillState.MergeHole(earlierWider, from, to));

        var laterWider = QueryStoreBackfillState.EncodeHole(from.AddHours(12), to.AddDays(1));
        Assert.Equal((from, to.AddDays(1)), QueryStoreBackfillState.MergeHole(laterWider, from, to));

        /* Garbage in the state row falls back to the fresh clamp, never a throw. */
        Assert.Equal((from, to), QueryStoreBackfillState.MergeHole("garbage", from, to));
    }

    /// <summary>
    /// The backfill horizon is the SMALLER of two conditions that both have to hold, and #3012 is what made
    /// them differ.
    ///
    /// <para>A slice lands rows at a BACKDATED <c>collection_time</c>, so two things must be true at the depth
    /// it lands: raw retention must not immediately drop them, and the hourly aggregates must still
    /// re-materialize the buckets it touched. The rollups are materialized-only — the watermark is a hard
    /// partition, not a fallback — so a bucket the refresh window no longer covers keeps whatever it was
    /// materialized with, and the backfilled rows become invisible to every window that routes at hourly grain
    /// while still being visible at raw grain. A split between two tiers, not a delay.</para>
    ///
    /// <para><b>Why the old single-term form was not merely simpler.</b> It read
    /// <c>Horizon == RetentionTierRouter.RawMaxAge</c>, and that was indistinguishable from correct only
    /// because the hourly refresh window was ALSO 3 days: the retention term was standing in for the refresh
    /// term. That is the coupling #3012 is about — the refresh window having to cover a depth retention chose,
    /// so every retention increase silently bought more refresh cost. With the window now chosen against its
    /// own cadence the two terms separate, and the refresh one is the binding one.</para>
    ///
    /// <para>The concrete value is pinned as well as the relationship, because the relationship alone would
    /// hold under either treatment: 23 hours is the hourly window minus one refresh interval. Reverting the
    /// window to 3 days makes it 71 hours; dropping the refresh term makes it 3 days. Both are red here.</para>
    /// </summary>
    [Fact]
    public void Horizon_IsTheSmallerOfTheRefreshWindowAndTheRawTier_NotEitherAlone()
    {
        var refreshReach = TimescaleSupport.HourlyRefreshStartSpan - TimescaleSupport.HourlyRefreshScheduleSpan;

        Assert.Equal(TimeSpan.FromHours(23), QueryStoreBackfill.RollupStoreHorizon);

        Assert.True(QueryStoreBackfill.RollupStoreHorizon <= refreshReach,
            "a slice below the depth the next hourly refresh still reaches lands rows no rollup will ever materialize");
        Assert.True(QueryStoreBackfill.RollupStoreHorizon <= RetentionTierRouter.RawMaxAge,
            "a slice below the raw read horizon lands rows the next purge immediately drops");
        Assert.True(QueryStoreBackfill.RollupStoreHorizon < TimescaleSupport.RawRetentionSpan,
            "the backfill horizon must sit strictly inside raw retention, or a slice could land rows the next purge immediately drops");
        Assert.True(QueryStoreBackfill.RollupStoreHorizon < TimescaleSupport.HourlyRefreshStartSpan,
            "the horizon must sit strictly inside the refresh window: the window slides forward by one schedule interval between runs, so its own boundary is already outside it by the time the policy next fires");
    }

    /// <summary>
    /// The refresh term applies only where there are rollups to fall out of.
    ///
    /// <para>TimescaleDB is optional and auto-detected, and plain PostgreSQL is a supported configuration
    /// rather than a degraded one. On such a store every read goes to raw, so a backdated row cannot be
    /// stranded outside a refresh window — and narrowing the horizon for that term anyway would have cost that
    /// deployment mode two days of catch-up reach in exchange for nothing. #3012's change narrowed
    /// unconditionally at first; this is the pin for the correction.</para>
    ///
    /// <para>Asserted as an INEQUALITY between the two horizons, not just as two values: the defect shape is
    /// the plain store silently inheriting the rollup store's narrower reach, and that reads as equal
    /// horizons. So a future change that collapses them back is red here even if both numbers move.</para>
    /// </summary>
    [Fact]
    public void PlainPostgresHorizon_KeepsTheFullRawDepth_BecauseThereAreNoRollupsToFallOutOf()
    {
        Assert.Equal(RetentionTierRouter.RawMaxAge, QueryStoreBackfill.PlainStoreHorizon);
        Assert.Equal(TimeSpan.FromDays(3), QueryStoreBackfill.PlainStoreHorizon);

        Assert.True(QueryStoreBackfill.PlainStoreHorizon > QueryStoreBackfill.RollupStoreHorizon,
            "a plain-PostgreSQL store must not inherit the rollup store's narrower reach — it has no rollups, so the refresh term does not apply to it");

        /* The selector is what the slice path actually calls, so it is pinned rather than only the pair. */
        Assert.Equal(QueryStoreBackfill.RollupStoreHorizon, QueryStoreBackfill.HorizonFor(hasContinuousAggregates: true));
        Assert.Equal(QueryStoreBackfill.PlainStoreHorizon, QueryStoreBackfill.HorizonFor(hasContinuousAggregates: false));
    }

    [Fact]
    public void BoundSliceFloor_CapsWideRanges_AndPassesNarrowOnesThrough()
    {
        /* #2102: a slice queries at most the top MaxSliceSpan of its remaining range — the byte
           budget bounds what ships, not what the query aggregates and sorts, so an unchunked wide
           window on a big database re-times-out every tick and the range never drains. The caller
           reads the verdict from the result: floor moved = chunk (an empty slice shrinks the
           ceiling and keeps walking); floor unmoved = the whole remainder was asked (an empty
           slice is terminal, the pre-chunking semantics). */
        var ceiling = new DateTime(2026, 8, 7, 12, 0, 0, DateTimeKind.Utc);

        var wideFloor = ceiling.AddHours(-23);
        Assert.Equal(ceiling - QueryStoreBackfillState.MaxSliceSpan, QueryStoreBackfillState.BoundSliceFloor(wideFloor, ceiling));

        var narrowFloor = ceiling.AddMinutes(-25);
        Assert.Equal(narrowFloor, QueryStoreBackfillState.BoundSliceFloor(narrowFloor, ceiling));

        /* Exactly MaxSliceSpan wide is narrow enough — one slice takes it whole, so its empty
           verdict stays terminal rather than saving a zero-width hole. */
        var exactFloor = ceiling - QueryStoreBackfillState.MaxSliceSpan;
        Assert.Equal(exactFloor, QueryStoreBackfillState.BoundSliceFloor(exactFloor, ceiling));
    }

    [Fact]
    public void AdaptiveSpan_HalvesPerFailure_FloorsAtFifteenMinutes_AndResetsAtZero()
    {
        /* #2111 promoted from reserve on field evidence: a member whose 1h window intermittently
           exceeds the command timeout stayed stuck for hours (Redstone, 3+ hours flat overnight) —
           halving toward a floor gives it a window that fits, and the skipped range rides the same
           hole records the clamp writes. Zero failures = full width, success resets the counter at
           every call site, and the exponent cap keeps the shift math from wrapping. */
        var full = QueryStoreBackfillState.MaxSliceSpan;

        Assert.Equal(full, QueryStoreBackfillState.AdaptiveSpan(full, 0));
        Assert.Equal(TimeSpan.FromMinutes(30), QueryStoreBackfillState.AdaptiveSpan(full, 1));
        Assert.Equal(TimeSpan.FromMinutes(15), QueryStoreBackfillState.AdaptiveSpan(full, 2));
        Assert.Equal(QueryStoreBackfillState.MinAdaptiveSpan, QueryStoreBackfillState.AdaptiveSpan(full, 3));
        Assert.Equal(QueryStoreBackfillState.MinAdaptiveSpan, QueryStoreBackfillState.AdaptiveSpan(full, 100));

        Assert.Equal(TimeSpan.FromMinutes(15), QueryStoreBackfillState.MinAdaptiveSpan);
    }

    [Fact]
    public void BoundSliceFloor_AdaptiveForm_CapsToThePassedSpan()
    {
        var ceiling = new DateTime(2026, 8, 8, 12, 0, 0, DateTimeKind.Utc);
        var wideFloor = ceiling.AddHours(-23);

        Assert.Equal(
            ceiling - TimeSpan.FromMinutes(15),
            QueryStoreBackfillState.BoundSliceFloor(wideFloor, ceiling, TimeSpan.FromMinutes(15)));

        /* The parameterless form stays the full-span behavior. */
        Assert.Equal(
            ceiling - QueryStoreBackfillState.MaxSliceSpan,
            QueryStoreBackfillState.BoundSliceFloor(wideFloor, ceiling));
    }

    [Fact]
    public void ShouldYieldToLive_YieldsInsideTheWindow_RunsOutsideIt_AndNeverOnNull()
    {
        /* #2111: a live query_store failure inside the window means the replica is contended NOW —
           the slice yields. At or beyond the window (or never failed), backfill runs. The window is
           two poll cycles: current-or-previous-cycle failures count, older ones are history. */
        var now = new DateTime(2026, 8, 7, 17, 0, 0, DateTimeKind.Utc);

        Assert.False(QueryStoreBackfillState.ShouldYieldToLive(null, now));
        Assert.True(QueryStoreBackfillState.ShouldYieldToLive(now.AddMinutes(-1), now));
        Assert.True(QueryStoreBackfillState.ShouldYieldToLive(now - QueryStoreBackfillState.YieldToLiveWindow + TimeSpan.FromSeconds(1), now));
        Assert.False(QueryStoreBackfillState.ShouldYieldToLive(now - QueryStoreBackfillState.YieldToLiveWindow, now));
        Assert.False(QueryStoreBackfillState.ShouldYieldToLive(now.AddHours(-2), now));

        Assert.Equal(TimeSpan.FromMinutes(10), QueryStoreBackfillState.YieldToLiveWindow);
    }

    [Fact]
    public void StateIdentity_IsTheWorkersOwn_NotTheDefinitions()
    {
        /* The worker owns its collector_state rows under its OWN name, so the query_store
           DEFINITION keeps declaring no StateKeys (pinned by CollectorStateContractTests'
           TheOnlyCollectorDeclaringStateIsDefaultTraceEvents — this is the seam that lets both
           stay true). The key prefixes are part of the stored contract: rows written today must
           decode after an upgrade. */
        Assert.Equal("query_store_backfill", QueryStoreBackfillState.StateCollectorName);
        Assert.Equal(QueryStoreBackfillState.StateCollectorName, QueryStoreBackfill.StateCollectorName);
        Assert.Equal("done:", QueryStoreBackfillState.DoneKeyPrefix);
        Assert.Equal("hole:", QueryStoreBackfillState.HoleKeyPrefix);
        Assert.Empty(QueryStoreCollector.Instance.StateKeys);
    }
}
