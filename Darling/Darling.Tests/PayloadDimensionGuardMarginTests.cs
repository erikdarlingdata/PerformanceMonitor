/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4503: <see cref="PayloadDimensions.LastSeenRefreshGuardHours"/> widened from 1 hour to 6 to cut the
/// non-HOT WAL a hot dimension row takes on every re-sighting (measured on one production store:
/// <c>query_plan_dim</c> took 894,656 non-HOT touches in 71 hours under the 1-hour guard, ~42 KB of WAL
/// per row, because <c>last_seen</c> is indexed and the update can never go HOT).
///
/// <para>The margin <see cref="DarlingRetention.ComputeDimensionCutoff"/> reserves for a trailing stamp
/// must stay at least as wide as EVERY guard that writes <see cref="PayloadDimensions.LastSeenColumn"/>,
/// or a dim row referenced by a live fact could be pruned while it still looked live to the fact. This
/// pins that relation directly, rather than trusting the doc comments to stay in sync with either
/// constant.</para>
/// </summary>
public sealed class PayloadDimensionGuardMarginTests
{
    /// <summary>
    /// The dimension GC's trailing margin, in hours — <c>ChunkIntervalDays + 1</c> days, the same
    /// arithmetic <see cref="DarlingRetention.ComputeDimensionCutoff"/> uses for both its coupled and
    /// its dedicated-knob arms.
    /// </summary>
    private const int DimensionCutoffMarginHours = (TimescaleSupport.ChunkIntervalDays + 1) * 24;

    /// <summary>
    /// The dim upsert's own guard must fit inside the margin the GC reserves for a trailing stamp — the
    /// direct consequence of widening it is checked here rather than assumed from the doc comment.
    /// </summary>
    [Fact]
    public void TheDimUpsertGuardFitsInsideTheDimensionCutoffMargin()
    {
        Assert.True(
            PayloadDimensions.LastSeenRefreshGuardHours <= DimensionCutoffMarginHours,
            $"the dim upsert's guard is {PayloadDimensions.LastSeenRefreshGuardHours}h, which must not exceed " +
            $"the {DimensionCutoffMarginHours}h margin ComputeDimensionCutoff reserves for a trailing stamp — " +
            "a wider guard than the margin could prune content a live fact still resolves to.");
    }

    /// <summary>
    /// The Query Store liveness touch's own guard (the OTHER writer of a dim row's <c>last_seen</c>) must
    /// also fit inside the same margin — restated here directly against the margin rather than only
    /// against <see cref="QueryStoreLivenessTouchGuard.StampSkewMarginHours"/>, which is itself just the
    /// map's/text's margin, not the dimension's.
    /// </summary>
    [Fact]
    public void TheLivenessTouchGuardAlsoFitsInsideTheDimensionCutoffMargin()
    {
        Assert.True(
            QueryStoreLivenessTouchGuard.GuardHours <= DimensionCutoffMarginHours,
            $"the liveness touch guard is {QueryStoreLivenessTouchGuard.GuardHours}h, which must not exceed " +
            $"the {DimensionCutoffMarginHours}h margin ComputeDimensionCutoff reserves for a trailing stamp.");
    }

    /// <summary>
    /// Both guards' effect on <see cref="DarlingRetention.ComputeDimensionCutoff"/>'s dedicated (plan-content
    /// knob) arm directly: a row stamped exactly at "now minus the wider guard" must still be strictly
    /// newer than the computed cutoff, for every knob width the knob accepts (7-365 per
    /// <c>StoreConfigProvider.ValidRetention</c>'s clamp, sampled at its ends and the shipped default).
    /// </summary>
    [Theory]
    [InlineData(7)]
    [InlineData(21)]
    [InlineData(365)]
    public void ARowStampedAtTheWiderGuardsAgeIsNeverPrunedByTheDedicatedCutoff(int planContentRetentionDays)
    {
        var now = new DateTime(2026, 9, 27, 12, 0, 0, DateTimeKind.Unspecified);
        var widerGuardHours = Math.Max(
            PayloadDimensions.LastSeenRefreshGuardHours,
            QueryStoreLivenessTouchGuard.GuardHours);

        var cutoff = DarlingRetention.ComputeDimensionCutoff(
            now, widestFactRetentionDays: 1, oldestSurvivingDigestFact: null, planContentRetentionDays: planContentRetentionDays);

        var stampAtWiderGuardAge = now.AddHours(-widerGuardHours);

        Assert.True(
            stampAtWiderGuardAge > cutoff,
            $"a row last touched {widerGuardHours}h ago (the wider of the two guards) must survive the " +
            $"cutoff {cutoff:yyyy-MM-dd HH:mm}Z computed for now {now:yyyy-MM-dd HH:mm}Z with a " +
            $"{planContentRetentionDays}-day plan-content knob.");
    }
}
