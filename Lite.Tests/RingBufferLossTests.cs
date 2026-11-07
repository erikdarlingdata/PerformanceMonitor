/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>#8146: the loss arithmetic over two reads of a ring-buffer target header and its session row.</summary>
public sealed class RingBufferLossTests
{
    private static readonly DateTime T0 = new(2026, 11, 7, 10, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Created = new(2026, 11, 7, 9, 0, 0, 360, DateTimeKind.Unspecified);

    private static RingBufferPrior Prior(long total = 30, long held = 10, long dropped = 0, bool truncated = false, DateTime? created = null) =>
        new(total, held, dropped, created ?? Created, T0, truncated);

    private static RingBufferReading Reading(
        long? total, long? held = 10, long? dropped = 0, DateTime? created = null, int secondsLater = 60,
        bool truncated = false, long? limit = 10, bool sessionMissing = false) =>
        new(total, held, sessionMissing ? null : dropped, sessionMissing ? null : created ?? Created, T0.AddSeconds(secondsLater), truncated, limit);

    [Fact]
    public void Compute_NoPrior_IsBaselineWithoutLoss()
    {
        var r = RingBufferLoss.Compute(null, new RingBufferReading(30, 10, 0, Created, T0, false, 10));
        Assert.False(r.Unknown);
        Assert.Equal(0, r.Evicted);
        Assert.Equal(30, r.NextPrior!.Value.Total);
    }

    [Fact]
    public void Compute_DeltaWithinHeld_NoLoss()
    {
        var r = RingBufferLoss.Compute(Prior(), Reading(34));
        Assert.Equal(0, r.Evicted);
        Assert.False(r.Unknown);
    }

    [Fact]
    public void Compute_DeltaAboveHeld_EvictsDifference()
    {
        var r = RingBufferLoss.Compute(Prior(total: 34), Reading(59));
        Assert.Equal(15, r.Evicted);
        Assert.Equal(60, r.WindowSeconds);
    }

    [Fact]
    public void Compute_CreatedJitterUnderOneSecond_IsSameSession()
    {
        var r = RingBufferLoss.Compute(Prior(), Reading(34, created: Created.AddMilliseconds(6.7)));
        Assert.False(r.Unknown);
    }

    [Fact]
    public void Compute_CreatedMovedOverOneSecond_IsUnknownForBothCounts()
    {
        var moved = Created.AddSeconds(5);
        var r = RingBufferLoss.Compute(Prior(dropped: 5), Reading(100, dropped: 90, created: moved));
        Assert.True(r.Unknown);
        Assert.Equal(0, r.Evicted);
        Assert.Equal(0, r.Dropped);
        Assert.Equal(100, r.NextPrior!.Value.Total);
        Assert.Equal(moved, r.NextPrior.Value.Created);
        Assert.Equal(90, r.NextPrior.Value.Dropped);
    }

    [Fact]
    public void Compute_TotalFell_IsUnknown()
    {
        var r = RingBufferLoss.Compute(Prior(total: 500), Reading(20));
        Assert.True(r.Unknown);
        Assert.Equal(20, r.NextPrior!.Value.Total);
    }

    [Fact]
    public void Compute_DroppedRose_ReportsDroppedSeparately()
    {
        var r = RingBufferLoss.Compute(Prior(dropped: 5), Reading(34, dropped: 90));
        Assert.Equal(85, r.Dropped);
        Assert.Equal(0, r.Evicted);
    }

    [Fact]
    public void Compute_GatedRun_DroppedRoseWithoutHeader()
    {
        var r = RingBufferLoss.Compute(Prior(total: 30, held: 10, dropped: 5), Reading(null, held: null, dropped: 20));
        Assert.True(r.Dropped > 0);
        Assert.Equal(0, r.Evicted);
        Assert.Equal(30, r.NextPrior!.Value.Total);
        Assert.Equal(10, r.NextPrior.Value.Held);
        Assert.Equal(20, r.NextPrior.Value.Dropped);
    }

    [Fact]
    public void Compute_GatedRunWithoutPrior_NoNextPrior()
    {
        var r = RingBufferLoss.Compute(null, Reading(null, held: null));
        Assert.Null(r.NextPrior);
    }

    [Fact]
    public void Compute_SessionMissing_NoNextPrior()
    {
        var r = RingBufferLoss.Compute(Prior(), Reading(99, sessionMissing: true));
        Assert.False(r.Unknown);
        Assert.Equal(0, r.Evicted);
        Assert.Equal(0, r.Dropped);
        Assert.Null(r.NextPrior);
    }

    [Fact]
    public void Compute_PriorTruncated_MarksAtLeast()
    {
        var r = RingBufferLoss.Compute(Prior(total: 34, truncated: true), Reading(59));
        Assert.True(r.AtLeast);
        Assert.False(RingBufferLoss.Compute(Prior(total: 34), Reading(59)).AtLeast);
    }

    [Theory]
    [InlineData(10000L, 519L, true)]
    [InlineData(10L, 10L, false)]
    [InlineData(0L, 10L, true)]
    public void Compute_MemoryCap(long limit, long held, bool capped)
    {
        var r = RingBufferLoss.Compute(Prior(total: 0, held: 0), Reading(held + 481, held: held, limit: limit));
        Assert.Equal(481, r.Evicted);
        Assert.Equal(capped, r.MemoryCapped);
    }

    [Fact]
    public void TryParseState_RoundTrips()
    {
        var prior = new RingBufferPrior(1234, 56, 7, Created, T0, true);
        var parsed = RingBufferLoss.TryParseState(RingBufferLoss.ToStateValue(prior));
        Assert.Equal(prior, parsed);
    }

    [Theory]
    [InlineData("")]
    [InlineData("x|y")]
    [InlineData("1|2|3")]
    [InlineData(null)]
    public void TryParseState_Garbage_IsNoPrior(string? value)
    {
        Assert.Null(RingBufferLoss.TryParseState(value));
    }
}
