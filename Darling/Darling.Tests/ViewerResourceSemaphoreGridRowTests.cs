/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6388 review round 1: the two Resource Semaphore grids must not print what the web's <c>get_resource_semaphore</c> nulls.
/// A pool that never had a waiter has no "peak instant" (the shared read's DISTINCT ON picks the newest snapshot, an
/// arbitrary row), and a row stored with the 0 interval marker has two fabricated delta zeros that are not "no timeouts
/// this interval". Pure row-shaping, no database.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerResourceSemaphoreGridRowTests
{
    private static MemoryGrantWindowRow Window(long peakWaiters, double peakGrantedMb)
    {
        var at = new DateTime(2026, 10, 23, 14, 58, 0, DateTimeKind.Unspecified);
        return new MemoryGrantWindowRow(
            ResourceSemaphoreId: 0, PoolId: 2, SnapshotsInWindow: 5, FirstSnapshotAt: at.AddHours(-1), LastSnapshotAt: at,
            PeakWaiterCount: peakWaiters, PeakWaitersAt: at, PeakGrantedMemoryMb: peakGrantedMb, MinAvailableMemoryMb: 10.0,
            TimeoutErrorsInWindow: 0, ForcedGrantsInWindow: 0, PeakGrantedAt: at);
    }

    [Fact]
    public void APoolThatNeverHadAWaiterOrAGrant_HasNoPeakInstant_AndAPoolThatDid_Has()
    {
        var quiet = ResourceSemaphorePressureGridRow.From(Window(0, 0));
        Assert.Null(quiet.PeakWaitersAtLocal);
        Assert.Null(quiet.PeakGrantedAtLocal);

        var busy = ResourceSemaphorePressureGridRow.From(Window(6, 128.5));
        Assert.False(string.IsNullOrEmpty(busy.PeakWaitersAtLocal));
        Assert.False(string.IsNullOrEmpty(busy.PeakGrantedAtLocal));

        /* Each instant follows its own peak: waiters with none granted, and the reverse. */
        var waitersOnly = ResourceSemaphorePressureGridRow.From(Window(3, 0));
        Assert.NotNull(waitersOnly.PeakWaitersAtLocal);
        Assert.Null(waitersOnly.PeakGrantedAtLocal);
    }

    private static ResourceSemaphoreRow Snapshot(int? intervalSeconds) => new(
        CollectionTime: new DateTime(2026, 10, 23, 14, 58, 0, DateTimeKind.Unspecified), ResourceSemaphoreId: 0, PoolId: 2,
        TargetMemoryMb: 1, MaxTargetMemoryMb: 1, TotalMemoryMb: 1, AvailableMemoryMb: 1, GrantedMemoryMb: 1, UsedMemoryMb: 1,
        GranteeCount: 0, WaiterCount: 0, TimeoutErrorCount: 9, ForcedGrantCount: 8,
        TimeoutErrorCountDelta: 0, ForcedGrantCountDelta: 0, SampleIntervalSeconds: intervalSeconds);

    [Fact]
    public void ARowStoredWithTheZeroIntervalMarker_ShowsNoDeltas_NotAMeasuredZero()
    {
        var restart = new ResourceSemaphoreSnapshotGridRow(Snapshot(0));
        Assert.Null(restart.TimeoutErrorCountDelta);
        Assert.Null(restart.ForcedGrantCountDelta);
        Assert.Equal(PerformanceMonitor.Common.DeltaSeriesShaping.UnknowableIntervalDisplay, restart.IntervalDisplay);
        /* The cumulative counters beside them are real and stay. */
        Assert.Equal(9, restart.TimeoutErrorCount);
        Assert.Equal(8, restart.ForcedGrantCount);

        var measured = new ResourceSemaphoreSnapshotGridRow(Snapshot(60));
        Assert.Equal(0L, measured.TimeoutErrorCountDelta);
        Assert.Equal(0L, measured.ForcedGrantCountDelta);
        Assert.Equal("60", measured.IntervalDisplay);

        /* A pre-V127 row has no stored interval, but its deltas are real and kept. */
        var unrecorded = new ResourceSemaphoreSnapshotGridRow(Snapshot(null));
        Assert.Equal(0L, unrecorded.TimeoutErrorCountDelta);
    }
}
