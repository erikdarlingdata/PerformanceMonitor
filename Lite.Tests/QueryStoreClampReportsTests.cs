/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9088: the rule that decides whether a stale-watermark catch-up clamp was already reported. The runner tests drive it through
/// the real collection loops. These cover the arms a loop cannot reach in test time: a gap longer than the catch-up window, and
/// databases that must not share a decision.
/// </summary>
public sealed class QueryStoreClampReportsTests
{
    private static readonly DateTime Raw = new(2026, 11, 19, 3, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T0 = Raw.AddHours(10);

    [Fact]
    public void ALandedCycleSilencesTheSameRawWatermark_WhileTheFloorIsStillInsideWhatItRead()
    {
        var reports = new QueryStoreClampReports();
        Assert.False(reports.AlreadyReported(1, "Sales", Raw, T0.AddHours(-1)));

        reports.Land(1, "Sales", Raw, T0);

        Assert.True(reports.AlreadyReported(1, "Sales", Raw, T0.AddHours(-1) + TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public void AGapLongerThanTheCatchUpWindow_IsANewGap_EvenForTheSameRawWatermark()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        /* Nothing ran for two days: the new floor lies past everything the landed cycle read, so the range between is unread. */
        var later = T0.AddDays(2);
        Assert.False(reports.AlreadyReported(1, "Sales", Raw, later - WatermarkPolicy.MaxCatchup));
    }

    [Fact]
    public void ADifferentRawWatermark_ADifferentDatabase_AndACycleThatDidNotClamp_AreNotSilenced()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        Assert.False(reports.AlreadyReported(1, "Sales", Raw.AddMinutes(1), T0.AddHours(-1)));
        Assert.False(reports.AlreadyReported(1, "Orders", Raw, T0.AddHours(-1)));
        Assert.False(reports.AlreadyReported(2, "Sales", Raw, T0.AddHours(-1)));

        reports.Land(1, "Sales", null, T0.AddMinutes(5));
        Assert.False(reports.AlreadyReported(1, "Sales", Raw, T0.AddHours(-1)));
    }

    [Fact]
    public void ANewGapAfterALandedCycle_OpensItsHoleAtWhatTheCycleRead_NotAtTheRawWatermark()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        /* Nothing ran for two days. The range from the raw watermark to T0 was reported and read by the landed cycle, so the new
           hole starts at T0. Starting it at the raw watermark made it overlap the earlier hole and re-walk what was filled (#9352). */
        Assert.Equal(T0, reports.HoleFloor(1, "Sales", Raw));
    }

    [Fact]
    public void ALookBackClamp_ThatIsANewGap_OpensItsHoleTheLookBackBeforeWhatTheCycleRead_NotBeforeTheRawWatermark()
    {
        var reports = new QueryStoreClampReports();
        var lookBack = TimeSpan.FromMinutes(20);
        var cutoff = QueryStoreReplicaRouting.ComputeCutoff(Raw, T0 + WatermarkPolicy.MaxCatchup + TimeSpan.FromHours(1), lookBack);
        Assert.Equal(Raw - lookBack, cutoff.HoleFrom);

        /* Nothing landed yet: the look-back reads start at the raw watermark minus the look-back, as the cutoff says. */
        Assert.Equal(Raw - lookBack, reports.HoleFrom(1, "Sales", Raw, cutoff));

        /* A landed cycle read through T0, so the new gap starts there. Handing the cutoff's own HoleFrom to HoleFloor never matched the
           landed raw watermark, so a look-back clamp kept opening its hole at Raw - lookBack and overlapping the earlier one (#9352). */
        reports.Land(1, "Sales", Raw, T0);
        Assert.Equal(T0 - lookBack, reports.HoleFrom(1, "Sales", Raw, cutoff));

        /* No look-back on the read: the same answer HoleFloor gives. */
        var plain = QueryStoreReplicaRouting.ComputeCutoff(Raw, T0 + WatermarkPolicy.MaxCatchup + TimeSpan.FromHours(1), TimeSpan.Zero);
        Assert.Equal(T0, reports.HoleFrom(1, "Sales", Raw, plain));
    }

    [Fact]
    public void ACycleThatRanOnTime_OnAnIdleDatabase_LeavesNothingToRecord()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        /* The next cycle, on time: its floor is inside what the landed cycle read, so it is already reported and opens no hole. */
        var next = T0.AddMinutes(5);
        var floor = next - WatermarkPolicy.MaxCatchup;
        Assert.True(reports.AlreadyReported(1, "Sales", Raw, floor));
        Assert.True(reports.HoleFloor(1, "Sales", Raw) >= floor);
    }

    [Fact]
    public void AnUnknownDatabase_AnotherRawWatermark_OrANonClampingCycle_OpenTheirHoleAtTheRawWatermark()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        Assert.Equal(Raw, reports.HoleFloor(1, "Orders", Raw));
        Assert.Equal(Raw, reports.HoleFloor(2, "Sales", Raw));
        Assert.Equal(Raw.AddMinutes(1), reports.HoleFloor(1, "Sales", Raw.AddMinutes(1)));

        reports.Land(1, "Sales", null, T0.AddMinutes(5));
        Assert.Equal(Raw, reports.HoleFloor(1, "Sales", Raw));
    }

    /* ---- #9359: the entry survives a restart ---- */

    private static Dictionary<string, string> StoredBy(QueryStoreLandedChange? change) =>
        change is { Value: not null } write
            ? new Dictionary<string, string> { [write.Key] = write.Value }
            : new Dictionary<string, string>();

    [Fact]
    public void ARestartedHost_SeededFromTheStoredEntry_OpensNoHoleForAnIdleDatabaseOnTime()
    {
        var before = new QueryStoreClampReports();
        var stored = StoredBy(before.Land(1, "Sales", Raw, T0));

        /* The process restarts: a fresh instance knows nothing until it is seeded from the rows the first one stored. */
        var after = new QueryStoreClampReports();
        Assert.False(after.AlreadyReported(1, "Sales", Raw, T0.AddMinutes(5) - WatermarkPolicy.MaxCatchup));
        Assert.False(after.IsSeeded(1));

        after.Seed(1, stored);

        var next = T0.AddMinutes(5);
        Assert.True(after.IsSeeded(1));
        Assert.True(after.AlreadyReported(1, "Sales", Raw, next - WatermarkPolicy.MaxCatchup));
    }

    [Fact]
    public void ARestartedHost_SeededFromTheStoredEntry_OpensARealGapAtWhatTheLastCycleRead()
    {
        var before = new QueryStoreClampReports();
        var stored = StoredBy(before.Land(1, "Sales", Raw, T0));

        var after = new QueryStoreClampReports();
        after.Seed(1, stored);

        /* The service was down for two days: the new floor lies past what the last cycle read, so the gap is real and its hole
           starts at the stored read-through, not at the raw watermark. */
        var later = T0.AddDays(2);
        Assert.False(after.AlreadyReported(1, "Sales", Raw, later - WatermarkPolicy.MaxCatchup));
        Assert.Equal(T0, after.HoleFloor(1, "Sales", Raw));
    }

    [Fact]
    public void AClampedCycle_WritesItsEntryEachTime_WithTheReadThroughMovingForward()
    {
        var reports = new QueryStoreClampReports();

        var first = reports.Land(1, "Sales", Raw, T0);
        var second = reports.Land(1, "Sales", Raw, T0.AddMinutes(1));

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal(first!.Value.Key, second!.Value.Key);
        Assert.True(QueryStoreBackfillState.TryDecodeLanded(second.Value.Value, out var raw, out var readThrough));
        Assert.Equal(Raw, raw);
        Assert.Equal(T0.AddMinutes(1), readThrough);
    }

    [Fact]
    public void ACycleThatStopsClamping_DeletesTheStoredEntry_ThenWritesNothingOnLaterCyclesOnceItIsConfirmed()
    {
        var reports = new QueryStoreClampReports();
        Assert.NotNull(reports.Land(1, "Sales", Raw, T0));

        var firstFresh = reports.Land(1, "Sales", null, T0.AddMinutes(1));
        Assert.NotNull(firstFresh);
        Assert.Null(firstFresh!.Value.Value);
        reports.Confirm(1, [firstFresh.Value]);

        Assert.Null(reports.Land(1, "Sales", null, T0.AddMinutes(2)));
        Assert.Null(reports.Land(1, "Sales", null, T0.AddMinutes(3)));
    }

    [Fact]
    public void ADeleteThatWasNeverConfirmed_IsReturnedAgainOnEveryCycleUntilItIs()
    {
        var reports = new QueryStoreClampReports();
        Assert.NotNull(reports.Land(1, "Sales", Raw, T0));

        /* The write failed (writes are best effort), so nobody confirmed it: the row is still in the store and must be retired. */
        var failed = reports.Land(1, "Sales", null, T0.AddMinutes(1));
        var again = reports.Land(1, "Sales", null, T0.AddMinutes(2));
        Assert.NotNull(failed);
        Assert.Null(failed!.Value.Value);
        Assert.NotNull(again);
        Assert.Equal(failed.Value.Key, again!.Value.Key);

        reports.Confirm(1, [again.Value]);
        Assert.Null(reports.Land(1, "Sales", null, T0.AddMinutes(3)));
    }

    [Fact]
    public void ADatabaseThatClampsAgain_KeepsItsStoredMark_WhateverBecomesOfAnEarlierDelete()
    {
        var reports = new QueryStoreClampReports();
        Assert.NotNull(reports.Land(1, "Sales", Raw, T0));
        var delete = reports.Land(1, "Sales", null, T0.AddMinutes(1));
        Assert.NotNull(delete);

        /* The database clamps again, so its upsert replaced the row. A late confirmation of the older delete must not forget it. */
        Assert.NotNull(reports.Land(1, "Sales", Raw, T0.AddMinutes(2)));
        reports.Confirm(1, [delete!.Value]);

        var retired = reports.Land(1, "Sales", null, T0.AddMinutes(3));
        Assert.NotNull(retired);
        Assert.Null(retired!.Value.Value);
    }

    [Fact]
    public void ADatabaseThatNeverClamps_IsNeverWrittenOrDeleted()
    {
        var reports = new QueryStoreClampReports();

        Assert.Null(reports.Land(1, "Sales", null, T0));
        Assert.Null(reports.Land(1, "Sales", null, T0.AddMinutes(1)));
    }

    [Fact]
    public void AnEntryReadFromTheStore_IsDeletedByTheFirstCycleThatNoLongerClamps()
    {
        var before = new QueryStoreClampReports();
        var stored = StoredBy(before.Land(1, "Sales", Raw, T0));

        /* After a restart this process never wrote the key, but the store holds it: the first fresh cycle must still retire it. */
        var after = new QueryStoreClampReports();
        after.Seed(1, stored);

        var retired = after.Land(1, "Sales", null, T0.AddHours(1));
        Assert.NotNull(retired);
        Assert.Null(retired!.Value.Value);
        after.Confirm(1, [retired.Value]);
        Assert.Null(after.Land(1, "Sales", null, T0.AddHours(2)));
    }

    [Fact]
    public void AMalformedStoredEntry_IsNoEntry_AndAnEntryOfAnotherServerIsNotUsed()
    {
        var reports = new QueryStoreClampReports();
        reports.Seed(1, new Dictionary<string, string>
        {
            [QueryStoreBackfillState.LandedKeyPrefix + "Sales"] = "not|a|time",
            [QueryStoreBackfillState.LandedKeyPrefix + "Orders"] = QueryStoreBackfillState.EncodeLanded(Raw, T0),
            [QueryStoreBackfillState.HoleKeyPrefix + "Sales"] = QueryStoreBackfillState.EncodeHole(Raw, T0),
        });

        Assert.True(reports.IsSeeded(1));
        Assert.False(reports.AlreadyReported(1, "Sales", Raw, T0.AddHours(-1)));
        Assert.True(reports.AlreadyReported(1, "Orders", Raw, T0.AddHours(-1)));
        Assert.False(reports.AlreadyReported(2, "Orders", Raw, T0.AddHours(-1)));
        Assert.False(reports.IsSeeded(2));
    }
}
