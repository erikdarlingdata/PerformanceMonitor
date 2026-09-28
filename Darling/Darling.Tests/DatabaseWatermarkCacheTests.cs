/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Unit pins for <see cref="DatabaseWatermarkCache"/>'s hit rule and the staging helper that feeds it (#4661).
/// </summary>
public sealed class DatabaseWatermarkCacheTests
{
    private static readonly DateTime Floor0 = new(2026, 9, 28, 10, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Now = Floor0.AddHours(3);
    private const int S = 7;
    private const string Db = "db1";

    /// <summary>
    /// The cache's exactness needs the witness row to outlive the read floor. Retention purges whole days and
    /// never fewer than the store's minimum, so that minimum must exceed the widest read floor plus the re-seed
    /// interval. A retention setting in hours would break this test instead of the data.
    /// </summary>
    [Fact]
    public void TheSmallestRetentionTheStoreCanApply_OutlivesTheReadFloorAndTheReseedInterval()
    {
        var smallest = TimeSpan.FromDays(DarlingRetention.EffectivePurgeRetentionDays(QueryStoreCollector.Instance.Name, 0));
        Assert.True(
            smallest > WatermarkPolicy.MaxCatchup + WatermarkPolicy.ReadFloorMargin + DatabaseWatermarkCache.ReseedInterval,
            "retention can drop a witness row inside the read floor");
    }

    [Fact]
    public void EmptyCache_Misses()
    {
        var c = new DatabaseWatermarkCache();
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));
    }

    [Fact]
    public void NullSeed_HitsNull_AtAnyLaterFloor_AndAfterZeroRowAdvance()
    {
        var c = new DatabaseWatermarkCache();
        c.Seed(S, Db, null, Floor0, Now, c.TokenFor(S, Db));

        Assert.True(c.TryGet(S, Db, Floor0.AddHours(5), Now, out var v));
        Assert.Null(v);

        c.Advance(S, Db, null, Floor0.AddMinutes(5));
        Assert.True(c.TryGet(S, Db, Floor0.AddHours(6), Now, out v));
        Assert.Null(v);
    }

    [Fact]
    public void NonNullSeed_MissesAtTheNextFloor_BecauseTheWitnessIsUnknown()
    {
        var c = new DatabaseWatermarkCache();
        c.Seed(S, Db, Floor0.AddHours(1), Floor0, Now, c.TokenFor(S, Db));
        Assert.False(c.TryGet(S, Db, Floor0.AddMinutes(1), Now, out _));
    }

    [Fact]
    public void AdvanceOnAMiss_IsANoOp_AndRejectsASeedTakenBefore()
    {
        var c = new DatabaseWatermarkCache();
        var before = c.TokenFor(S, Db);
        c.Advance(S, Db, Floor0.AddHours(1), Floor0.AddMinutes(1));
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));

        c.Seed(S, Db, null, Floor0, Now, before);
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));
    }

    [Fact]
    public void GreaterAdvance_SetsWitness_HitsWhileFloorIsBelowIt_AndMissesAtOrAbove()
    {
        var c = new DatabaseWatermarkCache();
        c.Seed(S, Db, null, Floor0, Now, c.TokenFor(S, Db));
        var ct = Floor0.AddHours(1);
        var m = Floor0.AddMinutes(50);
        c.Advance(S, Db, m, ct);

        Assert.True(c.TryGet(S, Db, ct.AddTicks(-10), Now, out var v));
        Assert.Equal(m, v);
        Assert.False(c.TryGet(S, Db, ct, Now, out _));
        Assert.False(c.TryGet(S, Db, ct.AddSeconds(1), Now, out _));
    }

    [Fact]
    public void LowerAdvance_ChangesNothing_AndATieMovesTheWitnessForward()
    {
        var c = new DatabaseWatermarkCache();
        c.Seed(S, Db, null, Floor0, Now, c.TokenFor(S, Db));
        var ct = Floor0.AddHours(1);
        var m = Floor0.AddMinutes(50);
        c.Advance(S, Db, m, ct);

        c.Advance(S, Db, m.AddMinutes(-1), ct.AddHours(1));
        Assert.False(c.TryGet(S, Db, ct.AddMinutes(1), Now, out _));
        Assert.True(c.TryGet(S, Db, ct.AddTicks(-10), Now, out var v));
        Assert.Equal(m, v);

        c.Advance(S, Db, m, ct.AddHours(1));
        Assert.True(c.TryGet(S, Db, ct.AddMinutes(1), Now, out v));
        Assert.Equal(m, v);
    }

    [Fact]
    public void Invalidate_AndInvalidateServer_DropEntriesAndDiscardAnInFlightSeed()
    {
        var c = new DatabaseWatermarkCache();
        c.Seed(S, Db, null, Floor0, Now, c.TokenFor(S, Db));
        c.Seed(S, "db2", null, Floor0, Now, c.TokenFor(S, "db2"));
        c.Seed(S + 1, Db, null, Floor0, Now, c.TokenFor(S + 1, Db));

        var inFlight = c.TokenFor(S, Db);
        c.Invalidate(S, Db);
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));
        Assert.True(c.TryGet(S, "db2", Floor0, Now, out _));
        c.Seed(S, Db, null, Floor0, Now, inFlight);
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));

        inFlight = c.TokenFor(S, Db);
        c.InvalidateServer(S);
        Assert.False(c.TryGet(S, "db2", Floor0, Now, out _));
        Assert.True(c.TryGet(S + 1, Db, Floor0, Now, out _));
        c.Seed(S, Db, null, Floor0, Now, inFlight);
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));
    }

    [Fact]
    public void ASeedOnOneKey_SurvivesEveryBumpOnAnotherServer()
    {
        var c = new DatabaseWatermarkCache();
        var token = c.TokenFor(S, Db);
        c.Invalidate(S + 1, Db);
        c.Advance(S + 1, Db, Floor0.AddHours(1), Floor0.AddHours(1));
        c.InvalidateServer(S + 1);
        c.Seed(S, Db, null, Floor0, Now, token);
        Assert.True(c.TryGet(S, Db, Floor0, Now, out _));
    }

    [Fact]
    public void ASeedOnOneKey_SurvivesEveryBumpOnAnotherDatabaseOfTheSameServer()
    {
        var c = new DatabaseWatermarkCache();
        var token = c.TokenFor(S, Db);
        c.Invalidate(S, "db2");
        c.Advance(S, "db2", Floor0.AddHours(1), Floor0.AddHours(1));
        c.Seed(S, Db, null, Floor0, Now, token);
        Assert.True(c.TryGet(S, Db, Floor0, Now, out _));
    }

    [Fact]
    public void ABumpOnTheSameKey_BetweenCaptureAndSeed_RejectsTheSeed()
    {
        var c = new DatabaseWatermarkCache();
        var token = c.TokenFor(S, Db);
        c.Invalidate(S, Db);
        c.Seed(S, Db, null, Floor0, Now, token);
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));
    }

    [Fact]
    public void InvalidateServer_BetweenCaptureAndSeed_RejectsTheSeed()
    {
        var c = new DatabaseWatermarkCache();
        var token = c.TokenFor(S, Db);
        c.InvalidateServer(S);
        c.Seed(S, Db, null, Floor0, Now, token);
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));
    }

    [Fact]
    public void ARejectedSeed_FollowedByAnAdvanceOnThatKey_DoesNotRejectAnUnrelatedKeysSeed()
    {
        var c = new DatabaseWatermarkCache();
        var tokenA = c.TokenFor(S, Db);
        var tokenB = c.TokenFor(S + 1, Db);
        c.Invalidate(S, Db);
        c.Seed(S, Db, null, Floor0, Now, tokenA);
        c.Advance(S, Db, Floor0.AddHours(1), Floor0.AddHours(1));
        c.Seed(S + 1, Db, null, Floor0, Now, tokenB);
        Assert.False(c.TryGet(S, Db, Floor0, Now, out _));
        Assert.True(c.TryGet(S + 1, Db, Floor0, Now, out _));
    }

    [Fact]
    public void AnEntry_MissesOnceSeededAnHourAgo_EvenAfterAnAdvance()
    {
        var c = new DatabaseWatermarkCache();
        var seededAt = Floor0.AddHours(3);
        c.Seed(S, Db, null, Floor0, seededAt, c.TokenFor(S, Db));
        var ct = seededAt.AddMinutes(1);
        var m = ct.AddMinutes(-2);
        c.Advance(S, Db, m, ct);

        var floor = ct.AddMinutes(-3);
        Assert.True(c.TryGet(S, Db, floor, seededAt.AddMinutes(59), out var v));
        Assert.Equal(m, v);
        Assert.False(c.TryGet(S, Db, floor, seededAt.AddMinutes(60), out _));
    }

    [Fact]
    public void FloorBelowSeedFloor_Misses()
    {
        var c = new DatabaseWatermarkCache();
        c.Seed(S, Db, null, Floor0, Now, c.TokenFor(S, Db));
        Assert.False(c.TryGet(S, Db, Floor0.AddTicks(-10), Now, out _));
    }

    [Fact]
    public void Value_IsMicrosecondTruncated_AndKindUnspecified()
    {
        var c = new DatabaseWatermarkCache();
        c.Seed(S, Db, null, Floor0, Now, c.TokenFor(S, Db));
        var raw = DateTime.SpecifyKind(Floor0.AddMinutes(5).AddTicks(1234567 % 10 + 3), DateTimeKind.Utc);
        c.Advance(S, Db, raw, Floor0.AddHours(1));

        Assert.True(c.TryGet(S, Db, Floor0.AddMinutes(1), Now, out var v));
        Assert.Equal(DateTimeKind.Unspecified, v!.Value.Kind);
        Assert.Equal(0, v.Value.Ticks % TimeSpan.TicksPerMicrosecond);
        Assert.Equal(raw.Ticks - raw.Ticks % TimeSpan.TicksPerMicrosecond, v.Value.Ticks);
    }

    private static QueryStoreCollector.Row Row(string db, DateTime? last) => new() { DatabaseName = db, LastExecutionTime = last };

    [Fact]
    public void Stage_TakesTheMax_IgnoresNullValues_AndFlagsForeignRows()
    {
        var t = Floor0.AddMinutes(3);
        var staged = DarlingCollectorRunner.StageQueryStoreDatabaseWatermark(
            new List<QueryStoreCollector.Row> { Row(Db, t), Row(Db, null), Row(Db, t.AddMinutes(-1)) }, Db, Floor0);
        Assert.False(staged.Foreign);
        Assert.Equal(t, staged.BatchMax);

        var foreign = DarlingCollectorRunner.StageQueryStoreDatabaseWatermark(
            new List<QueryStoreCollector.Row> { Row("other", t) }, Db, Floor0);
        Assert.True(foreign.Foreign);

        var nonQs = DarlingCollectorRunner.StageQueryStoreDatabaseWatermark(new List<string>(), Db, Floor0);
        Assert.True(nonQs.Foreign);
    }
}
