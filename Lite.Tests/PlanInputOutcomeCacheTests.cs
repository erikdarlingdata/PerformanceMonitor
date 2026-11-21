/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9195: the judged-plan-pair cache's own rules: its key, its cap, its lifetime and what it refuses to remember. How the
/// plan check uses it is covered in <c>PlanRegressionInputsTests</c>.
/// </summary>
public sealed class PlanInputOutcomeCacheTests
{
    private sealed class ManualTime : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }

    private static PlanInputOutcomeCache.Key Pair(long latest, long best = 1, int serverId = 7, string database = "Db") =>
        new(serverId, database, latest, best);

    [Fact]
    public void APairIsAnsweredOnlyForTheSameServerDatabaseAndBothPlanIds()
    {
        var cache = new PlanInputOutcomeCache();
        cache.Store(Pair(2), PlanInputOutcome.DropValuesOnly);

        Assert.True(cache.TryGet(Pair(2), out var held));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, held);
        Assert.False(cache.TryGet(Pair(2, serverId: 8), out _));
        Assert.False(cache.TryGet(Pair(2, database: "Other"), out _));
        Assert.False(cache.TryGet(Pair(3), out _));
        Assert.False(cache.TryGet(Pair(2, best: 4), out _));
    }

    [Theory]
    [InlineData(PlanInputOutcome.KeepUnverified)]
    [InlineData(PlanInputOutcome.NeedsPlans)]
    public void AnOutcomeThatSaysNothingWasDecided_IsNeverStored(PlanInputOutcome outcome)
    {
        var cache = new PlanInputOutcomeCache();
        cache.Store(Pair(2), outcome);

        Assert.Equal(0, cache.Count);
        Assert.False(cache.TryGet(Pair(2), out _));
    }

    [Fact]
    public void AnUncomparablePair_IsAnsweredUnverifiedForTheLifetime_AndExpires()
    {
        var time = new ManualTime();
        var cache = new PlanInputOutcomeCache(time);
        cache.StoreUncomparable(Pair(2));

        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryGet(Pair(2), out var held));
        Assert.Equal(PlanInputOutcome.KeepUnverified, held);
        Assert.False(cache.TryGet(Pair(3), out _));

        time.Advance(PlanInputOutcomeCache.Lifetime - TimeSpan.FromMinutes(1));
        Assert.True(cache.TryGet(Pair(2), out _));

        time.Advance(TimeSpan.FromMinutes(2));
        Assert.False(cache.TryGet(Pair(2), out _));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void UncomparablePairs_ShareTheCapWithTheOutcomes_AndTheOldestIsEvicted()
    {
        var cache = new PlanInputOutcomeCache();
        cache.Store(Pair(100), PlanInputOutcome.DropValuesOnly);
        for (var plan = 1; plan <= PlanInputOutcomeCache.MaxEntries; plan++)
            cache.StoreUncomparable(Pair(100 + plan));

        Assert.Equal(PlanInputOutcomeCache.MaxEntries, cache.Count);
        Assert.False(cache.TryGet(Pair(100), out _));
        Assert.True(cache.TryGet(Pair(100 + PlanInputOutcomeCache.MaxEntries), out _));
    }

    [Fact]
    public void PastTheCap_TheOldestEntryIsEvicted_AndTheRestStay()
    {
        var cache = new PlanInputOutcomeCache();
        for (var plan = 0; plan <= PlanInputOutcomeCache.MaxEntries; plan++)
            cache.Store(Pair(100 + plan), PlanInputOutcome.KeepSameValues);

        Assert.Equal(PlanInputOutcomeCache.MaxEntries, cache.Count);
        Assert.False(cache.TryGet(Pair(100), out _));
        Assert.True(cache.TryGet(Pair(101), out _));
        Assert.True(cache.TryGet(Pair(100 + PlanInputOutcomeCache.MaxEntries), out _));
    }

    [Fact]
    public void AStoreThatReplacesAnEntry_MakesItTheNewest_NotAnExtraEntry()
    {
        var cache = new PlanInputOutcomeCache();
        for (var plan = 0; plan < PlanInputOutcomeCache.MaxEntries; plan++)
            cache.Store(Pair(100 + plan), PlanInputOutcome.KeepSameValues);

        cache.Store(Pair(100), PlanInputOutcome.DropValuesOnly);
        cache.Store(Pair(5000), PlanInputOutcome.KeepSameValues);

        Assert.Equal(PlanInputOutcomeCache.MaxEntries, cache.Count);
        Assert.True(cache.TryGet(Pair(100), out var held));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, held);
        Assert.False(cache.TryGet(Pair(101), out _));
    }

    [Fact]
    public void AnEntryExpires_SoAReusedPlanIdIsJudgedAgain()
    {
        var time = new ManualTime();
        var cache = new PlanInputOutcomeCache(time);
        cache.Store(Pair(2), PlanInputOutcome.DropValuesOnly);

        time.Advance(PlanInputOutcomeCache.Lifetime - TimeSpan.FromMinutes(1));
        Assert.True(cache.TryGet(Pair(2), out _));

        time.Advance(TimeSpan.FromMinutes(2));
        Assert.False(cache.TryGet(Pair(2), out _));
        Assert.Equal(0, cache.Count);
    }

    private static PlanInputOutcomeCache.PlanKey Plan(long id, int serverId = 7, string database = "Db") => new(serverId, database, id);

    [Fact]
    public void ASkippedPlan_IsSkippedOnlyForTheSameServerDatabaseAndPlanId_AndIsNotAnOutcome()
    {
        var cache = new PlanInputOutcomeCache();
        cache.Skip(Plan(2));

        Assert.True(cache.IsSkipped(Plan(2)));
        Assert.False(cache.IsSkipped(Plan(2, serverId: 8)));
        Assert.False(cache.IsSkipped(Plan(2, database: "Other")));
        Assert.False(cache.IsSkipped(Plan(3)));
        Assert.False(cache.IsSkipped(Pair(2)));
        Assert.False(cache.TryGet(Pair(2), out _));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void ASkippedPair_IsSkippedOnlyForTheSameServerDatabaseAndBothPlanIds()
    {
        var cache = new PlanInputOutcomeCache();
        cache.Skip(Pair(2));

        Assert.True(cache.IsSkipped(Pair(2)));
        Assert.False(cache.IsSkipped(Pair(2, serverId: 8)));
        Assert.False(cache.IsSkipped(Pair(2, database: "Other")));
        Assert.False(cache.IsSkipped(Pair(3)));
        Assert.False(cache.IsSkipped(Pair(2, best: 4)));
        Assert.False(cache.IsSkipped(Plan(2)));
    }

    [Fact]
    public void ASkipEntry_ExpiresAfterTheSkipLifetime_NotTheOutcomeLifetime()
    {
        var time = new ManualTime();
        var cache = new PlanInputOutcomeCache(time);
        cache.Skip(Plan(2));
        cache.Skip(Pair(2));

        time.Advance(PlanInputOutcomeCache.SkipLifetime - TimeSpan.FromMinutes(1));
        Assert.True(cache.IsSkipped(Plan(2)));
        Assert.True(cache.IsSkipped(Pair(2)));

        time.Advance(TimeSpan.FromMinutes(2));
        Assert.False(cache.IsSkipped(Plan(2)));
        Assert.False(cache.IsSkipped(Pair(2)));
        Assert.Equal(0, cache.SkipCount);
    }

    [Fact]
    public void PastTheCap_TheOldestSkipEntryOfEachKindIsEvicted_AndTheRestStay()
    {
        var cache = new PlanInputOutcomeCache();
        for (var plan = 0; plan <= PlanInputOutcomeCache.MaxSkipEntries; plan++)
        {
            cache.Skip(Plan(100 + plan));
            cache.Skip(Pair(100 + plan));
        }

        Assert.Equal(2 * PlanInputOutcomeCache.MaxSkipEntries, cache.SkipCount);
        Assert.False(cache.IsSkipped(Plan(100)));
        Assert.False(cache.IsSkipped(Pair(100)));
        Assert.True(cache.IsSkipped(Plan(101)));
        Assert.True(cache.IsSkipped(Pair(101)));
        Assert.True(cache.IsSkipped(Plan(100 + PlanInputOutcomeCache.MaxSkipEntries)));
        Assert.True(cache.IsSkipped(Pair(100 + PlanInputOutcomeCache.MaxSkipEntries)));
    }

    [Fact]
    public void ASkipThatIsRepeated_MakesItTheNewest_NotAnExtraEntry()
    {
        var cache = new PlanInputOutcomeCache();
        for (var plan = 0; plan < PlanInputOutcomeCache.MaxSkipEntries; plan++)
            cache.Skip(Plan(100 + plan));

        cache.Skip(Plan(100));
        cache.Skip(Plan(5000));

        Assert.Equal(PlanInputOutcomeCache.MaxSkipEntries, cache.SkipCount);
        Assert.True(cache.IsSkipped(Plan(100)));
        Assert.False(cache.IsSkipped(Plan(101)));
    }
}
