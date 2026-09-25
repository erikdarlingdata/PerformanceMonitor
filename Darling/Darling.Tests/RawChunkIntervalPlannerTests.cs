/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>Unit tests for <see cref="RawChunkIntervalPlanner"/> (#4211) — pure inputs, no database, no
/// rig.</summary>
public sealed class RawChunkIntervalPlannerTests
{
    private static readonly DateTime AsOf = new(2026, 9, 25, 0, 0, 0, DateTimeKind.Utc);

    private const double GiB = 1024.0 * 1024.0 * 1024.0;
    private const double GB = 1_000_000_000.0;

    [Fact]
    public void Ladder_IsTwentyFourTwelveSix_ReadFromChunkIntervalDays()
    {
        /* The top rung is read from TimescaleSupport.ChunkIntervalDays (the #4211 ruling's "ladder's
           ceiling"), not a second literal 24, so the two can never silently disagree. */
        Assert.Equal(new[] { 24, 12, 6 }, RawChunkIntervalPlanner.RungHours);
        Assert.Equal(TimescaleSupport.ChunkIntervalDays * 24, RawChunkIntervalPlanner.RungHours[0]);
        Assert.Equal(6, RawChunkIntervalPlanner.FloorHours);
    }

    [Fact]
    public void SmallStore_NothingMoves()
    {
        /* A handful of monitored servers: every table's 24-hour open chunk fits comfortably under an 8 GB
           budget, so nothing narrows and nothing widens (everything is already at the ceiling). */
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("wait_stats", 5 * GB / 24, 24, null),
            new RawChunkIntervalPlanner.TableInput("query_stats", 3 * GB / 24, 24, null),
            new RawChunkIntervalPlanner.TableInput("deadlocks", 0.01 * GB, 24, null),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 8 * GiB, currentTotalChunkCount: 30, AsOf);

        Assert.All(decisions, d =>
        {
            Assert.Equal(24, d.TargetIntervalHours);
            Assert.False(d.Changes);
            Assert.Contains("fits within budget", d.Reason, StringComparison.Ordinal);
        });
    }

    /// <summary>Store A's rates from the #4211 design (issuecomment-5827299104): total ingest 2.2 GB/h, of
    /// which 1.62 GB/h sits in the three query tables, modeled here as query_stats (0.9), other_raw — standing
    /// in for every other raw hypertable in the store, since H3 requires the budget to count them too — (0.58),
    /// query_snapshots (0.5) and wait_stats (0.22). Both Facts start from "day 2": every table already
    /// narrowed once to 12 h (the most one day's reconcile could do from a 24-hour start against either
    /// budget below) and is eligible to move again, so the two host sizes can diverge on THIS call instead of
    /// both spending it on the same first rung.</summary>
    private static RawChunkIntervalPlanner.TableInput[] StoreADay2Tables() =>
        new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 0.9 * GB, 12, AsOf.AddDays(-2)),
            new RawChunkIntervalPlanner.TableInput("other_raw", 0.58 * GB, 12, AsOf.AddDays(-2)),
            new RawChunkIntervalPlanner.TableInput("query_snapshots", 0.5 * GB, 12, AsOf.AddDays(-2)),
            new RawChunkIntervalPlanner.TableInput("wait_stats", 0.22 * GB, 12, AsOf.AddDays(-2)),
        };

    [Fact]
    public void StoreA_SixtyThreeGiBHost_NarrowsTheThreeHeaviestAndHoldsTheLightestAtTwelve()
    {
        var budget = RawChunkIntervalPlanner.ManagedBudgetBytes((long)(63 * GiB));
        var decisions = RawChunkIntervalPlanner.Plan(StoreADay2Tables(), budget, currentTotalChunkCount: 100, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        /* Heaviest-rate-first narrowing closes the gap after three of the four tables move — the budget is
           satisfied before wait_stats, the lightest, is reached. */
        Assert.Equal(6, decisions["query_stats"].TargetIntervalHours);
        Assert.Equal(6, decisions["other_raw"].TargetIntervalHours);
        Assert.Equal(6, decisions["query_snapshots"].TargetIntervalHours);
        Assert.Equal(12, decisions["wait_stats"].TargetIntervalHours);
        Assert.False(decisions["wait_stats"].Changes);

        var finalTotal = 6 * 0.9 * GB + 6 * 0.58 * GB + 6 * 0.5 * GB + 12 * 0.22 * GB;
        Assert.True(finalTotal <= budget, $"final open-chunk bytes {finalTotal:N0} must fit under budget {budget:N0}");
    }

    [Fact]
    public void StoreA_ThirtyOnePointFiveGiBHost_EveryTableReachesTheFloorAndStillDoesNotFit()
    {
        var budget = RawChunkIntervalPlanner.ManagedBudgetBytes((long)(31.5 * GiB));
        var decisions = RawChunkIntervalPlanner.Plan(StoreADay2Tables(), budget, currentTotalChunkCount: 100, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        /* The smaller host's budget is too small for this rate even at the 6-hour floor — every table narrows
           all the way down, and the planner stops there (there is no rung left to try), leaving the store over
           budget. That is the honest outcome, not a bug: review finding H3 on #4211 made exactly this point
           about a host this size. */
        Assert.All(decisions.Values, d => Assert.Equal(6, d.TargetIntervalHours));

        var finalTotal = 6 * (0.9 + 0.58 + 0.5 + 0.22) * GB;
        Assert.True(finalTotal > budget, $"expected the floor to still exceed budget {budget:N0}, got {finalTotal:N0}");
    }

    [Fact]
    public void OneRungPerDay_RecentlyMovedTableIsHeldEvenWhenOverBudget()
    {
        var tables = new[]
        {
            /* Way over any reasonable budget, but changed under an hour ago. */
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 24, AsOf.AddHours(-1)),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 1 * GB, currentTotalChunkCount: 10, AsOf);

        var decision = Assert.Single(decisions);
        Assert.False(decision.Changes);
        Assert.Contains("moved within the last", decision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void OneRungPerDay_TableLastChangedExactlyOneDayAgoIsEligible()
    {
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 24, AsOf.AddDays(-1)),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 1 * GB, currentTotalChunkCount: 10, AsOf);

        var decision = Assert.Single(decisions);
        Assert.True(decision.Changes);
        Assert.Equal(12, decision.TargetIntervalHours);
    }

    [Fact]
    public void Hysteresis_WidensOnlyWhenUnderHalfItsShare()
    {
        /* Two tables, each already at 6 h from a busier past. lightTable's rate has since fallen far enough
           that even at the wider 12-hour rung it would sit under half its equal share of a generous budget;
           closeTable's rate is still just over that half-share line, so it must stay put — proving the
           hysteresis GAP (half, not all, of the share) actually gates something. */
        var budget = 24.0 * GB; // two tables => share = 12 GB each, half-share = 6 GB
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("light_table", 0.1 * GB, 6, AsOf.AddDays(-3)),
            new RawChunkIntervalPlanner.TableInput("close_table", 0.51 * GB, 6, AsOf.AddDays(-3)), // *12h = 6.12 GB, just over the 6 GB half-share
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budget, currentTotalChunkCount: 10, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.Equal(12, decisions["light_table"].TargetIntervalHours);
        Assert.True(decisions["light_table"].Changes);
        Assert.Contains("moved up", decisions["light_table"].Reason, StringComparison.Ordinal);

        Assert.Equal(6, decisions["close_table"].TargetIntervalHours);
        Assert.False(decisions["close_table"].Changes);
        Assert.Contains("not under half its", decisions["close_table"].Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Hysteresis_NeverWidensPastTheCeiling()
    {
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("idle_table", 0.001 * GB, 24, AsOf.AddDays(-10)),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 100 * GB, currentTotalChunkCount: 10, AsOf);

        var decision = Assert.Single(decisions);
        Assert.Equal(24, decision.TargetIntervalHours);
        Assert.False(decision.Changes);
    }

    [Fact]
    public void ChunkCountCap_BlocksNarrowingEvenWhenOverBudget()
    {
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 24, null),
        };

        var atCap = RawChunkIntervalPlanner.Plan(
            tables, budgetBytes: 1 * GB, currentTotalChunkCount: RawChunkIntervalPlanner.ChunkCountCapThreshold, AsOf);
        var capped = Assert.Single(atCap);
        Assert.False(capped.Changes);
        Assert.Contains("cap", capped.Reason, StringComparison.Ordinal);

        /* Same inputs, one chunk under the cap: the move goes through. */
        var underCap = RawChunkIntervalPlanner.Plan(
            tables, budgetBytes: 1 * GB, currentTotalChunkCount: RawChunkIntervalPlanner.ChunkCountCapThreshold - 1, AsOf);
        var allowed = Assert.Single(underCap);
        Assert.True(allowed.Changes);
        Assert.Equal(12, allowed.TargetIntervalHours);
    }

    [Fact]
    public void ChunkCountCap_NeverBlocksWidening()
    {
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("idle_table", 0.001 * GB, 6, AsOf.AddDays(-5)),
        };

        var decisions = RawChunkIntervalPlanner.Plan(
            tables, budgetBytes: 100 * GB, currentTotalChunkCount: RawChunkIntervalPlanner.ChunkCountCapThreshold + 500, AsOf);

        var decision = Assert.Single(decisions);
        Assert.Equal(12, decision.TargetIntervalHours);
        Assert.True(decision.Changes);
    }

    [Fact]
    public void CurrentIntervalOutsideTheLadder_IsLeftUnchanged()
    {
        /* An adopted bring-your-own store carrying a hand-set 7-day (168-hour) interval from before #4211 —
           Plan must not guess a rung for it in either direction. */
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("legacy_table", 50 * GB, 168, null),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 1 * GB, currentTotalChunkCount: 10, AsOf);

        var decision = Assert.Single(decisions);
        Assert.Equal(168, decision.TargetIntervalHours);
        Assert.False(decision.Changes);
        Assert.Contains("outside the", decision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Plan_ThrowsOnNullTables()
    {
        Assert.Throws<ArgumentNullException>(() =>
            RawChunkIntervalPlanner.Plan(null!, budgetBytes: 1, currentTotalChunkCount: 0, AsOf));
    }

    [Fact]
    public void ManagedBudgetBytes_IsExactlyTwentyFivePercentOfTheRawRamFigure()
    {
        /* NOT DeriveMemorySettings' SharedBuffersMb output, which is capped at 1 GB (#1559) — this reads the
           same raw byte count that method takes as input, per the #4211 ruling. */
        Assert.Equal(4L * GiB, RawChunkIntervalPlanner.ManagedBudgetBytes((long)(16 * GiB)));
        Assert.Equal(16 * GiB, RawChunkIntervalPlanner.ManagedBudgetBytes((long)(64 * GiB)));
    }

    [Fact]
    public void BringYourOwnBudgetBytes_TakesTheLargerOfSharedBuffersAndAThirdOfEffectiveCacheSize()
    {
        /* Untuned store: stock 128 MB shared_buffers, stock 4 GB effective_cache_size — the 1.33 GB floor
           wins, matching review finding H3's fix (not the 128 MB shared_buffers alone, which would call
           almost everything "heavy"). */
        var untuned = RawChunkIntervalPlanner.BringYourOwnBudgetBytes(
            sharedBuffersBytes: 128 * 1024.0 * 1024.0, effectiveCacheSizeBytes: 4 * GiB);
        Assert.Equal((4 * GiB) / 3.0, untuned);

        /* Tuned store: shared_buffers itself raised well past a third of effective_cache_size. */
        var tuned = RawChunkIntervalPlanner.BringYourOwnBudgetBytes(
            sharedBuffersBytes: 20 * GiB, effectiveCacheSizeBytes: 48 * GiB);
        Assert.Equal(20 * GiB, tuned);
    }

    [Fact]
    public void Plan_CoversEveryTablesOpenChunkNotOnlyTheHeaviestOnes()
    {
        /* Finding H3: a light table's 24-hour bytes still count against the budget, so a store where only the
           SUM exceeds budget still narrows the heaviest table even though no single table looks "heavy" on
           its own. */
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("a", 0.3 * GB, 24, null),
            new RawChunkIntervalPlanner.TableInput("b", 0.3 * GB, 24, null),
            new RawChunkIntervalPlanner.TableInput("c", 0.3 * GB, 24, null),
        };

        /* 24h * 0.9 GB/h combined = 21.6 GB, over a 10 GB budget, even though each table alone is only
           7.2 GB. */
        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 10 * GB, currentTotalChunkCount: 10, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.True(decisions.Values.Any(d => d.Changes), "expected at least one table to narrow once the combined total exceeded budget");
    }
}
