/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
            new RawChunkIntervalPlanner.TableInput("wait_stats", 5 * GB / 24, 24, null, NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("query_stats", 3 * GB / 24, 24, null, NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("deadlocks", 0.01 * GB, 24, null, NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 8 * GiB, AsOf);

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
            new RawChunkIntervalPlanner.TableInput("query_stats", 0.9 * GB, 12, AsOf.AddDays(-2), NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("other_raw", 0.58 * GB, 12, AsOf.AddDays(-2), NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("query_snapshots", 0.5 * GB, 12, AsOf.AddDays(-2), NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("wait_stats", 0.22 * GB, 12, AsOf.AddDays(-2), NumChunks: 10),
        };

    [Fact]
    public void StoreA_SixtyThreeGiBHost_NarrowsTheThreeHeaviestAndHoldsTheLightestAtTwelve()
    {
        var budget = RawChunkIntervalPlanner.ManagedBudgetBytes((long)(63 * GiB));
        var decisions = RawChunkIntervalPlanner.Plan(StoreADay2Tables(), budget, AsOf)
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
        var decisions = RawChunkIntervalPlanner.Plan(StoreADay2Tables(), budget, AsOf)
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
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 24, AsOf.AddHours(-1), NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 1 * GB, AsOf);

        var decision = Assert.Single(decisions);
        Assert.False(decision.Changes);
        Assert.Contains("moved within the last", decision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void OneRungPerDay_TableLastChangedExactlyOneDayAgoIsEligible()
    {
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 24, AsOf.AddDays(-1), NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 1 * GB, AsOf);

        var decision = Assert.Single(decisions);
        Assert.True(decision.Changes);
        Assert.Equal(12, decision.TargetIntervalHours);
    }

    [Fact]
    public void Hysteresis_WidensWhenStoreTotalStaysAtOrUnderHalfBudget()
    {
        /* One table, already narrowed to 6 h. Budget is set so the store-wide total after widening lands
           EXACTLY on the B/2 line (#4211 ruling, issuecomment-5836734285: "at or under half of B") — the
           boundary is inclusive, so this must widen, not hold. */
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 1 * GB, 6, AsOf.AddDays(-3), NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 24 * GB, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.Equal(12, decisions["query_stats"].TargetIntervalHours);
        Assert.True(decisions["query_stats"].Changes);
        Assert.Contains("moved up", decisions["query_stats"].Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Hysteresis_HeldWhenStoreTotalFallsInTheBandAboveHalfBudget()
    {
        /* heavy_table sits at the ceiling and never moves — it is only there to give the store a total large
           enough that widening narrowing_table lands in the BAND above B/2 but still under B (246 GB now,
           252 GB if narrowing_table widens; half-budget is 150 GB, budget is 300 GB). Under the ruling this
           must hold, not widen. Under the old equal-share rule it would have widened: with two tables,
           shareBytes = 150 GB, halfShare = 75 GB, and narrowing_table's OWN widened bytes (12 GB) sit far
           under that — the old rule never looked at the store-wide total against half the budget, only this
           table's own bytes against its slice, so it missed that the move would leave the store deep in the
           band. Confirmed once by hand against the pre-#4211-ruling pass 2: with these inputs it set
           narrowing_table's TargetIntervalHours to 12 (Changes = true), where the ruling requires 6 (held). */
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("heavy_table", 10 * GB, 24, null, NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("narrowing_table", 1 * GB, 6, AsOf.AddDays(-3), NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 300 * GB, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.Equal(6, decisions["narrowing_table"].TargetIntervalHours);
        Assert.False(decisions["narrowing_table"].Changes);
        Assert.Contains("over half", decisions["narrowing_table"].Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Hysteresis_LighterNarrowedTableWidensBeforeHeavierWhenOnlyOneFitsUnderHalfBudget()
    {
        /* Both tables are already at 6 h. Only one widen fits under the 50 GB half-budget line, and the
           lightest-rate table gets the chance first (#4211 ruling): light_table's move (36 -> 42 GB) fits,
           so it goes first and takes the room; by the time heavy_table is considered the store is already at
           42 GB, and its own move would push the total to 72 GB, over the line, so it is held. */
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("light_table", 1 * GB, 6, AsOf.AddDays(-3), NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("heavy_table", 5 * GB, 6, AsOf.AddDays(-3), NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 100 * GB, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.Equal(12, decisions["light_table"].TargetIntervalHours);
        Assert.True(decisions["light_table"].Changes);
        Assert.Contains("moved up", decisions["light_table"].Reason, StringComparison.Ordinal);

        Assert.Equal(6, decisions["heavy_table"].TargetIntervalHours);
        Assert.False(decisions["heavy_table"].Changes);
        Assert.Contains("over half", decisions["heavy_table"].Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Hysteresis_WidenedStoreIsNotNarrowedByASecondPlanCall()
    {
        /* Day 1: the table widens from 6 h to 12 h, landing the store total exactly on the B/2 line (as in
           Hysteresis_WidensWhenStoreTotalStaysAtOrUnderHalfBudget). Day 2, one day later (eligible again):
           feed that 12-hour result back in and confirm it does not flap back down to 6 h. Pass 1 never
           considers it (12 GB already fits comfortably under the 24 GB budget), and pass 2 holds it at 12 h
           rather than widening again to 24 h (24 GB would be over the 12 GB half-budget line). */
        var dayOne = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 1 * GB, 6, AsOf.AddDays(-3), NumChunks: 10),
        };
        var dayOneDecision = Assert.Single(RawChunkIntervalPlanner.Plan(dayOne, budgetBytes: 24 * GB, AsOf));
        Assert.Equal(12, dayOneDecision.TargetIntervalHours);

        var dayTwo = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 1 * GB, dayOneDecision.TargetIntervalHours, AsOf, NumChunks: 10),
        };
        var dayTwoDecision = Assert.Single(RawChunkIntervalPlanner.Plan(dayTwo, budgetBytes: 24 * GB, AsOf.AddDays(1)));

        Assert.Equal(12, dayTwoDecision.TargetIntervalHours);
        Assert.False(dayTwoDecision.Changes);
    }

    [Fact]
    public void Hysteresis_NeverWidensPastTheCeiling()
    {
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("idle_table", 0.001 * GB, 24, AsOf.AddDays(-10), NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 100 * GB, AsOf);

        var decision = Assert.Single(decisions);
        Assert.Equal(24, decision.TargetIntervalHours);
        Assert.False(decision.Changes);
    }

    [Fact]
    public void PerTableChunkCountCap_BoundaryAtOneThousandForecastChunks()
    {
        /* 24h -> 12h halves the interval, so it doubles the forecast chunk count. NumChunks 500 forecasts
           exactly 1,000 — at the cap, so it narrows; 501 forecasts 1,002 — over the cap, so it holds, and the
           reason names the forecast count. */
        var atBoundary = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 24, null, NumChunks: 500),
        };
        var narrowed = Assert.Single(RawChunkIntervalPlanner.Plan(atBoundary, budgetBytes: 1 * GB, AsOf));
        Assert.True(narrowed.Changes);
        Assert.Equal(12, narrowed.TargetIntervalHours);

        var overBoundary = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 24, null, NumChunks: 501),
        };
        var held = Assert.Single(RawChunkIntervalPlanner.Plan(overBoundary, budgetBytes: 1 * GB, AsOf));
        Assert.False(held.Changes);
        Assert.Contains("1,002", held.Reason, StringComparison.Ordinal);
        Assert.Contains("cap", held.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void PerTableChunkCountCap_BoundaryAtTwelveToSixHours()
    {
        /* 12h -> 6h also halves the interval. NumChunks 500 forecasts 1,000 (narrows); 501 forecasts 1,002
           (held). */
        var atBoundary = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 12, AsOf.AddDays(-3), NumChunks: 500),
        };
        var narrowed = Assert.Single(RawChunkIntervalPlanner.Plan(atBoundary, budgetBytes: 1 * GB, AsOf));
        Assert.True(narrowed.Changes);
        Assert.Equal(6, narrowed.TargetIntervalHours);

        var overBoundary = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 5 * GB, 12, AsOf.AddDays(-3), NumChunks: 501),
        };
        var held = Assert.Single(RawChunkIntervalPlanner.Plan(overBoundary, budgetBytes: 1 * GB, AsOf));
        Assert.False(held.Changes);
        Assert.Contains("1,002", held.Reason, StringComparison.Ordinal);
        Assert.Contains("cap", held.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void PerTableChunkCountCap_StoreWideTotalNoLongerMatters()
    {
        /* 20 tables of 60 chunks each — 1,200 total, well past the old store-wide 1,000 cap — but every
           table's OWN forecast at 12h is only 120, far under the per-table cap. The heaviest-rate table still
           narrows: the store-wide total plays no part in the decision any more. */
        var tables = Enumerable.Range(0, 20)
            .Select(i => new RawChunkIntervalPlanner.TableInput($"table_{i:D2}", (20 - i) * GB, 24, null, NumChunks: 60))
            .ToArray();

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 1 * GB, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.True(decisions["table_00"].Changes);
        Assert.Equal(12, decisions["table_00"].TargetIntervalHours);
    }

    [Fact]
    public void PerTableChunkCountCap_NeverBlocksWidening()
    {
        var tables = new[]
        {
            new RawChunkIntervalPlanner.TableInput("idle_table", 0.001 * GB, 6, AsOf.AddDays(-5), NumChunks: 2_000),
        };

        var decisions = RawChunkIntervalPlanner.Plan(
            tables, budgetBytes: 100 * GB, AsOf);

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
            new RawChunkIntervalPlanner.TableInput("legacy_table", 50 * GB, 168, null, NumChunks: 10),
        };

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 1 * GB, AsOf);

        var decision = Assert.Single(decisions);
        Assert.Equal(168, decision.TargetIntervalHours);
        Assert.False(decision.Changes);
        Assert.Contains("outside the", decision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Plan_ThrowsOnNullTables()
    {
        Assert.Throws<ArgumentNullException>(() =>
            RawChunkIntervalPlanner.Plan(null!, budgetBytes: 1, AsOf));
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
            new RawChunkIntervalPlanner.TableInput("a", 0.3 * GB, 24, null, NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("b", 0.3 * GB, 24, null, NumChunks: 10),
            new RawChunkIntervalPlanner.TableInput("c", 0.3 * GB, 24, null, NumChunks: 10),
        };

        /* 24h * 0.9 GB/h combined = 21.6 GB, over a 10 GB budget, even though each table alone is only
           7.2 GB. */
        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes: 10 * GB, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.True(decisions.Values.Any(d => d.Changes), "expected at least one table to narrow once the combined total exceeded budget");
    }

    /// <summary>
    /// The store now PERSISTS <see cref="RawChunkIntervalPlanner.Decision.Reason"/> to
    /// <c>collect.raw_chunk_interval_rung_history.reason</c> (V144), so its <c>N0</c> formatting has to read the
    /// same on every host regardless of the OS culture, unlike a log line that is merely displayed once. Runs a
    /// narrowing <see cref="RawChunkIntervalPlanner.Plan"/> and a widening one under de-DE and asserts each
    /// changing decision's reason carries the invariant, comma-grouped text — not de-DE's dot grouping.
    /// </summary>
    [Fact]
    public void ReasonText_FormatsNumbersInvariantly_UnderACommaDecimalCulture()
    {
        using var _ = new CultureScope("de-DE");
        Assert.Equal(",", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

        var narrowing = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 1_000_000, 24, null, NumChunks: 10),
        };
        var narrowed = RawChunkIntervalPlanner.Plan(narrowing, budgetBytes: 1_000_000, AsOf).Single();
        Assert.True(narrowed.Changes);
        Assert.Equal(
            "moved to 12 h: store-wide open-chunk bytes exceeded the 1,000,000 B budget (rate-ordered, 1,000,000 B/h)",
            narrowed.Reason);

        var widening = new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 100_000, 6, AsOf.AddDays(-3), NumChunks: 10),
        };
        var widened = RawChunkIntervalPlanner.Plan(widening, budgetBytes: 2_400_000, AsOf).Single();
        Assert.True(widened.Changes);
        Assert.Equal(
            "moved up to 12 h: the store holds 1,200,000 B, under half the 2,400,000 B budget",
            widened.Reason);
    }

    private const double MB = 1024.0 * 1024.0;

    /// <summary>A production SQL Server store's raw hypertable shape (#4457): 12 named tables (rates in MB/h,
    /// chunk counts from the store's catalog) plus one <c>remaining_27_tables</c> row standing in for the
    /// other 27 raw hypertables at their combined ≈0.39 GB open total (≈16.25 MB/h) — 13 rows covering the
    /// whole 19.79 GB store, 2.55x the 7.75 GB budget B. Every row starts at the 24 h ceiling with chunk
    /// counts far under the per-table cap (job_history's 73 is the largest), so this shape exercises "which
    /// tables move" and the store's own numbers, not the cap.</summary>
    private static RawChunkIntervalPlanner.TableInput[] ProductionStoreShapeTables(int currentIntervalHours, DateTime? lastChangedUtc) =>
        new[]
        {
            new RawChunkIntervalPlanner.TableInput("query_stats", 436.9 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 5),
            new RawChunkIntervalPlanner.TableInput("procedure_stats", 128.6 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 5),
            new RawChunkIntervalPlanner.TableInput("perfmon_stats", 96.6 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 32),
            new RawChunkIntervalPlanner.TableInput("spinlock_stats", 46.5 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 32),
            new RawChunkIntervalPlanner.TableInput("wait_stats", 42.2 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 32),
            new RawChunkIntervalPlanner.TableInput("query_snapshots", 23.0 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 9),
            new RawChunkIntervalPlanner.TableInput("index_object_stats", 14.6 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 71),
            new RawChunkIntervalPlanner.TableInput("file_io_stats", 11.8 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 32),
            new RawChunkIntervalPlanner.TableInput("collection_log", 11.3 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 32),
            new RawChunkIntervalPlanner.TableInput("system_health_events", 5.9 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 32),
            new RawChunkIntervalPlanner.TableInput("latch_stats", 5.5 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 32),
            new RawChunkIntervalPlanner.TableInput("job_history", 4.9 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 73),
            /* Stands in for the other 27 raw hypertables' combined ≈0.39 GB open total at 24 h. */
            new RawChunkIntervalPlanner.TableInput("remaining_27_tables", 16.25 * MB, currentIntervalHours, lastChangedUtc, NumChunks: 30),
        };

    [Fact]
    public void ProductionStoreShape_DayOne_EveryTableNarrowsOnceToTwelveHours()
    {
        var budget = 7.75 * GiB;
        var tables = ProductionStoreShapeTables(currentIntervalHours: 24, lastChangedUtc: null);

        var initialTotal = tables.Sum(t => t.IngestBytesPerHour * t.CurrentIntervalHours);
        Assert.True(initialTotal > 2.5 * budget && initialTotal < 2.6 * budget,
            $"expected the store's 24h total near 2.55x budget, got {initialTotal / budget:N2}x");

        var decisions = RawChunkIntervalPlanner.Plan(tables, budget, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        /* Halving every table only reaches ≈9.9 GB, still over the 7.75 GB budget, so pass 1 narrows all 13
           rows one rung each, heaviest-rate first — query_stats first. */
        Assert.All(decisions.Values, d =>
        {
            Assert.Equal(12, d.TargetIntervalHours);
            Assert.True(d.Changes);
        });

        var afterTotal = tables.Sum(t => t.IngestBytesPerHour * 12);
        Assert.True(afterTotal > budget, $"expected the 12h total to still exceed budget, got {afterTotal:N0}");
    }

    [Fact]
    public void ProductionStoreShape_DayTwo_OnlyQueryStatsNarrowsToSixHours()
    {
        var budget = 7.75 * GiB;
        var tables = ProductionStoreShapeTables(currentIntervalHours: 12, lastChangedUtc: AsOf.AddDays(-1));

        var decisions = RawChunkIntervalPlanner.Plan(tables, budget, AsOf)
            .ToDictionary(d => d.TableName, StringComparer.Ordinal);

        Assert.Equal(6, decisions["query_stats"].TargetIntervalHours);
        Assert.True(decisions["query_stats"].Changes);

        foreach (var name in new[]
        {
            "procedure_stats", "perfmon_stats", "spinlock_stats", "wait_stats", "query_snapshots",
            "index_object_stats", "file_io_stats", "collection_log", "system_health_events", "latch_stats",
            "job_history", "remaining_27_tables",
        })
        {
            Assert.Equal(12, decisions[name].TargetIntervalHours);
            Assert.False(decisions[name].Changes, $"expected {name} to hold at 12 h");
        }

        var finalTotal = tables.Where(t => t.TableName != "query_stats").Sum(t => t.IngestBytesPerHour * 12)
            + tables.Single(t => t.TableName == "query_stats").IngestBytesPerHour * 6;
        Assert.True(finalTotal <= budget, $"expected the final total to fit under budget, got {finalTotal:N0} vs {budget:N0}");
    }

    /// <summary>Sets the thread's culture for the scope and restores it on dispose — same pattern as
    /// <c>PgLogEventMetricsTests.CultureScope</c>.</summary>
    private sealed class CultureScope : IDisposable
    {
        private readonly CultureInfo _before = CultureInfo.CurrentCulture;
        public CultureScope(string name) => CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
        public void Dispose() => CultureInfo.CurrentCulture = _before;
    }
}
