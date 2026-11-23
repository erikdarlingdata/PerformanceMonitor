/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// DB-free tests for the window replay of the persisted object usage chain (#9349 part D1). Hand-written fact sequences: what the replay takes from
/// the chain, what it recomputes at a boundary step, and each reason it gives up and leaves the answer to the full fold.
/// </summary>
public sealed class ObjectUsageWindowReplayTests
{
    private const short Version = 1;
    private static readonly DateTime Day0 = new(2026, 3, 1, 6, 0, 0);
    private static readonly DateTime LongAgo = new(2025, 12, 1, 3, 0, 0);

    private static DateTime Day(int day) => Day0.AddDays(day);

    private static ObjectUsageSnapshotFact Fact(
        int day, string? node = "A", DateTime? start = null, int? comparedWith = null, ObjectUsageResetReasons reasons = ObjectUsageResetReasons.None,
        short version = Version, int indexResets = 0, int? blindFrom = null) =>
        new(Day(day), version, node, start ?? LongAgo, comparedWith is { } c ? Day(c) : null, reasons, indexResets, blindFrom is { } b ? Day(b) : null);

    private static ObjectUsageWindowReplay Run(int baselineDay, params ObjectUsageSnapshotFact[] facts) =>
        ObjectUsageWindowReplay.Run(facts, Day(baselineDay), facts[^1].CollectionTime, Version);

    [Fact]
    public void OneNode_TakesEveryStepFromTheChain_AndReadsOnlyTheBaselineRaw()
    {
        var replay = Run(0, Fact(0), Fact(1, comparedWith: 0), Fact(2, comparedWith: 1), Fact(3, comparedWith: 2));

        Assert.Null(replay.FallbackReason);
        Assert.Equal(4, replay.SnapshotCount);
        Assert.Equal(TimeSpan.FromDays(1), replay.LargestGap);
        Assert.Empty(replay.Resets);
        Assert.Empty(replay.Boundaries);
        Assert.Equal(new[] { Day(0) }, replay.RawTimes);
        Assert.Equal(new[] { "A" }, replay.AnsweringServerNames);
        Assert.Equal(Day(0), replay.CoveredFrom);
        Assert.Equal(Day(3), replay.CoveredTo);
    }

    [Fact]
    public void FirstNameAfterUnnamedSnapshots_TakesTheirStateOver_AndIsNotABoundary()
    {
        /* The chain took the unnamed node over at day 2, so its compared snapshot is day 1. A window that starts on an unnamed snapshot does the same. */
        var facts = new[] { Fact(0, node: null), Fact(1, node: null, comparedWith: 0), Fact(2, node: "A", comparedWith: 1), Fact(3, node: "A", comparedWith: 2) };

        foreach (var baseline in new[] { 0, 1 })
        {
            var replay = Run(baseline, facts.Where(f => f.CollectionTime >= Day(baseline)).ToArray());
            Assert.Null(replay.FallbackReason);
            Assert.Empty(replay.Boundaries);
            Assert.Empty(replay.Resets);
            Assert.Equal(new[] { "A" }, replay.AnsweringServerNames);
        }
    }

    [Fact]
    public void ANodeFirstSeenInTheWindow_IsABoundaryStep_ReadFromItsRawRows()
    {
        /* B answers at day 2: for a window that starts before that, the chain step is the window step only up to the point B first answers. A new node
           has no compared snapshot in the chain either, and counts whole because its start is recent. */
        var recent = Day(2).AddHours(-6);
        var facts = new[]
        {
            Fact(0), Fact(1, comparedWith: 0), Fact(2, node: "B", start: recent), Fact(3, node: "A", comparedWith: 1),
        };

        var replay = Run(0, facts);

        Assert.Null(replay.FallbackReason);
        var boundary = Assert.Single(replay.Boundaries);
        Assert.Equal(Day(2), boundary.Time);
        Assert.True(boundary.CountedWhole);
        Assert.Equal(new[] { Day(0), Day(2) }, replay.RawTimes);
        var reset = Assert.Single(replay.Resets);
        Assert.Equal(new ObjectUsageReset(Day(2), ObjectUsageResetReasons.NodeChanged, "A", "B"), reset);
        Assert.Equal(TimeSpan.Zero, reset.BlindTime);
        Assert.Equal(new[] { "A", "B" }, replay.AnsweringServerNames);
    }

    [Fact]
    public void ABoundaryStepAtTheEdge_IsTheNewNodesFirstSnapshotInTheWindow_WhateverTheChainDid()
    {
        /* The window starts on B's snapshot, so A's next answer is new to the window even though the chain compared it with day 0. */
        var facts = new[] { Fact(0), Fact(1, node: "B", start: Day(1).AddHours(-3)), Fact(2, node: "A", start: Day(1).AddHours(-2), comparedWith: 0) };

        var replay = Run(1, facts.Skip(1).ToArray());

        Assert.Null(replay.FallbackReason);
        Assert.Equal(Day(2), Assert.Single(replay.Boundaries).Time);
        Assert.Equal(ObjectUsageResetReasons.NodeChanged, Assert.Single(replay.Resets).Reasons);
    }

    [Theory]
    [InlineData(36, true)]
    [InlineData(24, false)]
    [InlineData(12, false)]
    public void ABoundaryNodeStartedMoreThanADayBeforeTheBaseline_CountsNothing(int hoursBeforeBaseline, bool countsNothing)
    {
        /* The margin is strict: a start exactly a day before the baseline is still inside it. */
        var facts = new[] { Fact(0), Fact(1, node: "B", start: Day(0).AddHours(-hoursBeforeBaseline)) };

        var replay = Run(0, facts);

        Assert.Null(replay.FallbackReason);
        var boundary = Assert.Single(replay.Boundaries);
        Assert.Equal(!countsNothing, boundary.CountedWhole);
        var reasons = Assert.Single(replay.Resets).Reasons;
        Assert.Equal(countsNothing, reasons.HasFlag(ObjectUsageResetReasons.NodeBaseline));
        Assert.True(reasons.HasFlag(ObjectUsageResetReasons.NodeChanged));
    }

    [Fact]
    public void ARestartInsideTheTolerance_IsNotARestart_AndOneOutsideItIsTakenFromTheChain()
    {
        var start = new DateTime(2026, 2, 1, 3, 0, 0);
        var restarted = Day(3).AddHours(-5);
        var facts = new[]
        {
            Fact(0, start: start),
            Fact(1, start: start.AddSeconds(30), comparedWith: 0),
            Fact(2, start: start.AddSeconds(45), comparedWith: 1),
            Fact(3, start: restarted, comparedWith: 2, reasons: ObjectUsageResetReasons.Restart | ObjectUsageResetReasons.CounterDecreased, indexResets: 0, blindFrom: 2),
            Fact(4, start: restarted, comparedWith: 3, indexResets: 2),
        };

        var replay = Run(0, facts);

        Assert.Null(replay.FallbackReason);
        var reset = Assert.Single(replay.Resets);
        Assert.Equal(new ObjectUsageReset(Day(3), ObjectUsageResetReasons.Restart | ObjectUsageResetReasons.CounterDecreased, "A", "A", Day(2)), reset);
        Assert.Equal(TimeSpan.FromDays(1), reset.BlindTime);
        Assert.Equal(2, replay.IndexResets);
        Assert.Empty(replay.Boundaries);
    }

    [Fact]
    public void AReplayThatStartsAfterTheBaseline_LeavesOutTheEarlierStepsResetsAndIndexResets()
    {
        var restarted = Day(1).AddHours(-5);
        var facts = new[]
        {
            Fact(0), Fact(1, start: restarted, comparedWith: 0, reasons: ObjectUsageResetReasons.Restart, indexResets: 3, blindFrom: 0),
            Fact(2, start: restarted, comparedWith: 1, indexResets: 1),
        };

        var replay = Run(1, facts.Skip(1).ToArray());

        Assert.Null(replay.FallbackReason);
        Assert.Empty(replay.Resets);
        Assert.Equal(1, replay.IndexResets);
        Assert.Equal(2, replay.SnapshotCount);
    }

    [Fact]
    public void NoFactsRow_AtTheBaseline_FallsBack()
    {
        var replay = ObjectUsageWindowReplay.Run(new[] { Fact(1, comparedWith: 0), Fact(2, comparedWith: 1) }, Day(0), Day(2), Version);

        Assert.Equal("no facts row at the baseline", replay.FallbackReason);
        Assert.Equal("no facts row at the baseline", ObjectUsageWindowReplay.Run(Array.Empty<ObjectUsageSnapshotFact>(), Day(0), Day(2), Version).FallbackReason);
    }

    [Fact]
    public void TheNewestFactsRow_BeforeTheNewestSnapshot_FallsBack()
    {
        var replay = ObjectUsageWindowReplay.Run(new[] { Fact(0), Fact(1, comparedWith: 0) }, Day(0), Day(2), Version);

        Assert.Equal("the newest facts row is before the newest snapshot", replay.FallbackReason);
    }

    [Fact]
    public void AnOlderStepVersion_OnAnyRow_FallsBack()
    {
        var replay = ObjectUsageWindowReplay.Run(
            new[] { Fact(0), Fact(1, comparedWith: 0, version: 0), Fact(2, comparedWith: 1) }, Day(0), Day(2), Version);

        Assert.Equal("a facts row has an older step version", replay.FallbackReason);
    }

    [Fact]
    public void ADifferentComparedSnapshot_FallsBack()
    {
        /* Unnamed rows after named ones: the chain gives day 2's unnamed snapshot node B's state, and compares day 3's answer from A with day 0. A
           window that starts on day 2 has no node A state but the unnamed one, which day 3's first name takes over: it compares with day 2. */
        var facts = new[]
        {
            Fact(0, node: "A"), Fact(1, node: "B", start: Day(1).AddHours(-1)), Fact(2, node: null, start: Day(1).AddHours(-1), comparedWith: 1),
            Fact(3, node: "A", comparedWith: 0),
        };

        Assert.Null(Run(0, facts).FallbackReason);

        var replay = Run(2, facts.Skip(2).ToArray());

        Assert.Equal("the window compares a snapshot the chain step did not", replay.FallbackReason);
    }

    [Fact]
    public void ARestartDecisionThatDiffersAcrossTheEdge_FallsBack()
    {
        /* The start drifts 50 seconds a snapshot: the chain, which kept the first start, sees 100 seconds and calls a restart; a window that starts
           one snapshot later keeps the second start and sees 50. */
        var start = new DateTime(2026, 2, 1, 3, 0, 0);
        var facts = new[]
        {
            Fact(0, start: start),
            Fact(1, start: start.AddSeconds(50), comparedWith: 0),
            Fact(2, start: start.AddSeconds(100), comparedWith: 1, reasons: ObjectUsageResetReasons.Restart, blindFrom: 1),
        };

        Assert.Null(Run(0, facts).FallbackReason);

        var replay = Run(1, facts.Skip(1).ToArray());

        Assert.Equal("the window's restart decision differs from the chain step's", replay.FallbackReason);
    }

    // ── Assemble ────────────────────────────────────────────────────────────────────────────

    private static ObjectUsageSample Raw(
        int day, int objectId, int indexId, long seeks = 0, long updates = 0, DateTime? lastSeek = null, DateTime? lastUpdate = null) =>
        new(Day(day), LongAgo, "A", objectId, indexId, "dbo", "T" + objectId, false, seeks, 0, 0, updates, lastSeek, null, null, lastUpdate, false);

    [Fact]
    public void Assemble_AddsBoundaryRowsToTheStoredDeltas_AndTakesStampsFromTheBaselineAndTheBoundaries()
    {
        var facts = new[] { Fact(0), Fact(1, comparedWith: 0), Fact(2, node: "B", start: Day(2).AddHours(-1)), Fact(3, comparedWith: 1) };
        var replay = Run(0, facts);
        var runs = new[]
        {
            new ObjectUsageRun(100, 1, Day(0), null, "dbo", "Orders", false, false),
            new ObjectUsageRun(100, 2, Day(1), null, "dbo", "Orders", false, null),
            new ObjectUsageRun(200, 1, Day(0), Day(2), "dbo", "Gone", false, null),
            new ObjectUsageRun(300, 1, Day(2), null, "sales", "NewView", true, true),
        };
        var deltas = new[] { new ObjectUsageDeltaTotals(100, 40, 3, Day(1).AddHours(-2), null) };
        var raw = new[]
        {
            Raw(0, 100, 1, seeks: 5, updates: 1, lastSeek: Day(0).AddHours(-30)),
            Raw(0, 200, 1, lastUpdate: Day(0).AddHours(-9)),
            Raw(2, 100, 1, seeks: 7, updates: 2),
            Raw(2, 100, 2, seeks: 4, updates: 9),
            Raw(2, 300, 1, seeks: 11, updates: 4, lastSeek: Day(2).AddHours(-1)),
        };

        var result = replay.Assemble(deltas, runs, raw, out var reason);

        Assert.Null(reason);
        Assert.NotNull(result);
        Assert.Equal(new[] { "Orders", "Gone", "NewView" }.Order(StringComparer.Ordinal), result.Objects.Select(o => o.ObjectName).Order(StringComparer.Ordinal));
        var orders = result.Objects.Single(o => o.ObjectId == 100);
        Assert.Equal(40 + 7 + 4, orders.Reads);
        Assert.Equal(3 + 2, orders.Writes);
        Assert.Equal(Day(1).AddHours(-2), orders.LastRead);
        Assert.Equal(Day(0), orders.FirstSnapshot);
        Assert.Equal(Day(3), orders.LastSnapshot);
        Assert.Equal(2, orders.IndexCount);
        Assert.False(orders.IsMemoryOptimized);
        var gone = result.Objects.Single(o => o.ObjectId == 200);
        Assert.Equal(Day(0).AddHours(-9), gone.LastWrite);
        Assert.Equal(Day(2), gone.LastSnapshot);
        var view = result.Objects.Single(o => o.ObjectId == 300);
        Assert.Equal((11L, 4L), (view.Reads, view.Writes));
        Assert.True(view.IsIndexedView);
        Assert.Equal(Day(2), view.FirstSnapshot);
        Assert.Equal(Day(2).AddHours(-1), view.LastRead);
    }

    [Fact]
    public void Assemble_WithABoundaryThatCountsNothing_AddsNoCountsFromItsRows()
    {
        var facts = new[] { Fact(0), Fact(1, node: "B", start: Day(0).AddDays(-4)) };
        var replay = Run(0, facts);
        var runs = new[] { new ObjectUsageRun(100, 1, Day(0), null, "dbo", "Orders", false, false) };
        var raw = new[] { Raw(0, 100, 1, seeks: 5), Raw(1, 100, 1, seeks: 900, updates: 900, lastSeek: Day(1).AddHours(-2)) };

        var result = replay.Assemble(Array.Empty<ObjectUsageDeltaTotals>(), runs, raw, out _);

        var orders = Assert.Single(result!.Objects);
        Assert.Equal((0L, 0L), (orders.Reads, orders.Writes));
        Assert.Equal(Day(1).AddHours(-2), orders.LastRead);
    }

    [Fact]
    public void Assemble_WithAnIndexThatHasNoRunWhileAnotherIndexOfTheObjectHasOne_GivesUp()
    {
        var replay = Run(0, Fact(0), Fact(1, comparedWith: 0));
        var runs = new[] { new ObjectUsageRun(100, 1, Day(0), null, "dbo", "Orders", false, false) };
        var raw = new[] { Raw(0, 100, 1), Raw(0, 100, 2) };

        Assert.Null(replay.Assemble(Array.Empty<ObjectUsageDeltaTotals>(), runs, raw, out var reason));
        Assert.NotNull(reason);
    }

    [Fact]
    public void Assemble_WithAnIndexWhoseOnlyRunEndsBeforeARawRowInTheWindow_GivesUp()
    {
        var replay = Run(0, Fact(0), Fact(1, node: "B", start: Day(1).AddHours(-1)), Fact(2, comparedWith: 1));
        /* The second index's run overlaps the window and ends before the boundary snapshot's raw row, so it does not cover that row. */
        var runs = new[]
        {
            new ObjectUsageRun(100, 1, Day(0), null, "dbo", "Orders", false, false),
            new ObjectUsageRun(100, 2, Day(0), Day(0).AddHours(1), "dbo", "Orders", false, false),
        };
        var raw = new[] { Raw(0, 100, 1), Raw(0, 100, 2), Raw(1, 100, 1), Raw(1, 100, 2) };

        Assert.Null(replay.Assemble(Array.Empty<ObjectUsageDeltaTotals>(), runs, raw, out var reason));
        Assert.Equal("a raw row names an index with no run", reason);
    }

    [Fact]
    public void Assemble_WithAnObjectThatHasNoRun_GivesUp()
    {
        var replay = Run(0, Fact(0), Fact(1, comparedWith: 0));
        var raw = new[] { Raw(0, 100, 1) };

        Assert.Null(replay.Assemble(new[] { new ObjectUsageDeltaTotals(100, 1, 0, null, null) }, Array.Empty<ObjectUsageRun>(), raw, out var reason));
        Assert.Equal("a stored delta names an object with no run", reason);
        Assert.Null(replay.Assemble(Array.Empty<ObjectUsageDeltaTotals>(), Array.Empty<ObjectUsageRun>(), raw, out reason));
        Assert.NotNull(reason);
        Assert.Null(replay.Assemble(
            Array.Empty<ObjectUsageDeltaTotals>(), new[] { new ObjectUsageRun(100, 1, Day(0), null, "dbo", "T", false, false) }, Array.Empty<ObjectUsageSample>(), out reason));
        Assert.Equal("a raw snapshot the replay needs holds no rows", reason);
    }
}
