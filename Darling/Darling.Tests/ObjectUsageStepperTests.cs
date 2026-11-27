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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// DB-free tests for the two layers of the shared per-snapshot stepper behind <see cref="ObjectUsageWindow"/> (#9349): the node layer, which runs
/// on per-snapshot facts only (unnamed takeover, a node first seen mid-series, the restart tolerance and kept start, NodeBaseline), and the
/// counter layer, which counts a snapshot's rows given the node decision (the half rule, per-index and whole contributions, the Max dedupe).
/// Hand-written sequences, no database.
/// </summary>
public sealed class ObjectUsageStepperTests
{
    private static readonly DateTime Day0 = new(2026, 1, 1, 6, 0, 0);
    private static readonly DateTime Start1 = new(2025, 12, 1, 3, 0, 0);

    private static ObjectUsageSnapshotFacts Facts(string? server, DateTime? start) => new(server, start);

    private static ObjectUsageNodeStep Resolve(ObjectUsageNodeTracker tracker, int day, string? server, DateTime? start)
    {
        var step = tracker.Resolve(Day0.AddDays(day), Facts(server, start));
        tracker.Commit(step, null);
        return step;
    }

    private static ObjectUsageSample Row(
        int day,
        long seeks = 0,
        long updates = 0,
        int objectId = 100,
        int indexId = 1,
        DateTime? start = null,
        string? node = "NODE1",
        DateTime? lastSeek = null) =>
        new(Day0.AddDays(day), start ?? Start1, node, objectId, indexId, "dbo", "T" + objectId, false,
            seeks, 0, 0, updates, lastSeek, null, null, null, false);

    private static ObjectUsageStep Step(ObjectUsageStepper stepper, int day, params ObjectUsageSample[] rows) =>
        stepper.Step(Day0.AddDays(day), rows);

    [Fact]
    public void Node_FirstNameAfterUnnamedSnapshots_TakesTheirStateOver()
    {
        var tracker = new ObjectUsageNodeTracker();
        Resolve(tracker, 0, null, Start1);
        var second = Resolve(tracker, 1, null, Start1);
        var named = Resolve(tracker, 2, "NODE1", Start1);

        Assert.Equal("", second.NodeKey);
        Assert.Equal("NODE1", named.NodeKey);
        Assert.True(named.NodeKnown);
        Assert.Equal(Day0.AddDays(1), named.ComparedWith);
        Assert.Equal(ObjectUsageResetReasons.None, named.Reasons);
        Assert.Equal(ObjectUsageNodeDecision.Compare, named.Decision);
    }

    [Fact]
    public void Node_UnnamedSnapshotAfterANamedOne_IsTheNodeBeforeIt()
    {
        var tracker = new ObjectUsageNodeTracker();
        Resolve(tracker, 0, "NODE1", Start1);
        var unnamed = Resolve(tracker, 1, null, Start1);

        Assert.Equal("NODE1", unnamed.NodeKey);
        Assert.Equal(Day0, unnamed.ComparedWith);
        Assert.Equal("NODE1", unnamed.PreviousServer);
    }

    [Fact]
    public void Node_FirstSeenMidSeries_IsNodeChangedAndNothingIsComparedWith()
    {
        var tracker = new ObjectUsageNodeTracker();
        var baseline = Resolve(tracker, 0, "NODE1", Start1);
        Resolve(tracker, 1, "NODE1", Start1);
        var other = Resolve(tracker, 2, "NODE2", Day0.AddDays(-0.5));
        var back = Resolve(tracker, 3, "NODE1", Start1);

        Assert.True(baseline.IsBaseline);
        Assert.Equal(ObjectUsageNodeDecision.CountNothing, baseline.Decision);
        Assert.False(other.NodeKnown);
        Assert.Null(other.ComparedWith);
        Assert.Equal(ObjectUsageResetReasons.NodeChanged, other.Reasons);
        Assert.Equal(ObjectUsageNodeDecision.AllWhole, other.Decision);
        Assert.Equal("NODE1", other.PreviousServer);

        /* A return to a node already seen is compared with that node's own last snapshot, and is not a reset. */
        Assert.True(back.NodeKnown);
        Assert.Equal(Day0.AddDays(1), back.ComparedWith);
        Assert.Equal(ObjectUsageResetReasons.None, back.Reasons);
    }

    [Fact]
    public void Node_StartMovingInsideTheTolerance_IsTheSameStartAndKeepsTheFirstValue()
    {
        var tracker = new ObjectUsageNodeTracker();
        Resolve(tracker, 0, "NODE1", Start1);
        var wobble = Resolve(tracker, 1, "NODE1", Start1.AddSeconds(40));

        Assert.Equal(ObjectUsageResetReasons.None, wobble.Reasons);
        Assert.Equal(ObjectUsageNodeDecision.Compare, wobble.Decision);
        Assert.Equal(Start1, wobble.KeptStart);
    }

    [Fact]
    public void Node_StartMovingOutsideTheTolerance_IsARestartAndKeepsTheNewStart()
    {
        var tracker = new ObjectUsageNodeTracker();
        Resolve(tracker, 0, "NODE1", Start1);
        var restarted = Resolve(tracker, 1, "NODE1", Start1.AddMinutes(2));

        Assert.Equal(ObjectUsageResetReasons.Restart, restarted.Reasons);
        Assert.Equal(ObjectUsageNodeDecision.AllWhole, restarted.Decision);
        Assert.Equal(Start1.AddMinutes(2), restarted.KeptStart);
        Assert.Equal(Day0, restarted.ComparedWith);
    }

    [Fact]
    public void Node_NewNodeStartedBeforeTheBaselineByMoreThanADay_IsItsOwnBaseline()
    {
        var tracker = new ObjectUsageNodeTracker();
        Resolve(tracker, 0, "NODE1", Start1);
        var old = Resolve(tracker, 1, "NODE2", Day0.AddDays(-2));
        var recent = Resolve(tracker, 2, "NODE3", Day0.AddHours(-12));

        Assert.Equal(ObjectUsageResetReasons.NodeChanged | ObjectUsageResetReasons.NodeBaseline, old.Reasons);
        Assert.Equal(ObjectUsageNodeDecision.CountNothing, old.Decision);
        Assert.Equal(ObjectUsageResetReasons.NodeChanged, recent.Reasons);
        Assert.Equal(ObjectUsageNodeDecision.AllWhole, recent.Decision);
    }

    [Fact]
    public void Counter_TakeoverOfUnnamedState_CountsADifferenceNotTheWholeValue()
    {
        var stepper = new ObjectUsageStepper();
        Step(stepper, 0, Row(0, seeks: 1_000, node: null));
        var named = Step(stepper, 1, Row(1, seeks: 1_040, node: "NODE1"));

        var index = Assert.Single(named.Indexes);
        Assert.Equal(40, index.Reads);
        Assert.False(index.Whole);
        Assert.Equal(ObjectUsageCounted.PerIndex, named.Counted);
        Assert.Equal(ObjectUsageResetReasons.None, named.Reasons);
        Assert.Null(named.BlindFrom);
    }

    [Fact]
    public void Counter_Baseline_CountsNothingButKeepsTheRows()
    {
        var stepper = new ObjectUsageStepper();
        var baseline = Step(stepper, 0, Row(0, seeks: 1_000, updates: 5));

        Assert.Equal(ObjectUsageCounted.Nothing, baseline.Counted);
        var index = Assert.Single(baseline.Indexes);
        Assert.Equal(0, index.Reads);
        Assert.Equal(0, index.Updates);
        Assert.Equal(1_000, baseline.Current[(100, 1)].Seeks);
    }

    [Fact]
    public void Counter_NodeBaseline_CountsNothingAndLosesNothing()
    {
        var stepper = new ObjectUsageStepper();
        Step(stepper, 0, Row(0, seeks: 1_000, node: "NODE1"));
        var step = Step(stepper, 1, Row(1, seeks: 5_000, start: Day0.AddDays(-2), node: "NODE2"));

        Assert.Equal(ObjectUsageResetReasons.NodeChanged | ObjectUsageResetReasons.NodeBaseline, step.Reasons);
        Assert.Equal(ObjectUsageCounted.Nothing, step.Counted);
        Assert.Equal(0, Assert.Single(step.Indexes).Reads);
        Assert.Null(step.BlindFrom);
        Assert.Null(step.Node.ComparedWith);
    }

    [Fact]
    public void Counter_Restart_CountsEveryIndexWholeAndNamesTheBlindStart()
    {
        var stepper = new ObjectUsageStepper();
        Step(stepper, 0, Row(0, seeks: 1_000, objectId: 100), Row(0, seeks: 500, objectId: 200));
        var step = Step(stepper, 1, Row(1, seeks: 30, start: Start1.AddDays(1), objectId: 100), Row(1, seeks: 20, start: Start1.AddDays(1), objectId: 200));

        Assert.Equal(ObjectUsageResetReasons.Restart, step.Reasons);
        Assert.Equal(ObjectUsageCounted.AllWhole, step.Counted);
        Assert.Equal(50, step.Indexes.Sum(i => i.Reads));
        Assert.All(step.Indexes, i => Assert.True(i.Whole));
        Assert.Equal(Day0, step.BlindFrom);
    }

    [Fact]
    public void Counter_OneOfFourIndexesCleared_IsAnIndexResetAndTheRestKeepTheirDifference()
    {
        var stepper = new ObjectUsageStepper();
        Step(stepper, 0, Row(0, seeks: 100, objectId: 1), Row(0, seeks: 100, objectId: 2), Row(0, seeks: 100, objectId: 3), Row(0, seeks: 100, objectId: 4));
        var step = Step(stepper, 1, Row(1, seeks: 10, objectId: 1), Row(1, seeks: 110, objectId: 2), Row(1, seeks: 120, objectId: 3), Row(1, seeks: 130, objectId: 4));

        Assert.Equal(ObjectUsageCounted.PerIndex, step.Counted);
        Assert.Equal(1, step.IndexResets);
        Assert.Equal(ObjectUsageResetReasons.None, step.Reasons);
        var cleared = step.Indexes.Single(i => i.ObjectId == 1);
        Assert.True(cleared.Whole);
        Assert.Equal(10, cleared.Reads);
        Assert.Equal(60, step.Indexes.Where(i => i.ObjectId != 1).Sum(i => i.Reads));
        Assert.All(step.Indexes.Where(i => i.ObjectId != 1), i => Assert.False(i.Whole));
    }

    [Fact]
    public void Counter_HalfOfTheIndexesWithUseCleared_IsADatabaseReset()
    {
        var stepper = new ObjectUsageStepper();
        Step(stepper, 0, Row(0, seeks: 100, objectId: 1), Row(0, seeks: 100, objectId: 2), Row(0, seeks: 100, objectId: 3), Row(0, seeks: 100, objectId: 4));
        var step = Step(stepper, 1, Row(1, seeks: 10, objectId: 1), Row(1, seeks: 10, objectId: 2), Row(1, seeks: 120, objectId: 3), Row(1, seeks: 130, objectId: 4));

        Assert.Equal(ObjectUsageCounted.AllWhole, step.Counted);
        Assert.Equal(0, step.IndexResets);
        Assert.Equal(ObjectUsageResetReasons.CounterDecreased, step.Reasons);
        Assert.Equal(Day0, step.BlindFrom);
        Assert.Equal(10 + 10 + 120 + 130, step.Indexes.Sum(i => i.Reads));
    }

    [Fact]
    public void Counter_DuplicateRowsOfOneIndex_KeepTheLargerOfEachField()
    {
        var stepper = new ObjectUsageStepper();
        Step(stepper, 0, Row(0, seeks: 100, updates: 10));
        var step = Step(stepper, 1, Row(1, seeks: 130, updates: 10), Row(1, seeks: 120, updates: 14, lastSeek: Day0));

        var index = Assert.Single(step.Indexes);
        Assert.Equal(30, index.Reads);
        Assert.Equal(4, index.Updates);
        Assert.Equal(130, index.Now.Seeks);
        Assert.Equal(Day0, index.Now.LastRead);
    }

    [Fact]
    public void Counter_IndexNewToTheNode_IsCountedWholeAndMoved_AnUnchangedOneIsNot()
    {
        var stepper = new ObjectUsageStepper();
        Step(stepper, 0, Row(0, seeks: 100, objectId: 1), Row(0, seeks: 100, objectId: 2));
        var step = Step(stepper, 1, Row(1, seeks: 100, objectId: 1), Row(1, seeks: 105, objectId: 2), Row(1, seeks: 7, objectId: 3));

        var unchanged = step.Indexes.Single(i => i.ObjectId == 1);
        var grown = step.Indexes.Single(i => i.ObjectId == 2);
        var created = step.Indexes.Single(i => i.ObjectId == 3);
        Assert.False(unchanged.Moved);
        Assert.Equal(0, unchanged.Reads);
        Assert.True(grown.Moved);
        Assert.Equal(5, grown.Reads);
        Assert.False(created.HasBefore);
        Assert.True(created.Whole);
        Assert.True(created.Moved);
        Assert.Equal(7, created.Reads);
    }

    [Fact]
    public void Stepper_FullFold_GivesTheWindowTheSameTotalsAsTheStepSums()
    {
        var rows = new[]
        {
            Row(0, seeks: 100, updates: 10), Row(1, seeks: 150, updates: 12), Row(2, seeks: 20, updates: 1, start: Start1.AddDays(2)),
        };
        var window = new ObjectUsageWindow();
        var stepper = new ObjectUsageStepper();
        long reads = 0;
        long writes = 0;
        foreach (var row in rows)
        {
            window.Add(row);
            var step = Step(stepper, (int)(row.CollectionTime - Day0).TotalDays, row);
            reads += step.Indexes.Sum(i => i.Reads);
            writes += step.Indexes.Sum(i => i.Updates);
        }

        var totals = Assert.Single(window.Complete().Objects);
        Assert.Equal(reads, totals.Reads);
        Assert.Equal(writes, totals.Writes);
        Assert.Equal(70, totals.Reads);
    }
}
