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
/// DB-free tests for the shared <see cref="ObjectUsageWindow"/> (#8784): usage totals over a window of daily index and object stats snapshots
/// that stay correct across counter resets. Each reset signal is exercised on its own (restart, a counter going down, a last-access stamp
/// cleared), as are the per-instance tracking (a new node, nodes that take turns), one index cleared on its own, the baseline, the
/// carry-forward of last seen, an index created inside the window, a dropped object, and the input-order guard. No database.
/// </summary>
public sealed class ObjectUsageWindowTests
{
    private static readonly DateTime Day0 = new(2026, 1, 1, 6, 0, 0);
    private static readonly DateTime Start1 = new(2025, 12, 1, 3, 0, 0);
    private static readonly DateTime Start2 = new(2026, 1, 15, 3, 0, 0);

    private static ObjectUsageSample Row(
        int day,
        long seeks = 0,
        long scans = 0,
        long lookups = 0,
        long updates = 0,
        int objectId = 100,
        int indexId = 1,
        string name = "Orders",
        DateTime? start = null,
        string? node = "NODE1",
        DateTime? lastSeek = null,
        DateTime? lastUpdate = null,
        bool indexedView = false,
        bool? memoryOptimized = false) =>
        new(Day0.AddDays(day), start ?? Start1, node, objectId, indexId, "dbo", name, indexedView,
            seeks, scans, lookups, updates, lastSeek, null, null, lastUpdate, memoryOptimized);

    private static ObjectUsageWindowResult Run(params ObjectUsageSample[] rows)
    {
        var window = new ObjectUsageWindow();
        foreach (var row in rows)
        {
            window.Add(row);
        }

        return window.Complete();
    }

    [Fact]
    public void NoReset_CountsTheGrowthSinceTheBaseline_NotTheBaselineItself()
    {
        var result = Run(Row(0, seeks: 1_000, updates: 50), Row(1, seeks: 1_010, updates: 55), Row(2, seeks: 1_030, updates: 60));

        var orders = Assert.Single(result.Objects);
        Assert.Equal(30, orders.Reads);
        Assert.Equal(10, orders.Writes);
        Assert.Empty(result.Resets);
        Assert.Equal(Day0, result.CoveredFrom);
        Assert.Equal(Day0.AddDays(2), result.CoveredTo);
        Assert.Equal(3, result.SnapshotCount);
    }

    [Fact]
    public void Restart_TheNewValueIsTheDelta()
    {
        /* Without the reset rule this reads 1,000 -> 1,010 -> 7: the last step is negative and the 7 reads after the restart are lost. */
        var result = Run(Row(0, seeks: 1_000), Row(1, seeks: 1_010), Row(2, seeks: 7, start: Start2));

        var orders = Assert.Single(result.Objects);
        Assert.Equal(17, orders.Reads);
        var reset = Assert.Single(result.Resets);
        Assert.Equal(Day0.AddDays(2), reset.CollectionTime);
        Assert.True(reset.Reasons.HasFlag(ObjectUsageResetReasons.Restart));
    }

    [Fact]
    public void Restart_WhoseCountersGrewPastTheOldValue_IsStillAReset()
    {
        /* 500 -> 900 looks like 400 more reads by subtraction, but the instance restarted in between: all 900 are new. */
        var result = Run(Row(0, seeks: 500), Row(1, seeks: 900, start: Start2));

        Assert.Equal(900, Assert.Single(result.Objects).Reads);
        Assert.Single(result.Resets);
    }

    [Fact]
    public void ANewNode_ThatStartedInsideTheWindow_CountsItsWholeValue()
    {
        /* NODE2 started after the baseline, so everything it has counted happened in the window. */
        var result = Run(Row(0, seeks: 500, node: "NODE1"), Row(20, seeks: 600, node: "NODE2", start: Start2));

        Assert.Equal(600, Assert.Single(result.Objects).Reads);
        var reset = Assert.Single(result.Resets);
        Assert.Equal(ObjectUsageResetReasons.NodeChanged, reset.Reasons);
        Assert.Equal("NODE1", reset.FromServerName);
        Assert.Equal("NODE2", reset.ToServerName);
        Assert.Equal(new[] { "NODE1", "NODE2" }, result.AnsweringServerNames);
    }

    [Fact]
    public void ANewNode_UpSinceBeforeTheWindow_IsItsOwnBaseline_NotAWholeCount()
    {
        /* NODE2 has been up since a month before the baseline: its 5,000 reads can be years old, so they are not counted. It is measured
           from its first snapshot, and its last-seen stamp still counts. */
        var seen = Day0.AddDays(-400);
        var result = Run(
            Row(0, seeks: 500, node: "NODE1"),
            Row(1, seeks: 5_000, node: "NODE2", lastSeek: seen),
            Row(2, seeks: 5_040, node: "NODE2", lastSeek: seen));

        var orders = Assert.Single(result.Objects);
        Assert.Equal(40, orders.Reads);
        Assert.Equal(seen, orders.LastRead);
        Assert.Equal(ObjectUsageResetReasons.NodeChanged | ObjectUsageResetReasons.NodeBaseline, Assert.Single(result.Resets).Reasons);
    }

    [Fact]
    public void ANewNode_WithNoStartTime_CountsItsWholeValue()
    {
        var window = new ObjectUsageWindow();
        window.Add(Row(0, seeks: 500, node: "NODE1"));
        window.Add(Row(1, seeks: 600, node: "NODE2") with { SqlServerStartTime = null });

        Assert.Equal(600, Assert.Single(window.Complete().Objects).Reads);
    }

    [Fact]
    public void NodesThatTakeTurns_EachKeepTheirOwnDifference()
    {
        /* Read-only routing alternates the answering node. Each snapshot is compared with the same node's last one, so a table with a large
           cumulative count and no reads stays at zero instead of having that count added again every day. */
        var result = Run(
            Row(0, seeks: 1_000, node: "NODE1", objectId: 100, name: "Orders"),
            Row(0, seeks: 5_000, node: "NODE1", objectId: 200, name: "Archive"),
            Row(1, seeks: 300, node: "NODE2", objectId: 100, name: "Orders"),
            Row(1, seeks: 7_000, node: "NODE2", objectId: 200, name: "Archive"),
            Row(2, seeks: 1_010, node: "NODE1", objectId: 100, name: "Orders"),
            Row(2, seeks: 5_000, node: "NODE1", objectId: 200, name: "Archive"),
            Row(3, seeks: 325, node: "NODE2", objectId: 100, name: "Orders"),
            Row(3, seeks: 7_000, node: "NODE2", objectId: 200, name: "Archive"),
            Row(4, seeks: 1_030, node: "NODE1", objectId: 100, name: "Orders"),
            Row(4, seeks: 5_000, node: "NODE1", objectId: 200, name: "Archive"));

        Assert.Equal(55, result.Objects.Single(o => o.ObjectName == "Orders").Reads);
        Assert.Equal(0, result.Objects.Single(o => o.ObjectName == "Archive").Reads);
        /* Only NODE2's first appearance is noted: a return to a node already seen is not a reset. */
        Assert.Single(result.Resets);
    }

    [Fact]
    public void AReturningNode_ThatRestartedWhileAway_IsAReset()
    {
        var result = Run(
            Row(0, seeks: 1_000, node: "NODE1"),
            Row(1, seeks: 300, node: "NODE2"),
            Row(2, seeks: 2_000, node: "NODE1", start: Start2));

        Assert.Equal(2_000, Assert.Single(result.Objects).Reads);
        Assert.Contains(result.Resets, r => r.Reasons == ObjectUsageResetReasons.Restart);
    }

    [Fact]
    public void OneIndexClearedOnItsOwn_CountsThatIndexWhole_AndLeavesTheRestAlone()
    {
        /* One index of four is dropped and created again (or rebuilt on a version that clears its usage row): its counter falls. That is not
           the database being cleared, so the other tables keep their difference: Archive, with 9,000 old reads, stays unread. */
        var result = Run(
            Row(0, seeks: 1_000, objectId: 100, name: "Orders"),
            Row(0, seeks: 9_000, objectId: 200, name: "Archive"),
            Row(0, seeks: 400, objectId: 300, name: "Customers"),
            Row(0, seeks: 70, objectId: 400, name: "Rebuilt"),
            Row(1, seeks: 1_020, objectId: 100, name: "Orders"),
            Row(1, seeks: 9_000, objectId: 200, name: "Archive"),
            Row(1, seeks: 405, objectId: 300, name: "Customers"),
            Row(1, seeks: 6, objectId: 400, name: "Rebuilt"));

        Assert.Equal(20, result.Objects.Single(o => o.ObjectName == "Orders").Reads);
        Assert.Equal(0, result.Objects.Single(o => o.ObjectName == "Archive").Reads);
        Assert.Equal(5, result.Objects.Single(o => o.ObjectName == "Customers").Reads);
        Assert.Equal(6, result.Objects.Single(o => o.ObjectName == "Rebuilt").Reads);
        Assert.Empty(result.Resets);
        Assert.Equal(1, result.IndexResets);
    }

    [Fact]
    public void IndexesThatNeverHadUse_DoNotDiluteTheEvidenceOfADatabaseReset()
    {
        /* The database was cleared: the one index with any use shows it. Three indexes with nothing to lose say nothing either way, so they
           must not make it look like one index cleared on its own. */
        var result = Run(
            Row(0, seeks: 1_000, objectId: 100, name: "Orders"),
            Row(0, objectId: 200, name: "A"),
            Row(0, objectId: 300, name: "B"),
            Row(0, objectId: 400, name: "C"),
            Row(1, seeks: 40, objectId: 100, name: "Orders"),
            Row(1, seeks: 3, objectId: 200, name: "A"),
            Row(1, objectId: 300, name: "B"),
            Row(1, objectId: 400, name: "C"));

        Assert.Equal(ObjectUsageResetReasons.CounterDecreased, Assert.Single(result.Resets).Reasons);
        Assert.Equal(0, result.IndexResets);
    }

    [Fact]
    public void AMemoryOptimizedTable_IsMarked()
    {
        var result = Run(Row(0, memoryOptimized: true, name: "SessionState"), Row(1, memoryOptimized: true, name: "SessionState"));

        Assert.True(Assert.Single(result.Objects).IsMemoryOptimized);
    }

    [Fact]
    public void NodeNameDifferingOnlyInCase_IsNotAReset()
    {
        var result = Run(Row(0, seeks: 500, node: "node1"), Row(1, seeks: 600, node: "NODE1"));

        Assert.Equal(100, Assert.Single(result.Objects).Reads);
        Assert.Empty(result.Resets);
    }

    [Fact]
    public void RowsStoredBeforeTheNodeColumn_DoNotCountAsAChangeOfNode()
    {
        var result = Run(Row(0, seeks: 500, node: null), Row(1, seeks: 600, node: "NODE1"), Row(2, seeks: 650, node: "NODE1"));

        Assert.Equal(150, Assert.Single(result.Objects).Reads);
        Assert.Empty(result.Resets);
    }

    [Fact]
    public void CounterGoingDown_WithoutARestart_IsAResetForTheWholeDatabase()
    {
        /* The database went offline and online: one index's counter shows it, and the other index, which happens to have grown back past its
           old value, is reset with it. Its whole new value counts. */
        var result = Run(
            Row(0, seeks: 1_000, objectId: 100, name: "Orders"),
            Row(0, seeks: 10, objectId: 200, name: "Customers"),
            Row(1, seeks: 40, objectId: 100, name: "Orders"),
            Row(1, seeks: 25, objectId: 200, name: "Customers"));

        Assert.Equal(40, result.Objects.Single(o => o.ObjectName == "Orders").Reads);
        Assert.Equal(25, result.Objects.Single(o => o.ObjectName == "Customers").Reads);
        Assert.Equal(ObjectUsageResetReasons.CounterDecreased, Assert.Single(result.Resets).Reasons);
    }

    [Fact]
    public void LastAccessStampCleared_IsAReset()
    {
        var seen = Day0.AddHours(-2);
        var result = Run(Row(0, seeks: 5, lastSeek: seen), Row(1, seeks: 5, lastSeek: null));

        Assert.Equal(ObjectUsageResetReasons.LastAccessCleared, Assert.Single(result.Resets).Reasons);
    }

    [Fact]
    public void LastSeen_IsCarriedForwardAcrossAReset()
    {
        var read = Day0.AddHours(-3);
        var write = Day0.AddHours(-1);
        var result = Run(Row(0, seeks: 5, updates: 2, lastSeek: read, lastUpdate: write), Row(1, start: Start2));

        var orders = Assert.Single(result.Objects);
        Assert.Equal(read, orders.LastRead);
        Assert.Equal(write, orders.LastWrite);
        Assert.Equal(0, orders.Reads);
    }

    [Fact]
    public void Writes_CountTheBaseIndexOnly_ReadsCountEveryIndex()
    {
        var result = Run(
            Row(0, indexId: 1, seeks: 0, updates: 0),
            Row(0, indexId: 2, seeks: 0, updates: 0),
            Row(1, indexId: 1, seeks: 3, updates: 10),
            Row(1, indexId: 2, seeks: 4, updates: 10));

        var orders = Assert.Single(result.Objects);
        Assert.Equal(7, orders.Reads);
        Assert.Equal(10, orders.Writes);
        Assert.Equal(2, orders.IndexCount);
    }

    [Fact]
    public void IndexCreatedInsideTheWindow_CountsItsWholeValue()
    {
        var result = Run(Row(0, indexId: 1), Row(1, indexId: 1), Row(1, indexId: 5, seeks: 12));

        Assert.Equal(12, Assert.Single(result.Objects).Reads);
        Assert.Empty(result.Resets);
    }

    [Fact]
    public void ObjectDroppedInsideTheWindow_KeepsItsLastSnapshot()
    {
        var result = Run(
            Row(0, objectId: 100, name: "Orders"),
            Row(0, objectId: 200, name: "OldStuff"),
            Row(1, objectId: 100, name: "Orders"));

        var dropped = result.Objects.Single(o => o.ObjectName == "OldStuff");
        Assert.Equal(Day0, dropped.LastSnapshot);
        Assert.True(dropped.LastSnapshot < result.CoveredTo);
    }

    [Fact]
    public void DuplicateRowForOneIndexInOneSnapshot_KeepsTheLargerReading()
    {
        var result = Run(Row(0, seeks: 10), Row(1, seeks: 15), Row(1, seeks: 20));

        Assert.Equal(10, Assert.Single(result.Objects).Reads);
        Assert.Empty(result.Resets);
    }

    [Fact]
    public void LargestGap_IsTheLongestTimeBetweenTwoSnapshots()
    {
        var result = Run(Row(0), Row(1), Row(5), Row(6));

        Assert.Equal(TimeSpan.FromDays(4), result.LargestGap);
    }

    [Fact]
    public void SingleSnapshot_IsOnlyABaseline()
    {
        var result = Run(Row(0, seeks: 999));

        Assert.Equal(0, Assert.Single(result.Objects).Reads);
        Assert.Null(result.LargestGap);
        Assert.Equal(1, result.SnapshotCount);
    }

    [Fact]
    public void NoRows_IsAnEmptyResult()
    {
        var result = Run();

        Assert.Empty(result.Objects);
        Assert.Null(result.CoveredFrom);
        Assert.Null(result.CoveredTo);
        Assert.Equal(0, result.SnapshotCount);
    }

    [Fact]
    public void IndexedView_IsReportedAsOne()
    {
        var result = Run(Row(0, indexedView: true, name: "vOrderTotals"), Row(1, indexedView: true, name: "vOrderTotals", seeks: 2));

        Assert.True(Assert.Single(result.Objects).IsIndexedView);
    }

    [Fact]
    public void RowsOutOfOrder_AreRefused()
    {
        var window = new ObjectUsageWindow();
        window.Add(Row(1));

        Assert.Throws<ArgumentException>(() => window.Add(Row(0)));
    }

    [Fact]
    public void AStartTimeThatOnlyWobbles_IsNotARestart()
    {
        /* The start time is computed by the server and can read a few milliseconds apart; counting that as a restart would add every
           index's whole cumulative value again and make a table last read years ago look read. */
        var result = Run(
            Row(0, seeks: 5_000),
            Row(1, seeks: 5_000, start: Start1.AddMilliseconds(3)),
            Row(2, seeks: 5_000, start: Start1.AddMilliseconds(-7)),
            Row(3, seeks: 5_000, start: Start1.AddSeconds(59)));

        Assert.Empty(result.Resets);
        Assert.Equal(0, Assert.Single(result.Objects).Reads);
        Assert.Equal(TimeSpan.Zero, result.BlindTime);
    }

    [Fact]
    public void AStartTimeThatDrifts_IsMeasuredFromTheFirstReading_SoItNeverAddsUpToARestart()
    {
        var rows = Enumerable.Range(0, 10).Select(d => Row(d, seeks: 5_000, start: Start1.AddSeconds(d * 10))).ToArray();

        var result = Run(rows);

        /* 90 seconds from the first reading by the last snapshot: more than the tolerance, so it is called once, not on every snapshot. */
        Assert.Single(result.Resets);
    }

    [Fact]
    public void ARestart_ReportsTheTimeItCouldNotSee()
    {
        /* Use between the last snapshot before the restart and the restart is gone with the counters: the window says how long that was. */
        var result = Run(Row(0, seeks: 100), Row(1, seeks: 110), Row(2, seeks: 5, start: Start2));

        var reset = Assert.Single(result.Resets);
        Assert.Equal(ObjectUsageResetReasons.Restart, reset.Reasons);
        Assert.Equal(Day0.AddDays(1), reset.BlindFrom);
        Assert.Equal(TimeSpan.FromDays(1), reset.BlindTime);
        Assert.Equal(TimeSpan.FromDays(1), result.BlindTime);
        Assert.Equal(15, Assert.Single(result.Objects).Reads);
    }

    [Fact]
    public void ANewInstance_LosesNothing_SoItHasNoBlindTime()
    {
        var result = Run(Row(0, seeks: 100), Row(1, seeks: 4, node: "NODE2", start: Start2));

        var reset = Assert.Single(result.Resets);
        Assert.True(reset.Reasons.HasFlag(ObjectUsageResetReasons.NodeChanged));
        Assert.Null(reset.BlindFrom);
        Assert.Equal(TimeSpan.Zero, result.BlindTime);
    }

    [Fact]
    public void ARowFromBeforeTheMemoryFlagWasCollected_DoesNotSayEitherWay()
    {
        var unknown = Run(Row(0, memoryOptimized: null), Row(1, memoryOptimized: null));
        Assert.Null(Assert.Single(unknown.Objects).IsMemoryOptimized);

        /* The newest snapshot that says decides, and an older row that does not say cannot undo it. */
        var settled = Run(Row(0, memoryOptimized: null), Row(1, memoryOptimized: false));
        Assert.False(Assert.Single(settled.Objects).IsMemoryOptimized);

        var dropped = Run(Row(0, memoryOptimized: true), Row(1, memoryOptimized: null));
        Assert.True(Assert.Single(dropped.Objects).IsMemoryOptimized);
    }
}
