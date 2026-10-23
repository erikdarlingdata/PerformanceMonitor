/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254 / #6279: the ship guard keeps a look-back read from writing a row it already wrote, or an older state of
/// it. T2 of the plan: unchanged drops, newer ships, older-after-primary drops, a failed write commits nothing,
/// eviction at W - L.
/// </summary>
public sealed class QueryStoreShipGuardTests
{
    private static readonly DateTime Day = new(2026, 10, 21, 0, 0, 0, DateTimeKind.Utc);

    private static DateTime At(int hour, int minute) => Day.AddHours(hour).AddMinutes(minute);

    private static readonly TimeSpan L = TimeSpan.FromMinutes(20);

    private static QueryStoreCollector.Row Row(long queryId, DateTime last, long count, long interval = 7, string? role = "Primary", string exec = "Regular") => new()
    {
        DatabaseName = "Sales",
        QueryId = queryId,
        PlanId = queryId * 10,
        RuntimeStatsIntervalId = interval,
        ReplicaRole = role,
        ExecutionTypeDesc = exec,
        FirstExecutionTime = At(9, 0),
        LastExecutionTime = last,
        ExecutionCount = count,
    };

    private static List<QueryStoreCollector.Row> Ship(QueryStoreShipGuard guard, params QueryStoreCollector.Row[] rows) =>
        guard.Filter("Sales", rows);

    private static void ShipAndWrite(QueryStoreShipGuard guard, DateTime watermark, params QueryStoreCollector.Row[] rows)
    {
        Ship(guard, rows);
        guard.Commit("Sales", watermark, L);
    }

    [Fact]
    public void NewKey_Ships()
    {
        var guard = new QueryStoreShipGuard();

        Assert.Equal(2, Ship(guard, Row(1, At(10, 0), 5), Row(2, At(10, 0), 5)).Count);
    }

    [Fact]
    public void UnchangedRow_Drops_AfterCommit()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(10, 0), 5));

        Assert.Empty(Ship(guard, Row(1, At(10, 0), 5)));
    }

    [Fact]
    public void NewerRow_Ships_ByTimeOrByCount()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(10, 0), 5));

        Assert.Single(Ship(guard, Row(1, At(10, 5), 6)));
        Assert.Single(Ship(guard, Row(1, At(10, 0), 6))); // same time, higher count
    }

    [Fact]
    public void OlderRow_AfterAPrimaryRead_Drops()
    {
        var guard = new QueryStoreShipGuard();
        // the primary read wrote the newer state; backfill/primary rows feed the guard
        ShipAndWrite(guard, At(10, 10), Row(1, At(10, 10), 9));

        // a replica read now shows the older state of the same row
        Assert.Empty(Ship(guard, Row(1, At(10, 2), 4)));
        Assert.Empty(Ship(guard, Row(1, At(10, 10), 8)));
    }

    [Fact]
    public void FailedWrite_CommitsNothing_SoTheRowShipsNextRun()
    {
        var guard = new QueryStoreShipGuard();
        Assert.Single(Ship(guard, Row(1, At(10, 0), 5)));
        guard.Discard("Sales"); // the write failed

        Assert.Single(Ship(guard, Row(1, At(10, 0), 5)));
        Assert.Equal(0, guard.EntryCount("Sales"));
    }

    [Fact]
    public void StagedButNeverCommitted_IsNotShipped_AsFarAsTheGuardIsConcerned()
    {
        var guard = new QueryStoreShipGuard();
        Ship(guard, Row(1, At(10, 0), 5)); // staged, then the run died

        Assert.Single(Ship(guard, Row(1, At(10, 0), 5)));
    }

    [Fact]
    public void DifferentKeyFields_AreDifferentKeys()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(10, 0), 5));

        Assert.Single(Ship(guard, Row(1, At(10, 0), 5, interval: 8)));
        Assert.Single(Ship(guard, Row(1, At(10, 0), 5, role: "Secondary")));
        Assert.Single(Ship(guard, Row(1, At(10, 0), 5, exec: "Aborted")));
        Assert.Single(Ship(guard, Row(2, At(10, 0), 5)));
    }

    [Fact]
    public void WithinOneBatch_OnlyTheNewestOfAKeyShips()
    {
        var guard = new QueryStoreShipGuard();

        var shipped = Ship(guard, Row(1, At(10, 0), 5), Row(1, At(10, 3), 6));

        Assert.Equal(2, shipped.Count); // ascending order: the second is newer than the staged first
        guard.Commit("Sales", At(10, 3), L);
        Assert.Empty(Ship(guard, Row(1, At(10, 3), 6)));
    }

    [Fact]
    public void Commit_EvictsEntriesAtOrBelowWatermarkMinusL()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(9, 40), 5), Row(2, At(9, 41), 5), Row(3, At(10, 0), 5));

        // W 10:00, L 20 min: the line is 09:40, entries at or below it go
        Assert.Equal(2, guard.EntryCount("Sales"));
        // 09:40 is outside every later read (cutoff strictly above W - L), so it may ship again if it ever came back
        Assert.Single(Ship(guard, Row(1, At(9, 40), 5)));
        Assert.Empty(Ship(guard, Row(2, At(9, 41), 5)));
    }

    [Fact]
    public void Eviction_UsesTheLargestLookBackSeenForTheDatabase()
    {
        var guard = new QueryStoreShipGuard();
        Ship(guard, Row(1, At(9, 50), 5));
        guard.Commit("Sales", At(10, 0), TimeSpan.FromMinutes(25)); // a capturing secondary appeared: L = 25
        Ship(guard, Row(2, At(9, 42), 5));
        guard.Commit("Sales", At(10, 5), TimeSpan.FromMinutes(20)); // back to L = 20 (the flush interval changed)

        // line = 10:05 - 25 = 09:40, so row 2 (09:42) stays; the current L alone (line 09:45) would have evicted it
        Assert.Equal(2, guard.EntryCount("Sales"));
        Assert.Empty(Ship(guard, Row(2, At(9, 42), 5)));
    }

    [Fact]
    public void WatermarkOnlyRises()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(9, 50), 5));
        ShipAndWrite(guard, At(9, 0), Row(2, At(9, 10), 5)); // a lower W later must not pull the line down

        // line stays 09:40: the 09:10 row was evicted by the second commit, the 09:50 row stays
        Assert.Equal(1, guard.EntryCount("Sales"));
    }

    [Fact]
    public void BackfillRows_FeedTheGuard()
    {
        var guard = new QueryStoreShipGuard();
        guard.RecordShipped("Sales", new[] { Row(1, At(9, 50), 9) });

        Assert.Empty(Ship(guard, Row(1, At(9, 50), 9)));
        Assert.Empty(Ship(guard, Row(1, At(9, 45), 3)));
        Assert.Single(Ship(guard, Row(1, At(9, 55), 10)));
    }

    [Fact]
    public void ADatabaseThatLeavesTheEnumeration_LosesItsMap()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(10, 0), 5));
        guard.Filter("Hr", new[] { Row(1, At(10, 0), 5) });
        guard.Commit("Hr", At(10, 0), L);

        guard.RetainDatabases(new[] { "sales" }); // case-insensitive, like database names

        Assert.True(guard.HasDatabase("Sales"));
        Assert.False(guard.HasDatabase("Hr"));
    }

    [Fact]
    public void ARemovedServer_ClearsEverything()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(10, 0), 5));
        guard.MarkCut("Sales", true);

        guard.Clear();

        Assert.False(guard.HasDatabase("Sales"));
        Assert.False(guard.WasCut("Sales"));
        Assert.Single(Ship(guard, Row(1, At(10, 0), 5)));
    }

    [Fact]
    public void DatabasesDoNotShareState()
    {
        var guard = new QueryStoreShipGuard();
        ShipAndWrite(guard, At(10, 0), Row(1, At(10, 0), 5));

        Assert.Single(guard.Filter("Hr", new[] { Row(1, At(10, 0), 5) }));
    }
}
