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
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The pure identity contract the deadlocks collector hands both hosts: a deadlock is dropped only when a
/// stored row carries the same microsecond time and the identical graph text, and an in-batch exact repeat
/// keeps one row.
/// </summary>
public sealed class DeadlockStoredIdentityTests
{
    private static readonly DateTime T = new(2026, 9, 30, 12, 0, 0, DateTimeKind.Unspecified);
    private const string Graph = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";

    private static DeadlocksCollector.Row Row(DateTime? time, string? graph, string? database = "GP", string? plan = null) => new()
    {
        DeadlockTime = time,
        GraphXml = graph,
        DatabaseName = database,
        VictimQueryPlanXml = plan,
    };

    private static HashSet<(DateTime Time, string Graph)> Stored(params (DateTime, string)[] identities) =>
        new(identities, new IdentityComparer());

    /* The host loads the stored identities into an ordinal set; the pure drop must hold with the default
       tuple comparer too, which compares the string ordinally. */
    private sealed class IdentityComparer : IEqualityComparer<(DateTime Time, string Graph)>
    {
        public bool Equals((DateTime Time, string Graph) x, (DateTime Time, string Graph) y) =>
            x.Time == y.Time && string.Equals(x.Graph, y.Graph, StringComparison.Ordinal);

        public int GetHashCode((DateTime Time, string Graph) obj) => HashCode.Combine(obj.Time, obj.Graph);
    }

    private static List<DeadlocksCollector.Row> Drop(List<DeadlocksCollector.Row> rows, HashSet<(DateTime Time, string Graph)> stored) =>
        DeadlocksCollector.Instance.DropAlreadyStored(rows, stored);

    [Fact]
    public void ATelemetryCopyOfAStoredRingBufferRow_IsDropped()
    {
        var stored = Stored((T, Graph));
        var telemetry = Row(T, Graph, "GP", plan: null);

        Assert.Empty(Drop(new List<DeadlocksCollector.Row> { telemetry }, stored));
    }

    [Fact]
    public void ABlobOnlyRow_IsKept()
    {
        var stored = Stored((T, Graph));
        var other = Row(T.AddMinutes(1), Graph.Replace("process1", "process9", StringComparison.Ordinal), "HS");

        Assert.Single(Drop(new List<DeadlocksCollector.Row> { other }, stored));
    }

    [Fact]
    public void ARingBufferOnlyRow_IsKept()
    {
        var kept = Drop(new List<DeadlocksCollector.Row> { Row(T, Graph, "GP", plan: "<ShowPlanXML/>") }, Stored());

        Assert.Single(kept);
    }

    [Fact]
    public void TheSameTimeWithAGraphDifferingByOneAttribute_IsKept()
    {
        var stored = Stored((T, Graph));
        var near = Row(T, Graph.Replace("process1", "process2", StringComparison.Ordinal));

        Assert.Single(Drop(new List<DeadlocksCollector.Row> { near }, stored));
    }

    [Fact]
    public void ANullOrEmptyGraphOrANullTime_IsNeverDropped()
    {
        var stored = Stored((T, Graph), (T, ""));
        var rows = new List<DeadlocksCollector.Row>
        {
            Row(T, null),
            Row(T, ""),
            Row(null, Graph),
            Row(T, null),
        };

        Assert.Equal(4, Drop(rows, stored).Count);
        Assert.Null(DeadlocksCollector.Instance.GetIdentity(rows[0]));
        Assert.Null(DeadlocksCollector.Instance.GetIdentity(rows[1]));
        Assert.Null(DeadlocksCollector.Instance.GetIdentity(rows[2]));
    }

    [Fact]
    public void AnInBatchExactRepeat_KeepsOnlyTheFirst_AndAnInBatchNearMissKeepsBoth()
    {
        var first = Row(T, Graph, "master");
        var repeat = Row(T, Graph, "master", plan: "<ShowPlanXML/>");
        var near = Row(T, Graph.Replace("process1", "process2", StringComparison.Ordinal), "master");

        var kept = Drop(new List<DeadlocksCollector.Row> { first, repeat, near }, Stored());

        Assert.Equal(2, kept.Count);
        Assert.Same(first, kept[0]);
        Assert.Same(near, kept[1]);
    }

    [Fact]
    public void ATimeAt100NanosecondsMatchesTheStoredMicrosecond()
    {
        var stored = Stored((T.AddTicks(10), Graph));
        var telemetry = Row(T.AddTicks(10).AddTicks(7), Graph);

        Assert.Empty(Drop(new List<DeadlocksCollector.Row> { telemetry }, stored));
        Assert.Equal(T.AddTicks(10), DeadlocksCollector.Instance.GetIdentity(telemetry)!.Value.Time);
    }
}
