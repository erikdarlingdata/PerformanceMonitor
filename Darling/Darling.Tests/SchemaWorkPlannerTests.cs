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

/// <summary>#7643: what the pass does for each database, from the stored cursor, the server's state and the events read.</summary>
[Trait("Stage", "Guard")]
public class SchemaWorkPlannerTests
{
    private const int ServerId = 7;
    private static readonly DateTime Session1 = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Pass = new(2026, 11, 1, 9, 0, 0, DateTimeKind.Utc);
    private static readonly Guid Fork = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid Group = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly string[] SalesAndOps = ["SalesDb", "OpsDb"];

    private const int UserTable = 8277;
    private const int View = 8278;
    private const int Trigger = 21076;
    private const int Index = 22601;
    private const int SchemaObject = 17235;

    private static SchemaCaptureStateRow Db(string name, Func<SchemaCaptureStateRow, SchemaCaptureStateRow>? change = null)
    {
        var row = new SchemaCaptureStateRow
        {
            ServerId = ServerId, DatabaseName = name, RecoveryForkGuid = Fork, HistoryServerId = ServerId,
            WasPrimary = false, BaselineAt = Pass.AddDays(-1), LastSeenInScopeAt = Pass,
        };
        return change is null ? row : change(row);
    }

    private static SchemaCaptureStateRow Server()
    {
        var row = new SchemaCaptureStateRow
        {
            ServerId = ServerId, DatabaseName = string.Empty, SessionCreateTime = Session1, LastEventSequence = 10,
            LastEventTime = Pass.AddMinutes(-5), TotalEventsProcessed = 10, LastSeenInScopeAt = Pass,
        };
        return row;
    }

    private static SchemaDatabaseProbe Probe(string name) =>
        new(name, 5, "ONLINE", false, false, null, true, Fork, null, null, null);

    private static SchemaEvent Event(
        long sequence, string database, string name = "object_altered", int type = UserTable, int? objectId = 100,
        int? relatedId = null) =>
        new(name, sequence, Pass, database, objectId, type, null, "Orders", "login1", "app1", "host1", relatedId);

    private static SchemaEventBatch Batch(long total, params SchemaEvent[] events) =>
        new(Session1, total, false, events);

    private static readonly SchemaSessionState Running = new(true, Session1, null, false);

    private static IReadOnlyList<SchemaDatabasePlan> Plan(
        IReadOnlyList<SchemaDatabaseProbe> probes, IReadOnlyDictionary<string, SchemaCaptureStateRow> states,
        SchemaEventBatch? batch, SchemaCaptureStateRow? server = null, bool runNow = false,
        SchemaSessionState? session = null, IReadOnlyCollection<string>? inScope = null) =>
        SchemaWorkPlanner.Plan(
            ServerId, server ?? Server(), states, session ?? Running, batch, probes, inScope ?? SalesAndOps, runNow);

    private static Dictionary<string, SchemaCaptureStateRow> States(params SchemaCaptureStateRow[] rows) =>
        rows.ToDictionary(r => r.DatabaseName, StringComparer.Ordinal);

    private static SchemaDatabasePlan For(IReadOnlyList<SchemaDatabasePlan> plans, string database) =>
        plans.Single(p => p.DatabaseName == database);

    [Fact]
    public void Planner_NoState_Baselines()
    {
        var plans = Plan([Probe("SalesDb")], States(), Batch(10), server: null);

        var plan = For(plans, "SalesDb");
        Assert.Equal(SchemaDatabaseWork.Baseline, plan.Work);
        Assert.Equal(new SchemaHistoryKey(ServerId, null, null), plan.Key);
    }

    [Fact]
    public void Planner_RunNow_ReconcilesWithReasonRunNow()
    {
        var plan = For(Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(10), runNow: true), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Reconcile, plan.Work);
        Assert.Equal("run_now", plan.Reason);
    }

    [Theory]
    [InlineData("secondary")]
    [InlineData("standby")]
    [InlineData("readonly")]
    [InlineData("restoring")]
    [InlineData("snapshot")]
    [InlineData("noaccess")]
    public void Planner_SecondaryDatabase_IsSkippedWithReason(string kind)
    {
        var probe = kind switch
        {
            "secondary" => Probe("SalesDb") with { AgGroupId = Group, AgName = "Ag1", IsPrimaryReplica = false },
            "standby" => Probe("SalesDb") with { IsInStandby = true },
            "readonly" => Probe("SalesDb") with { IsReadOnly = true },
            "restoring" => Probe("SalesDb") with { StateDesc = "RESTORING" },
            "snapshot" => Probe("SalesDb") with { SourceDatabaseId = 9 },
            _ => Probe("SalesDb") with { HasAccess = false },
        };

        var plan = For(Plan([probe], States(Db("SalesDb")), Batch(10), runNow: true), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Skip, plan.Work);
        Assert.False(string.IsNullOrEmpty(plan.Reason));
        Assert.Empty(plan.ObjectIds);
    }

    [Fact]
    public void Planner_SessionCreateTimeChanged_ReconcilesEveryInScopeDatabase()
    {
        var restarted = new SchemaEventBatch(Session1.AddHours(1), 0, false, []);
        var session = new SchemaSessionState(true, Session1.AddHours(1), null, false);

        var plans = Plan(
            [Probe("SalesDb"), Probe("OpsDb"), Probe("OtherDb")],
            States(Db("SalesDb"), Db("OpsDb"), Db("OtherDb")), restarted, session: session);

        Assert.All(plans, p => Assert.Equal("session_restarted", p.Reason));
        Assert.Equal(["OpsDb", "SalesDb"], plans.Select(p => p.DatabaseName).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Planner_SessionStartedAfterRunningWithout_ReconcilesEveryInScopeDatabase()
    {
        /* The stored server row holds no create time: the baseline ran with the session down. */
        var server = Server() with { SessionCreateTime = null, LastEventSequence = null, LastEventTime = null, TotalEventsProcessed = null };

        var plans = Plan(
            [Probe("SalesDb"), Probe("OpsDb")], States(Db("SalesDb"), Db("OpsDb")), Batch(0), server: server);

        Assert.All(plans, p => Assert.Equal("session_started", p.Reason));
        Assert.All(plans, p => Assert.Equal(SchemaDatabaseWork.Reconcile, p.Work));
        Assert.Equal(2, plans.Count);
    }

    [Fact]
    public void Planner_SessionCreateTimeJitterOfAFewMilliseconds_IsNotARestart()
    {
        var session = new SchemaSessionState(true, Session1.AddMilliseconds(3), null, false);

        var plan = For(Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(10), session: session), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Skip, plan.Work);
        Assert.Null(plan.Reason);
    }

    [Fact]
    public void Planner_SessionNotRunning_Skips()
    {
        var plan = For(
            Plan([Probe("SalesDb")], States(Db("SalesDb")), null, session: new SchemaSessionState(false, null, null, false)),
            "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Skip, plan.Work);
        Assert.Contains("change tracking", plan.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Planner_SequenceGapWithSameSession_ReconcilesEveryInScopeDatabase()
    {
        var plans = Plan(
            [Probe("SalesDb"), Probe("OpsDb")], States(Db("SalesDb"), Db("OpsDb")),
            Batch(11, Event(12, "SalesDb")));

        Assert.All(plans, p => Assert.Equal("events_lost", p.Reason));
        Assert.Equal(2, plans.Count);
    }

    [Fact]
    public void Planner_TotalEventsProcessedAheadOfWhatWasRead_Reconciles()
    {
        var plan = For(
            Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(15, Event(11, "SalesDb"), Event(12, "SalesDb"))),
            "SalesDb");

        Assert.Equal("events_lost", plan.Reason);
    }

    [Fact]
    public void Planner_TruncatedXml_Reconciles()
    {
        var batch = new SchemaEventBatch(Session1, 11, true, [Event(11, "SalesDb")]);

        Assert.Equal("events_lost", For(Plan([Probe("SalesDb")], States(Db("SalesDb")), batch), "SalesDb").Reason);
    }

    [Fact]
    public void Planner_NoStoredServerState_IsNotAGap()
    {
        var plan = For(
            Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(40, Event(30, "SalesDb")), server: new SchemaCaptureStateRow()),
            "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Targeted, plan.Work);
    }

    [Fact]
    public void Planner_EventsAlreadySeen_AreIgnoredAndRaiseNoGap()
    {
        var batch = Batch(11, Event(9, "SalesDb", objectId: 90), Event(10, "SalesDb", objectId: 91), Event(11, "SalesDb", objectId: 92));

        var plan = For(Plan([Probe("SalesDb")], States(Db("SalesDb")), batch), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Targeted, plan.Work);
        Assert.Equal([92], plan.ObjectIds.ToArray());
        Assert.Equal([11L], plan.Attribution.Select(e => e.Sequence).ToArray());
    }

    [Fact]
    public void Planner_OnlyEventsAlreadySeen_Skips()
    {
        var plan = For(Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(10, Event(10, "SalesDb"))), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Skip, plan.Work);
        Assert.Null(plan.Reason);
    }

    [Fact]
    public void Planner_ForkGuidChanged_ReconcilesThatDatabaseOnly()
    {
        var plans = Plan(
            [Probe("SalesDb") with { RecoveryForkGuid = Guid.NewGuid() }, Probe("OpsDb")],
            States(Db("SalesDb"), Db("OpsDb")), Batch(10));

        Assert.Equal("restored", For(plans, "SalesDb").Reason);
        Assert.Equal(SchemaDatabaseWork.Skip, For(plans, "OpsDb").Work);
    }

    [Fact]
    public void Planner_BecamePrimary_Reconciles()
    {
        var primary = Probe("SalesDb") with { AgGroupId = Group, AgName = "Ag1", IsPrimaryReplica = true };

        var plan = For(Plan([primary], States(Db("SalesDb", r => r with { WasPrimary = false })), Batch(10)), "SalesDb");

        Assert.Equal("became_primary", plan.Reason);
        Assert.Equal(new SchemaHistoryKey(null, Group, "Ag1"), plan.Key);
    }

    [Fact]
    public void Planner_StillPrimary_DoesNotReconcile()
    {
        var primary = Probe("SalesDb") with { AgGroupId = Group, AgName = "Ag1", IsPrimaryReplica = true };

        var plan = For(Plan([primary], States(Db("SalesDb", r => r with { WasPrimary = true })), Batch(10)), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Skip, plan.Work);
    }

    [Fact]
    public void Planner_DatabaseReturnsToScope_ReconcilesIt()
    {
        var states = States(Db("SalesDb", r => r with { LastSeenInScopeAt = Pass.AddMinutes(-15) }), Db("OpsDb"));

        var plans = Plan([Probe("SalesDb"), Probe("OpsDb")], states, Batch(10));

        Assert.Equal("returned_to_scope", For(plans, "SalesDb").Reason);
        Assert.Equal(SchemaDatabaseWork.Skip, For(plans, "OpsDb").Work);
    }

    [Fact]
    public void Planner_SchemaTransfer_Reconciles()
    {
        var plan = For(
            Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(11, Event(11, "SalesDb", type: SchemaObject, objectId: 3))),
            "SalesDb");

        Assert.Equal("schema_transfer", plan.Reason);
    }

    [Fact]
    public void Planner_TableAndIndexEvents_TargetTheTable()
    {
        var batch = Batch(
            14,
            Event(11, "SalesDb", "object_created", UserTable, 100),
            Event(12, "SalesDb", "object_altered", Index, 200),
            Event(13, "SalesDb", "object_deleted", View, 300),
            Event(14, "SalesDb", "object_created", Trigger, 400, relatedId: 500));

        var plan = For(Plan([Probe("SalesDb")], States(Db("SalesDb")), batch), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Targeted, plan.Work);
        Assert.Equal([100, 200, 300, 500], plan.ObjectIds.Order().ToArray());
        Assert.Equal(4, plan.Attribution.Count);
    }

    [Fact]
    public void Planner_TriggerEventWithoutParentTable_ReconcilesItsDatabase()
    {
        var plan = For(
            Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(11, Event(11, "SalesDb", "object_deleted", Trigger, 400))),
            "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Reconcile, plan.Work);
        Assert.Equal("trigger_changed", plan.Reason);
    }

    [Fact]
    public void Planner_RenameEvent_ReconcilesItsDatabase()
    {
        var rename = Event(11, "SalesDb", "module_end", type: 0, objectId: 77);

        var plans = Plan([Probe("SalesDb"), Probe("OpsDb")], States(Db("SalesDb"), Db("OpsDb")), Batch(11, rename));

        Assert.Equal("renamed", For(plans, "SalesDb").Reason);
        Assert.Equal(SchemaDatabaseWork.Skip, For(plans, "OpsDb").Work);
    }

    [Fact]
    public void Planner_OutOfScopeDatabaseEvents_AreIgnored()
    {
        var batch = Batch(12, Event(11, "OtherDb"), Event(12, "OtherDb", "module_end", 0, 77));

        var plans = Plan([Probe("SalesDb"), Probe("OtherDb")], States(Db("SalesDb"), Db("OtherDb")), batch);

        Assert.Equal(["SalesDb"], plans.Select(p => p.DatabaseName).ToArray());
        Assert.Equal(SchemaDatabaseWork.Skip, plans[0].Work);
    }

    [Fact]
    public void Planner_OutOfScopeEvents_StillCountForSequenceContinuity()
    {
        var batch = Batch(12, Event(11, "OtherDb"), Event(12, "SalesDb"));

        var plan = For(Plan([Probe("SalesDb"), Probe("OtherDb")], States(Db("SalesDb")), batch), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Targeted, plan.Work);
    }

    [Fact]
    public void Planner_NoEvents_Skips()
    {
        var plan = For(Plan([Probe("SalesDb")], States(Db("SalesDb")), Batch(10)), "SalesDb");

        Assert.Equal(SchemaDatabaseWork.Skip, plan.Work);
        Assert.Null(plan.Reason);
    }
}
