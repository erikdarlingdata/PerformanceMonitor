/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's Availability Group alert state machine (#1696) — the twin of Darling's AG evaluator tests. The
/// shared DECISIONS are pinned in <c>AgAlertPolicyTests</c> (duplicated in both suites); what is pinned here
/// is the part Lite owns: the per-grain edge state, the cooldown, recovery, and Forget.
/// </summary>
public class AgAlertEvaluatorTests
{
    private const int ServerId = 4242;
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);

    private static AgReplicaReading Replica(
        string? role = "SECONDARY", string? connected = "CONNECTED", string replica = "NODE2", string ag = "AG1") =>
        new(ag, replica, role, connected);

    private static AgDatabaseReading Database(
        long? lagSeconds = 0, long? redoKb = 0, bool? suspended = false, string? suspendReason = null,
        string database = "Sales", string replica = "NODE2", string ag = "AG1") =>
        new(ag, database, replica, lagSeconds, redoKb, suspended, suspendReason);

    /* ---------------- failover ---------------- */

    [Fact]
    public void Failover_FirstSightingIsSilent_ThenARoleChangeFires()
    {
        var e = new AgAlertEvaluator();

        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") }));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") }));

        var fired = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
        Assert.Equal(AgAlertPolicy.FailoverMetric, fired.MetricName);
        Assert.Equal("PRIMARY", fired.CurrentValue);
        Assert.Equal("SECONDARY", fired.ThresholdValue);
        Assert.False(fired.IsResolution);
    }

    [Fact]
    public void Failover_TracksEachReplicaSeparately()
    {
        var e = new AgAlertEvaluator();

        e.EvaluateReplicas(ServerId, new[]
        {
            Replica(role: "PRIMARY", replica: "NODE1"),
            Replica(role: "SECONDARY", replica: "NODE2"),
        });

        var alerts = e.EvaluateReplicas(ServerId, new[]
        {
            Replica(role: "SECONDARY", replica: "NODE1"),
            Replica(role: "PRIMARY", replica: "NODE2"),
        });

        Assert.Equal(2, alerts.Count);
        Assert.All(alerts, a => Assert.Equal(AgAlertPolicy.FailoverMetric, a.MetricName));
    }

    [Fact]
    public void Failover_NullRoleIsSkipped_AndTheRememberedRoleSurvivesIt()
    {
        var e = new AgAlertEvaluator();

        e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") });
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: null) }));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
    }

    /* ---------------- disconnect / reconnect ---------------- */

    [Fact]
    public void Disconnected_FiresOnTheEdge_DoesNotRepeat_ThenReconnectResolves()
    {
        var e = new AgAlertEvaluator();

        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }));

        var lost = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, lost.MetricName);
        Assert.False(lost.IsResolution);

        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));

        var back = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }));
        Assert.Equal(AgAlertPolicy.ReplicaReconnectedMetric, back.MetricName);
        Assert.True(back.IsResolution);
    }

    /* ---------------- #2426: the disconnect re-fire ---------------- */

    private static readonly DateTime Noon = new(2026, 8, 20, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Refire = TimeSpan.FromMinutes(10);

    [Fact]
    public void DisconnectRefire_ReAnnouncesUnderTheSameMetricName_AndSaysThatItIsStill()
    {
        var now = Noon;
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);

        var lost = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, lost.MetricName);
        Assert.DoesNotContain("STILL", lost.DetailText, StringComparison.Ordinal);
        e.NoteDelivered(lost);

        /* Inside the window there is nothing new to say. */
        now = now.AddMinutes(5);
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        /* Past it: the SAME metric name, because webhook automation keyed on it is what a re-fire exists to
           re-trigger — and a detail that tells an operator reading the history this is hour two, not a
           second outage. */
        now = now.AddMinutes(6);
        var again = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, again.MetricName);
        Assert.False(again.IsResolution);
        Assert.Contains("STILL DISCONNECTED", again.DetailText, StringComparison.Ordinal);
        Assert.Contains("re-alerting every 10 min", again.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public void DisconnectRefire_TheWindowOpensOnDeliveryOnly_NotOnTheDecision()
    {
        var now = Noon;
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);

        /* Evaluated and NOT delivered — exactly what MainWindow does every sweep while a server is
           acknowledged or silenced. */
        Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        /* So nothing consumed the window, and the next sweep still has something to say. Stamping on the
           decision instead would spend window after window on alerts nobody received, and the operator
           would come back from an acknowledgement to silence. */
        now = now.AddMinutes(1);
        var again = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Contains("STILL DISCONNECTED", again.DetailText, StringComparison.Ordinal);

        /* Delivered this time, so the clock finally starts. */
        e.NoteDelivered(again);
        now = now.AddMinutes(1);
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
    }

    [Fact]
    public void DisconnectRefire_ReconnectClearsTheClock_SoTheNextOutageIsNotHeldQuietByTheLastOnes()
    {
        var now = Noon;
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        e.NoteDelivered(Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire)));

        now = now.AddMinutes(1);
        Assert.True(Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire)).IsResolution);

        /* A second outage a minute later whose opening edge is suppressed. With the clock cleared the next
           sweep re-announces; with the first episode's stamp still on it, this replica would sit silent for
           the remaining eight minutes of a window that belongs to an outage already over. */
        now = now.AddMinutes(1);
        Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        now = now.AddMinutes(1);
        var again = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Contains("STILL DISCONNECTED", again.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public void DisconnectRefire_EachReplicaKeepsItsOwnClock()
    {
        var now = Noon;
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateReplicas(ServerId, new[]
        {
            Replica(connected: "CONNECTED", replica: "NODE2"),
            Replica(connected: "CONNECTED", replica: "NODE3"),
        }, Refire);

        var lost = e.EvaluateReplicas(ServerId, new[]
        {
            Replica(connected: "DISCONNECTED", replica: "NODE2"),
            Replica(connected: "DISCONNECTED", replica: "NODE3"),
        }, Refire);
        Assert.Equal(2, lost.Count);

        /* Only NODE2's alert was delivered; NODE3's window is still open. */
        Assert.Contains("NODE2", lost[0].DetailText, StringComparison.Ordinal);
        e.NoteDelivered(lost[0]);

        now = now.AddMinutes(1);
        var again = Assert.Single(e.EvaluateReplicas(ServerId, new[]
        {
            Replica(connected: "DISCONNECTED", replica: "NODE2"),
            Replica(connected: "DISCONNECTED", replica: "NODE3"),
        }, Refire));
        Assert.Contains("NODE3", again.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public void DisconnectRefire_Forget_DropsTheClockWithTheRestOfTheState()
    {
        var now = Noon;
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        e.NoteDelivered(Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire)));

        e.Forget(ServerId);

        /* Re-added a minute later: a first sighting again, and with re-fire on that announces rather than
           baselining silently. A clock that survived Forget would hold it quiet for nine more minutes. */
        now = now.AddMinutes(1);
        var again = Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Contains("STILL DISCONNECTED", again.DetailText, StringComparison.Ordinal);
    }

    /* ---------------- suspended ---------------- */

    [Fact]
    public void Suspended_FiresOnTheEdgeWithTheReason_ThenResumeIsAResolution()
    {
        var e = new AgAlertEvaluator();

        Assert.Empty(e.EvaluateDatabases(ServerId, new[] { Database(suspended: false) }, 300, 0, Cooldown));

        var fired = Assert.Single(e.EvaluateDatabases(
            ServerId, new[] { Database(suspended: true, suspendReason: "SUSPEND_FROM_USER") }, 300, 0, Cooldown));
        Assert.Equal(AgAlertPolicy.DatabaseSuspendedMetric, fired.MetricName);
        Assert.Equal("SUSPEND_FROM_USER", fired.CurrentValue);
        Assert.Contains("SET HADR RESUME", fired.DetailText, StringComparison.Ordinal);

        var resumed = Assert.Single(e.EvaluateDatabases(
            ServerId, new[] { Database(suspended: false) }, 300, 0, Cooldown));
        Assert.Equal("AG Data Movement Resumed", resumed.MetricName);
        Assert.True(resumed.IsResolution);
    }

    [Fact]
    public void DatabaseScopedAlerts_CarryTheDiscreteDatabaseFacts()
    {
        /* #2109: the database-scoped AG alerts carry Database / Availability Group / Replica as
           discrete fields (the wire contract downstream automation routes on), via the SAME shared
           builder Darling's evaluator uses — the fact names cannot drift between the SKUs. */
        var e = new AgAlertEvaluator();

        /* Suspension is edge-triggered with first-sighting-silent semantics — establish the healthy
           baseline first, exactly like the edge test above. */
        Assert.Empty(e.EvaluateDatabases(ServerId, new[] { Database(suspended: false) }, 300, 0, Cooldown));

        var suspended = Assert.Single(e.EvaluateDatabases(
            ServerId, new[] { Database(suspended: true, suspendReason: "SUSPEND_FROM_USER") }, 300, 0, Cooldown));
        var item = Assert.Single(suspended.Context!.Details);
        Assert.Contains(item.Fields, f => f.Label == "Database");
        Assert.Contains(item.Fields, f => f.Label == "Availability Group");
        Assert.Contains(item.Fields, f => f.Label == "Replica");
        Assert.Contains(("Suspend Reason", "SUSPEND_FROM_USER"), item.Fields);

        var behind = Assert.Single(
            e.EvaluateDatabases(
                ServerId, new[] { Database(suspended: false, lagSeconds: 600) }, 300, 0, Cooldown),
            a => a.MetricName == AgAlertPolicy.SyncFellBehindMetric);
        Assert.Contains(Assert.Single(behind.Context!.Details).Fields, f => f.Label == "Database");

        /* Resolutions stay context-less — they carry no database-scoped payload to route on. */
        var resumed = Assert.Single(
            e.EvaluateDatabases(ServerId, new[] { Database(suspended: false) }, 300, 0, Cooldown),
            a => a.IsResolution);
        Assert.Null(resumed.Context);
    }

    /* ---------------- sync fell behind ---------------- */

    [Fact]
    public void SyncFellBehind_FiresOnce_ReFiresOnlyOnCooldown_ThenRecovers()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var e = new AgAlertEvaluator(() => now);

        var fired = Assert.Single(e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown));
        Assert.Equal(AgAlertPolicy.SyncFellBehindMetric, fired.MetricName);

        now = now.AddMinutes(2);
        Assert.Empty(e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown));

        now = now.AddMinutes(4);
        Assert.Single(e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown));

        var recovered = Assert.Single(e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 1) }, 300, 0, Cooldown));
        Assert.Equal("AG Sync Recovered", recovered.MetricName);
        Assert.True(recovered.IsResolution);
        Assert.Contains("Sales", recovered.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public void SyncFellBehind_SuspendedSecondaryDriftingPastTheThreshold_StillFires()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 0) }, 300, 0, Cooldown);

        /* Lag ACCRUES while suspended (measured on a live AG, contradicting MS Learn), so this is the case
           an operator most needs paged for and it must not be swallowed by the suspend alert. */
        var alerts = e.EvaluateDatabases(
            ServerId, new[] { Database(lagSeconds: 600, suspended: true, suspendReason: "SUSPEND_FROM_USER") },
            300, 0, Cooldown);

        Assert.Contains(alerts, a => a.MetricName == AgAlertPolicy.DatabaseSuspendedMetric);
        Assert.Contains(alerts, a => a.MetricName == AgAlertPolicy.SyncFellBehindMetric);
    }

    [Fact]
    public void SyncFellBehind_SuspendOrQuorumLoss_NeverAnnouncesRecovery()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 0) }, 300, 0, Cooldown);
        e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown);

        /* A lagging database that becomes SUSPENDED reports lag 0 — it got worse, not better. */
        var suspended = e.EvaluateDatabases(
            ServerId, new[] { Database(lagSeconds: 0, suspended: true) }, 300, 0, Cooldown);
        Assert.DoesNotContain(suspended, a => a.MetricName == "AG Sync Recovered");

        /* Quorum loss nulls the columns: still no recovery. */
        var quorumLoss = e.EvaluateDatabases(
            ServerId, new[] { Database(lagSeconds: null, redoKb: null, suspended: null) }, 300, 0, Cooldown);
        Assert.DoesNotContain(quorumLoss, a => a.MetricName == "AG Sync Recovered");

        /* A genuine measured recovery still resolves it. */
        var recovered = e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 0) }, 300, 0, Cooldown);
        Assert.Contains(recovered, a => a.MetricName == "AG Sync Recovered");
    }

    [Fact]
    public void SyncFellBehind_TracksEachDatabaseIndependently()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var e = new AgAlertEvaluator(() => now);

        Assert.Single(e.EvaluateDatabases(ServerId, new[]
        {
            Database(lagSeconds: 600, database: "Sales"),
            Database(lagSeconds: 0, database: "Orders"),
        }, 300, 0, Cooldown));

        /* Orders falls behind INSIDE Sales's cooldown window and must not hide behind it. */
        now = now.AddMinutes(1);
        var second = Assert.Single(e.EvaluateDatabases(ServerId, new[]
        {
            Database(lagSeconds: 600, database: "Sales"),
            Database(lagSeconds: 600, database: "Orders"),
        }, 300, 0, Cooldown));
        Assert.Contains("Orders", second.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public void RecoverySweep_IsScopedToTheServerBeingEvaluated()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var e = new AgAlertEvaluator(() => now);
        const int otherServerId = 515151;

        e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown);
        e.EvaluateDatabases(otherServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown);

        /* This server catches up. Its snapshot says nothing about the other server, whose alert must stand. */
        var recovered = e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 0) }, 300, 0, Cooldown);
        Assert.Single(recovered, a => a.MetricName == "AG Sync Recovered");

        /* The other server is still inside its cooldown, so it stays quiet rather than re-firing fresh. */
        Assert.Empty(e.EvaluateDatabases(otherServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown));
    }

    /* ---------------- forget ---------------- */

    [Fact]
    public void Forget_DropsCompositeKeyedState_SoAReAddedServerReBaselines()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var e = new AgAlertEvaluator(() => now);

        e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") });
        e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown);

        e.Forget(ServerId);

        /* Re-added: the role is a fresh baseline, so no phantom failover ... */
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") }));

        /* ... and the sync episode starts over rather than sitting inside the dropped cooldown stamp. */
        Assert.Single(e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 600) }, 300, 0, Cooldown));
    }

    [Fact]
    public void Forget_DoesNotTouchAnotherServerWhoseIdSharesADigitPrefix()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var e = new AgAlertEvaluator(() => now);

        /* Server 4242 and server 42 would collide under a naive StartsWith on the id alone; the key
           separator is what keeps them apart. */
        e.EvaluateReplicas(42, new[] { Replica(role: "PRIMARY") });
        e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") });

        e.Forget(42);

        /* 4242's baseline survived, so its role change still fires. */
        Assert.Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") }));
    }
}
