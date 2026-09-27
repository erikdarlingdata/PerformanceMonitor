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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's Availability Groups tab (#991): the banding rules and the pure card projection. Ungated — this
/// half is separated from the Postgres half exactly so the rules test without a store, mirroring FleetViewTests.
/// The read SQL's dialect and latest-snapshot shape are pinned once, in <c>DarlingAgStatesReaderTests</c>
/// (#4228) — this file and <c>DarlingAgReaderTests</c> both call the SAME <c>DarlingAgStatesReader</c> now, so
/// there is one statement to disagree about instead of two.
/// </summary>
public sealed class AgTopologyCardsTests
{
    /* The SQL pins used to live here, against this file's own copy of the statement text (the viewer had no
       route to the service assembly that carried the other copy). Both copies moved to DarlingAgStatesReader
       in PerformanceMonitor.Darling.Storage (#4228), which this project already references, and the dialect /
       shape pins moved with it to DarlingAgStatesReaderTests — one set of pins for the one implementation,
       instead of two that could drift apart. */

    [Theory]
    [InlineData(true, "local")]
    [InlineData(false, "")]
    [InlineData(null, "")]
    public void LocalDisplay_ShownOnlyWhenExplicitlyLocal(bool? isLocal, string expected)
    {
        /* Mirror of Lite's pin. NULL is UNKNOWN, not remote — rows predating the column do not know. */
        var replica = new AgTopologyReplicaRow
        {
            ServerId = 1,
            ServerName = "NODE1",
            AgName = "AG1",
            ReplicaServerName = "NODE1",
            RoleDesc = "PRIMARY",
            IsLocal = isLocal,
        };

        var card = Assert.Single(AgTopology.BuildCards(new[] { replica }, Array.Empty<AgTopologyDatabaseRow>()));
        Assert.Equal(expected, Assert.Single(card.Replicas).LocalDisplay);
    }

    /* ─────────────────────────── banding ─────────────────────────── */

    [Theory]
    [InlineData("HEALTHY", HealthSeverity.Healthy)]
    [InlineData("PARTIALLY_HEALTHY", HealthSeverity.Warning)]
    [InlineData("NOT_HEALTHY", HealthSeverity.Critical)]
    [InlineData(null, HealthSeverity.Unknown)]
    public void SynchronizationHealth_BandsHealthyPartiallyNot(string? desc, HealthSeverity expected)
    {
        Assert.Equal(expected, AgTopology.SynchronizationHealthSeverity(desc));
    }

    [Fact]
    public void OperationalState_NullBandsUnknown_AndDoesNotClaimRecoveryHealthsValue()
    {
        /* operational_state_desc is reported for the LOCAL replica only, so every remote replica's row carries
           NULL — banding that Critical would paint a healthy AG red from every secondary's perspective. And
           ONLINE_IN_PROGRESS belongs to recovery_health_desc, not this column. */
        Assert.Equal(HealthSeverity.Unknown, AgTopology.OperationalStateSeverity(null));
        Assert.Equal(HealthSeverity.Unknown, AgTopology.OperationalStateSeverity("ONLINE_IN_PROGRESS"));
        Assert.Equal(HealthSeverity.Warning, AgTopology.RecoveryHealthSeverity("ONLINE_IN_PROGRESS"));
    }

    [Fact]
    public void DatabaseSync_SynchronizingIsHealthyOnAsync_WarningOnSync()
    {
        /* The load-bearing distinction: an ASYNCHRONOUS_COMMIT replica never reaches SYNCHRONIZED, so
           SYNCHRONIZING is its correct steady state; on SYNCHRONOUS_COMMIT the same text means the replica is
           not currently protecting a commit. A flat state map necessarily gets one of these wrong. */
        Assert.Equal(HealthSeverity.Healthy, AgTopology.DatabaseSyncSeverity("SYNCHRONIZING", "ASYNCHRONOUS_COMMIT", false));
        Assert.Equal(HealthSeverity.Warning, AgTopology.DatabaseSyncSeverity("SYNCHRONIZING", "SYNCHRONOUS_COMMIT", false));
    }

    [Theory]
    [InlineData("NOT SYNCHRONIZING")]
    [InlineData("NOT_SYNCHRONIZING")]
    public void DatabaseSync_AcceptsBothSpellingsOfNotSynchronizing(string state)
    {
        /* The database grain reports states space-separated where the replica grain uses underscores; both are
           stored verbatim, so the banding must normalize. */
        Assert.Equal(HealthSeverity.Critical, AgTopology.DatabaseSyncSeverity(state, "SYNCHRONOUS_COMMIT", false));
    }

    [Fact]
    public void DatabaseSync_SuspendedIsCritical_EvenWhenTheStateReadsSynchronized()
    {
        /* secondary_lag_seconds reads 0 — not null — while data movement is suspended, so a suspended replica
           otherwise presents as perfectly caught up. Suspension outranks the state text. */
        Assert.Equal(HealthSeverity.Critical, AgTopology.DatabaseSyncSeverity("SYNCHRONIZED", "SYNCHRONOUS_COMMIT", true));
    }

    [Fact]
    public void DrainMinutes_EmptyQueueIsZero_StalledQueueHasNoEstimate()
    {
        Assert.Equal<double?>(0, AgTopology.DrainMinutes(0, 0));
        Assert.Equal<double?>(1.0, AgTopology.DrainMinutes(6000, 100));
        Assert.Null(AgTopology.DrainMinutes(5000, 0));
        Assert.Null(AgTopology.DrainMinutes(null, 100));
    }

    /* ─────────────────────────── card projection ─────────────────────────── */

    private static AgTopologyReplicaRow Replica(
        int serverId, string serverName, string agName, string replicaName, string role,
        string syncHealth = "HEALTHY", string? connected = "CONNECTED", string? operational = "ONLINE") =>
        new()
        {
            ServerId = serverId,
            ServerName = serverName,
            CollectionTime = new DateTime(2026, 7, 26, 12, 0, 0),
            AgName = agName,
            ReplicaServerName = replicaName,
            RoleDesc = role,
            OperationalStateDesc = operational,
            ConnectedStateDesc = connected,
            RecoveryHealthDesc = "ONLINE",
            SynchronizationHealthDesc = syncHealth,
            AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
            FailoverModeDesc = "AUTOMATIC",
        };

    private static AgTopologyDatabaseRow Database(
        int serverId, string serverName, string agName, string db, string replicaName,
        string state = "SYNCHRONIZED", bool suspended = false) =>
        new()
        {
            ServerId = serverId,
            ServerName = serverName,
            CollectionTime = new DateTime(2026, 7, 26, 12, 0, 0),
            AgName = agName,
            DatabaseName = db,
            ReplicaServerName = replicaName,
            SynchronizationStateDesc = state,
            AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
            IsSuspended = suspended,
            SuspendReasonDesc = suspended ? "SUSPEND_FROM_USER" : null,
            SecondaryLagSeconds = 0,
        };

    [Fact]
    public void Build_OneAgSeenFromTwoServers_StaysTwoCardsEachNamingItsReporter()
    {
        /* The behavior the whole surface hangs on, and the one observed live on the AG fixture: the primary
           reports both replicas, the secondary reports only itself and no primary. Merging them would let the
           secondary's blind view overwrite the primary's complete one. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY", operational: null),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());

        Assert.Equal(2, cards.Count);
        var fromPrimary = Assert.Single(cards, c => c.ServerName == "NODE1");
        var fromSecondary = Assert.Single(cards, c => c.ServerName == "NODE2");

        Assert.Equal(2, fromPrimary.Replicas.Count);
        Assert.Equal("NODE1", fromPrimary.PrimaryReplica);

        Assert.Single(fromSecondary.Replicas);
        Assert.Null(fromSecondary.PrimaryReplica);
        Assert.Equal("No primary reported", fromSecondary.PrimaryDisplay);
    }

    [Fact]
    public void Build_UnknownDoesNotMaskHealthy_InTheCardRollUp()
    {
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY", connected: null, operational: null),
        };

        var card = Assert.Single(AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>()));
        Assert.Equal(HealthSeverity.Healthy, card.Severity);
        Assert.Equal("Healthy", card.SeverityLabel);
    }

    [Fact]
    public void Build_SuspendedDatabaseRedensTheCard_EvenWhenEveryReplicaIsHealthy()
    {
        var replicas = new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY") };
        var databases = new[] { Database(1, "NODE1", "AG1", "Sales", "NODE2", suspended: true) };

        var card = Assert.Single(AgTopology.BuildCards(replicas, databases));

        Assert.Equal(HealthSeverity.Critical, card.Severity);
        Assert.Equal(HealthSeverity.Healthy, card.Replicas[0].Severity);
        Assert.Contains("Suspended: SUSPEND_FROM_USER", Assert.Single(card.Databases).DataMovementDisplay, StringComparison.Ordinal);
    }

    [Fact]
    public void Build_DatabasesAttachToTheirOwnReportingServersCard()
    {
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "PRIMARY"),
        };
        var databases = new[] { Database(1, "NODE1", "AG1", "Sales", "NODE2") };

        var cards = AgTopology.BuildCards(replicas, databases);

        Assert.Single(Assert.Single(cards, c => c.ServerName == "NODE1").Databases);
        Assert.Empty(Assert.Single(cards, c => c.ServerName == "NODE2").Databases);
    }

    [Fact]
    public void Build_SortsWorstFirst()
    {
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG_CALM", "NODE1", "PRIMARY"),
            Replica(2, "NODE2", "AG_BROKEN", "NODE2", "PRIMARY", syncHealth: "NOT_HEALTHY"),
            Replica(3, "NODE3", "AG_SHAKY", "NODE3", "PRIMARY", syncHealth: "PARTIALLY_HEALTHY"),
        };

        Assert.Equal(
            new[] { "AG_BROKEN", "AG_SHAKY", "AG_CALM" },
            AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>()).Select(c => c.AgName).ToArray());
    }

    [Fact]
    public void Build_EmptyInputYieldsNoCards()
    {
        Assert.Empty(AgTopology.BuildCards(Array.Empty<AgTopologyReplicaRow>(), Array.Empty<AgTopologyDatabaseRow>()));
    }

    /* ─────────────────────────── header summary ─────────────────────────── */

    [Fact]
    public void Summary_DistinguishesDistinctGroupsFromViews()
    {
        /* A reader seeing the same AG name on two cards needs the difference stated, not inferred. */
        /* the primary's view lists every replica; a secondary's lists only itself (#4475) */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var summary = AvailabilityGroupsTab.BuildSummary(AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>()));

        Assert.Equal("1 group · 2 reporting servers · 2 views", summary);
    }

    [Fact]
    public void Summary_EmptyFleetSaysSo()
    {
        Assert.Equal("none observed", AvailabilityGroupsTab.BuildSummary(Array.Empty<AgTopologyCard>()));
    }

    /* ─────────────────────────── #4475: group identity is name + replica set ─────────────────────────── */

    [Fact]
    public void Counts_FortyTwoSameNamedAgsWithDistinctReplicaSets_AreFortyTwoGroups()
    {
        /* The exact field shape: every Amazon RDS for SQL Server Multi-AZ instance's internal AG is named
           RDSAG0, but each instance's replica set (itself + its own standby) is distinct. Counting by name
           alone reads this as "1 group"; the fix must read it as 42. */
        var replicas = new System.Collections.Generic.List<AgTopologyReplicaRow>();
        for (var i = 1; i <= 42; i++)
        {
            replicas.Add(Replica(i, $"NODE{i}A", "RDSAG0", $"NODE{i}A", "PRIMARY"));
            replicas.Add(Replica(i, $"NODE{i}A", "RDSAG0", $"NODE{i}B", "SECONDARY"));
        }

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());
        var (groups, servers, views) = AgTopology.Counts(cards);

        Assert.Equal(42, groups);
        Assert.Equal(42, servers);
        Assert.Equal(42, views);
    }

    [Fact]
    public void Counts_TwoReplicasOfTheSameAg_AreOneGroupWithTwoViews()
    {
        /* Two monitored replicas of the SAME real AG report the SAME replica set, so identity (name + replica
           set) still collapses them to one group — the case Views exists to distinguish from Groups. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY"),
            Replica(2, "NODE2", "AG1", "NODE1", "PRIMARY", operational: null),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());
        var (groups, servers, views) = AgTopology.Counts(cards);

        Assert.Equal(1, groups);
        Assert.Equal(2, servers);
        Assert.Equal(2, views);
    }

    /* ────────── #4475 follow-up: identity is overlap (connected components), not exact-set equality ────────── */

    [Fact]
    public void Counts_SameAgSeenFromItsPrimaryAndItsSecondary_IsOneGroup()
    {
        /* The blocking case: sys.dm_hadr_availability_replica_states returns LOCAL information only on a
           server hosting a SECONDARY (Microsoft Learn). The primary's card carries {P,S}; the secondary's
           card carries only {S}. Exact-set identity (the previous commit, still on this branch's head before
           this fix) counts that as 2 — this pin is RED against 5f0e119a9 and must go GREEN here. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());
        var (groups, _, views) = AgTopology.Counts(cards);

        Assert.Equal(1, groups);
        Assert.Equal(2, views);
    }

    [Fact]
    public void Counts_ThreeOverlappingPerspectivesOfOneAg_IsOneGroupByTransitivity()
    {
        /* {P,S1,S2} + {S1} + {S2}: no two sets are equal, but each overlaps the full-set card, so all three
           union into one group through it. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY"),
            Replica(1, "NODE1", "AG1", "NODE3", "SECONDARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
            Replica(3, "NODE3", "AG1", "NODE3", "SECONDARY"),
        };

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());
        var (groups, _, views) = AgTopology.Counts(cards);

        Assert.Equal(1, groups);
        Assert.Equal(3, views);
    }

    [Fact]
    public void Counts_TwoMonitoredSecondariesWithNoPrimaryMonitored_AreTwoGroups_TheDocumentedLimit()
    {
        /* {S1} + {S2}, same AG name, neither reporting the other: nothing in the collected rows links them
           without the AG's group_id, which a non-primary reporter's replica-states view does not expose. This
           is the known limit stated in the doc comment, not a bug. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "SECONDARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());
        var (groups, _, views) = AgTopology.Counts(cards);

        Assert.Equal(2, groups);
        Assert.Equal(2, views);
    }

    [Fact]
    public void Counts_SameReplicaNamesButDifferentAgNames_AreTwoGroups()
    {
        /* Overlap alone is never enough — the name must match too, or two entirely different AGs that happen
           to share a replica's server name would wrongly merge. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(2, "NODE1", "AG2", "NODE1", "PRIMARY"),
        };

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());
        var (groups, _, views) = AgTopology.Counts(cards);

        Assert.Equal(2, groups);
        Assert.Equal(2, views);
    }

    [Fact]
    public void Counts_TwoSameNameCardsWithEmptyReplicaSets_AreOneGroup()
    {
        /* An empty replica set can never overlap anything, so it falls back to name-only matching among
           itself rather than becoming a permanent singleton. Reachable when a card's replica rows are absent
           but its AG name is known some other way; exercised here directly against the counting helper since
           BuildCards' REPLICA grain cannot itself produce a card with zero replicas. */
        var groups = AgTopology.CountDistinctGroups(new (string?, IEnumerable<string?>)[]
        {
            ("AG1", Array.Empty<string?>()),
            ("AG1", Array.Empty<string?>()),
        });

        Assert.Equal(1, groups);
    }

    [Fact]
    public void CountDistinctGroups_EmptyReplicaSetCardAmongDisjointSameNameCards_DoesNotBridgeThem()
    {
        /* #4475/#4478 regression: one card whose replica names are all null (an empty set) must never union
           with a same-named card that DOES have replicas -- that bridges components with nothing in common.
           Two disjoint {P1,S1}/{P2,S2} cards plus one empty RDSAG0 card must stay 3 groups, not collapse to 1
           (which is what happens against 0a3738bae, the pre-fix head). */
        var groups = AgTopology.CountDistinctGroups(new (string?, IEnumerable<string?>)[]
        {
            ("RDSAG0", new string?[] { "P1", "S1" }),
            ("RDSAG0", new string?[] { "P2", "S2" }),
            ("RDSAG0", Array.Empty<string?>()),
        });

        Assert.Equal(3, groups);
    }

    [Fact]
    public void CountDistinctGroups_TwoEmptyReplicaSetCards_StillMatchEachOtherByNameOnly()
    {
        /* Two empty-set same-named cards still union by name alone -- nothing else distinguishes them, so this
           keeps the pre-existing #4475 empty-set pin's behavior for the all-empty case. */
        var groups = AgTopology.CountDistinctGroups(new (string?, IEnumerable<string?>)[]
        {
            ("RDSAG0", Array.Empty<string?>()),
            ("RDSAG0", Array.Empty<string?>()),
        });

        Assert.Equal(1, groups);
    }

    [Fact]
    public void CountDistinctGroups_OneReplicaOverlapsSharedNodePlusOneEmptyCard_AreTwoGroups()
    {
        /* {P,S} and {S} overlap on S and union into one group; the empty card unions with neither (it shares no
           replica with either, and it is not itself empty-paired with a same-named empty card), so the total
           is 2, not 1. */
        var groups = AgTopology.CountDistinctGroups(new (string?, IEnumerable<string?>)[]
        {
            ("RDSAG0", new string?[] { "P", "S" }),
            ("RDSAG0", new string?[] { "S" }),
            ("RDSAG0", Array.Empty<string?>()),
        });

        Assert.Equal(2, groups);
    }

    [Fact]
    public void Counts_FortyTwoDistinctRdsAg0Instances_AreFortyTwoGroups()
    {
        /* Re-proves the pre-existing #4475 case still holds under overlap identity: 42 disjoint replica sets
           sharing the RDSAG0 name never overlap each other, so they stay 42 groups, not 1. */
        var replicas = new List<AgTopologyReplicaRow>();
        for (var i = 1; i <= 42; i++)
        {
            replicas.Add(Replica(i, $"NODE{i}A", "RDSAG0", $"NODE{i}A", "PRIMARY"));
            replicas.Add(Replica(i, $"NODE{i}A", "RDSAG0", $"NODE{i}B", "SECONDARY"));
        }

        var cards = AgTopology.BuildCards(replicas, Array.Empty<AgTopologyDatabaseRow>());
        var (groups, servers, views) = AgTopology.Counts(cards);

        Assert.Equal(42, groups);
        Assert.Equal(42, servers);
        Assert.Equal(42, views);
    }
}
