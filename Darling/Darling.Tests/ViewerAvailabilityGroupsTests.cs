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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's Availability Groups tab (#991): the read SQL's dialect and latest-snapshot shape, the banding
/// rules, and the pure card projection. Ungated — the projection half is separated from the Postgres half exactly
/// so the rules test without a store, mirroring FleetViewTests.
///
/// <para>These deliberately duplicate <c>DarlingAgReaderTests</c>' expectations rather than sharing them: the
/// viewer reader is a COPY of the service reader (no ProjectReference exists), so the pins are what keep the two
/// from drifting into disagreeing about whether an AG is healthy.</para>
/// </summary>
public sealed class ViewerAvailabilityGroupsTests
{
    /* ─────────────────────────── SQL pins ─────────────────────────── */

    [Fact]
    public void AgSql_IsPostgresDialect_AndReadsTheBareCollectTables()
    {
        foreach (var sql in new[] { ViewerDataService.AgReplicaStatesSql, ViewerDataService.AgDatabaseReplicaStatesSql })
        {
            /* The viewer's store connection resolves `collect` through search_path, so these must NOT be
               schema-qualified the way the service reader's copies are. */
            Assert.DoesNotContain("collect.", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        }

        Assert.Contains("FROM ag_replica_states", ViewerDataService.AgReplicaStatesSql, StringComparison.Ordinal);
        Assert.Contains("FROM ag_database_replica_states", ViewerDataService.AgDatabaseReplicaStatesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void AgSql_KeepsEveryRowAtEachServersNewestCollection()
    {
        /* DISTINCT ON would keep ONE row per server, which is exactly wrong: a snapshot is many rows (one per
           replica / per database). The join against MAX(collection_time) keeps them all. */
        foreach (var sql in new[] { ViewerDataService.AgReplicaStatesSql, ViewerDataService.AgDatabaseReplicaStatesSql })
        {
            Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
            Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("DISTINCT ON", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AgSql_RestrictsToTheEnabledServerRegistry()
    {
        /* A server disabled in the control plane leaves the fleet surfaces at once; its AG cards go with it
           rather than lingering until retention expires the rows. */
        foreach (var sql in new[] { ViewerDataService.AgReplicaStatesSql, ViewerDataService.AgDatabaseReplicaStatesSql })
        {
            Assert.Contains("JOIN servers AS s", sql, StringComparison.Ordinal);
            Assert.Contains("s.is_enabled", sql, StringComparison.Ordinal);
        }
    }

    /* ─────────────────────────── banding ─────────────────────────── */

    [Theory]
    [InlineData("HEALTHY", HealthSeverity.Healthy)]
    [InlineData("PARTIALLY_HEALTHY", HealthSeverity.Warning)]
    [InlineData("NOT_HEALTHY", HealthSeverity.Critical)]
    [InlineData(null, HealthSeverity.Unknown)]
    public void SynchronizationHealth_BandsHealthyPartiallyNot(string? desc, HealthSeverity expected)
    {
        Assert.Equal(expected, ViewerDataService.AgSynchronizationHealthSeverity(desc));
    }

    [Fact]
    public void OperationalState_NullBandsUnknown_AndDoesNotClaimRecoveryHealthsValue()
    {
        /* operational_state_desc is reported for the LOCAL replica only, so every remote replica's row carries
           NULL — banding that Critical would paint a healthy AG red from every secondary's perspective. And
           ONLINE_IN_PROGRESS belongs to recovery_health_desc, not this column. */
        Assert.Equal(HealthSeverity.Unknown, ViewerDataService.AgOperationalStateSeverity(null));
        Assert.Equal(HealthSeverity.Unknown, ViewerDataService.AgOperationalStateSeverity("ONLINE_IN_PROGRESS"));
        Assert.Equal(HealthSeverity.Warning, ViewerDataService.AgRecoveryHealthSeverity("ONLINE_IN_PROGRESS"));
    }

    [Fact]
    public void DatabaseSync_SynchronizingIsHealthyOnAsync_WarningOnSync()
    {
        /* The load-bearing distinction: an ASYNCHRONOUS_COMMIT replica never reaches SYNCHRONIZED, so
           SYNCHRONIZING is its correct steady state; on SYNCHRONOUS_COMMIT the same text means the replica is
           not currently protecting a commit. A flat state map necessarily gets one of these wrong. */
        Assert.Equal(HealthSeverity.Healthy, ViewerDataService.AgDatabaseSyncSeverity("SYNCHRONIZING", "ASYNCHRONOUS_COMMIT", false));
        Assert.Equal(HealthSeverity.Warning, ViewerDataService.AgDatabaseSyncSeverity("SYNCHRONIZING", "SYNCHRONOUS_COMMIT", false));
    }

    [Theory]
    [InlineData("NOT SYNCHRONIZING")]
    [InlineData("NOT_SYNCHRONIZING")]
    public void DatabaseSync_AcceptsBothSpellingsOfNotSynchronizing(string state)
    {
        /* The database grain reports states space-separated where the replica grain uses underscores; both are
           stored verbatim, so the banding must normalize. */
        Assert.Equal(HealthSeverity.Critical, ViewerDataService.AgDatabaseSyncSeverity(state, "SYNCHRONOUS_COMMIT", false));
    }

    [Fact]
    public void DatabaseSync_SuspendedIsCritical_EvenWhenTheStateReadsSynchronized()
    {
        /* secondary_lag_seconds reads 0 — not null — while data movement is suspended, so a suspended replica
           otherwise presents as perfectly caught up. Suspension outranks the state text. */
        Assert.Equal(HealthSeverity.Critical, ViewerDataService.AgDatabaseSyncSeverity("SYNCHRONIZED", "SYNCHRONOUS_COMMIT", true));
    }

    [Fact]
    public void DrainMinutes_EmptyQueueIsZero_StalledQueueHasNoEstimate()
    {
        Assert.Equal<double?>(0, ViewerDataService.AgDrainMinutes(0, 0));
        Assert.Equal<double?>(1.0, ViewerDataService.AgDrainMinutes(6000, 100));
        Assert.Null(ViewerDataService.AgDrainMinutes(5000, 0));
        Assert.Null(ViewerDataService.AgDrainMinutes(null, 100));
    }

    /* ─────────────────────────── card projection ─────────────────────────── */

    private static ViewerAgReplicaRow Replica(
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

    private static ViewerAgDatabaseRow Database(
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

        var cards = ViewerDataService.BuildAvailabilityGroups(replicas, Array.Empty<ViewerAgDatabaseRow>());

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

        var card = Assert.Single(ViewerDataService.BuildAvailabilityGroups(replicas, Array.Empty<ViewerAgDatabaseRow>()));
        Assert.Equal(HealthSeverity.Healthy, card.Severity);
        Assert.Equal("Healthy", card.SeverityLabel);
    }

    [Fact]
    public void Build_SuspendedDatabaseRedensTheCard_EvenWhenEveryReplicaIsHealthy()
    {
        var replicas = new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY") };
        var databases = new[] { Database(1, "NODE1", "AG1", "Sales", "NODE2", suspended: true) };

        var card = Assert.Single(ViewerDataService.BuildAvailabilityGroups(replicas, databases));

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

        var cards = ViewerDataService.BuildAvailabilityGroups(replicas, databases);

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
            ViewerDataService.BuildAvailabilityGroups(replicas, Array.Empty<ViewerAgDatabaseRow>()).Select(c => c.AgName).ToArray());
    }

    [Fact]
    public void Build_EmptyInputYieldsNoCards()
    {
        Assert.Empty(ViewerDataService.BuildAvailabilityGroups(Array.Empty<ViewerAgReplicaRow>(), Array.Empty<ViewerAgDatabaseRow>()));
    }

    /* ─────────────────────────── header summary ─────────────────────────── */

    [Fact]
    public void Summary_DistinguishesDistinctGroupsFromViews()
    {
        /* A reader seeing the same AG name on two cards needs the difference stated, not inferred. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var summary = AvailabilityGroupsTab.BuildSummary(ViewerDataService.BuildAvailabilityGroups(replicas, Array.Empty<ViewerAgDatabaseRow>()));

        Assert.Equal("1 group · 2 reporting servers · 2 views", summary);
    }

    [Fact]
    public void Summary_EmptyFleetSaysSo()
    {
        Assert.Equal("none observed", AvailabilityGroupsTab.BuildSummary(Array.Empty<ViewerAvailabilityGroup>()));
    }
}
