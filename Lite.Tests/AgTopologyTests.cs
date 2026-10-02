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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's half of the twin matrix over the SHARED <see cref="AgTopology"/> projection (#991), plus the pins on
/// Lite's own DuckDB topology SQL. Darling.Tests runs the mirror of the banding/projection half.
///
/// <para>The duplication is the point: the two apps draw the same AG from different stores, and these matrices
/// are what make "they cannot disagree" a checked claim rather than an intention. If the shared rules change,
/// both sides fail together — and if someone forks the projection back into one app, only one side fails, which
/// is exactly the signal you want.</para>
/// </summary>
public sealed class AgTopologyTests
{
    /* ─────────────────────────── Lite's DuckDB SQL ─────────────────────────── */

    [Fact]
    public void TopologySql_TakesEachServersOwnLatestSnapshot()
    {
        /* The MAX is correlated PER SERVER. A single global MAX would erase a lagging server's rows entirely
           whenever a livelier one had a newer timestamp — Lite can monitor more than one instance. */
        Assert.Contains("x.server_id = r.server_id", LocalDataService.AgTopologyReplicaSql, StringComparison.Ordinal);
        Assert.Contains("x.server_id = d.server_id", LocalDataService.AgTopologyDatabaseSql, StringComparison.Ordinal);
        Assert.Contains("MAX(x.collection_time)", LocalDataService.AgTopologyReplicaSql, StringComparison.Ordinal);
        Assert.Contains("MAX(x.collection_time)", LocalDataService.AgTopologyDatabaseSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TopologySql_SelectsTheColumnsTheAlertReadDoesNot()
    {
        /* This read exists BECAUSE the alert read (#1696) is insufficient for a display: it selects five columns
           and drops NULL-named rows. If someone later "consolidates" the two, these columns are what breaks. */
        foreach (var column in new[]
        {
            "operational_state_desc", "recovery_health_desc", "synchronization_health_desc",
            "availability_mode_desc", "failover_mode_desc", "endpoint_url",
        })
        {
            Assert.Contains(column, LocalDataService.AgTopologyReplicaSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TopologySql_KeepsRowsWhoseNamesAreNull()
    {
        /* The alert read drops them (they cannot key an edge). A topology view must not: NULL names appear under
           WSFC quorum loss, which is precisely when someone opens this tab. */
        Assert.DoesNotContain("ag_name IS NOT NULL", LocalDataService.AgTopologyReplicaSql, StringComparison.Ordinal);
        Assert.DoesNotContain("replica_server_name IS NOT NULL", LocalDataService.AgTopologyReplicaSql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── shared banding (mirror of Darling.Tests) ─────────────────────────── */

    [Theory]
    [InlineData("HEALTHY", HealthSeverity.Healthy)]
    [InlineData("PARTIALLY_HEALTHY", HealthSeverity.Warning)]
    [InlineData("NOT_HEALTHY", HealthSeverity.Critical)]
    [InlineData(null, HealthSeverity.Unknown)]
    public void SynchronizationHealth_Bands(string? desc, HealthSeverity expected) =>
        Assert.Equal(expected, AgTopology.SynchronizationHealthSeverity(desc));

    [Fact]
    public void OperationalState_NullIsUnknown_AndOnlineInProgressBelongsToRecoveryHealth()
    {
        Assert.Equal(HealthSeverity.Unknown, AgTopology.OperationalStateSeverity(null));
        Assert.Equal(HealthSeverity.Unknown, AgTopology.OperationalStateSeverity("ONLINE_IN_PROGRESS"));
        Assert.Equal(HealthSeverity.Warning, AgTopology.RecoveryHealthSeverity("ONLINE_IN_PROGRESS"));
    }

    [Fact]
    public void DatabaseSync_SynchronizingIsHealthyOnAsync_WarningOnSync()
    {
        Assert.Equal(HealthSeverity.Healthy, AgTopology.DatabaseSyncSeverity("SYNCHRONIZING", "ASYNCHRONOUS_COMMIT", false));
        Assert.Equal(HealthSeverity.Warning, AgTopology.DatabaseSyncSeverity("SYNCHRONIZING", "SYNCHRONOUS_COMMIT", false));
    }

    [Theory]
    [InlineData("NOT SYNCHRONIZING")]
    [InlineData("NOT_SYNCHRONIZING")]
    public void DatabaseSync_AcceptsBothSpellings(string state) =>
        Assert.Equal(HealthSeverity.Critical, AgTopology.DatabaseSyncSeverity(state, "SYNCHRONOUS_COMMIT", false));

    [Fact]
    public void DatabaseSync_SuspendedOutranksTheStateText() =>
        Assert.Equal(HealthSeverity.Critical, AgTopology.DatabaseSyncSeverity("SYNCHRONIZED", "SYNCHRONOUS_COMMIT", true));

    [Fact]
    public void DrainMinutes_EmptyIsZero_StalledHasNoEstimate()
    {
        Assert.Equal<double?>(0, AgTopology.DrainMinutes(0, 0));
        Assert.Equal<double?>(1.0, AgTopology.DrainMinutes(6000, 100));
        Assert.Null(AgTopology.DrainMinutes(5000, 0));
        Assert.Null(AgTopology.DrainMinutes(null, 100));
    }

    /* ─────────────────────────── shared projection ─────────────────────────── */

    private static AgTopologyReplicaRow Replica(int serverId, string server, string ag, string replica, string role,
        string syncHealth = "HEALTHY", string? connected = "CONNECTED", string? operational = "ONLINE") => new()
    {
        ServerId = serverId,
        ServerName = server,
        CollectionTime = new DateTime(2026, 7, 26, 12, 0, 0, DateTimeKind.Utc),
        AgName = ag,
        ReplicaServerName = replica,
        RoleDesc = role,
        OperationalStateDesc = operational,
        ConnectedStateDesc = connected,
        RecoveryHealthDesc = "ONLINE",
        SynchronizationHealthDesc = syncHealth,
        AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
        FailoverModeDesc = "AUTOMATIC",
    };

    [Fact]
    public void Build_SingleServerPerspective_IsOneCardPerAg()
    {
        /* Lite's normal shape: one monitored instance, so one card per AG — the same per-reporting-server rule
           Darling applies, with one reporter. */
        var cards = AgTopology.BuildCards(
            new[] { Replica(1, "SQL01", "AG1", "SQL01", "PRIMARY"), Replica(1, "SQL01", "AG1", "SQL02", "SECONDARY", operational: null) },
            Array.Empty<AgTopologyDatabaseRow>());

        var card = Assert.Single(cards);
        Assert.Equal(2, card.Replicas.Count);
        Assert.Equal("SQL01", card.PrimaryReplica);
        Assert.Equal(HealthSeverity.Healthy, card.Severity);
    }

    [Fact]
    public void Build_OnASecondary_ShowsOneReplicaAndNoPrimary_WhichIsHonest()
    {
        /* Pointed at a secondary, the DMV returns only local information. Lite shows exactly what that server
           can see rather than implying it knows the whole AG. */
        var cards = AgTopology.BuildCards(
            new[] { Replica(2, "SQL02", "AG1", "SQL02", "SECONDARY") },
            Array.Empty<AgTopologyDatabaseRow>());

        var card = Assert.Single(cards);
        Assert.Single(card.Replicas);
        Assert.Null(card.PrimaryReplica);
        Assert.Equal("No primary reported", card.PrimaryDisplay);
    }

    [Fact]
    public void Build_SuspendedDatabaseRedensTheCard_EvenWhenReplicasAreHealthy()
    {
        var databases = new[]
        {
            new AgTopologyDatabaseRow
            {
                ServerId = 1,
                ServerName = "SQL01",
                CollectionTime = new DateTime(2026, 7, 26, 12, 0, 0, DateTimeKind.Utc),
                AgName = "AG1",
                DatabaseName = "Sales",
                ReplicaServerName = "SQL02",
                SynchronizationStateDesc = "SYNCHRONIZED",
                AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
                IsSuspended = true,
                SuspendReasonDesc = "SUSPEND_FROM_USER",
                SecondaryLagSeconds = 0,
            },
        };

        var card = Assert.Single(AgTopology.BuildCards(new[] { Replica(1, "SQL01", "AG1", "SQL01", "PRIMARY") }, databases));

        Assert.Equal(HealthSeverity.Critical, card.Severity);
        Assert.Equal(HealthSeverity.Healthy, card.Replicas[0].Severity);

        var db = Assert.Single(card.Databases);
        Assert.Contains("Suspended: SUSPEND_FROM_USER", db.DataMovementDisplay, StringComparison.Ordinal);
        Assert.Contains("(suspended)", db.LagDisplay, StringComparison.Ordinal);
    }

    [Fact]
    public void Build_UnknownNeverMasksHealthy()
    {
        var card = Assert.Single(AgTopology.BuildCards(
            new[]
            {
                Replica(1, "SQL01", "AG1", "SQL01", "PRIMARY"),
                Replica(1, "SQL01", "AG1", "SQL02", "SECONDARY", connected: null, operational: null),
            },
            Array.Empty<AgTopologyDatabaseRow>()));

        Assert.Equal(HealthSeverity.Healthy, card.Severity);
    }

    [Fact]
    public void Build_NullAgName_StillRenders_RatherThanVanishing()
    {
        /* Quorum loss can null the names. The card must appear — that is when someone is looking. */
        var row = Replica(1, "SQL01", "AG1", "SQL01", "PRIMARY");
        var quorumLost = new AgTopologyReplicaRow
        {
            ServerId = row.ServerId,
            ServerName = row.ServerName,
            CollectionTime = row.CollectionTime,
            AgName = null,
            ReplicaServerName = null,
        };

        var card = Assert.Single(AgTopology.BuildCards(new[] { quorumLost }, Array.Empty<AgTopologyDatabaseRow>()));

        Assert.Equal("—", card.AgNameDisplay);
        Assert.Equal("—", Assert.Single(card.Replicas).NameDisplay);
    }

    [Fact]
    public void Build_SortsWorstFirst()
    {
        var cards = AgTopology.BuildCards(
            new[]
            {
                Replica(1, "SQL01", "AG_CALM", "SQL01", "PRIMARY"),
                Replica(2, "SQL02", "AG_BROKEN", "SQL02", "PRIMARY", syncHealth: "NOT_HEALTHY"),
            },
            Array.Empty<AgTopologyDatabaseRow>());

        Assert.Equal(new[] { "AG_BROKEN", "AG_CALM" }, cards.Select(c => c.AgName).ToArray());
    }

    /* ─────────────────────────── header ─────────────────────────── */

    [Fact]
    public void Summary_DistinguishesGroupsFromViews()
    {
        /* the primary's view lists every replica; a secondary's lists only itself (#4475) */
        var cards = AgTopology.BuildCards(
            new[]
            {
                Replica(1, "SQL01", "AG1", "SQL01", "PRIMARY"),
                Replica(1, "SQL01", "AG1", "SQL02", "SECONDARY"),
                Replica(2, "SQL02", "AG1", "SQL02", "SECONDARY"),
            },
            Array.Empty<AgTopologyDatabaseRow>());

        Assert.Equal("1 group · 2 reporting servers · 2 views", AvailabilityGroupsTab.BuildSummary(cards));
    }

    /* ─────────────────────────── the local marker (V37) ─────────────────────────── */

    [Theory]
    [InlineData(true, "local")]
    [InlineData(false, "")]
    [InlineData(null, "")]
    public void LocalDisplay_ShownOnlyWhenExplicitlyLocal(bool? isLocal, string expected)
    {
        /* NULL means UNKNOWN, not remote: rows collected before is_local existed genuinely do not know, and
           labelling them either way would assert something the data cannot support. Both false and null render
           blank, but only false actually MEANS remote. */
        var replica = new AgTopologyReplicaRow
        {
            ServerId = 1,
            ServerName = "SQL01",
            AgName = "AG1",
            ReplicaServerName = "SQL01",
            RoleDesc = "PRIMARY",
            IsLocal = isLocal,
        };

        var card = Assert.Single(AgTopology.BuildCards(new[] { replica }, Array.Empty<AgTopologyDatabaseRow>()));
        Assert.Equal(expected, Assert.Single(card.Replicas).LocalDisplay);
        Assert.Equal(isLocal, card.Replicas[0].IsLocal);
    }

    [Fact]
    public void TopologySql_SelectsIsLocalOnTheReplicaGrain()
    {
        /* The whole point of the marker: without it a reader cannot tell a server describing ITSELF from a
           server describing a node it only sees across the wire. */
        Assert.Contains("is_local", LocalDataService.AgTopologyReplicaSql, StringComparison.Ordinal);
    }

    [Fact]
    public void Summary_EmptySaysSo() =>
        Assert.Equal("none observed", AvailabilityGroupsTab.BuildSummary(Array.Empty<AgTopologyCard>()));
}
