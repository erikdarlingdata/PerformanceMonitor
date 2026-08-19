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
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingAgReader;

namespace Darling.Tests;

/// <summary>
/// The Availability Group topology reader (#991), ungated: the banding rules, the derived drain estimates, the
/// per-(reporting server, AG) grouping, and the Postgres dialect of the two reads. Everything below is pure — the
/// reader's assembly half is separated from its Postgres half exactly so these rules test without a live store.
/// </summary>
public sealed class DarlingAgReaderTests
{
    /* ─────────────────────────── replica-grain banding ─────────────────────────── */

    [Theory]
    [InlineData("HEALTHY", HealthSeverity.Healthy)]
    [InlineData("PARTIALLY_HEALTHY", HealthSeverity.Warning)]
    [InlineData("NOT_HEALTHY", HealthSeverity.Critical)]
    [InlineData(null, HealthSeverity.Unknown)]
    [InlineData("", HealthSeverity.Unknown)]
    public void SynchronizationHealth_BandsHealthyPartiallyNot(string? desc, HealthSeverity expected)
    {
        Assert.Equal(expected, Reader.SynchronizationHealthSeverity(desc));
    }

    [Theory]
    [InlineData("CONNECTED", HealthSeverity.Healthy)]
    [InlineData("DISCONNECTED", HealthSeverity.Critical)]
    [InlineData(null, HealthSeverity.Unknown)]
    public void ConnectedState_BandsConnectedDisconnected(string? desc, HealthSeverity expected)
    {
        Assert.Equal(expected, Reader.ConnectedStateSeverity(desc));
    }

    [Theory]
    [InlineData("ONLINE", HealthSeverity.Healthy)]
    [InlineData("PENDING_FAILOVER", HealthSeverity.Warning)]
    [InlineData("FAILED_NO_QUORUM", HealthSeverity.Critical)]
    [InlineData("OFFLINE", HealthSeverity.Critical)]
    public void OperationalState_BandsOnlineTransitionalFailed(string desc, HealthSeverity expected)
    {
        Assert.Equal(expected, Reader.OperationalStateSeverity(desc));
    }

    [Fact]
    public void OperationalState_NullBandsUnknown_NotCritical()
    {
        /* The DMV reports operational_state_desc for the LOCAL replica only, so every remote replica's row carries
           NULL. Banding that Critical would paint a healthy AG red from every secondary's perspective. */
        Assert.Equal(HealthSeverity.Unknown, Reader.OperationalStateSeverity(null));
    }

    [Fact]
    public void OperationalState_DoesNotClaimRecoveryHealthsValue()
    {
        /* ONLINE_IN_PROGRESS is documented on recovery_health_desc, NOT operational_state_desc (whose values are
           exactly PENDING_FAILOVER / PENDING / ONLINE / OFFLINE / FAILED / FAILED_NO_QUORUM / NULL). Banding it
           here would be an arm that can never fire while the column it belongs to went unbanded. */
        Assert.Equal(HealthSeverity.Unknown, Reader.OperationalStateSeverity("ONLINE_IN_PROGRESS"));
        Assert.Equal(HealthSeverity.Warning, Reader.RecoveryHealthSeverity("ONLINE_IN_PROGRESS"));
    }

    [Theory]
    [InlineData("ONLINE", HealthSeverity.Healthy)]
    [InlineData("ONLINE_IN_PROGRESS", HealthSeverity.Warning)]
    [InlineData(null, HealthSeverity.Unknown)]
    public void RecoveryHealth_BandsItsTwoDocumentedValues(string? desc, HealthSeverity expected)
    {
        Assert.Equal(expected, Reader.RecoveryHealthSeverity(desc));
    }

    [Theory]
    [InlineData("PRIMARY", HealthSeverity.Healthy)]
    [InlineData("SECONDARY", HealthSeverity.Healthy)]
    [InlineData("RESOLVING", HealthSeverity.Warning)]
    [InlineData(null, HealthSeverity.Unknown)]
    public void Role_BandsResolvingAsAFinding(string? desc, HealthSeverity expected)
    {
        /* RESOLVING means the replica holds neither real role — what a replica looks like mid-failover or after
           losing quorum. It is also the only role under which operational_state can read PENDING_FAILOVER or
           FAILED_NO_QUORUM, so leaving it unbanded would drop the clearest signal the replica grain carries. */
        Assert.Equal(expected, Reader.RoleSeverity(desc));
    }

    [Fact]
    public void ReplicaSeverity_RollsUpEveryBandedColumn()
    {
        /* A replica whose only problem is a transitional recovery health must still surface — the roll-up covers
           sync health, connected, operational, recovery health AND role, not just the first three. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY", recoveryHealth: "ONLINE_IN_PROGRESS"),
        };

        var group = Assert.Single(Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0)).AvailabilityGroups);
        Assert.Equal(HealthSeverity.Warning, group.Replicas[0].Severity);
        Assert.Equal(HealthSeverity.Warning, group.Severity);
    }

    [Fact]
    public void ReplicaSeverity_ResolvingRoleReachesTheGroupBadge()
    {
        var replicas = new[] { Replica(1, "NODE1", "AG1", "NODE1", "RESOLVING") };

        var group = Assert.Single(Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0)).AvailabilityGroups);
        Assert.Equal(HealthSeverity.Warning, group.Severity);
        Assert.Null(group.PrimaryReplica);
    }

    /* ─────────────────────────── database-grain banding ─────────────────────────── */

    [Fact]
    public void DatabaseSync_SynchronizingIsHealthyOnAsync_WarningOnSync()
    {
        /* The load-bearing distinction: an ASYNCHRONOUS_COMMIT replica never reaches SYNCHRONIZED, so SYNCHRONIZING
           is its correct steady state; on a SYNCHRONOUS_COMMIT replica the same text means the replica is not
           currently protecting a commit. A flat state->color map gets one of these two wrong. */
        Assert.Equal(
            HealthSeverity.Healthy,
            Reader.DatabaseSyncSeverity("SYNCHRONIZING", "ASYNCHRONOUS_COMMIT", isSuspended: false));

        Assert.Equal(
            HealthSeverity.Warning,
            Reader.DatabaseSyncSeverity("SYNCHRONIZING", "SYNCHRONOUS_COMMIT", isSuspended: false));
    }

    [Theory]
    [InlineData("SYNCHRONIZED", HealthSeverity.Healthy)]
    [InlineData("NOT SYNCHRONIZING", HealthSeverity.Critical)]
    [InlineData("NOT_SYNCHRONIZING", HealthSeverity.Critical)]
    [InlineData("REVERTING", HealthSeverity.Warning)]
    [InlineData("INITIALIZING", HealthSeverity.Warning)]
    [InlineData(null, HealthSeverity.Unknown)]
    public void DatabaseSync_BandsTheRemainingStates(string? state, HealthSeverity expected)
    {
        /* Both spellings of NOT SYNCHRONIZING are accepted: the database grain reports states space-separated
           where the replica grain uses underscores, and the collectors store each verbatim. */
        Assert.Equal(expected, Reader.DatabaseSyncSeverity(state, "SYNCHRONOUS_COMMIT", isSuspended: false));
    }

    [Fact]
    public void DatabaseSync_SuspendedIsCritical_EvenWhenTheStateReadsSynchronized()
    {
        /* Suspension outranks the state text. MS Learn documents secondary_lag_seconds reading 0 — not NULL —
           while data movement is suspended, so a suspended replica otherwise presents as perfectly caught up. */
        Assert.Equal(
            HealthSeverity.Critical,
            Reader.DatabaseSyncSeverity("SYNCHRONIZED", "SYNCHRONOUS_COMMIT", isSuspended: true));
    }

    /* ─────────────────────────── derived drain estimates ─────────────────────────── */

    [Fact]
    public void DrainMinutes_EmptyQueueIsZero_RegardlessOfRate()
    {
        Assert.Equal<double?>(0, Reader.DrainMinutes(0, 0));
        Assert.Equal<double?>(0, Reader.DrainMinutes(0, 1024));
    }

    [Fact]
    public void DrainMinutes_NonEmptyQueueAtZeroRateHasNoEstimate()
    {
        /* Null, never an infinity and never a misleading 0 — a backlog that is not moving has no finite drain. */
        Assert.Null(Reader.DrainMinutes(5000, 0));
        Assert.Null(Reader.DrainMinutes(5000, null));
    }

    [Fact]
    public void DrainMinutes_DividesQueueByRateIntoMinutes()
    {
        /* 6000 KB at 100 KB/s = 60 s = 1 minute. */
        Assert.Equal<double?>(1.0, Reader.DrainMinutes(6000, 100));
        Assert.Equal<double?>(0.5, Reader.DrainMinutes(3000, 100));
    }

    [Fact]
    public void DrainMinutes_NullQueueHasNoEstimate()
    {
        Assert.Null(Reader.DrainMinutes(null, 100));
    }

    /* ─────────────────────────── grouping + roll-up ─────────────────────────── */

    private static DateTime At(int minutesAgo) =>
        DateTime.SpecifyKind(new DateTime(2026, 7, 26, 12, 0, 0).AddMinutes(-minutesAgo), DateTimeKind.Unspecified);

    private static Reader.ReplicaRow Replica(
        int serverId,
        string serverName,
        string agName,
        string replicaName,
        string role,
        string syncHealth = "HEALTHY",
        string? connected = "CONNECTED",
        string? operational = "ONLINE",
        string? recoveryHealth = "ONLINE",
        bool? isLocal = null) =>
        new(serverId, serverName, At(1), agName, replicaName, role, isLocal, operational, connected, recoveryHealth, syncHealth, "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://" + replicaName + ":5022");

    private static Reader.DatabaseRow Database(
        int serverId,
        string serverName,
        string agName,
        string databaseName,
        string replicaName,
        string state = "SYNCHRONIZED",
        bool isSuspended = false,
        long? lagSeconds = 0,
        int minutesAgo = 1) =>
        new(serverId, serverName, At(minutesAgo), agName, databaseName, replicaName, true, state, "0x00", "0x00", 0, 0, 1024, 1024, isSuspended, isSuspended ? "USER_ACTION" : null, "SYNCHRONOUS_COMMIT", lagSeconds);

    [Fact]
    public void Build_OneAgSeenFromTwoServers_StaysTwoGroupsEachNamingItsReporter()
    {
        /* Every replica reports the whole AG's replica set, so a two-node AG with both nodes monitored yields two
           views of one AG. They are deliberately NOT merged — the perspectives genuinely differ — and each names
           the server whose view it is. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY", operational: null),
            Replica(2, "NODE2", "AG1", "NODE1", "PRIMARY", operational: null),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(2, result.AvailabilityGroupCount);
        Assert.Equal(2, result.ReportingServerCount);
        Assert.Equal(1, result.DistinctAgCount);
        Assert.Equal(new[] { "NODE1", "NODE2" }, result.AvailabilityGroups.Select(g => g.ServerName).OrderBy(n => n, StringComparer.Ordinal));
        Assert.All(result.AvailabilityGroups, g => Assert.Equal(2, g.Replicas.Count));
        Assert.All(result.AvailabilityGroups, g => Assert.Equal("NODE1", g.PrimaryReplica));
    }

    [Fact]
    public void Build_UnknownDoesNotMaskHealthy_InTheGroupRollUp()
    {
        /* A secondary's row carries NULL operational/connected state; those band Unknown, which is the LOWEST enum
           value and so must never pull an otherwise-healthy group off Healthy. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY", connected: null, operational: null),
        };

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(HealthSeverity.Healthy, Assert.Single(result.AvailabilityGroups).Severity);
        Assert.Equal("Healthy", result.AvailabilityGroups[0].SeverityLabel);
    }

    [Fact]
    public void Build_WorstSeverityRollsUpFromDatabasesToo_NotJustReplicas()
    {
        /* A suspended database is the whole point of the surface: every replica can look HEALTHY while a database
           has stopped moving. The group badge has to reflect it. */
        var replicas = new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY") };
        var databases = new[] { Database(1, "NODE1", "AG1", "Sales", "NODE2", isSuspended: true) };

        var result = Reader.Build(replicas, databases, At(0));

        var group = Assert.Single(result.AvailabilityGroups);
        Assert.Equal(HealthSeverity.Critical, group.Severity);
        Assert.Equal(HealthSeverity.Critical, result.WorstSeverity);
        Assert.Equal(HealthSeverity.Healthy, group.Replicas[0].Severity);
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

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(
            new[] { "AG_BROKEN", "AG_SHAKY", "AG_CALM" },
            result.AvailabilityGroups.Select(g => g.AgName).ToArray());
    }

    [Fact]
    public void Build_DatabasesAttachToTheirOwnReportingServersGroup()
    {
        /* The database rows are keyed by (server_id, ag_name) exactly like the replica rows, so one server's
           database grain can never leak into another server's view of the same AG. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "PRIMARY"),
        };
        var databases = new[] { Database(1, "NODE1", "AG1", "Sales", "NODE2") };

        var result = Reader.Build(replicas, databases, At(0));

        var node1 = result.AvailabilityGroups.Single(g => g.ServerName == "NODE1");
        var node2 = result.AvailabilityGroups.Single(g => g.ServerName == "NODE2");
        Assert.Equal("Sales", Assert.Single(node1.Databases).DatabaseName);
        Assert.Empty(node2.Databases);
    }

    [Fact]
    public void Build_EmptyStoreYieldsEmptyResult_NotAnError()
    {
        var result = Reader.Build(Array.Empty<Reader.ReplicaRow>(), Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(0, result.AvailabilityGroupCount);
        Assert.Equal(0, result.ReportingServerCount);
        Assert.Equal(0, result.DistinctAgCount);
        Assert.Empty(result.AvailabilityGroups);
        Assert.Equal(HealthSeverity.Unknown, result.WorstSeverity);
    }

    [Fact]
    public void Build_CarriesBothCollectionInstants()
    {
        /* The two collectors sweep independently, so the database grain gets its own "as of" — the page shows it
           whenever it differs, since a group that stops refreshing keeps its last instant rather than aging out. */
        var replicas = new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY") };
        var databases = new[] { Database(1, "NODE1", "AG1", "Sales", "NODE1", minutesAgo: 7) };

        var group = Assert.Single(Reader.Build(replicas, databases, At(0)).AvailabilityGroups);

        Assert.Equal(At(1), group.CollectionTime);
        Assert.Equal(At(7), group.DatabaseCollectionTime);
    }

    [Fact]
    public void Build_DatabaseCollectionTimeIsNullWhenTheGroupHasNoDatabaseRows()
    {
        var replicas = new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY") };

        var group = Assert.Single(Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0)).AvailabilityGroups);

        Assert.Null(group.DatabaseCollectionTime);
    }

    /* ─────────────────────────── serialized shape ─────────────────────────── */

    [Fact]
    public void SerializedShape_CarriesTheFieldsBothConsumersRead()
    {
        var replicas = new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY") };
        var databases = new[] { Database(1, "NODE1", "AG1", "Sales", "NODE2", isSuspended: true) };

        var json = JsonSerializer.Serialize(Reader.Build(replicas, databases, At(0)), Reader.JsonOptions);

        foreach (var field in new[]
        {
            "\"generated_at\"", "\"availability_group_count\"", "\"reporting_server_count\"", "\"distinct_ag_count\"",
            "\"worst_severity\"", "\"availability_groups\"", "\"server_name\"", "\"ag_name\"", "\"collection_time\"",
            "\"primary_replica\"", "\"severity\"", "\"severity_label\"", "\"replicas\"", "\"databases\"",
            "\"database_collection_time\"",
            "\"replica_server_name\"", "\"role\"", "\"role_severity\"", "\"is_primary\"", "\"is_local\"",
            "\"operational_state\"", "\"operational_state_severity\"",
            "\"connected_state\"", "\"connected_state_severity\"",
            "\"recovery_health\"", "\"recovery_health_severity\"",
            "\"synchronization_health\"", "\"synchronization_health_severity\"", "\"availability_mode\"",
            "\"failover_mode\"", "\"endpoint_url\"", "\"database_name\"", "\"is_local\"",
            "\"synchronization_state\"",
            "\"synchronization_state_severity\"", "\"log_send_queue_kb\"", "\"redo_queue_kb\"",
            "\"log_send_rate_kb_sec\"", "\"redo_rate_kb_sec\"", "\"est_send_drain_minutes\"",
            "\"est_redo_completion_minutes\"", "\"secondary_lag_seconds\"", "\"is_suspended\"", "\"suspend_reason\"",
            "\"last_commit_lsn\"", "\"last_hardened_lsn\"",
        })
        {
            Assert.Contains(field, json, StringComparison.Ordinal);
        }

        /* Severities serialize as NAMES (the JsonStringEnumConverter), not integers — the browser maps the name to
           a CSS class, so a numeric enum would silently break every color. */
        JsonAssert.Contains("\"severity\": \"Critical\"", json);
        JsonAssert.DoesNotContain("\"severity\": 3", json);
    }

    /* ─────────────────────────── SQL dialect pins ─────────────────────────── */

    [Fact]
    public void ReadSql_IsPostgresDialect_WithPositionalServerFilter()
    {
        foreach (var sql in new[] { Reader.ReplicaStatesSql, Reader.DatabaseReplicaStatesSql })
        {
            /* Positional params (Npgsql), never a named @param or an N'' literal. */
            Assert.Contains("$1", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);

            /* The filter guards BOTH the inner latest-instant aggregate and the outer read — count, don't just
               Contains, or deleting either WHERE leaves this green. */
            Assert.Equal(2, Occurrences(sql, "$1::integer IS NULL"));
        }
    }

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    [Fact]
    public void ReadSql_RestrictsToTheEnabledServerRegistry()
    {
        /* Every other fleet read joins the enabled registry; without it a server disabled in the control plane
           keeps producing AG cards until its rows age out, after it has already left /api/fleet and the sidebar. */
        foreach (var sql in new[] { Reader.ReplicaStatesSql, Reader.DatabaseReplicaStatesSql })
        {
            Assert.Contains("JOIN servers AS s", sql, StringComparison.Ordinal);
            Assert.Contains("s.is_enabled", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ReadSql_KeepsEveryRowAtEachServersNewestCollection()
    {
        /* DISTINCT ON would keep ONE row per server — which is exactly wrong here, since a snapshot is many rows
           (one per replica / per database). The join against MAX(collection_time) keeps them all. */
        foreach (var sql in new[] { Reader.ReplicaStatesSql, Reader.DatabaseReplicaStatesSql })
        {
            Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
            Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("DISTINCT ON", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ReadSql_SelectsIsLocalOnBothGrains()
    {
        /* V37 put is_local on the replica grain; V34 already had it on the database grain. Inserting it shifted
           the replica read's ordinals, which is the classic silent mis-map. */
        Assert.Contains("r.is_local", Reader.ReplicaStatesSql, StringComparison.Ordinal);
        Assert.Contains("d.is_local", Reader.DatabaseReplicaStatesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadSql_ReadsTheCollectQualifiedBaseTables()
    {
        /* Post-V14 collectors get no v_* passthrough view, so the readers name the base tables directly. */
        Assert.Contains("FROM collect.ag_replica_states", Reader.ReplicaStatesSql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.ag_database_replica_states", Reader.DatabaseReplicaStatesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadSql_SelectsEveryColumnTheDtoSurfaces()
    {
        foreach (var column in new[]
        {
            "role_desc", "operational_state_desc", "connected_state_desc", "recovery_health_desc",
            "synchronization_health_desc", "availability_mode_desc", "failover_mode_desc", "endpoint_url",
        })
        {
            Assert.Contains(column, Reader.ReplicaStatesSql, StringComparison.Ordinal);
        }

        foreach (var column in new[]
        {
            "synchronization_state_desc", "last_hardened_lsn", "last_commit_lsn", "log_send_queue_size",
            "redo_queue_size", "log_send_rate", "redo_rate", "is_suspended", "suspend_reason_desc",
            "secondary_lag_seconds", "is_local",
        })
        {
            Assert.Contains(column, Reader.DatabaseReplicaStatesSql, StringComparison.Ordinal);
        }
    }
}
