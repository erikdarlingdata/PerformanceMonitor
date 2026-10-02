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
        bool? isLocal = null,
        string? groupId = null) =>
        new(serverId, serverName, At(1), agName, replicaName, role, isLocal, operational, connected, recoveryHealth, syncHealth, "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://" + replicaName + ":5022", groupId);

    private static Reader.DatabaseRow Database(
        int serverId,
        string serverName,
        string agName,
        string databaseName,
        string replicaName,
        string state = "SYNCHRONIZED",
        bool isSuspended = false,
        long? lagSeconds = 0,
        int minutesAgo = 1,
        string? groupId = null) =>
        new(serverId, serverName, At(minutesAgo), agName, databaseName, replicaName, true, state, "0x00", "0x00", 0, 0, 1024, 1024, isSuspended, isSuspended ? "USER_ACTION" : null, "SYNCHRONOUS_COMMIT", lagSeconds, groupId);

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

    /* The SQL dialect/shape pins used to live here, against this reader's own copy of the statement text. That
       copy moved to DarlingAgStatesReaderTests (#4228) along with the statement text itself — see
       DarlingAgStatesReader in PerformanceMonitor.Darling.Storage, now the one implementation ReadReplicasAsync
       and ReadDatabasesAsync above map into this file's ReplicaRow / DatabaseRow. */

    /* ────────────────── #4475: DistinctAgCount by identity (name + replica set), not name alone ────────────────── */

    [Fact]
    public void DistinctAgCount_FortyTwoSameNamedAgsWithDistinctReplicaSets_IsFortyTwo()
    {
        /* Every Amazon RDS for SQL Server Multi-AZ instance's internal AG is named RDSAG0; each instance's
           replica set is distinct, so identity (name + replica set) must count 42, not 1. */
        var replicas = new List<Reader.ReplicaRow>();
        for (var i = 1; i <= 42; i++)
        {
            replicas.Add(Replica(i, $"NODE{i}A", "RDSAG0", $"NODE{i}A", "PRIMARY"));
            replicas.Add(Replica(i, $"NODE{i}A", "RDSAG0", $"NODE{i}B", "SECONDARY"));
        }

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(42, result.DistinctAgCount);
        Assert.Equal(42, result.AvailabilityGroupCount);
        Assert.Equal(42, result.ReportingServerCount);
    }

    [Fact]
    public void DistinctAgCount_TwoReplicasOfTheSameAg_IsOne()
    {
        /* Two monitored replicas of the SAME real AG report the SAME replica set, so identity still collapses
           them to one — the pre-existing case Build_OneAgSeenFromTwoServers_StaysTwoGroupsEachNamingItsReporter
           already covers for AvailabilityGroupCount; this is the same fixture asserted on DistinctAgCount. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY", operational: null),
            Replica(2, "NODE2", "AG1", "NODE1", "PRIMARY", operational: null),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(1, result.DistinctAgCount);
        Assert.Equal(2, result.AvailabilityGroupCount);
    }

    [Fact]
    public void DistinctAgCount_SameAgSeenFromItsPrimaryAndItsSecondary_IsOne()
    {
        /* The blocking #4475 follow-up case, proved through this reader's own public entry point: a monitored
           SECONDARY's replica-states view returns only its own local information, so its card carries only
           {S} while the primary's carries {P,S}. Exact-set identity counted this as 2, which this pin proves
           wrong. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY"),
            Replica(1, "NODE1", "AG1", "NODE2", "SECONDARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(1, result.DistinctAgCount);
        Assert.Equal(2, result.AvailabilityGroupCount);
    }

    [Fact]
    public void DistinctAgCount_TwoSecondariesOfOneAgSameGroupId_DisjointReplicaSets_IsOne()
    {
        /* (a), through this reader's own public entry point rather than calling AgTopology.CountDistinctGroups
           directly: two monitored SECONDARIES of one real AG, its primary unmonitored, each reporting only
           itself (so their replica sets are disjoint), carrying the SAME group_id. Without the id, the
           name+overlap rule alone counts 2 (see DistinctAgCount_SameAgSeenFromItsPrimaryAndItsSecondary_IsOne's
           companion below for the id-less shape) -- with it, they collapse to 1. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "SECONDARY", groupId: "GROUP-GUID-1"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY", groupId: "GROUP-GUID-1"),
        };

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(1, result.DistinctAgCount);
        Assert.Equal(2, result.AvailabilityGroupCount);
    }

    [Fact]
    public void DistinctAgCount_TwoSecondariesOfOneAg_NoGroupId_DisjointReplicaSets_IsTwo()
    {
        /* The id-LESS companion to the pin above, proving the RED this branch closes: the SAME two disjoint-
           replica-set secondaries, with NO group_id on either row, count as 2 under the name+overlap rule
           alone -- exactly the gap #4475's follow-up (V151) exists to close. */
        var replicas = new[]
        {
            Replica(1, "NODE1", "AG1", "NODE1", "SECONDARY"),
            Replica(2, "NODE2", "AG1", "NODE2", "SECONDARY"),
        };

        var result = Reader.Build(replicas, Array.Empty<Reader.DatabaseRow>(), At(0));

        Assert.Equal(2, result.DistinctAgCount);
        Assert.Equal(2, result.AvailabilityGroupCount);
    }

    /* ─────────────────────── #4474: fill by measured serialized bytes, not a fixed count ─────────────────────── */

    /// <summary>A realistic wide group: 2 replicas plus <paramref name="databaseCount"/> databases, with
    /// field widths like a real fleet's (LSN strings the shape SQL Server actually reports, endpoint URLs,
    /// suspend reasons on the unhealthy path) rather than the thin fixture #4471's original DefaultGroupLimit=11
    /// was sized from.</summary>
    private static (Reader.ReplicaRow[] Replicas, Reader.DatabaseRow[] Databases) WideGroup(
        int serverId, string agName, int databaseCount, bool critical = false)
    {
        var serverName = $"NODE{serverId:D3}A";
        var replicas = new[]
        {
            Replica(serverId, serverName, agName, serverName, "PRIMARY"),
            Replica(serverId, serverName, agName, $"NODE{serverId:D3}B", "SECONDARY", syncHealth: critical ? "NOT_HEALTHY" : "HEALTHY"),
        };

        var databases = new Reader.DatabaseRow[databaseCount];
        for (var i = 0; i < databaseCount; i++)
        {
            databases[i] = new Reader.DatabaseRow(
                serverId, serverName, At(1), agName, $"AppDatabase_{agName}_{i:D2}", $"NODE{serverId:D3}B", true,
                critical && i == 0 ? "NOT SYNCHRONIZING" : "SYNCHRONIZED",
                "00000029000A6B4C000100AE", "00000029000A6B4B00010098",
                critical && i == 0 ? 9500L : 12L, critical && i == 0 ? 3800L : 4L, 1024L, 1024L,
                critical && i == 0, critical && i == 0 ? "SUSPEND_FROM_USER" : null,
                "SYNCHRONOUS_COMMIT", critical && i == 0 ? 4820L : 0L, null);
        }

        return (replicas, databases);
    }

    /// <summary>
    /// #4474: the RUNTIME RED this branch closes. A 42-group fleet shaped like the field report (2 replicas
    /// plus 10 databases/group, realistic field widths) overshoots the 32 KB budget by close to 2x at the OLD
    /// fixed DefaultGroupLimit=11 count-cap — the exact failure mode a byte-measured fill replaces. This pin
    /// asserts the NEW behavior (fits the budget, returns fewer than all 42, flags truncation) and is also the
    /// vehicle for the mutation: reverting <c>Build</c>'s fill loop to the old <c>groups.Take(limit)</c> shape
    /// turns this red (see the PR body for the measured before/after).
    /// </summary>
    [Fact]
    public void Build_FortyTwoWideGroups_FillsToTheByteBudgetMostSevereFirst()
    {
        var allReplicas = new List<Reader.ReplicaRow>();
        var allDatabases = new List<Reader.DatabaseRow>();
        for (var i = 0; i < 42; i++)
        {
            var (replicas, databases) = WideGroup(i + 1, $"AG_FLEET_{i:D2}", 10);
            allReplicas.AddRange(replicas);
            allDatabases.AddRange(databases);
        }

        var result = Reader.Build(allReplicas, allDatabases, At(0), limit: 100);
        var json = JsonSerializer.Serialize(result, Reader.JsonOptions);
        var bytes = System.Text.Encoding.UTF8.GetByteCount(json);

        Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"response was {bytes} bytes, over the {McpResponseBudget.DefaultBytes} budget");
        Assert.Equal(42, result.GroupsTotal);
        Assert.True(result.GroupsReturned < 42, "a byte-fit page over a 42-group wide fleet must not return every group");
        Assert.Equal(result.GroupsReturned, result.AvailabilityGroups.Count);
        Assert.True(result.GroupsTruncated);
        Assert.NotNull(result.GroupsTruncatedNote);
        Assert.Contains("byte", result.GroupsTruncatedNote, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>#4474 (b): a small fleet (3 databases/group, well under the byte budget on its own) still gets
    /// every group back — no regression from the byte-fit walk on the common case the old fixed cap of 11
    /// already handled fine.</summary>
    [Fact]
    public void Build_SmallFleet_ReturnsEveryGroupUnderTheByteBudget()
    {
        var allReplicas = new List<Reader.ReplicaRow>();
        var allDatabases = new List<Reader.DatabaseRow>();
        for (var i = 0; i < 5; i++)
        {
            var (replicas, databases) = WideGroup(i + 1, $"AG_SMALL_{i:D2}", 3);
            allReplicas.AddRange(replicas);
            allDatabases.AddRange(databases);
        }

        var result = Reader.Build(allReplicas, allDatabases, At(0), limit: 100);

        Assert.Equal(5, result.GroupsTotal);
        Assert.Equal(5, result.GroupsReturned);
        Assert.False(result.GroupsTruncated);
        Assert.Null(result.GroupsTruncatedNote);
    }

    /// <summary>#4474 (c): a single group so wide it alone exceeds the budget still comes back — exactly 1
    /// group, never 0 — with the note saying the budget, not the count, was the reason.</summary>
    [Fact]
    public void Build_OneOversizedGroup_ReturnsExactlyOneGroupNeverZero()
    {
        var (replicas, databases) = WideGroup(1, "AG_HUGE", 400);

        var result = Reader.Build(replicas, databases, At(0), limit: 100);
        var json = JsonSerializer.Serialize(result, Reader.JsonOptions);
        var bytes = System.Text.Encoding.UTF8.GetByteCount(json);

        Assert.True(bytes > McpResponseBudget.DefaultBytes, $"fixture must itself exceed the budget to prove the floor; measured {bytes}");
        Assert.Equal(1, result.GroupsTotal);
        Assert.Equal(1, result.GroupsReturned);
        Assert.Single(result.AvailabilityGroups);
        Assert.False(result.GroupsTruncated, "a single group, even oversized, is not itself a cut");
        Assert.Null(result.GroupsTruncatedNote);
    }

    /// <summary>
    /// #4568: the tail-correction pin. This fixture's 12-group page fits the byte budget WITHOUT the
    /// <c>groups_truncated_note</c> field (32,638 bytes, measured off a bare envelope the same way the old
    /// fill loop measured it) but goes OVER budget once the real, final note text is counted (32,810 bytes) —
    /// exactly the gap #4568 found: the fill loop's stopping point ignored the note it was about to add. Asserts
    /// the ACTUAL RETURNED response (the one a caller receives) fits the budget regardless. RED before the tail
    /// correction, because the fill loop's page (12 groups) is the one returned unmodified.
    /// </summary>


    [Fact]
    public void Build_NoteItselfWouldPushPastBudget_TailCorrectionDropsAGroupToFit()
    {
        var allReplicas = new List<Reader.ReplicaRow>();
        var allDatabases = new List<Reader.DatabaseRow>();
        for (var i = 0; i < 20; i++)
        {
            /* The single extra 'X' in every name (over the #4474 fixture's shape) is what makes the note's
               own byte cost tip a page that fits without the note over budget with it — see the PR body for
               the measured before/after this fixture was tuned from. */
            var (replicas, databases) = WideGroup(i + 1, $"AG_TAIL_{i:D2}XX", 3);
            allReplicas.AddRange(replicas);
            allDatabases.AddRange(databases);
        }

        var result = Reader.Build(allReplicas, allDatabases, At(0), limit: 21);
        var bytes = System.Text.Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(result, Reader.JsonOptions));

        Assert.True(result.GroupsTruncated);
        Assert.NotNull(result.GroupsTruncatedNote);
        Assert.True(bytes <= McpResponseBudget.DefaultBytes,
            $"the response actually returned (note included) must itself fit the budget; measured {bytes}");
        Assert.Equal(result.GroupsReturned, result.AvailabilityGroups.Count);
        /* The note's own group count must match what's actually returned -- the tail correction rebuilds the
           note after every group it drops, so a stale count (naming a page one group larger than what's back)
           would mean the correction dropped a group without re-wording the note that describes it. */
        Assert.Contains($"only {result.GroupsReturned} fit", result.GroupsTruncatedNote);
    }

    /// <summary>#4474 (d): an explicit limit smaller than what the byte budget would allow still caps at
    /// exactly that limit — limit stays an upper bound underneath the budget, not just a suggestion the budget
    /// walk can override upward.</summary>
    [Fact]
    public void Build_ExplicitLimitSmallerThanBudgetFit_CapsAtExactlyTheLimit()
    {
        var allReplicas = new List<Reader.ReplicaRow>();
        var allDatabases = new List<Reader.DatabaseRow>();
        for (var i = 0; i < 10; i++)
        {
            var (replicas, databases) = WideGroup(i + 1, $"AG_CAP_{i:D2}", 3);
            allReplicas.AddRange(replicas);
            allDatabases.AddRange(databases);
        }

        var result = Reader.Build(allReplicas, allDatabases, At(0), limit: 4);

        Assert.Equal(10, result.GroupsTotal);
        Assert.Equal(4, result.GroupsReturned);
        Assert.Equal(4, result.AvailabilityGroups.Count);
        Assert.True(result.GroupsTruncated);
        Assert.Contains("top 4", result.GroupsTruncatedNote);
    }
}
