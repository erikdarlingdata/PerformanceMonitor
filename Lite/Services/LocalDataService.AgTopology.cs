/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The Availability Group TOPOLOGY read for Lite's AG tab (#991) — the DuckDB half only. Banding and the card
/// projection come from <see cref="AgTopology"/> (PerformanceMonitor.Common), shared with Darling's viewer tab
/// and the web dashboard, so the three surfaces cannot disagree about the same AG.
///
/// <para><b>Why this is not <c>GetLatestAgReplicaStatesAsync</c>.</b> That read serves the ALERT path (#1696) and
/// is right for it, but wrong here for two reasons. It selects five columns — the topology view also needs
/// operational state, recovery health, synchronization health, availability and failover mode, and the endpoint.
/// And it DROPS rows whose <c>ag_name</c> or <c>replica_server_name</c> is NULL, correctly, because those are the
/// alert's state-key identity and a row that cannot be keyed cannot have an edge tracked for it. A topology view
/// must do the opposite: those NULLs appear under WSFC quorum loss, which is exactly when an operator most wants
/// to look at the page, so the rows are kept and render as "—" rather than vanishing.</para>
///
/// <para>Lite monitors one server, so this returns that server's own perspective — a single card per AG. That is
/// the same per-reporting-server rule Darling applies, with one reporter. On a secondary the DMV returns only
/// local information, so a Lite instance pointed at a secondary honestly shows one replica and no primary; that
/// is the truth from where it stands, not a defect.</para>
/// </summary>
public partial class LocalDataService
{
    /// <summary>Every replica row at this server's newest AG snapshot, all columns the topology view needs.
    /// No parameters — Lite may monitor more than one server, and each contributes its OWN newest snapshot.</summary>
    public const string AgTopologyReplicaSql = @"
SELECT
    server_id,
    server_name,
    collection_time,
    ag_name,
    replica_server_name,
    role_desc,
    is_local,
    operational_state_desc,
    connected_state_desc,
    recovery_health_desc,
    synchronization_health_desc,
    availability_mode_desc,
    failover_mode_desc,
    endpoint_url,
    group_id
FROM ag_replica_states AS r
WHERE r.collection_time = (SELECT MAX(x.collection_time) FROM ag_replica_states AS x WHERE x.server_id = r.server_id)
ORDER BY r.server_name, r.ag_name, r.replica_server_name";

    /// <summary>Every database row at this server's newest AG snapshot. Windowed independently of the replica
    /// grain — the two collectors sweep separately, so their newest instants need not coincide. $1 the server
    /// No parameters.</summary>
    public const string AgTopologyDatabaseSql = @"
SELECT
    server_id,
    server_name,
    collection_time,
    ag_name,
    database_name,
    replica_server_name,
    is_local,
    synchronization_state_desc,
    log_send_queue_size,
    redo_queue_size,
    log_send_rate,
    redo_rate,
    is_suspended,
    suspend_reason_desc,
    availability_mode_desc,
    secondary_lag_seconds,
    group_id
FROM ag_database_replica_states AS d
WHERE d.collection_time = (SELECT MAX(x.collection_time) FROM ag_database_replica_states AS x WHERE x.server_id = d.server_id)
ORDER BY d.server_name, d.ag_name, d.database_name, d.replica_server_name";

    /// <summary>
    /// This server's AG topology as cards. Zero replicas short-circuits before the database read — the normal
    /// case on an instance with no Availability Groups, and this runs on the tab's refresh.
    /// </summary>
    public async Task<List<AgTopologyCard>> GetAgTopologyAsync()
    {
        var replicas = await ReadAgTopologyReplicasAsync();
        if (replicas.Count == 0)
        {
            return new List<AgTopologyCard>();
        }

        return AgTopology.BuildCards(replicas, await ReadAgTopologyDatabasesAsync());
    }

    private async Task<List<AgTopologyReplicaRow>> ReadAgTopologyReplicasAsync()
    {
        var rows = new List<AgTopologyReplicaRow>();

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = AgTopologyReplicaSql;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new AgTopologyReplicaRow
            {
                ServerId = (int)ToInt64(reader.GetValue(0)),
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                CollectionTime = reader.IsDBNull(2) ? default : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                AgName = AgText(reader, 3),
                ReplicaServerName = AgText(reader, 4),
                RoleDesc = AgText(reader, 5),
                IsLocal = AgFlag(reader, 6),
                OperationalStateDesc = AgText(reader, 7),
                ConnectedStateDesc = AgText(reader, 8),
                RecoveryHealthDesc = AgText(reader, 9),
                SynchronizationHealthDesc = AgText(reader, 10),
                AvailabilityModeDesc = AgText(reader, 11),
                FailoverModeDesc = AgText(reader, 12),
                EndpointUrl = AgText(reader, 13),
                GroupId = AgText(reader, 14),
            });
        }

        return rows;
    }

    private async Task<List<AgTopologyDatabaseRow>> ReadAgTopologyDatabasesAsync()
    {
        var rows = new List<AgTopologyDatabaseRow>();

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = AgTopologyDatabaseSql;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new AgTopologyDatabaseRow
            {
                ServerId = (int)ToInt64(reader.GetValue(0)),
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                CollectionTime = reader.IsDBNull(2) ? default : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                AgName = AgText(reader, 3),
                DatabaseName = AgText(reader, 4),
                ReplicaServerName = AgText(reader, 5),
                IsLocal = AgFlag(reader, 6),
                SynchronizationStateDesc = AgText(reader, 7),
                LogSendQueueKb = AgCount(reader, 8),
                RedoQueueKb = AgCount(reader, 9),
                LogSendRateKbPerSec = AgCount(reader, 10),
                RedoRateKbPerSec = AgCount(reader, 11),
                IsSuspended = AgFlag(reader, 12),
                SuspendReasonDesc = AgText(reader, 13),
                AvailabilityModeDesc = AgText(reader, 14),
                SecondaryLagSeconds = AgCount(reader, 15),
                GroupId = AgText(reader, 16),
            });
        }

        return rows;
    }

    private static string? AgText(System.Data.Common.DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static bool? AgFlag(System.Data.Common.DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetBoolean(ordinal);

    private static long? AgCount(System.Data.Common.DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : ToInt64(reader.GetValue(ordinal));
}
