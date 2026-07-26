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
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Media;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Availability Group topology read for the viewer's fleet-level AG tab (#991) — the desktop twin of the web
/// dashboard's <c>/api/ag</c>. The banding rules and the latest-snapshot-per-server semantics are carried over
/// from the service's <c>DarlingAgReader</c> verbatim, because the two surfaces must never disagree about whether
/// an AG is healthy.
///
/// <para>Copied rather than referenced: the viewer has no ProjectReference to the headless service and that reader
/// is internal to it. Same reason the rest of this class re-implements service reads against Postgres.</para>
///
/// <para><b>Grain: one card per (reporting server, AG), deliberately not merged.</b> Every monitored replica
/// reports the whole AG's replica set, so an AG with several monitored replicas yields several cards. They are NOT
/// collapsed: the DMV populates <c>operational_state</c> / <c>recovery_health</c> for the LOCAL replica only,
/// <c>connected_state</c> is meaningful only from the primary, and a quorum-lost instance answers from cached
/// metadata. Observed live on the AG fixture: the primary reports two replicas and names the primary, while the
/// secondary reports ONE replica and no primary at all. Merging those would let the secondary's blind view
/// overwrite the primary's complete one.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Every replica row from each server's NEWEST AG collection. The inner aggregate picks the latest
    /// instant per server and the join keeps ALL rows at that instant (not <c>DISTINCT ON</c>, which would keep
    /// only one replica). Joined to the enabled registry so a disabled server's AGs leave the surface with it.
    /// Bare table names — the store's search_path resolves <c>collect</c>. $ none.</summary>
    public const string AgReplicaStatesSql = @"
SELECT
    r.server_id,
    r.server_name,
    r.collection_time,
    r.ag_name,
    r.replica_server_name,
    r.role_desc,
    r.operational_state_desc,
    r.connected_state_desc,
    r.recovery_health_desc,
    r.synchronization_health_desc,
    r.availability_mode_desc,
    r.failover_mode_desc,
    r.endpoint_url
FROM ag_replica_states AS r
JOIN
(
    SELECT server_id, MAX(collection_time) AS max_collection_time
    FROM ag_replica_states
    GROUP BY server_id
) AS latest
    ON r.server_id = latest.server_id
    AND r.collection_time = latest.max_collection_time
JOIN servers AS s
    ON s.server_id = r.server_id
    AND s.is_enabled
ORDER BY r.server_name, r.ag_name, r.replica_server_name";

    /// <summary>Every database-grain row from each server's NEWEST AG collection, same latest-snapshot shape as
    /// <see cref="AgReplicaStatesSql"/>. Windowed independently: the two collectors sweep separately, so their
    /// newest instants need not coincide. $ none.</summary>
    public const string AgDatabaseReplicaStatesSql = @"
SELECT
    d.server_id,
    d.server_name,
    d.collection_time,
    d.ag_name,
    d.database_name,
    d.replica_server_name,
    d.is_local,
    d.synchronization_state_desc,
    d.log_send_queue_size,
    d.redo_queue_size,
    d.log_send_rate,
    d.redo_rate,
    d.is_suspended,
    d.suspend_reason_desc,
    d.availability_mode_desc,
    d.secondary_lag_seconds
FROM ag_database_replica_states AS d
JOIN
(
    SELECT server_id, MAX(collection_time) AS max_collection_time
    FROM ag_database_replica_states
    GROUP BY server_id
) AS latest
    ON d.server_id = latest.server_id
    AND d.collection_time = latest.max_collection_time
JOIN servers AS s
    ON s.server_id = d.server_id
    AND s.is_enabled
ORDER BY d.server_name, d.ag_name, d.database_name, d.replica_server_name";

    /// <summary>
    /// Reads the fleet's AG topology into per-(reporting server, AG) cards. An AG-less fleet — the common case —
    /// costs exactly one indexed read that returns nothing, because the database-grain read is skipped entirely
    /// when there are no replicas. This runs on the viewer's refresh timer, so that short-circuit matters.
    /// </summary>
    public async Task<List<ViewerAvailabilityGroup>> GetAvailabilityGroupsAsync(CancellationToken cancellationToken = default)
    {
        var replicas = await ReadAgReplicasAsync(cancellationToken);
        if (replicas.Count == 0)
        {
            return new List<ViewerAvailabilityGroup>();
        }

        var databases = await ReadAgDatabasesAsync(cancellationToken);
        return BuildAvailabilityGroups(replicas, databases);
    }

    private async Task<List<ViewerAgReplicaRow>> ReadAgReplicasAsync(CancellationToken cancellationToken)
    {
        var rows = new List<ViewerAgReplicaRow>();
        await using var command = _dataSource.CreateCommand(AgReplicaStatesSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerAgReplicaRow
            {
                ServerId = reader.GetInt32(0),
                ServerName = reader.GetString(1),
                CollectionTime = reader.GetDateTime(2),
                AgName = Text(reader, 3),
                ReplicaServerName = Text(reader, 4),
                RoleDesc = Text(reader, 5),
                OperationalStateDesc = Text(reader, 6),
                ConnectedStateDesc = Text(reader, 7),
                RecoveryHealthDesc = Text(reader, 8),
                SynchronizationHealthDesc = Text(reader, 9),
                AvailabilityModeDesc = Text(reader, 10),
                FailoverModeDesc = Text(reader, 11),
                EndpointUrl = Text(reader, 12),
            });
        }

        return rows;
    }

    private async Task<List<ViewerAgDatabaseRow>> ReadAgDatabasesAsync(CancellationToken cancellationToken)
    {
        var rows = new List<ViewerAgDatabaseRow>();
        await using var command = _dataSource.CreateCommand(AgDatabaseReplicaStatesSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerAgDatabaseRow
            {
                ServerId = reader.GetInt32(0),
                ServerName = reader.GetString(1),
                CollectionTime = reader.GetDateTime(2),
                AgName = Text(reader, 3),
                DatabaseName = Text(reader, 4),
                ReplicaServerName = Text(reader, 5),
                IsLocal = Flag(reader, 6),
                SynchronizationStateDesc = Text(reader, 7),
                LogSendQueueKb = Count(reader, 8),
                RedoQueueKb = Count(reader, 9),
                LogSendRateKbPerSec = Count(reader, 10),
                RedoRateKbPerSec = Count(reader, 11),
                IsSuspended = Flag(reader, 12),
                SuspendReasonDesc = Text(reader, 13),
                AvailabilityModeDesc = Text(reader, 14),
                SecondaryLagSeconds = Count(reader, 15),
            });
        }

        return rows;
    }

    private static string? Text(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static bool? Flag(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetBoolean(ordinal);

    private static long? Count(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt64(reader.GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// Shapes raw rows into cards — pure, so the grouping and every band unit-test without a store. The REPLICA
    /// grain defines the card set: a database row whose (server, AG) has no replica row is dropped, which is only
    /// reachable when the two collectors' newest snapshots straddle an AG being created or dropped.
    /// </summary>
    internal static List<ViewerAvailabilityGroup> BuildAvailabilityGroups(
        IReadOnlyList<ViewerAgReplicaRow> replicas,
        IReadOnlyList<ViewerAgDatabaseRow> databases)
    {
        var databasesByGroup = databases
            .GroupBy(d => (d.ServerId, AgKey(d.AgName)))
            .ToDictionary(g => g.Key, g => g.ToList());

        var groups = new List<ViewerAvailabilityGroup>();

        foreach (var group in replicas.GroupBy(r => (r.ServerId, AgKey(r.AgName))))
        {
            var rows = group.ToList();
            var first = rows[0];
            databasesByGroup.TryGetValue(group.Key, out var dbRows);

            var replicaItems = rows.Select(ToReplicaItem).ToList();
            var databaseItems = (dbRows ?? new List<ViewerAgDatabaseRow>()).Select(ToDatabaseItem).ToList();

            var worst = HealthSeverity.Unknown;
            foreach (var replica in replicaItems)
            {
                worst = Worse(worst, replica.Severity);
            }

            foreach (var database in databaseItems)
            {
                worst = Worse(worst, database.SynchronizationStateSeverity);
            }

            groups.Add(new ViewerAvailabilityGroup
            {
                ServerId = first.ServerId,
                ServerName = first.ServerName,
                AgName = first.AgName,
                CollectionTime = first.CollectionTime,
                DatabaseCollectionTime = dbRows is { Count: > 0 } ? dbRows[0].CollectionTime : null,
                PrimaryReplica = replicaItems.FirstOrDefault(r => r.IsPrimary)?.ReplicaServerName,
                Severity = worst,
                Replicas = replicaItems,
                Databases = databaseItems,
            });
        }

        /* Worst-first, then AG name, then reporting server — so the several perspectives on one AG stay
           adjacent once severity ties, and problems surface without scrolling. */
        groups.Sort(static (a, b) =>
        {
            var bySeverity = b.Severity.CompareTo(a.Severity);
            if (bySeverity != 0)
            {
                return bySeverity;
            }

            var byName = string.Compare(a.AgName ?? "", b.AgName ?? "", StringComparison.OrdinalIgnoreCase);
            return byName != 0 ? byName : string.Compare(a.ServerName, b.ServerName, StringComparison.OrdinalIgnoreCase);
        });

        return groups;
    }

    private static string AgKey(string? agName) => (agName ?? "").ToUpperInvariant();

    private static HealthSeverity Worse(HealthSeverity a, HealthSeverity b) => a > b ? a : b;

    private static ViewerAgReplica ToReplicaItem(ViewerAgReplicaRow row)
    {
        var syncHealth = AgSynchronizationHealthSeverity(row.SynchronizationHealthDesc);
        var connected = AgConnectedStateSeverity(row.ConnectedStateDesc);
        var operational = AgOperationalStateSeverity(row.OperationalStateDesc);
        var recovery = AgRecoveryHealthSeverity(row.RecoveryHealthDesc);
        var role = AgRoleSeverity(row.RoleDesc);

        return new ViewerAgReplica
        {
            ReplicaServerName = row.ReplicaServerName,
            RoleDesc = row.RoleDesc,
            IsPrimary = string.Equals(row.RoleDesc, "PRIMARY", StringComparison.OrdinalIgnoreCase),
            OperationalStateDesc = row.OperationalStateDesc,
            ConnectedStateDesc = row.ConnectedStateDesc,
            RecoveryHealthDesc = row.RecoveryHealthDesc,
            SynchronizationHealthDesc = row.SynchronizationHealthDesc,
            AvailabilityModeDesc = row.AvailabilityModeDesc,
            FailoverModeDesc = row.FailoverModeDesc,
            EndpointUrl = row.EndpointUrl,
            Severity = Worse(Worse(Worse(Worse(syncHealth, connected), operational), recovery), role),
        };
    }

    private static ViewerAgDatabase ToDatabaseItem(ViewerAgDatabaseRow row) => new()
    {
        DatabaseName = row.DatabaseName,
        ReplicaServerName = row.ReplicaServerName,
        IsLocal = row.IsLocal,
        SynchronizationStateDesc = row.SynchronizationStateDesc,
        SynchronizationStateSeverity = AgDatabaseSyncSeverity(row.SynchronizationStateDesc, row.AvailabilityModeDesc, row.IsSuspended),
        AvailabilityModeDesc = row.AvailabilityModeDesc,
        LogSendQueueKb = row.LogSendQueueKb,
        RedoQueueKb = row.RedoQueueKb,
        LogSendRateKbPerSec = row.LogSendRateKbPerSec,
        RedoRateKbPerSec = row.RedoRateKbPerSec,
        EstimatedSendDrainMinutes = AgDrainMinutes(row.LogSendQueueKb, row.LogSendRateKbPerSec),
        EstimatedRedoCompletionMinutes = AgDrainMinutes(row.RedoQueueKb, row.RedoRateKbPerSec),
        SecondaryLagSeconds = row.SecondaryLagSeconds,
        IsSuspended = row.IsSuspended,
        SuspendReasonDesc = row.SuspendReasonDesc,
    };

    /* ─────────────────────────── banding (mirrors DarlingAgReader exactly) ─────────────────────────── */

    /// <summary>Bands a replica's <c>synchronization_health_desc</c> — the one health column the DMV populates for
    /// EVERY replica, not just the local one.</summary>
    internal static HealthSeverity AgSynchronizationHealthSeverity(string? healthDesc) =>
        NormalizeAg(healthDesc) switch
        {
            "HEALTHY" => HealthSeverity.Healthy,
            "PARTIALLY_HEALTHY" => HealthSeverity.Warning,
            "NOT_HEALTHY" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>connected_state_desc</c>; meaningful from the primary, NULL elsewhere.</summary>
    internal static HealthSeverity AgConnectedStateSeverity(string? connectedDesc) =>
        NormalizeAg(connectedDesc) switch
        {
            "CONNECTED" => HealthSeverity.Healthy,
            "DISCONNECTED" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>operational_state_desc</c> (PENDING_FAILOVER / PENDING / ONLINE / OFFLINE / FAILED /
    /// FAILED_NO_QUORUM / NULL). Reported for the LOCAL replica only, so a remote replica's NULL bands Unknown
    /// rather than reading as a problem. <c>ONLINE_IN_PROGRESS</c> belongs to recovery health, not here.</summary>
    internal static HealthSeverity AgOperationalStateSeverity(string? operationalDesc) =>
        NormalizeAg(operationalDesc) switch
        {
            "ONLINE" => HealthSeverity.Healthy,
            "PENDING" or "PENDING_FAILOVER" => HealthSeverity.Warning,
            "OFFLINE" or "FAILED" or "FAILED_NO_QUORUM" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>recovery_health_desc</c> — documented as exactly ONLINE_IN_PROGRESS / ONLINE / NULL.</summary>
    internal static HealthSeverity AgRecoveryHealthSeverity(string? recoveryDesc) =>
        NormalizeAg(recoveryDesc) switch
        {
            "ONLINE" => HealthSeverity.Healthy,
            "ONLINE_IN_PROGRESS" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>role_desc</c>. RESOLVING is itself a finding — the replica holds neither real role, which
    /// is what a replica looks like mid-failover or after losing quorum.</summary>
    internal static HealthSeverity AgRoleSeverity(string? roleDesc) =>
        NormalizeAg(roleDesc) switch
        {
            "PRIMARY" or "SECONDARY" => HealthSeverity.Healthy,
            "RESOLVING" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>
    /// Bands a database's synchronization state AGAINST ITS AVAILABILITY MODE — the distinction a flat state map
    /// gets wrong. SYNCHRONIZING is the steady, correct state for an ASYNCHRONOUS_COMMIT replica (async never
    /// reaches SYNCHRONIZED), so coloring it amber there would paint every healthy async secondary as a problem;
    /// on a SYNCHRONOUS_COMMIT replica the same state means it is NOT currently protecting a commit. Suspended
    /// data movement outranks the state text entirely, because <c>secondary_lag_seconds</c> reads 0 — not NULL —
    /// while suspended, so a suspended replica otherwise presents as perfectly caught up.
    /// </summary>
    internal static HealthSeverity AgDatabaseSyncSeverity(string? stateDesc, string? availabilityModeDesc, bool? isSuspended)
    {
        if (isSuspended == true)
        {
            return HealthSeverity.Critical;
        }

        return NormalizeAg(stateDesc) switch
        {
            "SYNCHRONIZED" => HealthSeverity.Healthy,
            "SYNCHRONIZING" => string.Equals(NormalizeAg(availabilityModeDesc), "ASYNCHRONOUS_COMMIT", StringComparison.Ordinal)
                ? HealthSeverity.Healthy
                : HealthSeverity.Warning,
            "NOT_SYNCHRONIZING" => HealthSeverity.Critical,
            "REVERTING" or "INITIALIZING" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };
    }

    /// <summary>Minutes to drain a queue at the current rate — DERIVED (queue / rate), because the DMV exposes only
    /// the queue (KB) and the rate (KB/s). An empty queue is 0; a non-empty queue moving at 0 KB/s has no finite
    /// estimate and returns null, never a divide-by-zero infinity and never a misleading 0.</summary>
    internal static double? AgDrainMinutes(long? queueKb, long? rateKbPerSec)
    {
        if (queueKb is null)
        {
            return null;
        }

        if (queueKb.Value <= 0)
        {
            return 0;
        }

        if (rateKbPerSec is null || rateKbPerSec.Value <= 0)
        {
            return null;
        }

        return Math.Round(queueKb.Value / (double)rateKbPerSec.Value / 60.0, 2, MidpointRounding.AwayFromZero);
    }

    /// <summary>Upper-cases and collapses the DMVs' two spellings to one form: the database grain reports states
    /// space-separated (<c>NOT SYNCHRONIZING</c>) where the replica grain uses underscores (<c>NOT_HEALTHY</c>),
    /// and both are stored verbatim.</summary>
    private static string NormalizeAg(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "" : value.Trim().Replace(' ', '_').ToUpperInvariant();
}

/* ─────────────────────────── raw rows ─────────────────────────── */

/// <summary>One replica-grain row, exactly as <c>ag_replica_states</c> stores it.</summary>
public sealed class ViewerAgReplicaRow
{
    public int ServerId { get; init; }
    public string ServerName { get; init; } = "";
    public DateTime CollectionTime { get; init; }
    public string? AgName { get; init; }
    public string? ReplicaServerName { get; init; }
    public string? RoleDesc { get; init; }
    public string? OperationalStateDesc { get; init; }
    public string? ConnectedStateDesc { get; init; }
    public string? RecoveryHealthDesc { get; init; }
    public string? SynchronizationHealthDesc { get; init; }
    public string? AvailabilityModeDesc { get; init; }
    public string? FailoverModeDesc { get; init; }
    public string? EndpointUrl { get; init; }
}

/// <summary>One database-grain row. Queue sizes are KB and rates KB/s (the DMV's units), both instantaneous
/// gauges rather than counters.</summary>
public sealed class ViewerAgDatabaseRow
{
    public int ServerId { get; init; }
    public string ServerName { get; init; } = "";
    public DateTime CollectionTime { get; init; }
    public string? AgName { get; init; }
    public string? DatabaseName { get; init; }
    public string? ReplicaServerName { get; init; }
    public bool? IsLocal { get; init; }
    public string? SynchronizationStateDesc { get; init; }
    public long? LogSendQueueKb { get; init; }
    public long? RedoQueueKb { get; init; }
    public long? LogSendRateKbPerSec { get; init; }
    public long? RedoRateKbPerSec { get; init; }
    public bool? IsSuspended { get; init; }
    public string? SuspendReasonDesc { get; init; }
    public string? AvailabilityModeDesc { get; init; }
    public long? SecondaryLagSeconds { get; init; }
}

/* ─────────────────────────── bound items ─────────────────────────── */

/// <summary>One replica chip on an AG card. Brushes come from the viewer's frozen severity palette, the same
/// four the Overview cards use.</summary>
public sealed class ViewerAgReplica
{
    private static readonly SolidColorBrush s_criticalBrush = MakeAgBrush("#E57373");
    private static readonly SolidColorBrush s_warningBrush = MakeAgBrush("#FFD54F");
    private static readonly SolidColorBrush s_healthyBrush = MakeAgBrush("#81C784");
    private static readonly SolidColorBrush s_unknownBrush = MakeAgBrush("#888888");

    internal static SolidColorBrush MakeAgBrush(string hex)
    {
        var brush = new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
        brush.Freeze();
        return brush;
    }

    internal static SolidColorBrush BrushFor(HealthSeverity severity) => severity switch
    {
        HealthSeverity.Critical => s_criticalBrush,
        HealthSeverity.Warning => s_warningBrush,
        HealthSeverity.Healthy => s_healthyBrush,
        _ => s_unknownBrush,
    };

    public string? ReplicaServerName { get; init; }
    public string? RoleDesc { get; init; }
    public bool IsPrimary { get; init; }
    public string? OperationalStateDesc { get; init; }
    public string? ConnectedStateDesc { get; init; }
    public string? RecoveryHealthDesc { get; init; }
    public string? SynchronizationHealthDesc { get; init; }
    public string? AvailabilityModeDesc { get; init; }
    public string? FailoverModeDesc { get; init; }
    public string? EndpointUrl { get; init; }
    public HealthSeverity Severity { get; init; }

    public string RoleDisplay => string.IsNullOrWhiteSpace(RoleDesc) ? "UNKNOWN ROLE" : RoleDesc!;
    public string NameDisplay => string.IsNullOrWhiteSpace(ReplicaServerName) ? "—" : ReplicaServerName!;
    public SolidColorBrush SeverityBrush => BrushFor(Severity);

    /// <summary>The states worth reading at chip size. A column the DMV leaves null is omitted rather than shown
    /// as an em dash the reader has to interpret; recovery health joins only when it is NOT plain ONLINE, since a
    /// healthy one would just repeat the operational state's "ONLINE".</summary>
    public string StateDisplay
    {
        get
        {
            var parts = new List<string>(4);
            foreach (var value in new[] { SynchronizationHealthDesc, ConnectedStateDesc, OperationalStateDesc })
            {
                if (!string.IsNullOrWhiteSpace(value))
                {
                    parts.Add(value!);
                }
            }

            if (!string.IsNullOrWhiteSpace(RecoveryHealthDesc)
                && !string.Equals(RecoveryHealthDesc, "ONLINE", StringComparison.OrdinalIgnoreCase))
            {
                parts.Add("recovery " + RecoveryHealthDesc);
            }

            return string.Join(" · ", parts);
        }
    }

    public string ModeDisplay =>
        string.Join(" · ", new[] { AvailabilityModeDesc, FailoverModeDesc }.Where(v => !string.IsNullOrWhiteSpace(v)));
}

/// <summary>One database row in an AG card's grid.</summary>
public sealed class ViewerAgDatabase
{
    public string? DatabaseName { get; init; }
    public string? ReplicaServerName { get; init; }
    public bool? IsLocal { get; init; }
    public string? SynchronizationStateDesc { get; init; }
    public HealthSeverity SynchronizationStateSeverity { get; init; }
    public string? AvailabilityModeDesc { get; init; }
    public long? LogSendQueueKb { get; init; }
    public long? RedoQueueKb { get; init; }
    public long? LogSendRateKbPerSec { get; init; }
    public long? RedoRateKbPerSec { get; init; }
    public double? EstimatedSendDrainMinutes { get; init; }
    public double? EstimatedRedoCompletionMinutes { get; init; }
    public long? SecondaryLagSeconds { get; init; }
    public bool? IsSuspended { get; init; }
    public string? SuspendReasonDesc { get; init; }

    public string DatabaseDisplay => DatabaseName ?? "—";
    public string ReplicaDisplay => ReplicaServerName ?? "—";
    public string SyncStateDisplay => SynchronizationStateDesc ?? "—";
    public SolidColorBrush SyncStateBrush => ViewerAgReplica.BrushFor(SynchronizationStateSeverity);

    public string SendQueueDisplay => LogSendQueueKb?.ToString("N0") ?? "—";
    public string RedoQueueDisplay => RedoQueueKb?.ToString("N0") ?? "—";
    public string SendRateDisplay => LogSendRateKbPerSec?.ToString("N0") ?? "—";
    public string RedoRateDisplay => RedoRateKbPerSec?.ToString("N0") ?? "—";
    public string SendDrainDisplay => EstimatedSendDrainMinutes?.ToString("N1") ?? "—";
    public string RedoDrainDisplay => EstimatedRedoCompletionMinutes?.ToString("N1") ?? "—";

    /// <summary>Lag needs its suspension context inline: the DMV reports 0 — not null — while data movement is
    /// suspended, so a suspended replica reads as perfectly caught up on the number alone.</summary>
    public string LagDisplay => SecondaryLagSeconds is null
        ? "—"
        : IsSuspended == true
            ? $"{SecondaryLagSeconds:N0} s (suspended)"
            : $"{SecondaryLagSeconds:N0} s";

    public string DataMovementDisplay => IsSuspended == true
        ? (string.IsNullOrWhiteSpace(SuspendReasonDesc) ? "Suspended" : "Suspended: " + SuspendReasonDesc)
        : IsSuspended == false ? "Moving" : "—";

    public SolidColorBrush DataMovementBrush =>
        ViewerAgReplica.BrushFor(IsSuspended == true ? HealthSeverity.Critical : HealthSeverity.Unknown);
}

/// <summary>One monitored server's VIEW of one Availability Group — the card the tab renders.</summary>
public sealed class ViewerAvailabilityGroup
{
    public int ServerId { get; init; }

    /// <summary>The REPORTING server — the monitored instance whose DMVs produced this view, not necessarily the
    /// AG's primary.</summary>
    public string ServerName { get; init; } = "";

    public string? AgName { get; init; }
    public DateTime CollectionTime { get; init; }
    public DateTime? DatabaseCollectionTime { get; init; }
    public string? PrimaryReplica { get; init; }
    public HealthSeverity Severity { get; init; }
    public IReadOnlyList<ViewerAgReplica> Replicas { get; init; } = Array.Empty<ViewerAgReplica>();
    public IReadOnlyList<ViewerAgDatabase> Databases { get; init; } = Array.Empty<ViewerAgDatabase>();

    public string AgNameDisplay => string.IsNullOrWhiteSpace(AgName) ? "—" : AgName!;
    public string SeverityLabel => Severity switch
    {
        HealthSeverity.Healthy => "Healthy",
        HealthSeverity.Warning => "Warning",
        HealthSeverity.Critical => "Critical",
        _ => "Unknown",
    };

    public SolidColorBrush SeverityBrush => ViewerAgReplica.BrushFor(Severity);

    public string PrimaryDisplay => string.IsNullOrWhiteSpace(PrimaryReplica)
        ? "No primary reported"
        : "Primary: " + PrimaryReplica;

    /// <summary>Whose view this is, and how fresh. The two collector sweeps land independently, so the database
    /// grain gets its own "as of" whenever it differs.</summary>
    public string SubtitleDisplay
    {
        get
        {
            var text = $"As reported by {ServerName} · collected {CollectionTime:yyyy-MM-dd HH:mm} UTC";
            if (DatabaseCollectionTime.HasValue && DatabaseCollectionTime.Value != CollectionTime)
            {
                text += $" · databases {DatabaseCollectionTime.Value:HH:mm} UTC";
            }

            return text;
        }
    }

    public bool HasDatabases => Databases.Count > 0;
}
