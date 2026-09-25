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
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Availability Group TOPOLOGY read (#991) — the cross-server AG surface assembled from the two collector
/// tables the AG collectors fill (<c>collect.ag_replica_states</c> at replica grain and
/// <c>collect.ag_database_replica_states</c> at database grain). The SAME reader powers the web dashboard's
/// <c>/api/ag</c> and the <c>get_ag_health</c> MCP tool, so a browser and an MCP client see one identically-banded
/// topology. STORED reads only, no live monitored-server hit.
///
/// <para><b>Grain: one group per (reporting server, AG).</b> Every monitored replica of an AG reports the WHOLE
/// AG's replica set, so a three-node AG with all three nodes monitored yields three groups for the same
/// <c>ag_name</c> — deliberately NOT merged. The perspectives genuinely differ (see the null-columns note below),
/// and silently folding them would invent a consensus the DMVs never agreed on. Each group therefore names its
/// <see cref="AvailabilityGroupView.ServerName"/> so a caller can reconcile the views itself.</para>
///
/// <para><b>Latest collection per server.</b> Each server contributes only the rows from its newest AG collection
/// (the <c>JOIN ... MAX(collection_time) GROUP BY server_id</c> shape <c>DarlingFleetReader</c> uses for its
/// latest-snapshot reads, now floor-bounded — see <see cref="DarlingAgStatesReader"/>, #4228). Because the
/// collectors write NO row when a server has no AGs, a server whose AGs were dropped keeps returning its last
/// non-empty snapshot no matter how old — so every group carries its <see cref="AvailabilityGroupView.CollectionTime"/>
/// and the UI shows it. <see cref="DarlingAgStatesReader.StalenessHorizon"/> only picks HOW that server's row gets
/// fetched (its own point read instead of feeding the shared floor); it never gates WHETHER the row comes back.</para>
///
/// <para><b>Banding lives here (R1).</b> Every severity below is derived server-side and serialized with the
/// result; the browser only maps a severity NAME to a CSS class and never re-derives a threshold. The mappings are
/// deliberately null-tolerant: <c>sys.dm_hadr_availability_replica_states</c> reports
/// <c>operational_state_desc</c> / <c>recovery_health_desc</c> only for the LOCAL replica and can return NULL for
/// everything under WSFC quorum loss, so an absent value bands <see cref="HealthSeverity.Unknown"/> — which, being
/// the lowest enum value, never inflates a group's worst-severity roll-up.</para>
///
/// <para><b>What the badge deliberately does NOT band: lag and queue depth.</b> Every severity here restates a
/// verdict the DMVs already made (a state or health string). Lag and queue depth are raw magnitudes, and turning
/// one into a band means choosing "how many seconds behind is a warning" — a monitoring POLICY, not a reading, and
/// one that belongs with the configurable alert thresholds rather than hardcoded in a read. So a badly-lagging
/// ASYNCHRONOUS_COMMIT secondary whose replica health still reports HEALTHY does NOT redden this badge; its lag,
/// queue sizes and drain estimates are surfaced per database instead, where an operator reads the number. Wiring
/// lag into the alert engine (with a real threshold) is the follow-on, not a change to make silently here.</para>
/// </summary>
internal static class DarlingAgReader
{
    /// <summary>Shared serializer options — snake_case field names come from the DTOs' <c>[JsonPropertyName]</c>
    /// attributes, severities serialize as their string names, and the output is COMPACT (#2350 - the MCP tool
    /// convention, since the reader on both ends is a parser rather than a person). ONE options object so
    /// <c>/api/ag</c> and <c>get_ag_health</c> serialize the identical shape.</summary>
    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false,
        Converters = { new JsonStringEnumConverter() },
    };

    /* ─────────────────────────── SQL ─────────────────────────── */

    /// <summary>The statement text and the two-step, floor-bounded read live in
    /// <see cref="DarlingAgStatesReader"/> (#4228) — the Viewer's AG tab, <c>/api/ag</c> and this tool all call
    /// the SAME implementation now, so there is one place, not three, that can disagree about what "the newest
    /// snapshot" means. <see cref="ReadReplicasAsync"/> / <see cref="ReadDatabasesAsync"/> below map its raw rows
    /// into this file's <see cref="ReplicaRow"/> / <see cref="DatabaseRow"/>, unchanged from before the move, so
    /// <see cref="Build"/> and everything downstream of it needed no edit.</summary>

    /* ─────────────────────────── the read ─────────────────────────── */

    /// <summary>
    /// Reads the whole fleet's AG topology (or one server's, when <paramref name="serverIdFilter"/> is set):
    /// every (reporting server, AG) group with its replicas and its per-database secondary state, each pre-banded.
    /// An empty store yields an empty result — never an error.
    /// </summary>
    public static async Task<AgHealthResult> GetAgHealthAsync(
        NpgsqlDataSource postgres,
        int? serverIdFilter = null,
        DateTime? nowUtc = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveNow = nowUtc ?? DateTime.UtcNow;
        var replicas = await ReadReplicasAsync(postgres, serverIdFilter, effectiveNow, cancellationToken);

        /* Short-circuit: no replica rows means no AGs anywhere in scope, and the database-grain table cannot
           hold rows for an AG that has no replicas. The common case (a fleet with no Always On at all) therefore
           costs exactly one indexed read that returns nothing — this read runs on every dashboard poll. */
        var databases = replicas.Count == 0
            ? new List<DatabaseRow>()
            : await ReadDatabasesAsync(postgres, serverIdFilter, effectiveNow, cancellationToken);

        return Build(replicas, databases, effectiveNow);
    }

    /// <summary>
    /// The nav-gate probe (#4189): "how many availability groups does this scope have", without building a
    /// single <see cref="AvailabilityGroupView"/> to answer it — no per-replica columns, no ORDER BY, no
    /// database-grain read. <see cref="DarlingAgStatesReader.GetReplicaGroupCountAsync"/> in place of the
    /// topology read <see cref="GetAgHealthAsync"/> runs; that read's own <c>AvailabilityGroupCount</c> is
    /// exactly this number for the same scope and instant.
    /// </summary>
    public static Task<int> GetAvailabilityGroupCountAsync(
        NpgsqlDataSource postgres,
        int? serverIdFilter = null,
        CancellationToken cancellationToken = default) =>
        DarlingAgStatesReader.GetReplicaGroupCountAsync(postgres, serverIdFilter, DateTime.UtcNow, cancellationToken);

    /// <summary>
    /// Assembles the raw rows into per-(server, AG) groups — pure, so the grouping and every band unit-test
    /// without a live store.
    ///
    /// <para>The REPLICA grain defines the group set: a database row whose (server, AG) has no replica row in the
    /// same read is dropped. That is only reachable when the two collectors' newest snapshots straddle an AG being
    /// created or dropped, and the alternative — a database grid under a header with no replicas to explain it —
    /// would be less legible than waiting one sweep for the two grains to agree.</para>
    /// </summary>
    internal static AgHealthResult Build(
        IReadOnlyList<ReplicaRow> replicas,
        IReadOnlyList<DatabaseRow> databases,
        DateTime nowUtc)
    {
        /* Group key is (server_id, ag_name) — one card per reporting server's view of an AG. A NULL ag_name is
           possible under quorum loss (the catalog views fall back to cached metadata); it groups under an empty
           key and renders as the unnamed group rather than being dropped on the floor. */
        var databasesByGroup = databases
            .GroupBy(d => (d.ServerId, Key(d.AgName)))
            .ToDictionary(g => g.Key, g => g.ToList());

        var groups = new List<AvailabilityGroupView>();

        foreach (var group in replicas.GroupBy(r => (r.ServerId, Key(r.AgName))))
        {
            var rows = group.ToList();
            var first = rows[0];
            databasesByGroup.TryGetValue(group.Key, out var dbRows);

            var replicaViews = rows.Select(ToReplicaView).ToList();
            var databaseViews = (dbRows ?? new List<DatabaseRow>()).Select(ToDatabaseView).ToList();

            /* The card's badge: the worst band anywhere in the group. Unknown is the lowest enum value, so the
               NULLs a non-local or quorum-lost replica reports never mask a real Healthy/Warning/Critical. */
            var worst = HealthSeverity.Unknown;
            foreach (var replica in replicaViews)
            {
                worst = Worse(worst, replica.Severity);
            }

            foreach (var database in databaseViews)
            {
                worst = Worse(worst, database.SynchronizationStateSeverity);
            }

            groups.Add(new AvailabilityGroupView
            {
                ServerId = first.ServerId,
                ServerName = first.ServerName,
                AgName = first.AgName,
                CollectionTime = first.CollectionTime,
                DatabaseCollectionTime = dbRows is { Count: > 0 } ? dbRows[0].CollectionTime : null,
                PrimaryReplica = replicaViews.FirstOrDefault(r => r.IsPrimary)?.ReplicaServerName,
                Severity = worst,
                SeverityLabel = SeverityLabel(worst),
                Replicas = replicaViews,
                Databases = databaseViews,
            });
        }

        /* Worst-first, then by name — the same "problems surface without scrolling" ordering the fleet roll-up
           uses, and DESC by severity matches the house grid default. */
        groups.Sort(CompareGroups);

        return new AgHealthResult
        {
            /* Naive UTC, like every other instant the API emits — the browser appends the zone itself (R5). */
            GeneratedAt = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified),
            AvailabilityGroupCount = groups.Count,
            ReportingServerCount = groups.Select(g => g.ServerId).Distinct().Count(),
            DistinctAgCount = groups.Select(g => Key(g.AgName)).Distinct(StringComparer.Ordinal).Count(),
            WorstSeverity = groups.Count == 0 ? HealthSeverity.Unknown : groups.Max(g => g.Severity),
            AvailabilityGroups = groups,
        };
    }

    /// <summary>Worst severity first, then AG name, then reporting server — so the several perspectives on one AG
    /// stay adjacent once severity ties.</summary>
    private static int CompareGroups(AvailabilityGroupView a, AvailabilityGroupView b)
    {
        var bySeverity = b.Severity.CompareTo(a.Severity);
        if (bySeverity != 0)
        {
            return bySeverity;
        }

        var byName = string.Compare(a.AgName ?? "", b.AgName ?? "", StringComparison.OrdinalIgnoreCase);
        return byName != 0
            ? byName
            : string.Compare(a.ServerName, b.ServerName, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The grouping key's AG half. Case-insensitive to match <c>DistinctAgCount</c> and
    /// <see cref="CompareGroups"/>; a NULL name (reachable under quorum loss, when the catalog views answer from
    /// cached metadata) folds to the empty key so the group renders unnamed instead of being dropped.</summary>
    private static string Key(string? agName) => (agName ?? "").ToUpperInvariant();

    private static HealthSeverity Worse(HealthSeverity a, HealthSeverity b) => a > b ? a : b;

    private static AgReplicaView ToReplicaView(ReplicaRow row)
    {
        var syncHealth = SynchronizationHealthSeverity(row.SynchronizationHealthDesc);
        var connected = ConnectedStateSeverity(row.ConnectedStateDesc);
        var operational = OperationalStateSeverity(row.OperationalStateDesc);
        var recovery = RecoveryHealthSeverity(row.RecoveryHealthDesc);
        var role = RoleSeverity(row.RoleDesc);

        var worst = Worse(Worse(Worse(Worse(syncHealth, connected), operational), recovery), role);

        return new AgReplicaView
        {
            ReplicaServerName = row.ReplicaServerName,
            RoleDesc = row.RoleDesc,
            RoleSeverity = role,
            IsPrimary = string.Equals(row.RoleDesc, "PRIMARY", StringComparison.OrdinalIgnoreCase),
            OperationalStateDesc = row.OperationalStateDesc,
            OperationalStateSeverity = operational,
            ConnectedStateDesc = row.ConnectedStateDesc,
            ConnectedStateSeverity = connected,
            RecoveryHealthDesc = row.RecoveryHealthDesc,
            RecoveryHealthSeverity = recovery,
            SynchronizationHealthDesc = row.SynchronizationHealthDesc,
            SynchronizationHealthSeverity = syncHealth,
            Severity = worst,
            AvailabilityModeDesc = row.AvailabilityModeDesc,
            FailoverModeDesc = row.FailoverModeDesc,
            EndpointUrl = row.EndpointUrl,
        };
    }

    private static AgDatabaseView ToDatabaseView(DatabaseRow row) => new()
    {
        DatabaseName = row.DatabaseName,
        ReplicaServerName = row.ReplicaServerName,
        IsLocal = row.IsLocal,
        SynchronizationStateDesc = row.SynchronizationStateDesc,
        SynchronizationStateSeverity = DatabaseSyncSeverity(row.SynchronizationStateDesc, row.AvailabilityModeDesc, row.IsSuspended),
        AvailabilityModeDesc = row.AvailabilityModeDesc,
        LogSendQueueKb = row.LogSendQueueSize,
        RedoQueueKb = row.RedoQueueSize,
        LogSendRateKbPerSec = row.LogSendRate,
        RedoRateKbPerSec = row.RedoRate,
        EstimatedSendDrainMinutes = DrainMinutes(row.LogSendQueueSize, row.LogSendRate),
        EstimatedRedoCompletionMinutes = DrainMinutes(row.RedoQueueSize, row.RedoRate),
        SecondaryLagSeconds = row.SecondaryLagSeconds,
        IsSuspended = row.IsSuspended,
        SuspendReasonDesc = row.SuspendReasonDesc,
        LastCommitLsn = row.LastCommitLsn,
        LastHardenedLsn = row.LastHardenedLsn,
    };

    /* ─────────────────────────── banding (internal for unit tests) ─────────────────────────── */

    /// <summary>Bands a replica's <c>synchronization_health_desc</c> — the one health column the DMV populates for
    /// EVERY replica, not just the local one, which is why it anchors the group's badge.</summary>
    internal static HealthSeverity SynchronizationHealthSeverity(string? healthDesc) =>
        Normalize(healthDesc) switch
        {
            "HEALTHY" => HealthSeverity.Healthy,
            "PARTIALLY_HEALTHY" => HealthSeverity.Warning,
            "NOT_HEALTHY" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>connected_state_desc</c>. Only meaningful on the primary's row for its own replicas;
    /// NULL elsewhere, which bands Unknown.</summary>
    internal static HealthSeverity ConnectedStateSeverity(string? connectedDesc) =>
        Normalize(connectedDesc) switch
        {
            "CONNECTED" => HealthSeverity.Healthy,
            "DISCONNECTED" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>
    /// Bands <c>operational_state_desc</c>, whose documented values are exactly PENDING_FAILOVER / PENDING /
    /// ONLINE / OFFLINE / FAILED / FAILED_NO_QUORUM / NULL. The DMV reports it for the LOCAL replica only
    /// (<c>NULL</c> = "replica isn't local"), so a remote replica's NULL bands Unknown rather than reading as a
    /// problem. A failover in progress is a warning (the AG is transitioning, not broken); a failed or
    /// quorum-less replica is critical. <c>ONLINE_IN_PROGRESS</c> deliberately does NOT appear here — it belongs
    /// to <see cref="RecoveryHealthSeverity"/>, not this column.
    /// </summary>
    internal static HealthSeverity OperationalStateSeverity(string? operationalDesc) =>
        Normalize(operationalDesc) switch
        {
            "ONLINE" => HealthSeverity.Healthy,
            "PENDING" or "PENDING_FAILOVER" => HealthSeverity.Warning,
            "OFFLINE" or "FAILED" or "FAILED_NO_QUORUM" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>
    /// Bands <c>recovery_health_desc</c> — documented as exactly ONLINE_IN_PROGRESS / ONLINE / NULL. It rolls up
    /// the replica's databases' <c>database_state</c>: ONLINE means every joined database is online, and
    /// ONLINE_IN_PROGRESS means at least one is still coming up, which is a transitional warning rather than a
    /// failure. NULL when the replica is not local, like <see cref="OperationalStateSeverity"/>.
    /// </summary>
    internal static HealthSeverity RecoveryHealthSeverity(string? recoveryDesc) =>
        Normalize(recoveryDesc) switch
        {
            "ONLINE" => HealthSeverity.Healthy,
            "ONLINE_IN_PROGRESS" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>
    /// Bands <c>role_desc</c> — PRIMARY / SECONDARY / RESOLVING. RESOLVING is the one role that is itself a
    /// finding: the replica holds neither role, which is what a replica looks like while the AG is failing over or
    /// has lost quorum (it is also the only role under which operational_state can read PENDING_FAILOVER or
    /// FAILED_NO_QUORUM). A healthy replica is in one of the two real roles.
    /// </summary>
    internal static HealthSeverity RoleSeverity(string? roleDesc) =>
        Normalize(roleDesc) switch
        {
            "PRIMARY" or "SECONDARY" => HealthSeverity.Healthy,
            "RESOLVING" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>
    /// Bands a database's <c>synchronization_state_desc</c> AGAINST ITS AVAILABILITY MODE — the distinction a flat
    /// state-to-color map gets wrong. SYNCHRONIZING is the steady, correct state for an ASYNCHRONOUS_COMMIT
    /// replica (async never reaches SYNCHRONIZED), so coloring it amber there would paint every healthy async
    /// secondary as a problem; on a SYNCHRONOUS_COMMIT replica the same state means the replica is NOT currently
    /// protecting a commit, which IS a warning. Suspended data movement outranks the state text entirely: MS Learn
    /// documents <c>secondary_lag_seconds</c> reading 0 (not NULL) while suspended, so a suspended replica
    /// otherwise presents as perfectly caught up.
    /// </summary>
    internal static HealthSeverity DatabaseSyncSeverity(string? stateDesc, string? availabilityModeDesc, bool? isSuspended)
    {
        if (isSuspended == true)
        {
            return HealthSeverity.Critical;
        }

        /* The database grain reports states space-separated ("NOT SYNCHRONIZING") where the replica grain uses
           underscores ("NOT_HEALTHY"); both spellings are accepted so neither collector's verbatim text slips
           through as Unknown. */
        return Normalize(stateDesc) switch
        {
            "SYNCHRONIZED" => HealthSeverity.Healthy,
            "SYNCHRONIZING" => string.Equals(Normalize(availabilityModeDesc), "ASYNCHRONOUS_COMMIT", StringComparison.Ordinal)
                ? HealthSeverity.Healthy
                : HealthSeverity.Warning,
            "NOT_SYNCHRONIZING" => HealthSeverity.Critical,
            "REVERTING" or "INITIALIZING" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };
    }

    /// <summary>
    /// Minutes to drain a queue at the current rate — the standard AG estimate, derived here because the DMV
    /// exposes only the queue (KB) and the rate (KB/s). An EMPTY queue is 0 regardless of rate; a non-empty queue
    /// moving at 0 KB/s has no finite estimate and returns null (never a divide-by-zero infinity, and never a
    /// misleading 0). Both inputs are instantaneous gauges, so this is a snapshot estimate, not a forecast.
    /// </summary>
    internal static double? DrainMinutes(long? queueKb, long? rateKbPerSec)
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

    /// <summary>The human label for a group's badge — the same vocabulary the fleet bands use.</summary>
    internal static string SeverityLabel(HealthSeverity severity) => severity switch
    {
        HealthSeverity.Healthy => "Healthy",
        HealthSeverity.Warning => "Warning",
        HealthSeverity.Critical => "Critical",
        _ => "Unknown",
    };

    /// <summary>Upper-cases and collapses the DMVs' two spellings (spaces vs underscores) to one form, so the
    /// switch arms above match either grain's verbatim text.</summary>
    private static string Normalize(string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? ""
            : value.Trim().Replace(' ', '_').ToUpperInvariant();

    /* ─────────────────────────── reads ─────────────────────────── */

    /// <summary>Maps <see cref="DarlingAgStatesReader"/>'s raw rows (#4228) into this file's own
    /// <see cref="ReplicaRow"/>, field for field, so <see cref="Build"/> and every banding rule below it needed
    /// no change when the read moved.</summary>
    private static async Task<List<ReplicaRow>> ReadReplicasAsync(
        NpgsqlDataSource postgres, int? serverIdFilter, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var raw = await DarlingAgStatesReader.GetReplicaStatesAsync(postgres, serverIdFilter, nowUtc, cancellationToken);
        var rows = new List<ReplicaRow>(raw.Count);
        foreach (var row in raw)
        {
            rows.Add(new ReplicaRow(
                row.ServerId, row.ServerName, row.CollectionTime, row.AgName, row.ReplicaServerName, row.RoleDesc,
                row.IsLocal, row.OperationalStateDesc, row.ConnectedStateDesc, row.RecoveryHealthDesc,
                row.SynchronizationHealthDesc, row.AvailabilityModeDesc, row.FailoverModeDesc, row.EndpointUrl));
        }

        return rows;
    }

    /// <summary>Same mapping as <see cref="ReadReplicasAsync"/>, for the database grain.</summary>
    private static async Task<List<DatabaseRow>> ReadDatabasesAsync(
        NpgsqlDataSource postgres, int? serverIdFilter, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var raw = await DarlingAgStatesReader.GetDatabaseReplicaStatesAsync(postgres, serverIdFilter, nowUtc, cancellationToken);
        var rows = new List<DatabaseRow>(raw.Count);
        foreach (var row in raw)
        {
            rows.Add(new DatabaseRow(
                row.ServerId, row.ServerName, row.CollectionTime, row.AgName, row.DatabaseName, row.ReplicaServerName,
                row.IsLocal, row.SynchronizationStateDesc, row.LastHardenedLsn, row.LastCommitLsn, row.LogSendQueueSize,
                row.RedoQueueSize, row.LogSendRate, row.RedoRate, row.IsSuspended, row.SuspendReasonDesc,
                row.AvailabilityModeDesc, row.SecondaryLagSeconds));
        }

        return rows;
    }

    /* ─────────────────────────── raw-read carriers ─────────────────────────── */

    /// <summary>One replica-grain row, exactly as <c>collect.ag_replica_states</c> stores it.</summary>
    internal readonly record struct ReplicaRow(
        int ServerId,
        string ServerName,
        DateTime CollectionTime,
        string? AgName,
        string? ReplicaServerName,
        string? RoleDesc,
        bool? IsLocal,
        string? OperationalStateDesc,
        string? ConnectedStateDesc,
        string? RecoveryHealthDesc,
        string? SynchronizationHealthDesc,
        string? AvailabilityModeDesc,
        string? FailoverModeDesc,
        string? EndpointUrl);

    /// <summary>One database-grain row, exactly as <c>collect.ag_database_replica_states</c> stores it. Queue sizes
    /// are KB and rates KB/s (the DMV's units), both instantaneous gauges rather than counters.</summary>
    internal readonly record struct DatabaseRow(
        int ServerId,
        string ServerName,
        DateTime CollectionTime,
        string? AgName,
        string? DatabaseName,
        string? ReplicaServerName,
        bool? IsLocal,
        string? SynchronizationStateDesc,
        string? LastHardenedLsn,
        string? LastCommitLsn,
        long? LogSendQueueSize,
        long? RedoQueueSize,
        long? LogSendRate,
        long? RedoRate,
        bool? IsSuspended,
        string? SuspendReasonDesc,
        string? AvailabilityModeDesc,
        long? SecondaryLagSeconds);
}

/// <summary>One replica inside a group, pre-banded. Every <c>*_severity</c> is derived server-side (R1).</summary>
public sealed class AgReplicaView
{
    [JsonPropertyName("replica_server_name")] public string? ReplicaServerName { get; init; }
    [JsonPropertyName("role")] public string? RoleDesc { get; init; }

    /// <summary>Bands the role itself: RESOLVING is a finding (the replica holds neither real role), PRIMARY and
    /// SECONDARY are healthy.</summary>
    [JsonPropertyName("role_severity")] public HealthSeverity RoleSeverity { get; init; }

    [JsonPropertyName("is_primary")] public bool IsPrimary { get; init; }

    /// <summary>Is this the replica the reporting server IS? NULL on rows collected before the column existed —
    /// UNKNOWN, not "remote".</summary>
    [JsonPropertyName("is_local")] public bool? IsLocal { get; init; }
    [JsonPropertyName("operational_state")] public string? OperationalStateDesc { get; init; }
    [JsonPropertyName("operational_state_severity")] public HealthSeverity OperationalStateSeverity { get; init; }
    [JsonPropertyName("connected_state")] public string? ConnectedStateDesc { get; init; }
    [JsonPropertyName("connected_state_severity")] public HealthSeverity ConnectedStateSeverity { get; init; }
    [JsonPropertyName("recovery_health")] public string? RecoveryHealthDesc { get; init; }
    [JsonPropertyName("recovery_health_severity")] public HealthSeverity RecoveryHealthSeverity { get; init; }
    [JsonPropertyName("synchronization_health")] public string? SynchronizationHealthDesc { get; init; }
    [JsonPropertyName("synchronization_health_severity")] public HealthSeverity SynchronizationHealthSeverity { get; init; }

    /// <summary>The replica's worst band — the roll-up of its synchronization-health, connected-state and
    /// operational-state severities, so one chip can carry the whole replica's state. Derived here because the
    /// browser never re-derives a band (R1).</summary>
    [JsonPropertyName("severity")] public HealthSeverity Severity { get; init; }

    [JsonPropertyName("availability_mode")] public string? AvailabilityModeDesc { get; init; }
    [JsonPropertyName("failover_mode")] public string? FailoverModeDesc { get; init; }
    [JsonPropertyName("endpoint_url")] public string? EndpointUrl { get; init; }
}

/// <summary>One database-on-a-replica row inside a group, pre-banded.</summary>
public sealed class AgDatabaseView
{
    [JsonPropertyName("database_name")] public string? DatabaseName { get; init; }
    [JsonPropertyName("replica_server_name")] public string? ReplicaServerName { get; init; }
    [JsonPropertyName("is_local")] public bool? IsLocal { get; init; }
    [JsonPropertyName("synchronization_state")] public string? SynchronizationStateDesc { get; init; }
    [JsonPropertyName("synchronization_state_severity")] public HealthSeverity SynchronizationStateSeverity { get; init; }
    [JsonPropertyName("availability_mode")] public string? AvailabilityModeDesc { get; init; }
    [JsonPropertyName("log_send_queue_kb")] public long? LogSendQueueKb { get; init; }
    [JsonPropertyName("redo_queue_kb")] public long? RedoQueueKb { get; init; }
    [JsonPropertyName("log_send_rate_kb_sec")] public long? LogSendRateKbPerSec { get; init; }
    [JsonPropertyName("redo_rate_kb_sec")] public long? RedoRateKbPerSec { get; init; }

    /// <summary>Minutes to drain the log-send queue at the current send rate — DERIVED (queue / rate), not a
    /// collected column; null when the queue is non-empty and the rate is 0.</summary>
    [JsonPropertyName("est_send_drain_minutes")] public double? EstimatedSendDrainMinutes { get; init; }

    /// <summary>Minutes to drain the redo queue at the current redo rate — DERIVED (queue / rate), not a collected
    /// column; null when the queue is non-empty and the rate is 0.</summary>
    [JsonPropertyName("est_redo_completion_minutes")] public double? EstimatedRedoCompletionMinutes { get; init; }

    /// <summary>The DMV's secondary lag. Reads 0 while data movement is SUSPENDED, so read it together with
    /// <see cref="IsSuspended"/> — a suspended replica is not caught up.</summary>
    [JsonPropertyName("secondary_lag_seconds")] public long? SecondaryLagSeconds { get; init; }

    [JsonPropertyName("is_suspended")] public bool? IsSuspended { get; init; }
    [JsonPropertyName("suspend_reason")] public string? SuspendReasonDesc { get; init; }
    [JsonPropertyName("last_commit_lsn")] public string? LastCommitLsn { get; init; }
    [JsonPropertyName("last_hardened_lsn")] public string? LastHardenedLsn { get; init; }
}

/// <summary>
/// One monitored server's view of one Availability Group. An AG whose replicas are ALL monitored appears once per
/// monitored replica — <see cref="ServerName"/> names whose perspective this is, so a caller can reconcile them.
/// </summary>
public sealed class AvailabilityGroupView
{
    /// <summary>The REPORTING server — the monitored instance whose DMVs produced this view, not necessarily the
    /// AG's primary.</summary>
    [JsonPropertyName("server_name")] public string ServerName { get; init; } = "";

    [JsonPropertyName("server_id")] public int ServerId { get; init; }
    [JsonPropertyName("ag_name")] public string? AgName { get; init; }

    /// <summary>When the reporting server's newest REPLICA-grain snapshot was taken (naive UTC). The collectors
    /// write nothing for a server with no AGs, so a group that stops refreshing keeps its last instant here —
    /// surfaced rather than silently aged out.</summary>
    [JsonPropertyName("collection_time")] public DateTime CollectionTime { get; init; }

    /// <summary>When the newest DATABASE-grain snapshot was taken; may differ from
    /// <see cref="CollectionTime"/> (separate collector sweeps) and is null when the group has no database rows.</summary>
    [JsonPropertyName("database_collection_time")] public DateTime? DatabaseCollectionTime { get; init; }

    /// <summary>The replica currently holding the PRIMARY role in this view, or null when no row reports one.</summary>
    [JsonPropertyName("primary_replica")] public string? PrimaryReplica { get; init; }

    /// <summary>The worst band anywhere in the group — across the replicas' sync health / connected /
    /// operational state and every database's synchronization state.</summary>
    [JsonPropertyName("severity")] public HealthSeverity Severity { get; init; }

    [JsonPropertyName("severity_label")] public string SeverityLabel { get; init; } = "";
    [JsonPropertyName("replicas")] public IReadOnlyList<AgReplicaView> Replicas { get; init; } = Array.Empty<AgReplicaView>();
    [JsonPropertyName("databases")] public IReadOnlyList<AgDatabaseView> Databases { get; init; } = Array.Empty<AgDatabaseView>();
}

/// <summary>The full AG topology payload — the <c>/api/ag</c> body and the <c>get_ag_health</c> MCP tool's
/// serialized result. An AG-less fleet returns zero groups, not an error.</summary>
public sealed class AgHealthResult
{
    [JsonPropertyName("generated_at")] public DateTime GeneratedAt { get; init; }

    /// <summary>How many (reporting server, AG) groups are below — the card count, NOT the number of distinct
    /// AGs (see <see cref="DistinctAgCount"/>).</summary>
    [JsonPropertyName("availability_group_count")] public int AvailabilityGroupCount { get; init; }

    /// <summary>How many monitored servers reported any AG.</summary>
    [JsonPropertyName("reporting_server_count")] public int ReportingServerCount { get; init; }

    /// <summary>How many distinct <c>ag_name</c>s are represented, collapsing the multiple monitored replicas that
    /// report the same AG.</summary>
    [JsonPropertyName("distinct_ag_count")] public int DistinctAgCount { get; init; }

    [JsonPropertyName("worst_severity")] public HealthSeverity WorstSeverity { get; init; }
    [JsonPropertyName("availability_groups")] public IReadOnlyList<AvailabilityGroupView> AvailabilityGroups { get; init; } = Array.Empty<AvailabilityGroupView>();
}

/// <summary>The <c>/api/ag/count</c> body (#4189) — the one field <see cref="AgHealthResult.AvailabilityGroupCount"/>
/// carries, under the same name, so <c>refreshAgNav</c> reads it without fetching the topology that field lives
/// inside.</summary>
public sealed class AvailabilityGroupCountResult
{
    [JsonPropertyName("availability_group_count")] public int AvailabilityGroupCount { get; init; }
}
