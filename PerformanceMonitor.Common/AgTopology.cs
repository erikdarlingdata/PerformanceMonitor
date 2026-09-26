/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The Availability Group TOPOLOGY banding and card projection, shared by every surface that draws it (#991):
/// the Darling web dashboard, the Darling WPF viewer's AG tab, and Lite's AG tab. Pure — no store, no WPF — so
/// the rules live in exactly one place and each app supplies only its own read and its own brushes.
///
/// <para>This is the display twin of <see cref="AgAlertPolicy"/>: that decides what to ALERT on, this decides
/// what to SHOW. They are deliberately separate because they need different rows — an alert keys on a
/// (ag_name, replica) identity and drops rows that cannot be keyed, whereas a topology view must still render a
/// quorum-lost AG whose names read NULL, which is precisely when an operator most wants to look at it.</para>
///
/// <para><b>One card per (reporting server, AG), never merged.</b> Every monitored replica reports the whole
/// AG's replica set, so an AG with several monitored replicas yields several cards. Collapsing them would be
/// wrong, not tidy: the DMV populates <c>operational_state</c> / <c>recovery_health</c> for the LOCAL replica
/// only, <c>connected_state</c> is meaningful only from the primary, and a quorum-lost instance answers from
/// cached metadata. Observed live: the primary reports two replicas and names the primary while the secondary
/// reports ONE replica and no primary at all. Merging lets the blind view overwrite the complete one. Lite is
/// single-server so it renders exactly one card per AG, which is the same rule with one reporter.</para>
/// </summary>
public static class AgTopology
{
    /* ─────────────────────────── banding ─────────────────────────── */

    /// <summary>Bands a replica's <c>synchronization_health_desc</c> — the one health column the DMV populates
    /// for EVERY replica rather than only the local one, which is why it anchors a card's badge.</summary>
    public static HealthSeverity SynchronizationHealthSeverity(string? healthDesc) =>
        Normalize(healthDesc) switch
        {
            "HEALTHY" => HealthSeverity.Healthy,
            "PARTIALLY_HEALTHY" => HealthSeverity.Warning,
            "NOT_HEALTHY" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>connected_state_desc</c>. Meaningful from the primary; NULL elsewhere, which bands
    /// Unknown rather than reading as a problem.</summary>
    public static HealthSeverity ConnectedStateSeverity(string? connectedDesc) =>
        Normalize(connectedDesc) switch
        {
            "CONNECTED" => HealthSeverity.Healthy,
            "DISCONNECTED" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>operational_state_desc</c>, whose documented values are exactly PENDING_FAILOVER /
    /// PENDING / ONLINE / OFFLINE / FAILED / FAILED_NO_QUORUM / NULL. Reported for the LOCAL replica only, so a
    /// remote replica's NULL bands Unknown. <c>ONLINE_IN_PROGRESS</c> deliberately does NOT appear — it belongs
    /// to <see cref="RecoveryHealthSeverity"/>, and banding it here would be an arm that can never fire.</summary>
    public static HealthSeverity OperationalStateSeverity(string? operationalDesc) =>
        Normalize(operationalDesc) switch
        {
            "ONLINE" => HealthSeverity.Healthy,
            "PENDING" or "PENDING_FAILOVER" => HealthSeverity.Warning,
            "OFFLINE" or "FAILED" or "FAILED_NO_QUORUM" => HealthSeverity.Critical,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>recovery_health_desc</c> — documented as exactly ONLINE_IN_PROGRESS / ONLINE / NULL.</summary>
    public static HealthSeverity RecoveryHealthSeverity(string? recoveryDesc) =>
        Normalize(recoveryDesc) switch
        {
            "ONLINE" => HealthSeverity.Healthy,
            "ONLINE_IN_PROGRESS" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>Bands <c>role_desc</c>. RESOLVING is itself a finding: the replica holds neither real role,
    /// which is what a replica looks like mid-failover or after losing quorum.</summary>
    public static HealthSeverity RoleSeverity(string? roleDesc) =>
        Normalize(roleDesc) switch
        {
            "PRIMARY" or "SECONDARY" => HealthSeverity.Healthy,
            "RESOLVING" => HealthSeverity.Warning,
            _ => HealthSeverity.Unknown,
        };

    /// <summary>
    /// Bands a database's synchronization state AGAINST ITS AVAILABILITY MODE — the distinction a flat
    /// state-to-color map necessarily gets wrong. SYNCHRONIZING is the steady, correct state for an
    /// ASYNCHRONOUS_COMMIT replica (async never reaches SYNCHRONIZED), so amber there would paint every healthy
    /// async secondary as a problem; on a SYNCHRONOUS_COMMIT replica the same text means it is NOT currently
    /// protecting a commit. Suspended data movement outranks the state text entirely, because
    /// <c>secondary_lag_seconds</c> reads 0 — not NULL — while suspended, so a suspended replica otherwise
    /// presents as perfectly caught up.
    /// </summary>
    public static HealthSeverity DatabaseSyncSeverity(string? stateDesc, string? availabilityModeDesc, bool? isSuspended)
    {
        if (isSuspended == true)
        {
            return HealthSeverity.Critical;
        }

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

    /// <summary>Minutes to drain a queue at the current rate — DERIVED (queue / rate), because the DMV exposes
    /// only the queue (KB) and the rate (KB/s). An empty queue is 0 regardless of rate; a non-empty queue moving
    /// at 0 KB/s has no finite estimate and returns null, never a divide-by-zero infinity and never a
    /// misleading 0. Both inputs are instantaneous gauges, so this is a snapshot estimate, not a forecast.</summary>
    public static double? DrainMinutes(long? queueKb, long? rateKbPerSec)
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

    /// <summary>The human label for a card's badge — the same vocabulary the fleet bands use.</summary>
    public static string SeverityLabel(HealthSeverity severity) => severity switch
    {
        HealthSeverity.Healthy => "Healthy",
        HealthSeverity.Warning => "Warning",
        HealthSeverity.Critical => "Critical",
        _ => "Unknown",
    };

    /// <summary>Upper-cases and collapses the DMVs' two spellings to one form: the database grain reports states
    /// space-separated (<c>NOT SYNCHRONIZING</c>) where the replica grain uses underscores (<c>NOT_HEALTHY</c>),
    /// and the collectors store each verbatim.</summary>
    private static string Normalize(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "" : value.Trim().Replace(' ', '_').ToUpperInvariant();

    /* ─────────────────────────── projection ─────────────────────────── */

    /// <summary>
    /// Shapes raw rows into cards. Pure, so the grouping and every band unit-test without a store — which is how
    /// both apps' test matrices pin the identical behavior.
    ///
    /// <para>The REPLICA grain defines the card set: a database row whose (server, AG) has no replica row is
    /// dropped, reachable only when the two collectors' newest snapshots straddle an AG being created or
    /// dropped. A database grid under a header with no replicas to explain it would be less legible than waiting
    /// one sweep for the grains to agree.</para>
    /// </summary>
    public static List<AgTopologyCard> BuildCards(
        IReadOnlyList<AgTopologyReplicaRow> replicas,
        IReadOnlyList<AgTopologyDatabaseRow> databases)
    {
        ArgumentNullException.ThrowIfNull(replicas);
        ArgumentNullException.ThrowIfNull(databases);

        var databasesByGroup = databases
            .GroupBy(d => (d.ServerId, Key(d.AgName)))
            .ToDictionary(g => g.Key, g => g.ToList());

        var cards = new List<AgTopologyCard>();

        foreach (var group in replicas.GroupBy(r => (r.ServerId, Key(r.AgName))))
        {
            var rows = group.ToList();
            var first = rows[0];
            databasesByGroup.TryGetValue(group.Key, out var dbRows);

            var replicaItems = rows.ConvertAll(ToReplica);
            var databaseItems = (dbRows ?? new List<AgTopologyDatabaseRow>()).ConvertAll(ToDatabase);

            /* Worst band anywhere in the card. Unknown is the LOWEST enum value, so the NULLs a non-local or
               quorum-lost replica reports never mask a real Healthy/Warning/Critical. */
            var worst = HealthSeverity.Unknown;
            foreach (var replica in replicaItems)
            {
                worst = Worse(worst, replica.Severity);
            }

            foreach (var database in databaseItems)
            {
                worst = Worse(worst, database.SynchronizationStateSeverity);
            }

            var card = new AgTopologyCard
            {
                ServerId = first.ServerId,
                ServerName = first.ServerName,
                AgName = first.AgName,
                CollectionTime = first.CollectionTime,
                DatabaseCollectionTime = dbRows is { Count: > 0 } ? dbRows[0].CollectionTime : null,
                PrimaryReplica = replicaItems.FirstOrDefault(r => r.IsPrimary)?.ReplicaServerName,
                Severity = worst,
            };

            /* Replicas/Databases are get-only ObservableCollections (#4238) — populated here, once, rather than
               assigned, so the SAME instance can be reused across a later UpdateFrom without ever handing WPF a
               new ItemsSource reference for the nested, non-virtualized replica-chip / database-grid lists. */
            foreach (var replica in replicaItems)
            {
                card.Replicas.Add(replica);
            }

            foreach (var database in databaseItems)
            {
                card.Databases.Add(database);
            }

            cards.Add(card);
        }

        /* Worst-first, then AG name, then reporting server — problems surface without scrolling, and the several
           perspectives on one AG stay adjacent once severity ties. */
        cards.Sort(static (a, b) =>
        {
            var bySeverity = b.Severity.CompareTo(a.Severity);
            if (bySeverity != 0)
            {
                return bySeverity;
            }

            var byName = string.Compare(a.AgName ?? "", b.AgName ?? "", StringComparison.OrdinalIgnoreCase);
            return byName != 0 ? byName : string.Compare(a.ServerName, b.ServerName, StringComparison.OrdinalIgnoreCase);
        });

        return cards;
    }

    /// <summary>The header counts. Groups and views differ exactly when an AG has more than one monitored
    /// reporter, and that difference is what a reader seeing one AG name twice needs said out loud.</summary>
    public static (int DistinctGroups, int ReportingServers, int Views) Counts(IReadOnlyList<AgTopologyCard> cards)
    {
        ArgumentNullException.ThrowIfNull(cards);

        return (
            cards.Select(c => Key(c.AgName)).Distinct(StringComparer.Ordinal).Count(),
            cards.Select(c => c.ServerId).Distinct().Count(),
            cards.Count);
    }

    /// <summary>The identity a card keeps across refreshes (#4238): the (reporting server, AG) pair BuildCards
    /// already groups by. A tab reconciles its new cards against its live collection by this key, so an AG whose
    /// topology did not change keeps its bound container instead of getting torn down and rebuilt.</summary>
    public static (int ServerId, string AgKey) CardKey(AgTopologyCard card)
    {
        ArgumentNullException.ThrowIfNull(card);
        return (card.ServerId, Key(card.AgName));
    }

    private static string Key(string? agName) => (agName ?? "").ToUpperInvariant();

    private static HealthSeverity Worse(HealthSeverity a, HealthSeverity b) => a > b ? a : b;

    private static AgTopologyReplica ToReplica(AgTopologyReplicaRow row)
    {
        var syncHealth = SynchronizationHealthSeverity(row.SynchronizationHealthDesc);
        var connected = ConnectedStateSeverity(row.ConnectedStateDesc);
        var operational = OperationalStateSeverity(row.OperationalStateDesc);
        var recovery = RecoveryHealthSeverity(row.RecoveryHealthDesc);
        var role = RoleSeverity(row.RoleDesc);

        return new AgTopologyReplica
        {
            ReplicaServerName = row.ReplicaServerName,
            RoleDesc = row.RoleDesc,
            IsPrimary = string.Equals(row.RoleDesc, "PRIMARY", StringComparison.OrdinalIgnoreCase),
            IsLocal = row.IsLocal,
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

    private static AgTopologyDatabase ToDatabase(AgTopologyDatabaseRow row) => new()
    {
        DatabaseName = row.DatabaseName,
        ReplicaServerName = row.ReplicaServerName,
        IsLocal = row.IsLocal,
        SynchronizationStateDesc = row.SynchronizationStateDesc,
        SynchronizationStateSeverity = DatabaseSyncSeverity(row.SynchronizationStateDesc, row.AvailabilityModeDesc, row.IsSuspended),
        AvailabilityModeDesc = row.AvailabilityModeDesc,
        LogSendQueueKb = row.LogSendQueueKb,
        RedoQueueKb = row.RedoQueueKb,
        LogSendRateKbPerSec = row.LogSendRateKbPerSec,
        RedoRateKbPerSec = row.RedoRateKbPerSec,
        EstimatedSendDrainMinutes = DrainMinutes(row.LogSendQueueKb, row.LogSendRateKbPerSec),
        EstimatedRedoCompletionMinutes = DrainMinutes(row.RedoQueueKb, row.RedoRateKbPerSec),
        SecondaryLagSeconds = row.SecondaryLagSeconds,
        IsSuspended = row.IsSuspended,
        SuspendReasonDesc = row.SuspendReasonDesc,
    };

    /* ─────────────────────────── in-place refresh (#4238) ─────────────────────────── */

    /// <summary>
    /// Syncs <paramref name="live"/> to hold exactly the identities in <paramref name="fresh"/>, in
    /// <paramref name="fresh"/>'s order, reusing existing instances (updated via <paramref name="updateInPlace"/>)
    /// for every identity that survives the refresh. This is what lets a bound WPF container list add or remove
    /// containers only for what actually changed topology, instead of tearing down and rebuilding every container
    /// on every refresh — the Darling viewer's AG tab uses it for its top-level card list, and
    /// <see cref="AgTopologyCard.UpdateFrom"/> uses the same method for a card's nested Replicas/Databases, so
    /// there is one reconciliation rule instead of a family of near-identical ones.
    /// </summary>
    public static void Reconcile<TItem, TKey>(
        ObservableCollection<TItem> live,
        IReadOnlyList<TItem> fresh,
        Func<TItem, TKey> keyOf,
        Action<TItem, TItem> updateInPlace)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(live);
        ArgumentNullException.ThrowIfNull(fresh);
        ArgumentNullException.ThrowIfNull(keyOf);
        ArgumentNullException.ThrowIfNull(updateInPlace);

        var freshKeys = new HashSet<TKey>(fresh.Count);
        foreach (var item in fresh)
        {
            freshKeys.Add(keyOf(item));
        }

        /* Remove first, so an identity that later re-inserts (edge case: duplicate keys in `fresh`) cannot be
           removed right after being (re)inserted below. */
        for (var i = live.Count - 1; i >= 0; i--)
        {
            if (!freshKeys.Contains(keyOf(live[i])))
            {
                live.RemoveAt(i);
            }
        }

        var liveByKey = new Dictionary<TKey, TItem>(live.Count);
        foreach (var item in live)
        {
            liveByKey[keyOf(item)] = item;
        }

        for (var i = 0; i < fresh.Count; i++)
        {
            var key = keyOf(fresh[i]);
            if (liveByKey.TryGetValue(key, out var existing))
            {
                updateInPlace(existing, fresh[i]);

                var currentIndex = live.IndexOf(existing);
                if (currentIndex != i)
                {
                    live.Move(currentIndex, i);
                }
            }
            else
            {
                live.Insert(i, fresh[i]);
            }
        }
    }

    /// <summary>
    /// A cheap fingerprint of every field a card renders, including both grains' collection times — a refresh
    /// whose rows hash identically to the last render is, by construction, a refresh that would change nothing
    /// on screen, so the tab can skip reconciling and re-binding 40+ cards entirely rather than walking them to
    /// discover that (#4238). Order-sensitive by design: <see cref="BuildCards"/>' sort is itself part of what
    /// "the same render" means.
    /// </summary>
    public static int ComputeDigest(IReadOnlyList<AgTopologyCard> cards)
    {
        ArgumentNullException.ThrowIfNull(cards);

        var hash = new HashCode();
        hash.Add(cards.Count);

        foreach (var card in cards)
        {
            hash.Add(card.ServerId);
            hash.Add(card.AgName);
            hash.Add(card.CollectionTime);
            hash.Add(card.DatabaseCollectionTime);
            hash.Add(card.PrimaryReplica);
            hash.Add(card.Severity);

            hash.Add(card.Replicas.Count);
            foreach (var replica in card.Replicas)
            {
                hash.Add(replica.ReplicaServerName);
                hash.Add(replica.RoleDesc);
                hash.Add(replica.IsPrimary);
                hash.Add(replica.IsLocal);
                hash.Add(replica.OperationalStateDesc);
                hash.Add(replica.ConnectedStateDesc);
                hash.Add(replica.RecoveryHealthDesc);
                hash.Add(replica.SynchronizationHealthDesc);
                hash.Add(replica.AvailabilityModeDesc);
                hash.Add(replica.FailoverModeDesc);
                hash.Add(replica.EndpointUrl);
                hash.Add(replica.Severity);
            }

            hash.Add(card.Databases.Count);
            foreach (var database in card.Databases)
            {
                hash.Add(database.DatabaseName);
                hash.Add(database.ReplicaServerName);
                hash.Add(database.IsLocal);
                hash.Add(database.SynchronizationStateDesc);
                hash.Add(database.SynchronizationStateSeverity);
                hash.Add(database.AvailabilityModeDesc);
                hash.Add(database.LogSendQueueKb);
                hash.Add(database.RedoQueueKb);
                hash.Add(database.LogSendRateKbPerSec);
                hash.Add(database.RedoRateKbPerSec);
                hash.Add(database.SecondaryLagSeconds);
                hash.Add(database.IsSuspended);
                hash.Add(database.SuspendReasonDesc);
            }
        }

        return hash.ToHashCode();
    }
}

/// <summary>One replica-grain row as collected, app-neutral. <c>ServerName</c> is the REPORTING server.</summary>
public sealed class AgTopologyReplicaRow
{
    public int ServerId { get; init; }
    public string ServerName { get; init; } = "";
    public DateTime CollectionTime { get; init; }
    public string? AgName { get; init; }
    public string? ReplicaServerName { get; init; }
    public string? RoleDesc { get; init; }
    public bool? IsLocal { get; init; }
    public string? OperationalStateDesc { get; init; }
    public string? ConnectedStateDesc { get; init; }
    public string? RecoveryHealthDesc { get; init; }
    public string? SynchronizationHealthDesc { get; init; }
    public string? AvailabilityModeDesc { get; init; }
    public string? FailoverModeDesc { get; init; }
    public string? EndpointUrl { get; init; }
}

/// <summary>One database-grain row as collected. Queue sizes are KB and rates KB/s (the DMV's units), both
/// instantaneous gauges rather than counters.</summary>
public sealed class AgTopologyDatabaseRow
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

/// <summary>
/// Base for the three bound view models a refresh updates in place (#4238) instead of replacing, so a WPF
/// container already realized for an item keeps showing that SAME item — only its property values move. Plain
/// BCL (<see cref="INotifyPropertyChanged"/>, no WPF dependency), matching this file's "pure" contract.
/// </summary>
public abstract class AgTopologyObservable : INotifyPropertyChanged
{
    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>One blanket notification refreshes every binding on this instance — including computed
    /// properties like <see cref="AgTopologyCard.SubtitleDisplay"/> — without hand-tracking which of a dozen
    /// display properties depends on which field. WPF treats a null/empty property name as "re-read everything
    /// bound to this object"; only an element a virtualizing panel has actually realized pays for that.</summary>
    protected void RaiseAllPropertiesChanged() => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(null));

    protected void RaisePropertyChanged(string propertyName) =>
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}

/// <summary>One replica chip, pre-banded. The view layer adds only a brush for <see cref="Severity"/>.</summary>
public sealed class AgTopologyReplica : AgTopologyObservable
{
    public string? ReplicaServerName { get; set; }
    public string? RoleDesc { get; set; }
    public bool IsPrimary { get; set; }
    public bool? IsLocal { get; set; }
    public string? OperationalStateDesc { get; set; }
    public string? ConnectedStateDesc { get; set; }
    public string? RecoveryHealthDesc { get; set; }
    public string? SynchronizationHealthDesc { get; set; }
    public string? AvailabilityModeDesc { get; set; }
    public string? FailoverModeDesc { get; set; }
    public string? EndpointUrl { get; set; }
    public HealthSeverity Severity { get; set; }

    public string RoleDisplay => string.IsNullOrWhiteSpace(RoleDesc) ? "UNKNOWN ROLE" : RoleDesc!;

    /// <summary>
    /// Marks the replica that IS the reporting server. This is the label that makes the per-perspective model
    /// legible: without it a reader cannot tell "this row is the server telling us about ITSELF" from "this is
    /// what that server BELIEVES about a node it only sees across the wire" — and those carry very different
    /// weight, because the DMV populates operational state and recovery health for the local replica only.
    ///
    /// <para>Shown only when explicitly true. A NULL means UNKNOWN, not remote: rows collected before the column
    /// existed genuinely do not know, and labelling them "remote" would assert something the data cannot
    /// support.</para>
    /// </summary>
    public string LocalDisplay => IsLocal == true ? "local" : "";
    public string NameDisplay => string.IsNullOrWhiteSpace(ReplicaServerName) ? "—" : ReplicaServerName!;

    /// <summary>The states worth reading at chip size. A column the DMV leaves null is omitted rather than shown
    /// as an em dash the reader must interpret; recovery health joins only when it is NOT plain ONLINE, since a
    /// healthy one would merely repeat the operational state's "ONLINE".</summary>
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

    /// <summary>Copies every field from a freshly read replica of the SAME identity, then raises one blanket
    /// change notification (#4238). The caller (<see cref="AgTopologyCard.UpdateFrom"/>, via
    /// <see cref="AgTopology.Reconcile{TItem,TKey}"/>) only calls this for a replica whose identity survived the
    /// refresh, so the bound chip element stays the same instance.</summary>
    public void UpdateFrom(AgTopologyReplica latest)
    {
        ArgumentNullException.ThrowIfNull(latest);

        ReplicaServerName = latest.ReplicaServerName;
        RoleDesc = latest.RoleDesc;
        IsPrimary = latest.IsPrimary;
        IsLocal = latest.IsLocal;
        OperationalStateDesc = latest.OperationalStateDesc;
        ConnectedStateDesc = latest.ConnectedStateDesc;
        RecoveryHealthDesc = latest.RecoveryHealthDesc;
        SynchronizationHealthDesc = latest.SynchronizationHealthDesc;
        AvailabilityModeDesc = latest.AvailabilityModeDesc;
        FailoverModeDesc = latest.FailoverModeDesc;
        EndpointUrl = latest.EndpointUrl;
        Severity = latest.Severity;

        RaiseAllPropertiesChanged();
    }
}

/// <summary>One database row in a card's grid, pre-banded.</summary>
public sealed class AgTopologyDatabase : AgTopologyObservable
{
    public string? DatabaseName { get; set; }
    public string? ReplicaServerName { get; set; }
    public bool? IsLocal { get; set; }
    public string? SynchronizationStateDesc { get; set; }
    public HealthSeverity SynchronizationStateSeverity { get; set; }
    public string? AvailabilityModeDesc { get; set; }
    public long? LogSendQueueKb { get; set; }
    public long? RedoQueueKb { get; set; }
    public long? LogSendRateKbPerSec { get; set; }
    public long? RedoRateKbPerSec { get; set; }
    public double? EstimatedSendDrainMinutes { get; set; }
    public double? EstimatedRedoCompletionMinutes { get; set; }
    public long? SecondaryLagSeconds { get; set; }
    public bool? IsSuspended { get; set; }
    public string? SuspendReasonDesc { get; set; }

    public string DatabaseDisplay => DatabaseName ?? "—";
    public string ReplicaDisplay => ReplicaServerName ?? "—";
    public string SyncStateDisplay => SynchronizationStateDesc ?? "—";
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
        : IsSuspended == true ? $"{SecondaryLagSeconds:N0} s (suspended)" : $"{SecondaryLagSeconds:N0} s";

    public string DataMovementDisplay => IsSuspended == true
        ? (string.IsNullOrWhiteSpace(SuspendReasonDesc) ? "Suspended" : "Suspended: " + SuspendReasonDesc)
        : IsSuspended == false ? "Moving" : "—";

    /// <summary>Only a suspended row earns the attention treatment; everything else is coloured by its sync
    /// state, which already carries the verdict.</summary>
    public HealthSeverity DataMovementSeverity =>
        IsSuspended == true ? HealthSeverity.Critical : HealthSeverity.Unknown;

    /// <summary>Same shape as <see cref="AgTopologyReplica.UpdateFrom"/> (#4238).</summary>
    public void UpdateFrom(AgTopologyDatabase latest)
    {
        ArgumentNullException.ThrowIfNull(latest);

        DatabaseName = latest.DatabaseName;
        ReplicaServerName = latest.ReplicaServerName;
        IsLocal = latest.IsLocal;
        SynchronizationStateDesc = latest.SynchronizationStateDesc;
        SynchronizationStateSeverity = latest.SynchronizationStateSeverity;
        AvailabilityModeDesc = latest.AvailabilityModeDesc;
        LogSendQueueKb = latest.LogSendQueueKb;
        RedoQueueKb = latest.RedoQueueKb;
        LogSendRateKbPerSec = latest.LogSendRateKbPerSec;
        RedoRateKbPerSec = latest.RedoRateKbPerSec;
        EstimatedSendDrainMinutes = latest.EstimatedSendDrainMinutes;
        EstimatedRedoCompletionMinutes = latest.EstimatedRedoCompletionMinutes;
        SecondaryLagSeconds = latest.SecondaryLagSeconds;
        IsSuspended = latest.IsSuspended;
        SuspendReasonDesc = latest.SuspendReasonDesc;

        RaiseAllPropertiesChanged();
    }
}

/// <summary>One reporting server's VIEW of one Availability Group — the card each app renders.</summary>
public sealed class AgTopologyCard : AgTopologyObservable
{
    public int ServerId { get; set; }

    /// <summary>The REPORTING server — the monitored instance whose DMVs produced this view, not necessarily the
    /// AG's primary.</summary>
    public string ServerName { get; set; } = "";

    public string? AgName { get; set; }
    public DateTime CollectionTime { get; set; }
    public DateTime? DatabaseCollectionTime { get; set; }
    public string? PrimaryReplica { get; set; }
    public HealthSeverity Severity { get; set; }

    /// <summary>Get-only and never reassigned (#4238): the SAME collection instance lives for the card's whole
    /// life, so the nested, non-virtualized replica-chip <c>ItemsControl</c> never sees a new
    /// <c>ItemsSource</c> reference and never rebuilds its containers either. <see cref="UpdateFrom"/> mutates
    /// its contents through <see cref="AgTopology.Reconcile{TItem,TKey}"/>.</summary>
    public ObservableCollection<AgTopologyReplica> Replicas { get; } = new();

    /// <summary>Same contract as <see cref="Replicas"/>, for the per-database grid.</summary>
    public ObservableCollection<AgTopologyDatabase> Databases { get; } = new();

    public string AgNameDisplay => string.IsNullOrWhiteSpace(AgName) ? "—" : AgName!;
    public string SeverityLabel => AgTopology.SeverityLabel(Severity);
    public bool HasDatabases => Databases.Count > 0;

    public string PrimaryDisplay => string.IsNullOrWhiteSpace(PrimaryReplica)
        ? "No primary reported"
        : "Primary: " + PrimaryReplica;

    /// <summary>Whose view this is, and how fresh. The two collector sweeps land independently, so the database
    /// grain gets its own "as of" whenever it differs — surfaced rather than hidden, because the collectors write
    /// nothing for a server with no AGs, so a card that stops refreshing keeps its last instant.</summary>
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

    public AgTopologyCard()
    {
        /* HasDatabases derives from Databases.Count, which a property-changed notification on this object alone
           cannot see — only the collection itself knows when it grows from 0 (no per-database rows collected
           yet) to some. UpdateFrom mutates Databases in place rather than replacing it, so without this the
           DataGrid's visibility trigger would go stale the first time a card's database rows show up after its
           card was already on screen (#4238). */
        Databases.CollectionChanged += (_, _) => RaisePropertyChanged(nameof(HasDatabases));
    }

    /// <summary>Updates every field from a freshly built card of the SAME (<see cref="AgTopology.CardKey"/>)
    /// identity, then raises one blanket change notification (#4238) — the caller only calls this when the
    /// identity survived a refresh, so the card's bound container stays the same instance and only its content
    /// redraws. The nested Replicas/Databases lists are reconciled the same way a tab reconciles its own card
    /// list, through the shared <see cref="AgTopology.Reconcile{TItem,TKey}"/>.</summary>
    public void UpdateFrom(AgTopologyCard latest)
    {
        ArgumentNullException.ThrowIfNull(latest);

        ServerId = latest.ServerId;
        ServerName = latest.ServerName;
        AgName = latest.AgName;
        CollectionTime = latest.CollectionTime;
        DatabaseCollectionTime = latest.DatabaseCollectionTime;
        PrimaryReplica = latest.PrimaryReplica;
        Severity = latest.Severity;

        AgTopology.Reconcile(
            Replicas, latest.Replicas,
            static r => (r.ReplicaServerName ?? "").ToUpperInvariant(),
            static (existing, updated) => existing.UpdateFrom(updated));

        AgTopology.Reconcile(
            Databases, latest.Databases,
            static d => ((d.DatabaseName ?? "").ToUpperInvariant(), (d.ReplicaServerName ?? "").ToUpperInvariant()),
            static (existing, updated) => existing.UpdateFrom(updated));

        RaiseAllPropertiesChanged();
    }
}
