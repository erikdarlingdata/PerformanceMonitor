/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>One Availability Group alert this sweep decided to raise. Deliberately a plain description
/// rather than a delivery: the evaluator is WPF-free and testable, and MainWindow does the sending through
/// the same path every other Lite alert uses.</summary>
/// <param name="MetricName">One of the <see cref="AgAlertPolicy"/> metric-name consts — a webhook automation
/// key, identical to the name Darling fires.</param>
/// <param name="CurrentValue">The alert's "current" column, matching Darling's for the same condition.</param>
/// <param name="ThresholdValue">The alert's "expected" column.</param>
/// <param name="DetailText">The operator-facing explanation.</param>
/// <param name="IsResolution">True for the reconnect notice, which renders green rather than as a page.</param>
/// <param name="Context">Discrete facts for database-scoped alerts (#2109) — null for the replica-grain
/// alerts and resolutions, which carry no database. Trailing optional so existing construction sites
/// (and the tests that pin them) stay untouched.</param>
/// <param name="RefireStampKey">Opaque token (#2426), non-null only on an alert whose DELIVERY opens a
/// re-fire window. The caller hands it back through <see cref="AgAlertEvaluator.NoteDelivered"/> after
/// sending, which is what keeps the window stamped on delivery rather than on the decision: Lite evaluates
/// even while a server is acknowledged or silenced and simply does not send, and a suppressed alert must not
/// consume a window it was never announced in.</param>
public readonly record struct AgAlert(
    string MetricName,
    string CurrentValue,
    string ThresholdValue,
    string DetailText,
    bool IsResolution,
    AlertContext? Context = null,
    string? RefireStampKey = null);

/// <summary>
/// Lite's Availability Group alert state machine (#1696) — the twin of Darling's
/// <c>DarlingSelfAlertEvaluator</c> AG section, over the SAME
/// <see cref="PerformanceMonitor.Common.AgAlertPolicy"/> decisions and the SAME metric names, so the two
/// apps cannot drift on when an AG alert fires or what it is called.
///
/// <para>What is Lite-shaped rather than shared: the state lives in plain dictionaries (Lite's alert loop is
/// single-threaded off a UI timer, unlike Darling's concurrent fleet sweep), and the result is returned for
/// the caller to deliver instead of being pushed through a deliverer. Everything about WHEN an alert fires
/// is the shared policy's.</para>
///
/// <para>Keyed per AG grain (ag+replica, ag+database+replica) rather than per server, because a server hosts
/// many replicas and databases and two lagging databases must track independently. The composite key is
/// separated by a UNIT SEPARATOR (U+001F) because AG / database / replica names are SQL Server identifiers
/// and may contain any printable character when delimited.</para>
/// </summary>
public sealed class AgAlertEvaluator
{
    private const char KeySeparator = '\u001f';

    private readonly Dictionary<string, string> _replicaRole = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _replicaConnectedState = new(StringComparer.Ordinal);
    private readonly Dictionary<string, bool> _databaseSuspended = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTime> _lastSyncBehindAlert = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTime> _lastDisconnectAlert = new(StringComparer.Ordinal);
    private readonly HashSet<string> _activeSyncBehind = new(StringComparer.Ordinal);

    private readonly Func<DateTime> _utcNow;

    public AgAlertEvaluator(Func<DateTime>? utcNow = null) => _utcNow = utcNow ?? (() => DateTime.UtcNow);

    /// <summary>
    /// The replica-grain conditions for one server: "AG Failover" and the disconnect/reconnect pair. Both are
    /// pure transitions per ag+replica, and the FIRST sighting of a replica is a silent baseline.
    /// </summary>
    /// <param name="disconnectRefireInterval">#2426: how often to re-announce a replica that is STILL
    /// disconnected. Null (the shipped default) is edge-only — one alert per outage, however long it lasts.
    /// The re-fire decision is the shared policy's, and the one departure from the silent-baseline rule is
    /// documented there: with re-fire on, a replica already down at first sighting announces, because Lite's
    /// AG state is in-memory and would otherwise let a restart silence a standing outage permanently.</param>
    public List<AgAlert> EvaluateReplicas(
        int serverId, IReadOnlyList<AgReplicaReading> replicas, TimeSpan? disconnectRefireInterval = null)
    {
        var alerts = new List<AgAlert>();
        if (replicas is null)
        {
            return alerts;
        }

        foreach (var replica in replicas)
        {
            var key = ReplicaKey(serverId, replica.AgName, replica.ReplicaServerName);

            if (!string.IsNullOrEmpty(replica.RoleDesc))
            {
                _replicaRole.TryGetValue(key, out var previousRole);
                bool failover = AgAlertPolicy.IsFailover(previousRole, replica.RoleDesc);
                _replicaRole[key] = replica.RoleDesc!;

                if (failover)
                {
                    alerts.Add(new AgAlert(
                        AgAlertPolicy.FailoverMetric,
                        replica.RoleDesc!,
                        previousRole!,
                        $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} changed role from " +
                        $"{previousRole} to {replica.RoleDesc}. A role change is either a failover somebody performed or " +
                        "an automatic one the cluster performed after a health-check timeout — if nobody initiated it, " +
                        "the previous primary had a problem worth finding. Confirm the new primary is the node you want " +
                        "serving the workload, check the WSFC cluster log for the failover reason, and verify that " +
                        "backups, index maintenance and integrity checks run against the new primary.",
                        IsResolution: false));
                }
            }

            if (!string.IsNullOrEmpty(replica.ConnectedStateDesc))
            {
                _replicaConnectedState.TryGetValue(key, out var previousState);
                var decision = AgAlertPolicy.DecideConnection(
                    previousState,
                    replica.ConnectedStateDesc,
                    disconnectRefireInterval,
                    _lastDisconnectAlert.TryGetValue(key, out var lastDisconnect) ? lastDisconnect : null,
                    _utcNow());
                _replicaConnectedState[key] = replica.ConnectedStateDesc!;

                if (decision is AgConnectionDecision.Disconnected or AgConnectionDecision.StillDisconnected)
                {
                    /* #2426: a re-fire is the SAME metric name and the same guidance — webhook automation
                       keyed on it is what the re-fire exists to re-trigger — and differs only in saying so,
                       because an operator reading the alert history has no other way to tell a fresh outage
                       from the sixth hour of one. The wording matches the connection re-fire's. */
                    var opening = decision == AgConnectionDecision.StillDisconnected
                        ? $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is STILL " +
                          $"DISCONNECTED from the primary (re-alerting every " +
                          $"{((int)disconnectRefireInterval!.Value.TotalMinutes).ToString(CultureInfo.InvariantCulture)} min)."
                        : $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is DISCONNECTED " +
                          "from the primary.";

                    alerts.Add(new AgAlert(
                        AgAlertPolicy.ReplicaDisconnectedMetric,
                        replica.ConnectedStateDesc!,
                        "CONNECTED",
                        opening +
                        " A disconnected replica receives no log at all, so it falls further behind " +
                        "every second and cannot be failed over to without losing whatever the primary has committed " +
                        "since. If it is a synchronous-commit replica, the primary also loses its automatic-failover " +
                        "partner. Check the replica's SQL Server service, the availability endpoint (TCP 5022 by " +
                        "default) and its firewall rule, the WSFC quorum, and the network between the nodes.",
                        IsResolution: false,
                        RefireStampKey: key));
                }
                else if (decision == AgConnectionDecision.Reconnected)
                {
                    /* The clock is cleared on the RECONNECT decision rather than on the resolution's
                       delivery: once the replica is back there is nothing left to re-announce, so a stamp
                       surviving a suppressed reconnect notice could only mis-date the NEXT outage — which
                       announces on its own edge regardless. */
                    _lastDisconnectAlert.Remove(key);
                    alerts.Add(new AgAlert(
                        AgAlertPolicy.ReplicaReconnectedMetric,
                        replica.ConnectedStateDesc!,
                        "CONNECTED",
                        $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is connected to " +
                        "the primary again. It is still behind by whatever accumulated while it was gone — watch the " +
                        "send and redo queues until they drain before you count it as a failover target again.",
                        IsResolution: true));
                }
            }
        }

        return alerts;
    }

    /// <summary>
    /// The database-grain conditions for one server: "AG Database Suspended" (a pure <c>is_suspended</c>
    /// false→true edge) and "AG Sync Fell Behind" (a standing condition with a cooldown re-fire).
    ///
    /// <para>Recovery is driven off the databases this sweep MEASURED as caught up, never off "everything
    /// tracked that did not breach" — a lagging database that becomes SUSPENDED, or whose columns go NULL
    /// under quorum loss, stops breaching without recovering, and the looser rule would announce it had
    /// caught up in the same sweep that reports it suspended. Resolutions are returned as
    /// <see cref="AgAlert.IsResolution"/> notices so the caller can record them without paging.</para>
    /// </summary>
    /// <param name="cooldown">How long a standing sync-behind alert waits before re-firing. Lite passes the
    /// user's configured alert cooldown, matching what its other standing alerts use.</param>
    public List<AgAlert> EvaluateDatabases(
        int serverId,
        IReadOnlyList<AgDatabaseReading> databases,
        int lagThresholdSeconds,
        long redoThresholdKb,
        TimeSpan cooldown)
    {
        var alerts = new List<AgAlert>();
        if (databases is null)
        {
            return alerts;
        }

        var now = _utcNow();
        var measuredCaughtUp = new List<string>();

        foreach (var database in databases)
        {
            var key = DatabaseKey(serverId, database.AgName, database.DatabaseName, database.ReplicaServerName);

            if (database.IsSuspended is bool suspended)
            {
                bool seen = _databaseSuspended.TryGetValue(key, out var wasSuspended);
                var decision = AgAlertPolicy.DecideSuspension(seen ? wasSuspended : null, suspended);
                _databaseSuspended[key] = suspended;

                if (decision == AgSuspensionDecision.Suspended)
                {
                    var suspendReason = string.IsNullOrWhiteSpace(database.SuspendReasonDesc)
                        ? "no reason reported"
                        : database.SuspendReasonDesc!;
                    alerts.Add(new AgAlert(
                        AgAlertPolicy.DatabaseSuspendedMetric,
                        suspendReason,
                        "SYNCHRONIZING",
                        $"Availability Group '{database.AgName}': data movement for database " +
                        $"{database.DatabaseName} on replica {database.ReplicaServerName} is SUSPENDED " +
                        $"({suspendReason}). While movement is suspended the secondary receives nothing AND the " +
                        "primary cannot truncate its transaction log, so the primary's log grows until its disk " +
                        "fills — this is a primary-side outage risk, not just a secondary-side one. Fix the " +
                        $"underlying cause, then resume it with ALTER DATABASE [{database.DatabaseName}] SET HADR RESUME.",
                        IsResolution: false,
                        Context: AgAlertContexts.ForDatabase(
                            database.DatabaseName, database.AgName, database.ReplicaServerName,
                            ("Suspend Reason", suspendReason))));
                }
                else if (decision == AgSuspensionDecision.Resumed)
                {
                    alerts.Add(new AgAlert(
                        "AG Data Movement Resumed",
                        "resumed",
                        "SYNCHRONIZING",
                        $"Availability Group '{database.AgName}': data movement for {database.DatabaseName} on " +
                        $"replica {database.ReplicaServerName} has resumed.",
                        IsResolution: true));
                }
            }

            var judgement = AgAlertPolicy.JudgeSync(database, lagThresholdSeconds, redoThresholdKb, out var behindReason);
            if (judgement == AgSyncJudgement.CaughtUp)
            {
                measuredCaughtUp.Add(key);
            }
            else if (judgement == AgSyncJudgement.Behind)
            {
                _activeSyncBehind.Add(key);
                if (!_lastSyncBehindAlert.TryGetValue(key, out var last) || now - last >= cooldown)
                {
                    _lastSyncBehindAlert[key] = now;
                    alerts.Add(new AgAlert(
                        AgAlertPolicy.SyncFellBehindMetric,
                        behindReason,
                        "caught up",
                        behindReason + " A secondary that trails the primary is a data-loss window: an automatic " +
                        "failover cannot complete until it catches up, and a forced failover throws away everything " +
                        "still queued. Look at the network throughput between the replicas, the secondary's redo " +
                        "thread, and whether something on the primary — an index rebuild, a bulk load, a long " +
                        "transaction — is generating log faster than the secondary can consume it. Read the figures " +
                        "for what they measure: the lag seconds are how STALE the secondary's last hardened log is, " +
                        "not how much data is queued behind it, so on a quiet group a large value can simply mean " +
                        "nothing has been written recently.",
                        IsResolution: false,
                        Context: AgAlertContexts.ForDatabase(
                            database.DatabaseName, database.AgName, database.ReplicaServerName)));
                }
            }
        }

        foreach (var key in measuredCaughtUp)
        {
            _lastSyncBehindAlert.Remove(key);
            if (_activeSyncBehind.Remove(key))
            {
                alerts.Add(new AgAlert(
                    "AG Sync Recovered",
                    "caught up",
                    "caught up",
                    $"{DescribeDatabaseKey(key)} has caught up with the primary.",
                    IsResolution: true));
            }
        }

        return alerts;
    }

    /// <summary>
    /// Opens the re-fire window for an alert the caller actually SENT (#2426). Alerts with no
    /// <see cref="AgAlert.RefireStampKey"/> are ignored, so the caller can hand every alert back without
    /// caring which ones carry a window.
    ///
    /// <para>Deliberately the caller's call rather than something the evaluator does while deciding, and it
    /// is the same split Lite's connection re-fire already uses (<c>_lastConnectionDownAlertUtc</c> is
    /// stamped beside <c>SendConnectionAlert</c>, not inside the policy). Lite evaluates AG health on every
    /// sweep but skips delivery while a server is acknowledged or silenced; stamping at decision time would
    /// let those suppressed sweeps eat window after window, and the operator would come back from an
    /// acknowledgement to silence rather than to a re-announcement.</para>
    /// </summary>
    public void NoteDelivered(AgAlert alert)
    {
        if (alert.RefireStampKey is string key)
        {
            _lastDisconnectAlert[key] = _utcNow();
        }
    }

    /// <summary>Drops all AG state for a server removed from the monitored list, so a later re-add starts at a
    /// fresh baseline rather than inheriting a stale role and paging a phantom failover.</summary>
    public void Forget(int serverId)
    {
        var prefix = ServerPrefix(serverId);
        ForgetByPrefix(_replicaRole, prefix);
        ForgetByPrefix(_replicaConnectedState, prefix);
        ForgetByPrefix(_databaseSuspended, prefix);
        ForgetByPrefix(_lastSyncBehindAlert, prefix);
        ForgetByPrefix(_lastDisconnectAlert, prefix);
        _activeSyncBehind.RemoveWhere(k => k.StartsWith(prefix, StringComparison.Ordinal));
    }

    private static void ForgetByPrefix<TValue>(Dictionary<string, TValue> state, string prefix)
    {
        var doomed = new List<string>();
        foreach (var key in state.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                doomed.Add(key);
            }
        }

        foreach (var key in doomed)
        {
            state.Remove(key);
        }
    }

    private static string ServerPrefix(int serverId) =>
        serverId.ToString(CultureInfo.InvariantCulture) + KeySeparator;

    private static string ReplicaKey(int serverId, string agName, string replicaServerName) =>
        ServerPrefix(serverId) + agName + KeySeparator + replicaServerName;

    private static string DatabaseKey(int serverId, string agName, string databaseName, string replicaServerName) =>
        ServerPrefix(serverId) + agName + KeySeparator + databaseName + KeySeparator + replicaServerName;

    private static string DescribeDatabaseKey(string key)
    {
        var parts = key.Split(KeySeparator);
        return parts.Length == 4
            ? $"Database {parts[2]} in AG {parts[1]} on replica {parts[3]}"
            : key;
    }
}
