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
using PerformanceMonitor.Alerting;
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
/// re-fire window. The caller hands the alert back through <see cref="AgAlertEvaluator.NoteSent"/> after
/// sending, which stamps the window (through <see cref="AgAlertEvaluator.NoteDelivered"/>) unless every channel
/// failed. That is what keeps the window stamped on delivery rather than on the decision: Lite evaluates
/// even while a server is acknowledged or silenced and simply does not send, and a suppressed alert must not
/// consume a window it was never announced in.</param>
/// <param name="RetryKey">Opaque token (#4795), non-null on an alert that is tried again when no channel delivered
/// it. The caller hands the send's answer back through <see cref="AgAlertEvaluator.NoteSent"/>, which records it
/// under this key, puts the alert's "already reported" marker back on a failure, and holds the alert back until
/// the retry is due. Null on resolution notices, which are not retried.</param>
public readonly record struct AgAlert(
    string MetricName,
    string CurrentValue,
    string ThresholdValue,
    string DetailText,
    bool IsResolution,
    AlertContext? Context = null,
    string? RefireStampKey = null,
    string? RetryKey = null)
{
    /// <summary>The server this alert was decided for (#4795). Set by the evaluator when it returns the alert, and
    /// read back by <see cref="AgAlertEvaluator.NoteSent"/> and <see cref="AgAlertEvaluator.NoteDelivered"/> to find
    /// the server's current <see cref="Generation"/>.</summary>
    public int ServerId { get; init; }

    /// <summary>How many times the evaluator had forgotten <see cref="ServerId"/> when it decided this alert (#4795).
    /// The send takes time, and the server can be removed while it runs: an answer that arrives with a generation
    /// the server has since moved past belongs to a server that is gone, and is dropped rather than recorded.</summary>
    public int Generation { get; init; }
}

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

    /// <summary>When an alert that no channel delivered is due again (#4795), by <see cref="AgAlert.RetryKey"/>.</summary>
    private readonly FailedSendRetryTracker _retries = new();

    /// <summary>How to put an alert's "already reported" marker back if its send reaches no channel (#4795): the
    /// prior value, or no entry when there was none. Registered when the alert is decided, run or dropped by
    /// <see cref="NoteSent"/>.</summary>
    private readonly Dictionary<string, Action> _putBack = new(StringComparer.Ordinal);

    /// <summary>How many times each server has been forgotten (#4795); a server never forgotten has no entry and reads as 0.
    /// An alert carries the value it was decided under (<see cref="AgAlert.Generation"/>), which is how an answer that
    /// arrives after <see cref="Forget"/> is told from one for an alert decided since. It is kept per server, so removing
    /// one server does not discard another's answers, and a plain check for "is there still state for this grain" would
    /// not do: a re-added server's first sweep creates that state again before the old answer can arrive. One small entry
    /// per server ever removed.</summary>
    private readonly Dictionary<int, int> _generations = new();

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
    /// <param name="sweepGeneration">#4795: the <see cref="GenerationOf"/> the sweep captured before it started reading.
    /// When it is given and the server has been forgotten since, the reading belongs to a server that is gone, so this
    /// returns nothing and records nothing. Null (the default) evaluates whatever it is handed.</param>
    public List<AgAlert> EvaluateReplicas(
        int serverId,
        IReadOnlyList<AgReplicaReading> replicas,
        TimeSpan? disconnectRefireInterval = null,
        int? sweepGeneration = null)
    {
        var alerts = new List<AgAlert>();
        if (sweepGeneration.HasValue && sweepGeneration.Value != GenerationOf(serverId))
        {
            return alerts;
        }

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

                /* #4795: the role marker moves to the new role when the alert is decided. If the send then
                   reaches no channel, NoteSent puts it back to the prior role so the next sweep sees the change
                   again; until the failed-send delay is up the marker stays put and the alert is held back. */
                var failoverRetryKey = RetryKeyFor(key, AgAlertPolicy.FailoverMetric);
                bool failoverHeld = failover && _retries.RetryPending(failoverRetryKey, _utcNow());
                if (!failoverHeld)
                {
                    _replicaRole[key] = replica.RoleDesc!;
                }

                if (!failover)
                {
                    _retries.Clear(failoverRetryKey);
                }

                /* #3653 A5 asked whether this edge should also forget the server's delta baselines, and the
                   answer is no, deliberately — the same answer as Darling's twin. A role change is a fact about
                   the REPLICA the row names, judged from whichever monitored connection can see the
                   AG; it is not a fact about which instance THIS server_id's connection reaches. A registration
                   pointed at a node directly keeps reading that node's cumulative DMVs through a failover —
                   the counters are continuous and a forget would throw one honest interval away on every
                   replica that can see the role change. A registration pointed at the LISTENER does land on a
                   different instance after a failover, and that instance reports a different @@SERVERNAME
                   and sqlserver_start_time, which is exactly the pair the identity-epoch carrier
                   (CpuUtilizationCollector -> ServerEpoch) compares every minute: the forget happens there,
                   named for the mechanism that actually moved the counters, and this edge stays what it is
                   — an alert about a role. */
                if (failover && !failoverHeld)
                {
                    _putBack[failoverRetryKey] = () => _replicaRole[key] = previousRole!;
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
                        IsResolution: false,
                        RetryKey: failoverRetryKey));
                }
            }

            if (!string.IsNullOrEmpty(replica.ConnectedStateDesc))
            {
                _replicaConnectedState.TryGetValue(key, out var previousState);
                var disconnectRetryKey = RetryKeyFor(key, AgAlertPolicy.ReplicaDisconnectedMetric);
                var decision = AgAlertPolicy.DecideConnection(
                    previousState,
                    replica.ConnectedStateDesc,
                    disconnectRefireInterval,
                    _lastDisconnectAlert.TryGetValue(key, out var lastDisconnect) ? lastDisconnect : null,
                    _utcNow(),
                    _retries.DueUtc(disconnectRetryKey));
                _replicaConnectedState[key] = replica.ConnectedStateDesc!;

                if (decision is AgConnectionDecision.Disconnected or AgConnectionDecision.StillDisconnected)
                {
                    /* #2426: a re-fire is the SAME metric name and the same guidance — webhook automation
                       keyed on it is what the re-fire exists to re-trigger — and differs only in saying so,
                       because an operator reading the alert history has no other way to tell a fresh outage
                       from the sixth hour of one. The wording matches the connection re-fire's. */
                    /* #4795: with re-fire off a still-disconnected decision can only be the retry of an alert no
                       channel delivered, and "re-alerting every 0 min" would be false. */
                    var opening = decision == AgConnectionDecision.StillDisconnected
                        ? disconnectRefireInterval is TimeSpan every && every > TimeSpan.Zero
                            ? $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is STILL " +
                              $"DISCONNECTED from the primary (re-alerting every " +
                              $"{((int)every.TotalMinutes).ToString(CultureInfo.InvariantCulture)} min)."
                            : $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is STILL " +
                              "DISCONNECTED from the primary (the previous alert reached no channel, so it is sent again)."
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
                        RefireStampKey: key,
                        RetryKey: disconnectRetryKey));
                }
                else if (decision == AgConnectionDecision.Reconnected)
                {
                    /* The clock is cleared on the RECONNECT decision rather than on the resolution's
                       delivery: once the replica is back there is nothing left to re-announce, so a stamp
                       surviving a suppressed reconnect notice could only mis-date the NEXT outage — which
                       announces on its own edge regardless. */
                    _lastDisconnectAlert.Remove(key);
                    _retries.Clear(disconnectRetryKey);
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

        return Stamp(alerts, serverId);
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
    /// <param name="sweepGeneration">#4795: as on <see cref="EvaluateReplicas"/>: given and out of date, this returns
    /// nothing and records nothing.</param>
    public List<AgAlert> EvaluateDatabases(
        int serverId,
        IReadOnlyList<AgDatabaseReading> databases,
        int lagThresholdSeconds,
        long redoThresholdKb,
        TimeSpan cooldown,
        int? sweepGeneration = null)
    {
        var alerts = new List<AgAlert>();
        if (sweepGeneration.HasValue && sweepGeneration.Value != GenerationOf(serverId))
        {
            return alerts;
        }

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

                /* #4795: the same put-back-and-hold as the failover marker. */
                var suspendedRetryKey = RetryKeyFor(key, AgAlertPolicy.DatabaseSuspendedMetric);
                bool suspensionHeld = decision == AgSuspensionDecision.Suspended
                    && _retries.RetryPending(suspendedRetryKey, now);
                if (!suspensionHeld)
                {
                    _databaseSuspended[key] = suspended;
                }

                if (decision != AgSuspensionDecision.Suspended)
                {
                    _retries.Clear(suspendedRetryKey);
                }

                if (decision == AgSuspensionDecision.Suspended && !suspensionHeld)
                {
                    _putBack[suspendedRetryKey] = () =>
                    {
                        if (seen)
                        {
                            _databaseSuspended[key] = wasSuspended;
                        }
                        else
                        {
                            _databaseSuspended.Remove(key);
                        }
                    };

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
                            ("Suspend Reason", suspendReason)),
                        RetryKey: suspendedRetryKey));
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
                var syncRetryKey = RetryKeyFor(key, AgAlertPolicy.SyncFellBehindMetric);
                var hadStamp = _lastSyncBehindAlert.TryGetValue(key, out var last);
                if (!_retries.RetryPending(syncRetryKey, now) && (!hadStamp || now - last >= cooldown))
                {
                    _lastSyncBehindAlert[key] = now;

                    /* #4795: the cooldown stamp is taken at the decision. A send that reaches no channel puts
                       it back (the prior stamp, or none), so the retry after the failed-send delay is not made
                       to wait out the whole cooldown. */
                    _putBack[syncRetryKey] = () =>
                    {
                        if (hadStamp)
                        {
                            _lastSyncBehindAlert[key] = last;
                        }
                        else
                        {
                            _lastSyncBehindAlert.Remove(key);
                        }
                    };
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
                            database.DatabaseName, database.AgName, database.ReplicaServerName),
                        RetryKey: syncRetryKey));
                }
            }
        }

        foreach (var key in measuredCaughtUp)
        {
            _lastSyncBehindAlert.Remove(key);
            _retries.Clear(RetryKeyFor(key, AgAlertPolicy.SyncFellBehindMetric));
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

        return Stamp(alerts, serverId);
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
        /* #4795: an alert decided before its server was removed opens no window. The server is gone, and a re-add
           would inherit the stamp, which is what Forget exists to prevent. */
        if (alert.RefireStampKey is string key && !IsStale(alert))
        {
            _lastDisconnectAlert[key] = _utcNow();
        }
    }

    /// <summary>
    /// What the sweep calls after it sends an alert (#4795), in place of calling <see cref="NoteDelivered"/> on every
    /// alert whatever the send did. When every channel failed
    /// (<see cref="FailedSendBackoff.EveryChannelFailed"/>) the alert's re-fire window is NOT opened, its "already
    /// reported" marker is put back to what it was before it was decided, and the alert is held back until the
    /// failed-send delay is up (a minute, doubling, never more than <paramref name="cap"/>, the alert cooldown), so
    /// a lasting channel failure is tried at 1, 2, 4 ... minutes rather than on every sweep. Any other answer
    /// (delivered, partly delivered, muted, throttled, unreported) ends the retry and does what
    /// <see cref="NoteDelivered"/> always did. Resolution notices carry no retry key and are not retried.
    ///
    /// <para>An answer for an alert decided before its server was forgotten (<see cref="Forget"/>) is dropped whole:
    /// no retry, no re-fire window, and the newer alert's put-back is left alone (#4795). The send was still running
    /// when the server was removed, and recording its answer would bring back state for a server that is gone.</para>
    /// </summary>
    public void NoteSent(AgAlert alert, AlertDelivery? delivery, TimeSpan cap)
    {
        if (IsStale(alert))
        {
            return;
        }

        if (alert.RetryKey is string retryKey)
        {
            if (_retries.Record(retryKey, delivery, _utcNow(), cap))
            {
                if (_putBack.Remove(retryKey, out var putBack))
                {
                    putBack();
                }

                return;
            }

            _putBack.Remove(retryKey);
        }

        NoteDelivered(alert);
    }

    private static string RetryKeyFor(string grainKey, string metric) => grainKey + KeySeparator + metric;

    /// <summary>Drops all AG state for a server removed from the monitored list, so a later re-add starts at a
    /// fresh baseline rather than inheriting a stale role and paging a phantom failover. That includes the
    /// server's pending retries and put-backs (#4795): a retry left behind would page a replica that is still
    /// disconnected on the re-add's first sweep instead of taking the silent baseline, and a failed-send streak
    /// left behind would lengthen the waits of its next outage. Every retry key starts with the server's prefix.
    /// It also moves the server to its next generation, so the answer of a send still running for an alert decided
    /// before this call is dropped by <see cref="NoteSent"/> rather than recording state for the removed server.</summary>
    public void Forget(int serverId)
    {
        _generations[serverId] = GenerationOf(serverId) + 1;
        var prefix = ServerPrefix(serverId);
        ForgetByPrefix(_replicaRole, prefix);
        ForgetByPrefix(_replicaConnectedState, prefix);
        ForgetByPrefix(_databaseSuspended, prefix);
        ForgetByPrefix(_lastSyncBehindAlert, prefix);
        ForgetByPrefix(_lastDisconnectAlert, prefix);
        _activeSyncBehind.RemoveWhere(k => k.StartsWith(prefix, StringComparison.Ordinal));
        _retries.ClearPrefix(prefix);
        ForgetByPrefix(_putBack, prefix);
    }

    /// <summary>How many times the server has been forgotten (#4795). A sweep captures it before its first await and
    /// passes it to both evaluations, so a sweep that was reading when its server was removed records nothing.</summary>
    public int GenerationOf(int serverId) => _generations.TryGetValue(serverId, out var generation) ? generation : 0;

    /// <summary>True for an alert decided before its server was last forgotten (#4795). A hand-built alert carries
    /// server 0 and generation 0, which is current until server 0 is forgotten.</summary>
    private bool IsStale(AgAlert alert) => alert.Generation != GenerationOf(alert.ServerId);

    /// <summary>Marks every alert an evaluation returns with the server and generation it was decided under (#4795).
    /// Done once where both evaluations finish, so an alert added to either later cannot go out unmarked.</summary>
    private List<AgAlert> Stamp(List<AgAlert> alerts, int serverId)
    {
        var generation = GenerationOf(serverId);
        for (var i = 0; i < alerts.Count; i++)
        {
            alerts[i] = alerts[i] with { ServerId = serverId, Generation = generation };
        }

        return alerts;
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
