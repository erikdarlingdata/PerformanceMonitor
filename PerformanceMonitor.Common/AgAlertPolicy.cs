/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>One replica's Availability Group state from the latest <c>ag_replica_states</c> snapshot — only
/// the columns the alerts judge. <c>AgName</c>/<c>ReplicaServerName</c> are non-null because they are the
/// identity of the alert's state key and a row that cannot be keyed is dropped by the reader. Every other
/// column is nullable ON PURPOSE: under WSFC quorum loss the AG catalog views serve only locally cached
/// metadata, so any state string can read NULL.</summary>
/// <param name="IsLocal">Whether this row describes the replica the collector is connected to. Null on a
/// store that predates the column. Used to prefer one node's view when the same AG is monitored from several
/// nodes — see <see cref="AgAlertPolicy"/>.</param>
public readonly record struct AgReplicaReading(
    string AgName,
    string ReplicaServerName,
    string? RoleDesc,
    string? ConnectedStateDesc,
    bool? IsLocal = null);

/// <summary>One database-on-one-replica's AG state from the latest <c>ag_database_replica_states</c>
/// snapshot. <c>RedoQueueSizeKb</c> is the DMV's <c>redo_queue_size</c>, which is KILOBYTES.</summary>
public readonly record struct AgDatabaseReading(
    string AgName,
    string DatabaseName,
    string ReplicaServerName,
    long? SecondaryLagSeconds,
    long? RedoQueueSizeKb,
    bool? IsSuspended,
    string? SuspendReasonDesc);

/// <summary>The outcome of <see cref="AgAlertPolicy.JudgeSync"/>. The third case is the one that matters:
/// "we could not measure this" is NOT "this is fine", and conflating them is how a standing alert gets
/// resolved by the very event that made it worse.</summary>
public enum AgSyncJudgement
{
    /// <summary>Nothing trustworthy to judge on — both thresholds off, the columns NULL (the primary's own
    /// row, or WSFC quorum loss), or a SUSPENDED row that is not over any threshold. No signal: neither fire
    /// nor clear.</summary>
    NotMeasurable,

    /// <summary>Measured, and within threshold.</summary>
    CaughtUp,

    /// <summary>Measured, and past a threshold.</summary>
    Behind,
}

/// <summary>What one replica-state observation should announce about connectivity, if anything.</summary>
public enum AgConnectionDecision
{
    /// <summary>Steady state, an unreadable value, or a silent first-sighting baseline.</summary>
    None,

    /// <summary>Crossed into DISCONNECTED from a known-connected state.</summary>
    Disconnected,

    /// <summary>Came back to CONNECTED from a known-disconnected state.</summary>
    Reconnected,

    /// <summary>A standing disconnect re-announcement (#1696, opt-in): the replica is still
    /// DISCONNECTED and the re-fire interval has elapsed since the last disconnect alert. Callers deliver
    /// this under the SAME metric name as <see cref="Disconnected"/> — webhook automation matches on the
    /// metric name, and re-triggering it is the entire point of a re-fire.</summary>
    StillDisconnected,
}

/// <summary>What one <c>is_suspended</c> observation should announce, if anything.</summary>
public enum AgSuspensionDecision
{
    /// <summary>Steady state, no reading, or a silent first-sighting baseline.</summary>
    None,

    /// <summary>Data movement stopped (false → true).</summary>
    Suspended,

    /// <summary>Data movement resumed (true → false).</summary>
    Resumed,
}

/// <summary>How good one monitored server's view of a given Availability Group is (#1696). Every replica in
/// an AG is visible from every node, so a fully-monitored 3-node AG yields three views of the same replica —
/// and judging all three reports one failover three times. Ranked so the best available view wins.</summary>
public enum AgVantage
{
    /// <summary>This server's snapshot says nothing about the AG.</summary>
    None,

    /// <summary>The AG appears, but no row is flagged local — either a pre-#1696 store whose rows predate
    /// the <c>is_local</c> column, or a genuinely remote-only view. Usable, but the weakest evidence.</summary>
    Remote,

    /// <summary>This server is one of the AG's replicas, but not its primary.</summary>
    Local,

    /// <summary>This server is the AG's PRIMARY. The best vantage by a distance: a SECONDARY's
    /// <c>sys.dm_hadr_*</c> carries only its OWN row, so only the primary sees the whole group.</summary>
    LocalPrimary,
}

/// <summary>
/// The single definition of when an Availability Group alert fires, shared by Darling's headless self-alert
/// evaluator and Lite's alert path (#991/#1696) — the <see cref="ConnectionAlertPolicy"/> discipline: pure
/// decisions live here, each app owns its own edge STATE and its own delivery, so the two cannot drift on
/// the part that is easy to get wrong.
///
/// <para>Three rules run through everything below and are worth stating once, because each was learned the
/// expensive way against a live AG rather than from the documentation:</para>
///
/// <para><b>1. A first sighting is a silent baseline.</b> Only a transition from a KNOWN state announces
/// anything. A service starting up must not page "failover" because it has never seen the role before, nor
/// "disconnected" for a replica that was already down when monitoring began.</para>
///
/// <para><b>2. NULL is never a transition.</b> Under WSFC quorum loss the AG catalog views return only
/// cached metadata and any column can read NULL. Treating that as an edge would spray alerts across the
/// whole fleet at the exact moment the cluster is already in trouble.</para>
///
/// <para><b>3. A SUSPENDED row may RAISE an alarm but may NEVER CLEAR one.</b> Every measured signal on a
/// suspended replica is untrustworthy in the REASSURING direction, and they fail that way for different
/// reasons — lag can sit at 0 because it never latched, the redo queue freezes at its last reading, the
/// <c>*_time</c> columns freeze so a commit delta stops growing exactly when replication stops, and a
/// drain-time ratio holds a healthy-looking constant. The discriminator for anything added later is the
/// failure DIRECTION, not the column: a reading that fails in ONE direction can be gated by this rule; a
/// reading that fails in BOTH under different conditions (see <see cref="JudgeSync"/> on commit deltas)
/// has to be abandoned, because gating it converts a silent failure into a noisy one rather than a correct
/// one.</para>
/// </summary>
public static class AgAlertPolicy
{
    /// <summary>The alert metric names. These are WEBHOOK AUTOMATION KEYS — downstream automation matches on
    /// them — so both apps take them from here rather than spelling them inline, and they must stay stable
    /// across releases. Each is registered in the shared severity map so a renderer that reaches the map
    /// without an explicit override styles it correctly instead of falling through to INFO-blue.</summary>
    public const string FailoverMetric = "AG Failover";

    /// <inheritdoc cref="FailoverMetric"/>
    public const string ReplicaDisconnectedMetric = "AG Replica Disconnected";

    /// <inheritdoc cref="FailoverMetric"/>
    public const string ReplicaReconnectedMetric = "AG Replica Reconnected";

    /// <inheritdoc cref="FailoverMetric"/>
    public const string SyncFellBehindMetric = "AG Sync Fell Behind";

    /// <inheritdoc cref="FailoverMetric"/>
    public const string DatabaseSuspendedMetric = "AG Database Suspended";

    /// <summary>The <c>connected_state_desc</c> value that means the replica is not talking to the primary.
    /// Compared EXACTLY (not "anything that is not CONNECTED") so an unexpected DMV value can never page an
    /// operator for a state the product never learned to interpret.</summary>
    private const string DisconnectedState = "DISCONNECTED";

    /// <summary>Whether a <c>connected_state_desc</c> reading means the replica is not talking to the
    /// primary. See <see cref="DisconnectedState"/> for why an unrecognized value reads as connected.</summary>
    public static bool IsDisconnected(string? connectedStateDesc) =>
        string.Equals(connectedStateDesc, DisconnectedState, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// How good this snapshot's view of <paramref name="agName"/> is (#1696), for choosing ONE monitored
    /// server to judge each Availability Group from.
    ///
    /// <para>Without this, a fully-monitored 3-node AG reports every failover three times — once per node
    /// that can see it. Preferring the PRIMARY's view is not just tie-breaking: measured on a live AG, a
    /// SECONDARY's <c>sys.dm_hadr_*</c> returns only that replica's OWN row, so only the primary actually
    /// sees the whole group. A secondary-only vantage is a one-row self-view.</para>
    ///
    /// <para>A row whose <c>IsLocal</c> is NULL counts as <see cref="AgVantage.Remote"/>, never as "not
    /// local": rows collected before the column existed genuinely do not know, and treating unknown as
    /// not-local would let a real vantage lose to nothing and drop the alert entirely. The safe direction is
    /// to keep judging.</para>
    /// </summary>
    public static AgVantage ClassifyVantage(IReadOnlyList<AgReplicaReading>? snapshot, string agName)
    {
        if (snapshot is null)
        {
            return AgVantage.None;
        }

        var best = AgVantage.None;
        foreach (var replica in snapshot)
        {
            if (!string.Equals(replica.AgName, agName, StringComparison.Ordinal))
            {
                continue;
            }

            if (replica.IsLocal == true)
            {
                if (string.Equals(replica.RoleDesc, "PRIMARY", StringComparison.OrdinalIgnoreCase))
                {
                    return AgVantage.LocalPrimary;
                }

                best = AgVantage.Local;
            }
            else if (best == AgVantage.None)
            {
                best = AgVantage.Remote;
            }
        }

        return best;
    }

    /// <summary>
    /// Whether a replica's role changed since the previous sweep — the "AG Failover" trigger. Any change
    /// counts, not just PRIMARY↔SECONDARY: a move into RESOLVING is a failover in progress and is worth
    /// saying so. Returns false when either role is missing, which covers both the first sighting
    /// (<paramref name="previousRoleDesc"/> null) and a quorum-loss NULL reading.
    /// </summary>
    public static bool IsFailover(string? previousRoleDesc, string? currentRoleDesc) =>
        !string.IsNullOrEmpty(previousRoleDesc)
        && !string.IsNullOrEmpty(currentRoleDesc)
        && !string.Equals(previousRoleDesc, currentRoleDesc, StringComparison.Ordinal);

    /// <summary>
    /// What a <c>connected_state_desc</c> observation should announce. A missing current reading is no
    /// signal, and a missing previous one is the silent first-sighting baseline — including for a replica
    /// that was ALREADY disconnected when monitoring began, whose eventual reconnect still reports so the
    /// operator learns the group is whole again.
    /// </summary>
    public static AgConnectionDecision DecideConnection(string? previousStateDesc, string? currentStateDesc)
    {
        if (string.IsNullOrEmpty(currentStateDesc) || string.IsNullOrEmpty(previousStateDesc))
        {
            return AgConnectionDecision.None;
        }

        bool was = IsDisconnected(previousStateDesc);
        bool now = IsDisconnected(currentStateDesc);

        if (now && !was)
        {
            return AgConnectionDecision.Disconnected;
        }

        return !now && was ? AgConnectionDecision.Reconnected : AgConnectionDecision.None;
    }

    /// <summary>
    /// The same connection decision with the #1696 re-fire folded in: an "AG Replica Disconnected" that
    /// nothing clears is otherwise a PURE EDGE, so a replica down for a week announces itself exactly once
    /// and a week-long outage reads identically to a momentary blip in the alert history.
    ///
    /// <para>Both apps call THIS overload rather than each pairing the edge decision with its own
    /// still-disconnected test — the combination is the part with the sharp corners (a re-fire must not
    /// double up with the edge that just fired, and an unrecognized state string must not count as down),
    /// and it is exactly the kind of condition that drifts when it is written twice.</para>
    ///
    /// <para><b>Where this deliberately departs from rule 1.</b> With re-fire ON, a FIRST sighting of an
    /// already-disconnected replica returns <see cref="AgConnectionDecision.StillDisconnected"/> rather than
    /// the silent baseline rule 1 otherwise mandates. That is on purpose and it is what the knob is for: both
    /// apps hold this edge state in memory, so every restart re-baselines, and under rule 1 alone a restart
    /// in the middle of a week-long outage would silence it permanently — the opt-in would fail precisely in
    /// the case it exists for. It is the AG family's equivalent of
    /// <see cref="ConnectionAlertDecision.AlreadyDownAtFirstSight"/>, folded into the one knob instead of
    /// given a second one, because a re-fire interval already IS the operator saying "keep telling me".
    /// Rule 1 still governs the pure edge: at the shipped default of off, a first sighting is silent.</para>
    /// </summary>
    /// <param name="refireInterval">How often to re-announce a still-disconnected replica. Null or
    /// non-positive = off, which is byte-for-byte the two-argument overload's edge-only behavior.</param>
    /// <param name="lastDisconnectAlertUtc">When this replica's disconnect was last ANNOUNCED, or null if it
    /// never has been. Callers stamp it on every Disconnected / StillDisconnected they DELIVER — never on the
    /// decision, or an alert a master switch suppressed would consume a window it was never announced in —
    /// and clear it on Reconnected so a later outage starts a fresh episode instead of re-firing late.</param>
    /// <param name="nowUtc">The caller's clock, injected so the decision pins under test.</param>
    public static AgConnectionDecision DecideConnection(
        string? previousStateDesc,
        string? currentStateDesc,
        TimeSpan? refireInterval,
        DateTime? lastDisconnectAlertUtc,
        DateTime nowUtc)
    {
        var edge = DecideConnection(previousStateDesc, currentStateDesc);

        /* A real transition wins outright: the Disconnected edge already announces, and a Reconnected one
           means there is nothing left to re-announce. Only the quiet cases can become a re-fire, and only
           when the current reading is one this product recognizes as down. */
        if (edge != AgConnectionDecision.None || !IsDisconnected(currentStateDesc))
        {
            return edge;
        }

        return AlertRefireWindow.IsDue(refireInterval, lastDisconnectAlertUtc, nowUtc)
            ? AgConnectionDecision.StillDisconnected
            : AgConnectionDecision.None;
    }

    /// <summary>
    /// What an <c>is_suspended</c> observation should announce. A NULL current reading is no signal at all —
    /// it must neither fire nor overwrite the remembered state, so a later genuine false→true still reads as
    /// an edge — and a missing previous reading is the silent baseline.
    /// </summary>
    public static AgSuspensionDecision DecideSuspension(bool? previousSuspended, bool? currentSuspended)
    {
        if (currentSuspended is not bool now || previousSuspended is not bool was)
        {
            return AgSuspensionDecision.None;
        }

        if (now && !was)
        {
            return AgSuspensionDecision.Suspended;
        }

        return !now && was ? AgSuspensionDecision.Resumed : AgSuspensionDecision.None;
    }

    /// <summary>
    /// How far behind a secondary is — the "AG Sync Fell Behind" trigger. Two independent triggers, each
    /// disabled by a zero threshold (the shipped default has seconds on at 300 and the redo queue off):
    /// <list type="bullet">
    /// <item><b>Lag seconds</b> — <c>secondary_lag_seconds &gt;= lagThresholdSeconds</c>.</item>
    /// <item><b>Redo queue</b> — <c>redo_queue_size &gt;= redoThresholdKb</c> (KB).</item>
    /// </list>
    /// <para>MS Learn says <c>secondary_lag_seconds</c> "shows as 0 if the data movement is suspended". THE
    /// DOCUMENTATION IS WRONG. Measured on a live clusterless AG, it reports the STALENESS OF THE LAST
    /// HARDENED LOG (roughly <c>now - last_hardened_time</c>), growing at wall-clock rate — not time since
    /// suspension — and the starting value depends on write activity, so under load it starts near 0 and
    /// climbs while on an idle group it can start at a large number. Worse, ON A QUIET GROUP IT MAY NEVER
    /// LATCH AT ALL: sampled every 15 seconds across a 60-second suspend with no writes, it read 0 at every
    /// sample while already NOT SYNCHRONIZING. So a lag threshold ALONE cannot detect suspended data
    /// movement — <see cref="DatabaseSuspendedMetric"/> is the alert that owns that case.</para>
    /// <para>Hence rule 3: a suspended row may fire but may never return <see cref="AgSyncJudgement.CaughtUp"/>.
    /// That is correct whichever way the column behaves — if lag accrues it crosses the threshold and fires;
    /// if it reads a flat 0, that 0 is under the threshold and yields NotMeasurable, so it still cannot
    /// resolve a standing alert. The redo trigger gets the same protection because its value FREEZES while
    /// suspended: frozen and over the threshold is a real backlog, frozen and under it is stale data.</para>
    /// <para>DO NOT ADD a lag derived from commit times. <c>now - last_commit_time</c> fails in BOTH
    /// directions — it stops growing on a suspended row AND grows without bound on a quiet but perfectly
    /// healthy replica (measured at 1757 seconds on a SYNCHRONIZED secondary reporting zero real lag) — so
    /// no directional gate can make it safe.</para>
    /// </summary>
    public static AgSyncJudgement JudgeSync(
        AgDatabaseReading reading, int lagThresholdSeconds, long redoThresholdKb, out string reason)
    {
        bool secondsUsable = lagThresholdSeconds > 0 && reading.SecondaryLagSeconds.HasValue;
        bool redoUsable = redoThresholdKb > 0 && reading.RedoQueueSizeKb.HasValue;

        if (secondsUsable && reading.SecondaryLagSeconds!.Value >= lagThresholdSeconds)
        {
            long lagSeconds = reading.SecondaryLagSeconds!.Value;
            reason = $"Availability Group '{reading.AgName}': database {reading.DatabaseName} on replica " +
                $"{reading.ReplicaServerName} is {lagSeconds.ToString(CultureInfo.InvariantCulture)} seconds behind " +
                $"the primary (threshold {lagThresholdSeconds.ToString(CultureInfo.InvariantCulture)}).";
            return AgSyncJudgement.Behind;
        }

        if (redoUsable && reading.RedoQueueSizeKb!.Value >= redoThresholdKb)
        {
            long redoKb = reading.RedoQueueSizeKb!.Value;
            reason = $"Availability Group '{reading.AgName}': database {reading.DatabaseName} on replica " +
                $"{reading.ReplicaServerName} has a {redoKb.ToString(CultureInfo.InvariantCulture)} KB redo queue " +
                $"(threshold {redoThresholdKb.ToString(CultureInfo.InvariantCulture)} KB) still to be applied.";
            return AgSyncJudgement.Behind;
        }

        reason = "";

        /* Under no threshold. A SUSPENDED row stops here rather than clearing — see rule 3. */
        if (reading.IsSuspended == true)
        {
            return AgSyncJudgement.NotMeasurable;
        }

        return secondsUsable || redoUsable ? AgSyncJudgement.CaughtUp : AgSyncJudgement.NotMeasurable;
    }
}
