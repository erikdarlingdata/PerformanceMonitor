/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The canonical <c>sys.databases.state_desc</c> tokens the database-state alert reasons about, so
/// the engine, both apps' stores, and their override editors reference one spelling each (a typo'd
/// "RECOVERY_PENDING" would silently never match). The alert is baseline-deviation: a database
/// alerts when its current state differs from its expected (auto-seeded baseline or user override)
/// state, so these tokens populate the override editor's dropdown rather than a global checklist.
/// The severity split (<see cref="SeverityFor"/>) is fixed here rather than per-app: a SUSPECT /
/// RECOVERY_PENDING / EMERGENCY database is a genuine integrity problem (CRITICAL), while OFFLINE /
/// RESTORING / RECOVERING are operationally-normal-or-transient (WARNING).
/// </summary>
public static class DatabaseStateTokens
{
    public const string Online = "ONLINE";
    public const string Offline = "OFFLINE";
    public const string Restoring = "RESTORING";
    public const string Recovering = "RECOVERING";
    public const string RecoveryPending = "RECOVERY_PENDING";
    public const string Suspect = "SUSPECT";
    public const string Emergency = "EMERGENCY";

    /// <summary>
    /// A synthetic effective-state token, not a raw <c>sys.databases.state_desc</c>: the stores compose it
    /// for a read-only log-shipping secondary (<c>is_in_standby = 1</c>, which otherwise reports ONLINE but
    /// flips through RESTORING on every log restore), so such a database baselines as a single stable STANDBY
    /// rather than churning. Non-critical.
    /// </summary>
    public const string Standby = "STANDBY";

    /// <summary>
    /// Sentinel <c>expected_state</c> value meaning "never alert on this database's state" — the
    /// user's per-database opt-out. Deliberately not a real <c>state_desc</c> (parenthesised,
    /// lower-case) so it can never collide with a genuine state.
    /// </summary>
    public const string Ignore = "(ignore)";

    /// <summary>The metric name every database-state alert fires under (the AlertSeverity map key).</summary>
    public const string MetricName = "Database State";

    /// <summary>
    /// The integrity-failure states as a SQL IN-list literal, for the arm of both stores' deviation reads
    /// that alerts a database with no baseline yet. Composed from the constants above so the
    /// one-spelling-each rule this class exists for reaches the SQL too, where a typo would not fail to
    /// compile — it would silently never match.
    ///
    /// <para>This list is deliberately NARROWER than <see cref="NeverBaselinedSqlList"/> and must stay that
    /// way. It answers "which states are bad enough to page about with no baseline to compare against",
    /// where the other answers "which states must never be LEARNED as normal". A transient state belongs
    /// only in the second: a database sitting in RESTORING has nothing wrong with it, so widening this list
    /// to match would page for every restore in progress.</para>
    /// </summary>
    public const string CriticalSqlList = "'" + Suspect + "', '" + RecoveryPending + "', '" + Emergency + "'";

    /// <summary>
    /// The effective states a first observation must never be baselined from, as a SQL IN-list literal —
    /// the integrity states plus the transient operational ones (#2189). A database in one of these is
    /// mid-something, not in a steady state anybody would choose as "expected", and learning one inverts
    /// the alert permanently: a database observed mid-restore learned RESTORING as its baseline and then
    /// deviated by being HEALTHY, forever (636 alerts in 24 hours from 5 databases on one fleet).
    ///
    /// <para>A database in one of these states simply stays pending — no row — until it settles into
    /// something the seed will learn. That keeps a NORECOVERY log-shipping secondary permanently quiet
    /// (pending plus a non-critical state alerts nothing) while still covering it for the integrity states
    /// through the pending arm; an operator who wants deviation coverage for one sets the expected state
    /// explicitly, which is an override and therefore honoured over anything inferred.</para>
    ///
    /// <para>Note STANDBY is absent on purpose. It is synthetic and stable by construction — the whole
    /// reason <see cref="Standby"/> exists is to give a log-shipping secondary one steady token instead of
    /// the RESTORING flicker underneath it — so it is exactly the kind of state worth learning.</para>
    /// </summary>
    public const string NeverBaselinedSqlList = CriticalSqlList + ", '" + Restoring + "', '" + Recovering + "'";

    /// <summary>
    /// The fixed severity for a given <c>state_desc</c>: CRITICAL for the integrity-failure states
    /// (SUSPECT / RECOVERY_PENDING / EMERGENCY), WARNING for everything else. Case-insensitive.
    /// </summary>
    public static AlertSeverityLevel SeverityFor(string? stateDesc) =>
        (stateDesc?.Trim().ToUpperInvariant()) switch
        {
            Suspect or RecoveryPending or Emergency => AlertSeverityLevel.Critical,
            _ => AlertSeverityLevel.Warning
        };

    /// <summary>
    /// Whether repetition carries information for this state (#2166).
    ///
    /// <para>OFFLINE and RESTORING are usually states somebody CHOSE — routine maintenance, a soft-delete
    /// park, a log-shipping secondary flickering through restores. Re-firing for weeks tells the operator
    /// nothing they did not already know, and the reporter's case (#2166) generated hundreds of identical
    /// alerts for one intended action. Those get edge semantics: alert on the transition, then stay quiet
    /// until the state changes.
    ///
    /// <para>The integrity states are never chosen — nobody parks a database in SUSPECT — so continued
    /// repetition IS the signal, and they keep re-firing on the cooldown. The engine already applies this
    /// discipline to Server Unreachable/Restored; this extends it to the database states that behave the
    /// same way.</para>
    /// </summary>
    public static bool RepeatsAreNoise(string? stateDesc) =>
        (stateDesc?.Trim().ToUpperInvariant()) switch
        {
            Offline or Restoring or Recovering or Standby => true,
            _ => false
        };

    /// <summary>A human-friendly rendering of a state token (RECOVERY_PENDING → "RECOVERY PENDING").</summary>
    public static string Humanize(string? stateDesc) =>
        string.IsNullOrWhiteSpace(stateDesc) ? "UNKNOWN" : stateDesc.Trim().Replace('_', ' ');
}
