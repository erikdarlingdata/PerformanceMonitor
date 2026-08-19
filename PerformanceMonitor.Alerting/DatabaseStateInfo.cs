/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Alerting;

/// <summary>
/// A database whose collected state deviates from its expected (baseline/override) state in the two
/// most recent collections — the unit the database-state alert fires on. The read adapter returns only
/// the deviating rows (current != expected in both samples, expected not the "ignore" sentinel), having
/// already auto-seeded a first-observation baseline for any database it has never seen in a HEALTHY
/// (non-critical) state; a critical first observation is left pending and alerts. The engine keys
/// per-database dedup/resolution off <see cref="DatabaseName"/> and grades severity off
/// <see cref="StateDesc"/> (the current effective state).
/// </summary>
public sealed class DatabaseStateInfo
{
    public string DatabaseName { get; set; } = "";

    /// <summary>The current <c>sys.databases.state_desc</c> token, e.g. OFFLINE / RESTORING / SUSPECT.</summary>
    public string StateDesc { get; set; } = "";

    /// <summary>
    /// The expected state this database is compared against — its auto-seeded first-observation
    /// baseline, or the user's override. Shown in the alert so an operator sees the transition
    /// (e.g. "ONLINE → OFFLINE").
    /// </summary>
    public string ExpectedState { get; set; } = "";

    /// <summary>
    /// The effective state this database was LAST alerted about (#2166), or empty when it has never been
    /// alerted. Persisted per (server, database) so it survives a service restart — without that, an
    /// edge-triggered alert would re-fire every deliberately-parked database on every restart, which is
    /// worse than the cooldown-repeat it replaces.
    ///
    /// <para>The engine compares this against <see cref="StateDesc"/> for the states where repetition is
    /// noise rather than signal (a chosen OFFLINE, a flickering RESTORING). The integrity states are
    /// never chosen, so they keep re-firing on the cooldown and ignore this field.</para>
    /// </summary>
    public string LastAlertedState { get; set; } = "";
}
