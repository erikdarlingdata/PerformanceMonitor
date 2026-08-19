/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Freeze headroom for one database. <paramref name="AutovacuumFreezeMaxAge"/> travels with the row
/// because the threshold that matters is the SERVER's own setting, not a constant: a cluster tuned to
/// 1.5 billion is in a very different place at an age of 400 million than a stock one at 200 million.
/// </summary>
/// <param name="DatabaseName">The database this headroom belongs to.</param>
/// <param name="XidAge">Age of the oldest unfrozen XID (datfrozenxid).</param>
/// <param name="MultiXactAge">Age of the oldest unfrozen MultiXact (datminmxid).</param>
/// <param name="AutovacuumFreezeMaxAge">The server's autovacuum_freeze_max_age — the age at which
/// autovacuum force-starts a wraparound-prevention vacuum whether or not the table is otherwise due.</param>
public sealed record PostgresWraparoundAlertInfo(
    string DatabaseName,
    long XidAge,
    long MultiXactAge,
    long AutovacuumFreezeMaxAge,
    long AutovacuumMultixactFreezeMaxAge)
{
    /* WorstAge/WorstCounter used to pick by raw age and the evaluator graded the winner against
       autovacuum_freeze_max_age whichever counter it was. That is wrong: the two counters have DIFFERENT
       governing settings — 200,000,000 versus 400,000,000 by default — so a MultiXact age was graded against
       a threshold half the size of its own and warned 2.2x premature, while the alert body printed
       "autovacuum_freeze_max_age N" next to a WorstCounter saying MultiXact. Each counter is now graded
       against its own setting and the worse RELATIVE breach wins, which is the only comparison that means
       anything when the denominators differ. */

    /// <summary>How far this database's XID age has gone toward its own freeze threshold, as a fraction.</summary>
    public double XidFractionOfSetting =>
        AutovacuumFreezeMaxAge > 0 ? (double)XidAge / AutovacuumFreezeMaxAge : 0;

    /// <summary>The same for MultiXacts, against <c>autovacuum_multixact_freeze_max_age</c>.</summary>
    public double MultiXactFractionOfSetting =>
        AutovacuumMultixactFreezeMaxAge > 0 ? (double)MultiXactAge / AutovacuumMultixactFreezeMaxAge : 0;

    /// <summary>Which counter is in the worse position RELATIVE to its own governing setting.</summary>
    public bool MultiXactIsWorse => MultiXactFractionOfSetting > XidFractionOfSetting;

    /// <summary>The age of whichever counter is relatively worse.</summary>
    public long WorstAge => MultiXactIsWorse ? MultiXactAge : XidAge;

    /// <summary>Which counter that is, so the message names the right remedy.</summary>
    public string WorstCounter => MultiXactIsWorse ? "MultiXact" : "XID";

    /// <summary>The setting that governs the relatively-worse counter — the one the message must quote.</summary>
    public long WorstSetting => MultiXactIsWorse ? AutovacuumMultixactFreezeMaxAge : AutovacuumFreezeMaxAge;

    /// <summary>The name of that setting, so the body cannot contradict itself.</summary>
    public string WorstSettingName =>
        MultiXactIsWorse ? "autovacuum_multixact_freeze_max_age" : "autovacuum_freeze_max_age";
}

/// <summary>
/// The current xmin-horizon holder, with how persistent it has been.
/// </summary>
/// <param name="Source">Which of the four causes wins — session, replication_slot,
/// replication_slot_catalog, standby_feedback or prepared_transaction. They are indistinguishable by
/// symptom and need completely different fixes, so the alert must name it.</param>
/// <param name="Identifier">The specific holder (pid, slot name, gid, replica).</param>
/// <param name="XminAge">How far behind the horizon this holder is holding, in transactions.</param>
/// <param name="ObservationsHeld">How many collections in the window showed this source winning — the
/// chronic-versus-transient discriminator.</param>
/// <param name="ObservationsTotal">Collections in the window, so a caller can read the ratio.</param>
/// <param name="Detail">Free-text state the collector captured (e.g. "state=idle in transaction").</param>
public sealed record PostgresXminHorizonAlertInfo(
    string Source,
    string? Identifier,
    long XminAge,
    int ObservationsHeld,
    int ObservationsTotal,
    string? Detail);

/// <summary>
/// One replication slot's retention risk.
/// </summary>
/// <param name="SlotName">The slot.</param>
/// <param name="WalStatus">reserved / extended / unreserved / lost — the single most diagnostic column.</param>
/// <param name="IsActive">Whether anything is currently consuming it.</param>
/// <param name="RetainedWalBytes">WAL held because of this slot.</param>
/// <param name="RetainedWalGrowthBytes">Change across the window. Growth is what turns a large figure into
/// an emergency; a flat figure is a consumer that is behind but keeping pace.</param>
/// <param name="InactiveSince">When it went quiet, when the server reports it (PG17+).</param>
public sealed record PostgresSlotAlertInfo(
    string SlotName,
    string? WalStatus,
    bool IsActive,
    long RetainedWalBytes,
    long RetainedWalGrowthBytes,
    DateTime? InactiveSince);
