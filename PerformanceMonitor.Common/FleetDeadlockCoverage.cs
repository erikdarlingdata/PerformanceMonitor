/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Common;


/// <summary>
/// Whether one server's deadlock count read a deadlock source at all, and when it did not, which cause
/// (#3017). Serialized as its STRING name (the fleet DTOs' <c>JsonStringEnumConverter</c>), so a consumer
/// switches on a word rather than on an ordinal that a reordering would move underneath it.
///
/// <para>The three uncovered arms exist as three rather than as one flag because they take OPPOSITE actions,
/// which is the whole reason a bare tally would not have been enough.</para>
/// </summary>
public enum FleetDeadlockSource
{
    /// <summary>The <c>deadlocks</c> collector read this server over the health window, so this server's
    /// deadlocks — however many that is, including none — are in the total. Its band may still be FAILING,
    /// STALE or WARNING: those collectors succeeded on some cycles, and their rows are in the total.</summary>
    Read,

    /// <summary>A PostgreSQL target, whose deadlocks the total cannot count at all: they are stored in
    /// <c>pg_deadlocks</c>, which nothing joins into <c>v_deadlocks</c>. Nothing about this server's health
    /// changes it and no grant addresses it — <c>get_pg_deadlocks</c> is the read that answers.</summary>
    PostgresTarget,

    /// <summary>The <c>deadlocks</c> collector is not being invoked — STOPPED, NEVER_RUN, or no row in the
    /// health window at all. Silence, not failure: a collector still running and erroring every cycle bands
    /// FAILING and counts as <see cref="Read"/>. Check the collector (<c>get_collection_health</c>).</summary>
    CollectorSilent,

    /// <summary>Every one of the <c>deadlocks</c> collector's attempts was refused for permissions
    /// (NO_PERMISSIONS). The one cause a grant fixes, which is why it is not folded in with
    /// <see cref="CollectorSilent"/> despite both meaning nothing was read.</summary>
    CollectorDenied,
}

/// <summary>
/// The denominator beside <c>total_deadlocks</c> (#3017): how many of the fleet's
/// servers that total actually read a deadlock source for, and for the rest, which of the three causes it
/// was.
///
/// <para><b>Why a total needs one at all.</b> <c>v_deadlocks</c> is the SQL Server extended-event capture and
/// only that, so <c>total_deadlocks</c> is structurally zero for a PostgreSQL server — permanently, whatever
/// its clusters do and whatever grants the monitoring role holds. Zero is also exactly what a genuinely quiet
/// SQL Server fleet reports. The reading that needs no action and the reading that does not cover the fleet
/// are the same character, and that is what makes the bare number unusable.</para>
///
/// <para><b>The convention this follows.</b> <c>get_pg_blocking</c> already reports <c>captures_total</c>
/// beside <c>captures_with_blocking</c> and a sentence saying what an empty answer does and does not mean,
/// because the denominator is the honest part of a sampled signal; <c>target_has_user_databases</c> (#1852)
/// already states a fact about the TARGET beside a persistently empty result rather than banding it, so
/// "nothing to collect" stops looking like "collecting nothing". This is the same move on a fleet total: a
/// count, a denominator, named causes, and a note — and deliberately not a new band, for #1852's reason. A
/// quiet SQL Server fleet with full coverage is healthy and must keep reading that way.</para>
///
/// <para><b>Why this sits in Common rather than beside either reader.</b> TWO surfaces compute this same
/// fleet total from the same <c>v_deadlocks</c>, out of two assemblies that do not reference each other —
/// the service's <c>get_fleet_overview</c> / <c>/api/fleet</c> reader and the WPF viewer's Overview
/// roll-up (#3029). The buckets, the enum they reduce from and
/// <see cref="ClassifyDeadlockSource"/> therefore live once, here: a cause added to
/// <see cref="FleetDeadlockSource"/> gets ONE decision instead of one per surface, and the surface that
/// was not updated would have mis-bucketed silently. What each surface keeps for itself is the PROSE: the
/// <see cref="Note"/> below names JSON fields and MCP tools because its reader is an API consumer, and a
/// desktop tile naming <c>window_start</c> would be describing a payload nobody there can see.</para>
/// </summary>
public sealed class FleetDeadlockCoverage
{
    /// <summary>Servers the total read a deadlock source for — the numerator of the coverage this object
    /// reports, and NOT a count of servers that had deadlocks (that is
    /// <c>total_deadlocks</c>'s job).</summary>
    [JsonPropertyName("servers_read")] public int ServersRead { get; init; }

    /// <summary>Enabled servers in the fleet — the same population as
    /// <c>total_servers</c>, repeated here so the denominator travels with its
    /// numerator and a consumer reading only this object is never one field short of the ratio.</summary>
    [JsonPropertyName("servers_total")] public int ServersTotal { get; init; }

    /// <summary>Uncovered because the target is PostgreSQL — <c>get_pg_deadlocks</c> is the read that
    /// answers for these.</summary>
    [JsonPropertyName("postgres_servers")] public int PostgresServers { get; init; }

    /// <summary>Uncovered because the <c>deadlocks</c> collector is not being invoked (STOPPED, NEVER_RUN,
    /// or no row in the health window) — check the collector.</summary>
    [JsonPropertyName("servers_collector_silent")] public int ServersCollectorSilent { get; init; }

    /// <summary>Uncovered because every one of the <c>deadlocks</c> collector's attempts was refused for
    /// permissions — these need a grant.</summary>
    [JsonPropertyName("servers_collector_denied")] public int ServersCollectorDenied { get; init; }

    /// <summary>
    /// The sentence that keeps the two figures from being read as one measurement.
    ///
    /// <para><b>The windows genuinely diverge and this is the whole point of the field.</b>
    /// <c>total_deadlocks</c> is counted between the caller's
    /// <c>window_start</c> and <c>window_end</c> (one hour by default); coverage is banded over the FIXED
    /// trailing seven days of collection health. That divergence is correct — whether a reader works is a
    /// durable fact, and the banding thresholds are themselves defined in DAYS, so an hour-wide health
    /// window could not produce a band at all — but a note claiming both were "read in this window" would be
    /// the same defect one level up: a surface asserting a scope it did not measure. So the sentence names
    /// each window against the figure it belongs to, and says outright that coverage makes no claim about
    /// the caller's window.</para>
    /// </summary>
    public const string WindowNote =
        "Coverage bands each server's deadlock reader over the fixed trailing seven days of collection "
        + "health - whether the reader works at all, which is a durable fact - while total_deadlocks counts "
        + "only between window_start and window_end. The two windows differ deliberately, and this coverage "
        + "figure therefore makes no claim about what was read inside window_start..window_end.";

    /// <summary>What to do about the PostgreSQL arm, appended after its count.</summary>
    public const string PostgresCause =
        "are PostgreSQL targets, whose deadlocks this total cannot count at all - they are stored separately "
        + "and served by get_pg_deadlocks.";

    /// <summary>What to do about the silent arm, appended after its count.</summary>
    public const string CollectorSilentCause =
        "have no current deadlock collection - the collector has stopped being invoked, or has never run; "
        + "check it with get_collection_health.";

    /// <summary>What to do about the denied arm, appended after its count.</summary>
    public const string CollectorDeniedCause =
        "had every deadlock-collector attempt refused for permissions - those need a grant.";

    /// <summary>
    /// The coverage in a sentence, with <see cref="WindowNote"/> and only the causes that actually apply.
    ///
    /// <para>DERIVED rather than assigned, for the reason each surface's own per-server <c>deadlock_source</c> is:
    /// a settable note is a note that can be omitted, or can drift from the counts it describes. Computed,
    /// the numbers and the sentence cannot disagree.</para>
    /// </summary>
    [JsonPropertyName("note")]
    public string Note
    {
        get
        {
            var note = new StringBuilder();

            note.Append("total_deadlocks read a deadlock source for ")
                .Append(ServersRead)
                .Append(" of ")
                .Append(ServersTotal)
                .Append(" enabled servers. ")
                .Append(WindowNote);

            if (PostgresServers > 0)
            {
                note.Append(' ').Append(PostgresServers).Append(' ').Append(PostgresCause);
            }

            if (ServersCollectorSilent > 0)
            {
                note.Append(' ').Append(ServersCollectorSilent).Append(' ').Append(CollectorSilentCause);
            }

            if (ServersCollectorDenied > 0)
            {
                note.Append(' ').Append(ServersCollectorDenied).Append(' ').Append(CollectorDeniedCause);
            }

            return note.ToString();
        }
    }

    /// <summary>
    /// Whether one server's deadlock count — and so the fleet total it sums into — actually read a deadlock
    /// source, and when it did not, which of the three causes it was (#3017). The three take OPPOSITE
    /// actions, which is why one uncovered tally would not have been enough: a different tool for a
    /// PostgreSQL target, a collector to look at for one that is not being invoked, a grant for one that was
    /// refused.
    ///
    /// <para><b>PostgreSQL is asked first and answers on its own.</b> It is not a degraded collector, it is
    /// the wrong table: every fleet deadlock total reads <c>v_deadlocks</c>, which holds only the SQL
    /// Server extended-event capture, and no health band on any collector changes that. A PostgreSQL target
    /// whose <c>pg_deadlocks</c> collector is perfectly healthy still contributes nothing here.</para>
    ///
    /// <para><b>A null band is uncovered, not covered.</b> Null means the <c>deadlocks</c> collector left no
    /// row in the health window — nothing was read for this server, whatever else is true of it — and
    /// defaulting an absent band to "read" is precisely how a coverage figure becomes another number nobody
    /// can trust. It is folded in with STOPPED and NEVER_RUN because all three take the same action.</para>
    ///
    /// <para><b>FAILING, STALE and WARNING count as READ, deliberately.</b> Those collectors succeeded on
    /// some cycles, so their rows really are in the total and a denominator excluding them would understate
    /// the fleet — a new wrong number in place of the old one. They are not hidden by being counted here:
    /// <c>servers_with_collection_failures</c> and each card's
    /// <c>failed_collector_count</c> are where a degraded collector shows, and the
    /// card's own <c>deadlock_collector_band</c> carries the band verbatim.</para>
    /// </summary>
    public static FleetDeadlockSource ClassifyDeadlockSource(bool isPostgres, string? deadlockCollectorBand)
    {
        if (isPostgres)
        {
            return FleetDeadlockSource.PostgresTarget;
        }

        /* Tested ahead of the general set below rather than as part of it: NO_PERMISSIONS is IN
           NothingReadBands, so asking the set first would swallow the one cause with a different fix. */
        if (string.Equals(deadlockCollectorBand, CollectorHealthClassifier.NoPermissions, StringComparison.Ordinal))
        {
            return FleetDeadlockSource.CollectorDenied;
        }

        return deadlockCollectorBand is null || CollectorHealthClassifier.ReadNothing(deadlockCollectorBand)
            ? FleetDeadlockSource.CollectorSilent
            : FleetDeadlockSource.Read;
    }
}
