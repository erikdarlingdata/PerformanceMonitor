/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The engine-neutral schema surface of a collector definition — everything a storage host needs
/// to create the destination table and address the standard prefix columns, without knowing the
/// definition's row type. <see cref="ICollectorDefinition{TRow}"/> extends this, so
/// <see cref="CollectorCatalog.All"/> can enumerate every definition heterogeneously (Darling
/// generates its Postgres DDL from exactly this surface).
/// </summary>
public interface ICollectorSchemaInfo
{
    /// <summary>Collector name as used in schedules and collection logs (e.g. "wait_stats").</summary>
    string Name { get; }

    /// <summary>
    /// The engine whose dialect this definition's query text is written in. Defaults to
    /// <see cref="CollectorTargetEngine.SqlServer"/> — a default interface implementation rather
    /// than a required member, so the existing definitions and the test doubles that implement this
    /// interface directly need no change. A definition is only dispatched at a target whose
    /// <see cref="CollectorTargetInfo.Engine"/> matches; see
    /// <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>.
    /// </summary>
    CollectorTargetEngine TargetEngine => CollectorTargetEngine.SqlServer;

    /// <summary>Destination table; hosts prepend their standard prefix columns when writing.</summary>
    string TargetTable { get; }

    /// <summary>
    /// Whether the target table's standard prefix includes an id as its first column
    /// (running_jobs is keyed by (collection_time, server) alone and has none).
    /// </summary>
    bool IncludesCollectionId { get; }

    /// <summary>
    /// Name of the prefix id column in the destination schema. "collection_id" almost everywhere;
    /// the XE tables use "deadlock_id"/"blocked_report_id" and the config snapshots "config_id".
    /// Storage is positional in Lite (DuckDB appender), so these names only matter to hosts that
    /// generate DDL or address columns by name (Darling's Postgres store, which mirrors Lite's
    /// names exactly so analysis SQL can twin without translation).
    /// </summary>
    string PrefixIdColumnName { get; }

    /// <summary>
    /// Name of the prefix timestamp column in the destination schema. "collection_time" almost
    /// everywhere; the config snapshots use "capture_time".
    /// </summary>
    string PrefixTimeColumnName { get; }

    /// <summary>Payload columns in exactly the order the definition emits them.</summary>
    IReadOnlyList<CollectorColumn> PayloadColumns { get; }

    /// <summary>
    /// Whether this collector applies to the target at all — the single authoritative target gate
    /// both SKUs share. e.g. memory_pressure_events returns false for Azure SQL DB (no
    /// <c>sys.dm_os_ring_buffers</c>); the SQL-Agent collectors return false without msdb access.
    /// Hosts skip the cycle entirely (no query, zero rows) when false: Darling's runner calls this
    /// directly, and Lite consults it (via <see cref="CollectorCatalog.AppliesTo(string, CollectorTargetInfo)"/>)
    /// for its pre-dispatch SKIPPED log. Declared here — on the non-generic surface
    /// <see cref="CollectorCatalog.All"/> exposes — so the gate can be evaluated by name without the
    /// row type, keeping the gate CONDITION in one place (each definition's override) rather than
    /// re-encoded per host.
    /// </summary>
    bool AppliesTo(CollectorTargetInfo target);

    /// <summary>
    /// True when the collector's own query carries a short <c>SET LOCK_TIMEOUT</c> guard as a
    /// never-be-a-blocker promise, making SQL error 1222 a DELIBERATE yield rather than a
    /// collection failure (#1805). Hosts classify a 1222 from such a collector as a
    /// <c>YIELDED</c> collection_log row — visible, counted separately, and excluded from the
    /// error counts that feed collector health and the daily health band. Declared here — like
    /// <see cref="AppliesTo"/> — so the runners' catch sites can evaluate it by name (via
    /// <see cref="CollectorCatalog.YieldsOnLockTimeout(string)"/>) without the row type. False
    /// everywhere except query_snapshots, the one collector that sets the guard; a 1222 from any
    /// other collector had to come from somewhere unexpected and stays an ERROR.
    /// </summary>
    bool YieldsOnLockTimeout { get; }

    /// <summary>
    /// The host-enforced wall-clock ceiling on ONE item's read + drain (#2673), or null for a collector
    /// with no ceiling. Host-enforced rather than definition-enforced because only the host owns the
    /// cancellation token and the loop, and the point is to bound the definition's own read.
    ///
    /// <para>On the base interface rather than <c>ICollectorDefinition&lt;TRow&gt;</c> (#2864), alongside
    /// <see cref="AppliesTo"/> and <see cref="YieldsOnLockTimeout"/> and for the same reason those are:
    /// the catalog is keyed by NAME and holds this interface, so a host that knows only which collector
    /// ran could not otherwise ask whether that collector is one of the budgeted heavy ones. Having a
    /// budget at all is what distinguishes the four collectors capable of occupying a target for minutes
    /// from the rest of a sweep body, which is a question the worker asks per run and must not answer
    /// from a hardcoded name list - the list that has to be edited whenever a fifth collector earns a
    /// budget is the list that silently stops being right.</para>
    /// </summary>
    TimeSpan? PerItemWallClockBudget { get; }

    /// <summary>
    /// Named pieces of per-server collector state the host loads from its own store before the query is
    /// built (exposed as <see cref="CollectorContext.State"/>) and persists back after the cycle (from
    /// <see cref="CollectorContext.PendingState"/>) — the sibling of
    /// <see cref="ICollectorDefinition{TRow}.WatermarkColumn"/> for state that is NOT derivable from the
    /// collected rows, and so cannot be a MAX() over the target table.
    ///
    /// <para>default_trace_events is the one declaring collector: its last-seen trace FILE path decides
    /// whether the cycle can read only the current rollover file or must re-read the whole set, and a
    /// cycle that collects zero rows must still record the path it saw — exactly the case a row-derived
    /// watermark cannot cover (#1962).</para>
    ///
    /// <para>Empty (the common case) means the host runs no state query at all, so this costs the other
    /// collectors nothing. Keys are declared rather than discovered so the load is a fixed, pinnable set,
    /// and declared HERE — on the non-generic surface <see cref="CollectorCatalog.All"/> exposes — so a
    /// host or a test can enumerate the collectors that carry state without the row type.</para>
    /// </summary>
    IReadOnlyList<string> StateKeys { get; }
}
