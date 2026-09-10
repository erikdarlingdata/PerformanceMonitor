/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The explicit command deadline for the store readers that serve the MCP surface (#2874) — the
/// <c>DarlingPg*Reader</c> family, <c>DarlingPgTrendReader</c> and <c>QueryStoreTrendRouting</c>.
///
/// <para>Before this, all forty-eight of those commands set no <c>CommandTimeout</c> and so inherited
/// Npgsql's undocumented 30 s default — a value nobody chose, and the defect class behind three
/// production failures (#2810, #2871, #2796): exceeding the ceiling fails in a way that looks like a
/// legitimate result.</para>
///
/// <para><b>Why one constant for this family and not the others.</b> These sites share a budget
/// regime: an interactive MCP tool call. Nothing encloses them — no <c>CancelAfter</c> anywhere on
/// the MCP path — and nothing restarts them; a stalled read holds one pooled store connection and
/// hangs the tool until the client gives up. The other <c>.Storage</c> regimes bound themselves with
/// their own deliberate constants instead: the migration session
/// (<c>PgMigrations.MigrationCommandTimeoutSeconds</c>), TimescaleDB setup and catalog bookkeeping
/// (<c>TimescaleSupport.SetupTimeoutSeconds</c> / <c>JobCatalogReadTimeoutSeconds</c>), the startup
/// self-test's per-layer budget, and plan-dim maintenance. A single blanket value across those would
/// have flattened choices that are deliberately different — the same reason #2871 left the
/// drill-down's own 30 s alone.</para>
/// </summary>
public static class StorageCommandDeadlines
{
    /// <summary>
    /// Bounded on both sides, from measurement rather than inheritance.
    ///
    /// <para>ABOVE the measured worst case: the family's read shapes timed against both production
    /// stores put every verified read at 97–685 ms — the 685 ms worst being the unbounded
    /// <c>min(collection_time)</c> class on the largest store's 23 GB hypertable, which TimescaleDB
    /// answers by chunk exclusion. The shipped trend reads are all per-server and time-windowed; a
    /// deliberately HARDER superset (the same aggregate unfiltered, fleet-wide, over seven days)
    /// measured 35.2 s, bounding any single shipped read far below it. 30 s keeps ≥8× headroom over
    /// the most pessimistic estimate and ~44× over anything actually observed.</para>
    ///
    /// <para>BELOW the point where a hung interactive read is worse than a failed one: these calls
    /// hold a pooled store connection with no enclosing budget and no retry cadence, so the deadline
    /// is the only thing standing between one stalled read and a tool that hangs until the client
    /// abandons it. Sixty seconds is where the budgeted analysis pass put a read that a 120 s
    /// <c>CancelAfter</c> would still rescue (#2871); an unbudgeted interactive read must sit
    /// strictly under that, not above it.</para>
    /// </summary>
    public const int McpReadSeconds = 30;
}
