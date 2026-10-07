/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's cached answer to "which retention rollups does this store have?" — the availability input the
/// #1661 tier routing must consult before choosing a relation (#1664). The rollups are runtime TimescaleDB
/// setup, so a plain-PostgreSQL store never has them; routing an old window there by age alone threw 42P01 in
/// front of the user, when raw (which plain PG never drops) held the complete answer all along.
/// </summary>
public sealed partial class ViewerDataService
{
    private RollupAvailability _rollups;
    private RollupCoverage _rollupCoverage = RollupCoverage.Unknown;
    private bool _rollupsProbed;
    private DateTime _rollupProbeAtUtc;

    /// <summary>
    /// Re-probe at most this often — so a service upgrade that creates the continuous aggregates mid-viewer-
    /// session converges without a restart, and (since #1759) so a <c>--backfill-rollups</c> run that moves a
    /// coverage floor is picked up without one either.
    /// </summary>
    private static readonly TimeSpan RollupReprobeInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The store's rollup availability AND each rollup's materialized-coverage floor, probed lazily and
    /// cached (<see cref="RollupReprobeInterval"/>). A failed probe answers
    /// <see cref="RollupAvailability.None"/> + <see cref="RollupCoverage.Unknown"/> — raw always exists, so
    /// "route everything to raw" is the never-wrong availability fallback, and "no coverage evidence" leaves
    /// the age ladder in charge. Benignly racy: concurrent tab loads may probe twice; the probes are two
    /// small lookups and last-write-wins caches the same answer.
    ///
    /// <para><b>The TTL is now unconditional (#1759).</b> It used to be skipped once every rollup existed,
    /// because a created aggregate is never dropped — true of EXISTENCE, false of COVERAGE, which moves
    /// backwards on a backfill and forwards on a retention drop. Keeping the permanent-cache shortcut would
    /// pin a pre-backfill floor for the life of the viewer session.</para>
    /// </summary>
    internal async ValueTask<(RollupAvailability Rollups, RollupCoverage Coverage)> GetRollupAvailabilityAsync(
        CancellationToken cancellationToken)
    {
        if (_rollupsProbed && DateTime.UtcNow - _rollupProbeAtUtc < RollupReprobeInterval)
        {
            return (_rollups, _rollupCoverage);
        }

        try
        {
            _rollups = await TimescaleSupport.DetectRollupsAsync(_dataSource, cancellationToken);
            _rollupCoverage = await TimescaleSupport.DetectRollupCoverageAsync(_dataSource, _rollups, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A store hiccup mid-probe must not take the tab down — raw is the safe answer, and the
               re-probe interval retries soon. */
            _rollups = RollupAvailability.None;
            _rollupCoverage = RollupCoverage.Unknown;
            _ = ex;
        }

        _rollupsProbed = true;
        _rollupProbeAtUtc = DateTime.UtcNow;
        return (_rollups, _rollupCoverage);
    }

    /// <summary>
    /// #5329: whether the hourly route for a window starting at <paramref name="startUtc"/> can read the io hourly
    /// rollup <paramref name="ioView"/> (<c>query_stats_io_hourly</c> / <c>procedure_stats_io_hourly</c>), the
    /// siblings that also keep logical reads, physical reads and logical writes. True only when the store HAS the
    /// view and its first materialized bucket is at or before the window's start: a view that starts later (a
    /// store whose ensure sweep built it recently and has not been backfilled) would leave the early part of the
    /// window out and show a reads total that is too small, so then the route stays on the interval rollup and
    /// its blank reads columns. An absent or empty view is the same answer, with no error.
    /// </summary>
    internal static bool IoHourlyCoversWindow(
        RollupAvailability rollups, RollupCoverage coverage, string ioView, DateTime startUtc)
        => rollups.Has(ioView) && coverage.FloorOf(ioView) is { } floor && floor <= startUtc;

    /// <summary>
    /// #5329: the SQL the hourly arms append for the materialization ceiling: <c>AND bucket &lt; $6</c> when the relation
    /// that answers the window's end has a measured ceiling (<see cref="RollupCoverage.HourlyEndCeiling"/>, the one rule
    /// the service's MCP reads bind too), the empty string when it has none (a null ceiling means no bound). A bucket at
    /// or after the ceiling is never read, so "nothing after the ceiling was read" holds by construction.
    /// </summary>
    internal static string HourlyCeilingSql(DateTime? ceiling) => ceiling is null ? "" : "\n        AND   bucket < $6";

    /// <summary>Binds the ceiling <see cref="HourlyCeilingSql"/> names, as a naive UTC instant, when there is one.</summary>
    internal static void AddHourlyCeilingParameter(NpgsqlCommand command, DateTime? ceiling)
    {
        if (ceiling is not null)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(ceiling.Value, DateTimeKind.Unspecified) });
        }
    }

    /// <summary>
    /// #5329: the window's real edges for an hourly-routed grid, in the words the MCP tools use
    /// (<see cref="HourlyWindowEdges.Note"/>, the text <c>get_top_queries_by_cpu</c> and <c>get_top_procedures_by_cpu</c>
    /// put in their precision note). <paramref name="legacyView"/> is the relation the read was served from (the io
    /// rollup, or the interval one); the floor is <see cref="RollupCoverage.HourlyServedFloor"/> of it and counts only
    /// when it lies after the window's start (a floor at or before the start moves no edge, which the note then
    /// derives from the hour alignment alone, as the service does when its first-bucket probe finds the start).
    /// </summary>
    internal static string? HourlyEdgesNote(
        RollupCoverage coverage, string legacyView, DateTime startUtc, DateTime endUtc, DateTime? ceiling)
    {
        var floor = coverage.HourlyServedFloor(legacyView);
        var firstBucket = floor is { } f && f > startUtc ? f : (DateTime?)null;
        var note = HourlyWindowEdges.Note(startUtc, firstBucket, endUtc, ceiling);
        return string.IsNullOrEmpty(note) ? null : note;
    }

    /// <summary>
    /// #4957: measures each rollup's coverage floor in the background shortly after the Viewer opens its store, so
    /// the first routed read (Overview, Queries, FinOps and the rest all go through
    /// <see cref="GetRollupAvailabilityAsync"/>) does not wait on the cold <c>min(bucket)</c> sort of each rollup's
    /// oldest compressed chunk. The Viewer is its own process, so the service's warm does not reach it. Fire and
    /// forget: <see cref="RollupCoverageWarmup.RunDelayedAsync"/> never throws, and a failed warm changes nothing
    /// here (this method does not touch the Viewer's own cached availability answer).
    /// </summary>
    internal Task WarmRollupCoverageAsync(CancellationToken cancellationToken = default)
        => RollupCoverageWarmup.RunDelayedAsync(_dataSource, logger: null, RollupCoverageWarmup.ViewerStartDelay, cancellationToken);
}

/// <summary>
/// #5329: a routed Top Queries / Top Procedures read: the rows, the tier that answered ("raw" or "hourly"), and, for an
/// hourly read only, the window's real edges (<c>ViewerDataService.HourlyEdgesNote</c>: the text the MCP tools put in
/// their precision note, null when no edge moved).
/// </summary>
public sealed record ViewerRoutedRead<TRow>(List<TRow> Rows, string Tier, string? HourlyEdgesNote);
