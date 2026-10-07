/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
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
