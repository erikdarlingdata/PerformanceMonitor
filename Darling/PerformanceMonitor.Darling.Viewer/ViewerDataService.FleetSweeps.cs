/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The reads the Fleet Sweeps tab makes (#6390), the same ones the web's sweeps page makes: the same
/// <see cref="FleetSweepStore"/> reads and the same <see cref="FleetSweepPresentation"/> builders the web's
/// <c>/api/sweeps</c> routes and <c>get_sweep_reports</c> call, so the Viewer and the web show the same rows.
/// Read only: the tab never writes, and the fleet sweep tables are the service's alone. Each method returns the
/// builder's JSON node as the web serves it; the tab reads its fields (tick-scale sweep ids ride as strings, as on
/// the web, because they are past the range a double holds exactly).
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The timeline the web's <c>GET /api/sweeps</c> serves: the runs swept inside the span, newest first, each with its
    /// report embedded under <c>sweeps</c>, and the span echoed back. Both bounds are UTC.</summary>
    public async Task<JsonObject> GetFleetSweepTimelineAsync(
        DateTime spanStartUtc, DateTime spanEndUtc, CancellationToken cancellationToken = default)
    {
        var runs = await FleetSweepStore.GetSweepsBySpanAsync(_dataSource, spanStartUtc, spanEndUtc, cancellationToken);
        return FleetSweepPresentation.BuildTimelineNode(runs, spanStartUtc, spanEndUtc);
    }

    /// <summary>One sweep in full, as <c>GET /api/sweeps/{id}</c> serves it, or the newest sweep when <paramref name="sweepId"/> is
    /// null (<c>GET /api/sweeps/latest</c>). Null means the store holds no such sweep (none recorded yet, or pruned by retention):
    /// absence, which the tab words as the web page does. A store fault throws, so it can never read as absence.</summary>
    public async Task<JsonObject?> GetFleetSweepDetailAsync(long? sweepId, CancellationToken cancellationToken = default)
    {
        var run = sweepId is { } id
            ? await FleetSweepStore.GetSweepAsync(_dataSource, id, cancellationToken)
            : await FleetSweepStore.GetLatestSweepAsync(_dataSource, cancellationToken);
        if (run is null)
        {
            return null;
        }

        var verdicts = await FleetSweepStore.GetServerVerdictsAsync(_dataSource, run.SweepId, cancellationToken);

        /* The ledger is read only for a master-off sweep: the engine writes none otherwise, and the builder omits the key then. */
        var ledger = run.AlertsEnabled
            ? new List<FleetSweepWouldHavePagedEntry>()
            : await FleetSweepStore.GetWouldHavePagedAsync(_dataSource, run.SweepId, cancellationToken);

        return FleetSweepPresentation.BuildSweepDetailNode(run, verdicts, ledger);
    }

    /// <summary>The watch-item worklist, as <c>GET /api/sweeps/watch-items</c> serves it: open + carried when
    /// <paramref name="state"/> is null, else the named state (pending, open, carried, closed). Server names join through the
    /// store's own gate, so a failed names read costs the names and never the worklist.</summary>
    public async Task<JsonObject> GetFleetSweepWatchItemsAsync(string? state, CancellationToken cancellationToken = default)
    {
        var items = state is null
            ? await FleetSweepStore.GetOpenAndCarriedWatchItemsAsync(_dataSource, cancellationToken)
            : await FleetSweepStore.GetWatchItemsByStateAsync(_dataSource, state, cancellationToken);

        var names = await FleetSweepStore.GetWatchItemNamesAsync(_dataSource, items, logger: null, cancellationToken);
        return FleetSweepPresentation.BuildWatchItemsNode(items, names);
    }
}
