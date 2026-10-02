/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Assembles the FinOps Server Inventory rows: one live row per server the monitor still lists, with the
/// collected overlay (average CPU, storage total, idle database count, provisioning status) laid on top.
/// Lifted out of <c>FinOpsTab</c> so the fail-safe below can be pinned without a window.
///
/// <para><b>The overlay is an optional extra and can never cost the grid a row.</b> Its read
/// (<see cref="LocalDataService.GetServerMetricsAsync"/>) and its per-server merge are both guarded here: an
/// exception in either leaves that overlay blank and is logged, and the live rows still come back. The merge
/// used to sit inside the per-server live-query catch, where any throw dropped the whole server from the
/// inventory, and the read's catch swallowed its exception without a log line.</para>
///
/// <para><b>Servers come from the monitor's own list, not from the collected data.</b> The collected set
/// also holds a server removed from the list until retention purges its rows; looking each listed server up
/// by id means those entries are never read and a removed server never appears in the inventory.</para>
/// </summary>
internal static class FinOpsServerInventory
{
    /// <summary>
    /// The inventory rows for <paramref name="servers"/>, in list order. A server whose live read fails is
    /// logged and left out, as before; a failing overlay read or merge is logged and leaves the overlay blank.
    /// </summary>
    /// <param name="servers">The servers the monitor lists. Only these can produce a row.</param>
    /// <param name="readLive">One server's live properties (a real network call to the monitored server).</param>
    /// <param name="readCollectedMetrics">The fleet-wide collected overlay, keyed by the storage-name hash id.</param>
    public static async Task<List<ServerPropertyRow>> BuildAsync(
        IEnumerable<ServerConnection> servers,
        Func<ServerConnection, Task<ServerPropertyRow>> readLive,
        Func<Task<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>> readCollectedMetrics)
    {
        var fleetMetrics = await ReadCollectedMetricsAsync(readCollectedMetrics);

        var tasks = servers.Select(async server =>
        {
            try
            {
                var item = await readLive(server);
                item.ServerName = server.DisplayName;
                item.MonthlyCost = server.MonthlyCostUsd;

                /* Step 2: lay this server's collected metrics from the fleet-wide read on top. Guarded on its
                   own (see ApplyCollectedMetrics), so it cannot turn the live row above into the null below. */
                ApplyCollectedMetrics(item, server, fleetMetrics);

                return item;
            }
            catch (Exception ex)
            {
                AppLogger.Error("FinOps", $"Failed to query {server.DisplayName}: {ex.Message}");
                return (ServerPropertyRow?)null;
            }
        });

        var results = await Task.WhenAll(tasks);
        return results.Where(r => r != null).Cast<ServerPropertyRow>().ToList();
    }

    /// <summary>
    /// The fleet-wide collected overlay, or an empty one when the read throws (an unreadable or
    /// not-yet-populated store). The throw is logged, never swallowed silently and never propagated.
    /// </summary>
    internal static async Task<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>> ReadCollectedMetricsAsync(
        Func<Task<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>> readCollectedMetrics)
    {
        try
        {
            return await readCollectedMetrics();
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to read collected server metrics; the Server Inventory overlay stays blank: {ex.Message}");
            return new Dictionary<int, LocalDataService.ServerMetricsRow>();
        }
    }

    /// <summary>
    /// Lays one server's collected metrics onto its live row. A server with no entry keeps the live row as
    /// it is. Every lookup finishes before the first assignment, so a failure leaves the overlay blank
    /// rather than half applied; it is logged and never propagates.
    /// </summary>
    internal static void ApplyCollectedMetrics(
        ServerPropertyRow item,
        ServerConnection server,
        IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow> fleetMetrics)
    {
        try
        {
            var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
            if (!fleetMetrics.TryGetValue(serverId, out var row)) return;

            if (row.AvgCpuPct.HasValue) item.AvgCpuPct = row.AvgCpuPct;
            if (row.StorageTotalGb.HasValue) item.StorageTotalGb = row.StorageTotalGb;
            if (row.IdleDbCount.HasValue) item.IdleDbCount = row.IdleDbCount;
            if (row.ProvisioningStatus != null) item.ProvisioningStatus = row.ProvisioningStatus;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to overlay collected metrics for {server.DisplayName}: {ex.Message}");
        }
    }
}
