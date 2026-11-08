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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Assembles the FinOps Server Inventory rows: one row per server the monitor still lists, read live from the server
/// or, when that read fails, built from the newest collected <c>server_properties</c> row the store holds for it, with
/// the collected overlay (average CPU, storage total, idle database count, provisioning status) laid on top.
/// Lifted out of <c>FinOpsTab</c> so the fail-safes below can be pinned without a window.
///
/// <para><b>A server that cannot be reached keeps its place.</b> Its row comes from its newest collected properties and
/// is marked last known with that row's collection time (<see cref="ServerPropertyRow.IsLastKnown"/>), so an inventory
/// of offline servers still lists what was collected. Only a server with neither a live answer nor a collected row is
/// left out, and <see cref="EmptyText"/> says so when that empties the grid.</para>
///
/// <para><b>The overlay is an optional extra and can never cost the grid a row.</b> Its read
/// (<see cref="LocalDataService.GetServerMetricsAsync"/>) and its per-server merge are both guarded here: an
/// exception in either leaves that overlay blank and is logged, and the rows still come back. The merge
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
    /// What the grid says when it is empty. With no server listed, nothing was collected. With servers listed, every one
    /// of them is missing for the one reason a server is left out: its live read failed and the store holds no row for it.
    /// </summary>
    internal static string EmptyText(int listedServers) =>
        listedServers == 0
            ? "No server property data collected yet"
            : $"{CountText.Plural(listedServers, "server")} could not be reached and {(listedServers == 1 ? "has" : "have")} no collected properties yet";

    /// <summary>
    /// The inventory rows for <paramref name="servers"/>, in list order. A server whose live read fails is logged and
    /// built from its collected row, marked last known; with no collected row it is left out. A failing overlay read or
    /// merge is logged and leaves the overlay blank.
    /// </summary>
    /// <param name="servers">The servers the monitor lists. Only these can produce a row.</param>
    /// <param name="readLive">One server's live properties (a real network call to the monitored server).</param>
    /// <param name="readCollected">One server's newest collected <c>server_properties</c> row, or null when the store holds none.</param>
    /// <param name="readCollectedMetrics">The fleet-wide collected overlay, keyed by the storage-name hash id.</param>
    public static async Task<List<ServerPropertyRow>> BuildAsync(
        IEnumerable<ServerConnection> servers,
        Func<ServerConnection, Task<ServerPropertyRow>> readLive,
        Func<ServerConnection, Task<ServerPropertiesRow?>> readCollected,
        Func<Task<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>> readCollectedMetrics)
    {
        var fleetMetrics = await ReadCollectedMetricsAsync(readCollectedMetrics);

        var tasks = servers.Select(async server =>
        {
            ServerPropertyRow? item;
            try
            {
                item = await readLive(server);
            }
            catch (Exception ex)
            {
                AppLogger.Error("FinOps", $"Failed to query {server.DisplayName}: {ex.Message}", ex);
                item = await ReadLastKnownAsync(server, readCollected);
                if (item == null) return null;
            }

            item.ServerName = server.DisplayName;
            item.MonthlyCost = server.MonthlyCostUsd;

            /* Step 2: lay this server's collected metrics from the fleet-wide read on top. Guarded on its
               own (see ApplyCollectedMetrics), so it cannot cost the row above its place. */
            ApplyCollectedMetrics(item, server, fleetMetrics);

            return item;
        });

        var results = await Task.WhenAll(tasks);
        return results.Where(r => r != null).Cast<ServerPropertyRow>().ToList();
    }

    /// <summary>
    /// The last-known row for a server whose live read failed: its newest collected properties, or null when the store
    /// holds none or the read throws. Either way the server is left out, and the log says which; the throw never
    /// propagates, so it costs the grid no other row.
    /// </summary>
    internal static async Task<ServerPropertyRow?> ReadLastKnownAsync(
        ServerConnection server,
        Func<ServerConnection, Task<ServerPropertiesRow?>> readCollected)
    {
        try
        {
            var collected = await readCollected(server);
            if (collected == null)
            {
                AppLogger.Warn("FinOps", $"{server.DisplayName} has no collected server properties to show in its place, so it is left out of the Server Inventory.");
                return null;
            }
            return FromCollected(collected);
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to read the collected server properties for {server.DisplayName}, so it is left out of the Server Inventory: {ex.Message}", ex);
            return null;
        }
    }

    /// <summary>
    /// A last-known inventory row from a collected <c>server_properties</c> row. The version reads as a live row's does
    /// (the product version with its update level, else its level). The collector stores no host OS, start time or AG
    /// replica role for the inventory, so those cells stay blank, and a 0 socket or core count is a value the store never
    /// had, so it reads as blank too. Last Updated is the row's collection time on the machine's clock, the clock a live
    /// row's read time is shown on.
    /// </summary>
    internal static ServerPropertyRow FromCollected(ServerPropertiesRow collected)
    {
        var level = string.IsNullOrEmpty(collected.ProductUpdateLevel) ? collected.ProductLevel : collected.ProductUpdateLevel;
        return new ServerPropertyRow
        {
            Edition = collected.Edition,
            ProductVersion = string.IsNullOrEmpty(collected.ProductVersion) || string.IsNullOrEmpty(level)
                ? collected.ProductVersion
                : $"{collected.ProductVersion} - {level}",
            ProductLevel = collected.ProductLevel,
            ProductUpdateLevel = collected.ProductUpdateLevel,
            EngineEdition = collected.EngineEdition,
            CpuCount = collected.CpuCount,
            PhysicalMemoryMb = collected.PhysicalMemoryMb,
            SocketCount = collected.SocketCount == 0 ? null : collected.SocketCount,
            CoresPerSocket = collected.CoresPerSocket == 0 ? null : collected.CoresPerSocket,
            IsHadrEnabled = collected.IsHadrEnabled,
            IsClustered = collected.IsClustered,
            AgReplicaRole = "",
            IsLastKnown = true,
            LastUpdated = DateTime.SpecifyKind(collected.CollectionTime, DateTimeKind.Utc).ToLocalTime(),
        };
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
            AppLogger.Error("FinOps", $"Failed to read collected server metrics; the Server Inventory overlay stays blank: {ex.Message}", ex);
            return new Dictionary<int, LocalDataService.ServerMetricsRow>();
        }
    }

    /// <summary>
    /// Lays one server's collected metrics onto its row. A server with no entry keeps the row as it is. Every lookup
    /// finishes before the first assignment, so a failure leaves the overlay blank rather than half applied; it is
    /// logged and never propagates.
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
            if (row.HealthScore.HasValue) item.HealthScore = row.HealthScore;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to overlay collected metrics for {server.DisplayName}: {ex.Message}", ex);
        }
    }
}
