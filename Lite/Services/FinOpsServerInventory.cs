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
using PerformanceMonitor.Analysis.Baselines;
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
/// exception in either leaves that overlay blank and is logged, and the rows still come back.</para>
///
/// <para><b>Servers come from the monitor's own list, not from the collected data.</b> The collected set
/// also holds a server removed from the list until retention purges its rows; looking each listed server up
/// by id means those entries are never read and a removed server never appears in the inventory.</para>
/// </summary>
internal static class FinOpsServerInventory
{
    /// <summary>
    /// What the grid says when it is empty. With no server listed, nothing was collected. With servers listed, every one
    /// of them is missing because its live read failed and no collected row stood in: the store holds none for it, or, for
    /// <paramref name="unreadableServers"/> of them, the collected read itself threw, which the sentence says rather than
    /// claiming nothing was collected.
    /// </summary>
    internal static string EmptyText(int listedServers, int unreadableServers)
    {
        if (listedServers == 0) return "No server property data collected yet";

        var servers = CountText.Plural(listedServers, "server");
        if (unreadableServers == 0)
            return $"{servers} could not be reached and {(listedServers == 1 ? "has" : "have")} no collected properties yet";
        if (unreadableServers >= listedServers)
            return $"{servers} could not be reached and {(listedServers == 1 ? "its" : "their")} collected properties could not be read";

        var none = listedServers - unreadableServers;
        return $"{servers} could not be reached: the collected properties of {unreadableServers} could not be read and {none} {(none == 1 ? "has" : "have")} none yet";
    }

    /// <summary>What <see cref="BuildAsync"/> produced: the rows, and the counts behind the sentence an empty grid shows.</summary>
    /// <param name="Rows">One row per server that answered live or had a collected row, in list order.</param>
    /// <param name="ListedServers">The servers the monitor lists.</param>
    /// <param name="UnreadableServers">The servers left out because their collected read threw after the live read failed.</param>
    internal sealed record InventoryBuild(List<ServerPropertyRow> Rows, int ListedServers, int UnreadableServers)
    {
        /// <summary>The sentence under the grid when it has no row, or null when it has rows.</summary>
        public string? EmptyText => Rows.Count == 0 ? FinOpsServerInventory.EmptyText(ListedServers, UnreadableServers) : null;
    }

    /// <summary>
    /// The inventory rows for <paramref name="servers"/>, in list order, with the counts behind the empty sentence. A
    /// server whose live read fails is logged and built from its collected row, marked last known; with no collected row
    /// it is left out, and so is one whose collected read throws, which the result counts. A failing overlay read or
    /// merge is logged and leaves the overlay blank.
    /// </summary>
    /// <param name="servers">The servers the monitor lists. Only these can produce a row.</param>
    /// <param name="readLive">One server's live properties (a real network call to the monitored server).</param>
    /// <param name="readCollected">One server's newest collected <c>server_properties</c> row, or null when the store holds none.</param>
    /// <param name="readCollectedMetrics">The fleet-wide collected overlay, keyed by the storage-name hash id.</param>
    /// <param name="readClock">One server's own clock, the one its Last Updated cell reads in Server Time. Null leaves a live row
    /// without one (it reads the active tab's) and a last-known row with the clock its collected row recorded.</param>
    public static async Task<InventoryBuild> BuildAsync(
        IEnumerable<ServerConnection> servers,
        Func<ServerConnection, Task<ServerPropertyRow>> readLive,
        Func<ServerConnection, Task<ServerPropertiesRow?>> readCollected,
        Func<Task<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>> readCollectedMetrics,
        Func<ServerConnection, Task<ServerClock>>? readClock = null)
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
                var (lastKnown, readFailed) = await ReadLastKnownAsync(server, readCollected);
                if (lastKnown == null) return (Row: (ServerPropertyRow?)null, ReadFailed: readFailed);
                item = lastKnown;
            }

            item.ServerName = server.DisplayName;
            item.MonthlyCost = server.MonthlyCostUsd;

            /* The row's own server's clock, guarded on its own (see StampClockAsync), so a failed read costs the row nothing else. */
            if (readClock != null) await StampClockAsync(item, server, readClock);

            /* Step 2: lay this server's collected metrics from the fleet-wide read on top. Guarded on its
               own (see ApplyCollectedMetrics), so it cannot cost the row above its place. */
            ApplyCollectedMetrics(item, server, fleetMetrics);

            return (Row: item, ReadFailed: false);
        });

        var results = await Task.WhenAll(tasks);
        return new InventoryBuild(
            results.Where(r => r.Row != null).Select(r => r.Row!).ToList(),
            ListedServers: results.Length,
            UnreadableServers: results.Count(r => r.ReadFailed));
    }

    /// <summary>
    /// The last-known row for a server whose live read failed: its newest collected properties, or no row when the store
    /// holds none (<c>ReadFailed</c> false) or the read throws (<c>ReadFailed</c> true), which the empty sentence tells
    /// apart. Either way the server is left out and the log says which; the throw never propagates, so it costs the grid
    /// no other row.
    /// </summary>
    internal static async Task<(ServerPropertyRow? Row, bool ReadFailed)> ReadLastKnownAsync(
        ServerConnection server,
        Func<ServerConnection, Task<ServerPropertiesRow?>> readCollected)
    {
        try
        {
            var collected = await readCollected(server);
            if (collected == null)
            {
                AppLogger.Warn("FinOps", $"{server.DisplayName} has no collected server properties to show in its place, so it is left out of the Server Inventory.");
                return (null, false);
            }
            return (FromCollected(collected), false);
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to read the collected server properties for {server.DisplayName}, so it is left out of the Server Inventory: {ex.Message}", ex);
            return (null, true);
        }
    }

    /// <summary>
    /// A last-known inventory row from a collected <c>server_properties</c> row. The version reads as a live row's does
    /// (the product version with its update level, else its level). The collector stores no host OS, start time or AG
    /// replica role for the inventory, so those cells stay blank, and a 0 socket or core count is a value the store never
    /// had, so it reads as blank too. A row with no CPU count and no memory is one the collector stored when the monitoring
    /// login was denied the hardware read, so it carries the Hardware Note a live row gets for that denial
    /// (<see cref="LocalDataService.HardwareUnavailableNote"/>). Last Updated is the row's collection time, an instant in UTC as
    /// a live row's read time is, and the row carries the clock its collected row recorded for the server (none when that row
    /// has no UTC offset), which the Last Updated cell reads in Server Time.
    /// </summary>
    internal static ServerPropertyRow FromCollected(ServerPropertiesRow collected)
    {
        var level = string.IsNullOrEmpty(collected.ProductUpdateLevel) ? collected.ProductLevel : collected.ProductUpdateLevel;
        var hardwareDenied = collected.CpuCount == 0 && collected.PhysicalMemoryMb == 0;
        return new ServerPropertyRow
        {
            HardwareUnavailableReason = hardwareDenied ? LocalDataService.HardwareUnavailableNote : null,
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
            LastUpdated = DateTime.SpecifyKind(collected.CollectionTime, DateTimeKind.Utc),
            Clock = collected.UtcOffsetMinutes.HasValue ? ServerClock.Resolve(collected.TimeZoneId, collected.UtcOffsetMinutes) : null,
        };
    }

    /// <summary>
    /// Gives <paramref name="item"/> its server's clock from <paramref name="readClock"/>. A read that throws is logged and never
    /// propagates: the row keeps the clock it has, and with none its Last Updated reads the active tab's clock.
    /// </summary>
    internal static async Task StampClockAsync(ServerPropertyRow item, ServerConnection server, Func<ServerConnection, Task<ServerClock>> readClock)
    {
        try
        {
            item.Clock = await readClock(server);
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to read the clock of {server.DisplayName}, so its Last Updated is not shown on its own clock: {ex.Message}", ex);
        }
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
