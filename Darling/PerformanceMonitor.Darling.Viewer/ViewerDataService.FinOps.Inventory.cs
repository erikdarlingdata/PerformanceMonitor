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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Viewer;


/// <summary>
/// FinOps Server Inventory reads. Lite queries each target LIVE (<c>GetServerPropertiesLiveAsync</c>) — the
/// headless viewer can't reach targets, so the inventory rows come from the COLLECTED
/// <c>server_properties</c> table (Darling collects it; <c>PgFactCollector.Config.ServerPropertiesSql</c>
/// reads it the same way) joined to the <c>servers</c> registry, with the collected metrics
/// (<see cref="ServerMetricsSql"/>) overlaid per server by the loader. Column parity vs Lite's live query:
/// the collected table carries edition/version/level/CPU/memory/sockets/cores/HADR/clustered, so those
/// surface; it does NOT carry sqlserver_start_time / host OS / AG replica role, so those Lite columns are
/// omitted (nothing stubbed). SQL kept in <c>public const</c> so tests pin it.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The fleet metrics read lives in Storage; this alias keeps the viewer's SQL pins on the same text.</summary>
    public const string ServerMetricsSql = DarlingFinOpsInventoryReader.ServerMetricsSql;

    /// <summary><see cref="ServerMetricsSql"/> with the idle check routed through the hourly stitched rollup; the text lives in Storage.</summary>
    public static string ServerMetricsSqlFor(RollupCoverage coverage, DateTime idleCutoffUtc, DateTime watermarkUtc) =>
        DarlingFinOpsInventoryReader.ServerMetricsSqlFor(coverage, idleCutoffUtc, watermarkUtc);

    /// <summary>One fleet server's overlay metrics — <see cref="GetServerMetricsAsync"/>'s per-row result.</summary>
    public readonly record struct ServerMetricsRow(decimal? AvgCpuPct, decimal? StorageTotalGb, int? IdleDbCount, string? ProvisioningStatus)
    {
        public static ServerMetricsRow From(ServerMetricsDto dto) =>
            new(dto.AvgCpuPct, dto.StorageTotalGb, dto.IdleDbCount, dto.ProvisioningStatus);
    }

    /// <summary>Every server's overlay metrics in one round trip (#4227); the read lives in Storage and the rollup probe stays cached here.</summary>
    public async Task<Dictionary<int, ServerMetricsRow>> GetServerMetricsAsync(CancellationToken cancellationToken = default)
    {
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);

        var dtos = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(
            _dataSource, rollups, coverage, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

        var results = new Dictionary<int, ServerMetricsRow>(dtos.Count);
        foreach (var (serverId, dto) in dtos) results[serverId] = ServerMetricsRow.From(dto);
        return results;
    }

    /// <summary>The provisioning verdict for one fleet-read row (see the Storage reader); kept here for the viewer's pins.</summary>
    internal static string? FleetProvisioningStatusFor(System.Data.Common.DbDataReader reader) =>
        DarlingFinOpsInventoryReader.FleetProvisioningStatusFor(reader);

    /// <summary>#1591: the Hardware Note for one inventory row (see the Storage reader).</summary>
    internal static string? HardwareNoteFor(bool cpuCountIsNull, bool physicalMemoryIsNull) =>
        DarlingFinOpsInventoryReader.HardwareNoteFor(cpuCountIsNull, physicalMemoryIsNull);

    /// <summary>Latest collected properties per server joined to the registry; the text lives in Storage.</summary>
    public const string ServerInventorySql = DarlingFinOpsInventoryReader.ServerInventorySql;

    public async Task<List<ServerPropertyRow>> GetServerInventoryAsync(CancellationToken cancellationToken = default)
    {
        /* #4766: Server Inventory is one row per server, so each row reads its times on ITS server's clock (the
           collected one, else the viewer machine's offset, the rule every list row uses) and not on the active server
           tab's. The fleet's clocks are read once per load. */
        var clocks = await GetServerClocksAsync(null, cancellationToken);
        var nowUtc = DateTime.UtcNow;

        var dtos = await DarlingFinOpsInventoryReader.GetServerInventoryAsync(
            _dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

        var items = new List<ServerPropertyRow>(dtos.Count);
        foreach (var dto in dtos)
        {
            var clock = ViewerTimeHelper.ClockForServerOrMachine(clocks, dto.ServerId, TimeZoneInfo.Local, nowUtc);
            items.Add(ServerPropertyRow.From(dto, clock));
        }
        return items;
    }
}
