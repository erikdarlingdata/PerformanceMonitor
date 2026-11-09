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
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Utilization sub-tab reads — Lite's <c>LocalDataService.FinOps.Utilization.cs</c> ported to
/// Postgres. The DuckDB→PG rewrite is near-verbatim (positional <c>$1/$2</c> params, PERCENTILE_CONT,
/// <c>CAST(... AS DECIMAL(p,s))</c>, and <c>LEFT JOIN ... ON true</c> all run identically on PG); the ONE
/// substantive change is that Lite's <c>server_info</c> CTE reads <c>v_server_properties</c>, which Darling
/// has no view for, so it reads the collected <c>server_properties</c> base table directly (bare name
/// resolves through the connection's Search Path). SQL kept in <c>public const</c> so tests pin it.
/// The two per-day reads bucket by the selected time display's days (<see cref="DisplayDays"/>), so a row's Day reads in the same
/// clock as every other time on the page; the reader's own UTC-day reads serve the MCP tools, which have no display.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The Utilization-efficiency read lives in Storage; this alias keeps the viewer's SQL pins on the same text.</summary>
    public const string UtilizationEfficiencySql = DarlingFinOpsUtilizationReader.UtilizationEfficiencySql;

    /// <summary>The provisioning-trend read lives in Storage; this alias keeps the viewer's SQL pins on the same text.</summary>
    public const string ProvisioningTrendSql = DarlingFinOpsUtilizationReader.ProvisioningTrendSql;

    /// <summary>The memory-grant-efficiency read lives in Storage; this alias keeps the viewer's SQL pins on the same text.</summary>
    public const string MemoryGrantEfficiencySql = DarlingFinOpsUtilizationReader.MemoryGrantEfficiencySql;

    public async Task<UtilizationEfficiencyRow?> GetUtilizationEfficiencyAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var dto = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return dto is null ? null : UtilizationEfficiencyRow.From(dto);
    }

    /// <summary>
    /// The 7-day provisioning trend, each row one calendar day of <paramref name="dayZone"/>: the selected time display's zone on the
    /// server's clock (<see cref="DisplayDayZoneAsync"/>), so a row's Day reads in the same clock as every other time on the page.
    /// </summary>
    public async Task<List<ProvisioningTrendRow>> GetProvisioningTrendAsync(int serverId, TimeZoneInfo dayZone, CancellationToken cancellationToken = default)
    {
        var nowUtc = DateTime.UtcNow;
        var cutoff = nowUtc.AddDays(-7);
        var dtos = await DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(
            _dataSource, serverId, cutoff, DisplayDays(cutoff, nowUtc, dayZone), ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return dtos.ConvertAll(ProvisioningTrendRow.From);
    }

    /// <summary>Memory-grant efficiency over the last <paramref name="hoursBack"/> hours, each row one calendar day of <paramref name="dayZone"/>.</summary>
    public async Task<List<MemoryGrantEfficiencyRow>> GetMemoryGrantEfficiencyAsync(int serverId, TimeZoneInfo dayZone, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var nowUtc = DateTime.UtcNow;
        var cutoff = nowUtc.AddHours(-hoursBack);
        var dtos = await DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(
            _dataSource, serverId, cutoff, DisplayDays(cutoff, nowUtc, dayZone), ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return dtos.ConvertAll(MemoryGrantEfficiencyRow.From);
    }

    /// <summary>
    /// The zone the FinOps day tables count <paramref name="serverId"/>'s days in: the selected time display on that server's clock (its
    /// collected clock, else the viewer machine's offset), the rule the Application Connections rows read their times by.
    /// </summary>
    internal async Task<TimeZoneInfo> DisplayDayZoneAsync(int serverId, CancellationToken cancellationToken = default) =>
        ViewerTimeHelper.DisplayZoneFor(ViewerTimeHelper.CurrentDisplayMode, await GetServerClockOrMachineAsync(serverId, cancellationToken));

    /// <summary>
    /// The days of [<paramref name="fromUtc"/>, <paramref name="toUtc"/>] as <paramref name="zone"/> counts them: one bucket per
    /// wall-clock day of the zone, bounded by the UTC instants its midnights fall on (<see cref="DisplayZone.ToUtcBound"/>). Adjacent
    /// days share a bound, so every instant lands in exactly one day, on the clock-change days too: the autumn change day runs 25
    /// hours and the spring one 23. UTC gives UTC days on their midnights, the reader's own.
    /// </summary>
    internal static List<DayBucket> DisplayDays(DateTime fromUtc, DateTime toUtc, TimeZoneInfo zone)
    {
        var (first, last) = fromUtc <= toUtc ? (fromUtc, toUtc) : (toUtc, fromUtc);
        var days = new List<DayBucket>();
        var lastDay = DisplayZone.ToDisplay(last, zone).Date;
        for (var day = DisplayZone.ToDisplay(first, zone).Date; day <= lastDay; day = day.AddDays(1))
        {
            days.Add(new DayBucket(day, DisplayZone.ToUtcBound(day, zone, BoundSide.From), DisplayZone.ToUtcBound(day.AddDays(1), zone, BoundSide.From)));
        }
        return days;
    }
}
