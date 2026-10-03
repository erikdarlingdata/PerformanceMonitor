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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Utilization sub-tab reads — Lite's <c>LocalDataService.FinOps.Utilization.cs</c> ported to
/// Postgres. The DuckDB→PG rewrite is near-verbatim (positional <c>$1/$2</c> params, PERCENTILE_CONT,
/// <c>CAST(... AS DECIMAL(p,s))</c>, and <c>LEFT JOIN ... ON true</c> all run identically on PG); the ONE
/// substantive change is that Lite's <c>server_info</c> CTE reads <c>v_server_properties</c>, which Darling
/// has no view for, so it reads the collected <c>server_properties</c> base table directly (bare name
/// resolves through the connection's Search Path). SQL kept in <c>public const</c> so tests pin it.
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

    public async Task<List<ProvisioningTrendRow>> GetProvisioningTrendAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var dtos = await DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return dtos.ConvertAll(ProvisioningTrendRow.From);
    }

    public async Task<List<MemoryGrantEfficiencyRow>> GetMemoryGrantEfficiencyAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var dtos = await DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(
            _dataSource, serverId, hoursBack, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return dtos.ConvertAll(MemoryGrantEfficiencyRow.From);
    }
}
