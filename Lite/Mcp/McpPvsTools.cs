/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The ADR persistent version store MCP surface (#2029) — Lite's twin of Darling's
/// <c>DarlingMcpPvsTools</c>, over the existing <see cref="LocalDataService"/> FinOps PVS reads (the same
/// newest-collection snapshot the grid shows and the #2018 top-5 trend the chart draws).
/// </summary>
[McpServerToolType]
public sealed class McpPvsTools
{
    [McpServerTool(Name = "get_pvs_stats"), Description(
        "Gets the Accelerated Database Recovery (ADR) persistent version store state per database: PVS size and percent-of-database, online-index version store size, aborted transaction count, version-cleaner run state (a start time without an end time means the cleaner is mid-run), and the oldest active/aborted transaction ids. Use when a database's size is growing without table growth, when ADR cleanup looks stuck, or alongside the PVS pressure alert. A large PVS is pinned by long-running or aborted transactions; the id gap shows how far cleanup is behind. Optionally returns the size trend for the top-5 databases over a window.")]
    public static async Task<string> GetPvsStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of size-trend history for the top-5 databases; 0 (default) returns the latest snapshot only.")] int trend_hours_back = 0)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            if (trend_hours_back != 0)
            {
                var hoursError = McpHelpers.ValidateHoursBack(trend_hours_back);
                if (hoursError != null) return hoursError;
            }

            var rows = await dataService.GetPvsStatsLatestAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "pvs_stats")
                    ?? McpHelpers.Status("empty",
                        "No PVS data collected for this server. The collector reads sys.dm_tran_persistent_version_store_stats " +
                        "(SQL Server 2019+); a server with no rows either predates ADR or has not completed a pvs_stats cycle yet.");
            }

            var databases = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                is_adr_on = r.IsAdrOn,
                pvs_size_mb = r.PvsSizeMb,
                /* The SAME denominator the FinOps grid and the pressure alert use, so no surface disagrees. */
                pct_of_database = r.PvsSizeMb is > 0 && r.DatabaseDataSizeMb is > 0
                    ? Math.Round((double)(r.PvsSizeMb.Value / r.DatabaseDataSizeMb.Value) * 100.0, 2)
                    : (double?)null,
                online_index_version_store_mb = r.OnlineIndexVersionStoreMb,
                database_data_size_mb = r.DatabaseDataSizeMb,
                aborted_transaction_count = r.AbortedTransactionCount,
                /* Cleaner state, Microsoft's shape: a start without an end means mid-run. Presented raw. */
                aborted_version_cleaner_start_time = r.AbortedCleanerStartTime?.ToString("o"),
                aborted_version_cleaner_end_time = r.AbortedCleanerEndTime?.ToString("o"),
                offrow_version_cleaner_start_time = r.OffrowCleanerStartTime?.ToString("o"),
                offrow_version_cleaner_end_time = r.OffrowCleanerEndTime?.ToString("o"),
                /* The lag between these ids is how far cleanup is behind — the gap itself, never a verdict. */
                oldest_active_transaction_id = r.OldestActiveTransactionId,
                oldest_aborted_transaction_id = r.OldestAbortedTransactionId,
            });

            object? trend = null;
            if (trend_hours_back > 0)
            {
                var points = await dataService.GetPvsTrendAsync(resolved.ServerId, DateTime.UtcNow.AddHours(-trend_hours_back));
                trend = points
                    .GroupBy(p => p.DatabaseName)
                    .Select(g => new
                    {
                        database_name = g.Key,
                        points = g.Select(p => new
                        {
                            collection_time = p.CollectionTime.ToString("o"),
                            pvs_size_mb = p.PvsSizeMb,
                            pct_of_database = p.PctOfDatabase is { } pct ? Math.Round(pct, 2) : (double?)null,
                        }),
                    });
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                as_of = rows[0].CollectionTime.ToString("o"),
                databases,
                trend_hours_back = trend_hours_back > 0 ? trend_hours_back : (int?)null,
                trend,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pvs_stats", ex);
        }
    }
}
