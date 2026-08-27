/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The ADR persistent version store MCP surface (#2029) — PVS was reachable over MCP only indirectly (the
/// alert-settings knob group and the custom-view compose measures), which an agent scanning tool names for
/// "what's my version store doing" would never find. One browsable tool tells the same story the FinOps
/// grid, the #2018 trend chart, and the #1984 pressure alert tell: per-database PVS size with
/// percent-of-database from the same data-file denominator, aborted-transaction counts, cleaner-run state
/// (Microsoft's start-time-without-end-time shape), and the transaction-id lag — presented as the gap
/// itself, never a verdict, for the same reason the grids refuse to invent a threshold Microsoft does not
/// document.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPvsTools
{
    [McpServerTool(Name = "get_pvs_stats"), Description(
        "Gets the Accelerated Database Recovery (ADR) persistent version store state per database: PVS size and percent-of-database, online-index version store size, aborted transaction count, version-cleaner run state (a start time without an end time means the cleaner is mid-run), and the oldest active/aborted transaction ids. Use when a database's size is growing without table growth, when ADR cleanup looks stuck, or alongside the PVS pressure alert. A large PVS is pinned by long-running or aborted transactions; the id gap shows how far cleanup is behind. Optionally returns the size trend for the top-5 databases over a window.")]
    public static async Task<string> GetPvsStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of size-trend history for the top-5 databases; 0 (default) returns the latest snapshot only.")] int trend_hours_back = 0)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        if (trend_hours_back != 0)
        {
            var validation = McpHelpers.ValidateHoursBack(trend_hours_back);
            if (validation != null) return validation;
        }

        try
        {
            var rows = await DarlingPvsReader.GetPvsStatsLatestAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "pvs_stats")
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
                    ? Math.Round(r.PvsSizeMb.Value / r.DatabaseDataSizeMb.Value * 100.0, 2)
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
                var points = await DarlingPvsReader.GetPvsTrendAsync(
                    postgres, resolved.ServerId, DateTime.UtcNow.AddHours(-trend_hours_back));
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
