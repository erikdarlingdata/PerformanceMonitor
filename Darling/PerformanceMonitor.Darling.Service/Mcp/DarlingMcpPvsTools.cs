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
using System.Threading;
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
        "ADR PVS state per database: size, % of database, aborted-txn count, cleaner times (a start with no end = mid-run). LATEST IS A TIME: the newest snapshot, not a window; as_of is when it was taken. trend_hours_back (0 = off) looks back from now, not from as_of, over the top-5 databases by current size. Cleaner times are de-skewed to UTC, like as_of. pct_of_database is null only if PVS size is unmeasured (pvs_measured false) or database size is missing or 0 (pct_of_database_reason says why); else measured 0 MB = 0.00. No rows: not_collected if this engine can't collect PVS, else empty. <<GUIDE>> Gets the Accelerated Database Recovery (ADR) persistent version store state per database: PVS size and percent-of-database, online-index version store size, aborted transaction count, version-cleaner run state (a start time without an end time means the cleaner is mid-run), and the oldest active/aborted transaction ids. Use when a database's size is growing without table growth, when ADR cleanup looks stuck, or alongside the PVS pressure alert. A large PVS is pinned by long-running or aborted transactions; the id gap shows how far cleanup is behind. Optionally returns the size trend for the top-5 databases over a window. Every timestamp here is UTC, the four cleaner times included - the DMV reports those in the monitored server's local clock and this read de-skews them - so a cleaner time compares directly against as_of. pvs_measured says whether the DMV reported a size for that database at all; a measured 0 MB is published as pvs_size_mb 0 and pct_of_database 0.00 (the healthy, fully-cleaned state), and pct_of_database is null only when the numerator was not measured or the denominator is absent or zero, with pct_of_database_reason saying which.")]
    public static async Task<string> GetPvsStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of size-trend history for the top-5 databases; 0 (default) returns the latest snapshot only.")] int trend_hours_back = 0,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        if (trend_hours_back != 0)
        {
            var validation = McpHelpers.ValidateHoursBack(trend_hours_back);
            if (validation != null) return validation;
        }

        try
        {
            var rows = await DarlingPvsReader.GetPvsStatsLatestAsync(postgres, resolved.ServerId, cancellationToken);
            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "pvs_stats", cancellationToken)
                    ?? McpHelpers.Status("empty",
                        "No PVS data collected for this server. The collector reads sys.dm_tran_persistent_version_store_stats " +
                        "(SQL Server 2019+); a server with no rows either predates ADR or has not completed a pvs_stats cycle yet.");
            }

            var databases = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                is_adr_on = r.IsAdrOn,
                pvs_size_mb = r.PvsSizeMb,
                /* Whether the DMV reported a size at all (#3541 A12, contract rule 5). A measured 0 MB — the
                   healthy, fully-cleaned state — used to be indistinguishable from a NULL the collector
                   could not read: both fell through to pct_of_database = null. */
                pvs_measured = r.PvsSizeMb.HasValue,
                /* The SAME denominator the FinOps grid and the pressure alert use, so no surface disagrees.
                   Any MEASURED size divides — 0 MB of a 100 GB database is 0.00%, a measurement — and only an
                   unmeasured numerator or an absent/zero denominator yields null, with the reason beside it. */
                pct_of_database = r.PvsSizeMb is { } pvsMb && r.DatabaseDataSizeMb is > 0
                    ? Math.Round(pvsMb / r.DatabaseDataSizeMb.Value * 100.0, 2)
                    : (double?)null,
                pct_of_database_reason = PctReason(r.PvsSizeMb.HasValue, r.DatabaseDataSizeMb),
                online_index_version_store_mb = r.OnlineIndexVersionStoreMb,
                database_data_size_mb = r.DatabaseDataSizeMb,
                aborted_transaction_count = r.AbortedTransactionCount,
                /* Cleaner state, Microsoft's shape: a start without an end means mid-run. The PAIRING is
                   presented as the DMV reports it; the FRAME is not. These four are server-local in the store
                   and the read de-skews them, so they compare directly against as_of above instead of reading
                   one whole UTC offset stale — 4 hours on the production fleet, on a value that is usually
                   seconds old, for the one question this tool exists to answer. */
                aborted_version_cleaner_start_time = r.AbortedCleanerStartTimeUtc?.ToString("o"),
                aborted_version_cleaner_end_time = r.AbortedCleanerEndTimeUtc?.ToString("o"),
                offrow_version_cleaner_start_time = r.OffrowCleanerStartTimeUtc?.ToString("o"),
                offrow_version_cleaner_end_time = r.OffrowCleanerEndTimeUtc?.ToString("o"),
                /* The lag between these ids is how far cleanup is behind — the gap itself, never a verdict. */
                oldest_active_transaction_id = r.OldestActiveTransactionId,
                oldest_aborted_transaction_id = r.OldestAbortedTransactionId,
            });

            object? trend = null;
            if (trend_hours_back > 0)
            {
                var points = await DarlingPvsReader.GetPvsTrendAsync(
                    postgres, resolved.ServerId, DateTime.UtcNow.AddHours(-trend_hours_back), cancellationToken);
                trend = points
                    .GroupBy(p => p.DatabaseName)
                    .Select(g => new
                    {
                        database_name = g.Key,
                        points = g.Select(p => new
                        {
                            collection_time = p.CollectionTime.ToString("o"),
                            /* #3653: an unmeasured point is null, with the same pvs_measured flag the latest
                               snapshot carries — never a fabricated 0 MB in the series. Lite's twin
                               (McpPvsTools.cs, #3666) spells these two keys identically. */
                            pvs_size_mb = p.PvsSizeMb,
                            pvs_measured = p.PvsSizeMb.HasValue,
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pvs_stats", ex);
        }
    }

    /// <summary>
    /// Why <c>pct_of_database</c> is null, when it is (#3541 A12): the numerator was not measured, or the
    /// denominator was absent or zero. Null when the percent is defined — including a defined 0.00 — so the
    /// healthy row carries no note. Lite's twin words it identically.
    /// </summary>
    internal static string? PctReason(bool pvsMeasured, double? databaseDataSizeMb)
    {
        if (!pvsMeasured)
            return "pvs_size_mb was not reported by sys.dm_tran_persistent_version_store_stats in this capture, so the share is unknown — not zero.";
        if (databaseDataSizeMb is null)
            return "database_data_size_mb was not captured for this database, so there is no denominator — the share is unknown, not zero.";
        if (databaseDataSizeMb <= 0)
            return "database_data_size_mb is 0, so the share has no denominator — the share is unknown, not zero.";
        return null;
    }
}
