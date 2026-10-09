/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpFinOpsTools
{
    internal const string DatabaseSizesView = "database_sizes";

    /* The served head is at its 620-character target with the eight earlier views, so this view is named in the
       Valid list and described in the guide tail only. */

    internal const string DatabaseSizesViewGuide =
        "database_sizes gives one row per database file from the latest size snapshot (captured_at, a time, not a window; hours_back is ignored), biggest first, with the volume it sits on. free_space_mb is total minus used and used_pct is used over total to 0.1; both are null when the file has no used size. auto_growth_mb is the growth step in MB when is_percent_growth is false, 0 meaning growth is off; when it is true, growth_pct is the step. vlf_count and recovery_model apply to log files and databases; vlf_count is null on a data file. monthly_cost_usd is the file's share of the server's registered monthly cost by allocated size, as the desktop shows it, rounded to 0.01; null for every row when no cost is set or it is 0 or less, and for a file with no allocated size. monthly_cost_usd is a share of the whole snapshot, so it still sums to the monthly cost when rows is cut. rows lists 70 files by default; limit 11 to 500 lists that many (the default 10 means 70); file_count is the number of files and truncated says when there were more. size_note carries the Azure SQL notes, as get_database_sizes.";

    /// <summary>How many files <c>rows</c> lists when the caller leaves <c>limit</c> at its default. Sized so a default
    /// call stays under the response target.</summary>
    internal const int DefaultDatabaseSizeRows = 70;

    /// <summary>The most files a caller may ask <c>rows</c> to list. It matches the 500-row ceiling of the database_resources
    /// and application_connections views.</summary>
    internal const int MaxDatabaseSizeRows = 500;

    /// <summary>The row count a <c>limit</c> asks for: the default <c>limit</c> means <see cref="DefaultDatabaseSizeRows"/>,
    /// any other value is the count.</summary>
    internal static int DatabaseSizeRowCap(int limit) => limit == DefaultLimit ? DefaultDatabaseSizeRows : limit;

    /// <summary>One file's share of the monthly cost by allocated size, to 2 places away from zero; null when no
    /// cost is set, the file has no allocated size, or the snapshot allocates nothing.</summary>
    internal static decimal? DatabaseSizeCostShare(decimal? sizeMb, decimal totalMb, decimal monthly)
    {
        if (monthly <= 0m || totalMb <= 0m || sizeMb is null) return null;
        return Math.Round(FinOpsCost.StorageShare(sizeMb.Value, totalMb, monthly), 2, MidpointRounding.AwayFromZero);
    }

    private static async Task<string> ReadDatabaseSizesAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int limit, CancellationToken ct)
    {
        var files = await DarlingFinOpsDatabaseSizesReader.GetLatestAsync(postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, ct);
        if (files.Count == 0)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "database_size_stats", ct)
                ?? McpHelpers.Status("unavailable", "No database size data available. The size collector may not have run yet.");
        }

        var monthly = await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, ct);
        return DatabaseSizesPayload(resolved.ServerName, files, monthly, DatabaseSizeRowCap(limit));
    }

    /// <summary>The database_sizes payload, shaped apart from the read so it can be pinned without a store.</summary>
    internal static string DatabaseSizesPayload(string serverName, IReadOnlyList<DatabaseSizeFileDto> files, decimal monthly, int rowCap = DefaultDatabaseSizeRows)
    {
        var hasCost = monthly > 0m;
        var totalMb = files.Sum(f => f.TotalSizeMb ?? 0m);
        var note = AzureSiblingDatabaseSize.DatabaseSizesNote(
            hasLogServiceFile: files.Any(f => f.TotalSizeMb is null),
            hasSiblingRow: files.Any(f => AzureSiblingDatabaseSize.IsSiblingRow(f.FileId, f.FileName)));
        return JsonSerializer.Serialize(new
        {
            server = serverName,
            view = DatabaseSizesView,
            captured_at = McpHelpers.FormatEffectiveStart(files[0].CollectionTime),
            monthly_cost_usd = hasCost ? monthly : (decimal?)null,
            cost_reason = hasCost ? null : "monthly cost not set",
            file_count = files.Count,
            truncated = files.Count > rowCap,
            note,
            rows = files.Take(rowCap).Select(f => DatabaseSizesRow(f, totalMb, monthly)).ToList(),
        }, McpHelpers.JsonOptions);
    }

    /// <summary>One database-sizes row in the wire shape: snake_case keys.</summary>
    internal static Dictionary<string, object?> DatabaseSizesRow(DatabaseSizeFileDto f, decimal totalMb, decimal monthly)
    {
        var isLog = string.Equals(f.FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase);
        var row = new Dictionary<string, object?>
        {
            ["database_name"] = f.DatabaseName,
            ["file_name"] = f.FileName,
            ["file_type"] = f.FileTypeDesc,
            ["total_size_mb"] = f.TotalSizeMb,
            ["used_size_mb"] = f.UsedSizeMb,
            ["free_space_mb"] = f.UsedSizeMb.HasValue && f.TotalSizeMb.HasValue ? f.TotalSizeMb.Value - f.UsedSizeMb.Value : null,
            ["used_pct"] = f.UsedSizeMb.HasValue && f.TotalSizeMb > 0 ? Math.Round(f.UsedSizeMb.Value * 100m / f.TotalSizeMb.Value, 1) : null,
            ["max_size_mb"] = f.MaxSizeMb,
            ["auto_growth_mb"] = f.AutoGrowthMb,
            ["is_percent_growth"] = f.IsPercentGrowth,
            ["growth_pct"] = f.GrowthPct,
            ["recovery_model"] = f.RecoveryModel,
            ["vlf_count"] = isLog ? f.VlfCount : null,
            ["volume_mount_point"] = f.VolumeMountPoint,
            ["volume_total_mb"] = f.VolumeTotalMb,
            ["volume_free_mb"] = f.VolumeFreeMb,
            ["monthly_cost_usd"] = DatabaseSizeCostShare(f.TotalSizeMb, totalMb, monthly),
        };
        if (AzureSiblingDatabaseSize.IsSiblingRow(f.FileId, f.FileName))
            row[AzureSiblingDatabaseSize.RowNoteKey] = AzureSiblingDatabaseSize.LogNote;
        return row;
    }
}
