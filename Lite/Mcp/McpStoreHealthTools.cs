/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// <c>get_store_health</c> (#8788): the health of Lite's own DuckDB store, as text in the four sections the Store Health window shows, plus the build history of Lite's starts.
/// It reads no monitored server. Lite-only: Darling's store is a PostgreSQL database with its own page.
/// </summary>
[McpServerToolType]
public sealed class McpStoreHealthTools
{
    internal const string StoreHealthDescription =
        "Health of Lite's own local store, not of a monitored server. Five sections: STORE (the data inside the monitor.duckdb file against the point (512 MB unless settings.json sets hot_store_reset_mb, and the section says which) where Lite archives all data and starts a fresh store, which it also does when the file reaches twice that point, plus the file size and the write-ahead log), " +
        "ARCHIVES (parquet archive file count, size on disk, oldest and newest month kept, and the retention period in months), " +
        "HOUSEKEEPING HISTORY (the timed archives, size resets and retention cleanups, newest first, with each outcome, rows or files moved, the error text of a failure, and the data in the store when the pass started; the history survives a restart and a size reset), " +
        "TABLES (estimated rows per table, largest first, so you can see which collector fills the store), " +
        "and BUILD HISTORY (one row per Lite start, newest first, at most 20: the build, meaning the version with any +hash removed, the schema version before and after that start's migrations and how long they took in ms, how many archive files there were at the start, the previous build when it changed, and the last time that run was seen, taken from its last housekeeping entry or collection; the newest row is the run answering). Use this when history looks reset, the disk is filling, a collector's rows look short, or to line a change up with the build that brought it.";

    [McpServerTool(Name = "get_store_health"), Description(StoreHealthDescription)]
    public static async Task<string> GetStoreHealth(StoreHealthReader reader)
    {
        try
        {
            var snapshot = await Task.Run(reader.Read);
            return StoreHealthText.Render(snapshot) + Environment.NewLine + Environment.NewLine + BuildHistory.Render(reader.ReadBuildHistory());
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_store_health", ex);
        }
    }
}
