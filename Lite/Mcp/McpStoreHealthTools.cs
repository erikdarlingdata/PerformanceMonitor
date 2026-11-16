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
/// <c>get_store_health</c> (#8788): the health of Lite's own DuckDB store, as text in the four sections the Store Health window shows.
/// It reads no monitored server. Lite-only: Darling's store is a PostgreSQL database with its own page.
/// </summary>
[McpServerToolType]
public sealed class McpStoreHealthTools
{
    internal const string StoreHealthDescription =
        "Health of Lite's own local store, not of a monitored server. Four sections: STORE (the monitor.duckdb file against the 512 MB point where Lite archives all data and starts a fresh store, plus the data inside it and the write-ahead log), " +
        "ARCHIVES (parquet archive file count, size on disk, oldest and newest month kept, and the retention period in months), " +
        "HOUSEKEEPING HISTORY (the timed archives, size resets and retention cleanups, newest first, with each outcome, rows or files moved, the error text of a failure, and the data in the store when the pass started; the history survives a restart and a size reset), " +
        "and TABLES (estimated rows per table, largest first, so you can see which collector fills the store). Use this when history looks reset, the disk is filling, or a collector's rows look short.";

    [McpServerTool(Name = "get_store_health"), Description(StoreHealthDescription)]
    public static async Task<string> GetStoreHealth(StoreHealthReader reader)
    {
        try
        {
            var snapshot = await Task.Run(reader.Read);
            return StoreHealthText.Render(snapshot);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_store_health", ex);
        }
    }
}
