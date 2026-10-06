/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Removal of EXACT duplicate rows already stored in the hot <c>deadlocks</c> table: an Azure SQL Database
/// registered at master could store one deadlock twice, once from the database's own session and once from the
/// server's telemetry. The write path now drops the second copy as it arrives; this removes the copies an older
/// build already stored. It runs on every start with no marker: with no duplicates it deletes nothing.
///
/// <para><b>This is a DELETE path, so the identity is deliberately narrow.</b> A row goes only when an EARLIER row
/// on the same server has the same <c>deadlock_time</c> and the byte-for-byte same <c>deadlock_graph_xml</c>.
/// The earliest <c>collection_time</c> stays, the lowest <c>deadlock_id</c> breaking a tie. A row with a NULL
/// <c>deadlock_time</c>, or a NULL or empty graph, is never touched. <c>deadlock_id</c> is the primary key, so the
/// delete addresses exactly the rows chosen.</para>
///
/// <para><b>Only the hot table.</b> Rows already moved into archived Parquet behind the <c>v_deadlocks</c> view
/// cannot be removed with a DELETE, so a copy that was archived stays until the archive ages out.</para>
/// </summary>
internal static class DeadlockDuplicateCleanup
{
    /* #4348: a graph the statement filter withheld WHOLE is the marker text, the same for every such graph, so two
       different deadlocks at the same time would look like copies. A marker graph is never touched, like a NULL or
       empty one. */
    private const string DeleteSql = @"
DELETE FROM deadlocks
WHERE deadlock_id IN (
    SELECT deadlock_id FROM (
        SELECT deadlock_id,
               row_number() OVER (PARTITION BY server_id, deadlock_time, deadlock_graph_xml
                                  ORDER BY collection_time, deadlock_id) AS rn
        FROM deadlocks
        WHERE deadlock_time IS NOT NULL
        AND   deadlock_graph_xml IS NOT NULL AND deadlock_graph_xml <> ''
        AND   deadlock_graph_xml <> '" + SensitiveStatements.PlaceholderText + @"'
    )
    WHERE rn > 1
)";

    /// <summary>
    /// Deletes the duplicates and returns how many rows went. Never throws: a failure is logged as a warning and
    /// reported as 0, because an initialisation that fails over a cleanup would stop the app starting.
    /// </summary>
    internal static async Task<int> RemoveAsync(DuckDBConnection connection, ILogger? logger)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = DeleteSql;
            var removed = await command.ExecuteNonQueryAsync();
            if (removed > 0)
            {
                logger?.LogInformation("Removed {Removed} exact duplicate deadlock row(s) from the hot deadlocks table, keeping the earliest of each", removed);
            }

            return removed;
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Could not remove exact duplicate deadlock rows; the next start retries");
            return 0;
        }
    }
}
