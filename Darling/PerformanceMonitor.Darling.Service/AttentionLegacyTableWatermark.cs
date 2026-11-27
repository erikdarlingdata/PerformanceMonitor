/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8967: the attention watermark's read of the swapped-out table. After the swap <c>collect.attention_events</c> holds only the rows
/// written since, and the older ones sit in <c>collect.attention_events_legacy</c> until the copy ends. The legacy table never changes,
/// so the greater of the two tables' MAX for the server is exact at any copy progress. This reads the legacy half, and answers NULL
/// when the table is gone.
/// </summary>
internal static class AttentionLegacyTableWatermark
{
    internal const string TargetTable = "attention_events";

    private const string LegacyTable = "collect.attention_events_legacy";

    /// <summary>
    /// The server's newest <paramref name="columnName"/> in the legacy table, or NULL when there is no legacy table or no row for the
    /// server. With <paramref name="databaseColumnName"/> and <paramref name="databaseName"/> the answer is for that one database only
    /// (the Azure SQL Database per-database read). The legacy table is the whole retained history, so the read is bounded the way the
    /// live table's is: anchored on the server's newest <c>collection_time</c> (one step down the <c>(server_id, collection_time)</c>
    /// index) and probed over the recent window, then the week, then unbounded only for a server (or database) whose last stretch held
    /// no timestamped row.
    /// </summary>
    internal static async Task<DateTime?> ReadAsync(
        NpgsqlConnection connection, int serverId, string columnName, int commandTimeoutSeconds, CancellationToken cancellationToken,
        string? databaseColumnName = null, string? databaseName = null)
    {
        using (var exists = new NpgsqlCommand($"SELECT to_regclass('{LegacyTable}') IS NOT NULL", connection) { CommandTimeout = commandTimeoutSeconds })
        {
            if (await exists.ExecuteScalarAsync(cancellationToken) is not true)
            {
                return null;
            }
        }

        DateTime? anchor;
        using (var newest = new NpgsqlCommand($"SELECT MAX(collection_time) FROM {LegacyTable} WHERE server_id = $1", connection) { CommandTimeout = commandTimeoutSeconds })
        {
            newest.Parameters.AddWithValue(serverId);
            anchor = await newest.ExecuteScalarAsync(cancellationToken) as DateTime?;
        }

        if (anchor is not { } newestCollected)
        {
            return null;
        }

        var perDatabase = databaseColumnName is not null && databaseName is not null;
        foreach (var window in new[] { WatermarkPolicy.RecentWatermarkWindow, WatermarkPolicy.WidenedWatermarkWindow })
        {
            using var bounded = new NpgsqlCommand(
                perDatabase
                    ? DarlingCollectorRunner.BuildServerWatermarkForDatabaseSql(LegacyTable, columnName, databaseColumnName!, bounded: true)
                    : DarlingCollectorRunner.BuildServerWatermarkSql(LegacyTable, columnName, bounded: true), connection) { CommandTimeout = commandTimeoutSeconds };
            bounded.Parameters.AddWithValue(serverId);
            if (perDatabase)
            {
                bounded.Parameters.AddWithValue(databaseName!);
            }

            bounded.Parameters.AddWithValue(DateTime.SpecifyKind(newestCollected - window, DateTimeKind.Unspecified));
            if (await bounded.ExecuteScalarAsync(cancellationToken) is DateTime hit)
            {
                return hit;
            }
        }

        using var unbounded = new NpgsqlCommand(
            perDatabase
                ? DarlingCollectorRunner.BuildServerWatermarkForDatabaseSql(LegacyTable, columnName, databaseColumnName!, bounded: false)
                : DarlingCollectorRunner.BuildServerWatermarkSql(LegacyTable, columnName, bounded: false), connection) { CommandTimeout = commandTimeoutSeconds };
        unbounded.Parameters.AddWithValue(serverId);
        if (perDatabase)
        {
            unbounded.Parameters.AddWithValue(databaseName!);
        }

        return await unbounded.ExecuteScalarAsync(cancellationToken) as DateTime?;
    }
}
