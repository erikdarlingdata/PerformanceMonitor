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
using Npgsql;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The lightweight store reads that drive the shell chrome ported from Lite's MainWindow: the sidebar
/// status dot (one <c>v_collection_log</c> freshness query for every server at once) and the status bar's
/// database-size field (<c>pg_database_size</c>). Both are single round-trips so they can run on the
/// refresh timers without weighing anything down; the SQL lives in public constants so tests can pin the
/// load-bearing clauses without a live Postgres.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Newest collection time per server across all collectors, in one pass — the sidebar dots and the
    /// status bar's collection field derive freshness from this (the same <c>MAX(collection_time)</c> the
    /// Overview cards use per server, so a dot and its card agree). Timestamps are the store's naive UTC.
    /// Excludes <c>server_id = 0</c>, the fleet-level retention run-record sentinel
    /// (<c>DarlingObservability.FleetServerId</c>) — it is not a real server, so it must not appear as a
    /// phantom key a future key-iterating consumer could render as "server 0".
    /// </summary>
    public const string ServerFreshnessSql = @"
SELECT server_id, MAX(collection_time)
FROM v_collection_log
WHERE server_id <> 0
GROUP BY server_id";

    /// <summary>The store's on-disk size in bytes (status-bar Database field). No parameters.</summary>
    public const string StoreSizeSql = "SELECT pg_database_size(current_database())";

    /// <summary>
    /// Reads MAX(collection_time) for every server in a single query, keyed by server_id. A server with no
    /// collection rows simply isn't in the dictionary (the caller treats a miss as "no collection" → Offline).
    /// </summary>
    public async Task<Dictionary<int, DateTime>> GetServerFreshnessAsync(CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<int, DateTime>();

        await using var command = _dataSource.CreateCommand(ServerFreshnessSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(1))
            {
                result[reader.GetInt32(0)] = reader.GetDateTime(1);
            }
        }

        return result;
    }

    /// <summary>The store database's size in bytes, or null when it can't be read.</summary>
    public async Task<long?> GetStoreSizeBytesAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(StoreSizeSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null || result == DBNull.Value ? null : Convert.ToInt64(result);
    }
}
