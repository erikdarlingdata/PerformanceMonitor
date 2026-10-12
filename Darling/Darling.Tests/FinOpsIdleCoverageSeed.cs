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

namespace Darling.Tests;

/// <summary>
/// Query-stats samples that satisfy the idle-database coverage rule (the oldest sample at or before now - 7 days, and a sample on
/// each of the 7 complete UTC days before today), for a test whose seed has to leave a database idle. The samples carry a database
/// no seed lists and no executions, so they add coverage and nothing else: no database turns active and no query row appears.
/// </summary>
internal static class FinOpsIdleCoverageSeed
{
    /// <summary>The coverage samples for one server as of <paramref name="now"/> (naive UTC): one old sample and a noon sample on each of D-7 through D-1.</summary>
    public static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, DateTime now)
    {
        await InsertAsync(c, ct, serverId, serverName, now.AddDays(-8), "oldest");
        for (var day = 1; day <= 7; day++)
        {
            await InsertAsync(c, ct, serverId, serverName, now.Date.AddDays(-day).AddHours(12), "day" + day);
        }
    }

    /// <summary>One zero-execution coverage sample at <paramref name="at"/> (naive UTC).</summary>
    public static Task InsertAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, DateTime at, string tag) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds) VALUES ($1, $2, $3, $4, 'CoverageProbe', $5, $5, 0, 0, 0, 60)",
            CollectionIdGenerator.Next(), at, serverId, serverName, "0xCOV" + tag + serverId);
}
