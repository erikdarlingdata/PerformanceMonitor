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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One (cacheobjtype, objtype) group at the latest plan-cache snapshot.</summary>
public sealed record PlanCacheGroupRow(
    DateTime CollectionTime, string CacheObjType, string ObjType, int TotalPlans, int TotalSizeMb,
    int SingleUsePlans, int SingleUseSizeMb, int MultiUsePlans, int MultiUseSizeMb, decimal AvgUseCount);

/// <summary>
/// The plan-cache composition read the web's <c>get_plan_cache_bloat</c> endpoint (the MCP tool) and the Viewer's
/// Plan Cache by Type grid share (#6388), moved here from the Service project's <c>DarlingPlanCacheSchedulerReader</c>
/// unchanged except that the command deadline is passed in.
/// </summary>
public static class DarlingPlanCacheBloatReader
{
    /// <summary>
    /// Every (cacheobjtype, objtype) group at the latest collection in the window — the Dashboard's
    /// <c>get_plan_cache_bloat</c> composition, ordered by cache size. The group cardinality is small
    /// (cacheobjtype × objtype), so the read is uncapped and the tool sums it for the exact summary strip.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string Sql = """
        WITH latest AS
        (
            SELECT MAX(collection_time) AS mx
            FROM v_plan_cache_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            collection_time,
            cacheobjtype,
            objtype,
            total_plans,
            total_size_mb,
            single_use_plans,
            single_use_size_mb,
            multi_use_plans,
            multi_use_size_mb,
            avg_use_count
        FROM v_plan_cache_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        ORDER BY total_size_mb DESC, total_plans DESC
        """;

    public static async Task<List<PlanCacheGroupRow>> GetAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = new List<PlanCacheGroupRow>();
        await using var command = postgres.CreateCommand(Sql);
        command.CommandTimeout = commandTimeoutSeconds;
        StoreReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PlanCacheGroupRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
                reader.IsDBNull(9) ? 0m : reader.GetDecimal(9)));
        }

        return rows;
    }
}
