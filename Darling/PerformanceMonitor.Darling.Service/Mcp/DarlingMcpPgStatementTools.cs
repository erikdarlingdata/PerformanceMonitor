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
/// The MCP surface for PostgreSQL query statistics, paired with the <c>pg_statement_stats</c>
/// collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgStatementTools
{
    [McpServerTool(Name = "get_pg_top_queries"), Description("Gets the top PostgreSQL query shapes by total execution time over a time period, for Amazon Aurora PostgreSQL targets. Includes Aurora's I/O source breakdown, which stock PostgreSQL cannot provide: a block 'read' may have come from the distributed storage volume or from the local NVMe Optimized Reads cache, and the two have very different costs. Also reports peak memory per statement, the closest PostgreSQL equivalent of a memory grant, and WAL bytes generated, which has no SQL Server DMV counterpart. Returns query_text for each statement, captured hourly and keyed on queryid, or null when none has been captured yet (a statement first seen minutes ago, or a queryid minted by a major-version upgrade). queryid itself is stable within a major version but changes across a major upgrade — which is exactly why the text is STORED rather than fetched live: after an upgrade the live view no longer holds the old ids, so their text would otherwise be unrecoverable and the history would read as a list of integers. This is a separate tool from get_top_queries_by_cpu, which covers SQL Server.")]
    public static async Task<string> GetPgTopQueries(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 20.")] int limit = 20)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingPgStatementReader.GetPgTopQueriesAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            if (rows.Count == 0)
            {
                return McpHelpers.Status(
                    "unavailable",
                    "No PostgreSQL query statistics for this server and window. If this server is SQL "
                    + "Server, use get_top_queries_by_cpu instead. If it is Aurora PostgreSQL, check that "
                    + "pg_stat_statements is installed in the database the collector connects to — on some "
                    + "clusters it exists only in the application database, not in postgres.");
            }

            var totalTimeMs = rows.Sum(r => r.TotalExecTimeMs);

            var result = rows.Take(limit).Select(r =>
            {
                var storageAndCache = r.StorageBlocksRead + r.OrcacheBlocksHit;
                return new
                {
                    queryid = r.QueryId,
                    database_id = r.DatabaseId,
                    calls = r.Calls,
                    total_exec_time_ms = r.TotalExecTimeMs,
                    avg_exec_time_ms = r.Calls > 0 ? Math.Round((double)r.TotalExecTimeMs / r.Calls, 3) : 0,
                    max_exec_time_ms = Math.Round(r.MaxExecTimeMs, 3),
                    rows_returned = r.RowsReturned,
                    pct_of_total_time = totalTimeMs > 0 ? Math.Round((double)r.TotalExecTimeMs / totalTimeMs * 100, 1) : 0,
                    /* Aurora's I/O split, which is the point of using aurora_stat_statements over the
                       vanilla view. A high orcache share means the reads were cheap local NVMe hits; a
                       high storage share means network round trips to the cluster volume. The community
                       cache-hit ratio cannot distinguish these and so overstates the cost of one and
                       understates the other. */
                    shared_blks_hit = r.SharedBlocksHit,
                    shared_blks_read = r.SharedBlocksRead,
                    storage_blks_read = r.StorageBlocksRead,
                    orcache_blks_hit = r.OrcacheBlocksHit,
                    orcache_hit_pct_of_reads = storageAndCache > 0
                        ? Math.Round((double)r.OrcacheBlocksHit / storageAndCache * 100, 1)
                        : (double?)null,
                    /* Spills. temp blocks are sort/hash spill to disk, NOT temporary tables - those are
                       the local_blks_* family and a different problem. */
                    temp_blks_read = r.TempBlocksRead,
                    temp_blks_written = r.TempBlocksWritten,
                    wal_bytes = r.WalBytes,
                    max_exec_peakmem_bytes = r.MaxPeakMemBytes,
                    // #2219: the statement text, or null when none has been captured for this queryid yet.
                    // Null is honest rather than a placeholder — text refreshes hourly, so a statement first
                    // seen minutes ago has none, and after a major-version upgrade re-keys queryid the new ids
                    // have none until the next refresh. An empty string would read as "the query is blank".
                    query_text = r.QueryText,
                };
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_exec_time_ms = totalTimeMs,
                /* Every counter here covers the window, so a caller can safely divide one by another.
                   Only the two high-water marks are not counters, and saying which is cheaper than
                   letting someone assume max_exec_time_ms is a windowed total. */
                note = "All counters cover the requested window: calls, total_exec_time_ms and "
                     + "rows_returned from stored per-interval deltas, and the block and WAL figures "
                     + "differenced across the window's snapshots. max_exec_time_ms and "
                     + "max_exec_peakmem_bytes are high-water marks, not windowed totals.",
                queries = result,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL query stats failed: {ex.Message}");
        }
    }
}
