/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The health of the monitoring store's OWN tables as an MCP read (#8787): which store table carries empty space, which
/// one autovacuum is behind on, which one would gain from a rewrite. <c>get_store_metrics</c> sizes hypertables,
/// aggregates, the payload dimensions and jobs; this adds the plain and partitioned tables and the empty-space estimate.
/// The read is <see cref="DarlingStoreTableHealthReader"/>, shared with the Darling Viewer.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreTableHealthTools
{
    /// <summary>Default rows returned.</summary>
    public const int DefaultTop = 50;

    /// <summary>The sentence every answer carries about what the empty-space figure is.</summary>
    internal const string EstimateNote =
        "estimated_empty_bytes and estimated_empty_share are ESTIMATES from table statistics (the extension-free estimate the "
        + "PostgreSQL target table bloat collector uses), not measurements. A measured figure needs the pgstattuple extension, "
        + "which this read never installs. A table with no usable statistics has a null estimate and a reason, never zero.";

    [McpServerTool(Name = "get_store_table_health"), Description(
        "Gets the health of the monitoring STORE's OWN tables (plain, partitioned, hypertable, continuous aggregate), not a "
        + "monitored server's; no server_name. Per table: size split (heap/index/toast), estimated empty space from table "
        + "statistics (an ESTIMATE; null with a reason when statistics are missing or stale, never zero), dead rows, last "
        + "vacuum/analyze, autovacuum off, 7-day growth where store_metrics records it, and rewrite_candidate. Rewrite "
        + "candidates first. <<GUIDE>> Reads the catalogs of the monitoring store live, one row per store table. "
        + "Hypertable chunks and the relations holding compressed data roll up to the hypertable (chunk_count), a continuous "
        + "aggregate to its view name, and a partitioned table's leaves to the parent (leaf_count); a leaf or chunk is never its own row. "
        + "estimated_empty_bytes / estimated_empty_share come from the same statistics estimate the PostgreSQL target table bloat "
        + "collector runs, and cover only relations with a heap of at least 1 MB and usable statistics: estimate_relations_usable of "
        + "estimate_relations says how much of a roll-up was estimated, and estimate_unavailable_reason says why a row has no "
        + "estimate (below_size_floor, never_analyzed, no_column_statistics, analyze_record_lost, stale_statistics, no_chunks). "
        + "It is NOT n_live_tup over n_live_tup plus n_dead_tup, which reads 100 percent after a vacuum. A measured figure needs "
        + "pgstattuple, which is never installed. rewrite_candidate = estimated share at least 50 percent AND estimated empty bytes "
        + "at least 64 MiB. growth_bytes_7d is null for objects store_metrics does not record. A live read of the catalogs: "
        + "allow a second on a store with many chunks.")]
    public static async Task<string> GetStoreTableHealth(
        NpgsqlDataSource postgres,
        [Description("How many tables to return, in the answer's order. Default 50, max 1000.")] int top = DefaultTop,
        [Description("Return only rewrite candidates. Default false.")] bool candidates_only = false,
        CancellationToken cancellationToken = default)
    {
        var invalidTop = McpHelpers.ValidateTop(top, "top");
        if (invalidTop != null)
        {
            return invalidTop;
        }

        try
        {
            var all = await DarlingStoreTableHealthReader.GetAsync(postgres, cancellationToken);
            var summary = DarlingStoreTableHealthReader.Summarize(all);
            var matching = candidates_only ? all.Where(r => r.RewriteCandidate).ToList() : all;
            var (page, truncated) = McpHelpers.BoundPage(matching, top);

            var noEstimate = summary.WithoutEstimateByReason.Count == 0
                ? "every table has a usable estimate"
                : string.Join(", ", summary.WithoutEstimateByReason.Select(p =>
                    $"{p.Value} {DarlingStoreTableHealthReader.DescribeReason(p.Key)}"));
            var header = string.Create(CultureInfo.InvariantCulture,
                $"{summary.TableCount} store tables, {summary.TotalBytes} bytes in total, {summary.RewriteCandidates} rewrite candidates, "
                + $"{summary.WithoutEstimate} without a usable empty-space estimate ({noEstimate}).");

            return JsonSerializer.Serialize(new
            {
                header,
                estimate_note = EstimateNote,
                store_tables = summary.TableCount,
                store_total_bytes = summary.TotalBytes,
                rewrite_candidates = summary.RewriteCandidates,
                tables_without_estimate = summary.WithoutEstimate,
                without_estimate_by_reason = summary.WithoutEstimateByReason.ToDictionary(p => p.Key, p => p.Value),
                candidates_only,
                top,
                tables_returned = page.Count,
                truncated,
                tables = page.Select(r => new
                {
                    schema_name = r.SchemaName,
                    table_name = r.TableName,
                    kind = r.Kind,
                    total_bytes = r.TotalBytes,
                    heap_bytes = r.HeapBytes,
                    index_bytes = r.IndexBytes,
                    toast_bytes = r.ToastBytes,
                    estimated_rows = r.EstimatedRows,
                    chunk_count = r.ChunkCount,
                    leaf_count = r.LeafCount,
                    estimated_empty_bytes = r.EstimatedEmptyBytes,
                    estimated_empty_share = r.EstimatedEmptyShare,
                    estimate_relations = r.EstimateRelations,
                    estimate_relations_usable = r.EstimateRelationsUsable,
                    estimate_unavailable_reason = r.EstimateUnavailableReason,
                    dead_rows = r.DeadRows,
                    rows_changed_since_analyze = r.RowsChangedSinceAnalyze,
                    last_vacuum = Iso(r.LastVacuum),
                    last_autovacuum = Iso(r.LastAutovacuum),
                    last_analyze = Iso(r.LastAnalyze),
                    last_autoanalyze = Iso(r.LastAutoanalyze),
                    autovacuum_disabled = r.AutovacuumDisabled,
                    growth_bytes_7d = r.GrowthBytes7d,
                    rewrite_candidate = r.RewriteCandidate,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_table_health", ex);
        }
    }

    private static string? Iso(DateTime? value) => value?.ToString("O", CultureInfo.InvariantCulture);
}
