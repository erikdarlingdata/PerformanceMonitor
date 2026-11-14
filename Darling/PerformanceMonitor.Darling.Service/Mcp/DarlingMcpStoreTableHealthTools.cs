/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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

    /// <summary>The sections the tool can return.</summary>
    internal static readonly string[] Sections = { "tables", "indexes", "all" };

    [McpServerTool(Name = "get_store_table_health"), Description(
        "Gets the health of the monitoring STORE's OWN tables and indexes, not a monitored server's; no server_name. Per table: size "
        + "split, estimated empty space (an ESTIMATE, null with a reason when statistics are missing or stale, never zero), dead rows, "
        + "autovacuum distance, change rates, whether plain VACUUM can give the space back, and rewrite_candidate (candidates first). "
        + "Per index: size, scans, bloat estimate, validity; plus index coverage for partitioned parents. "
        + "<<GUIDE>> Also per table: plain, partitioned, hypertable and continuous aggregate kinds, the size split (heap, index, toast), visibility map coverage, insert, update, delete and HOT rates since the statistics were reset, last vacuum and analyze, autovacuum off, 7-day growth where store_metrics records it. Per index: scans, index-to-heap ratio, larger_than_heap and rarely_used. Reads the catalogs of the "
        + "monitoring store live. Hypertable chunks and the relations holding compressed data roll up to the hypertable "
        + "(chunk_count), a continuous aggregate to its view name, and a partitioned table's leaves to the parent (leaf_count); a "
        + "leaf or chunk is never its own row, and chunk or leaf indexes that are clones of one parent index are one index row. "
        + "estimated_empty_bytes / estimated_empty_share come from the same statistics estimate the PostgreSQL target table bloat "
        + "collector runs, and cover only relations with a heap of at least 1 MB and usable statistics: estimate_relations_usable of "
        + "estimate_relations says how much of a roll-up was estimated, and estimate_unavailable_reason says why a row has no "
        + "estimate (below_size_floor, never_analyzed, no_column_statistics, analyze_record_lost, stale_statistics, no_chunks). "
        + "It is NOT n_live_tup over n_live_tup plus n_dead_tup, which reads 100 percent after a vacuum. A measured figure needs "
        + "pgstattuple, which is never installed. past_rewrite_floors = estimated share at least 50 percent AND estimated empty bytes "
        + "at least 64 MiB. rewrite_candidate = past_rewrite_floors AND tail_occupied: a live row in the last 64 pages of the file "
        + "means plain VACUUM cannot truncate it, so only a rewrite returns the space (for a roll-up the emptiest relations are "
        + "checked, at most 10: tail_relations_checked of tail_relations_eligible). tail_occupied is null when it was not read or the "
        + "read failed, and rewrite_candidate is then false while past_rewrite_floors stays true. vacuum_trigger_rows is "
        + "threshold + scale_factor x rows from the table's own settings or the server's (capped by the max threshold on PostgreSQL 18): "
        + "compare it with dead_rows, because many dead rows below the trigger are not yet due; relations_past_vacuum_trigger counts "
        + "the relations of a roll-up that are. The churn counters and rates are since stats_since (the statistics reset, or the "
        + "postmaster start when they were never reset, as stats_since_source says) and nothing is recorded; turnover_per_day is "
        + "deletes per day over live rows. rarely_used is only set when the statistics window is at least 7 days old, never for a "
        + "primary key or unique index. Index bloat is the extension-free estimate the target index bloat collector uses. "
        + "growth_bytes_7d is null for objects store_metrics does not record. A live read of the catalogs: allow a second or two on a "
        + "store with many chunks.")]
    public static async Task<string> GetStoreTableHealth(
        NpgsqlDataSource postgres,
        [Description("How many rows to return per section, in the answer's order. Default 50, max 1000.")] int top = DefaultTop,
        [Description("Return only rewrite candidates in the tables section. Default false.")] bool candidates_only = false,
        [Description("Which section: tables, indexes (the per-index rows and the partition leaf index coverage) or all. Default all.")] string? section = null,
        CancellationToken cancellationToken = default)
    {
        var invalidTop = McpHelpers.ValidateTop(top, "top");
        if (invalidTop != null)
        {
            return invalidTop;
        }

        var invalidSection = McpHelpers.ValidateChoice(section, Sections, "section");
        if (invalidSection != null)
        {
            return invalidSection;
        }

        var chosen = string.IsNullOrWhiteSpace(section) ? "all" : section.Trim().ToLowerInvariant();
        var wantTables = chosen is "tables" or "all";
        var wantIndexes = chosen is "indexes" or "all";

        try
        {
            var result = new Dictionary<string, object?>
            {
                ["section"] = chosen,
                ["top"] = top,
            };

            if (wantTables)
            {
                var all = await DarlingStoreTableHealthReader.GetAsync(postgres, cancellationToken);
                var summary = DarlingStoreTableHealthReader.Summarize(all);
                var matching = candidates_only ? all.Where(r => r.RewriteCandidate).ToList() : all;
                var (page, truncated) = McpHelpers.BoundPage(matching, top);

                var noEstimate = summary.WithoutEstimateByReason.Count == 0
                    ? "every table has a usable estimate"
                    : string.Join(", ", summary.WithoutEstimateByReason.Select(p =>
                        $"{p.Value} {DarlingStoreTableHealthReader.DescribeReason(p.Key)}"));
                var pastFloorsUnread = all.Count(r => r.PastRewriteFloors && r.TailOccupied is null);
                var header = string.Create(CultureInfo.InvariantCulture,
                    $"{CountText.Plural(summary.TableCount, "store table")}, {summary.TotalBytes} bytes in total, {CountText.Plural(summary.RewriteCandidates, "rewrite candidate")}, "
                    + $"{CountText.Grouped(summary.WithoutEstimate)} without a usable empty-space estimate ({noEstimate}).")
                    + (pastFloorsUnread > 0
                        ? string.Create(CultureInfo.InvariantCulture, $" {pastFloorsUnread} past the empty-space floors with an unreadable tail.")
                        : string.Empty);

                result["header"] = header;
                result["estimate_note"] = EstimateNote;
                result["store_tables"] = summary.TableCount;
                result["store_total_bytes"] = summary.TotalBytes;
                result["rewrite_candidates"] = summary.RewriteCandidates;
                result["tables_without_estimate"] = summary.WithoutEstimate;
                result["without_estimate_by_reason"] = summary.WithoutEstimateByReason.ToDictionary(p => p.Key, p => p.Value);
                result["without_estimate_reason_text"] = summary.WithoutEstimateByReason.ToDictionary(p => p.Key, p => DarlingStoreTableHealthReader.DescribeReason(p.Key));
                result["candidates_only"] = candidates_only;
                result["tables_returned"] = page.Count;
                result["truncated"] = truncated;
                result["tables"] = page.Select(Table).ToList();
            }

            if (wantIndexes)
            {
                var indexRows = await DarlingStoreTableHealthReader.GetIndexesAsync(postgres, cancellationToken);
                var coverageRows = await DarlingStoreTableHealthReader.GetIndexCoverageAsync(postgres, cancellationToken);
                var (indexPage, indexTruncated) = McpHelpers.BoundPage(indexRows, top);
                var (coveragePage, coverageTruncated) = McpHelpers.BoundPage(coverageRows, top);
                var incomplete = coverageRows.Count(c => c.LeavesMissingIndex > 0 || c.LeavesInvalidIndex > 0);

                result["index_header"] = string.Create(CultureInfo.InvariantCulture,
                    $"{CountText.Plural(indexRows.Count, "store index", "store indexes")}, {indexRows.Count(r => r.LargerThanHeap)} larger than their heap, {indexRows.Count(r => r.RarelyUsed)} rarely used, "
                    + $"{indexRows.Count(r => !r.Valid)} invalid, and {CountText.Plural(incomplete, "partitioned-parent index", "partitioned-parent indexes")} with a leaf missing or invalid.");
                result["indexes_total"] = indexRows.Count;
                result["indexes_returned"] = indexPage.Count;
                result["indexes_truncated"] = indexTruncated;
                result["indexes"] = indexPage.Select(Index).ToList();
                result["partition_index_coverage_total"] = coverageRows.Count;
                result["partition_index_coverage_returned"] = coveragePage.Count;
                result["partition_index_coverage_truncated"] = coverageTruncated;
                result["partition_index_coverage"] = coveragePage.Select(Coverage).ToList();
            }

            return JsonSerializer.Serialize(result, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_table_health", ex);
        }
    }

    private static object Table(DarlingStoreTableHealthReader.StoreTableHealthRow r) => new
    {
        schema_name = r.SchemaName,
        table_name = r.TableName,
        kind = r.Kind,
        total_bytes = r.TotalBytes,
        heap_bytes = r.HeapBytes,
        index_bytes = r.IndexBytes,
        toast_bytes = r.ToastBytes,
        estimated_rows = r.EstimatedRows,
        live_rows = r.LiveRows,
        chunk_count = r.ChunkCount,
        leaf_count = r.LeafCount,
        estimated_empty_bytes = r.EstimatedEmptyBytes,
        estimated_empty_share = r.EstimatedEmptyShare,
        estimate_relations = r.EstimateRelations,
        estimate_relations_usable = r.EstimateRelationsUsable,
        estimate_unavailable_reason = r.EstimateUnavailableReason,
        estimate_unavailable_text = r.EstimateUnavailableReason is null ? null : DarlingStoreTableHealthReader.DescribeReason(r.EstimateUnavailableReason),
        past_rewrite_floors = r.PastRewriteFloors,
        tail_occupied = r.TailOccupied,
        tail_relations_eligible = r.TailRelationsEligible,
        tail_relations_checked = r.TailRelationsChecked,
        tail_relations_occupied = r.TailRelationsOccupied,
        rewrite_candidate = r.RewriteCandidate,
        relpages = r.RelPages,
        relallvisible = r.RelAllVisible,
        relallfrozen = r.RelAllFrozen,
        pct_all_visible = r.PctAllVisible,
        dead_rows = r.DeadRows,
        vacuum_trigger_rows = r.VacuumTriggerRows,
        relations_past_vacuum_trigger = r.RelationsPastVacuumTrigger,
        inserts_since_vacuum = r.InsertsSinceVacuum,
        insert_trigger_rows = r.InsertTriggerRows,
        relations_past_insert_trigger = r.RelationsPastInsertTrigger,
        rows_changed_since_analyze = r.RowsChangedSinceAnalyze,
        last_vacuum = Iso(r.LastVacuum),
        last_autovacuum = Iso(r.LastAutovacuum),
        last_analyze = Iso(r.LastAnalyze),
        last_autoanalyze = Iso(r.LastAutoanalyze),
        autovacuum_disabled = r.AutovacuumDisabled,
        stats_since = Iso(r.StatsSince),
        stats_since_source = r.StatsSinceSource,
        stats_hours = r.StatsHours,
        tuples_inserted = r.TuplesInserted,
        tuples_updated = r.TuplesUpdated,
        tuples_hot_updated = r.TuplesHotUpdated,
        tuples_newpage_updated = r.TuplesNewPageUpdated,
        tuples_deleted = r.TuplesDeleted,
        autovacuum_count = r.AutovacuumCount,
        autoanalyze_count = r.AutoanalyzeCount,
        hot_update_share = r.HotUpdateShare,
        inserts_per_day = r.InsertsPerDay,
        updates_per_day = r.UpdatesPerDay,
        deletes_per_day = r.DeletesPerDay,
        turnover_per_day = r.TurnoverPerDay,
        growth_bytes_7d = r.GrowthBytes7d,
    };

    private static object Index(DarlingStoreTableHealthReader.StoreIndexHealthRow r) => new
    {
        schema_name = r.SchemaName,
        table_name = r.TableName,
        kind = r.Kind,
        index_name = r.IndexName,
        on_compressed_data = r.OnCompressedData,
        index_count = r.IndexCount,
        leaf_count = r.LeafCount,
        bytes = r.Bytes,
        heap_bytes = r.HeapBytes,
        index_to_heap = r.IndexToHeapRatio,
        idx_scan = r.IndexScans,
        stats_since = Iso(r.StatsSince),
        stats_since_source = r.StatsSinceSource,
        is_primary_key = r.IsPrimaryKey,
        is_unique = r.IsUnique,
        valid = r.Valid,
        larger_than_heap = r.LargerThanHeap,
        larger_than_heap_count = r.LargerThanHeapCount,
        rarely_used = r.RarelyUsed,
        estimated_reclaimable_bytes = r.EstimatedReclaimableBytes,
        estimated_bloat_share = r.EstimatedBloatShare,
        estimate_indexes_usable = r.EstimateIndexesUsable,
        estimate_skipped_reason = r.EstimateSkippedReason,
    };

    private static object Coverage(DarlingStoreTableHealthReader.StoreIndexCoverageRow r) => new
    {
        schema_name = r.SchemaName,
        table_name = r.TableName,
        index_name = r.IndexName,
        parent_index_valid = r.ParentIndexValid,
        leaf_count = r.LeafCount,
        leaves_with_index = r.LeavesWithIndex,
        leaves_missing_index = r.LeavesMissingIndex,
        missing_leaves = r.MissingLeaves,
        leaves_invalid_index = r.LeavesInvalidIndex,
        invalid_leaves = r.InvalidLeaves,
    };

    private static string? Iso(DateTime? value) => value?.ToString("O", CultureInfo.InvariantCulture);
}
