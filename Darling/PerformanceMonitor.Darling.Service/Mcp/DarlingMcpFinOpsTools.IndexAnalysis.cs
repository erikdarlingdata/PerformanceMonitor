/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpFinOpsTools
{
    internal const string IndexAnalysisView = "index_analysis";

    internal const string IndexAnalysisViewLine =
        "index_analysis: index cleanup findings and reclaimable space.";

    internal const string IndexAnalysisViewGuide =
        "index_analysis runs the monitor-side sp_IndexCleanup reproduction over each database's newest collected index snapshot; each databases entry carries captured_at, when that snapshot was collected (a database that left collection scope shows an older one), and overall carries none; hours_back other than 24 is refused. overall and databases are the reclaimable-space roll-ups in GB (3 decimals); workload counters are per database only. recommendations is ordered by index size, then name; limit caps it, recommendation_count and counts_by_action count all findings, and truncated says when rows were cut. databases lists at most 13, largest total_max_savings_gb first, and databases_truncated says when more exist; database_name reads any database, including one outside those 13. script and original_index_definition are cut to 300 characters unless full_text=true, which returns them in full with no size cap; the *_truncated flags say so. notes carries the uptime and dedupe-only caveats and the analyzer's stated limitations. REVIEW rows carry no script. Text numbers use the invariant culture.";

    /// <summary>The longest <c>script</c> or <c>original_index_definition</c> text kept when <c>full_text</c> is false.</summary>
    internal const int IndexAnalysisTextCap = 300;

    /// <summary>The fixed ceiling on <c>databases</c>: the largest count whose default response (ten full-length recommendations, every note, maximum-width figures and 60-character names) stays under 30,720 bytes.</summary>
    internal const int MaxIndexAnalysisDatabases = 13;

    internal const string IndexAnalysisUptimeNote =
        "Server uptime is under 14 days — index usage data may be incomplete, so some \"Unused Index\" findings could be premature.";

    internal const string IndexAnalysisDedupeNote =
        "Dedupe-only mode is in effect — unused indexes were NOT flagged (server uptime is 7 days or less). Only duplicate/consolidation and compression findings are shown.";

    private static readonly string[] IndexAnalysisActions = ["COMPRESS", "DISABLE", "DROP CONSTRAINT", "MAKE UNIQUE", "MERGE", "REVIEW"];

    /// <summary>Each optional parameter and the only views that accept it; every other view refuses it.</summary>
    private static readonly (string Parameter, string[] Views)[] OptionalParamViews =
    [
        ("database_name", [IndexAnalysisView, StorageGrowthView]),
        ("full_text", [IndexAnalysisView]),
        ("object_name", [StorageGrowthView]),
    ];

    /// <summary>
    /// Refuses <c>database_name</c>, <c>full_text</c> and <c>object_name</c> on every view that does not accept them,
    /// in that order, naming the views that do.
    /// </summary>
    internal static (string Parameter, string Message)? OptionalParamMisuse(string view, string? databaseName, bool fullText, string? objectName = null)
    {
        foreach (var (parameter, views) in OptionalParamViews)
        {
            var given = parameter switch
            {
                "database_name" => databaseName != null,
                "full_text" => fullText,
                "object_name" => objectName != null,
                _ => throw new UnreachableException(parameter),
            };
            if (!given || views.Contains(view)) continue;
            var only = views.Length == 1 ? $"view {views[0]}" : $"views {string.Join(" and ", views)}";
            return (parameter, $"{parameter} applies only to {only}; omit it for view {view}.");
        }
        return null;
    }

    /// <summary>Cuts <paramref name="text"/> to <paramref name="cap"/> characters plus an ellipsis, only when it is longer than the cap.</summary>
    internal static (string Text, bool Truncated) TruncateIndexAnalysisText(string? text, int cap)
    {
        if (text == null) return ("", false);
        return text.Length > cap ? (text[..McpHelpers.TextElementCutLength(text, cap)] + "…", true) : (text, false);
    }

    /// <summary>Treats an empty or whitespace-only optional text parameter (<c>database_name</c>, <c>object_name</c>) as not given.</summary>
    internal static string? NormalizeOptionalText(string? text) =>
        string.IsNullOrWhiteSpace(text) ? null : text;

    /// <summary>The recommendation ordering: size, then the full name chain, action label, consolidation rule (nulls first) and index id, so a cut never depends on analyzer order.</summary>
    internal static List<IndexCleanupRecommendation> OrderIndexAnalysisRecommendations(IEnumerable<IndexCleanupRecommendation> rows) =>
        rows.OrderByDescending(r => r.IndexSizeGb)
            .ThenBy(r => r.DatabaseName, StringComparer.Ordinal)
            .ThenBy(r => r.SchemaName, StringComparer.Ordinal)
            .ThenBy(r => r.TableName, StringComparer.Ordinal)
            .ThenBy(r => r.IndexName, StringComparer.Ordinal)
            .ThenBy(r => DarlingFinOpsIndexAnalysisReader.ActionLabelFor(r.Action, r.ResultKind), StringComparer.Ordinal)
            .ThenBy(r => r.ConsolidationRule ?? "", StringComparer.Ordinal)
            .ThenBy(r => r.IndexId)
            .ToList();

    /// <summary>The database ordering: largest possible reclaim, then name.</summary>
    internal static List<IndexCleanupRollup> OrderIndexAnalysisDatabases(IEnumerable<IndexCleanupRollup> rows) =>
        rows.OrderByDescending(r => r.TotalMaxSavingsGb)
            .ThenBy(r => r.DatabaseName, StringComparer.Ordinal)
            .ToList();

    private static decimal Gb(decimal value) => Math.Round(value, 3, MidpointRounding.AwayFromZero);

    private static decimal AvgWait(long waitInMs, long waitCount) =>
        Math.Round(IndexCleanupRollupFigures.AverageWaitMs(waitInMs, waitCount), 2, MidpointRounding.AwayFromZero);

    /// <summary>One roll-up row; the workload counters are included only for a per-database row.</summary>
    internal static object IndexAnalysisRollupRow(IndexCleanupRollup r, bool withWorkload, DateTime? capturedAt = null)
    {
        if (!withWorkload)
        {
            return new
            {
                tables_analyzed = r.TablesAnalyzed,
                index_count = r.IndexCount,
                total_size_gb = Gb(r.TotalSizeGb),
                total_rows = r.TotalRows,
                indexes_to_disable = r.IndexesToDisable,
                indexes_to_merge = r.IndexesToMerge,
                compressable_indexes = r.CompressableIndexes,
                unused_indexes = r.UnusedIndexes,
                unused_size_gb = Gb(r.UnusedSizeGb),
                compression_min_savings_gb = Gb(r.CompressionMinSavingsGb),
                compression_max_savings_gb = Gb(r.CompressionMaxSavingsGb),
                total_min_savings_gb = Gb(r.TotalMinSavingsGb),
                total_max_savings_gb = Gb(r.TotalMaxSavingsGb),
            };
        }

        return new
        {
            database_name = r.DatabaseName,
            /* When this database's newest snapshot was collected (null only if the read named no such database). */
            captured_at = capturedAt is { } at ? McpHelpers.FormatEffectiveStart(at) : null,
            tables_analyzed = r.TablesAnalyzed,
            index_count = r.IndexCount,
            total_size_gb = Gb(r.TotalSizeGb),
            total_rows = r.TotalRows,
            indexes_to_disable = r.IndexesToDisable,
            indexes_to_merge = r.IndexesToMerge,
            compressable_indexes = r.CompressableIndexes,
            unused_indexes = r.UnusedIndexes,
            unused_size_gb = Gb(r.UnusedSizeGb),
            compression_min_savings_gb = Gb(r.CompressionMinSavingsGb),
            compression_max_savings_gb = Gb(r.CompressionMaxSavingsGb),
            total_min_savings_gb = Gb(r.TotalMinSavingsGb),
            total_max_savings_gb = Gb(r.TotalMaxSavingsGb),
            user_seeks = r.UserSeeks,
            user_scans = r.UserScans,
            user_lookups = r.UserLookups,
            total_reads = r.UserSeeks + r.UserScans + r.UserLookups,
            writes = r.TotalWrites,
            lock_wait_count = r.LockWaitCount,
            avg_lock_wait_ms = AvgWait(r.LockWaitInMs, r.LockWaitCount),
            latch_wait_count = r.LatchWaitCount,
            avg_latch_wait_ms = AvgWait(r.LatchWaitInMs, r.LatchWaitCount),
        };
    }

    /// <summary>One recommendation row; a REVIEW row carries no script.</summary>
    internal static object IndexAnalysisRecommendationRow(IndexCleanupRecommendation r, bool fullText)
    {
        var review = r.ResultKind == IndexCleanupResultKind.Review;
        var (script, scriptCut) = fullText ? (review ? "" : r.Script, false) : TruncateIndexAnalysisText(review ? "" : r.Script, IndexAnalysisTextCap);
        var (definition, definitionCut) = fullText ? (r.OriginalIndexDefinition, false) : TruncateIndexAnalysisText(r.OriginalIndexDefinition, IndexAnalysisTextCap);
        return new
        {
            database_name = r.DatabaseName,
            schema_name = r.SchemaName,
            table_name = r.TableName,
            index_name = r.IndexName,
            index_id = r.IndexId,
            action = DarlingFinOpsIndexAnalysisReader.ActionLabelFor(r.Action, r.ResultKind),
            result_kind = DarlingFinOpsIndexAnalysisReader.ResultKindLabelFor(r.ResultKind),
            consolidation_rule = r.ConsolidationRule,
            target_index_name = r.TargetIndexName,
            superseded_by = r.SupersededBy,
            missing_included_columns = r.MissingIncludedColumns,
            additional_info = r.AdditionalInfo,
            index_size_gb = Gb(r.IndexSizeGb),
            index_rows = r.IndexRows,
            index_reads = r.IndexReads,
            index_writes = r.IndexWrites,
            can_compress = r.CanCompress,
            is_foreign_key = r.IsForeignKey,
            script_omits_partition_placement = r.ScriptOmitsPartitionPlacement,
            script,
            script_truncated = scriptCut,
            original_index_definition = definition,
            definition_truncated = definitionCut,
        };
    }

    /// <summary>
    /// Builds the payload from an analysis result, or an empty status when the (filtered) result holds no
    /// roll-ups. A null <paramref name="emptyStatus"/> falls back to the generic empty message.
    /// </summary>
    internal static string BuildIndexAnalysisPayload(
        string serverName, IndexCleanupAnalysisResult result, int limit, string? databaseName, bool fullText, string? emptyStatus,
        IReadOnlyDictionary<int, DateTime>? snapshotTimes = null)
    {
        if (result.DatabaseRollups.Count == 0)
        {
            return emptyStatus
                ?? McpHelpers.Status("empty", "No index statistics were collected for this server yet, so there is no index analysis.");
        }

        databaseName = NormalizeOptionalText(databaseName);
        var filtered = databaseName != null;
        var rollups = filtered
            ? result.DatabaseRollups.Where(r => string.Equals(r.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase)).ToList()
            : result.DatabaseRollups.ToList();
        if (filtered && rollups.Count == 0)
            return McpHelpers.Status("empty", $"No analyzed indexes for database '{databaseName}' in the latest snapshot.");

        var recs = filtered
            ? result.Recommendations.Where(r => string.Equals(r.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase))
            : result.Recommendations;
        var ordered = OrderIndexAnalysisRecommendations(recs);
        var dbs = OrderIndexAnalysisDatabases(rollups);

        var counts = new Dictionary<string, int>();
        foreach (var a in IndexAnalysisActions) counts[a] = 0;
        foreach (var r in ordered)
        {
            var label = DarlingFinOpsIndexAnalysisReader.ActionLabelFor(r.Action, r.ResultKind);
            counts[label] = counts.GetValueOrDefault(label) + 1;
        }

        var notes = new List<string>();
        if (result.UptimeWarning) notes.Add(IndexAnalysisUptimeNote);
        if (result.DedupeOnlyApplied) notes.Add(IndexAnalysisDedupeNote);
        notes.AddRange(result.Notes);

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            view = IndexAnalysisView,
            database_name = databaseName,
            full_text = fullText,
            text_cap_chars = fullText ? (int?)null : IndexAnalysisTextCap,
            uptime_warning = result.UptimeWarning,
            dedupe_only_applied = result.DedupeOnlyApplied,
            notes,
            overall = filtered ? null : IndexAnalysisRollupRow(result.OverallRollup, false),
            overall_workload_reason = filtered ? null : "workload counters are reported per database only, as sp_IndexCleanup does",
            database_count = dbs.Count,
            databases_truncated = dbs.Count > MaxIndexAnalysisDatabases,
            databases = dbs.Take(MaxIndexAnalysisDatabases).Select(d => IndexAnalysisRollupRow(d, true, SnapshotTimeOf(snapshotTimes, d.DatabaseId))).ToList(),
            recommendation_count = ordered.Count,
            truncated = ordered.Count > limit,
            limit,
            counts_by_action = counts,
            recommendations = ordered.Take(limit).Select(r => IndexAnalysisRecommendationRow(r, fullText)).ToList(),
        }, McpHelpers.JsonOptions);
    }

    private static DateTime? SnapshotTimeOf(IReadOnlyDictionary<int, DateTime>? times, int? databaseId)
    {
        if (times == null || databaseId == null || !times.TryGetValue(databaseId.Value, out var at))
        {
            return null;
        }

        return at;
    }

    private static async Task<string> ReadIndexAnalysisAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int hoursBack, int limit,
        string? databaseName, bool fullText, CancellationToken ct)
    {
        if (hoursBack != 24)
            return McpHelpers.Refusal("hours_back",
                $"Invalid hours_back value '{hoursBack}': view {IndexAnalysisView} reads each database's newest collected index snapshot; hours_back does not apply. Omit it or pass 24.");

        var read = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisWithSnapshotTimesAsync(
            postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, ct);
        var result = read.Result;
        string? notCollected = null;
        if (result.DatabaseRollups.Count == 0)
        {
            notCollected = await DarlingEngineCapability.NotCollectedStatusAsync(
                postgres, resolved.ServerId, resolved.ServerName, "index_object_stats", ct);
        }

        return BuildIndexAnalysisPayload(resolved.ServerName, result, limit, databaseName, fullText, notCollected, read.SnapshotTimes);
    }
}
