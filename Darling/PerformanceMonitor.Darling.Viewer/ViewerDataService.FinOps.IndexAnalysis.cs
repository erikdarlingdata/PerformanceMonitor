/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Viewer;

/*
 * FinOps Index Analysis sub-tab reads (Stage 3, deferred from the #1372 FinOps port). Lite runs the LIVE
 * dbo.sp_IndexCleanup proc against the target; the headless viewer cannot reach targets, so it reproduces the
 * analysis MONITOR-SIDE: it reads the latest collected per-index snapshot from v_index_object_stats (Stage 1),
 * maps each row into the Common IndexCleanupIndexInput contract, derives the server-fact options
 * (edition -> compression/online support, start time -> uptime days) from the collected server_properties, and
 * runs the pure Common IndexCleanupAnalyzer (Stage 2, 53 tests, incl. the Rule 7 Unique Constraint Replacement
 * family). No live proc, no "install sp_IndexCleanup" prompt, no server-side work — parse-on-read, mirroring the
 * System Events tab. The analysis is string-comparison-heavy so it runs off the UI thread (Task.Run), like the
 * deadlock-graph / system_health shreds. SQL kept in public const so tests pin it.
 */

public sealed partial class ViewerDataService
{
    /// <summary>The latest-snapshot read lives in Storage; this alias keeps the viewer's SQL pins on the same text.</summary>
    public const string IndexObjectStatsLatestSql = DarlingFinOpsIndexAnalysisReader.IndexObjectStatsLatestSql;

    /// <summary>The server-facts read lives in Storage; this alias keeps the viewer's SQL pins on the same text.</summary>
    public const string ServerCompressionInfoSql = DarlingFinOpsIndexAnalysisReader.ServerCompressionInfoSql;

    /// <summary>
    /// The analysis-relevant projection of one <c>v_index_object_stats</c> row — the viewer's own read-layer
    /// shape (NOT the collector Row: the Common contract is deliberately collector-free), mapped into
    /// <see cref="IndexCleanupIndexInput"/> by <see cref="MapToIndexInput"/>. Booleans are nullable because the
    /// Postgres columns are; the mapper collapses a NULL flag to <c>false</c> (an absent guard is not a guard).
    /// </summary>
    public sealed record IndexObjectStatsRow
    {
        public string DatabaseName { get; init; } = "";
        public int DatabaseId { get; init; }
        public string SchemaName { get; init; } = "";
        public int ObjectId { get; init; }
        public string TableName { get; init; } = "";
        public int IndexId { get; init; }
        public string? IndexName { get; init; }
        public string? IndexTypeDesc { get; init; }
        public string? KeyColumns { get; init; }
        public string? IncludedColumns { get; init; }
        public string? FilterDefinition { get; init; }
        public bool? IsUnique { get; init; }
        public bool? IsUniqueConstraint { get; init; }
        public bool? IsPrimaryKey { get; init; }
        public bool? IsForeignKey { get; init; }
        public bool? IsForeignKeyReference { get; init; }
        public bool? IsDisabled { get; init; }
        public bool? IsIndexedView { get; init; }
        public string? DataCompressionDesc { get; init; }
        public bool? OptimizeForSequentialKey { get; init; }
        public short? FillFactor { get; init; }
        public bool? IsPadded { get; init; }
        public bool? AllowPageLocks { get; init; }
        public bool? AllowRowLocks { get; init; }
        public long? UserSeeks { get; init; }
        public long? UserScans { get; init; }
        public long? UserLookups { get; init; }
        public long? UserUpdates { get; init; }
        public decimal? ReservedMb { get; init; }
        public long? TotalRows { get; init; }
        public int? PartitionCount { get; init; }
        public DateTime? SqlServerStartTime { get; init; }
        public long? RowLockWaitCount { get; init; }
        public long? RowLockWaitInMs { get; init; }
        public long? PageLockWaitCount { get; init; }
        public long? PageLockWaitInMs { get; init; }
        public long? PageLatchWaitCount { get; init; }
        public long? PageLatchWaitInMs { get; init; }
        public long? PageIoLatchWaitCount { get; init; }
        public long? PageIoLatchWaitInMs { get; init; }

        /// <summary>Builds the viewer row from the Storage read result.</summary>
        public static IndexObjectStatsRow From(DarlingFinOpsIndexAnalysisReader.IndexObjectStatsDto dto) => new()
        {
            DatabaseName = dto.DatabaseName,
            DatabaseId = dto.DatabaseId,
            SchemaName = dto.SchemaName,
            ObjectId = dto.ObjectId,
            TableName = dto.TableName,
            IndexId = dto.IndexId,
            IndexName = dto.IndexName,
            IndexTypeDesc = dto.IndexTypeDesc,
            KeyColumns = dto.KeyColumns,
            IncludedColumns = dto.IncludedColumns,
            FilterDefinition = dto.FilterDefinition,
            IsUnique = dto.IsUnique,
            IsUniqueConstraint = dto.IsUniqueConstraint,
            IsPrimaryKey = dto.IsPrimaryKey,
            IsForeignKey = dto.IsForeignKey,
            IsForeignKeyReference = dto.IsForeignKeyReference,
            IsDisabled = dto.IsDisabled,
            IsIndexedView = dto.IsIndexedView,
            DataCompressionDesc = dto.DataCompressionDesc,
            OptimizeForSequentialKey = dto.OptimizeForSequentialKey,
            FillFactor = dto.FillFactor,
            IsPadded = dto.IsPadded,
            AllowPageLocks = dto.AllowPageLocks,
            AllowRowLocks = dto.AllowRowLocks,
            UserSeeks = dto.UserSeeks,
            UserScans = dto.UserScans,
            UserLookups = dto.UserLookups,
            UserUpdates = dto.UserUpdates,
            ReservedMb = dto.ReservedMb,
            TotalRows = dto.TotalRows,
            PartitionCount = dto.PartitionCount,
            SqlServerStartTime = dto.SqlServerStartTime,
            RowLockWaitCount = dto.RowLockWaitCount,
            RowLockWaitInMs = dto.RowLockWaitInMs,
            PageLockWaitCount = dto.PageLockWaitCount,
            PageLockWaitInMs = dto.PageLockWaitInMs,
            PageLatchWaitCount = dto.PageLatchWaitCount,
            PageLatchWaitInMs = dto.PageLatchWaitInMs,
            PageIoLatchWaitCount = dto.PageIoLatchWaitCount,
            PageIoLatchWaitInMs = dto.PageIoLatchWaitInMs,
        };

        /// <summary>Converts back to the Storage shape the shared mapper takes.</summary>
        public DarlingFinOpsIndexAnalysisReader.IndexObjectStatsDto ToDto() => new()
        {
            DatabaseName = DatabaseName,
            DatabaseId = DatabaseId,
            SchemaName = SchemaName,
            ObjectId = ObjectId,
            TableName = TableName,
            IndexId = IndexId,
            IndexName = IndexName,
            IndexTypeDesc = IndexTypeDesc,
            KeyColumns = KeyColumns,
            IncludedColumns = IncludedColumns,
            FilterDefinition = FilterDefinition,
            IsUnique = IsUnique,
            IsUniqueConstraint = IsUniqueConstraint,
            IsPrimaryKey = IsPrimaryKey,
            IsForeignKey = IsForeignKey,
            IsForeignKeyReference = IsForeignKeyReference,
            IsDisabled = IsDisabled,
            IsIndexedView = IsIndexedView,
            DataCompressionDesc = DataCompressionDesc,
            OptimizeForSequentialKey = OptimizeForSequentialKey,
            FillFactor = FillFactor,
            IsPadded = IsPadded,
            AllowPageLocks = AllowPageLocks,
            AllowRowLocks = AllowRowLocks,
            UserSeeks = UserSeeks,
            UserScans = UserScans,
            UserLookups = UserLookups,
            UserUpdates = UserUpdates,
            ReservedMb = ReservedMb,
            TotalRows = TotalRows,
            PartitionCount = PartitionCount,
            SqlServerStartTime = SqlServerStartTime,
            RowLockWaitCount = RowLockWaitCount,
            RowLockWaitInMs = RowLockWaitInMs,
            PageLockWaitCount = PageLockWaitCount,
            PageLockWaitInMs = PageLockWaitInMs,
            PageLatchWaitCount = PageLatchWaitCount,
            PageLatchWaitInMs = PageLatchWaitInMs,
            PageIoLatchWaitCount = PageIoLatchWaitCount,
            PageIoLatchWaitInMs = PageIoLatchWaitInMs,
        };
    }

    /// <summary>Maps a collected snapshot row into the analyzer's per-index input contract (nullable flag -&gt; false).</summary>
    public static IndexCleanupIndexInput MapToIndexInput(IndexObjectStatsRow row) =>
        DarlingFinOpsIndexAnalysisReader.MapToIndexInput(row.ToDto());

    /// <summary>Derives the analyzer options from the collected server facts; the rules live in the Storage reader.</summary>
    public static IndexCleanupOptions DeriveIndexCleanupOptions(
        int? engineEdition,
        string? productVersion,
        DateTime? sqlServerStartTime,
        DateTime referenceLocalTime) =>
        DarlingFinOpsIndexAnalysisReader.DeriveIndexCleanupOptions(engineEdition, productVersion, sqlServerStartTime, referenceLocalTime);

    /// <summary>Parses the leading major-version integer from a ProductVersion string (e.g. "16.0.1000.6" -&gt; 16); 0 when unparseable.</summary>
    public static int ParseMajorVersion(string? productVersion) =>
        DarlingFinOpsIndexAnalysisReader.ParseMajorVersion(productVersion);

    /// <summary>Reads the latest per-index snapshot for the server and maps each row to the analyzer input contract.</summary>
    public Task<List<IndexCleanupIndexInput>> GetIndexCleanupInputsAsync(int serverId, CancellationToken cancellationToken = default) =>
        DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>Reads the server's latest collected edition/version/start-time facts and derives the analyzer options.</summary>
    public Task<IndexCleanupOptions> GetIndexCleanupOptionsAsync(int serverId, CancellationToken cancellationToken = default) =>
        DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>
    /// The full monitor-side index-cleanup analysis for one server. Returns the analyzer result verbatim; the tab
    /// projects it into the grids, rollup and banners.
    /// </summary>
    public Task<IndexCleanupAnalysisResult> GetIndexAnalysisAsync(int serverId, CancellationToken cancellationToken = default) =>
        DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>Projects the analyzer recommendations into the detail-grid view-models.</summary>
    public static List<IndexCleanupRecommendationRow> ProjectRecommendations(IndexCleanupAnalysisResult result) =>
        result.Recommendations.Select(r => new IndexCleanupRecommendationRow(r)).ToList();

    /// <summary>
    /// Projects the analyzer rollups into the summary-grid view-models: the overall "ALL DATABASES" total first,
    /// then one row per database.
    /// </summary>
    public static List<IndexCleanupRollupRow> ProjectRollups(IndexCleanupAnalysisResult result)
    {
        var rows = new List<IndexCleanupRollupRow>
        {
            new(result.OverallRollup, "ALL DATABASES"),
        };
        rows.AddRange(result.DatabaseRollups.Select(r => new IndexCleanupRollupRow(r, r.DatabaseName ?? "")));
        return rows;
    }
}

/// <summary>
/// One recommendation row for the Index Analysis detail grid — a flat, display-formatted projection of the
/// Common <see cref="IndexCleanupRecommendation"/>. <see cref="ActionDisplay"/> collapses (Action, ResultKind)
/// into the single operation label the tab filters by (DISABLE / MAKE UNIQUE / MERGE / DROP CONSTRAINT /
/// COMPRESS / REVIEW); REVIEW rows carry an empty <see cref="Script"/> (informational only — a recognized-but-
/// not-consolidated Reverse Duplicate / Equal-Except-Filter relationship).
/// </summary>
public sealed class IndexCleanupRecommendationRow
{
    private readonly IndexCleanupRecommendation _rec;

    public IndexCleanupRecommendationRow(IndexCleanupRecommendation rec)
    {
        _rec = rec;
    }

    public string DatabaseName => _rec.DatabaseName;
    public string SchemaName => _rec.SchemaName;
    public string TableName => _rec.TableName;
    public string IndexName => _rec.IndexName;
    public string ConsolidationRule => _rec.ConsolidationRule ?? "";
    public string ActionDisplay => ActionLabelFor(_rec.Action, _rec.ResultKind);
    public string ResultKindDisplay => ResultKindLabelFor(_rec.ResultKind);
    public string TargetIndexName => _rec.TargetIndexName ?? "";
    public string SupersededBy => _rec.SupersededBy ?? "";
    public string AdditionalInfo => _rec.AdditionalInfo ?? "";
    public decimal IndexSizeGb => _rec.IndexSizeGb;
    public long IndexRows => _rec.IndexRows;
    public long IndexReads => _rec.IndexReads;
    public long IndexWrites => _rec.IndexWrites;
    public bool CanCompress => _rec.CanCompress;
    public string OriginalIndexDefinition => _rec.OriginalIndexDefinition;
    public string Script => _rec.Script;

    /// <summary>The single filterable operation label mirroring sp_IndexCleanup's script buckets; the mapping lives in the Storage reader.</summary>
    public static string ActionLabelFor(IndexCleanupAction action, IndexCleanupResultKind resultKind) =>
        DarlingFinOpsIndexAnalysisReader.ActionLabelFor(action, resultKind);

    /// <summary>The display label for a script bucket.</summary>
    public static string ResultKindLabelFor(IndexCleanupResultKind resultKind) =>
        DarlingFinOpsIndexAnalysisReader.ResultKindLabelFor(resultKind);
}

/// <summary>
/// One reclaimable-space rollup row for the Index Analysis summary grid — a flat projection of the Common
/// <see cref="IndexCleanupRollup"/> plus its scope label (a database name, or "ALL DATABASES" for the overall
/// total). Mirrors sp_IndexCleanup's <c>#index_reporting_stats</c> DATABASE row.
/// </summary>
public sealed class IndexCleanupRollupRow
{
    private readonly IndexCleanupRollup _rollup;

    public IndexCleanupRollupRow(IndexCleanupRollup rollup, string scope)
    {
        _rollup = rollup;
        Scope = scope;
    }

    /// <summary>Database name, or "ALL DATABASES" for the overall total.</summary>
    public string Scope { get; }

    public int TablesAnalyzed => _rollup.TablesAnalyzed;
    public int IndexCount => _rollup.IndexCount;
    public decimal TotalSizeGb => _rollup.TotalSizeGb;
    public long TotalRows => _rollup.TotalRows;
    public int IndexesToDisable => _rollup.IndexesToDisable;
    public int IndexesToMerge => _rollup.IndexesToMerge;
    public int CompressableIndexes => _rollup.CompressableIndexes;
    public int UnusedIndexes => _rollup.UnusedIndexes;
    public decimal UnusedSizeGb => _rollup.UnusedSizeGb;
    public decimal CompressionMinSavingsGb => _rollup.CompressionMinSavingsGb;
    public decimal CompressionMaxSavingsGb => _rollup.CompressionMaxSavingsGb;
    public decimal TotalMinSavingsGb => _rollup.TotalMinSavingsGb;
    public decimal TotalMaxSavingsGb => _rollup.TotalMaxSavingsGb;

    /*
     * Workload-impact columns (Reads / Writes / Lock Waits / Latch Waits), ported from sp_IndexCleanup's
     * #index_reporting_stats. The proc surfaces these at its DATABASE level but emits 'N/A' at the SUMMARY
     * level (it does not populate the workload counters there), so the overall ("ALL DATABASES") row renders
     * 'N/A' to match Lite exactly; the per-database rows render the real aggregates. The formulas mirror the
     * proc's DATABASE-level display (reads_breakdown / writes / lock_wait_count / avg_lock_wait_ms /
     * latch_wait_count / avg_latch_wait_ms).
     */
    private bool IsOverall => _rollup.DatabaseName is null;

    private long TotalReads => _rollup.UserSeeks + _rollup.UserScans + _rollup.UserLookups;

    /// <summary>Total reads with the seeks/scans/lookups breakdown (e.g. "1,234 (1,000 seeks, 200 scans, 34 lookups)"); 'N/A' on the overall row.</summary>
    public string ReadsBreakdown =>
        IsOverall
            ? "N/A"
            : $"{TotalReads:N0} ({_rollup.UserSeeks:N0} seeks, {_rollup.UserScans:N0} scans, {_rollup.UserLookups:N0} lookups)";

    /// <summary>Total <c>user_updates</c> across analyzed indexes; 'N/A' on the overall row.</summary>
    public string Writes => IsOverall ? "N/A" : _rollup.TotalWrites.ToString("N0");

    /// <summary>Row + page lock waits across analyzed indexes; 'N/A' on the overall row.</summary>
    public string LockWaitCount => IsOverall ? "N/A" : _rollup.LockWaitCount.ToString("N0");

    /// <summary>Average lock wait (ms/wait) = lock_wait_in_ms / lock_wait_count; "0" when there were no waits; 'N/A' on the overall row.</summary>
    public string AvgLockWaitMs =>
        IsOverall
            ? "N/A"
            : _rollup.LockWaitCount > 0
                ? ((decimal)_rollup.LockWaitInMs / _rollup.LockWaitCount).ToString("N2")
                : "0";

    /// <summary>Page + page-IO latch waits across analyzed indexes; 'N/A' on the overall row.</summary>
    public string LatchWaitCount => IsOverall ? "N/A" : _rollup.LatchWaitCount.ToString("N0");

    /// <summary>Average latch wait (ms/wait) = latch_wait_in_ms / latch_wait_count; "0" when there were no waits; 'N/A' on the overall row.</summary>
    public string AvgLatchWaitMs =>
        IsOverall
            ? "N/A"
            : _rollup.LatchWaitCount > 0
                ? ((decimal)_rollup.LatchWaitInMs / _rollup.LatchWaitCount).ToString("N2")
                : "0";

    /* Numeric sort keys for the workload columns — the overall ('N/A') row sorts below every real value, like
       Lite's NumericSortHelper parse of "N/A" (→ -1). ReadsBreakdown sorts as text (no key), matching Lite. */
    public decimal WritesSort => IsOverall ? -1m : _rollup.TotalWrites;
    public decimal LockWaitCountSort => IsOverall ? -1m : _rollup.LockWaitCount;
    public decimal AvgLockWaitMsSort => IsOverall ? -1m : (_rollup.LockWaitCount > 0 ? (decimal)_rollup.LockWaitInMs / _rollup.LockWaitCount : 0m);
    public decimal LatchWaitCountSort => IsOverall ? -1m : _rollup.LatchWaitCount;
    public decimal AvgLatchWaitMsSort => IsOverall ? -1m : (_rollup.LatchWaitCount > 0 ? (decimal)_rollup.LatchWaitInMs / _rollup.LatchWaitCount : 0m);
}
