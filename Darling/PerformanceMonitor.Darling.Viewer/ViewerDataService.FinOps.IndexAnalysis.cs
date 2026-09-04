/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

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
    /// <summary>
    /// The latest collected snapshot of EVERY index for one server: DISTINCT ON keeps the newest row per
    /// (database_id, object_id, index_id) so the analyzer sees each index exactly once (never the whole
    /// history). Per-database collection cycles can carry different collection_times, so the dedupe keys on the
    /// index identity, not a single server-wide MAX(collection_time). $1 server_id. Column order is the read
    /// contract for <see cref="ReadIndexObjectStatsRow"/>.
    /// </summary>
    public const string IndexObjectStatsLatestSql = @"
SELECT DISTINCT ON (database_id, object_id, index_id)
    database_name,
    database_id,
    schema_name,
    object_id,
    table_name,
    index_id,
    index_name,
    index_type_desc,
    key_columns,
    included_columns,
    filter_definition,
    is_unique,
    is_unique_constraint,
    is_primary_key,
    is_foreign_key,
    is_foreign_key_reference,
    is_disabled,
    is_indexed_view,
    data_compression_desc,
    optimize_for_sequential_key,
    fill_factor,
    is_padded,
    allow_page_locks,
    allow_row_locks,
    user_seeks,
    user_scans,
    user_lookups,
    user_updates,
    reserved_mb,
    total_rows,
    partition_count,
    sqlserver_start_time,
    /* Operational lock/latch counters (dm_db_index_operational_stats), appended after the pre-existing
       columns so ordinals 0-31 are unchanged. Feed the reclaimable-space rollup's workload-impact columns
       (Lock Waits / Latch Waits), the same collected data the FinOps Locking sub-tab already reads. */
    row_lock_wait_count,
    row_lock_wait_in_ms,
    page_lock_wait_count,
    page_lock_wait_in_ms,
    page_latch_wait_count,
    page_latch_wait_in_ms,
    page_io_latch_wait_count,
    page_io_latch_wait_in_ms
FROM v_index_object_stats
WHERE server_id = $1
ORDER BY database_id, object_id, index_id, collection_time DESC";

    /// <summary>
    /// The server's latest edition / version / start-time facts for the analyzer options — the same three
    /// SERVERPROPERTY-derived values sp_IndexCleanup reads at runtime, sourced from the collected
    /// server_properties (Darling has no v_server_properties view; the base table is bare-name resolved via the
    /// connection Search Path, exactly like the Utilization/Inventory reads). $1 server_id.
    /// </summary>
    public const string ServerCompressionInfoSql = @"
SELECT
    engine_edition,
    product_version,
    sqlserver_start_time
FROM server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

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
    }

    /// <summary>Maps a collected snapshot row into the analyzer's per-index input contract (nullable flag -&gt; false).</summary>
    public static IndexCleanupIndexInput MapToIndexInput(IndexObjectStatsRow row) => new()
    {
        DatabaseName = row.DatabaseName,
        DatabaseId = row.DatabaseId,
        SchemaName = row.SchemaName,
        ObjectId = row.ObjectId,
        TableName = row.TableName,
        IndexId = row.IndexId,
        IndexName = row.IndexName,
        IndexTypeDesc = row.IndexTypeDesc,
        KeyColumns = row.KeyColumns,
        IncludedColumns = row.IncludedColumns,
        FilterDefinition = row.FilterDefinition,
        IsUnique = row.IsUnique ?? false,
        IsUniqueConstraint = row.IsUniqueConstraint ?? false,
        IsPrimaryKey = row.IsPrimaryKey ?? false,
        IsForeignKey = row.IsForeignKey ?? false,
        IsForeignKeyReference = row.IsForeignKeyReference ?? false,
        IsDisabled = row.IsDisabled ?? false,
        IsIndexedView = row.IsIndexedView ?? false,
        DataCompressionDesc = row.DataCompressionDesc,
        OptimizeForSequentialKey = row.OptimizeForSequentialKey ?? false,
        FillFactor = row.FillFactor,
        IsPadded = row.IsPadded ?? false,
        AllowPageLocks = row.AllowPageLocks ?? false,
        AllowRowLocks = row.AllowRowLocks ?? false,
        UserSeeks = row.UserSeeks ?? 0,
        UserScans = row.UserScans ?? 0,
        UserLookups = row.UserLookups ?? 0,
        UserUpdates = row.UserUpdates ?? 0,
        RowLockWaitCount = row.RowLockWaitCount ?? 0,
        RowLockWaitInMs = row.RowLockWaitInMs ?? 0,
        PageLockWaitCount = row.PageLockWaitCount ?? 0,
        PageLockWaitInMs = row.PageLockWaitInMs ?? 0,
        PageLatchWaitCount = row.PageLatchWaitCount ?? 0,
        PageLatchWaitInMs = row.PageLatchWaitInMs ?? 0,
        PageIoLatchWaitCount = row.PageIoLatchWaitCount ?? 0,
        PageIoLatchWaitInMs = row.PageIoLatchWaitInMs ?? 0,
        ReservedMb = row.ReservedMb,
        TotalRows = row.TotalRows,
        PartitionCount = row.PartitionCount,
        SqlServerStartTime = row.SqlServerStartTime,
    };

    /// <summary>
    /// Derives the analyzer options from the collected server facts, reproducing sp_IndexCleanup's runtime
    /// gates: <c>@can_compress</c> = EngineEdition IN (3,5,8) [Enterprise / Azure SQL DB / Managed Instance] OR
    /// (EngineEdition IN (2,4) [Standard / Express family] AND ProductVersion major &gt;= 13 [SQL 2016+]);
    /// <c>@online</c> = EngineEdition IN (3,5,8). The size/read/write/row minimums and dedupe-only match the
    /// proc's parameter DEFAULTS (all 0 / off) — no speculative settings knobs. Uptime days is computed from the
    /// server-LOCAL <c>sqlserver_start_time</c> vs the viewer's local now (both wall-clock, mirroring the proc's
    /// <c>DATEDIFF(DAY, sqlserver_start_time, SYSDATETIME())</c>); it drives the analyzer's &lt;14-day
    /// unused caveat and &lt;=7-day auto-dedupe. When no server_properties row is collected yet, edition is
    /// unknown: fall back to the safe defaults (compression assumed available — 2016+ is the product minimum —
    /// and ONLINE off).
    /// </summary>
    public static IndexCleanupOptions DeriveIndexCleanupOptions(
        int? engineEdition,
        string? productVersion,
        DateTime? sqlServerStartTime,
        DateTime referenceLocalTime)
    {
        bool supportsOnline;
        bool supportsCompression;

        if (engineEdition is { } edition)
        {
            bool isEnterpriseOrAzure = edition is 3 or 5 or 8;
            bool isStandardFamily = edition is 2 or 4;
            supportsOnline = isEnterpriseOrAzure;
            supportsCompression =
                isEnterpriseOrAzure
                || (isStandardFamily && ParseMajorVersion(productVersion) >= 13);
        }
        else
        {
            /* No collected properties row: assume compression is available (SQL 2016+ is the product minimum)
               and keep ONLINE off (the safe default the generated scripts also default to). */
            supportsOnline = false;
            supportsCompression = true;
        }

        double? uptimeDays = null;
        if (sqlServerStartTime is { } start)
        {
            var days = (referenceLocalTime - start).TotalDays;
            if (days >= 0)
            {
                uptimeDays = days;
            }
        }

        return new IndexCleanupOptions
        {
            DedupeOnly = false,
            MinSizeGb = 0m,
            MinReads = 0,
            MinWrites = 0,
            MinRows = 0,
            ServerSupportsCompression = supportsCompression,
            ServerSupportsOnline = supportsOnline,
            ServerUptimeDays = uptimeDays,
            /* CompressionMin/MaxSavingsFactor keep the record defaults (0.20 / 0.60 — sp_IndexCleanup's band). */
        };
    }

    /// <summary>Parses the leading major-version integer from a ProductVersion string (e.g. "16.0.1000.6" -&gt; 16); 0 when unparseable.</summary>
    public static int ParseMajorVersion(string? productVersion)
    {
        if (string.IsNullOrWhiteSpace(productVersion))
        {
            return 0;
        }

        var head = productVersion.Split('.', 2)[0].Trim();
        return int.TryParse(head, NumberStyles.Integer, CultureInfo.InvariantCulture, out var major) ? major : 0;
    }

    /// <summary>Reads one <c>v_index_object_stats</c> row at the <see cref="IndexObjectStatsLatestSql"/> column ordinals.</summary>
    private static IndexObjectStatsRow ReadIndexObjectStatsRow(NpgsqlDataReader reader)
    {
        long? L(int i) => reader.IsDBNull(i) ? null : Convert.ToInt64(reader.GetValue(i), CultureInfo.InvariantCulture);
        int? I(int i) => reader.IsDBNull(i) ? null : Convert.ToInt32(reader.GetValue(i), CultureInfo.InvariantCulture);
        short? Sh(int i) => reader.IsDBNull(i) ? null : Convert.ToInt16(reader.GetValue(i), CultureInfo.InvariantCulture);
        decimal? D(int i) => reader.IsDBNull(i) ? null : Convert.ToDecimal(reader.GetValue(i), CultureInfo.InvariantCulture);
        bool? B(int i) => reader.IsDBNull(i) ? null : reader.GetBoolean(i);
        string? S(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);

        return new IndexObjectStatsRow
        {
            DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
            DatabaseId = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
            SchemaName = reader.IsDBNull(2) ? "" : reader.GetString(2),
            ObjectId = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture),
            TableName = reader.IsDBNull(4) ? "" : reader.GetString(4),
            IndexId = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
            IndexName = S(6),
            IndexTypeDesc = S(7),
            KeyColumns = S(8),
            IncludedColumns = S(9),
            FilterDefinition = S(10),
            IsUnique = B(11),
            IsUniqueConstraint = B(12),
            IsPrimaryKey = B(13),
            IsForeignKey = B(14),
            IsForeignKeyReference = B(15),
            IsDisabled = B(16),
            IsIndexedView = B(17),
            DataCompressionDesc = S(18),
            OptimizeForSequentialKey = B(19),
            FillFactor = Sh(20),
            IsPadded = B(21),
            AllowPageLocks = B(22),
            AllowRowLocks = B(23),
            UserSeeks = L(24),
            UserScans = L(25),
            UserLookups = L(26),
            UserUpdates = L(27),
            ReservedMb = D(28),
            TotalRows = L(29),
            PartitionCount = I(30),
            SqlServerStartTime = reader.IsDBNull(31) ? null : reader.GetDateTime(31),
            RowLockWaitCount = L(32),
            RowLockWaitInMs = L(33),
            PageLockWaitCount = L(34),
            PageLockWaitInMs = L(35),
            PageLatchWaitCount = L(36),
            PageLatchWaitInMs = L(37),
            PageIoLatchWaitCount = L(38),
            PageIoLatchWaitInMs = L(39),
        };
    }

    /// <summary>Reads the latest per-index snapshot for the server and maps each row to the analyzer input contract.</summary>
    public async Task<List<IndexCleanupIndexInput>> GetIndexCleanupInputsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var inputs = new List<IndexCleanupIndexInput>();

        await using var command = _dataSource.CreateCommand(IndexObjectStatsLatestSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            inputs.Add(MapToIndexInput(ReadIndexObjectStatsRow(reader)));
        }

        return inputs;
    }

    /// <summary>Reads the server's latest collected edition/version/start-time facts and derives the analyzer options.</summary>
    public async Task<IndexCleanupOptions> GetIndexCleanupOptionsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        int? engineEdition = null;
        string? productVersion = null;
        DateTime? startTime = null;

        await using var command = _dataSource.CreateCommand(ServerCompressionInfoSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            engineEdition = reader.IsDBNull(0) ? null : Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            productVersion = reader.IsDBNull(1) ? null : reader.GetString(1);
            startTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
        }

        /* sqlserver_start_time is the server's LOCAL wall clock, compared to the viewer's local now — mirrors
           the proc's SYSDATETIME() derivation (both wall-clock; the coarse days measure tolerates any offset). */
        return DeriveIndexCleanupOptions(engineEdition, productVersion, startTime, DateTime.Now);
    }

    /// <summary>
    /// The full monitor-side index-cleanup analysis for one server: reads the latest collected snapshot, derives
    /// the server-fact options, and runs the pure Common analyzer off the UI thread (the dedupe is
    /// string-comparison-heavy). Returns the analyzer result verbatim — the tab projects it into the grids +
    /// rollup + banners.
    /// </summary>
    public async Task<IndexCleanupAnalysisResult> GetIndexAnalysisAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var inputs = await GetIndexCleanupInputsAsync(serverId, cancellationToken);
        var options = await GetIndexCleanupOptionsAsync(serverId, cancellationToken);
        return await Task.Run(() => IndexCleanupAnalyzer.Analyze(inputs, options), cancellationToken);
    }

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

    /// <summary>The single filterable operation label mirroring sp_IndexCleanup's script buckets: MAKE UNIQUE wins over its MERGE bucket; otherwise the result kind names the bucket.</summary>
    public static string ActionLabelFor(IndexCleanupAction action, IndexCleanupResultKind resultKind)
    {
        if (action == IndexCleanupAction.MakeUnique)
        {
            return "MAKE UNIQUE";
        }

        return ResultKindLabelFor(resultKind);
    }

    /// <summary>The display label for a script bucket.</summary>
    public static string ResultKindLabelFor(IndexCleanupResultKind resultKind) => resultKind switch
    {
        IndexCleanupResultKind.Disable => "DISABLE",
        IndexCleanupResultKind.Merge => "MERGE",
        IndexCleanupResultKind.Compress => "COMPRESS",
        IndexCleanupResultKind.Review => "REVIEW",
        IndexCleanupResultKind.DisableConstraint => "DROP CONSTRAINT",
        _ => resultKind.ToString().ToUpperInvariant(),
    };
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
