/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// One captured per-index row consumed by <see cref="IndexCleanupAnalyzer"/> — the analysis-relevant
    /// subset of the Stage-1 <c>index_object_stats</c> snapshot (the shared
    /// <c>IndexObjectStatsCollector.Row</c>). Kept as a Common-local contract (like
    /// <see cref="SystemHealthParser"/>'s tuple input) so the analyzer has NO dependency on the collector
    /// assembly; the caller maps its read-layer rows into this shape.
    ///
    /// <para><see cref="KeyColumns"/> / <see cref="IncludedColumns"/> arrive in sp_IndexCleanup's EXACT
    /// delimited <c>QUOTENAME</c> form — keys are <c>[a], [b] DESC</c> in key-ordinal order (with a
    /// <c> DESC</c> suffix on descending columns), includes are <c>[c], [d]</c> in column-name order — so
    /// the string-comparison dedupe (Exact/Reverse/Equal-Except-Filter, Key Duplicate, Key
    /// Subset/Superset) ports byte-for-byte and the reconstructed CREATE/MERGE scripts can drop the strings
    /// in verbatim.</para>
    /// </summary>
    public sealed record IndexCleanupIndexInput
    {
        /// <summary>Owning database name (used for scope grouping and script qualification).</summary>
        public string DatabaseName { get; init; } = "";

        /// <summary>Owning database id; with <see cref="ObjectId"/> forms the per-object dedupe scope.</summary>
        public int DatabaseId { get; init; }

        /// <summary>Schema name.</summary>
        public string SchemaName { get; init; } = "";

        /// <summary>Object (table/view) id; the dedupe scope is (<see cref="DatabaseId"/>, <see cref="ObjectId"/>).</summary>
        public int ObjectId { get; init; }

        /// <summary>Table (or indexed-view) name.</summary>
        public string TableName { get; init; } = "";

        /// <summary>Index id. 1 = clustered; &gt;1 = nonclustered; 0 = heap (heaps are not analyzed).</summary>
        public int IndexId { get; init; }

        /// <summary>Index name (null only for a heap, which is excluded from analysis).</summary>
        public string? IndexName { get; init; }

        /// <summary>e.g. <c>CLUSTERED</c>, <c>NONCLUSTERED</c>. Only CLUSTERED/NONCLUSTERED rowstore indexes are analyzed.</summary>
        public string? IndexTypeDesc { get; init; }

        /// <summary>Delimited key list <c>[a], [b] DESC</c> (key-ordinal order, DESC suffix on descending keys).</summary>
        public string? KeyColumns { get; init; }

        /// <summary>Delimited include list <c>[c], [d]</c> (column-name order).</summary>
        public string? IncludedColumns { get; init; }

        /// <summary>Raw filter predicate text (null for a non-filtered index).</summary>
        public string? FilterDefinition { get; init; }

        /// <summary><c>sys.indexes.is_unique</c>.</summary>
        public bool IsUnique { get; init; }

        /// <summary><c>sys.indexes.is_unique_constraint</c> — a hard drop/disable protection.</summary>
        public bool IsUniqueConstraint { get; init; }

        /// <summary><c>sys.indexes.is_primary_key</c> — a hard drop/disable protection.</summary>
        public bool IsPrimaryKey { get; init; }

        /// <summary>A key column backs an OUTGOING foreign key (this is a supporting index). Informational — sp_IndexCleanup does not hard-guard disable on it.</summary>
        public bool IsForeignKey { get; init; }

        /// <summary>A key column is REFERENCED by an incoming foreign key — a hard drop/disable protection (matches sp_IndexCleanup's <c>is_foreign_key_reference</c> guard).</summary>
        public bool IsForeignKeyReference { get; init; }

        /// <summary>Already disabled — excluded from analysis (mirrors sp_IndexCleanup's <c>i.is_disabled = 0</c> filter).</summary>
        public bool IsDisabled { get; init; }

        /// <summary>Index belongs to an indexed view (object type V) rather than a table. Informational; scope is per-object so table and view indexes never compare.</summary>
        public bool IsIndexedView { get; init; }

        /// <summary>Lowest <c>data_compression_desc</c> across the index's partitions (<c>NONE</c> ⇒ at least one uncompressed partition ⇒ a PAGE-compression candidate).</summary>
        public string? DataCompressionDesc { get; init; }

        /// <summary><c>optimize_for_sequential_key</c> (SQL 2019+/Azure). When set, the reconstructed rebuild carries <c>OPTIMIZE_FOR_SEQUENTIAL_KEY = ON</c>.</summary>
        public bool OptimizeForSequentialKey { get; init; }

        /// <summary><c>fill_factor</c> (0 = engine default). Captured for reference; sp_IndexCleanup's generated rebuilds hardcode <c>FILLFACTOR = 100</c>.</summary>
        public short? FillFactor { get; init; }

        /// <summary><c>is_padded</c>. Captured for reference.</summary>
        public bool IsPadded { get; init; }

        /// <summary><c>allow_page_locks</c>. Captured for reference.</summary>
        public bool AllowPageLocks { get; init; }

        /// <summary><c>allow_row_locks</c>. Captured for reference.</summary>
        public bool AllowRowLocks { get; init; }

        /// <summary>Cumulative <c>user_seeks</c> since the last usage-stats reset.</summary>
        public long UserSeeks { get; init; }

        /// <summary>Cumulative <c>user_scans</c>.</summary>
        public long UserScans { get; init; }

        /// <summary>Cumulative <c>user_lookups</c>.</summary>
        public long UserLookups { get; init; }

        /// <summary>Cumulative <c>user_updates</c> (write maintenance cost).</summary>
        public long UserUpdates { get; init; }

        /// <summary>Cumulative <c>row_lock_wait_count</c> (dm_db_index_operational_stats). Summed with <see cref="PageLockWaitCount"/> for the per-scope Lock Waits rollup.</summary>
        public long RowLockWaitCount { get; init; }

        /// <summary>Cumulative <c>row_lock_wait_in_ms</c>. With <see cref="PageLockWaitInMs"/> drives the average lock wait (ms/wait).</summary>
        public long RowLockWaitInMs { get; init; }

        /// <summary>Cumulative <c>page_lock_wait_count</c>.</summary>
        public long PageLockWaitCount { get; init; }

        /// <summary>Cumulative <c>page_lock_wait_in_ms</c>.</summary>
        public long PageLockWaitInMs { get; init; }

        /// <summary>Cumulative <c>page_latch_wait_count</c>. Summed with <see cref="PageIoLatchWaitCount"/> for the per-scope Latch Waits rollup.</summary>
        public long PageLatchWaitCount { get; init; }

        /// <summary>Cumulative <c>page_latch_wait_in_ms</c>. With <see cref="PageIoLatchWaitInMs"/> drives the average latch wait (ms/wait).</summary>
        public long PageLatchWaitInMs { get; init; }

        /// <summary>Cumulative <c>page_io_latch_wait_count</c>.</summary>
        public long PageIoLatchWaitCount { get; init; }

        /// <summary>Cumulative <c>page_io_latch_wait_in_ms</c>.</summary>
        public long PageIoLatchWaitInMs { get; init; }

        /// <summary>Reserved footprint in MB (summed across partitions). Divided by 1024 for the GB figures.</summary>
        public decimal? ReservedMb { get; init; }

        /// <summary>Row count (per partition sum). The base row (index_id 0/1) drives the object's <c>@min_rows</c> gate.</summary>
        public long? TotalRows { get; init; }

        /// <summary>Partition count; &gt;1 drives <c>REBUILD PARTITION = ALL</c> in the compression script (a proxy — the scheme name is not captured).</summary>
        public int? PartitionCount { get; init; }

        /// <summary>SQL Server start time (for the &lt;14-day uptime caveat / &lt;=7-day auto-dedupe, when the caller does not pass <see cref="IndexCleanupOptions.ServerUptimeDays"/> directly).</summary>
        public DateTime? SqlServerStartTime { get; init; }
    }

    /// <summary>
    /// Analyzer inputs that mirror sp_IndexCleanup's parameters and server-derived flags. Because the
    /// analyzer is database-free, the edition/uptime facts SQL Server derives at runtime are passed in.
    /// </summary>
    public sealed record IndexCleanupOptions
    {
        /// <summary>Only run the consolidation rules; do NOT flag unused indexes (sp_IndexCleanup's <c>@dedupe_only</c>).</summary>
        public bool DedupeOnly { get; init; }

        /// <summary>Minimum index size (GB) for a PAGE-compression candidate (sp_IndexCleanup's <c>@min_size_gb</c>, applied per index here).</summary>
        public decimal MinSizeGb { get; init; }

        /// <summary>Minimum object read count to analyze a table (applied only when &gt; 0; mirrors <c>@min_reads</c>).</summary>
        public long MinReads { get; init; }

        /// <summary>Minimum object write count to analyze a table (applied only when &gt; 0; mirrors <c>@min_writes</c>).</summary>
        public long MinWrites { get; init; }

        /// <summary>Minimum object row count to analyze a table (mirrors <c>@min_rows</c>).</summary>
        public long MinRows { get; init; }

        /// <summary>
        /// Server uptime in days. Drives the "usage data may be incomplete" caveat on unused indexes
        /// (&lt; 14 days) and sp_IndexCleanup's auto-enable of dedupe-only (&lt;= 7 days). When null, the
        /// analyzer derives it from the max <see cref="IndexCleanupIndexInput.SqlServerStartTime"/> and
        /// <see cref="ReferenceTimeUtc"/>; if that is also indeterminable, no caveat is applied.
        /// </summary>
        public double? ServerUptimeDays { get; init; }

        /// <summary>Reference "now" for deriving uptime from server start time. Defaults to <see cref="DateTime.UtcNow"/>.</summary>
        public DateTime? ReferenceTimeUtc { get; init; }

        /// <summary>Target engine supports data compression (Enterprise / Standard 2016+ / Azure). When false, nothing is a compression candidate (mirrors <c>@can_compress</c>).</summary>
        public bool ServerSupportsCompression { get; init; } = true;

        /// <summary>Target engine supports <c>ONLINE = ON</c> rebuilds (Enterprise / Azure). Controls the ONLINE token in generated scripts; defaults to OFF (safe).</summary>
        public bool ServerSupportsOnline { get; init; }

        /// <summary>Low end of the general PAGE-compression savings band (sp_IndexCleanup: 0.20 × size).</summary>
        public decimal CompressionMinSavingsFactor { get; init; } = 0.20m;

        /// <summary>High end of the general PAGE-compression savings band (sp_IndexCleanup: 0.60 × size).</summary>
        public decimal CompressionMaxSavingsFactor { get; init; } = 0.60m;
    }

    /// <summary>What to do with an index. Mirrors sp_IndexCleanup's <c>action</c> for the in-scope dedupe rules.</summary>
    public enum IndexCleanupAction
    {
        /// <summary>No action.</summary>
        None = 0,

        /// <summary>Retain this index (the keeper of a duplicate set).</summary>
        Keep = 1,

        /// <summary>Disable this index (redundant / unused). Never assigned to a protected index. When the
        /// index is a unique constraint (Rule 7.5 / 7.5b), the generated script is a <c>DROP CONSTRAINT</c>
        /// rather than an <c>ALTER INDEX … DISABLE</c>.</summary>
        Disable = 2,

        /// <summary>Retain but rebuild with includes merged from the indexes it supersedes.</summary>
        MergeIncludes = 3,

        /// <summary>
        /// Rebuild this nonclustered index as UNIQUE so it can replace a redundant unique constraint
        /// (sp_IndexCleanup's <c>MAKE UNIQUE</c>, Rule 7 / 7.5). The matching constraint is dropped
        /// (a companion <see cref="Disable"/> row). The generated script is a
        /// <c>CREATE UNIQUE INDEX … WITH (DROP_EXISTING = ON …)</c> rebuild.
        /// </summary>
        MakeUnique = 4,
    }

    /// <summary>Which generated-script bucket a recommendation belongs to. Mirrors sp_IndexCleanup's <c>result_type</c>.</summary>
    public enum IndexCleanupResultKind
    {
        /// <summary>An <c>ALTER INDEX … DISABLE</c> row.</summary>
        Disable = 0,

        /// <summary>A <c>CREATE INDEX … WITH (DROP_EXISTING = ON …)</c> merge row (the keeper/superset).</summary>
        Merge = 1,

        /// <summary>An <c>ALTER INDEX … REBUILD … DATA_COMPRESSION = PAGE</c> row.</summary>
        Compress = 2,

        /// <summary>
        /// An informational "review these related indexes" row that carries NO destructive script — the
        /// analyzer recognized a Reverse Duplicate or Equal Except For Filter relationship but, exactly like
        /// sp_IndexCleanup, deliberately does NOT auto-consolidate it (the indexes serve different access
        /// patterns / row coverage).
        /// </summary>
        Review = 3,

        /// <summary>
        /// An <c>ALTER TABLE … DROP CONSTRAINT</c> row: a redundant unique constraint being replaced by a
        /// nonclustered index made unique in its place, or (Rule 7.5b) a duplicate unique constraint dropped
        /// in favor of an identical surviving one. Mirrors sp_IndexCleanup's <c>CONSTRAINT</c> result bucket
        /// (its <c>DISABLE CONSTRAINT SCRIPT</c>) — distinct from <see cref="Disable"/>'s
        /// <c>ALTER INDEX … DISABLE</c>.
        /// </summary>
        DisableConstraint = 4,
    }

    /// <summary>The canonical <c>consolidation_rule</c> labels this analyzer emits.</summary>
    public static class IndexCleanupRules
    {
        /// <summary>No reads at all (Rule 1). Never-clustered, non-unique, non-PK/UC nonclustered indexes only.</summary>
        public const string UnusedIndex = "Unused Index";

        /// <summary>The &lt;14-day-uptime variant of <see cref="UnusedIndex"/> (usage data may be incomplete).</summary>
        public const string UnusedIndexUptimeWarning = "Unused Index (WARNING: Server uptime < 14 days - usage data may be incomplete)";

        /// <summary>Identical keys (with direction), includes, and filter (Rule 2) → the redundant one is disabled.</summary>
        public const string ExactDuplicate = "Exact Duplicate";

        /// <summary>
        /// Same key column SET but a different arrangement (reversed/reordered order or changed sort
        /// directions), with the same includes and filter. Recognized and surfaced for REVIEW but never
        /// auto-disabled — a different leading column serves different query patterns (sp_IndexCleanup's
        /// adversarial test 9a asserts these are not consolidated).
        /// </summary>
        public const string ReverseDuplicate = "Reverse Duplicate";

        /// <summary>
        /// Identical keys (with direction) and includes but a DIFFERENT filter predicate. Recognized and
        /// surfaced for REVIEW but never auto-disabled — the filters index different row sets
        /// (sp_IndexCleanup's adversarial test 10a asserts these are not consolidated).
        /// </summary>
        public const string EqualExceptForFilter = "Equal Except For Filter";

        /// <summary>Same keys (with direction) and filter but different includes (Rule 5); the keeper absorbs the includes.</summary>
        public const string KeyDuplicate = "Key Duplicate";

        /// <summary>The narrower index whose key is a leading prefix of a wider index's key (Rule 3); disabled in favor of the superset.</summary>
        public const string KeySubset = "Key Subset";

        /// <summary>The wider index a Key Subset defers to (Rule 4); rebuilt with the subset's missing includes merged in.</summary>
        public const string KeySuperset = "Key Superset";

        /// <summary>
        /// A nonclustered index whose key columns match a unique constraint's (Rule 7 / 7.5 / 7.5b). The index
        /// is made unique (<see cref="IndexCleanupAction.MakeUnique"/>) to take over the constraint's
        /// uniqueness enforcement and the now-redundant constraint is dropped
        /// (<see cref="IndexCleanupAction.Disable"/> → <see cref="IndexCleanupResultKind.DisableConstraint"/>);
        /// a constraint that backs an inbound foreign-key reference is never dropped. Rule 7.5b additionally
        /// collapses two or more identical unique constraints down to a single survivor.
        /// </summary>
        public const string UniqueConstraintReplacement = "Unique Constraint Replacement";
    }

    /// <summary>
    /// One actionable finding for a single index — the C# analog of a row in sp_IndexCleanup's script result
    /// set. Carries the matched rule, the action, the target/superseded linkage, the reconstructed T-SQL, and
    /// the size/usage figures the rollup sums.
    /// </summary>
    public sealed record IndexCleanupRecommendation
    {
        /// <summary>Database name.</summary>
        public string DatabaseName { get; init; } = "";

        /// <summary>Schema name.</summary>
        public string SchemaName { get; init; } = "";

        /// <summary>Table (or indexed-view) name.</summary>
        public string TableName { get; init; } = "";

        /// <summary>The index this finding is about.</summary>
        public string IndexName { get; init; } = "";

        /// <summary>Index id (1 = clustered).</summary>
        public int IndexId { get; init; }

        /// <summary>The matched <see cref="IndexCleanupRules"/> label (null for a pure compression row).</summary>
        public string? ConsolidationRule { get; init; }

        /// <summary>The recommended action.</summary>
        public IndexCleanupAction Action { get; init; }

        /// <summary>Which script bucket this row belongs to.</summary>
        public IndexCleanupResultKind ResultKind { get; init; }

        /// <summary>The keeper/superset this index defers to (on a DISABLE row), or null.</summary>
        public string? TargetIndexName { get; init; }

        /// <summary>On a keeper/superset row, the "Supersedes …" list of indexes it absorbs.</summary>
        public string? SupersededBy { get; init; }

        /// <summary>On a Key Superset row, the subset include columns merged in that were not already present (sp_IndexCleanup's <c>missing_included_columns</c>).</summary>
        public string? MissingIncludedColumns { get; init; }

        /// <summary>Human label for the script bucket: <c>DISABLE SCRIPT</c> / <c>MERGE SCRIPT</c> / <c>COMPRESSION SCRIPT</c>.</summary>
        public string ScriptType { get; init; } = "";

        /// <summary>The reconstructed T-SQL to run.</summary>
        public string Script { get; init; } = "";

        /// <summary>A short explanation of what the script does / why.</summary>
        public string? AdditionalInfo { get; init; }

        /// <summary>The reconstructed original CREATE/ADD CONSTRAINT statement (rollback + validation reference).</summary>
        public string OriginalIndexDefinition { get; init; } = "";

        /// <summary>Reserved size in GB.</summary>
        public decimal IndexSizeGb { get; init; }

        /// <summary>Row count.</summary>
        public long IndexRows { get; init; }

        /// <summary>seeks + scans + lookups.</summary>
        public long IndexReads { get; init; }

        /// <summary>user_updates.</summary>
        public long IndexWrites { get; init; }

        /// <summary>This index is (also) a PAGE-compression candidate.</summary>
        public bool CanCompress { get; init; }

        /// <summary>This index supports an outgoing foreign key (informational; not a disable guard).</summary>
        public bool IsForeignKey { get; init; }

        /// <summary>The generated script omits the trailing <c>ON &lt;filegroup/partition_scheme&gt;</c> placement clause because Stage 1 did not capture it.</summary>
        public bool ScriptOmitsPartitionPlacement { get; init; }
    }

    /// <summary>
    /// The reclaimable-space roll-up for a scope (a database, or the whole snapshot) — the C# analog of
    /// sp_IndexCleanup's <c>#index_reporting_stats</c> DATABASE row.
    /// </summary>
    public sealed record IndexCleanupRollup
    {
        /// <summary>Database this rollup covers; null for the overall (all-databases) rollup.</summary>
        public string? DatabaseName { get; init; }

        /// <summary>The database id the analyzer grouped this rollup by; null on the overall rollup and for any caller that does not fill it. Names can repeat across ids (a dropped and recreated database), the id cannot.</summary>
        public int? DatabaseId { get; init; }

        /// <summary>Distinct tables analyzed.</summary>
        public int TablesAnalyzed { get; init; }

        /// <summary>Indexes analyzed (rowstore CLUSTERED/NONCLUSTERED).</summary>
        public int IndexCount { get; init; }

        /// <summary>Total reserved size of analyzed indexes (GB).</summary>
        public decimal TotalSizeGb { get; init; }

        /// <summary>Total rows across analyzed tables' base row (index_id 0/1).</summary>
        public long TotalRows { get; init; }

        /// <summary>Indexes recommended for DISABLE (redundant + unused).</summary>
        public int IndexesToDisable { get; init; }

        /// <summary>Indexes recommended for MERGE INCLUDES (keepers/supersets).</summary>
        public int IndexesToMerge { get; init; }

        /// <summary>Indexes eligible for PAGE compression.</summary>
        public int CompressableIndexes { get; init; }

        /// <summary>Indexes flagged specifically as unused (subset of <see cref="IndexesToDisable"/>).</summary>
        public int UnusedIndexes { get; init; }

        /// <summary>Reclaimable size (GB) from disabling redundant/unused indexes (their full size).</summary>
        public decimal UnusedSizeGb { get; init; }

        /// <summary>Low-band compression savings (GB) over kept, compressible indexes (0.20 × size).</summary>
        public decimal CompressionMinSavingsGb { get; init; }

        /// <summary>High-band compression savings (GB) over kept, compressible indexes (0.60 × size).</summary>
        public decimal CompressionMaxSavingsGb { get; init; }

        /// <summary>Combined low-band reclaim (disabled full size + compression min).</summary>
        public decimal TotalMinSavingsGb { get; init; }

        /// <summary>Combined high-band reclaim (disabled full size + compression max).</summary>
        public decimal TotalMaxSavingsGb { get; init; }

        /*
         * Workload-impact aggregates — straight SUMs over EVERY analyzed index in the scope (mirroring
         * sp_IndexCleanup's #index_reporting_stats DATABASE roll-up of its analyzed_indexes rows). These feed
         * the reporting summary's Reads / Writes / Lock Waits / Latch Waits columns. sp_IndexCleanup only
         * surfaces them at its DATABASE (and TABLE) levels — its SUMMARY row shows them as 'N/A' — so the
         * viewer's rollup-row projection renders 'N/A' for the overall (all-databases) row to match Lite.
         */

        /// <summary>Sum of <c>user_seeks</c> across analyzed indexes (the "seeks" part of the Reads breakdown).</summary>
        public long UserSeeks { get; init; }

        /// <summary>Sum of <c>user_scans</c> across analyzed indexes.</summary>
        public long UserScans { get; init; }

        /// <summary>Sum of <c>user_lookups</c> across analyzed indexes.</summary>
        public long UserLookups { get; init; }

        /// <summary>Sum of <c>user_updates</c> across analyzed indexes (the Writes column).</summary>
        public long TotalWrites { get; init; }

        /// <summary>Sum of <c>row_lock_wait_count + page_lock_wait_count</c> across analyzed indexes (the Lock Waits column).</summary>
        public long LockWaitCount { get; init; }

        /// <summary>Sum of <c>row_lock_wait_in_ms + page_lock_wait_in_ms</c> across analyzed indexes (numerator of Avg Lock Wait ms).</summary>
        public long LockWaitInMs { get; init; }

        /// <summary>Sum of <c>page_latch_wait_count + page_io_latch_wait_count</c> across analyzed indexes (the Latch Waits column).</summary>
        public long LatchWaitCount { get; init; }

        /// <summary>Sum of <c>page_latch_wait_in_ms + page_io_latch_wait_in_ms</c> across analyzed indexes (numerator of Avg Latch Wait ms).</summary>
        public long LatchWaitInMs { get; init; }
    }

    /// <summary>The full result of one analysis pass.</summary>
    public sealed record IndexCleanupAnalysisResult
    {
        /// <summary>Every actionable finding (DISABLE / MERGE / COMPRESS), ordered by database, schema, table, then keepers before losers.</summary>
        public IReadOnlyList<IndexCleanupRecommendation> Recommendations { get; init; } = Array.Empty<IndexCleanupRecommendation>();

        /// <summary>One rollup per database.</summary>
        public IReadOnlyList<IndexCleanupRollup> DatabaseRollups { get; init; } = Array.Empty<IndexCleanupRollup>();

        /// <summary>The all-databases rollup.</summary>
        public IndexCleanupRollup OverallRollup { get; init; } = new();

        /// <summary>Server uptime was under 14 days, so unused-index usage data may be incomplete.</summary>
        public bool UptimeWarning { get; init; }

        /// <summary>Dedupe-only was in effect (either requested, or auto-enabled because uptime &lt;= 7 days), so unused indexes were not flagged.</summary>
        public bool DedupeOnlyApplied { get; init; }

        /// <summary>Precisely-surfaced limitations of this data-driven reproduction (e.g. omitted filegroup placement, compression exclusions not reproducible, out-of-scope rules).</summary>
        public IReadOnlyList<string> Notes { get; init; } = Array.Empty<string>();
    }
}
