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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The health of the monitoring store's OWN tables (#8787), read live from the catalogs. One row per store table,
/// shared by the service (the <c>get_store_table_health</c> MCP tool and its web read) and by the Darling Viewer, which
/// reads the store directly.
///
/// <para><b>Why it exists.</b> #8570 found a plain store table whose file was about ten times its live data after
/// heavy delete churn. Autovacuum had kept dead rows low, so the dead-row count looked healthy, but the pages were
/// never returned and every read of the table paid for them. A rewrite fixed it. <c>get_store_metrics</c> covers
/// hypertables, continuous aggregates, the payload dimensions and background jobs, and plain tables not at all, and
/// nothing in the product said which table carried the empty space.</para>
///
/// <para><b>The empty-space figure is an ESTIMATE from table statistics</b>, and it is the same extension-free
/// estimate the PostgreSQL target table bloat collector runs on monitored targets (<see cref="PgTableBloatStatsCollector.QueryText"/>,
/// spliced in whole, not reimplemented), with the same trust rules (<see cref="DarlingPgTableBloatReader.UnpublishableConditionSql"/>):
/// when statistics are missing, stale or unusable the estimate is NULL with a reason, never zero. It is NOT
/// <c>n_live_tup / (n_live_tup + n_dead_tup)</c>, which reads 100 percent right after a vacuum and says nothing
/// about pages a vacuum could not return. A measured figure needs <c>pgstattuple</c>, which this read never
/// installs or calls.</para>
///
/// <para><b>Roll-ups.</b> A hypertable's chunks, and the relations holding a chunk's compressed data, roll up to
/// the hypertable (<see cref="KindHypertable"/>). A continuous aggregate's materialization hypertable rolls up to
/// the aggregate's own view name (<see cref="KindContinuousAggregate"/>), the way <c>StoreSelfMetrics</c> names it.
/// A partitioned table's leaf partitions roll up to the top parent (<see cref="KindPartitioned"/>, with
/// <see cref="StoreTableHealthRow.LeafCount"/> the leaf count, found from <c>relkind = 'p'</c> roots through
/// <c>pg_partition_tree</c>, so a sub-partitioned leaf is walked). Every other relation is its own row
/// (<see cref="KindTable"/>).</para>
///
/// <para><b>What the estimate covers for a roll-up.</b> Only a relation whose heap is at least
/// <see cref="EstimateMinimumHeapBytes"/> gets an estimate (the collector's floor), and the relations that hold
/// compressed data never do: their arithmetic means nothing. <see cref="StoreTableHealthRow.EstimateRelations"/>
/// counts the heap relations the estimate was tried on and
/// <see cref="StoreTableHealthRow.EstimateRelationsUsable"/> how many returned a usable one.
/// <see cref="StoreTableHealthRow.EstimatedEmptyBytes"/> sums the usable ones and
/// <see cref="StoreTableHealthRow.EstimatedEmptyShare"/> divides that by the usable relations' own catalog heap
/// size, so a roll-up's share is the share of the part that could be estimated, not of the whole object.</para>
/// </summary>
public static class DarlingStoreTableHealthReader
{
    /// <summary>A plain or partitioned table, or a materialized view (<see cref="StoreTableHealthRow.Kind"/>).</summary>
    public const string KindTable = "table";

    /// <summary>A declarative partitioned table (for example <c>collect.query_store_interval_wide</c>), its leaf
    /// partitions rolled up under the top parent. A leaf never appears as its own row.</summary>
    public const string KindPartitioned = "partitioned";

    /// <summary>A TimescaleDB hypertable, its chunks and compressed data rolled up.</summary>
    public const string KindHypertable = "hypertable";

    /// <summary>A continuous aggregate, named by its view, its materialization hypertable rolled up.</summary>
    public const string KindContinuousAggregate = "continuous_aggregate";

    /// <summary>The collector's heap floor: a relation under this gets no estimate, because under a megabyte the
    /// figure is not actionable (see <see cref="PgTableBloatStatsCollector.MinimumHeapBytes"/>).</summary>
    public const long EstimateMinimumHeapBytes = 1048576;

    /// <summary>
    /// A table is a rewrite candidate when its estimated empty share is at least this fraction AND its estimated
    /// empty bytes are at least <see cref="RewriteCandidateMinEmptyBytes"/>. Half is the line the target table bloat
    /// tool already grades Critical at (<c>DarlingMcpPgTableBloatTools</c>, 50 percent), so the two surfaces agree
    /// on what "most of this table is empty" means.
    /// </summary>
    public const double RewriteCandidateMinShare = 0.5;

    /// <summary>
    /// The bytes line a rewrite candidate must also pass: the scorer's own heap-size floor
    /// (<see cref="PgTargetScorer.BloatSizeFloorBytes"/>, 64 MiB), below which the target bloat reads speak no
    /// percentage at all. Reused rather than invented: an empty space under it is not worth a rewrite's lock.
    /// </summary>
    public const long RewriteCandidateMinEmptyBytes = PgTargetScorer.BloatSizeFloorBytes;

    /// <summary>The relation is under <see cref="EstimateMinimumHeapBytes"/>, so the estimate was not run.</summary>
    public const string ReasonBelowSizeFloor = "below_size_floor";

    /// <summary>PostgreSQL 14+ marks a table that was never analyzed with a negative row estimate.</summary>
    public const string ReasonNeverAnalyzed = "never_analyzed";

    /// <summary>No column statistics: never analyzed on an older server, a name-typed column, or the login lacks
    /// SELECT on the table so <c>pg_stats</c> filtered every row out.</summary>
    public const string ReasonNoColumnStatistics = "no_column_statistics";

    /// <summary>Column statistics exist but PostgreSQL lost the record of when they were taken (crash recovery,
    /// immediate shutdown, <c>pg_stat_reset()</c>), so whether they are fresh cannot be judged.</summary>
    public const string ReasonAnalyzeRecordLost = "analyze_record_lost";

    /// <summary>A fifth or more of the rows changed since the last analyze, so the column widths are out of date
    /// (<see cref="DarlingPgTableBloatReader.StaleStatisticsChurnRatio"/>).</summary>
    public const string ReasonStaleStatistics = "stale_statistics";

    /// <summary>A hypertable or aggregate with no chunks has nothing to estimate.</summary>
    public const string ReasonNoChunks = "no_chunks";

    /// <summary>
    /// One store table (or roll-up). <c>EstimatedEmpty*</c> are ESTIMATES from table statistics and are NULL, never
    /// zero, when no relation of the object had a usable estimate; <see cref="EstimateUnavailableReason"/> then says
    /// why (one of the <c>Reason*</c> constants). Sizes are measured: <c>TotalBytes = HeapBytes + IndexBytes +
    /// ToastBytes</c> over every relation of the object, compressed data included.
    /// </summary>
    /// <param name="SchemaName">The object's schema (<c>collect</c> or <c>config</c> on a product store).</param>
    /// <param name="TableName">The table, hypertable, or the aggregate's view name.</param>
    /// <param name="Kind"><see cref="KindTable"/>, <see cref="KindPartitioned"/>, <see cref="KindHypertable"/> or <see cref="KindContinuousAggregate"/>.</param>
    /// <param name="TotalBytes">Heap, indexes and TOAST, measured from the files.</param>
    /// <param name="HeapBytes">The heap files only.</param>
    /// <param name="IndexBytes">Every index of every relation of the object.</param>
    /// <param name="ToastBytes">The TOAST tables and their indexes.</param>
    /// <param name="EstimatedRows">The planner's row estimate (<c>reltuples</c>) summed over the object's UNCOMPRESSED heap
    /// relations that have one. Rows inside compressed chunks are not counted, so a compressed hypertable reads low.
    /// NULL when no uncompressed relation has been analyzed or vacuumed.</param>
    /// <param name="ChunkCount">Chunks of a hypertable or aggregate, 0 for every other kind.</param>
    /// <param name="LeafCount">Leaf partitions of a partitioned table, 0 for every other kind.</param>
    /// <param name="EstimatedEmptyBytes">Estimated bytes of pages that hold no live row, summed over the relations with
    /// a usable estimate. NULL when none has one.</param>
    /// <param name="EstimatedEmptyShare">Estimated empty bytes over the same relations' catalog heap bytes, 0 to 1.
    /// NULL when <see cref="EstimatedEmptyBytes"/> is.</param>
    /// <param name="EstimateRelations">How many heap relations the estimate was tried on.</param>
    /// <param name="EstimateRelationsUsable">How many of them returned a usable estimate.</param>
    /// <param name="EstimateUnavailableReason">NULL when the object has an estimate. Otherwise the most actionable
    /// reason among its relations (a <c>Reason*</c> constant).</param>
    /// <param name="DeadRows">Dead tuples the statistics collector counts, summed.</param>
    /// <param name="RowsChangedSinceAnalyze">Rows inserted, updated or deleted since the last analyze, summed.</param>
    /// <param name="LastVacuum">Latest manual vacuum of any relation of the object (UTC), or NULL.</param>
    /// <param name="LastAutovacuum">Latest autovacuum (UTC), or NULL.</param>
    /// <param name="LastAnalyze">Latest manual analyze (UTC), or NULL.</param>
    /// <param name="LastAutoanalyze">Latest autoanalyze (UTC), or NULL.</param>
    /// <param name="AutovacuumDisabled">True when any relation of the object sets <c>autovacuum_enabled</c> off.</param>
    /// <param name="GrowthBytes7d">Latest minus earliest <c>total_bytes</c> recorded in <c>collect.store_metrics</c>
    /// in the last 7 days, over rows of one sizing form (estimate or exact). NULL when the object is not recorded
    /// there or has fewer than two rows in the window.</param>
    /// <param name="RewriteCandidate">True when <see cref="PastRewriteFloors"/> AND <see cref="TailOccupied"/> is true:
    /// the estimated empty space is large AND plain VACUUM cannot truncate the file, so only a rewrite gives it back.
    /// False when the tail is empty (VACUUM can truncate) and when the tail could not be read (see
    /// <see cref="PastRewriteFloors"/>, which stays true in that case).</param>
    /// <param name="RelPages">Visibility map: <c>pg_class.relpages</c> summed over the object's heap relations, as of the
    /// last vacuum or analyze.</param>
    /// <param name="RelAllVisible">Visibility map: <c>pg_class.relallvisible</c>, summed likewise. Pages marked all-visible.</param>
    /// <param name="RelAllFrozen">Visibility map: <c>pg_class.relallfrozen</c> (PostgreSQL 18 and later), summed likewise. NULL
    /// on an older major, where the column does not exist.</param>
    /// <param name="PctAllVisible">Visibility map coverage, 0 to 100: <c>100 * RelAllVisible / RelPages</c>. NULL when there
    /// are no pages. Low coverage makes index-only scans visit the heap.</param>
    /// <param name="VacuumTriggerRows">Distance to the next autovacuum: <c>threshold + scale_factor * reltuples</c> per heap
    /// relation, from the relation's own <c>autovacuum_vacuum_*</c> storage options when set and the server settings
    /// otherwise, capped by <c>autovacuum_vacuum_max_threshold</c> where the server has it (PostgreSQL 18), summed over the
    /// object's heap relations. For a plain table this is the table's own trigger and compares directly with
    /// <see cref="DeadRows"/>. For a roll-up it is the sum of its relations' triggers, and
    /// <see cref="RelationsPastVacuumTrigger"/> says how many relations are individually past theirs.</param>
    /// <param name="RelationsPastVacuumTrigger">Heap relations whose dead rows exceed their own vacuum trigger. Autovacuum is
    /// due on those. Zero means every relation is still below its trigger, however many dead rows there are.</param>
    /// <param name="InsertTriggerRows">The insert-driven autovacuum trigger the same way
    /// (<c>autovacuum_vacuum_insert_threshold</c> and <c>_scale_factor</c>, with the unfrozen-page share PostgreSQL 18 applies),
    /// summed. NULL when insert-driven vacuums are off (threshold -1).</param>
    /// <param name="InsertsSinceVacuum"><c>n_ins_since_vacuum</c>, summed: compare with <see cref="InsertTriggerRows"/>.</param>
    /// <param name="RelationsPastInsertTrigger">Heap relations whose inserts since vacuum exceed their own insert trigger.</param>
    /// <param name="LiveRows"><c>n_live_tup</c>, summed over the heap relations (the statistics counter, not the planner estimate).</param>
    /// <param name="TuplesInserted"><c>n_tup_ins</c> since the statistics were reset, summed over the heap relations.</param>
    /// <param name="TuplesUpdated"><c>n_tup_upd</c> since the reset.</param>
    /// <param name="TuplesHotUpdated"><c>n_tup_hot_upd</c> since the reset.</param>
    /// <param name="TuplesNewPageUpdated"><c>n_tup_newpage_upd</c> since the reset: updates that put the new row version on a
    /// different page. NULL before PostgreSQL 16.</param>
    /// <param name="TuplesDeleted"><c>n_tup_del</c> since the reset.</param>
    /// <param name="AutovacuumCount"><c>autovacuum_count</c> since the reset, summed.</param>
    /// <param name="AutoanalyzeCount"><c>autoanalyze_count</c> since the reset, summed.</param>
    /// <param name="StatsSince">When the counters above began (UTC): <c>pg_stat_database.stats_reset</c> for the store database,
    /// or the postmaster start when the statistics were never reset (see <see cref="StatsSinceSource"/>). Nothing is recorded
    /// anywhere: the rates are since this moment and nothing older.</param>
    /// <param name="StatsSinceSource"><see cref="StatsSourceReset"/> or <see cref="StatsSourcePostmasterStart"/>. The postmaster
    /// start is a lower bound on the counters' age, because statistics survive a clean restart.</param>
    /// <param name="StatsHours">Hours between <see cref="StatsSince"/> and the read. Rates over a short window are noisy.</param>
    /// <param name="PastRewriteFloors">True when <see cref="EstimatedEmptyShare"/> is at least
    /// <see cref="RewriteCandidateMinShare"/> and <see cref="EstimatedEmptyBytes"/> at least
    /// <see cref="RewriteCandidateMinEmptyBytes"/>. This was the whole of <see cref="RewriteCandidate"/> before the tail check.</param>
    /// <param name="TailRelationsEligible">For an object past the floors: how many of its heap relations are estimated at half
    /// empty or more, the pool the tail check picks from. Otherwise 0.</param>
    /// <param name="TailRelationsChecked">How many of those had their last <see cref="TailProbePages"/> pages read: the
    /// emptiest first, at most <see cref="TailProbeMaxRelations"/>. 0 when the object is not past the floors or the read failed.</param>
    /// <param name="TailRelationsOccupied">How many of the checked relations hold a live row in those pages.</param>
    /// <param name="TailOccupied">True when a checked relation holds a live row in its last <see cref="TailProbePages"/> pages, so
    /// plain VACUUM cannot truncate that file and only a rewrite returns the space. False when none does. NULL when the object
    /// is not past the floors (not read) or the tail read failed.</param>
    public sealed record StoreTableHealthRow(
        string SchemaName,
        string TableName,
        string Kind,
        long TotalBytes,
        long HeapBytes,
        long IndexBytes,
        long ToastBytes,
        long? EstimatedRows,
        int ChunkCount,
        int LeafCount,
        long? EstimatedEmptyBytes,
        double? EstimatedEmptyShare,
        int EstimateRelations,
        int EstimateRelationsUsable,
        string? EstimateUnavailableReason,
        long DeadRows,
        long RowsChangedSinceAnalyze,
        DateTime? LastVacuum,
        DateTime? LastAutovacuum,
        DateTime? LastAnalyze,
        DateTime? LastAutoanalyze,
        bool AutovacuumDisabled,
        long? GrowthBytes7d,
        bool RewriteCandidate,
        long RelPages,
        long RelAllVisible,
        long? RelAllFrozen,
        double? PctAllVisible,
        double? VacuumTriggerRows,
        int RelationsPastVacuumTrigger,
        double? InsertTriggerRows,
        long InsertsSinceVacuum,
        int RelationsPastInsertTrigger,
        long LiveRows,
        long TuplesInserted,
        long TuplesUpdated,
        long TuplesHotUpdated,
        long? TuplesNewPageUpdated,
        long TuplesDeleted,
        long AutovacuumCount,
        long AutoanalyzeCount,
        DateTime? StatsSince,
        string StatsSinceSource,
        double StatsHours,
        bool PastRewriteFloors,
        int TailRelationsEligible,
        int TailRelationsChecked,
        int TailRelationsOccupied,
        bool? TailOccupied)
    {
        /// <summary>Share of updates that were HOT (heap-only, no index entry written), 0 to 1. NULL when there were no updates.</summary>
        public double? HotUpdateShare => TuplesUpdated > 0 ? (double)TuplesHotUpdated / TuplesUpdated : null;

        /// <summary>Rows inserted per day since <see cref="StatsSince"/>. NULL when the window is empty.</summary>
        public double? InsertsPerDay => PerDay(TuplesInserted, StatsHours);

        /// <summary>Row updates per day since <see cref="StatsSince"/>.</summary>
        public double? UpdatesPerDay => PerDay(TuplesUpdated, StatsHours);

        /// <summary>Rows deleted per day since <see cref="StatsSince"/>.</summary>
        public double? DeletesPerDay => PerDay(TuplesDeleted, StatsHours);

        /// <summary><see cref="DeletesPerDay"/> over <see cref="LiveRows"/>: 1.0 means the table's whole contents are deleted and
        /// replaced once a day. This is what showed a table churning daily while its file did not grow. NULL with no live rows
        /// or no window.</summary>
        public double? TurnoverPerDay => LiveRows > 0 && DeletesPerDay is double d ? d / LiveRows : null;

        private static double? PerDay(long count, double hours) => hours > 0 ? count / (hours / 24.0) : null;
    }

    /// <summary>The counters began at <c>pg_stat_database.stats_reset</c>.</summary>
    public const string StatsSourceReset = "stats_reset";

    /// <summary>The statistics were never reset, so the counters are dated from the postmaster start: a lower bound on their age.</summary>
    public const string StatsSourcePostmasterStart = "postmaster_start";

    /// <summary>How many pages at the end of a file the tail check reads. A live row there means plain VACUUM, which can
    /// only truncate empty pages off the end, cannot shrink the file.</summary>
    public const int TailProbePages = 64;

    /// <summary>For a roll-up past the floors, the most relations whose tail is read (the emptiest first).</summary>
    public const int TailProbeMaxRelations = 10;

    /// <summary>An index must be at least this large (1 MB, the estimate floor) to be flagged larger than its heap: a 16 KB
    /// index over an 8 KB heap is not a finding.</summary>
    public const long IndexFlagMinimumBytes = EstimateMinimumHeapBytes;

    /// <summary>An index is rarely used when its scans since the statistics reset are below this.</summary>
    public const long RarelyUsedMaxScans = 10;

    /// <summary>The statistics must have been reset (or the postmaster started) at least this many days ago before any index
    /// is called rarely used. A younger window proves nothing.</summary>
    public const int RarelyUsedMinStatsDays = 7;

    /// <summary>
    /// One index of the store, rolled up like the table rows: chunk or leaf indexes that are clones of one parent index become
    /// one row named for the parent index, with <see cref="LeafCount"/> the number rolled up. Sizes are measured.
    /// </summary>
    /// <param name="SchemaName">The owning object's schema.</param>
    /// <param name="TableName">The owning table, hypertable, partitioned table or aggregate view name (as the table row).</param>
    /// <param name="Kind">The owning object's kind (the table row's <c>Kind</c>).</param>
    /// <param name="IndexName">The index. For a hypertable the chunk indexes are grouped by their name with the chunk's
    /// name prefix removed, which is the parent index's name unless PostgreSQL truncated a long name. For a partitioned table the
    /// leaf indexes are grouped under the parent index they are attached to.</param>
    /// <param name="OnCompressedData">True for indexes on the relations that hold a hypertable's compressed data.</param>
    /// <param name="IndexCount">Physical indexes in this row.</param>
    /// <param name="LeafCount">Of those, how many are on a chunk, a leaf partition or a compressed relation (0 for a plain table).</param>
    /// <param name="Bytes">Index size summed.</param>
    /// <param name="HeapBytes">The heap size of the tables these indexes are on, summed over the same indexes.</param>
    /// <param name="IndexScans"><c>idx_scan</c> since <see cref="StatsSince"/>, summed.</param>
    /// <param name="IndexToHeapRatio"><see cref="Bytes"/> over <see cref="HeapBytes"/>. NULL when the heaps are empty.</param>
    /// <param name="IsPrimaryKey">Any index in the row backs a primary key.</param>
    /// <param name="IsUnique">Any index in the row is unique.</param>
    /// <param name="Valid">Every index in the row is valid.</param>
    /// <param name="LargerThanHeap">At least one index in the row is larger than its own table's heap and at least
    /// <see cref="IndexFlagMinimumBytes"/>. A field primary key was once larger than its whole heap before a rewrite.</param>
    /// <param name="LargerThanHeapCount">How many indexes in the row are.</param>
    /// <param name="RarelyUsed">Scans below <see cref="RarelyUsedMaxScans"/> in a statistics window at least
    /// <see cref="RarelyUsedMinStatsDays"/> days old, never for a primary key or unique index.</param>
    /// <param name="StatsSince">When the scan counters began (UTC), as on the table row.</param>
    /// <param name="StatsSinceSource">As on the table row.</param>
    /// <param name="EstimatedReclaimableBytes">The extension-free btree bloat estimate the PostgreSQL target index bloat
    /// collector runs (<see cref="PgIndexBloatCollector.QueryText"/>, spliced in whole), summed over the indexes in the row it
    /// could estimate. NULL when none could be, with <see cref="EstimateSkippedReason"/>.</param>
    /// <param name="EstimatedBloatShare">Estimated reclaimable bytes over the same indexes' size, 0 to 1.</param>
    /// <param name="EstimateIndexesUsable">How many of the row's indexes the estimate covered.</param>
    /// <param name="EstimateSkippedReason">NULL when any index in the row has an estimate. Otherwise the estimate's own reason,
    /// or a note that the index is not a valid, ready btree.</param>
    public sealed record StoreIndexHealthRow(
        string SchemaName,
        string TableName,
        string Kind,
        string IndexName,
        bool OnCompressedData,
        int IndexCount,
        int LeafCount,
        long Bytes,
        long HeapBytes,
        long IndexScans,
        double? IndexToHeapRatio,
        bool IsPrimaryKey,
        bool IsUnique,
        bool Valid,
        bool LargerThanHeap,
        int LargerThanHeapCount,
        bool RarelyUsed,
        DateTime? StatsSince,
        string StatsSinceSource,
        long? EstimatedReclaimableBytes,
        double? EstimatedBloatShare,
        int EstimateIndexesUsable,
        string? EstimateSkippedReason);

    /// <summary>
    /// Leaf index coverage for one index of a declarative partitioned parent (#8549: a missing or invalid leaf index was silent).
    /// </summary>
    /// <param name="SchemaName">The parent's schema.</param>
    /// <param name="TableName">The partitioned parent.</param>
    /// <param name="IndexName">The parent's index.</param>
    /// <param name="ParentIndexValid">False for a parent index created <c>ON ONLY</c> and not yet attached everywhere.</param>
    /// <param name="LeafCount">Leaf partitions of the parent.</param>
    /// <param name="LeavesWithIndex">Leaves that have an index attached to this one.</param>
    /// <param name="LeavesMissingIndex">Leaves that do not (<c>LeafCount - LeavesWithIndex</c>).</param>
    /// <param name="MissingLeaves">Up to <see cref="CoverageNamesCap"/> of the leaves that lack it, schema-qualified.</param>
    /// <param name="LeavesInvalidIndex">Leaves whose index is invalid or not ready.</param>
    /// <param name="InvalidLeaves">Up to <see cref="CoverageNamesCap"/> of them, schema-qualified.</param>
    public sealed record StoreIndexCoverageRow(
        string SchemaName,
        string TableName,
        string IndexName,
        bool ParentIndexValid,
        int LeafCount,
        int LeavesWithIndex,
        int LeavesMissingIndex,
        IReadOnlyList<string> MissingLeaves,
        int LeavesInvalidIndex,
        IReadOnlyList<string> InvalidLeaves);

    /// <summary>The most leaf names listed per coverage row.</summary>
    public const int CoverageNamesCap = 10;

    /// <summary>The roll-up the header of every surface reads (<see cref="Summarize"/>).</summary>
    /// <param name="TableCount">Rows read.</param>
    /// <param name="TotalBytes">Sum of the rows' <c>TotalBytes</c>.</param>
    /// <param name="RewriteCandidates">Rows flagged <see cref="StoreTableHealthRow.RewriteCandidate"/>.</param>
    /// <param name="WithoutEstimate">Rows with no usable estimate.</param>
    /// <param name="WithoutEstimateByReason">Those rows, counted by <see cref="StoreTableHealthRow.EstimateUnavailableReason"/>,
    /// most common first.</param>
    public sealed record StoreTableHealthSummary(
        int TableCount,
        long TotalBytes,
        int RewriteCandidates,
        int WithoutEstimate,
        IReadOnlyList<KeyValuePair<string, int>> WithoutEstimateByReason);

    /// <summary>Counts the rows for a header line.</summary>
    public static StoreTableHealthSummary Summarize(IReadOnlyList<StoreTableHealthRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var byReason = rows
            .Where(r => r.EstimatedEmptyBytes is null)
            .GroupBy(r => r.EstimateUnavailableReason ?? "unknown", StringComparer.Ordinal)
            .Select(g => new KeyValuePair<string, int>(g.Key, g.Count()))
            .OrderByDescending(p => p.Value)
            .ThenBy(p => p.Key, StringComparer.Ordinal)
            .ToList();
        return new StoreTableHealthSummary(
            rows.Count,
            rows.Sum(r => r.TotalBytes),
            rows.Count(r => r.RewriteCandidate),
            byReason.Sum(p => p.Value),
            byReason);
    }

    /// <summary>A plain-language phrase for each <c>Reason*</c> value, for headers and tooltips.</summary>
    public static string DescribeReason(string reason) => reason switch
    {
        ReasonBelowSizeFloor => "heap under 1 MB, so no estimate is made",
        ReasonNeverAnalyzed => "never analyzed",
        ReasonNoColumnStatistics => "no column statistics (never analyzed, or this login cannot read them)",
        ReasonAnalyzeRecordLost => "the record of the last analyze is gone, so freshness cannot be judged",
        ReasonStaleStatistics => "statistics are stale (a fifth or more of the rows changed since the last analyze)",
        ReasonNoChunks => "no chunks yet",
        _ => reason,
    };

    /// <summary>
    /// Whether TimescaleDB is installed in the store's database, from <c>pg_extension</c>. A store the operator
    /// brought themselves may be plain PostgreSQL, where the chunk and aggregate catalogs do not exist.
    /// </summary>
    public const string TimescaleProbeSql =
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'timescaledb')";

    private const string TimescaleTargetsCtes = @"targets AS (
    SELECT h.hypertable_schema AS obj_schema, h.hypertable_name AS obj_name, 'hypertable'::text AS kind,
           h.hypertable_schema AS root_schema, h.hypertable_name AS root_name
    FROM timescaledb_information.hypertables AS h
    UNION ALL
    SELECT ca.view_schema, ca.view_name, 'continuous_aggregate'::text,
           ca.materialization_hypertable_schema, ca.materialization_hypertable_name
    FROM timescaledb_information.continuous_aggregates AS ca
),
root_rels AS (
    SELECT t.obj_schema, t.obj_name, t.kind, pc.oid AS rel_oid
    FROM targets AS t
    JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = t.root_schema
    JOIN pg_catalog.pg_class AS pc ON pc.relnamespace = pn.oid AND pc.relname = t.root_name
),
chunk_rels AS (
    SELECT t.obj_schema, t.obj_name, t.kind, pc.oid AS rel_oid,
           EXISTS (SELECT 1 FROM _timescaledb_catalog.compression_settings AS cs
                   WHERE cs.relid::oid = pc.oid AND cs.compress_relid IS NOT NULL) AS is_compressed
    FROM targets AS t
    JOIN timescaledb_information.chunks AS ch
        ON  ch.hypertable_schema = t.root_schema
        AND ch.hypertable_name = t.root_name
    JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = ch.chunk_schema
    JOIN pg_catalog.pg_class AS pc ON pc.relnamespace = pn.oid AND pc.relname = ch.chunk_name
),
";

    private const string TimescaleMemberArms = @"    SELECT obj_schema, obj_name, kind, rel_oid, 'root'::text AS member_role, false AS grouped, false AS is_compressed FROM root_rels
    UNION ALL
    SELECT obj_schema, obj_name, kind, rel_oid, 'heap', true, is_compressed FROM chunk_rels
    UNION ALL
    SELECT r.obj_schema, r.obj_name, r.kind, cs.compress_relid::oid, 'compressed', true, true
    FROM chunk_rels AS r
    JOIN _timescaledb_catalog.compression_settings AS cs ON cs.relid::oid = r.rel_oid
    WHERE cs.compress_relid IS NOT NULL
    UNION ALL
";


    /// <summary>The first PostgreSQL major with <c>pg_class.relallfrozen</c> and the insert-trigger unfrozen-page share.</summary>
    private const int RelAllFrozenMinVersion = 180000;

    /// <summary>The first PostgreSQL major with <c>pg_stat_all_tables.n_tup_newpage_upd</c>.</summary>
    private const int NewPageUpdMinVersion = 160000;

    private static string FloorsSql() =>
        $@"a.empty_bytes IS NOT NULL
     AND a.basis_bytes > 0
     AND a.empty_bytes::double precision / a.basis_bytes >= {RewriteCandidateMinShare.ToString("R", CultureInfo.InvariantCulture)}
     AND a.empty_bytes >= {RewriteCandidateMinEmptyBytes.ToString(CultureInfo.InvariantCulture)}";

    /// <summary>
    /// The CTEs both reads share: the store's relations grouped under their objects (<c>members</c>: one row per relation with
    /// its <c>obj_schema</c>, <c>obj_name</c>, <c>kind</c>, <c>member_role</c>, <c>grouped</c> and <c>is_compressed</c>).
    /// </summary>
    private static string BuildMembersCtes(bool timescale) => $@"{(timescale ? TimescaleTargetsCtes : string.Empty)}plain_roots AS (
    SELECT c.oid, n.nspname, c.relname, c.relkind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'm', 'p')
    AND   NOT c.relispartition
    AND   NOT c.relisshared
    AND   c.relpersistence <> 't'
    AND   NOT {StoreSelfMetrics.SystemSchemaPredicateSql}{(timescale ? "\n    AND   " + StoreSelfMetrics.TimescaleInventoriedPredicateSql : string.Empty)}
),
members AS (
{(timescale ? TimescaleMemberArms : string.Empty)}    SELECT pr.nspname AS obj_schema, pr.relname AS obj_name, CASE WHEN pr.relkind = 'p' THEN 'partitioned' ELSE 'table' END AS kind, x.rel_oid AS rel_oid, 'heap'::text AS member_role, (pr.relkind = 'p') AS grouped, false AS is_compressed
    FROM plain_roots AS pr
    CROSS JOIN LATERAL (
        SELECT pr.oid AS rel_oid WHERE pr.relkind <> 'p'
        UNION ALL
        SELECT t.relid FROM pg_catalog.pg_partition_tree(pr.oid::regclass) AS t WHERE pr.relkind = 'p' AND t.isleaf
    ) AS x
)";

    /// <summary>
    /// When the statistics counters began: <c>pg_stat_database.stats_reset</c> for the store database, or the postmaster start when
    /// that is NULL. One row, kept even when the database has no <c>pg_stat_database</c> row.
    /// </summary>
    private const string StatsInfoCte = @"statsinfo AS (
    SELECT COALESCE(d.stats_reset, pg_catalog.pg_postmaster_start_time()) AS since,
           (d.stats_reset IS NOT NULL) AS from_reset
    FROM (SELECT 1) AS one
    LEFT JOIN pg_catalog.pg_stat_database AS d ON d.datname = pg_catalog.current_database()
)";

    /// <summary>
    /// The statement for the per-table read. <paramref name="serverVersion"/> is <c>server_version_num</c>: PostgreSQL 18 adds
    /// <c>relallfrozen</c> (and the unfrozen share of the insert trigger), 16 adds <c>n_tup_newpage_upd</c>; older majors read NULL.
    /// </summary>
    public static string BuildSql(bool timescale, int serverVersion = int.MaxValue)
    {
        var relAllFrozen = serverVersion >= RelAllFrozenMinVersion ? "c.relallfrozen::bigint" : "NULL::bigint";
        var newPage = serverVersion >= NewPageUpdMinVersion ? "st.n_tup_newpage_upd" : "NULL::bigint";
        var unfrozen = serverVersion >= RelAllFrozenMinVersion
            ? "(CASE WHEN c.relpages > 0 THEN 1 - LEAST(c.relallfrozen, c.relpages)::float8 / c.relpages ELSE 1 END)"
            : "1";
        return $@"
WITH est AS MATERIALIZED (
{PgTableBloatStatsCollector.QueryText}
),
{BuildMembersCtes(timescale)},
{StatsInfoCte},
sized AS (
    SELECT
        m.obj_schema,
        m.obj_name,
        m.kind,
        m.member_role,
        m.grouped,
        m.is_compressed,
        c.oid AS rel_oid,
        pg_catalog.format('%I.%I', n.nspname, c.relname) AS rel_qualified,
        pg_catalog.pg_relation_size(c.oid) AS heap_bytes,
        pg_catalog.pg_indexes_size(c.oid) AS index_bytes,
        COALESCE(pg_catalog.pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0) AS toast_bytes,
        c.reltuples,
        c.relpages::bigint AS relpages,
        c.relallvisible::bigint AS relallvisible,
        {relAllFrozen} AS relallfrozen,
        EXISTS (
            SELECT 1 FROM pg_catalog.unnest(c.reloptions) AS o(opt)
            WHERE o.opt ~* '^autovacuum_enabled=(f|fa|fal|fals|false|of|off|n|no|0)$') AS autovacuum_disabled,
        st.n_dead_tup,
        st.n_mod_since_analyze,
        st.n_live_tup,
        st.n_ins_since_vacuum,
        st.n_tup_ins,
        st.n_tup_upd,
        st.n_tup_hot_upd,
        {newPage} AS n_tup_newpage_upd,
        st.n_tup_del,
        st.autovacuum_count,
        st.autoanalyze_count,
        st.last_vacuum,
        st.last_autovacuum,
        st.last_analyze,
        st.last_autoanalyze,
        trig.vac_trigger,
        trig.ins_trigger,
        (l.table_name IS NOT NULL AND NOT ({DarlingPgTableBloatReader.UnpublishableConditionSql})) AS usable,
        CASE WHEN l.table_name IS NULL THEN 1
             WHEN l.live_tuples < 0 THEN 2
             WHEN l.estimate_unavailable THEN 3
             WHEN l.last_analyzed IS NULL THEN 4
             ELSE 5 END AS reason_rank,
        l.bloat_bytes_estimate,
        l.heap_pages * pg_catalog.current_setting('block_size')::bigint AS catalog_heap_bytes
    FROM members AS m
    JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_stat_all_tables AS st ON st.relid = c.oid
    LEFT JOIN est AS l ON m.member_role = 'heap' AND l.schema_name = n.nspname AND l.table_name = c.relname
    LEFT JOIN LATERAL (
        /* The relation's own autovacuum storage options win over the server settings, as in PostgreSQL's
           relation_needs_vacanalyze: threshold + scale_factor * reltuples (a never-analyzed table counts as 0 rows), the
           vacuum trigger capped by autovacuum_vacuum_max_threshold where the server has it (PostgreSQL 18; -1 is no cap),
           and the insert trigger off when its threshold is negative. */
        SELECT
            LEAST(
                COALESCE(ro.vt, cur.vt) + COALESCE(ro.vs, cur.vs) * GREATEST(c.reltuples, 0),
                CASE WHEN COALESCE(ro.vm, cur.vm) >= 0 THEN COALESCE(ro.vm, cur.vm) END) AS vac_trigger,
            CASE WHEN COALESCE(ro.it, cur.it) >= 0
                 THEN COALESCE(ro.it, cur.it) + COALESCE(ro.isf, cur.isf) * GREATEST(c.reltuples, 0) * {unfrozen}
            END AS ins_trigger
        FROM (
            SELECT
                max(o.option_value) FILTER (WHERE o.option_name = 'autovacuum_vacuum_threshold')::float8 AS vt,
                max(o.option_value) FILTER (WHERE o.option_name = 'autovacuum_vacuum_scale_factor')::float8 AS vs,
                max(o.option_value) FILTER (WHERE o.option_name = 'autovacuum_vacuum_max_threshold')::float8 AS vm,
                max(o.option_value) FILTER (WHERE o.option_name = 'autovacuum_vacuum_insert_threshold')::float8 AS it,
                max(o.option_value) FILTER (WHERE o.option_name = 'autovacuum_vacuum_insert_scale_factor')::float8 AS isf
            FROM pg_catalog.pg_options_to_table(c.reloptions) AS o
        ) AS ro
        CROSS JOIN (
            SELECT
                pg_catalog.current_setting('autovacuum_vacuum_threshold')::float8 AS vt,
                pg_catalog.current_setting('autovacuum_vacuum_scale_factor')::float8 AS vs,
                pg_catalog.current_setting('autovacuum_vacuum_max_threshold', true)::float8 AS vm,
                pg_catalog.current_setting('autovacuum_vacuum_insert_threshold')::float8 AS it,
                pg_catalog.current_setting('autovacuum_vacuum_insert_scale_factor')::float8 AS isf
        ) AS cur
    ) AS trig ON m.member_role = 'heap'
),
eligible AS (
    /* The relations the tail check can pick from: estimated, and by themselves at least half empty. A roll-up past the
       floors always has one, because its share is the weighted average of its relations' shares. */
    SELECT s.*,
           (s.member_role = 'heap' AND s.usable AND s.catalog_heap_bytes > 0
            AND s.bloat_bytes_estimate::double precision / s.catalog_heap_bytes >= {RewriteCandidateMinShare.ToString("R", CultureInfo.InvariantCulture)}) AS tail_eligible
    FROM sized AS s
),
ranked AS (
    SELECT e.*,
           CASE WHEN e.tail_eligible
                THEN row_number() OVER (PARTITION BY e.obj_schema, e.obj_name, e.kind, e.tail_eligible
                                        ORDER BY e.bloat_bytes_estimate DESC NULLS LAST, e.rel_oid)
           END AS tail_rank
    FROM eligible AS e
),
agg AS (
    SELECT
        s.obj_schema,
        s.obj_name,
        s.kind,
        sum(s.heap_bytes + s.index_bytes + s.toast_bytes)::bigint AS total_bytes,
        sum(s.heap_bytes)::bigint AS heap_bytes,
        sum(s.index_bytes)::bigint AS index_bytes,
        sum(s.toast_bytes)::bigint AS toast_bytes,
        sum(s.reltuples) FILTER (WHERE s.member_role = 'heap' AND NOT s.is_compressed AND s.reltuples >= 0)::bigint AS estimated_rows,
        (count(*) FILTER (WHERE s.member_role = 'heap' AND s.grouped AND s.kind <> 'partitioned'))::int AS chunk_count,
        (count(*) FILTER (WHERE s.member_role = 'heap' AND s.grouped AND s.kind = 'partitioned'))::int AS leaf_count,
        (count(*) FILTER (WHERE s.member_role = 'heap'))::int AS estimate_relations,
        (count(*) FILTER (WHERE s.usable))::int AS estimate_relations_usable,
        sum(s.bloat_bytes_estimate) FILTER (WHERE s.usable)::bigint AS empty_bytes,
        sum(s.catalog_heap_bytes) FILTER (WHERE s.usable)::bigint AS basis_bytes,
        max(s.reason_rank) FILTER (WHERE s.member_role = 'heap' AND NOT s.usable) AS reason_rank,
        COALESCE(sum(s.n_dead_tup), 0)::bigint AS dead_rows,
        COALESCE(sum(s.n_mod_since_analyze), 0)::bigint AS rows_changed,
        (max(s.last_vacuum) AT TIME ZONE 'UTC') AS last_vacuum,
        (max(s.last_autovacuum) AT TIME ZONE 'UTC') AS last_autovacuum,
        (max(s.last_analyze) AT TIME ZONE 'UTC') AS last_analyze,
        (max(s.last_autoanalyze) AT TIME ZONE 'UTC') AS last_autoanalyze,
        bool_or(s.autovacuum_disabled) AS autovacuum_disabled,
        COALESCE(sum(s.relpages) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS relpages,
        COALESCE(sum(s.relallvisible) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS relallvisible,
        sum(s.relallfrozen) FILTER (WHERE s.member_role = 'heap')::bigint AS relallfrozen,
        sum(s.vac_trigger) FILTER (WHERE s.member_role = 'heap') AS vac_trigger,
        (count(*) FILTER (WHERE s.member_role = 'heap' AND s.n_dead_tup > s.vac_trigger))::int AS rel_past_vac,
        sum(s.ins_trigger) FILTER (WHERE s.member_role = 'heap') AS ins_trigger,
        COALESCE(sum(s.n_ins_since_vacuum) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS ins_since_vacuum,
        (count(*) FILTER (WHERE s.member_role = 'heap' AND s.n_ins_since_vacuum > s.ins_trigger))::int AS rel_past_ins,
        COALESCE(sum(s.n_live_tup) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS live_rows,
        COALESCE(sum(s.n_tup_ins) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS tup_ins,
        COALESCE(sum(s.n_tup_upd) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS tup_upd,
        COALESCE(sum(s.n_tup_hot_upd) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS tup_hot_upd,
        sum(s.n_tup_newpage_upd) FILTER (WHERE s.member_role = 'heap')::bigint AS tup_newpage_upd,
        COALESCE(sum(s.n_tup_del) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS tup_del,
        COALESCE(sum(s.autovacuum_count) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS autovacuum_count,
        COALESCE(sum(s.autoanalyze_count) FILTER (WHERE s.member_role = 'heap'), 0)::bigint AS autoanalyze_count,
        (count(*) FILTER (WHERE s.tail_eligible))::int AS tail_eligible,
        array_agg(s.rel_qualified ORDER BY s.tail_rank) FILTER (WHERE s.tail_rank <= {TailProbeMaxRelations}) AS tail_rels
    FROM ranked AS s
    GROUP BY s.obj_schema, s.obj_name, s.kind
),
scored AS (
    SELECT a.*, ({FloorsSql()}) AS past_floors
    FROM agg AS a
),
recent AS (
    SELECT
        CASE m.object_kind WHEN 'dimension' THEN 'table' ELSE m.object_kind END AS kind,
        CASE WHEN m.object_kind IN ('table') THEN m.object_name ELSE 'collect.' || m.object_name END AS qualified_name,
        m.total_bytes,
        m.metric_time,
        (m.size_estimated IS TRUE) AS is_estimated,
        first_value(m.size_estimated IS TRUE) OVER (PARTITION BY m.object_kind, m.object_name ORDER BY m.metric_time DESC) AS newest_is_estimated
    FROM collect.store_metrics AS m
    WHERE m.metric_time >= (pg_catalog.now() AT TIME ZONE 'UTC') - interval '7 days'
    AND   m.object_kind IN ('hypertable', 'continuous_aggregate', 'table', 'dimension')
    AND   m.total_bytes IS NOT NULL
),
growth AS (
    SELECT
        r.kind,
        r.qualified_name,
        (array_agg(r.total_bytes ORDER BY r.metric_time DESC))[1] - (array_agg(r.total_bytes ORDER BY r.metric_time))[1] AS growth_bytes
    FROM recent AS r
    WHERE r.is_estimated = r.newest_is_estimated
    GROUP BY r.kind, r.qualified_name
    HAVING count(*) >= 2
)
SELECT
    a.obj_schema,
    a.obj_name,
    a.kind,
    a.total_bytes,
    a.heap_bytes,
    a.index_bytes,
    a.toast_bytes,
    a.estimated_rows,
    a.chunk_count,
    a.leaf_count,
    a.empty_bytes,
    CASE WHEN a.empty_bytes IS NOT NULL AND a.basis_bytes > 0 THEN a.empty_bytes::double precision / a.basis_bytes END AS empty_share,
    a.estimate_relations,
    a.estimate_relations_usable,
    CASE WHEN a.estimate_relations_usable > 0 THEN NULL
         WHEN a.estimate_relations = 0 THEN '{ReasonNoChunks}'
         WHEN a.reason_rank = 1 THEN '{ReasonBelowSizeFloor}'
         WHEN a.reason_rank = 2 THEN '{ReasonNeverAnalyzed}'
         WHEN a.reason_rank = 3 THEN '{ReasonNoColumnStatistics}'
         WHEN a.reason_rank = 4 THEN '{ReasonAnalyzeRecordLost}'
         ELSE '{ReasonStaleStatistics}' END AS unavailable_reason,
    a.dead_rows,
    a.rows_changed,
    a.last_vacuum,
    a.last_autovacuum,
    a.last_analyze,
    a.last_autoanalyze,
    COALESCE(a.autovacuum_disabled, false) AS autovacuum_disabled,
    g.growth_bytes,
    a.past_floors,
    a.relpages,
    a.relallvisible,
    a.relallfrozen,
    round(100.0 * a.relallvisible / nullif(a.relpages, 0), 1)::double precision AS pct_all_visible,
    a.vac_trigger,
    a.rel_past_vac,
    a.ins_trigger,
    a.ins_since_vacuum,
    a.rel_past_ins,
    a.live_rows,
    a.tup_ins,
    a.tup_upd,
    a.tup_hot_upd,
    a.tup_newpage_upd,
    a.tup_del,
    a.autovacuum_count,
    a.autoanalyze_count,
    (si.since AT TIME ZONE 'UTC') AS stats_since,
    si.from_reset AS stats_from_reset,
    GREATEST(extract(epoch FROM (pg_catalog.now() - si.since)) / 3600.0, 0)::double precision AS stats_hours,
    a.tail_eligible,
    CASE WHEN a.past_floors THEN a.tail_rels END AS tail_rels
FROM scored AS a
CROSS JOIN statsinfo AS si
LEFT JOIN growth AS g ON g.kind = CASE a.kind WHEN 'partitioned' THEN 'table' ELSE a.kind END AND g.qualified_name = a.obj_schema || '.' || a.obj_name
ORDER BY a.past_floors DESC, a.empty_bytes DESC NULLS LAST, a.total_bytes DESC, a.obj_schema, a.obj_name";
    }

    /// <summary>
    /// The partitioned parents' indexes and the leaf indexes attached to them: <c>ptree</c> has one row per leaf index with the
    /// parent index it hangs under, found by walking <c>pg_partition_tree</c> on the parent index. A sub-partitioned leaf is
    /// walked down to its leaves.
    /// </summary>
    private static string PartitionIndexCtes() => $@"proots AS (
    SELECT c.oid, n.nspname, c.relname
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind = 'p'
    AND   NOT c.relispartition
    AND   NOT c.relisshared
    AND   c.relpersistence <> 't'
    AND   NOT {StoreSelfMetrics.SystemSchemaPredicateSql}
),
pidx AS (
    SELECT r.oid AS root_oid, r.nspname, r.relname, x.indexrelid, ic.relname AS index_name, x.indisvalid
    FROM proots AS r
    JOIN pg_catalog.pg_index AS x ON x.indrelid = r.oid
    JOIN pg_catalog.pg_class AS ic ON ic.oid = x.indexrelid
),
ptree AS (
    SELECT p.root_oid, p.nspname, p.relname, p.indexrelid AS root_index_oid, p.index_name, p.indisvalid AS parent_valid, t.relid AS leaf_index_oid
    FROM pidx AS p
    CROSS JOIN LATERAL pg_catalog.pg_partition_tree(p.indexrelid::regclass) AS t
    WHERE t.isleaf
)";

    /// <summary>The statement for the per-index read: see <see cref="StoreIndexHealthRow"/>.</summary>
    public static string BuildIndexSql(bool timescale) => $@"
WITH est AS MATERIALIZED (
{PgIndexBloatCollector.QueryText}
),
{BuildMembersCtes(timescale)},
{PartitionIndexCtes()},
{StatsInfoCte},
ix AS (
    SELECT
        m.obj_schema,
        m.obj_name,
        m.kind,
        (m.member_role = 'compressed') AS on_compressed,
        m.grouped,
        COALESCE(
            CASE WHEN m.kind = 'partitioned' THEN pt.index_name END,
            CASE WHEN m.grouped AND pg_catalog.starts_with(ic.relname, c.relname || '_')
                 THEN pg_catalog.substr(ic.relname, pg_catalog.length(c.relname) + 2) END,
            ic.relname) AS group_name,
        x.indisprimary,
        x.indisunique,
        x.indisvalid,
        pg_catalog.pg_relation_size(x.indexrelid) AS bytes,
        pg_catalog.pg_relation_size(c.oid) AS heap_bytes,
        COALESCE(s.idx_scan, 0) AS idx_scan,
        e.est_reclaimable_bytes,
        e.index_bytes AS est_index_bytes,
        e.skipped_reason
    FROM members AS m
    JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid
    JOIN pg_catalog.pg_index AS x ON x.indrelid = c.oid
    JOIN pg_catalog.pg_class AS ic ON ic.oid = x.indexrelid
    LEFT JOIN pg_catalog.pg_stat_all_indexes AS s ON s.indexrelid = x.indexrelid
    LEFT JOIN ptree AS pt ON pt.leaf_index_oid = x.indexrelid
    LEFT JOIN est AS e ON e.index_oid = x.indexrelid
),
grp AS (
    SELECT
        i.obj_schema,
        i.obj_name,
        i.kind,
        i.group_name,
        i.on_compressed,
        (count(*))::int AS index_count,
        (count(*) FILTER (WHERE i.grouped))::int AS leaf_count,
        sum(i.bytes)::bigint AS bytes,
        sum(i.heap_bytes)::bigint AS heap_bytes,
        sum(i.idx_scan)::bigint AS idx_scan,
        bool_or(i.indisprimary) AS is_primary,
        bool_or(i.indisunique) AS is_unique,
        bool_and(i.indisvalid) AS valid,
        (count(*) FILTER (WHERE i.bytes > i.heap_bytes AND i.bytes >= {IndexFlagMinimumBytes.ToString(CultureInfo.InvariantCulture)}))::int AS larger_count,
        sum(i.est_reclaimable_bytes)::bigint AS est_reclaimable_bytes,
        sum(i.est_index_bytes) FILTER (WHERE i.est_reclaimable_bytes IS NOT NULL)::bigint AS est_basis_bytes,
        (count(*) FILTER (WHERE i.est_reclaimable_bytes IS NOT NULL))::int AS est_usable,
        max(i.skipped_reason) AS skipped_reason
    FROM ix AS i
    GROUP BY i.obj_schema, i.obj_name, i.kind, i.group_name, i.on_compressed
)
SELECT
    g.obj_schema,
    g.obj_name,
    g.kind,
    g.group_name,
    g.on_compressed,
    g.index_count,
    g.leaf_count,
    g.bytes,
    g.heap_bytes,
    g.idx_scan,
    round(g.bytes::numeric / nullif(g.heap_bytes, 0), 2)::double precision AS index_to_heap,
    g.is_primary,
    g.is_unique,
    g.valid,
    g.larger_count,
    (si.since AT TIME ZONE 'UTC') AS stats_since,
    si.from_reset AS stats_from_reset,
    (NOT g.is_primary AND NOT g.is_unique
     AND si.since <= pg_catalog.now() - interval '{RarelyUsedMinStatsDays} days'
     AND g.idx_scan < {RarelyUsedMaxScans.ToString(CultureInfo.InvariantCulture)}) AS rarely_used,
    g.est_reclaimable_bytes,
    CASE WHEN g.est_basis_bytes > 0 THEN g.est_reclaimable_bytes::double precision / g.est_basis_bytes END AS est_bloat_share,
    g.est_usable,
    CASE WHEN g.est_usable > 0 THEN NULL
         ELSE COALESCE(g.skipped_reason, 'not estimated: not a valid, ready btree index') END AS est_skipped_reason
FROM grp AS g
CROSS JOIN statsinfo AS si
ORDER BY (NOT g.valid) DESC,
         (g.larger_count > 0) DESC,
         (NOT g.is_primary AND NOT g.is_unique AND si.since <= pg_catalog.now() - interval '{RarelyUsedMinStatsDays} days' AND g.idx_scan < {RarelyUsedMaxScans.ToString(CultureInfo.InvariantCulture)}) DESC,
         g.bytes DESC, g.obj_schema, g.obj_name, g.group_name";

    /// <summary>The statement for the leaf index coverage of partitioned parents: see <see cref="StoreIndexCoverageRow"/>.</summary>
    public static readonly string IndexCoverageSql = $@"
WITH {PartitionIndexCtes()},
pleaves AS (
    SELECT r.oid AS root_oid, t.relid AS leaf_oid
    FROM proots AS r
    CROSS JOIN LATERAL pg_catalog.pg_partition_tree(r.oid::regclass) AS t
    WHERE t.isleaf
),
have AS (
    SELECT t.root_oid, t.root_index_oid, x.indrelid AS leaf_oid, (x.indisvalid AND x.indisready) AS ok
    FROM ptree AS t
    JOIN pg_catalog.pg_index AS x ON x.indexrelid = t.leaf_index_oid
)
SELECT
    p.nspname,
    p.relname,
    p.index_name,
    p.indisvalid,
    (SELECT count(*) FROM pleaves AS l WHERE l.root_oid = p.root_oid)::int AS leaf_count,
    (SELECT count(*) FROM have AS h WHERE h.root_index_oid = p.indexrelid)::int AS leaves_with,
    (SELECT array_agg(q.name ORDER BY q.name) FROM (
        SELECT pg_catalog.format('%I.%I', ln.nspname, lc.relname) AS name
        FROM pleaves AS l
        JOIN pg_catalog.pg_class AS lc ON lc.oid = l.leaf_oid
        JOIN pg_catalog.pg_namespace AS ln ON ln.oid = lc.relnamespace
        WHERE l.root_oid = p.root_oid
        AND   NOT EXISTS (SELECT 1 FROM have AS h WHERE h.root_index_oid = p.indexrelid AND h.leaf_oid = l.leaf_oid)
        ORDER BY 1 LIMIT {CoverageNamesCap}) AS q) AS missing_leaves,
    (SELECT count(*) FROM have AS h WHERE h.root_index_oid = p.indexrelid AND NOT h.ok)::int AS leaves_invalid,
    (SELECT array_agg(q.name ORDER BY q.name) FROM (
        SELECT pg_catalog.format('%I.%I', ln.nspname, lc.relname) AS name
        FROM have AS h
        JOIN pg_catalog.pg_class AS lc ON lc.oid = h.leaf_oid
        JOIN pg_catalog.pg_namespace AS ln ON ln.oid = lc.relnamespace
        WHERE h.root_index_oid = p.indexrelid AND NOT h.ok
        ORDER BY 1 LIMIT {CoverageNamesCap}) AS q) AS invalid_leaves
FROM pidx AS p
ORDER BY (SELECT count(*) FROM pleaves AS l WHERE l.root_oid = p.root_oid)
         > (SELECT count(*) FROM have AS h WHERE h.root_index_oid = p.indexrelid) DESC,
         (SELECT count(*) FROM have AS h WHERE h.root_index_oid = p.indexrelid AND NOT h.ok) DESC,
         p.nspname, p.relname, p.index_name";

    private static async Task<(bool Timescale, int ServerVersion)> ProbeAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var probe = postgres.CreateCommand(TimescaleProbeSql + ", pg_catalog.current_setting('server_version_num')::int");
        probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetBoolean(0), reader.GetInt32(1));
    }

    /// <summary>
    /// Reads every store table's health, in the order the surfaces show it: rewrite candidates first, then objects past the
    /// floors whose tail could not be judged, then estimated empty bytes descending (rows with no estimate last), then total
    /// bytes descending. The probe, the read, and one small read per object past the floors (its last
    /// <see cref="TailProbePages"/> pages, for the emptiest <see cref="TailProbeMaxRelations"/> relations). Each statement is
    /// bounded by <see cref="StorageCommandDeadlines.McpReadSeconds"/>.
    /// </summary>
    public static async Task<List<StoreTableHealthRow>> GetAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var (timescale, serverVersion) = await ProbeAsync(postgres, cancellationToken);

        var read = new List<(StoreTableHealthRow Row, string[] TailRelations)>();
        await using (var command = postgres.CreateCommand(BuildSql(timescale, serverVersion)))
        {
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new StoreTableHealthRow(
                    SchemaName: reader.GetString(0),
                    TableName: reader.GetString(1),
                    Kind: reader.GetString(2),
                    TotalBytes: reader.GetInt64(3),
                    HeapBytes: reader.GetInt64(4),
                    IndexBytes: reader.GetInt64(5),
                    ToastBytes: reader.GetInt64(6),
                    EstimatedRows: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                    ChunkCount: reader.GetInt32(8),
                    LeafCount: reader.GetInt32(9),
                    EstimatedEmptyBytes: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                    EstimatedEmptyShare: reader.IsDBNull(11) ? null : reader.GetDouble(11),
                    EstimateRelations: reader.GetInt32(12),
                    EstimateRelationsUsable: reader.GetInt32(13),
                    EstimateUnavailableReason: reader.IsDBNull(14) ? null : reader.GetString(14),
                    DeadRows: reader.GetInt64(15),
                    RowsChangedSinceAnalyze: reader.GetInt64(16),
                    LastVacuum: Utc(reader, 17),
                    LastAutovacuum: Utc(reader, 18),
                    LastAnalyze: Utc(reader, 19),
                    LastAutoanalyze: Utc(reader, 20),
                    AutovacuumDisabled: reader.GetBoolean(21),
                    GrowthBytes7d: reader.IsDBNull(22) ? null : reader.GetInt64(22),
                    RewriteCandidate: false,
                    RelPages: reader.GetInt64(24),
                    RelAllVisible: reader.GetInt64(25),
                    RelAllFrozen: reader.IsDBNull(26) ? null : reader.GetInt64(26),
                    PctAllVisible: reader.IsDBNull(27) ? null : reader.GetDouble(27),
                    VacuumTriggerRows: reader.IsDBNull(28) ? null : reader.GetDouble(28),
                    RelationsPastVacuumTrigger: reader.GetInt32(29),
                    InsertTriggerRows: reader.IsDBNull(30) ? null : reader.GetDouble(30),
                    InsertsSinceVacuum: reader.GetInt64(31),
                    RelationsPastInsertTrigger: reader.GetInt32(32),
                    LiveRows: reader.GetInt64(33),
                    TuplesInserted: reader.GetInt64(34),
                    TuplesUpdated: reader.GetInt64(35),
                    TuplesHotUpdated: reader.GetInt64(36),
                    TuplesNewPageUpdated: reader.IsDBNull(37) ? null : reader.GetInt64(37),
                    TuplesDeleted: reader.GetInt64(38),
                    AutovacuumCount: reader.GetInt64(39),
                    AutoanalyzeCount: reader.GetInt64(40),
                    StatsSince: Utc(reader, 41),
                    StatsSinceSource: reader.GetBoolean(42) ? StatsSourceReset : StatsSourcePostmasterStart,
                    StatsHours: reader.GetDouble(43),
                    PastRewriteFloors: reader.GetBoolean(23),
                    TailRelationsEligible: reader.GetInt32(44),
                    TailRelationsChecked: 0,
                    TailRelationsOccupied: 0,
                    TailOccupied: null);
                read.Add((row, reader.IsDBNull(45) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(45)));
            }
        }

        var rows = new List<StoreTableHealthRow>(read.Count);
        foreach (var (row, tailRelations) in read)
        {
            rows.Add(row.PastRewriteFloors && tailRelations.Length > 0
                ? await WithTailAsync(postgres, row, tailRelations, cancellationToken)
                : row);
        }

        return rows
            .OrderByDescending(r => r.RewriteCandidate)
            .ThenByDescending(r => r.PastRewriteFloors)
            .ThenByDescending(r => r.EstimatedEmptyBytes ?? long.MinValue)
            .ThenByDescending(r => r.TotalBytes)
            .ThenBy(r => r.SchemaName, StringComparer.Ordinal)
            .ThenBy(r => r.TableName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// The tail statement: one row per relation, true when the relation holds a live row in its last
    /// <see cref="TailProbePages"/> pages. A TID range scan reads only those pages, but only when the bound is a sub-select:
    /// <c>pg_relation_size</c> is volatile, and a volatile bound in the filter makes PostgreSQL scan the whole table instead
    /// (measured: a minute on a cold 1 GB table). <paramref name="qualifiedRelations"/> come from
    /// <c>format('%I.%I')</c> in the read, already quoted.
    /// </summary>
    public static string BuildTailProbeSql(IReadOnlyList<string> qualifiedRelations)
    {
        ArgumentNullException.ThrowIfNull(qualifiedRelations);
        return string.Join("\nUNION ALL\n", qualifiedRelations.Select((q, i) =>
            string.Create(CultureInfo.InvariantCulture,
                $@"SELECT {i} AS ord, EXISTS (SELECT 1 FROM {q} WHERE ctid >= (SELECT pg_catalog.format('(%s,0)', GREATEST(pg_catalog.pg_relation_size('{q.Replace("'", "''", StringComparison.Ordinal)}'::pg_catalog.regclass) / pg_catalog.current_setting('block_size')::bigint - {TailProbePages}, 0))::pg_catalog.tid)) AS occupied")));
    }

    private static async Task<StoreTableHealthRow> WithTailAsync(NpgsqlDataSource postgres, StoreTableHealthRow row, string[] relations, CancellationToken cancellationToken)
    {
        try
        {
            var occupied = 0;
            await using var command = postgres.CreateCommand(BuildTailProbeSql(relations));
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.GetBoolean(1))
                {
                    occupied++;
                }
            }

            return row with
            {
                TailRelationsChecked = relations.Length,
                TailRelationsOccupied = occupied,
                TailOccupied = occupied > 0,
                RewriteCandidate = occupied > 0,
            };
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            /* A relation dropped between the two statements, or a permission the login lacks. The tail is then unknown, not
               empty: TailOccupied stays NULL and PastRewriteFloors still says the object is large and mostly empty. */
            return row;
        }
    }

    /// <summary>Reads every store index, rolled up (see <see cref="StoreIndexHealthRow"/>): invalid first, then larger than
    /// the heap, then rarely used, then by size.</summary>
    public static async Task<List<StoreIndexHealthRow>> GetIndexesAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var (timescale, _) = await ProbeAsync(postgres, cancellationToken);
        var rows = new List<StoreIndexHealthRow>();
        await using var command = postgres.CreateCommand(BuildIndexSql(timescale));
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var larger = reader.GetInt32(14);
            rows.Add(new StoreIndexHealthRow(
                SchemaName: reader.GetString(0),
                TableName: reader.GetString(1),
                Kind: reader.GetString(2),
                IndexName: reader.GetString(3),
                OnCompressedData: reader.GetBoolean(4),
                IndexCount: reader.GetInt32(5),
                LeafCount: reader.GetInt32(6),
                Bytes: reader.GetInt64(7),
                HeapBytes: reader.GetInt64(8),
                IndexScans: reader.GetInt64(9),
                IndexToHeapRatio: reader.IsDBNull(10) ? null : reader.GetDouble(10),
                IsPrimaryKey: reader.GetBoolean(11),
                IsUnique: reader.GetBoolean(12),
                Valid: reader.GetBoolean(13),
                LargerThanHeap: larger > 0,
                LargerThanHeapCount: larger,
                RarelyUsed: reader.GetBoolean(17),
                StatsSince: Utc(reader, 15),
                StatsSinceSource: reader.GetBoolean(16) ? StatsSourceReset : StatsSourcePostmasterStart,
                EstimatedReclaimableBytes: reader.IsDBNull(18) ? null : reader.GetInt64(18),
                EstimatedBloatShare: reader.IsDBNull(19) ? null : reader.GetDouble(19),
                EstimateIndexesUsable: reader.GetInt32(20),
                EstimateSkippedReason: reader.IsDBNull(21) ? null : reader.GetString(21)));
        }

        return rows;
    }

    /// <summary>Reads the leaf index coverage of every partitioned parent's indexes: missing or invalid leaf indexes first.</summary>
    public static async Task<List<StoreIndexCoverageRow>> GetIndexCoverageAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<StoreIndexCoverageRow>();
        await using var command = postgres.CreateCommand(IndexCoverageSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var leaves = reader.GetInt32(4);
            var with = reader.GetInt32(5);
            rows.Add(new StoreIndexCoverageRow(
                SchemaName: reader.GetString(0),
                TableName: reader.GetString(1),
                IndexName: reader.GetString(2),
                ParentIndexValid: reader.GetBoolean(3),
                LeafCount: leaves,
                LeavesWithIndex: with,
                LeavesMissingIndex: leaves - with,
                MissingLeaves: reader.IsDBNull(6) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(6),
                LeavesInvalidIndex: reader.GetInt32(7),
                InvalidLeaves: reader.IsDBNull(8) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(8)));
        }

        return rows;
    }

    private static DateTime? Utc(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);
}
