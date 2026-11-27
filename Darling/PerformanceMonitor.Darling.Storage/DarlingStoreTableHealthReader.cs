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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

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

    /// <summary>The note on a btree index that is not valid or not ready, so it has no estimate.</summary>
    public const string InvalidIndexReason = "The index is not valid or not ready, so no estimate is made.";

    /// <summary>The note on a valid, ready index of another access method (gin, gist, hash, brin, spgist): the estimate models btree pages only.
    /// A <c>format()</c> pattern whose one <c>%s</c> is the access method's name.</summary>
    public const string NonBtreeIndexReasonFormat = "The estimate covers btree indexes only; this is a %s index.";

    /// <summary>The note on a valid, ready btree index the estimate returned no row for.</summary>
    public const string NoEstimateRowReason = "The estimate returned no figure for this index.";

    /// <summary>
    /// The plain sentence for an index estimate's stored reason. The index bloat collector writes its reasons for people who
    /// read PostgreSQL catalogs ("reltuples = -1", "pg_stats", "name-typed"), and the collected data elsewhere keeps that text, so
    /// the collector is left alone and this read maps each known reason to a sentence the store health page, the Viewer tab and
    /// the MCP tool can show as it is. A reason this build does not recognize passes through unchanged.
    /// </summary>
    public static string? PlainEstimateReason(string? stored)
    {
        if (string.IsNullOrWhiteSpace(stored))
        {
            return stored;
        }

        return PgIndexBloatCoverage.Bucket(stored) switch
        {
            PgIndexBloatSuppression.Partial => "The index covers only some rows of its table, so no estimate is made.",
            PgIndexBloatSuppression.ParentNeverAnalyzed => "The table has never been analyzed, so no estimate is made.",
            PgIndexBloatSuppression.NameTypedKey => "A key column uses the name type, so no estimate is made.",
            PgIndexBloatSuppression.ColumnWidthsNotVisible => "The column sizes are not visible for every key column, so no estimate is made.",
            PgIndexBloatSuppression.NoPagesYet => "The index holds no pages yet, so there is nothing to estimate.",
            PgIndexBloatSuppression.NegativeBloat => "The estimate came out below zero, so it is not shown.",
            _ => stored,
        };
    }

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
    /// the estimated empty space is large AND plain VACUUM cannot give it back (see <see cref="DarlingVacuumTruncateRule"/>), so
    /// only a rewrite does. False when VACUUM can give it back, and when the tail was not read or could not be judged (see
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
    /// anywhere. Only a real reset dates the counters: the postmaster start is later than their true start, so no rate is worked out
    /// from it (see <see cref="RatesKnown"/>).</param>
    /// <param name="StatsSinceSource"><see cref="StatsSourceReset"/> or <see cref="StatsSourcePostmasterStart"/>. The postmaster
    /// start is NOT when the counters began: statistics survive a clean restart, so they can be much older. With that source the
    /// age of the counters is unknown and the per-day rates are NULL (#8823).</param>
    /// <param name="StatsHours">Hours between <see cref="StatsSince"/> and the read. Rates over a short window are noisy. Not the age of
    /// the counters when the source is the postmaster start.</param>
    /// <param name="PastRewriteFloors">True when <see cref="EstimatedEmptyShare"/> is at least
    /// <see cref="RewriteCandidateMinShare"/> and <see cref="EstimatedEmptyBytes"/> at least
    /// <see cref="RewriteCandidateMinEmptyBytes"/>. This was the whole of <see cref="RewriteCandidate"/> before the tail check.</param>
    /// <param name="TailRelationsEligible">For an object past the floors: how many of its heap relations are estimated at half
    /// empty or more, the pool the tail check picks from. Otherwise 0.</param>
    /// <param name="TailRelationsChecked">How many of those could be judged: the emptiest first, at most
    /// <see cref="TailProbeMaxRelations"/>, leaving out a relation whose window was over <see cref="TailProbeMaxWindowPages"/> pages
    /// or whose read failed. 0 when the object is not past the floors.</param>
    /// <param name="TailRelationsOccupied">How many of the checked relations VACUUM cannot give the space back for: a live row
    /// sits in the window at the end of the file, or the table's <c>vacuum_truncate</c> option is off.</param>
    /// <param name="TailOccupied">True when a checked relation's space VACUUM cannot give back, so only a rewrite returns it. False
    /// when every eligible relation was checked and VACUUM can give each one back. NULL when the object is not past the floors (not
    /// read), or when no relation was found occupied and some could not be judged (a window over the cap, a read that failed, or
    /// more eligible relations than are checked): the tail is then unknown, never "VACUUM can shrink it".</param>
    /// <param name="AutovacuumCancels">Autovacuums of the object's relations cancelled in the <see cref="CancelWindow"/> before the
    /// read, from the store log (<c>collect.store_log_events</c>, class <c>autovacuum_cancel</c>). A cancel on a TOAST table counts
    /// for the table that owns it, and a cancel on a chunk or partition for its hypertable or partitioned table. A cancel whose
    /// relation no longer exists (a dropped chunk) counts for no table.</param>
    /// <param name="LastAutovacuumCancel">When the latest of those cancels was captured (UTC). NULL when there were none.</param>
    /// <param name="RelationsPastTrigger">Heap relations past their vacuum trigger, or past their insert trigger on a plain table, with
    /// autovacuum on. A relation with autovacuum turned off is left out: nothing runs on it by design.</param>
    /// <param name="Behind">True when a relation counted in <see cref="RelationsPastTrigger"/> has had no vacuum, automatic or manual,
    /// complete within <see cref="BehindWindow"/> before the read. Autovacuum is not keeping up with it.</param>
    /// <param name="BehindSince">The oldest last completed vacuum among the relations past a trigger (UTC), or when the statistics
    /// began (<see cref="StatsSince"/>) when one of them has had no vacuum since. NULL when no relation is past a trigger.</param>
    /// <param name="BehindSinceSource"><see cref="BehindSourceLastVacuum"/> or <see cref="BehindSourceStatsSince"/>, saying which of
    /// the two <see cref="BehindSince"/> is. NULL with it.</param>
    /// <param name="BehindHours">Hours from <see cref="BehindSince"/> to the read: how long the relation past its trigger has gone
    /// without a completed vacuum. The statistics do not say when it crossed the trigger, so it has been past it for at most that
    /// long.</param>
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
        bool? TailOccupied,
        long AutovacuumCancels = 0,
        DateTime? LastAutovacuumCancel = null,
        int RelationsPastTrigger = 0,
        bool Behind = false,
        DateTime? BehindSince = null,
        string? BehindSinceSource = null,
        double? BehindHours = null)
    {
        /// <summary>True when autovacuum on the object was cancelled more than once in the <see cref="CancelWindow"/>
        /// (<see cref="RepeatedCancelMinimum"/> or more). A table cancelled again and again never finishes a vacuum.</summary>
        public bool RepeatedlyCancelled => AutovacuumCancels >= RepeatedCancelMinimum;

        /// <summary>Share of updates that were HOT (heap-only, no index entry written), 0 to 1. NULL when there were no updates.</summary>
        public double? HotUpdateShare => TuplesUpdated > 0 ? (double)TuplesHotUpdated / TuplesUpdated : null;

        /// <summary>True when <see cref="StatsSince"/> is a statistics reset, so the counters' age is known and the per-day rates are
        /// worked out. False when the source is the postmaster start: the counters are older than that start by an unknown amount, and
        /// dividing them by the time since it gives a rate too high by the ratio of the two ages (#8823), so no rate is given.</summary>
        public bool RatesKnown => StatsSinceSource == StatsSourceReset;

        /// <summary>Rows inserted per day since <see cref="StatsSince"/>. NULL when the window is empty or the counters' age is unknown
        /// (<see cref="RatesKnown"/>).</summary>
        public double? InsertsPerDay => PerDay(TuplesInserted, StatsHours);

        /// <summary>Row updates per day since <see cref="StatsSince"/>.</summary>
        public double? UpdatesPerDay => PerDay(TuplesUpdated, StatsHours);

        /// <summary>Rows deleted per day since <see cref="StatsSince"/>.</summary>
        public double? DeletesPerDay => PerDay(TuplesDeleted, StatsHours);

        /// <summary><see cref="DeletesPerDay"/> over <see cref="LiveRows"/>: 1.0 means the table's whole contents are deleted and
        /// replaced once a day. This is what showed a table churning daily while its file did not grow. NULL with no live rows
        /// or no window.</summary>
        public double? TurnoverPerDay => LiveRows > 0 && DeletesPerDay is double d ? d / LiveRows : null;

        private double? PerDay(long count, double hours) => RatesKnown && hours > 0 ? count / (hours / 24.0) : null;
    }

    /// <summary>How far back the cancel count looks from the read.</summary>
    public static readonly TimeSpan CancelWindow = TimeSpan.FromHours(24);

    /// <summary>Cancels in <see cref="CancelWindow"/> at which an object is <see cref="StoreTableHealthRow.RepeatedlyCancelled"/>: more
    /// than one a day.</summary>
    public const int RepeatedCancelMinimum = 2;

    /// <summary>How long a relation past its trigger may go without a completed vacuum before the object is
    /// <see cref="StoreTableHealthRow.Behind"/>. Autovacuum checks each database every <c>autovacuum_naptime</c> (a minute by
    /// default), so a relation past its trigger for a day is not waiting for its turn.</summary>
    public static readonly TimeSpan BehindWindow = TimeSpan.FromHours(24);

    /// <summary><see cref="StoreTableHealthRow.BehindSince"/> is the oldest last completed vacuum among the relations past a trigger.</summary>
    public const string BehindSourceLastVacuum = "last_vacuum";

    /// <summary><see cref="StoreTableHealthRow.BehindSince"/> is when the statistics began: a relation past a trigger has had no
    /// vacuum since.</summary>
    public const string BehindSourceStatsSince = "stats_since";

    /// <summary>The counters began at <c>pg_stat_database.stats_reset</c>.</summary>
    public const string StatsSourceReset = "stats_reset";

    /// <summary>The statistics were never reset, so the counters' start is unknown: the postmaster start is later than it, because
    /// statistics survive a clean restart. It is shown as "before" that start, and no per-day rate is given.</summary>
    public const string StatsSourcePostmasterStart = "postmaster_start";

    /// <summary>The most pages the tail check reads for one relation (256 MB at the 8 KB block size). The window the check reads is the
    /// larger of PostgreSQL's truncate minimum and the pages half the estimated empty bytes fill (see
    /// <see cref="DarlingVacuumTruncateRule.TailWindowPages"/>), and a file with no live row in it is read to the end of the window.
    /// A window over this cap is not read: the tail of that relation is unknown, which the row says.</summary>
    public const long TailProbeMaxWindowPages = 32768;

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
    /// <param name="EstimateSkippedReason">NULL when any index in the row has an estimate. Otherwise the estimate's own reason, or the
    /// note that fits the index: not valid or not ready (<see cref="InvalidIndexReason"/>), another access method than btree
    /// (<see cref="NonBtreeIndexReasonFormat"/>), or no figure returned (<see cref="NoEstimateRowReason"/>).</param>
    /// <param name="EstimatedBasisBytes">The size, as the estimate saw it, of the indexes it covered: what
    /// <see cref="EstimatedBloatShare"/> is a share of. Under <see cref="Bytes"/> when the estimate covered only some of the row's
    /// copies, so a rolled-up row can say which size its share is of. NULL when nothing was estimated.</param>
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
        string? EstimateSkippedReason,
        long? EstimatedBasisBytes = null);

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
    /// <param name="Behind">Rows flagged <see cref="StoreTableHealthRow.Behind"/>.</param>
    /// <param name="RepeatedlyCancelled">Rows flagged <see cref="StoreTableHealthRow.RepeatedlyCancelled"/>.</param>
    public sealed record StoreTableHealthSummary(
        int TableCount,
        long TotalBytes,
        int RewriteCandidates,
        int WithoutEstimate,
        IReadOnlyList<KeyValuePair<string, int>> WithoutEstimateByReason,
        int Behind = 0,
        int RepeatedlyCancelled = 0);

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
            byReason,
            rows.Count(r => r.Behind),
            rows.Count(r => r.RepeatedlyCancelled));
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
    /* Chunk membership from core inheritance, not timescaledb_information.chunks: that view joins every chunk to its dimension
       slices and to all of pg_class, and its row count is estimated far too low, which let the planner pick the wrong join order
       (#9224). No column of _timescaledb_catalog.chunk is named: TimescaleDB 2.29 rebuilt that catalog, so a column of it exists on
       one side of that version only. The compression_settings reads (relid, compress_relid), here and in TimescaleMemberArms, are
       the catalog columns still named, and both exist on 2.28.1 and on 2.29 and later. A chunk dropped by retention has no relation
       and no pg_inherits row, as in the view. The
       tiered-storage (OSM) chunk is a foreign table, which the view skipped.*/
    FROM root_rels AS t
    JOIN pg_catalog.pg_inherits AS inh ON inh.inhparent = t.rel_oid
    JOIN pg_catalog.pg_class AS pc ON pc.oid = inh.inhrelid AND pc.relkind <> 'f'
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

    /// <summary>
    /// The one extra condition the per-table read adds to the bloat estimate (#8806): only the relations that reach the size floor.
    /// The estimate is read only for a member relation's own heap and is NULL below the floor either way, so the rows are the
    /// same, and the estimate's column, statistics and size work is done for those relations and not for every chunk.
    /// </summary>
    private static readonly string EstimateRelationFilterSql =
        "\n              AND tbl.oid IN (SELECT z.rel_oid FROM relsz AS z WHERE z.member_role = 'heap' AND z.heap_bytes >= " + PgTableBloatStatsCollector.MinimumHeapBytes + ")";

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
{(timescale ? TimescaleMemberArms : string.Empty)}    SELECT pr.nspname AS obj_schema, pr.relname AS obj_name, 'table'::text AS kind, pr.oid AS rel_oid, 'heap'::text AS member_role, false AS grouped, false AS is_compressed
    FROM plain_roots AS pr
    WHERE pr.relkind <> 'p'
    UNION ALL
    /* Only the partitioned roots are walked: a set-returning function in a lateral join over every root is estimated at 1000
       rows per root, which made this CTE millions of estimated rows on a store with thousands of tables (#9224). */
    SELECT pr.nspname, pr.relname, 'partitioned'::text, t.relid, 'heap'::text, true, false
    FROM plain_roots AS pr
    CROSS JOIN LATERAL pg_catalog.pg_partition_tree(pr.oid::regclass) AS t
    WHERE pr.relkind = 'p' AND t.isleaf
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

    /// <summary>True when the relation's own storage options turn autovacuum off (<c>c</c> is its <c>pg_class</c> row).</summary>
    private const string AutovacuumDisabledSql = @"EXISTS (
            SELECT 1 FROM pg_catalog.unnest(c.reloptions) AS o(opt)
            WHERE o.opt ~* '^autovacuum_enabled=(f|fa|fal|fals|false|of|off|n|no|0)$')";

    /// <summary>
    /// The body of the LATERAL that works out a relation's two autovacuum triggers (<c>vac_trigger</c>, <c>ins_trigger</c>) from
    /// its <c>pg_class</c> row <c>c</c>. <paramref name="unfrozen"/> is the share of pages not all-frozen (PostgreSQL 18) or 1.
    /// </summary>
    private static string TriggerSql(string unfrozen) => $@"
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
        ) AS cur";

    /// <summary>
    /// The statement for the per-table read. <paramref name="serverVersion"/> is <c>server_version_num</c>: PostgreSQL 18 adds
    /// <c>relallfrozen</c> (and the unfrozen share of the insert trigger), 16 adds <c>n_tup_newpage_upd</c>; older majors read NULL.
    /// </summary>
    public static string BuildSql(bool timescale, int serverVersion = int.MaxValue, bool presized = false)
    {
        var relAllFrozen = serverVersion >= RelAllFrozenMinVersion ? "c.relallfrozen::bigint" : "NULL::bigint";
        var newPage = serverVersion >= NewPageUpdMinVersion ? "st.n_tup_newpage_upd" : "NULL::bigint";
        var unfrozen = serverVersion >= RelAllFrozenMinVersion
            ? "(CASE WHEN c.relpages > 0 THEN 1 - LEAST(c.relallfrozen, c.relpages)::float8 / c.relpages ELSE 1 END)"
            : "1";
        return $@"
WITH {BuildMembersCtes(timescale)},
relsz AS MATERIALIZED (
    /* Each member relation is sized once, here, and nothing below calls a size function again: on a store with thousands
       of chunks the size functions (a file stat or more per relation) are most of the time the read takes (#8806). */
    SELECT
        m.obj_schema,
        m.obj_name,
        m.kind,
        m.member_role,
        m.grouped,
        m.is_compressed,
        m.rel_oid,
        {(presized ? "COALESCE(z.heap_bytes, 0)" : "pg_catalog.pg_relation_size(m.rel_oid)")} AS heap_bytes,
        {(presized ? "COALESCE(z.index_bytes, 0)" : "pg_catalog.pg_indexes_size(m.rel_oid)")} AS index_bytes,
        {(presized ? "COALESCE(z.toast_bytes, 0)" : "COALESCE(pg_catalog.pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0)")} AS toast_bytes
    FROM members AS m
    JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid{(presized ? @"
    LEFT JOIN unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) AS z(rel_oid, heap_bytes, index_bytes, toast_bytes)
        ON m.rel_oid = z.rel_oid::oid" : string.Empty)}
),
est AS MATERIALIZED (
{PgTableBloatStatsCollector.BuildQueryText(EstimateRelationFilterSql)}
),
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
        m.heap_bytes,
        m.index_bytes,
        m.toast_bytes,
        c.reltuples,
        c.relpages::bigint AS relpages,
        c.relallvisible::bigint AS relallvisible,
        {relAllFrozen} AS relallfrozen,
        {AutovacuumDisabledSql} AS autovacuum_disabled,
        ({DarlingVacuumTruncateRule.TruncateOffSql}) AS truncate_off,
        m.heap_bytes / pg_catalog.current_setting('block_size')::bigint AS rel_heap_pages,
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
    FROM relsz AS m
    JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_stat_all_tables AS st ON st.relid = c.oid
    LEFT JOIN est AS l ON m.member_role = 'heap' AND l.schema_name = n.nspname AND l.table_name = c.relname
    LEFT JOIN LATERAL ({TriggerSql(unfrozen)}
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
        array_agg(s.rel_qualified ORDER BY s.tail_rank) FILTER (WHERE s.tail_rank <= {TailProbeMaxRelations}) AS tail_rels,
        array_agg(s.rel_heap_pages ORDER BY s.tail_rank) FILTER (WHERE s.tail_rank <= {TailProbeMaxRelations}) AS tail_pages,
        array_agg(s.bloat_bytes_estimate::bigint ORDER BY s.tail_rank) FILTER (WHERE s.tail_rank <= {TailProbeMaxRelations}) AS tail_empty_bytes,
        array_agg(s.truncate_off ORDER BY s.tail_rank) FILTER (WHERE s.tail_rank <= {TailProbeMaxRelations}) AS tail_truncate_off
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
    CASE WHEN a.past_floors THEN a.tail_rels END AS tail_rels,
    CASE WHEN a.past_floors THEN a.tail_pages END AS tail_pages,
    CASE WHEN a.past_floors THEN a.tail_empty_bytes END AS tail_empty_bytes,
    CASE WHEN a.past_floors THEN a.tail_truncate_off END AS tail_truncate_off
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

    /// <summary>
    /// The condition that keeps the estimate in the presized per-index read to the indexes it sized (#9224), in place of every
    /// btree index in the database. The size column the estimate carries is not read there: the read takes it from its own list.
    /// </summary>
    private const string EstimateIndexFilterSql = @"
    AND   c.oid::bigint = ANY($1::bigint[])";

    /// <summary>The statement for the per-index read: see <see cref="StoreIndexHealthRow"/>.</summary>
    public static string BuildIndexSql(bool timescale, bool presized = false) => $@"
WITH est AS MATERIALIZED (
{(presized ? PgIndexBloatCollector.BuildQueryText("0::bigint", EstimateIndexFilterSql) : PgIndexBloatCollector.QueryText)}
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
        (x.indisvalid AND x.indisready) AS usable,
        am.amname AS access_method,
        {(presized ? "COALESCE(zi.bytes, 0)" : "pg_catalog.pg_relation_size(x.indexrelid)")} AS bytes,
        {(presized ? "COALESCE(zh.bytes, 0)" : "pg_catalog.pg_relation_size(c.oid)")} AS heap_bytes,
        COALESCE(s.idx_scan, 0) AS idx_scan,
        e.est_reclaimable_bytes,
        {(presized ? "CASE WHEN e.index_oid IS NOT NULL THEN COALESCE(zi.bytes, 0) END" : "e.index_bytes")} AS est_index_bytes,
        e.skipped_reason
    FROM members AS m
    JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid
    JOIN pg_catalog.pg_index AS x ON x.indrelid = c.oid
    JOIN pg_catalog.pg_class AS ic ON ic.oid = x.indexrelid
    JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
    LEFT JOIN pg_catalog.pg_stat_all_indexes AS s ON s.indexrelid = x.indexrelid
    LEFT JOIN ptree AS pt ON pt.leaf_index_oid = x.indexrelid
    LEFT JOIN est AS e ON e.index_oid = x.indexrelid{(presized ? @"
    LEFT JOIN unnest($1::bigint[], $2::bigint[]) AS zi(rel_oid, bytes) ON x.indexrelid = zi.rel_oid::oid
    LEFT JOIN unnest($1::bigint[], $2::bigint[]) AS zh(rel_oid, bytes) ON c.oid = zh.rel_oid::oid" : string.Empty)}
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
        bool_or(NOT i.usable) AS any_unusable,
        max(i.access_method) FILTER (WHERE i.access_method <> 'btree') AS other_method,
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
         ELSE COALESCE(
                g.skipped_reason,
                CASE WHEN g.any_unusable THEN '{InvalidIndexReason}'
                     WHEN g.other_method IS NOT NULL THEN pg_catalog.format('{NonBtreeIndexReasonFormat}', g.other_method)
                     ELSE '{NoEstimateRowReason}' END) END AS est_skipped_reason,
    g.est_basis_bytes
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

    /// <summary>
    /// One object's autovacuum progress, from <see cref="BuildVacuumSql"/>: its cancels in the window and its relations past a
    /// trigger. <see cref="OldestVacuumWhilePast"/> is the oldest last completed vacuum (automatic or manual) among the relations
    /// past a trigger, <see cref="RelationsPastNeverVacuumed"/> counts those with none since the statistics began, and
    /// <see cref="StatsSince"/> is when the statistics began (as <see cref="StoreTableHealthRow.StatsSince"/>).
    /// </summary>
    public sealed record StoreVacuumObjectRow(
        string SchemaName,
        string TableName,
        string Kind,
        long Cancels,
        DateTime? LastCancel,
        int RelationsPast,
        int RelationsPastNeverVacuumed,
        DateTime? OldestVacuumWhilePast,
        DateTime? StatsSince)
    {
        /// <summary>True when the object's autovacuums were cancelled at least <see cref="RepeatedCancelMinimum"/> times in the window.</summary>
        public bool RepeatedlyCancelled => Cancels >= RepeatedCancelMinimum;
    }

    /// <summary>
    /// Whether an object is behind on autovacuum as of a read (<see cref="BehindAsOf"/>). <see cref="Since"/> is the oldest last
    /// completed vacuum among its relations past a trigger, or when the statistics began when one of them has had none
    /// (<see cref="Source"/> says which). <see cref="Hours"/> is the time from <see cref="Since"/> to the read.
    /// </summary>
    public readonly record struct StoreVacuumBehind(bool Behind, DateTime? Since, string? Source, double? Hours);

    /// <summary>
    /// Whether <paramref name="vacuum"/> is behind as of <paramref name="asOfUtc"/>: a relation past a trigger has had no
    /// completed vacuum, automatic or manual, within <see cref="BehindWindow"/>.
    /// </summary>
    public static StoreVacuumBehind BehindAsOf(StoreVacuumObjectRow vacuum, DateTime asOfUtc)
    {
        ArgumentNullException.ThrowIfNull(vacuum);
        DateTime? since = null;
        string? source = null;
        if (vacuum.RelationsPast > 0)
        {
            if (vacuum.OldestVacuumWhilePast is DateTime done)
            {
                since = done;
                source = BehindSourceLastVacuum;
            }

            if (vacuum.RelationsPastNeverVacuumed > 0 && vacuum.StatsSince is DateTime began && (since is null || began < since))
            {
                since = began;
                source = BehindSourceStatsSince;
            }
        }

        if (since is not DateTime from)
        {
            return default;
        }

        var asOf = asOfUtc.ToUniversalTime();
        return new StoreVacuumBehind(from <= asOf - BehindWindow, from, source, Math.Max(0, (asOf - from).TotalHours));
    }

    /// <summary>
    /// The statement for the autovacuum progress read: per object, the autovacuum cancels the store log holds between
    /// <c>@cancel_since</c> and <c>@cancel_until</c> (naive UTC, like <c>capture_time</c>) and the relations past a trigger. It reads
    /// the catalog, <c>pg_stat_all_tables</c> and one day of one class of the store log through its (event_class, capture_time)
    /// index, and calls no size function. Only objects with a cancel or a relation past a trigger come back.
    /// </summary>
    public static string BuildVacuumSql(bool timescale, int serverVersion = int.MaxValue)
    {
        var unfrozen = serverVersion >= RelAllFrozenMinVersion
            ? "(CASE WHEN c.relpages > 0 THEN 1 - LEAST(c.relallfrozen, c.relpages)::float8 / c.relpages ELSE 1 END)"
            : "1";
        return $@"
WITH {BuildMembersCtes(timescale)},
{StatsInfoCte},
cancel_log AS (
    /* The grouping key of a cancel names its relation, database first: canceling autovacuum task: db.schema.rel. A row whose
       key names no relation is left out here; the settings read counts every cancel. */
    SELECT
        pg_catalog.regexp_match(e.message_text, '^{AutovacuumCancelKeyPattern}(.+)\.([^.]+)\.([^.]+)$') AS p,
        e.occurrences,
        e.capture_time
    FROM collect.store_log_events AS e
    WHERE e.event_class = '{StoreLogClassifier.AutovacuumCancelClass}'
    AND   e.capture_time >= @cancel_since
    AND   e.capture_time < @cancel_until
),
cancel_rel AS (
    /* A TOAST table counts for the table that owns it. */
    SELECT
        COALESCE(owner.oid, c.oid) AS rel_oid,
        sum(l.occurrences)::bigint AS cancels,
        max(l.capture_time) AS last_cancel
    FROM cancel_log AS l
    JOIN pg_catalog.pg_namespace AS n ON n.nspname = l.p[2]
    JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = l.p[3]
    LEFT JOIN pg_catalog.pg_class AS owner ON n.nspname = 'pg_toast' AND owner.reltoastrelid = c.oid
    WHERE l.p[1] = pg_catalog.current_database()
    GROUP BY 1
),
rel AS (
    SELECT
        m.obj_schema,
        m.obj_name,
        m.kind,
        cr.cancels,
        cr.last_cancel,
        GREATEST(st.last_vacuum, st.last_autovacuum) AS last_done,
        COALESCE(m.member_role = 'heap' AND NOT {AutovacuumDisabledSql}
                 AND (st.n_dead_tup > trig.vac_trigger
                      OR (m.kind = 'table' AND st.n_ins_since_vacuum > trig.ins_trigger)), false) AS is_past
    FROM members AS m
    JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid
    LEFT JOIN pg_catalog.pg_stat_all_tables AS st ON st.relid = c.oid
    LEFT JOIN cancel_rel AS cr ON cr.rel_oid = m.rel_oid
    LEFT JOIN LATERAL ({TriggerSql(unfrozen)}
    ) AS trig ON m.member_role = 'heap'
)
SELECT
    r.obj_schema,
    r.obj_name,
    r.kind,
    COALESCE(sum(r.cancels), 0)::bigint AS cancels,
    max(r.last_cancel) AS last_cancel,
    (count(*) FILTER (WHERE r.is_past))::int AS rel_past,
    (count(*) FILTER (WHERE r.is_past AND r.last_done IS NULL))::int AS rel_past_never,
    (min(r.last_done) FILTER (WHERE r.is_past) AT TIME ZONE 'UTC') AS oldest_done,
    (SELECT si.since AT TIME ZONE 'UTC' FROM statsinfo AS si) AS stats_since
FROM rel AS r
GROUP BY r.obj_schema, r.obj_name, r.kind
HAVING sum(r.cancels) > 0 OR bool_or(r.is_past)";
    }

    /// <summary>The cancel key's prefix as a regular expression (<c>canceling autovacuum task: </c>).</summary>
    private const string AutovacuumCancelKeyPattern = "canceling autovacuum task: ";

    /// <summary>
    /// Reads every store object's autovacuum progress as of <paramref name="asOfUtc"/>: cancels in the <see cref="CancelWindow"/>
    /// ending there, and relations past a trigger. The probe and one statement, each bounded by
    /// <see cref="StorageCommandDeadlines.McpReadSeconds"/>. A statement past it throws <see cref="StoreTableHealthTimeoutException"/>.
    /// </summary>
    public static async Task<List<StoreVacuumObjectRow>> GetVacuumAsync(
        NpgsqlDataSource postgres, DateTime asOfUtc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        try
        {
            var (timescale, serverVersion) = await ProbeAsync(postgres, cancellationToken);
            return await ReadVacuumAsync(postgres, timescale, serverVersion, asOfUtc, cancellationToken);
        }
        catch (Exception ex) when (StoreTableHealthTimeoutException.IsDeadlineTrip(ex, cancellationToken))
        {
            throw new StoreTableHealthTimeoutException(ex);
        }
    }

    private static async Task<List<StoreVacuumObjectRow>> ReadVacuumAsync(
        NpgsqlDataSource postgres, bool timescale, int serverVersion, DateTime asOfUtc, CancellationToken cancellationToken)
    {
        var until = DateTime.SpecifyKind(asOfUtc.ToUniversalTime(), DateTimeKind.Unspecified);
        var rows = new List<StoreVacuumObjectRow>();
        await using var command = postgres.CreateCommand(BuildVacuumSql(timescale, serverVersion));
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>("cancel_since", NpgsqlDbType.Timestamp) { TypedValue = until - CancelWindow });
        command.Parameters.Add(new NpgsqlParameter<DateTime>("cancel_until", NpgsqlDbType.Timestamp) { TypedValue = until });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreVacuumObjectRow(
                SchemaName: reader.GetString(0),
                TableName: reader.GetString(1),
                Kind: reader.GetString(2),
                Cancels: reader.GetInt64(3),
                LastCancel: Utc(reader, 4),
                RelationsPast: reader.GetInt32(5),
                RelationsPastNeverVacuumed: reader.GetInt32(6),
                OldestVacuumWhilePast: Utc(reader, 7),
                StatsSince: Utc(reader, 8)));
        }

        return rows;
    }

    /// <summary>
    /// <paramref name="row"/> with its autovacuum progress from <paramref name="vacuum"/> (none when null) as of
    /// <paramref name="asOfUtc"/> (<see cref="BehindAsOf"/>).
    /// </summary>
    public static StoreTableHealthRow WithVacuum(StoreTableHealthRow row, StoreVacuumObjectRow? vacuum, DateTime asOfUtc)
    {
        ArgumentNullException.ThrowIfNull(row);
        if (vacuum is null)
        {
            return row;
        }

        var behind = BehindAsOf(vacuum, asOfUtc);
        return row with
        {
            AutovacuumCancels = vacuum.Cancels,
            LastAutovacuumCancel = vacuum.LastCancel,
            RelationsPastTrigger = vacuum.RelationsPast,
            Behind = behind.Behind,
            BehindSince = behind.Since,
            BehindSinceSource = behind.Source,
            BehindHours = behind.Hours,
        };
    }

    /// <summary>
    /// The store's autovacuum settings as they apply (the <c>autovacuum_*</c> value, or the plain <c>vacuum_*</c> or
    /// <c>maintenance_work_mem</c> value it falls back to at -1), the autovacuum workers running at the read (any role can count
    /// them in <c>pg_stat_activity</c>), and every autovacuum cancel the store log holds in the <see cref="CancelWindow"/>.
    /// </summary>
    /// <param name="CostLimit">The cost budget all running autovacuum workers share between sleeps.</param>
    /// <param name="CostDelayMs">How long a worker sleeps when the budget is spent. 0 means no sleep: vacuum is not throttled.</param>
    /// <param name="WriteBudgetMBps">The most autovacuum can dirty per second across all its workers: <c>CostLimit /
    /// vacuum_cost_page_dirty</c> pages per <c>CostDelayMs</c>, in MB (2^20 bytes). NULL when <c>CostDelayMs</c> is 0
    /// (unthrottled) or the page cost is 0.</param>
    /// <param name="CancelsInWindow">Every autovacuum cancel the store log holds in the window, whatever relation it names.</param>
    /// <param name="CancelsUnresolved">Those of <paramref name="CancelsInWindow"/> whose relation the catalog does not have: a
    /// relation since dropped (a chunk a retention policy removed), one in another database, or an entry that names none. They
    /// count for no table. A cancel on a system catalog resolves but is not a store table, so it is in neither figure.</param>
    public sealed record StoreAutovacuumSettings(
        bool AutovacuumOn,
        int MaxWorkers,
        int WorkersRunning,
        int NaptimeSeconds,
        int CostLimit,
        string CostLimitSetting,
        double CostDelayMs,
        string CostDelaySetting,
        long WorkMemKb,
        string WorkMemSetting,
        int CostPageDirty,
        int BlockSize,
        double? WriteBudgetMBps,
        long CancelsInWindow,
        long CancelsUnresolved);

    /// <summary>The statement for <see cref="GetAutovacuumSettingsAsync"/>. Reads <c>pg_settings</c>, counts autovacuum workers in
    /// <c>pg_stat_activity</c>, and sums one day of one class of the store log through its (event_class, capture_time) index,
    /// resolving each cancel's relation in the catalog.</summary>
    public static readonly string AutovacuumSettingsSql = $@"
WITH cancels AS (
    SELECT
        COALESCE(sum(e.occurrences), 0)::bigint AS total,
        COALESCE(sum(e.occurrences) FILTER (WHERE rel.oid IS NULL), 0)::bigint AS unresolved
    FROM collect.store_log_events AS e
    LEFT JOIN LATERAL (
        SELECT c.oid
        FROM pg_catalog.regexp_match(e.message_text, '^{AutovacuumCancelKeyPattern}(.+)\.([^.]+)\.([^.]+)$') AS p(m)
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = p.m[2]
        JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = p.m[3]
        WHERE p.m[1] = pg_catalog.current_database()
    ) AS rel ON true
    WHERE e.event_class = '{StoreLogClassifier.AutovacuumCancelClass}'
    AND   e.capture_time >= @cancel_since
    AND   e.capture_time < @cancel_until
)
SELECT
    max(s.setting) FILTER (WHERE s.name = 'autovacuum') = 'on',
    max(s.setting) FILTER (WHERE s.name = 'autovacuum_max_workers')::int,
    (SELECT count(*)::int FROM pg_catalog.pg_stat_activity AS a WHERE a.backend_type = 'autovacuum worker'),
    max(s.setting) FILTER (WHERE s.name = 'autovacuum_naptime')::int,
    max(s.setting) FILTER (WHERE s.name = 'autovacuum_vacuum_cost_limit')::int,
    max(s.setting) FILTER (WHERE s.name = 'vacuum_cost_limit')::int,
    max(s.setting) FILTER (WHERE s.name = 'autovacuum_vacuum_cost_delay')::float8,
    max(s.setting) FILTER (WHERE s.name = 'vacuum_cost_delay')::float8,
    max(s.setting) FILTER (WHERE s.name = 'autovacuum_work_mem')::bigint,
    max(s.setting) FILTER (WHERE s.name = 'maintenance_work_mem')::bigint,
    max(s.setting) FILTER (WHERE s.name = 'vacuum_cost_page_dirty')::int,
    max(s.setting) FILTER (WHERE s.name = 'block_size')::int,
    (SELECT k.total FROM cancels AS k),
    (SELECT k.unresolved FROM cancels AS k)
FROM pg_catalog.pg_settings AS s
WHERE s.name IN ('autovacuum', 'autovacuum_max_workers', 'autovacuum_naptime', 'autovacuum_vacuum_cost_limit', 'vacuum_cost_limit',
                 'autovacuum_vacuum_cost_delay', 'vacuum_cost_delay', 'autovacuum_work_mem', 'maintenance_work_mem',
                 'vacuum_cost_page_dirty', 'block_size')";

    /// <summary>
    /// Reads <see cref="StoreAutovacuumSettings"/> as of <paramref name="asOfUtc"/>. One statement, bounded by
    /// <see cref="StorageCommandDeadlines.McpReadSeconds"/>. A statement past it throws <see cref="StoreTableHealthTimeoutException"/>.
    /// </summary>
    public static async Task<StoreAutovacuumSettings> GetAutovacuumSettingsAsync(
        NpgsqlDataSource postgres, DateTime asOfUtc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        try
        {
            var until = DateTime.SpecifyKind(asOfUtc.ToUniversalTime(), DateTimeKind.Unspecified);
            await using var command = postgres.CreateCommand(AutovacuumSettingsSql);
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<DateTime>("cancel_since", NpgsqlDbType.Timestamp) { TypedValue = until - CancelWindow });
            command.Parameters.Add(new NpgsqlParameter<DateTime>("cancel_until", NpgsqlDbType.Timestamp) { TypedValue = until });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return Settings(
                autovacuumOn: !reader.IsDBNull(0) && reader.GetBoolean(0),
                maxWorkers: reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                workersRunning: reader.GetInt32(2),
                naptimeSeconds: reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                autovacuumCostLimit: reader.IsDBNull(4) ? -1 : reader.GetInt32(4),
                vacuumCostLimit: reader.IsDBNull(5) ? 200 : reader.GetInt32(5),
                autovacuumCostDelayMs: reader.IsDBNull(6) ? -1 : reader.GetDouble(6),
                vacuumCostDelayMs: reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                autovacuumWorkMemKb: reader.IsDBNull(8) ? -1 : reader.GetInt64(8),
                maintenanceWorkMemKb: reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                costPageDirty: reader.IsDBNull(10) ? 20 : reader.GetInt32(10),
                blockSize: reader.IsDBNull(11) ? 8192 : reader.GetInt32(11),
                cancelsInWindow: reader.GetInt64(12),
                cancelsUnresolved: reader.GetInt64(13));
        }
        catch (Exception ex) when (StoreTableHealthTimeoutException.IsDeadlineTrip(ex, cancellationToken))
        {
            throw new StoreTableHealthTimeoutException(ex);
        }
    }

    /// <summary>
    /// The settings as they apply, from the raw values: an <c>autovacuum_*</c> value of -1 falls back to its plain twin, and the
    /// write budget is worked out from what applies.
    /// </summary>
    public static StoreAutovacuumSettings Settings(
        bool autovacuumOn, int maxWorkers, int workersRunning, int naptimeSeconds, int autovacuumCostLimit, int vacuumCostLimit,
        double autovacuumCostDelayMs, double vacuumCostDelayMs, long autovacuumWorkMemKb, long maintenanceWorkMemKb,
        int costPageDirty, int blockSize, long cancelsInWindow, long cancelsUnresolved = 0)
    {
        var (limit, limitSetting) = autovacuumCostLimit >= 0
            ? (autovacuumCostLimit, "autovacuum_vacuum_cost_limit")
            : (vacuumCostLimit, "vacuum_cost_limit");
        var (delay, delaySetting) = autovacuumCostDelayMs >= 0
            ? (autovacuumCostDelayMs, "autovacuum_vacuum_cost_delay")
            : (vacuumCostDelayMs, "vacuum_cost_delay");
        var (workMem, workMemSetting) = autovacuumWorkMemKb >= 0
            ? (autovacuumWorkMemKb, "autovacuum_work_mem")
            : (maintenanceWorkMemKb, "maintenance_work_mem");
        double? budget = delay > 0 && costPageDirty > 0
            ? (double)limit / costPageDirty * (1000.0 / delay) * blockSize / (1024.0 * 1024.0)
            : null;
        return new StoreAutovacuumSettings(
            autovacuumOn, maxWorkers, workersRunning, naptimeSeconds, limit, limitSetting, delay, delaySetting,
            workMem, workMemSetting, costPageDirty, blockSize, budget, cancelsInWindow, cancelsUnresolved);
    }

    private static async Task<(bool Timescale, int ServerVersion)> ProbeAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var probe = postgres.CreateCommand(TimescaleProbeSql + ", pg_catalog.current_setting('server_version_num')::int");
        probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetBoolean(0), reader.GetInt32(1));
    }

    /// <summary>
    /// Reads every store table's health as of now. See <see cref="GetAsync(NpgsqlDataSource, DateTime, CancellationToken)"/>.
    /// </summary>
    public static Task<List<StoreTableHealthRow>> GetAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
        => GetAsync(postgres, DateTime.UtcNow, cancellationToken);

    /// <summary>
    /// Reads every store table's health as of <paramref name="asOfUtc"/>, in the order the surfaces show it: objects behind on
    /// autovacuum first (<see cref="StoreTableHealthRow.Behind"/>), then objects whose autovacuum was cancelled repeatedly in the
    /// <see cref="CancelWindow"/>, then rewrite candidates, then objects past the floors whose tail could not be judged, then
    /// estimated empty bytes descending (rows with no estimate last), then total bytes descending. The probe, the read, the
    /// autovacuum progress read (<see cref="BuildVacuumSql"/>), and one small read per object past the floors (a window at the end
    /// of the file for each of the emptiest <see cref="TailProbeMaxRelations"/> relations). Each statement is bounded by
    /// <see cref="StorageCommandDeadlines.McpReadSeconds"/>. A statement past it throws <see cref="StoreTableHealthTimeoutException"/>.
    /// </summary>
    public static async Task<List<StoreTableHealthRow>> GetAsync(
        NpgsqlDataSource postgres, DateTime asOfUtc, CancellationToken cancellationToken = default)
    {
        try
        {
            return await ReadTablesAsync(postgres, asOfUtc, cancellationToken);
        }
        catch (Exception ex) when (StoreTableHealthTimeoutException.IsDeadlineTrip(ex, cancellationToken))
        {
            throw new StoreTableHealthTimeoutException(ex);
        }
    }

    /// <summary>The same read, sized from <paramref name="sizes"/> (#9374) instead of a size pass of its own: the Viewer's first open hands in the
    /// last measured sizes the store holds (<see cref="StoreRelationSizesStore.ReadLastSizesAsync(NpgsqlDataSource, CancellationToken)"/>), so the read does no size work. A relation
    /// the map does not hold reads as empty, and one dropped since is not listed.</summary>
    public static async Task<List<StoreTableHealthRow>> GetAsync(
        NpgsqlDataSource postgres, DateTime asOfUtc, RelationSizePass sizes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sizes);
        try
        {
            return await ReadTablesAsync(postgres, asOfUtc, cancellationToken, sizes);
        }
        catch (Exception ex) when (StoreTableHealthTimeoutException.IsDeadlineTrip(ex, cancellationToken))
        {
            throw new StoreTableHealthTimeoutException(ex);
        }
    }

    /// <summary>The relations the per-index read sizes (#9170): every member relation (the heap each index hangs under) and every index on one.</summary>
    internal static string IndexRelationsSql(bool timescale) => $@"
WITH {BuildMembersCtes(timescale)}
SELECT m.rel_oid::bigint
FROM members AS m
JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid
UNION
SELECT x.indexrelid::bigint
FROM members AS m
JOIN pg_catalog.pg_index AS x ON x.indrelid = m.rel_oid";

    /// <summary>
    /// The main-fork size of every relation either read sizes, in one statement (#9364). A row carries the relation's oid, the heap
    /// it hangs under (0 for a heap) and <c>pg_relation_size(oid)</c>, which is the per-index read's size for an index and its heap
    /// and the per-table read's heap bytes. One stat per relation. This is the part of the sizing the per-index read waits for.
    /// $1 the relation oids.
    /// </summary>
    private const string MainSizesSql = @"
SELECT s.rel_oid,
       COALESCE(x.indrelid, 0)::bigint,
       COALESCE(pg_catalog.pg_relation_size(s.rel_oid::oid), 0)
FROM unnest($1::bigint[]) AS s(rel_oid)
JOIN pg_catalog.pg_class AS c ON c.oid = s.rel_oid::oid
LEFT JOIN pg_catalog.pg_index AS x ON x.indexrelid = c.oid";

    /// <summary>
    /// What only the per-table read needs, in one statement (#9364). An index row carries its other forks (free space, visibility and
    /// init): the main fork plus those is what <c>pg_indexes_size</c> adds up for the heap the index hangs under (it opens every index
    /// and stats every fork). A heap row carries its TOAST total, the same expression the per-table read ran. On a store with
    /// thousands of chunks the stats are the time the reads take (about half a millisecond each on Windows), and the forks other
    /// than the main one are most of them, so the per-index read does not wait for this statement.
    /// $1 the relation oids.
    /// </summary>
    private const string ExtraSizesSql = @"
SELECT s.rel_oid,
       CASE WHEN x.indexrelid IS NULL THEN 0
            ELSE COALESCE(pg_catalog.pg_relation_size(s.rel_oid::oid, 'fsm'), 0)
               + COALESCE(pg_catalog.pg_relation_size(s.rel_oid::oid, 'vm'), 0)
               + COALESCE(pg_catalog.pg_relation_size(s.rel_oid::oid, 'init'), 0) END,
       CASE WHEN x.indexrelid IS NULL THEN COALESCE(pg_catalog.pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0) ELSE 0 END
FROM unnest($1::bigint[]) AS s(rel_oid)
JOIN pg_catalog.pg_class AS c ON c.oid = s.rel_oid::oid
LEFT JOIN pg_catalog.pg_index AS x ON x.indexrelid = c.oid";

    /// <summary>The most store connections the size passes of one data source hold at once, both passes together (#9364).</summary>
    private const int SizeReadParallelism = 4;

    /// <summary>Fewest relations worth a connection of their own: a small store is sized on one.</summary>
    private const int SizeReadMinPerConnection = 200;

    /// <summary>
    /// How long one sizing pass serves the next read (#9224). The tables read and the indexes read each hold their rows for
    /// <see cref="DarlingStoreTableHealthCache.Lifetime"/>, so a size taken up to this much earlier is well inside what they already hold.
    /// It is long enough for the second of the two reads, which starts when the first ends.
    /// </summary>
    internal static readonly TimeSpan SizePassLifetime = TimeSpan.FromSeconds(15);

    /// <summary>The main pass: per relation its oid, the heap its index hangs under (0 for a heap) and its main-fork size.</summary>
    private sealed record MainSizePass(long[] Oids, long[] Parents, long[] Main)
    {
        /// <summary>The relation list this pass sized (#9374), so a main pass and an extras pass are only combined when they sized the same list.</summary>
        public long[]? Listed { get; init; }
    }

    /// <summary>The extras pass: per relation its oid, the size of its other forks (indexes only) and its TOAST total (heaps only).</summary>
    private sealed record ExtraSizePass(long[] Oids, long[] OtherForks, long[] Toast)
    {
        /// <summary>The relation list this pass sized (#9374).</summary>
        public long[]? Listed { get; init; }
    }

    /// <summary>What one relation list's main pass and extras pass sized together (#9374): per relation its oid, the heap its index hangs under
    /// (0 for a heap), its main-fork size, the size of its other forks (indexes only) and its TOAST total (heaps only). The store write and the
    /// reads that are handed their sizes take this shape.</summary>
    public sealed record RelationSizePass(long[] Oids, long[] Parents, long[] Main, long[] OtherForks, long[] Toast);

    /// <summary>The combined pass of one list: the extras are matched to the main rows by oid, and a relation the extras did not size has no
    /// other forks and no TOAST.</summary>
    private static RelationSizePass Combine(MainSizePass main, ExtraSizePass extras)
    {
        var otherForks = new Dictionary<long, long>(extras.Oids.Length);
        var toast = new Dictionary<long, long>(extras.Oids.Length);
        for (var i = 0; i < extras.Oids.Length; i++)
        {
            otherForks[extras.Oids[i]] = extras.OtherForks[i];
            toast[extras.Oids[i]] = extras.Toast[i];
        }

        return new RelationSizePass(
            main.Oids,
            main.Parents,
            main.Main,
            main.Oids.Select(o => otherForks.GetValueOrDefault(o)).ToArray(),
            main.Oids.Select(o => toast.GetValueOrDefault(o)).ToArray());
    }

    /// <summary>One data source's held list and passes, and how many relations each pass on it has sized (what a test counts).
    /// The relation list is read once for the main pass and the extras pass, which each hold their own result so that a failure
    /// of one never reaches the other.</summary>
    private sealed class SizePassSlot(TimeSpan lifetime)
    {
        public readonly SizeGate Gate = new(SizeReadParallelism);
        public readonly object HeldLock = new();
        public int Held;
        public int PeakHeld;
        public Func<SizePassPart, Task>? SliceHook;
        public readonly SingleFlightTtlCache<long[]> List = new(lifetime);
        public readonly SingleFlightTtlCache<MainSizePass> Main = new(lifetime);
        public readonly SingleFlightTtlCache<ExtraSizePass> Extras = new(lifetime);
        public long MainRelationsSized;
        public long ExtraRelationsSized;
        public Exception? ExtrasFault;

        private readonly object _pairLock = new();
        private MainSizePass? _pendingMain;
        private ExtraSizePass? _pendingExtras;

        /// <summary>The newest combined pass that finished on this data source and when it did (#9374), so the service writes the sizes a read just
        /// took instead of taking them again.</summary>
        public MeasuredSizePass? Last;

        /// <summary>Notes a finished main or extras pass. The combined pass is recorded (as <see cref="Last"/>) only when both halves of the same
        /// relation list have finished: a failed half never gets here, so it records nothing.</summary>
        public void Finished(MainSizePass? main, ExtraSizePass? extras)
        {
            lock (_pairLock)
            {
                _pendingMain = main ?? _pendingMain;
                _pendingExtras = extras ?? _pendingExtras;
                if (_pendingMain is { } m && _pendingExtras is { } e && m.Listed is not null && ReferenceEquals(m.Listed, e.Listed))
                {
                    Last = new MeasuredSizePass(Combine(m, e), DateTime.UtcNow);
                    _pendingMain = null;
                    _pendingExtras = null;
                }
            }
        }
    }

    /// <summary>Which read of a size pass holds a store connection (what a test seam reports).</summary>
    internal enum SizePassPart
    {
        /// <summary>The relation list read both passes share.</summary>
        RelationList,

        /// <summary>One slice of the main pass.</summary>
        Main,

        /// <summary>One slice of the extras pass.</summary>
        Extras,
    }

    /// <summary>
    /// The one limit on the store connections the size passes of a data source hold (#9364). The main pass and the extras pass each
    /// run their slices on up to <see cref="SizeReadParallelism"/> connections, and the tables read starts both together, so without
    /// a shared limit a cold read holds twice that. The size cache is one per data source and the data source's pool is shared (the
    /// service's worker pool carries collection too, and the web admin pool is small), so the passes together stay inside the one
    /// limit a single pass held before the split. A waiter holds nothing while it waits: the caller takes its connection after it is granted
    /// and returns the grant after the connection is closed.
    /// The main pass goes first, in two ways. A queued main slice is granted ahead of every queued extras slice. And no extras slice
    /// is granted while a caller is waiting on the main pass (the tables read counts as one from the moment it asks for the main
    /// pass), so the indexes read, which waits for the main pass only, never waits behind the extras' slices.
    /// </summary>
    internal sealed class SizeGate(int limit)
    {
        private readonly object _gate = new();
        private readonly Queue<TaskCompletionSource> _main = new();
        private readonly Queue<TaskCompletionSource> _extras = new();
        private int _free = limit;
        private int _mainCallers;

        public Task AcquireAsync(bool main)
        {
            var waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_gate)
            {
                (main ? _main : _extras).Enqueue(waiter);
                Grant();
            }

            return waiter.Task;
        }

        public void Release()
        {
            lock (_gate)
            {
                _free++;
                Grant();
            }
        }

        public void MainCallerStarted()
        {
            lock (_gate)
            {
                _mainCallers++;
            }
        }

        public void MainCallerFinished()
        {
            lock (_gate)
            {
                _mainCallers--;
                Grant();
            }
        }

        /// <summary>Hands out the free grants, main slices first. Runs under the lock.</summary>
        private void Grant()
        {
            while (_free > 0)
            {
                if (_main.Count > 0)
                {
                    _free--;
                    _main.Dequeue().SetResult();
                }
                else if (_extras.Count > 0 && _mainCallers == 0)
                {
                    _free--;
                    _extras.Dequeue().SetResult();
                }
                else
                {
                    break;
                }
            }
        }
    }

    /// <summary>Runs <paramref name="read"/> holding one grant of the data source's size limit, whatever way it ends. The read opens
    /// and closes its connection inside, so the grant is returned after the connection is closed. The wait for the grant is not
    /// cancellable: a pass is shared work that no caller's cancellation stops.</summary>
    private static async Task<T> HoldingAGrantAsync<T>(SizePassSlot slot, SizePassPart part, Func<Task<T>> read)
    {
        await slot.Gate.AcquireAsync(part != SizePassPart.Extras);
        try
        {
            lock (slot.HeldLock)
            {
                slot.Held++;
                slot.PeakHeld = Math.Max(slot.PeakHeld, slot.Held);
            }

            try
            {
                var hook = slot.SliceHook;
                if (hook is not null)
                {
                    await hook(part);
                }

                return await read();
            }
            finally
            {
                lock (slot.HeldLock)
                {
                    slot.Held--;
                }
            }
        }
        finally
        {
            slot.Gate.Release();
        }
    }

    private static readonly ConditionalWeakTable<NpgsqlDataSource, SizePassSlot> SizePasses = new();

    private static SizePassSlot SlotFor(NpgsqlDataSource postgres) => SizePasses.GetValue(postgres, static _ => new SizePassSlot(SizePassLifetime));

    /// <summary>The newest combined size pass that finished on <paramref name="postgres"/> (#9374), with the time it finished, or null when none has.
    /// It is recorded when both the main pass and the extras pass of one relation list have finished, so the service writes it to the store without a
    /// second pass. A failed extras pass records nothing.</summary>
    public static MeasuredSizePass? LastSizePassOn(NpgsqlDataSource postgres) => SlotFor(postgres).Last;

    /// <summary>How many relations the main size passes on <paramref name="postgres"/> have stat'ed so far. A test reads it to see
    /// which reads shared a pass.</summary>
    internal static long MainRelationsSizedOn(NpgsqlDataSource postgres) => SlotFor(postgres).MainRelationsSized;

    /// <summary>How many relations the extras size passes on <paramref name="postgres"/> have stat'ed so far.</summary>
    internal static long ExtraRelationsSizedOn(NpgsqlDataSource postgres) => SlotFor(postgres).ExtraRelationsSized;

    /// <summary>Makes every extras pass on <paramref name="postgres"/> fail with <paramref name="fault"/> (null clears it), so a test
    /// can see the per-index read does not depend on it.</summary>
    internal static void FailExtrasPassesOn(NpgsqlDataSource postgres, Exception? fault) => SlotFor(postgres).ExtrasFault = fault;

    /// <summary>How many size-pass grants (one per store connection) are held on <paramref name="postgres"/> now.</summary>
    internal static int SizeConnectionsHeldOn(NpgsqlDataSource postgres)
    {
        var slot = SlotFor(postgres);
        lock (slot.HeldLock)
        {
            return slot.Held;
        }
    }

    /// <summary>The most size-pass grants held at once on <paramref name="postgres"/> so far.</summary>
    internal static int PeakSizeConnectionsHeldOn(NpgsqlDataSource postgres)
    {
        var slot = SlotFor(postgres);
        lock (slot.HeldLock)
        {
            return slot.PeakHeld;
        }
    }

    /// <summary>Runs <paramref name="hook"/> (null clears it) each time a read of a size pass on <paramref name="postgres"/> has its
    /// grant and before it opens its connection. A test uses it to slow the reads so they overlap, to record the order they got
    /// the limit in, and to fail one.</summary>
    internal static void OnSizePassGrantOn(NpgsqlDataSource postgres, Func<SizePassPart, Task>? hook) => SlotFor(postgres).SliceHook = hook;

    /// <summary>
    /// The main-fork size of every relation the two reads use: the member heaps and every index on one (<see cref="IndexRelationsSql"/>),
    /// on up to <see cref="SizeReadParallelism"/> connections at once (#9170). The pass is held for <see cref="SizePassLifetime"/> and
    /// shared by callers in flight (one pass, whichever read asks first). A relation created after the pass has no size and counts
    /// as empty. One dropped since is not read. The per-index read waits for this pass only (#9364).
    /// </summary>
    private static Task<MainSizePass> GetMainSizePassAsync(NpgsqlDataSource postgres, bool timescale, CancellationToken cancellationToken)
    {
        var slot = SlotFor(postgres);

        /* The tables read asks for this pass before it asks for the extras, so the gate knows a caller is waiting on the main pass
           before any extras slice can ask for a grant. The count drops when the shared pass ends, not when this caller's wait is
           cancelled: the pass keeps running for the others. */
        slot.Gate.MainCallerStarted();
        var shared = slot.Main.GetOrStartAsync(() => ReadMainSizePassAsync(postgres, timescale, slot), CancellationToken.None);
        _ = shared.ContinueWith(
            static (done, state) =>
            {
                _ = done.Exception;
                ((SizeGate)state!).MainCallerFinished();
            },
            slot.Gate,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        return shared.WaitAsync(cancellationToken);
    }

    /// <summary>The other forks and TOAST totals of the same relations, held and shared the same way. The per-table read waits for
    /// this pass as well as the main one, and starts both together.</summary>
    private static Task<ExtraSizePass> GetExtraSizePassAsync(NpgsqlDataSource postgres, bool timescale, CancellationToken cancellationToken)
    {
        var slot = SlotFor(postgres);
        return slot.Extras.GetOrStartAsync(() => ReadExtraSizePassAsync(postgres, timescale, slot), cancellationToken);
    }

    /// <summary>The relations both passes size, listed once per pair of passes: whichever pass starts first lists them and the other
    /// takes the held list.</summary>
    private static Task<long[]> GetRelationListAsync(NpgsqlDataSource postgres, bool timescale, SizePassSlot slot)
        => slot.List.GetOrStartAsync(() => ReadRelationListAsync(postgres, timescale, slot), CancellationToken.None);

    /// <summary>Whichever pass starts the list, it takes a main grant: the main pass waits for the list, and an extras grant would
    /// wait for the main pass.</summary>
    private static Task<long[]> ReadRelationListAsync(NpgsqlDataSource postgres, bool timescale, SizePassSlot slot)
        => HoldingAGrantAsync(slot, SizePassPart.RelationList, async () =>
        {
            var oids = new List<long>();
            await using (var list = postgres.CreateCommand(IndexRelationsSql(timescale)))
            {
                list.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                await using var reader = await list.ExecuteReaderAsync(CancellationToken.None);
                while (await reader.ReadAsync(CancellationToken.None))
                {
                    oids.Add(reader.GetInt64(0));
                }
            }

            return oids.ToArray();
        });

    private static long[][] SliceForConnections(long[] oids)
    {
        var connections = Math.Clamp(oids.Length / SizeReadMinPerConnection, 1, SizeReadParallelism);
        return Enumerable.Range(0, connections)
            .Select(c => oids.Where((_, i) => i % connections == c).ToArray())
            .ToArray();
    }

    private static async Task<MainSizePass> ReadMainSizePassAsync(NpgsqlDataSource postgres, bool timescale, SizePassSlot slot)
    {
        var oids = await GetRelationListAsync(postgres, timescale, slot);
        var parts = await Task.WhenAll(SliceForConnections(oids).Select(slice => ReadMainSliceAsync(postgres, slot, slice)));
        var pass = new MainSizePass(
            parts.SelectMany(p => p.Oids).ToArray(),
            parts.SelectMany(p => p.Parents).ToArray(),
            parts.SelectMany(p => p.Main).ToArray())
        {
            Listed = oids,
        };
        Interlocked.Add(ref slot.MainRelationsSized, oids.Length);
        slot.Finished(pass, null);
        return pass;
    }

    private static async Task<ExtraSizePass> ReadExtraSizePassAsync(NpgsqlDataSource postgres, bool timescale, SizePassSlot slot)
    {
        var fault = slot.ExtrasFault;
        if (fault is not null)
        {
            throw fault;
        }

        var oids = await GetRelationListAsync(postgres, timescale, slot);
        var parts = await Task.WhenAll(SliceForConnections(oids).Select(slice => ReadExtraSliceAsync(postgres, slot, slice)));
        var pass = new ExtraSizePass(
            parts.SelectMany(p => p.Oids).ToArray(),
            parts.SelectMany(p => p.OtherForks).ToArray(),
            parts.SelectMany(p => p.Toast).ToArray())
        {
            Listed = oids,
        };
        Interlocked.Add(ref slot.ExtraRelationsSized, oids.Length);
        slot.Finished(null, pass);
        return pass;
    }

    private static Task<MainSizePass> ReadMainSliceAsync(NpgsqlDataSource postgres, SizePassSlot slot, long[] slice)
    {
        if (slice.Length == 0)
        {
            return Task.FromResult(new MainSizePass([], [], []));
        }

        return HoldingAGrantAsync(slot, SizePassPart.Main, async () =>
        {
            var oids = new List<long>(slice.Length);
            var parents = new List<long>(slice.Length);
            var main = new List<long>(slice.Length);
            await using (var command = postgres.CreateCommand(MainSizesSql))
            {
                command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                command.Parameters.Add(new NpgsqlParameter { Value = slice, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
                await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                while (await reader.ReadAsync(CancellationToken.None))
                {
                    oids.Add(reader.GetInt64(0));
                    parents.Add(reader.GetInt64(1));
                    main.Add(reader.GetInt64(2));
                }
            }

            return new MainSizePass(oids.ToArray(), parents.ToArray(), main.ToArray());
        });
    }

    private static Task<ExtraSizePass> ReadExtraSliceAsync(NpgsqlDataSource postgres, SizePassSlot slot, long[] slice)
    {
        if (slice.Length == 0)
        {
            return Task.FromResult(new ExtraSizePass([], [], []));
        }

        return HoldingAGrantAsync(slot, SizePassPart.Extras, async () =>
        {
            var oids = new List<long>(slice.Length);
            var other = new List<long>(slice.Length);
            var toast = new List<long>(slice.Length);
            await using (var command = postgres.CreateCommand(ExtraSizesSql))
            {
                command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                command.Parameters.Add(new NpgsqlParameter { Value = slice, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
                await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
                while (await reader.ReadAsync(CancellationToken.None))
                {
                    oids.Add(reader.GetInt64(0));
                    other.Add(reader.GetInt64(1));
                    toast.Add(reader.GetInt64(2));
                }
            }

            return new ExtraSizePass(oids.ToArray(), other.ToArray(), toast.ToArray());
        });
    }

    private static void AddSizeParameters(NpgsqlCommand command, params long[][] arrays)
    {
        foreach (var column in arrays)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = column, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
        }
    }

    /// <summary>The per-table read's inputs from the two passes: every member heap with its heap, index and TOAST bytes. A heap's
    /// index bytes are the sum of its indexes' forks, which is what <c>pg_indexes_size</c> returns for it. The extras are matched
    /// to the main rows by oid: a relation the extras pass did not size (it was listed after the main pass's list) has no other
    /// forks and no TOAST, the same as one created after a pass.</summary>
    private static void AddTableSizeParameters(NpgsqlCommand command, RelationSizePass given)
        => AddTableSizeParameters(
            command,
            new MainSizePass(given.Oids, given.Parents, given.Main),
            new ExtraSizePass(given.Oids, given.OtherForks, given.Toast));

    private static void AddTableSizeParameters(NpgsqlCommand command, MainSizePass main, ExtraSizePass extras)
    {
        var otherForks = new Dictionary<long, long>(extras.Oids.Length);
        var toast = new Dictionary<long, long>(extras.Oids.Length);
        for (var i = 0; i < extras.Oids.Length; i++)
        {
            otherForks[extras.Oids[i]] = extras.OtherForks[i];
            toast[extras.Oids[i]] = extras.Toast[i];
        }

        var indexBytes = new Dictionary<long, long>();
        for (var i = 0; i < main.Oids.Length; i++)
        {
            if (main.Parents[i] != 0)
            {
                indexBytes[main.Parents[i]] = indexBytes.GetValueOrDefault(main.Parents[i]) + main.Main[i] + otherForks.GetValueOrDefault(main.Oids[i]);
            }
        }

        var heaps = Enumerable.Range(0, main.Oids.Length).Where(i => main.Parents[i] == 0).ToArray();
        AddSizeParameters(
            command,
            heaps.Select(i => main.Oids[i]).ToArray(),
            heaps.Select(i => main.Main[i]).ToArray(),
            heaps.Select(i => indexBytes.GetValueOrDefault(main.Oids[i])).ToArray(),
            heaps.Select(i => toast.GetValueOrDefault(main.Oids[i])).ToArray());
    }

    private static async Task<List<StoreTableHealthRow>> ReadTablesAsync(
        NpgsqlDataSource postgres, DateTime asOfUtc, CancellationToken cancellationToken, RelationSizePass? given = null)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var (timescale, serverVersion) = await ProbeAsync(postgres, cancellationToken);

        var read = new List<(StoreTableHealthRow Row, TailRelation[] TailRelations)>();
        // Both passes start together: the per-table read needs the other forks and the TOAST totals as well as the main sizes.
        // A caller that hands in its sizes (#9374) starts neither.
        var mainPass = given is null ? GetMainSizePassAsync(postgres, timescale, cancellationToken) : null;
        var extraPass = given is null ? GetExtraSizePassAsync(postgres, timescale, cancellationToken) : null;
        if (mainPass is not null && extraPass is not null)
        {
            await Task.WhenAll(mainPass, extraPass);
        }

        await using (var command = postgres.CreateCommand(BuildSql(timescale, serverVersion, presized: true)))
        {
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            if (given is not null)
            {
                AddTableSizeParameters(command, given);
            }
            else
            {
                AddTableSizeParameters(command, await mainPass!, await extraPass!);
            }

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
                var tailNames = reader.IsDBNull(45) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(45);
                var tailPages = reader.IsDBNull(46) ? Array.Empty<long>() : reader.GetFieldValue<long[]>(46);
                var tailEmpty = reader.IsDBNull(47) ? Array.Empty<long>() : reader.GetFieldValue<long[]>(47);
                var tailOff = reader.IsDBNull(48) ? Array.Empty<bool>() : reader.GetFieldValue<bool[]>(48);
                read.Add((row, tailNames.Select((name, i) => new TailRelation(name, tailPages[i], tailEmpty[i], tailOff[i])).ToArray()));
            }
        }

        var vacuum = (await ReadVacuumAsync(postgres, timescale, serverVersion, asOfUtc, cancellationToken))
            .ToDictionary(v => (v.SchemaName, v.TableName));
        var rows = new List<StoreTableHealthRow>(read.Count);
        foreach (var (row, tailRelations) in read)
        {
            var withVacuum = WithVacuum(row, vacuum.GetValueOrDefault((row.SchemaName, row.TableName)), asOfUtc);
            rows.Add(withVacuum.PastRewriteFloors && tailRelations.Length > 0
                ? await WithTailAsync(postgres, withVacuum, tailRelations, cancellationToken)
                : withVacuum);
        }

        return rows
            .OrderByDescending(r => r.Behind)
            .ThenByDescending(r => r.RepeatedlyCancelled)
            .ThenByDescending(r => r.RewriteCandidate)
            .ThenByDescending(r => r.PastRewriteFloors)
            .ThenByDescending(r => r.EstimatedEmptyBytes ?? long.MinValue)
            .ThenByDescending(r => r.TotalBytes)
            .ThenBy(r => r.SchemaName, StringComparer.Ordinal)
            .ThenBy(r => r.TableName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>One relation the tail check picks: its quoted name, its size in pages at the read, its estimated empty bytes, and
    /// whether its <c>vacuum_truncate</c> option is off.</summary>
    public readonly record struct TailRelation(string QualifiedName, long Pages, long EstimatedEmptyBytes, bool TruncateOff)
    {
        /// <summary>The pages at the end of the file whose emptiness answers whether VACUUM can give the space back.</summary>
        public long WindowPages(long blockSize = 8192) => DarlingVacuumTruncateRule.TailWindowPages(Pages, EstimatedEmptyBytes, blockSize);
    }

    /// <summary>
    /// The tail statement: one row per relation, true when the relation holds a live row at or after the first block of its
    /// window (the last <c>WindowPages</c> pages). A TID range scan reads only those pages, but only when the bound is a sub-select:
    /// <c>pg_relation_size</c> is volatile, and a volatile bound in the filter makes PostgreSQL scan the whole table instead
    /// (measured: a minute on a cold 1 GB table). The qualified names come from <c>format('%I.%I')</c> in the read, already quoted.
    /// </summary>
    public static string BuildTailProbeSql(IReadOnlyList<(string QualifiedName, long WindowPages)> probes)
    {
        ArgumentNullException.ThrowIfNull(probes);
        return string.Join("\nUNION ALL\n", probes.Select((p, i) =>
            string.Create(CultureInfo.InvariantCulture,
                $@"SELECT {i} AS ord, EXISTS (SELECT 1 FROM {p.QualifiedName} WHERE ctid >= (SELECT pg_catalog.format('(%s,0)', GREATEST(pg_catalog.pg_relation_size('{p.QualifiedName.Replace("'", "''", StringComparison.Ordinal)}'::pg_catalog.regclass) / pg_catalog.current_setting('block_size')::bigint - {p.WindowPages}, 0))::pg_catalog.tid)) AS occupied")));
    }

    private static async Task<StoreTableHealthRow> WithTailAsync(NpgsqlDataSource postgres, StoreTableHealthRow row, TailRelation[] relations, CancellationToken cancellationToken)
    {
        /* A relation with vacuum_truncate off needs no read: VACUUM will not cut its file whatever sits at the end. A relation
           whose window is over the cap is not read and stays unknown. The rest are read, one window each. */
        var settled = relations.Count(r => r.TruncateOff);
        var occupied = settled;
        var checkedCount = settled;
        var probes = relations
            .Where(r => !r.TruncateOff && r.WindowPages() <= TailProbeMaxWindowPages)
            .Select(r => (r.QualifiedName, WindowPages: r.WindowPages()))
            .ToList();

        if (probes.Count > 0)
        {
            try
            {
                await using var command = postgres.CreateCommand(BuildTailProbeSql(probes));
                command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                var readChecked = 0;
                var readOccupied = 0;
                while (await reader.ReadAsync(cancellationToken))
                {
                    readChecked++;
                    if (reader.GetBoolean(1))
                    {
                        readOccupied++;
                    }
                }

                checkedCount += readChecked;
                occupied += readOccupied;
            }
            catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
            {
                /* A relation dropped between the two statements, or a permission the login lacks. Those relations are then
                   unknown, not empty: only what was settled without a read counts. */
            }
        }

        /* One relation VACUUM cannot give back makes the object a candidate whatever the others say. "VACUUM can give it
           back" is claimed only when every eligible relation was checked and each one passed. */
        var allJudged = checkedCount == relations.Length && relations.Length == row.TailRelationsEligible;
        bool? tailOccupied = occupied > 0 ? true : allJudged ? false : null;
        return row with
        {
            TailRelationsChecked = checkedCount,
            TailRelationsOccupied = occupied,
            TailOccupied = tailOccupied,
            RewriteCandidate = tailOccupied == true,
        };
    }

    /// <summary>Reads every store index, rolled up (see <see cref="StoreIndexHealthRow"/>): invalid first, then larger than
    /// the heap, then rarely used, then by size.</summary>
    public static async Task<List<StoreIndexHealthRow>> GetIndexesAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            return await ReadIndexesAsync(postgres, cancellationToken);
        }
        catch (Exception ex) when (StoreTableHealthTimeoutException.IsDeadlineTrip(ex, cancellationToken))
        {
            throw new StoreTableHealthTimeoutException(ex);
        }
    }

    /// <summary>The same read, sized from <paramref name="sizes"/> (#9374), as <see cref="GetAsync(NpgsqlDataSource, DateTime, RelationSizePass, CancellationToken)"/>.</summary>
    public static async Task<List<StoreIndexHealthRow>> GetIndexesAsync(NpgsqlDataSource postgres, RelationSizePass sizes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sizes);
        try
        {
            return await ReadIndexesAsync(postgres, cancellationToken, sizes);
        }
        catch (Exception ex) when (StoreTableHealthTimeoutException.IsDeadlineTrip(ex, cancellationToken))
        {
            throw new StoreTableHealthTimeoutException(ex);
        }
    }

    private static async Task<List<StoreIndexHealthRow>> ReadIndexesAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken, RelationSizePass? given = null)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var (timescale, _) = await ProbeAsync(postgres, cancellationToken);
        var rows = new List<StoreIndexHealthRow>();
        long[] sizeOids;
        long[] sizeMain;
        if (given is not null)
        {
            (sizeOids, sizeMain) = (given.Oids, given.Main);
        }
        else
        {
            var main = await GetMainSizePassAsync(postgres, timescale, cancellationToken);
            (sizeOids, sizeMain) = (main.Oids, main.Main);
        }

        await using var command = postgres.CreateCommand(BuildIndexSql(timescale, presized: true));
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddSizeParameters(command, sizeOids, sizeMain);
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
                EstimateSkippedReason: reader.IsDBNull(21) ? null : PlainEstimateReason(reader.GetString(21)),
                EstimatedBasisBytes: reader.IsDBNull(22) ? null : reader.GetInt64(22)));
        }

        return rows;
    }

    /// <summary>Reads the leaf index coverage of every partitioned parent's indexes: missing or invalid leaf indexes first.</summary>
    public static async Task<List<StoreIndexCoverageRow>> GetIndexCoverageAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            return await ReadIndexCoverageAsync(postgres, cancellationToken);
        }
        catch (Exception ex) when (StoreTableHealthTimeoutException.IsDeadlineTrip(ex, cancellationToken))
        {
            throw new StoreTableHealthTimeoutException(ex);
        }
    }

    private static async Task<List<StoreIndexCoverageRow>> ReadIndexCoverageAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
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
