/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-table and per-index size, usage, and locking statistics (growth trending, unused-index
/// detection, contention analysis). Extracted verbatim from Lite's
/// RemoteCollectorService.IndexObjectStats.cs. All three DMVs are database-scoped, so collection
/// runs ONE COMMAND PER DATABASE: on-prem enumerates accessible online databases then sends the
/// staged body through [db].sys.sp_executesql (quote-doubled); Azure SQL DB connects per database
/// (RunsPerDatabase). Within each database the three DMVs stage into #temps with single scans
/// then join — the sp_IndexCleanup technique that fixed the #1135 monolithic-join timeouts —
/// under a dedicated 300 s per-database budget (CommandTimeoutSecondsOverride; the enumeration
/// keeps the default). sqlserver_start_time flags restart-class counter resets for the read layer.
///
/// <para>Also captures the per-index DEFINITION metadata that monitor-side UNUSED/DUPLICATE index
/// analysis needs (FinOps Index Analysis, Stage 1): the ordered KEY column list (with sort
/// direction), the INCLUDE column set, the filter predicate, the uniqueness/constraint/PK/FK and
/// table-vs-indexed-view discriminators, disabled state, and the reconstruct-a-CREATE options
/// (compression, sequential key, fill factor, padding, lock granularity). key_columns/included_columns
/// use sp_IndexCleanup's
/// exact delimited STUFF/FOR XML representation so a Stage-2 analyzer's string-comparison dedupe
/// (Exact/Reverse Duplicate, Equal-Except-Filter, Key Subset/Superset) ports cleanly, and the FK
/// flags reproduce its "never drop an FK-supporting or FK-referenced index" protection. These come
/// from sys.indexes/sys.index_columns/sys.columns/sys.foreign_key_columns/sys.partitions — catalog
/// views present on every platform incl. Azure SQL DB — so they share the collector's existing
/// per-database execution unchanged.</para>
/// </summary>
public sealed class IndexObjectStatsCollector : CollectorDefinitionBase<IndexObjectStatsCollector.Row>
{
    public static IndexObjectStatsCollector Instance { get; } = new();

    private IndexObjectStatsCollector()
    {
    }

    public sealed class Row
    {
        public DateTime? SqlServerStartTime { get; set; }
        public string DatabaseName { get; set; } = "";
        public int DatabaseId { get; set; }
        public string SchemaName { get; set; } = "";
        public int ObjectId { get; set; }
        public string TableName { get; set; } = "";
        public int IndexId { get; set; }
        public string? IndexName { get; set; }
        public string? IndexTypeDesc { get; set; }
        public bool? IsUnique { get; set; }
        public bool? IsPrimaryKey { get; set; }
        public bool? IsFiltered { get; set; }
        public int? PartitionCount { get; set; }
        public decimal? ReservedMb { get; set; }
        public decimal? UsedMb { get; set; }
        public decimal? InRowDataMb { get; set; }
        public decimal? LobDataMb { get; set; }
        public decimal? RowOverflowMb { get; set; }
        public long? TotalRows { get; set; }
        public long? UserSeeks { get; set; }
        public long? UserScans { get; set; }
        public long? UserLookups { get; set; }
        public long? UserUpdates { get; set; }
        public DateTime? LastUserSeek { get; set; }
        public DateTime? LastUserScan { get; set; }
        public DateTime? LastUserLookup { get; set; }
        public DateTime? LastUserUpdate { get; set; }
        public long? LeafInsertCount { get; set; }
        public long? LeafUpdateCount { get; set; }
        public long? LeafDeleteCount { get; set; }
        public long? RangeScanCount { get; set; }
        public long? SingletonLookupCount { get; set; }
        public long? RowLockCount { get; set; }
        public long? RowLockWaitCount { get; set; }
        public long? RowLockWaitInMs { get; set; }
        public long? PageLockCount { get; set; }
        public long? PageLockWaitCount { get; set; }
        public long? PageLockWaitInMs { get; set; }
        public long? IndexLockPromotionAttemptCount { get; set; }
        public long? IndexLockPromotionCount { get; set; }
        public long? PageLatchWaitCount { get; set; }
        public long? PageLatchWaitInMs { get; set; }
        public long? PageIoLatchWaitCount { get; set; }
        public long? PageIoLatchWaitInMs { get; set; }
        public string? KeyColumns { get; set; }
        public string? IncludedColumns { get; set; }
        public string? FilterDefinition { get; set; }
        public bool? IsUniqueConstraint { get; set; }
        public bool? IsForeignKey { get; set; }
        public bool? IsForeignKeyReference { get; set; }
        public bool? IsDisabled { get; set; }
        public string? DataCompressionDesc { get; set; }
        public bool? OptimizeForSequentialKey { get; set; }
        public short? FillFactor { get; set; }
        public bool? IsPadded { get; set; }
        public bool? AllowPageLocks { get; set; }
        public bool? AllowRowLocks { get; set; }
        public bool? IsIndexedView { get; set; }
    }

    /// <summary>
    /// The per-database staged body — the ordinals of its final SELECT are the read contract.
    /// <paramref name="supportsOptimizeForSequentialKey"/> gates the SQL 2019+/Azure-only
    /// optimize_for_sequential_key column: a typed NULL is emitted on older engines, where the
    /// column does not exist and referencing it would fail the whole batch (mirrors
    /// sp_IndexCleanup's @supports_optimize_for_sequential_key gate). Everything else is
    /// engine-version-invariant.
    /// </summary>
    internal static string BuildPerDatabaseStatsBody(bool supportsOptimizeForSequentialKey)
    {
        string optimizeForSequentialKey =
            supportsOptimizeForSequentialKey
                ? "i.optimize_for_sequential_key"
                : "CONVERT(bit, NULL)";

        return @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Size + row counts (one scan of dm_db_partition_stats) */
SELECT
    dps.object_id,
    dps.index_id,
    partition_count = COUNT_BIG(*),
    reserved_pages = SUM(dps.reserved_page_count),
    used_pages = SUM(dps.used_page_count),
    in_row_pages = SUM(dps.in_row_data_page_count),
    lob_pages = SUM(dps.lob_used_page_count),
    row_overflow_pages = SUM(dps.row_overflow_used_page_count),
    total_rows = SUM(dps.row_count)
INTO #sizes
FROM sys.dm_db_partition_stats AS dps
GROUP BY
    dps.object_id,
    dps.index_id
OPTION(RECOMPILE);

/* Usage counters (one scan of dm_db_index_usage_stats for this database) */
SELECT
    us.object_id,
    us.index_id,
    us.user_seeks,
    us.user_scans,
    us.user_lookups,
    us.user_updates,
    us.last_user_seek,
    us.last_user_scan,
    us.last_user_lookup,
    us.last_user_update
INTO #usage
FROM sys.dm_db_index_usage_stats AS us
WHERE us.database_id = DB_ID()
OPTION(RECOMPILE);

/* Locking/latch counters (one scan of dm_db_index_operational_stats - the heavy DMV) */
SELECT
    ios.object_id,
    ios.index_id,
    leaf_insert_count = SUM(ios.leaf_insert_count),
    leaf_update_count = SUM(ios.leaf_update_count),
    leaf_delete_count = SUM(ios.leaf_delete_count),
    range_scan_count = SUM(ios.range_scan_count),
    singleton_lookup_count = SUM(ios.singleton_lookup_count),
    row_lock_count = SUM(ios.row_lock_count),
    row_lock_wait_count = SUM(ios.row_lock_wait_count),
    row_lock_wait_in_ms = SUM(ios.row_lock_wait_in_ms),
    page_lock_count = SUM(ios.page_lock_count),
    page_lock_wait_count = SUM(ios.page_lock_wait_count),
    page_lock_wait_in_ms = SUM(ios.page_lock_wait_in_ms),
    index_lock_promotion_attempt_count = SUM(ios.index_lock_promotion_attempt_count),
    index_lock_promotion_count = SUM(ios.index_lock_promotion_count),
    page_latch_wait_count = SUM(ios.page_latch_wait_count),
    page_latch_wait_in_ms = SUM(ios.page_latch_wait_in_ms),
    page_io_latch_wait_count = SUM(ios.page_io_latch_wait_count),
    page_io_latch_wait_in_ms = SUM(ios.page_io_latch_wait_in_ms)
INTO #ops
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS ios
GROUP BY
    ios.object_id,
    ios.index_id
OPTION(RECOMPILE);

/* Stage the per-index DEFINITION catalog ONCE, then build the metadata set-based - the
   sp_IndexCleanup technique (#3508). The stats DMVs above already stage this way; the
   definition reads used to run as five correlated subqueries in the final projection
   (key/include FOR XML, two FK EXISTS, and the compression TOP(1)), each re-probing the live
   catalog once per index - so a wide schema paid per index (a 54K-index database spent ~28s
   in this body). Staging drops that to one scan apiece; the emitted strings and flags are
   byte-identical. */
SELECT
    ic.object_id,
    ic.index_id,
    ic.column_id,
    ic.key_ordinal,
    ic.is_included_column,
    ic.is_descending_key,
    column_name = c.name
INTO #index_columns
FROM sys.index_columns AS ic
JOIN sys.columns AS c
  ON  c.object_id = ic.object_id
  AND c.column_id = ic.column_id
OPTION(RECOMPILE);

CREATE CLUSTERED INDEX cx_index_columns
    ON #index_columns (object_id, index_id, is_included_column, key_ordinal);

/* key_columns/included_columns reproduce sp_IndexCleanup's delimited STUFF/FOR XML
   representation EXACTLY (QUOTENAME + ' DESC', key order for keys, name order for the include
   set), built once here by correlating to the staged #index_columns instead of the live
   catalog. key_ordinal > 0 drops the partitioning column that rides at key_ordinal = 0 on a
   partitioned index (sp_IndexCleanup fix ae32a4c) so it cannot land as a phantom leading key. */
SELECT
    d.object_id,
    d.index_id,
    key_columns =
        STUFF
        (
          (
            SELECT
                N', ' +
                QUOTENAME(ic.column_name) +
                CASE
                    WHEN ic.is_descending_key = 1
                    THEN N' DESC'
                    ELSE N''
                END
            FROM #index_columns AS ic
            WHERE ic.object_id = d.object_id
            AND   ic.index_id = d.index_id
            AND   ic.is_included_column = 0
            AND   ic.key_ordinal > 0
            ORDER BY
                ic.key_ordinal
            FOR
                XML
                PATH(''),
                TYPE
          ).value('text()[1]', 'nvarchar(max)'),
          1,
          2,
          ''
        ),
    included_columns =
        STUFF
        (
          (
            SELECT
                N', ' +
                QUOTENAME(ic.column_name)
            FROM #index_columns AS ic
            WHERE ic.object_id = d.object_id
            AND   ic.index_id = d.index_id
            AND   ic.is_included_column = 1
            ORDER BY
                ic.column_name
            FOR
                XML
                PATH(''),
                TYPE
          ).value('text()[1]', 'nvarchar(max)'),
          1,
          2,
          ''
        )
INTO #index_definitions
FROM
(
    SELECT DISTINCT
        object_id,
        index_id
    FROM #index_columns
) AS d
OPTION(RECOMPILE);

/* FK protections (sp_IndexCleanup guards on is_foreign_key_reference): one set-based pass over
   the staged KEY columns (is_included_column = 0) instead of a correlated EXISTS per index.
   is_foreign_key = a key column backs an outgoing FK (supporting index); is_foreign_key_reference
   = a key column is referenced by an incoming FK. */
SELECT DISTINCT
    ic.object_id,
    ic.index_id
INTO #foreign_key_supporting
FROM #index_columns AS ic
JOIN sys.foreign_key_columns AS fkc
  ON  fkc.parent_object_id = ic.object_id
  AND fkc.parent_column_id = ic.column_id
WHERE ic.is_included_column = 0
OPTION(RECOMPILE);

SELECT DISTINCT
    ic.object_id,
    ic.index_id
INTO #foreign_key_referenced
FROM #index_columns AS ic
JOIN sys.foreign_key_columns AS fkc
  ON  fkc.referenced_object_id = ic.object_id
  AND fkc.referenced_column_id = ic.column_id
WHERE ic.is_included_column = 0
OPTION(RECOMPILE);

/* Compression aggregated to the index grain: the lowest level across partitions, so an index
   with ANY uncompressed partition surfaces as NONE (sp_IndexCleanup's compressibility signal).
   One windowed scan instead of a correlated TOP(1) per index; ties resolve to the same lowest
   data_compression level, hence the same desc the correlated ORDER BY returned. */
SELECT
    p.object_id,
    p.index_id,
    p.data_compression_desc
INTO #index_compression
FROM
(
    SELECT
        p.object_id,
        p.index_id,
        p.data_compression_desc,
        rn =
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    p.object_id,
                    p.index_id
                ORDER BY
                    p.data_compression
            )
    FROM sys.partitions AS p
) AS p
WHERE p.rn = 1
OPTION(RECOMPILE);

SELECT
    sqlserver_start_time = (SELECT osi.sqlserver_start_time FROM sys.dm_os_sys_info AS osi),
    database_name = DB_NAME(),
    database_id = DB_ID(),
    schema_name = s.name,
    object_id = o.object_id,
    table_name = o.name,
    index_id = i.index_id,
    index_name = i.name,
    index_type_desc = i.type_desc,
    is_unique = i.is_unique,
    is_primary_key = i.is_primary_key,
    is_filtered = i.has_filter,
    partition_count = ps.partition_count,
    reserved_mb = CONVERT(decimal(19,2), ps.reserved_pages * 8.0 / 1024.0),
    used_mb = CONVERT(decimal(19,2), ps.used_pages * 8.0 / 1024.0),
    in_row_data_mb = CONVERT(decimal(19,2), ps.in_row_pages * 8.0 / 1024.0),
    lob_data_mb = CONVERT(decimal(19,2), ps.lob_pages * 8.0 / 1024.0),
    row_overflow_mb = CONVERT(decimal(19,2), ps.row_overflow_pages * 8.0 / 1024.0),
    total_rows = ps.total_rows,
    user_seeks = us.user_seeks,
    user_scans = us.user_scans,
    user_lookups = us.user_lookups,
    user_updates = us.user_updates,
    last_user_seek = us.last_user_seek,
    last_user_scan = us.last_user_scan,
    last_user_lookup = us.last_user_lookup,
    last_user_update = us.last_user_update,
    leaf_insert_count = os.leaf_insert_count,
    leaf_update_count = os.leaf_update_count,
    leaf_delete_count = os.leaf_delete_count,
    range_scan_count = os.range_scan_count,
    singleton_lookup_count = os.singleton_lookup_count,
    row_lock_count = os.row_lock_count,
    row_lock_wait_count = os.row_lock_wait_count,
    row_lock_wait_in_ms = os.row_lock_wait_in_ms,
    page_lock_count = os.page_lock_count,
    page_lock_wait_count = os.page_lock_wait_count,
    page_lock_wait_in_ms = os.page_lock_wait_in_ms,
    index_lock_promotion_attempt_count = os.index_lock_promotion_attempt_count,
    index_lock_promotion_count = os.index_lock_promotion_count,
    page_latch_wait_count = os.page_latch_wait_count,
    page_latch_wait_in_ms = os.page_latch_wait_in_ms,
    page_io_latch_wait_count = os.page_io_latch_wait_count,
    page_io_latch_wait_in_ms = os.page_io_latch_wait_in_ms,
    /* Per-index DEFINITION metadata for monitor-side UNUSED/DUPLICATE analysis (sp_IndexCleanup
       parity, Stage 1), read from the temps staged above rather than rebuilt per index in this
       projection. key_columns/included_columns keep sp_IndexCleanup's delimited STUFF/FOR XML
       representation EXACTLY so Stage-2 string-comparison dedupe ports cleanly. */
    key_columns = defs.key_columns,
    included_columns = defs.included_columns,
    filter_definition = i.filter_definition,
    is_unique_constraint = i.is_unique_constraint,
    /* FK protections (sp_IndexCleanup guards on is_foreign_key_reference): a key column backs an
       outgoing FK (is_foreign_key) or is referenced by an incoming FK (is_foreign_key_reference).
       Presence in the staged sets IS the flag. */
    is_foreign_key =
        CONVERT(bit, CASE WHEN fks.object_id IS NOT NULL THEN 1 ELSE 0 END),
    is_foreign_key_reference =
        CONVERT(bit, CASE WHEN fkr.object_id IS NOT NULL THEN 1 ELSE 0 END),
    is_disabled = i.is_disabled,
    /* Lowest compression level across partitions, staged in #index_compression above. */
    data_compression_desc = comp.data_compression_desc,
    optimize_for_sequential_key = " + optimizeForSequentialKey + @",
    fill_factor = i.fill_factor,
    is_padded = i.is_padded,
    allow_page_locks = i.allow_page_locks,
    allow_row_locks = i.allow_row_locks,
    /* Table (U) vs indexed-view (V) index — the one table-vs-view discriminator index_type_desc
       cannot express (a view's clustered index is also CLUSTERED); mirrors sp_IndexCleanup's
       is_indexed_view so Stage 2 never dedupes across the two. */
    is_indexed_view =
        CONVERT
        (
            bit,
            CASE
                WHEN o.type = N'V'
                THEN 1
                ELSE 0
            END
        )
FROM sys.indexes AS i
JOIN sys.objects AS o
  ON o.object_id = i.object_id
JOIN sys.schemas AS s
  ON s.schema_id = o.schema_id
LEFT JOIN #sizes AS ps
  ON  ps.object_id = i.object_id
  AND ps.index_id = i.index_id
LEFT JOIN #usage AS us
  ON  us.object_id = i.object_id
  AND us.index_id = i.index_id
LEFT JOIN #ops AS os
  ON  os.object_id = i.object_id
  AND os.index_id = i.index_id
LEFT JOIN #index_definitions AS defs
  ON  defs.object_id = i.object_id
  AND defs.index_id = i.index_id
LEFT JOIN #foreign_key_supporting AS fks
  ON  fks.object_id = i.object_id
  AND fks.index_id = i.index_id
LEFT JOIN #foreign_key_referenced AS fkr
  ON  fkr.object_id = i.object_id
  AND fkr.index_id = i.index_id
LEFT JOIN #index_compression AS comp
  ON  comp.object_id = i.object_id
  AND comp.index_id = i.index_id
WHERE o.is_ms_shipped = 0
AND   o.type IN (N'U', N'V')
OPTION(RECOMPILE);";
    }

    /// <summary>
    /// optimize_for_sequential_key exists only on SQL Server 2019+ (major >= 15), Azure SQL DB, and
    /// Azure SQL Managed Instance (mirrors sp_IndexCleanup's engine-edition/version gate). Unknown
    /// version (0) fails safe to unsupported — a typed NULL is emitted rather than risk a hard bind
    /// error against a column that may not exist on the target.
    /// </summary>
    private static bool SupportsOptimizeForSequentialKey(CollectorTargetInfo target) =>
        target.IsAzureSqlDb
        || target.IsAzureManagedInstance
        || target.SqlMajorVersion >= 15;

    public override string Name => "index_object_stats";

    public override string TargetTable => "index_object_stats";

    /// <summary>The heavy per-database sweep gets the dedicated #1135 budget.</summary>
    public override int? CommandTimeoutSecondsOverride => 300;

    /// <summary>Azure SQL DB cannot cross databases — one connection per database.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>Azure per-database connections run the staged body directly.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context) =>
        new(BuildPerDatabaseStatsBody(SupportsOptimizeForSequentialKey(context.Target)));

    /// <summary>On-prem: enumerate accessible online databases (with the #3477 scope and the
    /// exclusions — the composed predicate, so the scope narrows the fan-out and the exclusion
    /// still wins), then per-item.</summary>
    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        var (exclusionClause, exclusionParameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");
        var text = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
    d.name
FROM sys.databases AS d
WHERE d.state_desc = N'ONLINE'
AND   d.database_id > 0
AND   HAS_DBACCESS(d.name) = 1
{exclusionClause}
ORDER BY
    d.name;";

        return new CollectorQuery(text, exclusionParameters);
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        /* Double single quotes so the body survives nesting inside [db].sys.sp_executesql N'...' */
        var escapedBody = BuildPerDatabaseStatsBody(SupportsOptimizeForSequentialKey(context.Target))
            .Replace("'", "''", StringComparison.Ordinal);
        var escapedDbName = item.Replace("]", "]]", StringComparison.Ordinal);
        return new CollectorQuery($"EXECUTE [{escapedDbName}].sys.sp_executesql N'{escapedBody}';");
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        await ParseRowsAsync(reader, rows, cancellationToken);
        return rows;
    }

    public override ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
        => new(ParseRowsAsync(reader, rows, cancellationToken));

    private static async Task ParseRowsAsync(DbDataReader reader, List<Row> rows, CancellationToken cancellationToken)
    {
        long? L(int i) => reader.IsDBNull(i) ? null : Convert.ToInt64(reader.GetValue(i), CultureInfo.InvariantCulture);
        int? I(int i) => reader.IsDBNull(i) ? null : Convert.ToInt32(reader.GetValue(i), CultureInfo.InvariantCulture);
        short? Sh(int i) => reader.IsDBNull(i) ? null : Convert.ToInt16(reader.GetValue(i), CultureInfo.InvariantCulture);
        decimal? D(int i) => reader.IsDBNull(i) ? null : reader.GetDecimal(i);
        DateTime? T(int i) => reader.IsDBNull(i) ? null : reader.GetDateTime(i);
        bool? B(int i) => reader.IsDBNull(i) ? null : (bool?)(Convert.ToInt32(reader.GetValue(i), CultureInfo.InvariantCulture) == 1);
        string? S(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row
            {
                SqlServerStartTime = T(0),
                DatabaseName = reader.GetString(1),
                DatabaseId = Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                SchemaName = reader.GetString(3),
                ObjectId = Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture),
                TableName = reader.GetString(5),
                IndexId = Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture),
                IndexName = reader.IsDBNull(7) ? null : reader.GetString(7),
                IndexTypeDesc = reader.IsDBNull(8) ? null : reader.GetString(8),
                IsUnique = B(9),
                IsPrimaryKey = B(10),
                IsFiltered = B(11),
                PartitionCount = I(12),
                ReservedMb = D(13),
                UsedMb = D(14),
                InRowDataMb = D(15),
                LobDataMb = D(16),
                RowOverflowMb = D(17),
                TotalRows = L(18),
                UserSeeks = L(19),
                UserScans = L(20),
                UserLookups = L(21),
                UserUpdates = L(22),
                LastUserSeek = T(23),
                LastUserScan = T(24),
                LastUserLookup = T(25),
                LastUserUpdate = T(26),
                LeafInsertCount = L(27),
                LeafUpdateCount = L(28),
                LeafDeleteCount = L(29),
                RangeScanCount = L(30),
                SingletonLookupCount = L(31),
                RowLockCount = L(32),
                RowLockWaitCount = L(33),
                RowLockWaitInMs = L(34),
                PageLockCount = L(35),
                PageLockWaitCount = L(36),
                PageLockWaitInMs = L(37),
                IndexLockPromotionAttemptCount = L(38),
                IndexLockPromotionCount = L(39),
                PageLatchWaitCount = L(40),
                PageLatchWaitInMs = L(41),
                PageIoLatchWaitCount = L(42),
                PageIoLatchWaitInMs = L(43),
                KeyColumns = S(44),
                IncludedColumns = S(45),
                FilterDefinition = S(46),
                IsUniqueConstraint = B(47),
                IsForeignKey = B(48),
                IsForeignKeyReference = B(49),
                IsDisabled = B(50),
                DataCompressionDesc = S(51),
                OptimizeForSequentialKey = B(52),
                FillFactor = Sh(53),
                IsPadded = B(54),
                AllowPageLocks = B(55),
                AllowRowLocks = B(56),
                IsIndexedView = B(57),
            });
        }
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("sqlserver_start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("database_id", CollectorColumnType.Integer),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("object_id", CollectorColumnType.Integer),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_id", CollectorColumnType.Integer),
        new CollectorColumn("index_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_type_desc", CollectorColumnType.Varchar),
        new CollectorColumn("is_unique", CollectorColumnType.Boolean),
        new CollectorColumn("is_primary_key", CollectorColumnType.Boolean),
        new CollectorColumn("is_filtered", CollectorColumnType.Boolean),
        new CollectorColumn("partition_count", CollectorColumnType.Integer),
        new CollectorColumn("reserved_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("used_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("in_row_data_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("lob_data_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("row_overflow_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("total_rows", CollectorColumnType.BigInt),
        new CollectorColumn("user_seeks", CollectorColumnType.BigInt),
        new CollectorColumn("user_scans", CollectorColumnType.BigInt),
        new CollectorColumn("user_lookups", CollectorColumnType.BigInt),
        new CollectorColumn("user_updates", CollectorColumnType.BigInt),
        new CollectorColumn("last_user_seek", CollectorColumnType.Timestamp),
        new CollectorColumn("last_user_scan", CollectorColumnType.Timestamp),
        new CollectorColumn("last_user_lookup", CollectorColumnType.Timestamp),
        new CollectorColumn("last_user_update", CollectorColumnType.Timestamp),
        new CollectorColumn("leaf_insert_count", CollectorColumnType.BigInt),
        new CollectorColumn("leaf_update_count", CollectorColumnType.BigInt),
        new CollectorColumn("leaf_delete_count", CollectorColumnType.BigInt),
        new CollectorColumn("range_scan_count", CollectorColumnType.BigInt),
        new CollectorColumn("singleton_lookup_count", CollectorColumnType.BigInt),
        new CollectorColumn("row_lock_count", CollectorColumnType.BigInt),
        new CollectorColumn("row_lock_wait_count", CollectorColumnType.BigInt),
        new CollectorColumn("row_lock_wait_in_ms", CollectorColumnType.BigInt),
        new CollectorColumn("page_lock_count", CollectorColumnType.BigInt),
        new CollectorColumn("page_lock_wait_count", CollectorColumnType.BigInt),
        new CollectorColumn("page_lock_wait_in_ms", CollectorColumnType.BigInt),
        new CollectorColumn("index_lock_promotion_attempt_count", CollectorColumnType.BigInt),
        new CollectorColumn("index_lock_promotion_count", CollectorColumnType.BigInt),
        new CollectorColumn("page_latch_wait_count", CollectorColumnType.BigInt),
        new CollectorColumn("page_latch_wait_in_ms", CollectorColumnType.BigInt),
        new CollectorColumn("page_io_latch_wait_count", CollectorColumnType.BigInt),
        new CollectorColumn("page_io_latch_wait_in_ms", CollectorColumnType.BigInt),
        new CollectorColumn("key_columns", CollectorColumnType.Varchar),
        new CollectorColumn("included_columns", CollectorColumnType.Varchar),
        new CollectorColumn("filter_definition", CollectorColumnType.Varchar),
        new CollectorColumn("is_unique_constraint", CollectorColumnType.Boolean),
        new CollectorColumn("is_foreign_key", CollectorColumnType.Boolean),
        new CollectorColumn("is_foreign_key_reference", CollectorColumnType.Boolean),
        new CollectorColumn("is_disabled", CollectorColumnType.Boolean),
        new CollectorColumn("data_compression_desc", CollectorColumnType.Varchar),
        new CollectorColumn("optimize_for_sequential_key", CollectorColumnType.Boolean),
        new CollectorColumn("fill_factor", CollectorColumnType.SmallInt),
        new CollectorColumn("is_padded", CollectorColumnType.Boolean),
        new CollectorColumn("allow_page_locks", CollectorColumnType.Boolean),
        new CollectorColumn("allow_row_locks", CollectorColumnType.Boolean),
        new CollectorColumn("is_indexed_view", CollectorColumnType.Boolean),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.SqlServerStartTime)
            .Value(row.DatabaseName)
            .Value(row.DatabaseId)
            .Value(row.SchemaName)
            .Value(row.ObjectId)
            .Value(row.TableName)
            .Value(row.IndexId)
            .Value(row.IndexName)
            .Value(row.IndexTypeDesc)
            .Value(row.IsUnique)
            .Value(row.IsPrimaryKey)
            .Value(row.IsFiltered)
            .Value(row.PartitionCount)
            .Value(row.ReservedMb)
            .Value(row.UsedMb)
            .Value(row.InRowDataMb)
            .Value(row.LobDataMb)
            .Value(row.RowOverflowMb)
            .Value(row.TotalRows)
            .Value(row.UserSeeks)
            .Value(row.UserScans)
            .Value(row.UserLookups)
            .Value(row.UserUpdates)
            .Value(row.LastUserSeek)
            .Value(row.LastUserScan)
            .Value(row.LastUserLookup)
            .Value(row.LastUserUpdate)
            .Value(row.LeafInsertCount)
            .Value(row.LeafUpdateCount)
            .Value(row.LeafDeleteCount)
            .Value(row.RangeScanCount)
            .Value(row.SingletonLookupCount)
            .Value(row.RowLockCount)
            .Value(row.RowLockWaitCount)
            .Value(row.RowLockWaitInMs)
            .Value(row.PageLockCount)
            .Value(row.PageLockWaitCount)
            .Value(row.PageLockWaitInMs)
            .Value(row.IndexLockPromotionAttemptCount)
            .Value(row.IndexLockPromotionCount)
            .Value(row.PageLatchWaitCount)
            .Value(row.PageLatchWaitInMs)
            .Value(row.PageIoLatchWaitCount)
            .Value(row.PageIoLatchWaitInMs)
            .Value(row.KeyColumns)
            .Value(row.IncludedColumns)
            .Value(row.FilterDefinition)
            .Value(row.IsUniqueConstraint)
            .Value(row.IsForeignKey)
            .Value(row.IsForeignKeyReference)
            .Value(row.IsDisabled)
            .Value(row.DataCompressionDesc)
            .Value(row.OptimizeForSequentialKey)
            .Value(row.FillFactor)
            .Value(row.IsPadded)
            .Value(row.AllowPageLocks)
            .Value(row.AllowRowLocks)
            .Value(row.IsIndexedView);
    }
}
