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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/*
 * FinOps Index Analysis reads. The headless store cannot reach monitored targets, so the analysis runs
 * MONITOR-SIDE: it reads the latest collected per-index snapshot from v_index_object_stats, maps each row into the
 * Common IndexCleanupIndexInput contract, derives the server-fact options (edition -> compression/online support,
 * start time -> uptime days) from the collected server_properties, and runs the pure Common IndexCleanupAnalyzer.
 * Shared by the viewer and any other reader of the store. SQL kept in public const so tests pin it.
 */

/// <summary>An index analysis and, per database name (case-insensitive), the naive-UTC instant its snapshot was collected.</summary>
public sealed record IndexAnalysisWithSnapshotTimes(IndexCleanupAnalysisResult Result, IReadOnlyDictionary<string, DateTime> SnapshotTimes);

public static class DarlingFinOpsIndexAnalysisReader
{
    /// <summary>
    /// Each database's newest collected snapshot of its indexes for one server: for every database the read
    /// anchors on that database's own max(collection_time) and returns the index rows stamped at exactly that
    /// instant, so the analyzer sees each index once. An index that is missing from its database's newest
    /// snapshot was dropped: the collector reads every user index of each database it sweeps, so a row that
    /// only an older cycle carries describes an index that no longer exists and is not shown. A database that
    /// left collection scope still shows its last snapshot, and a server that stopped collecting still shows
    /// its last cycle. The lower bound (the oldest per-database anchor) exists to keep the join on the newest
    /// chunks instead of every retained one. DISTINCT ON still dedupes on the index identity, and two rows for
    /// one index can share a collection_time (a re-run in the same instant), so the final ORDER BY key is
    /// collection_id DESC: the newest-written row wins the tie deterministically. $1 server_id. Column order
    /// is the read contract for <see cref="ReadIndexObjectStatsRow"/>; collection_time is the last column (ordinal 40).
    /// </summary>
    public const string IndexObjectStatsLatestSql = @"
WITH anchor AS MATERIALIZED
(
    SELECT
        database_id,
        max(collection_time) AS t
    FROM v_index_object_stats
    WHERE server_id = $1
    GROUP BY database_id
)
SELECT DISTINCT ON (s.database_id, s.object_id, s.index_id)
    s.database_name,
    s.database_id,
    s.schema_name,
    s.object_id,
    s.table_name,
    s.index_id,
    s.index_name,
    s.index_type_desc,
    s.key_columns,
    s.included_columns,
    s.filter_definition,
    s.is_unique,
    s.is_unique_constraint,
    s.is_primary_key,
    s.is_foreign_key,
    s.is_foreign_key_reference,
    s.is_disabled,
    s.is_indexed_view,
    s.data_compression_desc,
    s.optimize_for_sequential_key,
    s.fill_factor,
    s.is_padded,
    s.allow_page_locks,
    s.allow_row_locks,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.reserved_mb,
    s.total_rows,
    s.partition_count,
    s.sqlserver_start_time,
    /* Operational lock/latch counters (dm_db_index_operational_stats), appended after the pre-existing
       columns so ordinals 0-31 are unchanged. Feed the reclaimable-space rollup's workload-impact columns
       (Lock Waits / Latch Waits), the same collected data the FinOps Locking sub-tab already reads. */
    s.row_lock_wait_count,
    s.row_lock_wait_in_ms,
    s.page_lock_wait_count,
    s.page_lock_wait_in_ms,
    s.page_latch_wait_count,
    s.page_latch_wait_in_ms,
    s.page_io_latch_wait_count,
    s.page_io_latch_wait_in_ms,
    s.collection_time
FROM v_index_object_stats AS s
JOIN anchor AS a ON a.database_id = s.database_id AND a.t = s.collection_time
WHERE s.server_id = $1
AND   s.collection_time >= (SELECT min(t) FROM anchor)
ORDER BY s.database_id, s.object_id, s.index_id, s.collection_time DESC, s.collection_id DESC";

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
ORDER BY collection_time DESC, collection_id DESC
LIMIT 1";

    /// <summary>
    /// The analysis-relevant projection of one <c>v_index_object_stats</c> row — the read-layer
    /// shape (NOT the collector Row: the Common contract is deliberately collector-free), mapped into
    /// <see cref="IndexCleanupIndexInput"/> by <see cref="MapToIndexInput"/>. Booleans are nullable because the
    /// Postgres columns are; the mapper collapses a NULL flag to <c>false</c> (an absent guard is not a guard).
    /// </summary>
    public sealed record IndexObjectStatsDto
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

        /// <summary>The snapshot's own stamp, naive UTC (kind Utc): the per-database anchor the read resolved on,
        /// projected last (ordinal 40) so every earlier ordinal is unchanged.</summary>
        public DateTime CollectionTime { get; init; }
    }

    /// <summary>Maps a collected snapshot row into the analyzer's per-index input contract (nullable flag -&gt; false).</summary>
    public static IndexCleanupIndexInput MapToIndexInput(IndexObjectStatsDto row) => new()
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
    private static IndexObjectStatsDto ReadIndexObjectStatsRow(NpgsqlDataReader reader)
    {
        long? L(int i) => reader.IsDBNull(i) ? null : Convert.ToInt64(reader.GetValue(i), CultureInfo.InvariantCulture);
        int? I(int i) => reader.IsDBNull(i) ? null : Convert.ToInt32(reader.GetValue(i), CultureInfo.InvariantCulture);
        short? Sh(int i) => reader.IsDBNull(i) ? null : Convert.ToInt16(reader.GetValue(i), CultureInfo.InvariantCulture);
        decimal? D(int i) => reader.IsDBNull(i) ? null : Convert.ToDecimal(reader.GetValue(i), CultureInfo.InvariantCulture);
        bool? B(int i) => reader.IsDBNull(i) ? null : reader.GetBoolean(i);
        string? S(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);

        return new IndexObjectStatsDto
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
            /* collection_time is the store's naive UTC stamp; the anchor join makes it non-null. */
            CollectionTime = DateTime.SpecifyKind(reader.GetDateTime(40), DateTimeKind.Utc),
        };
    }

    /// <summary>Reads the latest per-index snapshot for the server and maps each row to the analyzer input contract.</summary>
    public static async Task<List<IndexCleanupIndexInput>> GetIndexCleanupInputsAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = await ReadIndexObjectStatsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
        return rows.ConvertAll(MapToIndexInput);
    }

    /// <summary>Runs <see cref="IndexObjectStatsLatestSql"/> and returns its rows with their snapshot stamps.</summary>
    private static async Task<List<IndexObjectStatsDto>> ReadIndexObjectStatsAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var inputs = new List<IndexObjectStatsDto>();

        await using var command = dataSource.CreateCommand(IndexObjectStatsLatestSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            inputs.Add(ReadIndexObjectStatsRow(reader));
        }

        return inputs;
    }

    /// <summary>
    /// Reads the server's latest collected edition/version/start-time facts and derives the analyzer options.
    /// The uptime comparison is an exception to the naive-UTC rule: <c>sqlserver_start_time</c> is the target's
    /// LOCAL wall-clock start time, so it is compared against the service host's local clock
    /// (<paramref name="referenceLocalTime"/>, or <see cref="DateTime.Now"/> when null).
    /// </summary>
    public static async Task<IndexCleanupOptions> GetIndexCleanupOptionsAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, DateTime? referenceLocalTime = null, CancellationToken cancellationToken = default)
    {
        int? engineEdition = null;
        string? productVersion = null;
        DateTime? startTime = null;

        await using var command = dataSource.CreateCommand(ServerCompressionInfoSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            engineEdition = reader.IsDBNull(0) ? null : Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            productVersion = reader.IsDBNull(1) ? null : reader.GetString(1);
            startTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
        }

        /* sqlserver_start_time is the server's LOCAL wall clock, compared to the caller's local now — mirrors
           the proc's SYSDATETIME() derivation (both wall-clock; the coarse days measure tolerates any offset). */
        return DeriveIndexCleanupOptions(engineEdition, productVersion, startTime, referenceLocalTime ?? DateTime.Now);
    }

    /// <summary>
    /// The full index-cleanup analysis for one server: reads the latest collected snapshot, derives the
    /// server-fact options, and runs the pure analyzer off the calling thread (the dedupe is string-comparison-heavy).
    /// </summary>
    public static async Task<IndexCleanupAnalysisResult> GetIndexAnalysisAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var inputs = await GetIndexCleanupInputsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
        var options = await GetIndexCleanupOptionsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken: cancellationToken);
        return await Task.Run(() => IndexCleanupAnalyzer.Analyze(inputs, options), cancellationToken);
    }

    /// <summary>
    /// <see cref="GetIndexAnalysisAsync"/> plus when each database's snapshot was collected. The time rides beside the
    /// analysis result (the Common types are shared with Lite, which has no stored snapshot to date): a map from
    /// database name to the naive-UTC instant of that database's newest snapshot. Names are unique within one SQL
    /// Server instance, and each database's newest cycle carries exactly one, so the map has no collisions.
    /// </summary>
    public static async Task<IndexAnalysisWithSnapshotTimes> GetIndexAnalysisWithSnapshotTimesAsync(NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = await ReadIndexObjectStatsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
        var options = await GetIndexCleanupOptionsAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken: cancellationToken);
        var times = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in rows)
        {
            times[row.DatabaseName] = row.CollectionTime;
        }

        var inputs = rows.ConvertAll(MapToIndexInput);
        var result = await Task.Run(() => IndexCleanupAnalyzer.Analyze(inputs, options), cancellationToken);
        return new IndexAnalysisWithSnapshotTimes(result, times);
    }

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
