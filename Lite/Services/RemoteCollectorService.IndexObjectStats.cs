/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Command timeout for the index/object-stats collector. This sweep reads
    /// sys.dm_db_index_operational_stats over every index in a database, which is
    /// far heavier than the other DMV collectors. It now runs one command PER
    /// DATABASE (see <see cref="CollectIndexObjectStatsAsync"/>), so this larger,
    /// dedicated budget applies to a single database rather than a whole-instance
    /// sweep. Matches the 300s the FinOps sp_IndexCleanup path already uses
    /// (LocalDataService.FinOps.IndexAnalysis). The global 30s CommandTimeoutSeconds
    /// was the root cause of #1135 (one cumulative all-or-nothing command timed out).
    /// </summary>
    private const int IndexObjectStatsCommandTimeoutSeconds = 300;

    /// <summary>
    /// Collects per-table and per-index size, usage, and locking statistics for growth
    /// trending, unused-index detection, and contention analysis.
    /// Size columns are absolute point-in-time values; usage and locking counters are
    /// cumulative (reset on instance restart / DB detach / AUTO_CLOSE) - sqlserver_start_time
    /// carries the reset boundary so deltas can be computed safely in the read layer.
    /// All three DMVs are database-scoped, so collection runs ONE COMMAND PER DATABASE:
    /// on-prem enumerates databases then sends each through [db].sys.sp_executesql; Azure
    /// SQL DB connects to each database individually. Each database is collected with its
    /// own command, timeout, and try/catch, so a slow or inaccessible database only fails
    /// itself instead of discarding the whole instance's results (#1135). Within each
    /// database, the three DMVs are staged into #temp tables with single scans and then
    /// joined - this gives the optimizer real cardinality and avoids the bad plans the old
    /// single monolithic multi-DMV join produced on large databases (the sp_IndexCleanup
    /// technique). In-Memory OLTP (Hekaton) objects are not represented by these DMVs.
    /// </summary>
    private async Task<int> CollectIndexObjectStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus?.SqlEngineEdition == 5;

        /*
        Per-database collection body. Runs inside a single database's context (the on-prem
        path wraps it in [db].sys.sp_executesql; the Azure path connects to the database).
        Each DMV is staged into its own #temp with one scan, then joined - sized/usage/
        locking counters get accurate cardinality so the final join gets a sane plan even
        on very large databases. The final SELECT's column order MUST match the ordinals in
        ReadIndexObjectStatRow (0..43).
        */
        const string perDbStatsBody = @"
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
    page_io_latch_wait_in_ms = os.page_io_latch_wait_in_ms
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
WHERE o.is_ms_shipped = 0
AND   o.type IN (N'U', N'V')
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var serverName = GetServerNameForStorage(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var rows = new List<IndexObjectStatRow>();
        var sqlSw = Stopwatch.StartNew();

        if (isAzureSqlDb)
        {
            /* Azure SQL DB: one connection per database (cannot cross databases). Already
               resilient - each database has its own command + try/catch. */
            var databases = await GetAzureDatabaseListAsync(server, cancellationToken);
            foreach (var dbName in databases)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    using var dbConn = await OpenAzureDatabaseConnectionAsync(server, dbName, cancellationToken);
                    using var cmd = new SqlCommand(perDbStatsBody, dbConn);
                    cmd.CommandTimeout = IndexObjectStatsCommandTimeoutSeconds;
                    using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        rows.Add(ReadIndexObjectStatRow(reader));
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger?.LogDebug("Skipping database '{Database}' for index/object stats: {Error}", dbName, ex.Message);
                }
            }
        }
        else
        {
            /* On-prem / Azure MI / AWS RDS: one connection, enumerate databases, then collect
               each one with its own command via [db].sys.sp_executesql. A slow or inaccessible
               database fails only itself; the rest still persist (#1135). */
            using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);

            var (exclusionClause, exclusionParams) = BuildDatabaseExclusionFilter(server.ExcludedDatabases, "d.name");
            var enumQuery = $@"
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

            var databases = new List<string>();
            using (var enumCommand = new SqlCommand(enumQuery, sqlConnection))
            {
                enumCommand.CommandTimeout = CommandTimeoutSeconds;
                foreach (var p in exclusionParams) enumCommand.Parameters.Add(p);
                using var enumReader = await enumCommand.ExecuteReaderAsync(cancellationToken);
                while (await enumReader.ReadAsync(cancellationToken))
                {
                    databases.Add(enumReader.GetString(0));
                }
            }

            /* Double single quotes so the body survives nesting inside [db].sys.sp_executesql N'...' */
            var escapedBody = perDbStatsBody.Replace("'", "''");
            foreach (var dbName in databases)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var escapedDbName = dbName.Replace("]", "]]");
                    var perDbQuery = $"EXECUTE [{escapedDbName}].sys.sp_executesql N'{escapedBody}';";
                    using var command = new SqlCommand(perDbQuery, sqlConnection);
                    command.CommandTimeout = IndexObjectStatsCommandTimeoutSeconds;
                    using var reader = await command.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        rows.Add(ReadIndexObjectStatRow(reader));
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Failed to collect index/object stats from [{Database}] on '{Server}': {Error}",
                        dbName, server.DisplayName, ex.Message);
                }
            }
        }
        sqlSw.Stop();

        var duckSw = Stopwatch.StartNew();
        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);
            using (var appender = duckConnection.CreateAppender("index_object_stats"))
            {
                foreach (var r in rows)
                {
                    appender.CreateRow()
                       .AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(serverName)
                       .AppendValue(r.SqlServerStartTime)
                       .AppendValue(r.DatabaseName)
                       .AppendValue(r.DatabaseId)
                       .AppendValue(r.SchemaName)
                       .AppendValue(r.ObjectId)
                       .AppendValue(r.TableName)
                       .AppendValue(r.IndexId)
                       .AppendValue(r.IndexName)
                       .AppendValue(r.IndexTypeDesc)
                       .AppendValue(r.IsUnique)
                       .AppendValue(r.IsPrimaryKey)
                       .AppendValue(r.IsFiltered)
                       .AppendValue(r.PartitionCount)
                       .AppendValue(r.ReservedMb)
                       .AppendValue(r.UsedMb)
                       .AppendValue(r.InRowDataMb)
                       .AppendValue(r.LobDataMb)
                       .AppendValue(r.RowOverflowMb)
                       .AppendValue(r.TotalRows)
                       .AppendValue(r.UserSeeks)
                       .AppendValue(r.UserScans)
                       .AppendValue(r.UserLookups)
                       .AppendValue(r.UserUpdates)
                       .AppendValue(r.LastUserSeek)
                       .AppendValue(r.LastUserScan)
                       .AppendValue(r.LastUserLookup)
                       .AppendValue(r.LastUserUpdate)
                       .AppendValue(r.LeafInsertCount)
                       .AppendValue(r.LeafUpdateCount)
                       .AppendValue(r.LeafDeleteCount)
                       .AppendValue(r.RangeScanCount)
                       .AppendValue(r.SingletonLookupCount)
                       .AppendValue(r.RowLockCount)
                       .AppendValue(r.RowLockWaitCount)
                       .AppendValue(r.RowLockWaitInMs)
                       .AppendValue(r.PageLockCount)
                       .AppendValue(r.PageLockWaitCount)
                       .AppendValue(r.PageLockWaitInMs)
                       .AppendValue(r.IndexLockPromotionAttemptCount)
                       .AppendValue(r.IndexLockPromotionCount)
                       .AppendValue(r.PageLatchWaitCount)
                       .AppendValue(r.PageLatchWaitInMs)
                       .AppendValue(r.PageIoLatchWaitCount)
                       .AppendValue(r.PageIoLatchWaitInMs)
                       .EndRow();
                    rowsCollected++;
                }
            }
        }
        duckSw.Stop();

        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} index/object stat rows for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }

    private static IndexObjectStatRow ReadIndexObjectStatRow(SqlDataReader reader)
    {
        long? L(int i) => reader.IsDBNull(i) ? null : Convert.ToInt64(reader.GetValue(i));
        int? I(int i) => reader.IsDBNull(i) ? null : Convert.ToInt32(reader.GetValue(i));
        decimal? D(int i) => reader.IsDBNull(i) ? null : reader.GetDecimal(i);
        DateTime? T(int i) => reader.IsDBNull(i) ? null : reader.GetDateTime(i);
        bool? B(int i) => reader.IsDBNull(i) ? null : (bool?)(Convert.ToInt32(reader.GetValue(i)) == 1);

        return new IndexObjectStatRow
        {
            SqlServerStartTime = T(0),
            DatabaseName = reader.GetString(1),
            DatabaseId = Convert.ToInt32(reader.GetValue(2)),
            SchemaName = reader.GetString(3),
            ObjectId = Convert.ToInt32(reader.GetValue(4)),
            TableName = reader.GetString(5),
            IndexId = Convert.ToInt32(reader.GetValue(6)),
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
            PageIoLatchWaitInMs = L(43)
        };
    }

    private sealed class IndexObjectStatRow
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
    }
}
