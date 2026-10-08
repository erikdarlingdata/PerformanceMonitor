/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /* #3896: $2 is the LOOKBACK start (AnalysisContext.LatestValueStartFor), not the window start. Without it
       the window function numbered every file row the server has retained, and a dropped database's files
       stayed in the sum until retention aged them out. */
    public const string DatabaseSizeSql = @"
WITH latest AS (
    SELECT database_name, file_name, size_mb,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_name ORDER BY collection_time DESC) AS rn
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   size_mb > 0
)
SELECT SUM(size_mb) AS total_size_mb
FROM latest
WHERE rn = 1";

    /* #5516: the cheap path for the sum DatabaseSizeSql computes. DatabaseSizeSql numbers every file row in the
       24-hour lookback to keep the newest per file - 1,788 blocks a call. This reads only the newest snapshot
       (the (server_id, collection_time) index serves MAX backward, then the rows by equality) and sums its
       files.

       DatabaseSizeSql also answers with a file's OLDER row when the newest snapshot lacks that file (a database
       dropped or taken offline inside the lookback, or a per-database read that failed in the newest Azure SQL
       Database sweep). No index leads with the file key, so no cheap read can find such a file; this read probes
       instead. It lists the files of the snapshot just before the newest and of the oldest one in the lookback,
       and reports whether either has a file the newest snapshot lacks. The caller then runs DatabaseSizeSql, so
       the answer is the old one whenever a file has dropped out. The one case the probe cannot see is a file
       that appears and vanishes strictly between those two snapshots and in neither of them; DatabaseSizeSql
       would carry it for up to 24 hours as a ghost, and #3896 exists to age ghosts out. Columns: total_size_mb
       (NULL with no qualifying row), dropped_out (true when DatabaseSizeSql must decide). */
    public const string DatabaseSizeNewestSql = @"
WITH newest AS (
    SELECT collection_time
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   size_mb > 0
    ORDER BY collection_time DESC
    LIMIT 1
),
cur AS (
    SELECT DISTINCT ON (database_name, file_name) database_name, file_name, size_mb
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT collection_time FROM newest)
    AND   size_mb > 0
    ORDER BY database_name, file_name
),
edge AS (
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time DESC
        LIMIT 1
    )
    UNION
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time ASC
        LIMIT 1
    )
)
SELECT
    (SELECT SUM(size_mb) FROM cur) AS total_size_mb,
    EXISTS (
        SELECT 1
        FROM file_io_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time IN (SELECT collection_time FROM edge)
        AND   f.size_mb > 0
        AND   NOT EXISTS (
            SELECT 1
            FROM cur AS c
            WHERE c.database_name = f.database_name
            AND   c.file_name = f.file_name
        )
    ) AS dropped_out";

    /// <summary>
    /// Collects total database data size from file_io_stats.
    /// Sums the latest size_mb across the database files seen within <see cref="AnalysisContext.LatestValueLookbackFor">its collector's lookback</see>
    /// of the window's end (#3896) — a file not seen in that span belongs to a database that no longer exists.
    /// The sum comes from the newest snapshot alone (#5516); when a file in the probed older snapshots is
    /// missing from it, <see cref="DatabaseSizeSql"/> decides. The one accepted difference from the full read: a file
    /// seen only in snapshots strictly between the two probed ones (the oldest in the lookback and the one just
    /// before the newest) is not carried as a ghost, where the full read carried it for up to 24 hours. That
    /// answer can also reappear when the lookback's oldest snapshot later falls inside the file's life; the total
    /// has no scoring weight, and #3896 treats such a ghost as not a database anyway.
    /// </summary>
    private async Task CollectDatabaseSizeFactAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            double? totalSize = null;
            var droppedOut = true;
            using (var cmd = new NpgsqlCommand(DatabaseSizeNewestSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.LatestValueStartFor(FileIoStatsCollector.Instance.Name)));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                if (await reader.ReadAsync(context.CancellationToken))
                {
                    totalSize = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0));
                    droppedOut = reader.GetBoolean(1);
                }
            }

            /* Nothing qualifying at all is DatabaseSizeSql's empty answer too, so only a dropped-out file
               needs the full read. */
            if (droppedOut)
            {
                using var full = new NpgsqlCommand(DatabaseSizeSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
                full.Parameters.AddWithValue(context.ServerId);
                full.Parameters.AddWithValue(AsNaive(context.LatestValueStartFor(FileIoStatsCollector.Instance.Name)));
                full.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var fullReader = await full.ExecuteReaderAsync(context.CancellationToken);
                totalSize = null;
                if (await fullReader.ReadAsync(context.CancellationToken))
                    totalSize = fullReader.IsDBNull(0) ? null : Convert.ToDouble(fullReader.GetValue(0));
            }

            if (totalSize is > 0)
                facts.Add(new Fact { Source = "config", Key = "DATABASE_TOTAL_SIZE_MB", Value = totalSize.Value, ServerId = context.ServerId });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string IoLatencySql = @"
SELECT
    SUM(delta_stall_read_ms) AS total_stall_read_ms,
    SUM(delta_reads) AS total_reads,
    SUM(delta_stall_write_ms) AS total_stall_write_ms,
    SUM(delta_writes) AS total_writes
FROM v_file_io_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   (delta_reads > 0 OR delta_writes > 0)";

    /// <summary>
    /// Collects I/O latency from file_io_stats delta columns.
    /// Computes average read and write latency across all database files.
    /// </summary>
    private async Task CollectIoLatencyFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(IoLatencySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalStallReadMs = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var totalReads = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalStallWriteMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalWrites = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));

            if (totalReads > 0)
            {
                var avgReadLatency = (double)totalStallReadMs / totalReads;
                facts.Add(new Fact
                {
                    Source = "io",
                    Key = "IO_READ_LATENCY_MS",
                    Value = avgReadLatency,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["avg_read_latency_ms"] = avgReadLatency,
                        ["total_stall_read_ms"] = totalStallReadMs,
                        ["total_reads"] = totalReads
                    }
                });
            }

            if (totalWrites > 0)
            {
                var avgWriteLatency = (double)totalStallWriteMs / totalWrites;
                facts.Add(new Fact
                {
                    Source = "io",
                    Key = "IO_WRITE_LATENCY_MS",
                    Value = avgWriteLatency,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["avg_write_latency_ms"] = avgWriteLatency,
                        ["total_stall_write_ms"] = totalStallWriteMs,
                        ["total_writes"] = totalWrites
                    }
                });
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string TempDbSql = @"
SELECT
    MAX(total_reserved_mb) AS max_total_reserved_mb,
    MAX(user_object_reserved_mb) AS max_user_object_mb,
    MAX(internal_object_reserved_mb) AS max_internal_object_mb,
    MAX(version_store_reserved_mb) AS max_version_store_mb,
    MIN(unallocated_mb) AS min_unallocated_mb,
    AVG(total_reserved_mb) AS avg_total_reserved_mb,
    /* #2515: the growth ceiling, and MIN is the right aggregate for it twice over. -1 (at least one
       unlimited data file) sorts below every real cap, so MIN finds the unlimited case anywhere in the
       window; and where the cap was raised mid-window MIN keeps the more conservative of the two. NULL
       rows — collected before the V81 rung — are skipped rather than dragging the answer down. */
    MIN(max_size_mb) AS min_max_size_mb
FROM v_tempdb_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

    /// <summary>
    /// Collects TempDB usage facts: max usage, version store size, and unallocated space.
    /// Value is max total_reserved_mb over the period.
    /// </summary>
    private async Task CollectTempDbFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(TempDbSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var maxReserved = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var maxUserObj = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var maxInternalObj = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxVersionStore = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var minUnallocated = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));
            var avgReserved = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));
            var maxSizeMb = reader.IsDBNull(6) ? 0.0 : Convert.ToDouble(reader.GetValue(6));

            if (maxReserved <= 0) return;

            /* #2515: against the CEILING where tempdb has one, against the current allocation where it does
               not (-1 unlimited, or 0 for a window collected before the ceiling was captured). reserved +
               unallocated is the files AS ALLOCATED, so on its own this fraction measures distance to the
               next autogrow — which reads as 96% full on an Azure SQL Database target holding one temp
               table. TempDbSpaceInfo.CapacityMb is the alert's twin of this, and the two must agree or
               analyze_server and the pager describe the same server differently. */
            var allocatedMb = maxReserved + minUnallocated;
            var totalSpace = maxSizeMb > 0 ? Math.Max(maxSizeMb, allocatedMb) : allocatedMb;
            var usageFraction = totalSpace > 0 ? maxReserved / totalSpace : 0;

            facts.Add(new Fact
            {
                Source = "tempdb",
                Key = "TEMPDB_USAGE",
                Value = usageFraction,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["max_reserved_mb"] = maxReserved,
                    ["avg_reserved_mb"] = avgReserved,
                    ["max_user_object_mb"] = maxUserObj,
                    ["max_internal_object_mb"] = maxInternalObj,
                    ["max_version_store_mb"] = maxVersionStore,
                    ["min_unallocated_mb"] = minUnallocated,
                    /* Added, never redefined — the existing keys are a consumer surface (FactAdvice reads
                       them by name). -1 unlimited, 0 not measured, positive = the ROWS ceiling in MB. */
                    ["max_size_mb"] = maxSizeMb,
                    ["usage_fraction"] = usageFraction
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /* #3896: bounded to the lookback like DatabaseSizeSql (it had no time bound at all, so a dropped
       database's file counted for the whole 90-day database_size_stats retention), and on the same bounds
       as the drill-down that lists these files (PgDrillDownCollector.AutogrowthPercentFilesSql). */
    public const string FileAutogrowthSql = @"
WITH latest AS (
    SELECT database_name, file_id, total_size_mb, is_percent_growth,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_id ORDER BY collection_time DESC) AS rn
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    COUNT(*) AS file_count,
    COUNT(DISTINCT database_name) AS database_count
FROM latest
WHERE rn = 1
AND   is_percent_growth = true
AND   total_size_mb >= 10240
AND   database_name NOT IN ('master', 'msdb', 'model', 'tempdb')";

    /// <summary>
    /// Collects the percent-autogrowth-on-large-files config fact (WS3): data/log files set
    /// to grow in PERCENTAGE steps that are also large (>= 10 GB), where a single growth is a
    /// huge, stalling allocation. Reads the latest snapshot per file from database_size_stats
    /// within <see cref="AnalysisContext.LatestValueLookbackFor">its collector's lookback</see> of the window's end (#3896),
    /// excludes system databases, and emits ONE aggregate FILE_AUTOGROWTH_PERCENT fact carrying
    /// the offending-file/database counts (the per-file detail + copy-paste fix is attached
    /// later by the drill-down collector).
    /// </summary>
    private async Task CollectFileAutogrowthFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(FileAutogrowthSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.LatestValueStartFor(DatabaseSizeStatsCollector.Instance.Name)));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var fileCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (fileCount == 0) return;

            var databaseCount = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));

            facts.Add(new Fact
            {
                Source = "config",
                Key = "FILE_AUTOGROWTH_PERCENT",
                Value = fileCount,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["file_count"] = fileCount,
                    ["database_count"] = databaseCount
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /* #3896: $2 is the lookback start (AnalysisContext.LatestValueStartFor). */
    public const string DiskSpaceSql = @"
WITH latest AS (
    SELECT volume_mount_point, volume_total_mb, volume_free_mb,
           ROW_NUMBER() OVER (PARTITION BY volume_mount_point ORDER BY collection_time DESC) AS rn
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   volume_total_mb > 0
)
SELECT
    MIN(volume_free_mb * 1.0 / volume_total_mb) AS min_free_pct,
    MIN(volume_free_mb) AS min_free_mb,
    COUNT(DISTINCT volume_mount_point) AS volume_count,
    SUM(volume_total_mb) AS total_volume_mb,
    SUM(volume_free_mb) AS total_free_mb
FROM latest WHERE rn = 1";

    /// <summary>
    /// Collects disk space facts from database_size_stats: volume free space, file sizes. Each volume's
    /// latest sample within <see cref="AnalysisContext.LatestValueLookbackFor">its collector's lookback</see> of the window's end (#3896).
    /// </summary>
    private async Task CollectDiskSpaceFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(DiskSpaceSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.LatestValueStartFor(DatabaseSizeStatsCollector.Instance.Name)));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var minFreePct = reader.IsDBNull(0) ? 1.0 : Convert.ToDouble(reader.GetValue(0));
            var minFreeMb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var volumeCount = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalVolumeMb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var totalFreeMb = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));

            if (volumeCount == 0) return;

            facts.Add(new Fact
            {
                Source = "disk",
                Key = "DISK_SPACE",
                Value = minFreePct,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["min_free_pct"] = minFreePct,
                    ["min_free_mb"] = minFreeMb,
                    ["volume_count"] = volumeCount,
                    ["total_volume_mb"] = totalVolumeMb,
                    ["total_free_mb"] = totalFreeMb
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }
}
