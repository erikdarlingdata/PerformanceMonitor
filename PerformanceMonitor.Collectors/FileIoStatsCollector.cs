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
/// File I/O statistics from sys.dm_io_virtual_file_stats. Extracted verbatim from Lite's
/// RemoteCollectorService.FileIo.cs. On-prem/MI reads server-wide (joined to sys.master_files,
/// with the per-server database-exclusion filter spliced at /*EXCLUSION_FILTER*/); Azure SQL DB
/// (edition 5) has no sys.master_files and scopes the DMV to the connected database, so the
/// definition runs per database there (RunsPerDatabase) with database-level filtering handled by
/// the host's database enumeration. Eight delta groups keyed "{database}|{file}".
/// </summary>
public sealed class FileIoStatsCollector : CollectorDefinitionBase<FileIoStatsCollector.Row>
{
    public static FileIoStatsCollector Instance { get; } = new();

    private FileIoStatsCollector()
    {
    }

    public readonly record struct Row(
        string DatabaseName,
        string FileName,
        string FileType,
        string PhysicalName,
        decimal? SizeMb,
        long NumOfReads,
        long NumOfWrites,
        long ReadBytes,
        long WriteBytes,
        long IoStallReadMs,
        long IoStallWriteMs,
        long IoStallQueuedReadMs,
        long IoStallQueuedWriteMs,
        int DatabaseId,
        int FileId);

    /// <summary>
    /// What every surface that shows a File I/O size says in place of a number when the row has none: the log
    /// file of an Azure SQL Database Hyperscale database (the log lives in the log service, so the file carries
    /// no size the database holds). Both apps' <c>get_file_io_stats</c> payloads and the web table use this text.
    /// </summary>
    public const string NoSizeLabel = "n/a (log service)";

    /* Azure SQL Database takes the file's size from sys.database_files, not from sys.dm_io_virtual_file_stats.
       On a Hyperscale database size_on_disk_bytes reads about 0.1 MB for the data file and for the log file,
       while sys.database_files.size (the current size in 8-KB pages) is correct for the data file. Database
       Sizes already reads it with this same arithmetic. On a General Purpose database the two agree.

       COALESCE keeps the DMV's number for a file the join does not match. A NULL would reach the store as 0
       (ReadAsync maps NULL to 0), and the database-size analysis facts skip rows where size_mb is 0 or less,
       so a missed file would drop out of the total instead of showing a size.

       The exception is the LOG file of a Hyperscale database: it lives in the log service, so neither
       number is storage the database holds. That row carries NO size (NULL, written as NULL), and the
       readers say NoSizeLabel for it. DATABASEPROPERTYEX's 'Edition' names the tier; its sql_variant is
       converted to nvarchar(64), the property's documented type, before the comparison. The size facts
       skip the NULL row (size_mb > 0), so no total includes it.

       The on-prem / Managed Instance query below is unchanged. */
    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    database_name = DB_NAME(),
    file_name = df.name,
    file_type = df.type_desc,
    physical_name = df.physical_name,
    size_mb =
        CASE
            WHEN df.type = 1 /*LOG*/
            AND  CONVERT(nvarchar(64), DATABASEPROPERTYEX(DB_NAME(), N'Edition')) = N'Hyperscale'
            THEN CONVERT(decimal(18,2), NULL)
            ELSE CONVERT(decimal(18,2), COALESCE(df.size * 8.0 / 1024.0, vfs.size_on_disk_bytes / 1048576.0))
        END,
    num_of_reads = vfs.num_of_reads,
    num_of_writes = vfs.num_of_writes,
    read_bytes = vfs.num_of_bytes_read,
    write_bytes = vfs.num_of_bytes_written,
    io_stall_read_ms = vfs.io_stall_read_ms,
    io_stall_write_ms = vfs.io_stall_write_ms,
    io_stall_queued_read_ms = vfs.io_stall_queued_read_ms,
    io_stall_queued_write_ms = vfs.io_stall_queued_write_ms,
    database_id = vfs.database_id,
    file_id = vfs.file_id
FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS vfs
LEFT JOIN sys.database_files AS df
  ON df.file_id = vfs.file_id
OPTION(RECOMPILE);";

    private const string OnPremQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    database_name = ISNULL(d.name, DB_NAME(vfs.database_id)),
    file_name = ISNULL(mf.name, N'File_' + CONVERT(nvarchar(10), vfs.file_id)),
    file_type = ISNULL(mf.type_desc, N'UNKNOWN'),
    physical_name = ISNULL(mf.physical_name, N''),
    size_mb = CONVERT(decimal(18,2), vfs.size_on_disk_bytes / 1048576.0),
    num_of_reads = vfs.num_of_reads,
    num_of_writes = vfs.num_of_writes,
    read_bytes = vfs.num_of_bytes_read,
    write_bytes = vfs.num_of_bytes_written,
    io_stall_read_ms = vfs.io_stall_read_ms,
    io_stall_write_ms = vfs.io_stall_write_ms,
    io_stall_queued_read_ms = vfs.io_stall_queued_read_ms,
    io_stall_queued_write_ms = vfs.io_stall_queued_write_ms,
    database_id = vfs.database_id,
    file_id = vfs.file_id
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
LEFT JOIN sys.master_files AS mf
  ON  mf.database_id = vfs.database_id
  AND mf.file_id = vfs.file_id
LEFT JOIN sys.databases AS d
  ON  d.database_id = vfs.database_id
WHERE (vfs.database_id > 4 OR vfs.database_id = 2)
AND   vfs.database_id < 32761
AND   vfs.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
/*EXCLUSION_FILTER*/
OPTION(RECOMPILE);";

    public override string Name => "file_io_stats";

    public override string TargetTable => "file_io_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>Azure SQL DB scopes dm_io_virtual_file_stats to the connected database.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        if (context.Target.IsAzureSqlDb)
        {
            /* Database exclusion happens in the host's database enumeration on Azure. */
            return new CollectorQuery(AzureSqlDbQueryText);
        }

        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        return new CollectorQuery(
            OnPremQueryText.Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal),
            exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("file_name", CollectorColumnType.Varchar),
        new CollectorColumn("file_type", CollectorColumnType.Varchar),
        new CollectorColumn("physical_name", CollectorColumnType.Varchar),
        new CollectorColumn("size_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("num_of_reads", CollectorColumnType.BigInt),
        new CollectorColumn("num_of_writes", CollectorColumnType.BigInt),
        new CollectorColumn("read_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("write_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("io_stall_read_ms", CollectorColumnType.BigInt),
        new CollectorColumn("io_stall_write_ms", CollectorColumnType.BigInt),
        new CollectorColumn("io_stall_queued_read_ms", CollectorColumnType.BigInt),
        new CollectorColumn("io_stall_queued_write_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_reads", CollectorColumnType.BigInt),
        new CollectorColumn("delta_writes", CollectorColumnType.BigInt),
        new CollectorColumn("delta_read_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("delta_write_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("delta_stall_read_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_stall_write_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_stall_queued_read_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_stall_queued_write_ms", CollectorColumnType.BigInt),
        /* Appended (Darling V127 / Lite v60, #3540): the measured seconds the row's deltas accrued over, or
           0 when no delta was knowable. At the END because both stores' writers are positional. */
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.IsDBNull(0) ? "Unknown" : reader.GetString(0),
                reader.IsDBNull(1) ? "Unknown" : reader.GetString(1),
                reader.IsDBNull(2) ? "Unknown" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetDecimal(4),
                reader.IsDBNull(5) ? 0L : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0L : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0L : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0L : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0L : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0L : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0L : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0L : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13), CultureInfo.InvariantCulture),
                reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* "{database}|{file}" delta key and the eight group names are the parity contract. */
        var deltaKey = $"{row.DatabaseName}|{row.FileName}";
        var deltaReads = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_reads", deltaKey, row.NumOfReads, out var readsInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWrites = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_writes", deltaKey, row.NumOfWrites, out var writesInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaReadBytes = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_read_bytes", deltaKey, row.ReadBytes, out var readBytesInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWriteBytes = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_write_bytes", deltaKey, row.WriteBytes, out var writeBytesInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaStallReadMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_stall_read", deltaKey, row.IoStallReadMs, out var stallReadInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaStallWriteMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_stall_write", deltaKey, row.IoStallWriteMs, out var stallWriteInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaStallQueuedReadMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_stall_queued_read", deltaKey, row.IoStallQueuedReadMs, out var stallQueuedReadInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaStallQueuedWriteMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "file_io_stall_queued_write", deltaKey, row.IoStallQueuedWriteMs, out var stallQueuedWriteInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        /* The interval is stored beside the deltas (#3540), the minimum over the row's eight groups, so the
           stored (0, 0) pair means "no delta in this row is knowable" and a latency reader can tell a
           restart's fabricated (0 stall, 0 reads) from a file that genuinely did nothing — the former used
           to render as a confident "0.00 ms" mid-restart. See WaitStatsCollector.WritePayload for the full
           argument; the eight groups share the key and collection time, and dm_io_virtual_file_stats resets
           every counter of a file together (restore, detach/attach, instance restart), so the minimum only
           ever differs from any one group's interval when seeding restored some groups and not others. */
        var sampleIntervalSeconds = Math.Min(
            Math.Min(Math.Min(readsInterval, writesInterval), Math.Min(readBytesInterval, writeBytesInterval)),
            Math.Min(Math.Min(stallReadInterval, stallWriteInterval), Math.Min(stallQueuedReadInterval, stallQueuedWriteInterval)));

        writer
            .Value(row.DatabaseName)
            .Value(row.FileName)
            .Value(row.FileType)
            .Value(row.PhysicalName)
            .Value(row.SizeMb)
            .Value(row.NumOfReads)
            .Value(row.NumOfWrites)
            .Value(row.ReadBytes)
            .Value(row.WriteBytes)
            .Value(row.IoStallReadMs)
            .Value(row.IoStallWriteMs)
            .Value(row.IoStallQueuedReadMs)
            .Value(row.IoStallQueuedWriteMs)
            .Value(deltaReads)
            .Value(deltaWrites)
            .Value(deltaReadBytes)
            .Value(deltaWriteBytes)
            .Value(deltaStallReadMs)
            .Value(deltaStallWriteMs)
            .Value(deltaStallQueuedReadMs)
            .Value(deltaStallQueuedWriteMs)
            .Value(sampleIntervalSeconds);   /* sample_interval_seconds INTEGER — measured, 0 = unknowable */
    }
}
