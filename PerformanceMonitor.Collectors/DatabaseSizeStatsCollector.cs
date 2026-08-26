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
/// Per-file database sizes for growth trending and capacity planning. Extracted verbatim from
/// Lite's RemoteCollectorService.DatabaseSize.cs. On-prem: one batch stages per-database
/// FILEPROPERTY(SpaceUsed) via a server-side cursor + nested sp_executesql into #file_space, then
/// joins sys.master_files + sys.dm_os_volume_stats (the exclusion filter splices at BOTH the
/// cursor and outer SELECT — same parameters referenced twice).
///
/// <para>Azure SQL DB takes an entirely database-scoped query on the EXISTING connection: the
/// connected database's own <c>sys.database_files</c> and <c>FILEPROPERTY(SpaceUsed)</c>, which MS
/// Learn documents as the canonical way to read file space on that platform and which need only the
/// <c>public</c> role. Nothing on that path reads <c>master</c>, <c>sys.master_files</c> (not
/// documented for Azure SQL DB at all) or <c>sys.dm_os_volume_stats</c> (SQL Server only) — see
/// <see cref="RunsPerDatabase"/> for why the enumeration that used to precede it was the actual
/// bug.</para>
///
/// <para>Deliberately NOT <c>sys.dm_db_file_space_usage</c>, which looks like the natural Azure
/// choice: on Basic/S0/S1 service objectives AND on any database in an elastic pool it requires
/// server-admin, Entra-admin or <c>##MS_ServerStateReader##</c> rather than
/// <c>VIEW DATABASE STATE</c>, so it would fail for exactly the reporter's configuration while
/// appearing to work everywhere else.</para>
/// </summary>
public sealed class DatabaseSizeStatsCollector : CollectorDefinitionBase<DatabaseSizeStatsCollector.Row>
{
    public static DatabaseSizeStatsCollector Instance { get; } = new();

    private DatabaseSizeStatsCollector()
    {
    }

    public readonly record struct Row(
        string DatabaseName,
        int DatabaseId,
        int FileId,
        string FileTypeDesc,
        string FileName,
        string PhysicalName,
        decimal TotalSizeMb,
        decimal? UsedSizeMb,
        decimal? AutoGrowthMb,
        decimal? MaxSizeMb,
        string? RecoveryModel,
        int? CompatibilityLevel,
        string? StateDesc,
        string? VolumeMountPoint,
        decimal? VolumeTotalMb,
        decimal? VolumeFreeMb,
        bool? IsPercentGrowth,
        int? GrowthPct,
        int? VlfCount);

    private const string OnPremQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

CREATE TABLE #file_space
(
    database_id int NOT NULL,
    file_id int NOT NULL,
    used_size_mb decimal(19,2) NULL,
    /* #2169: the file's CURRENT size, read in-database alongside SpaceUsed. sys.master_files.size is the
       size recorded at configuration time and does NOT track autogrowth for tempdb, so a grown tempdb
       reported used (current) against total (startup) and produced a used% above 100. Every database
       benefits — master_files can lag any autogrowth — but tempdb is where it is guaranteed to. */
    current_size_mb decimal(19,2) NULL
);

/* #1851: every failure below used to die in an empty CATCH, so a database that was mid-restore or
   inaccessible to the login contributed no used_size_mb and the cycle reported SUCCESS with that
   database's file space silently absent. These rows come back AFTER the payload as the payload path's
   probe-failure contract (EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync) — they cannot ride the
   payload result set, which is one row per FILE and is written to database_size_stats verbatim. */
DECLARE
    @probe_failures TABLE (name sysname, error_text nvarchar(4000));

DECLARE
    @db_name sysname,
    @sql nvarchar(MAX);

DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT
        d.name
    FROM sys.databases AS d
    WHERE d.state_desc = N'ONLINE'
    AND   d.database_id > 0
    AND   HAS_DBACCESS(d.name) = 1
    /*EXCLUSION_FILTER_CURSOR*/
    ORDER BY
        d.name;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @db_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        SET @sql = N'EXECUTE ' + QUOTENAME(@db_name) + N'.sys.sp_executesql N''
INSERT #file_space (database_id, file_id, used_size_mb, current_size_mb)
SELECT
    DB_ID(),
    df.file_id,
    CONVERT(decimal(19,2), FILEPROPERTY(df.name, N''''SpaceUsed'''') * 8.0 / 1024.0),
    CONVERT(decimal(19,2), df.size * 8.0 / 1024.0)
FROM sys.database_files AS df;'';';

        EXECUTE sys.sp_executesql @sql;
    END TRY
    BEGIN CATCH
        /* The failure modes this catches are ordinary and per-database (mid-restore, a database that
           went offline between the cursor and the probe, a login the cross-database reference is
           rejected for), so the cursor keeps going — but that database's files then join the payload
           below with used_size_mb NULL, on a row that still reports SUCCESS. */
        INSERT @probe_failures (name, error_text)
        VALUES (@db_name, ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT FROM db_cursor INTO @db_name;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;

SELECT
    database_name = d.name,
    database_id = d.database_id,
    file_id = mf.file_id,
    file_type_desc = mf.type_desc,
    file_name = mf.name,
    physical_name = mf.physical_name,
    total_size_mb =
        /* #2169: in-database current size when the probe got it, else master_files. Both operands of the
           used% the viewer computes then come from the SAME snapshot, so used can no longer exceed total
           on a database whose files grew since configuration (tempdb, always). A probe that failed leaves
           this NULL and falls back — worse precision, never a wrong ratio direction. */
        CONVERT(decimal(19,2), COALESCE(fs.current_size_mb, mf.size * 8.0 / 1024.0)),
    used_size_mb =
        fs.used_size_mb,
    auto_growth_mb =
        CASE
            WHEN mf.is_percent_growth = 1
            THEN CONVERT(decimal(19,2), NULL)
            ELSE CONVERT(decimal(19,2), mf.growth * 8.0 / 1024.0)
        END,
    max_size_mb =
        CASE
            WHEN mf.max_size = -1
            THEN CONVERT(decimal(19,2), -1)
            WHEN mf.max_size = 268435456
            THEN CONVERT(decimal(19,2), 2097152)
            ELSE CONVERT(decimal(19,2), mf.max_size * 8.0 / 1024.0)
        END,
    recovery_model_desc =
        d.recovery_model_desc,
    compatibility_level =
        CONVERT(int, d.compatibility_level),
    state_desc =
        d.state_desc,
    volume_mount_point =
        RTRIM(vs.volume_mount_point),
    volume_total_mb =
        CONVERT(decimal(19,2), vs.total_bytes / 1048576.0),
    volume_free_mb =
        CONVERT(decimal(19,2), vs.available_bytes / 1048576.0),
    is_percent_growth =
        mf.is_percent_growth,
    growth_pct =
        CASE WHEN mf.is_percent_growth = 1 THEN mf.growth ELSE NULL END,
    vlf_count =
        CASE WHEN mf.type = 1 /*LOG*/ THEN (SELECT COUNT(*) FROM sys.dm_db_log_info(mf.database_id) AS li WHERE li.file_id = mf.file_id) ELSE NULL END
FROM sys.master_files AS mf
JOIN sys.databases AS d
  ON d.database_id = mf.database_id
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
LEFT JOIN #file_space AS fs
  ON  fs.database_id = mf.database_id
  AND fs.file_id = mf.file_id
WHERE d.state_desc = N'ONLINE'
/*EXCLUSION_FILTER_OUTER*/
ORDER BY
    d.name,
    mf.file_id
OPTION(RECOMPILE);

/* Trailing result set = the payload path's probe-failure contract (#1851,
   EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync). Always returned, normally empty; the host
   reads zero rows and attaches no note. */
SELECT
    name,
    error_text
FROM @probe_failures
ORDER BY
    name;";

    /* #2643: TWO sources, and the second one cannot be referenced unless it exists.

       sys.database_files is database-scoped, so on Azure SQL DB this collector reported the connected
       database and nothing else. Correct, and indistinguishable from a collector that found only master:
       a reporter with fifty databases pointed the Viewer at master, saw master's two files, and filed it.

       sys.resource_stats is a MASTER-ONLY view carrying storage_in_megabytes per database with roughly
       fourteen days of history. Verified against a live Azure SQL Database rather than taken from
       documentation - so the sizes ARE reachable from one connection, and only the per-FILE breakdown is
       not. That shows in the projection: file_id NULL and a file_name that says so, never a fabricated
       file.

       The second arm runs through sp_executesql, and that is not stylistic. sys.resource_stats does not
       EXIST in a user database, and name resolution happens at PARSE time - so a plain UNION guarded by
       WHERE DB_NAME() = N'master' still fails with 208 on every user database, which is the common case.
       Measured: it did, immediately, the first time this was run from somewhere other than master.

       A table variable rather than two branches each repeating the file SELECT: the file rows are
       collected identically either way, and duplicating that projection is how the two copies drift. */
    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @database_sizes TABLE
(
    database_name nvarchar(128) NULL,
    database_id int NULL,
    file_id int NULL,
    file_type_desc nvarchar(60) NULL,
    file_name nvarchar(128) NULL,
    physical_name nvarchar(260) NULL,
    total_size_mb decimal(19,2) NULL,
    used_size_mb decimal(19,2) NULL,
    auto_growth_mb decimal(19,2) NULL,
    max_size_mb decimal(19,2) NULL,
    recovery_model_desc nvarchar(12) NULL,
    compatibility_level int NULL,
    state_desc nvarchar(60) NULL,
    volume_mount_point nvarchar(256) NULL,
    volume_total_mb decimal(19,2) NULL,
    volume_free_mb decimal(19,2) NULL,
    is_percent_growth bit NULL,
    growth_pct int NULL,
    vlf_count int NULL
);

INSERT
    @database_sizes
SELECT
    database_name = DB_NAME(),
    database_id = DB_ID(),
    file_id = df.file_id,
    file_type_desc = df.type_desc,
    file_name = df.name,
    physical_name = df.physical_name,
    total_size_mb =
        CONVERT(decimal(19,2), df.size * 8.0 / 1024.0),
    used_size_mb =
        CONVERT(decimal(19,2), FILEPROPERTY(df.name, N'SpaceUsed') * 8.0 / 1024.0),
    auto_growth_mb =
        CASE
            WHEN df.is_percent_growth = 1
            THEN CONVERT(decimal(19,2), NULL)
            ELSE CONVERT(decimal(19,2), df.growth * 8.0 / 1024.0)
        END,
    max_size_mb =
        CASE
            WHEN df.max_size = -1
            THEN CONVERT(decimal(19,2), -1)
            WHEN df.max_size = 268435456
            THEN CONVERT(decimal(19,2), 2097152)
            ELSE CONVERT(decimal(19,2), df.max_size * 8.0 / 1024.0)
        END,
    recovery_model_desc =
        CONVERT(nvarchar(12), DATABASEPROPERTYEX(DB_NAME(), N'Recovery')),
    compatibility_level =
        CONVERT(int, NULL),
    state_desc =
        N'ONLINE',
    volume_mount_point =
        CONVERT(nvarchar(256), NULL),
    volume_total_mb =
        CONVERT(decimal(19,2), NULL),
    volume_free_mb =
        CONVERT(decimal(19,2), NULL),
    is_percent_growth =
        df.is_percent_growth,
    growth_pct =
        CASE WHEN df.is_percent_growth = 1 THEN df.growth ELSE NULL END,
    vlf_count =
        CASE WHEN df.type = 1 /*LOG*/ THEN (SELECT COUNT(*) FROM sys.dm_db_log_info(DB_ID()) AS li WHERE li.file_id = df.file_id) ELSE NULL END
FROM sys.database_files AS df;

/* The sibling databases, on the one connection that can see them. Newest sample per database: the older
   ones are a growth series worth having later, and taking them all would multiply every database by the
   retention window. The connected database is excluded because the arm above already reported it with
   real files. */
IF DB_NAME() = N'master'
BEGIN
    INSERT
        @database_sizes
    (
        database_name, file_type_desc, file_name, total_size_mb, state_desc
    )
    EXEC sys.sp_executesql N'
SELECT
    rs.database_name,
    file_type_desc = N''ROWS'',
    file_name = N''(whole database)'',
    total_size_mb = CONVERT(decimal(19,2), rs.storage_in_megabytes),
    state_desc = N''ONLINE''
FROM
(
    SELECT
        r.database_name,
        r.storage_in_megabytes,
        rn = ROW_NUMBER() OVER (PARTITION BY r.database_name ORDER BY r.end_time DESC)
    FROM sys.resource_stats AS r
    WHERE r.database_name <> DB_NAME()
    AND   r.storage_in_megabytes IS NOT NULL
) AS rs
WHERE rs.rn = 1;';
END;

SELECT
    ds.database_name,
    ds.database_id,
    ds.file_id,
    ds.file_type_desc,
    ds.file_name,
    ds.physical_name,
    ds.total_size_mb,
    ds.used_size_mb,
    ds.auto_growth_mb,
    ds.max_size_mb,
    ds.recovery_model_desc,
    ds.compatibility_level,
    ds.state_desc,
    ds.volume_mount_point,
    ds.volume_total_mb,
    ds.volume_free_mb,
    ds.is_percent_growth,
    ds.growth_pct,
    ds.vlf_count
FROM @database_sizes AS ds
ORDER BY
    CASE WHEN ds.file_id IS NULL THEN 1 ELSE 0 END,
    ds.database_name,
    ds.file_id
OPTION(RECOMPILE);";

    public override string Name => "database_size_stats";

    public override string TargetTable => "database_size_stats";

    /// <summary>
    /// Never per-database. On Azure SQL DB this used to be true, which made the host ENUMERATE
    /// databases first — and that enumeration connects to <c>master</c>, which is the one database an
    /// Azure login reaching the server through a DATABASE-level firewall rule cannot open (#1631,
    /// TrudAX's error 40615). The enumeration bought nothing here: the query below is entirely
    /// database-scoped (<c>sys.database_files</c> + <c>FILEPROPERTY</c>, both satisfied by the
    /// <c>public</c> role), the connection already points at the database being monitored, and on Azure
    /// SQL DB a contained user can only see its own database anyway — so the sibling databases the
    /// enumeration went to <c>master</c> to discover were never readable from this connection.
    ///
    /// <para>Running once on the existing connection removes <c>master</c> from this collector's path
    /// completely, rather than relying on the enumeration's fallback to recover from an error it did not
    /// need to provoke. #1634's fallback still protects the collectors that genuinely must enumerate
    /// (the per-database XE readers); this one simply stops asking.</para>
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    /// <summary>
    /// This collector's on-prem batch returns its per-database probe failures after the payload (#1851).
    /// Its cursor probes every ONLINE database it can enter with a cross-database
    /// <c>[db].sys.sp_executesql</c>, and that probe is exactly what fails for a database that goes
    /// mid-restore or offline between the cursor and the call: before this, the CATCH was empty, so that
    /// database's files landed in the payload with <c>used_size_mb</c> NULL — indistinguishable from a
    /// file whose space was genuinely unreadable — under a SUCCESS row that said nothing had happened.
    ///
    /// <para>Declared unconditionally, including for Azure SQL DB, whose query is a single cursor-less
    /// statement that emits no such set. The contract treats an absent trailing set as zero failures, so
    /// the flag needs no target branch and the Azure path is byte-for-byte unchanged — see
    /// <see cref="ICollectorDefinition{TRow}.EmitsProbeFailures"/>.</para>
    /// </summary>
    public override bool EmitsProbeFailures => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        if (context.Target.IsAzureSqlDb)
        {
            /* Database exclusion happens in the host's database enumeration on Azure. */
            return new CollectorQuery(AzureSqlDbQueryText);
        }

        /* Both filter sites (cursor SELECT and final SELECT) are in outer T-SQL, not nested dynamic
           SQL, so parameter bindings work fine and the same @excl_db_N can be referenced twice. */
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        var text = OnPremQueryText
            .Replace("/*EXCLUSION_FILTER_CURSOR*/", exclusionClause, StringComparison.Ordinal)
            .Replace("/*EXCLUSION_FILTER_OUTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("database_id", CollectorColumnType.Integer),
        new CollectorColumn("file_id", CollectorColumnType.Integer),
        new CollectorColumn("file_type_desc", CollectorColumnType.Varchar),
        new CollectorColumn("file_name", CollectorColumnType.Varchar),
        new CollectorColumn("physical_name", CollectorColumnType.Varchar),
        new CollectorColumn("total_size_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("used_size_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("auto_growth_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("max_size_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("recovery_model_desc", CollectorColumnType.Varchar),
        new CollectorColumn("compatibility_level", CollectorColumnType.Integer),
        new CollectorColumn("state_desc", CollectorColumnType.Varchar),
        new CollectorColumn("volume_mount_point", CollectorColumnType.Varchar),
        new CollectorColumn("volume_total_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("volume_free_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("is_percent_growth", CollectorColumnType.Boolean),
        new CollectorColumn("growth_pct", CollectorColumnType.Integer),
        new CollectorColumn("vlf_count", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetString(0),
                Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetString(5),
                reader.GetDecimal(6),
                reader.IsDBNull(7) ? null : reader.GetDecimal(7),
                reader.IsDBNull(8) ? null : reader.GetDecimal(8),
                reader.IsDBNull(9) ? null : reader.GetDecimal(9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetDecimal(14),
                reader.IsDBNull(15) ? null : reader.GetDecimal(15),
                reader.IsDBNull(16) ? null : (bool?)(Convert.ToInt32(reader.GetValue(16), CultureInfo.InvariantCulture) == 1),
                reader.IsDBNull(17) ? null : Convert.ToInt32(reader.GetValue(17), CultureInfo.InvariantCulture),
                reader.IsDBNull(18) ? null : Convert.ToInt32(reader.GetValue(18), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DatabaseName)
            .Value(row.DatabaseId)
            .Value(row.FileId)
            .Value(row.FileTypeDesc)
            .Value(row.FileName)
            .Value(row.PhysicalName)
            .Value(row.TotalSizeMb)
            .Value(row.UsedSizeMb)
            .Value(row.AutoGrowthMb)
            .Value(row.MaxSizeMb)
            .Value(row.RecoveryModel)
            .Value(row.CompatibilityLevel)
            .Value(row.StateDesc)
            .Value(row.VolumeMountPoint)
            .Value(row.VolumeTotalMb)
            .Value(row.VolumeFreeMb)
            .Value(row.IsPercentGrowth)
            .Value(row.GrowthPct)
            .Value(row.VlfCount);
    }
}
