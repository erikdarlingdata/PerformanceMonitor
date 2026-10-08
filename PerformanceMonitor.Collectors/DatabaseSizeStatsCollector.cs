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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-file database sizes for growth trending and capacity planning. Extracted verbatim from
/// Lite's RemoteCollectorService.DatabaseSize.cs. On-prem: one batch stages per-database
/// FILEPROPERTY(SpaceUsed) via a server-side cursor + nested sp_executesql into #file_space, then
/// joins sys.master_files + sys.dm_os_volume_stats (the exclusion filter splices at BOTH the
/// cursor and outer SELECT — same parameters referenced twice).
///
/// <para>Azure SQL DB takes an entirely database-scoped query, run once per database (#5498,
/// <see cref="RunsPerDatabase"/>): the connected database's own <c>sys.database_files</c> and
/// <c>FILEPROPERTY(SpaceUsed)</c>, which MS Learn documents as the canonical way to read file space on that
/// platform and which need only the <c>public</c> role. Nothing on that path reads <c>master</c>,
/// <c>sys.master_files</c> (not documented for Azure SQL DB at all) or <c>sys.dm_os_volume_stats</c> (SQL
/// Server only).</para>
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

    /// <summary>
    /// <c>DatabaseId</c>, <c>FileId</c> and <c>PhysicalName</c> are nullable because the Azure
    /// sibling arm (#2643, removed by #5498; stored rows keep the shape) deliberately emitted them as NULL: <c>sys.resource_stats</c> has no
    /// per-file breakdown, so a sibling row carries a database name and a total size and honestly
    /// nothing else. Both stores hold the columns nullable (#3262).
    ///
    /// <para><c>TotalSizeMb</c> is nullable for one row: the LOG file of an Azure SQL Database Hyperscale
    /// database, whose size is not allocated storage (the log lives in the log service). See
    /// <see cref="PerformanceMonitor.Common.HyperscaleLogSize"/>.</para>
    /// </summary>
    public readonly record struct Row(
        string DatabaseName,
        int? DatabaseId,
        int? FileId,
        string FileTypeDesc,
        string FileName,
        string? PhysicalName,
        decimal? TotalSizeMb,
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
    database_id integer NOT NULL,
    file_id integer NOT NULL,
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
    @sql nvarchar(max);

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
        CONVERT(integer, d.compatibility_level),
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
        CASE WHEN mf.type = 1 /*LOG*/ THEN (SELECT CONVERT(integer, COUNT_BIG(*)) FROM sys.dm_db_log_info(mf.database_id) AS li WHERE li.file_id = mf.file_id) ELSE NULL END
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

    /* sys.database_files is database-scoped, so this query reports the CONNECTED database and nothing else.
       That is by design since #5498: the host runs it once per database (RunsPerDatabase), so every database
       on an Azure logical server reports its own real files. Before, a master-only sys.resource_stats arm
       tried to report the siblings from the one master connection (#2643), but that view lags database
       creation by about an hour and carries no per-file breakdown; see the note above the final SELECT.

       A table variable rather than a bare SELECT so the final projection stays the single place that fixes
       the payload's column order. */
    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Hyperscale keeps its transaction log in the log service, and sys.database_files still lists a LOG row for it,
   sized at about 1 TB (1,046,528 MB) - the ceiling on the log's ACTIVE portion, not storage the database holds
   or pays for. Hyperscale bills allocated DATA storage only (Microsoft Learn, 'What is the Hyperscale service
   tier?'), so that row's size, growth and ceiling are not reported: a NULL here becomes 'n/a (log service)' in
   every reader and stays out of every allocated total. The data file keeps its real size.

   DATABASEPROPERTYEX needs only the current database, so this adds no permission to the Azure path. Learn's
   DATABASEPROPERTYEX page describes Edition as 'the database edition or service tier' and its list of returned
   values does not name Hyperscale; the tier string itself is the one Learn shows for
   sys.database_service_objectives.edition (which needs dbmanager, so it is not usable here). CONVERT because
   DATABASEPROPERTYEX returns sql_variant, the same way Recovery is read below. */
DECLARE
    @is_hyperscale bit =
        CASE
            WHEN CONVERT(nvarchar(64), DATABASEPROPERTYEX(DB_NAME(), N'Edition')) = N'Hyperscale'
            THEN 1
            ELSE 0
        END;

DECLARE
    @database_sizes TABLE
(
    database_name nvarchar(128) NULL,
    database_id integer NULL,
    file_id integer NULL,
    file_type_desc nvarchar(60) NULL,
    file_name nvarchar(128) NULL,
    physical_name nvarchar(260) NULL,
    total_size_mb decimal(19,2) NULL,
    used_size_mb decimal(19,2) NULL,
    auto_growth_mb decimal(19,2) NULL,
    max_size_mb decimal(19,2) NULL,
    recovery_model_desc nvarchar(12) NULL,
    compatibility_level integer NULL,
    state_desc nvarchar(60) NULL,
    volume_mount_point nvarchar(256) NULL,
    volume_total_mb decimal(19,2) NULL,
    volume_free_mb decimal(19,2) NULL,
    is_percent_growth bit NULL,
    growth_pct integer NULL,
    vlf_count integer NULL
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
        CASE
            WHEN ls.is_log_service = 1
            THEN CONVERT(decimal(19,2), NULL)
            ELSE CONVERT(decimal(19,2), df.size * 8.0 / 1024.0)
        END,
    used_size_mb =
        CONVERT(decimal(19,2), FILEPROPERTY(df.name, N'SpaceUsed') * 8.0 / 1024.0),
    auto_growth_mb =
        CASE
            WHEN ls.is_log_service = 1
            THEN CONVERT(decimal(19,2), NULL)
            WHEN df.is_percent_growth = 1
            THEN CONVERT(decimal(19,2), NULL)
            ELSE CONVERT(decimal(19,2), df.growth * 8.0 / 1024.0)
        END,
    max_size_mb =
        CASE
            WHEN ls.is_log_service = 1
            THEN CONVERT(decimal(19,2), NULL)
            WHEN df.max_size = -1
            THEN CONVERT(decimal(19,2), -1)
            WHEN df.max_size = 268435456
            THEN CONVERT(decimal(19,2), 2097152)
            ELSE CONVERT(decimal(19,2), df.max_size * 8.0 / 1024.0)
        END,
    recovery_model_desc =
        CONVERT(nvarchar(12), DATABASEPROPERTYEX(DB_NAME(), N'Recovery')),
    compatibility_level =
        CONVERT(integer, NULL),
    state_desc =
        N'ONLINE',
    volume_mount_point =
        CONVERT(nvarchar(256), NULL),
    volume_total_mb =
        CONVERT(decimal(19,2), NULL),
    volume_free_mb =
        CONVERT(decimal(19,2), NULL),
    is_percent_growth =
        CASE
            WHEN ls.is_log_service = 1
            THEN CONVERT(bit, NULL)
            ELSE df.is_percent_growth
        END,
    growth_pct =
        CASE
            WHEN ls.is_log_service = 0
            AND  df.is_percent_growth = 1
            THEN df.growth
            ELSE NULL
        END,
    vlf_count =
        CASE WHEN df.type = 1 /*LOG*/ THEN (SELECT CONVERT(integer, COUNT_BIG(*)) FROM sys.dm_db_log_info(DB_ID()) AS li WHERE li.file_id = df.file_id) ELSE NULL END
FROM sys.database_files AS df
CROSS APPLY
(
    /* The one place that decides which row is the log service's: the LOG row (type 1) of a Hyperscale
       database. Used space and the VLF count stay as collected - neither feeds an allocated total. */
    SELECT
        is_log_service =
            CASE
                WHEN @is_hyperscale = 1
                AND  df.type = 1 /*LOG*/
                THEN CONVERT(bit, 1)
                ELSE CONVERT(bit, 0)
            END
) AS ls;

/* #5498: no sibling arm. Siblings used to be read here from master's resource-stats view, but that view
   ingests a new database only about an hour after it is created (#3262), and the collector runs rarely, so
   a fresh logical server stored master's two files and nothing else (the 3.10 release test: 45 minutes,
   two user databases, master only). The host now connects to EACH database (RunsPerDatabase) and every
   database reports its own real files, so the master-only view is not read at all: reading it beside the
   per-database runs would store every database twice. */

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
    /// Per-database on Azure SQL DB (#5498), the same gate database_scoped_config, index_object_stats and
    /// query_store use. The query is database-scoped, so one connection to master reported master's two files
    /// and nothing else: the siblings came from master's <c>sys.resource_stats</c>, which ingests a new
    /// database only about an hour after it exists (#3262), so a fresh logical server stored master alone (the
    /// 3.10 Azure release test). Connecting to each database reports every database's real files at once.
    ///
    /// <para>#1631 made this false because the enumeration connects to <c>master</c>, which a login admitted by a
    /// DATABASE-level firewall rule cannot open (error 40615). That is handled by the host now: a
    /// registration that names a database sweeps that database alone and never touches master (#2220), and a
    /// logical-server registration is connected to master anyway, so it had nothing to lose.</para>
    ///
    /// <para>On every other target the one connection reads every database through the cursor.</para>
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

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
            /* Database exclusion happens in the host's database enumeration on Azure; this text runs once in
               each enumerated database (RunsPerDatabase). */
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
            /* Ordinals 1, 2 and 5 are NULL on every Azure sibling row (#2643's arm omits what
               sys.resource_stats cannot measure), and an unguarded read here killed the whole
               collection — master's own rows included — the moment the first sibling row appeared
               (#3262). */
            rows.Add(new Row(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                reader.GetString(3),
                reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                /* Ordinal 6 is NULL on the Hyperscale log row (the log service is not allocated storage);
                   an unguarded GetDecimal here would kill the whole batch the way #3262 did. */
                reader.IsDBNull(6) ? null : reader.GetDecimal(6),
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
