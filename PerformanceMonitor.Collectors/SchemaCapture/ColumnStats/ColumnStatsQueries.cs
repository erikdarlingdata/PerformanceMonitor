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
using System.Text;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #8798: the T-SQL for the column_stats collector, as text only. Every batch runs inside the target database (the caller wraps it in
/// <c>EXECUTE [db].sys.sp_executesql</c>, or runs it directly on Azure SQL Database), so it uses database-local catalog views and
/// <c>DB_NAME()</c>, never three-part names. The prologue is the catalog collectors' one: LOCK_TIMEOUT 5000 so a blocked read fails
/// fast (error 1222) instead of holding the database budget, and no READ UNCOMMITTED. Only the density vector and header are ever
/// requested; the histogram is never read.
/// </summary>
public static class ColumnStatsQueries
{
    /* SchemaCatalogQueries.Prologue is private and also creates that collector's id table, so the two settings are repeated here. */
    private const string Prologue =
        "SET NOCOUNT ON; /* PerformanceMonitorLite */\r\n"
        + "SET LOCK_TIMEOUT 5000;\r\n";

    /// <summary>The ids a batch inlines per INSERT; a table value constructor takes at most 1000 rows.</summary>
    private const int IdsPerInsert = 1000;

    /// <summary>The permission probe: DBCC SHOW_STATISTICS needs SELECT on the table's columns, which this reports database-wide.</summary>
    public const string HasSelectSql = "SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'SELECT')";

    /// <summary>
    /// Result set 1: one row per candidate statistic, largest tables first, at most <see cref="ColumnStatsModel.MaxStatisticsPerDatabase"/>.
    /// Columns: object_id, schema_name, object_name, stats_id, stats_name, origin ('index', 'user' or 'auto'), stats_date, table_rows
    /// (heap or clustered partitions), stat_columns (the statistic's columns in order), clustered_key_columns (the clustering key
    /// columns of the table, only for a nonclustered index statistic, where SQL Server appends them to the density vector),
    /// stats_date_utc. stats_date is <c>STATS_DATE</c> in the server's local time, kept as it is for the change check; stats_date_utc is
    /// the same instant in UTC, for display: converted with <c>AT TIME ZONE</c> from the server's own time zone where SQL Server reports
    /// it (<c>CURRENT_TIMEZONE_ID()</c>: SQL Server 2022 and later, Azure SQL Database and Managed Instance), else moved by the server's
    /// current offset from UTC, which is an hour off for a statistic updated on the other side of a daylight saving change. Both lists
    /// are comma-joined with a backslash escaping <c>\</c> and <c>,</c> inside a name, as SchemaCatalogQueries writes its lists, and
    /// NULL when empty. Candidates are user tables and indexed views. The cap is applied to ids before any name or column list is
    /// resolved, so a database over the cap pays for the names of the capped statistics only.
    /// Result set 2: one row of counts of statistics left out, each in exactly one bucket, first match winning: filtered,
    /// memory_optimized, xml_spatial_columnstore (the statistic belongs to an XML, spatial, or clustered or nonclustered columnstore
    /// index), never_updated (STATS_DATE is NULL), and over_cap (candidates beyond the cap). Priority: memory_optimized, then
    /// xml_spatial_columnstore, then filtered, then never_updated.
    /// Result set 3: object_id, stats_id of EVERY candidate, only when the database is over the cap (empty otherwise): set 1 is then not
    /// the whole database, and the caller needs every id to tell a dropped statistic from one beyond the cap.
    /// </summary>
    public static readonly string CandidatesSql = BuildCandidatesSql(ColumnStatsModel.MaxStatisticsPerDatabase);

    /// <summary><see cref="CandidatesSql"/> with another cap, so a test can put a small database over it.</summary>
    public static string BuildCandidatesSql(int cap) =>
        Prologue + CandidatesBody.Replace("{CAP}", cap.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);

    private const string CandidatesBody = """
IF OBJECT_ID(N'tempdb..#pm_cs_base') IS NOT NULL DROP TABLE #pm_cs_base;
IF OBJECT_ID(N'tempdb..#pm_cs_top') IS NOT NULL DROP TABLE #pm_cs_top;

/* Every user statistic, as ids and the facts the buckets and the order need: no names and no column lists yet. */
SELECT
    s.object_id,
    s.stats_id,
    s.has_filter,
    CONVERT(bit, ISNULL(t.is_memory_optimized, 0)) AS is_memory_optimized,
    CONVERT(bit, CASE WHEN i.type IN (3, 4, 5, 6) THEN 1 ELSE 0 END) AS excluded_index_type,
    STATS_DATE(s.object_id, s.stats_id) AS stats_date,
    CONVERT(bigint, ISNULL(ps.table_rows, 0)) AS table_rows
INTO #pm_cs_base
FROM sys.stats AS s
JOIN sys.objects AS o ON o.object_id = s.object_id
LEFT JOIN sys.tables AS t ON t.object_id = o.object_id
LEFT JOIN sys.indexes AS i ON i.object_id = s.object_id AND i.index_id = s.stats_id
LEFT JOIN
(
    SELECT p.object_id, SUM(p.row_count) AS table_rows
    FROM sys.dm_db_partition_stats AS p
    WHERE p.index_id IN (0, 1)
    GROUP BY p.object_id
) AS ps ON ps.object_id = s.object_id
WHERE o.type IN ('U', 'V') AND o.is_ms_shipped = 0;

DECLARE @candidates int =
(
    SELECT COUNT(*) FROM #pm_cs_base AS b
    WHERE b.has_filter = 0 AND b.is_memory_optimized = 0 AND b.excluded_index_type = 0 AND b.stats_date IS NOT NULL
);

SELECT TOP ({CAP})
    b.object_id,
    b.stats_id,
    b.stats_date,
    b.table_rows,
    DATEADD(MINUTE, DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()), b.stats_date) AS stats_date_utc
INTO #pm_cs_top
FROM #pm_cs_base AS b
WHERE b.has_filter = 0
  AND b.is_memory_optimized = 0
  AND b.excluded_index_type = 0
  AND b.stats_date IS NOT NULL
ORDER BY b.table_rows DESC, b.object_id, b.stats_id;

/* The exact instant where the server names its time zone. CURRENT_TIMEZONE_ID(), sys.time_zone_info and AT TIME ZONE are in dynamic
   SQL, so an older build that knows none of them still compiles this batch and keeps the current offset above. */
DECLARE @tz nvarchar(256) = NULL;
IF CONVERT(int, SERVERPROPERTY('EngineEdition')) IN (5, 8) OR CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) >= 16
BEGIN
    BEGIN TRY
        EXECUTE sys.sp_executesql
            N'SET @z = CURRENT_TIMEZONE_ID(); IF NOT EXISTS (SELECT 1 FROM sys.time_zone_info AS z WHERE z.name = @z) SET @z = NULL;',
            N'@z nvarchar(256) OUTPUT',
            @z = @tz OUTPUT;

        IF @tz IS NOT NULL
        BEGIN
            EXECUTE sys.sp_executesql
                N'UPDATE #pm_cs_top SET stats_date_utc = CONVERT(datetime, CONVERT(datetime2(3), stats_date) AT TIME ZONE @z AT TIME ZONE N''UTC'');',
                N'@z nvarchar(256)',
                @z = @tz;
        END;
    END TRY
    BEGIN CATCH
        SET @tz = NULL;
    END CATCH;
END;

SELECT
    b.object_id,
    SCHEMA_NAME(o.schema_id) AS schema_name,
    o.name AS object_name,
    b.stats_id,
    s.name AS stats_name,
    CASE WHEN i.index_id IS NOT NULL THEN N'index' WHEN s.auto_created = 1 THEN N'auto' ELSE N'user' END AS origin,
    b.stats_date,
    b.table_rows,
    STUFF((
        SELECT N',' + REPLACE(REPLACE(c.name, N'\', N'\\'), N',', N'\,')
        FROM sys.stats_columns AS sc
        JOIN sys.columns AS c ON c.object_id = sc.object_id AND c.column_id = sc.column_id
        WHERE sc.object_id = b.object_id AND sc.stats_id = b.stats_id
        ORDER BY sc.stats_column_id
        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 1, N'') AS stat_columns,
    CASE WHEN i.index_id IS NOT NULL AND b.stats_id > 1 THEN STUFF((
        SELECT N',' + REPLACE(REPLACE(c.name, N'\', N'\\'), N',', N'\,')
        FROM sys.index_columns AS ic
        JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE ic.object_id = b.object_id AND ic.index_id = 1 AND ic.key_ordinal > 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 1, N'') END AS clustered_key_columns,
    b.stats_date_utc
FROM #pm_cs_top AS b
JOIN sys.objects AS o ON o.object_id = b.object_id
JOIN sys.stats AS s ON s.object_id = b.object_id AND s.stats_id = b.stats_id
LEFT JOIN sys.indexes AS i ON i.object_id = b.object_id AND i.index_id = b.stats_id
ORDER BY b.table_rows DESC, b.object_id, b.stats_id;

SELECT
    ISNULL(SUM(CASE WHEN b.is_memory_optimized = 0 AND b.excluded_index_type = 0 AND b.has_filter = 1 THEN 1 ELSE 0 END), 0) AS filtered,
    ISNULL(SUM(CASE WHEN b.is_memory_optimized = 1 THEN 1 ELSE 0 END), 0) AS memory_optimized,
    ISNULL(SUM(CASE WHEN b.is_memory_optimized = 0 AND b.excluded_index_type = 1 THEN 1 ELSE 0 END), 0) AS xml_spatial_columnstore,
    ISNULL(SUM(CASE WHEN b.is_memory_optimized = 0 AND b.excluded_index_type = 0 AND b.has_filter = 0 AND b.stats_date IS NULL THEN 1 ELSE 0 END), 0) AS never_updated,
    CASE WHEN @candidates > {CAP} THEN @candidates - {CAP} ELSE 0 END AS over_cap
FROM #pm_cs_base AS b;

SELECT b.object_id, b.stats_id
FROM #pm_cs_base AS b
WHERE @candidates > {CAP}
  AND b.has_filter = 0
  AND b.is_memory_optimized = 0
  AND b.excluded_index_type = 0
  AND b.stats_date IS NOT NULL;

DROP TABLE #pm_cs_top;
DROP TABLE #pm_cs_base;
""";

    /// <summary>
    /// STAT_HEADER gained Persisted Sample Percent in SQL Server 2016 SP1 CU4 (13.0.4446) and 2017 CU1 (14.0.3006); every later major
    /// has it. The column count of the header decides whether <c>INSERT ... EXEC</c> succeeds, so the caller picks the shape from the
    /// build. Minor is accepted for symmetry with the version triple and does not change the answer.
    /// </summary>
    public static bool HeaderHasPersistedSamplePercent(int major, int minor, int build)
    {
        _ = minor;
        return major switch
        {
            >= 15 => true,
            14 => build >= 3006,
            13 => build >= 4446,
            _ => false,
        };
    }

    /// <summary>
    /// A loop over the given statistics that reads each one's header and density vector with DBCC SHOW_STATISTICS (never the histogram),
    /// each inside its own TRY/CATCH so one failing statistic does not stop the rest. Names are resolved in the batch from the ids and
    /// quoted, apostrophes doubled for the dynamic string. A statistic that fails leaves no rows in sets 1 and 2 and one row in set 3.
    /// Result sets, in order: (1) headers keyed by object_id, stats_id: Rows, Rows Sampled, Steps, Density, Average key length, String
    /// Index, Unfiltered Rows and, when <paramref name="headerHasPersistedSamplePercent"/>, Persisted Sample Percent (the filter expression
    /// and DBCC's Updated text are captured by the DBCC call and never returned: Updated is text in the login's language, so it is kept as
    /// text and never converted); (2) density rows keyed by object_id, stats_id, with
    /// ord (1-based position within that statistic's vector), all_density and average_length: row ord describes the leading ord columns, and the Columns
    /// text of DBCC is discarded because the names come from the catalog; (3) errors: object_id, stats_id, error_number, error_message.
    /// Header shape contract: the header call fails with error 213 (column count mismatch) when the chosen shape is wrong for the
    /// server. That is systematic, so the loop stops at the first such failure, records it as an errors row with error_number 213, and
    /// returns what it has; the caller sees 213 and runs the batch again with the other value of the flag.
    /// Lock contract: after a lock timeout (1222) on one statistic, every later statistic of the same object_id in the batch is not tried
    /// and gets its own errors row with error_number 1222, so a table under a schema lock costs one LOCK_TIMEOUT wait, not one per
    /// statistic.
    /// </summary>
    public static string BuildReadBatch(IReadOnlyList<(int ObjectId, int StatsId)> stats, bool headerHasPersistedSamplePercent)
    {
        ArgumentNullException.ThrowIfNull(stats);

        var sb = new StringBuilder(Prologue);
        sb.Append("IF OBJECT_ID(N'tempdb..#todo') IS NOT NULL DROP TABLE #todo;\r\n");
        sb.Append("IF OBJECT_ID(N'tempdb..#hdr') IS NOT NULL DROP TABLE #hdr;\r\n");
        sb.Append("IF OBJECT_ID(N'tempdb..#dv') IS NOT NULL DROP TABLE #dv;\r\n");
        sb.Append("IF OBJECT_ID(N'tempdb..#errs') IS NOT NULL DROP TABLE #errs;\r\n");
        sb.Append("CREATE TABLE #todo (ord int IDENTITY(1, 1) NOT NULL PRIMARY KEY, object_id int NOT NULL, stats_id int NOT NULL);\r\n");
        sb.Append("CREATE TABLE #hdr (object_id int NULL, stats_id int NULL, [Name] sysname NULL, [Updated] nvarchar(128) NULL, [Rows] bigint NULL, "
            + "[Rows Sampled] bigint NULL, [Steps] int NULL, [Density] float NULL, [Average key length] float NULL, [String Index] varchar(3) NULL, "
            + "[Filter Expression] nvarchar(max) NULL, [Unfiltered Rows] bigint NULL"
            + (headerHasPersistedSamplePercent ? ", [Persisted Sample Percent] float NULL" : string.Empty)
            + ");\r\n");
        sb.Append("CREATE TABLE #dv (ord int IDENTITY(1, 1) NOT NULL, object_id int NULL, stats_id int NULL, all_density float NULL, average_length float NULL, columns nvarchar(max) NULL);\r\n");
        sb.Append("CREATE TABLE #errs (object_id int NOT NULL, stats_id int NOT NULL, error_number int NOT NULL, error_message nvarchar(4000) NULL);\r\n");

        for (var offset = 0; offset < stats.Count; offset += IdsPerInsert)
        {
            sb.Append("INSERT #todo (object_id, stats_id) VALUES ");
            var end = Math.Min(offset + IdsPerInsert, stats.Count);
            for (var i = offset; i < end; i++)
            {
                if (i > offset)
                {
                    sb.Append(", ");
                }

                sb.Append('(')
                    .Append(stats[i].ObjectId.ToString(CultureInfo.InvariantCulture))
                    .Append(", ")
                    .Append(stats[i].StatsId.ToString(CultureInfo.InvariantCulture))
                    .Append(')');
            }

            sb.Append(";\r\n");
        }

        var persisted = headerHasPersistedSamplePercent ? ", [Persisted Sample Percent]" : string.Empty;
        var headerInsertColumns = "[Name], [Updated], [Rows], [Rows Sampled], [Steps], [Density], [Average key length], [String Index], [Filter Expression], [Unfiltered Rows]" + persisted;
        var headerSelectColumns = "[Rows], [Rows Sampled], [Steps], [Density], [Average key length], [String Index], [Unfiltered Rows]" + persisted;

        sb.Append(LoopHead);
        sb.Append("        INSERT #hdr (").Append(headerInsertColumns).Append(") EXEC (@sql);\r\n");
        sb.Append(LoopTail);
        sb.Append("SELECT object_id, stats_id, ").Append(headerSelectColumns).Append(" FROM #hdr ORDER BY object_id, stats_id;\r\n");
        sb.Append("SELECT object_id, stats_id, ROW_NUMBER() OVER (PARTITION BY object_id, stats_id ORDER BY ord) AS ord, all_density, average_length FROM #dv ORDER BY object_id, stats_id, #dv.ord;\r\n");
        sb.Append("SELECT object_id, stats_id, error_number, error_message FROM #errs ORDER BY object_id, stats_id;\r\n");
        return sb.ToString();
    }

    /* The target is "[schema].[table]" (QUOTENAME) and the statistic name is passed unbracketed, because DBCC SHOW_STATISTICS looks a
       bracketed statistic name up literally and fails with error 2767. Both have their apostrophes doubled for the
       string literal of the dynamic DBCC call. The ids are the only inputs and are integers, so nothing the caller supplied is text.
       @stage tells the CATCH which call failed: 213 on the header call (stage 1) is the shape mismatch the caller retries. */
    private const string LoopHead = """
DECLARE @i int = 1, @n int = (SELECT COUNT(*) FROM #todo), @oid int, @sid int, @target nvarchar(520), @sname nvarchar(260), @sql nvarchar(max), @stage int, @err int, @msg nvarchar(4000);
WHILE @i <= @n
BEGIN
    SELECT @oid = object_id, @sid = stats_id FROM #todo WHERE ord = @i;
    SET @i += 1;
    IF EXISTS (SELECT 1 FROM #errs WHERE object_id = @oid AND error_number = 1222)
    BEGIN
        INSERT #errs (object_id, stats_id, error_number, error_message)
        VALUES (@oid, @sid, 1222, N'Not tried: an earlier statistic of this table hit the lock timeout in this batch.');
        CONTINUE;
    END;
    SET @target = NULL;
    SET @sname = NULL;
    SET @stage = 0;
    BEGIN TRY
        SELECT @target = QUOTENAME(SCHEMA_NAME(o.schema_id)) + N'.' + QUOTENAME(o.name), @sname = s.name
        FROM sys.objects AS o
        JOIN sys.stats AS s ON s.object_id = o.object_id
        WHERE o.object_id = @oid AND s.stats_id = @sid;
        IF @target IS NULL
        BEGIN
            THROW 50000, N'The statistic no longer exists.', 1;
        END;
        SET @sql = N'DBCC SHOW_STATISTICS (N''' + REPLACE(@target, N'''', N'''''') + N''', N''' + REPLACE(@sname, N'''', N'''''') + N''') WITH STAT_HEADER, NO_INFOMSGS';
        SET @stage = 1;

""";

    private const string LoopTail = """
        UPDATE #hdr SET object_id = @oid, stats_id = @sid WHERE object_id IS NULL;
        SET @stage = 2;
        SET @sql = N'DBCC SHOW_STATISTICS (N''' + REPLACE(@target, N'''', N'''''') + N''', N''' + REPLACE(@sname, N'''', N'''''') + N''') WITH DENSITY_VECTOR, NO_INFOMSGS';
        INSERT #dv (all_density, average_length, columns) EXEC (@sql);
        UPDATE #dv SET object_id = @oid, stats_id = @sid WHERE object_id IS NULL;
    END TRY
    BEGIN CATCH
        SET @err = ERROR_NUMBER();
        SET @msg = ERROR_MESSAGE();
        DELETE FROM #hdr WHERE object_id IS NULL OR (object_id = @oid AND stats_id = @sid);
        DELETE FROM #dv WHERE object_id IS NULL OR (object_id = @oid AND stats_id = @sid);
        INSERT #errs (object_id, stats_id, error_number, error_message) VALUES (@oid, @sid, @err, @msg);
        IF @err = 213 AND @stage = 1
        BEGIN
            BREAK;
        END;
    END CATCH;
END;

""";
}
