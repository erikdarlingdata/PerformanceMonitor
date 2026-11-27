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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// TempDB space usage from tempdb.sys.dm_db_file_space_usage plus the top tempdb-consuming
/// session (two result sets → one row). Extracted verbatim from Lite's
/// RemoteCollectorService.TempDb.cs. Always yields exactly one row — zeros when the result
/// sets are empty — matching the original collector's behavior. Applies to every SQL Server
/// target, Azure SQL Database included; see <see cref="AppliesTo"/> for the measurement that
/// removed the gate that used to exclude it.
///
/// <para><b>The third thing it captures, and why the DMV alone was not enough (#2515).</b>
/// <c>dm_db_file_space_usage</c> describes the tempdb data files AS CURRENTLY ALLOCATED, so
/// <c>total_reserved</c> ÷ (<c>total_reserved</c> + <c>unallocated</c>) is distance to the next
/// AUTOGROW, not distance to the point where tempdb can grow no further. That reads the same on a
/// pre-sized on-prem box only because such a tempdb has already reached its ceiling. Where the ceiling
/// is a real value it is discoverable — <c>SUM(max_size)</c> over tempdb's ROWS files — so
/// <c>max_size_mb</c> ships beside the allocation and the consumers divide by the ceiling instead.</para>
///
/// <para>Run verbatim against SQL Server 2022 (8 tempdb data files, 1 log file): unlimited files report
/// <c>-1</c>; capping the eight data files at 100 MB and the LOG at 2,048 MB reports <b>800.00</b>, so the
/// log's cap really is excluded rather than merely intended to be; returning one data file to UNLIMITED
/// takes the whole answer back to <c>-1</c>, which is the correct reading — tempdb as a whole can then grow
/// without limit.</para>
/// </summary>
public sealed class TempDbStatsCollector : CollectorDefinitionBase<TempDbStatsCollector.Row>
{
    public static TempDbStatsCollector Instance { get; } = new();

    private TempDbStatsCollector()
    {
    }

    public readonly record struct Row(
        decimal UserObjectReservedMb,
        decimal InternalObjectReservedMb,
        decimal VersionStoreReservedMb,
        decimal TotalReservedMb,
        decimal UnallocatedMb,
        long TotalSessions,
        int TopSessionId,
        decimal TopSessionMb,
        /* The ROWS-file growth ceiling in MB. -1 = at least one data file is unlimited, so there is no
           ceiling; 0 = the query returned NULL (no ROWS files visible). Appended LAST because both
           writers are positional and the payload column had to append too. */
        decimal MaxSizeMb,
        /* #8881: the tempdb LOG, appended LAST for the same reason. Every one is nullable and defaults to NULL:
           NULL means the engine could not say (a function that is missing or refuses), which is not a zero.
           The holder members are filled only while the log is at or above TempDbLogHolderLine.MinLogUsedPercent (#8881). */
        decimal? LogTotalMb = null,
        decimal? LogUsedMb = null,
        decimal? LogUsedPercent = null,
        string? LogReuseWaitDesc = null,
        string? LogTruncationHoldupReason = null,
        int? HolderSessionId = null,
        string? HolderLoginName = null,
        string? HolderProgramName = null,
        DateTime? HolderBeginTimeUtc = null,
        long? HolderLogBytesUsed = null,
        long? HolderLogBytesReserved = null);

    public override string Name => "tempdb_stats";

    public override string TargetTable => "tempdb_stats";

    public override string? WatermarkColumn => null;

    /// <summary>
    /// Applies to every SQL Server target, Azure SQL Database included.
    ///
    /// <para><b>The gate that used to be here, and why it is gone (#2512).</b> <c>!target.IsAzureSqlDb</c>
    /// excluded the whole Azure SQL Database tier, both General Purpose and Hyperscale, on the premise that
    /// the first result set's THREE-part <c>tempdb.sys.dm_db_file_space_usage</c> reference could not be
    /// served there — that the collector "could only ever fail" on the platform. That premise was
    /// checkable and it is false.</para>
    ///
    /// <para><b>What was measured</b> (2026-08-22, this collector's SQL verbatim, both result sets, over an
    /// Entra token). It binds and returns real data on both tiers. <c>GP_S_Gen5_2</c> (EngineEdition 5,
    /// 12.0.2000.8): user 5.44 MB, internal 1.81 MB, version store 0.00 MB, unallocated 54.19 MB, and one
    /// session over threshold. <c>HS_S_Gen5_2</c>: user 1.88 MB, unallocated 60.69 MB.
    /// <c>SELECT COUNT(*) FROM tempdb.sys.dm_db_file_space_usage</c> returns 4 on both. No Azure-specific
    /// query variant is needed, and the second result set (<c>sys.dm_db_session_space_usage</c>, two-part
    /// and in-database) works too — so the one row this collector promises is a WHOLE row on Azure SQL
    /// Database, which is the objection the old comment raised against recovering half of it.</para>
    ///
    /// <para><b>And the figures describe something, rather than merely returning.</b> That was the open
    /// question, so it was moved rather than reasoned about: allocating ~57 MB into a <c>#temp</c> table on
    /// <c>GP_S_Gen5_2</c> moved <c>user_mb</c> 1.88 → 59.75 and <c>unallocated_mb</c> 60.69 → 2.69,
    /// while <c>sys.dm_db_session_space_usage</c> attributed 59.25 MB to the session that did it. The
    /// counters track actual allocation to within the reserved-versus-allocated difference you would expect,
    /// and the session view attributes it to the right session. Not a constant, not a stub, and not another
    /// database's tempdb.</para>
    ///
    /// <para><b>Why this is worth MORE here than on a box.</b> The tempdb ceiling on Azure SQL Database is
    /// governed by the SERVICE TIER: you cannot add files and you cannot grow past the tier's cap. On a box
    /// "tempdb is filling" means "go look at the disk". Here it means "you are approaching a hard limit you
    /// cannot raise without changing service objective" — more actionable and more urgent, and
    /// completely invisible for as long as this was gated off.</para>
    ///
    /// <para><b>What <c>unallocated_mb</c> is, precisely — do not overstate it.</b> It is free space in
    /// the tempdb files AS CURRENTLY ALLOCATED. Azure SQL Database creates those files small (4 files,
    /// ~62 MB total on both 2-vCore tiers measured) and autogrows them toward the tier cap, so on this
    /// platform <c>total_reserved / (total_reserved + unallocated)</c> measures distance to the next
    /// autogrow, not distance to the tier ceiling. One ordinary temp table moves it a long way: the
    /// measurement above took that ratio from 3% to 96% with a single <c>#temp</c> — against the ALLOCATION.
    /// #2515 settled that by changing the denominator rather than adding a size floor: the ceiling is
    /// discoverable, so <c>max_size_mb</c> is collected beside the allocation and the consumers divide by
    /// it. The same <c>#temp</c> reads 0.09% against the 65,536 MB ROWS ceiling, which is the number worth
    /// alerting on. So the ratio DOES carry the <c>tempdb Space</c> alert on this tier now, and the absolute
    /// MB and the trend are corroboration rather than the only signal.</para>
    ///
    /// <para><b>Permissions, which is what the #2150 field report was actually about.</b> Error 262,
    /// "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'", is a real outcome for a
    /// login that lacks the grant — tempdb permissions are not persistable on Azure SQL Database, and in
    /// an elastic pool the database is not the permission boundary. But that is a property of the LOGIN, not
    /// of the tier, so it cannot be decided by a gate: it would have to deny every properly-permissioned
    /// Azure SQL Database target to spare the one that is not. #2512 classifies 262 as PERMISSIONS in both
    /// SKUs instead, so a login that genuinely cannot read this degrades to a non-fatal skip carrying an
    /// explanatory message, rather than the 11x-consecutive ERROR that motivated the gate.</para>
    ///
    /// <para><b>Managed Instance is unchanged</b> — it was never gated, it has a real tempdb and full
    /// DMV access, and <c>CollectorGateSurfacePinTests</c> asserts it explicitly so this cannot be
    /// re-narrowed by accident later.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    private static readonly string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    user_object_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.user_object_reserved_page_count) * 8 / 1024.0),
    internal_object_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.internal_object_reserved_page_count) * 8 / 1024.0),
    version_store_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.version_store_reserved_page_count) * 8 / 1024.0),
    total_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.user_object_reserved_page_count + dsu.internal_object_reserved_page_count + dsu.version_store_reserved_page_count) * 8 / 1024.0),
    unallocated_mb = CONVERT(decimal(18,2), SUM(dsu.unallocated_extent_page_count) * 8 / 1024.0),
    /* #2515: the ROWS-file growth ceiling, which is what the space alert's percentage divides by.
       Restricted to type = 0 deliberately — dm_db_file_space_usage above reports DATA allocation, so
       folding the log's cap into the same denominator would understate usage on every server.
       MIN() decides the unlimited case rather than any(): one unlimited data file means tempdb as a
       whole can grow without limit, and -1 sorts below every real cap so MIN finds it. max_size is int
       and a wide tempdb can carry several files at the 16 TB page maximum, so it is widened to bigint
       BEFORE the SUM rather than after it. */
    max_size_mb =
        (
            SELECT
                CASE
                    WHEN MIN(df.max_size) = -1
                    THEN CONVERT(decimal(18,2), -1)
                    ELSE CONVERT(decimal(18,2), SUM(CONVERT(bigint, df.max_size)) * 8 / 1024.0)
                END
            FROM tempdb.sys.database_files AS df
            WHERE df.type = 0 /*ROWS*/
        )
FROM tempdb.sys.dm_db_file_space_usage AS dsu
OPTION(RECOMPILE);

/* #8080: sys.dm_db_session_space_usage counts for the life of the session. The *_alloc_page_count columns are every
   page it has allocated since it connected and the *_dealloc_page_count columns every page it has released, so the
   pages it holds now are the difference. The figure, the ranking and the sessions-using-tempdb count all take that
   difference, so a long-lived session that churns temp tables is measured by its hold, not by everything it has
   ever allocated. A session is a consumer only while that difference is above zero: the pick filters on it, so a
   server where no session holds pages returns no row here and the reader stores the no-row shape (session 0,
   0.00 MB, 0 sessions) rather than some session at 0.00 MB, and the count takes the same filter. The difference
   can read below zero for a moment while a release lands, so the figure is clamped at 0 the way the active query
   snapshot clamps it. */
SELECT /* PerformanceMonitorLite */ TOP (1)
    session_id = ssu.session_id,
    tempdb_mb = CONVERT(decimal(18,2), CASE WHEN held.page_count < 0 THEN 0. ELSE held.page_count * 8 / 1024.0 END),
    total_sessions =
    (
        SELECT COUNT_BIG(*)
        FROM sys.dm_db_session_space_usage AS s
        WHERE (s.user_objects_alloc_page_count - s.user_objects_dealloc_page_count) + (s.internal_objects_alloc_page_count - s.internal_objects_dealloc_page_count) > 0
    )
FROM sys.dm_db_session_space_usage AS ssu
CROSS APPLY
(
    SELECT page_count = (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) + (ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count)
) AS held
WHERE held.page_count > 0
ORDER BY held.page_count DESC
OPTION(RECOMPILE);

/* #8881: the tempdb LOG. Every read below runs in its own sp_executesql inside its own TRY/CATCH, so an engine
   that lacks a view or function (dm_db_log_stats needs 2016 SP2, dm_db_log_space_usage needs 2012), or refuses
   it for a permission, leaves that one value NULL and takes nothing else with it. A missing object is a
   compile error in the same scope, which a TRY/CATCH in that scope does not catch, hence the dynamic batch.
   The third result set is always exactly one row. */
DECLARE
    @log_total_mb decimal(18,2),
    @log_used_mb decimal(18,2),
    @log_used_percent decimal(5,2),
    @log_reuse_wait_desc nvarchar(60),
    @log_truncation_holdup_reason nvarchar(60),
    @holder_session_id int,
    @holder_login_name nvarchar(128),
    @holder_program_name nvarchar(128),
    @holder_begin_time_utc datetime,
    @holder_log_bytes_used bigint,
    @holder_log_bytes_reserved bigint;

BEGIN TRY
    EXECUTE sys.sp_executesql
        N'SELECT /* PerformanceMonitorLite */
              @total_mb = CONVERT(decimal(18,2), lsu.total_log_size_in_bytes / 1048576.0),
              @used_mb = CONVERT(decimal(18,2), lsu.used_log_space_in_bytes / 1048576.0),
              @used_percent = CONVERT(decimal(5,2), lsu.used_log_space_in_percent)
          FROM tempdb.sys.dm_db_log_space_usage AS lsu;',
        N'@total_mb decimal(18,2) OUTPUT, @used_mb decimal(18,2) OUTPUT, @used_percent decimal(5,2) OUTPUT',
        @total_mb = @log_total_mb OUTPUT,
        @used_mb = @log_used_mb OUTPUT,
        @used_percent = @log_used_percent OUTPUT;
END TRY
BEGIN CATCH
END CATCH;

BEGIN TRY
    EXECUTE sys.sp_executesql
        N'SELECT /* PerformanceMonitorLite */ @reuse = d.log_reuse_wait_desc FROM sys.databases AS d WHERE d.database_id = 2;',
        N'@reuse nvarchar(60) OUTPUT',
        @reuse = @log_reuse_wait_desc OUTPUT;
END TRY
BEGIN CATCH
END CATCH;

BEGIN TRY
    EXECUTE sys.sp_executesql
        N'SELECT /* PerformanceMonitorLite */ @holdup = ls.log_truncation_holdup_reason FROM sys.dm_db_log_stats(2) AS ls;',
        N'@holdup nvarchar(60) OUTPUT',
        @holdup = @log_truncation_holdup_reason OUTPUT;
END TRY
BEGIN CATCH
END CATCH;

/* The holder: the oldest active tempdb transaction, read only while the log is at or above the bar. LEFT JOINs
   keep a transaction that no session owns. The begin time is the server's local time, moved to UTC here so the
   store holds one clock. */
IF @log_used_percent >= __LOG_HOLDER_MIN_USED_PERCENT__
BEGIN
    BEGIN TRY
        EXECUTE sys.sp_executesql
            N'SELECT /* PerformanceMonitorLite */ TOP (1)
                  @session_id = st.session_id,
                  @login_name = es.login_name,
                  @program_name = es.program_name,
                  @begin_time_utc = DATEADD(SECOND, DATEDIFF(SECOND, GETDATE(), GETUTCDATE()), dt.database_transaction_begin_time),
                  @log_bytes_used = dt.database_transaction_log_bytes_used,
                  @log_bytes_reserved = dt.database_transaction_log_bytes_reserved
              FROM sys.dm_tran_database_transactions AS dt
              LEFT JOIN sys.dm_tran_session_transactions AS st
                ON st.transaction_id = dt.transaction_id
              LEFT JOIN sys.dm_exec_sessions AS es
                ON es.session_id = st.session_id
              WHERE dt.database_id = 2
              AND   dt.database_transaction_begin_time IS NOT NULL
              ORDER BY dt.database_transaction_begin_time ASC;',
            N'@session_id int OUTPUT, @login_name nvarchar(128) OUTPUT, @program_name nvarchar(128) OUTPUT, @begin_time_utc datetime OUTPUT, @log_bytes_used bigint OUTPUT, @log_bytes_reserved bigint OUTPUT',
            @session_id = @holder_session_id OUTPUT,
            @login_name = @holder_login_name OUTPUT,
            @program_name = @holder_program_name OUTPUT,
            @begin_time_utc = @holder_begin_time_utc OUTPUT,
            @log_bytes_used = @holder_log_bytes_used OUTPUT,
            @log_bytes_reserved = @holder_log_bytes_reserved OUTPUT;
    END TRY
    BEGIN CATCH
    END CATCH;
END;

SELECT /* PerformanceMonitorLite */
    log_total_mb = @log_total_mb,
    log_used_mb = @log_used_mb,
    log_used_percent = @log_used_percent,
    log_reuse_wait_desc = @log_reuse_wait_desc,
    log_truncation_holdup_reason = @log_truncation_holdup_reason,
    holder_session_id = @holder_session_id,
    holder_login_name = @holder_login_name,
    holder_program_name = @holder_program_name,
    holder_begin_time_utc = @holder_begin_time_utc,
    holder_log_bytes_used = @holder_log_bytes_used,
    holder_log_bytes_reserved = @holder_log_bytes_reserved;
".Replace("__LOG_HOLDER_MIN_USED_PERCENT__", TempDbLogHolderLine.MinLogUsedPercent.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("user_object_reserved_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("internal_object_reserved_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("version_store_reserved_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("total_reserved_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("unallocated_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("total_sessions_using_tempdb", CollectorColumnType.BigInt),
        new CollectorColumn("top_session_id", CollectorColumnType.Integer),
        new CollectorColumn("top_session_tempdb_mb", CollectorColumnType.Decimal, 18, 2),
        /* Appended, never inserted: both stores generate their DDL from this list in order and both row
           writers are positional, so a column added anywhere else would silently re-map history. */
        new CollectorColumn("max_size_mb", CollectorColumnType.Decimal, 18, 2),
        /* #8881: the tempdb log, appended for the same reason. All nullable: NULL is "the engine could not say". */
        new CollectorColumn("log_total_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("log_used_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("log_used_percent", CollectorColumnType.Decimal, 5, 2),
        new CollectorColumn("log_reuse_wait_desc", CollectorColumnType.Varchar),
        new CollectorColumn("log_truncation_holdup_reason", CollectorColumnType.Varchar),
        new CollectorColumn("holder_session_id", CollectorColumnType.Integer),
        new CollectorColumn("holder_login_name", CollectorColumnType.Varchar),
        new CollectorColumn("holder_program_name", CollectorColumnType.Varchar),
        new CollectorColumn("holder_begin_time", CollectorColumnType.Timestamp),
        new CollectorColumn("holder_log_bytes_used", CollectorColumnType.BigInt),
        new CollectorColumn("holder_log_bytes_reserved", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        decimal userObjMb = 0, internalObjMb = 0, versionStoreMb = 0, totalReservedMb = 0, unallocatedMb = 0;
        int topSessionId = 0;
        long totalSessions = 0;
        decimal topSessionMb = 0;
        decimal maxSizeMb = 0;

        if (await reader.ReadAsync(cancellationToken))
        {
            userObjMb = reader.IsDBNull(0) ? 0m : reader.GetDecimal(0);
            internalObjMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1);
            versionStoreMb = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2);
            totalReservedMb = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3);
            unallocatedMb = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4);
            /* NULL means tempdb showed no ROWS files, which is not a measurement; 0 carries that through
               to the consumers, where it reads as "no ceiling measured" and the denominator stays the
               allocation — exactly what they did before this column existed. */
            maxSizeMb = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5);
        }

        if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
        {
            topSessionId = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            topSessionMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1);
            totalSessions = reader.IsDBNull(2) ? 0L : reader.GetInt64(2);
        }

        /* #8881: the third result set is the log, always one row of nullable values. A reader without it (a
           fake, an older query text) simply leaves every log member NULL. */
        decimal? logTotalMb = null, logUsedMb = null, logUsedPercent = null;
        string? reuseWait = null, holdupReason = null, holderLogin = null, holderProgram = null;
        int? holderSession = null;
        DateTime? holderBegin = null;
        long? holderUsed = null, holderReserved = null;

        if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
        {
            logTotalMb = reader.IsDBNull(0) ? null : reader.GetDecimal(0);
            logUsedMb = reader.IsDBNull(1) ? null : reader.GetDecimal(1);
            logUsedPercent = reader.IsDBNull(2) ? null : reader.GetDecimal(2);
            reuseWait = reader.IsDBNull(3) ? null : reader.GetString(3);
            holdupReason = reader.IsDBNull(4) ? null : reader.GetString(4);
            holderSession = reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5));
            holderLogin = reader.IsDBNull(6) ? null : reader.GetString(6);
            holderProgram = reader.IsDBNull(7) ? null : reader.GetString(7);
            holderBegin = reader.IsDBNull(8) ? null : DateTime.SpecifyKind(reader.GetDateTime(8), DateTimeKind.Unspecified);
            holderUsed = reader.IsDBNull(9) ? null : reader.GetInt64(9);
            holderReserved = reader.IsDBNull(10) ? null : reader.GetInt64(10);
        }

        return new List<Row>
        {
            new(userObjMb, internalObjMb, versionStoreMb, totalReservedMb, unallocatedMb, totalSessions, topSessionId, topSessionMb, maxSizeMb,
                logTotalMb, logUsedMb, logUsedPercent, reuseWait, holdupReason, holderSession, holderLogin, holderProgram, holderBegin, holderUsed, holderReserved),
        };
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.UserObjectReservedMb)      /* user_object_reserved_mb DECIMAL */
            .Value(row.InternalObjectReservedMb)  /* internal_object_reserved_mb DECIMAL */
            .Value(row.VersionStoreReservedMb)    /* version_store_reserved_mb DECIMAL */
            .Value(row.TotalReservedMb)           /* total_reserved_mb DECIMAL */
            .Value(row.UnallocatedMb)             /* unallocated_mb DECIMAL */
            .Value(row.TotalSessions)             /* total_sessions_using_tempdb BIGINT */
            .Value(row.TopSessionId)              /* top_session_id INTEGER */
            .Value(row.TopSessionMb)              /* top_session_tempdb_mb DECIMAL */
            .Value(row.MaxSizeMb)                 /* max_size_mb DECIMAL */
            .Value(row.LogTotalMb)                /* log_total_mb DECIMAL, NULL when the engine could not say */
            .Value(row.LogUsedMb)                 /* log_used_mb DECIMAL */
            .Value(row.LogUsedPercent)            /* log_used_percent DECIMAL */
            .Value(row.LogReuseWaitDesc)          /* log_reuse_wait_desc VARCHAR */
            .Value(row.LogTruncationHoldupReason) /* log_truncation_holdup_reason VARCHAR */
            .Value(row.HolderSessionId)           /* holder_session_id INTEGER, only at or above the bar */
            .Value(row.HolderLoginName)           /* holder_login_name VARCHAR */
            .Value(row.HolderProgramName)         /* holder_program_name VARCHAR */
            .Value(row.HolderBeginTimeUtc)        /* holder_begin_time TIMESTAMP, UTC */
            .Value(row.HolderLogBytesUsed)        /* holder_log_bytes_used BIGINT */
            .Value(row.HolderLogBytesReserved);   /* holder_log_bytes_reserved BIGINT */
    }
}
