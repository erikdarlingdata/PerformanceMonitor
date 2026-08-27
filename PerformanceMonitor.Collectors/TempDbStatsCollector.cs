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
        decimal MaxSizeMb);

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

    private const string QueryText = @"
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

SELECT /* PerformanceMonitorLite */ TOP (1)
    session_id = ssu.session_id,
    tempdb_mb = CONVERT(decimal(18,2), (ssu.user_objects_alloc_page_count + ssu.internal_objects_alloc_page_count) * 8 / 1024.0),
    total_sessions = (SELECT COUNT_BIG(*) FROM sys.dm_db_session_space_usage WHERE user_objects_alloc_page_count + internal_objects_alloc_page_count > 0)
FROM sys.dm_db_session_space_usage AS ssu
ORDER BY (ssu.user_objects_alloc_page_count + ssu.internal_objects_alloc_page_count) DESC
OPTION(RECOMPILE);";

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

        return new List<Row>
        {
            new(userObjMb, internalObjMb, versionStoreMb, totalReservedMb, unallocatedMb, totalSessions, topSessionId, topSessionMb, maxSizeMb),
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
            .Value(row.MaxSizeMb);                /* max_size_mb DECIMAL */
    }
}
