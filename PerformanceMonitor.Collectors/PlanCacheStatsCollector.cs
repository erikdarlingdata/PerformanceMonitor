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
/// Plan-cache composition snapshot from sys.dm_exec_cached_plans, aggregated by cacheobjtype/objtype
/// into single-use vs multi-use plan/size bloat (the Dashboard-parity port of
/// install/35_collect_plan_cache_stats.sql). A POINT-IN-TIME snapshot, not a cumulative-counter
/// delta collector: it reports the current cache composition directly, one row per
/// (cacheobjtype, objtype) group — no delta groups (mirrors <see cref="MemoryStatsCollector"/> /
/// <see cref="SessionStatsCollector"/>). oldest_plan_create_time is a single uncorrelated
/// MIN(creation_time) over sys.dm_exec_query_stats, so every group row carries the same value
/// (identical to the Dashboard proc). The proc's collection_log logging and RAISERROR single-use
/// debug output are the collector framework's job and are intentionally dropped.
///
/// <para>Available on SQL Server, Azure SQL Database, and Azure SQL Managed Instance (both DMVs
/// verified against MS Learn — "Applies to" lists all three). On Azure SQL Database the cache is
/// inherently scoped to the connected database (cross-tenant rows filtered), which is a platform
/// property, not a collection gap — so <see cref="AppliesTo"/> is unconditionally true. We do not
/// read the Azure-NULLed memory_object_address / pool_id columns.</para>
/// </summary>
public sealed class PlanCacheStatsCollector : CollectorDefinitionBase<PlanCacheStatsCollector.Row>
{
    public static PlanCacheStatsCollector Instance { get; } = new();

    private PlanCacheStatsCollector()
    {
    }

    public readonly record struct Row(
        string Cacheobjtype,
        string Objtype,
        int TotalPlans,
        int TotalSizeMb,
        int SingleUsePlans,
        int SingleUseSizeMb,
        int MultiUsePlans,
        int MultiUseSizeMb,
        decimal AvgUseCount,
        int AvgSizeKb,
        DateTime? OldestPlanCreateTime);

    /* Ported verbatim from install/35_collect_plan_cache_stats.sql's SELECT. The only addition over
       the proc body is CONVERT(integer, COUNT_BIG(*)) for total_plans (the proc relied on the INSERT
       into an integer column to convert implicitly) so the positional reader gets a clean int. All
       other expressions already emit their storage type. Values stored are identical to the
       Dashboard table. */
    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    cacheobjtype = cp.cacheobjtype,
    objtype = cp.objtype,
    total_plans = CONVERT(integer, COUNT_BIG(*)),
    total_size_mb = CONVERT(integer, SUM(CONVERT(bigint, cp.size_in_bytes)) / 1024 / 1024),
    single_use_plans =
        SUM
        (
            CASE
                WHEN cp.usecounts = 1
                THEN 1
                ELSE 0
            END
        ),
    single_use_size_mb =
        CONVERT
        (
            integer,
            SUM
            (
                CASE
                    WHEN cp.usecounts = 1
                    THEN CONVERT(bigint, cp.size_in_bytes)
                    ELSE 0
                END
            ) / 1024 / 1024
        ),
    multi_use_plans =
        SUM
        (
            CASE
                WHEN cp.usecounts > 1
                THEN 1
                ELSE 0
            END
        ),
    multi_use_size_mb =
        CONVERT
        (
            integer,
            SUM
            (
                CASE
                    WHEN cp.usecounts > 1
                    THEN CONVERT(bigint, cp.size_in_bytes)
                    ELSE 0
                END
            ) / 1024 / 1024
        ),
    avg_use_count = CONVERT(decimal(38,2), AVG(CONVERT(bigint, cp.usecounts))),
    avg_size_kb =
        CASE
            WHEN AVG(CONVERT(bigint, cp.size_in_bytes)) / 1024 > 2147483647
            THEN 2147483647
            ELSE CONVERT(integer, AVG(CONVERT(bigint, cp.size_in_bytes)) / 1024)
        END,
    oldest_plan_create_time =
    (
        SELECT
            MIN(deqs.creation_time)
        FROM sys.dm_exec_query_stats AS deqs
    )
FROM sys.dm_exec_cached_plans AS cp
GROUP BY
    cp.cacheobjtype,
    cp.objtype
OPTION(RECOMPILE);";

    public override string Name => "plan_cache_stats";

    public override string TargetTable => "plan_cache_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("cacheobjtype", CollectorColumnType.Varchar),
        new CollectorColumn("objtype", CollectorColumnType.Varchar),
        new CollectorColumn("total_plans", CollectorColumnType.Integer),
        new CollectorColumn("total_size_mb", CollectorColumnType.Integer),
        new CollectorColumn("single_use_plans", CollectorColumnType.Integer),
        new CollectorColumn("single_use_size_mb", CollectorColumnType.Integer),
        new CollectorColumn("multi_use_plans", CollectorColumnType.Integer),
        new CollectorColumn("multi_use_size_mb", CollectorColumnType.Integer),
        new CollectorColumn("avg_use_count", CollectorColumnType.Decimal, 38, 2),
        new CollectorColumn("avg_size_kb", CollectorColumnType.Integer),
        new CollectorColumn("oldest_plan_create_time", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                Cacheobjtype: reader.GetString(0),
                Objtype: reader.GetString(1),
                TotalPlans: reader.GetInt32(2),
                TotalSizeMb: reader.GetInt32(3),
                SingleUsePlans: reader.GetInt32(4),
                SingleUseSizeMb: reader.GetInt32(5),
                MultiUsePlans: reader.GetInt32(6),
                MultiUseSizeMb: reader.GetInt32(7),
                AvgUseCount: reader.GetDecimal(8),
                AvgSizeKb: reader.GetInt32(9),
                /* Empty query-stats DMV -> MIN(creation_time) is NULL; matches the nullable column. */
                OldestPlanCreateTime: reader.IsDBNull(10) ? null : reader.GetDateTime(10)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.Cacheobjtype)          /* cacheobjtype VARCHAR */
            .Value(row.Objtype)               /* objtype VARCHAR */
            .Value(row.TotalPlans)            /* total_plans INTEGER */
            .Value(row.TotalSizeMb)           /* total_size_mb INTEGER */
            .Value(row.SingleUsePlans)        /* single_use_plans INTEGER */
            .Value(row.SingleUseSizeMb)       /* single_use_size_mb INTEGER */
            .Value(row.MultiUsePlans)         /* multi_use_plans INTEGER */
            .Value(row.MultiUseSizeMb)        /* multi_use_size_mb INTEGER */
            .Value(row.AvgUseCount)           /* avg_use_count DECIMAL(38,2) */
            .Value(row.AvgSizeKb)             /* avg_size_kb INTEGER */
            .Value(row.OldestPlanCreateTime); /* oldest_plan_create_time TIMESTAMP (nullable) */
    }
}
