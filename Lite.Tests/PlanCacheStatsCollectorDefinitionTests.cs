/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the cross-SKU parity contract of the shared plan_cache_stats collector definition (the
/// Dashboard-parity port of install/35_collect_plan_cache_stats.sql): verbatim query text, payload
/// column order, ReadAsync mapping (including the nullable oldest_plan_create_time), and the
/// point-in-time no-delta WritePayload. Available on every platform (sys.dm_exec_cached_plans).
/// </summary>
public sealed class PlanCacheStatsCollectorDefinitionTests
{
    private const string ExpectedQuery = @"
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

    [Fact]
    public void Query_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());
        Assert.Equal(ExpectedQuery, PlanCacheStatsCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(PlanCacheStatsCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(PlanCacheStatsCollector.Instance.WatermarkColumn);
        Assert.Equal("plan_cache_stats", PlanCacheStatsCollector.Instance.Name);
        Assert.Equal("plan_cache_stats", PlanCacheStatsCollector.Instance.TargetTable);
    }

    [Fact]
    public void AppliesTo_Everywhere_IncludingAzure()
    {
        Assert.True(PlanCacheStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(PlanCacheStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(PlanCacheStatsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_AreInAppendOrder()
    {
        var names = PlanCacheStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "cacheobjtype",
                "objtype",
                "total_plans",
                "total_size_mb",
                "single_use_plans",
                "single_use_size_mb",
                "multi_use_plans",
                "multi_use_size_mb",
                "avg_use_count",
                "avg_size_kb",
                "oldest_plan_create_time",
            },
            names);

        /* avg_use_count carries explicit precision so Darling's numeric(38,2) matches the Dashboard
           column; oldest_plan_create_time is a timestamp. */
        var avgUseCount = PlanCacheStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "avg_use_count");
        Assert.Equal(CollectorColumnType.Decimal, avgUseCount.Type);
        Assert.Equal(38, avgUseCount.Precision);
        Assert.Equal(2, avgUseCount.Scale);
        Assert.Equal(CollectorColumnType.Timestamp, PlanCacheStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "oldest_plan_create_time").Type);
    }

    [Fact]
    public async Task ReadAsync_MapsColumns_IncludingNullableOldestPlanCreateTime()
    {
        var oldest = new DateTime(2026, 6, 1, 8, 0, 0, DateTimeKind.Unspecified);

        using var reader = new FakeCollectorDataReader(
            new object[] { "Compiled Plan", "Adhoc", 1000, 50, 800, 40, 200, 10, 1.50m, 51, oldest },
            /* An empty query-stats DMV yields a NULL oldest_plan_create_time. */
            new object[] { "Compiled Plan", "Proc", 300, 120, 5, 2, 295, 118, 42.00m, 410, DBNull.Value });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await PlanCacheStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(
            new PlanCacheStatsCollector.Row("Compiled Plan", "Adhoc", 1000, 50, 800, 40, 200, 10, 1.50m, 51, oldest),
            rows[0]);
        Assert.Equal(
            new PlanCacheStatsCollector.Row("Compiled Plan", "Proc", 300, 120, 5, 2, 295, 118, 42.00m, 410, null),
            rows[1]);
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_PointInTime_NoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();
        var oldest = new DateTime(2026, 6, 1, 8, 0, 0, DateTimeKind.Unspecified);
        var row = new PlanCacheStatsCollector.Row("Compiled Plan", "Adhoc", 1000, 50, 800, 40, 200, 10, 1.50m, 51, oldest);

        PlanCacheStatsCollector.Instance.WritePayload(row, writer, context);

        Assert.Equal(
            new object?[] { "Compiled Plan", "Adhoc", 1000, 50, 800, 40, 200, 10, 1.50m, 51, oldest },
            writer.Values);

        /* Point-in-time snapshot — no delta calculator interaction at all. */
        Assert.Empty(deltas.Calls);
    }
}
