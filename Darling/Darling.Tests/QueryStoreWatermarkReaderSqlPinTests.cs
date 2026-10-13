/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Text pins for the stitched Query Store watermark read (#5684). The live oracle proves the behavior; these keep the
/// invariants that make a value too HIGH impossible from being edited away: strict floor in every arm, the side arm's order, the
/// verification's full key, a distinct marker per statement so the seed-read counters count one seed once.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStoreWatermarkReaderSqlPinTests
{
    private static readonly string[] AllStatements =
    [
        QueryStoreWatermarkReader.SideSql,
        QueryStoreWatermarkReader.VerifySql,
        QueryStoreWatermarkReader.PreCoverageSql,
    ];

    [Fact]
    public void EveryArm_UsesAStrictFloor_AndNoneUsesAnInclusiveBound()
    {
        foreach (var sql in AllStatements)
        {
            Assert.DoesNotContain(">=", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("<=", sql, StringComparison.Ordinal);
        }

        Assert.Contains("collection_time > $3", QueryStoreWatermarkReader.SideSql, StringComparison.Ordinal);
        Assert.Contains("collection_time > $3 AND collection_time < $4", QueryStoreWatermarkReader.PreCoverageSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSideArm_OrdersLikeTheRawRead_AndSkipsNullValues()
    {
        Assert.Contains("AND batch_max IS NOT NULL ORDER BY batch_max DESC, collection_time DESC LIMIT 1", QueryStoreWatermarkReader.SideSql, StringComparison.Ordinal);
        Assert.StartsWith("/* pm:qs-watermark-seed */", QueryStoreWatermarkReader.SideSql, StringComparison.Ordinal);
        /* #5707: the marker has ONE definition (QueryStoreWatermarkReader.SeedMarker); the runner builds its raw read from it. */
        Assert.StartsWith(QueryStoreWatermarkReader.SeedMarker, DarlingCollectorRunner.BuildServerWatermarkWithWitnessForDatabaseSql("t", "c", "d"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheVerification_IsAFullKeyPointLookup_OnTheRawTable()
    {
        var sql = QueryStoreWatermarkReader.VerifySql;
        foreach (var column in new[] { "server_id = $1", "database_name = $2", "query_id = $3", "plan_id = $4", "collection_time = $5", "last_execution_time = $6" })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        Assert.Contains("FROM collect.query_store_stats", sql, StringComparison.Ordinal);
        Assert.EndsWith("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EachStatement_HasItsOwnMarker_SoOnlyTheSideArmCountsAsTheSeedRead()
    {
        var markers = new[]
        {
            QueryStoreWatermarkReader.SeedMarker, QueryStoreWatermarkReader.VerifyMarker,
            QueryStoreWatermarkReader.PreCoverageMarker, QueryStoreWatermarkReader.FallbackMarker,
            QueryStoreWatermarkReader.EmptySideMarker,
        };
        Assert.Equal(markers.Length, new HashSet<string>(markers).Count);

        /* The counters match on a substring of the seed marker's text: no other statement may carry it. */
        Assert.DoesNotContain("pm:qs-watermark-seed", QueryStoreWatermarkReader.PreCoverageSql, StringComparison.Ordinal);
        Assert.DoesNotContain("pm:qs-watermark-seed", QueryStoreWatermarkReader.VerifySql, StringComparison.Ordinal);
        Assert.DoesNotContain("pm:qs-watermark-seed", QueryStoreWatermarkReader.FallbackMarker, StringComparison.Ordinal);
        Assert.DoesNotContain("pm:qs-watermark-seed", QueryStoreWatermarkReader.EmptySideMarker, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePreCoverageArm_IsTheRawReadWithTheCoverageBoundAdded()
    {
        var raw = DarlingCollectorRunner.BuildServerWatermarkWithWitnessForDatabaseSql("collect.query_store_stats", "last_execution_time", "database_name")
            .Replace(QueryStoreWatermarkReader.SeedMarker, QueryStoreWatermarkReader.PreCoverageMarker, StringComparison.Ordinal)
            .Replace("collection_time > $3 ", "collection_time > $3 AND collection_time < $4 ", StringComparison.Ordinal);
        Assert.Equal(QueryStoreWatermarkReader.PreCoverageSql, raw);
    }
}
