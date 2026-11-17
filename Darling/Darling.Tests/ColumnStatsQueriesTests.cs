/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>#8798: the column_stats T-SQL builders. Pure text; the live behaviour is exercised against a real server elsewhere.</summary>
public sealed class ColumnStatsQueriesTests
{
    private static readonly IReadOnlyList<(int ObjectId, int StatsId)> OneStat = new[] { (101, 2) };

    [Fact]
    public void EveryBatchSetsLockTimeout5000()
    {
        Assert.Contains("SET LOCK_TIMEOUT 5000;", ColumnStatsQueries.CandidatesSql);
        Assert.Contains("SET LOCK_TIMEOUT 5000;", ColumnStatsQueries.BuildReadBatch(OneStat, true));
        Assert.Contains("SET LOCK_TIMEOUT 5000;", ColumnStatsQueries.BuildReadBatch(OneStat, false));
        Assert.DoesNotContain("READ UNCOMMITTED", ColumnStatsQueries.CandidatesSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("READ UNCOMMITTED", ColumnStatsQueries.BuildReadBatch(OneStat, true), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NeverRequestsTheHistogram()
    {
        Assert.DoesNotContain("HISTOGRAM", ColumnStatsQueries.CandidatesSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("HISTOGRAM", ColumnStatsQueries.HasSelectSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("HISTOGRAM", ColumnStatsQueries.BuildReadBatch(OneStat, true), StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("HISTOGRAM", ColumnStatsQueries.BuildReadBatch(OneStat, false), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CandidatesExcludeFilteredMemOptXmlSpatialColumnstoreNeverUpdated()
    {
        var sql = ColumnStatsQueries.CandidatesSql;

        Assert.Contains("has_filter = 0", sql);
        Assert.Contains("is_memory_optimized = 0", sql);
        Assert.Contains("type IN (3, 4, 5, 6)", sql);
        Assert.Contains("STATS_DATE(", sql);
        Assert.Contains("stats_date IS NOT NULL", sql);
        Assert.Contains("is_ms_shipped = 0", sql);
        Assert.Contains("TOP (" + ColumnStatsModel.MaxStatisticsPerDatabase + ")", sql);
        foreach (var reason in new[] { "filtered", "memory_optimized", "xml_spatial_columnstore", "never_updated", "over_cap" })
        {
            Assert.Contains(reason, sql);
        }
    }

    [Fact]
    public void HasSelectSql_IsThePermissionProbe()
    {
        Assert.Equal("SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'SELECT')", ColumnStatsQueries.HasSelectSql);
    }

    [Fact]
    public void HeaderTempTable_MatchesBuild()
    {
        var with = ColumnStatsQueries.BuildReadBatch(OneStat, true);
        var without = ColumnStatsQueries.BuildReadBatch(OneStat, false);

        Assert.Contains("[Persisted Sample Percent]", with);
        Assert.DoesNotContain("Persisted Sample Percent", without);
        Assert.Contains("[Unfiltered Rows]", with);
        Assert.Contains("[Unfiltered Rows]", without);
    }

    [Fact]
    public void QuotesApostrophesInNames()
    {
        var batch = ColumnStatsQueries.BuildReadBatch(OneStat, true);

        Assert.Contains("QUOTENAME(SCHEMA_NAME(o.schema_id))", batch);
        Assert.Contains("QUOTENAME(o.name)", batch);
        Assert.Contains("@sname = s.name", batch);
        Assert.Contains("REPLACE(@sname, N'''', N'''''')", batch);
        Assert.Contains("REPLACE(@target, N'''', N'''''')", batch);
    }

    [Fact]
    public void NoFilterExpressionColumnSelectedOut()
    {
        var batch = ColumnStatsQueries.BuildReadBatch(OneStat, true);

        Assert.Contains("[Filter Expression]", batch);
        var selectStart = batch.IndexOf("SELECT object_id, stats_id, [Updated]", StringComparison.Ordinal);
        Assert.True(selectStart >= 0, "header result set select not found");
        var selectEnd = batch.IndexOf(';', selectStart);
        Assert.DoesNotContain("Filter Expression", batch.Substring(selectStart, selectEnd - selectStart));
    }

    [Fact]
    public void BatchReturnsHeaderDensityAndErrorSets_AndSurfaces213()
    {
        var batch = ColumnStatsQueries.BuildReadBatch(OneStat, false);

        Assert.Contains("FROM #hdr", batch);
        Assert.Contains("FROM #dv", batch);
        Assert.Contains("FROM #errs", batch);
        Assert.Contains("ERROR_NUMBER()", batch);
        Assert.Contains("= 213", batch);
        Assert.Contains("(101, 2)", batch);
    }

    [Fact]
    public void CatalogLookupIsInsideTheTry()
    {
        var batch = ColumnStatsQueries.BuildReadBatch(OneStat, true);

        var tryAt = batch.IndexOf("BEGIN TRY", StringComparison.Ordinal);
        var lookupAt = batch.IndexOf("@sname = s.name", StringComparison.Ordinal);
        var catchAt = batch.IndexOf("BEGIN CATCH", StringComparison.Ordinal);
        Assert.True(tryAt >= 0 && lookupAt > tryAt && lookupAt < catchAt, "the sys.objects/sys.stats lookup must sit between BEGIN TRY and BEGIN CATCH");
    }

    [Fact]
    public void EmptyStatList_StillBuilds()
    {
        Assert.Contains("FROM #errs", ColumnStatsQueries.BuildReadBatch(Array.Empty<(int ObjectId, int StatsId)>(), true));
    }

    [Theory]
    [InlineData(13, 0, 4445, false)]
    [InlineData(13, 0, 4446, true)]
    [InlineData(13, 0, 5000, true)]
    [InlineData(13, 0, 2000, false)]
    [InlineData(12, 0, 6000, false)]
    [InlineData(14, 0, 3005, false)]
    [InlineData(14, 0, 3006, true)]
    [InlineData(14, 0, 1000, false)]
    [InlineData(15, 0, 2000, true)]
    [InlineData(16, 0, 1000, true)]
    [InlineData(17, 0, 1, true)]
    public void HeaderHasPersistedSamplePercent_Pins(int major, int minor, int build, bool expected)
    {
        Assert.Equal(expected, ColumnStatsQueries.HeaderHasPersistedSamplePercent(major, minor, build));
    }
}
