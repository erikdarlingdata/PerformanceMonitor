/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1319 global database filter (Darling). Pins that every database-scoped reader's SQL carries the
/// always-present, PARAMETERIZED <c>= ANY($n)</c> predicate (guarded by <c>$n::text[] IS NULL</c> so an
/// empty selection short-circuits to unfiltered) at the expected positional index, and that the shared
/// <see cref="ViewerDataService.DatabaseFilterParameter"/> binds a Postgres <c>text[]</c> (SQL NULL for
/// "All", the array otherwise). No live Postgres needed — the SQL is a compile-time contract.
/// </summary>
public sealed class DatabaseFilterTests
{
    [Fact]
    public void DatabaseFilterParameter_NullSelection_BindsSqlNullTypedAsTextArray()
    {
        var p = ViewerDataService.DatabaseFilterParameter(null);

        Assert.Equal(NpgsqlDbType.Array | NpgsqlDbType.Text, p.NpgsqlDbType);
        Assert.IsType<DBNull>(p.Value);
    }

    [Fact]
    public void DatabaseFilterParameter_EmptySelection_BindsSqlNullTypedAsTextArray()
    {
        var p = ViewerDataService.DatabaseFilterParameter(new List<string>());

        Assert.Equal(NpgsqlDbType.Array | NpgsqlDbType.Text, p.NpgsqlDbType);
        Assert.IsType<DBNull>(p.Value);
    }

    [Fact]
    public void DatabaseFilterParameter_NonEmptySelection_BindsTheDatabasesAsTextArray()
    {
        var p = ViewerDataService.DatabaseFilterParameter(new[] { "AppDB1", "AppDB2" });

        Assert.Equal(NpgsqlDbType.Array | NpgsqlDbType.Text, p.NpgsqlDbType);
        var arr = Assert.IsType<string[]>(p.Value);
        Assert.Equal(new[] { "AppDB1", "AppDB2" }, arr);
    }

    [Fact]
    public void DatabaseFilterClause_EmitsGuardedParameterizedAnyPredicate()
    {
        Assert.Equal(
            " AND ($4::text[] IS NULL OR database_name = ANY($4))",
            ViewerDataService.DatabaseFilterClause("database_name", 4));
    }

    // Each touched reader's SQL const must carry the guarded, parameterized ANY predicate at the index
    // one past its previous highest positional parameter. (data, columnQualifier, index)
    [Theory]
    // Query stats: window ($1-$3) + top ($4) → $5; comparison ($1-$5) → $6; slicer ($1-$3) → $4.
    [InlineData(nameof(ViewerDataService.TopQueriesSql), "database_name", 5)]
    [InlineData(nameof(ViewerDataService.QueryStatsComparisonSql), "database_name", 6)]
    [InlineData(nameof(ViewerDataService.QueryStatsSlicerSql), "database_name", 4)]
    // Procedure stats.
    [InlineData(nameof(ViewerDataService.TopProceduresSql), "database_name", 5)]
    [InlineData(nameof(ViewerDataService.ProcedureStatsComparisonSql), "database_name", 6)]
    [InlineData(nameof(ViewerDataService.ProcStatsSlicerSql), "database_name", 4)]
    // Query Store.
    [InlineData(nameof(ViewerDataService.QueryStoreTopSql), "database_name", 5)]
    [InlineData(nameof(ViewerDataService.QueryStoreComparisonSql), "database_name", 6)]
    [InlineData(nameof(ViewerDataService.QueryStoreSlicerSql), "database_name", 4)]
    // Active-query snapshots.
    [InlineData(nameof(ViewerDataService.LatestQuerySnapshotsSql), "database_name", 4)]
    [InlineData(nameof(ViewerDataService.ActiveQuerySlicerSql), "database_name", 4)]
    // Blocking.
    [InlineData(nameof(ViewerDataService.BlockedProcessReportsSql), "database_name", 4)]
    [InlineData(nameof(ViewerDataService.DmvBlockingSnapshotsSql), "database_name", 4)]
    [InlineData(nameof(ViewerDataService.BlockingSlicerSql), "database_name", 4)]
    [InlineData(nameof(ViewerDataService.BlockingTrendSql), "database_name", 4)]
    [InlineData(nameof(ViewerDataService.BlockedSessionTrendSql), "database_name", 4)]
    // Config + config changes: server_id only ($1) → $2; config changes ($1-$2) → $3.
    [InlineData(nameof(ViewerDataService.DatabaseConfigSql), "database_name", 2)]
    [InlineData(nameof(ViewerDataService.DatabaseScopedConfigSql), "database_name", 2)]
    [InlineData(nameof(ViewerDataService.DatabaseConfigChangesSnapshotsSql), "database_name", 3)]
    // Query trends (all window-only → $4).
    [InlineData(nameof(ViewerDataService.QueryDurationTrendSql), "database_name", 4)]
    [InlineData(nameof(ViewerDataService.ProcedureDurationTrendSql), "database_name", 4)]
    [InlineData(nameof(ViewerDataService.QueryStoreDurationTrendSql), "database_name", 4)]
    // The #2736 rollup-routed Query Store trend adds the raw boundary as $4, so its filter is $5.
    [InlineData(nameof(ViewerDataService.QueryStoreDurationTrendRollupSql), "database_name", 5)]
    [InlineData(nameof(ViewerDataService.ExecutionCountTrendSql), "database_name", 4)]
    // Default Trace (real column, qualified alias).
    [InlineData(nameof(ViewerDataService.DefaultTraceEventsByWindowSql), "dte.database_name", 4)]
    public void TouchedReaderSql_CarriesGuardedParameterizedAnyPredicate(string sqlFieldName, string column, int index)
    {
        var sql = GetPublicSqlField(sqlFieldName);

        Assert.Contains($"{column} = ANY(${index})", sql, StringComparison.Ordinal);
        Assert.Contains($"${index}::text[] IS NULL", sql, StringComparison.Ordinal);
    }

    private static string GetPublicSqlField(string name)
    {
        var field = typeof(ViewerDataService).GetField(
            name,
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        Assert.NotNull(field);
        return (string)field!.GetValue(null)!;
    }
}
