/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the raw Query Store reads place their window by the interval's start and bound <c>collection_time</c> above it by
/// <see cref="QueryStoreIntervalWide.CollectionCeiling"/>, the longest a row's collection trails its interval's start. The bound is
/// chunk exclusion for a historical window, so it must cover the longest real lag: one interval plus the collector's catch-up cap.
/// The unit pins hold the bound to the collector's own limits and keep its SQL text out of the edge-margin rewrite; the live test
/// puts a row collected at the longest lag, and one collected past the ceiling, through every raw read.
/// </summary>
/* #1776 own-store: the live test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and
   then works entirely inside it, the same shape as QueryStoreAbortsWindowPlacementLiveTests. */
public sealed class QueryStoreCollectionCeilingTests
{
    private const int Server = 1;
    private const long Inside = 1;
    private const long LongestLag = 2;
    private const long PastTheCeiling = 3;

    private static readonly DateTime Day = new(2026, 10, 14, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = Day.AddHours(12);
    private static readonly DateTime WindowEnd = Day.AddHours(16);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Every raw read that carries the ceiling, by name.</summary>
    public static TheoryData<string> RawReads() => new()
    {
        nameof(DarlingDataReader.QueryStoreTopSql),
        nameof(DarlingDataReader.QueryStoreWindowProbeRawRowsSql),
        nameof(DarlingQueryStoreAbortsReader.RawSql),
        "Viewer." + nameof(ViewerDataService.QueryStoreTopSql),
        "Viewer." + nameof(ViewerDataService.QueryStoreSlicerSql),
    };

    private static string RawSql(string name) => name switch
    {
        nameof(DarlingDataReader.QueryStoreTopSql) => DarlingDataReader.QueryStoreTopSql,
        nameof(DarlingDataReader.QueryStoreWindowProbeRawRowsSql) => DarlingDataReader.QueryStoreWindowProbeRawRowsSql,
        nameof(DarlingQueryStoreAbortsReader.RawSql) => DarlingQueryStoreAbortsReader.RawSql,
        "Viewer." + nameof(ViewerDataService.QueryStoreTopSql) => ViewerDataService.QueryStoreTopSql,
        "Viewer." + nameof(ViewerDataService.QueryStoreSlicerSql) => ViewerDataService.QueryStoreSlicerSql,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, null),
    };

    [Fact]
    public void CollectionCeiling_CoversAnIntervalPlusTheCatchupCap()
    {
        /* A live row is collected less than MaxCatchup after its interval ends, and a backfill slice, no wider than
           MaxSliceSpan, stamps its ceiling as collection_time for intervals ending after its floor. An interval spans at
           most IntervalSpanMargin, so the lag from interval start to collection is under the margin plus the larger cap. */
        Assert.True(QueryStoreBackfillState.MaxSliceSpan <= WatermarkPolicy.MaxCatchup,
            "a backfill slice wider than the catch-up cap can stamp a collection_time later than the ceiling allows");
        Assert.True(QueryStoreIntervalWide.CollectionCeiling >= QueryStoreIntervalWide.IntervalSpanMargin + WatermarkPolicy.MaxCatchup);
        Assert.True(QueryStoreIntervalWide.CollectionCeiling >= QueryStoreIntervalWide.IntervalSpanMargin + QueryStoreBackfillState.MaxSliceSpan);
        Assert.True(QueryStoreIntervalWide.CollectionCeiling <= QueryStoreIntervalWide.IntervalSpanMargin + WatermarkPolicy.MaxCatchup + TimeSpan.FromHours(1),
            "a ceiling past the longest lag plus its hour of slack reads chunks no row of the window can be in");
    }

    [Theory]
    [MemberData(nameof(RawReads))]
    public void RawRead_IsLeftAloneByTheEdgeMarginRewrite(string name)
    {
        var sql = RawSql(name);
        Assert.Equal(sql, QueryStoreIntervalWide.WithEdgeMargin(sql, "interval '0 minutes'"));
    }

    [Fact]
    public async Task ARowCollectedAtTheLongestLag_IsReadByEveryRawRead()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, ct);
        }

        var expected = new[] { Inside, LongestLag };

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var top = await DarlingDataReader.GetQueryStoreTopAsync(postgres, Server, WindowStart, WindowEnd, 20, DatabaseFilter.All, null, null, ct);
        Assert.Equal(expected, top.Select(r => r.QueryId).OrderBy(i => i).ToArray());

        var aborts = await DarlingQueryStoreAbortsReader.ReadAsync(postgres, Server, WindowStart, WindowEnd, 20, DatabaseFilter.All, ct);
        Assert.Null(aborts.Table);   /* a 4 hour window reads the raw tier */
        Assert.Equal(expected, aborts.Rows.Select(r => r.QueryId).OrderBy(i => i).ToArray());

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreTopQueriesAsync(Server, WindowStart, WindowEnd, cancellationToken: ct);
        Assert.Equal(expected, grid.Select(r => r.QueryId).OrderBy(i => i).ToArray());

        var slicer = await viewer.GetQueryStoreSlicerDataAsync(Server, WindowStart, WindowEnd, null, ct);
        Assert.Equal(expected.Length, slicer.Sum(b => b.SessionCount));
    }

    /// <summary>One interval plainly inside the window; a day-long interval starting at the window's end whose row is collected a
    /// minute short of the longest lag the collector produces, its interval's length plus the catch-up cap; and one starting there
    /// whose row is collected a minute past the ceiling, which no raw read reaches.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var longestLag = QueryStoreIntervalWide.IntervalSpanMargin + WatermarkPolicy.MaxCatchup - TimeSpan.FromMinutes(1);
        (long Query, DateTime Started, DateTime Collected)[] rows =
        [
            (Inside, Day.AddHours(13), Day.AddHours(14).AddMinutes(5)),
            (LongestLag, WindowEnd, WindowEnd + longestLag),
            (PastTheCeiling, WindowEnd, WindowEnd + QueryStoreIntervalWide.CollectionCeiling + TimeSpan.FromMinutes(1)),
        ];
        foreach (var (q, started, collected) in rows)
        {
            await using var command = new NpgsqlCommand($"""
                INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
                VALUES ({q}, timestamp '{collected:yyyy-MM-dd HH:mm}', {Server}, 'srv1', 'db0', {q}, {q}, 'Aborted',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', 'mod', md5('h{q}'), 4, 500, 300, 900, 700, NULL, {q},
                        timestamp '{started:yyyy-MM-dd HH:mm}')
                """, connection);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
