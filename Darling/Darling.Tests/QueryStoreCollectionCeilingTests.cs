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
/// chunk exclusion for a historical window, so it must cover the longest real lag: one interval, the collector's catch-up cap and
/// its longest replica look-back. The unit pin holds the bound to the collector's own limits; the live test puts rows collected at
/// the longest lags, and one collected past the ceiling, through every raw read, the inline-text lookup and the existence probe.
/// </summary>
/* #1776 own-store: the live test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and
   then works entirely inside it, the same shape as QueryStoreAbortsWindowPlacementLiveTests. */
public sealed class QueryStoreCollectionCeilingTests
{
    private const int Server = 1;
    private const int OtherServer = 2;
    private const long Inside = 1;
    private const long SlowFlushLookBack = 2;
    private const long LongestLag = 3;
    private const long PastTheCeiling = 4;
    private const long OtherServerPastTheCeiling = 5;

    private static readonly DateTime Day = new(2026, 10, 14, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = Day.AddHours(12);
    private static readonly DateTime WindowEnd = Day.AddHours(16);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void CollectionCeiling_CoversAnIntervalTheCatchupCapAndTheLongestLookBack()
    {
        /* A live read keeps intervals ending after its cutoff, which is the clamped watermark, or W - L when it looks back and
           the clamp did not fire, so a live row is collected less than MaxCatchup plus L after its interval ends, and LookBackFor
           holds L within MaxLookBack. A backfill slice, no wider than MaxSliceSpan, stamps its ceiling as collection_time for
           intervals ending after its floor. An interval spans at most IntervalSpanMargin. */
        var longestLag = QueryStoreIntervalWide.IntervalSpanMargin + WatermarkPolicy.MaxCatchup + QueryStoreReplicaRouting.MaxLookBack;
        Assert.True(QueryStoreBackfillState.MaxSliceSpan <= WatermarkPolicy.MaxCatchup,
            "a backfill slice wider than the catch-up cap can stamp a collection_time later than the ceiling allows");
        Assert.True(QueryStoreReplicaRouting.LookBackFor(int.MaxValue, hasCapturingSecondary: true) <= QueryStoreReplicaRouting.MaxLookBack,
            "a look-back wider than MaxLookBack collects rows later than the ceiling allows");
        Assert.True(QueryStoreIntervalWide.CollectionCeiling >= longestLag);
        Assert.True(QueryStoreIntervalWide.CollectionCeiling < longestLag + TimeSpan.FromHours(1),
            "the ceiling is the longest lag rounded up to whole hours; past that it reads chunks no row of the window can be in");
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

        var expected = new[] { Inside, SlowFlushLookBack, LongestLag };

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var top = await DarlingDataReader.GetQueryStoreTopAsync(postgres, Server, WindowStart, WindowEnd, 20, DatabaseFilter.All, null, null, ct);
        Assert.Equal(expected, top.Select(r => r.QueryId).OrderBy(i => i).ToArray());

        var aborts = await DarlingQueryStoreAbortsReader.ReadAsync(postgres, Server, WindowStart, WindowEnd, 20, DatabaseFilter.All, ct);
        Assert.Null(aborts.Table);   /* a 4 hour window reads the raw tier */
        Assert.Equal(expected, aborts.Rows.Select(r => r.QueryId).OrderBy(i => i).ToArray());

        /* The empty-answer probe over a window that holds only the lagged intervals' starts: true on this server through the rows
           inside the ceiling, false on a server whose one row there is collected past it. */
        Assert.True(await DarlingDataReader.QueryStoreWindowHasRowsAsync(postgres, Server, WindowEnd.AddMinutes(-1), WindowEnd, DatabaseFilter.All, ct));
        Assert.False(await DarlingDataReader.QueryStoreWindowHasRowsAsync(postgres, OtherServer, WindowEnd.AddMinutes(-1), WindowEnd, DatabaseFilter.All, ct));

        /* The seed has no query_store_text rows, so each grid row's text comes from the inline-text lookup over its own row. */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreTopQueriesAsync(Server, WindowStart, WindowEnd, cancellationToken: ct);
        Assert.Equal(expected, grid.Select(r => r.QueryId).OrderBy(i => i).ToArray());
        Assert.All(grid, r => Assert.Equal(TextOf(r.QueryId), r.QueryText));

        var slicer = await viewer.GetQueryStoreSlicerDataAsync(Server, WindowStart, WindowEnd, null, ct);
        Assert.Equal(expected.Length, slicer.Sum(b => b.SessionCount));
    }

    private static string TextOf(long query) => $"SELECT {query} AS ceiling_probe";

    /// <summary>One interval plainly inside the window; day-long intervals starting in the window's last minute whose rows are collected
    /// at an interval's length plus the catch-up cap plus the look-back of an hour-long flush, and at the longest lag the collector
    /// produces (the same with <see cref="QueryStoreReplicaRouting.MaxLookBack"/>); one starting there whose row is collected a minute
    /// past the ceiling, which no raw read reaches; and the same on another server, whose only row it is.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var lagged = QueryStoreIntervalWide.IntervalSpanMargin + WatermarkPolicy.MaxCatchup;
        var lastStart = WindowEnd.AddMinutes(-1);
        var pastTheCeiling = WindowEnd + QueryStoreIntervalWide.CollectionCeiling + TimeSpan.FromMinutes(1);
        (int Server, long Query, DateTime Started, DateTime Collected)[] rows =
        [
            (Server, Inside, Day.AddHours(13), Day.AddHours(14).AddMinutes(5)),
            (Server, SlowFlushLookBack, lastStart, lastStart + lagged + QueryStoreReplicaRouting.LookBackFor(3600, hasCapturingSecondary: false)),
            (Server, LongestLag, lastStart, lastStart + lagged + QueryStoreReplicaRouting.MaxLookBack),
            (Server, PastTheCeiling, lastStart, pastTheCeiling),
            (OtherServer, OtherServerPastTheCeiling, lastStart, pastTheCeiling),
        ];
        foreach (var (server, q, started, collected) in rows)
        {
            await using var command = new NpgsqlCommand($"""
                INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                    execution_type_desc, first_execution_time, module_name, query_hash, query_text, execution_count, avg_duration_us,
                    avg_cpu_time_us, max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
                VALUES ({q}, timestamp '{collected:yyyy-MM-dd HH:mm}', {server}, 'srv{server}', 'db0', {q}, {q}, 'Aborted',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', 'mod', md5('h{q}'), '{TextOf(q)}', 4, 500, 300, 900, 700, NULL, {q},
                        timestamp '{started:yyyy-MM-dd HH:mm}')
                """, connection);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
