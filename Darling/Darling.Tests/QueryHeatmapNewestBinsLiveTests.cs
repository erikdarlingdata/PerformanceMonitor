/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */

/// <summary>
/// #9965: get_query_heatmap keeps the newest cells, so it reads the newest bins first and reads the whole window only when those answer too few
/// cells. The whole-window read (<c>boundTheRead: false</c>) is the oracle: the two replies must be the same text.
/// </summary>
public sealed class QueryHeatmapNewestBinsLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9965 heat map tests.";
    private const int ServerId = -996501;
    private const string ServerName = "a9965-heatmap";
    private const int Bin = 5;

    private static DateTime NewEnd()
    {
        /* A whole hour three hours back: in the past for as_of, and aligned to every bin width these tests use. */
        var now = LiveClock.Now();
        var real = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        if (real < now)
        {
            now = real;
        }

        return new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-3);
    }

    private sealed record Store(ScratchPostgres Scratch, NpgsqlConnection Connection);

    private static async Task<Store> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The live #9965 heat map tests need TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            return new Store(scratch, connection);
        }
        catch
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// <paramref name="hashes"/> queries in every <paramref name="stepMinutes"/> step of [from, to), one sample a step, one minute into the step. Query k runs 1 + k
    /// times (the most-executed query of a cell is the highest k in it, with no ties) at 2 * 10^(k mod 4) ms an execution, so the queries land in four magnitude
    /// buckets and a step holds four cells.
    /// <para>The rows go in ordered by time, the order a collector writes them in, so a chunk's pages hold them in time order and the newest bins sit on its last
    /// few pages (#9965). Without the ORDER BY the planner chooses which series is the outer loop, and with the query series outside each query's rows fill their own
    /// pages: every row of the newest bins is then on a different page from the row before it, and an index scan counts a page for each row.</para>
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime from, DateTime to, int stepMinutes, int hashes, string database, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, query_text,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              SELECT $5::bigint + row_number() OVER (ORDER BY x.t, k), x.t + INTERVAL '1 minute', $3, $4, $9,
                     '0xHM' || lpad(k::text, 3, '0'), '0xSH' || lpad(k::text, 3, '0'), 'SELECT ' || k,
                     1 + k,
                     (1 + k) * (2 * pow(10, k % 4)::bigint * 1000),
                     (1 + k) * (2 * pow(10, k % 4)::bigint * 1000),
                     100, 1000, 300
              FROM generate_series($1::timestamp, $2::timestamp - ($6::int * INTERVAL '1 minute'), $6::int * INTERVAL '1 minute') AS x(t)
              CROSS JOIN generate_series(0, $7::int - 1) AS k
              ORDER BY x.t, k", connection);
        command.CommandTimeout = 300;
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 1_000_000_000L);
        command.Parameters.AddWithValue(stepMinutes);
        command.Parameters.AddWithValue(hashes);
        command.Parameters.AddWithValue(0);
        command.Parameters.AddWithValue(database);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteAfterAsync(NpgsqlConnection connection, DateTime after, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE server_id = $1 AND collection_time > $2", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(after, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static string AsOf(DateTime naive) => DateTime.SpecifyKind(naive, DateTimeKind.Utc).ToString("o");

    private static Task<string> ToolAsync(NpgsqlDataSource pg, DateTime end, int hours, int bin, int limit, DatabaseFilter databases, bool bound, CancellationToken ct)
        => DarlingMcpQueryHeatmapTools.GetQueryHeatmap(pg, ServerName, hours, null, databases, bin, limit, AsOf(end), false, null, bound, ct);

    private static async Task<(DarlingQueryHeatmapReader.HeatmapRead Read, long Pages, long WholePages)> RoutedAsync(
        NpgsqlDataSource pg, RollupCoverage coverage, DateTime start, DateTime end, int bin, int cap, CancellationToken ct)
    {
        var read = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
            pg, coverage, ServerId, HeatmapMetric.Duration, start, end, DatabaseFilter.One(null), bin, cap, 80, ct);
        var sql = DarlingQueryHeatmapReader.BuildQueryHeatmapSql(HeatmapMetric.Duration);
        var pages = await PagesAsync(pg, sql, read.ReadFromUtc == start ? start : read.ReadFromUtc.AddTicks(-10), end, bin, cap, ct);
        var wholePages = await PagesAsync(pg, sql, start, end, bin, cap, ct);
        return (read, pages, wholePages);
    }

    private static async Task<long> PagesAsync(NpgsqlDataSource pg, string sql, DateTime start, DateTime end, int bin, int cap, CancellationToken ct)
    {
        await using var command = pg.CreateCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        DarlingMcpReadParameters.AddWindow(command, ServerId, start, end);
        command.Parameters.Add(DatabaseFilter.One(null).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bin });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = cap });
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        return ExplainBuffers.DataPages(doc.RootElement[0].GetProperty("Plan").Clone());
    }

    private static JsonElement Root(string json)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private static async Task RunAsync(Func<Store, DateTime, CancellationToken, Task> body, CancellationToken ct)
    {
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await body(store, NewEnd(), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Every bin filled: the newest 41 bins answer the cap, so one statement runs, over the slice only, and the reply is the whole-window reply.</summary>
    [Fact]
    public Task EveryBinFilled_ReadsOnlyTheNewestBins_AndTheReplyIsTheWholeWindowReply() => RunAsync(async (store, end, ct) =>
    {
        await SeedAsync(store.Connection, end.AddHours(-167), end, Bin, hashes: 8, "Alpha", ct);
        await Exec(store.Connection, "ANALYZE collect.query_stats", ct);
        var pg = NpgsqlDataSource.Create(store.Scratch.ConnectionString);
        await using var _ = pg;
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var start = end.AddHours(-168);

        var routed = await RoutedAsync(pg, coverage, start, end, Bin, cap: 41, ct);
        Assert.Equal(1, routed.Read.Statements);
        Assert.Equal(end.AddMinutes(-200), routed.Read.ReadFromUtc);
        Assert.Equal(41, routed.Read.Rows.Count);
        Assert.True(routed.Pages * 10 < routed.WholePages, $"the slice read {routed.Pages} data pages, the whole window {routed.WholePages}");

        var bounded = await ToolAsync(pg, end, 168, Bin, 40, DatabaseFilter.One(null), bound: true, ct);
        var whole = await ToolAsync(pg, end, 168, Bin, 40, DatabaseFilter.One(null), bound: false, ct);
        Assert.Equal(whole, bounded);
        var reply = Root(bounded);
        Assert.True(reply.GetProperty("truncated").GetBoolean());
        Assert.True(reply.GetProperty("cell_count").GetInt32() > 0);
    }, TestContext.Current.CancellationToken);

    /// <summary>Empty bins fill the newest of the slice: it answers fewer cells than the cap, the read falls back to the whole window, and the reply is the whole-window reply.</summary>
    [Fact]
    public Task EmptyBinsInTheSlice_FallBackToTheWholeWindow_WithTheSameReply() => RunAsync(async (store, end, ct) =>
    {
        await SeedAsync(store.Connection, end.AddHours(-167), end, Bin, hashes: 8, "Alpha", ct);
        await DeleteAfterAsync(store.Connection, end.AddMinutes(-190), ct);
        var pg = NpgsqlDataSource.Create(store.Scratch.ConnectionString);
        await using var _ = pg;
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var start = end.AddHours(-168);

        var routed = await RoutedAsync(pg, coverage, start, end, Bin, cap: 41, ct);
        Assert.Equal(2, routed.Read.Statements);
        Assert.Equal(start, routed.Read.ReadFromUtc);
        Assert.Equal(41, routed.Read.Rows.Count);

        var bounded = await ToolAsync(pg, end, 168, Bin, 40, DatabaseFilter.One(null), bound: true, ct);
        var whole = await ToolAsync(pg, end, 168, Bin, 40, DatabaseFilter.One(null), bound: false, ct);
        Assert.Equal(whole, bounded);
        Assert.True(Root(bounded).GetProperty("cell_count").GetInt32() > 0);
    }, TestContext.Current.CancellationToken);

    /// <summary>A database filter that matches only old bins leaves the slice empty: the read falls back and the filtered reply is the whole-window reply.</summary>
    [Fact]
    public Task ADatabaseFilter_MatchingOnlyOldBins_FallsBack_WithTheSameReply() => RunAsync(async (store, end, ct) =>
    {
        await SeedAsync(store.Connection, end.AddHours(-167), end, Bin, hashes: 8, "Alpha", ct);
        await SeedAsync(store.Connection, end.AddHours(-30), end.AddHours(-20), Bin, hashes: 8, "Beta", ct);
        var pg = NpgsqlDataSource.Create(store.Scratch.ConnectionString);
        await using var _ = pg;
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var beta = DatabaseFilter.One("Beta");

        var read = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
            pg, coverage, ServerId, HeatmapMetric.Duration, end.AddHours(-168), end, beta, Bin, 41, 80, ct);
        Assert.Equal(2, read.Statements);
        Assert.Equal(41, read.Rows.Count);

        var bounded = await ToolAsync(pg, end, 168, Bin, 40, beta, bound: true, ct);
        var whole = await ToolAsync(pg, end, 168, Bin, 40, beta, bound: false, ct);
        Assert.Equal(whole, bounded);
    }, TestContext.Current.CancellationToken);

    /// <summary>The hourly rollup route at 60-minute bins: every bin filled reads one slice statement, and the reply is the whole-window reply.</summary>
    [Fact]
    public Task RollupRoute_EveryBinFilled_ReadsOneSlice_AndTheReplyIsTheWholeWindowReply() => RunAsync(async (store, end, ct) =>
    {
        await SeedAsync(store.Connection, end.AddHours(-60), end.AddHours(1), 60, hashes: 8, "Alpha", ct);
        await RefreshAsync(store.Connection, end.AddHours(-61), end, ct);
        var pg = NpgsqlDataSource.Create(store.Scratch.ConnectionString);
        await using var _ = pg;
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var windowEnd = end.AddMinutes(30);

        var read = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
            pg, coverage, ServerId, HeatmapMetric.Duration, windowEnd.AddHours(-48), windowEnd, DatabaseFilter.One(null), 60, 21, 80, ct);
        Assert.Equal(DarlingQueryHeatmapReader.SourceHourlyRollup, read.Source);
        Assert.Equal(1, read.Statements);
        Assert.Equal(end.AddHours(-20), read.ReadFromUtc);

        var bounded = await ToolAsync(pg, windowEnd, 48, 60, 20, DatabaseFilter.One(null), bound: true, ct);
        var whole = await ToolAsync(pg, windowEnd, 48, 60, 20, DatabaseFilter.One(null), bound: false, ct);
        Assert.Equal(whole, bounded);
        var reply = Root(bounded);
        Assert.Equal(DarlingQueryHeatmapReader.SourceHourlyRollup, reply.GetProperty("source").GetString());
        Assert.True(reply.GetProperty("truncated").GetBoolean());
    }, TestContext.Current.CancellationToken);

    /// <summary>The rollup route with empty hours in the slice: the read falls back to the whole window and the reply is the whole-window reply.</summary>
    [Fact]
    public Task RollupRoute_EmptyBinsInTheSlice_FallBack_WithTheSameReply() => RunAsync(async (store, end, ct) =>
    {
        await SeedAsync(store.Connection, end.AddHours(-60), end.AddHours(1), 60, hashes: 8, "Alpha", ct);
        await DeleteAfterAsync(store.Connection, end.AddHours(-19), ct);
        await RefreshAsync(store.Connection, end.AddHours(-61), end, ct);
        var pg = NpgsqlDataSource.Create(store.Scratch.ConnectionString);
        await using var _ = pg;
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var windowEnd = end.AddMinutes(30);

        var read = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
            pg, coverage, ServerId, HeatmapMetric.Duration, windowEnd.AddHours(-48), windowEnd, DatabaseFilter.One(null), 60, 21, 80, ct);
        Assert.Equal(DarlingQueryHeatmapReader.SourceHourlyRollup, read.Source);
        Assert.Equal(2, read.Statements);

        var bounded = await ToolAsync(pg, windowEnd, 48, 60, 20, DatabaseFilter.One(null), bound: true, ct);
        var whole = await ToolAsync(pg, windowEnd, 48, 60, 20, DatabaseFilter.One(null), bound: false, ct);
        Assert.Equal(whole, bounded);
        Assert.True(Root(bounded).GetProperty("cell_count").GetInt32() > 0);
    }, TestContext.Current.CancellationToken);

    /// <summary>Five bins of four cells (20 cells). A cap of exactly 20 is not truncated, a cap of 19 is, with the partial oldest column dropped, and each reply is the whole-window reply.</summary>
    [Fact]
    public Task TheCapEdge_ExactlyLimitCells_IsNotTruncated_AndOneMoreIs() => RunAsync(async (store, end, ct) =>
    {
        await SeedAsync(store.Connection, end.AddMinutes(-25), end, Bin, hashes: 4, "Alpha", ct);
        var pg = NpgsqlDataSource.Create(store.Scratch.ConnectionString);
        await using var _ = pg;

        foreach (var limit in new[] { 20, 19, 21 })
        {
            var bounded = await ToolAsync(pg, end, 168, Bin, limit, DatabaseFilter.One(null), bound: true, ct);
            var whole = await ToolAsync(pg, end, 168, Bin, limit, DatabaseFilter.One(null), bound: false, ct);
            Assert.Equal(whole, bounded);
        }

        var exact = Root(await ToolAsync(pg, end, 168, Bin, 20, DatabaseFilter.One(null), bound: true, ct));
        Assert.False(exact.GetProperty("truncated").GetBoolean());
        Assert.Equal(20, exact.GetProperty("cell_count").GetInt32());

        var over = Root(await ToolAsync(pg, end, 168, Bin, 19, DatabaseFilter.One(null), bound: true, ct));
        Assert.True(over.GetProperty("truncated").GetBoolean());
        Assert.Equal(16, over.GetProperty("cell_count").GetInt32());
    }, TestContext.Current.CancellationToken);

    /// <summary>A window that starts inside the newest bins is read whole, in one statement, with no slice.</summary>
    [Fact]
    public Task AWindowStartingInsideTheSlice_IsReadWhole_InOneStatement() => RunAsync(async (store, end, ct) =>
    {
        await SeedAsync(store.Connection, end.AddHours(-3), end, Bin, hashes: 8, "Alpha", ct);
        var pg = NpgsqlDataSource.Create(store.Scratch.ConnectionString);
        await using var _ = pg;
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var start = end.AddHours(-2);

        var read = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
            pg, coverage, ServerId, HeatmapMetric.Duration, start, end, DatabaseFilter.One(null), Bin, 500, 80, ct);
        Assert.Equal(1, read.Statements);
        Assert.Equal(start, read.ReadFromUtc);

        Assert.Equal(
            await ToolAsync(pg, end, 2, Bin, 499, DatabaseFilter.One(null), bound: false, ct),
            await ToolAsync(pg, end, 2, Bin, 499, DatabaseFilter.One(null), bound: true, ct));
    }, TestContext.Current.CancellationToken);

    /// <summary>The slice boundary sits on the bin edge of the epoch origin, for a bin width that does not divide the hour.</summary>
    [Theory]
    [InlineData(5)]
    [InlineData(7)]
    [InlineData(70)]
    [InlineData(1440)]
    public void TheSliceFloor_IsABinEdgeOnTheEpochOrigin(int bin)
    {
        var end = new DateTime(2026, 11, 27, 13, 17, 41, DateTimeKind.Unspecified);
        var start = end.AddDays(-400);
        var floor = DarlingQueryHeatmapReader.NewestBinsFloor(start, end, bin, 30);
        Assert.NotNull(floor);
        var sinceEpoch = (floor!.Value - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified)).Ticks;
        Assert.Equal(0, sinceEpoch % TimeSpan.FromMinutes(bin).Ticks);
        Assert.True(floor.Value <= end);
        Assert.Equal(29, (long)Math.Floor((end - floor.Value).TotalMinutes / bin));

        Assert.Null(DarlingQueryHeatmapReader.NewestBinsFloor(end.AddMinutes(-bin * 3), end, bin, 30));
        Assert.Null(DarlingQueryHeatmapReader.NewestBinsFloor(start, end, bin, 0));
    }
}
