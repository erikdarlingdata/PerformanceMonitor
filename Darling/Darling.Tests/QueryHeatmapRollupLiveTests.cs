/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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
/// #8570: the query heat map reads whole hours from the hourly rollup at a bin width of 60 minutes or more, and the raw rows for the hours the
/// rollup does not hold. The raw read is the oracle: with one sample per query per hour the two routes must give the same cells.
/// </summary>
public sealed class QueryHeatmapRollupLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8570 heat map tests.";
    private const int ServerId = -857101;
    private const string ServerName = "a8570-heatmap";

    /// <summary><c>End</c> is a whole hour three hours back: the rollup is refreshed up to it, so the hour that starts at <c>End</c> is closed but not built.</summary>
    private sealed record Timeline(DateTime End)
    {
        public DateTime Utc(DateTime naive) => DateTime.SpecifyKind(naive, DateTimeKind.Utc);
    }

    private static Timeline NewTimeline()
    {
        /* The reply tests send the window's end as as_of, and the product refuses an as_of after its own clock. A pinned test clock ahead of the
           real clock (the 23:30 and 00:30 passes) would put that end in the future, so the timeline stands on the earlier of the two clocks. */
        var now = LiveClock.Now();
        var real = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        if (real < now)
        {
            now = real;
        }

        return new Timeline(new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-3));
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
            Assert.SkipWhen(!timescaleEnabled, "The live #8570 heat map tests need TimescaleDB.");
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

    private static async Task RefreshAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// <paramref name="hashes"/> queries, each with <paramref name="samplesPerHour"/> samples in every hour of [from, to). Query k runs 1 + k times in a
    /// sample (so the most-executed query of a cell is the highest k in it, with no ties) and takes 2 * 10^(k mod 4) * (1 + 2 * (hour mod 5)) ms an execution, so the
    /// queries land in different magnitude buckets and move between them from hour to hour. The text of a query never changes.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime from, DateTime to, int hashes, int samplesPerHour, CancellationToken ct)
    {
        var step = 40 / samplesPerHour;
        await using var command = new NpgsqlCommand(
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, query_text,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              SELECT $5::bigint + row_number() OVER (), x.h + ((10 + s * $6::int) * INTERVAL '1 minute'), $3, $4, 'Alpha',
                     '0xHM' || lpad(k::text, 3, '0'), '0xSH' || lpad(k::text, 3, '0'), 'SELECT ' || k,
                     1 + k,
                     (1 + k) * (pow(10, k % 4)::bigint * (1 + 2 * (floor(extract(epoch FROM x.h) / 3600)::bigint % 5)) * 1000),
                     (1 + k) * (2 * pow(10, k % 4)::bigint * (1 + 2 * (floor(extract(epoch FROM x.h) / 3600)::bigint % 5)) * 1000),
                     100, 1000, 3600
              FROM generate_series($1::timestamp, $2::timestamp - INTERVAL '1 hour', INTERVAL '1 hour') AS x(h)
              CROSS JOIN generate_series(0, $7::int - 1) AS k
              CROSS JOIN generate_series(0, $8::int - 1) AS s", connection);
        command.CommandTimeout = 300;
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 1_000_000_000L);
        command.Parameters.AddWithValue(step);
        command.Parameters.AddWithValue(hashes);
        command.Parameters.AddWithValue(samplesPerHour);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task InsertAsync(NpgsqlConnection connection, DateTime at, string hash, long executions, long elapsedUs, CancellationToken ct)
        => DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, query_text,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, 'Alpha', $5, $6, $7, $8, $9, $9, 100, 1000, 3600)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, hash, "0xSH" + hash, "SELECT " + hash, executions, elapsedUs);

    private static async Task<(NpgsqlDataSource Pg, RollupCoverage Coverage)> OpenAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var pg = NpgsqlDataSource.Create(scratch.ConnectionString);
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        return (pg, coverage);
    }

    private static Task<List<DarlingQueryHeatmapReader.HeatmapCellRow>> RawAsync(NpgsqlDataSource pg, HeatmapMetric metric, DateTime start, DateTime end, int bin, CancellationToken ct)
        => DarlingQueryHeatmapReader.GetQueryHeatmapAsync(pg, ServerId, metric, start, end, DatabaseFilter.One(null), bin, 5000, 80, ct);

    private static string AsOf(DateTime naive) => DateTime.SpecifyKind(naive, DateTimeKind.Utc).ToString("o");

    private static JsonElement Root(string json)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    /// <summary>One sample per query per hour: the rollup's hour and the raw sample are the same figure, so both routes must give the same cell for cell
    /// at 60, 120 and 240 minute bins, for both metrics the rollup carries. The window starts and ends inside an hour, so both edges read raw.</summary>
    [Fact]
    public async Task RollupRoute_GivesTheRawCells_AtEveryWholeHourBin()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, t.End.AddHours(-30), t.End.AddHours(1), hashes: 9, samplesPerHour: 1, ct);
            await RefreshAsync(connection, t.End.AddHours(-31), t.End, ct);
            var (pg, coverage) = await OpenAsync(scratch, ct);
            await using var _ = pg;
            var start = t.End.AddHours(-27).AddMinutes(5);
            var end = t.End.AddMinutes(30);

            foreach (var metric in new[] { HeatmapMetric.Duration, HeatmapMetric.Cpu })
            {
                foreach (var bin in new[] { 60, 120, 240 })
                {
                    var routed = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
                        pg, coverage, ServerId, metric, start, end, DatabaseFilter.One(null), bin, 5000, 80, ct);
                    var raw = await RawAsync(pg, metric, start, end, bin, ct);

                    Assert.Equal(DarlingQueryHeatmapReader.SourceHourlyRollup, routed.Source);
                    Assert.NotNull(routed.Rollup);
                    Assert.True(raw.Count > 10, "the seed should fill many cells");
                    Assert.True(raw.Select(r => r.BucketIndex).Distinct().Count() >= 3, "the seed should reach several magnitude buckets");
                    Assert.Equal(raw, routed.Rows);
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The hour that starts at the rollup's ceiling is closed but not built, and the hour the window ends in is still open: both come from the raw rows, and
    /// the rollup serves no hour from its ceiling on.</summary>
    [Fact]
    public async Task HoursPastTheRollup_AndTheStillOpenHour_AreServedRaw()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, t.End.AddHours(-6), t.End, hashes: 3, samplesPerHour: 1, ct);
            await RefreshAsync(connection, t.End.AddHours(-7), t.End, ct);

            /* Both of these arrive after the refresh: the rollup holds neither. PAST is in the hour that starts at the ceiling, OPEN in the hour the window ends in. */
            await InsertAsync(connection, t.End.AddMinutes(10), "PAST", 1, 5_000, ct);
            await InsertAsync(connection, t.End.AddHours(1).AddMinutes(10), "OPEN", 1, 5_000, ct);
            var (pg, coverage) = await OpenAsync(scratch, ct);
            await using var _ = pg;
            var start = t.End.AddHours(-5);
            var end = t.End.AddHours(1).AddMinutes(30);

            var routed = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
                pg, coverage, ServerId, HeatmapMetric.Duration, start, end, DatabaseFilter.One(null), 60, 5000, 80, ct);

            Assert.Equal(DarlingQueryHeatmapReader.SourceHourlyRollup, routed.Source);
            Assert.Equal(t.End, routed.Rollup!.ToUtc);
            Assert.Equal(start, routed.Rollup.FromUtc);
            var past = routed.Rows.Single(r => r.TopQueryHash == "PAST");
            Assert.Equal(t.End, past.TimeBucket);
            Assert.Equal("SELECT PAST", past.TopQueryText);
            var open = routed.Rows.Single(r => r.TopQueryHash == "OPEN");
            Assert.Equal(t.End.AddHours(1), open.TimeBucket);
            Assert.Equal(await RawAsync(pg, HeatmapMetric.Duration, start, end, 60, ct), routed.Rows);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A bin of 70 minutes is no multiple of an hour, so a bin edge falls inside some hours. Each rollup hour still counts once, in the bin that
    /// holds its start: one query per hour, so the cells' query counts add up to the hours, and each bin holds exactly the hours that start in it.</summary>
    [Fact]
    public async Task RollupRoute_AtA70MinuteBin_CountsEveryHourOnce_InTheBinHoldingItsStart()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            const int hours = 12;
            const int bin = 70;
            var first = t.End.AddHours(-hours);
            for (var i = 0; i < hours; i++)
            {
                await InsertAsync(connection, first.AddHours(i).AddMinutes(10), "H" + i.ToString("00"), 1, 5_000, ct);
            }

            await RefreshAsync(connection, first.AddHours(-1), t.End, ct);
            var (pg, coverage) = await OpenAsync(scratch, ct);
            await using var _ = pg;

            var routed = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
                pg, coverage, ServerId, HeatmapMetric.Duration, first, t.End.AddMinutes(30), DatabaseFilter.One(null), bin, 5000, 80, ct);

            Assert.Equal(DarlingQueryHeatmapReader.SourceHourlyRollup, routed.Source);
            Assert.Equal(first, routed.Rollup!.FromUtc);
            Assert.Equal(t.End, routed.Rollup.ToUtc);

            /* The bin an hour belongs to: the 70-minute step from 1970-01-01 00:00 that holds the hour's start. */
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
            var expected = Enumerable.Range(0, hours)
                .Select(i => first.AddHours(i))
                .GroupBy(h => epoch.AddMinutes(Math.Floor((h - epoch).TotalMinutes / bin) * bin))
                .ToDictionary(g => g.Key, g => (long)g.Count());
            Assert.Contains(1L, expected.Values);
            Assert.Contains(2L, expected.Values);

            var actual = routed.Rows
                .GroupBy(r => r.TimeBucket)
                .ToDictionary(g => g.Key, g => g.Sum(r => r.QueryCount));
            Assert.Equal(expected.OrderBy(e => e.Key).ToArray(), actual.OrderBy(e => e.Key).ToArray());
            Assert.Equal(hours, routed.Rows.Sum(r => r.QueryCount));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A bin under 60 minutes reads raw whatever the rollup holds, and so do the metrics the rollup cannot rebuild. Two samples in one hour land in two 30 minute bins.</summary>
    [Fact]
    public async Task NarrowBins_AndOtherMetrics_ReadRaw_AndTheReplyNamesTheSource()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, t.End.AddHours(-6), t.End, hashes: 3, samplesPerHour: 1, ct);
            var hour = t.End.AddHours(-3);
            await InsertAsync(connection, hour.AddMinutes(40), "HALF", 1000, 500_000_000, ct);
            await InsertAsync(connection, hour.AddMinutes(20), "HALF", 1000, 5_000_000, ct);
            await RefreshAsync(connection, t.End.AddHours(-7), t.End, ct);
            var (pg, coverage) = await OpenAsync(scratch, ct);
            await using var _ = pg;
            var start = t.End.AddHours(-5);
            var end = t.End.AddMinutes(30);

            var narrow = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
                pg, coverage, ServerId, HeatmapMetric.Duration, start, end, DatabaseFilter.One(null), 30, 5000, 80, ct);
            Assert.Equal(DarlingQueryHeatmapReader.SourceRaw, narrow.Source);
            Assert.Null(narrow.Rollup);
            Assert.Equal(new[] { hour, hour.AddMinutes(30) }, narrow.Rows.Where(r => r.TopQueryHash == "HALF").Select(r => r.TimeBucket).OrderBy(x => x).ToArray());

            foreach (var metric in new[] { HeatmapMetric.LogicalReads, HeatmapMetric.LogicalWrites, HeatmapMetric.ExecutionCount })
            {
                var other = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
                    pg, coverage, ServerId, metric, start, end, DatabaseFilter.One(null), 60, 5000, 80, ct);
                Assert.Equal(DarlingQueryHeatmapReader.SourceRaw, other.Source);
            }

            /* The reply names the source: raw for 5 minute bins, the hourly rollup (with its span and the precision note) for 60. */
            var rawReply = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(pg, ServerName, 5, null, null, 30, 500, AsOf(end)));
            Assert.Equal("raw", rawReply.GetProperty("source").GetString());
            Assert.Equal(JsonValueKind.Null, rawReply.GetProperty("precision_note").ValueKind);
            Assert.Equal(JsonValueKind.Null, rawReply.GetProperty("rollup_from").ValueKind);

            var rollupReply = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(pg, ServerName, 5, null, null, 60, 500, AsOf(end)));
            Assert.Equal("hourly_rollup", rollupReply.GetProperty("source").GetString());
            Assert.Equal(t.Utc(t.End.AddHours(-4)).ToString("o"), rollupReply.GetProperty("rollup_from").GetString());
            Assert.Equal(t.Utc(t.End).ToString("o"), rollupReply.GetProperty("rollup_to").GetString());
            Assert.Equal(DarlingMcpQueryHeatmapTools.RollupPrecisionNote, rollupReply.GetProperty("precision_note").GetString());
            Assert.True(rollupReply.GetProperty("cell_count").GetInt32() > 0);

            var readsReply = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(pg, ServerName, 5, "logical_reads", null, 60, 500, AsOf(end)));
            Assert.Equal("raw", readsReply.GetProperty("source").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A store whose rollup holds nothing reads raw at every width.</summary>
    [Fact]
    public async Task UnbuiltRollup_ReadsRaw()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, t.End.AddHours(-6), t.End, hashes: 3, samplesPerHour: 1, ct);
            var (pg, coverage) = await OpenAsync(scratch, ct);
            await using var _ = pg;
            var routed = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
                pg, coverage, ServerId, HeatmapMetric.Duration, t.End.AddHours(-5), t.End.AddMinutes(30), DatabaseFilter.One(null), 60, 5000, 80, ct);
            Assert.Equal(DarlingQueryHeatmapReader.SourceRaw, routed.Source);
            Assert.NotEmpty(routed.Rows);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A seven day read at six hour bins touches fewer data pages than the raw read does. The seed is a 12-sample hour of 60 queries, so the raw read
    /// walks twelve times the rows the rollup holds. The pages the winners' text lookups read are counted too (the rollup statement carries them).</summary>
    [Fact]
    public async Task SevenDayRead_TouchesFewerPagesThanTheRawRead()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, t.End.AddHours(-170), t.End, hashes: 60, samplesPerHour: 12, ct);
            await RefreshAsync(connection, t.End.AddHours(-171), t.End, ct);
            await Exec(connection, "ANALYZE collect.query_stats", ct);
            await Exec(connection, $"ANALYZE collect.{TimescaleSupport.QueryStatsIntervalHourlyView}", ct);
            var (pg, coverage) = await OpenAsync(scratch, ct);
            await using var _ = pg;
            var start = t.End.AddHours(-168);
            var end = t.End.AddMinutes(30);
            var plan = DarlingQueryHeatmapReader.PlanRollup(coverage, HeatmapMetric.Duration, 360, start, end);
            Assert.NotNull(plan);

            var rollupPages = await PagesAsync(pg, DarlingQueryHeatmapReader.BuildQueryHeatmapRollupSql(HeatmapMetric.Duration, plan!.HourlyRelation), start, end, 360, plan, ct);
            var rawPages = await PagesAsync(pg, DarlingQueryHeatmapReader.BuildQueryHeatmapSql(HeatmapMetric.Duration), start, end, 360, null, ct);

            /* The same cells come back, on top of fewer pages. */
            var routed = await DarlingQueryHeatmapReader.GetQueryHeatmapRoutedAsync(
                pg, coverage, ServerId, HeatmapMetric.Duration, start, end, DatabaseFilter.One(null), 360, 5000, 80, ct);
            Assert.Equal(DarlingQueryHeatmapReader.SourceHourlyRollup, routed.Source);
            Assert.NotEmpty(routed.Rows);
            Assert.True(rollupPages * 3 < rawPages, $"rollup read {rollupPages} data pages, raw read {rawPages}");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<long> PagesAsync(
        NpgsqlDataSource pg, string sql, DateTime start, DateTime end, int bin, DarlingQueryHeatmapReader.HeatmapRollupPlan? plan, CancellationToken ct)
    {
        await using var command = pg.CreateCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        DarlingMcpReadParameters.AddWindow(command, ServerId, start, end);
        command.Parameters.Add(DatabaseFilter.One(null).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bin });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 5000 });
        if (plan is not null)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.FromUtc, NpgsqlDbType = NpgsqlDbType.Timestamp });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.ToUtc, NpgsqlDbType = NpgsqlDbType.Timestamp });
        }

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        return ExplainBuffers.DataPages(doc.RootElement[0].GetProperty("Plan").Clone());
    }
}
