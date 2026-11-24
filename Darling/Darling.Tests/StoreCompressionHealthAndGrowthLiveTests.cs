/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9428 and #9429 against a real store. <c>get_store_compression_health</c> lists a hypertable with a chunk past its compression
/// delay and counts it, then shows the horizon and the job's last success after a run, and answers <c>not_collected</c> on a store
/// without TimescaleDB. <c>get_store_host</c>'s growth forecast reads the stored size history: seven days give the rate and the
/// days figure, a short history gives null.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class StoreCompressionHealthAndGrowthLiveTests
{
    private const string Table = "ch9428_tick";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task CompressionHealth_ListsTheHypertableWithAChunkPastItsDelay_ThenShowsTheHorizonAfterARun()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #9428 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecAsync(connection, $"CREATE TABLE collect.{Table} (collection_time timestamp NOT NULL, server_id integer NOT NULL, value bigint)", ct);
        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql($"collect.{Table}", "collection_time"), ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql($"collect.{Table}"), ct);

        /* The policy is created and parked in one transaction so the scheduler never sees it armed and cannot compress the chunk
           before the first read. run_job still runs a parked job. */
        await using (var tx = await connection.BeginTransactionAsync(ct))
        {
            await using (var create = new NpgsqlCommand(TimescaleSupport.AddCompressionPolicySql($"collect.{Table}"), connection, tx))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            await using (var park = new NpgsqlCommand(PolicyJobSql("SELECT alter_job(job_id, scheduled => false, next_start => 'infinity'::timestamptz)"), connection, tx))
            {
                await park.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
        }

        /* Midday five days back: its chunk is a whole day past the policy's one-day delay however close to midnight the test runs. */
        await ExecAsync(connection,
            $"INSERT INTO collect.{Table} SELECT date_trunc('day', now()::timestamp) - INTERVAL '5 days' + INTERVAL '12 hours' + (g || ' seconds')::interval, 1, g FROM generate_series(1, 200) g", ct);

        DateTime chunkRangeEnd;
        await using (var range = new NpgsqlCommand(
            $"SELECT range_end FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = '{Table}'", connection))
        {
            chunkRangeEnd = ((DateTime)(await range.ExecuteScalarAsync(ct))!).ToUniversalTime();
        }

        using (var before = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, ct)))
        {
            var row = Row(before);
            Assert.Equal(1, row.GetProperty("eligible_uncompressed_chunks").GetInt32());
            Assert.Equal(chunkRangeEnd, ParseUtc(row.GetProperty("oldest_eligible_range_end").GetString()));
            Assert.Equal(JsonValueKind.Null, row.GetProperty("newest_compressed_range_end").ValueKind);
            Assert.Equal(1d, row.GetProperty("compress_after_days").GetDouble());
            Assert.True(row.GetProperty("job").GetProperty("last_error_readable").GetBoolean(), "the owner role can read job errors");

            /* The parked job's next start is infinity and its last success is -infinity: both read as null, not as a date in year 1. */
            Assert.Equal(JsonValueKind.Null, row.GetProperty("job").GetProperty("next_start").ValueKind);
            Assert.Equal(JsonValueKind.Null, row.GetProperty("job").GetProperty("last_successful_finish").ValueKind);
            Assert.True(before.RootElement.GetProperty("summary").GetProperty("hypertables_with_backlog").GetInt32() >= 1);
            Assert.True(before.RootElement.GetProperty("summary").GetProperty("backlog_chunks").GetInt64() >= 1);
        }

        int jobId;
        await using (var probe = new NpgsqlCommand(PolicyJobSql("SELECT job_id"), connection))
        {
            jobId = Convert.ToInt32((await probe.ExecuteScalarAsync(ct))!, CultureInfo.InvariantCulture);
        }

        await using (var run = new NpgsqlCommand("CALL run_job($1::integer)", connection))
        {
            run.Parameters.AddWithValue(jobId);
            await run.ExecuteNonQueryAsync(ct);
        }

        using (var after = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, ct)))
        {
            var row = Row(after);
            Assert.Equal(0, row.GetProperty("eligible_uncompressed_chunks").GetInt32());
            Assert.Equal(JsonValueKind.Null, row.GetProperty("oldest_eligible_range_end").ValueKind);
            Assert.Equal(chunkRangeEnd, ParseUtc(row.GetProperty("newest_compressed_range_end").GetString()));
        }

        /* The error statement itself runs against this TimescaleDB's job_errors view. No failed run exists to return a row, so it
           returns none, and a view or column that this version lacks would throw here. */
        await using (var errors = new NpgsqlCommand(DarlingStoreCompressionHealthReader.LastErrorSql, connection))
        {
            errors.Parameters.AddWithValue(new[] { -1 });
            await using var reader = await errors.ExecuteReaderAsync(ct);
            Assert.False(await reader.ReadAsync(ct));
        }

        JsonElement Row(JsonDocument doc) => doc.RootElement.GetProperty("hypertables").EnumerateArray()
            .Single(h => string.Equals(h.GetProperty("hypertable").GetString(), Table, StringComparison.Ordinal));
    }

    [Fact]
    public async Task CompressionHealth_OnAStoreWithoutTimescaleDb_AnswersNotCollected_NotEmpty()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9428 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, ct));
        Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task StoreHost_GrowthForecast_ReadsTheStoredSizeHistory_AndAShortHistoryReadsNull()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9429 live tests.");
        var ct = TestContext.Current.CancellationToken;

        const long DailyGrowth = 1_000_000_000L;
        /* The data volume is read off the managed data directory's disk, so the directory must exist. */
        var dataDirectory = System.IO.Directory.CreateTempSubdirectory("darling-9429-");
        var config = new PostgresConfig { Managed = true, DataDirectory = dataDirectory.FullName };

        /* A floor of 0.001 percent keeps the headroom positive on any disk that is not completely full, so the days figure is the
           rate's own answer and never the "already at the floor" zero. */
        const double Floor = 0.001d;

        await using (var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct))
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* Eight daily points, today minus 8 through today minus 1, each a day apart and 1 GB larger: seven full days of growth. */
            for (var daysAgo = 8; daysAgo >= 1; daysAgo--)
            {
                await SeedStoreSizeAsync(connection, daysAgo, 50_000_000_000L + (8 - daysAgo) * DailyGrowth, ct);
            }

            using var full = JsonDocument.Parse(await DarlingMcpStoreHostTools.GetStoreHost(
                dataSource, config, new StoreHostProfileCache(TimeSpan.FromMinutes(1)), null, Floor, ct));
            var forecast = full.RootElement.GetProperty("growth_forecast");
            Assert.Equal(7, forecast.GetProperty("growth_days_in_window").GetInt32());
            Assert.Equal((double)DailyGrowth, forecast.GetProperty("growth_bytes_per_day_7d").GetDouble());
            var headroom = forecast.GetProperty("headroom_bytes").GetInt64();
            Assert.True(headroom > 0, "the test disk must have free space above a 0.001 percent floor");
            Assert.Equal(Math.Round(headroom / (double)DailyGrowth, 1), forecast.GetProperty("days_until_free_floor").GetDouble());
            Assert.Equal(JsonValueKind.Null, forecast.GetProperty("growth_note").ValueKind);
        }

        await using (var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct))
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* Four days of history: three full days of growth, short of the seven the rate needs. */
            for (var daysAgo = 4; daysAgo >= 1; daysAgo--)
            {
                await SeedStoreSizeAsync(connection, daysAgo, 50_000_000_000L + (4 - daysAgo) * DailyGrowth, ct);
            }

            using var shortHistory = JsonDocument.Parse(await DarlingMcpStoreHostTools.GetStoreHost(
                dataSource, config, new StoreHostProfileCache(TimeSpan.FromMinutes(1)), null, Floor, ct));
            var forecast = shortHistory.RootElement.GetProperty("growth_forecast");
            Assert.Equal(3, forecast.GetProperty("growth_days_in_window").GetInt32());
            Assert.Equal(JsonValueKind.Null, forecast.GetProperty("growth_bytes_per_day_7d").ValueKind);
            Assert.Equal(JsonValueKind.Null, forecast.GetProperty("days_until_free_floor").ValueKind);
            Assert.Equal(JsonValueKind.String, forecast.GetProperty("growth_note").ValueKind);
        }

        dataDirectory.Delete(true);
    }

    [Fact]
    public async Task StoreHost_RefusesAFloorOutsideZeroToOneHundred()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9429 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        foreach (var floor in new[] { 0d, 100d, -5d, double.NaN })
        {
            using var doc = JsonDocument.Parse(await DarlingMcpStoreHostTools.GetStoreHost(
                dataSource, new PostgresConfig { Managed = true }, new StoreHostProfileCache(TimeSpan.FromMinutes(1)), null, floor, ct));
            Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        }
    }

    /// <summary>The arithmetic on fixed volume figures, so no disk decides it.</summary>
    [Fact]
    public void ForecastDaysUntilFloor_DividesTheHeadroomByTheSevenDayRate_AndSaysWhyWhenItCannot()
    {
        var now = new DateTime(2026, 11, 24, 10, 0, 0, DateTimeKind.Utc);
        const long Tb = 1_000_000_000_000L;
        const long Gb = 1_000_000_000L;

        DarlingStoreMetricsReader.DailyGrowthPoint Day(int daysAgo, long delta, bool partial = false) =>
            new(now.Date.AddDays(-daysAgo), delta, null, Partial: partial);

        var sevenDays = Enumerable.Range(1, 7).Select(d => Day(d, 10 * Gb)).ToList();

        /* 1 TB disk, 400 GB free, floor 25 percent = 250 GB: 150 GB of headroom at 10 GB a day is 15 days. */
        var forecast = DarlingMcpStoreHostTools.ForecastDaysUntilFloor(sevenDays, now, Tb, 400 * Gb, true, true, 25d);
        Assert.Equal(15d, forecast.DaysUntilFloor);
        Assert.Equal(150 * Gb, forecast.HeadroomBytes);
        Assert.Equal(250 * Gb, forecast.FloorBytes);
        Assert.Equal(10d * Gb, forecast.GrowthBytesPerDay);
        Assert.Null(forecast.Note);

        /* Today's partial point is not a full day, and a day before the window is not in it. */
        var withExtras = sevenDays.Concat(new[] { Day(0, 900 * Gb, partial: true), Day(8, 900 * Gb) }).ToList();
        Assert.Equal(15d, DarlingMcpStoreHostTools.ForecastDaysUntilFloor(withExtras, now, Tb, 400 * Gb, true, true, 25d).DaysUntilFloor);

        /* Six days is a short history: no rate, no days, a note, and the day count. */
        var six = DarlingMcpStoreHostTools.ForecastDaysUntilFloor(sevenDays.Take(6).ToList(), now, Tb, 400 * Gb, true, true, 25d);
        Assert.Null(six.GrowthBytesPerDay);
        Assert.Null(six.DaysUntilFloor);
        Assert.NotNull(six.Note);
        Assert.Equal(6, six.DaysInWindow);

        /* A gap day inside the week leaves six points, so the week has no rate either. */
        Assert.Null(DarlingMcpStoreHostTools.ForecastDaysUntilFloor(sevenDays.Where(p => p.Day != now.Date.AddDays(-3)).ToList(), now, Tb, 400 * Gb, true, true, 25d).DaysUntilFloor);

        /* A store that shrank or stayed flat is not heading for the floor. */
        var flat = Enumerable.Range(1, 7).Select(d => Day(d, d == 7 ? 0 : d % 2 == 0 ? -5 * Gb : 5 * Gb)).ToList();
        var flatForecast = DarlingMcpStoreHostTools.ForecastDaysUntilFloor(flat, now, Tb, 400 * Gb, true, true, 25d);
        Assert.Null(flatForecast.DaysUntilFloor);
        Assert.NotNull(flatForecast.Note);

        /* Already at the floor: 0 days, with no history needed. */
        var atFloor = DarlingMcpStoreHostTools.ForecastDaysUntilFloor(Array.Empty<DarlingStoreMetricsReader.DailyGrowthPoint>(), now, Tb, 200 * Gb, true, true, 25d);
        Assert.Equal(0d, atFloor.DaysUntilFloor);

        /* A store the service does not manage has no days figure: its volume is the service's own disk. An unread volume has none either. */
        Assert.Null(DarlingMcpStoreHostTools.ForecastDaysUntilFloor(sevenDays, now, Tb, 400 * Gb, true, false, 25d).DaysUntilFloor);
        Assert.Null(DarlingMcpStoreHostTools.ForecastDaysUntilFloor(sevenDays, now, 0, 0, false, true, 25d).DaysUntilFloor);
    }

    private static string PolicyJobSql(string select) =>
        $"{select} FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = '{Table}' AND (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')";

    private static async Task SeedStoreSizeAsync(NpgsqlConnection connection, int daysAgo, long totalBytes, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes) "
            + "VALUES (date_trunc('day', now() AT TIME ZONE 'UTC') - make_interval(days => $1) + interval '12 hours', 'store_9429', 'store', $2)",
            connection);
        command.Parameters.AddWithValue(daysAgo);
        command.Parameters.AddWithValue(totalBytes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static DateTime ParseUtc(string? text) =>
        DateTime.SpecifyKind(DateTime.Parse(text!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind), DateTimeKind.Utc);
}
