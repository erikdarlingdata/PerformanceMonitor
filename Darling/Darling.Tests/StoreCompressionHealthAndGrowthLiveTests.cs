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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
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

        /* A row from now sits in today's chunk, which is not yet past the one-day delay: it must not count as backlog. */
        await ExecAsync(connection, $"INSERT INTO collect.{Table} VALUES (now()::timestamp, 1, 0)", ct);

        DateTime chunkRangeEnd;
        await using (var range = new NpgsqlCommand(
            $"SELECT min(range_end) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = '{Table}'", connection))
        {
            chunkRangeEnd = ((DateTime)(await range.ExecuteScalarAsync(ct))!).ToUniversalTime();
        }

        using (var before = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, full_detail: true, cancellationToken: ct)))
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

            /* A parked job is paused, which is not a crash backoff, so it carries no backoff sentence. */
            Assert.Equal("paused", row.GetProperty("job").GetProperty("next_start_state").GetString());
            Assert.Equal(JsonValueKind.Null, row.GetProperty("job").GetProperty("next_start_note").ValueKind);
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

        using (var after = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, full_detail: true, cancellationToken: ct)))
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

        using var doc = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, full_detail: true, cancellationToken: ct));
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

        /* One clock for the seed and the reader: the days are seeded from this UTC day and the reader is told it is this instant, so a
           UTC midnight between the two reads cannot move the window off the seeded days. */
        var anchor = LiveClock.Now();

        await using (var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct))
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* Eight daily points, today minus 8 through today minus 1, each a day apart and 1 GB larger: seven full days of growth. */
            for (var daysAgo = 8; daysAgo >= 1; daysAgo--)
            {
                await SeedStoreSizeAsync(connection, anchor, daysAgo, 50_000_000_000L + (8 - daysAgo) * DailyGrowth, ct);
            }

            using var full = JsonDocument.Parse(await DarlingMcpStoreHostTools.GetStoreHostAt(
                dataSource, config, new StoreHostProfileCache(TimeSpan.FromMinutes(1)), null, Floor, anchor, ct));
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
                await SeedStoreSizeAsync(connection, anchor, daysAgo, 50_000_000_000L + (4 - daysAgo) * DailyGrowth, ct);
            }

            using var shortHistory = JsonDocument.Parse(await DarlingMcpStoreHostTools.GetStoreHostAt(
                dataSource, config, new StoreHostProfileCache(TimeSpan.FromMinutes(1)), null, Floor, anchor, ct));
            var forecast = shortHistory.RootElement.GetProperty("growth_forecast");
            Assert.Equal(3, forecast.GetProperty("growth_days_in_window").GetInt32());
            Assert.Equal(JsonValueKind.Null, forecast.GetProperty("growth_bytes_per_day_7d").ValueKind);
            Assert.Equal(JsonValueKind.Null, forecast.GetProperty("days_until_free_floor").ValueKind);
            Assert.Equal(JsonValueKind.String, forecast.GetProperty("growth_note").ValueKind);
        }

        dataDirectory.Delete(true);
    }

    [Fact]
    public async Task DailyPoints_ForOneKind_AreTheRowsTheAllKindsReadReturnsForThatKind()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9429 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var anchor = LiveClock.Now();
        for (var daysAgo = 3; daysAgo >= 1; daysAgo--)
        {
            await SeedStoreSizeAsync(connection, anchor, daysAgo, 50_000_000_000L + (3 - daysAgo) * 1_000_000_000L, ct);
            await ExecAsync(connection,
                $"INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes, size_estimated) VALUES "
                + $"(date_trunc('day', now()::timestamp) - INTERVAL '{daysAgo} days' + INTERVAL '13 hours', 'store_9429', 'store', 1, true), "
                + $"(date_trunc('day', now()::timestamp) - INTERVAL '{daysAgo} days' + INTERVAL '12 hours', 'tbl_9429', 'table', 7, false)", ct);
        }

        var since = anchor.AddDays(-5);
        var all = await DarlingStoreMetricsReader.GetDailyAsync(dataSource, since, ct);
        var onlyStore = await DarlingStoreMetricsReader.GetDailyForKindAsync(dataSource, since, "store", ct);

        Assert.Contains(all, p => p.ObjectKind == "table");
        Assert.NotEmpty(onlyStore);
        Assert.All(onlyStore, p => Assert.Equal("store", p.ObjectKind));
        Assert.Equal(
            all.Where(p => p.ObjectKind == "store").OrderBy(p => p.Day).ToList(),
            onlyStore.OrderBy(p => p.Day).ToList());
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

    [Fact]
    public async Task CompressionHealth_ShowsAFailedPolicyJobsError_ToItsOwner_AndOnlyTheStatusToARoleThatCannotReadErrors()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #9428 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var roleName = "ch9428_reader_" + Guid.NewGuid().ToString("N")[..8];
        var memberRoleName = "ch9428_member_" + Guid.NewGuid().ToString("N")[..8];
        const string RolePassword = "ch9428-test-pw";

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* A job the scheduler runs and that fails every time. The procedure's name carries "compression", which is how the read
           finds a compression policy job. The job's owner is the connected owner role. */
        await ExecAsync(connection, $"CREATE TABLE collect.{Table} (collection_time timestamp NOT NULL, server_id integer NOT NULL, value bigint)", ct);
        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql($"collect.{Table}", "collection_time"), ct);
        await ExecAsync(connection, "CREATE PROCEDURE collect.fail_compression_9428(job_id integer, config jsonb) LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'ch9428 boom'; END $$", ct);
        int jobId;
        await using (var add = new NpgsqlCommand("SELECT add_job('collect.fail_compression_9428', INTERVAL '1 hour', initial_start => now())", connection))
        {
            jobId = Convert.ToInt32((await add.ExecuteScalarAsync(ct))!, CultureInfo.InvariantCulture);
        }

        var failed = false;
        for (var i = 0; i < 60 && !failed; i++)
        {
            await using var status = new NpgsqlCommand("SELECT count(*) FROM timescaledb_information.job_stats WHERE job_id = $1 AND last_run_status = 'Failed'", connection);
            status.Parameters.AddWithValue(jobId);
            failed = Convert.ToInt32(await status.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0;
            if (!failed)
            {
                await Task.Delay(2000, ct);
            }
        }

        Assert.True(failed, "the scheduler is expected to run the failing job within two minutes");

        JsonElement Job(JsonDocument doc) => doc.RootElement.GetProperty("hypertables").EnumerateArray()
            .Select(h => h.GetProperty("job"))
            .Single(j => j.GetProperty("job_id").GetInt32() == jobId);

        using (var asOwner = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(owner, full_detail: true, cancellationToken: ct)))
        {
            var job = Job(asOwner);
            Assert.Equal("Failed", job.GetProperty("last_run_status").GetString());
            Assert.True(job.GetProperty("last_error_readable").GetBoolean());
            Assert.Equal("ch9428 boom", job.GetProperty("last_error").GetProperty("message").GetString());
            Assert.Equal("P0001", job.GetProperty("last_error").GetProperty("sqlstate").GetString());
            Assert.Equal(JsonValueKind.Null, asOwner.RootElement.GetProperty("errors_read_failure").ValueKind);
        }

        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(connection, statement, ct);
            }

            await using var asRole = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString);

            /* The store's mcp role is neither the job's owner nor in the database owner's role: the view would hide the row, so the
               read says so instead of reporting no error. The status still comes from job_stats, which every role can read. */
            using var viewed = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(asRole, full_detail: true, cancellationToken: ct));
            var job = Job(viewed);
            Assert.Equal("Failed", job.GetProperty("last_run_status").GetString());
            Assert.False(job.GetProperty("last_error_readable").GetBoolean());
            Assert.Equal(JsonValueKind.Null, job.GetProperty("last_error").ValueKind);
            Assert.Equal(JsonValueKind.Null, viewed.RootElement.GetProperty("errors_read_failure").ValueKind);

            /* A role that is a member of the job's owner, so the read asks for the job's error, but that holds no right to read the error
               view: NOINHERIT keeps the owner's own rights from reaching it, and the view's grant to everyone is withdrawn. The error
               statement is refused, and the tool says so instead of returning a Failed job with no error and no word why. */
            await ExecAsync(connection, $"CREATE ROLE {memberRoleName} LOGIN NOSUPERUSER NOINHERIT PASSWORD '{RolePassword}'", ct);
            string ownerRole;
            await using (var who = new NpgsqlCommand("SELECT current_user::text", connection))
            {
                ownerRole = (string)(await who.ExecuteScalarAsync(ct))!;
            }

            await ExecAsync(connection, $"GRANT \"{ownerRole}\" TO {memberRoleName}", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(memberRoleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(connection, statement, ct);
            }

            await ExecAsync(connection, $"REVOKE ALL ON timescaledb_information.job_errors FROM PUBLIC, {memberRoleName}", ct);

            await using var asMember = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = memberRoleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString);

            using var refused = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(asMember, full_detail: true, cancellationToken: ct));
            var refusedJob = Job(refused);
            Assert.Equal("Failed", refusedJob.GetProperty("last_run_status").GetString());
            Assert.True(refusedJob.GetProperty("last_error_readable").GetBoolean(), "a member of the job's owner is asked for the error");
            Assert.Equal(JsonValueKind.Null, refusedJob.GetProperty("last_error").ValueKind);
            Assert.Equal(JsonValueKind.String, refused.RootElement.GetProperty("errors_read_failure").ValueKind);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(connection, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName}; DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{memberRoleName}') THEN DROP OWNED BY {memberRoleName}; DROP ROLE {memberRoleName}; END IF; END $$;", CancellationToken.None));
        }
    }

    [Fact]
    public async Task CompressionHealth_OnATimescaleStoreWithNoCompressionPolicy_AnswersEmpty_NotNotCollected()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #9428 live tests.");
        var ct = TestContext.Current.CancellationToken;

        /* Unmigrated: the extension is on, and no policy job has been created. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = JsonDocument.Parse(await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, full_detail: true, cancellationToken: ct));
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task HourlyTick_WhenTheCompressionStatementFails_LogsAWarning_AndReadsNoPolicies()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9428 live tests.");
        var ct = TestContext.Current.CancellationToken;

        /* Without the extension the TimescaleDB views the statement reads do not exist, so it fails for real. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var logger = new CapturingTestLogger();
        var activity = await TimescaleSupport.ReadCompressionActivityAsync(connection, logger, ct);

        Assert.Empty(activity);
        Assert.True(logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning) == 1, "the failed read is logged once at warning level: " + logger.Joined);
    }

    /// <summary>What a null <c>next_start</c> means, by the job's status and the value the scheduler holds.</summary>
    [Theory]
    [InlineData("Scheduled", "infinity", "paused")]
    [InlineData("Paused", "infinity", "paused")]
    [InlineData("Paused", null, "paused")]
    [InlineData("Scheduled", "-infinity", "crash_backoff")]
    [InlineData("Running", "-infinity", "scheduled")]
    [InlineData("Scheduled", null, "scheduled")]
    [InlineData("Running", null, "scheduled")]
    public void NextStartState_SeparatesAParkedJobFromACrashBackoff(string? jobStatus, string? sentinel, string expected) =>
        Assert.Equal(expected, DarlingStoreCompressionHealthReader.NextStartStateFor(jobStatus, sentinel));

    /* #9527: the answer grew a ~400 B object per hypertable, healthy or not, and passed the default reply budget. A hypertable that is
       keeping up is a name in one list now. The scratch store already holds every collector hypertable, so the extra ones here
       make the full answer larger than the budget on any store this runs against. */
    [Fact]
    public async Task CompressionHealth_ListsAHealthyHypertableByNameOnly_AndKeepsTheFullRowForOneWithABacklog_UnderTheReplyBudget()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #9527 live test.");
        var ct = TestContext.Current.CancellationToken;
        const int healthyCount = 90;
        const string backlogTable = "ch9527_backlog";

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var healthyNames = Enumerable.Range(0, healthyCount).Select(i => $"ch9527_h{i:D3}").ToList();
        foreach (var name in healthyNames.Append(backlogTable))
        {
            await ExecAsync(connection, $"CREATE TABLE collect.{name} (collection_time timestamp NOT NULL, server_id integer NOT NULL, value bigint)", ct);
            await ExecAsync(connection, TimescaleSupport.CreateHypertableSql($"collect.{name}", "collection_time"), ct);
            await ExecAsync(connection, TimescaleSupport.EnableCompressionSql($"collect.{name}"), ct);
            await ExecAsync(connection, TimescaleSupport.AddCompressionPolicySql($"collect.{name}"), ct);
        }

        /* Five days back: its chunk is a whole day past the policy's one-day delay, so this one hypertable has a backlog. The others hold no rows. */
        await ExecAsync(connection,
            $"INSERT INTO collect.{backlogTable} SELECT date_trunc('day', now()::timestamp) - INTERVAL '5 days' + INTERVAL '12 hours' + (g || ' seconds')::interval, 1, g FROM generate_series(1, 50) g", ct);

        var defaultAnswer = await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, cancellationToken: ct);
        var fullAnswer = await DarlingMcpStoreCompressionHealthTools.GetStoreCompressionHealth(dataSource, full_detail: true, cancellationToken: ct);

        Assert.True(Encoding.UTF8.GetByteCount(fullAnswer) > McpResponseBudget.DefaultBytes, "the fixture is big enough that the full answer alone would pass the budget");
        Assert.True(Encoding.UTF8.GetByteCount(defaultAnswer) <= McpResponseBudget.DefaultBytes, "the default answer stays under the reply budget");

        using var doc = JsonDocument.Parse(defaultAnswer);
        var root = doc.RootElement;
        var detailed = root.GetProperty("hypertables").EnumerateArray().ToList();
        var listed = root.GetProperty("healthy_hypertables").EnumerateArray().Select(e => e.GetString()).ToList();

        /* The backlog hypertable keeps its whole row. No healthy one has a row, and each is in the name list. */
        var backlogRow = Assert.Single(detailed, h => h.GetProperty("hypertable").GetString() == backlogTable);
        Assert.Equal(1, backlogRow.GetProperty("eligible_uncompressed_chunks").GetInt32());
        Assert.Equal("scheduled", backlogRow.GetProperty("job").GetProperty("next_start_state").GetString());
        Assert.DoesNotContain(detailed, h => healthyNames.Contains(h.GetProperty("hypertable").GetString()!));
        Assert.All(healthyNames, name => Assert.Contains(name, listed));
        Assert.DoesNotContain(backlogTable, listed);

        /* The summary counts both lists, and every row left in hypertables is one that needs a look. */
        var summary = root.GetProperty("summary");
        Assert.Equal(detailed.Count + listed.Count, summary.GetProperty("hypertables").GetInt32());
        Assert.Equal(listed.Count, summary.GetProperty("healthy_hypertables").GetInt32());
        Assert.All(detailed, h => Assert.True(
            h.GetProperty("eligible_uncompressed_chunks").GetInt64() > 0
            || h.GetProperty("job").GetProperty("last_run_status").ValueKind == JsonValueKind.String
            || h.GetProperty("job").GetProperty("status").GetString() != "Scheduled"));

        /* full_detail=true is the old answer: every hypertable with its row, no name list. */
        using var full = JsonDocument.Parse(fullAnswer);
        Assert.Equal(JsonValueKind.Null, full.RootElement.GetProperty("healthy_hypertables").ValueKind);
        var fullRows = full.RootElement.GetProperty("hypertables").EnumerateArray().ToList();
        Assert.Equal(full.RootElement.GetProperty("summary").GetProperty("hypertables").GetInt32(), fullRows.Count);
        Assert.All(healthyNames.Append(backlogTable), name => Assert.Single(fullRows, h => h.GetProperty("hypertable").GetString() == name));
    }

    [Fact]
    public void CrashBackoffNote_SaysTheSchedulerClearsItOnlyFromTheFixedVersion()
    {
        var fixedStore = DarlingStoreCompressionHealthReader.CrashBackoffNote(new Version(2, 26, 4));
        var oldStore = DarlingStoreCompressionHealthReader.CrashBackoffNote(new Version(2, 26, 3));
        Assert.NotEqual(fixedStore, oldStore);
        Assert.Equal(oldStore, DarlingStoreCompressionHealthReader.CrashBackoffNote(null));
    }

    private static string PolicyJobSql(string select) =>
        $"{select} FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = '{Table}' AND (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')";

    private static async Task SeedStoreSizeAsync(NpgsqlConnection connection, DateTime anchorUtc, int daysAgo, long totalBytes, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes) VALUES ($1, 'store_9429', 'store', $2)",
            connection);
        command.Parameters.Add(new NpgsqlParameter { Value = DateTime.SpecifyKind(anchorUtc.Date.AddDays(-daysAgo).AddHours(12), DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        command.Parameters.AddWithValue(totalBytes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static DateTime ParseUtc(string? text) =>
        DateTime.SpecifyKind(DateTime.Parse(text!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind), DateTimeKind.Utc);
}
