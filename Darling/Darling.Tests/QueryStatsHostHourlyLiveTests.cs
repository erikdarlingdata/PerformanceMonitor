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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9164: the hourly host rollup's migration and the tick that fills it. The migration applied twice leaves the same schema. One tick on a
/// seeded store builds the rows of a closed hour and builds nothing for an hour that closed less than the grace ago. Every rollup column equals the
/// raw aggregate, the distinct digests of a range equal the raw distinct texts, a late row makes an hour incomplete until the next tick builds it
/// again, and the tick prunes the hours raw's retention took.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStatsHostHourlyLiveTests
{
    private const int ServerId = -9164287;
    private const int RetentionDays = 30;

    /// <summary>The tick's clock. The 11:00 hour closed at 12:00, five minutes before it, so it is inside the ten-minute grace.</summary>
    private static readonly DateTime TickNow = new(2026, 9, 15, 12, 5, 0, DateTimeKind.Unspecified);

    private static readonly DateTime ClosedHour = new(2026, 9, 15, 9, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime GraceHour = new(2026, 9, 15, 11, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The migration, run twice over a store that has no trace of it, leaves the same tables, columns and indexes, and no rows.</summary>
    [Fact]
    public async Task TheMigration_AppliedTwice_LeavesTheSameSchema()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        /* Found by its version, not as the newest rung, so a later rung does not break this test. */
        var rung = PgMigrations.Scripts.Single(s => s.Version == QueryStatsHostHourly.Version);

        await ExecuteAsync(connection, "DROP TABLE IF EXISTS collect.query_stats_host_hourly; DROP TABLE IF EXISTS collect.query_stats_host_hourly_built;", ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        var once = await SchemaAsync(connection, ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        var twice = await SchemaAsync(connection, ct);

        Assert.NotEmpty(once);
        Assert.Equal(once, twice);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built", ct));

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));
    }

    /// <summary>
    /// A tick on a seeded store builds the closed hour (two keys, one of them from two raw rows), records it as built, and leaves the hour
    /// that closed five minutes ago alone: no rollup row and no built row for it.
    /// </summary>
    [Fact]
    public async Task OneTick_BuildsAClosedHour_AndNothingForAnHourInsideTheGrace()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecuteAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (@id, 'hourly-9164', TRUE) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct,
            ("id", ServerId));
        await InsertRawAsync(connection, 1, ClosedHour.AddMinutes(10), "DbA", "0xH1", ct);
        await InsertRawAsync(connection, 2, ClosedHour.AddMinutes(40), "DbA", "0xH1", ct);
        await InsertRawAsync(connection, 3, ClosedHour.AddMinutes(20), "DbA", "0xH2", ct);
        await InsertRawAsync(connection, 4, GraceHour.AddMinutes(30), "DbA", "0xH1", ct);
        await QueryStatsLedgerSeed.SeedAsync(connection, ClosedHour.AddHours(-1), GraceHour.AddHours(2), ct);

        var result = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);

        Assert.Equal(0, result.Failed);
        Assert.True(result.HoursBuilt > 0);

        var closed = new Dictionary<string, long>();
        await using (var read = new NpgsqlCommand("SELECT query_hash, raw_rows FROM collect.query_stats_host_hourly WHERE server_id = @id AND bucket = @bucket", connection))
        {
            read.Parameters.AddWithValue("id", ServerId);
            read.Parameters.AddWithValue("bucket", ClosedHour);
            await using var reader = await read.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                closed[reader.GetString(0)] = reader.GetInt64(1);
            }
        }

        Assert.Equal(new Dictionary<string, long> { ["0xH1"] = 2, ["0xH2"] = 1 }, closed);
        Assert.Equal(3L, await ScalarAsync<long>(connection, "SELECT raw_rows FROM collect.query_stats_host_hourly_built WHERE server_id = @id AND bucket = @bucket", ct,
            ("id", ServerId), ("bucket", ClosedHour)));

        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE server_id = @id AND bucket = @bucket", ct,
            ("id", ServerId), ("bucket", GraceHour)));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built WHERE server_id = @id AND bucket = @bucket", ct,
            ("id", ServerId), ("bucket", GraceHour)));
    }

    private const int OtherServerId = ServerId - 1;

    private static readonly DateTime Hour8 = new(2026, 9, 15, 8, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Hour9 = new(2026, 9, 15, 9, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Hour10 = new(2026, 9, 15, 10, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The rows the raw top-queries statement reads for its measures.</summary>
    private const string Honest = TimescaleSupport.IntervalHonestSourceFilter;

    /// <summary>
    /// Every rollup column equals the raw statement's aggregate for the same server, hour and key, over closed hours of two servers. The
    /// seed holds zero-interval rows that carry extreme values (the filter must drop them from every measure and keep them for the
    /// observations), keys with a NULL database, hash or host object, NULL <c>max_dop</c>, a key whose rows are all zero-interval, and two
    /// groups tied on CPU.
    /// </summary>
    [Fact]
    public async Task EveryRollupColumn_EqualsTheRawAggregate_ForClosedHoursOfTwoServers()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await EnableServersAsync(connection, ct);
        var seeder = new RawSeeder(connection, ct);
        foreach (var serverId in new[] { ServerId, OtherServerId })
        {
            foreach (var hour in new[] { Hour8, Hour9, Hour10 })
            {
                await seeder.SeedHourAsync(serverId, hour);
            }
        }

        await QueryStatsLedgerSeed.SeedAsync(connection, Hour8, Hour10.AddHours(1), ct);
        var result = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);
        Assert.Equal(0, result.Failed);

        var compared = await AssertRollupEqualsRawAsync(connection, ct);
        Assert.Equal(2 * 3 * 6, compared);

        // The seed bites: a group of only zero-interval rows is a row of NULL sums, a group's max_dop is NULL where no honest row had one,
        // a NULL database and host object is a key like any other, and two groups of an hour tie on CPU.
        Assert.Equal(6L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE raw_rows = 0 AND total_cpu_us IS NULL", ct));
        Assert.True(await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE raw_rows > 0 AND max_dop IS NULL", ct) >= 6);
        Assert.True(await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE database_name IS NULL AND host_object_name IS NULL", ct) >= 6);
        Assert.True(await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT server_id, bucket, total_cpu_us FROM collect.query_stats_host_hourly WHERE total_cpu_us IS NOT NULL GROUP BY 1, 2, 3 HAVING count(*) > 1) AS t", ct) >= 6);
    }

    /// <summary>
    /// The distinct texts of a range, taken as the distinct union of the hours' <c>digests</c>, equal the raw statement's distinct text
    /// count over the same hours, for the whole server and for each group, including a text that appears in two hours and one that only
    /// a zero-interval row carries.
    /// </summary>
    [Fact]
    public async Task DistinctDigests_OfARangeOfHours_EqualTheRawDistinctTextCount()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await EnableServersAsync(connection, ct);
        var seeder = new RawSeeder(connection, ct);

        // Text 1 is in hours 8 and 9 and in two groups of hour 8, text 2 is in hours 9 and 10, text 3 is in two groups of hour 10. Text 9 is only on a zero-interval row.
        await seeder.InsertAsync(ServerId, Hour8.AddMinutes(5), "DbA", "0xQ1", null, digest: 1);
        await seeder.InsertAsync(ServerId, Hour8.AddMinutes(6), "DbA", "0xQ1", "hostA", digest: 1);
        await seeder.InsertAsync(ServerId, Hour8.AddMinutes(7), "DbA", "0xQ1", null, digest: 4);
        await seeder.InsertAsync(ServerId, Hour9.AddMinutes(5), "DbA", "0xQ1", null, digest: 1);
        await seeder.InsertAsync(ServerId, Hour9.AddMinutes(15), "DbA", "0xQ2", null, digest: 2);
        await seeder.InsertAsync(ServerId, Hour9.AddMinutes(25), "DbA", "0xQ2", null, digest: 9, interval: 0);
        await seeder.InsertAsync(ServerId, Hour10.AddMinutes(5), "DbA", "0xQ2", null, digest: 2);
        await seeder.InsertAsync(ServerId, Hour10.AddMinutes(8), "DbA", "0xQ2", null, digest: 3);
        await seeder.InsertAsync(ServerId, Hour10.AddMinutes(9), "DbB", "0xQ3", null, digest: 3);
        await seeder.InsertAsync(ServerId, Hour10.AddMinutes(10), "DbB", "0xQ3", null, digest: null);
        await QueryStatsLedgerSeed.SeedAsync(connection, Hour8, Hour10.AddHours(1), ct);
        var result = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);
        Assert.Equal(0, result.Failed);

        var overlap = 0L;
        foreach (var (from, to) in new[] { (Hour8, Hour10), (Hour8, Hour10.AddHours(1)), (Hour9, Hour10.AddHours(1)), (Hour10, Hour10.AddHours(1)) })
        {
            var raw = await ScalarAsync<long>(connection, $"SELECT count(DISTINCT query_text_digest) FILTER (WHERE {Honest}) FROM collect.query_stats WHERE server_id = @id AND collection_time >= @from AND collection_time < @to", ct,
                ("id", ServerId), ("from", from), ("to", to));
            var rolled = await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT DISTINCT unnest(digests) FROM collect.query_stats_host_hourly WHERE server_id = @id AND bucket >= @from AND bucket < @to) AS t", ct,
                ("id", ServerId), ("from", from), ("to", to));
            var partsSum = await ScalarAsync<long>(connection, "SELECT COALESCE(sum(cardinality(digests)), 0)::bigint FROM collect.query_stats_host_hourly WHERE server_id = @id AND bucket >= @from AND bucket < @to", ct,
                ("id", ServerId), ("from", from), ("to", to));
            Assert.True(rolled > 0, "Each range holds at least one text.");
            Assert.Equal(raw, rolled);
            overlap += partsSum - rolled;
        }

        Assert.True(overlap > 0, "The seed should carry a text in two hours or two groups, so that summing the parts over-counts.");

        // Per group over the whole range: the union of the group's hours equals the raw distinct count of the group.
        var badGroups = await ScalarAsync<long>(connection, $@"
SELECT count(*) FROM (SELECT DISTINCT database_name, query_hash, host_object_name FROM collect.query_stats WHERE server_id = @id) AS g
WHERE (SELECT count(DISTINCT q.query_text_digest) FILTER (WHERE {Honest}) FROM collect.query_stats AS q
       WHERE q.server_id = @id AND q.database_name IS NOT DISTINCT FROM g.database_name AND q.query_hash IS NOT DISTINCT FROM g.query_hash AND q.host_object_name IS NOT DISTINCT FROM g.host_object_name)
   <> (SELECT count(*) FROM (SELECT DISTINCT unnest(h.digests) FROM collect.query_stats_host_hourly AS h
       WHERE h.server_id = @id AND h.database_name IS NOT DISTINCT FROM g.database_name AND h.query_hash IS NOT DISTINCT FROM g.query_hash AND h.host_object_name IS NOT DISTINCT FROM g.host_object_name) AS u)", ct,
            ("id", ServerId));
        Assert.Equal(0L, badGroups);
    }

    /// <summary>
    /// A raw row that arrives for an hour after it was built makes the hour incomplete (its build counted fewer rows than the ledger holds
    /// now), the next tick builds it again with no help from the settle rule, and the hour is complete and equal to raw once more.
    /// </summary>
    [Fact]
    public async Task ALateRow_MakesTheHourIncomplete_UntilTheNextTickRebuildsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await EnableServersAsync(connection, ct);
        var seeder = new RawSeeder(connection, ct);
        foreach (var hour in new[] { Hour8, Hour9, Hour10 })
        {
            await seeder.SeedHourAsync(ServerId, hour);
        }

        await QueryStatsLedgerSeed.SeedAsync(connection, Hour8, Hour10.AddHours(1), ct);
        Assert.Equal(0, (await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct)).Failed);

        Assert.Empty(await QueryStatsHostHourly.IncompleteHoursAsync(connection, null, ServerId, Hour8, Hour10.AddHours(1), 30, ct));
        await AssertRollupEqualsRawAsync(connection, ct);

        // The collector's late batch: a new honest row for hour 9, counted in the ledger as the writer would.
        await seeder.InsertAsync(ServerId, Hour9.AddMinutes(50), "DbA", "0xLATE", null, digest: 7);
        await QueryStatsLedgerSeed.RecountAsync(connection, Hour9, Hour9.AddHours(1), ct);

        Assert.False(await QueryStatsHostHourly.IsCompleteAsync(connection, null, ServerId, Hour8, Hour10.AddHours(1), 30, ct));
        Assert.Equal(new[] { Hour9 }, await QueryStatsHostHourly.IncompleteHoursAsync(connection, null, ServerId, Hour8, Hour10.AddHours(1), 30, ct));
        Assert.True(await QueryStatsHostHourly.IsCompleteAsync(connection, null, ServerId, Hour10, Hour10.AddHours(1), 30, ct), "The hours around the late row stay complete.");

        var second = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);
        Assert.Equal(0, second.Failed);
        Assert.True(await QueryStatsHostHourly.IsCompleteAsync(connection, null, ServerId, Hour8, Hour10.AddHours(1), 30, ct));
        await AssertRollupEqualsRawAsync(connection, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE server_id = @id AND bucket = @bucket AND query_hash = '0xLATE'", ct,
            ("id", ServerId), ("bucket", Hour9)));
    }

    /// <summary>The second tick's clock: past the settle time of an hour that the first tick built at <see cref="TickNow"/>.</summary>
    private static readonly DateTime LaterNow = TickNow.AddHours(1);

    /// <summary>
    /// An hour whose first build matched the ledger is final (#9457): a second tick past the settle time leaves its rollup rows (their
    /// <c>xmin</c>) and its built row (<c>built_at</c> and <c>xmin</c>) untouched. The old settle rule deleted and rebuilt it.
    /// </summary>
    [Fact]
    public async Task AMatchedBuild_IsNotRebuilt_PastTheSettleTime()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await EnableServersAsync(connection, ct);
        await new RawSeeder(connection, ct).SeedHourAsync(ServerId, Hour10);
        await QueryStatsLedgerSeed.SeedAsync(connection, Hour10, Hour10.AddHours(1), ct);

        Assert.Equal(0, (await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct)).Failed);
        var before = await HourStateAsync(connection, Hour10, ct);
        Assert.NotEqual(0L, before.RollupRows);
        Assert.Equal(before.RawRows, before.LedgerRows);
        Assert.Equal(TickNow, before.BuiltAt);

        Assert.Equal(0, (await QueryStatsHostHourly.RunTickAsync(postgres, LaterNow, RetentionDays, NullLogger.Instance, ct)).Failed);

        Assert.Equal(before, await HourStateAsync(connection, Hour10, ct));
        await AssertRollupEqualsRawAsync(connection, ct);
    }

    /// <summary>An hour whose build counted a different number of raw rows than the ledger held (rows no ledger saw) is still built once more after the settle time.</summary>
    [Fact]
    public async Task AMismatchedBuild_IsRebuilt_PastTheSettleTime()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await EnableServersAsync(connection, ct);
        await new RawSeeder(connection, ct).SeedHourAsync(ServerId, Hour10);
        await QueryStatsLedgerSeed.SeedAsync(connection, Hour10, Hour10.AddHours(1), ct);

        // A raw row the ledger never counted, as an older writer leaves it: the hour's ledger is one below raw.
        await ExecuteAsync(connection, "UPDATE collect.query_stats_hour_ledger SET n = n - 1 WHERE server_id = @id AND bucket = @bucket", ct,
            ("id", ServerId), ("bucket", Hour10));

        Assert.Equal(0, (await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct)).Failed);
        var before = await HourStateAsync(connection, Hour10, ct);
        Assert.NotEqual(before.RawRows, before.LedgerRows);
        Assert.Equal(TickNow, before.BuiltAt);

        Assert.Equal(0, (await QueryStatsHostHourly.RunTickAsync(postgres, LaterNow, RetentionDays, NullLogger.Instance, ct)).Failed);

        var after = await HourStateAsync(connection, Hour10, ct);
        Assert.Equal(LaterNow, after.BuiltAt);
        Assert.NotEqual(before.RollupXmin, after.RollupXmin);
        await AssertRollupEqualsRawAsync(connection, ct);
    }

    /// <summary>A row added after the first build moves the ledger, so the hour is rebuilt on a tick past the settle time and is complete again.</summary>
    [Fact]
    public async Task ARowAfterTheFirstBuild_StillRebuildsTheHour_PastTheSettleTime()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await EnableServersAsync(connection, ct);
        var seeder = new RawSeeder(connection, ct);
        await seeder.SeedHourAsync(ServerId, Hour10);
        await QueryStatsLedgerSeed.SeedAsync(connection, Hour10, Hour10.AddHours(1), ct);

        Assert.Equal(0, (await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct)).Failed);
        var before = await HourStateAsync(connection, Hour10, ct);

        await seeder.InsertAsync(ServerId, Hour10.AddMinutes(50), "DbA", "0xLATE", null, digest: 7);
        await QueryStatsLedgerSeed.RecountAsync(connection, Hour10, Hour10.AddHours(1), ct);

        Assert.Equal(0, (await QueryStatsHostHourly.RunTickAsync(postgres, LaterNow, RetentionDays, NullLogger.Instance, ct)).Failed);

        var after = await HourStateAsync(connection, Hour10, ct);
        Assert.Equal(LaterNow, after.BuiltAt);
        Assert.NotEqual(before.RollupXmin, after.RollupXmin);
        Assert.Equal(before.RawRows + 1, after.RawRows);
        Assert.Equal(after.RawRows, after.LedgerRows);
        Assert.True(await QueryStatsHostHourly.IsCompleteAsync(connection, null, ServerId, Hour10, Hour10.AddHours(1), 30, ct));
        await AssertRollupEqualsRawAsync(connection, ct);
    }

    /// <summary>What one hour of the rollup holds for the test server: its row count and the <c>xmin</c> of those rows, and its built row's counts, <c>built_at</c> and <c>xmin</c>.</summary>
    private readonly record struct HourState(long RollupRows, string RollupXmin, long RawRows, long LedgerRows, DateTime BuiltAt, string BuiltXmin);

    private static async Task<HourState> HourStateAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT (SELECT count(*) FROM collect.query_stats_host_hourly AS h WHERE h.server_id = @id AND h.bucket = @bucket),
                     COALESCE((SELECT string_agg(DISTINCT h.xmin::text, ',') FROM collect.query_stats_host_hourly AS h WHERE h.server_id = @id AND h.bucket = @bucket), ''),
                     b.raw_rows, b.ledger_rows, b.built_at, b.xmin::text
              FROM collect.query_stats_host_hourly_built AS b
              WHERE b.server_id = @id AND b.bucket = @bucket", connection);
        command.Parameters.AddWithValue("id", ServerId);
        command.Parameters.AddWithValue("bucket", hour);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "The hour has no built row.");
        return new HourState(reader.GetInt64(0), reader.GetString(1), reader.GetInt64(2), reader.GetInt64(3), DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Unspecified), reader.GetString(5));
    }

    /// <summary>
    /// A tick removes the rollup and built rows of the hours before the first whole hour inside raw's retention, one of them the hour
    /// that straddles the cutoff, and keeps the first whole hour inside it and the hour the same tick built.
    /// </summary>
    [Fact]
    public async Task ATick_PrunesHoursOlderThanRawRetention_AndKeepsTheRest()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await EnableServersAsync(connection, ct);
        var seeder = new RawSeeder(connection, ct);
        await seeder.SeedHourAsync(ServerId, Hour9);
        await QueryStatsLedgerSeed.SeedAsync(connection, Hour8, Hour10, ct);

        // TickNow less 30 days is 08-16 12:05, so 08-16 13:00 is the first whole hour inside retention and 08-16 12:00 straddles the cutoff.
        var firstKept = new DateTime(2026, 8, 16, 13, 0, 0, DateTimeKind.Unspecified);
        var straddling = firstKept.AddHours(-1);
        var wayOld = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
        foreach (var bucket in new[] { wayOld, straddling, firstKept })
        {
            await ExecuteAsync(connection, "INSERT INTO collect.query_stats_host_hourly (server_id, bucket, database_name, query_hash, host_object_name, raw_rows) VALUES (@id, @bucket, 'DbOld', '0xOLD', NULL, 1)", ct,
                ("id", ServerId), ("bucket", bucket));
            await ExecuteAsync(connection, "INSERT INTO collect.query_stats_host_hourly_built (server_id, bucket, built_at, raw_rows, ledger_rows) VALUES (@id, @bucket, @built, 1, 0)", ct,
                ("id", ServerId), ("bucket", bucket), ("built", TickNow));
        }

        var result = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);
        Assert.Equal(0, result.Failed);
        Assert.Equal(2L, result.RowsRemoved);

        foreach (var table in new[] { QueryStatsHostHourly.Table, QueryStatsHostHourly.BuiltTable })
        {
            var kept = new List<DateTime>();
            await using (var read = new NpgsqlCommand($"SELECT DISTINCT bucket FROM {table} WHERE server_id = @id ORDER BY 1", connection))
            {
                read.Parameters.AddWithValue("id", ServerId);
                await using var reader = await read.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    kept.Add(reader.GetDateTime(0));
                }
            }

            Assert.DoesNotContain(wayOld, kept);
            Assert.DoesNotContain(straddling, kept);
            Assert.Contains(firstKept, kept);
            Assert.Contains(Hour9, kept);
        }
    }

    /// <summary>The first whole hour inside four days of <see cref="TickNow"/>: the prune's cutoff on a store whose raw is a raw hypertable.</summary>
    private static readonly DateTime FirstKeptInFourDays = new(2026, 9, 11, 13, 0, 0, DateTimeKind.Unspecified);

    private const int OldHours = 40;
    private const int KeptHours = 10;
    private const int SliceHours = 6;
    private const int RowsPerServerHour = 3;

    /// <summary>Built rows whose rollup rows are gone: the ledger would claim an hour the rollup no longer holds.</summary>
    private const string LedgerWithoutRollupRows = @"
SELECT count(*) FROM collect.query_stats_host_hourly_built AS b
WHERE NOT EXISTS (SELECT 1 FROM collect.query_stats_host_hourly AS h WHERE h.server_id = b.server_id AND h.bucket = b.bucket)";

    /// <summary>Rollup rows whose built row is gone.</summary>
    private const string RollupRowsWithoutLedger = @"
SELECT count(*) FROM collect.query_stats_host_hourly AS h
WHERE NOT EXISTS (SELECT 1 FROM collect.query_stats_host_hourly_built AS b WHERE b.server_id = h.server_id AND b.bucket = h.bucket)";

    /// <summary>Runs <paramref name="onSlice"/> after each committed prune slice, through the pacer seam the prune already takes.</summary>
    private sealed class SliceWatcher(Func<Task> onSlice) : IBatchWalPacer
    {
        public Task<long?> ReadWalPositionAsync(NpgsqlConnection connection, CancellationToken cancellationToken) => Task.FromResult<long?>(null);

        public Task AfterBatchAsync(NpgsqlConnection connection, long? before, CancellationToken cancellationToken) => onSlice();
    }

    /// <summary>
    /// A prune over more old hours than one slice takes removes them oldest first, a slice per transaction, and after every commit
    /// the ledger names no hour whose rollup rows are gone and no rollup row lacks its ledger row. It ends at the cutoff in both tables.
    /// </summary>
    [Fact]
    public Task ThePrune_RemovesOldHoursInSlices_AndNeverLeavesALedgerRowWithoutItsRollupRows() =>
        WithStoreAsync(tiered: false, async (scratch, connection, ct) =>
        {
            var firstOld = FirstKeptInFourDays.AddHours(-OldHours);
            await SeedBuiltHoursAsync(connection, firstOld, OldHours + KeptHours, ct);
            var oldRollupRows = 2L * OldHours * RowsPerServerHour;
            Assert.Equal(oldRollupRows, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE bucket < @cutoff", ct, ("cutoff", FirstKeptInFourDays)));

            var oldestAfterEachSlice = new List<DateTime>();
            var watcher = new SliceWatcher(async () =>
            {
                Assert.Equal(0L, await ScalarAsync<long>(connection, LedgerWithoutRollupRows, ct));
                Assert.Equal(0L, await ScalarAsync<long>(connection, RollupRowsWithoutLedger, ct));
                oldestAfterEachSlice.Add(await ScalarAsync<DateTime>(connection, "SELECT min(bucket) FROM collect.query_stats_host_hourly_built", ct));
            });

            var removed = await QueryStatsHostHourly.PruneAsync(
                connection, TickNow, 4, SliceHours, () => TimeSpan.Zero, _ => Task.FromResult<IBatchWalPacer?>(watcher), ct);

            Assert.Equal(oldRollupRows, removed);
            Assert.Equal(
                Enumerable.Range(1, OldHours / SliceHours).Select(i => firstOld.AddHours(i * SliceHours)).Append(FirstKeptInFourDays).ToList(),
                oldestAfterEachSlice);
            foreach (var table in new[] { QueryStatsHostHourly.Table, QueryStatsHostHourly.BuiltTable })
            {
                Assert.Equal(FirstKeptInFourDays, await ScalarAsync<DateTime>(connection, $"SELECT min(bucket) FROM {table}", ct));
            }

            Assert.Equal(2L * KeptHours * RowsPerServerHour, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly", ct));
            Assert.Equal(2L * KeptHours, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built", ct));
            Assert.Equal(0L, await QueryStatsHostHourly.PruneAsync(connection, TickNow, 4, SliceHours, () => TimeSpan.Zero, null, ct));
        });

    /// <summary>
    /// A prune that reaches its share of the tick (<see cref="QueryStatsHostHourly.MaxPruneDuration"/>) after two slices stops with both tables cut at the same hour and no ledger row without its rollup
    /// rows, and the next call removes the rest. The budget is a counter that reads spent from its third look, so no clock is involved.
    /// </summary>
    [Fact]
    public Task AnOutOfBudgetPrune_LeavesAConsistentState_AndTheNextCallFinishesIt() =>
        WithStoreAsync(tiered: false, async (scratch, connection, ct) =>
        {
            var firstOld = FirstKeptInFourDays.AddHours(-OldHours);
            await SeedBuiltHoursAsync(connection, firstOld, OldHours + KeptHours, ct);
            var oldRollupRows = 2L * OldHours * RowsPerServerHour;

            var looks = 0;
            Func<TimeSpan> twoSlicesOfBudget = () => ++looks <= 2 ? TimeSpan.Zero : QueryStatsHostHourly.MaxPruneDuration;
            var first = await QueryStatsHostHourly.PruneAsync(connection, TickNow, 4, SliceHours, twoSlicesOfBudget, null, ct);

            Assert.Equal(2L * 2 * SliceHours * RowsPerServerHour, first);
            foreach (var table in new[] { QueryStatsHostHourly.Table, QueryStatsHostHourly.BuiltTable })
            {
                Assert.Equal(firstOld.AddHours(2 * SliceHours), await ScalarAsync<DateTime>(connection, $"SELECT min(bucket) FROM {table}", ct));
            }

            Assert.Equal(0L, await ScalarAsync<long>(connection, LedgerWithoutRollupRows, ct));
            Assert.Equal(0L, await ScalarAsync<long>(connection, RollupRowsWithoutLedger, ct));

            var second = await QueryStatsHostHourly.PruneAsync(connection, TickNow, 4, SliceHours, () => TimeSpan.Zero, null, ct);

            Assert.Equal(oldRollupRows, first + second);
            foreach (var table in new[] { QueryStatsHostHourly.Table, QueryStatsHostHourly.BuiltTable })
            {
                Assert.Equal(FirstKeptInFourDays, await ScalarAsync<DateTime>(connection, $"SELECT min(bucket) FROM {table}", ct));
            }

            Assert.Equal(0L, await ScalarAsync<long>(connection, LedgerWithoutRollupRows, ct));
            Assert.Equal(0L, await ScalarAsync<long>(connection, RollupRowsWithoutLedger, ct));
        });

    /// <summary>
    /// #9711: a prune slice that fails does not stop the tick. The second one-hour slice raises, the first slice's rows are gone and counted, the failing
    /// slice rolls back, one warning names its hours, and the tick still builds the planned hours on the same connection. Once the failure is gone the
    /// next tick finishes the prune.
    /// </summary>
    [Fact]
    public Task AFailedPruneSlice_DoesNotStopTheBuilds_AndIsLoggedOnce() =>
        WithStoreAsync(tiered: false, async (scratch, connection, ct) =>
        {
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var oldFrom = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
            await SeedBuiltHoursAsync(connection, oldFrom, 3, ct);

            var seeder = new RawSeeder(connection, ct);
            foreach (var serverId in new[] { ServerId, OtherServerId })
            {
                await seeder.SeedHourAsync(serverId, Hour9);
            }

            await QueryStatsLedgerSeed.SeedAsync(connection, Hour8, Hour10.AddHours(1), ct);
            await ExecuteAsync(connection, @"
CREATE FUNCTION collect.fail_prune_9711() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'prune blocked'; END $$;
CREATE TRIGGER fail_prune_9711 BEFORE DELETE ON collect.query_stats_host_hourly FOR EACH ROW
WHEN (OLD.bucket = '2026-08-01 01:00'::timestamp) EXECUTE FUNCTION collect.fail_prune_9711()", ct);

            var logger = new CapturingTestLogger();
            var result = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, logger, ct);

            Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Contains("2026-08-01 01:00", logger.Lines.Single(l => l.StartsWith("Warning:", StringComparison.Ordinal)), StringComparison.Ordinal);
            Assert.NotNull(result.PruneFailure);
            Assert.Contains("2026-08-01 01:00", result.PruneFailure, StringComparison.Ordinal);
            Assert.Contains("prune blocked", result.PruneFailure, StringComparison.Ordinal);
            Assert.Equal(0, result.Failed);
            Assert.Equal(2L * RowsPerServerHour, result.RowsRemoved);
            Assert.True(result.HoursBuilt > 0);
            Assert.Equal(2L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built WHERE bucket = @b", ct, ("b", Hour9)));
            Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE bucket = @b", ct, ("b", oldFrom)));
            Assert.Equal(2L * 2 * RowsPerServerHour, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE bucket >= @b AND bucket < @c", ct, ("b", oldFrom.AddHours(1)), ("c", oldFrom.AddHours(3))));

            await ExecuteAsync(connection, "DROP TRIGGER fail_prune_9711 ON collect.query_stats_host_hourly", ct);
            var next = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);

            Assert.Null(next.PruneFailure);
            Assert.Equal(2L * 2 * RowsPerServerHour, next.RowsRemoved);
            Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE bucket < @b", ct, ("b", Hour8)));
        });

    /// <summary>
    /// The tick prunes and plans at the horizon <see cref="QueryStatsHostHourly.HorizonDaysAsync"/> resolves. Where raw is a raw hypertable
    /// that is four days, and a tick removes the older rollup and built rows, builds nothing older, and builds every hour inside. Where raw
    /// is a plain table the collector's thirty days stand and the older rows stay.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public Task ATick_KeepsTheHoursRawHolds_ByTheTableRawIs(bool tiered) =>
        WithStoreAsync(tiered, async (scratch, connection, ct) =>
        {
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var rolledUpFiveDaysBack = new DateTime(2026, 9, 8, 0, 0, 0, DateTimeKind.Unspecified);
            await SeedBuiltHoursAsync(connection, rolledUpFiveDaysBack, 1, ct);
            var insideHour = new DateTime(2026, 9, 12, 9, 0, 0, DateTimeKind.Unspecified);
            var outsideHour = new DateTime(2026, 9, 9, 9, 0, 0, DateTimeKind.Unspecified);
            var seeder = new RawSeeder(connection, ct);
            await seeder.InsertAsync(ServerId, insideHour.AddMinutes(5), "DbA", "0xIN", null);
            await seeder.InsertAsync(ServerId, outsideHour.AddMinutes(5), "DbA", "0xOUT", null);
            await QueryStatsLedgerSeed.SeedAsync(connection, rolledUpFiveDaysBack, TickNow, ct);

            var built = 0;
            for (var ticks = 0; ticks < 8; ticks++)
            {
                var result = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);
                Assert.Equal(0, result.Failed);
                built += result.HoursBuilt;
                if (!tiered || result.HoursBuilt == 0)
                {
                    break;
                }
            }

            var rollupRowsOfFiveDaysBack = await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE bucket = @bucket", ct, ("bucket", rolledUpFiveDaysBack));
            var builtRowsOfFiveDaysBack = await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built WHERE bucket = @bucket", ct, ("bucket", rolledUpFiveDaysBack));
            if (!tiered)
            {
                Assert.Equal(2L * RowsPerServerHour, rollupRowsOfFiveDaysBack);
                Assert.Equal(2L, builtRowsOfFiveDaysBack);
                return;
            }

            Assert.Equal(0L, rollupRowsOfFiveDaysBack);
            Assert.Equal(0L, builtRowsOfFiveDaysBack);
            Assert.Equal(FirstKeptInFourDays, await ScalarAsync<DateTime>(connection, "SELECT min(bucket) FROM collect.query_stats_host_hourly_built", ct));
            Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built WHERE bucket = @bucket", ct, ("bucket", outsideHour)));
            Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT raw_rows FROM collect.query_stats_host_hourly_built WHERE server_id = @id AND bucket = @bucket", ct, ("id", ServerId), ("bucket", insideHour)));

            /* Every hour from the first whole one inside four days to the last closed one is built for both servers: 13:00 on the 11th through 10:00 on the 15th. */
            Assert.Equal(2L * 94, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built", ct));
            Assert.Equal(94, built);
        });

    /// <summary>Plants <paramref name="hours"/> consecutive hours from <paramref name="from"/> for both servers: a built row each, and three rollup rows each.</summary>
    private static async Task SeedBuiltHoursAsync(NpgsqlConnection connection, DateTime from, int hours, CancellationToken ct)
    {
        await ExecuteAsync(connection, @"
INSERT INTO collect.query_stats_host_hourly (server_id, bucket, database_name, query_hash, host_object_name, raw_rows)
SELECT s, h, 'DbOld', '0xK' || k, NULL, 1
FROM unnest(ARRAY[@a, @b]) AS s
CROSS JOIN generate_series(@from::timestamp, @from::timestamp + (@hours - 1) * interval '1 hour', interval '1 hour') AS h
CROSS JOIN generate_series(1, 3) AS k", ct, ("a", ServerId), ("b", OtherServerId), ("from", from), ("hours", hours));
        await ExecuteAsync(connection, @"
INSERT INTO collect.query_stats_host_hourly_built (server_id, bucket, built_at, raw_rows, ledger_rows)
SELECT s, h, @built, 3, 0
FROM unnest(ARRAY[@a, @b]) AS s
CROSS JOIN generate_series(@from::timestamp, @from::timestamp + (@hours - 1) * interval '1 hour', interval '1 hour') AS h", ct,
            ("a", ServerId), ("b", OtherServerId), ("from", from), ("hours", hours), ("built", TickNow));
    }

    /// <summary>
    /// A migrated scratch store with the two servers enabled for the rollup. <paramref name="tiered"/> converts <c>collect.query_stats</c> to a
    /// hypertable first, the shape the gated raw purge owns.
    /// </summary>
    private static async Task WithStoreAsync(bool tiered, Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9684 host hourly live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        Assert.SkipUnless(!tiered || scratch.TimescaleEnabled, "A tiered raw table needs the TimescaleDB extension.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        if (tiered)
        {
            await ExecuteAsync(connection, TimescaleSupport.CreateHypertableSql("collect.query_stats", "collection_time"), ct);
        }

        await EnableServersAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt)));
            });
        }
    }

    private static Task<int> EnableServersAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecuteAsync(connection, @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
SELECT s, 'hourly-9164-' || s::text, TRUE FROM unnest(ARRAY[@a, @b]) AS s
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ("a", ServerId), ("b", OtherServerId));

    /// <summary>
    /// The aggregates of the raw statement's wide-row pass (<c>DarlingDataReader.TopQueriesSql</c>), written out here, over each (server,
    /// hour, database, hash, host object) of <c>collect.query_stats</c>, set against the rollup's rows by column name as JSON. Both
    /// directions are compared, so a rollup column with no raw counterpart here fails too. Returns the number of groups compared.
    /// </summary>
    private static async Task<int> AssertRollupEqualsRawAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sums = new[] { ("total_executions", "delta_execution_count"), ("total_cpu_us", "delta_worker_time"), ("total_elapsed_us", "delta_elapsed_time"), ("total_reads", "delta_logical_reads"),
            ("total_writes", "delta_logical_writes"), ("total_physical_reads", "delta_physical_reads"), ("total_rows", "delta_rows"), ("total_spills", "delta_spills") };
        var mins = new[] { "min_dop", "min_worker_time", "min_elapsed_time", "min_physical_reads", "min_rows", "min_grant_kb", "min_used_grant_kb", "min_ideal_grant_kb", "min_spills", "min_reserved_threads", "min_used_threads" };
        var maxes = new[] { "max_dop", "max_worker_time", "max_elapsed_time", "query_plan_hash", "sql_handle", "plan_handle", "creation_time", "max_physical_reads", "max_rows", "max_grant_kb", "max_used_grant_kb",
            "max_ideal_grant_kb", "max_spills", "max_reserved_threads", "max_used_threads", "total_clr_time", "plan_generation_num" };

        var columns = new List<string> { $"COUNT(*) FILTER (WHERE {Honest}) AS raw_rows" };
        columns.AddRange(sums.Select(s => $"CAST(SUM({s.Item2}) FILTER (WHERE {Honest}) AS bigint) AS {s.Item1}"));
        columns.AddRange(mins.Select(c => $"MIN({c}) FILTER (WHERE {Honest}) AS {c}"));
        columns.AddRange(maxes.Select(c => $"MAX({c}) FILTER (WHERE {Honest}) AS {c}"));
        columns.Add("MAX(last_execution_time) AS last_execution_time");
        columns.Add("MAX(CAST(delta_worker_time AS double precision) / NULLIF(sample_interval_seconds, 0) / 1000.0) AS worker_time_per_second");
        columns.Add($"array_agg(DISTINCT query_text_digest) FILTER (WHERE {Honest} AND query_text_digest IS NOT NULL) AS digests");
        /* #9802: the oracle takes the largest key as the first element of the largest one-element array, because PostgreSQL 16 and 17 have no max(bytea). */
        columns.Add(@"(MAX(ARRAY[timestamp_send(collection_time) || int8send(collection_id)
            || CASE WHEN query_text IS NOT NULL THEN '\x00'::bytea || convert_to(query_text, 'UTF8') ELSE '\x01'::bytea || query_text_digest END])
            FILTER (WHERE query_text IS NOT NULL OR query_text_digest IS NOT NULL))[1] AS latest_text_key");

        var reference = await ReadJsonByKeyAsync(connection, @$"
SELECT concat_ws('|', r.server_id, r.bucket, COALESCE(r.database_name, '<null>'), COALESCE(r.query_hash, '<null>'), COALESCE(r.host_object_name, '<null>')),
       (to_jsonb(r) - 'server_id' - 'bucket' - 'database_name' - 'query_hash' - 'host_object_name')::text
FROM (
    SELECT server_id, date_trunc('hour', collection_time) AS bucket, database_name, query_hash, host_object_name,
           {string.Join(",\n           ", columns)}
    FROM collect.query_stats
    WHERE server_id = ANY(@servers)
    GROUP BY server_id, date_trunc('hour', collection_time), database_name, query_hash, host_object_name
) AS r", ct);
        var rolled = await ReadJsonByKeyAsync(connection, @"
SELECT concat_ws('|', h.server_id, h.bucket, COALESCE(h.database_name, '<null>'), COALESCE(h.query_hash, '<null>'), COALESCE(h.host_object_name, '<null>')),
       (to_jsonb(h) - 'server_id' - 'bucket' - 'database_name' - 'query_hash' - 'host_object_name')::text
FROM collect.query_stats_host_hourly AS h
WHERE h.server_id = ANY(@servers)", ct);

        Assert.NotEmpty(reference);
        Assert.Equal(reference.Keys.OrderBy(k => k, StringComparer.Ordinal), rolled.Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var (key, want) in reference)
        {
            var got = rolled[key];
            Assert.Equal(want.Keys.OrderBy(k => k, StringComparer.Ordinal), got.Keys.OrderBy(k => k, StringComparer.Ordinal));
            foreach (var column in want.Keys)
            {
                Assert.True(want[column] == got[column], $"Column {column} of group {key}: raw {want[column]}, rollup {got[column]}.");
            }
        }

        return reference.Count;
    }

    private static async Task<Dictionary<string, Dictionary<string, string>>> ReadJsonByKeyAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var rows = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("servers", new[] { ServerId, OtherServerId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            using var document = System.Text.Json.JsonDocument.Parse(reader.GetString(1));
            rows.Add(reader.GetString(0), document.RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText(), StringComparer.Ordinal));
        }

        return rows;
    }

    /// <summary>Plants raw rows of known shape. Values are drawn from a fixed seed, so a run is repeatable.</summary>
    private sealed class RawSeeder(NpgsqlConnection connection, CancellationToken ct)
    {
        private readonly Random _random = new(9164);
        private long _collectionId = 9_164_000;

        /// <summary>
        /// Six groups for one server and hour: a group with an honest run and a newest zero-interval row of extreme values; a group tied
        /// with it on CPU under a host object; a NULL database and hash-bearing group with no <c>max_dop</c>; a NULL hash group with some;
        /// a group of only zero-interval rows; and a group whose interval is NULL.
        /// </summary>
        public async Task SeedHourAsync(int serverId, DateTime hour)
        {
            await InsertAsync(serverId, hour.AddMinutes(1), "DbA", "0xG1", null, digest: 1, cpu: 200);
            await InsertAsync(serverId, hour.AddMinutes(2), "DbA", "0xG1", null, digest: 2, cpu: 400);
            await InsertAsync(serverId, hour.AddMinutes(3), "DbA", "0xG1", null, digest: 1, cpu: 600);
            await InsertAsync(serverId, hour.AddMinutes(50), "DbA", "0xG1", null, digest: 3, interval: 0, cpu: 700, extreme: true);
            await InsertAsync(serverId, hour.AddMinutes(4), "DbA", "0xG2", "procX", digest: 2, cpu: 1200);
            await InsertAsync(serverId, hour.AddMinutes(5), null, "0xG3", null, digest: 4, cpu: 50, noDop: true);
            await InsertAsync(serverId, hour.AddMinutes(6), null, "0xG3", null, digest: null, cpu: 60, noDop: true);
            await InsertAsync(serverId, hour.AddMinutes(7), "DbB", null, null, digest: 5, cpu: 80, noDop: true);
            await InsertAsync(serverId, hour.AddMinutes(8), "DbB", null, null, digest: 5, cpu: 90);
            await InsertAsync(serverId, hour.AddMinutes(9), "DbA", "0xG5", "procY", digest: 6, interval: 0, cpu: 300, extreme: true);
            await InsertAsync(serverId, hour.AddMinutes(10), "DbA", "0xG5", "procY", digest: 7, interval: 0, cpu: 301, extreme: true);
            await InsertAsync(serverId, hour.AddMinutes(11), null, "0xG6", "procZ", digest: 8, interval: null, cpu: 70);
        }

        public async Task InsertAsync(int serverId, DateTime at, string? database, string? hash, string? host, int? digest = null, int? interval = 300, long cpu = 100, bool noDop = false, bool extreme = false)
        {
            var id = ++_collectionId;
            var values = new List<(string Column, object? Value)>
            {
                ("collection_id", id), ("collection_time", at), ("server_id", serverId), ("server_name", $"hourly-9164-{serverId}"),
                ("database_name", database), ("query_hash", hash), ("host_object_name", host), ("sample_interval_seconds", interval),
                ("delta_worker_time", cpu), ("delta_elapsed_time", cpu * 2), ("delta_execution_count", extreme ? 7 : 1 + _random.Next(0, 5)),
                ("delta_logical_reads", Draw(1000)), ("delta_logical_writes", Draw(50)), ("delta_physical_reads", Draw(20)), ("delta_rows", Draw(500)), ("delta_spills", Draw(3)),
                ("query_plan_hash", extreme ? "zzzz" : $"0xP{_random.Next(1, 9)}"), ("sql_handle", extreme ? "zzzz" : $"0xS{_random.Next(1, 9)}"), ("plan_handle", extreme ? "zzzz" : $"0xH{_random.Next(1, 9)}"),
                ("last_execution_time", at), ("creation_time", extreme ? new DateTime(2099, 1, 1, 0, 0, 0, DateTimeKind.Unspecified) : at.AddDays(-3).AddMinutes(_random.Next(0, 600))),
                ("min_dop", noDop ? null : Bound(1, 4, extreme, low: true)), ("max_dop", noDop ? null : Bound(4, 16, extreme, low: false)),
                ("total_clr_time", Bound(0, 900, extreme, low: false)), ("plan_generation_num", Bound(1, 9, extreme, low: false)),
                ("query_text", digest is null || id % 4 == 0 ? $"select {id}" : null),
                ("query_text_digest", digest is { } d ? Enumerable.Repeat((byte)d, 32).ToArray() : null),
            };
            foreach (var pair in new[] { "worker_time", "elapsed_time", "physical_reads", "rows", "grant_kb", "used_grant_kb", "ideal_grant_kb", "spills", "reserved_threads", "used_threads" })
            {
                values.Add(($"min_{pair}", Bound(1, 100, extreme, low: true)));
                values.Add(($"max_{pair}", Bound(100, 1000, extreme, low: false)));
            }

            var named = values.Where(v => v.Value is not null).ToList();
            var sql = $"INSERT INTO collect.query_stats ({string.Join(", ", values.Select(v => v.Column))}) VALUES ({string.Join(", ", values.Select(v => v.Value is null ? "NULL" : "@" + v.Column))})";
            await using var command = new NpgsqlCommand(sql, connection);
            foreach (var (column, value) in named)
            {
                command.Parameters.AddWithValue(column, value!);
            }

            await command.ExecuteNonQueryAsync(ct);
        }

        private long? Draw(int max) => _random.Next(0, 6) == 0 ? null : _random.Next(0, max);

        /// <summary>A min or max column. A zero-interval row ("extreme") holds a value no honest row has, below every minimum and above every maximum.</summary>
        private long? Bound(int from, int to, bool extreme, bool low)
        {
            if (extreme)
            {
                return low ? 0 : 5_000_000 + _random.Next(0, 1000);
            }

            return _random.Next(0, 8) == 0 ? null : _random.Next(from, to);
        }
    }

    private static async Task InsertRawAsync(NpgsqlConnection connection, long collectionId, DateTime at, string database, string hash, CancellationToken ct) =>
        await ExecuteAsync(connection, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (@collection_id, @at, @server_id, 'hourly-9164', @database, @hash, @hash, 100, 100, 1, 300)", ct,
            ("collection_id", collectionId), ("at", at), ("server_id", ServerId), ("database", database), ("hash", hash));

    /// <summary>The two tables' columns and indexes as sorted text, so a second run that changed either shows as a different list.</summary>
    private static async Task<List<string>> SchemaAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var lines = new List<string>();
        await using var command = new NpgsqlCommand(@"
SELECT table_name || '.' || column_name || ' ' || data_type || ' ' || is_nullable
FROM information_schema.columns
WHERE table_schema = 'collect' AND table_name IN ('query_stats_host_hourly', 'query_stats_host_hourly_built')
UNION ALL
SELECT tablename || ' ' || indexdef
FROM pg_indexes
WHERE schemaname = 'collect' AND tablename IN ('query_stats_host_hourly', 'query_stats_host_hourly_built')
ORDER BY 1", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(reader.GetString(0));
        }

        return lines;
    }

    private static async Task<int> ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
