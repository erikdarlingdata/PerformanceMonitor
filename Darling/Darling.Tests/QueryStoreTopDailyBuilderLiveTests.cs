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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the builder of the daily Query Store summary (#5094), <see cref="QueryStoreTopDaily"/>. The summary is
/// approximate by design: a day is summarized after it ends, and a row that changes afterwards is missed or, for about a
/// day, counted twice. These facts pin what the builder does exactly (a built day equals a direct GROUP BY of the wide
/// table, to the second), when it builds (two passes, the oldest days first, a cap per tick), and the one residual the
/// second pass clears. Every seed is at a fixed past date and every call passes <c>nowUtc</c> explicitly, so no fact
/// reads the clock. Live facts run when <c>DARLING_TEST_PG</c> is set, each on its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreTopDailyBuilderLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the daily summary builder's live pins (each mints its own scratch database).";

    private const int RetentionDays = 9;

    private static readonly DateTime Day = new(2026, 1, 10, 0, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string At(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Enrolls the server (enabled unless told otherwise) and gives it a wide-table coverage claim.</summary>
    private static async Task CoverAsync(
        NpgsqlConnection connection, int serverId, CancellationToken ct, string filledSince = "2025-12-01 00:00:00", bool enabled = true)
    {
        await ExecAsync(connection,
            $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({serverId}, 'srv{serverId}', {(enabled ? "TRUE" : "FALSE")}) ON CONFLICT (server_id) DO NOTHING", ct);
        await ExecAsync(connection,
            $"INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES ({serverId}, TIMESTAMP '{filledSince}', TIMESTAMP '2026-03-01 00:00:00')", ct);
    }

    private static int s_interval;

    /// <summary>One wide-table row. Unset average columns stay NULL; the interval began ten minutes before it was collected.</summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, long queryId, CancellationToken ct,
        string? moduleName = null, string? replicaRole = null, long? executionCount = null, long? duration = null,
        long? cpu = null, long? reads = null, long? writes = null, long? physical = null, long? rowcount = null,
        DateTime? lastExecution = null, string? planHash = null, DateTime? firstExecution = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
 avg_physical_io_reads, avg_rowcount, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
VALUES
(@ct, @server, 'db1', @query, @query, 'Regular', @first, @last,
 @module, 'h' || @query, @ec, @dur, @cpu, @reads, @writes,
 @phys, @rows, @ph, @replica, @interval, @ct - interval '10 minutes')", connection);
        command.Parameters.Add(new NpgsqlParameter("ct", NpgsqlDbType.Timestamp) { Value = collectionTime });
        command.Parameters.Add(new NpgsqlParameter("server", NpgsqlDbType.Integer) { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter("query", NpgsqlDbType.Bigint) { Value = queryId });
        command.Parameters.Add(new NpgsqlParameter("first", NpgsqlDbType.Timestamp) { Value = firstExecution ?? collectionTime.AddMinutes(-10) });
        command.Parameters.Add(new NpgsqlParameter("last", NpgsqlDbType.Timestamp) { Value = (object?)lastExecution ?? collectionTime });
        command.Parameters.Add(new NpgsqlParameter("module", NpgsqlDbType.Text) { Value = (object?)moduleName ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("ec", NpgsqlDbType.Bigint) { Value = (object?)executionCount ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("dur", NpgsqlDbType.Bigint) { Value = (object?)duration ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("cpu", NpgsqlDbType.Bigint) { Value = (object?)cpu ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("reads", NpgsqlDbType.Bigint) { Value = (object?)reads ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("writes", NpgsqlDbType.Bigint) { Value = (object?)writes ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("phys", NpgsqlDbType.Bigint) { Value = (object?)physical ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("rows", NpgsqlDbType.Bigint) { Value = (object?)rowcount ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("ph", NpgsqlDbType.Text) { Value = (object?)planHash ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("replica", NpgsqlDbType.Text) { Value = (object?)replicaRole ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("interval", NpgsqlDbType.Bigint) { Value = (long)Interlocked.Increment(ref s_interval) });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlDataSource DataSource(ScratchPostgres scratch) => NpgsqlDataSource.Create(scratch.ConnectionString);

    private static async Task<long> TickBuiltAsync(NpgsqlDataSource source, DateTime nowUtc)
    {
        var result = await QueryStoreTopDaily.RunTickAsync(source, nowUtc, RetentionDays, NullLogger.Instance, TestContext.Current.CancellationToken);
        Assert.Equal(0, result.Failed);
        return result.Built;
    }

    private static async Task<(short Pass, DateTime BuiltAt, long SourceRows)?> BuiltRowAsync(NpgsqlConnection connection, int serverId, DateTime day, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT pass, built_at, source_rows FROM collect.query_store_top_daily_built WHERE server_id = {serverId} AND day = DATE '{At(day)[..10]}'", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetInt16(0), reader.GetDateTime(1), reader.GetInt64(2)) : null;
    }

    private static async Task<long> SummarizedRowsAsync(NpgsqlConnection connection, int serverId, DateTime day, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection,
            $"SELECT COALESCE(sum(interval_rows), 0) FROM collect.query_store_top_daily WHERE server_id = {serverId} AND day = DATE '{At(day)[..10]}'", ct));

    private static async Task RunLiveAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ABuiltDay_EqualsADirectGroupByOfTheWideRows_EveryColumn_NullsIncluded()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            /* Group 1: three intervals, one with every average NULL. Group 2: every average NULL on both rows (so
               _n = 0 and _sum is NULL). Group 3: a module and a replica role. Group 4: another server, same day. */
            await SeedAsync(connection, 1, Day.AddHours(3), 1, ct, executionCount: 5, duration: 100, cpu: 10, reads: 1000, writes: 7, physical: 2, rowcount: 3, planHash: "a");
            await SeedAsync(connection, 1, Day.AddHours(4), 1, ct, executionCount: 7, duration: 200, cpu: 30, reads: 3000, writes: 9, physical: 4, rowcount: 5, planHash: "b", lastExecution: Day.AddHours(9));
            await SeedAsync(connection, 1, Day.AddHours(5), 1, ct);
            await SeedAsync(connection, 1, Day.AddHours(6), 2, ct, executionCount: 11);
            await SeedAsync(connection, 1, Day.AddHours(7), 2, ct);
            await SeedAsync(connection, 1, Day.AddHours(8), 3, ct, moduleName: "dbo.p", replicaRole: "PRIMARY", executionCount: 2, duration: 50, cpu: 5);
            await SeedAsync(connection, 2, Day.AddHours(8), 1, ct, executionCount: 99, duration: 1);
            await CoverAsync(connection, 1, ct);
            await CoverAsync(connection, 2, ct);

            var sourceRows = await QueryStoreTopDaily.BuildDayAsync(connection, 1, DateOnly.FromDateTime(Day), 1, Day.AddDays(1).AddHours(2), ct);
            Assert.Equal(6L, sourceRows);

            const string Direct = @"
SELECT server_id, DATE '2026-01-10' AS day, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name,
       count(*) AS interval_rows, sum(execution_count) AS execution_count_sum,
       sum(avg_duration_us::numeric) AS avg_duration_us_sum, count(avg_duration_us) AS avg_duration_us_n,
       sum(avg_cpu_time_us::numeric) AS avg_cpu_time_us_sum, count(avg_cpu_time_us) AS avg_cpu_time_us_n,
       sum(avg_logical_io_reads::numeric) AS avg_logical_io_reads_sum, count(avg_logical_io_reads) AS avg_logical_io_reads_n,
       sum(avg_logical_io_writes::numeric) AS avg_logical_io_writes_sum, count(avg_logical_io_writes) AS avg_logical_io_writes_n,
       sum(avg_physical_io_reads::numeric) AS avg_physical_io_reads_sum, count(avg_physical_io_reads) AS avg_physical_io_reads_n,
       sum(avg_rowcount::numeric) AS avg_rowcount_sum, count(avg_rowcount) AS avg_rowcount_n,
       max(last_execution_time) AS last_execution_time_max, max(query_plan_hash) AS query_plan_hash_max, min(first_execution_time) AS first_execution_time_min
FROM collect.query_store_interval_wide
WHERE server_id = 1 AND collection_time >= TIMESTAMP '2026-01-10' AND collection_time < TIMESTAMP '2026-01-11'
GROUP BY server_id, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name";
            const string Stored = "SELECT * FROM collect.query_store_top_daily WHERE server_id = 1 AND day = DATE '2026-01-10'";

            Assert.Equal(3L, await ScalarAsync(connection, $"SELECT count(*) FROM ({Stored}) s", ct));
            Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM (({Stored}) EXCEPT ({Direct})) x", ct));
            Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM (({Direct}) EXCEPT ({Stored})) x", ct));

            /* The all-NULL group keeps its row: _n = 0, _sum NULL; and the key's NULLs stay NULL. */
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM collect.query_store_top_daily WHERE server_id = 1 AND query_id = 2 AND avg_duration_us_n = 0 AND avg_duration_us_sum IS NULL AND avg_rowcount_n = 0 AND replica_role IS NULL AND module_name IS NULL AND execution_count_sum = 11 AND interval_rows = 2", ct));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM collect.query_store_top_daily WHERE server_id = 1 AND query_id = 1 AND avg_duration_us_sum = 300 AND avg_duration_us_n = 2 AND interval_rows = 3 AND execution_count_sum = 12 AND query_plan_hash_max = 'b'", ct));

            Assert.Equal((short)1, (await BuiltRowAsync(connection, 1, Day, ct))!.Value.Pass);
            Assert.Equal(6L, (await BuiltRowAsync(connection, 1, Day, ct))!.Value.SourceRows);
            Assert.Null(await BuiltRowAsync(connection, 2, Day, ct));
            Assert.Equal(0L, await SummarizedRowsAsync(connection, 2, Day, ct));
        });
    }

    [Fact]
    public async Task TheDayIsHalfOpen_RowsAtMidnightStartTheDay_AndNextMidnightIsNotIn()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await SeedAsync(connection, 1, Day.AddTicks(-10), 1, ct, executionCount: 1);
            await SeedAsync(connection, 1, Day, 2, ct, executionCount: 1);
            await SeedAsync(connection, 1, Day.AddDays(1).AddTicks(-10), 3, ct, executionCount: 1);
            await SeedAsync(connection, 1, Day.AddDays(1), 4, ct, executionCount: 1);
            await CoverAsync(connection, 1, ct);

            Assert.Equal(2L, await QueryStoreTopDaily.BuildDayAsync(connection, 1, DateOnly.FromDateTime(Day), 1, Day.AddDays(1).AddHours(2), ct));
            Assert.Equal(new long[] { 2, 3 },
                (await ScalarAsync(connection, "SELECT array_agg(query_id ORDER BY query_id) FROM collect.query_store_top_daily WHERE server_id = 1 AND day = DATE '2026-01-10'", ct) as long[])!);
        });
    }

    [Fact]
    public async Task TheSchedule_BuildsAtDayEndPlusTwoHours_RebuildsAtPlusTwentyFive_AndThenStops()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await SeedAsync(connection, 1, Day.AddHours(5), 1, ct, executionCount: 1, duration: 10);
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);
            var dayEnd = Day.AddDays(1);

            await TickBuiltAsync(source, dayEnd.AddHours(1).AddMinutes(59));
            Assert.Null(await BuiltRowAsync(connection, 1, Day, ct));

            var pass1At = dayEnd.AddHours(2);
            await TickBuiltAsync(source, pass1At);
            var built = await BuiltRowAsync(connection, 1, Day, ct);
            Assert.Equal((short)1, built!.Value.Pass);
            Assert.Equal(pass1At, built.Value.BuiltAt);
            Assert.Equal(1L, built.Value.SourceRows);

            await TickBuiltAsync(source, dayEnd.AddHours(24).AddMinutes(59));
            built = await BuiltRowAsync(connection, 1, Day, ct);
            Assert.Equal((short)1, built!.Value.Pass);
            Assert.Equal(pass1At, built.Value.BuiltAt);

            var pass2At = dayEnd.AddHours(25);
            await TickBuiltAsync(source, pass2At);
            built = await BuiltRowAsync(connection, 1, Day, ct);
            Assert.Equal((short)2, built!.Value.Pass);
            Assert.Equal(pass2At, built.Value.BuiltAt);

            await TickBuiltAsync(source, dayEnd.AddDays(3));
            built = await BuiltRowAsync(connection, 1, Day, ct);
            Assert.Equal((short)2, built!.Value.Pass);
            Assert.Equal(pass2At, built.Value.BuiltAt);

            /* Nothing is planned for the day once it is final. */
            await using var planConnection = await source.OpenConnectionAsync(ct);
            var plan = await QueryStoreTopDaily.PlanBuildsAsync(planConnection, dayEnd.AddDays(3), RetentionDays, ct);
            Assert.DoesNotContain(plan, b => b.Day == DateOnly.FromDateTime(Day));
        });
    }

    [Fact]
    public async Task TheDocumentedResidual_ARowMovedIntoTheNextDay_IsCountedTwiceUntilThePassTwoRebuild()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await SeedAsync(connection, 1, Day.AddHours(10), 1, ct, executionCount: 1);
            await SeedAsync(connection, 1, Day.AddHours(23).AddMinutes(30), 2, ct, executionCount: 1);
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);
            var dayEnd = Day.AddDays(1);

            await TickBuiltAsync(source, dayEnd.AddHours(2));
            Assert.Equal(2L, await SummarizedRowsAsync(connection, 1, Day, ct));

            /* The interval resumed: its row now belongs to the next day. */
            await ExecAsync(connection, "UPDATE collect.query_store_interval_wide SET collection_time = TIMESTAMP '2026-01-11 00:30:00' WHERE query_id = 2", ct);
            Assert.Equal(2L, await SummarizedRowsAsync(connection, 1, Day, ct));

            await TickBuiltAsync(source, dayEnd.AddHours(25));
            Assert.Equal(1L, await SummarizedRowsAsync(connection, 1, Day, ct));
            Assert.Equal(1L, (await BuiltRowAsync(connection, 1, Day, ct))!.Value.SourceRows);
        });
    }

    [Fact]
    public async Task AnEmptyDay_GetsABuiltRowWithZeroSourceRows_AndNoSummaryRows()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);

            await TickBuiltAsync(source, Day.AddDays(1).AddHours(2));

            var built = await BuiltRowAsync(connection, 1, Day, ct);
            Assert.Equal((short)1, built!.Value.Pass);
            Assert.Equal(0L, built.Value.SourceRows);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily", ct)));
        });
    }

    [Fact]
    public async Task AServerWithoutACoverageClaim_IsNotBuilt()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await SeedAsync(connection, 1, Day.AddHours(5), 1, ct, executionCount: 1);
            await using var source = DataSource(scratch);

            Assert.Equal(0L, await TickBuiltAsync(source, Day.AddDays(1).AddHours(2)));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built", ct)));
        });
    }

    [Fact]
    public async Task Gc_RemovesDaysBelowTheCutoff_AndOnlyThose()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            /* now 2026-01-20 12:00, retention 9 days: the cutoff is (2026-01-11)::date - 1 = 2026-01-10. */
            await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name) VALUES (1, 'srv1')", ct);
            foreach (var d in new[] { 8, 9, 10, 11 })
            {
                await ExecAsync(connection,
                    $"INSERT INTO collect.query_store_top_daily (server_id, day, interval_rows, avg_duration_us_n, avg_cpu_time_us_n, avg_logical_io_reads_n, avg_logical_io_writes_n, avg_physical_io_reads_n, avg_rowcount_n) VALUES (1, DATE '2026-01-{d:00}', 1, 0, 0, 0, 0, 0, 0)", ct);
                await ExecAsync(connection,
                    $"INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) VALUES (1, DATE '2026-01-{d:00}', 2, TIMESTAMP '2026-01-15 00:00:00', 1)", ct);
            }

            var removed = await QueryStoreTopDaily.GcAsync(connection, new DateTime(2026, 1, 20, 12, 0, 0, DateTimeKind.Unspecified), RetentionDays, ct);

            Assert.Equal(2L, removed);
            Assert.Equal("2026-01-10,2026-01-11", await ScalarAsync(connection, "SELECT string_agg(day::text, ',' ORDER BY day) FROM collect.query_store_top_daily", ct));
            Assert.Equal("2026-01-10,2026-01-11", await ScalarAsync(connection, "SELECT string_agg(day::text, ',' ORDER BY day) FROM collect.query_store_top_daily_built", ct));
        });
    }

    [Fact]
    public async Task ABacklogLargerThanTheCap_BuildsTheOldestDaysFirst_AndExactlyTheCap()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);
            var now = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Unspecified);
            const int Retention = 100;

            var result = await QueryStoreTopDaily.RunTickAsync(source, now, Retention, NullLogger.Instance, ct);

            Assert.Equal(QueryStoreTopDaily.MaxBuildsPerTick, result.Built);
            Assert.Equal(0, result.Failed);
            Assert.Equal((long)QueryStoreTopDaily.MaxBuildsPerTick, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built", ct)));
            var first = (now - TimeSpan.FromDays(Retention)).Date.AddDays(2);
            Assert.Equal(first, (DateTime)(await ScalarAsync(connection, "SELECT min(day)::timestamp FROM collect.query_store_top_daily_built", ct))!);
            Assert.Equal(first.AddDays(QueryStoreTopDaily.MaxBuildsPerTick - 1), (DateTime)(await ScalarAsync(connection, "SELECT max(day)::timestamp FROM collect.query_store_top_daily_built", ct))!);

            /* Every one of those days was already past its final-build time, so each was built once, at pass 2. */
            Assert.Equal((long)QueryStoreTopDaily.MaxBuildsPerTick, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built WHERE pass = 2", ct)));
            Assert.Equal(0, result.BuiltPass1);
        });
    }

    [Fact]
    public async Task ABacklogPastTheFinalThreshold_BuildsEachDayOnceAtPassTwo_AndTheNextTickReachesNewerDays()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);
            var now = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Unspecified);
            const int Retention = 150;
            var cap = QueryStoreTopDaily.MaxBuildsPerTick;
            var first = (now - TimeSpan.FromDays(Retention)).Date.AddDays(2);

            var one = await QueryStoreTopDaily.RunTickAsync(source, now, Retention, NullLogger.Instance, ct);
            Assert.Equal(cap, one.BuiltPass2);
            Assert.Equal(0, one.BuiltPass1);
            var firstBuiltAt = (DateTime)(await ScalarAsync(connection, "SELECT built_at FROM collect.query_store_top_daily_built WHERE day = DATE '" + At(first)[..10] + "'", ct))!;

            var two = await QueryStoreTopDaily.RunTickAsync(source, now, Retention, NullLogger.Instance, ct);
            Assert.Equal(0, two.Failed);
            Assert.Equal(0, two.BuiltPass1);
            Assert.Equal(cap, two.BuiltPass2);

            /* The second tick built the next days; it did not redo the oldest ones. */
            Assert.Equal((long)(2 * cap), Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built WHERE pass = 2", ct)));
            Assert.Equal(first.AddDays(2 * cap - 1), (DateTime)(await ScalarAsync(connection, "SELECT max(day)::timestamp FROM collect.query_store_top_daily_built", ct))!);
            Assert.Equal(firstBuiltAt, (DateTime)(await ScalarAsync(connection, "SELECT built_at FROM collect.query_store_top_daily_built WHERE day = DATE '" + At(first)[..10] + "'", ct))!);
        });
    }

    [Fact]
    public async Task ALateWriteIntoABuiltDay_IsMissed_AsDocumented()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await SeedAsync(connection, 1, Day.AddHours(5), 1, ct, executionCount: 1);
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);
            var dayEnd = Day.AddDays(1);

            await TickBuiltAsync(source, dayEnd.AddHours(2));
            await TickBuiltAsync(source, dayEnd.AddHours(25));
            var finalBuild = await BuiltRowAsync(connection, 1, Day, ct);
            Assert.Equal((short)2, finalBuild!.Value.Pass);
            Assert.Equal(1L, await SummarizedRowsAsync(connection, 1, Day, ct));

            /* A backfill lands a wide row in the built day. Nothing plans the day again, so the summary does not see it. */
            await SeedAsync(connection, 1, Day.AddHours(6), 2, ct, executionCount: 1);
            await TickBuiltAsync(source, dayEnd.AddDays(3));

            Assert.Equal(1L, await SummarizedRowsAsync(connection, 1, Day, ct));
            Assert.Equal(finalBuild, await BuiltRowAsync(connection, 1, Day, ct));
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(connection,
                "SELECT count(*) FROM collect.query_store_interval_wide WHERE server_id = 1 AND collection_time >= TIMESTAMP '2026-01-10' AND collection_time < TIMESTAMP '2026-01-11'", ct)));
        });
    }

    [Fact]
    public async Task TheFirstExecutionRange_KeepsTheRowsAtItsEdges_AndDropsTheOneAtTheUpperEdge()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            var collected = Day.AddHours(12);
            var upper = Day.AddDays(1) + QueryStoreTopDaily.SkewSlack;

            /* 1: the oldest start a stored row can have (a second inside the span margin plus catch-up). 2 and 3: a start
               just inside the upper edge, and exactly on it. 4: an ordinary row. */
            await SeedAsync(connection, 1, collected, 1, ct, executionCount: 1, duration: 10,
                firstExecution: collected - QueryStoreTopDaily.FinalBuildAfter + TimeSpan.FromSeconds(1));
            await SeedAsync(connection, 1, collected, 2, ct, executionCount: 2, duration: 20, firstExecution: upper - TimeSpan.FromSeconds(1));
            await SeedAsync(connection, 1, collected, 3, ct, executionCount: 4, duration: 40, firstExecution: upper);
            await SeedAsync(connection, 1, collected, 4, ct, executionCount: 8, duration: 80);
            await CoverAsync(connection, 1, ct);

            Assert.Equal(3L, await QueryStoreTopDaily.BuildDayAsync(connection, 1, DateOnly.FromDateTime(Day), 1, Day.AddDays(1).AddHours(2), ct));

            const string Direct = @"
SELECT server_id, DATE '2026-01-10' AS day, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name,
       count(*) AS interval_rows, sum(execution_count) AS execution_count_sum,
       sum(avg_duration_us::numeric) AS avg_duration_us_sum, count(avg_duration_us) AS avg_duration_us_n,
       min(first_execution_time) AS first_execution_time_min
FROM collect.query_store_interval_wide
WHERE server_id = 1 AND collection_time >= TIMESTAMP '2026-01-10' AND collection_time < TIMESTAMP '2026-01-11'
  AND first_execution_time < TIMESTAMP '2026-01-11 01:00:00'
GROUP BY server_id, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name";
            const string Stored = @"
SELECT server_id, day, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name,
       interval_rows, execution_count_sum, avg_duration_us_sum, avg_duration_us_n, first_execution_time_min
FROM collect.query_store_top_daily WHERE server_id = 1 AND day = DATE '2026-01-10'";

            Assert.Equal(new long[] { 1, 2, 4 },
                (await ScalarAsync(connection, "SELECT array_agg(query_id ORDER BY query_id) FROM collect.query_store_top_daily WHERE server_id = 1", ct) as long[])!);
            Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM (({Stored}) EXCEPT ({Direct})) x", ct));
            Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM (({Direct}) EXCEPT ({Stored})) x", ct));
            Assert.Equal(3L, (await BuiltRowAsync(connection, 1, Day, ct))!.Value.SourceRows);
        });
    }

    [Fact]
    public async Task TheOldestPlannedDay_IsTwoDaysAfterTheRetentionHorizon()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);
            var now = new DateTime(2026, 1, 20, 12, 0, 0, DateTimeKind.Unspecified);

            await using var planConnection = await source.OpenConnectionAsync(ct);
            var plan = await QueryStoreTopDaily.PlanBuildsAsync(planConnection, now, RetentionDays, ct);

            /* (now - 9 days)::date is 2026-01-11; the purge may already have cut part of 2026-01-12. */
            Assert.Equal(new DateOnly(2026, 1, 13), plan.Min(b => b.Day));
            Assert.Equal(new DateOnly(2026, 1, 19), plan.Max(b => b.Day));
            Assert.Equal(7, plan.Count);
        });
    }

    [Fact]
    public async Task OnlyEnabledServers_AndOnlyDaysAfterTheirCoverageClaim_ArePlanned_AndADisabledServersRowsAreCollected()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            const int Retention = 20;
            var now = new DateTime(2026, 1, 20, 12, 0, 0, DateTimeKind.Unspecified);
            await CoverAsync(connection, 1, ct, filledSince: "2026-01-08 15:00:00");
            await CoverAsync(connection, 2, ct, enabled: false);
            await SeedAsync(connection, 1, new DateTime(2026, 1, 9, 5, 0, 0), 1, ct, executionCount: 1);
            foreach (var d in new[] { 10, 11 })
            {
                await ExecAsync(connection,
                    $"INSERT INTO collect.query_store_top_daily (server_id, day, interval_rows, avg_duration_us_n, avg_cpu_time_us_n, avg_logical_io_reads_n, avg_logical_io_writes_n, avg_physical_io_reads_n, avg_rowcount_n) VALUES (2, DATE '2026-01-{d:00}', 1, 0, 0, 0, 0, 0, 0)", ct);
                await ExecAsync(connection,
                    $"INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) VALUES (2, DATE '2026-01-{d:00}', 2, TIMESTAMP '2026-01-15 00:00:00', 1)", ct);
            }

            await using var source = DataSource(scratch);
            await using (var planConnection = await source.OpenConnectionAsync(ct))
            {
                var plan = await QueryStoreTopDaily.PlanBuildsAsync(planConnection, now, Retention, ct);
                Assert.DoesNotContain(plan, b => b.ServerId == 2);
                Assert.All(plan, b => Assert.Equal(1, b.ServerId));
                Assert.Equal(new DateOnly(2026, 1, 9), plan.Min(b => b.Day));
                Assert.Equal(new DateOnly(2026, 1, 19), plan.Max(b => b.Day));
            }

            var result = await QueryStoreTopDaily.RunTickAsync(source, now, Retention, NullLogger.Instance, ct);

            Assert.Equal(0, result.Failed);
            Assert.Equal(11, result.Built);
            Assert.Equal(2L, result.DaysRemoved);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily WHERE server_id = 2", ct)));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built WHERE server_id = 2", ct)));
            Assert.Equal(11L, Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built WHERE server_id = 1", ct)));
            Assert.Equal(1L, await SummarizedRowsAsync(connection, 1, new DateTime(2026, 1, 9), ct));
            Assert.Null(await BuiltRowAsync(connection, 1, new DateTime(2026, 1, 8), ct));
        });
    }

    [Fact]
    public async Task ATickPastItsTimeBudget_StopsStartingBuilds_AndReportsTheRestDeferred()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await using var source = DataSource(scratch);
            var now = new DateTime(2026, 1, 20, 12, 0, 0, DateTimeKind.Unspecified);

            /* The elapsed time is read before each build: zero before the first, past the budget from then on. */
            var reads = 0;
            var result = await QueryStoreTopDaily.RunTickAsync(source, now, RetentionDays, NullLogger.Instance,
                () => reads++ == 0 ? TimeSpan.Zero : QueryStoreTopDaily.MaxTickDuration, ct);

            Assert.Equal(1, result.Built);
            Assert.Equal(6, result.Deferred);
            Assert.Equal(0, result.Failed);
            Assert.Equal("2026-01-13", await ScalarAsync(connection, "SELECT string_agg(day::text, ',') FROM collect.query_store_top_daily_built", ct));

            /* The next tick plans what was deferred. */
            var next = await QueryStoreTopDaily.RunTickAsync(source, now, RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(6, next.Built);
            Assert.Equal(0, next.Deferred);
        });
    }

    [Fact]
    public void TheTenant_IsTheLastAwaitInsideTheTimescaleGate_WithItsOwnCatchAll()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);
        var tick = Body(code, "private async Task RunStoreMaintenanceTickAsync(CancellationToken stoppingToken)");
        Assert.False(string.IsNullOrEmpty(tick), "could not locate RunStoreMaintenanceTickAsync");

        const string Converge = "await ConvergeStoreObjectsAsync(stoppingToken);";
        const string Build = "await BuildQueryStoreTopDailyAsync(stoppingToken);";
        var convergeAt = tick.IndexOf(Converge, StringComparison.Ordinal);
        var buildAt = tick.IndexOf(Build, StringComparison.Ordinal);
        Assert.True(convergeAt >= 0 && buildAt > convergeAt, "the summary builder is awaited after the store-object convergence");
        Assert.Equal(1, tick.Split(Build).Length - 1);
        Assert.True(string.IsNullOrWhiteSpace(tick[(convergeAt + Converge.Length)..buildAt]), "nothing sits between the convergence and the builder");
        Assert.True(tick[(buildAt + Build.Length)..].TrimStart().StartsWith('}'), "the summary builder is the last await inside the TimescaleDB gate (only the closing brace of the gated block follows it)");
        Assert.Equal(';', tick[..buildAt].TrimEnd()[^1]);

        var builder = Body(code, "private async Task BuildQueryStoreTopDailyAsync(CancellationToken stoppingToken)");
        Assert.False(string.IsNullOrEmpty(builder), "could not locate BuildQueryStoreTopDailyAsync");
        Assert.Contains("catch (Exception ex) when (ex is not OperationCanceledException)", builder, StringComparison.Ordinal);
        Assert.Contains("QueryStoreTopDaily.RunTickAsync(", builder, StringComparison.Ordinal);
        Assert.Contains("DarlingRetention.QueryStoreIntervalWideRetentionDays", builder, StringComparison.Ordinal);
        Assert.DoesNotContain("throw", builder, StringComparison.Ordinal);
    }

    [Fact]
    public void TheConstants_AreTheDocumentedOnes()
    {
        Assert.Equal(TimeSpan.FromHours(2), QueryStoreTopDaily.BuildAfter);
        Assert.Equal(TimeSpan.FromHours(25), QueryStoreTopDaily.FinalBuildAfter);
        Assert.Equal(60, QueryStoreTopDaily.MaxBuildsPerTick);
        Assert.Equal(60, QueryStoreTopDaily.BuildStatementTimeoutSeconds);
        Assert.Equal(TimeSpan.FromMinutes(10), QueryStoreTopDaily.MaxTickDuration);
        Assert.Equal(TimeSpan.FromHours(1), QueryStoreTopDaily.SkewSlack);
        Assert.Contains("60s", typeof(QueryStoreTopDaily).GetField("StatementTimeoutSql", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.GetValue(null)!.ToString(), StringComparison.Ordinal);
    }

    private static string Body(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        if (start < 0)
        {
            return string.Empty;
        }

        var open = source.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                return source[(open + 1)..i];
            }
        }

        return string.Empty;
    }
}
