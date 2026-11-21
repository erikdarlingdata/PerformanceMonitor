/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The block pre-aggregate of the per-day plan totals (#9225): <see cref="PlanRegressionBlock"/>, the half of the daily builder's tick that
/// sums whole blocks of four days. What a block holds against a hand sum, which blocks exist, when a block stops being valid and is summed
/// again, the garbage collection, and that a block failure never touches the daily tick. The seeds sit at a fixed past date with the late-row
/// trigger disabled and <c>nowUtc</c> passed explicitly, as <see cref="PlanRegressionDailyBuilderLiveTests"/> does. Live facts run when
/// <c>DARLING_TEST_PG</c> is set, each on its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class PlanRegressionBlockLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the PLAN_REGRESSION block pre-aggregate's live pins (each mints its own scratch database).";

    private static readonly DateTime Now = new(2026, 3, 20, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The first day a tick over <see cref="Now"/> builds whole: T - 14.</summary>
    private static readonly DateTime FirstDay = new(2026, 3, 6, 0, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static long s_interval;

    /// <summary>The starts of the blocks whose four days all lie in the days a tick builds whole (T - 14 through T + 1).</summary>
    private static List<DateOnly> WholeBlocks()
    {
        var first = DateOnly.FromDateTime(FirstDay);
        /* The store's clock is months past Now, so tomorrow's stamp (T + 1, built with no totals) is a whole valid day here: the last block runs to it. */
        var last = DateOnly.FromDateTime(Now.AddDays(PlanRegressionDaily.NewestBuiltDayAhead));
        var blocks = new List<DateOnly>();
        for (var start = PlanRegressionBlock.BlockStart(first); start.AddDays(PlanRegressionBlock.BlockDays - 1) <= last; start = start.AddDays(PlanRegressionBlock.BlockDays))
        {
            if (start >= first)
            {
                blocks.Add(start);
            }
        }

        return blocks;
    }

    private static string DateText(DateOnly value) => value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

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

    private static async Task CoverAsync(NpgsqlConnection connection, int serverId, CancellationToken ct, bool enabled = true)
    {
        await ExecAsync(connection,
            $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({serverId}, 'srv{serverId}', {(enabled ? "TRUE" : "FALSE")}) ON CONFLICT (server_id) DO NOTHING", ct);
        await ExecAsync(connection,
            $"INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES ({serverId}, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-03-01 00:00:00')", ct);
    }

    /// <summary>One interval row: first at <paramref name="first"/>, last half an hour after it.</summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, int serverId, string? db, long plan, DateTime first, long execs, long cpu, long dur, bool forced, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.query_store_interval_latest (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count) "
            + "VALUES (@server, @db, 10, @plan, NULL, @interval, @first, @first + interval '1 hour', 'h' || @plan, @execs, @cpu, @dur, @first + interval '30 minutes', @forced, 0)", connection);
        command.Parameters.Add(new NpgsqlParameter("server", NpgsqlDbType.Integer) { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter("db", NpgsqlDbType.Text) { Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("plan", NpgsqlDbType.Bigint) { Value = plan });
        command.Parameters.Add(new NpgsqlParameter("interval", NpgsqlDbType.Bigint) { Value = Interlocked.Increment(ref s_interval) });
        command.Parameters.Add(new NpgsqlParameter("first", NpgsqlDbType.Timestamp) { Value = first });
        command.Parameters.Add(new NpgsqlParameter("execs", NpgsqlDbType.Bigint) { Value = execs });
        command.Parameters.Add(new NpgsqlParameter("cpu", NpgsqlDbType.Bigint) { Value = cpu });
        command.Parameters.Add(new NpgsqlParameter("dur", NpgsqlDbType.Bigint) { Value = dur });
        command.Parameters.Add(new NpgsqlParameter("forced", NpgsqlDbType.Boolean) { Value = forced });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One row on every closed day a tick builds, so every block has rows to sum.</summary>
    private static async Task SeedEveryDayAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        for (var day = FirstDay; day <= Now.Date; day = day.AddDays(1))
        {
            await SeedAsync(connection, 1, "db1", 1, day.AddHours(2), 10, 100, 1000, false, ct);
        }
    }

    private static NpgsqlDataSource DataSource(ScratchPostgres scratch) => NpgsqlDataSource.Create(scratch.ConnectionString);

    /// <summary>The blocks the read would take, as the read asks for them (<see cref="PlanRegressionBlock.ReadValidBlocksAsync"/>).</summary>
    private static async Task<List<DateOnly>> ValidAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var valid = await PlanRegressionBlock.ReadValidBlocksAsync(connection, transaction, 1, FirstDay, 30, null, _ => false, ct);
        await transaction.CommitAsync(ct);
        return valid;
    }

    private static async Task RunLiveAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database of its own, so no other class's migrations race it.
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            /* The trigger's WHEN clause is not under test here, and an old-dated insert would mark days: disable it. */
            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ATickBuildsEveryWholeBlock_EachEqualsAHandSum_AndAnotherTickBuildsNone()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            var whole = WholeBlocks();
            Assert.True(whole.Count >= 3, "a window of 15 days holds at least three whole blocks");
            var target = whole[1];
            var t = target.ToDateTime(TimeOnly.MinValue);

            /* Plan 1 runs on three of the target block's days: 10 x 100 + 20 x 200 + 30 x 300 = 14000 cpu, 10 x 1000 + 20 x 2000 + 30 x 3000 = 140000 dur, one day forced.
               Plan 2 (NULL database) runs on the other day. The rows on the day before and the day after the block belong to the blocks beside it. */
            await SeedAsync(connection, 1, "db1", 1, t.AddHours(2), 10, 100, 1000, false, ct);
            await SeedAsync(connection, 1, "db1", 1, t.AddDays(1).AddHours(2), 20, 200, 2000, true, ct);
            await SeedAsync(connection, 1, "db1", 1, t.AddDays(3).AddHours(2), 30, 300, 3000, false, ct);
            await SeedAsync(connection, 1, null, 2, t.AddDays(2).AddHours(3), 5, 50, 500, false, ct);
            await SeedAsync(connection, 1, "db1", 1, t.AddDays(-1).AddHours(2), 999, 1, 1, false, ct);
            await SeedAsync(connection, 1, "db1", 1, t.AddDays(4).AddHours(2), 999, 1, 1, false, ct);

            await using var source = DataSource(scratch);
            var result = await PlanRegressionDaily.RunTickAsync(source, Now, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);

            Assert.Equal(whole.Count, result.BlocksBuilt);
            Assert.Equal(string.Join(";", whole.Select(DateText)), await ScalarAsync(connection,
                "SELECT string_agg(block_start::text, ';' ORDER BY block_start) FROM collect.plan_regression_block_built", ct));
            Assert.Equal(whole, await ValidAsync(connection, ct));
            Assert.Equal("db1|1|60|14000|140000|true|" + t.AddDays(3).AddHours(2).AddMinutes(30).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + ";<null>|2|5|250|2500|false|" + t.AddDays(2).AddHours(3).AddMinutes(30).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                await ScalarAsync(connection,
                    $"SELECT string_agg(COALESCE(database_name, '<null>') || '|' || plan_id || '|' || execs || '|' || cpu_us_sum || '|' || dur_us_sum || '|' || is_forced_plan::text || '|' || to_char(last_exec, 'YYYY-MM-DD HH24:MI:SS'), ';' ORDER BY plan_id) FROM collect.plan_regression_block WHERE block_start = DATE '{DateText(target)}'", ct));

            /* Every block's sums equal the daily rows of its four days, and no daily row of a day outside the whole blocks is summed. */
            Assert.Equal(0L, await ScalarAsync(connection,
                @"SELECT count(*) FROM
                  (
                      (SELECT b.block_start, b.database_name, b.query_id, b.plan_id, b.execs, b.cpu_us_sum, b.dur_us_sum FROM collect.plan_regression_block AS b
                       EXCEPT
                       SELECT k.block_start, d.database_name, d.query_id, d.plan_id, SUM(d.execs), SUM(d.cpu_us_sum), SUM(d.dur_us_sum)
                       FROM collect.plan_regression_block_built AS k JOIN collect.plan_regression_daily AS d ON d.server_id = k.server_id AND d.day >= k.block_start AND d.day < k.block_start + 4
                       GROUP BY k.block_start, d.database_name, d.query_id, d.plan_id)
                      UNION ALL
                      (SELECT k.block_start, d.database_name, d.query_id, d.plan_id, SUM(d.execs), SUM(d.cpu_us_sum), SUM(d.dur_us_sum)
                       FROM collect.plan_regression_block_built AS k JOIN collect.plan_regression_daily AS d ON d.server_id = k.server_id AND d.day >= k.block_start AND d.day < k.block_start + 4
                       GROUP BY k.block_start, d.database_name, d.query_id, d.plan_id
                       EXCEPT
                       SELECT b.block_start, b.database_name, b.query_id, b.plan_id, b.execs, b.cpu_us_sum, b.dur_us_sum FROM collect.plan_regression_block AS b)
                  ) AS diff", ct));

            var again = await PlanRegressionDaily.RunTickAsync(source, Now, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
            Assert.Equal(0, again.BlocksBuilt);
        });
    }

    [Fact]
    public async Task ABlockWithAMissingDay_IsNotValid_ALateRowMakesAnotherStale_AndTheNextTickSumsOnlyTheStaleOne()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await SeedEveryDayAsync(connection, ct);
            var whole = WholeBlocks();
            var rebuilt = whole[0];
            var missing = whole[1];
            var untouched = whole.Skip(2).ToList();

            await using var source = DataSource(scratch);
            await PlanRegressionDaily.RunTickAsync(source, Now, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
            Assert.Equal(whole, await ValidAsync(connection, ct));

            /* A day of the second block loses its built row: the block no longer has four whole days. */
            await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(missing.AddDays(2))}'", ct);
            /* A late row lands on a day of the first block (the late-row trigger moves late_seq, which the test disabled) and the day is built again: it is whole
               again, but with a new built_seq, so the block's stamp no longer matches. */
            var day = rebuilt.AddDays(1);
            await SeedAsync(connection, 1, "db1", 1, day.ToDateTime(TimeOnly.MinValue).AddHours(5), 7, 100, 1000, false, ct);
            await ExecAsync(connection, $"UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1 WHERE server_id = 1 AND day = DATE '{DateText(day)}'", ct);
            Assert.NotNull(await PlanRegressionDaily.BuildDayAsync(connection, 1, day, Now, ct));

            Assert.Equal(untouched, await ValidAsync(connection, ct));

            /* The next tick builds the missing day again. No late row moved its late_seq, so it carries the built_seq the block was summed from and the block is valid
               again without a new sum. The first block is summed again, with the new interval in it. */
            var tick = await PlanRegressionDaily.RunTickAsync(source, Now, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
            Assert.Equal(1, tick.BlocksBuilt);
            Assert.Equal(whole, await ValidAsync(connection, ct));
            Assert.Equal(47L, await ScalarAsync(connection,
                $"SELECT execs::bigint FROM collect.plan_regression_block WHERE server_id = 1 AND block_start = DATE '{DateText(rebuilt)}' AND plan_id = 1", ct));
        });
    }

    [Fact]
    public async Task TheGc_RemovesExpiredBlocksAndTheBlocksOfADisabledServer_AndKeepsTheRest()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await CoverAsync(connection, 9, ct, enabled: false);
            var keep = WholeBlocks()[0];
            const string Block = "INSERT INTO collect.plan_regression_block (server_id, block_start, database_name, query_id, plan_id, query_plan_hash, execs) VALUES ({0}, DATE '{1}', 'db1', 1, 1, 'h', 1)";
            const string Built = "INSERT INTO collect.plan_regression_block_built (server_id, block_start, day_seqs) VALUES ({0}, DATE '{1}', ARRAY[0,0,0,0]::bigint[])";

            /* Now is 03-20, so the horizon is 03-04 (T - 16): a block that starts on 02-28 is gone and one that starts on 03-04 stays. Server 9 is not enabled: all its blocks go. */
            foreach (var (server, start) in new[] { (1, "2026-02-28"), (1, "2026-03-04"), (1, DateText(keep)), (9, DateText(keep)) })
            {
                await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, Block, server, start), ct);
                await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, Built, server, start), ct);
            }

            Assert.Equal(2, await PlanRegressionBlock.GcAsync(connection, Now, ct));

            var expected = "1|2026-03-04;1|" + DateText(keep);
            Assert.Equal(expected, await ScalarAsync(connection, "SELECT string_agg(server_id || '|' || block_start, ';' ORDER BY server_id, block_start) FROM collect.plan_regression_block", ct));
            Assert.Equal(expected, await ScalarAsync(connection, "SELECT string_agg(server_id || '|' || block_start, ';' ORDER BY server_id, block_start) FROM collect.plan_regression_block_built", ct));
        });
    }

    [Fact]
    public async Task ABlockThatFailsToSum_IsLoggedAndLeavesNoStamp_AndTheDailyTickIsUntouched()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await SeedEveryDayAsync(connection, ct);
            /* Any insert into the block table now fails, and every block has rows. */
            await ExecAsync(connection, "ALTER TABLE collect.plan_regression_block ADD CONSTRAINT zz_refuse CHECK (false) NOT VALID", ct);

            var logger = new CapturingTestLogger();
            await using var source = DataSource(scratch);
            var result = await PlanRegressionDaily.RunTickAsync(source, Now, logger, ct);

            Assert.Equal(PlanRegressionDaily.WindowDays + 1, result.Built);
            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.Skipped);
            Assert.Equal(0, result.BlocksBuilt);
            Assert.Equal(WholeBlocks().Count, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("PLAN_REGRESSION blocks: summing", logger.Joined, StringComparison.Ordinal);
            Assert.Equal((long)PlanRegressionDaily.WindowDays + 1, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built WHERE built_seq = late_seq AND day <= DATE '2026-03-20'", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_block_built", ct));
        });
    }

    [Fact]
    public async Task AFailedBlockStep_IsLoggedAndNeverFailsTheDailyTick_AndTheBlockReadAnswersNone()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await SeedEveryDayAsync(connection, ct);
            /* A store whose block step cannot run: the stamp table is gone, so the step's first statement throws. */
            await ExecAsync(connection, "DROP TABLE collect.plan_regression_block_built", ct);

            var logger = new CapturingTestLogger();
            await using var source = DataSource(scratch);
            var result = await PlanRegressionDaily.RunTickAsync(source, Now, logger, ct);

            Assert.Equal(PlanRegressionDaily.WindowDays + 1, result.Built);
            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.BlocksBuilt);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("the block step failed", logger.Joined, StringComparison.Ordinal);
            Assert.Equal((long)PlanRegressionDaily.WindowDays + 1, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built WHERE built_seq = late_seq AND day <= DATE '2026-03-20'", ct));

            /* The read's block lookup rolls back to its savepoint and answers none, so the snapshot it runs in is still usable. */
            await using var transaction = await connection.BeginTransactionAsync(ct);
            Assert.Empty(await PlanRegressionBlock.ReadValidBlocksAsync(connection, transaction, 1, FirstDay, 30, null, _ => false, ct));
            await using (var probe = new NpgsqlCommand("SELECT 1", connection, transaction))
            {
                Assert.Equal(1, await probe.ExecuteScalarAsync(ct));
            }

            await transaction.CommitAsync(ct);
        });
    }
}
