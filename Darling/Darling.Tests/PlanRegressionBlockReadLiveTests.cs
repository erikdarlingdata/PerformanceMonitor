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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PLAN_REGRESSION read over whole blocks (#9225). The statement that reads <c>plan_regression_block</c> for the days whole blocks cover
/// returns the rows the daily statement returns, and asks the daily table for fewer rows (counted from the plan's actual rows). A block that
/// is stale after a rebuilt day, and a store without the block tables, read daily rows and give the same fact. Seeded through
/// <see cref="LiveClock"/> and built by the daily tick, so the blocks are the ones the product builds. Live facts run when
/// <c>DARLING_TEST_PG</c> is set, each on its own scratch store.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionBlockReadLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the PLAN_REGRESSION block read's live pins (each mints its own scratch database).";
    private const int ServerId = 9225301;
    private const int QueriesPerDay = 120;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig
    {
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Source { get; init; }
        public required DateTime End { get; init; }
        public required DateTime Now { get; init; }
        public required DateTime WindowFloor { get; init; }

        /// <summary>How many whole 4-day blocks the days the tick built allow (<see cref="WholeBlocks"/>).</summary>
        public required int ExpectedBlocks { get; init; }
    }

    /// <summary>
    /// The aligned blocks whose days are all whole builds. Today is a partial build, and so is yesterday before the partial watermark lag has passed,
    /// so the rig's whole days are 13 or 14 consecutive days. Blocks are aligned to the day number, so those days hold three whole blocks on some dates
    /// and only two on others (one date in four). The read is right either way: it takes the blocks there are and reads the other days from daily rows.
    /// </summary>
    private static int WholeBlocks(IEnumerable<PlanRegressionDaily.BuiltDay> built)
    {
        var whole = built.Where(b => b.Through is null).Select(b => b.Day).ToHashSet();
        return whole.Select(PlanRegressionBlock.BlockStart).Distinct()
            .Count(start => Enumerable.Range(0, PlanRegressionBlock.BlockDays).All(i => whole.Contains(start.AddDays(i))));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertIntervalAsync(
        NpgsqlConnection connection, long queryId, long planId, string hash, long cpuUs, long execs, DateTime firstExec, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES (@s, 'blockdb', @q, @p, NULL, @p, @first, @col, @h, @qh, @execs, @cpu, @dur, @last, false, 0)", connection);
        cmd.Parameters.AddWithValue("s", ServerId);
        cmd.Parameters.AddWithValue("q", queryId);
        cmd.Parameters.AddWithValue("p", planId);
        cmd.Parameters.AddWithValue("first", NpgsqlDbType.Timestamp, firstExec);
        cmd.Parameters.AddWithValue("col", NpgsqlDbType.Timestamp, firstExec.AddMinutes(35));
        cmd.Parameters.AddWithValue("h", hash);
        cmd.Parameters.AddWithValue("qh", "qh" + queryId);
        cmd.Parameters.AddWithValue("execs", execs);
        cmd.Parameters.AddWithValue("cpu", cpuUs);
        cmd.Parameters.AddWithValue("dur", cpuUs * 3);
        cmd.Parameters.AddWithValue("last", NpgsqlDbType.Timestamp, firstExec.AddMinutes(30));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// A store with a busy-enough server (a few hundred query and plan keys a day over 17 days), one regressed query (a cheap plan on each of eight
    /// days with a different cost every day, a costly plan in the last hour), the coverage claim, and one daily tick, which builds the days and the blocks.
    /// </summary>
    private static async Task RunLiveAsync(Func<Rig, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var now = LiveClock.Now();
            var end = now;
            var windowFloor = PgFactCollector.PlanRegressionWindowFloor(end.AddHours(-4).AddDays(-PgFactCollector.PlanRegressionWindowDays));

            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
            await ExecAsync(connection, $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({ServerId}, 'blocksrv', TRUE) ON CONFLICT (server_id) DO NOTHING", ct);
            await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
SELECT
    @s, 'blockdb', q, q * 10 + p, NULL, (q % 22) + 1,
    d.d + (q % 22) * interval '1 hour',
    d.d + (q % 22) * interval '1 hour' + interval '35 minutes',
    'hash' || (q * 10 + p), 'qh' || q, 10 + (q % 5), 1000 + ((q + p) % 13) * 100 + (@t::date - d.d::date) * 7, 3000 + (@t::date - d.d::date) * 3,
    d.d + (q % 22) * interval '1 hour' + interval '30 minutes', (q % 17 = 0 AND p = 1), (q % 3)
FROM generate_series((@t::date - 16)::timestamp, @t::date::timestamp, interval '1 day') AS d (d)
CROSS JOIN generate_series(1, @q) AS q
CROSS JOIN generate_series(1, 2) AS p", connection) { CommandTimeout = 120 })
            {
                seed.Parameters.AddWithValue("s", ServerId);
                seed.Parameters.AddWithValue("t", NpgsqlDbType.Timestamp, now);
                seed.Parameters.AddWithValue("q", QueriesPerDay);
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* One regression: a cheap plan on eight built days, each at a different cost, so the baseline depends on every day's sum; a costly plan in the last hour. */
            for (var back = 13; back >= 6; back--)
            {
                await InsertIntervalAsync(connection, 900001, 9000011, "0xCHEAP", 8000 + back * 250, 40, now.Date.AddDays(-back).AddHours(3), ct);
            }

            await InsertIntervalAsync(connection, 900001, 9000012, "0xCOSTLY", 900_000, 100, end.AddHours(-1), ct);
            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

            await using (var coverage = new NpgsqlCommand(
                "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES (@s, @f, @f) ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since", connection))
            {
                coverage.Parameters.AddWithValue("s", ServerId);
                coverage.Parameters.AddWithValue("f", NpgsqlDbType.Timestamp, now.Date.AddDays(-40));
                await coverage.ExecuteNonQueryAsync(ct);
            }

            var tick = await PlanRegressionDaily.RunTickAsync(source, now, NullLogger.Instance, ct);
            Assert.True(tick.Built >= 14, "the tick builds the window's days");
            var builtAfterTick = await QueryStoreIntervalLatest.ReadBuiltDaysAsync(connection, null, ServerId, windowFloor, 60, null, _ => false, ct);
            var expectedBlocks = WholeBlocks(builtAfterTick);
            Assert.True(expectedBlocks >= 2, "the window's whole days hold at least two whole blocks, on any date");
            Assert.Equal(expectedBlocks, tick.BlocksBuilt);
            await ExecAsync(connection, "ANALYZE collect.plan_regression_daily", ct);
            await ExecAsync(connection, "ANALYZE collect.plan_regression_block", ct);

            await body(new Rig { Connection = connection, Source = source, End = end, Now = now, WindowFloor = windowFloor, ExpectedBlocks = expectedBlocks }, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private readonly record struct Read(List<string> Rows, long DailyRowsRead);

    /// <summary>The statement the collector would run for these days and blocks: its rows (every column, sorted) and the rows its plan read from the daily table.</summary>
    private static async Task<Read> ReadAsync(Rig rig, List<PlanRegressionDaily.BuiltDay> builtDays, List<DateOnly>? blocks, List<DateOnly>? leftover, CancellationToken ct)
    {
        await ExecAsync(rig.Connection, "SET max_parallel_workers_per_gather = 0", ct);
        var sql = blocks is null ? PgFactCollector.PlanRegressionDailySql : PgFactCollector.PlanRegressionDailyBlocksSql;
        var liveFloor = PgFactCollector.PlanRegressionLiveFloorWithWatermarks(rig.WindowFloor, builtDays);

        NpgsqlCommand Bind(string text)
        {
            var cmd = new NpgsqlCommand(text, rig.Connection) { CommandTimeout = 120 };
            cmd.Parameters.AddWithValue(ServerId);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, rig.WindowFloor);
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = builtDays.Select(b => b.Day).ToArray() });
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, liveFloor);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, rig.WindowFloor.AddDays(-1));
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = builtDays.Select(b => b.ThroughOrDayEnd).ToArray() });
            if (blocks is not null)
            {
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = leftover!.ToArray() });
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = blocks.ToArray() });
            }

            return cmd;
        }

        var rows = new List<string>();
        await using (var run = Bind(sql))
        await using (var reader = await run.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i =>
                    reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
            }
        }

        await using var explain = Bind("EXPLAIN (ANALYZE, FORMAT JSON) " + sql);
        var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!).RootElement[0].GetProperty("Plan");
        rows.Sort(StringComparer.Ordinal);
        return new Read(rows, DailyRowsRead(plan));
    }

    /// <summary>The rows the plan's nodes on the daily table returned, over all their loops.</summary>
    private static long DailyRowsRead(JsonElement node)
    {
        long total = 0;
        if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "plan_regression_daily")
        {
            total += (long)(node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble());
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            total += children.EnumerateArray().Sum(DailyRowsRead);
        }

        return total;
    }

    private static async Task<long> CountDailyRowsAsync(Rig rig, IEnumerable<DateOnly> days, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM collect.plan_regression_daily WHERE server_id = @s AND day = ANY(@d)", rig.Connection);
        cmd.Parameters.AddWithValue("s", ServerId);
        cmd.Parameters.Add(new NpgsqlParameter("d", NpgsqlDbType.Array | NpgsqlDbType.Date) { Value = days.ToArray() });
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task<(List<PlanRegressionDaily.BuiltDay> Built, List<DateOnly> Blocks, List<DateOnly> Leftover)> PlanAsync(Rig rig, CancellationToken ct)
    {
        var built = await QueryStoreIntervalLatest.ReadBuiltDaysAsync(rig.Connection, null, ServerId, rig.WindowFloor, 60, null, _ => false, ct);
        await using var transaction = await rig.Connection.BeginTransactionAsync(ct);
        var valid = await PlanRegressionBlock.ReadValidBlocksAsync(rig.Connection, transaction, ServerId, rig.WindowFloor, 60, null, _ => false, ct);
        await transaction.CommitAsync(ct);
        var (blocks, leftover) = PlanRegressionBlock.Select(built, valid);
        return (built, blocks, leftover);
    }

    private static AnalysisContext NewContext(Rig rig, bool anchored = false) => new()
    {
        ServerId = ServerId,
        ServerName = "blocksrv",
        TimeRangeStart = rig.End.AddHours(-4),
        TimeRangeEnd = rig.End,
        ServerUtcOffset = TimeSpan.Zero,
        AsOfUtc = anchored ? rig.End : null,
    };

    private readonly record struct Answer(string Fact, List<string> Offenders, int Blocks, int LeftoverDays);

    /// <summary>The fact the real collector builds, with the block plan its read took.</summary>
    private static async Task<Answer> FactAsync(Rig rig, bool anchored = false)
    {
        var blocks = -1;
        var leftover = -1;
        PgFactCollector.TestOnlyBlockPlan.Value = (b, d) => { blocks = b.Count; leftover = d.Count; };
        try
        {
            var context = NewContext(rig, anchored);
            var facts = await new PgFactCollector(rig.Source).CollectFactsAsync(context);
            var fact = facts.Single(f => f.Key == "PLAN_REGRESSION");
            var text = string.Join(";", fact.Metadata.OrderBy(m => m.Key, StringComparer.Ordinal).Select(m => m.Key + "=" + m.Value.ToString("R", CultureInfo.InvariantCulture)))
                + "|" + fact.Value.ToString("R", CultureInfo.InvariantCulture);
            Assert.True(context.PlanRegressionWindowStart is not null, "the read took the built days");
            return new Answer(text, (context.PlanRegressionOffenders ?? []).Select(o => o.DatabaseName + "/" + o.QueryId).OrderBy(x => x, StringComparer.Ordinal).ToList(), blocks, leftover);
        }
        finally
        {
            PgFactCollector.TestOnlyBlockPlan.Value = null;
        }
    }

    [Fact]
    public async Task TheBlockStatement_ReturnsTheDailyStatementsRows_AndAsksTheDailyTableForFewerRows()
    {
        await RunLiveAsync(async (rig, ct) =>
        {
            var (built, blocks, leftover) = await PlanAsync(rig, ct);
            Assert.Equal(rig.ExpectedBlocks, blocks.Count);
            Assert.True(leftover.Count < built.Count, "the blocks' days leave the daily list");

            var daily = await ReadAsync(rig, built, null, null, ct);
            var viaBlocks = await ReadAsync(rig, built, blocks, leftover, ct);

            Assert.NotEmpty(daily.Rows);
            Assert.Equal(daily.Rows, viaBlocks.Rows);

            /* The daily table is asked for the leftover days only: the rows the plan read are the rows of those days, and fewer than the daily statement read. */
            Assert.Equal(await CountDailyRowsAsync(rig, built.Select(b => b.Day), ct), daily.DailyRowsRead);
            Assert.Equal(await CountDailyRowsAsync(rig, leftover, ct), viaBlocks.DailyRowsRead);
            Assert.True(viaBlocks.DailyRowsRead < daily.DailyRowsRead / 2, $"daily rows read: {viaBlocks.DailyRowsRead} with blocks, {daily.DailyRowsRead} without");
        });
    }

    [Fact]
    public async Task ABlockWhoseDayWasRebuilt_IsReadFromDailyRows_AndTheAnswerIsTheSame()
    {
        await RunLiveAsync(async (rig, ct) =>
        {
            var (_, blocks, leftover) = await PlanAsync(rig, ct);

            /* A late row lands on a day of the first block and the day is built again: whole, with a new built_seq, so the block's stamp is stale. */
            var stale = blocks[0];
            var day = stale.AddDays(1);
            await ExecAsync(rig.Connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
            await InsertIntervalAsync(rig.Connection, 900001, 9000011, "0xCHEAP", 4000, 60, day.ToDateTime(TimeOnly.MinValue).AddHours(5), ct);
            await ExecAsync(rig.Connection, $"UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1 WHERE server_id = {ServerId} AND day = DATE '{day:yyyy-MM-dd}'", ct);
            Assert.NotNull(await PlanRegressionDaily.BuildDayAsync(rig.Connection, ServerId, day, rig.Now, ct));

            var (builtAfter, blocksAfter, leftoverAfter) = await PlanAsync(rig, ct);
            Assert.DoesNotContain(stale, blocksAfter);
            Assert.Equal(blocks.Count - 1, blocksAfter.Count);
            Assert.Equal(leftover.Count + PlanRegressionBlock.BlockDays, leftoverAfter.Count);
            for (var i = 0; i < PlanRegressionBlock.BlockDays; i++)
            {
                Assert.Contains(stale.AddDays(i), leftoverAfter);
            }

            /* The statement over the remaining blocks gives the daily statement's rows, including the late row's day. */
            var daily = await ReadAsync(rig, builtAfter, null, null, ct);
            var viaBlocks = await ReadAsync(rig, builtAfter, blocksAfter, leftoverAfter, ct);
            Assert.Equal(daily.Rows, viaBlocks.Rows);

            /* And the fact: the same answer through the collector as with no block taken at all. */
            var factAfter = await FactAsync(rig);
            Assert.Equal(blocksAfter.Count, factAfter.Blocks);
            Assert.Equal(leftoverAfter.Count, factAfter.LeftoverDays);
            await ExecAsync(rig.Connection, "DELETE FROM collect.plan_regression_block_built", ct);
            var factDaily = await FactAsync(rig);
            Assert.Equal(0, factDaily.Blocks);
            Assert.Equal(factDaily.Fact, factAfter.Fact);
            Assert.Equal(factDaily.Offenders, factAfter.Offenders);
        });
    }

    [Fact]
    public async Task TheFact_IsTheSameWithAndWithoutBlocks_AndAStoreWithoutTheBlockTablesReadsDailyRows()
    {
        await RunLiveAsync(async (rig, ct) =>
        {
            var withBlocks = await FactAsync(rig);
            Assert.True(withBlocks.Blocks >= rig.ExpectedBlocks, "the read takes the blocks the tick built");
            Assert.True(withBlocks.LeftoverDays > 0 && withBlocks.LeftoverDays < 14, "the days no block covers are read from the daily rows");
            Assert.Contains("blockdb/900001", withBlocks.Offenders);

            /* An older store: the stamp table does not exist. The read answers "no blocks" under its savepoint and runs the daily statement. */
            await ExecAsync(rig.Connection, "DROP TABLE collect.plan_regression_block_built", ct);
            var without = await FactAsync(rig);
            Assert.Equal(0, without.Blocks);

            Assert.Equal(withBlocks.Fact, without.Fact);
            Assert.Equal(withBlocks.Offenders, without.Offenders);
        });
    }

    [Fact]
    public async Task TheAnchoredFact_TakesBlocks_AndIsTheSameAsWithNoBlockTaken()
    {
        await RunLiveAsync(async (rig, ct) =>
        {
            /* A pass anchored at an instant runs the anchored block statement, whose window end is $9 after the two block lists. */
            var withBlocks = await FactAsync(rig, anchored: true);
            Assert.True(withBlocks.Blocks >= 2, "the anchored read takes the whole valid blocks of the days that were whole by its end");
            Assert.True(withBlocks.LeftoverDays > 0, "the days no block covers are read from the daily rows and the interval table");
            Assert.Contains("blockdb/900001", withBlocks.Offenders);

            await ExecAsync(rig.Connection, "DELETE FROM collect.plan_regression_block_built", ct);
            var without = await FactAsync(rig, anchored: true);
            Assert.Equal(0, without.Blocks);
            Assert.True(without.LeftoverDays > withBlocks.LeftoverDays, "with no block the daily list holds every built day");

            Assert.Equal(without.Fact, withBlocks.Fact);
            Assert.Equal(without.Offenders, withBlocks.Offenders);
        });
    }
}
