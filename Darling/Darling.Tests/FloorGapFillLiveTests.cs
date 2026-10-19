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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6081: the hourly retention pass fills a small rollup floor gap by itself. Live pins on a scratch store: raw rows
/// that arrive after the hourly refresh moved past them leave the rollup starting one bucket late, the coverage gate
/// holds the raw purge, and the pass refreshes that bucket (and the dailies above it) instead of waiting for
/// <c>--backfill-rollups</c>.
///
/// <para>Each seed follows the real order of events: the rollups are refreshed over the hours the store already holds
/// FIRST (which moves the invalidation threshold past them), and only then do the late rows land below the rollup's
/// floor. Seeded the other way round, the late rows would sit below an untouched threshold, log no invalidation, and a
/// plain refresh over them would materialize nothing.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class FloorGapFillLiveTests
{
    private const string Raw = "query_store_stats";
    private const int SetupTimeoutSeconds = 120;
    private const int RowsPerHour = 3;
    private const int LateRows = 40;

    private static readonly string Hourly = TimescaleSupport.QueryStoreStatsHourlyView;
    private static readonly string IntervalHourly = TimescaleSupport.QueryStoreStatsIntervalHourlyView;

    /// <summary>The scratch store: migrated, TimescaleDB on, hypertables and aggregates in place, refresh policies and
    /// background workers off so only the test refreshes anything.</summary>
    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #6081 floor-gap pins.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        NpgsqlConnection? connection = null;
        try
        {
            var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
            connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* #1922: the extension is created on the probe's own connection, never the one this method hands back. */
            var enabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            Assert.SkipWhen(!enabled, "The live #6081 floor-gap pins need TimescaleDB.");

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
            {
                await ExecAsync(connection, $"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)", ct);
            }

            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            return (connection, scratch);
        }
        catch
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }

            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    /// <summary>Raw rows, <paramref name="perStep"/> per step, one step per <paramref name="step"/> from <paramref name="from"/>
    /// for <paramref name="steps"/> steps.</summary>
    private static async Task SeedRawAsync(NpgsqlConnection connection, DateTime from, int steps, TimeSpan step, int perStep, CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
SELECT
    (extract(epoch FROM $1::timestamp)::bigint * 100000) + (s * 1000) + n,
    $1::timestamp + (s * $2::interval) + (n * interval '1 millisecond'),
    -6081, 'FILL01', 'AdventureWorks', 'dbo.GetOrders', '0xABCD',
    17, 19, 'Regular', 'PRIMARY',
    s, $1::timestamp + (s * $2::interval), $1::timestamp + (s * $2::interval),
    n, 100, 40, 900, 400
FROM generate_series(0, $3 - 1) AS s
CROSS JOIN generate_series(1, $4) AS n", connection) { CommandTimeout = SetupTimeoutSeconds };
        seed.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        seed.Parameters.AddWithValue(step);
        seed.Parameters.AddWithValue(steps);
        seed.Parameters.AddWithValue(perStep);
        await seed.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = SetupTimeoutSeconds };
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime?> FloorAsync(NpgsqlConnection connection, string view, CancellationToken ct)
        => (DateTime?)await ScalarAsync(connection, $"SELECT min(bucket) FROM collect.{view}", ct);

    private static async Task<bool> ArmedAsync(NpgsqlConnection connection, CancellationToken ct)
        => (bool)(await ScalarAsync(connection, TimescaleSupport.RawArmedStateSql(Raw), ct))!;

    /// <summary>
    /// The timeline every pin shares. The store holds <c>Hours</c> complete hours of raw, <c>B1</c> onward; both hourly
    /// consumers are refreshed from <c>refreshFromHourly</c> / <c>refreshFromInterval</c> (default <c>B1</c>); and then
    /// <paramref name="lateRows"/> raw rows land in the bucket <c>B0</c> just below, which neither rollup has.
    /// </summary>
    private sealed record Timeline(DateTime B0, DateTime B1, DateTime End);

    private static async Task<Timeline> SeedAsync(
        NpgsqlConnection connection, int lateRows, TimeSpan? intervalRefreshOffset, CancellationToken ct)
    {
        const int Hours = 30;
        var end = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1)) - TimeSpan.FromHours(1);
        var b0 = end - TimeSpan.FromHours(Hours);
        var b1 = b0 + TimeSpan.FromHours(1);

        await SeedRawAsync(connection, b1, Hours, TimeSpan.FromHours(1), RowsPerHour, ct);
        await RefreshAsync(connection, Hourly, b1, end + TimeSpan.FromHours(1), ct);
        await RefreshAsync(connection, IntervalHourly, b1 + (intervalRefreshOffset ?? TimeSpan.Zero), end + TimeSpan.FromHours(1), ct);

        /* The late arrival: below the threshold the refreshes above just moved, so the invalidation is logged. */
        await SeedRawAsync(connection, b0 + TimeSpan.FromMinutes(10), 1, TimeSpan.FromHours(1), lateRows, ct);
        return new Timeline(b0, b1, end);
    }

    private static Task<TimescaleSupport.RetentionPolicySweepSummary> PeriodicPassAsync(
        NpgsqlConnection connection, CapturingTestLogger logger, Func<bool>? otherRepairRunning, CancellationToken ct)
        => TimescaleSupport.EnsureRetentionPoliciesAsync(
            connection, logger, TimescaleSupport.RetentionSweepPass.Periodic, otherRepairRunning, ct);

    private static string HeldLine(CapturingTestLogger logger)
        => logger.Lines.Single(l => l.Contains($"Retention policy for {Raw} HELD PAUSED", StringComparison.Ordinal));

    private static async Task DisposeAsync(NpgsqlConnection connection, ScratchPostgres scratch)
    {
        await connection.DisposeAsync();
        await scratch.DisposeAsync();
    }

    /// <summary>
    /// The case #6071 measured, scaled down: late raw rows one bucket below both hourly consumers' floors. The pass
    /// fills the bucket on each consumer, writes one INFORMATION line per fill, and carries the daily above over the
    /// same day. The purge is still held on that pass (the measurement ran first) and the NEXT pass arms it.
    /// </summary>
    [Fact]
    public async Task OneBucketBelowTheFloor_IsFilledByTheHourlyPass_AndTheNextPassArms()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, scratch) = await OpenAsync(ct);
        try
        {
            var t = await SeedAsync(connection, LateRows, null, ct);
            Assert.Equal(t.B1, await FloorAsync(connection, Hourly, ct));
            Assert.Equal(t.B1, await FloorAsync(connection, IntervalHourly, ct));
            Assert.Null(await FloorAsync(connection, TimescaleSupport.QueryStoreStatsDailyView, ct));

            var first = new CapturingTestLogger();
            await PeriodicPassAsync(connection, first, () => false, ct);

            Assert.Equal(t.B0, await FloorAsync(connection, Hourly, ct));
            Assert.Equal(t.B0, await FloorAsync(connection, IntervalHourly, ct));
            Assert.Equal(TimescaleSupport.AlignDown(t.B0, TimeSpan.FromDays(1)), await FloorAsync(connection, TimescaleSupport.QueryStoreStatsDailyView, ct));

            var fillLines = first.Lines.Where(l => l.StartsWith("Information: Floor-gap fill (#6081)", StringComparison.Ordinal)).ToList();
            Assert.Equal(2, fillLines.Count);
            Assert.Contains(fillLines, l => l.Contains(Hourly, StringComparison.Ordinal) && l.Contains($"held {LateRows} row(s)", StringComparison.Ordinal));
            Assert.Contains(fillLines, l => l.Contains(IntervalHourly, StringComparison.Ordinal) && l.Contains(Raw, StringComparison.Ordinal));
            Assert.False(await ArmedAsync(connection, ct), "the pass measured the gap before it filled it, so this pass still holds");

            var second = new CapturingTestLogger();
            await PeriodicPassAsync(connection, second, () => false, ct);
            Assert.True(await ArmedAsync(connection, ct), $"the next pass measures both consumers covered and arms the purge; log: {second.Joined}");
            Assert.DoesNotContain(second.Lines, l => l.Contains("Floor-gap fill", StringComparison.Ordinal));
        }
        finally
        {
            await DisposeAsync(connection, scratch);
        }
    }

    /// <summary>
    /// Two short consumers, two remedies: the hourly rollup is one bucket short (the service fills it), the interval
    /// rollup starts more than a day above raw (past the fill, so the line sends the operator to the verb for that one
    /// only). The line names both, and the wide one is not refreshed.
    /// </summary>
    [Fact]
    public async Task TwoShortConsumers_AreBothNamed_EachWithItsOwnRemedy_AndAGapOverADayIsNotRefreshed()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, scratch) = await OpenAsync(ct);
        try
        {
            var t = await SeedAsync(connection, LateRows, TimeSpan.FromHours(26), ct);
            var intervalFloor = await FloorAsync(connection, IntervalHourly, ct);
            Assert.Equal(t.B1 + TimeSpan.FromHours(26), intervalFloor);

            var log = new CapturingTestLogger();
            await PeriodicPassAsync(connection, log, () => false, ct);

            var held = HeldLine(log);
            Assert.Contains(Hourly, held, StringComparison.Ordinal);
            Assert.Contains(IntervalHourly, held, StringComparison.Ordinal);
            Assert.Contains($"{Hourly} - the service filled it this pass", held, StringComparison.Ordinal);
            Assert.Contains($"{IntervalHourly} - the gap is wider than 1 day, past the service's own fill: run ", held, StringComparison.Ordinal);
            Assert.Contains("--backfill-rollups", held, StringComparison.Ordinal);

            Assert.Equal(t.B0, await FloorAsync(connection, Hourly, ct));
            Assert.Equal(intervalFloor, await FloorAsync(connection, IntervalHourly, ct));
            Assert.Single(log.Lines, l => l.StartsWith("Information: Floor-gap fill (#6081)", StringComparison.Ordinal));
            Assert.False(await ArmedAsync(connection, ct), "the interval rollup is still short, so the purge stays held");

            await PeriodicPassAsync(connection, new CapturingTestLogger(), () => false, ct);
            Assert.False(await ArmedAsync(connection, ct), "a gap past the fill never arms by itself");
        }
        finally
        {
            await DisposeAsync(connection, scratch);
        }
    }

    /// <summary>
    /// A gap that holds more rows than <see cref="TimescaleSupport.FloorGapFillMaxRows"/> is left to the verb: no
    /// refresh, the purge stays held, and the line names the verb for each consumer. The probe stops at the bound: it
    /// reports bound + 1, not the rows the gap holds.
    /// </summary>
    [Fact]
    public async Task AGapOverTheRowBound_IsNotRefreshed_AndTheProbeStopsAtTheBound()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, scratch) = await OpenAsync(ct);
        try
        {
            var lateRows = TimescaleSupport.FloorGapFillMaxRows + 5_000;
            var t = await SeedAsync(connection, lateRows, null, ct);

            var log = new CapturingTestLogger();
            await PeriodicPassAsync(connection, log, () => false, ct);

            Assert.Equal(t.B1, await FloorAsync(connection, Hourly, ct));
            Assert.Equal(t.B1, await FloorAsync(connection, IntervalHourly, ct));
            Assert.DoesNotContain(log.Lines, l => l.Contains("Floor-gap fill (#6081)", StringComparison.Ordinal));
            var held = HeldLine(log);
            Assert.Contains($"{Hourly} - the gap holds more than {TimescaleSupport.FloorGapFillMaxRows:N0} rows, past the service's own fill: run ", held, StringComparison.Ordinal);
            Assert.Contains($"{IntervalHourly} - the gap holds more than {TimescaleSupport.FloorGapFillMaxRows:N0} rows, past the service's own fill: run ", held, StringComparison.Ordinal);
            Assert.False(await ArmedAsync(connection, ct));

            var fills = await TimescaleSupport.FillSmallFloorGapsAsync(
                connection, null, Raw, "collection_time", new[] { Hourly }, fill: true, otherRepairRunning: null, ct);
            var fill = Assert.Single(fills);
            Assert.Equal(TimescaleSupport.FloorGapOutcome.TooManyRows, fill.Outcome);
            Assert.Equal(TimescaleSupport.FloorGapFillMaxRows + 1L, fill.Rows);
        }
        finally
        {
            await DisposeAsync(connection, scratch);
        }
    }

    /// <summary>
    /// The row count touches only the chunks of the gap: with raw spread over many one-day chunks, the plan for the
    /// gap's window names at most the chunk or two the window overlaps, never the whole table.
    /// </summary>
    [Fact]
    public async Task TheGapProbe_TouchesOnlyTheChunksBelowTheFloor()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, scratch) = await OpenAsync(ct);
        try
        {
            var t = await SeedAsync(connection, LateRows, null, ct);
            var chunks = (long)(await ScalarAsync(connection,
                "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats'", ct))!;
            Assert.True(chunks >= 3, $"the seed must span several chunks or this pin proves nothing (got {chunks})");

            static string Literal(DateTime value) => $"'{value:yyyy-MM-dd HH:mm:ss}'::timestamp";
            var sql = TimescaleSupport.FloorGapRowCountSql(Raw, "collection_time", string.Empty)
                .Replace("$1", Literal(t.B0), StringComparison.Ordinal)
                .Replace("$2", Literal(t.B1), StringComparison.Ordinal)
                .Replace("$3", "100001", StringComparison.Ordinal);
            var lines = new List<string>();
            await using (var explain = new NpgsqlCommand("EXPLAIN " + sql, connection) { CommandTimeout = SetupTimeoutSeconds })
            await using (var reader = await explain.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    lines.Add(reader.GetString(0));
                }
            }

            var scanned = Regex.Matches(string.Join("\n", lines), @"_hyper_\d+_\d+_chunk").Select(m => m.Value).Distinct().Count();
            Assert.True(scanned is >= 1 and <= 2, $"the gap probe should read the chunk its window overlaps (1 or 2 across a day boundary), not {scanned} of {chunks}:\n{string.Join("\n", lines)}");
        }
        finally
        {
            await DisposeAsync(connection, scratch);
        }
    }

    /// <summary>
    /// While another repair holds the aggregates the pass fills nothing and says so at Debug; the line tells the
    /// operator the service retries. The next pass, with the repair finished, fills the gap.
    /// </summary>
    [Fact]
    public async Task WhileAnotherRepairRuns_TheFillSkips_AndTheNextPassFills()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, scratch) = await OpenAsync(ct);
        try
        {
            var t = await SeedAsync(connection, LateRows, null, ct);

            var busy = new CapturingTestLogger();
            await PeriodicPassAsync(connection, busy, () => true, ct);
            Assert.Equal(t.B1, await FloorAsync(connection, Hourly, ct));
            Assert.Equal(t.B1, await FloorAsync(connection, IntervalHourly, ct));
            Assert.Contains(busy.Lines, l => l.StartsWith("Debug: Floor-gap fill (#6081): skipping", StringComparison.Ordinal));
            Assert.DoesNotContain(busy.Lines, l => l.StartsWith("Information: Floor-gap fill", StringComparison.Ordinal));
            Assert.Contains("the service tries to fill it again next pass", HeldLine(busy), StringComparison.Ordinal);

            var free = new CapturingTestLogger();
            await PeriodicPassAsync(connection, free, () => false, ct);
            Assert.Equal(t.B0, await FloorAsync(connection, Hourly, ct));
            Assert.Equal(t.B0, await FloorAsync(connection, IntervalHourly, ct));
        }
        finally
        {
            await DisposeAsync(connection, scratch);
        }
    }

    /// <summary>
    /// The startup pass never fills (the hole repair runs there): it still names every short consumer, saying the
    /// service fills a small gap on its hourly pass, and refreshes nothing.
    /// </summary>
    [Fact]
    public async Task TheStartupPass_NamesEveryShortConsumer_AndRefreshesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, scratch) = await OpenAsync(ct);
        try
        {
            var t = await SeedAsync(connection, LateRows, null, ct);

            var log = new CapturingTestLogger();
            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, log, ct);

            Assert.Equal(t.B1, await FloorAsync(connection, Hourly, ct));
            Assert.Equal(t.B1, await FloorAsync(connection, IntervalHourly, ct));
            var held = HeldLine(log);
            Assert.Contains(Hourly, held, StringComparison.Ordinal);
            Assert.Contains(IntervalHourly, held, StringComparison.Ordinal);
            Assert.Contains("on its hourly pass", held, StringComparison.Ordinal);
        }
        finally
        {
            await DisposeAsync(connection, scratch);
        }
    }
}
