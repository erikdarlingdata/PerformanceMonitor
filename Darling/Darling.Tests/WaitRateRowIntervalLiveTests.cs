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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8106: <c>sample_interval_seconds</c> is a per-row figure, the seconds since that wait type's own previous sighting. A type that
/// returns after missing some passes stores several cadences while its siblings in the same collection store one, so a read that
/// divides the collection's summed wait by its longest row interval deflates every collection that holds a returning row. The
/// wait-rate anomaly's window, the young-baseline bar and the stored WaitMsPerSec baseline all rate each row over its own interval
/// instead, so the window and the baseline it is scored against agree. The rows are the issue's: two steady types at 500 and
/// 100 ms/s every minute, and a third seen every fifth minute (600 ms over its own 300 s, 2 ms/s). A collection credits every row
/// measured over a positive interval whole and rates the sum over its cadence, the shortest interval any of its rows measured, so the
/// returning row's 600 ms lands on the collection that observed it: 48 collections read 600, 12 read 610, and the hour averages 602,
/// the sum of the per-type rates (500 + 100 + 2) and the hour's 2,167,200 ms over its 3,600 s.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): each test mints its own scratch database through <see cref="ScratchPostgres"/>,
/// builds it the way the service does (ladder, TimescaleDB, hypertables, ensure sweep), and drops it whole.</para>
/// </summary>
public sealed class WaitRateRowIntervalLiveTests
{
    private const int ServerId = 8106;
    private const string ServerName = "row-interval-server";

    /// <summary>The window's clock for <c>$4..$6</c>: no transition inside the window and a zero offset, so local time is UTC.</summary>
    private static readonly DateTime NoTransition = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The seeded hour: two days ago at 09:00 UTC, inside the baseline tier and the backfill horizon.</summary>
    private static DateTime SeededHour => DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2).Date.AddHours(9), DateTimeKind.Unspecified);

    [Fact]
    public async Task TheAnomalyWindowAndTheYoungBaselineBar_RateEachRowOverItsOwnInterval()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8106 window test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await BuildStoreAsync(scratch, ct);
        var hour = SeededHour;
        await SeedTheIssuesHourAsync(connection, hour, ct);

        await using (var window = new NpgsqlCommand(PgAnomalyDetector.WaitRateTileWindowSql, connection))
        {
            BindWindow(window, hour);
            await using var reader = await window.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), "the hour tiles to one local-hour row");
            Assert.Equal(610.0, reader.GetDouble(1), 2);
            Assert.Equal(602.0, reader.GetDouble(2), 2);
            Assert.Equal(60, Convert.ToInt32(reader.GetValue(4)));
            Assert.False(await reader.ReadAsync(ct));
        }

        await using (var bar = new NpgsqlCommand(PgAnomalyDetector.YoungBaselineBarPeakSql, connection))
        {
            bar.Parameters.AddWithValue(ServerId);
            bar.Parameters.AddWithValue(hour);
            bar.Parameters.AddWithValue(hour.AddHours(1));
            Assert.Equal(610.0, Convert.ToDouble(await bar.ExecuteScalarAsync(ct)), 2);
        }
    }

    /// <summary>The stored baseline over the successor supply, read through the provider's own WaitMsPerSec arm: every collection's
    /// wait is rated over its cadence, so the hour's mean is 602, the sum of the per-type rates, rather than the 504.4 of twelve
    /// deflated collections.</summary>
    [Fact]
    public async Task TheWaitMsPerSecBaseline_ReadsEachCollectionAsItsSummedRowRates()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8106 baseline test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await BuildStoreAsync(scratch, ct);
        var hour = SeededHour;
        await SeedTheIssuesHourAsync(connection, hour, ct);

        Assert.Equal(602.0, await WholeTierMeanAsync(connection, PgBaselineProvider.GetBaselineQuery(MetricNames.WaitMsPerSec)!, hour, ct), 2);

        /* And through the provider itself: the analysis time's window reaches the seeded hour, and every tier holds the same 60 samples. */
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new PgBaselineProvider(postgres);
        var bucket = await provider.GetBaselineAsync(ServerId, MetricNames.WaitMsPerSec, DateTime.UtcNow, ct);
        Assert.Equal(60, bucket.SampleCount);
        Assert.Equal(602.0, bucket.Mean, 2);
    }

    /// <summary>The upgrade: a store that carries <c>wait_stats_interval_baseline</c> as its registered wait supply, stamped below V255,
    /// runs the ladder. V255 creates the successor aggregate; the ensure sweep gives it its policies and the backfill materializes the
    /// seeded hour; the provider's supply choice takes the successor, which reads the corrected baseline; and the interval supply
    /// stays until the successor covers the 35-day tier.</summary>
    [Fact]
    public async Task AStoreWithTheIntervalSupply_UpgradesToTheRateSupply_AndReadsTheCorrectedBaseline()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8106 upgrade test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await BuildStoreAsync(scratch, ct);
        var hour = SeededHour;
        await SeedTheIssuesHourAsync(connection, hour, ct);

        /* Back to the store shape before this rung: the interval supply registered (so it exists), the successor absent, V255 unstamped. */
        await ExecuteAsync(connection, TimescaleSupport.CreateWaitStatsIntervalBaselineSql, ct);
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 252, ct);
        await ExecuteAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 255", ct);
        Assert.False(await RelationExistsAsync(connection, TimescaleSupport.WaitStatsRateBaselineView, ct));
        Assert.Equal(252, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version", ct));

        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await RelationExistsAsync(connection, TimescaleSupport.WaitStatsRateBaselineView, ct), "V255 creates the successor on a store whose wait_stats is a hypertable");
        Assert.True(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationIsContinuousAggregateSql(TimescaleSupport.WaitStatsRateBaselineView), ct));
        Assert.Equal(StorageVersion.SchemaVersion, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version", ct));

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await TimescaleSupport.BackfillBaselineAggregatesAsync(connection, null, ct);
        Assert.Equal(hour, await ScalarAsync<DateTime>(connection, $"SELECT min(bucket) FROM collect.{TimescaleSupport.WaitStatsRateBaselineView} WHERE server_id = {ServerId}", ct));
        /* timescaledb_information.jobs reports a continuous aggregate's jobs under the user view's own identity. */
        Assert.True(await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = '{TimescaleSupport.WaitStatsRateBaselineView}' AND proc_name = 'policy_refresh_continuous_aggregate'", ct) == 1,
            "the ensure sweep gives the successor its refresh policy");

        /* The interval supply still holds the hour and the successor reaches it, so the provider reads the successor and the corrected figure. */
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bucket = await new PgBaselineProvider(postgres).GetBaselineAsync(ServerId, MetricNames.WaitMsPerSec, DateTime.UtcNow, ct);
        Assert.Equal(600.4, bucket.Mean, 2);

        /* The interval supply is not dropped yet: the successor's oldest bucket is two days old, the tier is 35. */
        var verdict = await TimescaleSupport.JudgeSupersededBaselineRelationAsync(
            connection, TimescaleSupport.WaitStatsIntervalBaselineView, TimescaleSupport.WaitStatsRateBaselineView, DateTime.UtcNow, ct);
        Assert.Equal(TimescaleSupport.SupersededBaselineDecision.SuccessorShort, verdict.Decision);
        Assert.Equal(0, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, DateTime.UtcNow, ct));
        Assert.True(await RelationExistsAsync(connection, TimescaleSupport.WaitStatsIntervalBaselineView, ct));

        /* Forty days on, the successor covers the tier and the interval supply retires. */
        Assert.Equal(1, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, DateTime.UtcNow.AddDays(40), ct));
        Assert.False(await RelationExistsAsync(connection, TimescaleSupport.WaitStatsIntervalBaselineView, ct));
    }

    /// <summary>The viewer's probe: the successor relation is the V255 sentinel and the top arm.</summary>
    [Fact]
    public void TheProbe_MapsTheRateBaselineToTheSchemaVersion_AndItsAbsenceTo252()
    {
        var map = typeof(ViewerDataService).GetMethod(
            nameof(ViewerDataService.MapProbedSchemaVersion),
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var parameters = map.GetParameters();
        var rate = Array.FindIndex(parameters, p => p.Name == "hasWaitStatsRateBaseline");
        Assert.True(rate >= 0, "the probe has a hasWaitStatsRateBaseline sentinel");
        Assert.Equal(parameters.Length - 1, rate);

        var allTrue = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)map.Invoke(null, allTrue)!);
        Assert.Equal(PgMigrations.Scripts[^1].Version, (int)map.Invoke(null, allTrue)!);

        var withoutRate = (object[])allTrue.Clone();
        withoutRate[rate] = false;
        Assert.Equal(252, (int)map.Invoke(null, withoutRate)!);
    }

    private static async Task<NpgsqlConnection> BuildStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #8106 tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return connection;
    }

    /// <summary>The issue's hour: SOS_SCHEDULER_YIELD 30,000 ms and PAGEIOLATCH_SH 6,000 ms over 60 s every minute, CXCONSUMER
    /// 600 ms over its own 300 s on every fifth minute.</summary>
    private static async Task SeedTheIssuesHourAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct)
    {
        var id = 810600000L;
        for (var m = 0; m < 60; m++)
        {
            var at = hour.AddMinutes(m);
            await InsertWaitAsync(connection, id++, at, "SOS_SCHEDULER_YIELD", 30000, 60, ct);
            await InsertWaitAsync(connection, id++, at, "PAGEIOLATCH_SH", 6000, 60, ct);
            if (m % 5 == 4)
            {
                await InsertWaitAsync(connection, id++, at, "CXCONSUMER", 600, 300, ct);
            }
        }
    }

    private static void BindWindow(NpgsqlCommand command, DateTime hour)
    {
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(hour);
        command.Parameters.AddWithValue(hour.AddHours(1));
        command.Parameters.AddWithValue(NoTransition);
        command.Parameters.AddWithValue(0);
        command.Parameters.AddWithValue(0);
    }

    /// <summary>Runs a provider arm over the seeded hour's window and returns the whole-tier (-1, -1) mean.</summary>
    private static async Task<double> WholeTierMeanAsync(NpgsqlConnection connection, string armSql, DateTime hour, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(armSql, connection);
        BindWindow(command, hour);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetInt32(0) == -1 && reader.GetInt32(1) == -1)
            {
                return reader.GetDouble(2);
            }
        }

        throw new Xunit.Sdk.XunitException("the arm returned no whole-tier row");
    }

    private static async Task InsertWaitAsync(NpgsqlConnection connection, long collectionId, DateTime at, string waitType, long deltaWaitMs, int intervalSeconds, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(deltaWaitMs);
        command.Parameters.AddWithValue(intervalSeconds);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static Task<bool> RelationExistsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
        => ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationExistsSql(view), ct);
}
