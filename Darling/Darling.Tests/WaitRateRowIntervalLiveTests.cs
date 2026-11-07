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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8106: <c>sample_interval_seconds</c> is a per-row figure, the seconds since that wait type's own previous sighting. A type that
/// returns after missing some passes stores several cadences while its siblings in the same collection store one, so a read that
/// divides the collection's summed wait by its longest row interval deflates every collection that holds a returning row. The
/// wait-rate anomaly's window, the young-baseline bar and the stored WaitMsPerSec baseline all rate each row over its own interval
/// instead, so the window and the baseline it is scored against agree. The rows are the issue's: two steady types at 500 and
/// 100 ms/s every minute, and a third seen every fifth minute (600 ms over its own 300 s, 2 ms/s), so every collection reads 600
/// or 602 and the hour averages 600.4.
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
            Assert.Equal(602.0, reader.GetDouble(1), 2);
            Assert.Equal(600.4, reader.GetDouble(2), 2);
            Assert.Equal(60, Convert.ToInt32(reader.GetValue(4)));
            Assert.False(await reader.ReadAsync(ct));
        }

        await using (var bar = new NpgsqlCommand(PgAnomalyDetector.YoungBaselineBarPeakSql, connection))
        {
            bar.Parameters.AddWithValue(ServerId);
            bar.Parameters.AddWithValue(hour);
            bar.Parameters.AddWithValue(hour.AddHours(1));
            Assert.Equal(602.0, Convert.ToDouble(await bar.ExecuteScalarAsync(ct)), 2);
        }
    }

    /// <summary>The stored baseline over the successor supply, read through the provider's own WaitMsPerSec arm: every collection's
    /// rate is its summed row rates, so the hour's mean is 600.4 rather than the 504.4 of twelve deflated collections.</summary>
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

        Assert.Equal(600.4, await WholeTierMeanAsync(connection, PgBaselineProvider.GetBaselineQuery(MetricNames.WaitMsPerSec)!, hour, ct), 2);

        /* And through the provider itself: the analysis time's window reaches the seeded hour, and every tier holds the same 60 samples. */
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new PgBaselineProvider(postgres);
        var bucket = await provider.GetBaselineAsync(ServerId, MetricNames.WaitMsPerSec, DateTime.UtcNow, ct);
        Assert.Equal(60, bucket.SampleCount);
        Assert.Equal(600.4, bucket.Mean, 2);
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
