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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7885: the blocking and deadlock spike detectors compare the window's per-hour rate to the bucket's REAL mean. On a
/// server whose trusted baseline sits under one event an hour, which is most servers, "3x baseline" must mean three
/// times that mean, not three events an hour. The count gates (5 blocking events, 3 deadlocks) still bound the
/// numerator. History is thirty days of SUCCESS runs for the family's collector before yesterday, with events at 12:10 on
/// six of those days, so the hour-12 bucket pools to a trusted mean of about 0.2 an hour; the analysis window is the
/// default four hours from yesterday 12:00. Gated on DARLING_TEST_PG; every test works in its own scratch store, through
/// the product's own detector and baseline provider. Lite's <c>SpikeBaselineFloorTests</c> is the twin.
/// </summary>
[Collection("live-postgres")]
public sealed class SpikeBaselineFloorLiveTests
{
    private const int ServerId = -788501;
    private const string ServerName = "srv-7885";

    /// <summary>One clock read per test instance (#3676): the wait_stats row, the collection log, the planted events and the analysis
    /// window all derive from it, so a run that crosses UTC midnight between the seeds and the detector still puts them on the same day.</summary>
    private readonly DateTime _today = LiveClock.Now().Date;
    private DateTime WindowStart => _today.AddDays(-1).AddHours(12);

    [Fact]
    public async Task FourDeadlocksInFourHours_AgainstATrustedFifthOfOneAnHour_Fire()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedTrustedBaselineAsync(connection, deadlocks: true, ct);
        await SeedWindowAsync(connection, deadlocks: true, events: 4, ct);

        var spike = await SpikeAsync(connection, scratch.ConnectionString, "ANOMALY_DEADLOCK_SPIKE", ct);
        Assert.NotNull(spike);
        Assert.Equal(0.0, spike!.Metadata["is_new"]);
        Assert.InRange(spike.Metadata["baseline_rate"], 0.15, 0.25);
        Assert.True(spike.Metadata["ratio"] >= 3.0, $"ratio {spike.Metadata["ratio"]} is the real multiple of a 0.2/h baseline");
        Assert.Equal(4.0, spike.Metadata["current_count"]);
    }

    [Fact]
    public async Task OneDeadlockInFourHours_DoesNotPassTheCountGate()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedTrustedBaselineAsync(connection, deadlocks: true, ct);
        await SeedWindowAsync(connection, deadlocks: true, events: 1, ct);

        Assert.Null(await SpikeAsync(connection, scratch.ConnectionString, "ANOMALY_DEADLOCK_SPIKE", ct));
    }

    [Fact]
    public async Task SixBlockedProcessReportsInFourHours_AgainstATrustedFifthOfOneAnHour_Fire()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedTrustedBaselineAsync(connection, deadlocks: false, ct);
        await SeedWindowAsync(connection, deadlocks: false, events: 6, ct);

        var spike = await SpikeAsync(connection, scratch.ConnectionString, "ANOMALY_BLOCKING_SPIKE", ct);
        Assert.NotNull(spike);
        Assert.Equal(0.0, spike!.Metadata["is_new"]);
        Assert.InRange(spike.Metadata["baseline_rate"], 0.15, 0.25);
        Assert.True(spike.Metadata["ratio"] >= 3.0, $"ratio {spike.Metadata["ratio"]} is the real multiple of a 0.2/h baseline");
        Assert.Equal(6.0, spike.Metadata["current_count"]);
    }

    [Fact]
    public async Task FourBlockedProcessReportsInFourHours_DoNotPassTheCountGate()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedTrustedBaselineAsync(connection, deadlocks: false, ct);
        await SeedWindowAsync(connection, deadlocks: false, events: 4, ct);

        Assert.Null(await SpikeAsync(connection, scratch.ConnectionString, "ANOMALY_BLOCKING_SPIKE", ct));
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    private async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)?> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return null;
        }

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await Exec(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)", ServerId, ServerName);
        /* The detector skips every family on a server with no wait or CPU rows in the thirty days before the window. */
        await Exec(connection, ct,
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1, $2, $3, $4, 'PAGEIOLATCH_SH', 1, 1, 0)",
            CollectionIdGenerator.Next(), _today.AddDays(-3), ServerId, ServerName);
        return (scratch, connection);
    }

    private static async Task Exec(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task PlantAsync(NpgsqlConnection connection, bool deadlocks, DateTime at, int spid, CancellationToken ct) => deadlocks
        ? Exec(connection, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, $4, $2, '<deadlock/>', 'appdb')",
            CollectionIdGenerator.Next(), at, ServerId, ServerName)
        : Exec(connection, ct,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms) VALUES ($1, $2, $3, $4, $2, 'appdb', $5, 60, 5000)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, spid);

    /// <summary>Thirty days of SUCCESS runs every quarter hour for the family's collector, ending at yesterday's midnight, and
    /// one event at 12:10 on six of those days. The baseline relations are the plain views a store without TimescaleDB has.</summary>
    private async Task SeedTrustedBaselineAsync(NpgsqlConnection connection, bool deadlocks, CancellationToken ct)
    {
        await Exec(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
SELECT $1 + row_number() OVER (), $2, $3, $4, t, 100, 'SUCCESS', NULL, 1, 50, 50
FROM generate_series($5::timestamp, $6::timestamp - interval '1 second', interval '15 minutes') AS t",
            CollectionIdGenerator.Next() * 1000, ServerId, ServerName, deadlocks ? "deadlocks" : "blocked_process_report", _today.AddDays(-31), _today.AddDays(-1));
        for (var day = 2; day <= 7; day++)
            await PlantAsync(connection, deadlocks, _today.AddDays(-1 - day).AddHours(12).AddMinutes(10), 100 + day, ct);
        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
    }

    private async Task SeedWindowAsync(NpgsqlConnection connection, bool deadlocks, int events, CancellationToken ct)
    {
        for (var i = 0; i < events; i++)
            await PlantAsync(connection, deadlocks, WindowStart.AddMinutes(10 + i), 200 + i, ct);
    }

    /// <summary>Runs the real detector over the default four hour window from yesterday 12:00.</summary>
    private async Task<Fact?> SpikeAsync(NpgsqlConnection connection, string connectionString, string key, CancellationToken ct)
    {
        var context = new AnalysisContext
        {
            ServerId = ServerId, ServerName = ServerName, TimeRangeStart = WindowStart, TimeRangeEnd = WindowStart.AddHours(4),
            ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
        };
        await using var postgres = NpgsqlDataSource.Create(connectionString);
        var anomalies = await new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres)).DetectAnomaliesAsync(context);
        return anomalies.SingleOrDefault(a => a.Key == key);
    }
}
