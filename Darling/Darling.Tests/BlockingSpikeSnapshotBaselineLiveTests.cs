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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7588: the blocking spike counts its window as blocked process reports, or as DMV snapshot rows when the window holds no
/// report (RDS, Azure). The history it is compared with has to be in the same unit: a snapshot window is judged against
/// snapshot history, a report window against report history. Gated on DARLING_TEST_PG; every test works in its own scratch
/// store, through the product's own detector and baseline provider.
/// </summary>
/* #1776 own-store: each test creates and drops its own database through ScratchPostgres. */
[Collection("live-postgres")]
public sealed class BlockingSpikeSnapshotBaselineLiveTests
{
    private const int ServerId = -758801;
    private const string ServerName = "srv-7588";

    /* Rows per hour of history, and per collection (four collections an hour). */
    private const int RowsPerCollection = 3;
    private const int RowsPerHour = RowsPerCollection * 4;

    private static DateTime Today => DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)?> OpenAsync(CancellationToken ct)
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
            CollectionIdGenerator.Next(), Today.AddDays(-3), ServerId, ServerName);
        /* The baseline relations as a store without TimescaleDB has them: plain views over the raw tables. */
        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
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

    /// <summary>Rows of one kind, <paramref name="perCollection"/> per quarter hour over [from, to), plus one SUCCESS run of that
    /// kind's collector per quarter hour in the collection log.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, bool reports, DateTime from, DateTime to, int perCollection, CancellationToken ct)
    {
        var table = reports ? "blocked_process_reports" : "dmv_blocking_snapshots";
        var idColumn = reports ? "blocked_report_id" : "collection_id";
        await Exec(connection, ct, $@"
INSERT INTO {table} ({idColumn}, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms)
SELECT $1 + row_number() OVER (), t, $2, $3, t, 'appdb', 100 + j, 60, 5000
FROM generate_series($4::timestamp, $5::timestamp - interval '1 second', interval '15 minutes') AS t
CROSS JOIN generate_series(1, $6) AS j",
            CollectionIdGenerator.Next() * 1000, ServerId, ServerName, from, to, perCollection);
        await Exec(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
SELECT $1 + row_number() OVER (), $2, $3, $4, t, 100, 'SUCCESS', NULL, 1, 50, 50
FROM generate_series($5::timestamp, $6::timestamp - interval '1 second', interval '15 minutes') AS t",
            CollectionIdGenerator.Next() * 1000, ServerId, ServerName, reports ? "blocked_process_report" : "dmv_blocking_snapshot", from, to);
    }

    /// <summary>Runs the real detector over a four hour window yesterday afternoon. History is the thirteen days before it.</summary>
    private static async Task<Fact?> SpikeAsync(NpgsqlConnection connection, string connectionString, CancellationToken ct)
    {
        var start = Today.AddDays(-1).AddHours(12);
        var context = new AnalysisContext
        {
            ServerId = ServerId, ServerName = ServerName, TimeRangeStart = start, TimeRangeEnd = start.AddHours(4),
            ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
        };
        await using var postgres = NpgsqlDataSource.Create(connectionString);
        var anomalies = await new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres)).DetectAnomaliesAsync(context);
        return anomalies.SingleOrDefault(a => a.Key == "ANOMALY_BLOCKING_SPIKE");
    }

    private static Task SeedWindowAsync(NpgsqlConnection connection, bool reports, int perCollection, CancellationToken ct)
        => SeedAsync(connection, reports, Today.AddDays(-1).AddHours(12), Today.AddDays(-1).AddHours(16), perCollection, ct);

    private static Task SeedHistoryAsync(NpgsqlConnection connection, bool reports, int days, int perCollection, CancellationToken ct)
        => SeedAsync(connection, reports, Today.AddDays(-1 - days), Today.AddDays(-1), perCollection, ct);

    /* A snapshot-only server whose window runs at its usual snapshot rate is not a spike. Before #7588 the window's snapshot rows
       were divided by an empty report history, which is untrusted, so the count alone fired. */
    [Fact]
    public async Task SnapshotWindow_AtItsOwnUsualRate_IsNotASpike()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedHistoryAsync(connection, reports: false, days: 12, RowsPerCollection, ct);
        await SeedWindowAsync(connection, reports: false, RowsPerCollection, ct);

        Assert.Null(await SpikeAsync(connection, scratch.ConnectionString, ct));
    }

    /* The same history with a window at five times the rate: a real spike, judged as a ratio of snapshot rows to snapshot rows,
       and stamped as the snapshot arm. */
    [Fact]
    public async Task SnapshotWindow_AtFiveTimesItsHistory_SpikesAsAFiveTimesRatio()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedHistoryAsync(connection, reports: false, days: 12, RowsPerCollection, ct);
        await SeedWindowAsync(connection, reports: false, RowsPerCollection * 5, ct);

        var spike = await SpikeAsync(connection, scratch.ConnectionString, ct);
        Assert.NotNull(spike);
        Assert.Equal(1.0, spike!.Metadata[CountFamilyMetadata.FromDmvSnapshotsKey]);
        Assert.Equal(0.0, spike.Metadata.GetValueOrDefault("is_new"));
        Assert.InRange(spike.Metadata["ratio"], 4.5, 5.5);
        Assert.Equal(RowsPerHour, spike.Metadata["baseline_rate"], 1.0);
    }

    /* A server with blocked process reports keeps its report ratio exactly, whatever the snapshot history holds: here a snapshot
       history a hundred times the report history sits beside it. */
    [Fact]
    public async Task ReportWindow_KeepsItsReportRatio_WhateverTheSnapshotHistoryHolds()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedHistoryAsync(connection, reports: true, days: 12, RowsPerCollection, ct);
        await SeedHistoryAsync(connection, reports: false, days: 12, RowsPerCollection * 100, ct);
        await SeedWindowAsync(connection, reports: true, RowsPerCollection * 5, ct);

        var spike = await SpikeAsync(connection, scratch.ConnectionString, ct);
        Assert.NotNull(spike);
        Assert.False(spike!.Metadata.ContainsKey(CountFamilyMetadata.FromDmvSnapshotsKey));
        Assert.InRange(spike.Metadata["ratio"], 4.5, 5.5);
        Assert.Equal(RowsPerHour, spike.Metadata["baseline_rate"], 1.0);
    }

    [Fact]
    public async Task ReportWindow_AtItsUsualRate_IsNotASpike()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedHistoryAsync(connection, reports: true, days: 12, RowsPerCollection, ct);
        await SeedWindowAsync(connection, reports: true, RowsPerCollection, ct);

        Assert.Null(await SpikeAsync(connection, scratch.ConnectionString, ct));
    }

    /* Too little snapshot history behaves as a short report history does: the count alone fires, worded as a first occurrence
       (is_new), never as a ratio over zero. */
    [Fact]
    public async Task SnapshotWindow_WithTooLittleSnapshotHistory_ClaimsNoRatio()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        await SeedHistoryAsync(connection, reports: false, days: 1, RowsPerCollection, ct);
        await SeedWindowAsync(connection, reports: false, RowsPerCollection * 5, ct);

        var spike = await SpikeAsync(connection, scratch.ConnectionString, ct);
        Assert.NotNull(spike);
        Assert.Equal(1.0, spike!.Metadata[CountFamilyMetadata.FromDmvSnapshotsKey]);
        Assert.Equal(1.0, spike.Metadata["is_new"]);
    }

    /* The snapshot baseline is a plain view over the raw table: a fresh store gets it, a store from before this build gets it on its
       next start, and a store that has it is left alone (no rebuild on every start). */
    [Fact]
    public async Task SnapshotBaseline_ExistsOnFreshAndUpgradedStores_AndIsNotRebuiltOnEveryStart()
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await OpenAsync(ct);
        Assert.SkipWhen(opened is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        await using var scratch = opened!.Value.Scratch;
        await using var connection = opened.Value.Connection;

        /* OpenAsync ran the start path's fill: a fresh store has the view, and it is an ordinary view, not an aggregate. */
        Assert.Equal("v", await KindAsync(connection, ct));
        Assert.Null(await TimescaleSupport.ResolveMaterializationAsync(connection, TimescaleSupport.DmvBlockingBaselineView, ct));
        var freshOid = await OidAsync(connection, ct);
        Assert.Equal(0, await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct));
        Assert.Equal(freshOid, await OidAsync(connection, ct));

        /* A store from before this build has every other baseline and not this one. */
        await Exec(connection, ct, $"DROP VIEW collect.{TimescaleSupport.DmvBlockingBaselineView}");
        Assert.Equal(1, await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct));
        Assert.Equal("v", await KindAsync(connection, ct));
        var upgradedOid = await OidAsync(connection, ct);
        Assert.Equal(0, await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct));
        Assert.Equal(upgradedOid, await OidAsync(connection, ct));
    }

    private static async Task<string?> KindAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname = '{TimescaleSupport.DmvBlockingBaselineView}'", connection);
        return (string?)await command.ExecuteScalarAsync(ct);
    }

    private static async Task<uint> OidAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT 'collect.{TimescaleSupport.DmvBlockingBaselineView}'::regclass::oid", connection);
        return Convert.ToUInt32(await command.ExecuteScalarAsync(ct));
    }
}
