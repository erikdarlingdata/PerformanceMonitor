/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8881, the TempDB Log Space switch and bar, against live PostgreSQL. The service replaces its alert settings from
/// <c>config_alert_settings</c> at every start, so a switch or bar that lived only in darling.json read its default whatever the
/// file said: the alert was always on and always at 80. These tests set the switch and bar in the store and read them back the way
/// the engine, the Viewer and an agent do.
/// </summary>
/* #1776 own-store: a scratch database of its own through ScratchPostgres, never the shared one, so deliberately not in the live-postgres collection. */
public sealed class TempDbLogSpaceSettingsLiveTests
{
    private const int ServerId = 8881;

    [Fact]
    public async Task TheSwitchAndBarInTheStore_ReachTheEngineSettings_TheViewer_AndAnAgent()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb log settings test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "tempdb-log-settings", ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ($1, 'tempdb-log-settings', 'tempdb-log-settings') ON CONFLICT DO NOTHING", ServerId);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new StoreConfigProvider(postgres);

        /* Seeding a new store writes the file's switch and bar. */
        var seed = new DarlingConfig();
        seed.Alerts.TempDbLogSpaceEnabled = false;
        seed.Alerts.TempDbLogSpaceThresholdPercent = 65;
        await provider.SeedIfEmptyAsync(seed, ct);
        Assert.Equal((false, 65), await EngineReadsAsync(provider, ct));

        /* A value changed in the store later is what the engine reads at the next start, whatever the file says. */
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        await using (var update = new NpgsqlCommand("UPDATE config.config_alert_settings SET tempdb_log_space_enabled = TRUE, tempdb_log_space_threshold_percent = 90 WHERE id = 1", connection))
        {
            Assert.Equal(1, await update.ExecuteNonQueryAsync(ct));
        }

        Assert.Equal((true, 90), await EngineReadsAsync(provider, ct));

        /* The Viewer reads the stored values and its save writes them back, including the bar it was given. */
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            var row = await viewer.GetAlertSettingsAsync(ct);
            Assert.NotNull(row);
            Assert.Equal((true, 90), (row!.TempDbLogSpaceEnabled, row.TempDbLogSpaceThresholdPercent));
            row.TempDbLogSpaceEnabled = false;
            row.TempDbLogSpaceThresholdPercent = 55;
            await viewer.UpsertAlertSettingsAsync(row, ct);
        }

        Assert.Equal((false, 55), await EngineReadsAsync(provider, ct));

        /* An agent sees the same, and a per-server override still wins over the global bar. */
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            await ThresholdOverrideStore.UpsertAsync(connection, transaction, ServerId, null, "tempdb_log_space.threshold_percent", 40, "test", null, ct);
            await transaction.CommitAsync(ct);
        }

        using var doc = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertSettings(postgres, "tempdb-log-settings"));
        var root = doc.RootElement.TryGetProperty("settings", out var wrapped) ? wrapped : doc.RootElement;
        var log = root.GetProperty("tempdb_log_space");
        Assert.False(log.GetProperty("enabled").GetBoolean());
        Assert.Equal(55, log.GetProperty("threshold_percent").GetInt32());
        var threshold = root.GetProperty("server_view").GetProperty("thresholds").EnumerateArray()
            .Single(t => t.GetProperty("key").GetString() == "tempdb_log_space.threshold_percent");
        Assert.Equal(55, threshold.GetProperty("global_value").GetInt32());
        Assert.Equal(40, threshold.GetProperty("effective_value").GetInt32());
        Assert.Equal("server_override", threshold.GetProperty("source").GetString());
    }

    /// <summary>What the alert engine reads after a start: the store's row applied over a config whose file says the opposite.</summary>
    private static async Task<(bool Enabled, int Percent)> EngineReadsAsync(StoreConfigProvider provider, System.Threading.CancellationToken ct)
    {
        var file = new DarlingConfig();
        file.Alerts.TempDbLogSpaceEnabled = true;
        file.Alerts.TempDbLogSpaceThresholdPercent = 80;
        var view = await provider.LoadViewAsync(file, ct);
        Assert.NotNull(view);
        StoreConfigProvider.ApplyToConfig(file, view!);
        var settings = new DarlingAlertSettings(file);
        return (settings.TempDbLogSpaceEnabled, settings.TempDbLogSpaceThresholdPercent);
    }
}
