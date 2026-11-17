/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch database through ScratchPostgres and never touch another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The TempDB Growth Fit switch (#8882, V270) on a real PostgreSQL store. The service replaces its alert settings from
/// <c>config_alert_settings</c> at every start, so a switch that lives only in the file is always on. These facts hold that
/// the stored value is the one the alert engine reads, that a store upgraded from before the rung gets the switch on, and that
/// the Viewer and the MCP tools write and read the same column.
/// </summary>
public sealed class TempDbGrowthFitSwitchLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SwitchSql = "SELECT tempdb_growth_fit_enabled FROM config.config_alert_settings WHERE id = 1";

    [Fact]
    public async Task TheStoredSwitch_IsWhatTheEngineSettingsRead_AfterTheStartupReload()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the TempDB Growth Fit switch facts (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* A new store seeded from a file that turns the switch off holds off. */
            var file = new DarlingConfig();
            file.Alerts.TempDbGrowthFitEnabled = false;
            var provider = new StoreConfigProvider(dataSource);
            await provider.SeedIfEmptyAsync(file, ct);
            Assert.False(await ReadColumnAsync(dataSource, ct));

            /* The startup reload replaces the live alert settings with the store's. The engine reads the stored value. */
            Assert.False(await EngineSwitchAfterReloadAsync(provider, ct));

            /* The store wins over the file: the stored switch is turned on and the reload over a file that says off reads on. */
            await ExecAsync(dataSource, "UPDATE config.config_alert_settings SET tempdb_growth_fit_enabled = TRUE WHERE id = 1", ct);
            Assert.True(await EngineSwitchAfterReloadAsync(provider, ct));
            await ExecAsync(dataSource, "UPDATE config.config_alert_settings SET tempdb_growth_fit_enabled = FALSE WHERE id = 1", ct);
            Assert.False(await EngineSwitchAfterReloadAsync(provider, ct));

            /* The Viewer's save writes the switch and its read sees it. */
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var row = await viewer.GetAlertSettingsAsync(ct);
            Assert.NotNull(row);
            Assert.False(row.TempDbGrowthFitEnabled);
            row.TempDbGrowthFitEnabled = true;
            await viewer.UpsertAlertSettingsAsync(row, ct);
            Assert.True(await ReadColumnAsync(dataSource, ct));
            Assert.True((await viewer.GetAlertSettingsAsync(ct))!.TempDbGrowthFitEnabled);
            Assert.True(await EngineSwitchAfterReloadAsync(provider, ct));

            /* The MCP pair reads and writes the same column. */
            Assert.True(SwitchFromGet(await DarlingMcpAlertTools.GetAlertSettings(dataSource)));
            var updated = await DarlingMcpAlertTools.UpdateAlertSettings(dataSource, "{\"tempdb_growth_fit\":{\"enabled\":false}}");
            Assert.False(McpHelpers.IsErrorEnvelope(updated), updated);
            Assert.False(await ReadColumnAsync(dataSource, ct));
            Assert.False(SwitchFromGet(await DarlingMcpAlertTools.GetAlertSettings(dataSource)));
            Assert.False(await EngineSwitchAfterReloadAsync(provider, ct));

            var refused = await DarlingMcpAlertTools.UpdateAlertSettings(dataSource, "{\"tempdb_growth_fit\":{\"enabled\":true,\"threshold\":5}}");
            Assert.Contains("tempdb_growth_fit.threshold", refused, StringComparison.Ordinal);
            Assert.False(await ReadColumnAsync(dataSource, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreUpgradedFromBeforeTheRung_GetsTheSwitchOn_AndKeepsItsOtherSettings()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the TempDB Growth Fit switch facts (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(new DarlingConfig(), ct);

            /* Roll the store back to look like the one before the rung: the settings row exists with an operator's own tempdb space bar, the column does not. */
            await ExecAsync(dataSource, "UPDATE config.config_alert_settings SET tempdb_space_threshold_percent = 91 WHERE id = 1", ct);
            await ExecAsync(dataSource, "ALTER TABLE config.config_alert_settings DROP COLUMN tempdb_growth_fit_enabled", ct);
            await ExecAsync(dataSource, "DELETE FROM darling_schema_version WHERE version >= 270", ct);

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.True(await ReadColumnAsync(dataSource, ct));
            var provider = new StoreConfigProvider(dataSource);
            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.Equal(91, view.Alerts.TempDbSpaceThresholdPercent);
            Assert.True(view.Alerts.TempDbGrowthFitEnabled);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<bool> EngineSwitchAfterReloadAsync(StoreConfigProvider provider, CancellationToken ct)
    {
        /* The file says on (its default) and the store may say off. What the engine reads is the store's. */
        var config = new DarlingConfig();
        var view = await provider.LoadViewAsync(config, ct);
        Assert.NotNull(view);
        StoreConfigProvider.ApplyToConfig(config, view);
        IAlertEngineSettings engine = new DarlingAlertSettings(config);
        return engine.TempDbGrowthFitEnabled;
    }

    private static bool SwitchFromGet(string json)
    {
        var node = JsonNode.Parse(json)!;
        return node["tempdb_growth_fit"]!["enabled"]!.GetValue<bool>();
    }

    private static async Task<bool> ReadColumnAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(SwitchSql);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlDataSource dataSource, string sql, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
