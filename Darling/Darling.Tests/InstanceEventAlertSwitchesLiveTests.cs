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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8884: the Instance Restarted and Memory Dumps switches live in <c>config.config_alert_settings</c> (V271), because the service replaces
/// <c>config.Alerts</c> with what the store holds at every start. A switch that exists only in darling.json is lost at that reload and always reads
/// on. These tests set the columns in the store, load them the way the service does, and check the engine's settings read the stored value.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each fact mints its own scratch database through
   ScratchPostgres and never touches the shared store's tables. */
public sealed class InstanceEventAlertSwitchesLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8884 alert switch tests (they mint their own scratch databases).";

    private static async Task<(bool Restarted, bool Dumps)> EngineSettingsAfterTheServiceLoadAsync(string connectionString, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        var config = new DarlingConfig();
        var view = await new StoreConfigProvider(dataSource).LoadViewAsync(config, ct);
        Assert.NotNull(view);
        StoreConfigProvider.ApplyToConfig(config, view);
        var settings = new DarlingAlertSettings(config);
        return (settings.InstanceRestartedEnabled, settings.MemoryDumpsEnabled);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task TheStoredSwitches_SurviveTheServiceReload_AndStartOnForAFreshStore()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: a scratch database of its own, migrated from the ladder. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await using (var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(new DarlingConfig(), ct);
            }

            /* A fresh store: both on. */
            Assert.Equal((true, true), await EngineSettingsAfterTheServiceLoadAsync(scratch.ConnectionString, ct));

            /* Switched off in the store, each on its own: the reload keeps it off (it read true before the columns existed). */
            await ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET instance_restarted_enabled = FALSE WHERE id = 1");
            Assert.Equal((false, true), await EngineSettingsAfterTheServiceLoadAsync(scratch.ConnectionString, ct));
            await ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET instance_restarted_enabled = TRUE, memory_dumps_enabled = FALSE WHERE id = 1");
            Assert.Equal((true, false), await EngineSettingsAfterTheServiceLoadAsync(scratch.ConnectionString, ct));

            /* The Viewer's read and save carry both columns: a save of the loaded row with both off reaches the service. */
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var row = await viewer.GetAlertSettingsAsync(ct);
                Assert.NotNull(row);
                Assert.True(row.InstanceRestartedEnabled);
                Assert.False(row.MemoryDumpsEnabled);
                row.InstanceRestartedEnabled = false;
                await viewer.UpsertAlertSettingsAsync(row, ct);
            }

            Assert.Equal((false, false), await EngineSettingsAfterTheServiceLoadAsync(scratch.ConnectionString, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ADarlingJsonSwitchOff_SeedsANewStoreOff()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            var config = JsonSerializer.Deserialize<DarlingConfig>("{\"alerts\":{\"instanceRestartedEnabled\":false,\"memoryDumpsEnabled\":false}}")!;
            await using (var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(config, ct);
            }

            Assert.Equal((false, false), await EngineSettingsAfterTheServiceLoadAsync(scratch.ConnectionString, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreUpgradedFromV270_GainsBothSwitchesOn_KeepsItsOtherSettings_AndTheProbeNamesTheRung()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await using (var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(new DarlingConfig(), ct);
            }

            await ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET cpu_threshold_percent = 91 WHERE id = 1");

            /* The store as V270 left it: neither column, and no V271 row in the version table. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 270, ct);
            await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 271");
            await using (var check = new NpgsqlCommand(
                "SELECT count(*) FROM pg_attribute WHERE attrelid = 'config.config_alert_settings'::regclass AND NOT attisdropped " +
                "AND attname IN ('instance_restarted_enabled', 'memory_dumps_enabled')", connection))
            {
                Assert.Equal(0L, await check.ExecuteScalarAsync(ct));
            }

            await using (var older = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.Equal(270, await older.GetStoreSchemaVersionAsync(ct));
            }

            var applied = await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 271), applied);

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            /* The store has climbed the whole ladder, so the probe names the newest rung. */
            Assert.Equal(PgMigrations.Scripts[^1].Version, await viewer.GetStoreSchemaVersionAsync(ct));
            var row = await viewer.GetAlertSettingsAsync(ct);
            Assert.NotNull(row);
            Assert.True(row.InstanceRestartedEnabled);
            Assert.True(row.MemoryDumpsEnabled);
            Assert.Equal(91, row.CpuThresholdPercent);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
