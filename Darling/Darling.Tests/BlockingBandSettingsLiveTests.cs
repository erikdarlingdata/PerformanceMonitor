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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The blocking band tiers (#6927, V228, and the two snapshot wait tiers of #7028, V230) on a real PostgreSQL store: an upgraded store whose settings row already exists keeps
/// today's defaults and its other values, and the tiers round trip through <c>update_alert_settings</c> and <c>get_alert_settings</c>
/// with a critical bar below its warn bar refused and nothing written. Each fact mints its own scratch database.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each fact mints its own scratch database through
   ScratchPostgres and never touches the shared store's tables. */
public sealed class BlockingBandSettingsLiveTests
{
    private const string TiersSql = "SELECT blocking_warn_per_hour, blocking_critical_per_hour, blocking_warn_wait_seconds, blocking_critical_wait_seconds FROM config_alert_settings WHERE id = 1";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnUpgradedStoreWithASettingsRow_KeepsTodaysDefaults_AndItsOtherValues()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V228 upgrade test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);

        /* Roll the store back to look like V227: the settings row exists and carries an operator's own deadlock tier, the four
           columns do not. */
        await ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET deadlock_warn_per_hour = 7.0, deadlock_critical_per_hour = 30.0 WHERE id = 1");
        foreach (var column in new[] { "blocking_warn_per_hour", "blocking_critical_per_hour", "blocking_warn_wait_seconds", "blocking_critical_wait_seconds" })
        {
            await ExecAsync(connection, ct, "ALTER TABLE config.config_alert_settings DROP COLUMN " + column);
        }

        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 228");

        var applied = await PgMigrations.MigrateAsync(connection, ct);
        Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 228), applied);

        Assert.Equal((5.0, 20.0, 10.0, 60.0), await ReadTiersAsync(connection, ct));

        /* The band reads today's constants from the upgraded row, and the operator's deadlock tier did not move. */
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        Assert.Equal(BlockingBandThresholds.Default, await BlockingBandStore.ReadAsync(dataSource, 10, ct));
        using var deadlock = new NpgsqlCommand("SELECT deadlock_warn_per_hour, deadlock_critical_per_hour FROM config_alert_settings WHERE id = 1", connection);
        using var reader = await deadlock.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(7.0, reader.GetDouble(0));
        Assert.Equal(30.0, reader.GetDouble(1));
    }

    [Fact]
    public async Task UpdateAlertSettings_SavesTheFourTiers_GetAlertSettingsReadsThemBack_AndACriticalBelowWarnIsRefused()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the blocking band settings test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The seeded row reads today's constants. */
        AssertBands(await DarlingMcpAlertTools.GetAlertSettings(postgres), 5, 20, 10, 60);

        /* A valid body saves, and a fresh read carries it. */
        var saved = await DarlingMcpAlertTools.UpdateAlertSettings(postgres,
            "{\"health_bands\":{\"blocking_warn_per_hour\":50,\"blocking_critical_per_hour\":100,\"blocking_warn_wait_seconds\":60,\"blocking_critical_wait_seconds\":180}}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(saved));
        Assert.Equal((50.0, 100.0, 60.0, 180.0), await ReadTiersAsync(connection, ct));
        AssertBands(await DarlingMcpAlertTools.GetAlertSettings(postgres), 50, 100, 60, 180);
        Assert.Equal(new BlockingBandThresholds(50, 100, 60, 180), await BlockingBandStore.ReadAsync(postgres, 10, ct));

        /* A critical bar below its warn bar is refused with nothing written: in one body, and as a lone value against the stored other one. */
        foreach (var body in new[]
        {
            "{\"health_bands\":{\"blocking_warn_per_hour\":80,\"blocking_critical_per_hour\":30}}",
            "{\"health_bands\":{\"blocking_critical_per_hour\":40}}",
            "{\"health_bands\":{\"blocking_warn_wait_seconds\":200}}",
            "{\"health_bands\":{\"blocking_warn_wait_seconds\":90,\"blocking_critical_wait_seconds\":30}}",
        })
        {
            var refused = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, body);
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(refused));
            Assert.Equal((50.0, 100.0, 60.0, 180.0), await ReadTiersAsync(connection, ct));
        }

        /* Out of range is refused too. */
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres,
            "{\"health_bands\":{\"blocking_warn_per_hour\":0}}")));
        Assert.Equal((50.0, 100.0, 60.0, 180.0), await ReadTiersAsync(connection, ct));
    }

    [Fact]
    public async Task AnUpgradedStoreWithASettingsRow_GetsTheTwoSnapshotTiersOff_AndKeepsItsV228Tiers()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V230 upgrade test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);

        /* Roll the store back to look like V229: the settings row exists with an operator's own V228 tiers, the two columns do not. */
        await ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET blocking_warn_per_hour = 8.0, blocking_critical_per_hour = 40.0 WHERE id = 1");
        foreach (var column in new[] { "blocking_warn_snapshot_wait_seconds", "blocking_critical_snapshot_wait_seconds" })
        {
            await ExecAsync(connection, ct, "ALTER TABLE config.config_alert_settings DROP COLUMN " + column);
        }

        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 230");

        var applied = await PgMigrations.MigrateAsync(connection, ct);
        Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 230), applied);

        /* Both snapshot tiers are off, the operator's V228 count tiers did not move, and the band reads them. */
        Assert.Equal((8.0, 40.0, 10.0, 60.0, 0.0, 0.0), await ReadAllSixAsync(connection, ct));
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var band = await BlockingBandStore.ReadAsync(dataSource, 10, ct);
        Assert.Equal(new BlockingBandThresholds(8, 40, 10, 60), band);
        Assert.Equal(0.0, band.WarnSnapshotWaitSeconds);
        Assert.Equal(0.0, band.CriticalSnapshotWaitSeconds);

        /* A re-run is a no-op: the columns are added only when absent, so a second pass leaves a stored value alone. */
        await ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET blocking_warn_snapshot_wait_seconds = 45.0 WHERE id = 1");
        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 230");
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.Equal(45.0, (await ReadAllSixAsync(connection, ct)).Item5);
    }

    [Fact]
    public async Task UpdateAlertSettings_SavesTheSnapshotTiers_ZeroTurnsATierOff_AndBadValuesAreRefusedWithNothingWritten()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the blocking snapshot tier test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(scratch.ConnectionString, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The seeded row has both tiers off, and the read names the unit of the count tiers. */
        using (var seeded = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertSettings(postgres)))
        {
            var bands = seeded.RootElement.GetProperty("health_bands");
            Assert.Equal(0.0, bands.GetProperty("blocking_warn_snapshot_wait_seconds").GetDouble());
            Assert.Equal(0.0, bands.GetProperty("blocking_critical_snapshot_wait_seconds").GetDouble());
            Assert.Equal("waits_per_hour", bands.GetProperty("blocking_band_unit").GetString());
        }

        /* A valid pair saves, and the read and the band carry it. */
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres,
            "{\"health_bands\":{\"blocking_warn_snapshot_wait_seconds\":30,\"blocking_critical_snapshot_wait_seconds\":120}}")));
        Assert.Equal((5.0, 20.0, 10.0, 60.0, 30.0, 120.0), await ReadAllSixAsync(connection, ct));
        using (var saved = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertSettings(postgres)))
        {
            var bands = saved.RootElement.GetProperty("health_bands");
            Assert.Equal(30.0, bands.GetProperty("blocking_warn_snapshot_wait_seconds").GetDouble());
            Assert.Equal(120.0, bands.GetProperty("blocking_critical_snapshot_wait_seconds").GetDouble());
        }

        var band = await BlockingBandStore.ReadAsync(postgres, 10, ct);
        Assert.Equal(30.0, band.WarnSnapshotWaitSeconds);
        Assert.Equal(120.0, band.CriticalSnapshotWaitSeconds);

        /* Critical below warn is refused when both are above 0, in one body and as a lone value against the stored other one. */
        foreach (var body in new[]
        {
            "{\"health_bands\":{\"blocking_warn_snapshot_wait_seconds\":200,\"blocking_critical_snapshot_wait_seconds\":100}}",
            "{\"health_bands\":{\"blocking_critical_snapshot_wait_seconds\":10}}",
            "{\"health_bands\":{\"blocking_warn_snapshot_wait_seconds\":500}}",
        })
        {
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres, body)));
            Assert.Equal((5.0, 20.0, 10.0, 60.0, 30.0, 120.0), await ReadAllSixAsync(connection, ct));
        }

        /* Out of range is refused: below the floor but not 0 (the band would clamp it up, so it would not stick), negative, and past the ceiling. */
        foreach (var bad in new[] { "0.5", "-1", "86401", "\"30\"", "null" })
        {
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres,
                "{\"health_bands\":{\"blocking_warn_snapshot_wait_seconds\":" + bad + "}}")));
            Assert.Equal((5.0, 20.0, 10.0, 60.0, 30.0, 120.0), await ReadAllSixAsync(connection, ct));
        }

        /* 0 turns a tier off. Critical off under a warn that is on is accepted, and so is warn off under a critical that is on. */
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres,
            "{\"health_bands\":{\"blocking_critical_snapshot_wait_seconds\":0}}")));
        Assert.Equal((5.0, 20.0, 10.0, 60.0, 30.0, 0.0), await ReadAllSixAsync(connection, ct));
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres,
            "{\"health_bands\":{\"blocking_warn_snapshot_wait_seconds\":0,\"blocking_critical_snapshot_wait_seconds\":15}}")));
        Assert.Equal((5.0, 20.0, 10.0, 60.0, 0.0, 15.0), await ReadAllSixAsync(connection, ct));

        /* The read-only unit label comes home from a round trip without being refused. */
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres,
            "{\"health_bands\":{\"blocking_band_unit\":\"waits_per_hour\",\"blocking_warn_per_hour\":6}}")));
        Assert.Equal((6.0, 20.0, 10.0, 60.0, 0.0, 15.0), await ReadAllSixAsync(connection, ct));
    }

    private static void AssertBands(string settings, double warnPerHour, double criticalPerHour, double warnWait, double criticalWait)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(settings), settings);
        using var doc = JsonDocument.Parse(settings);
        var bands = doc.RootElement.GetProperty("health_bands");
        Assert.Equal(warnPerHour, bands.GetProperty("blocking_warn_per_hour").GetDouble());
        Assert.Equal(criticalPerHour, bands.GetProperty("blocking_critical_per_hour").GetDouble());
        Assert.Equal(warnWait, bands.GetProperty("blocking_warn_wait_seconds").GetDouble());
        Assert.Equal(criticalWait, bands.GetProperty("blocking_critical_wait_seconds").GetDouble());
    }

    private static async Task<(double WarnPerHour, double CriticalPerHour, double WarnWait, double CriticalWait)> ReadTiersAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(TiersSql, connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "config_alert_settings has no singleton row");
        return (reader.GetDouble(0), reader.GetDouble(1), reader.GetDouble(2), reader.GetDouble(3));
    }

    private static async Task<(double WarnPerHour, double CriticalPerHour, double WarnWait, double CriticalWait, double WarnSnapshot, double CriticalSnapshot)> ReadAllSixAsync(
        NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT blocking_warn_per_hour, blocking_critical_per_hour, blocking_warn_wait_seconds, blocking_critical_wait_seconds, "
            + "blocking_warn_snapshot_wait_seconds, blocking_critical_snapshot_wait_seconds FROM config_alert_settings WHERE id = 1", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "config_alert_settings has no singleton row");
        return (reader.GetDouble(0), reader.GetDouble(1), reader.GetDouble(2), reader.GetDouble(3), reader.GetDouble(4), reader.GetDouble(5));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Seeds the singleton rows through the product's own path, so this test need not track their other NOT NULL columns.</summary>
    private static async Task SeedAsync(string connectionString, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(new DarlingConfig(), ct);
    }
}
