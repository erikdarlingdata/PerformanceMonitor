/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// V216 (#5736): the Client Timeouts alert ships on. A fresh store's settings row starts with the switch on. A store that was at V215 with the
/// switch off (V179's old default, which nothing offered as a choice apart from the default) reads on after the upgrade, and an operator who
/// switches it off AFTER the upgrade keeps it off, because the rung has stamped its version and does not run again.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own database. */
public sealed class ClientTimeoutsOnByDefaultV216LiveTests
{
    private const string EnabledSql = "SELECT client_timeouts_enabled FROM config.config_alert_settings WHERE id = 1";

    private const string DefaultSql =
        "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef AS d JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum "
        + "WHERE d.adrelid = to_regclass('config.config_alert_settings') AND a.attname = 'client_timeouts_enabled'";

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync(ct))!, typeof(T), CultureInfo.InvariantCulture);
    }

    [Fact]
    public void TheConfigSeed_StartsWithTheAlertOn()
    {
        Assert.True(new AlertsConfig().ClientTimeoutsEnabled);
        Assert.True(new AlertSettingsRow().ClientTimeoutsEnabled);
    }

    [Fact]
    public async Task AFreshStore_StartsWithTheAlertOn()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);

        await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id) VALUES (1)", ct);

        Assert.True(await ScalarAsync<bool>(connection, EnabledSql, ct));
        Assert.Equal("true", await ScalarAsync<string>(connection, DefaultSql, ct));
    }

    [Fact]
    public async Task AStoreAtV215WithTheAlertOff_ReadsOnAfterTheUpgrade_AndAnOperatorsLaterOffStaysOff()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* Back to V215: the default is V179's FALSE again and the version stamp is gone. The row is the one a V215 store holds: off, with a bar
           an operator had changed, which the upgrade must not touch. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 215, ct);
        await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 216", ct);
        await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id, client_timeouts_warning_per_minute) VALUES (1, 7)", ct);
        Assert.False(await ScalarAsync<bool>(connection, EnabledSql, ct));
        Assert.Equal("false", await ScalarAsync<string>(connection, DefaultSql, ct));
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            Assert.Equal(215, await viewer.GetStoreSchemaVersionAsync(ct));
        }

        var applied = await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(StorageVersion.SchemaVersion - 215, applied);
        Assert.True(await ScalarAsync<bool>(connection, EnabledSql, ct));
        Assert.Equal("true", await ScalarAsync<string>(connection, DefaultSql, ct));
        Assert.Equal(7, await ScalarAsync<int>(connection, "SELECT client_timeouts_warning_per_minute FROM config.config_alert_settings WHERE id = 1", ct));
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));
        }

        /* An operator switches it off after the upgrade, and the next start finds the rung stamped and leaves it. */
        await ExecAsync(connection, "UPDATE config.config_alert_settings SET client_timeouts_enabled = FALSE WHERE id = 1", ct);
        Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
        Assert.False(await ScalarAsync<bool>(connection, EnabledSql, ct));
    }
}
