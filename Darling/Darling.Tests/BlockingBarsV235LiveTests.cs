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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// V235 (#7083): the blocking bars move to 3, 60 and 3. A fresh store's settings row starts on them. A store at V233 whose row still holds the old
/// shipped defaults (1, 0, 1) reads the new bars after the upgrade. A column an operator moved off the old default keeps its value, and each column
/// moves on its own.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own database. */
public sealed class BlockingBarsV235LiveTests
{
    private const string BarsSql =
        "SELECT blocking_count_threshold || '/' || blocking_wait_seconds_threshold || '/' || pg_blocking_count_threshold FROM config.config_alert_settings WHERE id = 1";

    private static async Task<string> BarsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(BarsSql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    [Fact]
    public async Task AFreshStore_StartsOnTheNewBars()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id) VALUES (1)", ct);

        Assert.Equal("3/60/3", await BarsAsync(connection, ct));
    }

    [Fact]
    public async Task AStoreAtV233OnTheOldDefaults_ReadsTheNewBars_AnOperatorsOwnValueIsKept_AndItAppliesOnce()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* Back to V233: the defaults are the old 1, 0 and 1 again and the version stamp is gone. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 233, ct);
        await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 235", ct);
        await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id) VALUES (1)", ct);
        Assert.Equal("1/0/1", await BarsAsync(connection, ct));
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            Assert.Equal(233, await viewer.GetStoreSchemaVersionAsync(ct));
        }

        Assert.Equal(StorageVersion.SchemaVersion - 234, await PgMigrations.MigrateAsync(connection, ct)); /* V235 and every rung above it */

        Assert.Equal("3/60/3", await BarsAsync(connection, ct));
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));
        }

        /* Applies once: an operator who sets 1 after the upgrade keeps it. */
        await ExecAsync(connection, "UPDATE config.config_alert_settings SET blocking_count_threshold = 1 WHERE id = 1", ct);
        Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
        Assert.Equal("1/60/3", await BarsAsync(connection, ct));

        /* Each column moves on its own: a count of 5 is a choice and stays, the wait bar at 0 moves, the PostgreSQL count at 7 stays. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 233, ct);
        await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 235", ct);
        await ExecAsync(connection,
            "UPDATE config.config_alert_settings SET blocking_count_threshold = 5, blocking_wait_seconds_threshold = 0, pg_blocking_count_threshold = 7 WHERE id = 1", ct);
        Assert.Equal(StorageVersion.SchemaVersion - 234, await PgMigrations.MigrateAsync(connection, ct)); /* V235 and every rung above it */
        Assert.Equal("5/60/7", await BarsAsync(connection, ct));
    }
}
