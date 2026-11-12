/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that adds the Runnable Queue alert's three settings to <c>config.config_alert_settings</c>
/// (V177, #5641): the rung's place in the ladder, the shape of its script (three guarded ADD COLUMNs with NOT NULL
/// defaults equal to the evaluator's), the schema-version probe's sentinel and its mapping arm, and the behaviour on
/// a fresh store, on a store one rung behind that holds a settings row, and on a re-run.
/// </summary>
public sealed class RunnableQueueSettingsRungTests
{
    private const string RungName = "runnable-queue-alert-settings";

    private const int RungVersion = 177;

    /// <summary>The sentinel's column in the probe's select list and its parameter in <c>MapProbedSchemaVersion</c>.</summary>
    private const int ProbeOrdinal = 152;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_AtVersion177()
    {
        Assert.Equal(RungVersion, Rung.Version);
        Assert.Equal(1, PgMigrations.Scripts.Count(m => m.Name == RungName));
        Assert.Equal(1, PgMigrations.Scripts.Count(m => m.Version == RungVersion));
        Assert.True(RungVersion <= StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void TheScript_AddsThreeGuardedColumns_WithTheEvaluatorsDefaults_AndNothingElse()
    {
        var sql = Statements();

        Assert.Contains("ALTER TABLE config.config_alert_settings", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS runnable_queue_enabled boolean NOT NULL DEFAULT TRUE", sql, StringComparison.Ordinal);
        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS runnable_queue_warning_per_scheduler integer NOT NULL DEFAULT {RunnableQueueEvaluator.DefaultWarningPerScheduler.ToString(CultureInfo.InvariantCulture)}",
            sql, StringComparison.Ordinal);
        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS runnable_queue_critical_per_scheduler integer NOT NULL DEFAULT {RunnableQueueEvaluator.DefaultCriticalPerScheduler.ToString(CultureInfo.InvariantCulture)}",
            sql, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(sql, "ADD COLUMN").Count);

        /* Catalog-only: no data is rewritten, no row is dropped, no GRANT is needed. */
        foreach (var verb in new[] { "DELETE", "INSERT", "UPDATE", "GRANT", "REVOKE", "TRUNCATE", "DROP" })
        {
            Assert.DoesNotMatch(@"(?m)^\s*" + verb + @"\b", sql);
        }
    }

    [Fact]
    public async Task MigratingAFreshStore_AddsTheColumnsWithTheirDefaults_AnUpgradeKeepsTheRow_AndARerunChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V177 rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            const string ColumnsSql =
                @"SELECT string_agg(column_name || ':' || data_type || ':' || is_nullable || ':' || column_default, ' | ' ORDER BY column_name)
                  FROM information_schema.columns
                  WHERE table_schema = 'config' AND table_name = 'config_alert_settings' AND column_name LIKE 'runnable\_queue\_%'";
            const string Expected =
                "runnable_queue_critical_per_scheduler:integer:NO:16"
                + " | runnable_queue_enabled:boolean:NO:true"
                + " | runnable_queue_warning_per_scheduler:integer:NO:8";

            /* A fresh install: the three columns are there, NOT NULL, with the evaluator's defaults, and the probe maps
               the store to the newest rung. */
            Assert.Equal(Expected, await ScalarAsync(connection, ColumnsSql, ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT max(version) FROM darling_schema_version", ct), CultureInfo.InvariantCulture));

            /* An upgrade: one rung behind (the columns gone, the version row gone) with a settings row that must survive
               the rung and read the defaults afterwards. Every other NOT NULL column of the row has its own default. */
            await ExecAsync(connection, "ALTER TABLE config.config_alert_settings DROP COLUMN runnable_queue_enabled, DROP COLUMN runnable_queue_warning_per_scheduler, DROP COLUMN runnable_queue_critical_per_scheduler", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion, ct);
            await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            Assert.Equal(0L, await ScalarAsync(connection,
                @"SELECT count(*) FROM pg_attribute WHERE attrelid = 'config.config_alert_settings'::regclass AND attname LIKE 'runnable\_queue\_%' AND NOT attisdropped", ct));

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(Expected, await ScalarAsync(connection, ColumnsSql, ct));
            Assert.Equal("true:8:16", await ScalarAsync(connection,
                "SELECT runnable_queue_enabled::text || ':' || runnable_queue_warning_per_scheduler || ':' || runnable_queue_critical_per_scheduler FROM config.config_alert_settings WHERE id = 1", ct));

            /* A configured value is not reset by the rung's text run again, or by another migrate pass. */
            await ExecAsync(connection, "UPDATE config.config_alert_settings SET runnable_queue_enabled = FALSE, runnable_queue_warning_per_scheduler = 11, runnable_queue_critical_per_scheduler = 22 WHERE id = 1", ct);
            await ExecAsync(connection, Rung.Sql, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(Expected, await ScalarAsync(connection, ColumnsSql, ct));
            Assert.Equal("false:11:22", await ScalarAsync(connection,
                "SELECT runnable_queue_enabled::text || ':' || runnable_queue_warning_per_scheduler || ':' || runnable_queue_critical_per_scheduler FROM config.config_alert_settings WHERE id = 1", ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
