/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4479: the Viewer's own product path — constructing a real <see cref="ViewerDataService"/> in
/// BRING-YOUR-OWN mode, exactly the way the Viewer's own live tests build one (see
/// <c>StoreSizeCacheLiveTests</c>) — carries the same naming pool <see cref="StoreApplicationNamePinTests"/>
/// only proves through the helper directly. This is the wiring pin: a real read through
/// <see cref="ViewerDataService"/>'s own constructor and its own store call, checked against
/// <c>pg_stat_activity</c> from a second connection, not a call to
/// <see cref="DarlingStoreConnection.WithApplicationName"/> standing in for it.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here reaches
   DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then works entirely
   inside it, so it cannot race the shared store's tables. */
public sealed class ViewerApplicationNameLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// BRING-YOUR-OWN, no <c>Application Name</c> supplied: the Viewer names its own backend
    /// <c>PerformanceMonitorDarling-Viewer</c> — a literal here so this pin compiles and reds against dev,
    /// which has no <see cref="ViewerSettings.ApplicationName"/> naming path for a BYO string at all.
    /// </summary>
    [Fact]
    public async Task ViewerDataService_BringYourOwn_NamesItsBackendInPgStatActivity()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4479 Viewer product-path live pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        try
        {
            await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
            {
                await setup.OpenAsync(ct);
                await PgMigrations.MigrateAsync(setup, ct);
            }

            /* ScratchPostgres disables pooling on its own connection string so a stray in-process pooled
               connection can never pin the scratch database; that leaves nothing idle in pg_stat_activity
               by the time a SECOND connection could look for it. This test's own claim is about naming, not
               about pooling, so it re-enables pooling (min size 1) on the SAME scratch string — the Viewer's
               real constructor is still what wires the name onto whatever pool settings the string carries. */
            var pooledConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Pooling = true,
                MinPoolSize = 1,
            }.ConnectionString;

            /* The real product path: the Viewer's own constructor, no ApplicationName on the BYO string
               it is handed — the constructor's own set-if-absent call is what has to carry the name. */
            await using var service = new ViewerDataService(pooledConnectionString);

            /* One real read through the product's own store call, not a bare Open. */
            await service.GetStoreSchemaVersionAsync(ct);

            await using var probe = new NpgsqlConnection(scratch.ConnectionString);
            await probe.OpenAsync(ct);
            await using var check = new NpgsqlCommand(
                "SELECT application_name FROM pg_stat_activity "
                + "WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
                probe);
            var applicationName = (string)(await check.ExecuteScalarAsync(ct))!;

            Assert.Equal("PerformanceMonitorDarling-Viewer", applicationName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// BRING-YOUR-OWN with the operator's own <c>Application Name</c>: the set-if-ABSENT contract means the
    /// operator's value wins, and the backend the Viewer opens shows it, not
    /// <see cref="ViewerSettings.ApplicationName"/>.
    /// </summary>
    [Fact]
    public async Task ViewerDataService_BringYourOwnWithAnOperatorSuppliedName_KeepsTheOperatorsValue()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4479 Viewer product-path live pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        try
        {
            await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
            {
                await setup.OpenAsync(ct);
                await PgMigrations.MigrateAsync(setup, ct);
            }

            /* Re-enable pooling on the scratch string for the same reason the sibling test does — the
               scratch fixture disables it so a stray pooled connection can never pin the scratch database,
               which would otherwise also leave nothing idle for this test's pg_stat_activity probe. */
            var operatorConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Pooling = true,
                MinPoolSize = 1,
                ApplicationName = "OperatorsOwn",
            }.ConnectionString;

            await using var service = new ViewerDataService(operatorConnectionString);

            await service.GetStoreSchemaVersionAsync(ct);

            await using var probe = new NpgsqlConnection(scratch.ConnectionString);
            await probe.OpenAsync(ct);
            await using var check = new NpgsqlCommand(
                "SELECT application_name FROM pg_stat_activity "
                + "WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
                probe);
            var applicationName = (string)(await check.ExecuteScalarAsync(ct))!;

            Assert.Equal("OperatorsOwn", applicationName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
