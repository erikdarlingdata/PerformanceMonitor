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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres (dropped when the fixture is
   disposed) and touches nothing on the shared one. It reads no wall clock. */

/// <summary>
/// #6630: on a store the operator brings, the MCP and web hosts open the store with the operator's string. A string with no
/// Search Path, on a database whose default search path was never set (a login that does not own the database cannot set it),
/// left every bare table name unresolvable: the server-name tools answered 42P01. The hosts now build their data source through
/// <see cref="DarlingWorker.CreateHostStoreDataSource"/>, which gives the string the collect/config path the way the worker does.
/// </summary>
public sealed class DarlingHostStoreSearchPathLiveTests
{
    private static async Task<ScratchPostgres> OpenStoreWithNoDatabaseDefaultPathAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live host store search path test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        try
        {
            var migrating = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
            await using (var connection = new NpgsqlConnection(migrating))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, null, ct);
                await using var seed = new NpgsqlCommand(
                    "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
                    + "VALUES (7, 'alpha-sql-01', 'alpha-sql-01', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')", connection);
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* The state a login that does not own the database leaves: whatever default the service set is gone. */
            await using (var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString!)))
            {
                await admin.OpenAsync(ct);
                await using var reset = new NpgsqlCommand($"ALTER DATABASE \"{scratch.DatabaseName}\" RESET search_path", admin);
                await reset.ExecuteNonQueryAsync(ct);
            }

            return scratch;
        }
        catch
        {
            await scratch.DisposeAsync();
            throw;
        }
    }

    [Theory]
    [InlineData("mcp")]
    [InlineData("web")]
    public async Task AHostDataSource_OnAStringWithNoSearchPath_ResolvesBareNames_AndAServerNameTool_Answers(string host)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenStoreWithNoDatabaseDefaultPathAsync();
        var applicationName = host == "mcp" ? DarlingManagedPostgres.McpApplicationName : DarlingManagedPostgres.WebApplicationName;
        Assert.DoesNotContain("Search Path", scratch.UnpinnedConnectionString, StringComparison.OrdinalIgnoreCase);

        /* The premise: the string as the operator gave it cannot see the store's tables. */
        await using (var asGiven = NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(scratch.UnpinnedConnectionString)))
        {
            var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var command = asGiven.CreateCommand("SELECT count(*) FROM servers");
                await command.ExecuteScalarAsync(ct);
            });
            Assert.Equal("42P01", refused.SqlState);
        }

        await using var postgres = DarlingWorker.CreateHostStoreDataSource(scratch.UnpinnedConnectionString, applicationName);
        await using (var command = postgres.CreateCommand("SELECT count(*) FROM servers"))
        {
            Assert.Equal(1L, Convert.ToInt64(await command.ExecuteScalarAsync(ct)));
        }

        /* The helper keeps the surface name and the UTC pin the hosts always had. */
        await using (var command = postgres.CreateCommand("SELECT current_setting('application_name') || '|' || current_setting('TimeZone')"))
        {
            Assert.Equal(applicationName + "|UTC", Convert.ToString(await command.ExecuteScalarAsync(ct)));
        }

        var (resolved, error) =await DarlingServerResolver.ResolveOrErrorAsync(postgres, "alpha-sql-01", ct);
        Assert.Null(error);
        Assert.Equal(7, resolved.ServerId);
    }

    [Fact]
    public async Task AHostDataSource_KeepsAnOperatorsOwnSearchPath()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenStoreWithNoDatabaseDefaultPathAsync();
        var theirs = new NpgsqlConnectionStringBuilder(scratch.UnpinnedConnectionString) { SearchPath = "public" }.ConnectionString;

        await using var postgres = DarlingWorker.CreateHostStoreDataSource(theirs, DarlingManagedPostgres.McpApplicationName);
        await using var command = postgres.CreateCommand("SHOW search_path");
        Assert.Equal("public", Convert.ToString(await command.ExecuteScalarAsync(ct)));
    }
}
