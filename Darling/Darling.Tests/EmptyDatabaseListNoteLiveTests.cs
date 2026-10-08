/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Drives the host's REAL per-database loop with a database list that has nothing left to read. The run returns
/// no rows, does not fail, and carries the note that says why: every user database monitored as its own server,
/// or every database excluded. An empty list opens no per-database connection, so the only hook is the list.
/// </summary>
[Collection("live-postgres")]
public sealed class EmptyDatabaseListNoteLiveTests
{
    private const int LiveServerId = -490061;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task APerDatabaseRunWithNoDatabaseToRead_ReturnsItsNote_AndNoRows(bool everyDatabaseSeparatelyMonitored)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live empty database list test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the run reads and writes only under a distinctive fake server id, and the cleanup removes
           whatever the run saved for it. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(
            postgres,
            new CollectorDeltaCalculator(),
            separatelyMonitoredDatabases: _ => everyDatabaseSeparatelyMonitored ? new[] { "alpha", "zeta" } : Array.Empty<string>());

        var bodySucceeded = false;
        try
        {
            /* Separately monitored: the server lists both databases and the long-query read leaves both out.
               Excluded: the server's exclusions took every database out of the list before the read saw it. */
            runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(
                everyDatabaseSeparatelyMonitored ? new List<string> { "alpha", "zeta" } : new List<string>());

            var server = new ServerRuntime
            {
                Config = new MonitoredServer
                {
                    Name = "t",
                    Host = "h",
                    ExcludedDatabases = everyDatabaseSeparatelyMonitored ? new List<string>() : new List<string> { "alpha", "zeta" },
                },
                ConnectionString = "Server=azure;Database=master",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "h",
                ServerId = LiveServerId,
            };

            var result = await runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(0, result.Rows);
            Assert.Equal(
                everyDatabaseSeparatelyMonitored ? EmptyDatabaseListNote.EverySeparatelyMonitored : EmptyDatabaseListNote.EveryExcluded,
                result.Note);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// A real logical server lists master beside its user databases, and the long-query trace never keeps a session in
    /// master, so the read leaves it out. A list of master plus user databases that are all monitored separately has
    /// nothing left to read and says so; master alone says why only when the exclusions took the user databases (#4961).
    /// The first run above lists user databases only. Here a provider refuses every connection, so a read that
    /// reaches any database, master included, fails the run by name instead of reading nothing.
    /// </summary>
    [Theory]
    [InlineData("master,alpha,zeta", "", "alpha,zeta", EmptyDatabaseListNote.EverySeparatelyMonitored)]
    [InlineData("master", "alpha,zeta", "", EmptyDatabaseListNote.EveryExcluded)]
    [InlineData("master", "", "", null)]
    public async Task APerDatabaseRunWhoseListHoldsMaster_ReturnsItsNote_AndReadsNoDatabase(
        string listed, string excluded, string separatelyMonitored, string? expectedNote)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live empty database list test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the run reads and writes only under a distinctive fake server id, and the cleanup removes
           whatever the run saved for it. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(
            postgres,
            new CollectorDeltaCalculator(),
            separatelyMonitoredDatabases: _ => Names(separatelyMonitored));

        var bodySucceeded = false;
        try
        {
            runner.TargetProviderOverrideForTests = _ => new RefusingTargetProvider();
            runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string>(Names(listed)));

            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "t", Host = "h", ExcludedDatabases = new List<string>(Names(excluded)) },
                ConnectionString = "Server=azure;Database=master",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "h",
                ServerId = LiveServerId,
            };

            var result = await runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(0, result.Rows);
            Assert.Equal(expectedNote, result.Note);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static string[] Names(string commaSeparated) =>
        commaSeparated.Split(',', StringSplitOptions.RemoveEmptyEntries);

    /// <summary>A target provider that refuses to open any connection, naming the database the run asked for.</summary>
    private sealed class RefusingTargetProvider : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) =>
            throw new InvalidOperationException("the long-query read opened a connection to " + connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            throw new NotSupportedException();

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "database " + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var id = LiveServerId.ToString(CultureInfo.InvariantCulture);
        using var command = new NpgsqlCommand($"DELETE FROM collect.collector_state WHERE server_id = {id}", connection);
        command.CommandTimeout = 30;
        await command.ExecuteNonQueryAsync(ct);
    }
}
