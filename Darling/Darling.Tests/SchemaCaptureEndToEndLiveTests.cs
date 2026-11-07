/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643, the whole pass against a real SQL Server and a real store: the pass creates its event session and baselines a scratch database,
/// the database then changes (a column added, a table renamed, a view dropped, an index rebuilt) and the next passes record an altered, a
/// renamed and a dropped version, and no version for the index rebuild, which changes no definition. Gated on <c>DARLING_TEST_SQL</c> and
/// <c>DARLING_TEST_PG</c>; <c>DARLING_TEST_SQL_READONLY=1</c> skips it because it creates a session and a database. The session has an
/// install id of its own, so it never touches another install's. It shares a collection with the other live schema classes because a
/// server-wide session sees every database's DDL: the event tests count the events they read, and DDL from a class running beside them
/// would be among them.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
[Collection("schema-xe-live-sql")]
public sealed class SchemaCaptureEndToEndLiveTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) and DARLING_TEST_PG to run the live schema capture test; DARLING_TEST_SQL_READONLY=1 skips it.";

    private static string? SqlConnectionString(string database = "master")
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host) || Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1")
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "Darling.Tests schema capture",
            Pooling = false,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        await ActionProposalStoreLiveTests.ScalarAsync(connection, sql, ct);

    [Fact]
    public async Task Pass_BaselinesThenRecordsAlteredRenamedAndDropped_AndNothingForAnIndexRebuild()
    {
        var master = SqlConnectionString();
        var storeConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(master is null || string.IsNullOrEmpty(storeConnection), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var installId = Guid.NewGuid().ToString("N")[..8];
        var sessionName = SchemaChangesXeSession.NameFor(installId);
        var database = "SchemaE2e_" + Guid.NewGuid().ToString("N")[..10];
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnection!, ct);
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var pg = new NpgsqlConnection(scratch.ConnectionString);
        await pg.OpenAsync(ct);
        await PgMigrations.MigrateAsync(pg, ct);

        await ExecuteAsync(master!, "CREATE DATABASE [" + database + "];", ct);
        try
        {
            var db = SqlConnectionString(database)!;
            await ExecuteAsync(db, "CREATE TABLE dbo.Orders (OrderId int NOT NULL CONSTRAINT PK_Orders PRIMARY KEY, Total int NOT NULL);", ct);
            await ExecuteAsync(db, "CREATE TABLE dbo.Customers (CustomerId int NOT NULL CONSTRAINT PK_Customers PRIMARY KEY);", ct);
            await ExecuteAsync(db, "CREATE INDEX IX_Orders_Total ON dbo.Orders (Total);", ct);
            await ExecuteAsync(db, "CREATE VIEW dbo.OrderTotals AS SELECT OrderId, Total FROM dbo.Orders;", ct);

            var config = new MonitoredServer { Name = "schema-e2e", Host = "schema-e2e-sql" };
            var server = new ServerRuntime
            {
                Config = config,
                ConnectionString = master!,
                Target = new CollectorTargetInfo(),
                StorageName = config.StorageName,
                ServerId = config.ServerId,
            };
            var runner = new DarlingCollectorRunner(
                store,
                new CollectorDeltaCalculator(),
                databaseScope: (_, _) => new List<string> { database },
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => installId);

            /* The first pass creates the session and baselines the database: two tables and a view. */
            var first = await DarlingSchemaCapture.RunAsync(runner, server, ct);
            Assert.Equal("3", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND change_kind = 'baseline' AND valid_to IS NULL", ct));
            Assert.Equal("1", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = '{database}' AND pass_kind = 'baseline' AND complete", ct));
            Assert.True(await SchemaCaptureStateStore.HasSessionAsync(pg, server.ServerId, ct), first.HostNote);

            /* A column, a view drop and an index rebuild. The session delivers an event within seconds, so the pass is repeated until both
               versions are in, and every pass is a legitimate one: what it has already read, it does not read again. */
            await ExecuteAsync(db, "ALTER TABLE dbo.Orders ADD Discount int NULL;", ct);
            await ExecuteAsync(db, "DROP VIEW dbo.OrderTotals;", ct);
            await ExecuteAsync(db, "ALTER INDEX IX_Orders_Total ON dbo.Orders REBUILD;", ct);
            await PassUntilAsync(
                runner, server, pg, ct,
                $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND change_kind IN ('altered', 'dropped')", "2");

            Assert.Equal("1", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND change_kind = 'altered' AND object_name = 'Orders' AND detected_by = 'xe_event' AND change_login IS NOT NULL", ct));
            Assert.Equal("1", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND change_kind = 'dropped' AND object_name = 'OrderTotals' AND detected_by = 'xe_event' AND valid_to IS NOT NULL", ct));
            /* The rebuild moved no definition: the table has its baseline and its one altered version, and no more. */
            Assert.Equal("2", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND object_name = 'Orders'", ct));

            /* A rename raises no object event of its own; the session sees sp_rename, and the pass reads the database in full. */
            await ExecuteAsync(db, "EXEC sp_rename 'dbo.Customers', 'Clients';", ct);
            await PassUntilAsync(
                runner, server, pg, ct,
                $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND change_kind = 'renamed'", "1");

            Assert.Equal("1", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND change_kind = 'renamed' AND object_name = 'Clients' AND valid_to IS NULL", ct));
            Assert.Equal("1", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = '{database}' AND pass_kind = 'reconcile' AND reason = 'renamed' AND complete", ct));
            Assert.Equal("0", await ScalarAsync(pg, $"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{database}' AND change_kind = 'created'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await ExecuteAsync(master!, "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'" + database + "') BEGIN ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + database + "]; END;", CancellationToken.None);
            await ExecuteAsync(master!, SchemaChangesXeSession.BuildDropSql(sessionName, databaseScoped: false), CancellationToken.None);
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task PassUntilAsync(
        DarlingCollectorRunner runner, ServerRuntime server, NpgsqlConnection pg, CancellationToken ct, string countSql, string expected)
    {
        for (var attempt = 0; attempt < 10; attempt++)
        {
            await Task.Delay(attempt == 0 ? TimeSpan.FromSeconds(7) : TimeSpan.FromSeconds(3), ct);
            await DarlingSchemaCapture.RunAsync(runner, server, ct);
            if (await ScalarAsync(pg, countSql, ct) == expected)
            {
                return;
            }
        }
    }
}
