/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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

    /// <summary>One live scenario: scratch databases (each given its setup batch), a store, a runner whose scope is those databases, and cleanup.</summary>
    private sealed record Scenario(
        DarlingCollectorRunner Runner, ServerRuntime Server, NpgsqlConnection Pg, string Master, IReadOnlyList<string> Databases, CancellationToken Ct)
    {
        public string Db(int i) => SqlConnectionString(Databases[i])!;

        public Task<string> ScalarAsync(string sql) => SchemaCaptureEndToEndLiveTests.ScalarAsync(Pg, sql, Ct);

        public Task PassAsync() => DarlingSchemaCapture.RunAsync(Runner, Server, Ct);

        public Task PassUntilAsync(string countSql, string expected) => SchemaCaptureEndToEndLiveTests.PassUntilAsync(Runner, Server, Pg, Ct, countSql, expected);
    }

    private static async Task RunScenarioAsync(string[] setups, Func<Scenario, Task> body)
    {
        var master = SqlConnectionString();
        var storeConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(master is null || string.IsNullOrEmpty(storeConnection), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var installId = Guid.NewGuid().ToString("N")[..8];
        var sessionName = SchemaChangesXeSession.NameFor(installId);
        var databases = setups.Select(_ => "SchemaE2e_" + Guid.NewGuid().ToString("N")[..10]).ToList();
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnection!, ct);
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var pg = new NpgsqlConnection(scratch.ConnectionString);
        await pg.OpenAsync(ct);
        await PgMigrations.MigrateAsync(pg, ct);
        try
        {
            for (var i = 0; i < databases.Count; i++)
            {
                await ExecuteAsync(master!, "CREATE DATABASE [" + databases[i] + "];", ct);
                await ExecuteAsync(SqlConnectionString(databases[i])!, setups[i], ct);
            }

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
                databaseScope: (_, _) => databases,
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => installId);

            await body(new Scenario(runner, server, pg, master!, databases, ct));
            bodySucceeded = true;
        }
        finally
        {
            foreach (var database in databases)
            {
                await ExecuteAsync(master!, "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'" + database + "') BEGIN ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + database + "]; END;", CancellationToken.None);
            }

            await ExecuteAsync(master!, SchemaChangesXeSession.BuildDropSql(sessionName, databaseScoped: false), CancellationToken.None);
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Pass_DdlNamingAnotherDatabase_IsRecordedUnderThatDatabase()
    {
        await RunScenarioAsync(
            [
                "CREATE TABLE dbo.Orders (OrderId int NOT NULL CONSTRAINT PK_Orders PRIMARY KEY, Total int NOT NULL);"
                + " CREATE TABLE dbo.Customers (CustomerId int NOT NULL CONSTRAINT PK_Customers PRIMARY KEY);"
                + " CREATE TABLE dbo.Staging (StagingId int NOT NULL);",
                "CREATE TABLE dbo.Tools (ToolId int NOT NULL);",
            ],
            async s =>
            {
                var sales = s.Databases[0];
                await s.PassAsync();
                Assert.Equal("3", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND change_kind = 'baseline'"));

                /* Three-part names, from master and from another user database: the session's database is not the object's. */
                await ExecuteAsync(s.Master, $"CREATE INDEX IX_Orders_Total ON [{sales}].[dbo].[Orders] (Total);", s.Ct);
                await ExecuteAsync(s.Db(1), $"ALTER TABLE [{sales}].dbo.Customers ADD Note int NULL;", s.Ct);
                await ExecuteAsync(s.Db(1), $"DROP TABLE [{sales}].dbo.Staging;", s.Ct);
                await ExecuteAsync(s.Master, $"CREATE TABLE [{sales}].dbo.Returns (ReturnId int NOT NULL);", s.Ct);

                await s.PassUntilAsync(
                    $"SELECT count(DISTINCT object_name || ':' || change_kind) FROM collect.schema_objects WHERE database_name = '{sales}' AND detected_by = 'xe_event' AND change_kind IN ('altered', 'dropped', 'created')",
                    "4");

                Assert.NotEqual("0", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'Orders' AND change_kind = 'altered' AND detected_by = 'xe_event'"));
                Assert.NotEqual("0", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'Customers' AND change_kind = 'altered' AND detected_by = 'xe_event'"));
                Assert.Equal("1", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'Staging' AND change_kind = 'dropped' AND detected_by = 'xe_event'"));
                Assert.Equal("1", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'Returns' AND change_kind = 'created' AND detected_by = 'xe_event'"));
                Assert.Equal("0", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{s.Databases[1]}' AND change_kind <> 'baseline'"));
            });
    }

    [Fact]
    public async Task Pass_MoreEventsThanTheBufferHolds_ReconcilesOnceThenIsQuiet()
    {
        await RunScenarioAsync(
            ["CREATE TABLE dbo.Orders (OrderId int NOT NULL CONSTRAINT PK_Orders PRIMARY KEY, Total int NOT NULL); CREATE INDEX IX_Orders_Total ON dbo.Orders (Total);"],
            async s =>
            {
                var sales = s.Databases[0];
                await s.PassAsync();

                /* Each REORGANIZE commits one object_altered event and changes no definition: more of them than the buffer holds. */
                await ExecuteAsync(
                    s.Db(0),
                    $"DECLARE @i int = 0; WHILE @i < {SchemaChangesXeSession.MaxEventsLimit + 200} BEGIN ALTER INDEX IX_Orders_Total ON dbo.Orders REORGANIZE; SET @i += 1; END;",
                    s.Ct);
                await s.PassUntilAsync($"SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = '{sales}' AND reason = 'events_lost'", "1");
                Assert.Equal("1", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = '{sales}' AND reason = 'events_lost' AND pass_kind = 'reconcile' AND complete"));

                /* The next passes read a whole buffer and find nothing new: no read, no pass row. */
                var rows = await s.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = '{sales}'");
                await Task.Delay(TimeSpan.FromSeconds(7), s.Ct);
                await s.PassAsync();
                await s.PassAsync();
                var passes = await s.ScalarAsync($"SELECT string_agg(pass_kind || ':' || coalesce(reason, '-') || ':' || events_read, ' | ' ORDER BY collection_time) FROM collect.schema_capture_passes WHERE database_name = '{sales}'");
                Assert.True(rows == await s.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = '{sales}'"), passes);
                Assert.Equal("0", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND change_kind <> 'baseline'"));
            });
    }

    [Fact]
    public async Task Pass_ViewRefreshedAfterItsTableChanged_RecordsTheViewAltered()
    {
        await RunScenarioAsync(
            ["CREATE TABLE dbo.Orders (OrderId int NOT NULL, Total int NOT NULL); EXEC(N'CREATE VIEW dbo.AllOrders AS SELECT * FROM dbo.Orders;');"],
            async s =>
            {
                var sales = s.Databases[0];
                await s.PassAsync();

                /* The table changes; the view's columns do not, until it is refreshed. */
                await ExecuteAsync(s.Db(0), "ALTER TABLE dbo.Orders ALTER COLUMN Total bigint NOT NULL; ALTER TABLE dbo.Orders ADD Discount int NULL;", s.Ct);
                await s.PassUntilAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'Orders' AND valid_to IS NULL AND change_kind = 'altered'", "1");
                Assert.Equal("0", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'AllOrders' AND change_kind = 'altered'"));

                await ExecuteAsync(s.Db(0), "EXEC sp_refreshview N'dbo.AllOrders';", s.Ct);
                await s.PassUntilAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'AllOrders' AND change_kind = 'altered'", "1");

                Assert.Equal("1", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_objects WHERE database_name = '{sales}' AND object_name = 'AllOrders' AND change_kind = 'altered' AND valid_to IS NULL"));
                Assert.Equal("1", await s.ScalarAsync($"SELECT count(*) FROM collect.schema_capture_passes WHERE database_name = '{sales}' AND pass_kind = 'reconcile' AND reason = 'module_refreshed' AND complete"));
                Assert.Equal("bigint", await s.ScalarAsync($"SELECT c.type_name FROM collect.schema_columns AS c JOIN collect.schema_objects AS o ON o.version_id = c.version_id WHERE o.database_name = '{sales}' AND o.object_name = 'AllOrders' AND o.valid_to IS NULL AND c.name = 'Total'"));
            });
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
