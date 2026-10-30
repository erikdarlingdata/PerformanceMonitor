/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6387: the Viewer shows the three PostgreSQL reads the web has - configuration changes, the logging audit and one
/// deadlock's wait graph. Each Viewer method must hand back exactly what the shared read returns for the same inputs (the
/// web endpoint and the MCP tool call that same read), and each grid row must carry what the reader row held.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerPgConfigAuditDeadlockReadsTests
{
    private const int ServerId = -638701;
    private const string ServerName = "viewer-6387";

    private static readonly DateTime Base = new(2026, 7, 4, 12, 0, 0, DateTimeKind.Unspecified);

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the Viewer PostgreSQL config/audit/deadlock live tests.");
        return connectionString!;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_server_config WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_deadlocks WHERE server_id = $1", ServerId);
    }

    private static Task SeedConfigAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string name, string setting, string bootVal) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
     source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, NULL, 'Logging', 'user', 'string', 'configuration file', $7, $6, NULL, 0, false, NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, name, setting, bootVal);

    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await DeleteAsync(c, ct);

        /* Two snapshots ten minutes apart: work_mem moves between them (one change), log_lock_waits is in the newest one only. */
        await SeedConfigAsync(c, ct, Base, "work_mem", "128MB", "4MB");
        await SeedConfigAsync(c, ct, Base.AddMinutes(10), "work_mem", "256MB", "4MB");
        await SeedConfigAsync(c, ct, Base.AddMinutes(10), "log_lock_waits", "off", "off");

        await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $5, 4242, 2, 'AAAA6387AAAA6387AAAA6387AAAA6387', 'ShareLock', 'relation orders', 'UPDATE orders SET status = ?', 'Process 4242 waits for ShareLock on relation orders; blocked by process 4243.')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(Base.AddMinutes(12)), ServerId, ServerName, DarlingMcpTestData.Naive(Base.AddMinutes(11)));
    }

    [Fact]
    public async Task ConfigChanges_TheViewerMethodReturnsWhatTheSharedReadReturns()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct);
            await using var postgres = OpenDataSource(cs);
            await using var viewer = new ViewerDataService(cs);
            var start = Base.AddHours(-1);
            var end = Base.AddHours(1);

            var shared = await DarlingPgServerConfigReader.GetConfigChangesAsync(postgres, ServerId, start, end, 100, ct);
            var fromViewer = await viewer.GetPgServerConfigChangesAsync(ServerId, start, end, cancellationToken: ct);

            var change = Assert.Single(shared);
            Assert.Equal("work_mem", change.Name);
            Assert.Equal(shared, fromViewer);

            var gridRow = PgDisplay.ConfigChange(fromViewer[0]);
            Assert.Equal("128MB", gridRow.OldValue);
            Assert.Equal("256MB", gridRow.NewValue);
            Assert.Equal("Server", gridRow.Scope);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task LoggingAudit_TheViewerMethodReturnsTheSharedJudgmentOfTheNewestSnapshot()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct);
            await using var postgres = OpenDataSource(cs);
            await using var viewer = new ViewerDataService(cs);

            var snapshot = await DarlingPgLoggingAuditReader.GetNewestSnapshotAsync(postgres, ServerId, ct);
            var shared = DarlingPgLoggingAudit.Audit(snapshot);
            var fromViewer = await viewer.GetPgLoggingAuditAsync(ServerId, ct);

            Assert.NotNull(fromViewer);
            Assert.Equal(shared.CapturedAt, fromViewer!.CapturedAt);
            Assert.Equal(shared.Facets, fromViewer.Facets);
            Assert.Equal(DarlingPgLoggingAudit.Off, fromViewer.Facets.Single(f => f.Setting == "log_lock_waits").Verdict);

            var rows = fromViewer.Facets.Select(PgDisplay.LoggingAudit).ToList();
            Assert.Equal(shared.Facets.Select(f => f.Setting), rows.Select(r => r.Setting));
            Assert.Equal(DarlingPgLoggingAudit.Off, rows.Single(r => r.Setting == "log_lock_waits").Verdict);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task LoggingAudit_AServerWithNoSnapshotIsNullNotAnAuditOfNothing()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteAsync(connection, ct);
            await using var viewer = new ViewerDataService(cs);

            Assert.Null(await viewer.GetPgLoggingAuditAsync(ServerId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task DeadlockDetail_TheViewerMethodReturnsWhatTheSharedReadReturnsForTheListedIdentity()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct);
            await using var postgres = OpenDataSource(cs);
            await using var viewer = new ViewerDataService(cs);

            var listed = await viewer.GetPgDeadlocksAsync(ServerId, Base.AddHours(-1), Base.AddHours(1), cancellationToken: ct);
            var row = Assert.Single(listed);

            var shared = await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, row.DeadlockHash, 1, ct);
            var fromViewer = await viewer.GetPgDeadlockDetailAsync(ServerId, row.DeadlockHash, cancellationToken: ct);

            Assert.Single(shared);
            Assert.Equal(shared, fromViewer);
            Assert.Contains("ShareLock on relation orders", fromViewer[0].GraphText);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Theory]
    [InlineData(null, null, "Server")]
    [InlineData("appdb", null, "database appdb")]
    [InlineData(null, "etl", "role etl")]
    [InlineData("appdb", "etl", "database appdb, role etl")]
    public void ConfigChangeScope_NamesTheOverrideScopeAndCallsAServerWideChangeServer(string? database, string? role, string expected)
    {
        var row = new DarlingPgServerConfigReader.PgConfigChangeRow(
            Base, "work_mem", "4MB", "8MB", null, "user", "configuration file", null, database, role);

        Assert.Equal(expected, PgDisplay.ConfigChangeScope(row));
    }
}
