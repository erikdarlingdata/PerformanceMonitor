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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact here mints its own scratch database through ScratchPostgres (and, for the privilege fact, a
   role with a per-run name) and never touches another test's rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The live half of #5655: the reader that the viewers use, and the write the service makes at startup, over a scratch store
/// migrated by the product's own ladder. The reader returns the rows, returns none for an empty table and for a missing
/// table, and does not swallow any other error. The startup write upserts: a second start of the same host updates its one row,
/// a second host adds a row, and a failed write logs a warning and never throws.
/// </summary>
public sealed class ServiceVersionLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the service version live pins (each mints its own scratch database).";

    [Fact]
    public async Task TheReader_ReturnsEveryHostsRow_InHostOrder_WithMicrosecondTimes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = await OpenMigratedAsync(scratch, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner,
                "INSERT INTO config.service_version (service_host, product_version, process_started_at, updated_at) VALUES "
                + "('ZED', '3.11.0-nightly.7', '2026-10-01 12:00:00.123456+00', '2026-10-01 12:00:01.654321+00'), "
                + "('alpha', '3.10.0', '2026-09-30 01:02:03.000001+00', '2026-09-30 01:02:04+00')", ct);

            var rows = await ServiceVersionTable.ReadAsync(dataSource, 15, ct);

            Assert.Equal(new[] { "alpha", "ZED" }.Order(StringComparer.Ordinal).ToArray(), rows.Select(r => r.ServiceHost).Order(StringComparer.Ordinal).ToArray());
            var zed = rows.Single(r => r.ServiceHost == "ZED");
            Assert.Equal("3.11.0-nightly.7", zed.ProductVersion);
            Assert.Equal(new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc).AddTicks(1234560), zed.ProcessStartedUtc);
            Assert.Equal(DateTimeKind.Utc, zed.ProcessStartedUtc.Kind);
            Assert.Equal(new DateTime(2026, 10, 1, 12, 0, 1, DateTimeKind.Utc).AddTicks(6543210), zed.UpdatedUtc);
            Assert.Equal("3.10.0", rows.Single(r => r.ServiceHost == "alpha").ProductVersion);
            Assert.Equal(2, rows.Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheReader_ReturnsNoRows_ForAnEmptyTable_AndForAMissingTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = await OpenMigratedAsync(scratch, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            /* A store whose service has not started since the upgrade: the table is there and empty. */
            Assert.Empty(await ServiceVersionTable.ReadAsync(dataSource, 15, ct));

            /* A store an older service made: no table at all (42P01), answered the same way. */
            await ExecAsync(owner, "DROP TABLE config.service_version", ct);
            Assert.Empty(await ServiceVersionTable.ReadAsync(dataSource, 15, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheReader_AnswersOnlyTheUndefinedTableError_AnyOtherFailureReachesTheCaller()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = await OpenMigratedAsync(scratch, ct);
        var role = "sv_noaccess_" + Guid.NewGuid().ToString("N")[..12];

        var bodySucceeded = false;
        try
        {
            /* A login that can connect but was granted nothing on the config schema: the SELECT fails with 42501, not 42P01. */
            await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER; GRANT CONNECT ON DATABASE \"{scratch.DatabaseName}\" TO {role};", ct);
            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Pooling = false };
            await using var noAccess = NpgsqlDataSource.Create(builder.ConnectionString);

            var ex = await Assert.ThrowsAsync<PostgresException>(() => ServiceVersionTable.ReadAsync(noAccess, 15, ct));
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, ex.SqlState);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};", cleanupCt));
        }
    }

    [Fact]
    public async Task TheStartupWrite_Upserts_ASecondStartUpdatesTheRow_AndEachHostKeepsOne()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = await OpenMigratedAsync(scratch, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new CapturingLogger();

        var bodySucceeded = false;
        try
        {
            var configVersionBefore = await ScalarAsync(owner, "SELECT max(config_version) FROM config.config_service", ct);

            /* The first start of a host, with a start time that has more precision than the store keeps. */
            var firstStart = new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc).AddTicks(1234567);
            Assert.True(await DarlingServiceVersionRecorder.RecordAsync(dataSource, logger, "HOST-A", "3.10.0", firstStart, ct));
            var first = Assert.Single(await ServiceVersionTable.ReadAsync(dataSource, 15, ct));
            Assert.Equal("HOST-A", first.ServiceHost);
            Assert.Equal("3.10.0", first.ProductVersion);
            Assert.Equal(new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc).AddTicks(1234560), first.ProcessStartedUtc);
            Assert.True(DateTime.UtcNow - first.UpdatedUtc < TimeSpan.FromMinutes(5), "updated_at is the store's clock at the write");

            /* A restart of the same host with a newer version: the same row, updated. */
            var secondStart = firstStart.AddDays(3);
            Assert.True(await DarlingServiceVersionRecorder.RecordAsync(dataSource, logger, "HOST-A", "3.11.0", secondStart, ct));
            var second = Assert.Single(await ServiceVersionTable.ReadAsync(dataSource, 15, ct));
            Assert.Equal("3.11.0", second.ProductVersion);
            Assert.Equal(ServiceVersionTable.ToStoredInstant(secondStart), second.ProcessStartedUtc);
            Assert.True(second.UpdatedUtc >= first.UpdatedUtc);

            /* A second host adds a row and leaves the first alone. */
            Assert.True(await DarlingServiceVersionRecorder.RecordAsync(dataSource, logger, "HOST-B", "3.10.0", firstStart, ct));
            var both = await ServiceVersionTable.ReadAsync(dataSource, 15, ct);
            Assert.Equal(new[] { ("HOST-A", "3.11.0"), ("HOST-B", "3.10.0") }, both.Select(r => (r.ServiceHost, r.ProductVersion)).ToArray());

            /* The write never bumps config_version: that would make every service reload its configuration. */
            Assert.Equal(configVersionBefore, await ScalarAsync(owner, "SELECT max(config_version) FROM config.config_service", ct));
            Assert.Empty(logger.Lines);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheStartupWrite_RecordsThisMachinesHostAndTheProductVersion()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = await OpenMigratedAsync(scratch, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            Assert.True(await DarlingServiceVersionRecorder.RecordAsync(dataSource, new CapturingLogger(), DateTime.UtcNow, ct));
            var row = Assert.Single(await ServiceVersionTable.ReadAsync(dataSource, 15, ct));

            /* The same method --version, the diagnostics bundle and get_collection_health use. */
            Assert.Equal(DarlingCliCommands.ProductVersion(), row.ProductVersion);
            Assert.Equal(Environment.MachineName, row.ServiceHost);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AFailedWrite_LogsAWarning_ReturnsFalse_AndDoesNotThrow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = await OpenMigratedAsync(scratch, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new CapturingLogger();

        var bodySucceeded = false;
        try
        {
            /* A store that has no table: the write fails and the service goes on. */
            await ExecAsync(owner, "DROP TABLE config.service_version", ct);
            Assert.False(await DarlingServiceVersionRecorder.RecordAsync(dataSource, logger, "HOST-A", "3.10.0", DateTime.UtcNow, ct));

            var line = Assert.Single(logger.Lines);
            Assert.StartsWith("Warning:", line, StringComparison.Ordinal);
            Assert.Contains("HOST-A", line, StringComparison.Ordinal);
            Assert.Contains("config.service_version", line, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ACancelledWrite_StillCancels_ItIsNotSwallowedAsAFailedWrite()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = await OpenMigratedAsync(scratch, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                DarlingServiceVersionRecorder.RecordAsync(dataSource, new CapturingLogger(), "HOST-A", "3.10.0", DateTime.UtcNow, cancelled.Token));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public void TheWorker_RecordsTheVersion_BeforeTheConfigViewIsBuilt()
    {
        var worker = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var record = worker.IndexOf("DarlingServiceVersionRecorder.RecordAsync(postgres, _logger, ProcessStartUtc(), stoppingToken)", StringComparison.Ordinal);
        var provider = worker.IndexOf("var configProvider = new StoreConfigProvider(postgres, _logger);", StringComparison.Ordinal);

        Assert.True(record > 0, "the worker records the service version at startup");
        Assert.True(provider > record, "the version row is written before the config provider's first read");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(worker, "DarlingServiceVersionRecorder\\.RecordAsync"));
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(logLevel + ": " + formatter(state, exception));
    }
}
