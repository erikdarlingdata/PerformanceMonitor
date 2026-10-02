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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact runs in the one scratch database the fixture mints through ScratchPostgres, resets
   the install id table to its freshly migrated shape first, and never touches another test's rows, so the class is
   deliberately NOT [Collection("live-postgres")]. */

/// <summary>The scratch store behind <see cref="StoreInstallIdLiveTests"/>: a database of its own, migrated once
/// through the real ladder, dropped when the class is done. Nothing here enables TimescaleDB, so there is no
/// scheduler to race.</summary>
public sealed class InstallIdScratchStore : IAsyncLifetime
{
    private ScratchPostgres? _scratch;

    /// <summary>The scratch store's connection string, or null when <c>DARLING_TEST_PG</c> is unset (the normal
    /// ungated run), in which case every fact skips.</summary>
    public string? ConnectionString => _scratch?.ConnectionString;

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, CancellationToken.None);
        await using var connection = new NpgsqlConnection(_scratch.ConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        await PgMigrations.MigrateAsync(connection, CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        if (_scratch is not null)
        {
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>
/// The install id store against a real PostgreSQL (#4961). The service makes the id's row at start and the CLI and
/// the Viewer only read it, so what matters is that the make is safe to run twice at once, that a stored id which
/// does not belong to this store (a mismatched binding, or a value that is not eight lowercase hex digits) is
/// replaced rather than trusted, and that the reader writes nothing.
/// </summary>
public sealed class StoreInstallIdLiveTests : IClassFixture<InstallIdScratchStore>
{
    private const string Table = "config.config_install_id";

    private static readonly string[] BadIds =
    {
        "ABCDEF12",     // upper case: on a case-sensitive collation it would name a different session
        "abcdef1",      // one short
        "abcdef123",    // one long
        "abcdefgh",     // not hex
        "abcdef12\n",   // a trailing line ending: a `$` anchor in some engines would let it through
        " abcdef12",    // padding
    };

    private readonly InstallIdScratchStore _store;

    public StoreInstallIdLiveTests(InstallIdScratchStore store)
    {
        _store = store;
    }

    public static TheoryData<string> BadIdData() => new(BadIds);

    private string ConnectionString
    {
        get
        {
            Assert.SkipWhen(string.IsNullOrEmpty(_store.ConnectionString), "Set DARLING_TEST_PG to run the live install id store pins.");
            return _store.ConnectionString!;
        }
    }

    private static string RungSql => PgMigrations.Scripts.Single(m => m.Name == InstallIdRungTests.RungName).Sql;

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = parameter });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The table as the rung leaves it: dropped and made again from the product's own SQL.</summary>
    private async Task<NpgsqlConnection> ResetAsync(CancellationToken ct)
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, $"DROP TABLE IF EXISTS {Table}", ct);
        await ExecAsync(connection, RungSql, ct);
        return connection;
    }

    private static async Task<(string InstallId, long SystemIdentifier, long DatabaseOid)> ReadRowAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT install_id, system_identifier, database_oid FROM {Table} WHERE id = 1", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the install id table has no row");
        return (reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    /// <summary>The binding this store really has, read the way the product's make reads it.</summary>
    private static async Task<(long SystemIdentifier, long DatabaseOid)> RealBindingAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT s.system_identifier, d.oid::bigint FROM pg_control_system() AS s CROSS JOIN pg_database AS d WHERE d.datname = current_database()",
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    [Fact]
    public async Task Ensure_OnAnEmptyTable_MakesOneValidIdBoundToThisStore_AndLogsNoWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);

        var logger = new CapturingTestLogger();
        var id = await StoreInstallId.EnsureAsync(connection, logger, ct);

        Assert.True(InstallId.IsValid(id), $"'{id}' is not eight lowercase hex digits");
        Assert.Equal(1L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {Table}", ct));
        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(id, row.InstallId);
        var binding = await RealBindingAsync(connection, ct);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.DatabaseOid, row.DatabaseOid);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));

        /* A start after that one finds the row and keeps the id: nothing changes and nothing is logged. */
        var again = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(connection, again, ct));
        Assert.Equal(0, again.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {Table}", ct));
    }

    [Fact]
    public async Task Ensure_ConcurrentStartsOnOneStore_MakeOneRowAndTheSameId()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var setup = await ResetAsync(ct);

        const int starts = 12;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loggers = Enumerable.Range(0, starts).Select(_ => new CapturingTestLogger()).ToArray();
        var tasks = loggers.Select(async logger =>
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync(ct);
            await gate.Task;
            return await StoreInstallId.EnsureAsync(connection, logger, ct);
        }).ToArray();

        gate.SetResult();
        var ids = await Task.WhenAll(tasks);

        Assert.Single(ids.Distinct());
        Assert.True(InstallId.IsValid(ids[0]));
        Assert.Equal(1L, await ScalarAsync<long>(setup, $"SELECT count(*) FROM {Table}", ct));
        Assert.Equal(ids[0], (await ReadRowAsync(setup, ct)).InstallId);
        Assert.All(loggers, logger => Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning)));
    }

    [Theory]
    [InlineData("system_identifier")]
    [InlineData("database_oid")]
    public async Task Ensure_AMismatchedBinding_MakesANewId_AndLogsOneWarningNamingBothIds(string boundColumn)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);

        var oldId = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        await ExecAsync(connection, $"UPDATE {Table} SET {boundColumn} = {boundColumn} + 1", ct);

        var logger = new CapturingTestLogger();
        var newId = await StoreInstallId.EnsureAsync(connection, logger, ct);

        Assert.True(InstallId.IsValid(newId));
        Assert.NotEqual(oldId, newId);
        var warning = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains(oldId, warning, StringComparison.Ordinal);
        Assert.Contains(newId, warning, StringComparison.Ordinal);

        var row = await ReadRowAsync(connection, ct);
        var binding = await RealBindingAsync(connection, ct);
        Assert.Equal(newId, row.InstallId);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.DatabaseOid, row.DatabaseOid);

        /* The next start is an ordinary one: same id, no second warning. */
        var after = new CapturingTestLogger();
        Assert.Equal(newId, await StoreInstallId.EnsureAsync(connection, after, ct));
        Assert.Equal(0, after.CountAtLevel(LogLevel.Warning));
    }

    [Theory]
    [MemberData(nameof(BadIdData))]
    public async Task Ensure_ABadStoredId_MakesANewId_AndLogsOneWarningNamingBothIds(string badId)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);

        /* The CHECK is what keeps a bad value out, so a fact that plants one has to take it away first (in a
           store of its own, so nothing else sees the table without it). The binding stays the real one: only
           the id is wrong. */
        await ExecAsync(connection, $"ALTER TABLE {Table} DROP CONSTRAINT ck_config_install_id_format", ct);
        var binding = await RealBindingAsync(connection, ct);
        await ExecAsync(connection,
            $"INSERT INTO {Table} (id, install_id, system_identifier, database_oid) VALUES (1, $1, $2, $3)", ct,
            badId, binding.SystemIdentifier, binding.DatabaseOid);

        var logger = new CapturingTestLogger();
        var newId = await StoreInstallId.EnsureAsync(connection, logger, ct);

        Assert.True(InstallId.IsValid(newId));
        Assert.NotEqual(badId, newId);
        var warning = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains(badId, warning, StringComparison.Ordinal);
        Assert.Contains(newId, warning, StringComparison.Ordinal);
        Assert.Equal(newId, (await ReadRowAsync(connection, ct)).InstallId);
    }

    [Fact]
    public async Task Ensure_ConcurrentStartsOnAMismatchedRow_MakeOneNewId_AndLogOneWarningInTotal()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var setup = await ResetAsync(ct);

        var oldId = await StoreInstallId.EnsureAsync(setup, new CapturingTestLogger(), ct);
        await ExecAsync(setup, $"UPDATE {Table} SET system_identifier = system_identifier + 1", ct);

        const int starts = 12;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loggers = Enumerable.Range(0, starts).Select(_ => new CapturingTestLogger()).ToArray();
        var tasks = loggers.Select(async logger =>
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync(ct);
            await gate.Task;
            return await StoreInstallId.EnsureAsync(connection, logger, ct);
        }).ToArray();

        gate.SetResult();
        var ids = await Task.WhenAll(tasks);

        var newId = Assert.Single(ids.Distinct());
        Assert.NotEqual(oldId, newId);
        Assert.True(InstallId.IsValid(newId));
        Assert.Equal(1, loggers.Sum(logger => logger.CountAtLevel(LogLevel.Warning)));
        Assert.Equal(1L, await ScalarAsync<long>(setup, $"SELECT count(*) FROM {Table}", ct));
        Assert.Equal(newId, (await ReadRowAsync(setup, ct)).InstallId);
    }

    [Theory]
    [MemberData(nameof(BadIdData))]
    public async Task TheTable_RefusesAnIdThatIsNotEightLowercaseHexDigits(string badId)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var binding = await RealBindingAsync(connection, ct);

        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
            $"INSERT INTO {Table} (id, install_id, system_identifier, database_oid) VALUES (1, $1, $2, $3)", ct,
            badId, binding.SystemIdentifier, binding.DatabaseOid));

        Assert.Equal("23514", ex.SqlState);
        Assert.Equal("ck_config_install_id_format", ex.ConstraintName);
    }

    [Fact]
    public async Task TheTable_HoldsOneRowOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var binding = await RealBindingAsync(connection, ct);

        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
            $"INSERT INTO {Table} (id, install_id, system_identifier, database_oid) VALUES (2, 'abcdef12', $1, $2)", ct,
            binding.SystemIdentifier, binding.DatabaseOid));

        Assert.Equal("23514", ex.SqlState);
    }

    [Fact]
    public async Task TryRead_ReadsTheStoredId_AndNeverWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);

        /* No row yet: nothing comes back and the read makes none. */
        Assert.Null(await StoreInstallId.TryReadAsync(connection, ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {Table}", ct));

        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var before = await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct);

        Assert.Equal(id, await StoreInstallId.TryReadAsync(connection, ct));
        await using (var dataSource = NpgsqlDataSource.Create(ConnectionString))
        {
            Assert.Equal(id, await StoreInstallId.TryReadAsync(dataSource, ct));
        }

        /* A read leaves the row exactly as it found it (a write would have given it a new xmin). */
        Assert.Equal(before, await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct));
    }

    [Fact]
    public async Task TryRead_OnAStoreWithoutTheTable_IsNull_AndOnABadStoredValue_IsNull_WithoutRepairingIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);

        await ExecAsync(connection, $"DROP TABLE {Table}", ct);
        Assert.Null(await StoreInstallId.TryReadAsync(connection, ct));

        await ExecAsync(connection, RungSql, ct);
        await ExecAsync(connection, $"ALTER TABLE {Table} DROP CONSTRAINT ck_config_install_id_format", ct);
        var binding = await RealBindingAsync(connection, ct);
        await ExecAsync(connection,
            $"INSERT INTO {Table} (id, install_id, system_identifier, database_oid) VALUES (1, 'ABCDEF12', $1, $2)", ct,
            binding.SystemIdentifier, binding.DatabaseOid);

        Assert.Null(await StoreInstallId.TryReadAsync(connection, ct));
        Assert.Equal("ABCDEF12", (await ReadRowAsync(connection, ct)).InstallId);
    }
}
