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
using NpgsqlTypes;
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

    private static string TableOidRungSql => PgMigrations.Scripts.Single(m => m.Name == InstallIdTableOidRungTests.RungName).Sql;

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
        await ExecAsync(connection, TableOidRungSql, ct);
        return connection;
    }

    private static async Task<(string InstallId, long? SystemIdentifier, long DatabaseOid, long? TableOid, int? ServerMajor)> ReadRowAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT install_id, system_identifier, database_oid, table_oid, server_major FROM {Table} WHERE id = 1", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the install id table has no row");
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetInt64(3), reader.IsDBNull(4) ? null : reader.GetInt32(4));
    }

    /// <summary>The binding this store really has, read the way the product's make reads it.</summary>
    private static async Task<(long SystemIdentifier, long DatabaseOid, long TableOid, int ServerMajor)> RealBindingAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT s.system_identifier, d.oid::bigint, 'config.config_install_id'::regclass::oid::bigint, current_setting('server_version_num')::int / 10000 FROM pg_control_system() AS s CROSS JOIN pg_database AS d WHERE d.datname = current_database()",
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt32(3));
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
        Assert.Equal(binding.TableOid, row.TableOid);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);
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
    [InlineData("database_oid", "database OID")]
    [InlineData("table_oid", "table's OID")]
    public async Task Ensure_AMismatchedBinding_MakesANewId_AndLogsOneWarningNamingBothIdsAndTheBindingThatDiffered(string boundColumn, string namedBinding)
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
        Assert.Contains(namedBinding, warning, StringComparison.Ordinal);

        var row = await ReadRowAsync(connection, ct);
        var binding = await RealBindingAsync(connection, ct);
        Assert.Equal(newId, row.InstallId);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.DatabaseOid, row.DatabaseOid);
        Assert.Equal(binding.TableOid, row.TableOid);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);

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
        await ExecAsync(setup, $"UPDATE {Table} SET table_oid = table_oid + 1", ct);

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

    /// <summary>The CLI can read a store before the upgraded service has migrated it: a store still at the rung below
    /// the table OID has neither that column nor the major's, and the reader's statement must not name either.</summary>
    [Fact]
    public async Task TryRead_OnAStoreStillBelowTheTableOidRung_ReadsTheId_AndWritesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);

        await ExecAsync(connection, $"ALTER TABLE {Table} DROP COLUMN table_oid, DROP COLUMN server_major", ct);
        var before = await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct);

        Assert.Equal(id, await StoreInstallId.TryReadAsync(connection, ct));
        await using (var dataSource = NpgsqlDataSource.Create(ConnectionString))
        {
            Assert.Equal(id, await StoreInstallId.TryReadAsync(dataSource, ct));
        }

        Assert.Equal(before, await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct));
        Assert.DoesNotContain("table_oid", StoreInstallId.ReadSql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_major", StoreInstallId.ReadSql, StringComparison.Ordinal);
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
        Assert.Equal("ABCDEF12", await ScalarAsync<string>(connection, $"SELECT install_id FROM {Table} WHERE id = 1", ct));
    }

    /// <summary>
    /// The race-safety rule at the statement itself, deterministically: two starts that read the same row both try to
    /// replace it, and only the first one's UPDATE finds the id it read. The timing-dependent fact above can pass
    /// when one start finishes before the others begin, so this one pins the guard without depending on timing.
    /// </summary>
    [Fact]
    public async Task TheReplacement_IsGuardedByTheIdTheStartRead_SoOnlyOneOfTwoStartsThatSawTheSameRowMakesTheNewId()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var staleId = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var binding = await RealBindingAsync(connection, ct);

        async Task<int> ReplaceAsync(string newId)
        {
            await using var command = new NpgsqlCommand(StoreInstallId.ReplaceSql, connection);
            command.Parameters.Add(new NpgsqlParameter { Value = newId });
            command.Parameters.Add(new NpgsqlParameter { Value = binding.SystemIdentifier });
            command.Parameters.Add(new NpgsqlParameter { Value = binding.DatabaseOid });
            command.Parameters.Add(new NpgsqlParameter { Value = binding.TableOid });
            command.Parameters.Add(new NpgsqlParameter { Value = binding.ServerMajor });
            command.Parameters.Add(new NpgsqlParameter { Value = staleId });
            return await command.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(1, await ReplaceAsync("aaaaaaaa"));
        Assert.Equal(0, await ReplaceAsync("bbbbbbbb"));
        Assert.Equal("aaaaaaaa", (await ReadRowAsync(connection, ct)).InstallId);
    }

    /// <summary>
    /// The rebind's guard at the statement itself: it matches a row exactly when it would change something in it, so of
    /// several starts that saw the same row exactly one writes and logs. When the cluster id is not known, a row that has
    /// one keeps its major, so the statement finds nothing to write although the major differs (0 rows); a row that has
    /// none takes the major (1 row, then none left), and a row that has none of its table OID takes that and keeps the
    /// major. When the cluster id is known the row takes it and the major together.
    /// </summary>
    [Fact]
    public async Task TheRebind_MatchesARowExactlyWhenItWouldChangeSomethingInIt_AndKeepsTheMajorOfARowThatHasAClusterIdWhenNoneIsKnown()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var binding = await RealBindingAsync(connection, ct);
        var oldCluster = binding.SystemIdentifier + 1;
        var oldMajor = binding.ServerMajor - 1;

        async Task<int> RebindAsync(long? knownCluster)
        {
            await using var command = new NpgsqlCommand(StoreInstallId.RebindSql, connection);
            command.Parameters.Add(new NpgsqlParameter { Value = binding.TableOid });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)knownCluster ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { Value = binding.ServerMajor });
            command.Parameters.Add(new NpgsqlParameter { Value = id });
            return await command.ExecuteNonQueryAsync(ct);
        }

        /* A row that has a cluster id, with none known: nothing to write, although the major differs. */
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = {oldCluster}, server_major = {oldMajor}", ct);
        Assert.Equal(0, await RebindAsync(null));
        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(oldCluster, row.SystemIdentifier);
        Assert.Equal(oldMajor, row.ServerMajor);

        /* The same row with the cluster id known: both are written, once. */
        Assert.Equal(1, await RebindAsync(binding.SystemIdentifier));
        row = await ReadRowAsync(connection, ct);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);
        Assert.Equal(0, await RebindAsync(binding.SystemIdentifier));

        /* A row with no cluster id takes the major although none is known, once. */
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = NULL, server_major = {oldMajor}", ct);
        Assert.Equal(1, await RebindAsync(null));
        row = await ReadRowAsync(connection, ct);
        Assert.Null(row.SystemIdentifier);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);
        Assert.Equal(0, await RebindAsync(null));

        /* A row that has a cluster id and no table OID: the table OID is written, and the major stays. */
        await ExecAsync(connection, $"UPDATE {Table} SET table_oid = NULL, system_identifier = {oldCluster}, server_major = {oldMajor}", ct);
        Assert.Equal(1, await RebindAsync(null));
        row = await ReadRowAsync(connection, ct);
        Assert.Equal(binding.TableOid, row.TableOid);
        Assert.Equal(oldCluster, row.SystemIdentifier);
        Assert.Equal(oldMajor, row.ServerMajor);
        Assert.Equal(0, await RebindAsync(null));
    }

    /// <summary>A major upgrade makes a new cluster id, keeps both OIDs and raises the major (the row here is the one the
    /// older version wrote: its cluster id, and a lower major): the id stays, the row is rebound to the new cluster id and
    /// the current major, and one Information line names both cluster ids and both majors. Nothing is a Warning, and a
    /// start after it changes nothing.</summary>
    [Fact]
    public async Task Ensure_AChangedClusterIdWithAHigherMajor_KeepsTheId_RebindsIt_AndLogsOneInformationNamingBothClusterIds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var binding = await RealBindingAsync(connection, ct);
        var oldCluster = binding.SystemIdentifier + 1;
        var oldMajor = binding.ServerMajor - 1;
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = {oldCluster}, server_major = {oldMajor}", ct);

        var logger = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(connection, logger, ct));

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(logger.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal));
        Assert.Contains(oldCluster.ToString(System.Globalization.CultureInfo.InvariantCulture), line, StringComparison.Ordinal);
        Assert.Contains(binding.SystemIdentifier.ToString(System.Globalization.CultureInfo.InvariantCulture), line, StringComparison.Ordinal);
        Assert.Contains($"from {oldMajor} to {binding.ServerMajor}", line, StringComparison.Ordinal);
        Assert.Contains("id stays", line, StringComparison.Ordinal);
        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(id, row.InstallId);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.TableOid, row.TableOid);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);

        var before = await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct);
        var after = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(connection, after, ct));
        Assert.Empty(after.Lines);
        Assert.Equal(before, await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct));
    }

    /// <summary>A changed cluster id with both OIDs matching and no rise in the major is not an upgrade. It is what a copy of
    /// the row into a fresh install of the same version looks like (a fresh cluster numbers its objects from the same start,
    /// so the OIDs can match), or into an install of a lower one, and the copy gets an id of its own: one Warning naming both
    /// ids, the cluster ids and the majors, and a row bound to this store.</summary>
    [Theory]
    [InlineData(0)]     // the stored major is this server's: the same major
    [InlineData(1)]     // the stored major is higher than this server's: a lower major
    public async Task Ensure_AChangedClusterIdWithoutAHigherMajor_MakesANewId_AndLogsOneWarningNamingTheClusterIdsAndMajors(int storedMajorAbove)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var oldId = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var binding = await RealBindingAsync(connection, ct);
        var oldCluster = binding.SystemIdentifier + 1;
        var oldMajor = binding.ServerMajor + storedMajorAbove;
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = {oldCluster}, server_major = {oldMajor}", ct);

        var logger = new CapturingTestLogger();
        var newId = await StoreInstallId.EnsureAsync(connection, logger, ct);

        Assert.True(InstallId.IsValid(newId));
        Assert.NotEqual(oldId, newId);
        Assert.Equal(0, logger.Lines.Count(l => l.StartsWith("Information:", StringComparison.Ordinal)));
        var warning = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains(oldId, warning, StringComparison.Ordinal);
        Assert.Contains(newId, warning, StringComparison.Ordinal);
        Assert.Contains("cluster id", warning, StringComparison.Ordinal);
        Assert.Contains(oldCluster.ToString(System.Globalization.CultureInfo.InvariantCulture), warning, StringComparison.Ordinal);
        Assert.Contains("major version", warning, StringComparison.Ordinal);

        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(newId, row.InstallId);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.TableOid, row.TableOid);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);

        var after = new CapturingTestLogger();
        Assert.Equal(newId, await StoreInstallId.EnsureAsync(connection, after, ct));
        Assert.Empty(after.Lines);
    }

    /// <summary>Several starts that all see the changed cluster id and a lower stored major end on the same id, and one of
    /// them says so.</summary>
    [Fact]
    public async Task Ensure_ConcurrentStartsOnAChangedClusterIdWithAHigherMajor_KeepTheId_AndLogOneInformationInTotal()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var setup = await ResetAsync(ct);
        var id = await StoreInstallId.EnsureAsync(setup, new CapturingTestLogger(), ct);
        await ExecAsync(setup, $"UPDATE {Table} SET system_identifier = system_identifier + 1, server_major = server_major - 1", ct);

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

        Assert.Equal(id, Assert.Single(ids.Distinct()));
        Assert.Equal(0, loggers.Sum(logger => logger.CountAtLevel(LogLevel.Warning)));
        Assert.Equal(1, loggers.Sum(logger => logger.Lines.Count(l => l.StartsWith("Information:", StringComparison.Ordinal))));
        var binding = await RealBindingAsync(setup, ct);
        var row = await ReadRowAsync(setup, ct);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);
    }

    /// <summary>A stored cluster id of NULL (made while the login was refused it) is filled in once the current one is
    /// known, and the id stays. The cluster id was unknown, so nothing says it changed and the two OIDs decide, as they
    /// did before the major was kept.</summary>
    [Fact]
    public async Task Ensure_AStoredNullClusterId_IsFilledIn_AndKeepsTheId()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = NULL", ct);

        var logger = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(connection, logger, ct));

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal((await RealBindingAsync(connection, ct)).SystemIdentifier, (await ReadRowAsync(connection, ct)).SystemIdentifier);
    }

    /// <summary>An unknown cluster id stays unknown to the rule whatever the stored major is: a row stored with a NULL
    /// cluster id and a major that differs from this server's (higher or lower) keeps its id, takes the cluster id and the
    /// current major, and logs no Warning.</summary>
    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public async Task Ensure_AStoredNullClusterId_WithAnotherMajor_KeepsTheId_AndWritesTheCurrentMajor(int storedMajorOffset)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var binding = await RealBindingAsync(connection, ct);
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = NULL, server_major = {binding.ServerMajor + storedMajorOffset}", ct);

        var logger = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(connection, logger, ct));

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(id, row.InstallId);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);
    }

    /// <summary>A row that has its table OID but no major (the column was added to a store that already had rows) must still
    /// match the cluster id. With the cluster id unchanged it keeps its id and takes the current major, quietly. With a
    /// changed one nothing shows the server was upgraded, so the row gets a new id and one Warning.</summary>
    [Fact]
    public async Task Ensure_ARowWithATableOidAndNoMajor_MustMatchTheClusterId_ThenAdoptsTheMajor()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var binding = await RealBindingAsync(connection, ct);

        /* The cluster id matches: kept, and the major is filled in without a line in the log. */
        await ExecAsync(connection, $"UPDATE {Table} SET server_major = NULL", ct);
        var kept = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(connection, kept, ct));
        Assert.Empty(kept.Lines);
        Assert.Equal(binding.ServerMajor, (await ReadRowAsync(connection, ct)).ServerMajor);

        /* The cluster id changed and there is no stored major to show a rise: a new id. */
        await ExecAsync(connection, $"UPDATE {Table} SET server_major = NULL, system_identifier = system_identifier + 1", ct);
        var replaced = new CapturingTestLogger();
        var newId = await StoreInstallId.EnsureAsync(connection, replaced, ct);
        Assert.NotEqual(id, newId);
        var warning = Assert.Single(replaced.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains(id, warning, StringComparison.Ordinal);
        Assert.Contains(newId, warning, StringComparison.Ordinal);
        Assert.Contains("major version", warning, StringComparison.Ordinal);
        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(newId, row.InstallId);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);
    }

    /// <summary>The column the major is kept in: nullable integer, made by the rung together with the table OID. Running the
    /// rung again, as a store that has it already would, changes nothing and does not fail.</summary>
    [Fact]
    public async Task TheMajorColumn_IsANullableInteger_AndTheRungIsSafeToRunAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);

        async Task<(string Type, string Nullable)> ColumnAsync(string name)
        {
            await using var command = new NpgsqlCommand(
                "SELECT data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_install_id' AND column_name = $1",
                connection);
            command.Parameters.Add(new NpgsqlParameter { Value = name });
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), $"the install id table has no {name} column");
            return (reader.GetString(0), reader.GetString(1));
        }

        Assert.Equal(("integer", "YES"), await ColumnAsync("server_major"));
        Assert.Equal(("bigint", "YES"), await ColumnAsync("table_oid"));

        var id = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        var before = await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct);
        await ExecAsync(connection, TableOidRungSql, ct);
        Assert.Equal(("integer", "YES"), await ColumnAsync("server_major"));
        Assert.Equal(before, await ScalarAsync<string>(connection, $"SELECT xmin::text FROM {Table}", ct));
        Assert.Equal(id, (await ReadRowAsync(connection, ct)).InstallId);
    }

    /// <summary>The upgrade of a store that holds the id the previous rung's service made: the rung adds both columns and
    /// leaves the row alone, and the next start keeps the id and fills in the table OID and the major.</summary>
    [Fact]
    public async Task TheRung_OnAStoreWithTheRowThePreviousRungMade_KeepsTheId_AndTheNextStartFillsInBothValues()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        await ExecAsync(connection, $"DROP TABLE {Table}", ct);
        await ExecAsync(connection, RungSql, ct);
        var binding = await RealBindingAsync(connection, ct);
        await ExecAsync(connection,
            $"INSERT INTO {Table} (id, install_id, system_identifier, database_oid) VALUES (1, 'abcdef12', $1, $2)", ct,
            binding.SystemIdentifier, binding.DatabaseOid);

        await ExecAsync(connection, TableOidRungSql, ct);
        var migrated = await ReadRowAsync(connection, ct);
        Assert.Equal("abcdef12", migrated.InstallId);
        Assert.Null(migrated.TableOid);
        Assert.Null(migrated.ServerMajor);

        var logger = new CapturingTestLogger();
        Assert.Equal("abcdef12", await StoreInstallId.EnsureAsync(connection, logger, ct));
        Assert.Empty(logger.Lines);
        var row = await ReadRowAsync(connection, ct);
        var real = await RealBindingAsync(connection, ct);
        Assert.Equal("abcdef12", row.InstallId);
        Assert.Equal(real.TableOid, row.TableOid);
        Assert.Equal(real.ServerMajor, row.ServerMajor);
    }

    /// <summary>A changed table OID is a different store, and it is one with a NULL cluster id too: the OIDs decide, so a
    /// refused or unknown cluster id cannot hide it. The new row carries the real binding.</summary>
    [Fact]
    public async Task Ensure_AChangedTableOid_WithANullClusterId_MakesANewId_AndOneWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var oldId = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = NULL, table_oid = table_oid + 1", ct);

        var logger = new CapturingTestLogger();
        var newId = await StoreInstallId.EnsureAsync(connection, logger, ct);

        Assert.NotEqual(oldId, newId);
        var warning = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains(oldId, warning, StringComparison.Ordinal);
        Assert.Contains(newId, warning, StringComparison.Ordinal);
        Assert.Contains("table's OID", warning, StringComparison.Ordinal);
        var binding = await RealBindingAsync(connection, ct);
        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(newId, row.InstallId);
        Assert.Equal(binding.TableOid, row.TableOid);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
    }

    /// <summary>A row made before the table OID was kept has none (and no major). It is compared by the old rule once (the
    /// cluster id counts when both sides have one) and, when it is this store's, has the table OID and the major filled in
    /// and keeps its id.</summary>
    [Fact]
    public async Task Ensure_ARowWithNoTableOid_FollowsTheOldRuleOnce_ThenHasTheOidAndTheMajorFilledIn()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ResetAsync(ct);
        var binding = await RealBindingAsync(connection, ct);

        /* This store's row: kept, and filled in. */
        await ExecAsync(connection,
            $"INSERT INTO {Table} (id, install_id, system_identifier, database_oid) VALUES (1, 'abcdef12', $1, $2)", ct,
            binding.SystemIdentifier, binding.DatabaseOid);
        var kept = new CapturingTestLogger();
        Assert.Equal("abcdef12", await StoreInstallId.EnsureAsync(connection, kept, ct));
        Assert.Equal(0, kept.CountAtLevel(LogLevel.Warning));
        var adopted = await ReadRowAsync(connection, ct);
        Assert.Equal(binding.TableOid, adopted.TableOid);
        Assert.Equal(binding.ServerMajor, adopted.ServerMajor);

        /* The old rule still holds for such a row: another cluster id with no table OID is another store, once, even
           when the server's major is not what it was. */
        await ExecAsync(connection, $"UPDATE {Table} SET table_oid = NULL, server_major = NULL, system_identifier = system_identifier + 1", ct);
        var replaced = new CapturingTestLogger();
        var newId = await StoreInstallId.EnsureAsync(connection, replaced, ct);
        Assert.NotEqual("abcdef12", newId);
        var warning = Assert.Single(replaced.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains("cluster id", warning, StringComparison.Ordinal);
        var row = await ReadRowAsync(connection, ct);
        Assert.Equal(newId, row.InstallId);
        Assert.Equal(binding.TableOid, row.TableOid);
        Assert.Equal(binding.SystemIdentifier, row.SystemIdentifier);
        Assert.Equal(binding.ServerMajor, row.ServerMajor);

        /* After that the table OID and the major are there, and the new rule applies: a changed cluster id at the same major
           is another store, and at a lower stored major it is an upgrade. */
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = system_identifier + 1", ct);
        var copy = await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct);
        Assert.NotEqual(newId, copy);
        await ExecAsync(connection, $"UPDATE {Table} SET system_identifier = system_identifier + 1, server_major = server_major - 1", ct);
        Assert.Equal(copy, await StoreInstallId.EnsureAsync(connection, new CapturingTestLogger(), ct));
    }
}
