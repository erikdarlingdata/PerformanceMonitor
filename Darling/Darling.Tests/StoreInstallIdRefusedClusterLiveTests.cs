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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact runs in the one scratch database the fixture mints through ScratchPostgres, empties the
   install id table first, and puts the cluster-id grant back to its default, so the class is deliberately NOT
   [Collection("live-postgres")]. The REVOKE the facts make is on a function's ACL, which lives in the database's own
   catalog, so it never reaches another database on the cluster. */

/// <summary>The scratch store behind <see cref="StoreInstallIdRefusedClusterLiveTests"/>: a database of its own,
/// migrated once through the real ladder, and a login that has no more than the store's runtime role has on the
/// install id table (it is not a superuser, so a REVOKE on <c>pg_control_system()</c> reaches it). A second
/// connection string puts a function that always fails ahead of the real one on the login's search path, to make a
/// failure that is not a refusal. The login is dropped with the database.</summary>
public sealed class RefusedClusterScratchStore : IAsyncLifetime
{
    private const string LoginPassword = "install-id-login-pw";

    private readonly string _loginName = "install_id_login_" + Guid.NewGuid().ToString("N")[..12];
    private string? _baseConnectionString;
    private ScratchPostgres? _scratch;

    /// <summary>The scratch store as its owner, or null when <c>DARLING_TEST_PG</c> is unset (the normal ungated
    /// run), in which case every fact skips.</summary>
    public string? OwnerConnectionString => _scratch?.ConnectionString;

    /// <summary>The same store as the login without superuser rights.</summary>
    public string? LoginConnectionString => _scratch is null ? null : LoginBuilder().ConnectionString;

    /// <summary>The login's connection with a failing <c>pg_control_system()</c> found before the real one.</summary>
    public string? FailingFunctionConnectionString
    {
        get
        {
            if (_scratch is null)
            {
                return null;
            }

            var builder = LoginBuilder();
            builder.SearchPath = "shadow,pg_catalog";
            return builder.ConnectionString;
        }
    }

    private NpgsqlConnectionStringBuilder LoginBuilder() =>
        new(_scratch!.ConnectionString) { Username = _loginName, Password = LoginPassword, Pooling = false };

    public async ValueTask InitializeAsync()
    {
        _baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(_baseConnectionString))
        {
            return;
        }

        _scratch = await ScratchPostgres.CreateAsync(_baseConnectionString, CancellationToken.None);
        await using var connection = new NpgsqlConnection(_scratch.ConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        await PgMigrations.MigrateAsync(connection, CancellationToken.None);

        /* What the runtime role has on this table: the schema's USAGE and SELECT, INSERT, UPDATE and DELETE (the
           blanket grants provisioning makes on the config schema). Nothing on pg_control_system() but the
           default every login has until a server revokes it. */
        var sql = $@"
CREATE ROLE {_loginName} LOGIN NOSUPERUSER PASSWORD '{LoginPassword}';
GRANT USAGE ON SCHEMA config TO {_loginName};
GRANT SELECT, INSERT, UPDATE, DELETE ON config.config_install_id TO {_loginName};
CREATE SCHEMA shadow;
CREATE FUNCTION shadow.pg_control_system() RETURNS TABLE(system_identifier bigint) LANGUAGE plpgsql
AS $body$ BEGIN RAISE EXCEPTION 'the cluster id read failed' USING ERRCODE = '55000'; END $body$;
GRANT USAGE ON SCHEMA shadow TO {_loginName};
GRANT EXECUTE ON FUNCTION shadow.pg_control_system() TO {_loginName};";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        if (_scratch is null)
        {
            return;
        }

        await _scratch.DisposeAsync();

        try
        {
            await using var admin = new NpgsqlConnection(_baseConnectionString);
            await admin.OpenAsync(CancellationToken.None);
            await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {_loginName}", admin);
            await drop.ExecuteNonQueryAsync(CancellationToken.None);
        }
        catch
        {
            /* Best-effort: a leaked login on a throwaway test cluster is harmless, and failing a passing test in
               its cleanup would invert the signal. */
        }
    }
}

/// <summary>
/// The install id store on a server that refuses a login <c>pg_control_system()</c> (#4961). A managed or hardened
/// server may revoke it, and the service has to start there anyway: the id is then bound to the database alone, the
/// stored cluster id is left empty, and a later start that can read the cluster id (or can no longer) keeps the same id
/// rather than making a new one on its own. Only a refusal falls back; any other failure of that read still fails the
/// start.
/// </summary>
public sealed class StoreInstallIdRefusedClusterLiveTests : IClassFixture<RefusedClusterScratchStore>
{
    private const string Table = "config.config_install_id";

    private readonly RefusedClusterScratchStore _store;

    public StoreInstallIdRefusedClusterLiveTests(RefusedClusterScratchStore store)
    {
        _store = store;
    }

    private string OwnerConnectionString
    {
        get
        {
            Assert.SkipWhen(string.IsNullOrEmpty(_store.OwnerConnectionString), "Set DARLING_TEST_PG to run the live install id fallback pins.");
            return _store.OwnerConnectionString!;
        }
    }

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The owner's connection to a store with no install id row and the cluster id readable, the state a
    /// fact then changes to the one it needs.</summary>
    private async Task<NpgsqlConnection> OwnerAsync(CancellationToken ct)
    {
        var owner = await OpenAsync(OwnerConnectionString, ct);
        await ExecAsync(owner, $"DELETE FROM {Table}", ct);
        await AllowClusterIdAsync(owner, ct);
        return owner;
    }

    private static Task RefuseClusterIdAsync(NpgsqlConnection owner, CancellationToken ct) =>
        ExecAsync(owner, "REVOKE EXECUTE ON FUNCTION pg_control_system() FROM PUBLIC", ct);

    private static Task AllowClusterIdAsync(NpgsqlConnection owner, CancellationToken ct) =>
        ExecAsync(owner, "GRANT EXECUTE ON FUNCTION pg_control_system() TO PUBLIC", ct);

    private static async Task<(string InstallId, long? SystemIdentifier, long DatabaseOid)> ReadRowAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT install_id, system_identifier, database_oid FROM {Table} WHERE id = 1", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the install id table has no row");
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetInt64(2));
    }

    private static Task<long> DatabaseOidAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<long>(connection, "SELECT oid::bigint FROM pg_database WHERE datname = current_database()", ct);

    private static Task<long> ClusterIdAsync(NpgsqlConnection owner, CancellationToken ct) =>
        ScalarAsync<long>(owner, "SELECT system_identifier FROM pg_control_system()", ct);

    private static Task<int> ServerMajorAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<int>(connection, "SELECT current_setting('server_version_num')::int / 10000", ct);

    private static Task<int> StoredMajorAsync(NpgsqlConnection owner, CancellationToken ct) =>
        ScalarAsync<int>(owner, $"SELECT server_major FROM {Table}", ct);

    /// <summary>The row's version: a write, even one that sets the values it already has, gives the row a new one.</summary>
    private static Task<string> RowVersionAsync(NpgsqlConnection owner, CancellationToken ct) =>
        ScalarAsync<string>(owner, $"SELECT xmin::text FROM {Table}", ct);

    /// <summary>Makes the stored row the one the cluster before a major upgrade left: another cluster id and a major below
    /// this server's, with both OIDs as they are. This server then reads as the same store after an upgrade.</summary>
    private static async Task<(long Cluster, int Major)> StoreTheRowOfTheClusterBeforeAnUpgradeAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var cluster = await ClusterIdAsync(owner, ct) + 1;
        var major = await ServerMajorAsync(owner, ct) - 1;
        await ExecAsync(owner, $"UPDATE {Table} SET system_identifier = {cluster}, server_major = {major}", ct);
        return (cluster, major);
    }

    private static bool IsTheDatabaseAloneLine(string line) =>
        line.StartsWith("Information:", StringComparison.Ordinal) && line.Contains("database alone", StringComparison.Ordinal);

    [Fact]
    public async Task Ensure_WhenTheLoginMayNotReadTheClusterId_StoresARowBoundToTheDatabaseAlone_AndLogsOneInformation()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await RefuseClusterIdAsync(owner, ct);
        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);

        var logger = new CapturingTestLogger();
        var id = await StoreInstallId.EnsureAsync(login, logger, ct);

        Assert.True(InstallId.IsValid(id), $"'{id}' is not eight lowercase hex digits");
        Assert.Equal(1L, await ScalarAsync<long>(owner, $"SELECT count(*) FROM {Table}", ct));
        var row = await ReadRowAsync(owner, ct);
        Assert.Equal(id, row.InstallId);
        Assert.Null(row.SystemIdentifier);
        Assert.Equal(await DatabaseOidAsync(owner, ct), row.DatabaseOid);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Single(logger.Lines, IsTheDatabaseAloneLine);

        /* The read path (the CLI's and the Viewer's) sees that row too. */
        Assert.Equal(id, await StoreInstallId.TryReadAsync(login, ct));
    }

    [Fact]
    public async Task TryRead_OfARowStoredWithoutAClusterId_ReadsTheId_AndWritesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await RefuseClusterIdAsync(owner, ct);
        var oid = await DatabaseOidAsync(owner, ct);
        await ExecAsync(owner,
            $"INSERT INTO {Table} (id, install_id, system_identifier, database_oid) VALUES (1, 'abcdef12', NULL, {oid})", ct);
        var before = await ScalarAsync<string>(owner, $"SELECT xmin::text FROM {Table}", ct);

        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);
        Assert.Equal("abcdef12", await StoreInstallId.TryReadAsync(login, ct));
        await using (var dataSource = NpgsqlDataSource.Create(_store.LoginConnectionString!))
        {
            Assert.Equal("abcdef12", await StoreInstallId.TryReadAsync(dataSource, ct));
        }

        /* A read leaves the row exactly as it found it (a write would have given it a new xmin). */
        Assert.Equal(before, await ScalarAsync<string>(owner, $"SELECT xmin::text FROM {Table}", ct));
    }

    [Fact]
    public async Task Ensure_AfterTheClusterIdWasRefused_ThenAllowed_KeepsTheSameId_AndLogsNoWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);

        await RefuseClusterIdAsync(owner, ct);
        var first = await StoreInstallId.EnsureAsync(login, new CapturingTestLogger(), ct);

        await AllowClusterIdAsync(owner, ct);
        var logger = new CapturingTestLogger();
        var second = await StoreInstallId.EnsureAsync(login, logger, ct);

        Assert.Equal(first, second);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1L, await ScalarAsync<long>(owner, $"SELECT count(*) FROM {Table}", ct));
        Assert.Equal(first, (await ReadRowAsync(owner, ct)).InstallId);
    }

    [Fact]
    public async Task Ensure_AfterTheClusterIdWasAllowed_ThenRefused_KeepsTheSameId_AndLogsNoWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);

        var first = await StoreInstallId.EnsureAsync(login, new CapturingTestLogger(), ct);
        var stored = await ReadRowAsync(owner, ct);
        Assert.Equal(await ClusterIdAsync(owner, ct), stored.SystemIdentifier);

        await RefuseClusterIdAsync(owner, ct);
        var logger = new CapturingTestLogger();
        var second = await StoreInstallId.EnsureAsync(login, logger, ct);

        Assert.Equal(first, second);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1L, await ScalarAsync<long>(owner, $"SELECT count(*) FROM {Table}", ct));
        Assert.Equal(stored, await ReadRowAsync(owner, ct));
    }

    /// <summary>The server was upgraded to a new major version (the row says cluster A and major M, the server now has
    /// cluster B and major M+1, and both OIDs are as they were), and the first start after it can't read the cluster id.
    /// The row keeps the id, and it keeps the major that goes with the cluster id it has: the new major written beside the
    /// old cluster id would make a later start that can read the new cluster id see a changed cluster id at an equal
    /// major, which is what a copy of the row looks like. The start writes nothing, so the row is as it found it.</summary>
    [Fact]
    public async Task Ensure_AfterAMajorUpgrade_AStartThatCannotReadTheClusterId_KeepsTheId_AndLeavesTheRowAsItWas()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);
        var id = await StoreInstallId.EnsureAsync(login, new CapturingTestLogger(), ct);
        var (oldCluster, oldMajor) = await StoreTheRowOfTheClusterBeforeAnUpgradeAsync(owner, ct);
        await RefuseClusterIdAsync(owner, ct);
        var before = await RowVersionAsync(owner, ct);

        var logger = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(login, logger, ct));

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        var row = await ReadRowAsync(owner, ct);
        Assert.Equal(id, row.InstallId);
        Assert.Equal(oldCluster, row.SystemIdentifier);
        Assert.Equal(oldMajor, await StoredMajorAsync(owner, ct));
        Assert.Equal(before, await RowVersionAsync(owner, ct));
    }

    /// <summary>The same upgrade, and a later start can read the cluster id. The id stays, one Information line says the
    /// cluster id changed and the major rose, and the row is rebound to the new cluster id and major: the start that could
    /// not read the cluster id did not make the later one look like a copy.</summary>
    [Fact]
    public async Task Ensure_AfterAMajorUpgrade_AStartThatCannotReadTheClusterId_ThenOneThatCan_KeepsTheId_AndRebindsTheRow()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);
        var id = await StoreInstallId.EnsureAsync(login, new CapturingTestLogger(), ct);
        var (oldCluster, oldMajor) = await StoreTheRowOfTheClusterBeforeAnUpgradeAsync(owner, ct);
        var newCluster = await ClusterIdAsync(owner, ct);
        var newMajor = await ServerMajorAsync(owner, ct);

        await RefuseClusterIdAsync(owner, ct);
        var refused = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(login, refused, ct));

        await AllowClusterIdAsync(owner, ct);
        var allowed = new CapturingTestLogger();
        var later = await StoreInstallId.EnsureAsync(login, allowed, ct);

        Assert.Equal(id, later);
        Assert.Equal(0, refused.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, allowed.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(allowed.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal));
        Assert.Contains(oldCluster.ToString(CultureInfo.InvariantCulture), line, StringComparison.Ordinal);
        Assert.Contains(newCluster.ToString(CultureInfo.InvariantCulture), line, StringComparison.Ordinal);
        Assert.Contains($"from {oldMajor} to {newMajor}", line, StringComparison.Ordinal);
        Assert.Contains("id stays", line, StringComparison.Ordinal);
        var row = await ReadRowAsync(owner, ct);
        Assert.Equal(id, row.InstallId);
        Assert.Equal(newCluster, row.SystemIdentifier);
        Assert.Equal(newMajor, await StoredMajorAsync(owner, ct));
        Assert.Equal(1L, await ScalarAsync<long>(owner, $"SELECT count(*) FROM {Table}", ct));
    }

    /// <summary>A row with no cluster id has no cluster id for a major to stay beside, so a start that can't read the cluster
    /// id still writes the current major into it, whether the stored one was lower or higher. The next such start finds
    /// nothing left to write.</summary>
    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public async Task Ensure_ARowWithNoClusterId_AStartThatCannotReadTheClusterId_StillWritesTheMajor(int storedMajorOffset)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);
        await RefuseClusterIdAsync(owner, ct);
        var id = await StoreInstallId.EnsureAsync(login, new CapturingTestLogger(), ct);
        var major = await ServerMajorAsync(owner, ct);
        await ExecAsync(owner, $"UPDATE {Table} SET server_major = {major + storedMajorOffset}", ct);

        var logger = new CapturingTestLogger();
        Assert.Equal(id, await StoreInstallId.EnsureAsync(login, logger, ct));

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        var row = await ReadRowAsync(owner, ct);
        Assert.Equal(id, row.InstallId);
        Assert.Null(row.SystemIdentifier);
        Assert.Equal(major, await StoredMajorAsync(owner, ct));

        var written = await RowVersionAsync(owner, ct);
        Assert.Equal(id, await StoreInstallId.EnsureAsync(login, new CapturingTestLogger(), ct));
        Assert.Equal(written, await RowVersionAsync(owner, ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Ensure_WhenTheClusterIdIsRefused_ADifferentDatabaseOid_MakesANewId_AndLogsOneWarningNamingBothIds(bool firstStartReadTheClusterId)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await using var login = await OpenAsync(_store.LoginConnectionString!, ct);

        if (!firstStartReadTheClusterId)
        {
            await RefuseClusterIdAsync(owner, ct);
        }

        var oldId = await StoreInstallId.EnsureAsync(login, new CapturingTestLogger(), ct);
        await RefuseClusterIdAsync(owner, ct);
        await ExecAsync(owner, $"UPDATE {Table} SET database_oid = database_oid + 1", ct);

        var logger = new CapturingTestLogger();
        var newId = await StoreInstallId.EnsureAsync(login, logger, ct);

        Assert.True(InstallId.IsValid(newId));
        Assert.NotEqual(oldId, newId);
        var warning = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains(oldId, warning, StringComparison.Ordinal);
        Assert.Contains(newId, warning, StringComparison.Ordinal);

        var row = await ReadRowAsync(owner, ct);
        Assert.Equal(newId, row.InstallId);
        Assert.Null(row.SystemIdentifier);
        Assert.Equal(await DatabaseOidAsync(owner, ct), row.DatabaseOid);

        /* The next start is an ordinary one: same id, no second warning. */
        var after = new CapturingTestLogger();
        Assert.Equal(newId, await StoreInstallId.EnsureAsync(login, after, ct));
        Assert.Equal(0, after.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>Only a refusal (permission denied) falls back. A read that fails for any other reason is a store that
    /// is not answering as it should, so the start fails and its retry takes over, as it does for a failed migration.
    /// Guards against a fallback written as a catch of every database error.</summary>
    [Fact]
    public async Task Ensure_AFailureOfTheClusterIdReadOtherThanARefusal_IsNotSwallowed_AndNothingIsStored()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await OwnerAsync(ct);
        await using var login = await OpenAsync(_store.FailingFunctionConnectionString!, ct);

        var logger = new CapturingTestLogger();
        var ex = await Assert.ThrowsAsync<PostgresException>(() => StoreInstallId.EnsureAsync(login, logger, ct));

        Assert.Equal("55000", ex.SqlState);
        Assert.Equal(0L, await ScalarAsync<long>(owner, $"SELECT count(*) FROM {Table}", ct));
        Assert.DoesNotContain(logger.Lines, IsTheDatabaseAloneLine);
    }
}
