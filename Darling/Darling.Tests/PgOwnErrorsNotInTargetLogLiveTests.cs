/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6888: Darling must not put ERROR lines into a monitored PostgreSQL server's own log. Three sources did: an extension
/// collector sending its query when the extension was not created (42883 / 42P01), the connect-time Aurora probe
/// calling <c>aurora_version()</c> on a server that is not Aurora, and the connect-time identity probe calling
/// <c>pg_control_system()</c> as a role the server refuses it.
/// <para>The throwaway cluster's server log is read after each run (its file sits beside the data directory, as the
/// start helpers write it). A bare call that is known to fail is run first as the control, so a log that cannot be read
/// fails the test instead of passing it with nothing in it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgOwnErrorsNotInTargetLogLiveTests
{
    private const string Database = "pm6888_target";
    private const string Monitor = "pm6888_monitor";

    [Fact]
    public async Task MissingExtension_IsRecordedWithoutSendingTheQuery_AndLeavesNoErrorInTheServerLog()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString), "Set DARLING_TEST_PG to run the server-log live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var target = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };
        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database}", ct);
            var log = await ServerLog.OpenAsync(admin.ConnectionString, ct);

            await using var connection = new NpgsqlConnection(target.ConnectionString);
            await connection.OpenAsync(ct);

            /* The control: the query the collector used to send, sent. It fails, and the server writes the failure down. */
            var mark = log.Mark();
            await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var bare = new NpgsqlCommand("SELECT * FROM pg_catalog.pg_qualstats()", connection);
                await bare.ExecuteNonQueryAsync(ct);
            });
            Assert.Single(log.ErrorLinesSince(mark, "pg_qualstats"));

            /* The collector's run: pg_qualstats is not created here. */
            mark = log.Mark();
            var context = NewContext(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major });
            var fault = await Assert.ThrowsAsync<PostgresException>(async () =>
                await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(context, PgPredicateStatsCollector.Instance, connection, null, ct));
            Assert.Equal("42P01", fault.SqlState);
            Assert.Empty(log.ErrorLinesSince(mark, "pg_qualstats"));

            /* The worker classifies that fault with the same method it classifies the query's own failure with. */
            var (status, explanation) = DarlingWorker.PostgresFaultOutcome(fault, PgPredicateStatsCollector.Instance.Name, Database);
            Assert.Equal(CollectorRuntimePrecondition.ExtensionMissingStatus, status);
            Assert.Contains("the pg_qualstats extension this collector reads is not installed on this target", explanation, StringComparison.Ordinal);
            Assert.Contains($"CREATE EXTENSION pg_qualstats in database '{Database}'", explanation, StringComparison.Ordinal);
            Assert.Contains("shared_preload_libraries", explanation, StringComparison.Ordinal);

            /* pg_buffercache: a CREATE EXTENSION-only dependency, a different advice. */
            var buffers = await Assert.ThrowsAsync<PostgresException>(async () =>
                await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(context, PgBufferUsageCollector.Instance, connection, null, ct));
            var (bufferStatus, bufferExplanation) = DarlingWorker.PostgresFaultOutcome(buffers, PgBufferUsageCollector.Instance.Name, Database);
            Assert.Equal(CollectorRuntimePrecondition.ExtensionMissingStatus, bufferStatus);
            Assert.Contains($"CREATE EXTENSION pg_buffercache in database '{Database}'", bufferExplanation, StringComparison.Ordinal);
            Assert.DoesNotContain("shared_preload_libraries", bufferExplanation, StringComparison.Ordinal);
            Assert.Empty(log.ErrorLinesSince(mark, "pg_buffercache"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ACollectorThatRunsWithoutTheExtension_KeepsRunning_AndACreatedExtensionIsNotAFault()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString), "Set DARLING_TEST_PG to run the server-log live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var target = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };
        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database}", ct);
            var log = await ServerLog.OpenAsync(admin.ConnectionString, ct);

            await using var connection = new NpgsqlConnection(target.ConnectionString);
            await connection.OpenAsync(ct);
            var major = connection.PostgreSqlVersion.Major;

            /* The control: the query the statements collector used to send on a database without the extension, sent. */
            var mark = log.Mark();
            await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var bare = new NpgsqlCommand("SELECT * FROM pg_stat_statements", connection);
                await bare.ExecuteNonQueryAsync(ct);
            });
            Assert.Single(log.ErrorLinesSince(mark, "pg_stat_statements"));
            mark = log.Mark();

            /* Aurora reads aurora_stat_statements(), which needs no extension: pg_stat_statements is not created and the
               collector still runs. A stock target needs it. */
            var aurora = NewContext(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major, IsAurora = true });
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(aurora, PgStatementStatsCollector.Instance, connection, null, ct));
            Assert.False(aurora.PgExtensionSchemas!.ContainsKey("pg_stat_statements"));
            var stock = NewContext(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major });
            var statements = await Assert.ThrowsAsync<PostgresException>(async () =>
                await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(stock, PgStatementStatsCollector.Instance, connection, null, ct));
            Assert.Equal("42P01", statements.SqlState);

            /* pg_wait_sampling: the sampler arm runs without it. The extension arm, picked at connect, does not. */
            var sampler = NewContext(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major });
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(sampler, PgWaitSamplingCollector.Instance, connection, null, ct));
            var dropped = NewContext(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major, HasPgWaitSamplingExtension = true });
            var waits = await Assert.ThrowsAsync<PostgresException>(async () =>
                await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(dropped, PgWaitSamplingCollector.Instance, connection, null, ct));
            Assert.Equal("42P01", waits.SqlState);

            /* Neither arm put anything into the server's log: the host raised both faults itself. */
            Assert.Empty(log.ErrorLinesSince(mark, "pg_stat_statements"));
            Assert.Empty(log.ErrorLinesSince(mark, "pg_wait_sampling"));

            /* Created: the read succeeds and names the extension's schema, as before. */
            await ExecAsync(target.ConnectionString, "CREATE EXTENSION pg_stat_statements SCHEMA public", ct);
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(stock, PgStatementStatsCollector.Instance, connection, null, ct));
            Assert.Equal("public", stock.PgExtensionSchemas!["pg_stat_statements"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AuroraProbe_ReadsNotAuroraOnCommunityPostgres_WithNoErrorInTheServerLog_AndStillReadsAurora()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString), "Set DARLING_TEST_PG to run the server-log live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var target = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };
        var monitor = new NpgsqlConnectionStringBuilder(target.ConnectionString) { Username = Monitor, Password = null, Pooling = false };
        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Monitor} LOGIN; GRANT pg_monitor TO {Monitor}", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database}", ct);
            var log = await ServerLog.OpenAsync(admin.ConnectionString, ct);

            /* A monitoring login that owns nothing and is no superuser, as the service signs in. */
            await using var connection = new NpgsqlConnection(monitor.ConnectionString);
            await connection.OpenAsync(ct);

            /* The control: the bare call the probe used to make. */
            var mark = log.Mark();
            await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var bare = new NpgsqlCommand("SELECT pg_catalog.aurora_version()", connection);
                await bare.ExecuteScalarAsync(ct);
            });
            Assert.Single(log.ErrorLinesSince(mark, "aurora_version"));

            mark = log.Mark();
            Assert.False(await DarlingServerConnector.ProbeAuroraAsync(connection, ct));
            Assert.Empty(log.ErrorLinesSince(mark, "aurora_version"));

            /* A stand-in aurora_version(), in pg_catalog where the probe calls it. */
            await ExecAsync(target.ConnectionString, "CREATE FUNCTION pg_catalog.aurora_version() RETURNS text LANGUAGE sql AS $$SELECT '17.7.2'::text$$", ct);
            mark = log.Mark();
            Assert.True(await DarlingServerConnector.ProbeAuroraAsync(connection, ct));
            Assert.Empty(log.ErrorLinesSince(mark, "aurora_version"));

            /* The probe leaves nothing behind on the connection: a second run reads the same, and a stand-in that
               returns NULL is not Aurora. */
            Assert.True(await DarlingServerConnector.ProbeAuroraAsync(connection, ct));
            await ExecAsync(target.ConnectionString, "CREATE OR REPLACE FUNCTION pg_catalog.aurora_version() RETURNS text LANGUAGE sql AS $$SELECT NULL::text$$", ct);
            Assert.False(await DarlingServerConnector.ProbeAuroraAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task IdentityProbe_OnARoleRefusedPgControlSystem_ReadsNoIdentity_WithNoErrorInTheServerLog_AndStillReadsItWhenGranted()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString), "Set DARLING_TEST_PG to run the server-log live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var target = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };
        var monitor = new NpgsqlConnectionStringBuilder(target.ConnectionString) { Username = Monitor, Password = null, Pooling = false };
        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Monitor} LOGIN; GRANT pg_monitor TO {Monitor}", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database}", ct);
            var log = await ServerLog.OpenAsync(admin.ConnectionString, ct);

            /* A hardened or managed server that revokes the function. The grant lives in the target database's own catalog,
               which the database drop takes away. */
            await ExecAsync(target.ConnectionString, "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC", ct);

            await using var connection = new NpgsqlConnection(monitor.ConnectionString);
            await connection.OpenAsync(ct);

            /* The control: the bare call the probe used to make. The server refuses it and writes the refusal down. */
            var mark = log.Mark();
            var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var bare = new NpgsqlCommand("SELECT * FROM pg_catalog.pg_control_system()", connection);
                await bare.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, refused.SqlState);
            Assert.Single(log.ErrorLinesSince(mark, "pg_control_system"));

            /* The probe: the same answer as before (no identity, the connection still usable), and nothing in the log. */
            mark = log.Mark();
            Assert.Null(await DarlingServerConnector.ProbePostgresIdentityAsync(connection, null, ct));
            Assert.Empty(log.ErrorLinesSince(mark, "pg_control_system"));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, "SELECT 1", ct), System.Globalization.CultureInfo.InvariantCulture));

            /* Granted again: the identity is read, and the log is still clean. */
            await ExecAsync(target.ConnectionString, "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO PUBLIC", ct);
            mark = log.Mark();
            Assert.NotNull(await DarlingServerConnector.ProbePostgresIdentityAsync(connection, null, ct));
            Assert.Empty(log.ErrorLinesSince(mark, "pg_control_system"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static CollectorContext NewContext(CollectorTargetInfo target) => new()
    {
        ServerId = 6888,
        ServerName = "pm6888-target",
        CollectionTime = LiveClock.Now(),
        Deltas = new CollectorDeltaCalculator(),
        Target = target,
    };

    private static async Task ResetAsync(string adminConnectionString, CancellationToken ct)
    {
        NpgsqlConnection.ClearAllPools();
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        await DropAsync(adminConnectionString, connection, ct);
    }

    private static async Task DropAsync(string adminConnectionString, NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var exists = new NpgsqlCommand($"SELECT pg_catalog.count(*) FROM pg_catalog.pg_database WHERE datname = '{Database}'", connection))
        {
            if (Convert.ToInt64(await exists.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) > 0)
            {
                await ScratchPostgres.QuiesceTimescaleJobsAsync(adminConnectionString, Database, ct);
            }
        }

        await using (var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS {Database} WITH (FORCE)", connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        await using var role = new NpgsqlCommand($"DROP ROLE IF EXISTS {Monitor}", connection);
        await role.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The throwaway cluster's stderr log: <c>pg.log</c> beside the data directory. Skips the test when it is not there.</summary>
    internal sealed class ServerLog
    {
        private readonly string _path;

        private ServerLog(string path) => _path = path;

        public static async Task<ServerLog> OpenAsync(string adminConnectionString, CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(adminConnectionString);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand("SELECT pg_catalog.current_setting('data_directory')", connection);
            var dataDirectory = (string)(await command.ExecuteScalarAsync(ct))!;
            var path = Path.Combine(Path.GetDirectoryName(dataDirectory.TrimEnd('/', '\\')) ?? string.Empty, "pg.log");
            Assert.SkipUnless(File.Exists(path), $"No server log at {path}.");
            return new ServerLog(path);
        }

        public long Mark() => new FileInfo(_path).Length;

        /// <summary>ERROR lines written after <paramref name="mark"/>, optionally only those that mention <paramref name="containing"/>.</summary>
        public IReadOnlyList<string> ErrorLinesSince(long mark, string? containing = null)
        {
            using var stream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            stream.Seek(mark, SeekOrigin.Begin);
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd()
                .Split('\n')
                .Where(line => line.Contains(" ERROR:  ", StringComparison.Ordinal)
                    && (containing is null || line.Contains(containing, StringComparison.Ordinal)))
                .ToList();
        }
    }
}
