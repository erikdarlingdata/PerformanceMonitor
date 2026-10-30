/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and a role with a per-run name,
   and touches nothing on the shared one, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// <c>config.request_snapshot_now</c> (#5941) as the two roles that use it. The web route runs as <c>viewer</c> and the
/// MCP tool as <c>mcp</c>; neither holds a grant on <c>config_command</c>, because a free-form command row would reach
/// the service-credential pivot that <c>test_connect</c>'s <c>args_json</c> is. The grants are TAKEN FROM
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/> (the table and schema grants through
/// <see cref="ViewerGrantReplay"/>, the function's own EXECUTE grant from the batch's statement), so dropping the EXECUTE
/// grant or adding a table grant to the product's SQL fails these facts. The function body is the shared builder's, owned
/// by the owner as in a deployment. Collector NAMES are checked by the service (the route, the command plan and the worker;
/// <c>RunCollectorsNowTests</c>); the function checks only the shape, so a caller cannot smuggle another argument in.
/// </summary>
public sealed class RequestSnapshotNowRoleLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private const int ServerId = 41;

    [Theory]
    [InlineData("viewer")]
    [InlineData("mcp")]
    public async Task TheRole_QueuesAFixedTypeCommand_ThroughTheFunction(string managedRole)
    {
        await WithRoleAsync(managedRole, async (owner, asRole, ct) =>
        {
            await using var call = asRole.CreateCommand("SELECT config.request_snapshot_now($1, $2, $3)");
            call.Parameters.AddWithValue(ServerId);
            call.Parameters.AddWithValue("{\"collectors\":[\"wait_stats\",\"query_stats\"]}");
            call.Parameters.AddWithValue("web:tester");
            var id = Convert.ToInt64(await call.ExecuteScalarAsync(ct));

            await using var read = owner.CreateCommand(
                "SELECT command_type, status, target_server_id, requested_by, args_json::text FROM config.config_command WHERE command_id = $1");
            read.Parameters.AddWithValue(id);
            await using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("snapshot_now", reader.GetString(0));
            Assert.Equal("pending", reader.GetString(1));
            Assert.Equal(ServerId, reader.GetInt32(2));
            Assert.Equal("web:tester", reader.GetString(3));
            Assert.Contains("wait_stats", reader.GetString(4), StringComparison.Ordinal);
        });
    }

    [Theory]
    [InlineData("viewer")]
    [InlineData("mcp")]
    public async Task TheRole_WithNoListOfNames_QueuesTheRunAllForm(string managedRole)
    {
        await WithRoleAsync(managedRole, async (owner, asRole, ct) =>
        {
            await using var call = asRole.CreateCommand("SELECT config.request_snapshot_now($1, NULL, NULL)");
            call.Parameters.AddWithValue(ServerId);
            var id = Convert.ToInt64(await call.ExecuteScalarAsync(ct));

            await using var read = owner.CreateCommand(
                "SELECT command_type || '/' || COALESCE(args_json::text, 'null') || '/' || requested_by FROM config.config_command WHERE command_id = $1");
            read.Parameters.AddWithValue(id);
            Assert.Equal("snapshot_now/null/web", (string)(await read.ExecuteScalarAsync(ct))!);
        });
    }

    [Theory]
    [InlineData("viewer")]
    [InlineData("mcp")]
    public async Task TheRole_CannotInsertACommandRowDirectly(string managedRole)
    {
        await WithRoleAsync(managedRole, async (owner, asRole, ct) =>
        {
            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var insert = asRole.CreateCommand(
                    "INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at) " +
                    "VALUES ('x', 'test_connect', 41, '{}'::jsonb, 'pending', now() AT TIME ZONE 'UTC')");
                await insert.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    [Theory]
    [InlineData("viewer", 999, "{\"collectors\":[\"wait_stats\"]}", "DL404")]
    [InlineData("mcp", 999, null, "DL404")]
    [InlineData("viewer", ServerId, "{\"collectors\":[\"wait_stats\"],\"command_type\":\"test_connect\"}", "DL400")]
    [InlineData("mcp", ServerId, "{\"command_type\":\"test_connect\"}", "DL400")]
    [InlineData("viewer", ServerId, "[\"wait_stats\"]", "DL400")]
    [InlineData("mcp", ServerId, "{\"collectors\":\"wait_stats\"}", "DL400")]
    [InlineData("viewer", ServerId, "{\"collectors\":[\"wait_stats\",7]}", "DL400")]
    public async Task TheFunction_RefusesAnUnknownServer_AndAnythingButAListOfCollectorNames(
        string managedRole, int serverId, string? argsJson, string expectedState)
    {
        await WithRoleAsync(managedRole, async (owner, asRole, ct) =>
        {
            var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var call = asRole.CreateCommand("SELECT config.request_snapshot_now($1, $2, 'web')");
                call.Parameters.AddWithValue(serverId);
                call.Parameters.AddWithValue(argsJson is null ? DBNull.Value : argsJson);
                await call.ExecuteScalarAsync(ct);
            });
            Assert.Equal(expectedState, refused.SqlState);

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    [Theory]
    [InlineData("viewer")]
    [InlineData("mcp")]
    public async Task TheFunction_RefusesASecondRequest_WhileOneIsPending_AndWithin60SecondsOfTheLast_NamingWhenTheNextIsAllowed(string managedRole)
    {
        await WithRoleAsync(managedRole, async (owner, asRole, ct) =>
        {
            async Task<long> RequestAsync()
            {
                await using var call = asRole.CreateCommand("SELECT config.request_snapshot_now($1, NULL, 'web')");
                call.Parameters.AddWithValue(ServerId);
                return Convert.ToInt64(await call.ExecuteScalarAsync(ct));
            }

            var first = await RequestAsync();

            /* One is pending: refused. */
            var pending = await Assert.ThrowsAsync<PostgresException>(RequestAsync);
            Assert.Equal("DL409", pending.SqlState);

            /* It has finished but is less than 60 seconds old: refused, with the time the next is allowed. */
            await ExecAsync(owner, $"UPDATE config.config_command SET status = 'succeeded' WHERE command_id = {first}", ct);
            var tooSoon = await Assert.ThrowsAsync<PostgresException>(RequestAsync);
            Assert.Equal("DL429", tooSoon.SqlState);
            Assert.True(DateTime.TryParse(tooSoon.Hint, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out var nextAllowed));
            Assert.InRange(nextAllowed, DateTime.UtcNow, DateTime.UtcNow.AddSeconds(61));

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(1L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));

            /* A minute on, it is queued. */
            await ExecAsync(owner, $"UPDATE config.config_command SET created_at = created_at - interval '61 seconds' WHERE command_id = {first}", ct);
            var second = await RequestAsync();
            Assert.NotEqual(first, second);
        });
    }

    /// <summary>
    /// A request that arrives while another request for the same server holds the per-server lock is refused with DL409 at
    /// once, not queued behind it (provisioning never blocks: the function takes the lock with the try form).
    /// </summary>
    [Theory]
    [InlineData("viewer")]
    public async Task TheFunction_RefusesAtOnce_WhileAnotherRequestForTheSameServerHoldsTheLock(string managedRole)
    {
        await WithRoleAsync(managedRole, async (owner, asRole, ct) =>
        {
            await using var holder = await owner.OpenConnectionAsync(ct);
            await using var transaction = await holder.BeginTransactionAsync(ct);
            await using (var hold = new NpgsqlCommand($"SELECT pg_advisory_xact_lock(5941, {ServerId})", holder, transaction))
            {
                await hold.ExecuteScalarAsync(ct);
            }

            await using var call = asRole.CreateCommand("SELECT config.request_snapshot_now($1, NULL, 'web')");
            call.Parameters.AddWithValue(ServerId);
            call.CommandTimeout = 5; // a call that waited on the lock would time out here instead of refusing
            var refused = await Assert.ThrowsAsync<PostgresException>(async () => await call.ExecuteScalarAsync(ct));
            Assert.Equal("DL409", refused.SqlState);

            await transaction.RollbackAsync(ct);
            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    /// <summary>A transaction that keeps one snapshot is refused with DL400 and queues nothing; the same call at READ COMMITTED queues.</summary>
    [Theory]
    [InlineData("viewer")]
    [InlineData("mcp")]
    public async Task TheFunction_RefusesARepeatableReadTransaction_WithDL400(string managedRole)
    {
        await WithRoleAsync(managedRole, async (owner, asRole, ct) =>
        {
            await using (var connection = await asRole.OpenConnectionAsync(ct))
            {
                await using (var begin = new NpgsqlCommand("BEGIN ISOLATION LEVEL REPEATABLE READ", connection))
                {
                    await begin.ExecuteNonQueryAsync(ct);
                }

                await using (var snapshot = new NpgsqlCommand("SELECT 1", connection))
                {
                    await snapshot.ExecuteScalarAsync(ct);
                }

                await using var call = new NpgsqlCommand($"SELECT config.request_snapshot_now({ServerId}, NULL, 'web')", connection);
                var refused = await Assert.ThrowsAsync<PostgresException>(async () => await call.ExecuteScalarAsync(ct));
                Assert.Equal("DL400", refused.SqlState);
                Assert.Contains("READ COMMITTED", refused.MessageText, StringComparison.Ordinal);
            }

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));

            await using var allowed = asRole.CreateCommand("SELECT config.request_snapshot_now($1, NULL, 'web')");
            allowed.Parameters.AddWithValue(ServerId);
            Assert.True(Convert.ToInt64(await allowed.ExecuteScalarAsync(ct)) > 0);
        });
    }

    private static async Task WithRoleAsync(
        string managedRole, Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the request_snapshot_now role live tests (they mint their own scratch database and role).");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "snap_req_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner,
                $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerId}, 'snap-target', 'snap-target', TRUE)", ct);

            /* The function is created by the owner, as the managed batch does; the batch's own EXECUTE grant, retargeted. */
            await ExecAsync(owner, DarlingManagedRoles.BuildRequestSnapshotNowFunctionSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, managedRole))
            {
                await ExecAsync(owner, statement, ct);
            }

            var execute = ViewerGrantReplay.BatchStatements().Single(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.request_snapshot_now", StringComparison.Ordinal));
            var at = execute.LastIndexOf(" TO ", StringComparison.Ordinal);
            Assert.Contains(managedRole, execute[(at + 4)..].Split(',').Select(t => t.Trim()));
            await ExecAsync(owner, execute[..at] + " TO " + roleName, ct);

            await using var asRole = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString);

            await body(owner, asRole, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
