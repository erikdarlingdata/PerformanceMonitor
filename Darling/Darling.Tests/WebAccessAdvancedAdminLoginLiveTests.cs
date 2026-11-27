/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: an isolated database, and the cluster-wide admin role, which is why the class is in the pg-cluster-roles collection. */

/// <summary>
/// #9662: a provisioned <c>admin</c> login flips the advanced web access switch through the one argument setter, which journals that login
/// as the actor. Only the store owner can call the three argument form, so only the store owner can name a different actor.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class WebAccessAdvancedAdminLoginLiveTests
{
    private const string AdminPassword = "Adm9662Probe";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    [Fact]
    public async Task AnAdminLogin_CanFlipTheSwitchAndIsJournaled_ButCannotCallTheThreeArgumentForm()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the advanced web access admin login live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            Assert.SkipWhen(await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin')", ct) is true,
                "A cluster-wide admin role already exists on this server, and this test makes its own.");

            await ExecAsync(owner, $"CREATE ROLE admin LOGIN NOSUPERUSER PASSWORD '{AdminPassword}'", ct);
            await ExecAsync(owner, $"GRANT CONNECT ON DATABASE \"{scratch.DatabaseName}\" TO admin; GRANT USAGE ON SCHEMA config TO admin", ct);
            /* Provisioning replays the hardening after it grants the login, so the test does too. */
            await ExecAsync(owner, "SELECT config.harden_web_access_advanced()", ct);

            await using var admin = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "admin",
                Password = AdminPassword,
                Pooling = false,
            }.ConnectionString);
            await admin.OpenAsync(ct);

            var denied = await Assert.ThrowsAsync<PostgresException>(() =>
                ScalarAsync(admin, "SELECT config.set_web_access_advanced(true, 'someone-else', true)", ct));
            Assert.Equal("42501", denied.SqlState);
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct));

            await ExecAsync(admin, "SELECT config.set_web_access_advanced(true)", ct);
            Assert.Equal("admin", await ScalarAsync(owner,
                "SELECT actor FROM config.web_access_group_events WHERE event = 'advanced_enabled' ORDER BY event_id DESC LIMIT 1", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(["admin"], "9662probe", cleanupCt));
        }
    }
}
