/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts here mint their own scratch database and roles with per-run names through
   ScratchPostgres and touch nothing on the shared one, so the class is not serialized against the live-postgres
   collection. (The one cluster-wide thing they take is the provisioning advisory lock, which a product provisioning
   in another class only ever waits behind for a second or two.) */

/// <summary>
/// Pins the two halves of #5560, "tuple concurrently updated" (XX000) from role and privilege DDL. PostgreSQL does not
/// lock the catalog row a GRANT, DROP OWNED or ALTER ROLE rewrites, so two sessions rewriting the same row at once
/// make the second fail once the first commits. The row is shared by every session on the cluster: the CONNECT ACL of
/// the store's database, a role, a table's ACL.
/// <list type="bullet">
/// <item>Test side: a replay of the viewer's provisioned grants used to keep the managed store's database name, so every
/// test that replayed rewrote the CONNECT ACL of the cluster's own <c>darling</c> database at once, and the cleanup's
/// <c>DROP OWNED BY</c> rewrote it again. The replay is now aimed at the test's own scratch database.</item>
/// <item>Product side: two services can start against one cluster (the managed store adopts a running postmaster), each
/// running the same provisioning batch. The batch now takes an advisory lock first, so the second waits for the first.</item>
/// </list>
/// </summary>
public sealed class ProvisioningSerializationTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheViewerReplay_AimsEveryDatabaseLevelPrivilege_AtTheScratchDatabase_NotTheClustersOwn()
    {
        var statements = ViewerGrantReplay.StatementsFor("replay_role_5560", "scratch_5560");

        Assert.Contains("GRANT CONNECT ON DATABASE \"scratch_5560\" TO replay_role_5560", statements);
        Assert.DoesNotContain(statements, x => x.Contains("ON DATABASE darling", StringComparison.Ordinal));
        Assert.All(
            statements.Where(x => x.Contains(" ON DATABASE ", StringComparison.Ordinal)),
            x => Assert.Contains("ON DATABASE \"scratch_5560\"", x, StringComparison.Ordinal));
    }

    [Fact]
    public void TheReplayRetarget_QuotesTheScratchName_AndLeavesEveryOtherStatementAlone()
    {
        Assert.Equal("GRANT CONNECT ON DATABASE \"a\"\"b\"", ViewerGrantReplay.OnDatabase("GRANT CONNECT ON DATABASE darling", "a\"b"));
        Assert.Equal("GRANT SELECT ON ALL TABLES IN SCHEMA collect", ViewerGrantReplay.OnDatabase("GRANT SELECT ON ALL TABLES IN SCHEMA collect", "x"));
    }

    [Fact]
    public void EveryRolePrivilegeWrite_OfTheProvisioningPaths_GoesThroughTheLockedTransaction()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedRoles.cs");

        /* The provisioning batch and the reload's ALTER ROLE lines are the two writes; a third that built its own
           NpgsqlCommand from either renderer would run unserialized. */
        Assert.Matches(new Regex(@"new NpgsqlCommand\(\s*BuildProvisioningSql\(.*?CommandTimeout = [^}]*\};\s*await ExecuteSerializedAsync\(command, cancellationToken\);", RegexOptions.Singleline, TimeSpan.FromSeconds(5)), source);
        Assert.Matches(new Regex(@"new NpgsqlCommand\(\s*BuildComposeStatementTimeoutSql\(.*?CommandTimeout = [^}]*\};\s*await ExecuteSerializedAsync\(command, cancellationToken\);", RegexOptions.Singleline, TimeSpan.FromSeconds(5)), source);
    }

    [Fact]
    public async Task TheReplayOfTheViewersGrants_LeavesTheClustersOwnDarlingDatabaseAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;
        var role = "replay_ro_" + Guid.NewGuid().ToString("N")[..8];

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, $"CREATE ROLE {role} NOLOGIN", ct);
            var databaseLevel = ViewerGrantReplay.StatementsFor(role, scratch.DatabaseName)
                .Where(x => x.Contains(" ON DATABASE ", StringComparison.Ordinal))
                .ToList();
            Assert.NotEmpty(databaseLevel);
            foreach (var statement in databaseLevel)
            {
                await ExecAsync(connection, statement, ct);
            }

            /* The privilege landed on the test's own database, and the cluster's shared one never heard of the role. */
            Assert.True(await ScalarBoolAsync(connection, $"SELECT has_database_privilege('{role}', '{scratch.DatabaseName}', 'CONNECT')", ct));
            Assert.False(await ScalarBoolAsync(connection,
                $"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'darling' AND COALESCE(datacl::text, '') LIKE '%{role}=%')", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public async Task ASecondProvisioningBatch_WaitsForTheFirst_InsteadOfFailingTupleConcurrentlyUpdated()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var siblingRole = "prov_a_" + suffix;
        var ourRole = "prov_b_" + suffix;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var sibling = new NpgsqlConnection(scratch.ConnectionString);
        await using var service = new NpgsqlConnection(scratch.ConnectionString);
        await sibling.OpenAsync(ct);
        await service.OpenAsync(ct);
        var database = "\"" + scratch.DatabaseName.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

        NpgsqlTransaction? siblingTransaction = null;
        var siblingOpen = false;
        var bodySucceeded = false;
        try
        {
            await ExecAsync(sibling, $"CREATE ROLE {siblingRole} NOLOGIN; CREATE ROLE {ourRole} NOLOGIN", ct);

            /* A sibling service in the middle of ITS batch: it holds the provisioning lock and has rewritten the
               database's ACL row, uncommitted. This is the whole race, made deterministic. */
            siblingTransaction = await sibling.BeginTransactionAsync(ct);
            siblingOpen = true;
            await using (var hold = new NpgsqlCommand($"SELECT pg_advisory_xact_lock({DarlingManagedRoles.ProvisioningLockKey})", sibling, siblingTransaction))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            await using (var grant = new NpgsqlCommand($"GRANT CONNECT ON DATABASE {database} TO {siblingRole}", sibling, siblingTransaction))
            {
                await grant.ExecuteNonQueryAsync(ct);
            }

            var ours = SerializedAsync(service, $"GRANT CONNECT ON DATABASE {database} TO {ourRole}", 60, ct);
            await Task.Delay(TimeSpan.FromSeconds(1.5), ct);
            Assert.False(ours.IsCompleted, "the second batch ran while the first held the provisioning lock");

            await siblingTransaction.CommitAsync(ct);
            siblingOpen = false;

            /* Without the lock this throws PostgresException XX000 "tuple concurrently updated" here: the second GRANT
               waited on the first's uncommitted row and then found it rewritten. */
            await ours.WaitAsync(TimeSpan.FromSeconds(30), ct);

            Assert.True(await ScalarBoolAsync(sibling, $"SELECT has_database_privilege('{siblingRole}', '{scratch.DatabaseName}', 'CONNECT')", ct));
            Assert.True(await ScalarBoolAsync(sibling, $"SELECT has_database_privilege('{ourRole}', '{scratch.DatabaseName}', 'CONNECT')", ct));

            bodySucceeded = true;
        }
        finally
        {
            if (siblingOpen && siblingTransaction is not null)
            {
                await siblingTransaction.RollbackAsync(CancellationToken.None);
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand(
                    $"DROP OWNED BY {siblingRole}, {ourRole}; DROP ROLE IF EXISTS {siblingRole}; DROP ROLE IF EXISTS {ourRole};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public async Task TheLockedTransaction_ReleasesTheLockOnItsOwn_SoTheNextBatchRunsAtOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;
        var role = "prov_c_" + Guid.NewGuid().ToString("N")[..8];

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* A batch that fails rolls back and frees the lock, then a good batch on the SAME pooled-style connection runs. */
            await Assert.ThrowsAsync<PostgresException>(() => SerializedAsync(connection, "SELECT 1/0", 30, ct));
            await SerializedAsync(connection, $"CREATE ROLE {role} NOLOGIN", 30, ct);
            Assert.True(await ScalarBoolAsync(connection, $"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}')", ct));
            Assert.False(await ScalarBoolAsync(connection,
                $"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid())", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {role};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    internal static async Task SerializedAsync(NpgsqlConnection connection, string sql, int timeoutSeconds, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = timeoutSeconds };
        await DarlingManagedRoles.ExecuteSerializedAsync(command, ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}

/* The real batch creates the cluster-wide admin/viewer/mcp roles, so this class shares the live-postgres collection with
   the other classes that do (PasswordKeyTablesLiveTests, DarlingSecuritySplitLiveTests) and runs after or before them,
   never beside them. */

/// <summary>
/// The product's real provisioning batch, run through the locked transaction (#5560) the way the service runs it, on a
/// scratch store: it must apply in full inside one explicit transaction (nothing in it may need to run outside one), and
/// it must be re-runnable, since a second service re-runs it right after the first.
/// </summary>
[Collection("live-postgres")]
public sealed class ProvisioningBatchLockedTransactionLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheRealBatch_AppliesInsideTheLockedTransaction_AndAppliesAgain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning batch pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PerformanceMonitor.Darling.Storage.PgMigrations.MigrateAsync(owner, ct);
        await using (var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner))
        {
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!,
                "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            var batch = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));

            await ProvisioningSerializationTests.SerializedAsync(owner, batch, 60, ct);
            await ProvisioningSerializationTests.SerializedAsync(owner, batch, 60, ct);

            await using var roles = new NpgsqlCommand("SELECT count(*) FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", owner);
            Assert.Equal(3L, (long)(await roles.ExecuteScalarAsync(ct))!);
            await using var connect = new NpgsqlCommand($"SELECT has_database_privilege('viewer', '{scratch.DatabaseName}', 'CONNECT')", owner);
            Assert.True((bool)(await connect.ExecuteScalarAsync(ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand(
                    "DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
