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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts here mint their own scratch database and roles with per-run names through
   ScratchPostgres and touch nothing on the shared one, so the class is not serialized against the live-postgres
   collection. (The one thing they take that another database's session could meet is the provisioning advisory lock,
   and an advisory lock belongs to the database it was taken in: each class's scratch database has its own, so these
   tests never wait behind a product provisioning in another class, nor it behind them.) */

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

        /* These are the writers of the shared catalog rows that the product runs: the provisioning batch, the reload's
           ALTER ROLE lines, and the password rules' function and trigger (both its check and its command). The
           provisioning scripts an operator runs by hand (tools/provision-roles.sql) and the migrations are outside
           the service's own writes. A fourth that built its own NpgsqlCommand from one of these renderers would run
           unserialized, so each renderer is built into exactly one command, inside the helper's call. */
        Assert.Matches(new Regex(@"ExecuteSerializedAsync\(connection, async \(transaction, token\) =>\s*\{(?:(?!ExecuteNonQueryAsync).)*?return new NpgsqlCommand\(\s*BuildProvisioningSql\(.*?connection, transaction\) \{ CommandTimeout = [^}]*\};\s*\}, logger, cancellationToken\);", RegexOptions.Singleline, TimeSpan.FromSeconds(5)), source);
        Assert.Matches(new Regex(@"new NpgsqlCommand\(\s*BuildComposeStatementTimeoutSql\(.*?CommandTimeout = [^}]*\};\s*await ExecuteSerializedAsync\(\s*command, logger, cancellationToken, lockWait: TimeSpan\.FromSeconds\(ServiceCommandDeadlines\.SerialLoopSeconds\)\);", RegexOptions.Singleline, TimeSpan.FromSeconds(5)), source);
        Assert.Matches(new Regex(@"ExecuteSerializedAsync\(connection, async \(transaction, token\) =>\s*\{\s*alreadyInPlace = await ServerPasswordRulesAreInPlaceAsync\(connection, transaction, token\);.*?new NpgsqlCommand\(BuildServerPasswordRulesSql\(""config""\), connection, transaction\)\s*\{\s*CommandTimeout = [^}]*\};\s*\}, logger, cancellationToken\);", RegexOptions.Singleline, TimeSpan.FromSeconds(5)), source);

        foreach (var renderer in new[] { "BuildProvisioningSql", "BuildComposeStatementTimeoutSql", "BuildServerPasswordRulesSql" })
        {
            Assert.Single(Regex.Matches(source, @"new NpgsqlCommand\(\s*" + renderer + @"\(", RegexOptions.None, TimeSpan.FromSeconds(5)));
        }
    }

    /// <summary>The reload runs at the top of a sweep, so it waits for the key only the serial-loop deadline, not the
    /// full provisioning wait; its bound is that wait plus the batch at its own timeout.</summary>
    [Fact]
    public void TheReload_WaitsForTheKeyOnlyTheSerialLoopDeadline()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedRoles.cs");
        Assert.Single(Regex.Matches(source, @"ExecuteSerializedAsync\(\s*command, logger, cancellationToken,\s*lockWait: TimeSpan\.FromSeconds\(ServiceCommandDeadlines\.SerialLoopSeconds\)\)", RegexOptions.None, TimeSpan.FromSeconds(5)));
        Assert.True(ServiceCommandDeadlines.SerialLoopSeconds < DarlingManagedRoles.ProvisioningLockWaitSeconds);
    }

    [Fact]
    public void TheLockWait_IsBounded_AndPollsAboutOnceASecond_RatherThanBlocking()
    {
        Assert.InRange(DarlingManagedRoles.ProvisioningLockWaitSeconds, 10, 15);
        Assert.InRange(DarlingManagedRoles.ProvisioningLockPollMilliseconds, 500, 2000);
        Assert.Equal(2, DarlingManagedRoles.ProvisioningConcurrentUpdateRetries);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedRoles.cs");
        Assert.Contains("pg_try_advisory_xact_lock(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_advisory_xact_lock(", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The lock is enough only because every provisioning writer connects to ONE database (an advisory lock belongs to the
    /// database it was taken in, while the catalog rows it protects are cluster-wide). The managed store always uses the
    /// fixed database name (also pinned by <c>DarlingManagedRolesTests.BuildProvisioningSql_HardensPublic_ButKeepsAdminViewerConnect</c>),
    /// and the compose path refuses a cluster that holds any other non-template database (<c>DarlingStoreLoginsTests</c>,
    /// "AClusterThatAlsoHoldsAnotherDatabase_IsNotTheServicesOwn", #3914 review F2). This pins both, here, beside the lock.
    /// </summary>
    [Fact]
    public void TheProvisioningLock_IsEnoughBecauseEveryWriterUsesTheStoresOneDatabase()
    {
        Assert.Equal("darling", ProvisioningTarget.Managed.DatabaseIdentifier);
        Assert.Equal("darling", PerformanceMonitor.Darling.Service.DarlingManagedPostgres.DatabaseName);

        var otherDatabase = new ComposeStoreFacts(
            "darling", "darling", BootstrapSuperuser: true,
            new System.Collections.Generic.Dictionary<string, string?>(), new[] { "operator_app" });
        Assert.NotNull(DarlingManagedRoles.RefuseComposeStore(otherDatabase));
        Assert.Null(DarlingManagedRoles.RefuseComposeStore(otherDatabase with { OtherDatabases = Array.Empty<string>() }));
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

    /// <summary>
    /// The provisioning key held by a session that has nothing but the right to connect (#5560): a login with no
    /// privilege on anything, in another session, application name set, with the key taken at SESSION level so it stays
    /// held until the session ends. Returns the session and its pid.
    /// </summary>
    internal static async Task<(NpgsqlConnection Session, int Pid)> HoldTheKeyAsync(
        ScratchPostgres scratch, string login, string application, CancellationToken ct)
    {
        var holder = new NpgsqlConnection(LoginConnectionString(scratch, login, application));
        await holder.OpenAsync(ct);
        await ExecAsync(holder, $"SELECT pg_advisory_lock({DarlingManagedRoles.ProvisioningLockKey})", ct);
        await using var pid = new NpgsqlCommand("SELECT pg_backend_pid()", holder);
        return (holder, (int)(await pid.ExecuteScalarAsync(ct))!);
    }

    internal static string LoginConnectionString(ScratchPostgres scratch, string login, string application) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = login,
            Password = null,
            ApplicationName = application,
            Pooling = false,
        }.ConnectionString;

    internal static async Task SerializedAsync(NpgsqlConnection connection, string sql, int timeoutSeconds, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = timeoutSeconds };
        await DarlingManagedRoles.ExecuteSerializedAsync(command, NullLogger.Instance, ct);
    }

    [Fact]
    public async Task AKeyHeldPastTheWait_GivesOneWarningNamingTheHolder_AndTheBatchStillRuns()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var holderLogin = "prov_hold_" + suffix;
        var ourRole = "prov_free_" + suffix;
        var application = "prov_held_" + suffix;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var service = new NpgsqlConnection(scratch.ConnectionString);
        await service.OpenAsync(ct);
        var database = "\"" + scratch.DatabaseName.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

        NpgsqlConnection? holder = null;
        var bodySucceeded = false;
        try
        {
            await ExecAsync(service, $"CREATE ROLE {holderLogin} LOGIN; CREATE ROLE {ourRole} NOLOGIN", ct);
            var held = await HoldTheKeyAsync(scratch, holderLogin, application, ct);
            holder = held.Session;

            /* The login that holds the key can do nothing in this database but connect, and it is not a superuser. */
            Assert.False(await ScalarBoolAsync(service, $"SELECT rolsuper OR rolcreaterole OR rolcreatedb FROM pg_roles WHERE rolname = '{holderLogin}'", ct));

            var log = new CapturingTestLogger();
            var wait = TimeSpan.FromSeconds(3);
            var clock = System.Diagnostics.Stopwatch.StartNew();
            await DarlingManagedRoles.ExecuteSerializedAsync(
                service,
                (transaction, _) => Task.FromResult<NpgsqlCommand?>(
                    new NpgsqlCommand($"GRANT CONNECT ON DATABASE {database} TO {ourRole}", service, transaction) { CommandTimeout = 10 }),
                log, ct, wait);
            clock.Stop();

            /* The wait ran its whole budget, then the batch ran without the key: the old blocking acquire sat behind the
               holder until the command timeout and threw. */
            Assert.True(clock.Elapsed >= wait - TimeSpan.FromMilliseconds(500), $"gave up the wait after {clock.Elapsed}");
            Assert.True(clock.Elapsed < wait + TimeSpan.FromSeconds(20), $"took {clock.Elapsed}");
            Assert.True(await ScalarBoolAsync(service, $"SELECT has_database_privilege('{ourRole}', '{scratch.DatabaseName}', 'CONNECT')", ct));

            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            var warning = Assert.Single(log.Lines, x => x.StartsWith("Warning:", StringComparison.Ordinal));
            Assert.Contains("4441524C524F4C45", warning, StringComparison.Ordinal);
            Assert.Contains($"pid {held.Pid}", warning, StringComparison.Ordinal);
            Assert.Contains($"user '{holderLogin}'", warning, StringComparison.Ordinal);
            Assert.Contains($"application '{application}'", warning, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            if (holder is not null)
            {
                await holder.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand(
                    $"DROP OWNED BY {holderLogin}, {ourRole}; DROP ROLE IF EXISTS {holderLogin}; DROP ROLE IF EXISTS {ourRole};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWriteThatFailsXx000_IsRunAgainInAFreshTransaction_WithOrWithoutTheKey(bool someoneHoldsTheKey)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var holderLogin = "prov_hold_" + suffix;
        var role = "prov_retry_" + suffix;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);

        /* The product's connection comes from a data source, so this one does too. Npgsql closes the connection on an
           XX-class error, so the retry only works if the helper opens it again. */
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var service = await dataSource.OpenConnectionAsync(ct);

        NpgsqlConnection? holder = null;
        var bodySucceeded = false;
        try
        {
            await ExecAsync(service, $"CREATE ROLE {holderLogin} LOGIN", ct);
            if (someoneHoldsTheKey)
            {
                holder = (await HoldTheKeyAsync(scratch, holderLogin, "prov_held_" + suffix, ct)).Session;
            }

            /* The first run creates the role and then fails the way a sibling's concurrent rewrite does. If the retry did
               not start from a rolled-back transaction, its CREATE ROLE would fail "already exists". */
            var attempts = 0;
            var log = new CapturingTestLogger();
            await DarlingManagedRoles.ExecuteSerializedAsync(
                service,
                (transaction, _) =>
                {
                    attempts++;
                    var sql = attempts == 1
                        ? $"CREATE ROLE {role} NOLOGIN; DO $$ BEGIN RAISE EXCEPTION 'tuple concurrently updated' USING ERRCODE = 'XX000'; END $$"
                        : $"CREATE ROLE {role} NOLOGIN";
                    return Task.FromResult<NpgsqlCommand?>(new NpgsqlCommand(sql, service, transaction) { CommandTimeout = 10 });
                },
                log, ct, TimeSpan.FromSeconds(1));

            Assert.Equal(2, attempts);
            Assert.True(await ScalarBoolAsync(service, $"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}')", ct));
            Assert.Equal(someoneHoldsTheKey ? 1 : 0, log.CountAtLevel(LogLevel.Warning));
            Assert.Contains(log.Lines, x => x.Contains("retry 1 of 2", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            if (holder is not null)
            {
                await holder.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand(
                    $"DROP OWNED BY {holderLogin}; DROP ROLE IF EXISTS {holderLogin}; DROP ROLE IF EXISTS {role};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /* A rerun is right for each retried code: the batch is idempotent and the failed transaction rolled back, so the
       rerun sees the row the other session made (the role exists, so the existence check skips the create). */
    [Theory]
    [InlineData("XX000")]
    [InlineData("23505")]
    [InlineData("42710")]
    [InlineData("40P01")]
    public void AConcurrentWriteFailure_IsRetried(string sqlState)
    {
        Assert.True(DarlingManagedRoles.IsConcurrentProvisioningConflict(
            new PostgresException("failed", "ERROR", "ERROR", sqlState)));
    }

    [Theory]
    [InlineData("42501")]
    [InlineData("57014")]
    [InlineData("22012")]
    public void AnyOtherFailure_IsNotRetried(string sqlState)
    {
        Assert.False(DarlingManagedRoles.IsConcurrentProvisioningConflict(
            new PostgresException("failed", "ERROR", "ERROR", sqlState)));
    }

    [Fact]
    public async Task AWriteThatKeepsFailingXx000_IsRunThreeTimesThenThrows_AndAnyOtherErrorIsNotRetried()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var service = new NpgsqlConnection(scratch.ConnectionString);
        await service.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            var attempts = 0;
            var failure = await Assert.ThrowsAsync<PostgresException>(() => DarlingManagedRoles.ExecuteSerializedAsync(
                service,
                (transaction, _) =>
                {
                    attempts++;
                    return Task.FromResult<NpgsqlCommand?>(new NpgsqlCommand(
                        "DO $$ BEGIN RAISE EXCEPTION 'tuple concurrently updated' USING ERRCODE = 'XX000'; END $$", service, transaction));
                },
                NullLogger.Instance, ct));
            Assert.Equal("XX000", failure.SqlState);
            Assert.Equal(1 + DarlingManagedRoles.ProvisioningConcurrentUpdateRetries, attempts);

            /* The last failure left the connection closed (see above); a plain error is thrown at once and not retried. */
            await service.OpenAsync(ct);
            var other = 0;
            var divide = await Assert.ThrowsAsync<PostgresException>(() => DarlingManagedRoles.ExecuteSerializedAsync(
                service,
                (transaction, _) =>
                {
                    other++;
                    return Task.FromResult<NpgsqlCommand?>(new NpgsqlCommand("SELECT 1/0", service, transaction));
                },
                NullLogger.Instance, ct));
            Assert.Equal("22012", divide.SqlState);
            Assert.Equal(1, other);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ThePlanningCallback_RunsAfterTheLockIsTaken_AndInsideTheLockedTransaction()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var sibling = new NpgsqlConnection(scratch.ConnectionString);
        await using var service = new NpgsqlConnection(scratch.ConnectionString);
        await sibling.OpenAsync(ct);
        await service.OpenAsync(ct);

        NpgsqlTransaction? siblingTransaction = null;
        var siblingOpen = false;
        var bodySucceeded = false;
        try
        {
            siblingTransaction = await sibling.BeginTransactionAsync(ct);
            siblingOpen = true;
            await using (var hold = new NpgsqlCommand($"SELECT pg_advisory_xact_lock({DarlingManagedRoles.ProvisioningLockKey})", sibling, siblingTransaction))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            var planned = 0;
            var holdingInsideTheCallback = false;
            var ours = DarlingManagedRoles.ExecuteSerializedAsync(
                service,
                async (transaction, token) =>
                {
                    planned++;
                    await using var check = new NpgsqlCommand(
                        $"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid() AND objid = {DarlingManagedRoles.ProvisioningLockKey & 0xFFFFFFFFL})",
                        service, transaction);
                    holdingInsideTheCallback = (bool)(await check.ExecuteScalarAsync(token))!;
                    return null;
                },
                NullLogger.Instance, ct);
            await Task.Delay(TimeSpan.FromSeconds(1.5), ct);
            Assert.Equal(0, planned);
            Assert.False(ours.IsCompleted, "the planning callback ran, or the write finished, while a sibling held the lock");

            await siblingTransaction.CommitAsync(ct);
            siblingOpen = false;
            await ours.WaitAsync(TimeSpan.FromSeconds(30), ct);

            Assert.Equal(1, planned);
            Assert.True(holdingInsideTheCallback, "the planning callback did not run while holding the lock");

            bodySucceeded = true;
        }
        finally
        {
            if (siblingOpen && siblingTransaction is not null)
            {
                await siblingTransaction.RollbackAsync(CancellationToken.None);
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AReadThatFailsInsideTheLockedTransaction_AndIsAnsweredWithADefault_DoesNotPoisonTheBatch()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning-serialization pins (each mints its own scratch database and role).");
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var login = "prov_reader_" + suffix;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {login} LOGIN", ct);

            /* The scratch store has no config schema: the timeout read and the rules check both fail on a missing relation
               and answer with a default; the batch after them must still run (and commit) in the same transaction. */
            var log = new CapturingTestLogger();
            int? timeout = null;
            var inPlace = true;
            var ran = false;
            await DarlingManagedRoles.ExecuteSerializedAsync(
                owner,
                async (transaction, token) =>
                {
                    timeout = await DarlingManagedRoles.ReadComposeStatementTimeoutAsync(owner, transaction, log, token);
                    inPlace = await DarlingManagedRoles.ServerPasswordRulesAreInPlaceAsync(owner, transaction, token);
                    ran = true;
                    return new NpgsqlCommand($"ALTER ROLE {login} SET statement_timeout = '7s'", owner, transaction);
                },
                log, ct);
            Assert.True(ran);
            Assert.False(inPlace);
            Assert.Equal(McpCommandDeadlines.ComposedQueryFallbackSeconds, timeout);
            Assert.True(await ScalarBoolAsync(owner, $"SELECT EXISTS (SELECT 1 FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = '{login}' AND s.setconfig::text LIKE '%7s%')", ct));

            /* And the stored-verifier read, refused to a login that may not read pg_authid, answers empty. */
            await using var reader = new NpgsqlConnection(LoginConnectionString(scratch, login, "prov_reader_" + suffix));
            await reader.OpenAsync(ct);
            var stored = new System.Collections.Generic.Dictionary<string, string?>();
            var batchRan = false;
            await DarlingManagedRoles.ExecuteSerializedAsync(
                reader,
                async (transaction, token) =>
                {
                    stored = await DarlingManagedRoles.ReadStoredRoleSecretsAsync(reader, transaction, log, token);
                    batchRan = true;
                    return new NpgsqlCommand("SELECT 1", reader, transaction);
                },
                log, ct);
            Assert.Empty(stored);
            Assert.True(batchRan);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {login}; DROP ROLE IF EXISTS {login};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
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
[Collection("pg-cluster-roles")]
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

    /// <summary>
    /// A login with nothing but CONNECT holds the provisioning key for the whole test (#5560). Provisioning must still
    /// complete: each run waits out its budget, logs one warning naming the holder, then runs the real batch without the
    /// key. The batch runs twice that way, which is also the proof that it is safe to run twice outside the lock (the
    /// retry after a concurrent-update failure, and a sibling that gave up waiting, both do exactly that).
    /// </summary>
    [Fact]
    public async Task AHeldKey_DoesNotStopTheRealBatch_ItRunsWithoutTheKey_TwiceInARow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live provisioning batch pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var holderLogin = "prov_hold_" + Guid.NewGuid().ToString("N")[..8];

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PerformanceMonitor.Darling.Storage.PgMigrations.MigrateAsync(owner, ct);
        await using (var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner))
        {
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!,
                "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");
        }

        NpgsqlConnection? holder = null;
        var bodySucceeded = false;
        try
        {
            await using (var create = new NpgsqlCommand($"CREATE ROLE {holderLogin} LOGIN", owner))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            var held = await ProvisioningSerializationTests.HoldTheKeyAsync(scratch, holderLogin, "prov_held_" + holderLogin, ct);
            holder = held.Session;

            var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            var log = new CapturingTestLogger();
            var wait = TimeSpan.FromSeconds(2);
            var clock = System.Diagnostics.Stopwatch.StartNew();
            for (var run = 1; run <= 2; run++)
            {
                await DarlingManagedRoles.ExecuteSerializedAsync(
                    owner,
                    (transaction, _) => Task.FromResult<NpgsqlCommand?>(new NpgsqlCommand(
                        DarlingManagedRoles.BuildProvisioningSql(
                            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                            15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)),
                        owner, transaction) { CommandTimeout = 60 }),
                    log, ct, wait);
                Assert.Equal(run, log.CountAtLevel(LogLevel.Warning));
            }

            clock.Stop();
            Assert.True(clock.Elapsed >= 2 * wait - TimeSpan.FromSeconds(1), $"the waits were cut short: {clock.Elapsed}");
            Assert.True(clock.Elapsed < 2 * wait + TimeSpan.FromSeconds(90), $"provisioning took {clock.Elapsed}");
            Assert.All(log.Lines.Where(x => x.StartsWith("Warning:", StringComparison.Ordinal)), x => Assert.Contains($"pid {held.Pid}", x, StringComparison.Ordinal));

            await using var roles = new NpgsqlCommand("SELECT count(*) FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", owner);
            Assert.Equal(3L, (long)(await roles.ExecuteScalarAsync(ct))!);
            await using var connect = new NpgsqlCommand($"SELECT has_database_privilege('viewer', '{scratch.DatabaseName}', 'CONNECT')", owner);
            Assert.True((bool)(await connect.ExecuteScalarAsync(ct))!);

            bodySucceeded = true;
        }
        finally
        {
            if (holder is not null)
            {
                await holder.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand(
                    $"DROP OWNED BY admin, viewer, mcp, {holderLogin}; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp; DROP ROLE IF EXISTS {holderLogin}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
