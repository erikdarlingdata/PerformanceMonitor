/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4251 against a live rig: <c>pg_settings.pending_restart</c> is backend-local, so a NEW connection reads
/// <c>false</c> for a setting genuinely pending a restart — the collector reconnects every cycle, so this was
/// not a corner case, it was every cycle. <see cref="PgServerConfigCollector"/> now folds in
/// <c>pg_file_settings</c> when <see cref="PgFileSettingsCapability"/> finds it readable.
///
/// <para>Each test reproduces the underlying PostgreSQL behaviour first (the bug) and then runs the fixed
/// collector query against the SAME state (the fix), rather than asserting against a canned fixture — the
/// point of #4251 is that the two disagree.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgServerConfigPendingRestartLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* Npgsql pools physical connections process-wide by default: a disposed NpgsqlConnection's backend can be
       handed back out by a LATER `new NpgsqlConnection(cs).OpenAsync()` on the same connection string, rather
       than forking a genuinely new backend. An OLD (pooled, already-open-through-the-reload) backend DOES
       correctly see pending_restart - only a truly NEW backend has #4251's blind spot - so every connection
       standing in for "a fresh collector cycle" below must disable pooling to be a real repro. */
    private static string PooledOffConnectionString(string cs) => cs.TrimEnd(';') + ";Pooling=false";

    /* ALTER SYSTEM refuses to run inside a transaction block, and PostgreSQL's simple query protocol treats
       multiple ';'-separated statements sent in ONE command text as an implicit transaction block — so the
       ALTER SYSTEM statement and the reload that makes it take effect must be two separate round trips, never
       one CommandText. */
    private static async Task AlterSystemAndReloadAsync(NpgsqlConnection connection, string alterSystemSql, System.Threading.CancellationToken ct)
    {
        await using (var alter = connection.CreateCommand())
        {
            alter.CommandText = alterSystemSql;
            await alter.ExecuteNonQueryAsync(ct);
        }

        await using var reload = connection.CreateCommand();
        reload.CommandText = "SELECT pg_reload_conf()";
        await reload.ExecuteNonQueryAsync(ct);
    }

    private static async Task<PgServerConfigCollector.Row?> RunCollectorAsync(
        NpgsqlConnection connection, bool fileSettingsReadable, string settingName)
    {
        var context = new CollectorContext
        {
            ServerId = -4251,
            ServerName = "pg-4251-e2e",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            PgFileSettingsReadable = fileSettingsReadable,
        };

        var plan = PgServerConfigCollector.Instance.BuildQuery(context);
        await using var command = connection.CreateCommand();
        command.CommandText = plan.Text;
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var rows = await PgServerConfigCollector.Instance.ReadAsync(reader, context, TestContext.Current.CancellationToken);
        return rows.FirstOrDefault(r => r.DatabaseName is null && r.RoleName is null && r.Name == settingName);
    }

    [Fact]
    public async Task APostmasterSettingPendingARestart_ReadsFalseOnPgSettingsAlone_ButTrueOnceFileSettingsIsFolded_In()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4251 round trip.");
        var ct = TestContext.Current.CancellationToken;

        await using var setup = new NpgsqlConnection(cs);
        await setup.OpenAsync(ct);
        await AlterSystemAndReloadAsync(setup, "ALTER SYSTEM RESET shared_buffers", ct);

        var bodySucceeded = false;
        try
        {
            /* The bug's own repro: change a postmaster-context setting and reload. pg_reload_conf() only
               SENDS SIGHUP; postmaster and backends re-read the file asynchronously, so a short pause gives
               that propagation time to finish before the next connection forks — matching how the bug
               actually presents (the next collection cycle, minutes later, never microseconds later). */
            await AlterSystemAndReloadAsync(setup, "ALTER SYSTEM SET shared_buffers = '256MB'", ct);
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            var freshCs = PooledOffConnectionString(cs!);

            /* A NEW connection — exactly what the collector opens every cycle — reads pending_restart = false
               directly off pg_settings. This is #4251 itself, asserted before the fix is exercised at all. */
            await using (var freshConnection = new NpgsqlConnection(freshCs))
            {
                await freshConnection.OpenAsync(ct);
                await using var raw = freshConnection.CreateCommand();
                raw.CommandText = "SELECT pending_restart FROM pg_settings WHERE name = 'shared_buffers'";
                var rawPendingRestart = (bool)(await raw.ExecuteScalarAsync(ct))!;
                Assert.False(rawPendingRestart, "pg_settings.pending_restart must read false from a fresh connection after a reload — this is #4251, and a true here means the repro itself changed.");
            }

            /* The OLD collector query (PgFileSettingsReadable = false) reproduces the bug end to end: BEFORE
               this fix shipped, this was the only query PgServerConfigCollector ever sent. */
            await using (var beforeConnection = new NpgsqlConnection(freshCs))
            {
                await beforeConnection.OpenAsync(ct);
                var before = await RunCollectorAsync(beforeConnection, fileSettingsReadable: false, "shared_buffers");
                Assert.NotNull(before);
                Assert.False(before!.Value.PendingRestart, "the pre-fix query path must still reproduce #4251 exactly, so the AFTER assertion below is a real fix and not a query that always says true.");
            }

            /* The FIXED collector query (PgFileSettingsReadable = true, what a superuser or granted role now
               gets) reads the same live state and correctly reports the setting as pending a restart. */
            await using (var afterConnection = new NpgsqlConnection(freshCs))
            {
                await afterConnection.OpenAsync(ct);
                var after = await RunCollectorAsync(afterConnection, fileSettingsReadable: true, "shared_buffers");
                Assert.NotNull(after);
                Assert.True(after!.Value.PendingRestart, "pg_file_settings has a postmaster-context row with error = 'setting could not be applied' for shared_buffers, so the fixed query must report it pending.");
            }

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: RunOwnedAsync, not RunAsync — the cleanup runs ALTER SYSTEM RESET on the SAME `setup`
               connection the body used throughout, and a cluster-wide file reset needs no fresh connection. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
                await AlterSystemAndReloadAsync(setup, "ALTER SYSTEM RESET shared_buffers", ct));
        }
    }

    [Fact]
    public async Task ARejectedValueAtANonPostmasterContext_IsNeverStoredAsPendingARestart()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4251 rejection round trip.");
        var ct = TestContext.Current.CancellationToken;

        await using var setup = new NpgsqlConnection(cs);
        await setup.OpenAsync(ct);

        /* guc.c's 'setting could not be applied' also marks an outright REJECTED value — out of range here —
           at a setting whose context is NOT postmaster. ALTER SYSTEM validates range itself and refuses to
           write this, so the file is edited directly (a SHOW data_directory + File.AppendAllText round trip
           below), which is exactly how a hand-edited postgresql.conf produces the same row in production. */
        var dataDirectory = (string)(await new NpgsqlCommand("SHOW data_directory", setup).ExecuteScalarAsync(ct))!;
        var autoConfPath = System.IO.Path.Combine(dataDirectory, "postgresql.auto.conf");
        var originalContents = await System.IO.File.ReadAllTextAsync(autoConfPath, ct);

        var bodySucceeded = false;
        try
        {
            await System.IO.File.AppendAllTextAsync(autoConfPath, "\nwork_mem = '1kB'\n", ct);
            await using (var reload = setup.CreateCommand())
            {
                reload.CommandText = "SELECT pg_reload_conf()";
                await reload.ExecuteNonQueryAsync(ct);
            }

            await using var afterConnection = new NpgsqlConnection(PooledOffConnectionString(cs!));
            await afterConnection.OpenAsync(ct);
            var workMem = await RunCollectorAsync(afterConnection, fileSettingsReadable: true, "work_mem");
            Assert.NotNull(workMem);
            Assert.Equal("user", workMem!.Value.Context);
            Assert.False(workMem.Value.PendingRestart, "work_mem is context=user, not postmaster: an out-of-range file value there is a REJECTED value, never a pending restart, even though pg_file_settings.error is the identical text guc.c uses for both cases.");

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: RunOwnedAsync — the file write and the reload that makes it take effect both have to run
               against this rig's own data directory and `setup` connection, not a fresh store connection. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await System.IO.File.WriteAllTextAsync(autoConfPath, originalContents, ct);
                await using var reload = setup.CreateCommand();
                reload.CommandText = "SELECT pg_reload_conf()";
                await reload.ExecuteNonQueryAsync(ct);
            });
        }
    }

    [Fact]
    public async Task ANonSuperuserRoleWithoutTheGrants_KeepsTodaysAnswer_AndTheCapabilityLogsItOnce()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4251 permission round trip.");
        var ct = TestContext.Current.CancellationToken;
        const string roleName = "pm_4251_monitor_role";
        var targetKey = "pg-4251-permissions-" + Guid.NewGuid().ToString("N");

        await using var setup = new NpgsqlConnection(cs);
        await setup.OpenAsync(ct);
        await using (var dropFirst = setup.CreateCommand())
        {
            dropFirst.CommandText = $"DROP ROLE IF EXISTS {roleName}";
            await dropFirst.ExecuteNonQueryAsync(ct);
        }
        await using (var create = setup.CreateCommand())
        {
            /* pg_monitor membership alone (#4251's own premise): the role a real Darling deployment grants,
               which does NOT extend to pg_file_settings — verified against a live 18.6 rig, not assumed. */
            create.CommandText = $"CREATE ROLE {roleName} LOGIN; GRANT pg_monitor TO {roleName};";
            await create.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            PgFileSettingsCapability.Invalidate(targetKey);

            var roleConnectionString = new NpgsqlConnectionStringBuilder(cs) { Username = roleName, Password = null }.ToString();
            await using var roleConnection = new NpgsqlConnection(roleConnectionString);
            await roleConnection.OpenAsync(ct);

            var readable = await PgFileSettingsCapability.IsReadableAsync(roleConnection, targetKey, ct);
            Assert.False(readable, "pg_monitor membership must not be read as pg_file_settings access — that is #4251's own premise about a least-privilege monitoring role.");

            /* The collector itself, run exactly as PgFileSettingsReadable = false leaves it: it must still
               collect every pg_settings row as a non-superuser, today's answer, not an error. */
            var context = new CollectorContext
            {
                ServerId = -4251,
                ServerName = "pg-4251-permissions",
                CollectionTime = DateTime.UtcNow,
                Deltas = null!,
                PgFileSettingsReadable = readable,
            };
            var plan = PgServerConfigCollector.Instance.BuildQuery(context);
            await using var command = roleConnection.CreateCommand();
            command.CommandText = plan.Text;
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = await PgServerConfigCollector.Instance.ReadAsync(reader, context, ct);
            Assert.True(rows.Count > 100, "a non-superuser role with no pg_file_settings grant must still collect the full pg_settings snapshot — the whole point of resolving the capability BEFORE BuildQuery.");

            /* Logged once, not every cycle. */
            Assert.True(PgFileSettingsCapability.ShouldLogUnreadable(targetKey), "the first discovery must be logged.");
            Assert.False(PgFileSettingsCapability.ShouldLogUnreadable(targetKey), "a target already found unreadable must not be re-logged on a later cycle, even though IsReadableAsync itself is re-checked hourly.");

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: RunOwnedAsync — dropping the role this test created needs the SAME admin `setup`
               connection, not a fresh store connection. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                PgFileSettingsCapability.Invalidate(targetKey);
                await using var drop = setup.CreateCommand();
                drop.CommandText = $"DROP ROLE IF EXISTS {roleName}";
                await drop.ExecuteNonQueryAsync(ct);
            });
        }
    }
}
