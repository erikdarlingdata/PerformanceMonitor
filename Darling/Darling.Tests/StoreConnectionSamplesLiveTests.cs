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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #9437 against a real store: sessions opened under a second role are counted by role, application and state, one waits on a lock and is counted
/// under the Lock wait type, the minute's slot headroom is the settings less the backends counted, the role filter cuts in SQL, and a capture with no
/// host reading stores NULL host columns and the read says not_available.
/// </summary>
public sealed class StoreConnectionSamplesLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9437 tests (each mints its own scratch database).";

    private static async Task<NpgsqlConnection> OpenAsRoleAsync(string connectionString, string role, string application, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Username = role, Password = "probe-pw", ApplicationName = application, Pooling = false };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task<JsonElement> ReadAsync(NpgsqlDataSource source, CancellationToken ct, int limit = 60, string? role = null, string group_by = "none")
    {
        var text = await DarlingMcpStoreConnectionsTools.GetStoreConnections(source, limit: limit, role: role, group_by: group_by, cancellationToken: ct);
        return JsonDocument.Parse(text).RootElement;
    }

    [Fact]
    public async Task ExtraSessions_AreCounted_WithTheMinutesSlotHeadroom_AndTheRoleFilterCutsInSql_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var role = "conn_probe_" + Guid.NewGuid().ToString("N")[..8];
        var sessions = new List<NpgsqlConnection>();
        var bodySucceeded = false;
        try
        {
            await using var admin = new NpgsqlConnection(scratch.ConnectionString);
            await admin.OpenAsync(ct);
            await PgMigrations.MigrateAsync(admin, ct);

            await using (var setup = new NpgsqlCommand($"CREATE ROLE {role} LOGIN PASSWORD 'probe-pw'; CREATE TABLE public.conn_probe_lock (id int); GRANT ALL ON public.conn_probe_lock TO {role}", admin))
            {
                await setup.ExecuteNonQueryAsync(ct);
            }

            for (var i = 0; i < 3; i++)
            {
                sessions.Add(await OpenAsRoleAsync(scratch.ConnectionString, role, "conn-probe-idle", ct));
            }

            /* One session waits on a lock another holds. */
            var holder = await OpenAsRoleAsync(scratch.ConnectionString, role, "conn-probe-holder", ct);
            sessions.Add(holder);
            await using (var begin = new NpgsqlCommand("BEGIN; LOCK TABLE public.conn_probe_lock IN ACCESS EXCLUSIVE MODE", holder))
            {
                await begin.ExecuteNonQueryAsync(ct);
            }

            var waiter = await OpenAsRoleAsync(scratch.ConnectionString, role, "conn-probe-waiter", ct);
            sessions.Add(waiter);
            var waiting = Task.Run(async () =>
            {
                await using var lockCommand = new NpgsqlCommand("BEGIN; LOCK TABLE public.conn_probe_lock IN ACCESS EXCLUSIVE MODE; ROLLBACK", waiter);
                await lockCommand.ExecuteNonQueryAsync(CancellationToken.None);
            }, ct);
            for (var attempt = 0; attempt < 100 && await ScalarAsync(admin, "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'conn-probe-waiter' AND wait_event_type = 'Lock'", ct) == 0; attempt++)
            {
                await Task.Delay(100, ct);
            }

            await StoreConnectionSamples.CaptureAsync(admin, DateTime.UtcNow, hostLoad: null, ct);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* The idle sessions: 3 backends for that role and application, in state idle. */
            var all = await ReadAsync(source, ct);
            Assert.Equal("ok", all.GetProperty("status").GetString());
            var minute = all.GetProperty("minutes")[0];
            var groups = minute.GetProperty("groups").EnumerateArray().ToList();
            var idle = groups.Single(g => g.GetProperty("application").GetString() == "conn-probe-idle");
            Assert.Equal(role, idle.GetProperty("role").GetString());
            Assert.Equal("idle", idle.GetProperty("state").GetString());
            Assert.Equal(3, idle.GetProperty("backends").GetInt32());

            /* The waiter is counted under the Lock wait type. */
            var lockWaiter = groups.Single(g => g.GetProperty("application").GetString() == "conn-probe-waiter");
            Assert.Equal("Lock", lockWaiter.GetProperty("wait_event_type").GetString());

            /* The headroom is the settings less the backends counted. */
            var slots = minute.GetProperty("slots");
            var counted = groups.Sum(g => g.GetProperty("backends").GetInt32());
            Assert.Equal(counted, slots.GetProperty("backends_total").GetInt32());
            var max = (int)await ScalarAsync(admin, "SELECT current_setting('max_connections')::int", ct);
            var superuserReserved = (int)await ScalarAsync(admin, "SELECT current_setting('superuser_reserved_connections')::int", ct);
            var reserved = (int)await ScalarAsync(admin, "SELECT coalesce(current_setting('reserved_connections', true), '0')::int", ct);
            Assert.Equal(max - superuserReserved - reserved - counted, slots.GetProperty("slots_available").GetInt32());

            /* No host reading: NULL host columns, and the read says so. */
            Assert.Equal("not_available", all.GetProperty("host_load").GetString());
            Assert.Equal(JsonValueKind.Null, minute.GetProperty("host").GetProperty("cpu_percent").ValueKind);

            /* The role filter cuts in SQL: another role matches no minute, and this role's groups are all this role's. */
            var other = await ReadAsync(source, ct, role: "no_such_role");
            Assert.Equal("empty", other.GetProperty("status").GetString());
            var mine = await ReadAsync(source, ct, role: role, group_by: "application");
            Assert.Equal(role, mine.GetProperty("role").GetString());
            var mineGroups = mine.GetProperty("minutes")[0].GetProperty("groups").EnumerateArray().ToList();
            Assert.Equal(3, mineGroups.Single(g => g.GetProperty("application").GetString() == "conn-probe-idle").GetProperty("backends").GetInt32());
            Assert.DoesNotContain(mineGroups, g => g.GetProperty("application").GetString() is null);

            /* An out-of-range limit is refused, never clamped. */
            Assert.Equal("invalid", (await ReadAsync(source, ct, limit: 100000)).GetProperty("status").GetString());

            await using (var release = new NpgsqlCommand("ROLLBACK", holder))
            {
                await release.ExecuteNonQueryAsync(ct);
            }

            await waiting;
            bodySucceeded = true;
        }
        finally
        {
            foreach (var session in sessions)
            {
                await session.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP TABLE IF EXISTS public.conn_probe_lock; DROP ROLE IF EXISTS {role}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public async Task AHostReading_IsStored_AndTheReadReportsItAvailable_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var admin = new NpgsqlConnection(scratch.ConnectionString);
            await admin.OpenAsync(ct);
            await PgMigrations.MigrateAsync(admin, ct);
            await StoreConnectionSamples.CaptureAsync(admin, DateTime.UtcNow, new StoreHostLoad(42.5, 1234567890L, 1.5), ct);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var answer = await ReadAsync(source, ct);
            Assert.Equal("available", answer.GetProperty("host_load").GetString());
            var host = answer.GetProperty("minutes")[0].GetProperty("host");
            Assert.Equal(42.5, host.GetProperty("cpu_percent").GetDouble());
            Assert.Equal(1234567890L, host.GetProperty("available_memory_bytes").GetInt64());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AManagedStoresCapture_StoresTheHostsAvailableMemory_AndTheReadReportsItAvailable_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The managed store's host reading is the Windows GlobalMemoryStatusEx read.");
        var ct = TestContext.Current.CancellationToken;

        /* A store the service does not manage has no host reading: null, never a made-up figure. */
        Assert.Null(DarlingWorker.ReadStoreHostLoad(managedStore: false));

        var managed = DarlingWorker.ReadStoreHostLoad(managedStore: true);
        Assert.NotNull(managed);
        Assert.True(managed!.Value.AvailableMemoryBytes > 0);
        Assert.Null(managed.Value.CpuPercent);
        Assert.Null(managed.Value.DiskQueueLength);

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var admin = new NpgsqlConnection(scratch.ConnectionString);
            await admin.OpenAsync(ct);
            await PgMigrations.MigrateAsync(admin, ct);
            await StoreConnectionSamples.CaptureAsync(admin, DateTime.UtcNow, managed, ct);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var answer = await ReadAsync(source, ct);
            Assert.Equal("available", answer.GetProperty("host_load").GetString());
            var host = answer.GetProperty("minutes")[0].GetProperty("host");
            Assert.True(host.GetProperty("available_memory_bytes").GetInt64() > 0);
            Assert.Equal(JsonValueKind.Null, host.GetProperty("cpu_percent").ValueKind);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
