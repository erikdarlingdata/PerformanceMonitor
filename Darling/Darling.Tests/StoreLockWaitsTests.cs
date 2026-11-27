/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The log line shapes the lock wait parser reads, and the session map's rule for which reading answers (#9439).</summary>
public sealed class StoreLockWaitsParserTests
{
    private static string Line(string stamp, int pid, string app, string severity, string message) =>
        $"{stamp} UTC [{pid}] {app} {severity}:  {message}\n";

    private const string Open = "2026-11-24 10:00:01.000";
    private const string Close = "2026-11-24 10:00:07.000";

    [Fact]
    public void AWaitThatIsOpenedAndGranted_IsOneWait_WithTheHoldersTheRelationAndTheDuration()
    {
        var slab =
            Line(Open, 200, "app-b", "LOG", "process 200 still waiting for AccessShareLock on relation 16385 of database 16384 after 1000.200 ms")
            + Line(Open, 200, "app-b", "DETAIL", "Process holding the lock: 100. Wait queue: 200.")
            + Line(Close, 200, "app-b", "LOG", "process 200 acquired AccessShareLock on relation 16385 of database 16384 after 6000.300 ms");

        var parsed = StoreLockWaits.Parse(slab, detailInEnglish: true);

        var wait = Assert.Single(parsed.Waits);
        Assert.Equal(200, wait.WaiterPid);
        Assert.Equal("app-b", wait.WaiterApplication);
        Assert.Equal("AccessShareLock", wait.LockMode);
        Assert.Equal(16385L, wait.RelationOid);
        Assert.Equal(new[] { 100 }, wait.HolderPids.ToArray());
        Assert.Equal(StoreLockWaits.Granted, wait.Outcome);
        Assert.Equal(6000.3, wait.ClosedAfterMs!.Value);
        Assert.Equal("app-b", parsed.ApplicationByPid[200]);
    }

    private static string Checkpoints(int count)
    {
        var text = new System.Text.StringBuilder();
        for (var i = 0; i < count; i++)
        {
            text.Append(Line(Open, 300 + i, string.Empty, "LOG", "checkpoint complete: wrote 12 buffers (0.0%); write=0.001 s, sync=0.001 s, total=0.003 s"));
        }

        return text.ToString();
    }

    [Fact]
    public void ASlabOfOrdinaryLog_HoldsNothingToWrite()
    {
        Assert.True(StoreLockWaits.Parse(Checkpoints(200), detailInEnglish: true).IsEmpty);
    }

    [Theory]
    [InlineData("LOG", "process 200 acquired ShareLock on transaction 777 after 6000.300 ms", StoreLockWaits.Granted)]
    [InlineData("LOG", "process 200 detected deadlock while waiting for ShareLock on transaction 777 after 1000.100 ms", StoreLockWaits.Deadlock)]
    [InlineData("ERROR", "canceling statement due to lock timeout", StoreLockWaits.TimedOut)]
    [InlineData("FATAL", "terminating connection due to administrator command", StoreLockWaits.Cancelled)]
    [InlineData("PANIC", "could not write to file", StoreLockWaits.Cancelled)]
    public void AnEndAmongOrdinaryLog_IsStillRead_WhateverLineEndsTheWait(string severity, string message, string outcome)
    {
        var slab = Checkpoints(200) + Line(Close, 200, "app-b", severity, message) + Checkpoints(5);

        var close = Assert.Single(StoreLockWaits.Parse(slab, detailInEnglish: true).Closes);

        Assert.Equal(200, close.Pid);
        Assert.Equal(outcome, close.Outcome);
    }

    [Fact]
    public void AWaitStillOpenAmongOrdinaryLog_IsStillRead()
    {
        var slab = Checkpoints(200)
            + Line(Open, 200, "app-b", "LOG", "process 200 still waiting for ShareLock on transaction 777 after 1000.100 ms")
            + Checkpoints(5);

        var wait = Assert.Single(StoreLockWaits.Parse(slab, detailInEnglish: true).Waits);

        Assert.Equal(StoreLockWaits.Waiting, wait.Outcome);
    }

    [Fact]
    public void ALockTimeoutError_EndsTheWait_AsTimedOut()
    {
        var slab =
            Line(Open, 200, "app-b", "LOG", "process 200 still waiting for ShareLock on transaction 777 after 1000.100 ms")
            + Line(Close, 200, "app-b", "ERROR", "canceling statement due to lock timeout");

        var wait = Assert.Single(StoreLockWaits.Parse(slab, detailInEnglish: true).Waits);

        Assert.Equal(StoreLockWaits.TimedOut, wait.Outcome);
    }

    [Fact]
    public void AWaitThatTheSlabLeavesOpen_IsWaiting_AndAnEndWithNoOpeningIsAClose()
    {
        var open = Line(Open, 200, "app-b", "LOG", "process 200 still waiting for ShareLock on transaction 777 after 1000.100 ms");
        var close = Line(Close, 300, "app-c", "LOG", "process 300 acquired ShareLock on transaction 888 after 9000.000 ms");

        var parsed = StoreLockWaits.Parse(open + close, detailInEnglish: true);

        Assert.Equal(StoreLockWaits.Waiting, Assert.Single(parsed.Waits).Outcome);
        var end = Assert.Single(parsed.Closes);
        Assert.Equal(300, end.Pid);
        Assert.Equal(StoreLockWaits.Granted, end.Outcome);
    }

    [Fact]
    public void ALogThatIsNotInEnglish_YieldsNothing()
    {
        var slab = Line(Open, 200, "app-b", "LOG", "process 200 still waiting for ShareLock on transaction 777 after 1000.100 ms");

        Assert.True(StoreLockWaits.Parse(slab, detailInEnglish: false).IsEmpty);
    }

    [Theory]
    [InlineData("")]
    [InlineData("[unknown]")]
    public void ATimeWithNoApplicationInThePrefix_ReadsAsAnEmptyApplication_InEitherForm(string app)
    {
        var slab = Line(Open, 200, app, "LOG", "process 200 still waiting for ShareLock on transaction 777 after 1000.100 ms");

        var parsed = StoreLockWaits.Parse(slab, detailInEnglish: true);

        Assert.Equal(string.Empty, Assert.Single(parsed.Waits).WaiterApplication);
        Assert.Equal(string.Empty, parsed.ApplicationByPid[200]);
    }

    [Fact]
    public void AReadingAnswersForAWaitThatBeganWhileTheSessionLived_AndForOneThatEndedWithinTheSlack_ButNotLater()
    {
        var map = new StoreLockWaits.PidMap();
        var started = new DateTime(2026, 11, 24, 10, 0, 0, DateTimeKind.Unspecified);
        map.Add(100, new StoreLockWaits.PidMap.Entry(started, started.AddMinutes(5), "holder", "app-a"));

        Assert.Equal("holder", map.Resolve(100, started.AddMinutes(3))!.Value.Role);
        Assert.Equal("holder", map.Resolve(100, started.AddMinutes(5) + StoreLockWaits.PidMapResolveSlack)!.Value.Role);
        Assert.Null(map.Resolve(100, started.AddMinutes(5) + StoreLockWaits.PidMapResolveSlack + TimeSpan.FromSeconds(1)));
        Assert.Null(map.Resolve(100, started.AddSeconds(-1)));
        Assert.Null(map.Resolve(101, started.AddMinutes(3)));
    }

    [Fact]
    public void WhenTwoSessionsHadTheSameId_TheOneThatStartedLastBeforeTheWaitAnswers()
    {
        var map = new StoreLockWaits.PidMap();
        var first = new DateTime(2026, 11, 24, 10, 0, 0, DateTimeKind.Unspecified);
        var second = first.AddMinutes(1);
        map.Add(100, new StoreLockWaits.PidMap.Entry(first, first.AddSeconds(50), "old", "app-old"));
        map.Add(100, new StoreLockWaits.PidMap.Entry(second, second.AddMinutes(5), "new", "app-new"));

        Assert.Equal("new", map.Resolve(100, second.AddSeconds(5))!.Value.Role);
        Assert.Equal("old", map.Resolve(100, first.AddSeconds(30))!.Value.Role);
    }
}

/// <summary>
/// A lock wait from two real sessions, read through the store log sweep's file seam (#9439). A scratch store keeps no log file, so the
/// log lines are built from the sessions' real process ids and the relation's real oid.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreLockWaitsLiveTests
{
    private const string LogName = "postgresql-Tue.log";
    private const string WaiterRole = "lw9439_waiter";
    private const string HolderRole = "lw9439_holder";
    private const string Password = "Lw9439-test-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class FixedLog(string text) : StoreLogSweep.IStoreLogFiles
    {
        private readonly byte[] _bytes = Encoding.UTF8.GetBytes(text);

        public Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string Name, long Size)>>([(LogName, _bytes.Length)]);

        public Task<byte[]> ReadAsync(string name, long offset, int length, CancellationToken cancellationToken) =>
            Task.FromResult(_bytes.AsSpan((int)offset, (int)Math.Min(length, _bytes.Length - offset)).ToArray());
    }

    /// <summary>Several log files, oldest first, each a fixed set of bytes. The sweep's listing order is the order given here.</summary>
    private sealed class FixedLogs(params (string Name, byte[] Bytes)[] files) : StoreLogSweep.IStoreLogFiles
    {
        public Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string Name, long Size)>>(files.Select(f => (f.Name, (long)f.Bytes.Length)).ToList());

        public Task<byte[]> ReadAsync(string name, long offset, int length, CancellationToken cancellationToken)
        {
            var bytes = files.First(f => f.Name == name).Bytes;
            return Task.FromResult(bytes.AsSpan((int)offset, (int)Math.Min(length, bytes.Length - offset)).ToArray());
        }
    }

    private sealed record Scenario(
        NpgsqlConnection Owner, int HolderPid, int WaiterPid, long RelationOid, long DatabaseOid, DateTime SeenUtc,
        StoreLockWaits.PidMap Map);

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

    private static async Task<NpgsqlConnection> OpenAsRoleAsync(string baseConnectionString, string role, string application, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Username = role,
            Password = Password,
            ApplicationName = application,
            Pooling = false,
        };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    /// <summary>
    /// Two sessions under two roles and two application names: the holder takes an exclusive lock, the waiter blocks on it, and the map
    /// reads the sessions while both are alive. When <paramref name="holderEndsBeforeTheSweep"/> the holder ends after that read.
    /// </summary>
    private static async Task WithScenarioAsync(bool holderEndsBeforeTheSweep, Func<Scenario, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9439 lock wait tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        foreach (var role in new[] { WaiterRole, HolderRole })
        {
            await ExecAsync(owner, $"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{Password}'; END IF; END $$", ct);
        }

        await ExecAsync(owner, "CREATE TABLE public.lw_target (id int)", ct);
        await ExecAsync(owner, $"GRANT ALL ON public.lw_target TO {WaiterRole}, {HolderRole}", ct);

        await using var holder = await OpenAsRoleAsync(scratch.ConnectionString, HolderRole, "lw-holder-app", ct);
        await using var waiter = await OpenAsRoleAsync(scratch.ConnectionString, WaiterRole, "lw-waiter-app", ct);
        var holderPid = await ScalarAsync<int>(holder, "SELECT pg_backend_pid()", ct);
        var waiterPid = await ScalarAsync<int>(waiter, "SELECT pg_backend_pid()", ct);

        await ExecAsync(holder, "BEGIN", ct);
        await ExecAsync(holder, "LOCK TABLE public.lw_target IN ACCESS EXCLUSIVE MODE", ct);
        var blocked = ExecAsync(waiter, "SELECT count(*) FROM public.lw_target", ct);

        for (var i = 0; i < 100; i++)
        {
            var waiting = await ScalarAsync<long>(owner, $"SELECT count(*) FROM pg_stat_activity WHERE pid = {waiterPid} AND wait_event_type = 'Lock'", ct);
            if (waiting > 0)
            {
                break;
            }

            await Task.Delay(100, ct);
        }

        await Task.Delay(1500, ct);
        var map = new StoreLockWaits.PidMap();
        await map.RefreshAsync(owner, ct);
        var seen = await ScalarAsync<DateTime>(owner, "SELECT (clock_timestamp() AT TIME ZONE 'UTC')", ct);

        await ExecAsync(holder, "COMMIT", ct);
        await blocked;
        if (holderEndsBeforeTheSweep)
        {
            await holder.CloseAsync();
        }

        var relationOid = await ScalarAsync<long>(owner, "SELECT 'public.lw_target'::regclass::oid::bigint", ct);
        var databaseOid = await ScalarAsync<long>(owner, "SELECT oid::bigint FROM pg_database WHERE datname = current_database()", ct);
        await body(new Scenario(owner, holderPid, waiterPid, relationOid, databaseOid, seen, map), ct);
    }

    /// <summary>The time as the store's log writes it: in <c>log_timezone</c>, with milliseconds.</summary>
    private static Task<string> LogStampAsync(NpgsqlConnection owner, DateTime utc, CancellationToken ct) =>
        ScalarAsync<string>(owner, string.Create(CultureInfo.InvariantCulture,
            $"SELECT to_char(('{utc:yyyy-MM-dd HH:mm:ss.fff}'::timestamp AT TIME ZONE 'UTC') AT TIME ZONE current_setting('log_timezone'), 'YYYY-MM-DD HH24:MI:SS.MS')"), ct);

    private static string Line(string stamp, int pid, string app, string severity, string message) =>
        $"{stamp} UTC [{pid}] {app} {severity}:  {message}\n";

    /// <summary>The log of one wait that began a second before the map's reading, with the holder pid the log names.</summary>
    private static async Task<string> WaitLogAsync(Scenario s, int holderPid, double durationMs, bool timedOut, CancellationToken ct, string waiterApp = "lw-waiter-app")
    {
        var started = s.SeenUtc.AddSeconds(-1);
        var opened = await LogStampAsync(s.Owner, started.AddMilliseconds(1000.5), ct);
        var closed = await LogStampAsync(s.Owner, started.AddMilliseconds(durationMs), ct);
        var obj = $"relation {s.RelationOid} of database {s.DatabaseOid}";
        var log =
            Line(opened, s.WaiterPid, waiterApp, "LOG", $"process {s.WaiterPid} still waiting for AccessShareLock on {obj} after 1000.500 ms")
            + Line(opened, s.WaiterPid, waiterApp, "DETAIL", $"Process holding the lock: {holderPid}. Wait queue: {s.WaiterPid}.");
        return log + (timedOut
            ? Line(closed, s.WaiterPid, waiterApp, "ERROR", "canceling statement due to lock timeout")
            : Line(closed, s.WaiterPid, waiterApp, "LOG", $"process {s.WaiterPid} acquired AccessShareLock on {obj} after {durationMs.ToString("0.000", CultureInfo.InvariantCulture)} ms"));
    }

    /// <summary>The opening lines of the scenario's wait: a first <c>still waiting</c> at 1000.5 ms, one second after the wait began.</summary>
    private static async Task<string> OpeningLinesAsync(Scenario s, CancellationToken ct)
    {
        var started = s.SeenUtc.AddSeconds(-1);
        var stamp = await LogStampAsync(s.Owner, started.AddMilliseconds(1000.5), ct);
        return Line(stamp, s.WaiterPid, "lw-waiter-app", "LOG", $"process {s.WaiterPid} still waiting for AccessShareLock on relation {s.RelationOid} of database {s.DatabaseOid} after 1000.500 ms")
            + Line(stamp, s.WaiterPid, "lw-waiter-app", "DETAIL", $"Process holding the lock: {s.HolderPid}. Wait queue: {s.WaiterPid}.");
    }

    /// <summary>A line from the scenario's waiter at an offset from the wait's start.</summary>
    private static async Task<string> WaiterLineAsync(Scenario s, double afterStartMs, string message, CancellationToken ct) =>
        Line(await LogStampAsync(s.Owner, s.SeenUtc.AddSeconds(-1).AddMilliseconds(afterStartMs), ct), s.WaiterPid, "lw-waiter-app", "LOG", message);

    private static string Acquired(Scenario s, double ms) =>
        $"process {s.WaiterPid} acquired AccessShareLock on relation {s.RelationOid} of database {s.DatabaseOid} after {ms.ToString("0.000", CultureInfo.InvariantCulture)} ms";

    private static string StillWaiting(Scenario s, double ms) =>
        $"process {s.WaiterPid} still waiting for AccessShareLock on relation {s.RelationOid} of database {s.DatabaseOid} after {ms.ToString("0.000", CultureInfo.InvariantCulture)} ms";

    /// <summary>Filler the classifier and the lock wait parser both skip: tab-led continuation lines with no entry before them. Past one 4 MiB slab.</summary>
    private static string Padding()
    {
        var line = "\t" + new string('p', 98) + "\n";
        return string.Concat(Enumerable.Repeat(line, (StoreLogSlab.MaxBytesPerRead / line.Length) + 2000));
    }

    private sealed record Raw(string Outcome, long DurationMs, DateTime StartedUtc);

    private static async Task<List<Raw>> RawRowsAsync(Scenario s, CancellationToken ct)
    {
        var rows = new List<Raw>();
        await using var command = new NpgsqlCommand("SELECT outcome, duration_ms, wait_started_at FROM collect.store_lock_waits ORDER BY wait_id", s.Owner);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Raw(reader.GetString(0), reader.GetInt64(1), reader.GetDateTime(2)));
        }

        return rows;
    }

    private sealed record Row(string WaiterRole, string WaiterApplication, string[] HolderRoles, string[] HolderApplications, string LockMode, string? Relation, long DurationMs, string Outcome);

    private static async Task<List<Row>> SweepAndReadAsync(Scenario s, string log, CancellationToken ct)
    {
        await StoreLogSweep.SweepAsync(s.Owner, DateTime.UtcNow, null, new FixedLog(log), s.Map, ct);
        var rows = new List<Row>();
        await using var command = new NpgsqlCommand(
            "SELECT waiter_role, waiter_application, holder_roles, holder_applications, lock_mode, relation_name, duration_ms, outcome FROM collect.store_lock_waits ORDER BY wait_id", s.Owner);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Row(reader.GetString(0), reader.GetString(1), reader.GetFieldValue<string[]>(2), reader.GetFieldValue<string[]>(3),
                reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetInt64(6), reader.GetString(7)));
        }

        return rows;
    }

    [Fact]
    public async Task AWaitPastTheThreshold_NamesBothRolesAndApplications_TheRelationTheModeAndTheDuration()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            var row = Assert.Single(await SweepAndReadAsync(s, await WaitLogAsync(s, s.HolderPid, 6200, timedOut: false, ct), ct));

            Assert.Equal(WaiterRole, row.WaiterRole);
            Assert.Equal("lw-waiter-app", row.WaiterApplication);
            Assert.Equal(new[] { HolderRole }, row.HolderRoles);
            Assert.Equal(new[] { "lw-holder-app" }, row.HolderApplications);
            Assert.Equal("public.lw_target", row.Relation);
            Assert.Equal("AccessShareLock", row.LockMode);
            Assert.True(row.DurationMs >= StoreLockWaits.ThresholdMilliseconds);
            Assert.Equal(StoreLockWaits.Granted, row.Outcome);
        });
    }

    [Fact]
    public async Task ALockTimeoutEnding_IsRecordedAsTimedOut()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            var row = Assert.Single(await SweepAndReadAsync(s, await WaitLogAsync(s, s.HolderPid, 6000, timedOut: true, ct), ct));

            Assert.Equal(StoreLockWaits.TimedOut, row.Outcome);
            Assert.True(row.DurationMs >= StoreLockWaits.ThresholdMilliseconds);
        });
    }

    [Fact]
    public async Task AWaitUnderTheThreshold_MakesNoRow()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            Assert.Empty(await SweepAndReadAsync(s, await WaitLogAsync(s, s.HolderPid, 3200, timedOut: false, ct), ct));
        });
    }

    [Fact]
    public async Task AHolderIdTheMapNeverSaw_IsUnresolved_AndTheWaiterStillResolves()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            var row = Assert.Single(await SweepAndReadAsync(s, await WaitLogAsync(s, 2_000_000_000, 6200, timedOut: false, ct), ct));

            Assert.Equal(new[] { StoreLockWaits.Unresolved }, row.HolderRoles);
            Assert.Equal(new[] { StoreLockWaits.Unresolved }, row.HolderApplications);
            Assert.Equal(WaiterRole, row.WaiterRole);
        });
    }

    [Fact]
    public async Task AHolderThatEndedBeforeTheSweep_ButWasSeenByAnEarlierMinuteRead_StillResolves()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: true, async (s, ct) =>
        {
            var row = Assert.Single(await SweepAndReadAsync(s, await WaitLogAsync(s, s.HolderPid, 6200, timedOut: false, ct), ct));

            Assert.Equal(new[] { HolderRole }, row.HolderRoles);
            Assert.Equal(new[] { "lw-holder-app" }, row.HolderApplications);
        });
    }

    [Fact]
    public async Task ABackgroundProcessThatHoldsALock_ResolvesToItsBackendType_NotUnresolved()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            /* A process that lives as long as the server, so it is the same process at the map's read and at the sweep. A short-lived
               background process (a parallel worker of another test's query) can start after the scenario read the map, and the map
               then rightly reads it unresolved. The map is read again here, as the store pass reads it once a minute. */
            await using var command = new NpgsqlCommand(
                "SELECT pid, COALESCE(usename::text, backend_type) FROM pg_stat_activity WHERE backend_type IN ('checkpointer', 'background writer', 'walwriter', 'autovacuum launcher') AND pid IS NOT NULL AND backend_start IS NOT NULL ORDER BY pid LIMIT 1", s.Owner);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), "the store shows no checkpointer, background writer, walwriter or autovacuum launcher to hold a lock");
            var (backgroundPid, expectedRole) = (reader.GetInt32(0), reader.GetString(1));
            await reader.CloseAsync();
            await s.Map.RefreshAsync(s.Owner, ct);

            var row = Assert.Single(await SweepAndReadAsync(s, await WaitLogAsync(s, backgroundPid, 6200, timedOut: false, ct), ct));

            Assert.Equal(new[] { expectedRole }, row.HolderRoles);
            Assert.NotEqual(StoreLockWaits.Unresolved, row.HolderRoles[0]);
        });
    }

    [Fact]
    public async Task AnEmptyApplicationInTheLogPrefix_TakesTheApplicationTheMapHas()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            var row = Assert.Single(await SweepAndReadAsync(s, await WaitLogAsync(s, s.HolderPid, 6200, timedOut: false, ct, waiterApp: string.Empty), ct));

            Assert.Equal("lw-waiter-app", row.WaiterApplication);
        });
    }

    [Fact]
    public async Task AWaitThatOpensInAnOlderFilesLaterSlab_AndEndsInTheNewerFile_IsClosedByTheSameSweep()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            var older = Padding() + await OpeningLinesAsync(s, ct);
            var newer = await WaiterLineAsync(s, 6200, Acquired(s, 6200), ct);
            Assert.True(Encoding.UTF8.GetByteCount(older) > StoreLogSlab.MaxBytesPerRead, "the opening must sit past the older file's first slab");

            await StoreLogSweep.SweepAsync(s.Owner, DateTime.UtcNow, null,
                new FixedLogs(("postgresql-Mon.log", Encoding.UTF8.GetBytes(older)), ("postgresql-Tue.log", Encoding.UTF8.GetBytes(newer))), s.Map, ct);

            var row = Assert.Single(await RawRowsAsync(s, ct));
            Assert.Equal(StoreLockWaits.Granted, row.Outcome);
            Assert.Equal(6200, row.DurationMs);
        });
    }

    [Fact]
    public async Task ARepeatedStillWaitingThatStartsASlab_ClosesTheRowAnEarlierSlabLeftOpen_InsteadOfAddingASecond()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            var first = await OpeningLinesAsync(s, ct);
            await StoreLogSweep.SweepAsync(s.Owner, DateTime.UtcNow, null, new FixedLog(first), s.Map, ct);
            Assert.Equal(StoreLockWaits.Waiting, Assert.Single(await RawRowsAsync(s, ct)).Outcome);

            var second = await WaiterLineAsync(s, 5600, StillWaiting(s, 5600), ct) + await WaiterLineAsync(s, 6200, Acquired(s, 6200), ct);
            await StoreLogSweep.SweepAsync(s.Owner, DateTime.UtcNow, null, new FixedLog(first + second), s.Map, ct);

            var row = Assert.Single(await RawRowsAsync(s, ct));
            Assert.Equal(StoreLockWaits.Granted, row.Outcome);
            Assert.Equal(6200, row.DurationMs);
        });
    }

    [Fact]
    public async Task ANewWaitOfTheSameProcessOnTheSameLock_IsRecordedBesideTheOlderWaitAnEarlierSlabLeftOpen()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            var first = await OpeningLinesAsync(s, ct);
            await StoreLogSweep.SweepAsync(s.Owner, DateTime.UtcNow, null, new FixedLog(first), s.Map, ct);
            Assert.Equal(StoreLockWaits.Waiting, Assert.Single(await RawRowsAsync(s, ct)).Outcome);

            /* The next slab opens on the end of the older wait (its opening is in the earlier slab), and then holds a whole new wait of the same process on the same lock. */
            var second = await WaiterLineAsync(s, 9000, Acquired(s, 9000), ct)
                + await WaiterLineAsync(s, 10000, StillWaiting(s, 1000), ct)
                + await WaiterLineAsync(s, 16000, Acquired(s, 7000), ct);
            await StoreLogSweep.SweepAsync(s.Owner, DateTime.UtcNow, null, new FixedLog(first + second), s.Map, ct);

            var rows = await RawRowsAsync(s, ct);
            Assert.Equal(2, rows.Count);
            Assert.All(rows, r => Assert.Equal(StoreLockWaits.Granted, r.Outcome));
            Assert.Equal(9000, rows[0].DurationMs);
            Assert.Equal(7000, rows[1].DurationMs);
            Assert.InRange((rows[1].StartedUtc - rows[0].StartedUtc).TotalMilliseconds, 8990, 9010);
        });
    }

    [Fact]
    public async Task AnOpenRowOlderThanADay_IsNotClosedByALaterEndOfTheSameProcessId()
    {
        await WithScenarioAsync(holderEndsBeforeTheSweep: false, async (s, ct) =>
        {
            await ExecAsync(s.Owner, string.Create(CultureInfo.InvariantCulture,
                $"INSERT INTO collect.store_lock_waits (wait_started_at, captured_at, waiter_pid, waiter_role, waiter_application, holder_pids, holder_roles, holder_applications, lock_mode, lock_object, relation_name, duration_ms, outcome) VALUES ('{s.SeenUtc.AddDays(-3):yyyy-MM-dd HH:mm:ss}', '{s.SeenUtc.AddDays(-3):yyyy-MM-dd HH:mm:ss}', {s.WaiterPid}, 'r', 'a', '{{1}}', '{{r}}', '{{a}}', 'ShareLock', 'transaction 1', NULL, 1000, 'waiting')"), ct);

            await StoreLogSweep.SweepAsync(s.Owner, DateTime.UtcNow, null, new FixedLog(await WaiterLineAsync(s, 6200, Acquired(s, 6200), ct)), s.Map, ct);

            var row = Assert.Single(await RawRowsAsync(s, ct));
            Assert.Equal(StoreLockWaits.Waiting, row.Outcome);
            Assert.Equal(1000, row.DurationMs);
        });
    }

    private static string StatusOf(string json) => JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    [Fact]
    public async Task AStoreThatLogsInAnotherLanguage_AnswersPrecondition_AndAnEnglishOneThatWasCapturedAnswersEmpty()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9439 lock wait tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        await ExecAsync(owner, $"ALTER DATABASE \"{scratch.DatabaseName}\" SET log_lock_waits = on", ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var german = await DarlingMcpStoreLockWaitTools.GetStoreLockWaits(dataSource, 24, 50, null, null, null, lcMessagesOverride: "de_DE.UTF-8", ct);
        Assert.Equal("precondition", StatusOf(german));
        Assert.Contains("de_DE.UTF-8", german);

        Assert.Equal("precondition", StatusOf(await DarlingMcpStoreLockWaitTools.GetStoreLockWaits(dataSource, 24, 50, null, null, null, lcMessagesOverride: string.Empty, ct)));

        /* One sweep over a log that holds no lock wait: the store log was read in the window, so an empty answer is an observation. */
        var quiet = Line("2026-01-01 00:00:00.000", 1, "app", "LOG", "connection received: host=[local]");
        await StoreLogSweep.SweepAsync(owner, DateTime.UtcNow, null, new FixedLog(quiet), new StoreLockWaits.PidMap(), ct);
        Assert.Equal("empty", StatusOf(await DarlingMcpStoreLockWaitTools.GetStoreLockWaits(dataSource, 24, 50, null, null, null, lcMessagesOverride: "en_US.UTF-8", ct)));
    }

    [Fact]
    public async Task AStoreWhoseLogWasNeverCaptured_AnswersUnavailable_NotEmpty_AndPointsToGetStoreLog()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9439 lock wait tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        await ExecAsync(owner, $"ALTER DATABASE \"{scratch.DatabaseName}\" SET log_lock_waits = on", ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var answer = await DarlingMcpStoreLockWaitTools.GetStoreLockWaits(dataSource, 24, 50, null, null, null, lcMessagesOverride: "en_US.UTF-8", ct);

        Assert.Equal("unavailable", StatusOf(answer));
        Assert.Contains("get_store_log", answer);
    }
}
