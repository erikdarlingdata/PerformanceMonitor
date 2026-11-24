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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9414: the checkpoint lines of PostgreSQL 14 to 18 (a time start, a WAL start, a shutdown, an end-of-recovery, a restartpoint) read into figures,
/// and a line that does not read is kept as unparsed rather than guessed. The live class below runs the same lines through the sweep and the read.
/// </summary>
public sealed class StoreCheckpoints9414Tests
{
    private const string Pg16Complete =
        "2026-11-24 10:05:31.412 UTC [88] LOG:  checkpoint complete: wrote 1204 buffers (7.3%); 0 WAL file(s) added, 1 removed, 3 recycled; write=29.931 s, sync=0.012 s, total=29.987 s; sync files=212, longest=0.004 s, average=0.001 s; distance=48211 kB, estimate=51337 kB; lsn=0/5A3F2C8, redo lsn=0/5A3E000";

    private const string Pg18Complete =
        "2026-11-24 10:10:31.001 UTC [88] LOG:  checkpoint complete: wrote 9 buffers (0.1%), wrote 3 SLRU buffers; 2 WAL file(s) added, 0 removed, 0 recycled; write=0.801 s, sync=1.250 s, total=2.100 s; sync files=7, longest=1.002 s, average=0.178 s; distance=16384 kB, estimate=20000 kB; lsn=1/0, redo lsn=0/FFFFFF8";

    private static StoreCheckpoints.Line Only(string log) => Assert.Single(StoreCheckpoints.Parse(log).Lines);

    [Fact]
    public void Pg16CompleteLine_GivesEveryFigure()
    {
        var line = Only(Pg16Complete);
        Assert.False(line.IsStart);
        Assert.Equal("checkpoint", line.Kind);
        Assert.Equal(new DateTime(2026, 11, 24, 10, 5, 31, 412), line.Stamp);
        var f = line.Figures!;
        Assert.Equal(1204, f.BuffersWritten);
        Assert.Equal(7.3f, f.BuffersPct);
        Assert.Null(f.SlruBuffersWritten);
        Assert.Equal((0, 1, 3), (f.WalFilesAdded, f.WalFilesRemoved, f.WalFilesRecycled));
        Assert.Equal((29931L, 12L, 29987L), (f.WriteMs, f.SyncMs, f.TotalMs));
        Assert.Equal(212, f.FilesSynced);
        Assert.Equal((4L, 1L), (f.LongestSyncMs, f.AverageSyncMs));
        Assert.Equal((48211L, 51337L), (f.DistanceKb, f.EstimateKb));
        Assert.Equal(("0/5A3F2C8", "0/5A3E000"), (f.Lsn, f.RedoLsn));
        Assert.False(line.Unparsed);
    }

    [Fact]
    public void Pg17And18CompleteLine_GivesTheSlruBuffersToo()
    {
        var f = Only(Pg18Complete).Figures!;
        Assert.Equal(9, f.BuffersWritten);
        Assert.Equal(3, f.SlruBuffersWritten);
        Assert.Equal(2, f.WalFilesAdded);
        Assert.Equal((801L, 1250L, 2100L), (f.WriteMs, f.SyncMs, f.TotalMs));
        Assert.Equal((7, 1002L, 178L), (f.FilesSynced, f.LongestSyncMs, f.AverageSyncMs));
        Assert.Equal("1/0", f.Lsn);
    }

    [Fact]
    public void RestartpointAndAnOlderLineWithNoLsn_ReadToo()
    {
        var older = "2026-11-24 10:00:00.000 UTC [1] LOG:  restartpoint complete: wrote 5 buffers (0.0%); 0 WAL file(s) added, 0 removed, 1 recycled; write=0.100 s, sync=0.000 s, total=0.101 s; sync files=1, longest=0.000 s, average=0.000 s; distance=1 kB, estimate=2 kB";
        var line = Only(older);
        Assert.Equal("restartpoint", line.Kind);
        Assert.Null(line.Figures!.Lsn);
        Assert.Equal(101L, line.Figures.TotalMs);
    }

    /// <summary>The complete line each major writes, built from <c>LogCheckpointEnd</c> in that major's <c>xlog.c</c> (the REL_14 to REL_18 stable branches).
    /// 14 and 15 end after the estimate, 16 and 17 add the two LSNs, 18 adds the SLRU buffer count after the buffer figure.</summary>
    private static string CompleteLineOfMajor(int major, string kind)
    {
        var buffers = major >= 18 ? "wrote 1204 buffers (7.3%), wrote 3 SLRU buffers; " : "wrote 1204 buffers (7.3%); ";
        var tail = major >= 16 ? "; lsn=0/5A3F2C8, redo lsn=0/5A3E000" : string.Empty;
        return $"2026-11-24 10:05:31.412 UTC [88] LOG:  {kind} complete: {buffers}0 WAL file(s) added, 1 removed, 3 recycled; "
            + "write=29.931 s, sync=0.012 s, total=29.987 s; sync files=212, longest=0.004 s, average=0.001 s; "
            + "distance=48211 kB, estimate=51337 kB" + tail;
    }

    [Theory]
    [InlineData(14, "checkpoint")]
    [InlineData(14, "restartpoint")]
    [InlineData(15, "checkpoint")]
    [InlineData(15, "restartpoint")]
    [InlineData(16, "checkpoint")]
    [InlineData(16, "restartpoint")]
    [InlineData(17, "checkpoint")]
    [InlineData(17, "restartpoint")]
    [InlineData(18, "checkpoint")]
    [InlineData(18, "restartpoint")]
    public void TheCompleteLineOfEveryMajor_ReadsIntoItsFigures(int major, string kind)
    {
        var line = Only(CompleteLineOfMajor(major, kind));
        Assert.Equal(kind, line.Kind);
        Assert.False(line.Unparsed);
        var f = line.Figures!;
        Assert.Equal(1204, f.BuffersWritten);
        Assert.Equal(7.3f, f.BuffersPct);
        Assert.Equal(major >= 18 ? 3 : null, f.SlruBuffersWritten);
        Assert.Equal((0, 1, 3), (f.WalFilesAdded, f.WalFilesRemoved, f.WalFilesRecycled));
        Assert.Equal((29931L, 12L, 29987L), (f.WriteMs, f.SyncMs, f.TotalMs));
        Assert.Equal((212, 4L, 1L), (f.FilesSynced, f.LongestSyncMs, f.AverageSyncMs));
        Assert.Equal((48211L, 51337L), (f.DistanceKb, f.EstimateKb));
        Assert.Equal(major >= 16 ? "0/5A3F2C8" : null, f.Lsn);
        Assert.Equal(major >= 16 ? "0/5A3E000" : null, f.RedoLsn);
    }

    [Theory]
    [InlineData("checkpoint")]
    [InlineData("restartpoint")]
    public void TheStartLine_IsTheSameOnEveryMajorAndKeepsItsFlagWords(string kind)
    {
        // The flag words and their order are the same in the 14 to 18 sources (shutdown, end-of-recovery, immediate, force, wait, wal, time, flush-all).
        var line = Only($"2026-11-24 10:05:01.000 UTC [88] LOG:  {kind} starting: immediate force wait wal flush-all");
        Assert.True(line.IsStart);
        Assert.Equal(kind, line.Kind);
        Assert.Equal("wal", line.Reason);
        Assert.Equal("immediate force wait wal flush-all", line.Flags);
    }

    [Theory]
    [InlineData("time", "time")]
    [InlineData("wal", "wal")]
    [InlineData("shutdown immediate", "shutdown")]
    [InlineData("end-of-recovery immediate wait", "end-of-recovery")]
    [InlineData("immediate force wait", "immediate")]
    [InlineData("force wait", "requested")]
    [InlineData("", "requested")]
    public void StartLine_GivesItsReason(string flags, string reason)
    {
        var line = Only($"2026-11-24 10:05:01.000 UTC [88] LOG:  checkpoint starting:{(flags.Length == 0 ? string.Empty : " " + flags)}");
        Assert.True(line.IsStart);
        Assert.Equal(reason, line.Reason);
    }

    [Fact]
    public void ALineItCannotReadIsKeptUnparsedAndOneWithNoStampIsCounted()
    {
        var parsed = StoreCheckpoints.Parse(
            "2026-11-24 10:05:31.412 UTC [88] LOG:  checkpoint complete: wrote lots of buffers\n"
            + "[88] LOG:  checkpoint complete: wrote 1 buffers (0.0%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.000 s, sync=0.000 s, total=0.000 s; sync files=0, longest=0.000 s, average=0.000 s; distance=0 kB, estimate=0 kB\n"
            + "2026-11-24 10:05:32.000 UTC [88] LOG:  checkpoint complete: ein Checkpoint ist fertig\n"
            + "2026-11-24 10:05:33.000 UTC [88] LOG:  statement: SELECT 'checkpoint complete: 1'\n");

        Assert.Equal(1, parsed.NoStamp);
        Assert.Equal(2, parsed.Lines.Count);
        Assert.All(parsed.Lines, l => Assert.True(l.Unparsed && l.Figures is null));
    }

    [Fact]
    public void ASlabHoldsItsNewestLinesOnly()
    {
        var log = new StringBuilder();
        for (var i = 0; i < StoreCheckpoints.MaxLinesPerSlab + 5; i++)
        {
            log.Append(string.Create(System.Globalization.CultureInfo.InvariantCulture, $"2026-11-24 10:{i / 60 % 60:00}:{i % 60:00}.000 UTC [88] LOG:  checkpoint starting: time\n"));
        }

        var parsed = StoreCheckpoints.Parse(log.ToString());
        Assert.Equal(StoreCheckpoints.MaxLinesPerSlab, parsed.Lines.Count);
        Assert.Equal(5, parsed.Dropped);
    }
}

/// <summary>
/// #9414: a log with a shutdown checkpoint, a time one and a WAL one goes through the sweep and comes back through the read, paired and in order.
/// The read's four empty answers (precondition, not_collected, empty, unavailable) are each reached on a store of their own.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreCheckpoints9414LiveTests
{
    internal sealed class FixedLogFiles(string name, byte[] bytes) : StoreLogSweep.IStoreLogFiles
    {
        public Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string Name, long Size)>>([(name, bytes.Length)]);

        public Task<byte[]> ReadAsync(string file, long offset, int length, CancellationToken cancellationToken) =>
            Task.FromResult(bytes.AsSpan((int)offset, (int)Math.Min(length, bytes.Length - offset)).ToArray());
    }

    internal static string Complete(string stamp, int buffers, string total, int files) =>
        $"{stamp} UTC [88] LOG:  checkpoint complete: wrote {buffers} buffers (1.0%); 0 WAL file(s) added, 0 removed, 2 recycled; write=1.000 s, sync=2.000 s, total={total} s; sync files={files}, longest=1.500 s, average=0.500 s; distance=100 kB, estimate=200 kB; lsn=0/1000, redo lsn=0/900\n";

    /// <summary>Runs a body against a scratch store of its own (#1776 own-store), cleaned up the way every live class here is.</summary>
    internal static async Task WithStoreAsync(bool emptyStore, Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9414 checkpoint tests.");
        var ct = TestContext.Current.CancellationToken;
        // #1776 own-store: a scratch clone of the migrated template, on its own database.
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: emptyStore);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    internal static Task SweepAsync(NpgsqlConnection connection, string log, CancellationToken ct) =>
        StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, new FixedLogFiles("postgresql-Tue.log", Encoding.UTF8.GetBytes(log)), ct);

    internal static async Task<JsonElement> ReadAsync(
        NpgsqlDataSource postgres, int hours, DarlingStoreCheckpointReader.Settings? settings, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpStoreCheckpointTools.GetStoreCheckpoints(postgres, hours, 100, null, 0, null, ct, settings)).RootElement;

    internal static readonly DarlingStoreCheckpointReader.Settings LoggingOnEnglish = new(true, "on", "C");

    [Fact]
    public Task SweepPairsStartAndCompleteLines_AndTheReadListsThemNewestFirstWithAnHourSummary() => WithStoreAsync(false, async (connection, postgres, ct) =>
    {
        var now = DateTime.UtcNow.Date.AddHours(DateTime.UtcNow.Hour);
        string At(int minutes, int seconds) => now.AddMinutes(-minutes).AddSeconds(seconds).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + ".000";
        var log = new StringBuilder();
        log.Append($"{At(40, 0)} UTC [88] LOG:  checkpoint starting: shutdown immediate\n");
        log.Append(Complete(At(40, 3), 50, "3.000", 9));
        log.Append($"{At(30, 0)} UTC [88] LOG:  checkpoint starting: time\n");
        log.Append(Complete(At(30, 30), 900, "30.000", 300));
        log.Append($"{At(20, 0)} UTC [88] LOG:  checkpoint starting: wal\n");
        log.Append(Complete(At(20, 10), 400, "10.000", 40));
        log.Append(Complete(At(10, 5), 7, "5.000", 1));

        await SweepAsync(connection, log.ToString(), ct);

        var json = await ReadAsync(postgres, 24, null, ct);
        var rows = json.GetProperty("checkpoints").EnumerateArray().ToList();
        Assert.Equal(4, rows.Count);
        Assert.Equal(new[] { 1L, 40L, 300L, 9L }, rows.Select(r => (long)r.GetProperty("files_synced").GetInt32()).ToArray());
        Assert.Equal(new[] { "none", "wal", "time", "shutdown" }, rows.Select(r => r.GetProperty("reason").ValueKind == JsonValueKind.Null ? "none" : r.GetProperty("reason").GetString()!).ToArray());
        Assert.Equal(7, rows[0].GetProperty("buffers_written").GetInt32());

        var hours = json.GetProperty("hours").EnumerateArray().ToList();
        Assert.Equal(4L, hours.Sum(h => h.GetProperty("checkpoints").GetInt64()));
        Assert.Equal(300, hours.Max(h => h.GetProperty("max_files_synced").GetInt32()));
    });

    /// <summary>The log sweep sees only the lines written so far, and a sweep a capture later sees the next ones. Each call hands the sweep the log as it stands, from the start.</summary>
    internal static Task SweepLaterAsync(NpgsqlConnection connection, string logSoFar, int capturesAgo, CancellationToken ct) =>
        StoreLogSweep.SweepAsync(connection, DateTime.UtcNow.AddMinutes(-capturesAgo), null, new FixedLogFiles("postgresql-Tue.log", Encoding.UTF8.GetBytes(logSoFar)), ct);

    [Fact]
    public Task ACheckpointStillRunningIsNotCounted_UntilALaterOneOfTheSameKindStarts() => WithStoreAsync(false, async (connection, postgres, ct) =>
    {
        var now = DateTime.UtcNow.Date.AddHours(DateTime.UtcNow.Hour);
        string At(int minutes, int seconds) => now.AddMinutes(-minutes).AddSeconds(seconds).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + ".000";
        var log = new StringBuilder();
        log.Append($"{At(40, 0)} UTC [88] LOG:  checkpoint starting: time\n");
        log.Append(Complete(At(40, 30), 900, "30.000", 300));
        log.Append($"{At(30, 0)} UTC [88] LOG:  checkpoint starting: time\n");

        // The newest checkpoint's start line is read before its complete line: it is still running, not lost.
        await SweepLaterAsync(connection, log.ToString(), 3, ct);
        var json = await ReadAsync(postgres, 24, LoggingOnEnglish, ct);
        Assert.Equal(0L, json.GetProperty("hours").EnumerateArray().Sum(h => h.GetProperty("not_completed").GetInt64()));
        Assert.Equal(JsonValueKind.Null, json.GetProperty("checkpoints")[0].GetProperty("ended_at").ValueKind);

        // A later checkpoint of the same kind started, so the open one cannot still be running.
        log.Append($"{At(20, 0)} UTC [88] LOG:  checkpoint starting: time\n");
        await SweepLaterAsync(connection, log.ToString(), 2, ct);
        json = await ReadAsync(postgres, 24, LoggingOnEnglish, ct);
        Assert.Equal(1L, json.GetProperty("hours").EnumerateArray().Sum(h => h.GetProperty("not_completed").GetInt64()));

        // Its complete line is then the newest checkpoint's, and pairs with it, not with the older open one.
        log.Append(Complete(At(20, 10), 400, "10.000", 40));
        await SweepLaterAsync(connection, log.ToString(), 1, ct);
        json = await ReadAsync(postgres, 24, LoggingOnEnglish, ct);
        var rows = json.GetProperty("checkpoints").EnumerateArray().ToList();
        Assert.Equal(3, rows.Count);
        Assert.Equal(40, rows[0].GetProperty("files_synced").GetInt32());
        Assert.NotEqual(JsonValueKind.Null, rows[0].GetProperty("ended_at").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("ended_at").ValueKind);
        Assert.Equal(1L, json.GetProperty("hours").EnumerateArray().Sum(h => h.GetProperty("not_completed").GetInt64()));
    });

    [Fact]
    public Task AnEmptyWindowWithLogCheckpointsOff_AnswersPreconditionNamingTheSetting() => WithStoreAsync(false, async (_, postgres, ct) =>
    {
        // log_checkpoints only changes in the server's config file, so the setting is handed to the read. The store, its table and its window are real.
        var json = await ReadAsync(postgres, 24, new DarlingStoreCheckpointReader.Settings(true, "off", "C"), ct);
        Assert.Equal("precondition", json.GetProperty("status").GetString());
        Assert.Contains("log_checkpoints", json.GetProperty("message").GetString(), StringComparison.Ordinal);
    });

    [Fact]
    public Task AnEmptyWindowWithANonEnglishLcMessages_AnswersPreconditionNamingTheSetting() => WithStoreAsync(false, async (_, postgres, ct) =>
    {
        // lc_messages only takes a locale the host has installed, so the setting is handed to the read.
        var json = await ReadAsync(postgres, 24, new DarlingStoreCheckpointReader.Settings(true, "on", "de_DE.UTF-8"), ct);
        Assert.Equal("precondition", json.GetProperty("status").GetString());
        Assert.Contains("lc_messages", json.GetProperty("message").GetString(), StringComparison.Ordinal);
    });

    [Fact]
    public Task AWindowTheStoreLogWasNeverReadIn_AnswersNotCollected() => WithStoreAsync(false, async (_, postgres, ct) =>
    {
        var json = await ReadAsync(postgres, 24, LoggingOnEnglish, ct);
        Assert.Equal("not_collected", json.GetProperty("status").GetString());
    });

    [Fact]
    public Task AWindowTheLogWasReadInWithNoCheckpointLine_AnswersEmpty() => WithStoreAsync(false, async (connection, postgres, ct) =>
    {
        await SweepAsync(connection, $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}.000 UTC [88] LOG:  database system is ready to accept connections\n", ct);

        var json = await ReadAsync(postgres, 24, LoggingOnEnglish, ct);
        Assert.Equal("empty", json.GetProperty("status").GetString());
    });

    [Fact]
    public Task AStoreBelowTheCheckpointMigration_AnswersUnavailable() => WithStoreAsync(true, async (connection, postgres, ct) =>
    {
        // Migrated in full, then rewound below the checkpoint rung the way the ladder tests do, so the store is the one the previous build left.
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, StoreCheckpoints.Version - 1, ct);

        var json = await ReadAsync(postgres, 24, null, ct);
        Assert.Equal("unavailable", json.GetProperty("status").GetString());
    });
}

/// <summary>
/// #9414: the checkpoint write path on a store whose log clock is not UTC. <c>log_timezone</c> only changes by <c>ALTER SYSTEM</c> and a reload, which is
/// cluster-wide, so this one test runs in the collection that runs alone.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class StoreCheckpoints9414ClockLiveTests
{
    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> LogTimezoneAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT current_setting('log_timezone')", connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// The whole write path on a server whose log clock is not UTC, read back through the tool: a start line and its complete line in one slab,
    /// a complete line whose start line was never read, a start line that never completes, and a catch-up sweep that reads one checkpoint's
    /// complete line together with a whole later checkpoint. Every time must come back as UTC, each complete line on its own checkpoint.
    /// log_timezone only changes by reload, so the test sets it on the scratch cluster and puts it back.
    /// </summary>
    [Fact]
    public Task ALogOnANonUtcClock_IsKeptAsUtc_EachCompleteLineOnItsOwnCheckpoint_AndOnlyACheckpointAnotherFollowedIsNotCompleted() => StoreCheckpoints9414LiveTests.WithStoreAsync(false, async (connection, postgres, ct) =>
    {
        const string Zone = "Asia/Kolkata";
        try
        {
            try
            {
                await ExecAsync(connection, $"ALTER SYSTEM SET log_timezone = '{Zone}'", ct);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                Assert.Skip("The test login may not ALTER SYSTEM, so the log clock cannot be moved off UTC here.");
            }

            await ExecAsync(connection, "SELECT pg_reload_conf()", ct);
            for (var attempt = 0; attempt < 50 && await LogTimezoneAsync(connection, ct) != Zone; attempt++)
            {
                await Task.Delay(100, ct);
            }

            Assert.Equal(Zone, await LogTimezoneAsync(connection, ct));

            var now = DateTime.UtcNow.Date.AddHours(DateTime.UtcNow.Hour);
            DateTime Utc(int minutes, int seconds) => now.AddMinutes(-minutes).AddSeconds(seconds);
            // The log writes the stamp on the server's clock: UTC plus five and a half hours.
            string Stamp(int minutes, int seconds) => Utc(minutes, seconds).AddHours(5.5).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + ".000";
            var log = new StringBuilder();

            // Sweep 1 reads checkpoint A's start line only.
            log.Append($"{Stamp(50, 0)} IST [88] LOG:  checkpoint starting: time\n");
            await StoreCheckpoints9414LiveTests.SweepLaterAsync(connection, log.ToString(), 3, ct);

            // Sweep 2 reads A's complete line, a complete line with no start line (B), and all of a later checkpoint (E).
            log.Append(StoreCheckpoints9414LiveTests.Complete(Stamp(50, 30), 900, "30.000", 300));
            log.Append(StoreCheckpoints9414LiveTests.Complete(Stamp(40, 10), 60, "10.000", 40));
            log.Append($"{Stamp(30, 0)} IST [88] LOG:  checkpoint starting: wal\n");
            log.Append(StoreCheckpoints9414LiveTests.Complete(Stamp(30, 20), 50, "20.000", 7));
            await StoreCheckpoints9414LiveTests.SweepLaterAsync(connection, log.ToString(), 2, ct);

            // Sweep 3 reads a start line that never completes (C) and a newer one still running (D).
            log.Append($"{Stamp(20, 0)} IST [88] LOG:  checkpoint starting: wal\n");
            log.Append($"{Stamp(10, 0)} IST [88] LOG:  checkpoint starting: time\n");
            await StoreCheckpoints9414LiveTests.SweepLaterAsync(connection, log.ToString(), 1, ct);

            var json = await StoreCheckpoints9414LiveTests.ReadAsync(postgres, 24, StoreCheckpoints9414LiveTests.LoggingOnEnglish, ct);
            var rows = json.GetProperty("checkpoints").EnumerateArray().ToList();
            Assert.Equal(5, rows.Count);
            Assert.Equal(new[] { Utc(10, 0), Utc(20, 0), Utc(30, 0), Utc(40, 10).AddSeconds(-10), Utc(50, 0) },
                rows.Select(r => r.GetProperty("started_at").GetDateTime()).ToArray());
            Assert.Equal(new[] { "time", "wal", "wal", "none", "time" },
                rows.Select(r => r.GetProperty("reason").ValueKind == JsonValueKind.Null ? "none" : r.GetProperty("reason").GetString()!).ToArray());

            // D and C are open. E, B and A closed on their own complete lines, at their own end times, with their own figures.
            Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("ended_at").ValueKind);
            Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("ended_at").ValueKind);
            Assert.Equal(new[] { Utc(30, 20), Utc(40, 10), Utc(50, 30) }, rows.Skip(2).Select(r => r.GetProperty("ended_at").GetDateTime()).ToArray());
            Assert.Equal(new[] { 20000L, 10000L, 30000L }, rows.Skip(2).Select(r => r.GetProperty("total_ms").GetInt64()).ToArray());
            Assert.Equal(new[] { 7, 40, 300 }, rows.Skip(2).Select(r => r.GetProperty("files_synced").GetInt32()).ToArray());
            Assert.Equal(new[] { 50, 60, 900 }, rows.Skip(2).Select(r => r.GetProperty("buffers_written").GetInt32()).ToArray());

            // Only C is counted: D started last, so it may still be running.
            Assert.Equal(1L, json.GetProperty("hours").EnumerateArray().Sum(h => h.GetProperty("not_completed").GetInt64()));
        }
        finally
        {
            await ExecAsync(connection, "ALTER SYSTEM RESET log_timezone", CancellationToken.None);
            await ExecAsync(connection, "SELECT pg_reload_conf()", CancellationToken.None);
        }
    });
}
