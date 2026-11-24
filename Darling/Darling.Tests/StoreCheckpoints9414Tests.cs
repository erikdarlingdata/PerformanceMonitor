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
/// #9414: the checkpoint lines of PostgreSQL 16, 17 and 18 (a time start, a WAL start, a shutdown, an end-of-recovery, a restartpoint) read into figures,
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

/// <summary>#9414: a log with a shutdown checkpoint, a time one and a WAL one goes through the sweep and comes back through the read, paired and in order.</summary>
[Collection("live-postgres")]
public sealed class StoreCheckpoints9414LiveTests
{
    private sealed class FixedLogFiles(string name, byte[] bytes) : StoreLogSweep.IStoreLogFiles
    {
        public Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string Name, long Size)>>([(name, bytes.Length)]);

        public Task<byte[]> ReadAsync(string file, long offset, int length, CancellationToken cancellationToken) =>
            Task.FromResult(bytes.AsSpan((int)offset, (int)Math.Min(length, bytes.Length - offset)).ToArray());
    }

    private static string Complete(string stamp, int buffers, string total, int files) =>
        $"{stamp} UTC [88] LOG:  checkpoint complete: wrote {buffers} buffers (1.0%); 0 WAL file(s) added, 0 removed, 2 recycled; write=1.000 s, sync=2.000 s, total={total} s; sync files={files}, longest=1.500 s, average=0.500 s; distance=100 kB, estimate=200 kB; lsn=0/1, redo lsn=0/0\n";

    [Fact]
    public async Task SweepPairsStartAndCompleteLines_AndTheReadListsThemNewestFirstWithAnHourSummary()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9414 checkpoint tests.");
        var ct = TestContext.Current.CancellationToken;
        // #1776 own-store: a scratch clone of the migrated template, on its own database.
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

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

        await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, new FixedLogFiles("postgresql-Tue.log", Encoding.UTF8.GetBytes(log.ToString())), ct);

        var json = JsonDocument.Parse(await DarlingMcpStoreCheckpointTools.GetStoreCheckpoints(postgres, 24, 100, null, 0, null, ct)).RootElement;
        var rows = json.GetProperty("checkpoints").EnumerateArray().ToList();
        Assert.Equal(4, rows.Count);
        Assert.Equal(new[] { 1L, 40L, 300L, 9L }, rows.Select(r => (long)r.GetProperty("files_synced").GetInt32()).ToArray());
        Assert.Equal(new[] { "none", "wal", "time", "shutdown" }, rows.Select(r => r.GetProperty("reason").ValueKind == JsonValueKind.Null ? "none" : r.GetProperty("reason").GetString()!).ToArray());
        Assert.Equal(7, rows[0].GetProperty("buffers_written").GetInt32());

        var hours = json.GetProperty("hours").EnumerateArray().ToList();
        Assert.Equal(4L, hours.Sum(h => h.GetProperty("checkpoints").GetInt64()));
        Assert.Equal(300, hours.Max(h => h.GetProperty("max_files_synced").GetInt32()));

        // Revert proof of the read's precondition: with no rows in a window the read says why, never an empty success.
        var empty = JsonDocument.Parse(await DarlingMcpStoreCheckpointTools.GetStoreCheckpoints(postgres, 1, 100, now.AddDays(-30).ToString("o", System.Globalization.CultureInfo.InvariantCulture), 0, null, ct)).RootElement;
        Assert.NotEqual("ok", empty.GetProperty("status").GetString());

    }
}
