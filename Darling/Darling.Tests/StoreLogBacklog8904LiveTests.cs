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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A burst of log after a restart (#8904): one capture reads on through the file instead of one 4 MiB slab, up to the
/// per-capture bound, with the same entries and resume offset as several captures would give. While bytes are still
/// pending the note names the bound and the captures the backlog needs, and the answer says the newest hours are unread.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreLogBacklog8904LiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #8904 store-log tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(connection, postgres, ct);
    }

    /// <summary>One file of log made of whole entries, a little over <paramref name="bytes"/> long, and how many entries it holds.</summary>
    private static (byte[] Bytes, int Entries) LogOf(long bytes)
    {
        var text = new StringBuilder();
        var entries = 0;
        while (text.Length < bytes)
        {
            entries++;
            text.Append(EntryLine(entries));
        }

        return (Encoding.UTF8.GetBytes(text.ToString()), entries);
    }

    private static string EntryLine(int i) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"2026-11-17 16:{i / 60 % 60:00}:{i % 60:00}.000 UTC [{i}] LOG:  checkpoint complete: wrote {i} buffers (0.0%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.001 s, sync=0.001 s, total=0.003 s; sync files=1, longest=0.001 s, average=0.001 s; distance=1 kB, estimate=1 kB\n");

    /// <summary>What one capture reading this file slab by slab, from offset 0, consumes: each slab is cut at its last
    /// newline, exactly as the sweep does.</summary>
    private static List<int> SlabSizes(byte[] file)
    {
        var sizes = new List<int>();
        long offset = 0;
        while (offset < file.Length)
        {
            var length = (int)Math.Min(StoreLogSlab.MaxBytesPerRead, file.Length - offset);
            var slab = StoreLogSlab.TrimToLastNewline(file.AsSpan((int)offset, length));
            Assert.True(slab.BytesConsumed > 0);
            sizes.Add(slab.BytesConsumed);
            offset += slab.BytesConsumed;
        }

        return sizes;
    }

    private sealed class FixedLogFiles(string name, byte[] bytes) : StoreLogSweep.IStoreLogFiles
    {
        public Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string Name, long Size)>>([(name, bytes.Length)]);

        public Task<byte[]> ReadAsync(string file, long offset, int length, CancellationToken cancellationToken)
        {
            Assert.Equal(name, file);
            return Task.FromResult(bytes.AsSpan((int)offset, (int)Math.Min(length, bytes.Length - offset)).ToArray());
        }
    }

    private static async Task<(long Offset, int Rows, long BytesRead, long Pending)> MarkerAndRowsAsync(NpgsqlConnection connection, string file, CancellationToken ct)
    {
        long offset;
        await using (var marker = new NpgsqlCommand("SELECT byte_offset FROM config.store_log_read_marker WHERE log_file = $1", connection))
        {
            marker.Parameters.AddWithValue(file);
            offset = (long)(await marker.ExecuteScalarAsync(ct) ?? -1L);
        }

        await using var rows = new NpgsqlCommand(
            "SELECT count(*)::int, coalesce(sum(bytes_read), 0)::bigint, coalesce(min(bytes_pending), 0)::bigint FROM collect.store_log_captures WHERE log_file = $1",
            connection);
        rows.Parameters.AddWithValue(file);
        await using var reader = await rows.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (offset, reader.GetInt32(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    [Fact]
    public async Task OneCapture_ReadsOnThroughAFileLargerThanOneSlab_WithTheEntriesAndResumeOffsetSeveralCapturesGive()
    {
        await WithStoreAsync(async (connection, _, ct) =>
        {
            var (file, entries) = LogOf(10L * 1024 * 1024);
            var sizes = SlabSizes(file);
            Assert.True(sizes.Count >= 3, "the file has to span several slabs for this to prove anything.");

            var captured = await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, new FixedLogFiles("postgresql-Tue.log", file), ct);

            var capture = Assert.Single(captured);
            Assert.Equal(sizes.Sum(), capture.BytesRead);
            Assert.Equal(0, capture.BytesPending);
            Assert.Equal(entries, capture.Census.EntriesRead);

            var (offset, rows, bytesRead, pending) = await MarkerAndRowsAsync(connection, "postgresql-Tue.log", ct);
            Assert.Equal(sizes.Sum(), offset);
            Assert.Equal(1, rows);
            Assert.Equal(sizes.Sum(), bytesRead);
            Assert.Equal(0, pending);

            await using var stored = new NpgsqlCommand("SELECT coalesce(sum(entries_read), 0)::int, coalesce(sum(lines_read), 0)::int FROM collect.store_log_captures", connection);
            await using var reader = await stored.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(entries, reader.GetInt32(0));
            Assert.Equal(entries, reader.GetInt32(1));
        });
    }

    [Fact]
    public async Task OneCapture_StopsAtThePerCaptureBound_ThenTheNextCaptureReadsTheRest()
    {
        await WithStoreAsync(async (connection, _, ct) =>
        {
            var (file, entries) = LogOf(StoreLogSweep.MaxBytesPerCapture + 3L * 1024 * 1024);
            var sizes = SlabSizes(file);
            Assert.True(sizes.Count > StoreLogSweep.MaxSlabsPerCapture);
            var boundBytes = sizes.Take(StoreLogSweep.MaxSlabsPerCapture).Sum();
            var source = new FixedLogFiles("postgresql-Tue.log", file);

            var first = Assert.Single(await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow.AddHours(-1), null, source, ct));
            Assert.Equal(boundBytes, first.BytesRead);
            Assert.Equal(file.Length - boundBytes, first.BytesPending);
            Assert.True(first.Census.EntriesRead < entries);
            Assert.Equal(boundBytes, (await MarkerAndRowsAsync(connection, "postgresql-Tue.log", ct)).Offset);

            var second = Assert.Single(await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, source, ct));
            Assert.Equal(file.Length - boundBytes, second.BytesRead);
            Assert.Equal(0, second.BytesPending);
            Assert.Equal(entries, first.Census.EntriesRead + second.Census.EntriesRead);

            var (offset, rows, bytesRead, pending) = await MarkerAndRowsAsync(connection, "postgresql-Tue.log", ct);
            Assert.Equal(file.Length, offset);
            Assert.Equal(2, rows);
            Assert.Equal(file.Length, bytesRead);
            Assert.Equal(0, pending);
        });
    }

    private static DarlingStoreLogReader.StoreLogReport ReportWith(long pending, string? stamp = null) => new()
    {
        WindowHours = 3,
        Captures = new DarlingStoreLogReader.CaptureSummary { Captures = 3, CapturesExpected = 3, EntriesRead = 10, BytesPending = pending },
        NewestSampleLogStamp = stamp,
    };

    [Fact]
    public void Note_ForABacklogOverTheBound_NamesTheBoundAndTheCapturesItNeeds_AndOtherwiseKeepsTheOldSentence()
    {
        var bound = StoreLogSweep.MaxBytesPerCapture;
        var over = DarlingStoreLogReader.BuildNote(ReportWith(2 * bound + 1));
        Assert.Contains($"One capture reads at most {bound:N0} bytes (8 slabs of 4,194,304 bytes)", over, StringComparison.Ordinal);
        Assert.Contains("needs at least 3 captures to read", over, StringComparison.Ordinal);
        Assert.DoesNotContain("the next capture reads them", over, StringComparison.Ordinal);

        var level = DarlingStoreLogReader.BuildNote(ReportWith(bound));
        Assert.Contains("the next capture reads them", level, StringComparison.Ordinal);
        Assert.DoesNotContain("One capture reads at most", level, StringComparison.Ordinal);

        Assert.Equal(1, StoreLogSweep.CapturesNeeded(1));
        Assert.Equal(1, StoreLogSweep.CapturesNeeded(bound));
        Assert.Equal(2, StoreLogSweep.CapturesNeeded(bound + 1));
    }

    [Fact]
    public async Task Answer_WhileBytesArePending_SaysTheNewestHoursAreUnread_AndNamesTheLogTimeReached_AndWhenLevelHasNoSuchField()
    {
        await WithStoreAsync(async (store, postgres, ct) =>
        {
            const string stamp = "2026-11-17 16:40:03.123 UTC";
            var slab = stamp + " [5288] LOG:  duration: 5012.331 ms  execute <unnamed>: \n\tSELECT sum(amount) FROM ledger_pending GROUP BY account_id\n";

            await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), slab, ct, bytesPending: 10_675_571);
            var pending = JsonDocument.Parse(await DarlingMcpStoreLogTools.GetStoreLog(postgres, cancellationToken: ct)).RootElement;
            var backlog = pending.GetProperty("log_backlog");
            Assert.Equal(10_675_571, backlog.GetProperty("bytes_pending").GetInt64());
            Assert.Equal(StoreLogSweep.MaxBytesPerCapture, backlog.GetProperty("per_capture_bound_bytes").GetInt64());
            Assert.Equal(1, backlog.GetProperty("captures_needed").GetInt64());
            Assert.Equal(stamp, backlog.GetProperty("newest_sample_log_stamp").GetString());
            Assert.Contains("not a quiet hour", backlog.GetProperty("note").GetString(), StringComparison.Ordinal);
            Assert.Contains(stamp, pending.GetProperty("note").GetString(), StringComparison.Ordinal);
            Assert.Contains("can read low", pending.GetProperty("note").GetString(), StringComparison.Ordinal);

            await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow), slab, ct);
            var level = JsonDocument.Parse(await DarlingMcpStoreLogTools.GetStoreLog(postgres, cancellationToken: ct)).RootElement;
            Assert.False(level.TryGetProperty("log_backlog", out _), "a level reader has no unread hours to name.");
            Assert.DoesNotContain("can read low", level.GetProperty("note").GetString(), StringComparison.Ordinal);
        });
    }

    [Theory]
    [InlineData("2026-11-17 16:40:03.123 UTC [5288] LOG:  duration: 5 ms", "2026-11-17 16:40:03.123 UTC")]
    [InlineData("2026-11-17 16:40:03 EST [1] LOG:  x", "2026-11-17 16:40:03 EST")]
    [InlineData("2026-11-17 16:40:03.123 +03 [1] LOG:  x", "2026-11-17 16:40:03.123 +03")]
    [InlineData("LOG:  duration: 5 ms", null)]
    [InlineData("", null)]
    public void LogStampOf_ReadsTheStampAsTheLogWroteIt(string head, string? expected) =>
        Assert.Equal(expected, DarlingStoreLogReader.LogStampOf(head));
}
