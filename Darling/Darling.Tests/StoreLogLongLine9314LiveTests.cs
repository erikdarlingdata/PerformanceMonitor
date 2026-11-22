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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A finished log line longer than one read (#9314) used to stop the store-log marker at its first byte until the file
/// rotated: the 4 MiB slab held no newline, so the trim consumed nothing. The sweep now hands the classifier the line's
/// first bytes, moves the marker past its newline, and goes on with the lines after it. A line still being written
/// waits as it always did, and a line longer than a whole capture's budget takes more than one capture without ever
/// leaving the marker where it was.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreLogLongLine9314LiveTests
{
    private const string LogName = "postgresql-Tue.log";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9314 store-log tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(connection, postgres, ct);
    }

    private static string NormalLine(int i) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"2026-11-17 16:00:{i % 60:00}.000 UTC [{i}] LOG:  checkpoint complete: wrote {i} buffers (0.0%)\n");

    /// <summary>One ERROR entry of <paramref name="padding"/> bytes after its head, without its newline.</summary>
    private static string LongLineWithoutNewline(int padding) =>
        "2026-11-17 16:00:01.000 UTC [7] ERROR:  " + new string('x', padding);

    private static byte[] Join(params string[] parts) => Encoding.UTF8.GetBytes(string.Concat(parts));

    private sealed class GrowingLogFiles : StoreLogSweep.IStoreLogFiles
    {
        public byte[] Bytes { get; set; } = [];

        public Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string Name, long Size)>>([(LogName, Bytes.Length)]);

        public Task<byte[]> ReadAsync(string file, long offset, int length, CancellationToken cancellationToken) =>
            Task.FromResult(Bytes.AsSpan((int)offset, (int)Math.Min(length, Bytes.Length - offset)).ToArray());
    }

    private sealed class SeveralLogFiles(params (string Name, byte[] Bytes)[] files) : StoreLogSweep.IStoreLogFiles
    {
        public Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string Name, long Size)>>(files.Select(f => (f.Name, (long)f.Bytes.Length)).ToList());

        public Task<byte[]> ReadAsync(string file, long offset, int length, CancellationToken cancellationToken)
        {
            var bytes = files.Single(f => f.Name == file).Bytes;
            return Task.FromResult(bytes.AsSpan((int)offset, (int)Math.Min(length, bytes.Length - offset)).ToArray());
        }
    }

    private static async Task<long> MarkerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var marker = new NpgsqlCommand("SELECT byte_offset FROM config.store_log_read_marker WHERE log_file = $1", connection);
        marker.Parameters.AddWithValue(LogName);
        return (long)(await marker.ExecuteScalarAsync(ct) ?? -1L);
    }

    private static int Occurrences(StoreLogSweep.FileCapture capture, string severity) =>
        capture.Census.Groups.Where(g => string.Equals(g.Severity, severity, StringComparison.Ordinal)).Sum(g => g.Occurrences);

    [Fact]
    public async Task AFinishedLineLongerThanOneRead_IsCountedByItsStart_AndTheMarkerMovesPastIt_ToTheLinesAfter()
    {
        await WithStoreAsync(async (connection, _, ct) =>
        {
            var file = Join(NormalLine(1), LongLineWithoutNewline(StoreLogSlab.MaxBytesPerRead + 300_000), "\n", NormalLine(2), NormalLine(3));
            var source = new GrowingLogFiles { Bytes = file };

            var capture = Assert.Single(await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, source, ct));

            Assert.Equal(file.Length, capture.BytesRead);
            Assert.Equal(0, capture.BytesPending);
            Assert.Equal(4, capture.Census.LinesRead);
            Assert.Equal(4, capture.Census.EntriesRead);
            Assert.Equal(1, Occurrences(capture, "ERROR"));
            Assert.Equal(file.Length, await MarkerAsync(connection, ct));
        });
    }

    /// <summary>
    /// The prefix of a long line is handed to the classifier as cut text. A Japanese syntax error opens with its quoted
    /// token and ends with the words after it, so a read that stops inside the token holds an unfinished quoted part:
    /// that part is withheld from its opening quote on, and the literal inside it is not stored.
    /// </summary>
    [Fact]
    public async Task ThePrefixOfALongTokenFirstSyntaxError_IsWithheldFromItsOpeningQuote_NotStoredAsTheSample()
    {
        await WithStoreAsync(async (connection, _, ct) =>
        {
            const string leak = "Leak9314Literal";
            var line = "2026-11-17 16:00:01.000 UTC [7] ERROR:  \"'" + leak + " "
                + new string('x', StoreLogSlab.MaxBytesPerRead + 300_000)
                + "'\"またはその近辺で構文エラー(40文字目)\n";
            var file = Join(NormalLine(1), line, NormalLine(2), NormalLine(3));
            var source = new GrowingLogFiles { Bytes = file };

            var capture = Assert.Single(await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, source, ct));

            Assert.Equal(file.Length, await MarkerAsync(connection, ct));
            var group = Assert.Single(capture.Census.Groups, g => string.Equals(g.Severity, "ERROR", StringComparison.Ordinal));
            Assert.DoesNotContain(leak, group.MessageText, StringComparison.Ordinal);
            Assert.DoesNotContain(leak, group.SampleLine, StringComparison.Ordinal);
            Assert.Contains(StoreLogClassifier.WithheldStatement, group.SampleLine, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ALineLongerThanOneRead_ThatIsStillBeingWritten_Waits_AndIsReadOnceItsNewlineArrives_AndTheAnswerNamesTheStall()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var head = NormalLine(1);
            var unfinished = Join(head, LongLineWithoutNewline(StoreLogSlab.MaxBytesPerRead + 300_000));
            var source = new GrowingLogFiles { Bytes = unfinished };
            var start = DateTime.UtcNow.AddHours(-2);

            var first = Assert.Single(await StoreLogSweep.SweepAsync(connection, start, null, source, ct));
            Assert.Equal(head.Length, first.BytesRead);
            Assert.Equal(unfinished.Length - head.Length, first.BytesPending);
            Assert.Equal(head.Length, await MarkerAsync(connection, ct));

            var again = Assert.Single(await StoreLogSweep.SweepAsync(connection, start.AddHours(1), null, source, ct));
            Assert.Equal(0, again.BytesRead);
            Assert.Equal(head.Length, await MarkerAsync(connection, ct));

            /* One capture that read nothing after one that read is not a stall: a heartbeat over a short line at the end of a file reads nothing too. */
            var oneZero = await DarlingStoreLogReader.GetCaptureSummaryAsync(postgres, start.AddMinutes(-1), DateTime.UtcNow, 3, ct);
            Assert.False(oneZero.MarkerStalled);

            Assert.Single(await StoreLogSweep.SweepAsync(connection, start.AddMinutes(90), null, source, ct));

            /* Two in a row read nothing while bytes are pending: the answer says the marker is not moving, and since when. */
            var stalled = await DarlingStoreLogReader.GetCaptureSummaryAsync(postgres, start.AddMinutes(-1), DateTime.UtcNow, 3, ct);
            Assert.True(stalled.MarkerStalled);
            Assert.Equal(0, stalled.NewestCaptureBytesRead);
            Assert.NotNull(stalled.MarkerLastMovedAt);
            var stalledNote = DarlingStoreLogReader.BuildNote(new DarlingStoreLogReader.StoreLogReport { WindowHours = 3, Captures = stalled });
            Assert.Contains("has not moved since " + stalled.MarkerLastMovedAt, stalledNote, StringComparison.Ordinal);
            Assert.DoesNotContain("the next capture reads them", stalledNote, StringComparison.Ordinal);

            source.Bytes = Join(Encoding.UTF8.GetString(unfinished), "\n", NormalLine(2));
            var third = Assert.Single(await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, source, ct));
            Assert.Equal(source.Bytes.Length - head.Length, third.BytesRead);
            Assert.Equal(0, third.BytesPending);
            Assert.Equal(2, third.Census.LinesRead);
            Assert.Equal(2, third.Census.EntriesRead);
            Assert.Equal(source.Bytes.Length, await MarkerAsync(connection, ct));

            var level = await DarlingStoreLogReader.GetCaptureSummaryAsync(postgres, start.AddMinutes(-1), DateTime.UtcNow.AddMinutes(1), 3, ct);
            Assert.False(level.MarkerStalled);
        });
    }

    [Fact]
    public async Task AsOfWindowEndingBeforeALaterReadingCapture_ReportsTheInWindowTimeTheMarkerLastMoved()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var source = new GrowingLogFiles { Bytes = Join(NormalLine(1)) };
            var first = DateTime.UtcNow.AddHours(-6);
            Assert.Single(await StoreLogSweep.SweepAsync(connection, first, null, source, ct));
            source.Bytes = Join(NormalLine(1), NormalLine(2));
            Assert.Single(await StoreLogSweep.SweepAsync(connection, first.AddHours(3), null, source, ct));

            var narrow = await DarlingStoreLogReader.GetCaptureSummaryAsync(postgres, first.AddMinutes(-1), first.AddMinutes(30), 3, ct);
            var wide = await DarlingStoreLogReader.GetCaptureSummaryAsync(postgres, first.AddMinutes(-1), DateTime.UtcNow, 3, ct);

            Assert.NotNull(narrow.MarkerLastMovedAt);
            Assert.Equal(narrow.LastCaptureAt, narrow.MarkerLastMovedAt);
            Assert.NotEqual(wide.MarkerLastMovedAt, narrow.MarkerLastMovedAt);
        });
    }

    [Fact]
    public async Task ACaptureOverSeveralFiles_OneHoldingALongLine_ReadsAtMostTheCapturesByteBudget_AndLeavesTheOtherFilesForTheNextCapture()
    {
        await WithStoreAsync(async (connection, _, ct) =>
        {
            var normal = new StringBuilder();
            for (var i = 0; normal.Length < 5 * 1024 * 1024; i++)
            {
                normal.Append(NormalLine(i));
            }

            var other = Encoding.UTF8.GetBytes(normal.ToString());
            var longFile = Join(LongLineWithoutNewline((int)StoreLogSweep.MaxBytesPerCapture - 200_000), "\n", NormalLine(2));
            var source = new SeveralLogFiles(("postgresql-Mon.log", longFile), ("postgresql-Tue.log", other), ("postgresql-Wed.log", other), ("postgresql-Thu.log", other));

            var captures = await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, null, source, ct);

            Assert.True(captures.Sum(c => c.BytesRead) > 0);
            Assert.True(captures.Sum(c => c.BytesRead) <= StoreLogSweep.MaxBytesPerCapture, "every slab and every scan chunk draws on the one byte budget.");
        });
    }

    [Fact]
    public async Task ALineLongerThanOneCapturesWholeBudget_TakesMoreThanOneCapture_HandsOnItsPrefixOnce_AndNeverATailAsALine()
    {
        await WithStoreAsync(async (connection, _, ct) =>
        {
            var padding = (int)StoreLogSweep.MaxBytesPerCapture + 8 * 1024 * 1024;
            var file = Join(NormalLine(1), LongLineWithoutNewline(padding), "\n", NormalLine(2), NormalLine(3));
            var source = new GrowingLogFiles { Bytes = file };

            var captures = new List<StoreLogSweep.FileCapture>();
            var markers = new List<long>();
            for (var i = 0; i < 4 && (markers.Count == 0 || markers[^1] < file.Length); i++)
            {
                captures.Add(Assert.Single(await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow.AddHours(i - 4), null, source, ct)));
                markers.Add(await MarkerAsync(connection, ct));
            }

            Assert.True(captures.Count >= 2, "a line longer than one capture's budget has to take more than one capture.");
            Assert.Equal(file.Length, markers[^1]);
            for (var i = 1; i < markers.Count; i++)
            {
                Assert.True(markers[i] > markers[i - 1], "the marker has to move forward on every capture.");
            }

            /* Three whole lines and the long line's prefix, once. A tail fragment handed on as a line would make it five. */
            Assert.Equal(4, captures.Sum(c => c.Census.LinesRead));
            Assert.Equal(4, captures.Sum(c => c.Census.EntriesRead));
            Assert.Equal(1, captures.Sum(c => Occurrences(c, "ERROR")));
            Assert.Equal(1, captures.Count(c => Occurrences(c, "ERROR") > 0));
            Assert.Equal(file.Length, captures.Sum(c => c.BytesRead));
        });
    }

    [Fact]
    public void ThePrefixOfALongLine_NeverSplitsAMultiByteCharacter_AtTheCutPoint()
    {
        const int lineBytes = StoreLogSlab.MaxBytesPerRead + 1000;

        /* A two, three and four byte character whose first bytes are the last bytes of the slab. */
        foreach (var (character, bytesInSlab) in new[] { ("é", 1), ("€", 1), ("€", 2), ("\U0001F600", 1), ("\U0001F600", 2), ("\U0001F600", 3) })
        {
            var encoded = Encoding.UTF8.GetBytes(character);
            var slab = new byte[StoreLogSlab.MaxBytesPerRead];
            Array.Fill(slab, (byte)'a');
            Array.Copy(encoded, 0, slab, slab.Length - bytesInSlab, bytesInSlab);

            var trimmed = StoreLogSlab.TrimLongLine(slab, lineBytes);

            Assert.Equal(lineBytes, trimmed.BytesConsumed);
            Assert.Equal(new string('a', StoreLogSlab.MaxBytesPerRead - bytesInSlab), trimmed.Text);
            Assert.DoesNotContain('�', trimmed.Text);
        }

        /* A whole character at the end is kept. */
        var whole = new byte[StoreLogSlab.MaxBytesPerRead];
        Array.Fill(whole, (byte)'a');
        var euro = Encoding.UTF8.GetBytes("€");
        Array.Copy(euro, 0, whole, whole.Length - euro.Length, euro.Length);
        var kept = StoreLogSlab.TrimLongLine(whole, lineBytes);
        Assert.EndsWith("€", kept.Text, StringComparison.Ordinal);
        Assert.Equal(StoreLogSlab.MaxBytesPerRead - 2, kept.Text.Length);
    }

    [Fact]
    public void TheNote_NamesAStalledMarker_OnlyWhenTheNewestCaptureReadNothingWhileBytesArePending()
    {
        string NoteFor(long pending, long? newestBytesRead, string? movedAt, long? previousBytesRead = 0) => DarlingStoreLogReader.BuildNote(new DarlingStoreLogReader.StoreLogReport
        {
            WindowHours = 3,
            Captures = new DarlingStoreLogReader.CaptureSummary
            {
                Captures = 3,
                CapturesExpected = 3,
                EntriesRead = 10,
                BytesPending = pending,
                NewestCaptureBytesRead = newestBytesRead,
                PreviousCaptureBytesRead = previousBytesRead,
                MarkerLastMovedAt = movedAt,
            },
        });

        var stalled = NoteFor(9_000, 0, "2026-11-17T16:40:03Z");
        Assert.Contains("has not moved since 2026-11-17T16:40:03Z", stalled, StringComparison.Ordinal);
        Assert.DoesNotContain("the next capture reads them", stalled, StringComparison.Ordinal);

        Assert.Contains("has not moved in the latest capture", NoteFor(9_000, 0, null), StringComparison.Ordinal);

        var heartbeat = NoteFor(9_000, 0, "2026-11-17T16:40:03Z", previousBytesRead: 4_194_000);
        Assert.DoesNotContain("has not moved", heartbeat, StringComparison.Ordinal);

        var backlog = NoteFor(9_000, 4_194_000, "2026-11-17T16:40:03Z");
        Assert.Contains("the next capture reads them", backlog, StringComparison.Ordinal);
        Assert.DoesNotContain("has not moved", backlog, StringComparison.Ordinal);

        var caughtUp = NoteFor(0, 0, "2026-11-17T16:40:03Z");
        Assert.DoesNotContain("has not moved", caughtUp, StringComparison.Ordinal);
        Assert.DoesNotContain("past the read marker", caughtUp, StringComparison.Ordinal);
    }
}
