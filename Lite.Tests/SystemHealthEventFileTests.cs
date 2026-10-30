/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6441 part 2. The system_health collector (one definition, shared by Lite and Darling) reads the session's event_file
/// target from a (file name, buffer offset) bookmark, because the ring buffer's XML is cut at about 4 MB and keeps the
/// OLDEST events. These tests run the collector's own code: what it asks the server (<c>BuildQuery</c>), what it
/// records from the answer (<c>ReadAsync</c>: the bookmark, the source counts, the ring-buffer fallback with its truncation
/// counts), and that an event read from the file parses to the same values as the same event read from the ring buffer.
/// </summary>
public sealed class SystemHealthEventFileTests
{
    private const string FileA = @"C:\sql\Log\system_health_0_134370637615340000.xel";
    private const string FileB = @"C:\sql\Log\system_health_0_134370999999990000.xel";

    private static readonly DateTime s_watermark = new(2026, 10, 22, 9, 0, 0, DateTimeKind.Utc);

    private static CollectorContext MakeContext(
        IReadOnlyDictionary<string, string>? state = null,
        DateTime? watermark = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 10, 22, 9, 30, 0, DateTimeKind.Utc),
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo(),
            Watermark = watermark,
            State = state ?? CollectorContext.NoState,
        };

    private static long? Measured(CollectorContext context, string label) =>
        context.Measurements.Where(m => m.Label == label).Select(m => (long?)m.Value).FirstOrDefault();

    private static object[] FileRow(DateTime time, string type, string file, long offset) =>
        new object[] { time, type, "<event name=\"" + type + "\"/>", file, offset };

    private static object[] RingRow(DateTime time, string type) =>
        new object[] { time, type, "<event name=\"" + type + "\"/>", DBNull.Value, DBNull.Value };

    private static object[] Trailer(
        bool truncated, long hidden, object age, bool fileRead, object? error = null, bool bookmarkRefused = false,
        string? scanFile = null, long? scanOffset = null, bool bufferCut = false, bool wildcard = false) =>
        new object[]
        {
            truncated, hidden, age, fileRead, error ?? DBNull.Value, bookmarkRefused,
            (object?)scanFile ?? DBNull.Value, (object?)scanOffset ?? DBNull.Value, bufferCut, wildcard,
        };

    [Fact]
    public async Task AFileRead_LeavesTheBookmarkOfTheNewestEvent_AndCountsTheEvents()
    {
        var context = MakeContext();
        var newest = s_watermark.AddMinutes(7);
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[]
            {
                FileRow(s_watermark.AddMinutes(1), "error_reported", FileA, 6_000_000),
                FileRow(newest, "sp_server_diagnostics_component_result", FileB, 4_096),
                FileRow(s_watermark.AddMinutes(3), "wait_info", FileB, 2_048),
            },
            new[] { Trailer(false, 0, DBNull.Value, true) });

        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(3, rows.Count);
        Assert.Equal(3, Measured(context, "event_file_events"));
        Assert.Null(Measured(context, "event_file_fallback"));
        Assert.Null(Measured(context, RingBufferTruncationNote.TruncatedLabel));
        Assert.Equal(
            SystemHealthEventsCollector.FormatBookmark(newest, 4_096, FileB),
            context.PendingState[SystemHealthEventsCollector.BookmarkStateKey]);
    }

    [Fact]
    public async Task AQuietFilePass_CountsZeroEvents_AndLeavesTheBookmarkAlone()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { Trailer(false, 0, DBNull.Value, true) });

        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Equal(0, Measured(context, "event_file_events"));
        Assert.DoesNotContain(SystemHealthEventsCollector.BookmarkStateKey, context.PendingState.Keys);
    }

    /// <summary>
    /// #6453: the health-parser reads answer <c>empty</c> for a server with no stored events only when the newest run
    /// read the live session through its event_file target. This holds the reader's rule to the note the collector really
    /// renders: a file read (quiet pass included) is accepted; a ring buffer fallback, with or without an error number, a
    /// truncated ring buffer and a pass that recorded nothing are not.
    /// </summary>
    [Fact]
    public async Task TheNoteTheCollectorRenders_TellsAFileReadFromAFallback_ToTheHealthParserReads()
    {
        async Task<string> NoteOf(object[][] events, object[] trailer)
        {
            var context = MakeContext();
            using var reader = FakeCollectorDataReader.WithResultSets(events, new[] { trailer });
            await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
            return CollectorMeasurementNote.Render(context.Measurements) ?? "";
        }

        var quietFile = await NoteOf(Array.Empty<object[]>(), Trailer(false, 0, DBNull.Value, true));
        Assert.Contains("event_file_events=0", quietFile, StringComparison.Ordinal);
        Assert.True(SystemHealthSourceNote.ReadThroughEventFile(quietFile), quietFile);

        var fallback = await NoteOf(new[] { RingRow(s_watermark.AddMinutes(1), "error_reported") }, Trailer(false, 0, DBNull.Value, false));
        Assert.False(SystemHealthSourceNote.ReadThroughEventFile(fallback), fallback);

        var fallbackWithError = await NoteOf(Array.Empty<object[]>(), Trailer(false, 0, DBNull.Value, false, error: 297));
        Assert.False(SystemHealthSourceNote.ReadThroughEventFile(fallbackWithError), fallbackWithError);

        var truncated = await NoteOf(Array.Empty<object[]>(), Trailer(true, 773, 19380L, false));
        Assert.False(SystemHealthSourceNote.ReadThroughEventFile(truncated), truncated);

        Assert.False(SystemHealthSourceNote.ReadThroughEventFile(null));
        Assert.False(SystemHealthSourceNote.ReadThroughEventFile(""));
        Assert.False(SystemHealthSourceNote.ReadThroughEventFile("some host note"));
    }

    /// <summary>
    /// #6453 review: a file pass the per-pass cap stopped has read only the oldest events past the watermark. On a first connect its
    /// 3,000 events can all be older than the cutoff, so it stores 0 rows with events unread behind it. Its note must not carry
    /// <c>event_file_events=</c>, or the health-parser reads would take it for "read to the end, found nothing" and answer empty.
    /// </summary>
    [Fact]
    public async Task ACappedFilePass_WritesNoEventFileEvents_SoTheHealthParserReadsDoNotTakeItForAnEmptySession()
    {
        async Task<(string Note, long? Counted)> RunAsync(object[][] events, object[] trailer)
        {
            var context = MakeContext();
            using var reader = FakeCollectorDataReader.WithResultSets(events, new[] { trailer });
            await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
            return (CollectorMeasurementNote.Render(context.Measurements) ?? "", Measured(context, "event_file_events"));
        }

        /* The cap bound (scan position set) and every scanned event was older than the cutoff: no rows stored. */
        var cappedEmpty = await RunAsync(Array.Empty<object[]>(), Trailer(false, 0, DBNull.Value, true, scanFile: FileB, scanOffset: 4_096));
        Assert.Null(cappedEmpty.Counted);
        Assert.False(SystemHealthSourceNote.ReadThroughEventFile(cappedEmpty.Note), cappedEmpty.Note);

        /* The cap bound and some rows were stored: still not a read to the end. */
        var cappedWithRows = await RunAsync(
            new[] { FileRow(s_watermark.AddMinutes(1), "error_reported", FileA, 6_000_000) },
            Trailer(false, 0, DBNull.Value, true, scanFile: FileB, scanOffset: 4_096));
        Assert.Null(cappedWithRows.Counted);
        Assert.False(SystemHealthSourceNote.ReadThroughEventFile(cappedWithRows.Note), cappedWithRows.Note);

        /* The same quiet pass with no cap bound is the clear negative it always was. */
        var uncapped = await RunAsync(Array.Empty<object[]>(), Trailer(false, 0, DBNull.Value, true));
        Assert.Equal(0, uncapped.Counted);
        Assert.True(SystemHealthSourceNote.ReadThroughEventFile(uncapped.Note), uncapped.Note);
    }

    [Fact]
    public async Task TheRingBufferFallback_RecordsTheFallback_KeepsTheTruncationCounts_AndLeavesNoBookmark()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { RingRow(s_watermark.AddMinutes(1), "error_reported") },
            new[] { Trailer(true, 773, 19380L, false) });

        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Null(rows[0].SourceFile);
        Assert.Equal(1, Measured(context, "event_file_fallback"));
        Assert.Null(Measured(context, "event_file_events"));
        Assert.Equal(1, Measured(context, RingBufferTruncationNote.TruncatedLabel));
        Assert.Equal(773, Measured(context, RingBufferTruncationNote.EventsHiddenLabel));
        Assert.StartsWith(
            "ring_buffer_truncated=1 ring_buffer_events_hidden=773 newest_visible_event_age_s=19380 event_file_fallback=1",
            CollectorMeasurementNote.Render(context.Measurements),
            StringComparison.Ordinal);
        Assert.Empty(context.PendingState);
    }

    [Fact]
    public async Task AFallbackCausedByAnError_RecordsTheErrorNumberNextToTheFallback()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { RingRow(s_watermark.AddMinutes(1), "error_reported") },
            new[] { Trailer(false, 0, DBNull.Value, false, error: 297) });

        await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(1, Measured(context, "event_file_fallback"));
        Assert.Equal(297, Measured(context, "event_file_error"));
        Assert.StartsWith(
            "event_file_fallback=1 event_file_error=297",
            CollectorMeasurementNote.Render(context.Measurements),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFallbackWithNoError_RecordsNoErrorNumber()
    {
        /* No event_file target at all: the read never started, so there is no error to report. */
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { RingRow(s_watermark.AddMinutes(1), "error_reported") },
            new[] { Trailer(false, 0, DBNull.Value, false) });

        await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(1, Measured(context, "event_file_fallback"));
        Assert.Null(Measured(context, "event_file_error"));
    }

    [Fact]
    public async Task ARefusedBookmarkThatNothingReplaces_IsDropped_SoTheNextPassDoesNotTryItFirstForEver()
    {
        /* The bookmark's file was deleted (error 25717) and the current file has nothing newer: the pass is quiet, but the
           bookmark must not be left in place. An empty value reads back as no bookmark. */
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { Trailer(false, 0, DBNull.Value, true, bookmarkRefused: true) });

        await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(string.Empty, context.PendingState[SystemHealthEventsCollector.BookmarkStateKey]);
        Assert.Null(SystemHealthEventsCollector.TryUseBookmark(
            context.PendingState, s_watermark.AddMinutes(30)));
    }

    [Fact]
    public async Task ARefusedBookmarkThatTheCurrentFileReplaces_LeavesTheNewBookmark()
    {
        var context = MakeContext();
        var newest = s_watermark.AddMinutes(4);
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { FileRow(newest, "wait_info", FileB, 4_096) },
            new[] { Trailer(false, 0, DBNull.Value, true, bookmarkRefused: true) });

        await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(
            SystemHealthEventsCollector.FormatBookmark(newest, 4_096, FileB),
            context.PendingState[SystemHealthEventsCollector.BookmarkStateKey]);
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(13, false)]
    [InlineData(14, true)]
    [InlineData(15, true)]
    [InlineData(17, true)]
    [InlineData(12, true, true)]
    public void TheTimestampUtcBatch_IsChosenFrom2017_AndThe2016BatchNeverNamesTheColumn(
        int major, bool expectsTimestampUtc, bool managedInstance = false)
    {
        /* Managed Instance runs the current engine but reports a low ProductMajorVersion, so it is matched by edition. */
        /* Naming sys.fn_xe_file_target_read_file's timestamp_utc on 2016 fails the whole batch (the column is not there). */
        var context = MakeContext();
        var query = SystemHealthEventsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = context.ServerId,
            ServerName = context.ServerName,
            CollectionTime = context.CollectionTime,
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { SqlMajorVersion = major, IsAzureManagedInstance = managedInstance },
        });

        Assert.Equal(expectsTimestampUtc, query.Text.Contains("timestamp_utc", StringComparison.Ordinal));
        Assert.Equal(SystemHealthEventsCollector.MaxEventsPerPass, query.Parameters.Single(p => p.Name == "@max_events").Value);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void OnManagedInstance_TheEventFileIsReadByTheBarePattern_AndTheBookmarkFileIsPassedUnchanged(
        bool managedInstance, bool withBookmark)
    {
        /* #6897: Managed Instance refuses a directory path and the _0_ pattern (error 40538) and accepts the bare pattern
           for the fresh read and the bookmark resume alike. Every other engine keeps the usual paths. */
        var state = withBookmark
            ? new Dictionary<string, string>
            {
                [SystemHealthEventsCollector.BookmarkStateKey] =
                    SystemHealthEventsCollector.FormatBookmark(s_watermark.AddMinutes(-5), 4_096, FileB),
            }
            : null;
        var context = MakeContext(state, watermark: s_watermark);
        var query = SystemHealthEventsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = context.ServerId,
            ServerName = context.ServerName,
            CollectionTime = context.CollectionTime,
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { IsAzureManagedInstance = managedInstance },
            Watermark = s_watermark,
            State = context.State,
        });

        Assert.Equal(
            managedInstance ? "system_health*.xel" : null,
            query.Parameters.Single(p => p.Name == "@read_path_override").Value);
        Assert.Equal(withBookmark ? FileB : null, query.Parameters.Single(p => p.Name == "@bookmark_file").Value);
        Assert.Equal(withBookmark ? 4_096L : null, query.Parameters.Single(p => p.Name == "@bookmark_offset").Value);
    }

    [Fact]
    public async Task APassIsReadFromTheBookmarkItLeftBehind_AndTheStoredRowsAreUnchanged()
    {
        var newest = s_watermark.AddMinutes(7);
        var first = MakeContext();
        using var firstReader = FakeCollectorDataReader.WithResultSets(
            new[] { FileRow(newest, "error_reported", FileB, 8_192) },
            new[] { Trailer(false, 0, DBNull.Value, true) });
        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(firstReader, first, CancellationToken.None);

        /* The store keeps the event, so its watermark is the event's time; the next pass resumes from the bookmark. */
        var second = MakeContext(first.PendingState, watermark: newest);
        var query = SystemHealthEventsCollector.Instance.BuildQuery(second);

        Assert.Equal(FileB, query.Parameters.Single(p => p.Name == "@bookmark_file").Value);
        Assert.Equal(8_192L, query.Parameters.Single(p => p.Name == "@bookmark_offset").Value);
        Assert.Equal(newest, query.Parameters.Single(p => p.Name == "@cutoff_time").Value);

        /* The payload the store sees is the same three columns as before. */
        var writer = new RecordingCollectorRowWriter();
        SystemHealthEventsCollector.Instance.WritePayload(rows[0], writer, first);
        Assert.Equal(3, writer.Values.Count);
        Assert.Equal(newest, writer.Values[0]);
        Assert.Equal("error_reported", writer.Values[1]);
    }

    [Fact]
    public void ABookmarkAheadOfTheStoredWatermark_IsNotUsed()
    {
        /* A failed write, or a purged store: the bookmark already skipped events the store never got. */
        var state = new Dictionary<string, string>
        {
            [SystemHealthEventsCollector.BookmarkStateKey] =
                SystemHealthEventsCollector.FormatBookmark(s_watermark.AddMinutes(5), 4_096, FileB),
        };
        var query = SystemHealthEventsCollector.Instance.BuildQuery(MakeContext(state, watermark: s_watermark));

        Assert.Null(query.Parameters.Single(p => p.Name == "@bookmark_file").Value);
        Assert.Null(query.Parameters.Single(p => p.Name == "@bookmark_offset").Value);
    }

    [Fact]
    public void ABookmarkBehindTheCutoff_IsUsed_WithOrWithoutAStoredWatermark()
    {
        var state = new Dictionary<string, string>
        {
            [SystemHealthEventsCollector.BookmarkStateKey] =
                SystemHealthEventsCollector.FormatBookmark(s_watermark.AddMinutes(-5), 4_096, FileB),
        };

        var behind = SystemHealthEventsCollector.Instance.BuildQuery(MakeContext(state, watermark: s_watermark));
        Assert.Equal(FileB, behind.Parameters.Single(p => p.Name == "@bookmark_file").Value);
        Assert.Equal(4_096L, behind.Parameters.Single(p => p.Name == "@bookmark_offset").Value);

        /* No stored watermark: the cutoff is the ten-minute fallback window (09:20 here), and this bookmark (08:55)
           is behind it. */
        var noWatermark = SystemHealthEventsCollector.Instance.BuildQuery(MakeContext(state));
        Assert.Equal(FileB, noWatermark.Parameters.Single(p => p.Name == "@bookmark_file").Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData("garbage")]
    [InlineData("2026-10-22T09:00:00.0000000|notanumber|C:\\x.xel")]
    [InlineData("2026-10-22T09:00:00.0000000|4096|")]
    [InlineData("2026-10-22T09:00:00.0000000|-4096|C:\\x.xel")]
    public void AMalformedBookmark_IsNotUsed(string raw)
    {
        var state = new Dictionary<string, string> { [SystemHealthEventsCollector.BookmarkStateKey] = raw };

        Assert.Null(SystemHealthEventsCollector.TryUseBookmark(state, s_watermark));
    }

    [Fact]
    public void ABookmarkTimeIsComparedAtTheMicrosecondTheStoresKeep()
    {
        /* The store keeps microseconds, so a watermark read back is the event time with its last digit cut. */
        var eventTime = new DateTime(2026, 10, 22, 9, 0, 0, DateTimeKind.Utc).AddTicks(1_234_567);
        var stored = new DateTime(eventTime.Ticks - (eventTime.Ticks % 10), DateTimeKind.Utc);
        var state = new Dictionary<string, string>
        {
            [SystemHealthEventsCollector.BookmarkStateKey] = SystemHealthEventsCollector.FormatBookmark(eventTime, 4_096, FileB),
        };

        Assert.NotNull(SystemHealthEventsCollector.TryUseBookmark(state, stored));
    }

    [Fact]
    public async Task ARowWithNoSourceColumns_StillReads_AsARingBufferRow()
    {
        /* A reader with the old three-column shape (a test fake, an older batch) is a ring-buffer read. */
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { new object[] { s_watermark.AddMinutes(1), "wait_info", "<event name=\"wait_info\"/>" } });

        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Null(rows[0].SourceFile);
        Assert.Empty(context.PendingState);
    }

    /* The same sp_server_diagnostics_component_result event as sql2019 returns it from each source: the ring buffer
       keeps a <type> element inside every <data> element, the event file does not. */
    private const string RingEvent =
        "<event name=\"sp_server_diagnostics_component_result\" package=\"sqlserver\" timestamp=\"2026-10-22T10:07:44.268Z\">"
        + "<data name=\"component\"><type name=\"sp_server_diagnostics_component\" package=\"sqlserver\"/><value>0</value><text>SYSTEM</text></data>"
        + "<data name=\"state\"><type name=\"sp_server_diagnostics_state\" package=\"sqlserver\"/><value>1</value><text>CLEAN</text></data>"
        + "<data name=\"data\"><type name=\"xml\" package=\"package0\"/><value><system spinlockBackoffs=\"4\" sickSpinlockType=\"none\" sickSpinlockTypeAfterAv=\"none\" latchWarnings=\"1\" isAccessViolationOccurred=\"0\" writeAccessViolationCount=\"0\" totalDumpRequests=\"2\" intervalDumpRequests=\"0\" nonYieldingTasksReported=\"3\" pageFaults=\"712\" systemCpuUtilization=\"9\" sqlCpuUtilization=\"7\" BadPagesDetected=\"0\" BadPagesFixed=\"0\" LastBadPageAddress=\"0x0\"/></value></data></event>";

    private const string FileEvent =
        "<event name=\"sp_server_diagnostics_component_result\" package=\"sqlserver\" timestamp=\"2026-10-22T10:07:44.268Z\">"
        + "<data name=\"component\"><value>0</value><text>SYSTEM</text></data>"
        + "<data name=\"state\"><value>1</value><text>CLEAN</text></data>"
        + "<data name=\"data\"><value><system spinlockBackoffs=\"4\" sickSpinlockType=\"none\" sickSpinlockTypeAfterAv=\"none\" latchWarnings=\"1\" isAccessViolationOccurred=\"0\" writeAccessViolationCount=\"0\" totalDumpRequests=\"2\" intervalDumpRequests=\"0\" nonYieldingTasksReported=\"3\" pageFaults=\"712\" systemCpuUtilization=\"9\" sqlCpuUtilization=\"7\" BadPagesDetected=\"0\" BadPagesFixed=\"0\" LastBadPageAddress=\"0x0\"/></value></data></event>";

    [Fact]
    public void AnEventReadFromTheFile_ParsesLikeTheSameEventReadFromTheRingBuffer()
    {
        var fromRing = SystemHealthParser.ParseSystemHealth(RingEvent);
        var fromFile = SystemHealthParser.ParseSystemHealth(FileEvent);

        Assert.NotNull(fromRing);
        Assert.NotNull(fromFile);
        Assert.Equal(fromRing!.EventTime, fromFile!.EventTime);
        Assert.Equal(fromRing.State, fromFile.State);
        Assert.Equal(fromRing.SpinlockBackoffs, fromFile.SpinlockBackoffs);
        Assert.Equal(fromRing.NonYieldingTasksReported, fromFile.NonYieldingTasksReported);
        Assert.Equal(fromRing.PageFaults, fromFile.PageFaults);
        Assert.Equal(fromRing.SystemCpuUtilization, fromFile.SystemCpuUtilization);
        Assert.Equal(fromRing.SqlCpuUtilization, fromFile.SqlCpuUtilization);
        Assert.Equal(4, fromFile.SpinlockBackoffs);
        Assert.Equal("CLEAN", fromFile.State);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ACappedPass_WhoseEventsWereAllOlderThanTheCutoff_MovesTheBookmarkToItsScanPosition_AtTheCutoff(bool hasWatermark)
    {
        /* #6441 round 2: the cap bound, and every event it kept was at or before the cutoff, so no row came back. The
           bookmark still moves to the last buffer the read kept, stamped with the cutoff, or the next pass would read the
           same old events again for ever. */
        var context = MakeContext(watermark: hasWatermark ? s_watermark : null);
        using var reader = FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { Trailer(false, 0, DBNull.Value, true, scanFile: FileA, scanOffset: 81_920) });

        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        var cutoff = hasWatermark ? s_watermark : context.CollectionTime - CollectorContext.EventFallbackWindow;
        Assert.Equal(
            SystemHealthEventsCollector.FormatBookmark(cutoff, 81_920, FileA),
            context.PendingState[SystemHealthEventsCollector.BookmarkStateKey]);
        /* The next pass, at the same cutoff, accepts it. */
        Assert.Equal((FileA, 81_920L), SystemHealthEventsCollector.TryUseBookmark(context.PendingState, cutoff));
    }

    [Theory]
    [InlineData(FileA, 6_000_000L, FileA, 6_000_000L)]
    [InlineData(FileA, 6_090_112L, FileA, 6_090_112L)]
    [InlineData(FileB, 1_024L, FileB, 1_024L)]
    [InlineData(FileA, 4_096L, FileA, 6_000_000L)]
    public async Task ACappedPass_LeavesTheBookmarkAtTheLaterOfItsLastRowAndItsScanPosition_WithTheNewestTime(
        string scanFile, long scanOffset, string expectedFile, long expectedOffset)
    {
        /* Rows end at FileA offset 6,000,000. A scan position past it (later in FileA, or in a later file) moves the bookmark
           there; one at or before it leaves the bookmark at the last row's buffer. The time is the newest stored. */
        var context = MakeContext(watermark: s_watermark);
        var newest = s_watermark.AddMinutes(4);
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[]
            {
                FileRow(s_watermark.AddMinutes(1), "error_reported", FileA, 4_096),
                FileRow(newest, "wait_info", FileA, 6_000_000),
            },
            new[] { Trailer(false, 0, DBNull.Value, true, scanFile: scanFile, scanOffset: scanOffset) });

        await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(
            SystemHealthEventsCollector.FormatBookmark(newest, expectedOffset, expectedFile),
            context.PendingState[SystemHealthEventsCollector.BookmarkStateKey]);
    }

    [Fact]
    public async Task ABufferCut_IsRecorded_AndThePassKeepsItsRowsAndBookmark()
    {
        var context = MakeContext(watermark: s_watermark);
        var newest = s_watermark.AddMinutes(2);
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { FileRow(newest, "wait_info", FileA, 4_096) },
            new[] { Trailer(false, 0, DBNull.Value, true, scanFile: FileA, scanOffset: 4_096, bufferCut: true) });

        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Equal(1, Measured(context, "event_file_buffer_cut"));
        Assert.Equal(
            SystemHealthEventsCollector.FormatBookmark(newest, 4_096, FileA),
            context.PendingState[SystemHealthEventsCollector.BookmarkStateKey]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AWildcardRead_IsRecorded_AndAnExactPathReadWritesNothing(bool wildcard)
    {
        var context = MakeContext(watermark: s_watermark);
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { FileRow(s_watermark.AddMinutes(2), "wait_info", FileA, 4_096) },
            new[] { Trailer(false, 0, DBNull.Value, true, wildcard: wildcard) });

        await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(wildcard ? 1 : null, Measured(context, "event_file_wildcard"));
    }
}
