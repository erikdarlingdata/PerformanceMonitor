/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6441. SQL Server cuts a ring_buffer target's XML at about 4 MB, marks it <c>truncated="1"</c> and keeps the OLDEST
/// events, so a watermarked read of a busy session stored 0 rows, recorded SUCCESS and banded HEALTHY. The ring-buffer
/// collectors now record the cut as counts on the run, and the collection-health ladder bands those counts WARNING.
/// </summary>
public sealed class RingBufferTruncationSignalTests
{
    private const string TruncatedNote = "ring_buffer_truncated=1 ring_buffer_events_hidden=773 newest_visible_event_age_s=19380";

    private static CollectorContext MakeContext() => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 10, 22, 9, 30, 0, DateTimeKind.Utc),
        Deltas = new RecordingCollectorDeltaCalculator(),
        Target = new CollectorTargetInfo(),
    };

    private static long? Measured(CollectorContext context, string label) =>
        context.Measurements.Where(m => m.Label == label).Select(m => (long?)m.Value).FirstOrDefault();

    private static object[][] NoRows => Array.Empty<object[]>();

    [Fact]
    public async Task SystemHealth_ATruncatedBuffer_IsMeasured_FromTheTrailingResult()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            NoRows,
            new[] { new object[] { true, 773L, 19380L } });

        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Equal(1, Measured(context, RingBufferTruncationNote.TruncatedLabel));
        Assert.Equal(773, Measured(context, RingBufferTruncationNote.EventsHiddenLabel));
        Assert.Equal(19380, Measured(context, RingBufferTruncationNote.NewestVisibleAgeLabel));
        Assert.Equal(TruncatedNote, CollectorMeasurementNote.Render(context.Measurements));
    }

    [Fact]
    public async Task SystemHealth_AWholeBuffer_LeavesNoNote()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            NoRows,
            new[] { new object[] { false, 0L, DBNull.Value } });

        await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(context.Measurements);
    }

    [Fact]
    public async Task LongQueryCompletions_ATruncatedBuffer_IsMeasured_FromTheTrailingResult()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            NoRows,
            new[] { new object[] { true, 12L, 600L } });

        await LongQueryCompletionsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(1, Measured(context, RingBufferTruncationNote.TruncatedLabel));
        Assert.Equal(12, Measured(context, RingBufferTruncationNote.EventsHiddenLabel));
    }

    [Fact]
    public async Task Deadlocks_ATruncatedBuffer_IsMeasured_FromTheGateRow()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            NoRows,
            new[] { new object[] { 5L, false, true, 3L, 60L } });

        await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(1, Measured(context, RingBufferTruncationNote.TruncatedLabel));
        Assert.Equal(3, Measured(context, RingBufferTruncationNote.EventsHiddenLabel));
        Assert.Equal(60, Measured(context, RingBufferTruncationNote.NewestVisibleAgeLabel));
    }

    [Fact]
    public async Task BlockedProcessReports_ATruncatedBuffer_IsMeasured_FromTheGateRow()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            NoRows,
            new[] { new object[] { 5L, false, true, 4L, 61L } });

        await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(1, Measured(context, RingBufferTruncationNote.TruncatedLabel));
        Assert.Equal(4, Measured(context, RingBufferTruncationNote.EventsHiddenLabel));
    }

    [Fact]
    public async Task Deadlocks_AGatedRun_WithNullTruncationColumns_RecordsNoTruncation()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(
            NoRows,
            new[] { new object[] { 5L, true, false, 0L, DBNull.Value } });

        await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Null(Measured(context, RingBufferTruncationNote.TruncatedLabel));
    }

    private static string Band(string? latestRunNote) =>
        CollectorHealthClassifier.Classify(
            totalRuns: 20, successCount: 20, errorCount: 0, permissionDeniedCount: 0, extensionMissingCount: 0,
            abandonedCount: 0, hoursSinceLastSuccess: 0.05, hoursSinceLastRun: 0.05, frequencyMinutes: 5,
            latestRunNote: latestRunNote, trailingSuccessRuns: 20);

    [Fact]
    public void Classify_ATruncatedNewestRun_BandsWarning_AndACleanOneStaysHealthy()
    {
        Assert.Equal(CollectorHealthClassifier.Warning, Band(TruncatedNote));
        Assert.Equal(CollectorHealthClassifier.Warning, Band("some host note; " + TruncatedNote));
        Assert.Equal(CollectorHealthClassifier.Healthy, Band(null));
        Assert.Equal(CollectorHealthClassifier.Healthy, Band("shred_gated=0"));
        Assert.Equal(CollectorHealthClassifier.Healthy, Band("ring_buffer_truncated=0"));
    }

    [Fact]
    public void FormatCollectionNote_ATruncatedNote_SaysSoInPlainWords()
    {
        var note = CollectorHealthClassifier.FormatCollectionNote(TruncatedNote, noteCount: 20, totalRuns: 20);

        Assert.StartsWith("latest run: the Extended Events ring buffer this collector reads is truncated", note, StringComparison.Ordinal);
        Assert.Contains("773 event(s) are hidden from the read", note, StringComparison.Ordinal);
        Assert.Contains("the newest visible event is 5 h 23 min old", note, StringComparison.Ordinal);
        Assert.Contains(TruncatedNote, note, StringComparison.Ordinal);

        var some = CollectorHealthClassifier.FormatCollectionNote(TruncatedNote, noteCount: 3, totalRuns: 20);
        Assert.Contains("3 of 20 runs", some, StringComparison.Ordinal);
        Assert.Contains("773 event(s) are hidden", some, StringComparison.Ordinal);
    }

    /* #6650: the health-parser truncation answer says how many events the read counted when it counted some, and keeps
       "No ... events were found" only when it counted none. */
    [Fact]
    public void HealthParserMessage_WithEventsRead_SaysHowManyAndWhyNoneQualified_NotThatNoneWereFound()
    {
        var truncation = RingBufferTruncationNote.Describe(TruncatedNote)!;

        var counted = RingBufferTruncationNote.HealthParserMessage(
            "memory_broker", "srv", 24, truncation, eventsRead: 412, noneQualifiedBecause: "none passed the significance gate");

        Assert.StartsWith("412 memory_broker event(s) were read for srv in the last 24 hour(s), and none passed the significance gate, and this is NOT an all-clear", counted, StringComparison.Ordinal);
        Assert.DoesNotContain("were found", counted, StringComparison.Ordinal);
        Assert.Contains("773 event(s) are hidden", counted, StringComparison.Ordinal);

        var none = RingBufferTruncationNote.HealthParserMessage("memory_broker", "srv", 24, truncation);
        Assert.StartsWith("No memory_broker events were found for srv in the last 24 hour(s), and this is NOT an all-clear", none, StringComparison.Ordinal);
        Assert.Contains("773 event(s) are hidden", none, StringComparison.Ordinal);
    }

    /* #6827: the one-run sentence closes its appositive before "reported that", as the assembled message reads on the wire. */
    [Fact]
    public void HealthParserMessage_WithOneTruncatedRun_ClosesTheClauseBeforeReportedThat()
    {
        var stamp = new DateTime(2026, 10, 24, 7, 0, 0, DateTimeKind.Utc);
        var one = new RingBufferTruncationNote.TruncatedStretch(1, stamp, stamp, TruncatedNote);

        var message = RingBufferTruncationNote.HealthParserMessage(
            "wait_info", "srv", 24, RingBufferTruncationNote.Describe(TruncatedNote)!, stretch: one);

        Assert.Contains("the system_health collection run at 2026-10-24 07:00 UTC, which read events of this window, reported that the Extended Events ring buffer", message, StringComparison.Ordinal);
    }

    /* #6827: the readers' SQL only pre-filters, so a row it returns that TryParse does not call truncated is not counted, and the
       newest note named is the newest one that parses. */
    [Fact]
    public void TruncatedStretch_FromRuns_CountsOnlyNotesThatParse_AndNamesTheNewestOfThose()
    {
        var t0 = new DateTime(2026, 10, 24, 1, 0, 0, DateTimeKind.Utc);
        var runs = new (DateTime, string?)[]
        {
            (t0, "ring_buffer_truncated=1 ring_buffer_events_hidden=5 newest_visible_event_age_s=60"),
            (t0.AddHours(1), "ring_buffer_truncated=1x ring_buffer_events_hidden=9"),
            (t0.AddHours(2), "old_ring_buffer_truncated=1"),
            (t0.AddHours(3), "ring_buffer_truncated=1 ring_buffer_truncated=0"),
            (t0.AddHours(4), null),
            (t0.AddHours(5), "ring_buffer_truncated=2 ring_buffer_events_hidden=7 newest_visible_event_age_s=120"),
        };

        var stretch = RingBufferTruncationNote.TruncatedStretch.FromRuns(runs);

        Assert.NotNull(stretch);
        Assert.Equal(2, stretch!.Value.Count);
        Assert.Equal(t0, stretch.Value.First);
        Assert.Equal(t0.AddHours(5), stretch.Value.Last);
        Assert.Equal("ring_buffer_truncated=2 ring_buffer_events_hidden=7 newest_visible_event_age_s=120", stretch.Value.NewestNote);
        Assert.Null(RingBufferTruncationNote.TruncatedStretch.FromRuns(runs.Skip(1).Take(4)));
    }

    /* Two runs sharing one collection_time name the same note whichever order a store returns them in, so the two products agree. */
    [Fact]
    public void TruncatedStretch_FromRuns_BreaksATieOnTimeByTheGreaterNote_InEitherOrder()
    {
        var at = new DateTime(2026, 10, 24, 1, 0, 0, DateTimeKind.Utc);
        var low = "ring_buffer_truncated=1 ring_buffer_events_hidden=5";
        var high = "ring_buffer_truncated=1 ring_buffer_events_hidden=6";

        Assert.Equal(high, RingBufferTruncationNote.TruncatedStretch.FromRuns(new (DateTime, string?)[] { (at, low), (at, high) })!.Value.NewestNote);
        Assert.Equal(high, RingBufferTruncationNote.TruncatedStretch.FromRuns(new (DateTime, string?)[] { (at, high), (at, low) })!.Value.NewestNote);
    }

    [Theory]
    [InlineData(45, "45 s")]
    [InlineData(1200, "20 min")]
    [InlineData(19380, "5 h 23 min")]
    [InlineData(200000, "2 d 7 h")]
    public void FormatAge_ReadsAsPlainUnits(long seconds, string expected) =>
        Assert.Equal(expected, RingBufferTruncationNote.FormatAge(seconds));
}
