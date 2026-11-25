/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9518: with the monitored server's <c>blocked process threshold (s)</c> at 0 in use SQL Server never raises
/// <c>blocked_process_report</c>, so the collector's batch reads the setting and nothing of its session, and the run carries
/// <c>blocked_process_threshold_off=1</c> on a SUCCESS. These drive the collector's own <c>ReadAsync</c> over the batch's
/// three result sets and the note the readers parse. The batch itself runs against a live server in
/// <c>XeShredGateLiveSqlTests</c>; the answer get_blocking gives is in <c>DarlingMcpBlockingThresholdOffTests</c>.
/// </summary>
public sealed class BlockedProcessThresholdOffTests
{
    private static CollectorContext Context() => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 11, 24, 12, 0, 0, DateTimeKind.Utc),
        Deltas = null!,
        Target = new CollectorTargetInfo(),
    };

    /// <summary>
    /// The batch's three result sets: the payload (no row), the gate row with all twelve columns, and the probe-failure set.
    /// <paramref name="threshold"/> null is the NULL the batch returns where the setting is not read.
    /// </summary>
    private static DataTableReader MakeReader(long? executionCount, bool gated, int? threshold)
    {
        var payload = new DataTable("payload");
        payload.Columns.Add("event_time", typeof(DateTime));
        payload.Columns.Add("blocked_process_report_xml", typeof(string));
        payload.Columns.Add("object_id", typeof(int));
        payload.Columns.Add("database_id", typeof(int));
        payload.Columns.Add("contentious_object", typeof(string));

        var gate = new DataTable("gate");
        gate.Columns.Add("execution_count", typeof(long));
        gate.Columns.Add("gated", typeof(bool));
        gate.Columns.Add("ring_buffer_truncated", typeof(bool));
        gate.Columns.Add("ring_buffer_events_hidden", typeof(long));
        gate.Columns.Add("newest_visible_event_age_s", typeof(long));
        gate.Columns.Add("ring_total_events", typeof(long));
        gate.Columns.Add("ring_dropped_events", typeof(long));
        gate.Columns.Add("session_create_time", typeof(DateTime));
        gate.Columns.Add("ring_events_limit", typeof(long));
        gate.Columns.Add("ring_memory_limit", typeof(long));
        gate.Columns.Add("read_at", typeof(DateTime));
        gate.Columns.Add("blocked_process_threshold", typeof(int));
        gate.Rows.Add(
            (object?)executionCount ?? DBNull.Value,
            gated,
            DBNull.Value, DBNull.Value, DBNull.Value,
            DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
            (object?)threshold ?? DBNull.Value);

        var probeFailures = new DataTable("probe_failures");
        probeFailures.Columns.Add("database_name", typeof(string));
        probeFailures.Columns.Add("error_number", typeof(int));
        probeFailures.Columns.Add("error_text", typeof(string));

        var dataSet = new DataSet();
        dataSet.Tables.Add(payload);
        dataSet.Tables.Add(gate);
        dataSet.Tables.Add(probeFailures);
        return dataSet.CreateDataReader();
    }

    [Fact]
    public async Task ThresholdZero_MeasuresTheOffCountAndReadsNothingOfTheGateRow_WhileASetThresholdReadsItAll()
    {
        /* Off: the gate row's other columns describe no read. execution_count 0 and gated 1 are deliberately present here, values a
           real off run never returns, so a ReadAsync that still read them would stage the 0 and measure the gate. */
        var off = Context();
        var offRows = await BlockedProcessReportCollector.Instance.ReadAsync(
            MakeReader(executionCount: 0, gated: true, threshold: 0), off, CancellationToken.None);

        Assert.Empty(offRows);
        Assert.Empty(off.StagedItemState);
        Assert.DoesNotContain(off.Measurements, m => m.Label == BlockedProcessReportCollector.ShredGatedMeasurement);
        Assert.Equal(1, off.Measurements.Single(m => m.Label == BlockedProcessReportCollector.ThresholdOffMeasurement).Value);

        var offNote = CollectorMeasurementNote.Render(off.Measurements);
        Assert.Equal(
            "blocked_process_threshold_off=1 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0",
            offNote);

        /* The collector's label is the one the readers parse, and the note is a count on a healthy run: not a fault record and not
           a precondition sentence for anybody to act on. */
        Assert.True(BlockedProcessThresholdOffNote.TryParse(offNote));
        Assert.True(CollectorMeasurementNote.IsMeasurementsOnly(offNote));
        Assert.Null(CollectorRuntimePrecondition.CollectionOutcomeMessage(
            "test-server", BlockedProcessReportCollector.Instance.Name, "SUCCESS", offNote, off.CollectionTime));

        /* On (a threshold of 5) and unknown (NULL, where the batch does not read the setting): the row is read as a gate row. */
        foreach (var threshold in new int?[] { 5, null })
        {
            var on = Context();
            var onRows = await BlockedProcessReportCollector.Instance.ReadAsync(
                MakeReader(executionCount: 7, gated: true, threshold), on, CancellationToken.None);

            Assert.Empty(onRows);
            Assert.Equal("7", on.StagedItemState[XeShredGate.StateKey]);
            Assert.Equal(1, on.Measurements.Single(m => m.Label == BlockedProcessReportCollector.ShredGatedMeasurement).Value);
            Assert.DoesNotContain(on.Measurements, m => m.Label == BlockedProcessReportCollector.ThresholdOffMeasurement);
            Assert.False(BlockedProcessThresholdOffNote.TryParse(CollectorMeasurementNote.Render(on.Measurements)));
        }
    }

    [Theory]
    [InlineData("blocked_process_threshold_off=1", true)]
    [InlineData("blocked_process_threshold_off=1 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0", true)]
    [InlineData("a host note in front; blocked_process_threshold_off=1 events_read=0", true)]
    [InlineData("blocked_process_threshold_off=0 events_read=0", false)]
    [InlineData("events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0", false)]
    [InlineData("shred_gated=1 events_read=0", false)]
    [InlineData("not_blocked_process_threshold_off=1", false)]
    [InlineData("blocked_process_threshold_off=", false)]
    [InlineData("blocked_process_threshold_off=-1", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void TheNoteParsesOnlyACountAboveZeroUnderItsOwnLabel(string? note, bool expected)
    {
        Assert.Equal(expected, BlockedProcessThresholdOffNote.TryParse(note));
    }

    [Fact]
    public void TheDesktopHealthGridsSayWhatTheCountMeans_WhileTheRawNoteKeepsTheCounts()
    {
        const string Note = "blocked_process_threshold_off=1 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0";

        Assert.Equal(
            "latest run: blocked process threshold off: 1 (blocked process reports are off because 'blocked process threshold (s)' is 0, so the session was not read), "
            + "events read: 0, report xml empty: 0, report xml unparsed: 0, events stored: 0",
            CollectorHealthClassifier.FormatCollectionNoteForDisplay(Note, 12, 12, BlockedProcessReportCollector.Instance.Name));

        /* The MCP note_summary and the health bands are built on the raw text, which stays counts and nothing else. */
        Assert.Equal("latest run: " + Note, CollectorHealthClassifier.FormatCollectionNote(Note, 12, 12, BlockedProcessReportCollector.Instance.Name));
    }

    [Fact]
    public void TheHintIsAddedBesideWhatTheAnswerAlreadyCarries_AndOnlyWhenTheReportsAreOff()
    {
        var hints = new { window_start_utc = "2026-11-24T00:00:00Z", ring_buffer_loss = new { evicted_events = 12 } };

        /* Reports on: the very object the answer had, so the answer is what it was. */
        Assert.Same(hints, BlockedProcessThresholdOffNote.WithHint(hints, reportsOff: false));
        Assert.Null(BlockedProcessThresholdOffNote.WithHint(null, reportsOff: false));

        var withHint = Assert.IsType<JsonObject>(BlockedProcessThresholdOffNote.WithHint(hints, reportsOff: true));
        var json = JsonDocument.Parse(withHint.ToJsonString()).RootElement;
        Assert.Equal("2026-11-24T00:00:00Z", json.GetProperty("window_start_utc").GetString());
        Assert.Equal(12, json.GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
        Assert.Equal(BlockedProcessThresholdOffNote.Hint, json.GetProperty(BlockedProcessThresholdOffNote.HintKey).GetString());

        /* No hints at all (a failed window probe): the sentence stands alone. */
        var alone = Assert.IsType<JsonObject>(BlockedProcessThresholdOffNote.WithHint(null, reportsOff: true));
        Assert.Equal(BlockedProcessThresholdOffNote.Hint, Assert.Single(alone).Value!.GetValue<string>());

        /* What the sentence has to say: the setting by its sys.configurations name, that blocking is still answered, and both
           ways to turn reports on. It asks nobody to change anything. */
        Assert.Contains("'" + BlockedProcessThresholdOffNote.SettingName + "' is 0", BlockedProcessThresholdOffNote.Hint, StringComparison.Ordinal);
        Assert.Contains("still answered from DMV snapshots", BlockedProcessThresholdOffNote.Hint, StringComparison.Ordinal);
        Assert.Contains("sp_configure", BlockedProcessThresholdOffNote.Hint, StringComparison.Ordinal);
        Assert.Contains("DB parameter group on RDS", BlockedProcessThresholdOffNote.Hint, StringComparison.Ordinal);
    }
}
