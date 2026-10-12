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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, collection census for the event and history collectors (R5): long-query completions
/// <c>statement_text</c>, default-trace <c>text_data</c>, system_health <c>event_xml</c> and job history
/// <c>message</c>. Each case plants the canary in the collector's INPUT (a reader over a <see cref="DataTable"/>),
/// runs the definition's read and then its <c>WritePayload</c> into a <see cref="StatementScrubRecordingWriter"/>, and
/// asserts on what was written: no secret needle, the marker where the statement was, and the plain statement is the
/// SAME instance the reader returned. The cases are registered in <see cref="StatementCollectionCensusCases"/>.
/// </summary>
public sealed partial class StatementCollectionCensusTests
{
    private static readonly DateTime Stamp = new(2026, 9, 22, 12, 0, 0, DateTimeKind.Utc);

    private static CollectorContext EventsContext() => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = Stamp,
        Deltas = new EventsNoDeltas(),
        Target = new CollectorTargetInfo(),
    };

    private sealed class EventsNoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }

    private static DataTable EventsTable(params (string Name, Type Type)[] columns)
    {
        var t = new DataTable("payload");
        foreach (var (name, type) in columns)
        {
            t.Columns.Add(name, type);
        }

        return t;
    }

    private static void AssertNoSecret(StatementScrubRecordingWriter writer)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles.Append("canary_ssf"))
        {
            Assert.False(writer.AnyStringContains(needle), "a written value still holds " + needle);
        }
    }

    // ── long_query_completions.statement_text ──

    private static DataTable LongQueryInput(string canary, string plain)
    {
        var t = EventsTable(
            ("event_time", typeof(DateTime)), ("event_type", typeof(string)), ("duration", typeof(long)),
            ("cpu_time", typeof(long)), ("physical_reads", typeof(long)), ("logical_reads", typeof(long)),
            ("writes", typeof(long)), ("row_count", typeof(long)), ("result", typeof(string)),
            ("statement_text", typeof(string)), ("object_name", typeof(string)), ("client_app_name", typeof(string)),
            ("client_pid", typeof(int)), ("database_id", typeof(int)), ("database_name", typeof(string)),
            ("event_sequence", typeof(long)), ("nt_username", typeof(string)), ("query_hash", typeof(string)),
            ("server_principal_name", typeof(string)), ("session_id", typeof(int)));
        foreach (var text in new[] { canary, plain })
        {
            t.Rows.Add(Stamp, "sql_batch_completed", 5_000_000L, 4_000_000L, 1L, 2L, 3L, 4L, "OK", text, "obj", "app",
                100, 5, "db", 7L, "nt", "0xABCD", "login", 55);
        }

        return t;
    }

    private static async Task<StatementScrubRecordingWriter> RunLongQuery(DataTable input, CollectorContext context)
    {
        var def = LongQueryCompletionsCollector.Instance;
        using var reader = input.CreateDataReader();
        var rows = await def.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            def.WritePayload(row, writer, context);
        }

        return writer;
    }

    [Fact]
    public async Task LongQueryCompletions_StatementText_IsWithheldAndThePlainStatementIsUntouched()
    {
        var input = LongQueryInput(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement);
        var plainInstance = (string)input.Rows[1]["statement_text"];

        var writer = await RunLongQuery(input, EventsContext());
        var again = await RunLongQuery(input, EventsContext());

        AssertNoSecret(writer);
        Assert.Contains(SensitiveStatements.PlaceholderText, writer.Strings);
        Assert.Contains(writer.Strings, s => ReferenceEquals(s, plainInstance));
        Assert.Equal(writer.Values, again.Values);
    }

    // ── default_trace_events.text_data ──

    private static DataTable DefaultTraceInput(string canary, string plain)
    {
        var t = EventsTable(
            ("event_time", typeof(DateTime)), ("event_name", typeof(string)), ("event_class", typeof(int)),
            ("spid", typeof(int)), ("database_name", typeof(string)), ("database_id", typeof(int)),
            ("login_name", typeof(string)), ("host_name", typeof(string)), ("application_name", typeof(string)),
            ("object_name", typeof(string)), ("filename", typeof(string)), ("integer_data", typeof(long)),
            ("integer_data_2", typeof(long)), ("text_data", typeof(string)), ("session_login_name", typeof(string)),
            ("error_number", typeof(int)), ("severity", typeof(int)), ("state", typeof(int)),
            ("event_sequence", typeof(long)), ("duration_us", typeof(long)), ("end_time", typeof(DateTime)));
        foreach (var text in new[] { canary, plain })
        {
            t.Rows.Add(Stamp, "Object:Created", 164, 55, "db", 5, "login", "host", "app", "obj", "f.trc", 1L, 2L,
                text, "slogin", 0, 0, 0, 9L, 100L, Stamp);
        }

        return t;
    }

    private static async Task<StatementScrubRecordingWriter> RunDefaultTrace(DataTable input, CollectorContext context)
    {
        var def = DefaultTraceEventsCollector.Instance;
        using var reader = input.CreateDataReader();
        var rows = await def.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            def.WritePayload(row, writer, context);
        }

        return writer;
    }

    [Fact]
    public async Task DefaultTraceEvents_TextData_IsWithheldAndThePlainStatementIsUntouched()
    {
        var input = DefaultTraceInput(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement);
        var plainInstance = (string)input.Rows[1]["text_data"];

        var writer = await RunDefaultTrace(input, EventsContext());
        var again = await RunDefaultTrace(input, EventsContext());

        AssertNoSecret(writer);
        Assert.Contains(SensitiveStatements.PlaceholderText, writer.Strings);
        Assert.Contains(writer.Strings, s => ReferenceEquals(s, plainInstance));
        Assert.Equal(writer.Values, again.Values);
    }

    // ── system_health_events.event_xml ──

    private static string WaitInfoEvent(string sqlText) =>
        "<event name=\"wait_info\" package=\"sqlos\" timestamp=\"2026-09-22T12:00:00.000Z\">"
        + "<data name=\"wait_type\"><value>11</value><text>LCK_M_X</text></data>"
        + "<data name=\"duration\"><value>9000</value></data>"
        + "<data name=\"signal_duration\"><value>3</value></data>"
        + "<action name=\"sql_text\" package=\"sqlserver\"><value>" + System.Security.SecurityElement.Escape(sqlText) + "</value></action>"
        + "<action name=\"session_id\" package=\"sqlserver\"><value>55</value></action>"
        + "</event>";

    private static DataTable SystemHealthInput(string canaryXml, string plainXml)
    {
        var t = EventsTable(("event_time", typeof(DateTime)), ("event_type", typeof(string)), ("event_xml", typeof(string)));
        t.Rows.Add(Stamp, "wait_info", canaryXml);
        t.Rows.Add(Stamp, "wait_info", plainXml);
        return t;
    }

    private static async Task<(StatementScrubRecordingWriter Writer, CollectorContext Context)> RunSystemHealth(DataTable input)
    {
        var context = EventsContext();
        var def = SystemHealthEventsCollector.Instance;
        using var reader = input.CreateDataReader();
        var rows = await def.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            def.WritePayload(row, writer, context);
        }

        return (writer, context);
    }

    [Fact]
    public async Task SystemHealthEvents_EventXml_WithholdsTheSqlTextActionAndKeepsTheRestOfTheEvent()
    {
        var canaryXml = WaitInfoEvent(StatementScrubCanary.CanaryStatement);
        var plainXml = WaitInfoEvent(StatementScrubCanary.PlainStatement);
        var input = SystemHealthInput(canaryXml, plainXml);
        var plainInstance = (string)input.Rows[1]["event_xml"];

        var (writer, context) = await RunSystemHealth(input);
        var (again, _) = await RunSystemHealth(input);

        AssertNoSecret(writer);
        var written = writer.Strings.Where(s => s.StartsWith("<event", StringComparison.Ordinal)).ToArray();
        Assert.Equal(2, written.Length);
        Assert.Contains("withheld (#4348)", written[0], StringComparison.Ordinal);
        Assert.Same(plainInstance, written[1]);
        Assert.Equal(writer.Values, again.Values);

        /* The stage-2 parse still reads every other field of the withheld event. */
        var raw = SystemHealthParser.ParseSignificantWait(canaryXml)!;
        var scrubbed = SystemHealthParser.ParseSignificantWait(written[0])!;
        Assert.Equal(raw.WaitType, scrubbed.WaitType);
        Assert.Equal(raw.DurationMs, scrubbed.DurationMs);
        Assert.Equal(raw.SignalDurationMs, scrubbed.SignalDurationMs);
        Assert.Equal(raw.SessionId, scrubbed.SessionId);
        Assert.Equal(raw.EventTime, scrubbed.EventTime);
        Assert.Contains("withheld (#4348)", scrubbed.QueryText, StringComparison.Ordinal);
        Assert.Equal(StatementScrubCanary.PlainStatement, SystemHealthParser.ParseSignificantWait(written[1])!.QueryText);

        /* The cycle's note carries that one value was named. */
        Assert.Contains(context.Measurements, m => m.Label == CollectorContext.StatementScrubNamedMeasurement && m.Value == 1);
    }

    // ── job_history.message ──

    private static DataTable JobHistoryInput(string canary, string plain)
    {
        var t = EventsTable(
            ("instance_id", typeof(long)), ("job_id", typeof(string)), ("job_name", typeof(string)),
            ("job_enabled", typeof(bool)), ("category_name", typeof(string)), ("step_id", typeof(int)),
            ("step_name", typeof(string)), ("run_status", typeof(int)), ("run_status_desc", typeof(string)),
            ("run_datetime", typeof(DateTime)), ("run_duration_seconds", typeof(long)),
            ("retries_attempted", typeof(int)), ("message", typeof(string)));
        long id = 1;
        foreach (var text in new[] { canary, plain })
        {
            t.Rows.Add(id++, "00000000-0000-0000-0000-000000000001", "job", true, "cat", 1, "step", 0, "Failed",
                Stamp, 61L, 0, text);
        }

        return t;
    }

    private static async Task<StatementScrubRecordingWriter> RunJobHistory(DataTable input, CollectorContext context)
    {
        var def = JobHistoryCollector.Instance;
        using var reader = input.CreateDataReader();
        var rows = await def.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            def.WritePayload(row, writer, context);
        }

        return writer;
    }

    [Fact]
    public async Task JobHistory_Message_IsWithheldWhenItEchoesAStatement_AndAPlainMessageIsUntouched()
    {
        var canaryMessage = "Executed as user: sa. " + StatementScrubCanary.CanaryStatement + " [SQLSTATE 42000] (Error 156)";
        var input = JobHistoryInput(canaryMessage, "The job succeeded.  The Job was invoked by Schedule 3.");
        var plainInstance = (string)input.Rows[1]["message"];

        var writer = await RunJobHistory(input, EventsContext());
        var again = await RunJobHistory(input, EventsContext());

        AssertNoSecret(writer);
        Assert.Contains(SensitiveStatements.PlaceholderText, writer.Strings);
        Assert.Contains(writer.Strings, s => ReferenceEquals(s, plainInstance));
        Assert.Equal(writer.Values, again.Values);
    }
}
