/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using ModelContextProtocol.Protocol;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, rows 16-23 of the read inventory: the blocking, deadlock and Extended Events family. Each case plants
/// <see cref="StatementScrubCanary"/> text in the rows the tool reads, runs the tool RAW (the control: the answer must
/// still HOLD the canary, so a clean filtered answer proves the filter and not an empty plant) and then sends that answer
/// through the host's output filter (<see cref="SensitiveStatementOutputFilter"/>, the instance
/// <c>DarlingMcpHostService</c> registers last), and asserts what a client would receive: no secret needle, the marker
/// where the statement was, and the plain statement beside it unchanged.
///
/// <para>The tools: <c>get_blocking</c>, <c>get_deadlocks</c>, <c>get_deadlock_detail</c>, <c>get_blocked_process_xml</c>,
/// <c>get_long_query_completions</c>, <c>get_default_trace_events</c>, <c>get_health_parser_significant_waits</c> and
/// <c>get_health_parser_severe_errors</c>. The class calls the filter directly rather than through
/// <see cref="StatementFilterCensus.FilterThroughHostAsync"/>: that helper's host also runs the GCF filter, which reads
/// <c>DARLING_OUTPUT_FORMAT</c> on every call, so a test in this live collection could be re-encoded by a class in the
/// <c>DarlingOutputFormatEnv</c> collection that sets it. The filter's slot in the host's list is pinned by
/// <c>SensitiveStatementHostFilterTests</c>.</para>
///
/// <para>The deadlock graph is the one field with two shapes: at <c>full_graph: true</c> the XML parses and only the named
/// statement text is replaced (precise); at the default the tool cuts the graph at 2000 characters, the cut text no longer
/// parses, and the filter withholds that value whole.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class StatementFilterBlockingReadsLiveTests
{
    private const string ServerName = "darling-ssf-blocking-reads";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "SsfBlockDb";
    private const string Marker = StatementFilterCensus.Marker;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The tool's answer through the host's output filter, as a client receives it.</summary>
    private static string Filtered(string raw)
    {
        var swept = SensitiveStatementOutputFilter.Sweep(new CallToolResult
        {
            Content = new List<ContentBlock> { new TextContentBlock { Text = raw } },
        });
        Assert.NotEqual(true, swept.IsError);
        return ((TextContentBlock)swept.Content![0]).Text;
    }

    private static void AssertFiltered(string label, string raw, string filtered)
    {
        try
        {
            StatementFilterCensus.AssertRawHoldsTheCanary(raw);
            StatementFilterCensus.AssertFilteredWithholdsTheCanary(raw, filtered);
            using var parsed = JsonDocument.Parse(filtered);
        }
        catch (Exception ex)
        {
            throw new Xunit.Sdk.XunitException(label + ": " + ex.Message);
        }
    }

    /// <summary>Every string value in <paramref name="json"/> that opens with <c>&lt;</c> (an embedded XML document).</summary>
    private static List<string> EmbeddedXml(string json)
    {
        var found = new List<string>();
        using var doc = JsonDocument.Parse(json);
        void Walk(JsonElement e)
        {
            switch (e.ValueKind)
            {
                case JsonValueKind.String:
                    var s = e.GetString();
                    if (s is not null && s.TrimStart().StartsWith('<')) found.Add(s);
                    break;
                case JsonValueKind.Object:
                    foreach (var p in e.EnumerateObject()) Walk(p.Value);
                    break;
                case JsonValueKind.Array:
                    foreach (var i in e.EnumerateArray()) Walk(i);
                    break;
            }
        }

        Walk(doc.RootElement);
        return found;
    }

    private static string DeadlockGraph(int paddingProcesses)
    {
        var sb = new StringBuilder();
        sb.Append("<deadlock><victim-list><victimProcess id=\"process1a\"/></victim-list><process-list>");
        sb.Append("<process id=\"process1a\" spid=\"61\" status=\"suspended\" currentdbname=\"").Append(Db).Append("\" loginname=\"l\">")
          .Append("<executionStack><frame procname=\"adhoc\" line=\"1\">").Append(StatementScrubCanary.CanaryStatement).Append("</frame></executionStack>")
          .Append("<inputbuf>").Append(StatementScrubCanary.CanaryStatement).Append("</inputbuf></process>");
        sb.Append("<process id=\"process2b\" spid=\"62\" status=\"suspended\" currentdbname=\"").Append(Db).Append("\" loginname=\"l\">")
          .Append("<executionStack><frame procname=\"adhoc\" line=\"1\">").Append(StatementScrubCanary.PlainStatement).Append("</frame></executionStack>")
          .Append("<inputbuf>").Append(StatementScrubCanary.PlainStatement).Append("</inputbuf></process>");
        for (int i = 0; i < paddingProcesses; i++)
        {
            sb.Append("<process id=\"processpad").Append(i).Append("\" spid=\"").Append(100 + i % 900).Append("\" status=\"suspended\" currentdbname=\"").Append(Db).Append("\" loginname=\"l\">")
              .Append("<executionStack><frame procname=\"adhoc\" line=\"1\">SELECT canary_pad_ssf FROM dbo.pad WHERE id = @c</frame></executionStack>")
              .Append("<inputbuf>SELECT canary_pad_ssf FROM dbo.pad WHERE id = @c</inputbuf></process>");
        }

        sb.Append("</process-list><resource-list><keylock objectname=\"").Append(Db).Append(".dbo.t\"><owner-list><owner id=\"process2b\" mode=\"X\"/></owner-list>")
          .Append("<waiter-list><waiter id=\"process1a\" mode=\"S\"/></waiter-list></keylock></resource-list></deadlock>");
        return sb.ToString();
    }

    private static string BlockedProcessReport() =>
        "<blocked-process-report monitorLoop=\"1\"><blocked-process>"
        + "<process id=\"processB1\" waittime=\"12000\" status=\"suspended\" spid=\"61\" ecid=\"0\" clientapp=\"app\" hostname=\"h\" loginname=\"l\">"
        + "<executionStack><frame line=\"1\" stmtstart=\"0\" sqlhandle=\"0x01\">" + StatementScrubCanary.CanaryStatement + "</frame></executionStack>"
        + "<inputbuf>" + StatementScrubCanary.CanaryStatement + "</inputbuf></process></blocked-process>"
        + "<blocking-process><process status=\"suspended\" spid=\"62\" ecid=\"0\" clientapp=\"app\" hostname=\"h\" loginname=\"l\">"
        + "<executionStack/><inputbuf>" + StatementScrubCanary.PlainStatement + "</inputbuf></process></blocking-process></blocked-process-report>";

    [Fact]
    public async Task TheEightBlockingAndEventReads_AnswerWithTheCanaryStatementWithheld()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-read filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-9));
            string wait = System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", "wait_info.xml"))
                .Replace("SELECT * FROM dbo.big_table WHERE id = @p1;", StatementScrubCanary.CanaryStatement, StringComparison.Ordinal);
            string error = System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", "error_reported.xml"))
                .Replace("The operating system returned error 21 to SQL Server during a read at offset 0x00000c60000 in file 'D:\\data\\prod.mdf'.",
                    "The statement failed: " + StatementScrubCanary.CanaryStatement, StringComparison.Ordinal);
            Assert.Contains("S3cret-canary-ssf", wait, StringComparison.Ordinal);
            Assert.Contains("S3cret-canary-ssf", error, StringComparison.Ordinal);

            /* row 16 and row 19: one blocked-process report with the canary on the blocked side. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid,
     blocked_ecid, blocking_ecid, blocking_status, database_name, blocked_sql_text, blocking_sql_text, blocked_process_report_xml)
VALUES ($1, $2, $3, $4, $5, 12000, 62, 61, 0, 0, 'suspended', $6, $7, $8, $9)",
                CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, t, Db,
                StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement, BlockedProcessReport());

            /* rows 17 and 18: one deadlock with the canary as the victim's statement and in its graph. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, Db, t, "process1a", StatementScrubCanary.CanaryStatement, DeadlockGraph(0));

            /* row 20 */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, t, "rpc_completed", Db, 5_000_000L, StatementScrubCanary.CanaryStatement);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, t, "rpc_completed", Db, 4_000_000L, StatementScrubCanary.PlainStatement);

            /* row 21: a severe ErrorLog entry whose text_data is the canary, and a plain one beside it. */
            foreach (var (text, offset) in new[] { (StatementScrubCanary.CanaryStatement, 0), (StatementScrubCanary.PlainStatement, 1) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, severity, text_data)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, t.AddSeconds(offset), "ErrorLog", 20, text);
            }

            /* rows 22 and 23 */
            foreach (var (xml, type) in new[] { (wait, SystemHealthParser.WaitInfoEvent), (error, SystemHealthParser.ErrorReportedEvent) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, t, type, xml);
            }

            /* Every row is checked before the test fails, so one run names each row that leaks. */
            var failures = new List<string>();
            void Check(string label, string raw, Action<string>? more = null)
            {
                try
                {
                    string filtered = Filtered(raw);
                    AssertFiltered(label, raw, filtered);
                    more?.Invoke(filtered);
                }
                catch (Exception ex)
                {
                    failures.Add(label + ": " + ex.Message.Split('\n')[0]);
                }
            }

            Check("16 get_blocking", await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName, cancellationToken: ct));
            Check("17 get_deadlocks", await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName, cancellationToken: ct));
            Check("18 get_deadlock_detail", await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, cancellationToken: ct));

            /* row 19: the report XML still parses, the canary's inputbuf and frame hold the marker, and the plain side is intact. */
            Check("19 get_blocked_process_xml", await DarlingMcpBlockingTools.GetBlockedProcessXml(postgres, ServerName, cancellationToken: ct), filtered =>
            {
                var reports = EmbeddedXml(filtered);
                Assert.Single(reports);
                var bprDoc = XDocument.Parse(reports[0]);
                var inputbufs = bprDoc.Descendants("inputbuf").Select(e => e.Value).ToList();
                Assert.Equal(new[] { Marker, StatementScrubCanary.PlainStatement }, inputbufs);
                Assert.DoesNotContain(bprDoc.Descendants("frame"), f => f.Value.Contains("S3cret", StringComparison.Ordinal));
            });

            Check("20 get_long_query_completions", await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, cancellationToken: ct));
            Check("21 get_default_trace_events", await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, ServerName, cancellationToken: ct));
            Check("22 get_health_parser_significant_waits", await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, ServerName, cancellationToken: ct));
            Check("23 get_health_parser_severe_errors", await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName, cancellationToken: ct));
            Assert.True(failures.Count == 0, string.Join(" | ", failures));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// <c>get_deadlock_detail</c>'s graph, both ways. With <c>full_graph</c> true the answer carries the whole graph: it parses,
    /// the victim's frame and inputbuf hold the marker, and the other process, its statement and the lock resource are the
    /// stored ones (precise). By default the tool cuts the graph at 2000 characters before the filter sees it, the cut text is
    /// not a document, and the graph value is withheld whole (the marker alone), while the rest of the answer stays.
    /// </summary>
    [Fact]
    public async Task GetDeadlockDetail_FullGraphIsPrecise_AndTheDefaultCutGraphIsWithheldWhole()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-read filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-9));
            /* 20 padding processes put the graph well past the 2000-character preview; the canary processes come first, so it sits inside the cut. */
            string graph = DeadlockGraph(20);
            Assert.True(graph.Length > 2500, "the graph is " + graph.Length + " characters");
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, Db, t, "process1a", StatementScrubCanary.CanaryStatement, graph);

            string full = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, full_graph: true, cancellationToken: ct);
            string fullFiltered = Filtered(full);
            AssertFiltered("full_graph true", full, fullFiltered);
            var fullGraphs = EmbeddedXml(fullFiltered);
            Assert.Single(fullGraphs);
            var doc = XDocument.Parse(fullGraphs[0]);
            var victim = doc.Descendants("process").Single(p => (string?)p.Attribute("id") == "process1a");
            Assert.Equal(Marker, victim.Element("inputbuf")!.Value);
            Assert.Equal(Marker, victim.Descendants("frame").Single().Value);
            var other = doc.Descendants("process").Single(p => (string?)p.Attribute("id") == "process2b");
            Assert.Equal(StatementScrubCanary.PlainStatement, other.Element("inputbuf")!.Value);
            Assert.Equal(StatementScrubCanary.PlainStatement, other.Descendants("frame").Single().Value);
            Assert.Equal(22, doc.Descendants("process").Count());
            Assert.Equal(Db + ".dbo.t", (string?)doc.Descendants("keylock").Single().Attribute("objectname"));

            string cut = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, cancellationToken: ct);
            string cutFiltered = Filtered(cut);
            AssertFiltered("full_graph false", cut, cutFiltered);
            using (var rawDoc = JsonDocument.Parse(cut))
            {
                var rawRow = rawDoc.RootElement.GetProperty("deadlocks")[0];
                Assert.True(rawRow.GetProperty("deadlock_graph_xml_truncated").GetBoolean());
                Assert.Contains("S3cret-canary-ssf", rawRow.GetProperty("deadlock_graph_xml").GetString(), StringComparison.Ordinal);
            }

            using var cutDoc = JsonDocument.Parse(cutFiltered);
            var cutRow = cutDoc.RootElement.GetProperty("deadlocks")[0];
            Assert.Equal(Marker, cutRow.GetProperty("deadlock_graph_xml").GetString());
            Assert.Equal("process1a", cutRow.GetProperty("victim_process_id").GetString());
            Assert.True(cutRow.GetProperty("deadlock_graph_xml_truncated").GetBoolean());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The filter's cost on <c>get_deadlock_detail</c> at <c>full_graph: true</c> over a graph of about 4 MB (the answer carries the
    /// graph and the per-process rows built from it). The ceiling is loose (a slow runner is not a failure); it catches a read that
    /// spends the filter's whole budget and so answers with every value withheld. The answer must still hold the precise shape.
    /// Set <c>SSF_L6_TIMINGS=1</c> to fail with the measured milliseconds.
    /// </summary>
    [Fact]
    public async Task GetDeadlockDetail_OnAFourMegabyteGraph_StaysPreciseInsideTheCeiling()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-read filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-9));
            string graph = DeadlockGraph(14_000);
            Assert.InRange(graph.Length, 4_000_000, 5_500_000);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), t.AddSeconds(5), ServerId, ServerName, Db, t, "process1a", StatementScrubCanary.CanaryStatement, graph);

            var read = Stopwatch.StartNew();
            string raw = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, full_graph: true, cancellationToken: ct);
            read.Stop();
            var filter = Stopwatch.StartNew();
            string filtered = Filtered(raw);
            filter.Stop();
            string timings = string.Create(CultureInfo.InvariantCulture,
                $"graph {graph.Length} chars, answer {raw.Length} chars: tool {read.ElapsedMilliseconds} ms, filter {filter.ElapsedMilliseconds} ms");

            Assert.True(filter.Elapsed < TimeSpan.FromSeconds(10), timings);
            AssertFiltered("4 MB graph", raw, filtered);
            var graphs = EmbeddedXml(filtered);
            Assert.Single(graphs);
            var doc = XDocument.Parse(graphs[0]);
            Assert.Equal(Marker, doc.Descendants("process").First(p => (string?)p.Attribute("id") == "process1a").Element("inputbuf")!.Value);
            Assert.Equal(StatementScrubCanary.PlainStatement,
                doc.Descendants("process").First(p => (string?)p.Attribute("id") == "process2b").Element("inputbuf")!.Value);
            Assert.True(doc.Descendants("process").Count() >= 14_000, timings);

            if (Environment.GetEnvironmentVariable("SSF_L6_TIMINGS") == "1")
                Assert.Fail(timings);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[]
        {
            "blocked_process_reports", "deadlocks", "long_query_completions", "default_trace_events", "system_health_events", "servers",
        })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}
