/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, lane R4: the blocking and deadlock collectors judge each statement string where it first enters a row. The
/// census cases below plant the canary in the collector's INPUT (a <c>DataTableReader</c>), run the read and
/// <c>WritePayload</c> through <see cref="StatementScrubRecordingWriter"/>, and check what was written. Columns covered:
/// blocked_process_report blocked_sql_text, blocking_sql_text, blocked_process_report_xml, blocked_query_plan_xml,
/// blocking_query_plan_xml; deadlocks victim_sql_text, deadlock_graph_xml, victim_query_plan_xml; dmv_blocking_snapshot
/// blocked_sql_text, blocking_sql_text.
/// </summary>
public sealed partial class StatementCollectionCensusTests
{
    private static readonly DateTime EventTime = new(2026, 10, 6, 11, 59, 0, DateTimeKind.Utc);
    private const string Frame = "<frame line=\"1\" stmtstart=\"10\" stmtend=\"90\" sqlhandle=\"0x0200AA01\">";

    private static string BlockedReportXml(string blockedText, string blockingText) =>
        "<blocked-process-report monitorLoop=\"7\">"
        + "<blocked-process><process spid=\"55\" ecid=\"0\" waittime=\"9000\" currentdbname=\"db1\">"
        + "<executionStack>" + Frame + System.Security.SecurityElement.Escape(blockedText) + "</frame></executionStack>"
        + "<inputbuf>" + System.Security.SecurityElement.Escape(blockedText) + "</inputbuf></process></blocked-process>"
        + "<blocking-process><process spid=\"66\" ecid=\"0\"><executionStack/>"
        + "<inputbuf>" + System.Security.SecurityElement.Escape(blockingText) + "</inputbuf></process></blocking-process>"
        + "</blocked-process-report>";

    private static string DeadlockGraph(string victimText, string otherText) =>
        "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list>"
        + "<process id=\"process1\" currentdbname=\"db1\"><executionStack>" + Frame
        + System.Security.SecurityElement.Escape(victimText) + "</frame></executionStack>"
        + "<inputbuf>" + System.Security.SecurityElement.Escape(victimText) + "</inputbuf></process>"
        + "<process id=\"process2\"><executionStack/><inputbuf>" + System.Security.SecurityElement.Escape(otherText) + "</inputbuf></process>"
        + "</process-list><resource-list>"
        + "<keylock hobtid=\"1\" dbid=\"5\" objectname=\"db1.dbo.t1\" id=\"lock1\" mode=\"X\"><owner-list/><waiter-list/></keylock>"
        + "<keylock hobtid=\"2\" dbid=\"5\" objectname=\"db1.dbo.t2\" id=\"lock2\" mode=\"X\"><owner-list/><waiter-list/></keylock>"
        + "</resource-list></deadlock>";

    private static CollectorContext BlockingContext() => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Utc),
        Deltas = null!,
        CapturePlanXml = true,
        Target = new CollectorTargetInfo(),
    };

    private static DataTableReader BprReader(params (string Xml, string? BlockedPlan, string? BlockingPlan)[] reports)
    {
        var dataSet = new DataSet();
        var payload = new DataTable("payload");
        payload.Columns.Add("event_time", typeof(DateTime));
        payload.Columns.Add("blocked_process_report_xml", typeof(string));
        payload.Columns.Add("object_id", typeof(int));
        payload.Columns.Add("database_id", typeof(int));
        payload.Columns.Add("contentious_object", typeof(string));
        payload.Columns.Add("blocked_query_plan_xml", typeof(string));
        payload.Columns.Add("blocking_query_plan_xml", typeof(string));
        foreach (var (xml, blocked, blocking) in reports)
        {
            payload.Rows.Add(EventTime, xml, DBNull.Value, 6, DBNull.Value, (object?)blocked ?? DBNull.Value, (object?)blocking ?? DBNull.Value);
        }

        dataSet.Tables.Add(payload);
        var gate = new DataTable("gate");
        gate.Columns.Add("execution_count", typeof(long));
        gate.Columns.Add("gated", typeof(bool));
        gate.Rows.Add(1L, false);
        dataSet.Tables.Add(gate);
        return dataSet.CreateDataReader();
    }

    private static DataTableReader DeadlockReader(params (DateTime Time, string Graph, string? Plan)[] graphs)
    {
        var dataSet = new DataSet();
        var payload = new DataTable("payload");
        payload.Columns.Add("deadlock_time", typeof(DateTime));
        payload.Columns.Add("victim_process_id", typeof(string));
        payload.Columns.Add("deadlock_graph_xml", typeof(string));
        payload.Columns.Add("victim_query_plan_xml", typeof(string));
        payload.Columns.Add("source_database_name", typeof(string));
        foreach (var (time, graph, plan) in graphs)
        {
            payload.Rows.Add(time, "process1", graph, (object?)plan ?? DBNull.Value, DBNull.Value);
        }

        dataSet.Tables.Add(payload);
        var gate = new DataTable("gate");
        gate.Columns.Add("execution_count", typeof(long));
        gate.Columns.Add("gated", typeof(bool));
        gate.Rows.Add(1L, false);
        dataSet.Tables.Add(gate);
        return dataSet.CreateDataReader();
    }

    private static async Task<(List<BlockedProcessReportCollector.Row> Rows, StatementScrubRecordingWriter Writer)> RunBprAsync(
        CollectorContext context, DataTableReader reader)
    {
        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            BlockedProcessReportCollector.Instance.WritePayload(row, writer, context);
        }

        return (rows, writer);
    }

    private static async Task<(List<DeadlocksCollector.Row> Rows, StatementScrubRecordingWriter Writer)> RunDeadlocksAsync(
        CollectorContext context, DataTableReader reader)
    {
        var rows = await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            DeadlocksCollector.Instance.WritePayload(row, writer, context);
        }

        return (rows, writer);
    }

    private static void AssertNoNeedle(StatementScrubRecordingWriter writer)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.False(writer.AnyStringContains(needle), "a written value still holds " + needle);
        }
    }

    private static void AssertPlanScrubbed(string? plan)
    {
        Assert.NotNull(plan);
        XDocument.Parse(plan!);
        Assert.Contains(SensitiveStatements.PlaceholderText, plan, StringComparison.Ordinal);
        Assert.Contains("canary_plain_ssf", plan, StringComparison.Ordinal);
        Assert.Contains("param-canary-ssf", plan, StringComparison.Ordinal);
    }

    // ── blocked_process_report: blocked_sql_text, blocking_sql_text, blocked_process_report_xml,
    //    blocked_query_plan_xml, blocking_query_plan_xml ──

    [Fact]
    public async Task BlockedProcessReport_blocked_sql_text_blocking_sql_text_blocked_process_report_xml_and_both_plans_AreWithheld()
    {
        var xml = BlockedReportXml(StatementScrubCanary.CanaryStatement, StatementScrubCanary.CanaryStatement);
        var context = BlockingContext();

        var (rows, writer) = await RunBprAsync(context, BprReader((xml, StatementScrubCanary.CanaryPlan(), StatementScrubCanary.CanaryPlan())));

        var row = Assert.Single(rows);
        AssertNoNeedle(writer);
        Assert.Equal(SensitiveStatements.PlaceholderText, row.BlockedSqlText);
        Assert.Equal(SensitiveStatements.PlaceholderText, row.BlockingSqlText);
        AssertPlanScrubbed(row.BlockedQueryPlanXml);
        AssertPlanScrubbed(row.BlockingQueryPlanXml);

        // The stored report still parses, holds the marker and keeps the frame attributes a reader uses.
        var stored = XElement.Parse(row.ReportXml!);
        Assert.Contains(SensitiveStatements.PlaceholderText, row.ReportXml, StringComparison.Ordinal);
        var frame = stored.Descendants("frame").Single();
        Assert.Equal("0x0200AA01", (string?)frame.Attribute("sqlhandle"));
        Assert.Equal("10", (string?)frame.Attribute("stmtstart"));
        Assert.Equal("55", (string?)stored.Element("blocked-process")!.Element("process")!.Attribute("spid"));

        // A second run over the same input stores the same text (identity is computed from the filtered value).
        var (_, again) = await RunBprAsync(BlockingContext(), BprReader((xml, StatementScrubCanary.CanaryPlan(), StatementScrubCanary.CanaryPlan())));
        Assert.Equal(writer.Strings.ToList(), again.Strings.ToList());

        // The cycle reports what the filter did.
        Assert.Contains(context.Measurements, m => m.Label == CollectorContext.StatementScrubNamedMeasurement && m.Value > 0);
    }

    [Fact]
    public async Task BlockedProcessReport_AReportWithPlainStatements_IsStoredByteForByte()
    {
        var xml = BlockedReportXml(StatementScrubCanary.PlainStatement, StatementScrubCanary.PlainStatement);

        var (rows, writer) = await RunBprAsync(BlockingContext(), BprReader((xml, null, null)));

        var row = Assert.Single(rows);
        Assert.Same(xml, row.ReportXml);
        Assert.Equal(StatementScrubCanary.PlainStatement, row.BlockedSqlText);
        Assert.Equal(StatementScrubCanary.PlainStatement, row.BlockingSqlText);
        Assert.Contains(StatementScrubCanary.PlainStatement, writer.Strings);
    }

    [Fact]
    public async Task BlockedProcessReport_AResolvedProcedureName_IsJudgedAgain()
    {
        // The resolved "schema.object" replaces a placeholder that was not statement text; it is a new string entering
        // the row, so it is judged again. A name that reads as a statement is withheld.
        var context = BlockingContext();
        var xml = BlockedReportXml("Proc [Database Id = 5 Object Id = 7]", "Proc [Database Id = 5 Object Id = 8]");
        var (rows, _) = await RunBprAsync(context, BprReader((xml, null, null)));
        Assert.Equal("Proc [Database Id = 5 Object Id = 7]", rows[0].BlockedSqlText);

        var supplemental = new DataTable();
        supplemental.Columns.Add("database_id", typeof(int));
        supplemental.Columns.Add("object_id", typeof(int));
        supplemental.Columns.Add("database_name", typeof(string));
        supplemental.Columns.Add("schema_name", typeof(string));
        supplemental.Columns.Add("object_name", typeof(string));
        supplemental.Rows.Add(5, 7, "db1", "dbo", "usp_plain");
        supplemental.Rows.Add(5, 8, "db1", "dbo", "usp_other");
        await BlockedProcessReportCollector.Instance.ApplySupplementalAsync(rows, supplemental.CreateDataReader(), context, CancellationToken.None);

        Assert.Contains("usp_plain", rows[0].BlockedSqlText, StringComparison.Ordinal);
        Assert.DoesNotContain("Proc [", rows[0].BlockedSqlText, StringComparison.Ordinal);
    }

    // ── deadlocks: victim_sql_text, deadlock_graph_xml, victim_query_plan_xml ──

    [Fact]
    public async Task Deadlocks_victim_sql_text_deadlock_graph_xml_and_victim_query_plan_xml_AreWithheld()
    {
        var graph = DeadlockGraph(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement);
        var context = BlockingContext();

        var (rows, writer) = await RunDeadlocksAsync(context, DeadlockReader((EventTime, graph, StatementScrubCanary.CanaryPlan())));

        var row = Assert.Single(rows);
        AssertNoNeedle(writer);
        Assert.Equal(SensitiveStatements.PlaceholderText, row.VictimSqlText);
        AssertPlanScrubbed(row.VictimQueryPlanXml);

        // The stored graph parses, holds the marker, keeps the frame handles, keeps the other process's statement,
        // and the object extractor finds the same objects it finds in the raw graph.
        var stored = XElement.Parse(row.GraphXml!);
        Assert.Contains(SensitiveStatements.PlaceholderText, row.GraphXml, StringComparison.Ordinal);
        Assert.Contains(StatementScrubCanary.PlainStatement, row.GraphXml, StringComparison.Ordinal);
        var frame = stored.Descendants("frame").Single();
        Assert.Equal("0x0200AA01", (string?)frame.Attribute("sqlhandle"));
        Assert.Equal("10", (string?)frame.Attribute("stmtstart"));
        var objects = DeadlockObjectExtractor.FromGraphXml(row.GraphXml);
        Assert.NotEmpty(objects);
        Assert.Equal(DeadlockObjectExtractor.FromGraphXml(graph), objects);
    }

    [Fact]
    public async Task Deadlocks_ANamedDeadlockReadTwice_IsDroppedTheSecondTime()
    {
        var graph = DeadlockGraph(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement);

        var (first, _) = await RunDeadlocksAsync(BlockingContext(), DeadlockReader((EventTime, graph, null)));
        var stored = new HashSet<(DateTime Time, string Graph)>(
            first.Select(r => DeadlocksCollector.Instance.GetIdentity(r)!.Value));
        var (second, _) = await RunDeadlocksAsync(BlockingContext(), DeadlockReader((EventTime, graph, null)));

        Assert.Empty(DeadlocksCollector.Instance.DropAlreadyStored(second, stored));
    }

    [Fact]
    public async Task Deadlocks_TwoWholeMarkerGraphsAtTheSameTime_AreBothKept_AndAReReadStoresThemAgain()
    {
        // A graph that does not parse and holds the canary is withheld whole. Two different ones at the same time
        // share the marker, so they have no identity and neither is dropped.
        var truncatedA = "<deadlock><process-list><process id=\"process1\"><inputbuf>" + StatementScrubCanary.CanaryStatement;
        var truncatedB = "<deadlock><process-list><process id=\"process9\"><inputbuf>" + StatementScrubCanary.CanaryStatement;

        var (rows, writer) = await RunDeadlocksAsync(BlockingContext(), DeadlockReader((EventTime, truncatedA, null), (EventTime, truncatedB, null)));

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(SensitiveStatements.PlaceholderText, r.GraphXml));
        Assert.All(rows, r => Assert.Null(DeadlocksCollector.Instance.GetIdentity(r)));
        AssertNoNeedle(writer);
        Assert.Equal(2, DeadlocksCollector.Instance.DropAlreadyStored(rows, new HashSet<(DateTime Time, string Graph)>()).Count);

        // Recorded for the overlap (r2 L-H): a whole-marker graph has no stored identity, so every re-read inside
        // the 10-minute overlap that withholds it whole again stores one more row. Three reads leave three rows.
        var stored = new HashSet<(DateTime Time, string Graph)>();
        var kept = 0;
        for (var cycle = 0; cycle < 3; cycle++)
        {
            var (again, _) = await RunDeadlocksAsync(BlockingContext(), DeadlockReader((EventTime, truncatedA, null)));
            var survivors = DeadlocksCollector.Instance.DropAlreadyStored(again, stored);
            kept += survivors.Count;
        }

        Assert.Equal(3, kept);
    }

    [Fact]
    public async Task Deadlocks_APlainGraph_IsStoredByteForByte_WithTheSameIdentityAcrossRuns()
    {
        var graph = DeadlockGraph(StatementScrubCanary.PlainStatement, StatementScrubCanary.PlainStatement);

        var (rows, _) = await RunDeadlocksAsync(BlockingContext(), DeadlockReader((EventTime, graph, null)));
        var (again, _) = await RunDeadlocksAsync(BlockingContext(), DeadlockReader((EventTime, graph, null)));

        Assert.Same(graph, rows[0].GraphXml);
        Assert.Equal(StatementScrubCanary.PlainStatement, rows[0].VictimSqlText);
        Assert.Equal(DeadlocksCollector.Instance.GetIdentity(rows[0]), DeadlocksCollector.Instance.GetIdentity(again[0]));
    }

    // ── dmv_blocking_snapshot: blocked_sql_text, blocking_sql_text ──

    [Fact]
    public async Task DmvBlockingSnapshot_blocked_sql_text_and_blocking_sql_text_AreWithheld()
    {
        var table = new DataTable();
        foreach (var (name, type) in new (string, Type)[]
        {
            ("database_name", typeof(string)), ("blocked_spid", typeof(int)), ("blocked_ecid", typeof(int)),
            ("blocked_last_tran_started", typeof(DateTime)), ("blocking_spid", typeof(int)), ("blocking_ecid", typeof(int)),
            ("blocking_last_tran_started", typeof(DateTime)), ("wait_time_ms", typeof(long)), ("lock_mode", typeof(string)),
            ("blocking_status", typeof(string)), ("contentious_object", typeof(string)), ("blocked_sql_text", typeof(string)),
            ("blocking_sql_text", typeof(string)), ("blocked_login_name", typeof(string)), ("blocked_host_name", typeof(string)),
            ("blocked_client_app", typeof(string)), ("blocking_login_name", typeof(string)), ("blocking_host_name", typeof(string)),
            ("blocking_client_app", typeof(string)),
        })
        {
            table.Columns.Add(name, type);
        }

        table.Rows.Add("db1", 55, 0, EventTime, 66, 0, EventTime, 9000L, "X", "sleeping", "t1",
            StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement, "l1", "h1", "a1", "l2", "h2", "a2");

        var context = BlockingContext();
        var rows = await DmvBlockingSnapshotCollector.Instance.ReadAsync(table.CreateDataReader(), context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            DmvBlockingSnapshotCollector.Instance.WritePayload(row, writer, context);
        }

        AssertNoNeedle(writer);
        Assert.Equal(SensitiveStatements.PlaceholderText, rows[0].BlockedSqlText);
        Assert.Equal(StatementScrubCanary.PlainStatement, rows[0].BlockingSqlText);
        Assert.Contains(StatementScrubCanary.PlainStatement, writer.Strings);
    }

    // ── timing: a 4 MB ring buffer of reports and graphs ──

    [Fact]
    public async Task ABlockingRingBufferOfAboutFourMegabytes_IsJudgedWellUnderTheBudget()
    {
        var reports = new List<(string, string?, string?)>();
        var size = 0;
        for (var i = 0; size < 4_000_000; i++)
        {
            var text = i % 5 == 0 ? StatementScrubCanary.CanaryStatement : "SELECT col" + i + " FROM dbo.t" + i + " WHERE c = @c";
            var xml = BlockedReportXml(text, StatementScrubCanary.PlainStatement + new string('x', 1000));
            reports.Add((xml, null, null));
            size += xml.Length;
        }

        var watch = Stopwatch.StartNew();
        var (rows, writer) = await RunBprAsync(BlockingContext(), BprReader(reports.ToArray()));
        var bprSeconds = watch.Elapsed.TotalSeconds;

        var graphs = new List<(DateTime, string, string?)>();
        size = 0;
        for (var i = 0; size < 4_000_000; i++)
        {
            var text = i % 5 == 0 ? StatementScrubCanary.CanaryStatement : "SELECT col" + i + " FROM dbo.t" + i + " WHERE c = @c";
            var graph = DeadlockGraph(text, StatementScrubCanary.PlainStatement + new string('x', 1000));
            graphs.Add((EventTime.AddSeconds(i), graph, null));
            size += graph.Length;
        }

        watch.Restart();
        var (graphRows, graphWriter) = await RunDeadlocksAsync(BlockingContext(), DeadlockReader(graphs.ToArray()));
        var deadlockSeconds = graphWriter.Strings.Count() >= 0 ? watch.Elapsed.TotalSeconds : 0;

        TestContext.Current.SendDiagnosticMessage(
            $"R4 timing: {rows.Count} reports in {bprSeconds:F2} s, {graphRows.Count} graphs in {deadlockSeconds:F2} s");
        AssertNoNeedle(writer);
        AssertNoNeedle(graphWriter);
        Assert.True(bprSeconds < 5 && deadlockSeconds < 5, $"{bprSeconds:F2} s for reports, {deadlockSeconds:F2} s for graphs");
    }
}
