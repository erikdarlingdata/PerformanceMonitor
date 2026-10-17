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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The deadlock, blocking and regression evidence sections (#6002, #5632 Phase C C6) on a live store: one fact per behaviour the
/// issue pins, plus a metric that joins after the other sections froze and the report XML's fit under the section cap.
/// </summary>
public sealed class InvestigationEvidenceFamiliesLiveTests
{
    private const int ServerId = 7_602_001;
    private const string ServerName = "EvFamiliesSrv";
    private const string Db = "SalesDb";

    private static readonly DateTime AlertAt = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-5));

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime WindowStart => AlertAt - InvestigationEvidenceCapture.BreachLeadIn;

    private static DateTime WindowEnd => AlertAt + InvestigationEvidenceCapture.BreachTail;

    private static InvestigationAlert Alert(string family, string metric, DateTime at) =>
        new(1, ServerId, ServerName, family, metric, at, new[] { ServerId + ":" + metric }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    [Fact]
    public async Task ATemporaryObjectDeadlock_KeepsItsObjectAndIndexNames_AfterTheGraphRowIsGone()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantDeadlockAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), "tempdb.dbo.#t_____________0001", "PK_t");
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, InvestigationEvidenceCapture.DeadlockMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "deadlocks", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            /* The graph row ages out of retention after the capture. */
            await DarlingMcpTestData.ExecAsync(setup.Connection, ct, "DELETE FROM deadlocks WHERE server_id = $1", ServerId);
            setup.Clock.Advance(TimeSpan.FromMinutes(1));
            await setup.Capture.CaptureOnceAsync(ct);

            var section = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "deadlocks");
            Assert.Equal("ready", section.State);
            var deadlock = Assert.Single(JsonNode.Parse(section.ContentJson!)!["rows"]!.AsArray())!;
            var keylock = Assert.Single(deadlock["objects"]!.AsArray())!;
            Assert.Equal("tempdb.dbo.#t_____________0001", keylock["object_name"]!.GetValue<string>());
            Assert.Equal("PK_t", keylock["index_name"]!.GetValue<string>());
            Assert.Equal(2, deadlock["parties"]!.AsArray().Count);
        });
    }

    [Fact]
    public async Task TheHeadBlockersStatement_IsTheOneItsFrameOffsetsName_NotTheProcedureCall_AndIsExact()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* The cached statement the frame's handle and offsets name, collected the hour before the window. */
            await PlantCachedStatementAsync(setup.Connection, ct, WindowStart.AddHours(-1), "0x0200AB", 10, 90,
                "UPDATE dbo.Orders SET qty = qty + 1 WHERE id = @id");
            var report = BlockedProcessReport(blockingFrame: "<frame line=\"3\" stmtstart=\"10\" stmtend=\"90\" sqlhandle=\"0x0200ab\"/>");
            await PlantBlockedReportAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), report);
            var requestId = await OpenBlockingAndCaptureAsync(setup, ct);

            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var head = Assert.Single(Rows(evidence, "head_blocker"));
            Assert.Equal(62, head["spid"]!.GetValue<int>());
            Assert.Equal("exact", head["statement_source"]!.GetValue<string>());
            Assert.Equal("UPDATE dbo.Orders SET qty = qty + 1 WHERE id = @id", head["statement_text"]!.GetValue<string>());
            Assert.Equal("EXEC dbo.UpdateOrders @id = 1", head["input_buffer"]!.GetValue<string>());

            /* The chain row keeps the whole report, which fits the cap. */
            var chain = Assert.Single(Rows(evidence, "blocking_chains"));
            Assert.Equal(report, chain["report_xml"]!.GetValue<string>());
            Assert.Null(chain["report_xml_truncated"]);
        });
    }

    [Fact]
    public async Task AHeadBlockerWithNoFrame_KeepsItsInputBuffer_MarkedInferred()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* A sleeping blocker: its report has no execution stack, only the input buffer. */
            await PlantBlockedReportAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), BlockedProcessReport(blockingFrame: null));
            var requestId = await OpenBlockingAndCaptureAsync(setup, ct);

            var head = Assert.Single(Rows(await setup.Requests.ReadEvidenceAsync(requestId, ct), "head_blocker"));
            Assert.Equal("inferred", head["statement_source"]!.GetValue<string>());
            Assert.Equal("EXEC dbo.UpdateOrders @id = 1", head["statement_text"]!.GetValue<string>());
            Assert.Null(head["sql_handle"]);
        });
    }

    [Fact]
    public async Task ARegressionTargetThatStoppedRunning_IsKept_AndMarkedStopped()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* Both plans last ran before the window: the bad (newest) plan costs ten times the good one per call. */
            await PlantQueryStoreAsync(setup.Connection, ct, 501, 1, 1, cpuUs: 1_000, lastExec: WindowStart.AddHours(-3));
            await PlantQueryStoreAsync(setup.Connection, ct, 501, 2, 2, cpuUs: 10_000, lastExec: WindowStart.AddHours(-2));
            var requestId = await OpenQueryPerformanceAndCaptureAsync(setup, ct);

            var target = Assert.Single(Rows(await setup.Requests.ReadEvidenceAsync(requestId, ct), "regression_targets"));
            Assert.Equal(501, target["query_id"]!.GetValue<long>());
            Assert.Equal(1, target["good_plan_id"]!.GetValue<long>());
            Assert.Equal(2, target["bad_plan_id"]!.GetValue<long>());
            Assert.Equal(0, target["window_executions"]!.GetValue<long>());
            Assert.True(target["stopped_running"]!.GetValue<bool>());
        });
    }

    [Fact]
    public async Task GoodAndBadCompiledValues_BothAppear_AndNeitherSurvivesMetadataOnly()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantQueryStoreAsync(setup.Connection, ct, 502, 3, 1, cpuUs: 1_000, lastExec: AlertAt.AddMinutes(-4));
            await PlantQueryStoreAsync(setup.Connection, ct, 502, 4, 2, cpuUs: 8_000, lastExec: AlertAt.AddMinutes(-1));
            await PlantPlanAsync(setup.Connection, ct, 3, "N'Lisbon'");
            await PlantPlanAsync(setup.Connection, ct, 4, "N'Oslo'");
            var requestId = await OpenQueryPerformanceAndCaptureAsync(setup, ct);

            var target = Assert.Single(Rows(await setup.Requests.ReadEvidenceAsync(requestId, ct), "regression_targets"));
            Assert.False(target["stopped_running"]!.GetValue<bool>());
            var good = Assert.Single(target["good_compiled"]!.AsArray())!;
            var bad = Assert.Single(target["bad_compiled"]!.AsArray())!;
            Assert.Equal("@city", good["parameter"]!.GetValue<string>());
            Assert.Equal("N'Lisbon'", good["compiled_value"]!.GetValue<string>());
            Assert.Equal("N'Oslo'", bad["compiled_value"]!.GetValue<string>());

            var sources = await new PgInvestigationBriefSourceReader(setup.DataSource, utcNow: () => setup.Clock.Now).ReadAsync(requestId, ct);
            var full = InvestigationBriefComposer.Compose(sources!, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, setup.Clock.Now);
            Assert.Contains("Lisbon", full, StringComparison.Ordinal);
            Assert.Contains("Oslo", full, StringComparison.Ordinal);
            var masked = InvestigationBriefComposer.Compose(sources!, DarlingInvestigationBriefTools.Tools, McpDataDetail.MetadataOnly, setup.Clock.Now);
            Assert.Contains("regression_targets", masked, StringComparison.Ordinal);
            Assert.DoesNotContain("Lisbon", masked, StringComparison.Ordinal);
            Assert.DoesNotContain("Oslo", masked, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AMetricThatJoinsAfterTheSectionsFroze_GetsItsSectionOnALaterPass_WithNoSecondEvidenceReady()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* Opened on Blocking Detected: its two sections freeze empty (a true negative) and evidence is ready. */
            var requestId = await OpenBlockingAndCaptureAsync(setup, ct);
            var first = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.DoesNotContain(first, r => r.Section == "deadlocks");
            var chains = Assert.Single(first, r => r.Section == "blocking_chains");
            Assert.Equal("ready", chains.State);
            Assert.Equal("empty", JsonNode.Parse(chains.ContentJson!)!["status"]!.GetValue<string>());
            Assert.Equal(1, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));

            /* Deadlocks Detected joins the open request two minutes later; its window is built around ITS first alert. */
            var joinedAt = AlertAt.AddMinutes(2);
            await PlantDeadlockAsync(setup.Connection, ct, joinedAt.AddSeconds(-30), "dbo.Orders", "PK_Orders");
            Assert.Equal(requestId, (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, InvestigationEvidenceCapture.DeadlockMetric, joinedAt), ct)).RequestId);
            await PlantRunAsync(setup.Connection, ct, "deadlocks", joinedAt + InvestigationEvidenceCapture.BreachTail + TimeSpan.FromSeconds(5));
            setup.Clock.Advance(TimeSpan.FromMinutes(3));
            await setup.Capture.CaptureOnceAsync(ct);

            var later = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var deadlocks = Assert.Single(later, r => r.Section == "deadlocks");
            Assert.Equal("ready", deadlocks.State);
            var row = Assert.Single(JsonNode.Parse(deadlocks.ContentJson!)!["rows"]!.AsArray())!;
            Assert.Equal("dbo.Orders", row["objects"]!.AsArray()[0]!["object_name"]!.GetValue<string>());
            Assert.Equal(chains.ContentJson, later.Single(r => r.Section == "blocking_chains").ContentJson);
            Assert.Equal(1, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));
        });
    }

    [Fact]
    public async Task ReportXml_TakesWhatTheCapLeavesAfterTheChains_CutAndFlaggedAtTheCap_NeverLeftOut()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, AlertEngine.BlockingWatermarkMetric, AlertAt), ct)).RequestId!.Value;
            /* Three 14,000-character reports: more than 8,000 each, and 42,000 together, over the 32 KB cap. */
            var reports = Enumerable.Range(0, 3)
                .Select(i => "<r" + i + ">" + string.Concat(Enumerable.Repeat("<n>x</n>", 2_000))[..13_990] + "</r" + i + ">")
                .ToList();
            var rows = reports.Select((_, i) => new JsonObject { ["blocked_spid"] = 60 + i, ["report_xml"] = null }).ToList();

            var fitted = InvestigationEvidenceCapture.FitReportXml(WindowStart, WindowEnd, rows, reports);

            Assert.True(InvestigationEvidenceCapture.StoredBytes(fitted) <= InvestigationToolCore.MaxEvidenceBytes);
            var kept = JsonNode.Parse(fitted)!["rows"]!.AsArray();
            Assert.Equal(3, kept.Count);
            Assert.Equal(reports[0], kept[0]!["report_xml"]!.GetValue<string>());
            Assert.Null(kept[0]!["report_xml_truncated"]);
            Assert.Equal(reports[1], kept[1]!["report_xml"]!.GetValue<string>());
            var cut = kept[2]!["report_xml"]!.GetValue<string>();
            Assert.True(kept[2]!["report_xml_truncated"]!.GetValue<bool>());
            Assert.StartsWith(cut, reports[2], StringComparison.Ordinal);
            Assert.InRange(cut.Length, 1, reports[2].Length - 1);

            /* The store takes what the cap fits. */
            await setup.Requests.RecordEvidenceAsync(requestId, ServerId,
                new[] { new InvestigationEvidenceWrite("blocking_chains", "ready", fitted) }, ct);
            var stored = await new NpgsqlCommand(
                "SELECT octet_length(content::text) FROM config.investigation_evidence WHERE section = 'blocking_chains' AND request_id = '" + requestId + "'",
                setup.Connection).ExecuteScalarAsync(ct);
            Assert.True(Convert.ToInt32(stored, System.Globalization.CultureInfo.InvariantCulture) <= InvestigationToolCore.MaxEvidenceBytes);
        });
    }

    private static List<JsonNode> Rows(IReadOnlyList<InvestigationEvidenceRow> evidence, string section)
    {
        var row = Assert.Single(evidence, r => r.Section == section);
        Assert.Equal("ready", row.State);
        return JsonNode.Parse(row.ContentJson!)!["rows"]!.AsArray().Select(r => r!).ToList();
    }

    private static async Task<Guid> OpenBlockingAndCaptureAsync(Setup setup, CancellationToken ct)
    {
        var requestId = (await setup.Requests.OpenOrJoinAsync(
            Alert(InvestigationFamily.Blocking, AlertEngine.BlockingWatermarkMetric, AlertAt), ct)).RequestId!.Value;
        await PlantRunAsync(setup.Connection, ct, "blocked_process_report", WindowEnd.AddSeconds(5));
        await setup.Capture.CaptureOnceAsync(ct);
        return requestId;
    }

    private static async Task<Guid> OpenQueryPerformanceAndCaptureAsync(Setup setup, CancellationToken ct)
    {
        var requestId = (await setup.Requests.OpenOrJoinAsync(
            Alert(InvestigationFamily.QueryPerformance, "Long-Running Query", AlertAt), ct)).RequestId!.Value;
        await PlantRunAsync(setup.Connection, ct, "query_store", WindowEnd.AddSeconds(5));
        await setup.Capture.CaptureOnceAsync(ct);
        return requestId;
    }

    private static string BlockedProcessReport(string? blockingFrame) =>
        "<blocked-process-report monitorLoop=\"7\">"
        + "<blocked-process><process id=\"process1\" spid=\"61\" ecid=\"0\" status=\"suspended\" waitresource=\"KEY: 5:72057594 (a1)\">"
        + "<executionStack><frame line=\"1\" stmtstart=\"0\" stmtend=\"-1\" sqlhandle=\"0x0200cd\"/></executionStack>"
        + "<inputbuf>SELECT * FROM dbo.Orders WHERE id = 1</inputbuf></process></blocked-process>"
        + "<blocking-process><process status=\"" + (blockingFrame is null ? "sleeping" : "suspended") + "\" spid=\"62\" ecid=\"0\">"
        + (blockingFrame is null ? "<executionStack/>" : "<executionStack>" + blockingFrame + "</executionStack>")
        + "<inputbuf>EXEC dbo.UpdateOrders @id = 1</inputbuf></process></blocking-process>"
        + "</blocked-process-report>";

    private static Task PlantBlockedReportAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string reportXml) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid,
     blocked_ecid, blocking_ecid, blocking_status, database_name, blocked_sql_text, blocking_sql_text, blocked_process_report_xml)
VALUES ($1, $2, $3, $4, $5, 12000, 62, 61, 0, 0, NULL, $6, 'SELECT * FROM dbo.Orders WHERE id = 1', 'EXEC dbo.UpdateOrders @id = 1', $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddSeconds(5)), ServerId, ServerName, DarlingMcpTestData.Naive(at), Db, reportXml);

    private static Task PlantCachedStatementAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string sqlHandle, int start, int end, string text) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
                         sql_handle, plan_handle, statement_start_offset, statement_end_offset, query_text, delta_execution_count,
                         delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
VALUES ($1, $2, $3, $4, $5, '0xQH6002', '0xQPH6002', $6, '0xPLANH', $7, $8, $9, 1, 100, 100, 1, 1, 1)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, Db, sqlHandle, start, end, text);

    private static string DeadlockGraph(string objectName, string indexName) =>
        "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list>"
        + "<process id=\"process1\" spid=\"61\" lockMode=\"U\" isolationlevel=\"read committed (2)\" currentdbname=\"tempdb\">"
        + "<executionStack><frame procname=\"adhoc\" line=\"1\">UPDATE t SET x = 1</frame></executionStack><inputbuf>UPDATE t SET x = 1</inputbuf></process>"
        + "<process id=\"process2\" spid=\"62\" lockMode=\"X\" isolationlevel=\"read committed (2)\" currentdbname=\"tempdb\">"
        + "<executionStack><frame procname=\"adhoc\" line=\"1\">DELETE t</frame></executionStack><inputbuf>DELETE t</inputbuf></process>"
        + "</process-list><resource-list>"
        + "<keylock hobtid=\"72057594\" dbid=\"2\" objectname=\"" + objectName + "\" indexname=\"" + indexName + "\" id=\"lock1\" mode=\"X\" associatedObjectId=\"72057594\">"
        + "<owner-list><owner id=\"process2\" mode=\"X\"/></owner-list><waiter-list><waiter id=\"process1\" mode=\"U\" requestType=\"wait\"/></waiter-list></keylock>"
        + "</resource-list></deadlock>";

    private static Task PlantDeadlockAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string objectName, string indexName) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, 'tempdb', $5, 'process1', 'UPDATE t SET x = 1', $6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddSeconds(5)), ServerId, ServerName, DarlingMcpTestData.Naive(at),
            DeadlockGraph(objectName, indexName));

    private static Task PlantQueryStoreAsync(
        NpgsqlConnection connection, CancellationToken ct, long queryId, long planId, long intervalId, long cpuUs, DateTime lastExec) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time, query_hash, query_plan_hash,
     execution_count, avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count, avg_rowcount)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', NULL, $8, $9, $9, $10, '0xQH' || $6::text, '0xQPH' || $7::text, 50, $11, $11, false, 0, 1)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(lastExec.AddMinutes(1)), ServerId, ServerName, Db, queryId, planId, intervalId,
            DarlingMcpTestData.Naive(lastExec.AddMinutes(-30)), DarlingMcpTestData.Naive(lastExec), cpuUs);

    /// <summary>A stored Query Store plan whose one parameter, <c>@city</c>, was compiled for <paramref name="compiledValue"/>.</summary>
    private static async Task PlantPlanAsync(NpgsqlConnection connection, CancellationToken ct, long planId, string compiledValue)
    {
        var plan = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"SELECT * FROM dbo.Customers WHERE city = @city\" StatementId=\"1\" StatementType=\"SELECT\">"
            + "<QueryPlan CachedPlanSize=\"16\"><ParameterList><ColumnReference Column=\"@city\" ParameterDataType=\"nvarchar(40)\" ParameterCompiledValue=\""
            + System.Security.SecurityElement.Escape(compiledValue) + "\"/></ParameterList>"
            + "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";
        var digest = PayloadDimensions.Digest(plan);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            digest, plan, DarlingMcpTestData.Naive(AlertAt));
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) VALUES ($1, $2, $3, $4, $5, $6)",
            ServerId, Db, planId, digest, "0xQPH" + planId, DarlingMcpTestData.Naive(AlertAt));
    }

    private static Task PlantRunAsync(NpgsqlConnection connection, CancellationToken ct, string collector, DateTime at) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
              VALUES ($1, $2, $3, $4, $5, 5, 'SUCCESS')",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(at));

    private static async Task<long> EvidenceReadyRowsAsync(NpgsqlConnection connection, Guid requestId, CancellationToken ct) =>
        (long)(await new NpgsqlCommand(
            "SELECT count(*) FROM config.investigation_journal WHERE event = 'evidence_ready' AND request_id = '" + requestId + "'", connection)
            .ExecuteScalarAsync(ct))!;

    private sealed class TestClock
    {
        private DateTime _now = AlertAt.AddMinutes(1);
        public DateTime Now => _now;
        public void Advance(TimeSpan by) => _now += by;
    }

    private sealed record Setup(
        NpgsqlDataSource DataSource, NpgsqlConnection Connection, InvestigationRequestStore Requests,
        InvestigationEvidenceCapture Capture, TestClock Clock);

    private static async Task RunAsync(Func<Setup, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation evidence family live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var clock = new TestClock();
        var requests = new InvestigationRequestStore(dataSource, 30, () => clock.Now);
        var capture = new InvestigationEvidenceCapture(dataSource, requests, NullLogger.Instance, () => clock.Now, (_, _) => 5);
        await body(new Setup(dataSource, connection, requests, capture, clock), ct);
    }
}
