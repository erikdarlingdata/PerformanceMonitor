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
    public async Task AClientTimeoutsRequest_FreezesTheTopAppsHostsAndStatements_OfTheAlertWindow()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* #6553: 3 sampled timeouts from WS-ERIK and 1 from APP01 in the window, plus a row OUTSIDE it that must not count. */
            await PlantAttentionAsync(setup.Connection, ct, 1, AlertAt.AddMinutes(-2), "count", 4, null, null, null, null, null);
            await PlantAttentionAsync(setup.Connection, ct, 2, AlertAt.AddMinutes(-2), "sample", 0, Db, "SSMS", "WS-ERIK", "fp-a", "SELECT 'CANARYTEXT' FROM t");
            await PlantAttentionAsync(setup.Connection, ct, 3, AlertAt.AddMinutes(-2), "sample", 0, Db, "SSMS", "WS-ERIK", "fp-a", "SELECT 'CANARYTEXT' FROM t");
            await PlantAttentionAsync(setup.Connection, ct, 4, AlertAt.AddMinutes(-2), "sample", 0, Db, "SSMS", "WS-ERIK", "fp-a", "SELECT 'CANARYTEXT' FROM t");
            await PlantAttentionAsync(setup.Connection, ct, 5, AlertAt.AddMinutes(-2), "sample", 0, Db, "web", "APP01", "fp-b", "SELECT 2");
            await PlantAttentionAsync(setup.Connection, ct, 6, AlertAt.AddHours(-3), "sample", 0, Db, "old-app", "OLD01", "fp-old", "SELECT 3");
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.QueryPerformance, AlertFamily.ClientTimeoutsMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "attention", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var row = Assert.Single(Rows(await setup.Requests.ReadEvidenceAsync(requestId, ct), "client_timeouts"));
            Assert.Equal(4, row["count"]!.GetValue<long>());
            var hosts = row["top_hosts"]!.AsArray();
            Assert.Equal(new[] { "WS-ERIK", "APP01" }, hosts.Select(h => h!["client_hostname"]!.GetValue<string>()));
            Assert.Equal(3, hosts[0]!["sampled_events"]!.GetValue<long>());
            Assert.Equal(new[] { "SSMS", "web" }, row["top_client_apps"]!.AsArray().Select(a => a!["client_app_name"]!.GetValue<string>()));
            var statements = row["top_statements"]!.AsArray();
            Assert.Equal(new[] { "fp-a", "fp-b" }, statements.Select(s => s!["statement_fingerprint"]!.GetValue<string>()));
            Assert.Equal("SELECT 'CANARYTEXT' FROM t", statements[0]!["sql_text"]!.GetValue<string>());

            /* #6579 review: the statement text is a statement-class key (sql_text), so metadataOnly masks it; full keeps it. */
            var sources = await new PgInvestigationBriefSourceReader(setup.DataSource, utcNow: () => setup.Clock.Now).ReadAsync(requestId, ct);
            Assert.Contains("CANARYTEXT", InvestigationBriefComposer.Compose(sources!, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, setup.Clock.Now), StringComparison.Ordinal);
            var masked = InvestigationBriefComposer.Compose(sources!, DarlingInvestigationBriefTools.Tools, McpDataDetail.MetadataOnly, setup.Clock.Now);
            Assert.Contains("client_timeouts", masked, StringComparison.Ordinal);
            Assert.DoesNotContain("CANARYTEXT", masked, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AClientTimeoutsRequest_WithNoTimeoutsInTheWindow_FreezesAnEmptySection()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.QueryPerformance, AlertFamily.ClientTimeoutsMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "attention", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var section = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "client_timeouts");
            Assert.Equal("ready", section.State);
            Assert.Equal("empty", JsonNode.Parse(section.ContentJson!)!["status"]!.GetValue<string>());
        });
    }

    /// <summary>#6579 review: a top statement longer than the cap is cut at a text-element boundary, never between the halves of a surrogate pair.</summary>
    [Fact]
    public async Task AClientTimeoutsStatement_CutAtTheCap_NeverSplitsASurrogatePair()
    {
        await RunAsync(async (setup, ct) =>
        {
            var text = new string('a', InvestigationEvidenceCapture.ClientTimeoutStatementMaxChars - 1) + "😀 tail";
            await PlantAttentionAsync(setup.Connection, ct, 1, AlertAt.AddMinutes(-2), "sample", 0, Db, "SSMS", "WS-ERIK", "fp-a", text);
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.QueryPerformance, AlertFamily.ClientTimeoutsMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "attention", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var row = Assert.Single(Rows(await setup.Requests.ReadEvidenceAsync(requestId, ct), "client_timeouts"));
            var cut = Assert.Single(row["top_statements"]!.AsArray())!["sql_text"]!.GetValue<string>();
            Assert.False(char.IsHighSurrogate(cut[^1]), "the cut left a lone high surrogate");
            Assert.True(cut.Length <= InvestigationEvidenceCapture.ClientTimeoutStatementMaxChars);
        });
    }

    private static Task PlantAttentionAsync(
        NpgsqlConnection connection, CancellationToken ct, long id, DateTime at, string kind, int count,
        string? db, string? app, string? host, string? fingerprint, string? text) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO attention_events
    (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, client_hostname, statement_fingerprint, statement_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            id, DarlingMcpTestData.Naive(at), ServerId, ServerName, kind, count, db, app, host, fingerprint, text);

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
            /* A decoy for the SAME handle at other offsets (the procedure header), collected later: a lookup by handle alone
               would take it. */
            await PlantCachedStatementAsync(setup.Connection, ct, WindowStart.AddMinutes(-10), "0x0200AB", 0, 9,
                "CREATE PROCEDURE dbo.UpdateOrders @id int AS");
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
    public async Task ARegressionOfSqlServersOwnCleanup_IsLeftOut_SoTheApplicationQueryLeads()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* #6548: change tracking cleanup regressed 100 times over; the application query 10 times over. */
            await PlantQueryStoreAsync(setup.Connection, ct, 511, 21, 1, cpuUs: 1_000, lastExec: AlertAt.AddMinutes(-4));
            await PlantQueryStoreAsync(setup.Connection, ct, 511, 22, 2, cpuUs: 10_000, lastExec: AlertAt.AddMinutes(-1));
            await PlantQueryStoreAsync(setup.Connection, ct, 512, 23, 1, cpuUs: 1_000, lastExec: AlertAt.AddMinutes(-4));
            await PlantQueryStoreAsync(setup.Connection, ct, 512, 24, 2, cpuUs: 100_000, lastExec: AlertAt.AddMinutes(-1));
            /* #6665: the monitor's own statement regressed 200 times over, stored only in the text table; it is no target either. */
            await PlantQueryStoreAsync(setup.Connection, ct, 513, 25, 1, cpuUs: 1_000, lastExec: AlertAt.AddMinutes(-4));
            await PlantQueryStoreAsync(setup.Connection, ct, 513, 26, 2, cpuUs: 200_000, lastExec: AlertAt.AddMinutes(-1));
            foreach (var (queryId, text) in new[]
            {
                (511L, "SELECT * FROM dbo.Orders"),
                (512L, "(@csn bigint,@batch_size int)delete top(@batch_size) from sys.[change_tracking_1228027148] where sys_change_xdes_id < @csn"),
                (513L, "(@cutoff_time datetime2(7))SELECT /* " + PerformanceMonitor.Collectors.QueryStoreCollector.SelfQueryMarker + " */ qsrs.plan_id"),
            })
            {
                await DarlingMcpTestData.ExecAsync(setup.Connection, ct,
                    "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, $2, $3, $4, $5)",
                    ServerId, Db, queryId, text, DarlingMcpTestData.Naive(AlertAt));
            }

            var requestId = await OpenQueryPerformanceAndCaptureAsync(setup, ct);

            var target = Assert.Single(Rows(await setup.Requests.ReadEvidenceAsync(requestId, ct), "regression_targets"));
            Assert.Equal(511, target["query_id"]!.GetValue<long>());
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

    [Fact]
    public async Task ReportXml_PastTheFirstCut_EveryLaterRowKeepsItsFlagInsideTheCap_AndTheStoreTakesTheRow()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, AlertEngine.BlockingWatermarkMetric, AlertAt), ct)).RequestId!.Value;
            /* Ten 14,000-character reports: the third is cut, and the seven after it each still carry a flag and an empty
               report, which must fit beside the cut one. */
            var reports = Enumerable.Range(0, InvestigationEvidenceCapture.SectionRows)
                .Select(i => "<r" + i + ">" + string.Concat(Enumerable.Repeat("<n>x</n>", 2_000))[..13_990] + "</r" + i + ">")
                .ToList();
            var rows = reports.Select((_, i) => new JsonObject { ["blocked_spid"] = 60 + i, ["report_xml"] = null }).ToList();

            var fitted = InvestigationEvidenceCapture.FitReportXml(WindowStart, WindowEnd, rows, reports);

            Assert.True(InvestigationEvidenceCapture.StoredBytes(fitted) <= InvestigationToolCore.MaxEvidenceBytes);
            var kept = JsonNode.Parse(fitted)!["rows"]!.AsArray();
            Assert.Equal(reports.Count, kept.Count);
            Assert.Equal(reports[0], kept[0]!["report_xml"]!.GetValue<string>());
            Assert.Equal(reports[1], kept[1]!["report_xml"]!.GetValue<string>());
            Assert.StartsWith(kept[2]!["report_xml"]!.GetValue<string>(), reports[2], StringComparison.Ordinal);
            for (var i = 2; i < kept.Count; i++)
                Assert.True(kept[i]!["report_xml_truncated"]!.GetValue<bool>());
            for (var i = 3; i < kept.Count; i++)
                Assert.Equal("", kept[i]!["report_xml"]!.GetValue<string>());

            /* The store takes the row: its content check would refuse the whole write over the cap. */
            await setup.Requests.RecordEvidenceAsync(requestId, ServerId,
                new[] { new InvestigationEvidenceWrite("blocking_chains", "ready", fitted) }, ct);
            var stored = await new NpgsqlCommand(
                "SELECT octet_length(content::text) FROM config.investigation_evidence WHERE section = 'blocking_chains' AND request_id = '" + requestId + "'",
                setup.Connection).ExecuteScalarAsync(ct);
            Assert.InRange(Convert.ToInt32(stored, System.Globalization.CultureInfo.InvariantCulture), 1, InvestigationToolCore.MaxEvidenceBytes);
        });
    }

    [Fact]
    public async Task AParallelSelfWaitReport_IsLeftOut_SoTheRealHeadStaysTheHead()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* Session 70 is blocked by 55; a newer report has 70 waiting on itself (a parallel query's thread, not blocking). */
            var report = BlockedProcessReport(blockingFrame: null);
            await PlantBlockedReportAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), report, blockedSpid: 70, blockingSpid: 55);
            await PlantBlockedReportAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), report, blockedSpid: 70, blockingSpid: 70);
            var requestId = await OpenBlockingAndCaptureAsync(setup, ct);

            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var chain = Assert.Single(Rows(evidence, "blocking_chains"));
            Assert.Equal(70, chain["blocked_spid"]!.GetValue<int>());
            Assert.Equal(55, chain["blocking_spid"]!.GetValue<int>());
            Assert.Equal(55, chain["head_blocker_spid"]!.GetValue<int>());
            Assert.Equal(55, Assert.Single(Rows(evidence, "head_blocker"))["spid"]!.GetValue<int>());
        });
    }

    [Fact]
    public async Task ASessionBlockedByTwoBlockersInOneWindow_GetsOneHead_AndTheSectionsFreeze()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* Session 70 is blocked by 55, then by 61: 55 blocks but no chain resolves to it any more. */
            var report = BlockedProcessReport(blockingFrame: null);
            await PlantBlockedReportAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), report, blockedSpid: 70, blockingSpid: 55);
            await PlantBlockedReportAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), report, blockedSpid: 70, blockingSpid: 61);
            var requestId = await OpenBlockingAndCaptureAsync(setup, ct);

            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var head = Assert.Single(Rows(evidence, "head_blocker"));
            Assert.Equal(61, head["spid"]!.GetValue<int>());
            Assert.Equal(2, head["reports_in_chain"]!.GetValue<int>());
            /* Two pairs with the same blocked session and the same wait: the blocking session breaks the tie. */
            var chains = Rows(evidence, "blocking_chains");
            Assert.Equal(new[] { 55, 61 }, chains.Select(c => c["blocking_spid"]!.GetValue<int>()).ToArray());
            Assert.All(chains, c => Assert.Equal(61, c["head_blocker_spid"]!.GetValue<int>()));
        });
    }

    [Fact]
    public async Task ASectionThatFails_StaysPendingThroughItsGrace_ThenFreezesUnavailable_AndTheOthersStillFreeze()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantBlockedReportAsync(setup.Connection, ct, AlertAt.AddMinutes(-1),
                BlockedProcessReport(blockingFrame: "<frame line=\"3\" stmtstart=\"10\" stmtend=\"90\" sqlhandle=\"0x0200ab\"/>"));
            /* The head blocker's statement read fails: the relation it reads is gone. */
            await DarlingMcpTestData.ExecAsync(setup.Connection, ct, "ALTER VIEW v_query_stats RENAME TO v_query_stats_gone");
            var requestId = await OpenBlockingAndCaptureAsync(setup, ct);

            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Single(Rows(evidence, "blocking_chains"));
            Assert.Equal("pending", Assert.Single(evidence, r => r.Section == "head_blocker").State);

            setup.Clock.Advance(InvestigationEvidenceCapture.PendingGrace + TimeSpan.FromMinutes(1));
            await setup.Capture.CaptureOnceAsync(ct);

            var failed = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "head_blocker");
            Assert.Equal("ready", failed.State);
            var content = JsonNode.Parse(failed.ContentJson!)!;
            Assert.Equal("unavailable", content["status"]!.GetValue<string>());
            Assert.Equal("PostgresException", content["error"]!.GetValue<string>());
            Assert.Empty(content["rows"]!.AsArray());
        });
    }

    [Fact]
    public void ALateJoinedSectionsGrace_EndsBeforeTheCaptureHorizon()
    {
        var opened = AlertAt;
        var margin = InvestigationEvidenceCapture.HorizonMargin;
        var horizon = InvestigationEvidenceCapture.CaptureHorizon;
        /* A metric that joined 5 h 50 m in: its two-hour grace would run past the horizon, where the request is no longer read. */
        var joined = opened + TimeSpan.FromMinutes(350);
        var capped = InvestigationEvidenceCapture.CappedGrace(TimeSpan.FromHours(2), joined, opened);
        Assert.Equal(opened + horizon - margin, joined + capped);
        /* A grace that ends well inside the horizon is untouched, and one that starts past the last pass is over at once. */
        Assert.Equal(InvestigationEvidenceCapture.PendingGrace,
            InvestigationEvidenceCapture.CappedGrace(InvestigationEvidenceCapture.PendingGrace, opened, opened));
        Assert.Equal(TimeSpan.Zero, InvestigationEvidenceCapture.CappedGrace(TimeSpan.FromHours(2), opened + horizon, opened));
    }

    [Fact]
    public async Task ADeadlockOfManyParties_KeepsItsVictimAndObjects_UnderTheCap_AndCountsThePartiesDropped()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* Twenty-five parties, each with a 1,000-character statement: whole, the one deadlock is over the 32 KB cap. */
            var statement = "UPDATE dbo.Orders SET qty = qty + 1 WHERE note = '" + new string('n', 1_100) + "'";
            await PlantDeadlockGraphAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), ManyPartyGraph(25, victim: 25, statement));
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, InvestigationEvidenceCapture.DeadlockMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "deadlocks", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var deadlock = Assert.Single(Rows(await setup.Requests.ReadEvidenceAsync(requestId, ct), "deadlocks"));
            var parties = deadlock["parties"]!.AsArray();
            Assert.Equal(InvestigationEvidenceCapture.DeadlockPartiesMax, parties.Count);
            Assert.Equal(25 - InvestigationEvidenceCapture.DeadlockPartiesMax, deadlock["parties_dropped"]!.GetValue<int>());
            Assert.True(parties[0]!["is_victim"]!.GetValue<bool>());
            Assert.Equal("process25", parties[0]!["process_id"]!.GetValue<string>());
            Assert.Equal("tempdb.dbo.#t_____________0001", Assert.Single(deadlock["objects"]!.AsArray())!["object_name"]!.GetValue<string>());
        });
    }

    [Fact]
    public async Task ADeadlockStatementTheFilterNames_IsWithheldWhole_NotCutAndKept()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* The statement names a passphrase function past the 1,000-character cut: cut first, its head would be kept. */
            var statement = "UPDATE dbo.Vault SET v = 1 WHERE note = '" + new string('n', 1_100) + "' AND PWDCOMPARE(N'hunter2', h) = 1";
            await PlantDeadlockGraphAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), ManyPartyGraph(2, victim: 1, statement));
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, InvestigationEvidenceCapture.DeadlockMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "deadlocks", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var section = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "deadlocks");
            Assert.Equal("ready", section.State);
            Assert.DoesNotContain("dbo.Vault", section.ContentJson!, StringComparison.Ordinal);
            Assert.DoesNotContain("hunter2", section.ContentJson!, StringComparison.Ordinal);
            Assert.Equal(2, Assert.Single(JsonNode.Parse(section.ContentJson!)!["rows"]!.AsArray())!["parties"]!.AsArray().Count);
        });
    }

    [Fact]
    public async Task ACompiledValueInAStatementTheFilterNames_IsNeverFrozen()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantQueryStoreAsync(setup.Connection, ct, 503, 5, 1, cpuUs: 1_000, lastExec: AlertAt.AddMinutes(-4));
            await PlantQueryStoreAsync(setup.Connection, ct, 503, 6, 2, cpuUs: 8_000, lastExec: AlertAt.AddMinutes(-1));
            const string Statement = "SELECT DECRYPTBYPASSPHRASE(@city, secret_col) FROM dbo.Vault";
            await PlantPlanAsync(setup.Connection, ct, 5, "N'hunter2'", Statement);
            await PlantPlanAsync(setup.Connection, ct, 6, "N'swordfish'", Statement);
            var requestId = await OpenQueryPerformanceAndCaptureAsync(setup, ct);

            var section = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "regression_targets");
            var target = Assert.Single(Rows(new[] { section }, "regression_targets"));
            Assert.Null(target["good_compiled"]);
            Assert.Null(target["bad_compiled"]);
            Assert.DoesNotContain("hunter2", section.ContentJson!, StringComparison.Ordinal);
            Assert.DoesNotContain("swordfish", section.ContentJson!, StringComparison.Ordinal);
        });
    }

    /* #6002 review round 2: one slow section must not starve the one after it. query_stats is locked, so top_statements (the first
       High CPU section) runs into its own deadline; active_requests, read after it, is still recorded on the same pass, and the
       slow section freezes unavailable once its grace is over (a TimeoutException, never a collected message). */
    [Fact]
    public async Task ASlowSection_RunsIntoItsOwnDeadline_AndTheNextSectionIsStillRead()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert("resources", InvestigationEvidenceCapture.HighCpuMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "query_stats", WindowEnd.AddSeconds(5));
            await PlantRunAsync(setup.Connection, ct, "query_snapshots", WindowEnd.AddSeconds(5));

            await using var locker = await setup.DataSource.OpenConnectionAsync(ct);
            await new NpgsqlCommand("BEGIN", locker).ExecuteNonQueryAsync(ct);
            await new NpgsqlCommand("LOCK TABLE query_stats IN ACCESS EXCLUSIVE MODE", locker).ExecuteNonQueryAsync(ct);
            try
            {
                await setup.Capture.CaptureOnceAsync(ct);
                var first = await setup.Requests.ReadEvidenceAsync(requestId, ct);
                Assert.Equal("pending", Assert.Single(first, r => r.Section == "top_statements").State);
                Assert.Equal("ready", Assert.Single(first, r => r.Section == "active_requests").State);

                setup.Clock.Advance(TimeSpan.FromMinutes(25));
                await setup.Capture.CaptureOnceAsync(ct);
                var later = await setup.Requests.ReadEvidenceAsync(requestId, ct);
                var slow = Assert.Single(later, r => r.Section == "top_statements");
                Assert.Equal("ready", slow.State);
                var content = JsonNode.Parse(slow.ContentJson!)!;
                Assert.Equal("unavailable", content["status"]!.GetValue<string>());
                Assert.Equal("TimeoutException", content["error"]!.GetValue<string>());
                Assert.Empty(content["rows"]!.AsArray());
                Assert.Equal("ready", Assert.Single(later, r => r.Section == "active_requests").State);
            }
            finally
            {
                await new NpgsqlCommand("ROLLBACK", locker).ExecuteNonQueryAsync(CancellationToken.None);
            }
        }, sectionDeadline: TimeSpan.FromSeconds(1));
    }

    /* #6002 review round 2: a section whose grace starts so late that it is capped to nothing has a negative age until its anchor
       passes, so the grace never ends before the request ages out of the candidate query. A request takes a link only within
       half an hour of its open, so the late anchor is planted on the stored link (an alert stamped ahead of the service's clock).
       The pass inside the last HorizonMargin freezes the section. */
    [Fact]
    public async Task ALateLink_InsideTheLastMinutesOfTheHorizon_FreezesNotCollected_InsteadOfStayingPending()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, AlertEngine.BlockingWatermarkMetric, AlertAt), ct)).RequestId!.Value;
            await PlantRunAsync(setup.Connection, ct, "blocked_process_report", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var joinedAt = AlertAt.AddMinutes(2);
            Assert.Equal(requestId, (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, InvestigationEvidenceCapture.DeadlockMetric, joinedAt), ct)).RequestId);

            /* Five hours fifty-six minutes in, the pass sees the deadlocks link stamped two minutes ahead of its clock. */
            setup.Clock.Advance(TimeSpan.FromHours(5) + TimeSpan.FromMinutes(55));
            await DarlingMcpTestData.ExecAsync(setup.Connection, ct,
                "UPDATE config.investigation_request_alerts SET alert_time = $1 WHERE request_id = $2 AND metric_name = $3",
                DateTime.SpecifyKind(setup.Clock.Now.AddMinutes(2), DateTimeKind.Utc), requestId, InvestigationEvidenceCapture.DeadlockMetric);
            await setup.Capture.CaptureOnceAsync(ct);

            var deadlocks = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "deadlocks");
            Assert.Equal("not_collected", deadlocks.State);
        });
    }

    /* #6002 review round 2: the write-time scrub can swap a short value for the 34-character placeholder where the capture's judge
       let it through, growing content the capture filled to the cap. The section is stored unavailable, the check never fires and the
       other section of the batch commits. */
    [Fact]
    public async Task ContentThatTheWriteScrubGrowsPastTheCap_IsStoredUnavailable_AndTheBatchCommits()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, AlertEngine.BlockingWatermarkMetric, AlertAt), ct)).RequestId!.Value;
            Assert.Equal(InvestigationToolCore.MaxEvidenceBytes, InvestigationRequestStore.EvidenceContentCapBytes);
            var rowCount = 1;
            string Content(int rows) => new JsonObject
            {
                ["window_start"] = "2026-01-01T00:00:00Z",
                ["window_end"] = "2026-01-01T00:10:00Z",
                ["rows"] = new JsonArray(Enumerable.Range(0, rows).Select(_ => (JsonNode?)new JsonObject { ["s"] = "pwdencrypt" }).ToArray()),
            }.ToJsonString();
            while (InvestigationEvidenceCapture.StoredBytes(Content(rowCount + 1)) <= InvestigationToolCore.MaxEvidenceBytes)
                rowCount++;
            var content = Content(rowCount);
            Assert.True(InvestigationEvidenceCapture.StoredBytes(content) <= InvestigationToolCore.MaxEvidenceBytes);
            var scrubbed = DerivedStoreScrub.Json(content)!;
            Assert.True(InvestigationEvidenceCapture.StoredBytes(scrubbed) > InvestigationToolCore.MaxEvidenceBytes);

            await setup.Requests.RecordEvidenceAsync(requestId, ServerId, new[]
            {
                new InvestigationEvidenceWrite("blocking_chains", "ready", content),
                new InvestigationEvidenceWrite("head_blocker", "ready", Content(0)),
            }, ct);

            var stored = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var big = Assert.Single(stored, r => r.Section == "blocking_chains");
            Assert.Equal("ready", big.State);
            var node = JsonNode.Parse(big.ContentJson!)!;
            Assert.Equal("unavailable", node["status"]!.GetValue<string>());
            Assert.Equal("EvidenceTooLarge", node["error"]!.GetValue<string>());
            Assert.Equal("2026-01-01T00:10:00Z", node["window_end"]!.GetValue<string>());
            Assert.Empty(node["rows"]!.AsArray());
            Assert.DoesNotContain("pwdencrypt", big.ContentJson!, StringComparison.Ordinal);
            Assert.Equal("ready", Assert.Single(stored, r => r.Section == "head_blocker").State);
        });
    }

    /* #6002 review round 2: content the write scrub withholds whole comes back null; a ready row is stored unavailable, never null. */
    [Fact]
    public async Task ContentThatTheWriteScrubWithholds_IsStoredUnavailable_NeverAsNullOnAReadyRow()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(
                Alert(InvestigationFamily.Blocking, AlertEngine.BlockingWatermarkMetric, AlertAt), ct)).RequestId!.Value;
            const string unreadable = "{\"window_start\": ";
            Assert.Null(DerivedStoreScrub.Json(unreadable));

            await setup.Requests.RecordEvidenceAsync(requestId, ServerId, new[]
            {
                new InvestigationEvidenceWrite("blocking_chains", "ready", unreadable),
            }, ct);

            var row = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "blocking_chains");
            Assert.Equal("ready", row.State);
            Assert.NotNull(row.ContentJson);
            Assert.Equal("EvidenceWithheld", JsonNode.Parse(row.ContentJson!)!["error"]!.GetValue<string>());
        });
    }

    /* #6002 review round 2: the statement filter is synchronous, so the rows loop stops at cancellation instead of finishing. */
    [Fact]
    public void FilterWork_StopsAtCancellation_BetweenRows()
    {
        var rows = Enumerable.Range(0, 3).Select(i => new JsonObject { ["n"] = i }).ToList();
        Assert.Equal(3, InvestigationEvidenceCapture.ScrubRows(rows, CancellationToken.None).Count);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Assert.Throws<OperationCanceledException>(() => InvestigationEvidenceCapture.ScrubRows(rows, cancelled.Token));
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

    private static Task PlantBlockedReportAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string reportXml, int blockedSpid = 61, int blockingSpid = 62) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid,
     blocked_ecid, blocking_ecid, blocking_status, database_name, blocked_sql_text, blocking_sql_text, blocked_process_report_xml)
VALUES ($1, $2, $3, $4, $5, 12000, $8, $9, 0, 0, NULL, $6, 'SELECT * FROM dbo.Orders WHERE id = 1', 'EXEC dbo.UpdateOrders @id = 1', $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddSeconds(5)), ServerId, ServerName, DarlingMcpTestData.Naive(at), Db, reportXml,
            blockingSpid, blockedSpid);

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

    /// <summary>A deadlock of <paramref name="parties"/> processes on one temporary-table key lock, each running
    /// <paramref name="statement"/>; <c>process{victim}</c> is the victim.</summary>
    private static string ManyPartyGraph(int parties, int victim, string statement)
    {
        var text = System.Security.SecurityElement.Escape(statement);
        var processes = string.Concat(Enumerable.Range(1, parties).Select(i =>
            "<process id=\"process" + i + "\" spid=\"" + (60 + i) + "\" lockMode=\"U\" isolationlevel=\"read committed (2)\" currentdbname=\"tempdb\">"
            + "<executionStack><frame procname=\"adhoc\" line=\"1\">" + text + "</frame></executionStack><inputbuf>" + text + "</inputbuf></process>"));
        return "<deadlock><victim-list><victimProcess id=\"process" + victim + "\"/></victim-list><process-list>" + processes
            + "</process-list><resource-list>"
            + "<keylock hobtid=\"72057594\" dbid=\"2\" objectname=\"tempdb.dbo.#t_____________0001\" indexname=\"PK_t\" id=\"lock1\" mode=\"X\" associatedObjectId=\"72057594\">"
            + "<owner-list><owner id=\"process1\" mode=\"X\"/></owner-list><waiter-list><waiter id=\"process2\" mode=\"U\" requestType=\"wait\"/></waiter-list></keylock>"
            + "</resource-list></deadlock>";
    }

    private static Task PlantDeadlockGraphAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string graph) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, 'tempdb', $5, 'process1', NULL, $6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddSeconds(5)), ServerId, ServerName, DarlingMcpTestData.Naive(at), graph);

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
    private static async Task PlantPlanAsync(
        NpgsqlConnection connection, CancellationToken ct, long planId, string compiledValue,
        string statement = "SELECT * FROM dbo.Customers WHERE city = @city")
    {
        var plan = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + System.Security.SecurityElement.Escape(statement) + "\" StatementId=\"1\" StatementType=\"SELECT\">"
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

    private static async Task RunAsync(Func<Setup, CancellationToken, Task> body, TimeSpan? sectionDeadline = null)
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
        var capture = new InvestigationEvidenceCapture(dataSource, requests, NullLogger.Instance, () => clock.Now, (_, _) => 5, sectionDeadline);
        await body(new Setup(dataSource, connection, requests, capture, clock), ct);
    }
}
