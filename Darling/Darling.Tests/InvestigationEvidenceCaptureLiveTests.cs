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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
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
/// The evidence capture of a High CPU investigation (#6001, #5632 Phase C D9 and D10) on a live store. The capture is built
/// from the store's data source alone: the scratch store has no monitored-server connection string, and no monitored server
/// exists to connect to. One clock drives the store and the capture (#3676).
/// </summary>
public sealed class InvestigationEvidenceCaptureLiveTests
{
    private const int ServerId = 7_601_001;
    private const string ServerName = "EvCaptureSrv";

    private static readonly DateTime AlertAt = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-5));

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert HighCpu() =>
        new(1, ServerId, ServerName, "resources", InvestigationEvidenceCapture.HighCpuMetric, AlertAt,
            new[] { ServerId + ":High CPU" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    [Fact]
    public async Task TopStatements_AreFrozenAtTheOpen_AndALaterCollectionDoesNotChangeThem()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), "0xAAAA", 9_000_000);
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-2), "0xBBBB", 4_000_000);
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;

            await setup.Capture.CaptureOnceAsync(ct);
            var first = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "top_statements");
            Assert.Equal("ready", first.State);
            var rows = JsonNode.Parse(first.ContentJson!)!["rows"]!.AsArray();
            Assert.Equal(new[] { "0xAAAA", "0xBBBB" }, rows.Select(r => r!["query_hash"]!.GetValue<string>()));

            /* A heavier statement is collected into the same window afterwards, and the capture runs again. */
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-1), "0xCCCC", 99_000_000);
            setup.Clock.Advance(TimeSpan.FromMinutes(1));
            await setup.Capture.CaptureOnceAsync(ct);

            var second = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "top_statements");
            Assert.Equal(first.ContentJson, second.ContentJson);
            Assert.Equal(first.CapturedAtUtc, second.CapturedAtUtc);

            /* The store itself refuses to rewrite a section that is no longer pending, whoever asks. */
            await setup.Requests.RecordEvidenceAsync(requestId, ServerId,
                new[] { new InvestigationEvidenceWrite("top_statements", "ready", "{\"rows\":[]}") }, ct);
            var third = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "top_statements");
            Assert.Equal(first.ContentJson, third.ContentJson);
        });
    }

    [Fact]
    public async Task ASectionThatLandsLater_FlipsFromPendingToReady_AndEvidenceReadyIsJournaledOnce_EvenWhenCaptureRunsTwice()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), "0xAAAA", 9_000_000);
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;

            /* Inside the grace: the statements are ready, the four other sections are pending, nothing is journaled. */
            await setup.Capture.CaptureOnceAsync(ct);
            await setup.Capture.CaptureOnceAsync(ct);
            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal("ready", evidence.Single(r => r.Section == "top_statements").State);
            Assert.Equal(4, evidence.Count(r => r.State == "pending"));
            Assert.Equal("pending", await RequestEvidenceStateAsync(setup.Connection, requestId, ct));
            Assert.Equal(0, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));

            /* The wait collector lands: that section flips to ready; the rest are still pending, so still nothing is journaled. */
            await DarlingMcpTestData.ExecAsync(setup.Connection, ct,
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
                  VALUES ($1, $2, $3, $4, 'SOS_SCHEDULER_YIELD', 50, 9000)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(AlertAt.AddMinutes(-2)), ServerId, ServerName);
            await setup.Capture.CaptureOnceAsync(ct);
            evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal("ready", evidence.Single(r => r.Section == "waits").State);
            Assert.Equal(3, evidence.Count(r => r.State == "pending"));
            Assert.Equal(0, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));

            /* The grace passes: what never landed is not_collected, nothing is pending, and evidence_ready is journaled once. */
            setup.Clock.Advance(InvestigationEvidenceCapture.PendingGrace + TimeSpan.FromMinutes(1));
            await setup.Capture.CaptureOnceAsync(ct);
            await setup.Capture.CaptureOnceAsync(ct);
            evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal(new[] { "not_collected", "not_collected", "not_collected" },
                evidence.Where(r => r.Section is "active_requests" or "query_store" or "jobs").Select(r => r.State));
            Assert.DoesNotContain(evidence, r => r.State == "pending");
            Assert.Equal("ready", await RequestEvidenceStateAsync(setup.Connection, requestId, ct));
            Assert.Equal(1, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));

            /* The store's own write, repeated with the same input, still journals once. */
            var again = await setup.Requests.RecordEvidenceAsync(requestId, ServerId,
                new[] { new InvestigationEvidenceWrite("waits", "ready", "{}") }, ct);
            Assert.False(again.JournaledReady);
            Assert.Equal(1, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));
        });
    }

    [Fact]
    public async Task ASectionNeverCollected_SaysNotCollected_AndJournalsNothing()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;
            setup.Clock.Advance(InvestigationEvidenceCapture.PendingGrace + TimeSpan.FromMinutes(1));

            await setup.Capture.CaptureOnceAsync(ct);

            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal(InvestigationEvidenceCapture.Sections.Count, evidence.Count);
            Assert.All(evidence, r => Assert.Equal("not_collected", r.State));
            Assert.All(evidence, r => Assert.Null(r.ContentJson));
            Assert.Equal("not_collected", await RequestEvidenceStateAsync(setup.Connection, requestId, ct));
            Assert.Equal(0, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));
        });
    }

    [Fact]
    public async Task ASectionIsHeldToTheReportEvidenceCap_AndTheStoreAcceptsWhatTheCapFits()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;
            var rows = Enumerable.Range(0, 400).Select(i => new JsonObject
            {
                ["query_hash"] = "0x" + i.ToString("X8"),
                ["note"] = new string('x', 300),
            }).ToList();

            var fitted = InvestigationEvidenceCapture.Fit(AlertAt.AddMinutes(-15), AlertAt, rows);

            Assert.True(Encoding.UTF8.GetByteCount(fitted) <= InvestigationToolCore.MaxEvidenceBytes);
            Assert.True(JsonNode.Parse(fitted)!["rows_dropped"]!.GetValue<int>() > 0);
            await setup.Requests.RecordEvidenceAsync(requestId, ServerId,
                new[] { new InvestigationEvidenceWrite("top_statements", "ready", fitted) }, ct);
            var stored = await new NpgsqlCommand(
                "SELECT octet_length(content::text) FROM config.investigation_evidence WHERE request_id = '" + requestId + "'", setup.Connection)
                .ExecuteScalarAsync(ct);
            Assert.True(Convert.ToInt32(stored, System.Globalization.CultureInfo.InvariantCulture) <= InvestigationToolCore.MaxEvidenceBytes);
        });
    }

    [Fact]
    public async Task EvidenceInTheBrief_StoresPlansByHashNotXml_AndIsMaskedAtMetadataOnly()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), "0xAAAA", 9_000_000);
            await DarlingMcpTestData.ExecAsync(setup.Connection, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted,
                                               query_text, status, cpu_time_ms, host_name, program_name, login_name)
                  VALUES ($1, $2, $3, $4, 61, 'Db', '00 00:00:05.000', 'SELECT 1', 'running', 4000, 'SECRETHOST01', 'OrdersApp', 'corp\\alice')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(AlertAt.AddMinutes(-2)), ServerId, ServerName);
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;
            await setup.Capture.CaptureOnceAsync(ct);

            var stored = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var statements = stored.Single(r => r.Section == "top_statements").ContentJson!;
            Assert.Contains("query_plan_hash", statements, StringComparison.Ordinal);
            Assert.DoesNotContain("ShowPlanXML", statements, StringComparison.Ordinal);

            var sources = await new PgInvestigationBriefSourceReader(setup.DataSource, utcNow: () => setup.Clock.Now)
                .ReadAsync(requestId, ct);
            Assert.NotNull(sources!.Evidence);

            var full = InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, setup.Clock.Now);
            Assert.Contains("SECRETHOST01", full, StringComparison.Ordinal);
            Assert.Equal("ready", JsonNode.Parse(full)!["evidence"]!.AsArray()
                .Single(e => e!["section"]!.GetValue<string>() == "active_requests")!["state"]!.GetValue<string>());

            var masked = InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.MetadataOnly, setup.Clock.Now);
            Assert.DoesNotContain("SECRETHOST01", masked, StringComparison.Ordinal);
            Assert.DoesNotContain("alice", masked, StringComparison.Ordinal);
            Assert.Contains("evidence", masked, StringComparison.Ordinal);
            Assert.Contains("OrdersApp", masked, StringComparison.Ordinal);
        });
    }

    private static async Task<string?> RequestEvidenceStateAsync(NpgsqlConnection connection, Guid requestId, CancellationToken ct) =>
        (string?)await new NpgsqlCommand(
            "SELECT evidence_state FROM config.investigation_requests WHERE request_id = '" + requestId + "'", connection).ExecuteScalarAsync(ct);

    private static async Task<long> EvidenceReadyRowsAsync(NpgsqlConnection connection, Guid requestId, CancellationToken ct) =>
        (long)(await new NpgsqlCommand(
            "SELECT count(*) FROM config.investigation_journal WHERE event = 'evidence_ready' AND request_id = '" + requestId + "'", connection)
            .ExecuteScalarAsync(ct))!;

    private static Task PlantStatementAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string queryHash, long cpuUs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
                                       sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                       delta_logical_reads, min_dop, max_dop)
              VALUES ($1, $2, $3, $4, 'Db', $5, '0xPLAN' || $5, '0xSQLH' || $5, '0xPLANH', 'SELECT 1', 10, $6, $6, 100, 1, 1)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, queryHash, cpuUs);

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
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation evidence capture live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var clock = new TestClock();
        var requests = new InvestigationRequestStore(dataSource, 30, () => clock.Now);
        /* The capture is handed the store's data source and nothing else: there is no monitored-server connection string to give it. */
        var capture = new InvestigationEvidenceCapture(dataSource, requests, NullLogger.Instance, () => clock.Now);
        await body(new Setup(dataSource, connection, requests, capture, clock), ct);
    }
}
