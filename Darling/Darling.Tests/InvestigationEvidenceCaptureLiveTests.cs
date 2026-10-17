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

    /// <summary><paramref name="hourlyLimit"/> and <paramref name="dailyLimit"/> are the route's open limits (the bulk test opens thirty).</summary>
    private static InvestigationAlert HighCpu(int serverId = ServerId, string serverName = ServerName, int hourlyLimit = 20, int dailyLimit = 100) =>
        new(1, serverId, serverName, "resources", InvestigationEvidenceCapture.HighCpuMetric, AlertAt,
            new[] { serverId + ":High CPU" }, null, hourlyLimit, dailyLimit, LeaseMinutes: 15, ExpiryMinutes: 240);

    /// <summary>The window end the capture waits a collector run for (the first alert plus the tail).</summary>
    private static DateTime WindowEnd => AlertAt + InvestigationEvidenceCapture.BreachTail;

    [Fact]
    public async Task TopStatements_AreNotFrozenByTheFirstRows_ButByTheCollectorsRunAfterTheWindow_AndAreThenFrozen()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), "0xAAAA", 9_000_000);
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-2), "0xBBBB", 4_000_000);
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;

            /* Rows are in the window, but the collector has not run since the window end (its last run, before the window end,
               does not count): the section is NOT frozen on them. */
            await PlantRunAsync(setup.Connection, ct, "query_stats", WindowEnd.AddSeconds(-30));
            await setup.Capture.CaptureOnceAsync(ct);
            var early = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "top_statements");
            Assert.Equal("pending", early.State);
            Assert.Null(early.ContentJson);

            /* A snapshot covering the breach lands inside the window AFTER that first pass, then the collector runs past the window end. */
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddSeconds(30), "0xCCCC", 99_000_000);
            await PlantRunAsync(setup.Connection, ct, "query_stats", WindowEnd.AddSeconds(5));
            setup.Clock.Advance(TimeSpan.FromMinutes(1));
            await setup.Capture.CaptureOnceAsync(ct);

            var first = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "top_statements");
            Assert.Equal("ready", first.State);
            var rows = JsonNode.Parse(first.ContentJson!)!["rows"]!.AsArray();
            Assert.Equal(new[] { "0xCCCC", "0xAAAA", "0xBBBB" }, rows.Select(r => r!["query_hash"]!.GetValue<string>()));

            /* Frozen from here: a still heavier statement collected into the same window changes nothing. */
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddSeconds(40), "0xDDDD", 999_000_000);
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
    public async Task ASectionThatLandsLater_FlipsFromPendingToReady_AndEvidenceReadyIsJournaledOnce_NamingTheReadyAndTheNotCollectedSections()
    {
        await RunAsync(async (setup, ct) =>
        {
            await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), "0xAAAA", 9_000_000);
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;

            /* Inside the grace, no collector has run since the window end: all five sections are pending (the statements in the
               window do not freeze one), and nothing is journaled. */
            await setup.Capture.CaptureOnceAsync(ct);
            await setup.Capture.CaptureOnceAsync(ct);
            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal(InvestigationEvidenceCapture.Sections.Count, evidence.Count(r => r.State == "pending"));
            Assert.Equal("pending", await RequestEvidenceStateAsync(setup.Connection, requestId, ct));
            Assert.Equal(0, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));

            /* The statement and wait collectors complete a run past the window end: those sections flip to ready; the rest are
               still pending, so still nothing is journaled. */
            await DarlingMcpTestData.ExecAsync(setup.Connection, ct,
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
                  VALUES ($1, $2, $3, $4, 'SOS_SCHEDULER_YIELD', 50, 9000)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(AlertAt.AddMinutes(-2)), ServerId, ServerName);
            await PlantRunAsync(setup.Connection, ct, "query_stats", WindowEnd.AddSeconds(5));
            await PlantRunAsync(setup.Connection, ct, "wait_stats", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);
            evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal("ready", evidence.Single(r => r.Section == "top_statements").State);
            Assert.Equal("ready", evidence.Single(r => r.Section == "waits").State);
            Assert.Equal(3, evidence.Count(r => r.State == "pending"));
            Assert.Equal(0, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));

            /* The grace passes: the collectors that never ran are not_collected, nothing is pending, and evidence_ready is
               journaled once. The detail names the READY sections and, separately, the not_collected ones, though the final
               call wrote only the not_collected ones. */
            setup.Clock.Advance(InvestigationEvidenceCapture.PendingGrace + TimeSpan.FromMinutes(1));
            await setup.Capture.CaptureOnceAsync(ct);
            await setup.Capture.CaptureOnceAsync(ct);
            evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal(new[] { "not_collected", "not_collected", "not_collected" },
                evidence.Where(r => r.Section is "active_requests" or "query_store" or "jobs").Select(r => r.State));
            Assert.DoesNotContain(evidence, r => r.State == "pending");
            Assert.Equal("ready", await RequestEvidenceStateAsync(setup.Connection, requestId, ct));
            Assert.Equal(1, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));
            var detail = JsonNode.Parse(await EvidenceReadyDetailAsync(setup.Connection, requestId, ct))!;
            Assert.Equal(new[] { "top_statements", "waits" }, detail["sections"]!.AsArray().Select(n => n!.GetValue<string>()).OrderBy(n => n, StringComparer.Ordinal));
            Assert.Equal(new[] { "active_requests", "jobs", "query_store" },
                detail["not_collected"]!.AsArray().Select(n => n!.GetValue<string>()).OrderBy(n => n, StringComparer.Ordinal));

            /* The store's own write, repeated with the same input, still journals once. */
            var again = await setup.Requests.RecordEvidenceAsync(requestId, ServerId,
                new[] { new InvestigationEvidenceWrite("waits", "ready", "{}") }, ct);
            Assert.False(again.JournaledReady);
            Assert.Equal(1, await EvidenceReadyRowsAsync(setup.Connection, requestId, ct));
        });
    }

    [Fact]
    public async Task ACollectorThatRanAfterTheWindowFoundNothing_IsReadyWithAnEmptyList_OnTheFirstPass_AndOneThatDidNotRunWaitsOutTheGrace()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;

            /* No Agent job ran in the breach window, but the job collector completed a run after it. The statement collector's
               only run is BEFORE the window end, so it does not count. */
            await PlantRunAsync(setup.Connection, ct, "running_jobs", WindowEnd.AddSeconds(5));
            await PlantRunAsync(setup.Connection, ct, "query_stats", WindowEnd.AddSeconds(-1));
            await setup.Capture.CaptureOnceAsync(ct);

            var evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var jobs = evidence.Single(r => r.Section == "jobs");
            Assert.Equal("ready", jobs.State);
            Assert.Empty(JsonNode.Parse(jobs.ContentJson!)!["rows"]!.AsArray());
            Assert.All(evidence.Where(r => r.Section != "jobs"), r => Assert.Equal("pending", r.State));
            Assert.Equal("pending", await RequestEvidenceStateAsync(setup.Connection, requestId, ct));

            /* Only after the grace is a collector that has not run since the window end not_collected. */
            setup.Clock.Advance(InvestigationEvidenceCapture.PendingGrace + TimeSpan.FromMinutes(1));
            await setup.Capture.CaptureOnceAsync(ct);
            evidence = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            Assert.Equal("ready", evidence.Single(r => r.Section == "jobs").State);
            Assert.All(evidence.Where(r => r.Section != "jobs"), r => Assert.Equal("not_collected", r.State));
            Assert.Equal("ready", await RequestEvidenceStateAsync(setup.Connection, requestId, ct));
        });
    }

    [Fact]
    public async Task ActiveRequests_CountOneRequestOnce_WhateverHowManySnapshotsSawIt_AndRankByThatFigure()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;
            var t0 = AlertAt.AddMinutes(-6);

            /* One request, seen in three one-minute snapshots, its running CPU total 100, 200, 300 ms. */
            await PlantSnapshotAsync(setup.Connection, ct, t0, 61, 100, 1_000, "HOSTA", "AppA");
            await PlantSnapshotAsync(setup.Connection, ct, t0.AddMinutes(1), 61, 200, 61_000, "HOSTA", "AppA");
            await PlantSnapshotAsync(setup.Connection, ct, t0.AddMinutes(2), 61, 300, 121_000, "HOSTA", "AppA");
            /* A NEW batch on the same session (its CPU total starts again) is a second request of the same host. */
            await PlantSnapshotAsync(setup.Connection, ct, t0.AddMinutes(3), 61, 50, 500, "HOSTA", "AppA");
            /* A second host's single request is heavier than the first host's 300 ms request but lighter than the 600 ms a SUM would give it. */
            await PlantSnapshotAsync(setup.Connection, ct, t0.AddMinutes(1), 62, 400, 2_000, "HOSTB", "AppB");
            await PlantRunAsync(setup.Connection, ct, "query_snapshots", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var section = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "active_requests");
            Assert.Equal("ready", section.State);
            var rows = JsonNode.Parse(section.ContentJson!)!["rows"]!.AsArray();
            Assert.Equal(new[] { "HOSTB", "HOSTA" }, rows.Select(r => r!["host_name"]!.GetValue<string>()));
            Assert.Equal(400, rows[0]!["total_cpu_ms"]!.GetValue<long>());
            Assert.Equal(350, rows[1]!["total_cpu_ms"]!.GetValue<long>());
            Assert.Equal(2, rows[1]!["requests"]!.GetValue<long>());
            Assert.Equal(300, rows[1]!["max_cpu_ms"]!.GetValue<long>());
            Assert.Equal(4, rows[1]!["samples"]!.GetValue<long>());
        });
    }

    [Fact]
    public async Task QueryStore_KeepsTheTenDatabasesThatBurnedTheMostCpu_AndCountsWhatItDropped()
    {
        await RunAsync(async (setup, ct) =>
        {
            /* Twelve databases; the alphabetically FIRST two are the cheapest in the window. */
            var names = Enumerable.Range(1, 12).Select(i => "Db" + i.ToString("00", System.Globalization.CultureInfo.InvariantCulture)).ToList();
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;
            for (var i = 0; i < names.Count; i++)
            {
                await PlantQueryStoreHealthAsync(setup.Connection, ct, ServerId, ServerName, names[i], WindowEnd.AddSeconds(2));
                await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-2), "0x" + i.ToString("X4", System.Globalization.CultureInfo.InvariantCulture),
                    1_000_000L * (i + 1), names[i]);
            }

            await PlantRunAsync(setup.Connection, ct, "query_store_health", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var section = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "query_store");
            Assert.Equal("ready", section.State);
            var content = JsonNode.Parse(section.ContentJson!)!;
            var kept = content["rows"]!.AsArray().Select(r => r!["database_name"]!.GetValue<string>()).ToList();
            Assert.Equal(InvestigationEvidenceCapture.QueryStoreDatabasesPerCapture, kept.Count);
            Assert.Equal(names.Skip(2).Reverse(), kept);
            Assert.Equal(2, content["databases_dropped"]!.GetValue<int>());
        });
    }

    [Fact]
    public async Task QueryStore_WithExactlyTheCapDatabases_DropsNone()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;
            for (var i = 1; i <= InvestigationEvidenceCapture.QueryStoreDatabasesPerCapture; i++)
            {
                await PlantQueryStoreHealthAsync(setup.Connection, ct, ServerId, ServerName,
                    "Db" + i.ToString("00", System.Globalization.CultureInfo.InvariantCulture), WindowEnd.AddSeconds(2));
            }

            await PlantRunAsync(setup.Connection, ct, "query_store_health", WindowEnd.AddSeconds(5));
            await setup.Capture.CaptureOnceAsync(ct);

            var section = Assert.Single(await setup.Requests.ReadEvidenceAsync(requestId, ct), r => r.Section == "query_store");
            var content = JsonNode.Parse(section.ContentJson!)!;
            Assert.Equal(InvestigationEvidenceCapture.QueryStoreDatabasesPerCapture, content["rows"]!.AsArray().Count);
            Assert.Equal(0, content["databases_dropped"]!.GetValue<int>());
        });
    }

    [Fact]
    public async Task ThirtyPendingRequests_AreAllReachedOnTheFirstPass_OldestFirst_AndNoneIsMarkedNotCollectedForItsPosition()
    {
        await RunAsync(async (setup, ct) =>
        {
            const int Requests = 30;
            var ids = new List<Guid>();
            for (var i = 0; i < Requests; i++)
            {
                var serverId = ServerId + 100 + i;
                var name = "EvCaptureBulk" + i.ToString("00", System.Globalization.CultureInfo.InvariantCulture);
                await DarlingMcpTestData.RegisterServerAsync(setup.Connection, serverId, name, ct);
                ids.Add((await setup.Requests.OpenOrJoinAsync(HighCpu(serverId, name, hourlyLimit: 100, dailyLimit: 500), ct)).RequestId!.Value);
                setup.Clock.Advance(TimeSpan.FromSeconds(1));
            }

            /* The oldest request is one second short of its grace; every collector that feeds the OLDEST FIVE has run. */
            setup.Clock.Advance(InvestigationEvidenceCapture.PendingGrace - TimeSpan.FromSeconds(Requests + 1));
            for (var i = 0; i < 5; i++)
            {
                foreach (var collector in InvestigationEvidenceCapture.SectionCollectors.Values.Distinct())
                    await PlantRunAsync(setup.Connection, ct, collector, WindowEnd.AddSeconds(5), ServerId + 100 + i, "EvCaptureBulk" + i.ToString("00", System.Globalization.CultureInfo.InvariantCulture));
            }

            var recorded = await setup.Capture.CaptureOnceAsync(ct);

            Assert.Equal(Requests, recorded);
            foreach (var (id, index) in ids.Select((id, index) => (id, index)))
            {
                var evidence = await setup.Requests.ReadEvidenceAsync(id, ct);
                Assert.Equal(InvestigationEvidenceCapture.Sections.Count, evidence.Count);
                Assert.DoesNotContain(evidence, r => r.State == "not_collected");
                Assert.Equal(index < 5 ? "ready" : "pending", await RequestEvidenceStateAsync(setup.Connection, id, ct));
            }
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
    public async Task EvidenceInTheBrief_StoresPlansByHashNotXml_AndHostAndLoginAreMaskedAtMetadataOnly()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = await OpenWithHostAndLoginEvidenceAsync(setup, ct);

            var stored = await setup.Requests.ReadEvidenceAsync(requestId, ct);
            var statements = stored.Single(r => r.Section == "top_statements").ContentJson!;
            Assert.Contains("query_plan_hash", statements, StringComparison.Ordinal);
            Assert.DoesNotContain("ShowPlanXML", statements, StringComparison.Ordinal);
            /* The section captures the login the request ran under (and the host and program), as collected. */
            var requestsContent = stored.Single(r => r.Section == "active_requests").ContentJson!;
            Assert.Contains("alice", requestsContent, StringComparison.Ordinal);

            var sources = await new PgInvestigationBriefSourceReader(setup.DataSource, utcNow: () => setup.Clock.Now)
                .ReadAsync(requestId, ct);
            Assert.NotNull(sources!.Evidence);

            var full = InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, setup.Clock.Now);
            Assert.Contains("SECRETHOST01", full, StringComparison.Ordinal);
            Assert.Contains("alice", full, StringComparison.Ordinal);
            Assert.Equal("ready", JsonNode.Parse(full)!["evidence"]!.AsArray()
                .Single(e => e!["section"]!.GetValue<string>() == "active_requests")!["state"]!.GetValue<string>());

            var masked = InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.MetadataOnly, setup.Clock.Now);
            Assert.DoesNotContain("SECRETHOST01", masked, StringComparison.Ordinal);
            Assert.DoesNotContain("alice", masked, StringComparison.Ordinal);
            Assert.Contains("login_name", masked, StringComparison.Ordinal);
            Assert.Contains("evidence", masked, StringComparison.Ordinal);
            Assert.Contains("OrdersApp", masked, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ABriefStoredAtFull_IsReMaskedWhenServedAtMetadataOnly_HostAndLoginInItsEvidenceIncluded()
    {
        await RunAsync(async (setup, ct) =>
        {
            var requestId = await OpenWithHostAndLoginEvidenceAsync(setup, ct);
            var sources = await new PgInvestigationBriefSourceReader(setup.DataSource, utcNow: () => setup.Clock.Now)
                .ReadAsync(requestId, ct);
            var storedAtFull = InvestigationBriefComposer.Compose(sources!, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, setup.Clock.Now);
            Assert.Contains("SECRETHOST01", storedAtFull, StringComparison.Ordinal);

            /* An operator moves mcp.dataDetail from full to metadataOnly after the brief was stored. */
            var served = InvestigationFieldMasking.StoredBrief(
                storedAtFull, McpDataDetail.MetadataOnly, McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly))!.ToJsonString();

            Assert.DoesNotContain("SECRETHOST01", served, StringComparison.Ordinal);
            Assert.DoesNotContain("alice", served, StringComparison.Ordinal);
            Assert.Contains("OrdersApp", served, StringComparison.Ordinal);
            Assert.Equal(McpDataDetailModes.Word(McpDataDetail.MetadataOnly), JsonNode.Parse(served)!["data_detail"]!.GetValue<string>());

            /* Served at the level it was stored at, it comes back as stored. */
            var unchanged = InvestigationFieldMasking.StoredBrief(
                storedAtFull, McpDataDetail.Full, McpDataDetailMasker.ForAnswer(McpDataDetail.Full))!.ToJsonString();
            Assert.Contains("SECRETHOST01", unchanged, StringComparison.Ordinal);
        });
    }

    /// <summary>Opens a High CPU request whose active-request evidence holds a host and a login, and captures it.</summary>
    private static async Task<Guid> OpenWithHostAndLoginEvidenceAsync(Setup setup, CancellationToken ct)
    {
        await PlantStatementAsync(setup.Connection, ct, AlertAt.AddMinutes(-3), "0xAAAA", 9_000_000);
        await PlantSnapshotAsync(setup.Connection, ct, AlertAt.AddMinutes(-2), 61, 4000, 5000, "SECRETHOST01", "OrdersApp", "corp\\alice");
        var requestId = (await setup.Requests.OpenOrJoinAsync(HighCpu(), ct)).RequestId!.Value;
        await PlantRunAsync(setup.Connection, ct, "query_stats", WindowEnd.AddSeconds(5));
        await PlantRunAsync(setup.Connection, ct, "query_snapshots", WindowEnd.AddSeconds(5));
        await setup.Capture.CaptureOnceAsync(ct);
        return requestId;
    }

    private static async Task<string> EvidenceReadyDetailAsync(NpgsqlConnection connection, Guid requestId, CancellationToken ct) =>
        (string)(await new NpgsqlCommand(
            "SELECT detail::text FROM config.investigation_journal WHERE event = 'evidence_ready' AND request_id = '" + requestId + "'", connection)
            .ExecuteScalarAsync(ct))!;

    /// <summary>A completed collector run in the store's run record (<c>collect.collection_log</c>), with no rows required.</summary>
    private static Task PlantRunAsync(
        NpgsqlConnection connection, CancellationToken ct, string collector, DateTime at, int serverId = ServerId, string serverName = ServerName) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
              VALUES ($1, $2, $3, $4, $5, 5, 'SUCCESS')",
            CollectionIdGenerator.Next(), serverId, serverName, collector, DarlingMcpTestData.Naive(at));

    private static Task PlantSnapshotAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, int session, long cpuMs, long elapsedMs, string host, string program,
        string login = "corp\\svc") =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted,
                                           query_text, status, cpu_time_ms, total_elapsed_time_ms, host_name, program_name, login_name)
              VALUES ($1, $2, $3, $4, $5, 'Db', '00 00:00:05.000', 'SELECT 1', 'running', $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, session, cpuMs, elapsedMs, host, program, login);

    private static Task PlantQueryStoreHealthAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, string database, DateTime captured) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason)
              VALUES ($1, $2, $3, $4, $5, 'READ_WRITE', 'READ_WRITE', 0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(captured), serverId, serverName, database);

    private static async Task<string?> RequestEvidenceStateAsync(NpgsqlConnection connection, Guid requestId, CancellationToken ct) =>
        (string?)await new NpgsqlCommand(
            "SELECT evidence_state FROM config.investigation_requests WHERE request_id = '" + requestId + "'", connection).ExecuteScalarAsync(ct);

    private static async Task<long> EvidenceReadyRowsAsync(NpgsqlConnection connection, Guid requestId, CancellationToken ct) =>
        (long)(await new NpgsqlCommand(
            "SELECT count(*) FROM config.investigation_journal WHERE event = 'evidence_ready' AND request_id = '" + requestId + "'", connection)
            .ExecuteScalarAsync(ct))!;

    private static Task PlantStatementAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string queryHash, long cpuUs, string database = "Db") =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
                                       sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                       delta_logical_reads, min_dop, max_dop)
              VALUES ($1, $2, $3, $4, $7, $5, '0xPLAN' || $5, '0xSQLH' || $5, '0xPLANH', 'SELECT 1', 10, $6, $6, 100, 1, 1)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, queryHash, cpuUs, database);

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
