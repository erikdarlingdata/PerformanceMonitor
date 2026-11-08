/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8081: <c>get_sweep_reports</c> and <c>analyze_server</c> answer within <see cref="McpResponseBudget.DefaultBytes"/>
/// and say when they cut. Each case seeds its own scratch store (<see cref="ScratchPostgres"/>) and reads it through
/// the tool itself, so the bytes measured are the bytes a client receives.
///
/// <para><c>get_sweep_reports</c>: the timeline entries are the web feed's and the Viewer's own, each older entry with its
/// document embedded and the newest sweep's document under <c>latest</c> (#8318; <c>ViewerFleetSweepsReadTests</c>
/// compares them field for field), so a window holding more of them than the
/// budget fits is cut newest first and flagged (<c>truncated</c>, <c>sweeps_returned</c> beside the window's
/// <c>count</c>, <c>budget_note</c>); <c>latest</c> is never cut. <c>analyze_server</c>: the findings are already severity-first, so a cut keeps
/// the most severe and the reply says so (<c>truncated</c>, <c>findings_returned</c> beside <c>finding_count</c>,
/// <c>truncation_note</c> naming <c>get_analysis_findings</c>).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class McpSweepAnalyzeResponseBudgetLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* ---------------- get_sweep_reports ---------------- */

    /// <summary>192 sweeps at a 15-minute cadence over a 48-hour window: far more entries than fit. The reply
    /// stays under the budget, keeps the newest sweeps with their documents, says how many of the window's
    /// count it carries, and embeds the newest document on <c>latest</c>.</summary>
    [Fact]
    public async Task GetSweepReports_AWindowOfManySweeps_FitsTheBudget_NewestFirst_AndSaysItWasCut()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        const int sweepCount = 192;
        var ids = await SeedSweepsAsync(postgres, sweepCount, TimeSpan.FromMinutes(15), serversPerReport: 10, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, hours_back: 48, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the default-shaped reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        Assert.Equal(sweepCount, root.GetProperty("count").GetInt32());
        Assert.True(root.TryGetProperty("truncated", out var truncated) && truncated.GetBoolean(), "a cut timeline says truncated: true");
        Assert.True(root.TryGetProperty("sweeps_returned", out var returned), "sweeps_returned rides beside count");
        var sweeps = root.GetProperty("sweeps");
        Assert.Equal(returned.GetInt32(), sweeps.GetArrayLength());
        Assert.InRange(returned.GetInt32(), 1, sweepCount - 1);
        Assert.Contains("response budget", root.GetProperty("budget_note").GetString()!, StringComparison.Ordinal);

        /* Newest first: the cut leaves the OLDEST sweeps off, and the first header is the newest sweep. */
        Assert.Equal(ids[0].ToString(CultureInfo.InvariantCulture), sweeps[0].GetProperty("sweep_id").GetString());
        Assert.Equal(ids[returned.GetInt32() - 1].ToString(CultureInfo.InvariantCulture),
            sweeps[sweeps.GetArrayLength() - 1].GetProperty("sweep_id").GetString());

        /* Each entry is the web feed's own, document embedded; latest carries the newest in full. */
        Assert.Equal(JsonValueKind.Object, sweeps[0].GetProperty("report").ValueKind);
        var latest = root.GetProperty("latest");
        Assert.Equal(ids[0].ToString(CultureInfo.InvariantCulture), latest.GetProperty("sweep_id").GetString());
        Assert.Equal(JsonValueKind.Object, latest.GetProperty("report").ValueKind);
    }

    /// <summary>Four sweeps in the default one-hour window, each with a document of about 12 KB: the case the
    /// issue measured at 60 KB. The reply is cut to the budget newest first and says so; the newest sweep still
    /// rides in full on <c>latest</c>, and every entry kept is the web feed's own.</summary>
    [Fact]
    public async Task GetSweepReports_AFewSweepsWithLargeDocuments_IsCutToTheBudget_NewestFirst_AndLatestCarriesTheDocument()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        var ids = await SeedSweepsAsync(postgres, sweepCount: 4, TimeSpan.FromMinutes(12), serversPerReport: 40, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the default-shaped reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        Assert.Equal(4, root.GetProperty("count").GetInt32());
        Assert.True(root.TryGetProperty("truncated", out var truncated) && truncated.GetBoolean(), "a cut timeline says truncated: true");
        var returned = root.GetProperty("sweeps_returned").GetInt32();
        Assert.InRange(returned, 1, 3);
        var sweeps = root.GetProperty("sweeps");
        Assert.Equal(returned, sweeps.GetArrayLength());
        Assert.Contains("response budget", root.GetProperty("budget_note").GetString()!, StringComparison.Ordinal);
        Assert.Equal(ids[0].ToString(CultureInfo.InvariantCulture), sweeps[0].GetProperty("sweep_id").GetString());
        /* #8318: the newest sweep's document rides once, under latest; its entry says so. The older entries keep theirs. */
        Assert.Equal("latest", sweeps[0].GetProperty("report_under").GetString());
        foreach (var older in sweeps.EnumerateArray().Skip(1))
        {
            Assert.Equal(40, older.GetProperty("report").GetProperty("servers").GetArrayLength());
        }

        var latest = root.GetProperty("latest");
        Assert.Equal(ids[0].ToString(CultureInfo.InvariantCulture), latest.GetProperty("sweep_id").GetString());
        Assert.Equal(40, latest.GetProperty("report").GetProperty("servers").GetArrayLength());
        Assert.Equal(2, latest.GetProperty("verdicts").GetArrayLength());
    }

    /// <summary>Four sweeps with small documents in the default hour: well under the budget. Nothing is cut, the
    /// reply says so, and every entry carries its document and the header facts the honesty rules need (mute
    /// header, liveness verdict and block).</summary>
    [Fact]
    public async Task GetSweepReports_AFewSweepsWithSmallDocuments_AnswersWhole_AndSaysSo()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep-report budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));

        var ids = await SeedSweepsAsync(postgres, sweepCount: 4, TimeSpan.FromMinutes(12), serversPerReport: 6, ct);

        var json = await DarlingMcpFleetSweepTools.GetSweepReports(postgres, NullLogger.Instance, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the default-shaped reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        Assert.Equal(4, root.GetProperty("count").GetInt32());
        Assert.True(root.TryGetProperty("truncated", out var truncated) && !truncated.GetBoolean(), "an uncut timeline says truncated: false");
        Assert.Equal(4, root.GetProperty("sweeps_returned").GetInt32());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);

        var sweeps = root.GetProperty("sweeps");
        Assert.Equal(4, sweeps.GetArrayLength());
        foreach (var sweep in sweeps.EnumerateArray())
        {
            /* #8318: the newest sweep's document rides once, under latest; its entry says so. The older entries keep theirs. */
            if (sweep.GetProperty("sweep_id").GetString() == ids[0].ToString(CultureInfo.InvariantCulture))
            {
                Assert.Equal("latest", sweep.GetProperty("report_under").GetString());
            }
            else
            {
                Assert.Equal(6, sweep.GetProperty("report").GetProperty("servers").GetArrayLength());
            }

            Assert.Equal(JsonValueKind.String, sweep.GetProperty("sweep_id").ValueKind);
            Assert.Equal(JsonValueKind.String, sweep.GetProperty("swept_at").ValueKind);
            Assert.True(sweep.GetProperty("alerts_enabled").GetBoolean());
            Assert.True(sweep.GetProperty("instruments_alive").GetBoolean());
            Assert.Equal(JsonValueKind.Object, sweep.GetProperty("instrument_liveness").ValueKind);
            Assert.Equal(10, sweep.GetProperty("servers_expected").GetInt32());
        }

        var latest = root.GetProperty("latest");
        Assert.Equal(ids[0].ToString(CultureInfo.InvariantCulture), latest.GetProperty("sweep_id").GetString());
        var report = latest.GetProperty("report");
        Assert.Equal(JsonValueKind.Object, report.ValueKind);
        Assert.Equal(6, report.GetProperty("servers").GetArrayLength());
        Assert.Equal(2, latest.GetProperty("verdicts").GetArrayLength());
    }

    /* ---------------- analyze_server ---------------- */

    /// <summary>Every scored wait type saturated for the whole window, with blocking and deadlocks beside them:
    /// more findings than the budget holds. The reply stays under the budget, keeps the most severe findings,
    /// and says how many of finding_count it carries.</summary>
    [Fact]
    public async Task AnalyzeServer_AWindowOfManyFindings_FitsTheBudget_MostSevereFirst_AndSaysItWasCut()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live analyze_server budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));
        const string serverName = "analyze-budget-many";
        await SeedManyFindingsAsync(postgres, serverName, ct);

        var json = await DarlingMcpTools.AnalyzeServer(new DarlingAnalysisService(postgres), postgres, serverName, 4);
        Assert.StartsWith("{", json, StringComparison.Ordinal);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("findings", root.GetProperty("status").GetString());
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        var findingCount = root.GetProperty("finding_count").GetInt32();
        Assert.True(root.TryGetProperty("truncated", out var truncated) && truncated.GetBoolean(), "a cut reply says truncated: true");
        Assert.True(root.TryGetProperty("findings_returned", out var returned), "findings_returned rides beside finding_count");
        var findings = root.GetProperty("findings");
        Assert.Equal(returned.GetInt32(), findings.GetArrayLength());
        Assert.InRange(returned.GetInt32(), 1, findingCount - 1);
        Assert.Contains("get_analysis_findings", root.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);

        /* The most severe are kept: the returned list is non-increasing in severity. */
        var severities = findings.EnumerateArray().Select(f => f.GetProperty("severity").GetDouble()).ToList();
        for (var i = 1; i < severities.Count; i++)
            Assert.True(severities[i] <= severities[i - 1], $"finding {i} ({severities[i]}) is more severe than finding {i - 1} ({severities[i - 1]}); the cut must keep the most severe");
    }

    /// <summary>The same window anchored with as_of: nothing is persisted on an anchored run, so the findings the
    /// cut left off are not in the store, and the note must not send the reader to get_analysis_findings for
    /// them. It says the cut findings were not saved and names hours_back as the way to see them.</summary>
    [Fact]
    public async Task AnalyzeServer_AnAnchoredWindowOfManyFindings_SaysTheCutFindingsWereNotSaved()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live analyze_server budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));
        const string serverName = "analyze-budget-anchored";
        var windowEnd = await SeedManyFindingsAsync(postgres, serverName, ct);

        var asOf = DateTime.SpecifyKind(windowEnd, DateTimeKind.Utc).ToString("o");
        var json = await DarlingMcpTools.AnalyzeServer(new DarlingAnalysisService(postgres), postgres, serverName, 4, asOf);
        Assert.StartsWith("{", json, StringComparison.Ordinal);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("findings", root.GetProperty("status").GetString());
        Assert.False(root.GetProperty("persisted").GetBoolean());
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        Assert.True(root.TryGetProperty("truncated", out var truncated) && truncated.GetBoolean(), "a cut reply says truncated: true");
        var note = root.GetProperty("truncation_note").GetString()!;
        Assert.DoesNotContain("get_analysis_findings", note, StringComparison.Ordinal);
        Assert.Contains("not persisted", note, StringComparison.Ordinal);
        Assert.Contains("narrow hours_back", note, StringComparison.Ordinal);
    }

    /// <summary>One wait type over the window: one or two findings, well under the budget. Nothing is cut and
    /// the reply says so, with every finding on it.</summary>
    [Fact]
    public async Task AnalyzeServer_AWindowOfFewFindings_AnswersWhole_AndSaysSo()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live analyze_server budget test.");
        var ct = Ct;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(WithSearchPath(scratch));
        const string serverName = "analyze-budget-few";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);
            await RegisterServerAsync(connection, serverId, serverName, ct);
            await PlantWaitAsync(connection, serverId, serverName, windowEnd.AddHours(-30), "OLD_WAIT", 1_000L, ct);
            await PlantWaitAsync(connection, serverId, serverName, windowStart, "CXPACKET", 0L, ct);
            for (var i = 1; i <= 16; i++)
                await PlantWaitAsync(connection, serverId, serverName, windowStart.AddMinutes(15 * i), "CXPACKET", 450_000L, ct);
        }

        var json = await DarlingMcpTools.AnalyzeServer(new DarlingAnalysisService(postgres), postgres, serverName, 4);
        Assert.StartsWith("{", json, StringComparison.Ordinal);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("findings", root.GetProperty("status").GetString());
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        var findingCount = root.GetProperty("finding_count").GetInt32();
        Assert.InRange(findingCount, 1, 3);
        Assert.True(root.TryGetProperty("truncated", out var truncated) && !truncated.GetBoolean(), "an uncut reply says truncated: false");
        Assert.Equal(findingCount, root.GetProperty("findings_returned").GetInt32());
        Assert.Equal(findingCount, root.GetProperty("findings").GetArrayLength());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    /* ---------------- seeding ---------------- */

    /// <summary>Every scored wait type saturated for a four-hour window ending a minute ago, 40 blocking events,
    /// six deadlocks and six regressed queries (the PLAN_REGRESSION finding carries five force-plan targets, the
    /// finding's cap, the shape the issue measured at about 15 KB): about 44 KB of findings before any cut.
    /// Returns the window's end.</summary>
    private static async Task<DateTime> SeedManyFindingsAsync(NpgsqlDataSource postgres, string serverName, CancellationToken ct)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
        var windowStart = windowEnd.AddHours(-4);
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await RegisterServerAsync(connection, serverId, serverName, ct);
        await PlantWaitAsync(connection, serverId, serverName, windowEnd.AddHours(-30), "OLD_WAIT", 1_000L, ct);
        foreach (var waitType in ScoredWaitTypes)
        {
            await PlantWaitAsync(connection, serverId, serverName, windowStart, waitType, 0L, ct);
            for (var i = 1; i <= 16; i++)
                await PlantWaitAsync(connection, serverId, serverName, windowStart.AddMinutes(15 * i), waitType, 900_000L, ct);
        }
        for (var i = 0; i < 40; i++)
            await PlantBlockingAsync(connection, serverId, serverName, windowStart.AddMinutes(3 + (i * 6)), 70 + i, ct);
        for (var i = 0; i < 6; i++)
            await PlantDeadlockAsync(connection, serverId, serverName, windowStart.AddMinutes(20 + (i * 30)), ct);
        for (long q = 1; q <= 6; q++)
        {
            await PlantQueryStorePlanAsync(connection, serverId, serverName, q, planId: (q * 10) + 1, "0xCHEAP" + q, cpuUs: 100_000,
                firstIntervalId: 1, lastExec: windowEnd.AddDays(-5), windowEnd, ct);
            await PlantQueryStorePlanAsync(connection, serverId, serverName, q, planId: (q * 10) + 2, "0xCOSTLY" + q, cpuUs: 100_000 * (2 + (q / 2.0)),
                firstIntervalId: 11, lastExec: windowEnd, windowEnd, ct);
        }

        return windowEnd;
    }

    /// <summary>The wait types <c>FactScorer</c> grades, each one its own fact and its own story root.</summary>
    private static readonly string[] ScoredWaitTypes =
    [
        "CXPACKET", "SOS_SCHEDULER_YIELD", "THREADPOOL", "PAGEIOLATCH_SH", "PAGEIOLATCH_EX",
        "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE", "WRITELOG", "PAGELATCH_UP",
    ];

    /// <summary>Records <paramref name="sweepCount"/> alerts-on sweeps ending one minute ago at <paramref name="cadence"/>,
    /// each with a document of <paramref name="serversPerReport"/> server cards (about 300 bytes a card) and two
    /// verdict rows. Returns the sweep ids newest first.</summary>
    private static async Task<List<long>> SeedSweepsAsync(
        NpgsqlDataSource postgres, int sweepCount, TimeSpan cadence, int serversPerReport, CancellationToken ct)
    {
        var newest = DateTime.UtcNow.AddMinutes(-1);
        var ids = new List<long>(sweepCount);
        const long idBase = 8_081_000_000_000_000_000L;
        for (var i = 0; i < sweepCount; i++)
        {
            var sweptAt = newest - (cadence * i);
            var id = idBase + (sweepCount - i);
            ids.Add(id);
            var cards = string.Join(",", Enumerable.Range(1, serversPerReport).Select(n =>
                $"{{\"server\":\"server-{n}\",\"band\":\"healthy\",\"reason\":\"{new string('x', 240)}\"}}"));
            var run = new FleetSweepRun(
                SweepId: id,
                SweptAtUtc: sweptAt,
                SpanStartUtc: sweptAt - cadence,
                SpanEndUtc: sweptAt,
                PreviousSweepId: i + 1 < sweepCount ? idBase + (sweepCount - i - 1) : null,
                AlertsEnabled: true,
                ServersExpected: 10,
                ServersReported: 10,
                InstrumentsAlive: true,
                InstrumentLivenessJson: """{"alert_pass_counter":{"read":true,"advancing":true}}""",
                ReportJson: $"{{\"sweep_id\":{id},\"servers\":[{cards}]}}");
            await FleetSweepStore.RecordSweepAsync(
                postgres,
                run,
                new[]
                {
                    new FleetSweepServerVerdict(1, "server-1", "Healthy", "Healthy", null, null),
                    new FleetSweepServerVerdict(2, "server-2", "Warning", "Healthy", "cpu above the warning line", """{"cpu":82}"""),
                },
                Array.Empty<FleetSweepWouldHavePagedEntry>(),
                Array.Empty<FleetSweepWatchItem>(),
                ct);
        }

        return ids;
    }

    /// <summary>The scratch store's connection string with the store's search_path on every pooled session, so the
    /// tools' and the seeding's unqualified table names resolve (the <c>QueryStoreClutterLivePostgresTests</c> shape).</summary>
    private static string WithSearchPath(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, string serverName, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 16, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 16;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at, string waitType, long deltaWaitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, 0)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(deltaWaitMs > 0 ? 10L : 0L);
        command.Parameters.AddWithValue(deltaWaitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantBlockingAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at, int blockedSpid, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time,
     wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name)
VALUES ($1, $2, $3, $4, $2, 12000, 60, $5, 'suspended', 'AppDb')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(blockedSpid);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Two hourly intervals of one plan's Query Store runtime stats, collected twice each, the way
    /// <c>PlanRegressionDrillDownReuseLiveTests</c> plants them: a cheap plan whose last execution is days old
    /// beside a costly plan executing now is one regressed query.</summary>
    private static async Task PlantQueryStorePlanAsync(
        NpgsqlConnection connection, int serverId, string serverName, long queryId, long planId, string planHash, double cpuUs,
        long firstIntervalId, DateTime lastExec, DateTime windowEnd, CancellationToken ct)
    {
        for (var interval = 0; interval < 2; interval++)
        {
            var firstExec = lastExec.AddHours(-(2 - interval));
            for (var collection = 1; collection <= 2; collection++)
            {
                using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     query_hash, query_plan_hash, execution_count,
     avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, 'AppDb', $5, $6, 'Regular', NULL, $7, $8, $8, $9, $10, $11, $12, $13, $14, false, 0)", connection);
                command.Parameters.AddWithValue(CollectionIdGenerator.Next());
                command.Parameters.AddWithValue(windowEnd.AddMinutes(-10 + collection));
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(serverName);
                command.Parameters.AddWithValue(queryId);
                command.Parameters.AddWithValue(planId);
                command.Parameters.AddWithValue(firstIntervalId + interval);
                command.Parameters.AddWithValue(firstExec);
                command.Parameters.AddWithValue(lastExec.AddHours(-(1 - interval)));
                command.Parameters.AddWithValue("0xQH" + queryId.ToString(CultureInfo.InvariantCulture));
                command.Parameters.AddWithValue(planHash);
                command.Parameters.AddWithValue(50L * collection);
                command.Parameters.AddWithValue((long)cpuUs);
                command.Parameters.AddWithValue((long)cpuUs + 20_000);
                await command.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task PlantDeadlockAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name)
VALUES ($1, $2, $3, $4, $2, '<deadlock/>', 'AppDb')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }
}
