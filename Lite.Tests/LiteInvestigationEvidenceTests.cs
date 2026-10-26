/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's evidence capture and its schema step (#6010, the DuckDB twins of Darling's #6001 and #6002 behaviours): a section
/// freezes once its collector has run and later collection never changes it, a missing section is pending and then
/// <c>evidence_ready</c> is journaled once, the deadlock and blocking sections carry their rows, and the v72 step applies to a new and
/// an existing database and changes nothing the second time.
/// </summary>
public sealed class LiteInvestigationEvidenceTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);
    private const int Server = 7;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly DuckDbInvestigationStore _store;
    private DateTime _now = T0;

    public LiteInvestigationEvidenceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteInvestigationEvidenceTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _db = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _db.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbInvestigationStore(_db, () => _now, DuckDbInvestigationStoreTests.TestLockBudget);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best effort */ }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private LiteInvestigationEvidenceCapture Capture() => new(_db, _store, null, () => _now);

    private async Task<(Guid RequestId, int X)> Open(string metric, string family)
    {
        var result = await _store.OpenOrJoinAsync(
            new InvestigationAlert(1, Server, "srv", family, metric, T0, new[] { "k-" + metric }, null, 20, 100, 15, 240), Ct);
        return (result.RequestId!.Value, 0);
    }

    private void Exec(string sql)
    {
        using var connection = _db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private string? Scalar(string sql)
    {
        using var connection = _db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private static string Ts(DateTime t) => t.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private int _logId;

    private void CollectorRan(string collector, DateTime at) =>
        Exec($"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ({++_logId}, {Server}, 'srv', '{collector}', '{Ts(at)}', 'SUCCESS')");

    private int _deadlockId;

    private void DeadlockRow(DateTime at) =>
        Exec($@"INSERT INTO deadlocks (deadlock_id, collection_time, deadlock_time, server_id, server_name, database_name, victim_process_id, deadlock_graph_xml)
VALUES ({++_deadlockId}, '{Ts(at)}', '{Ts(at)}', {Server}, 'srv', 'AppDb', 'p1',
'<deadlock><victim-list><victimProcess id=""p1""/></victim-list><process-list><process id=""p1"" spid=""55"" currentdbname=""AppDb""><inputbuf>UPDATE a SET x = 1</inputbuf></process><process id=""p2"" spid=""56"" currentdbname=""AppDb""><inputbuf>UPDATE b SET x = 1</inputbuf></process></process-list><resource-list><keylock objectname=""AppDb.dbo.Orders"" indexname=""PK_Orders"" mode=""X""/></resource-list></deadlock>')");

    [Fact]
    public async Task DeadlockSection_IsPendingUntilItsCollectorRuns_ThenFrozenWithTheRowsAndTheEventIsJournaledOnce()
    {
        var opened = await Open("Deadlocks Detected", "blocking");
        DeadlockRow(T0.AddSeconds(-30));

        /* The collector has not completed a run since the window end: the section is pending and nothing is journaled. */
        _now = T0.AddMinutes(2);
        await Capture().CaptureOnceAsync(Ct);
        var pending = await _store.ReadEvidenceAsync(opened.RequestId, Ct);
        Assert.Equal("pending", Assert.Single(pending).State);
        Assert.Equal("0", Scalar("SELECT count(*) FROM investigation_journal WHERE event = 'evidence_ready'"));

        CollectorRan("deadlocks", T0.AddMinutes(2));
        await Capture().CaptureOnceAsync(Ct);
        var ready = Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct));
        Assert.Equal(("deadlocks", "ready"), (ready.Section, ready.State));
        var rows = JsonNode.Parse(ready.ContentJson!)!["rows"]!.AsArray();
        var deadlock = Assert.Single(rows)!.AsObject();
        Assert.Equal("p1", deadlock["victim_process_id"]!.GetValue<string>());
        Assert.Equal(2, deadlock["parties"]!.AsArray().Count);
        Assert.Equal("ready", Scalar($"SELECT evidence_state FROM investigation_requests WHERE request_id = '{opened.RequestId}'"));

        /* A second pass writes no second journal row. */
        await Capture().CaptureOnceAsync(Ct);
        Assert.Equal("1", Scalar("SELECT count(*) FROM investigation_journal WHERE event = 'evidence_ready'"));
    }

    [Fact]
    public async Task ReadySection_DoesNotChange_WhenMoreRowsAreCollectedLater()
    {
        var opened = await Open("Deadlocks Detected", "blocking");
        DeadlockRow(T0.AddSeconds(-30));
        CollectorRan("deadlocks", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);
        var frozen = Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct));

        /* Another deadlock lands inside the same window, and the collector runs again. */
        DeadlockRow(T0.AddSeconds(-10));
        CollectorRan("deadlocks", T0.AddMinutes(4));
        _now = T0.AddMinutes(5);
        await Capture().CaptureOnceAsync(Ct);
        Assert.Equal(frozen.ContentJson, Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct)).ContentJson);

        /* The store itself refuses to rewrite a frozen section too. */
        await _store.RecordEvidenceAsync(opened.RequestId, Server,
            new[] { new LiteEvidenceWrite("deadlocks", "ready", "{\"rows\":[]}") }, null, false, Ct);
        Assert.Equal(frozen.ContentJson, Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct)).ContentJson);
    }

    [Fact]
    public async Task BlockingSections_AreReadyWithTheChainAndHead_WhenTheBlockedProcessReportRan()
    {
        var opened = await Open("Blocking Detected", "blocking");
        Exec($@"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, blocked_spid, blocking_spid, wait_time_ms, blocked_process_report_xml)
VALUES (1, '{Ts(T0)}', '{Ts(T0.AddSeconds(-20))}', {Server}, 'srv', 'AppDb', 60, 55, 9000, '<blocked-process-report><blocked-process><process spid=""60""/></blocked-process><blocking-process><process status=""sleeping"" spid=""55""><inputbuf>BEGIN TRAN</inputbuf></process></blocking-process></blocked-process-report>')");
        CollectorRan("blocked_process_report", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var sections = (await _store.ReadEvidenceAsync(opened.RequestId, Ct)).ToDictionary(r => r.Section);
        Assert.Equal(new[] { "blocking_chains", "head_blocker" }, sections.Keys.OrderBy(k => k).ToArray());
        Assert.All(sections.Values, s => Assert.Equal("ready", s.State));
        var chain = JsonNode.Parse(sections["blocking_chains"].ContentJson!)!["rows"]![0]!;
        Assert.Equal(60, chain["blocked_spid"]!.GetValue<int>());
        Assert.Equal(55, chain["head_blocker_spid"]!.GetValue<int>());
        var head = JsonNode.Parse(sections["head_blocker"].ContentJson!)!["rows"]![0]!;
        Assert.Equal(55, head["spid"]!.GetValue<int>());
        Assert.Equal("inferred", head["statement_source"]!.GetValue<string>());
    }

    private const string BlockedReportXml =
        "<blocked-process-report><blocked-process><process spid=\"60\"/></blocked-process><blocking-process><process status=\"sleeping\" spid=\"55\"><inputbuf>BEGIN TRAN</inputbuf></process></blocking-process></blocked-process-report>";

    private int _reportId;

    private void BlockedReportRow(DateTime eventAt, DateTime collectedAt, int blockedSpid = 60, int blockingSpid = 55) =>
        Exec($@"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, blocked_spid, blocking_spid, wait_time_ms, blocked_process_report_xml)
VALUES ({++_reportId}, '{Ts(collectedAt)}', '{Ts(eventAt)}', {Server}, 'srv', 'AppDb', {blockedSpid}, {blockingSpid}, 9000, '{BlockedReportXml}')");

    /// <summary>A parallel query's thread waiting on its own session (blocked spid = blocking spid) is not blocking
    /// (BlockingWaits.NotSelfWaitSql): a newer self-wait report for the blocked session must not make it its own head.</summary>
    [Fact]
    public async Task BlockingSections_LeaveAParallelSelfWaitOut_SoTheRealHeadStaysTheHead()
    {
        var opened = await Open("Blocking Detected", "blocking");
        BlockedReportRow(T0.AddSeconds(-20), T0);
        BlockedReportRow(T0.AddSeconds(-5), T0, blockedSpid: 60, blockingSpid: 60);
        CollectorRan("blocked_process_report", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var sections = (await _store.ReadEvidenceAsync(opened.RequestId, Ct)).ToDictionary(r => r.Section);
        var chain = Assert.Single(JsonNode.Parse(sections["blocking_chains"].ContentJson!)!["rows"]!.AsArray())!;
        Assert.Equal(60, chain["blocked_spid"]!.GetValue<int>());
        Assert.Equal(55, chain["blocking_spid"]!.GetValue<int>());
        Assert.Equal(55, chain["head_blocker_spid"]!.GetValue<int>());
        var head = Assert.Single(JsonNode.Parse(sections["head_blocker"].ContentJson!)!["rows"]!.AsArray())!;
        Assert.Equal(55, head["spid"]!.GetValue<int>());
    }

    [Fact]
    public async Task DeadlockSection_ListsADeadlockStoredInTwoBatchesOnce()
    {
        var opened = await Open("Deadlocks Detected", "blocking");
        var eventAt = T0.AddSeconds(-30);
        DeadlockRow(eventAt);
        /* The same event again, collected by a later batch. */
        Exec($"INSERT INTO deadlocks SELECT {++_deadlockId} AS deadlock_id, TIMESTAMP '{Ts(T0.AddMinutes(1))}' AS collection_time, * EXCLUDE (deadlock_id, collection_time) FROM deadlocks WHERE deadlock_id = 1");
        Assert.Equal("2", Scalar("SELECT count(*) FROM deadlocks"));
        CollectorRan("deadlocks", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var ready = Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct));
        Assert.Single(JsonNode.Parse(ready.ContentJson!)!["rows"]!.AsArray());
    }

    [Fact]
    public async Task BlockingSections_CountAReportStoredInTwoBatchesOnce()
    {
        var opened = await Open("Blocking Detected", "blocking");
        var eventAt = T0.AddSeconds(-20);
        BlockedReportRow(eventAt, T0);
        BlockedReportRow(eventAt, T0.AddMinutes(1));
        CollectorRan("blocked_process_report", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var sections = (await _store.ReadEvidenceAsync(opened.RequestId, Ct)).ToDictionary(r => r.Section);
        var head = JsonNode.Parse(sections["head_blocker"].ContentJson!)!["rows"]!.AsArray();
        Assert.Equal(1, Assert.Single(head)!["reports_in_chain"]!.GetValue<int>());
    }

    private async Task<Dictionary<string, JsonNode>> HighCpuSections(Action seed)
    {
        var opened = await Open("High CPU", "resources");
        seed();
        foreach (var collector in new[] { "query_stats", "query_snapshots", "query_store_health", "running_jobs", "wait_stats" })
            CollectorRan(collector, T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);
        var sections = await _store.ReadEvidenceAsync(opened.RequestId, Ct);
        Assert.All(sections, s => Assert.Equal("ready", s.State));
        return sections.ToDictionary(s => s.Section, s => JsonNode.Parse(s.ContentJson!)!);
    }

    private static JsonObject OnlyRow(JsonNode section) => Assert.Single(section["rows"]!.AsArray())!.AsObject();

    [Fact]
    public async Task HighCpuSections_EachFreezeTheRowCollectedInTheWindow()
    {
        var at = T0.AddMinutes(-5);
        var sections = await HighCpuSections(() =>
        {
            Exec($@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes)
VALUES (1, '{Ts(at)}', {Server}, 'srv', 'AppDb', '0xQH1', '0xPH1', 10, 5000000, 6000000, 123, 1)");
            Exec($@"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, blocking_session_id, cpu_time_ms, total_elapsed_time_ms, login_name, host_name, program_name)
VALUES (2, '{Ts(at)}', {Server}, 'srv', 61, 'AppDb', 'SELECT 1', 'running', 0, 4000, 5000, 'app_login', 'web01', 'AppSvc')");
            Exec($@"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES (3, '{Ts(at)}', {Server}, 'srv', 'SOS_SCHEDULER_YIELD', 10, 1000, 100, 5, 500, 50)");
            Exec($@"INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES ('{Ts(at)}', {Server}, 'srv', 'Nightly ETL', 'job-1', true, '{Ts(at.AddMinutes(-60))}', 3600, 900, 1200, 42, true, 400.0)");
            Exec($@"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
VALUES (4, '{Ts(at)}', {Server}, 'srv', 'AppDb', 'READ_ONLY', 'READ_WRITE', 8, 0, 1000, 'AUTO', 30, 200, 60)");
        });

        Assert.Equal(new[] { "active_requests", "jobs", "query_store", "top_statements", "waits" }, sections.Keys.OrderBy(k => k).ToArray());

        var top = OnlyRow(sections["top_statements"]);
        Assert.Equal("0xQH1", top["query_hash"]!.GetValue<string>());
        Assert.Equal(5000000L, top["total_cpu_us"]!.GetValue<long>());
        Assert.Equal(10L, top["executions"]!.GetValue<long>());

        var active = OnlyRow(sections["active_requests"]);
        Assert.Equal("app_login", active["login_name"]!.GetValue<string>());
        Assert.Equal("web01", active["host_name"]!.GetValue<string>());
        Assert.Equal(4000L, active["total_cpu_ms"]!.GetValue<long>());

        var wait = OnlyRow(sections["waits"]);
        Assert.Equal("SOS_SCHEDULER_YIELD", wait["wait_type"]!.GetValue<string>());
        Assert.Equal(500L, wait["total_wait_time_ms"]!.GetValue<long>());

        var job = OnlyRow(sections["jobs"]);
        Assert.Equal("Nightly ETL", job["job_name"]!.GetValue<string>());
        Assert.True(job["is_running_long"]!.GetValue<bool>());

        var queryStore = OnlyRow(sections["query_store"]);
        Assert.Equal("AppDb", queryStore["database_name"]!.GetValue<string>());
        Assert.Equal("READ_ONLY", queryStore["actual_state"]!.GetValue<string>());
        Assert.Equal(5000000L, queryStore["window_cpu_us"]!.GetValue<long>());
    }

    private int _attentionId;

    private void AttentionRow(DateTime at, string kind, int count, string? app, string? host, string? fingerprint, string? text) =>
        Exec($@"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, client_hostname, statement_fingerprint, statement_text)
VALUES ({++_attentionId}, '{Ts(at)}', {Server}, 'srv', '{kind}', {count}, {(app is null ? "NULL" : "'AppDb'")}, {(app is null ? "NULL" : $"'{app}'")}, {(host is null ? "NULL" : $"'{host}'")}, {(fingerprint is null ? "NULL" : $"'{fingerprint}'")}, {(text is null ? "NULL" : $"'{text}'")})");

    /// <summary>#6579: a Client Timeouts request freezes the exact count and the top apps, hosts and statements of the window, the statement
    /// text under a statement-class field (so metadataOnly masks it), cut at a text-element boundary; a row outside the window does not count.</summary>
    [Fact]
    public async Task ClientTimeoutsSection_FreezesTheCountTopAppsHostsAndStatements_TheTextIsMaskedAtMetadataOnly_AndTheCutKeepsSurrogatePairsWhole()
    {
        var opened = await Open("Client Timeouts", "query_performance");
        var longText = new string('a', LiteInvestigationEvidenceCapture.ClientTimeoutStatementMaxChars - 1) + "😀 tail";
        AttentionRow(T0.AddMinutes(-2), "count", 4, null, null, null, null);
        AttentionRow(T0.AddMinutes(-2), "sample", 0, "SSMS", "WS-ERIK", "fp-a", "SELECT ''CANARYTEXT'' FROM t");
        AttentionRow(T0.AddMinutes(-2), "sample", 0, "SSMS", "WS-ERIK", "fp-a", "SELECT ''CANARYTEXT'' FROM t");
        AttentionRow(T0.AddMinutes(-2), "sample", 0, "SSMS", "WS-ERIK", "fp-a", "SELECT ''CANARYTEXT'' FROM t");
        AttentionRow(T0.AddMinutes(-2), "sample", 0, "web", "APP01", "fp-b", longText);
        AttentionRow(T0.AddHours(-3), "sample", 0, "old-app", "OLD01", "fp-old", "SELECT 3");
        CollectorRan("attention", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var section = Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct), r => r.Section == "client_timeouts");
        Assert.Equal("ready", section.State);
        var row = OnlyRow(JsonNode.Parse(section.ContentJson!)!);
        Assert.Equal(4L, row["count"]!.GetValue<long>());
        var hosts = row["top_hosts"]!.AsArray();
        Assert.Equal(new[] { "WS-ERIK", "APP01" }, hosts.Select(h => h!["client_hostname"]!.GetValue<string>()).ToArray());
        Assert.Equal(3L, hosts[0]!["sampled_events"]!.GetValue<long>());
        Assert.Equal(new[] { "SSMS", "web" }, row["top_client_apps"]!.AsArray().Select(a => a!["client_app_name"]!.GetValue<string>()).ToArray());
        var statements = row["top_statements"]!.AsArray();
        Assert.Equal(new[] { "fp-a", "fp-b" }, statements.Select(st => st!["statement_fingerprint"]!.GetValue<string>()).ToArray());

        /* Every field that carries the statement text is masked at metadataOnly. */
        var first = statements[0]!.AsObject();
        var carriers = first.Where(p => p.Value is JsonValue v && v.TryGetValue<string>(out var t) && t.Contains("CANARYTEXT", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(carriers);
        foreach (var carrier in carriers)
        {
            var text = carrier.Value!.GetValue<string>();
            Assert.DoesNotContain("CANARYTEXT", McpDataDetailMasker.MaskField(carrier.Key, text, McpDataDetail.MetadataOnly), StringComparison.Ordinal);
            Assert.Equal(text, McpDataDetailMasker.MaskField(carrier.Key, text, McpDataDetail.Full));
        }

        var cut = statements[1]!["sql_text"]!.GetValue<string>();
        Assert.False(char.IsHighSurrogate(cut[^1]), "the cut left a lone high surrogate");
        Assert.True(cut.Length <= LiteInvestigationEvidenceCapture.ClientTimeoutStatementMaxChars);
    }

    /// <summary>#6579: no timeouts in the window freezes a ready section with no rows (a true negative).</summary>
    [Fact]
    public async Task ClientTimeoutsSection_WithNoTimeoutsInTheWindow_FreezesEmpty()
    {
        var opened = await Open("Client Timeouts", "query_performance");
        CollectorRan("attention", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var section = Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct), r => r.Section == "client_timeouts");
        Assert.Equal("ready", section.State);
        Assert.Empty(JsonNode.Parse(section.ContentJson!)!["rows"]!.AsArray());
    }

    [Fact]
    public async Task RegressionTargetsSection_FreezesTheRegressedQueryWithItsGoodAndBadPlan()
    {
        var opened = await Open("Query Slow", InvestigationFamily.QueryPerformance);
        var seen = T0.AddMinutes(-5);
        void Plan(int id, int plan, long avgCpu, long executions) =>
            Exec($@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, query_plan_hash, runtime_stats_interval_id)
VALUES ({id}, '{Ts(seen)}', {Server}, 'srv', 'AppDb', 7, {plan}, 'Regular', '{Ts(seen.AddHours(-1))}', '{Ts(seen.AddMinutes(plan))}', {executions}, {avgCpu}, {avgCpu}, '0xHASH{plan}', {plan})");
        Plan(1, 1, 100, 50);
        Plan(2, 2, 1000, 40);
        CollectorRan("query_store", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var ready = Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct));
        Assert.Equal(("regression_targets", "ready"), (ready.Section, ready.State));
        var target = OnlyRow(JsonNode.Parse(ready.ContentJson!)!);
        Assert.Equal(7L, target["query_id"]!.GetValue<long>());
        Assert.Equal(1L, target["good_plan_id"]!.GetValue<long>());
        Assert.Equal(2L, target["bad_plan_id"]!.GetValue<long>());
        Assert.Equal(10.0, target["regression_factor"]!.GetValue<double>(), 3);
    }

    [Fact]
    public async Task RegressionTargetsSection_LeavesOutSqlServerOwnHousekeepingStatements_SoTheApplicationQueryLeads()
    {
        // #6548: the change tracking cleanup regressed 100 times over, the application query 10 times over.
        var opened = await Open("Query Slow", InvestigationFamily.QueryPerformance);
        var seen = T0.AddMinutes(-5);
        void Plan(int id, int queryId, int plan, long avgCpu, long executions, string text) =>
            Exec($@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, query_plan_hash, runtime_stats_interval_id, query_text)
VALUES ({id}, '{Ts(seen)}', {Server}, 'srv', 'AppDb', {queryId}, {plan}, 'Regular', '{Ts(seen.AddHours(-1))}', '{Ts(seen.AddMinutes(plan))}', {executions}, {avgCpu}, {avgCpu}, '0xHASH{plan}', {plan}, '{text}')");
        Plan(1, 7, 1, 100, 50, "SELECT * FROM dbo.Orders");
        Plan(2, 7, 2, 1000, 40, "SELECT * FROM dbo.Orders");
        const string cleanup = "delete top(@batch_size) from sys.[change_tracking_1228027148] where sys_change_xdes_id < @cutoff";
        Plan(3, 8, 3, 100, 50, cleanup);
        Plan(4, 8, 4, 10000, 40, cleanup);
        CollectorRan("query_store", T0.AddMinutes(2));
        _now = T0.AddMinutes(3);
        await Capture().CaptureOnceAsync(Ct);

        var ready = Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct));
        var target = OnlyRow(JsonNode.Parse(ready.ContentJson!)!);
        Assert.Equal(7L, target["query_id"]!.GetValue<long>());
    }

    [Theory]
    [InlineData("delete top(@batch_size) from sys.change_tracking_1234567 where x < @y", true)]
    [InlineData("(@csn bigint,@batch_size int)delete top(@batch_size) from sys.[change_tracking_1228027148] where sys_change_xdes_id < @csn", true)]
    [InlineData("DELETE TOP (@batch_size)\r\n FROM SYS.[CHANGE_TRACKING_1228027148]", true)]
    [InlineData("DELETE TOP (@batch_size)\r\n FROM sys.change_tracking_99", true)]
    [InlineData("EXEC sys.sp_flush_commit_table_on_demand 1000", true)]
    [InlineData("EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_Orders'", true)]
    [InlineData("SELECT * FROM sys.[change_tracking_tables]", false)]
    [InlineData("SELECT * FROM sys.change_tracking_tables", false)]
    [InlineData("SELECT * FROM dbo.Orders", false)]
    [InlineData(null, false)]
    public void InternalStatements_RecognizesTheEnginesOwnCleanup_AndNothingElse(string? text, bool expected) =>
        Assert.Equal(expected, PerformanceMonitor.Common.InternalStatements.IsInternal(text));

    [Fact]
    public async Task SectionWithNoCollectorRun_BecomesNotCollected_AfterItsGrace()
    {
        var opened = await Open("Deadlocks Detected", "blocking");
        _now = T0.AddMinutes(21);
        await Capture().CaptureOnceAsync(Ct);
        Assert.Equal("not_collected", Assert.Single(await _store.ReadEvidenceAsync(opened.RequestId, Ct)).State);
        Assert.Equal("not_collected", Scalar($"SELECT evidence_state FROM investigation_requests WHERE request_id = '{opened.RequestId}'"));
    }

    [Fact]
    public async Task SchemaStep_AppliesToAnExistingV71Database_AndASecondRunChangesNothing()
    {
        Assert.Equal("0", Scalar("SELECT count(*) FROM investigation_evidence"));
        Assert.Equal("1", Scalar("SELECT count(*) FROM information_schema.columns WHERE table_name = 'investigation_requests' AND column_name = 'evidence_state'"));

        /* Roll the file back to what a v71 database holds: no evidence table, no evidence_state column. */
        var path = Path.Combine(_tempDir, "old.duckdb");
        using (var old = new DuckDbInitializer(path))
        {
            await old.InitializeAsync();
            Exec2(old, "DROP TABLE investigation_evidence");
            Exec2(old, "ALTER TABLE investigation_requests DROP COLUMN evidence_state");
            Exec2(old, "UPDATE schema_version SET version = 71");
        }

        string Shape(DuckDbInitializer db) =>
            Scalar2(db, "SELECT string_agg(table_name || '.' || column_name, ',' ORDER BY table_name, column_name) FROM information_schema.columns WHERE table_name IN ('investigation_evidence', 'investigation_requests')")!;

        using var upgraded = new DuckDbInitializer(path);
        await upgraded.InitializeAsync();
        var first = Shape(upgraded);
        Assert.Contains("investigation_evidence.section", first);
        Assert.Contains("investigation_requests.evidence_state", first);
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar2(upgraded, "SELECT max(version) FROM schema_version"));

        using var again = new DuckDbInitializer(path);
        await again.InitializeAsync();
        Assert.Equal(first, Shape(again));
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar2(again, "SELECT max(version) FROM schema_version"));
    }

    private static void Exec2(DuckDbInitializer db, string sql)
    {
        using var connection = db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static string? Scalar2(DuckDbInitializer db, string sql)
    {
        using var connection = db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }
}
