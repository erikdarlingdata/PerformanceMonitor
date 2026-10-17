/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
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

    private Task<InvestigationOpenResult> Open(string metric, string family) =>
        _store.OpenOrJoinAsync(new InvestigationAlert(1, Server, "srv", family, metric, T0, new[] { "k-" + metric }, null, 20, 100, 15, 240), Ct);

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
        await using (var old = new DuckDbInitializer(path))
        {
            await old.InitializeAsync();
            Exec2(old, "DROP TABLE investigation_evidence");
            Exec2(old, "ALTER TABLE investigation_requests DROP COLUMN evidence_state");
            Exec2(old, "UPDATE schema_version SET version = 71");
        }

        string Shape(DuckDbInitializer db) =>
            Scalar2(db, "SELECT string_agg(table_name || '.' || column_name, ',' ORDER BY table_name, column_name) FROM information_schema.columns WHERE table_name IN ('investigation_evidence', 'investigation_requests')")!;

        await using var upgraded = new DuckDbInitializer(path);
        await upgraded.InitializeAsync();
        var first = Shape(upgraded);
        Assert.Contains("investigation_evidence.section", first);
        Assert.Contains("investigation_requests.evidence_state", first);
        Assert.Equal("72", Scalar2(upgraded, "SELECT max(version) FROM schema_version"));

        await using var again = new DuckDbInitializer(path);
        await again.InitializeAsync();
        Assert.Equal(first, Shape(again));
        Assert.Equal("72", Scalar2(again, "SELECT max(version) FROM schema_version"));
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
