/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6010 part 2, Lite's twins of Darling's #6003 pins over DuckDB: a join that brings no new metric and no higher severity is
/// journaled <c>joined_quiet</c> (not <c>joined</c>), a new metric or a higher severity journals <c>joined</c>, a request is
/// <c>resolved</c> only when every linked metric has a resolution row after its last alert on that server, and the v73 step
/// adds <c>investigation_requests.max_severity</c> to a new and an existing database and changes nothing the second time.
/// </summary>
public sealed class LiteInvestigationRequestEventsTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);
    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly DuckDbInvestigationStore _store;
    private DateTime _now = T0;

    public LiteInvestigationRequestEventsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteInvestigationRequestEventsTests_" + Guid.NewGuid().ToString("N")[..8]);
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

    private static InvestigationAlert Alert(DateTime at, string metric, string? severity = null, int serverId = 7) =>
        new(1, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), "blocking", metric, at, new[] { "k-" + metric }, null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240)
        { Severity = severity };

    private string? Scalar(string sql, DuckDbInitializer? db = null)
    {
        using var connection = (db ?? _db).CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private void Exec(string sql, DuckDbInitializer? db = null)
    {
        using var connection = (db ?? _db).CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    /// <summary>A resolution row as Lite's history store writes one: the title is the metric_name and the context names the metric it resolves.</summary>
    private void Resolution(DateTime at, string firingMetric, int serverId = 7, string? context = null)
    {
        var json = (context ?? "{\"Resolves\":\"" + firingMetric + "\"}").Replace("'", "''");
        Exec($@"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, context_json)
VALUES ('{at:yyyy-MM-dd HH:mm:ss.ffffff}', {serverId}, 'srv-{serverId}', '{firingMetric} Resolved', 0, 0, '{json}')");
    }

    private string Journal(string evt) => Scalar($"SELECT count(*) FROM investigation_journal WHERE event = '{evt}'")!;

    [Fact]
    public async Task ASameSeverityJoinWithNoNewMetric_JournalsAQuietJoin_AndNewMetricOrHigherSeverityJournalsJoined()
    {
        var first = await _store.OpenOrJoinAsync(Alert(T0, "Blocking", "warning"), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, first.Outcome);
        Assert.Equal("warning", Scalar("SELECT max_severity FROM investigation_requests"));

        _now = T0.AddMinutes(1);
        var same = await _store.OpenOrJoinAsync(Alert(_now, "Blocking", "warning"), Ct);
        Assert.Equal(InvestigationOpenOutcome.Joined, same.Outcome);
        Assert.False(same.Evented);
        Assert.Equal("1", Journal(InvestigationEvents.QuietJoin));
        Assert.Equal("0", Journal(InvestigationEvents.Joined));

        _now = T0.AddMinutes(2);
        var other = await _store.OpenOrJoinAsync(Alert(_now, "Blocking Wait", "warning"), Ct);
        Assert.True(other.Evented);
        Assert.Equal("1", Journal(InvestigationEvents.Joined));

        _now = T0.AddMinutes(3);
        var higher = await _store.OpenOrJoinAsync(Alert(_now, "Blocking", "critical"), Ct);
        Assert.True(higher.Evented);
        Assert.Equal("2", Journal(InvestigationEvents.Joined));
        Assert.Equal("critical", Scalar("SELECT max_severity FROM investigation_requests"));

        /* The high-water mark holds: a warning after a critical is quiet again. */
        _now = T0.AddMinutes(4);
        Assert.False((await _store.OpenOrJoinAsync(Alert(_now, "Blocking", "warning"), Ct)).Evented);
        Assert.Equal("2", Journal(InvestigationEvents.QuietJoin));
        Assert.Equal("critical", Scalar("SELECT max_severity FROM investigation_requests"));
    }

    /* A follow-up opens now but copies its source's alert rows with their original times, so its last alert can be weeks old.
       A clear logged after that alert still resolves it: the read's bound is the oldest last alert, not a fixed window. */
    [Fact]
    public async Task ARequestWhoseAlertsAreOlderThanThreeDays_IsResolvedByAClearAfterThem()
    {
        await _store.OpenOrJoinAsync(Alert(T0, "Blocking", "warning"), Ct);
        var alertAt = T0.AddDays(-10);
        Exec($"UPDATE investigation_request_alerts SET alert_time = '{alertAt:yyyy-MM-dd HH:mm:ss.ffffff}'");
        Resolution(alertAt.AddDays(1), "Blocking");
        _now = T0.AddMinutes(1);
        Assert.Equal(1, (await _store.SweepAsync(Ct)).Resolved);
    }

    [Fact]
    public async Task ARequestIsResolved_OnlyWhenEveryLinkedMetricHasAResolutionRowAfterItsLastAlert_OnItsOwnServer()
    {
        var opened = await _store.OpenOrJoinAsync(Alert(T0, "Blocking", "warning"), Ct);
        _now = T0.AddMinutes(2);
        await _store.OpenOrJoinAsync(Alert(_now, "Blocking Wait", "warning"), Ct);

        /* One of two metrics resolved: still live. A row on another server, and one from before the last alert, count for nothing. */
        _now = T0.AddMinutes(10);
        Resolution(T0.AddMinutes(5), "Blocking");
        Resolution(T0.AddMinutes(6), "Blocking Wait", serverId: 99);
        Resolution(T0.AddMinutes(1), "Blocking Wait");
        var waiting = await _store.SweepAsync(Ct);
        Assert.Equal(0, waiting.Resolved);
        Assert.Equal("0", Journal(InvestigationEvents.Resolved));

        /* The second metric clears: resolved, once, however often the sweep runs. */
        Resolution(T0.AddMinutes(7), "Blocking Wait");
        var done = await _store.SweepAsync(Ct);
        Assert.Equal(1, done.Resolved);
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE event = 'resolved' AND request_id = '{opened.RequestId}'"));
        Assert.Equal(0, (await _store.SweepAsync(Ct)).Resolved);
        Assert.Equal("1", Journal(InvestigationEvents.Resolved));
    }

    [Fact]
    public async Task ARealertAfterTheResolutionRow_KeepsTheRequestLive()
    {
        await _store.OpenOrJoinAsync(Alert(T0, "Blocking", "warning"), Ct);
        Resolution(T0.AddMinutes(3), "Blocking");
        _now = T0.AddMinutes(4);
        await _store.OpenOrJoinAsync(Alert(_now, "Blocking", "warning"), Ct);
        Assert.Equal(0, (await _store.SweepAsync(Ct)).Resolved);
    }

    [Fact]
    public async Task SchemaStepV73_AppliesToANewAndAnExistingDatabase_AndASecondRunChangesNothing()
    {
        const string ColumnSql = "SELECT count(*) FROM information_schema.columns WHERE table_name = 'investigation_requests' AND column_name = 'max_severity'";
        Assert.Equal("1", Scalar(ColumnSql));
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar("SELECT max(version) FROM schema_version"));

        var path = Path.Combine(_tempDir, "old.duckdb");
        using (var old = new DuckDbInitializer(path))
        {
            await old.InitializeAsync();
            Exec("ALTER TABLE investigation_requests DROP COLUMN max_severity", old);
            Exec("UPDATE schema_version SET version = 72", old);
        }

        using var upgraded = new DuckDbInitializer(path);
        await upgraded.InitializeAsync();
        Assert.Equal("1", Scalar(ColumnSql, upgraded));
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar("SELECT max(version) FROM schema_version", upgraded));
        var shape = Scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'investigation_requests'", upgraded);

        using var again = new DuckDbInitializer(path);
        await again.InitializeAsync();
        Assert.Equal(shape, Scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'investigation_requests'", again));
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar("SELECT max(version) FROM schema_version", again));
    }
}
