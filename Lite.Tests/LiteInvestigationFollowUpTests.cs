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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6012 part A, Lite's twins of Darling's #6008 pins over DuckDB: a report's <c>follow_up_at</c> opens one follow-up request
/// when its time comes (never before, never twice), whose brief carries the earlier report; a route holding 20 pending
/// follow-ups refuses the 21st and stores nothing of that report; a time past 30 days is refused through the tool; and the
/// v75 step adds the two columns to a new and an existing database. Lite has no event delivery, so the follow-up is found in the
/// request list.
/// </summary>
public sealed class LiteInvestigationFollowUpTests : IDisposable
{
    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\",\"basis\":\"verified\"}]";
    private const string RootCause = "a nightly index rebuild held the lock follow-up-canary";
    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly DuckDbInvestigationStore _store;
    private DateTime _now = T0;

    public LiteInvestigationFollowUpTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteInvestigationFollowUpTests_" + Guid.NewGuid().ToString("N")[..8]);
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

    private InvestigationAlert Alert(int serverId = 7, int routeId = 1) =>
        new(routeId, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), "blocking", "Blocking", _now,
            new[] { "key-" + serverId.ToString(CultureInfo.InvariantCulture) }, null, 100, 1000, LeaseMinutes: 15, ExpiryMinutes: 240);

    private InvestigationBriefComposer Composer() => new(
        new DuckDbInvestigationBriefSourceReader(_db), new McpDataDetailSetting(McpDataDetail.Full), LiteInvestigationBriefTools.Tools);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private string? Scalar(string sql, DuckDbInitializer? db = null)
    {
        using var connection = (db ?? _db).CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private static void Exec(string sql, DuckDbInitializer db)
    {
        using var connection = db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    /// <summary>Opens a request for a server on a route, claims it and reports it through the tool with <paramref name="followUpAt"/>.</summary>
    private async Task<(Guid Id, JsonElement Answer)> ReportAsync(int serverId, int routeId, string? followUpAt)
    {
        var id = (await _store.OpenOrJoinAsync(Alert(serverId, routeId), Ct)).RequestId!.Value;
        var claim = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), id.ToString(), "agent-a", null, null, Ct));
        var answer = Root(await McpInvestigationTools.SubmitAsync(_store, id.ToString(), claim.GetProperty("claim_id").GetString()!, 1,
            "real", RootCause, Evidence, "summary", null, null, Ct, follow_up_at: followUpAt));
        return (id, answer);
    }

    private static string Iso(DateTime value) => value.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    [Fact]
    public async Task AFollowUp_OpensOnceAtItsTime_NamesTheEarlierRequest_AndItsBriefCarriesTheEarlierReport()
    {
        _now = DateTime.UtcNow;
        var due = _now.AddHours(2);
        var (earlier, answer) = await ReportAsync(7, 1, Iso(due));
        Assert.Equal("reported", answer.GetProperty("status").GetString());

        /* Before the time: nothing opens. */
        _now = due.AddMinutes(-1);
        Assert.Equal(0, (await _store.SweepAsync(Ct)).FollowUps);
        Assert.Equal("1", Scalar("SELECT count(*) FROM investigation_requests"));

        /* At the time: one request opens, naming the earlier one, with one follow_up journal row of trigger time. */
        _now = due.AddMinutes(1);
        Assert.Equal(1, (await _store.SweepAsync(Ct)).FollowUps);
        var followUp = Scalar($"SELECT request_id FROM investigation_requests WHERE follow_up_of = '{earlier}'");
        Assert.NotNull(followUp);
        Assert.Equal("open", Scalar($"SELECT state FROM investigation_requests WHERE request_id = '{followUp}'"));
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{followUp}' AND event = 'follow_up' AND detail LIKE '%\"trigger\":\"time\"%' AND detail LIKE '%{earlier}%'"));

        /* A second sweep opens no second one. */
        _now = due.AddMinutes(5);
        Assert.Equal(0, (await _store.SweepAsync(Ct)).FollowUps);
        Assert.Equal("2", Scalar("SELECT count(*) FROM investigation_requests"));

        /* Its brief carries the earlier report, through the same read as any earlier report of the server and family. */
        var brief = await Composer().ComposeAsync(Guid.Parse(followUp!), Ct);
        Assert.NotNull(brief);
        Assert.Contains("follow-up-canary", brief, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARouteHolding20PendingFollowUps_RefusesThe21st_AndStoresNothingFromThatReport()
    {
        var future = Iso(DateTime.UtcNow.AddDays(3));
        _now = DateTime.UtcNow;
        for (var server = 1; server <= InvestigationReportFields.MaxPendingFollowUpsPerRoute; server++)
        {
            var (_, ok) = await ReportAsync(server, 1, future);
            Assert.Equal("reported", ok.GetProperty("status").GetString());
        }

        var (id, refused) = await ReportAsync(99, 1, future);
        Assert.Equal("refused", refused.GetProperty("status").GetString());
        Assert.Equal("follow_up_at", refused.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains("20 pending follow-ups", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("0", Scalar($"SELECT count(*) FROM investigation_reports WHERE request_id = '{id}'"));
        Assert.Equal("claimed", Scalar($"SELECT state FROM investigation_requests WHERE request_id = '{id}'"));
        Assert.Null(Scalar($"SELECT follow_up_at FROM investigation_requests WHERE request_id = '{id}'"));

        /* Another route has its own count, and the refused report is accepted without a time. */
        var (_, otherRoute) = await ReportAsync(98, 2, future);
        Assert.Equal("reported", otherRoute.GetProperty("status").GetString());
    }

    [Fact]
    public async Task AFollowUpTime31DaysAhead_IsRefusedThroughTheTool_AndNothingIsStored()
    {
        _now = DateTime.UtcNow;
        var (id, answer) = await ReportAsync(7, 1, Iso(DateTime.UtcNow.AddDays(31)));

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Contains("follow_up_at", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("0", Scalar($"SELECT count(*) FROM investigation_reports WHERE request_id = '{id}'"));

        var (_, past) = await ReportAsync(8, 1, Iso(DateTime.UtcNow.AddHours(-1)));
        Assert.Equal("invalid", past.GetProperty("status").GetString());
    }

    [Fact]
    public async Task APurge_ClearsAFollowUpOfThatNamesARequestItDeletes()
    {
        var gone = Guid.NewGuid();
        var kept = Guid.NewGuid();
        var oldTime = _now.AddDays(-400);
        foreach (var (id, followUpOf, opened, state) in new (Guid, Guid?, DateTime, string)[]
                 { (gone, null, oldTime, "reported"), (kept, gone, _now.AddDays(-1), "reported") })
        {
            Exec($@"INSERT INTO investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, state,
     opened_at, expires_at, follow_up_of)
VALUES ('{id}', 1, 7, 'srv-7', 'blocking', 'Blocking', TIMESTAMP '{opened:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{opened:yyyy-MM-dd HH:mm:ss}',
    TIMESTAMP '{opened:yyyy-MM-dd HH:mm:ss}', '{state}', TIMESTAMP '{opened:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{opened.AddDays(1):yyyy-MM-dd HH:mm:ss}',
    {(followUpOf is null ? "NULL" : "'" + followUpOf + "'")})", _db);
        }

        Assert.Equal(1, await _store.PurgeAsync(Ct));
        Assert.Equal("1", Scalar("SELECT count(*) FROM investigation_requests"));
        Assert.Null(Scalar($"SELECT follow_up_of FROM investigation_requests WHERE request_id = '{kept}'"));
    }

    [Fact]
    public async Task SchemaStepV75_AppliesToANewAndAnExistingDatabase_AndASecondRunChangesNothing()
    {
        const string ColumnsSql = "SELECT string_agg(column_name, ',' ORDER BY column_name) FROM information_schema.columns WHERE table_name = 'investigation_requests' AND column_name IN ('follow_up_of', 'follow_up_at')";
        Assert.Equal("follow_up_at,follow_up_of", Scalar(ColumnsSql));
        Assert.Equal("75", Scalar("SELECT max(version) FROM schema_version"));

        var path = Path.Combine(_tempDir, "old.duckdb");
        using (var old = new DuckDbInitializer(path))
        {
            await old.InitializeAsync();
            Exec("ALTER TABLE investigation_requests DROP COLUMN follow_up_of", old);
            Exec("ALTER TABLE investigation_requests DROP COLUMN follow_up_at", old);
            Exec("UPDATE schema_version SET version = 74", old);
        }

        using var upgraded = new DuckDbInitializer(path);
        await upgraded.InitializeAsync();
        Assert.Equal("follow_up_at,follow_up_of", Scalar(ColumnsSql, upgraded));
        Assert.Equal("75", Scalar("SELECT max(version) FROM schema_version", upgraded));

        using var again = new DuckDbInitializer(path);
        await again.InitializeAsync();
        Assert.Equal("follow_up_at,follow_up_of", Scalar(ColumnsSql, again));
        Assert.Equal("75", Scalar("SELECT max(version) FROM schema_version", again));
    }
}
