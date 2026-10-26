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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's investigation store (#5820) on a throwaway DuckDB per test, held to the behavior of Darling's
/// <c>InvestigationRequestStoreLiveTests</c>: each fact here carries the name of the Darling fact it ports (the role and
/// bring-your-own-script facts have no Lite twin). Alerts of one server and family group within the 30-minute window, a
/// reported request is final, the hourly limit opens nothing and journals <c>limit_reached</c> once, two concurrent claims
/// have one winner, a superseded claim's report is <c>lease_lost</c>, a late report under a lapsed or expired lease lands
/// until a newer claim, the sweep expires without reopening, a release keeps its claim counted, and the agent journal is
/// capped at 200. Times are bound from a moved clock; the suite runs in the machine's zone, which need not be UTC.
/// </summary>
public sealed class DuckDbInvestigationStoreTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The write-lock wait the investigation store tests give their store (#5940). The store's write lock is
    /// process-wide (one static lock shared by every <c>DuckDbInitializer</c>), so while other test classes build their own
    /// databases in parallel, one of them can hold it past the shipped 5 s budget on a loaded CI runner. A test that makes
    /// 150 writes in a row (the journal page one) then hit <c>StoreLockTimeoutException</c> from ClaimAsync, which says
    /// nothing about the journal page. These tests are not about the lock budget, so they wait long enough for the holder
    /// to finish. Lock timeout behaviour has its own tests (<c>WriteLockBudgetTests</c>, <c>StoreLockHolderTests</c>).</summary>
    internal static readonly TimeSpan TestLockBudget = TimeSpan.FromMinutes(2);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly DuckDbInvestigationStore _store;
    private DateTime _now = T0;

    public DuckDbInvestigationStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DuckDbInvestigationStoreTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _db = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _db.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbInvestigationStore(_db, () => _now, TestLockBudget);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best effort */ }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static InvestigationAlert Alert(DateTime at, string family = "blocking", int serverId = 7, int routeId = 1,
        int maxPerHour = 20, int maxPerDay = 100, params string[] keys) =>
        new(routeId, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), family, "Blocking", at, keys, null,
            maxPerHour, maxPerDay, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static InvestigationReportSubmission Report(Guid requestId, Guid claimId) =>
        new(requestId, claimId, "agent-a", 1, "real", "a long blocking chain", true, "summary", "[{\"tool\":\"get_blocking\"}]");

    private string? Scalar(string sql)
    {
        using var connection = _db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private void Exec(string sql)
    {
        using var connection = _db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    [Fact]
    public async Task AlertsGroup_WithinTheWindow_AndAKnownKeyPastItOpensAnother()
    {
        var first = await _store.OpenOrJoinAsync(Alert(T0, keys: new[] { "key-1" }), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, first.Outcome);

        _now = T0.AddMinutes(20);
        var second = await _store.OpenOrJoinAsync(Alert(_now, keys: new[] { "key-2", "key-3" }), Ct);
        Assert.Equal(InvestigationOpenOutcome.Joined, second.Outcome);
        Assert.Equal(first.RequestId, second.RequestId);

        _now = T0.AddMinutes(21);
        Assert.Equal(InvestigationOpenOutcome.Opened, (await _store.OpenOrJoinAsync(Alert(_now, family: "resources"), Ct)).Outcome);

        /* 31 minutes after the latest alert (at 20) with a key already on the request: a key never decides a join, so it opens another. */
        _now = T0.AddMinutes(51);
        var late = await _store.OpenOrJoinAsync(Alert(_now, keys: new[] { "key-3" }), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, late.Outcome);
        Assert.NotEqual(first.RequestId, late.RequestId);

        _now = T0.AddMinutes(52);
        Assert.Equal(late.RequestId, (await _store.OpenOrJoinAsync(Alert(_now, keys: new[] { "key-9" }), Ct)).RequestId);

        Assert.Equal("2|3", Scalar($@"
SELECT concat_ws('|', alert_count, (SELECT count(*) FROM investigation_request_alerts WHERE request_id = '{first.RequestId}'))
FROM investigation_requests WHERE request_id = '{first.RequestId}'"));
        /* #6010: every alert here is the same metric at the default severity, so both joins are quiet. */
        Assert.Equal("2", Scalar("SELECT count(*) FROM investigation_journal WHERE event = 'joined_quiet'"));
        /* Every alert row has its own id: three on the first request, one each on the other three opens and the last join. */
        Assert.Equal("6|6", Scalar("SELECT concat_ws('|', count(*), count(DISTINCT request_alert_id)) FROM investigation_request_alerts"));
    }

    [Fact]
    public async Task ReadRequest_ReturnsTheRequestAndItsBrief_AndNullForAnUnknownId()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;

        var opened = await _store.ReadRequestAsync(id, Ct);
        Assert.NotNull(opened);
        Assert.Equal((id, "open", "blocking", "srv-7", 1), (opened!.Value.Row.RequestId, opened.Value.Row.State,
            opened.Value.Row.Family, opened.Value.Row.ServerName, opened.Value.Row.AlertCount));
        Assert.Null(opened.Value.Brief);
        Assert.Equal(T0, opened.Value.Row.OpenedAtUtc);
        Assert.Equal(DateTimeKind.Utc, opened.Value.Row.OpenedAtUtc.Kind);

        _now = T0.AddMinutes(5);
        Assert.Equal(id, (await _store.OpenOrJoinAsync(Alert(_now), Ct)).RequestId);
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        Assert.True(await _store.SetBriefAsync(id, claim.ClaimId!.Value, 1, "{\"brief_format\":1,\"data_detail\":\"full\"}", Ct));

        var claimed = await _store.ReadRequestAsync(id, Ct);
        Assert.Equal(("claimed", 2, "agent-a"), (claimed!.Value.Row.State, claimed.Value.Row.AlertCount, claimed.Value.Row.ClaimedBy));
        Assert.Contains("\"data_detail\"", claimed.Value.Brief, StringComparison.Ordinal);

        Assert.Null(await _store.ReadRequestAsync(Guid.NewGuid(), Ct));
    }

    /// <summary>#5632: the grouping window slides: an alert joins within 30 minutes of the request's latest alert, up to 4 hours
    /// after its first. Checked against the old rule (30 minutes after the first alert): the 25- and 50-minute joins and the
    /// 235-minute join fail on it.</summary>
    [Fact]
    public async Task TheWindowSlides_AnAlertJoinsWithin30MinutesOfTheLatest_AndNeverPast4HoursAfterTheFirst()
    {
        /* #5632: the window slides. Each alert 25 minutes after the last joins, so the request is 50 minutes old at the third. */
        var first = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        _now = T0.AddMinutes(25);
        Assert.Equal(first, (await _store.OpenOrJoinAsync(Alert(_now), Ct)).RequestId);
        _now = T0.AddMinutes(50);
        var at50 = await _store.OpenOrJoinAsync(Alert(_now), Ct);
        Assert.Equal(InvestigationOpenOutcome.Joined, at50.Outcome);
        Assert.Equal(first, at50.RequestId);
        /* 35 minutes after the latest alert (at 50): a new request. */
        _now = T0.AddMinutes(85);
        var at85 = await _store.OpenOrJoinAsync(Alert(_now), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, at85.Outcome);
        Assert.NotEqual(first, at85.RequestId);
        /* The 4-hour cap, on another server with an expiry past it so only the cap decides: an alert every 25 minutes joins up to 235
           minutes, and one at 245 (10 minutes after the latest, 245 after the first) opens a new request. */
        _now = T0;
        var capped = (await _store.OpenOrJoinAsync(Alert(T0, serverId: 9) with { ExpiryMinutes = 600 }, Ct)).RequestId!.Value;
        for (var minute = 25; minute <= 225; minute += 25)
        {
            _now = T0.AddMinutes(minute);
            Assert.Equal(capped, (await _store.OpenOrJoinAsync(Alert(_now, serverId: 9) with { ExpiryMinutes = 600 }, Ct)).RequestId);
        }
        _now = T0.AddMinutes(235);
        Assert.Equal(capped, (await _store.OpenOrJoinAsync(Alert(_now, serverId: 9) with { ExpiryMinutes = 600 }, Ct)).RequestId);
        _now = T0.AddMinutes(245);
        var pastCap = await _store.OpenOrJoinAsync(Alert(_now, serverId: 9) with { ExpiryMinutes = 600 }, Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, pastCap.Outcome);
        Assert.NotEqual(capped, pastCap.RequestId);
    }

    /// <summary>#5632 seat fix: the wall-clock test is on the request's open time, not its latest alert. An alert stamped hours
    /// before it reaches the store (a late or replayed alert) still joins the request its own time falls in. Comparing the alert
    /// time with now refused it, and this test fails on that rule. Darling's twin is InvestigationPracticeTests' three alerts
    /// stamped 3 hours ago.</summary>
    [Fact]
    public async Task AnAlertStampedHoursBeforeItArrives_StillJoinsTheRequestItsTimeFallsIn()
    {
        _now = T0.AddHours(3);
        var first = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        Assert.Equal(first, (await _store.OpenOrJoinAsync(Alert(T0.AddMinutes(1)), Ct)).RequestId);
        Assert.Equal(first, (await _store.OpenOrJoinAsync(Alert(T0.AddMinutes(2)), Ct)).RequestId);
    }

    [Fact]
    public async Task AClaimedRequest_JoinsAt29Minutes_AndAnAlert31MinutesAfterThatOpensANewOne_AndAReportedOneTakesAQuietReFire()
    {
        const string Fallback = "7:Blocking";
        var first = (await _store.OpenOrJoinAsync(Alert(T0, keys: new[] { Fallback }), Ct)).RequestId!.Value;
        Assert.Equal(InvestigationClaimOutcome.Claimed, (await _store.ClaimAsync(first, "agent-a", Ct)).Outcome);

        _now = T0.AddMinutes(29);
        var joined = await _store.OpenOrJoinAsync(Alert(_now, keys: new[] { Fallback }), Ct);
        Assert.Equal(InvestigationOpenOutcome.Joined, joined.Outcome);
        Assert.Equal(first, joined.RequestId);

        _now = T0.AddMinutes(60);
        var opened = await _store.OpenOrJoinAsync(Alert(_now, keys: new[] { Fallback }), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, opened.Outcome);
        Assert.NotEqual(first, opened.RequestId);
        Assert.Equal("claimed", Scalar($"SELECT state FROM investigation_requests WHERE request_id = '{first}'"));

        /* Report the second; an alert two minutes later, inside its window, joins it quietly (#6778). */
        var second = opened.RequestId!.Value;
        var secondClaim = await _store.ClaimAsync(second, "agent-a", Ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(second, secondClaim.ClaimId!.Value), Ct));
        _now = T0.AddMinutes(62);
        var third = await _store.OpenOrJoinAsync(Alert(_now, keys: new[] { Fallback }), Ct);
        Assert.Equal(InvestigationOpenOutcome.Joined, third.Outcome);
        Assert.Equal(second, third.RequestId);
        Assert.False(third.Evented);
    }

    /// <summary>Opens a request on <paramref name="serverId"/> at <see cref="T0"/> and reports it (#6778). Each case uses its own
    /// server, so the request one case opens is not the one the next case finds.</summary>
    private async Task<Guid> OpenReportedAsync(int serverId)
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0, serverId: serverId) with { Severity = "warning" }, Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(id, claim.ClaimId!.Value), Ct));
        return id;
    }

    [Fact]
    public async Task AReportedRequest_TakesTheSameMetricAtTheSameSeverity_AsAQuietJoin()
    {
        var id = await OpenReportedAsync(7);
        var requestsBefore = Scalar("SELECT count(*) FROM investigation_requests");

        _now = T0.AddMinutes(5);
        var again = await _store.OpenOrJoinAsync(Alert(_now) with { Severity = "warning" }, Ct);
        Assert.Equal(InvestigationOpenOutcome.Joined, again.Outcome);
        Assert.Equal(id, again.RequestId);
        Assert.False(again.Evented);
        Assert.Equal("2|reported", Scalar($"SELECT alert_count || '|' || state FROM investigation_requests WHERE request_id = '{id}'"));
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{id}' AND event = 'joined_quiet'"));
        Assert.Equal(requestsBefore, Scalar("SELECT count(*) FROM investigation_requests"));

        /* The window slides: 25 minutes after the join it joins again. */
        _now = T0.AddMinutes(30);
        Assert.Equal(id, (await _store.OpenOrJoinAsync(Alert(_now) with { Severity = "warning" }, Ct)).RequestId);
    }

    [Fact]
    public async Task AReportedRequest_IsNotJoined_ByAHigherSeverity_ANewMetric_OrAnotherServer()
    {
        var higherId = await OpenReportedAsync(1);
        var metricId = await OpenReportedAsync(2);
        var serverId = await OpenReportedAsync(3);

        _now = T0.AddMinutes(5);
        var higher = await _store.OpenOrJoinAsync(Alert(_now, serverId: 1) with { Severity = "critical" }, Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, higher.Outcome);
        Assert.NotEqual(higherId, higher.RequestId);

        var newMetric = await _store.OpenOrJoinAsync(Alert(_now, serverId: 2) with { MetricName = "Deadlocks", Severity = "warning" }, Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, newMetric.Outcome);
        Assert.NotEqual(metricId, newMetric.RequestId);

        /* Server 3 has a reported request; the same family, metric and severity on server 4 opens its own. */
        var otherServer = await _store.OpenOrJoinAsync(Alert(_now, serverId: 4) with { Severity = "warning" }, Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, otherServer.Outcome);
        Assert.NotEqual(serverId, otherServer.RequestId);
        Assert.Equal("1", Scalar($"SELECT alert_count FROM investigation_requests WHERE request_id = '{serverId}'"));
    }

    [Fact]
    public async Task AReportedRequest_IsNotJoined_AfterItResolved_AfterTheWindow_AfterTheCap_OrWhenClosed()
    {
        var resolved = await OpenReportedAsync(1);
        var windowed = await OpenReportedAsync(2);
        var capped = await OpenReportedAsync(3);
        var closed = await OpenReportedAsync(4);

        /* The sweep's D7 event: a resolved journal row. */
        Exec($@"INSERT INTO investigation_journal (journal_id, event_time, request_id, route_id, server_id, event, actor, once_key)
SELECT (SELECT coalesce(max(journal_id), 0) FROM investigation_journal) + 1, TIMESTAMP '2026-10-14 12:01:00', request_id, route_id, server_id, 'resolved', 'service', 'resolved:' || request_id::VARCHAR
FROM investigation_requests WHERE request_id = '{resolved}'");
        Exec($"UPDATE investigation_requests SET state = 'closed', closed_at = TIMESTAMP '2026-10-14 12:01:00' WHERE request_id = '{closed}'");

        _now = T0.AddMinutes(5);
        var afterResolve = await _store.OpenOrJoinAsync(Alert(_now, serverId: 1), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, afterResolve.Outcome);
        Assert.NotEqual(resolved, afterResolve.RequestId);

        var afterClose = await _store.OpenOrJoinAsync(Alert(_now, serverId: 4), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, afterClose.Outcome);
        Assert.NotEqual(closed, afterClose.RequestId);

        /* Server 3 joins every 29 minutes up to 232, which slides its window to the 4-hour cap. */
        for (var minute = 29; minute <= 232; minute += 29)
        {
            _now = T0.AddMinutes(minute);
            Assert.Equal(capped, (await _store.OpenOrJoinAsync(Alert(_now, serverId: 3), Ct)).RequestId);
        }

        /* 31 minutes after server 2's only alert; 241 minutes after server 3's first. */
        _now = T0.AddMinutes(241);
        var pastCap = await _store.OpenOrJoinAsync(Alert(_now, serverId: 3), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, pastCap.Outcome);
        Assert.NotEqual(capped, pastCap.RequestId);

        var late = await _store.OpenOrJoinAsync(Alert(_now, serverId: 2), Ct);
        Assert.Equal(InvestigationOpenOutcome.Opened, late.Outcome);
        Assert.NotEqual(windowed, late.RequestId);
    }

    [Fact]
    public async Task TheHourlyLimit_OpensNothing_AndJournalsLimitReachedOnce_AndTheDailyLimitHoldsAnHourLater()
    {
        Assert.Equal(InvestigationOpenOutcome.Opened, (await _store.OpenOrJoinAsync(Alert(T0, serverId: 1, maxPerHour: 2, maxPerDay: 3), Ct)).Outcome);
        Assert.Equal(InvestigationOpenOutcome.Opened, (await _store.OpenOrJoinAsync(Alert(T0, serverId: 2, maxPerHour: 2, maxPerDay: 3), Ct)).Outcome);
        Assert.Equal(InvestigationOpenOutcome.LimitReached, (await _store.OpenOrJoinAsync(Alert(T0, serverId: 3, maxPerHour: 2, maxPerDay: 3), Ct)).Outcome);
        Assert.Equal(InvestigationOpenOutcome.LimitReached, (await _store.OpenOrJoinAsync(Alert(T0, serverId: 4, maxPerHour: 2, maxPerDay: 3), Ct)).Outcome);

        Assert.Equal(InvestigationOpenOutcome.Joined, (await _store.OpenOrJoinAsync(Alert(T0.AddMinutes(1), serverId: 1, maxPerHour: 2, maxPerDay: 3), Ct)).Outcome);
        Assert.Equal("1", Scalar("SELECT count(*) FROM investigation_journal WHERE event = 'limit_reached'"));

        _now = T0.AddMinutes(61);
        Assert.Equal(InvestigationOpenOutcome.Opened, (await _store.OpenOrJoinAsync(Alert(_now, serverId: 5, maxPerHour: 2, maxPerDay: 3), Ct)).Outcome);
        Assert.Equal(InvestigationOpenOutcome.LimitReached, (await _store.OpenOrJoinAsync(Alert(_now, serverId: 6, maxPerHour: 2, maxPerDay: 3), Ct)).Outcome);
        Assert.Equal("2|daily", Scalar(@"
SELECT concat_ws('|', count(*), max(reason) FILTER (WHERE event_time > (SELECT min(event_time) FROM investigation_journal WHERE event = 'limit_reached')))
FROM investigation_journal WHERE event = 'limit_reached'"));
        Assert.Equal("3", Scalar("SELECT count(*) FROM investigation_requests"));

        var use = await _store.RouteUseAsync(Ct);
        Assert.Equal((1, 3), use[1]);
    }

    [Fact]
    public async Task TwoConcurrentClaims_HaveOneWinner()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;

        /* Task.Run: the DuckDB calls complete synchronously, so without it the eight claims would run one after another. */
        var claims = await Task.WhenAll(Enumerable.Range(0, 8).Select(i =>
            Task.Run(() => _store.ClaimAsync(id, "agent-" + i.ToString(CultureInfo.InvariantCulture), Ct), Ct)));
        Assert.Single(claims, c => c.Outcome == InvestigationClaimOutcome.Claimed);
        Assert.All(claims.Where(c => c.Outcome != InvestigationClaimOutcome.Claimed),
            c => Assert.Equal(InvestigationClaimOutcome.AlreadyClaimed, c.Outcome));

        Assert.Equal("1|1", Scalar($@"
SELECT concat_ws('|', claim_count, (SELECT count(*) FROM investigation_journal WHERE event = 'claimed'))
FROM investigation_requests WHERE request_id = '{id}'"));
        Assert.Equal("1", Scalar("SELECT count(*) FROM investigation_journal WHERE event = 'refused'"));
        Assert.Equal(InvestigationClaimOutcome.NotFound, (await _store.ClaimAsync(Guid.NewGuid(), "agent-x", Ct)).Outcome);
    }

    [Fact]
    public async Task ALateReport_UnderALapsedLease_IsAccepted_UntilANewerClaimSupersedesIt()
    {
        var a = (await _store.OpenOrJoinAsync(Alert(T0, serverId: 1), Ct)).RequestId!.Value;
        var b = (await _store.OpenOrJoinAsync(Alert(T0, serverId: 2), Ct)).RequestId!.Value;

        var claimA = await _store.ClaimAsync(a, "agent-a", Ct);
        var claimB1 = await _store.ClaimAsync(b, "agent-a", Ct);
        Assert.True(await _store.SetBriefAsync(a, claimA.ClaimId!.Value, 1, "{\"brief_format\":1}", Ct));

        _now = T0.AddMinutes(20);
        Assert.Equal(2, (await _store.ListAsync("open", null, null, 10, Ct)).Count);
        Assert.Equal("2", Scalar("SELECT count(*) FROM investigation_requests WHERE state = 'claimed'"));
        Assert.Equal(2, (await _store.SweepAsync(Ct)).LeasesLapsed);
        Assert.Equal(0, (await _store.SweepAsync(Ct)).LeasesLapsed);
        Assert.Equal("2|2", Scalar(@"
SELECT concat_ws('|', (SELECT count(*) FROM investigation_journal WHERE event = 'lease_expired'),
                      (SELECT count(*) FROM investigation_requests WHERE state = 'claimed'))"));

        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(a, claimA.ClaimId!.Value), Ct));
        Assert.Equal(InvestigationSubmitOutcome.AlreadyReported, await _store.SubmitAsync(Report(a, claimA.ClaimId!.Value), Ct));

        var claimB2 = await _store.ClaimAsync(b, "agent-b", Ct);
        Assert.Equal(InvestigationClaimOutcome.Claimed, claimB2.Outcome);
        Assert.Equal(2, claimB2.ClaimCount);
        Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await _store.SubmitAsync(Report(b, claimB1.ClaimId!.Value), Ct));
        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(b, claimB2.ClaimId!.Value), Ct));
        Assert.Equal("2|reported", Scalar("SELECT concat_ws('|', count(*), min(state)) FROM investigation_requests"));
    }

    [Fact]
    public async Task AReport_UnderAClaimTheSweepExpired_IsAccepted_AndAnExpiredRequestRefusesAClaim()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);

        _now = T0.AddMinutes(241);
        Assert.Equal(1, (await _store.SweepAsync(Ct)).Expired);
        Assert.Equal(0, (await _store.SweepAsync(Ct)).Expired);
        var refused = await _store.ClaimAsync(id, "agent-b", Ct);
        Assert.Equal(InvestigationClaimOutcome.NotOpen, refused.Outcome);
        Assert.Equal("expired", refused.Reason);
        Assert.Single(await _store.ListAsync("expired", 7, "blocking", 10, Ct));
        Assert.Equal("expired", Scalar($"SELECT event FROM investigation_journal WHERE request_id = '{id}' AND once_key LIKE 'expired:%'"));

        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(id, claim.ClaimId!.Value), Ct));
        Assert.Equal("reported|1", Scalar($@"
SELECT concat_ws('|', r.state, (SELECT count(*) FROM investigation_reports WHERE request_id = r.request_id))
FROM investigation_requests AS r WHERE r.request_id = '{id}'"));
    }

    [Fact]
    public async Task ThreeClaimReleaseCycles_RefuseTheFourthClaim_AndTheSweepJournalsClaimsExhaustedOnce()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        for (var cycle = 1; cycle <= 3; cycle++)
        {
            var claim = await _store.ClaimAsync(id, "agent-a", Ct);
            Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
            Assert.Equal(cycle, claim.ClaimCount);
            Assert.True(await _store.ReleaseAsync(id, claim.ClaimId!.Value, Ct));
        }

        var fourth = await _store.ClaimAsync(id, "agent-a", Ct);
        Assert.Equal(InvestigationClaimOutcome.NotOpen, fourth.Outcome);
        Assert.Equal("claim_limit", fourth.Reason);
        Assert.Single(await _store.ListAsync("expired", 7, "blocking", 10, Ct));

        Assert.Equal(1, (await _store.SweepAsync(Ct)).Expired);
        Assert.Equal(0, (await _store.SweepAsync(Ct)).Expired);
        Assert.Equal("expired|3|1", Scalar($@"
SELECT concat_ws('|', r.state, r.claim_count,
       (SELECT count(*) FROM investigation_journal WHERE request_id = r.request_id AND event = 'claims_exhausted'))
FROM investigation_requests AS r WHERE r.request_id = '{id}'"));
    }

    [Fact]
    public async Task AnInvalidAgentId_IsRefusedAndJournaledOnce_AndARequestsAgentJournal_IsCappedAt200_AndUnknownIdsJournalHourly()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        var tooLong = new string('a', InvestigationRules.MaxAgentIdLength + 1);
        var longest = new string('b', InvestigationRules.MaxAgentIdLength);
        const string AgentRows = "SELECT count(*) FROM investigation_journal WHERE actor LIKE 'agent:%'";

        var refused = await _store.ClaimAsync(id, tooLong, Ct);
        Assert.Equal(InvestigationClaimOutcome.AgentIdInvalid, refused.Outcome);
        Assert.Equal("agent_id_invalid", refused.Reason);
        Assert.Equal(InvestigationClaimOutcome.AgentIdInvalid, (await _store.ClaimAsync(id, "x · closed · person:admin", Ct)).Outcome);
        Assert.Equal(InvestigationClaimOutcome.AgentIdInvalid, (await _store.ClaimAsync(id, string.Empty, Ct)).Outcome);
        Assert.Equal("1|agent:invalid|claim:agent_id_invalid", Scalar(
            "SELECT concat_ws('|', count(*), min(actor), min(reason)) FROM investigation_journal WHERE actor LIKE 'agent:%'"));

        var claim = await _store.ClaimAsync(id, longest, Ct);
        Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
        Assert.Equal(InvestigationSubmitOutcome.AgentIdInvalid,
            await _store.SubmitAsync(Report(id, claim.ClaimId!.Value) with { AgentId = tooLong }, Ct));
        Assert.Equal("3", Scalar(AgentRows));

        /* 199 agent rows; one more refused submit under a guessed claim id is the 200th. */
        Exec($@"
INSERT INTO investigation_journal (journal_id, event_time, request_id, server_id, event, actor, reason)
SELECT 100000 + i, TIMESTAMP '2026-10-14 12:00:00', '{id}', 7, 'refused', 'agent:seed', 'submit:lease_lost' FROM range(1, 197) AS t(i)");
        Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await _store.SubmitAsync(Report(id, Guid.NewGuid()), Ct));
        Assert.Equal("200", Scalar(AgentRows));

        Assert.Equal(InvestigationSubmitOutcome.JournalCapped, await _store.SubmitAsync(Report(id, Guid.NewGuid()), Ct));
        Assert.Equal(InvestigationSubmitOutcome.JournalCapped, await _store.SubmitAsync(Report(id, claim.ClaimId!.Value), Ct));
        var capped = await _store.ClaimAsync(id, "agent-b", Ct);
        Assert.Equal(InvestigationClaimOutcome.JournalCapped, capped.Outcome);
        Assert.Equal("journal_capped", capped.Reason);
        Assert.Equal("201|1", Scalar(@"
SELECT concat_ws('|', count(*), count(*) FILTER (WHERE event = 'journal_capped'))
FROM investigation_journal WHERE actor LIKE 'agent:%'"));

        Assert.Equal(InvestigationClaimOutcome.NotFound, (await _store.ClaimAsync(Guid.NewGuid(), "agent-c", Ct)).Outcome);
        Assert.Equal(InvestigationClaimOutcome.NotFound, (await _store.ClaimAsync(Guid.NewGuid(), "agent-c", Ct)).Outcome);
        Assert.Equal(InvestigationSubmitOutcome.NotFound, await _store.SubmitAsync(Report(Guid.NewGuid(), Guid.NewGuid()), Ct));
        Assert.Equal(InvestigationSubmitOutcome.NotFound, await _store.SubmitAsync(Report(Guid.NewGuid(), Guid.NewGuid()), Ct));
        Assert.Equal("1|1", Scalar(@"
SELECT concat_ws('|', count(*) FILTER (WHERE reason = 'claim:not_found'), count(*) FILTER (WHERE reason = 'submit:not_found'))
FROM investigation_journal"));
    }

    [Fact]
    public async Task GuessedClaimIds_JournalLeaseLostOnce_TheHoldersReportLands_AndTheClaimedRowHidesTheClaimId()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
        var claimId = claim.ClaimId!.Value.ToString();
        Assert.Equal("1|0|claimed:" + claimId, Scalar(@"
SELECT concat_ws('|', count(*), count(reason), min(once_key))
FROM investigation_journal WHERE event = 'claimed'"));

        for (var i = 0; i < 200; i++)
            Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await _store.SubmitAsync(Report(id, Guid.NewGuid()), Ct));
        Assert.Equal("1|refused:submit:" + id + ":lease_lost", Scalar(@"
SELECT concat_ws('|', count(*), min(once_key))
FROM investigation_journal WHERE reason = 'submit:lease_lost'"));

        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(id, claim.ClaimId!.Value), Ct));
        Assert.Equal("1", Scalar("SELECT count(*) FROM investigation_reports"));

        var read = await _store.ReadReportsAsync(id, null, T0.AddDays(-1), 10, 200, Ct);
        Assert.Single(read.Reports);
        Assert.DoesNotContain(read.Journal, j => (j.Reason ?? string.Empty).Contains(claimId, StringComparison.Ordinal)
            || (j.DetailJson ?? string.Empty).Contains(claimId, StringComparison.Ordinal));
    }

    /// <summary>Darling's V184 CHECKs hold the report caps in the database; Lite's tables carry none, so the store refuses
    /// a report over a cap before it writes (an <see cref="ArgumentException"/> where PostgreSQL raises 23514). Each cap
    /// passes at the cap and refuses one over it; an agent id over the rule is the store's own refusal.</summary>
    [Fact]
    public async Task V184Checks_HoldTheReportCaps_AtTheCapAndOneOver()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;

        Task<InvestigationSubmitOutcome> SubmitAsync(string agent, string rootCause, string evidence) =>
            _store.SubmitAsync(new InvestigationReportSubmission(id, Guid.NewGuid(), agent, 1, "real", rootCause, true, null, evidence), Ct);

        static string Items(int count, int valueChars) =>
            "[" + string.Join(",", Enumerable.Repeat("{\"tool\":\"" + new string('t', valueChars) + "\"}", count)) + "]";

        Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await SubmitAsync(new string('a', 64), new string('r', 8000), Items(50, 10)));
        await Assert.ThrowsAsync<ArgumentException>(() => SubmitAsync("a", new string('r', 8001), Items(1, 10)));
        await Assert.ThrowsAsync<ArgumentException>(() => SubmitAsync("a", "r", Items(51, 10)));
        Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await SubmitAsync("a", "r", Items(1, 1000)));
        await Assert.ThrowsAsync<ArgumentException>(() => SubmitAsync("a", "r", Items(1, 1001)));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            SubmitAsync("a", "r", "[{\"arguments\":{\"deep\":[\"" + new string('d', 1001) + "\"]}}]"));
        await Assert.ThrowsAsync<ArgumentException>(() => SubmitAsync("a", "r", Items(33, 1000)));
        Assert.Equal(InvestigationSubmitOutcome.AgentIdInvalid, await SubmitAsync(new string('a', 65), "r", Items(1, 10)));
        Assert.Equal(InvestigationSubmitOutcome.AgentIdInvalid, await SubmitAsync("agent a", "r", Items(1, 10)));
        Assert.Equal("0", Scalar("SELECT count(*) FROM investigation_reports"));
    }

    /// <summary>Plan section 4: the list and the report read order by <c>opened_at DESC, request_id::VARCHAR</c>, so tied
    /// requests come back in the ordinal order of their lowercase id text, as PostgreSQL's <c>uuid::text</c> sorts.</summary>
    [Fact]
    public async Task ListAndReports_OrderNewestFirst_ThenByTheIdText()
    {
        var tied = new System.Collections.Generic.List<Guid>();
        for (var server = 1; server <= 6; server++)
            tied.Add((await _store.OpenOrJoinAsync(Alert(T0, serverId: server), Ct)).RequestId!.Value);
        _now = T0.AddMinutes(1);
        var newest = (await _store.OpenOrJoinAsync(Alert(_now, serverId: 9), Ct)).RequestId!.Value;

        var expected = new[] { newest }.Concat(tied.OrderBy(g => g.ToString("D"), StringComparer.Ordinal)).ToList();
        Assert.Equal(expected, (await _store.ListAsync(null, null, null, 10, Ct)).Select(r => r.RequestId).ToList());
        var read = await _store.ReadReportsAsync(null, null, T0.AddHours(-1), 3, 200, Ct);
        Assert.Equal(expected.Take(3), read.Requests.Select(r => r.RequestId));
        Assert.True(read.RequestsTruncated);

        var since = await _store.OpenedSinceAsync(T0.AddSeconds(-1), new[] { 1 }, 10, Ct);
        Assert.Equal(tied.OrderBy(g => g.ToString("D"), StringComparer.Ordinal).Append(newest), since.Select(r => r.RequestId));
        Assert.Empty(await _store.OpenedSinceAsync(T0.AddSeconds(-1), new[] { 2 }, 10, Ct));
    }

    /// <summary>A proposed note lands pending with the request's server, story and first alert, and expires 90 days after
    /// the report, from the bound clock rather than DuckDB's local-zone default.</summary>
    [Fact]
    public async Task Submit_AProposedNote_LandsPending()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0, keys: new[] { "7:Blocking" }), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        _now = T0.AddMinutes(3);
        Assert.Equal(InvestigationSubmitOutcome.Reported,
            await _store.SubmitAsync(Report(id, claim.ClaimId!.Value) with { ProposedNote = "the nightly ETL holds this lock" }, Ct));
        Assert.Equal("pending|7|7:Blocking|Blocking|agent:agent-a|2026-10-14 12:03:00|2027-01-12 12:03:00", Scalar($@"
SELECT concat_ws('|', state, server_id, dedup_key, metric_name, author, strftime(created_at, '%Y-%m-%d %H:%M:%S'),
                 strftime(expires_at, '%Y-%m-%d %H:%M:%S'))
FROM investigation_notes WHERE source_request_id = '{id}'"));
    }

    /// <summary>A claim renews its own lease while it is live, never past the request's expiry; the holder is read back.</summary>
    [Fact]
    public async Task Renew_ExtendsALiveLease_AndNotALapsedOne()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        Assert.Equal(T0.AddMinutes(15), claim.LeaseUntilUtc);
        Assert.Equal("agent-a", await _store.ClaimHolderAsync(id, claim.ClaimId!.Value, Ct));
        Assert.Null(await _store.ClaimHolderAsync(id, Guid.NewGuid(), Ct));

        _now = T0.AddMinutes(10);
        var renewed = await _store.RenewAsync(id, claim.ClaimId!.Value, Ct);
        Assert.Equal((true, (DateTime?)T0.AddMinutes(25)), (renewed.Renewed, renewed.LeaseUntilUtc));
        Assert.Equal(DateTimeKind.Utc, renewed.LeaseUntilUtc!.Value.Kind);

        _now = T0.AddMinutes(26);
        Assert.False((await _store.RenewAsync(id, claim.ClaimId!.Value, Ct)).Renewed);
    }

    /// <summary>Retention, as Darling's: a finished request older than a year goes with its alert rows and report (no
    /// cascade, so the children first); an open one stays whatever its age; a brief is cleared at 90 days.</summary>
    [Fact]
    public async Task Purge_DeletesFinishedRequestsPastAYear_WithTheirChildren_AndClearsOldBriefs()
    {
        var old = (await _store.OpenOrJoinAsync(Alert(T0, serverId: 1), Ct)).RequestId!.Value;
        var oldClaim = await _store.ClaimAsync(old, "agent-a", Ct);
        Assert.True(await _store.SetBriefAsync(old, oldClaim.ClaimId!.Value, 1, "{}", Ct));
        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(old, oldClaim.ClaimId!.Value), Ct));
        var stillOpen = (await _store.OpenOrJoinAsync(Alert(T0, serverId: 2), Ct)).RequestId!.Value;

        _now = T0.AddDays(100);
        var recent = (await _store.OpenOrJoinAsync(Alert(_now, serverId: 3), Ct)).RequestId!.Value;
        var recentClaim = await _store.ClaimAsync(recent, "agent-a", Ct);
        Assert.True(await _store.SetBriefAsync(recent, recentClaim.ClaimId!.Value, 1, "{}", Ct));
        Assert.Equal(0, await _store.PurgeAsync(Ct));
        Assert.Equal("1", Scalar("SELECT count(*) FROM investigation_requests WHERE brief IS NOT NULL"));

        _now = T0.AddDays(366);
        Assert.Equal(1, await _store.PurgeAsync(Ct));
        Assert.Null(await _store.ReadRequestAsync(old, Ct));
        Assert.NotNull(await _store.ReadRequestAsync(stillOpen, Ct));
        Assert.Equal($"0|0|0", Scalar($@"
SELECT concat_ws('|', (SELECT count(*) FROM investigation_request_alerts WHERE request_id = '{old}'),
                      (SELECT count(*) FROM investigation_reports WHERE request_id = '{old}'),
                      (SELECT count(*) FROM investigation_journal WHERE request_id = '{old}'))"));
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{recent}' AND event = 'opened'"));
    }

    /// <summary>#5914 review r1: the report-link write on DuckDB, which Lite's submit tool does not reach today but the shared
    /// store contract carries. Links go in as one statement (max + row_number(), as the sweeps do), so the ids are distinct and
    /// continue the table's own; a URL given twice (as two kinds) writes one row, the first kind kept; the read hands the links
    /// back on the report in the order the agent gave them; a second request's links continue the ids without a collision.</summary>
    [Fact]
    public async Task ReportLinks_AreWrittenInOneStatement_WithDistinctIds_AndReadBackInOrder()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        var links = new[]
        {
            new InvestigationReportLink("pull_request", "https://git.example.invalid/pm/pull/42", "#42"),
            new InvestigationReportLink("jira", "https://jira.example.invalid/browse/OPS-1", "OPS-1"),
            new InvestigationReportLink("bug_report", "https://jira.example.invalid/browse/OPS-1", null),
            new InvestigationReportLink("bug_report", "https://bugs.example.invalid/9", null),
        };
        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(id, claim.ClaimId!.Value) with { Links = links }, Ct));

        Assert.Equal("3|3|agent:agent-a", Scalar($@"
SELECT concat_ws('|', count(*), count(DISTINCT journal_id), min(actor)) FROM investigation_journal
WHERE request_id = '{id}' AND event = 'link_recorded'"));
        Assert.Equal("pull_request|jira|bug_report", Scalar($@"
SELECT string_agg(reason, '|' ORDER BY journal_id) FROM investigation_journal WHERE request_id = '{id}' AND event = 'link_recorded'"));
        /* Every id of the journal is distinct, the lifecycle rows' and the links' alike. */
        Assert.Equal("t", Scalar("SELECT CASE WHEN count(*) = count(DISTINCT journal_id) THEN 't' ELSE 'f' END FROM investigation_journal"));

        var report = Assert.Single((await _store.ReadReportsAsync(id, null, T0.AddDays(-1), 10, 200, Ct)).Reports);
        Assert.Equal(new[]
        {
            "pull_request #42 https://git.example.invalid/pm/pull/42",
            "jira OPS-1 https://jira.example.invalid/browse/OPS-1",
            "bug_report  https://bugs.example.invalid/9",
        }, report.Links!.Select(l => l.Kind + " " + l.Key + " " + l.Url));

        _now = T0.AddMinutes(1);
        var other = (await _store.OpenOrJoinAsync(Alert(_now, family: "jobs"), Ct)).RequestId!.Value;
        var otherClaim = await _store.ClaimAsync(other, "agent-b", Ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(other, otherClaim.ClaimId!.Value) with
        {
            Links = new[] { new InvestigationReportLink("jira", "https://jira.example.invalid/browse/OPS-1", "OPS-1") },
        }, Ct));
        /* The same URL on another request is its own row (the once-key carries the request). */
        Assert.Equal("4", Scalar("SELECT count(*) FROM investigation_journal WHERE event = 'link_recorded'"));
        Assert.Equal("t", Scalar("SELECT CASE WHEN count(*) = count(DISTINCT journal_id) THEN 't' ELSE 'f' END FROM investigation_journal"));
    }

    /// <summary>#5914 review r1: a link whose once_key is already on the journal hits ON CONFLICT (once_key) on DuckDB's plain
    /// VARCHAR UNIQUE column (Schema.cs notes it is not Darling's partial index) and writes nothing, without failing the
    /// submit; the other link of the same report is still written.</summary>
    [Fact]
    public async Task ARepeatedLinkUrl_HitsOnConflictOnTheOnceKey_AndWritesOneRow()
    {
        var id = (await _store.OpenOrJoinAsync(Alert(T0), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        const string url = "https://jira.example.invalid/browse/OPS-1";
        var onceKey = InvestigationReportLinks.OnceKey(id, url);
        Exec($@"
INSERT INTO investigation_journal (journal_id, event_time, request_id, server_id, event, actor, reason, detail, once_key)
VALUES ((SELECT coalesce(max(journal_id), 0) + 1 FROM investigation_journal), now(), '{id}', 7, 'link_recorded', 'agent:agent-a',
        'jira', '{{""kind"":""jira"",""url"":""{url}""}}', '{onceKey}')");

        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(id, claim.ClaimId!.Value) with
        {
            Links = new[]
            {
                new InvestigationReportLink("jira", url, "OPS-1"),
                new InvestigationReportLink("pull_request", "https://git.example.invalid/pm/pull/42", "#42"),
            },
        }, Ct));

        Assert.Equal("2|2", Scalar($@"
SELECT concat_ws('|', count(*), count(DISTINCT once_key)) FROM investigation_journal
WHERE request_id = '{id}' AND event = 'link_recorded'"));
        Assert.Equal(2, Assert.Single((await _store.ReadReportsAsync(id, null, T0.AddDays(-1), 10, 200, Ct)).Reports).Links!.Count);
    }

    /// <summary>#5914 review r1: as Darling's pin. link_recorded rows are served whole on each report, so the capped journal
    /// page leaves them out; 50 reported requests with two links each would otherwise overflow the 200-row page.</summary>
    [Fact]
    public async Task ReportLinks_DoNotCompeteForTheJournalPage_WithTheLifecycleRows()
    {
        const int requests = 50;
        for (var i = 0; i < requests; i++)
        {
            _now = T0.AddSeconds(i * 10);
            var id = (await _store.OpenOrJoinAsync(Alert(_now, serverId: 100 + i, maxPerHour: 1000, maxPerDay: 1000), Ct)).RequestId!.Value;
            var claim = await _store.ClaimAsync(id, "agent-a", Ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(Report(id, claim.ClaimId!.Value) with
            {
                Links = new[]
                {
                    new InvestigationReportLink("jira", "https://jira.example.invalid/browse/OPS-" + i, "OPS-" + i),
                    new InvestigationReportLink("pull_request", "https://git.example.invalid/pm/pull/" + i, "#" + i),
                },
            }, Ct));
        }

        var read = await _store.ReadReportsAsync(null, null, T0.AddDays(-1), 100, 200, Ct);
        Assert.Equal(requests, read.Reports.Count);
        Assert.All(read.Reports, r => Assert.Equal(2, r.Links!.Count));
        Assert.DoesNotContain(read.Journal, j => j.Event == InvestigationReportLinks.JournalEvent);
        Assert.False(read.JournalTruncated);
        foreach (var evt in new[] { "opened", "claimed", "reported" })
        {
            Assert.Equal(requests, read.Journal.Count(j => j.Event == evt));
        }
    }
}
