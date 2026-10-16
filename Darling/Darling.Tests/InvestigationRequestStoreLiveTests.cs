/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The investigation store's operations on a live store (#5632 Phase A, rulings R4 and R6): alerts of one server and family
/// group within the 30-minute window, never by a dedup key alone; the hourly limit opens nothing and journals
/// <c>limit_reached</c> once; two concurrent claims have one winner; a superseded claim's report is refused as
/// <c>lease_lost</c>; a late report under a lapsed or expired lease is accepted until a newer claim; the list computes the
/// state without writing; the sweep expires without reopening; a release keeps its claim counted; and a long agent id and
/// a full agent journal are refused.
/// </summary>
public sealed class InvestigationRequestStoreLiveTests
{
    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private DateTime _now = T0;

    private static InvestigationAlert Alert(DateTime at, string family = "blocking", int serverId = 7, int routeId = 1,
        int maxPerHour = 20, int maxPerDay = 100, params string[] keys) =>
        new(routeId, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), family, "Blocking", at, keys, null,
            maxPerHour, maxPerDay, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static InvestigationReportSubmission Report(Guid requestId, Guid claimId) =>
        new(requestId, claimId, "agent-a", 1, "real", "a long blocking chain", true, "summary", "[{\"tool\":\"get_blocking\"}]");

    [Fact]
    public async Task AlertsGroup_WithinTheWindow_AndAKnownKeyPastItOpensAnother()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var first = await store.OpenOrJoinAsync(Alert(T0, keys: new[] { "key-1" }), ct);
            Assert.Equal(InvestigationOpenOutcome.Opened, first.Outcome);

            /* Same server and family, inside 30 minutes, two incident keys: joins, a row per key. */
            _now = T0.AddMinutes(20);
            var second = await store.OpenOrJoinAsync(Alert(_now, keys: new[] { "key-2", "key-3" }), ct);
            Assert.Equal(InvestigationOpenOutcome.Joined, second.Outcome);
            Assert.Equal(first.RequestId, second.RequestId);

            /* Another family on the same server opens its own. */
            _now = T0.AddMinutes(21);
            var other = await store.OpenOrJoinAsync(Alert(_now, family: "resources"), ct);
            Assert.Equal(InvestigationOpenOutcome.Opened, other.Outcome);

            /* Past the window with a key already on the request: a key never decides a join (#5632 review M2), so it opens
               another. */
            _now = T0.AddMinutes(50);
            var late = await store.OpenOrJoinAsync(Alert(_now, keys: new[] { "key-3" }), ct);
            Assert.Equal(InvestigationOpenOutcome.Opened, late.Outcome);
            Assert.NotEqual(first.RequestId, late.RequestId);

            /* Inside the new request's window with a new key: joins it, not the first. */
            _now = T0.AddMinutes(51);
            var next = await store.OpenOrJoinAsync(Alert(_now, keys: new[] { "key-9" }), ct);
            Assert.Equal(late.RequestId, next.RequestId);

            Assert.Equal("2|3", await ScalarAsync(connection, $@"
SELECT alert_count || '|' || (SELECT count(*) FROM config.investigation_request_alerts WHERE request_id = '{first.RequestId}')
FROM config.investigation_requests WHERE request_id = '{first.RequestId}'", ct));
            Assert.Equal("2", await ScalarAsync(connection,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'joined'", ct));
        });
    }

    /// <summary>The join is bounded by the window even for a request still claimed (#5632 review round 1, M2): the
    /// constant <c>{server}:{metric}</c> key every alert of a metric carries joins nothing on its own. An alert 29 minutes
    /// after the request opened joins it; one 31 minutes after opens a new request, though the first is still claimed.
    /// A reported request is final: a later alert inside its window opens a new one.</summary>
    /// <summary>#5821: the generic webhook's read-back of the request an alert opened or joined, with its stored brief.</summary>
    [Fact]
    public async Task ReadRequest_ReturnsTheRequestAndItsBrief_AndNullForAnUnknownId()
    {
        await RunAsync(async (store, _, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;

            var opened = await store.ReadRequestAsync(id, ct);
            Assert.NotNull(opened);
            Assert.Equal((id, "open", "blocking", "srv-7", 1), (opened!.Value.Row.RequestId, opened.Value.Row.State,
                opened.Value.Row.Family, opened.Value.Row.ServerName, opened.Value.Row.AlertCount));
            Assert.Null(opened.Value.Brief);

            _now = T0.AddMinutes(5);
            var joined = await store.OpenOrJoinAsync(Alert(_now), ct);
            Assert.Equal(id, joined.RequestId);
            var claim = await store.ClaimAsync(id, "agent-a", ct);
            Assert.True(await store.SetBriefAsync(id, claim.ClaimId!.Value, 1, "{\"brief_format\":1,\"data_detail\":\"full\"}", ct));

            var claimed = await store.ReadRequestAsync(id, ct);
            Assert.Equal(("claimed", 2, "agent-a"), (claimed!.Value.Row.State, claimed.Value.Row.AlertCount, claimed.Value.Row.ClaimedBy));
            Assert.Contains("\"data_detail\"", claimed.Value.Brief, StringComparison.Ordinal);

            Assert.Null(await store.ReadRequestAsync(Guid.NewGuid(), ct));
        });
    }

    [Fact]
    public async Task AClaimedRequest_JoinsAt29Minutes_AndAnAlertAt31MinutesOpensANewOne_AndAReportedOneIsFinal()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            const string Fallback = "7:Blocking";
            var first = (await store.OpenOrJoinAsync(Alert(T0, keys: new[] { Fallback }), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(first, "agent-a", ct);
            Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);

            _now = T0.AddMinutes(29);
            var joined = await store.OpenOrJoinAsync(Alert(_now, keys: new[] { Fallback }), ct);
            Assert.Equal(InvestigationOpenOutcome.Joined, joined.Outcome);
            Assert.Equal(first, joined.RequestId);

            _now = T0.AddMinutes(31);
            var opened = await store.OpenOrJoinAsync(Alert(_now, keys: new[] { Fallback }), ct);
            Assert.Equal(InvestigationOpenOutcome.Opened, opened.Outcome);
            Assert.NotEqual(first, opened.RequestId);
            Assert.Equal("claimed", await ScalarAsync(connection,
                $"SELECT state FROM config.investigation_requests WHERE request_id = '{first}'", ct));

            /* Report the second; an alert two minutes later, inside its window, opens a third. */
            var second = opened.RequestId!.Value;
            var secondClaim = await store.ClaimAsync(second, "agent-a", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(Report(second, secondClaim.ClaimId!.Value), ct));
            _now = T0.AddMinutes(33);
            var third = await store.OpenOrJoinAsync(Alert(_now, keys: new[] { Fallback }), ct);
            Assert.Equal(InvestigationOpenOutcome.Opened, third.Outcome);
            Assert.NotEqual(second, third.RequestId);
        });
    }

    [Fact]
    public async Task TheHourlyLimit_OpensNothing_AndJournalsLimitReachedOnce_AndTheDailyLimitHoldsAnHourLater()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            /* Two families per alert server, so each alert opens (joins never count). Limit 2 an hour, 3 a day. */
            Assert.Equal(InvestigationOpenOutcome.Opened, (await store.OpenOrJoinAsync(Alert(T0, serverId: 1, maxPerHour: 2, maxPerDay: 3), ct)).Outcome);
            Assert.Equal(InvestigationOpenOutcome.Opened, (await store.OpenOrJoinAsync(Alert(T0, serverId: 2, maxPerHour: 2, maxPerDay: 3), ct)).Outcome);
            Assert.Equal(InvestigationOpenOutcome.LimitReached, (await store.OpenOrJoinAsync(Alert(T0, serverId: 3, maxPerHour: 2, maxPerDay: 3), ct)).Outcome);
            Assert.Equal(InvestigationOpenOutcome.LimitReached, (await store.OpenOrJoinAsync(Alert(T0, serverId: 4, maxPerHour: 2, maxPerDay: 3), ct)).Outcome);

            /* A join under the limit still joins. */
            Assert.Equal(InvestigationOpenOutcome.Joined, (await store.OpenOrJoinAsync(Alert(T0.AddMinutes(1), serverId: 1, maxPerHour: 2, maxPerDay: 3), ct)).Outcome);
            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'limit_reached'", ct));

            _now = T0.AddMinutes(61);
            Assert.Equal(InvestigationOpenOutcome.Opened, (await store.OpenOrJoinAsync(Alert(_now, serverId: 5, maxPerHour: 2, maxPerDay: 3), ct)).Outcome);
            Assert.Equal(InvestigationOpenOutcome.LimitReached, (await store.OpenOrJoinAsync(Alert(_now, serverId: 6, maxPerHour: 2, maxPerDay: 3), ct)).Outcome);
            Assert.Equal("2|daily", await ScalarAsync(connection, @"
SELECT count(*) || '|' || max(reason) FILTER (WHERE event_time > (SELECT min(event_time) FROM config.investigation_journal WHERE event = 'limit_reached'))
FROM config.investigation_journal WHERE event = 'limit_reached'", ct));
            Assert.Equal("3", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests", ct));
        });
    }

    [Fact]
    public async Task TwoConcurrentClaims_HaveOneWinner()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var opened = await store.OpenOrJoinAsync(Alert(T0), ct);
            var id = opened.RequestId!.Value;

            var claims = await Task.WhenAll(
                Enumerable.Range(0, 8).Select(i => store.ClaimAsync(id, "agent-" + i.ToString(CultureInfo.InvariantCulture), ct)));
            Assert.Single(claims, c => c.Outcome == InvestigationClaimOutcome.Claimed);
            Assert.All(claims.Where(c => c.Outcome != InvestigationClaimOutcome.Claimed),
                c => Assert.Equal(InvestigationClaimOutcome.AlreadyClaimed, c.Outcome));

            Assert.Equal("1|1", await ScalarAsync(connection, $@"
SELECT claim_count || '|' || (SELECT count(*) FROM config.investigation_journal WHERE event = 'claimed')
FROM config.investigation_requests WHERE request_id = '{id}'", ct));

            /* Seven refusals of one reason are journaled once. */
            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused'", ct));
            Assert.Equal(InvestigationClaimOutcome.NotFound, (await store.ClaimAsync(Guid.NewGuid(), "agent-x", ct)).Outcome);
        });
    }

    [Fact]
    public async Task ALateReport_UnderALapsedLease_IsAccepted_UntilANewerClaimSupersedesIt()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var a = (await store.OpenOrJoinAsync(Alert(T0, serverId: 1), ct)).RequestId!.Value;
            var b = (await store.OpenOrJoinAsync(Alert(T0, serverId: 2), ct)).RequestId!.Value;

            var claimA = await store.ClaimAsync(a, "agent-a", ct);
            var claimB1 = await store.ClaimAsync(b, "agent-a", ct);
            Assert.True(await store.SetBriefAsync(a, claimA.ClaimId!.Value, 1, "{\"brief_format\":1}", ct));

            /* Both leases lapse. The list says open and writes nothing; the sweep journals each lapse once and reopens nothing. */
            _now = T0.AddMinutes(20);
            var listed = await store.ListAsync("open", null, null, 10, ct);
            Assert.Equal(2, listed.Count);
            Assert.Equal("2", await ScalarAsync(connection,
                "SELECT count(*) FROM config.investigation_requests WHERE state = 'claimed'", ct));
            await store.SweepAsync(ct);
            await store.SweepAsync(ct);
            Assert.Equal("2|2", await ScalarAsync(connection, @"
SELECT (SELECT count(*) FROM config.investigation_journal WHERE event = 'lease_expired') || '|' ||
       (SELECT count(*) FROM config.investigation_requests WHERE state = 'claimed')", ct));

            /* A: the slow agent's report under its lapsed lease is accepted, and a second one is refused. */
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(Report(a, claimA.ClaimId!.Value), ct));
            Assert.Equal(InvestigationSubmitOutcome.AlreadyReported, await store.SubmitAsync(Report(a, claimA.ClaimId!.Value), ct));

            /* B: a newer claim takes the lapsed lease over; the first claim's report is then lease_lost, the newer one's lands. */
            var claimB2 = await store.ClaimAsync(b, "agent-b", ct);
            Assert.Equal(InvestigationClaimOutcome.Claimed, claimB2.Outcome);
            Assert.Equal(2, claimB2.ClaimCount);
            Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await store.SubmitAsync(Report(b, claimB1.ClaimId!.Value), ct));
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(Report(b, claimB2.ClaimId!.Value), ct));
            Assert.Equal("2|reported", await ScalarAsync(connection,
                "SELECT count(*) || '|' || min(state) FROM config.investigation_requests", ct));
        });
    }

    [Fact]
    public async Task AReport_UnderAClaimTheSweepExpired_IsAccepted_AndAnExpiredRequestRefusesAClaim()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(id, "agent-a", ct);

            /* Past the 240-minute expiry: the sweep expires it once, and a new claim is refused. */
            _now = T0.AddMinutes(241);
            Assert.Equal(1, (await store.SweepAsync(ct)).Expired);
            Assert.Equal(0, (await store.SweepAsync(ct)).Expired);
            var refused = await store.ClaimAsync(id, "agent-b", ct);
            Assert.Equal(InvestigationClaimOutcome.NotOpen, refused.Outcome);
            Assert.Equal("expired", refused.Reason);
            Assert.Single(await store.ListAsync("expired", 7, "blocking", 10, ct));

            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(Report(id, claim.ClaimId!.Value), ct));
            Assert.Equal("reported|1", await ScalarAsync(connection, $@"
SELECT r.state || '|' || (SELECT count(*) FROM config.investigation_reports WHERE request_id = r.request_id)
FROM config.investigation_requests AS r WHERE r.request_id = '{id}'", ct));
        });
    }

    /// <summary>A release keeps its claim counted (#5632 review round 1, L4): three claim-release cycles use the three
    /// claims, the fourth claim is refused, and the sweep expires the request and journals <c>claims_exhausted</c> once.</summary>
    [Fact]
    public async Task ThreeClaimReleaseCycles_RefuseTheFourthClaim_AndTheSweepJournalsClaimsExhaustedOnce()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            for (var cycle = 1; cycle <= 3; cycle++)
            {
                var claim = await store.ClaimAsync(id, "agent-a", ct);
                Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
                Assert.Equal(cycle, claim.ClaimCount);
                Assert.True(await store.ReleaseAsync(id, claim.ClaimId!.Value, ct));
            }

            var fourth = await store.ClaimAsync(id, "agent-a", ct);
            Assert.Equal(InvestigationClaimOutcome.NotOpen, fourth.Outcome);
            Assert.Equal("claim_limit", fourth.Reason);
            Assert.Single(await store.ListAsync("expired", 7, "blocking", 10, ct));

            Assert.Equal(1, (await store.SweepAsync(ct)).Expired);
            Assert.Equal(0, (await store.SweepAsync(ct)).Expired);
            Assert.Equal("expired|3|1", await ScalarAsync(connection, $@"
SELECT r.state || '|' || r.claim_count || '|'
       || (SELECT count(*) FROM config.investigation_journal WHERE request_id = r.request_id AND event = 'claims_exhausted')
FROM config.investigation_requests AS r WHERE r.request_id = '{id}'", ct));
        });
    }

    /// <summary>The store bounds what a token holder can write (#5632 review round 1, L3; round 2, L4): an agent id over
    /// 64 characters, or with a character outside <c>[A-Za-z0-9._:-]</c>, is refused and journaled once per request and
    /// reason under <c>agent:invalid</c>, never under the id it was given; a request takes at most 200 agent-written journal rows, after which
    /// every agent call on it is refused and one <c>journal_capped</c> row records that; and claims of unknown ids journal
    /// once per hour, not once per guessed id.</summary>
    [Fact]
    public async Task AnInvalidAgentId_IsRefusedAndJournaledOnce_AndARequestsAgentJournal_IsCappedAt200_AndUnknownIdsJournalHourly()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            var tooLong = new string('a', InvestigationRules.MaxAgentIdLength + 1);
            var longest = new string('b', InvestigationRules.MaxAgentIdLength);
            const string AgentRows = "SELECT count(*) FROM config.investigation_journal WHERE actor LIKE 'agent:%'";

            var refused = await store.ClaimAsync(id, tooLong, ct);
            Assert.Equal(InvestigationClaimOutcome.AgentIdInvalid, refused.Outcome);
            Assert.Equal("agent_id_invalid", refused.Reason);
            /* An id built to read as a person's close on the web panel's journal line: refused, and the once-key holds. */
            Assert.Equal(InvestigationClaimOutcome.AgentIdInvalid, (await store.ClaimAsync(id, "x · closed · person:admin", ct)).Outcome);
            Assert.Equal(InvestigationClaimOutcome.AgentIdInvalid, (await store.ClaimAsync(id, string.Empty, ct)).Outcome);
            Assert.Equal("1|agent:invalid|claim:agent_id_invalid", await ScalarAsync(connection,
                "SELECT count(*) || '|' || min(actor) || '|' || min(reason) FROM config.investigation_journal WHERE actor LIKE 'agent:%'", ct));

            var claim = await store.ClaimAsync(id, longest, ct);
            Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
            Assert.Equal(InvestigationSubmitOutcome.AgentIdInvalid,
                await store.SubmitAsync(Report(id, claim.ClaimId!.Value) with { AgentId = tooLong }, ct));
            Assert.Equal("3", await ScalarAsync(connection, AgentRows, ct));

            /* 199 agent rows; one more refused submit under a guessed claim id is the 200th. */
            await ExecAsync(connection, $@"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason)
SELECT '2026-10-14 12:00:00+00', '{id}', 7, 'refused', 'agent:seed', 'submit:lease_lost' FROM generate_series(1, 196)", ct);
            Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await store.SubmitAsync(Report(id, Guid.NewGuid()), ct));
            Assert.Equal("200", await ScalarAsync(connection, AgentRows, ct));

            /* The cap: every agent call on the request is refused, and journal_capped is written once. */
            Assert.Equal(InvestigationSubmitOutcome.JournalCapped, await store.SubmitAsync(Report(id, Guid.NewGuid()), ct));
            Assert.Equal(InvestigationSubmitOutcome.JournalCapped, await store.SubmitAsync(Report(id, claim.ClaimId!.Value), ct));
            var capped = await store.ClaimAsync(id, "agent-b", ct);
            Assert.Equal(InvestigationClaimOutcome.JournalCapped, capped.Outcome);
            Assert.Equal("journal_capped", capped.Reason);
            Assert.Equal("201|1", await ScalarAsync(connection, @"
SELECT count(*) || '|' || count(*) FILTER (WHERE event = 'journal_capped')
FROM config.investigation_journal WHERE actor LIKE 'agent:%'", ct));

            /* Unknown ids: one not_found row per hour, whatever ids are guessed. */
            Assert.Equal(InvestigationClaimOutcome.NotFound, (await store.ClaimAsync(Guid.NewGuid(), "agent-c", ct)).Outcome);
            Assert.Equal(InvestigationClaimOutcome.NotFound, (await store.ClaimAsync(Guid.NewGuid(), "agent-c", ct)).Outcome);
            Assert.Equal(InvestigationSubmitOutcome.NotFound, await store.SubmitAsync(Report(Guid.NewGuid(), Guid.NewGuid()), ct));
            Assert.Equal(InvestigationSubmitOutcome.NotFound, await store.SubmitAsync(Report(Guid.NewGuid(), Guid.NewGuid()), ct));
            Assert.Equal("1|1", await ScalarAsync(connection, @"
SELECT count(*) FILTER (WHERE reason = 'claim:not_found') || '|' || count(*) FILTER (WHERE reason = 'submit:not_found')
FROM config.investigation_journal", ct));
        });
    }

    /// <summary>#5632 review round 2, L1 and L4: 200 submits under guessed claim ids write ONE <c>lease_lost</c> row (its
    /// once-key names the request and the reason, never a claim id), so the real claim holder's report still lands; and
    /// the <c>claimed</c> row publishes no claim id: its reason is null and the id lives only in the unreturned once-key.</summary>
    [Fact]
    public async Task GuessedClaimIds_JournalLeaseLostOnce_TheHoldersReportLands_AndTheClaimedRowHidesTheClaimId()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(id, "agent-a", ct);
            Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
            var claimId = claim.ClaimId!.Value.ToString();
            Assert.Equal("1|0|claimed:" + claimId, await ScalarAsync(connection, @"
SELECT count(*) || '|' || count(reason) || '|' || min(once_key)
FROM config.investigation_journal WHERE event = 'claimed'", ct));

            for (var i = 0; i < 200; i++)
                Assert.Equal(InvestigationSubmitOutcome.LeaseLost, await store.SubmitAsync(Report(id, Guid.NewGuid()), ct));
            Assert.Equal("1|refused:submit:" + id + ":lease_lost", await ScalarAsync(connection, @"
SELECT count(*) || '|' || min(once_key)
FROM config.investigation_journal WHERE reason = 'submit:lease_lost'", ct));

            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(Report(id, claim.ClaimId!.Value), ct));
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_reports", ct));

            /* No reader returns a claim id: the report read's journal holds none of it. */
            var read = await store.ReadReportsAsync(id, null, T0.AddDays(-1), 10, 200, ct);
            Assert.DoesNotContain(read.Journal, j => (j.Reason ?? string.Empty).Contains(claimId, StringComparison.Ordinal)
                || (j.DetailJson ?? string.Empty).Contains(claimId, StringComparison.Ordinal));
        });
    }

    /// <summary>#5632 review round 2, M2 and L4: V184 holds the caps the tool enforces, so a writer that skips the tool still
    /// cannot store an oversized report or an agent id outside <c>[A-Za-z0-9._:-]{1,64}</c>. Each cap passes at the cap
    /// and refuses one over it (23514).</summary>
    [Fact]
    public async Task V184Checks_HoldTheReportCaps_AtTheCapAndOneOver()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;

            async Task<string?> InsertAsync(string agent, string rootCause, string evidence)
            {
                await using var tx = await connection.BeginTransactionAsync(ct);
                await using var command = new NpgsqlCommand(@"
INSERT INTO config.investigation_reports (request_id, report_format, agent_id, claim_id, verdict, root_cause, evidence, server_id)
VALUES ($1, 1, $2, $3, 'real', $4, $5::jsonb, 7)", connection, tx);
                command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = id });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = agent });
                command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = Guid.NewGuid() });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = rootCause });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = evidence });
                try
                {
                    await command.ExecuteNonQueryAsync(ct);
                    return null;
                }
                catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.CheckViolation)
                {
                    return ex.ConstraintName;
                }
                finally
                {
                    await tx.RollbackAsync(ct);
                }
            }

            static string Items(int count, int valueChars) =>
                "[" + string.Join(",", Enumerable.Repeat("{\"tool\":\"" + new string('t', valueChars) + "\"}", count)) + "]";

            Assert.Null(await InsertAsync(new string('a', 64), new string('r', 8000), Items(50, 10)));
            Assert.Equal("ck_investigation_reports_root_cause", await InsertAsync("a", new string('r', 8001), Items(1, 10)));
            Assert.Equal("ck_investigation_reports_evidence_size", await InsertAsync("a", "r", Items(51, 10)));
            Assert.Null(await InsertAsync("a", "r", Items(1, 1000)));
            Assert.Equal("ck_investigation_reports_evidence_size", await InsertAsync("a", "r", Items(1, 1001)));
            Assert.Equal("ck_investigation_reports_evidence_size",
                await InsertAsync("a", "r", "[{\"arguments\":{\"deep\":[\"" + new string('d', 1001) + "\"]}}]"));
            /* 32 strings of 1,000 characters print as 32 * 1,000 plus their quotes, keys and separators: over 32,768. */
            Assert.Equal("ck_investigation_reports_evidence_size", await InsertAsync("a", "r", Items(33, 1000)));
            Assert.Equal("ck_investigation_reports_agent", await InsertAsync(new string('a', 65), "r", Items(1, 10)));
            Assert.Equal("ck_investigation_reports_agent", await InsertAsync("agent a", "r", Items(1, 10)));

            /* The tool's byte measure is never below what PostgreSQL prints, so a report the tool passes never trips the
               check: exact on these, wider only where the relaxed encoder escapes a character PostgreSQL keeps raw. */
            foreach (var json in new[]
            {
                "[1e3, 1.0E-2, -0.5e1, 12.340]", "[{\"a\": {\"b\": [true, false, null, {}, []]}}]",
                "[\"<\\u00e9\\u0001\\\"\\\\ \\n\\t\"]", Items(3, 7),
            })
            {
                var printed = long.Parse((await ScalarAsync(connection, $"SELECT octet_length('{json.Replace("'", "''")}'::jsonb::text)", ct))!,
                    CultureInfo.InvariantCulture);
                Assert.Equal(printed, InvestigationToolCore.PgJsonbTextBytes(
                    System.Text.Json.Nodes.JsonNode.Parse(json)));
            }
        });
    }

    /// <summary>Claim, brief, release, re-claim and submit as a non-superuser holding exactly the managed <c>mcp</c>
    /// role's investigation grants (#5632). Every other fact connects as the owner, which can write anything, so a
    /// missing grant would pass them all and fail only in a deployment, with a 42501. The grants are TAKEN FROM
    /// <see cref="DarlingManagedRoles.BuildProvisioningSql"/>'s output with the role name swapped (the
    /// <c>ServerTagMcpRoleLiveTests</c> pattern: a per-run role name, since a role belongs to the whole cluster), so
    /// removing one from the product's SQL fails this test. The same role may not set a note's state or link an alert.</summary>
    [Fact]
    public async Task TheMcpRoleGrants_LetClaimBriefReleaseAndSubmit_AndNothingMore()
    {
        var roleName = "inv_mcp_" + Guid.NewGuid().ToString("N")[..8];
        var rolePassword = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
        var grants = PerformanceMonitor.Darling.Service.DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp)
            .Split(';')
            .Select(statement => string.Join(' ', statement.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)))
            .Where(statement => statement.StartsWith("GRANT ", StringComparison.Ordinal)
                && statement.EndsWith(" TO mcp", StringComparison.Ordinal)
                && statement.Contains(" config.investigation_", StringComparison.Ordinal)
                /* The Level 2 read on the feedback table (V191, #5909) is pinned by AgentLevel2StoreGrantLiveTests. */
                && !statement.Contains("investigation_feedback", StringComparison.Ordinal))
            .Select(statement => statement[..^" TO mcp".Length] + " TO " + roleName)
            .ToList();

        /* SELECT on the six, UPDATE on the claim and report columns, INSERT on reports, the journal and notes. */
        Assert.Equal(5, grants.Count);

        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0, keys: new[] { "key-1" }), ct)).RequestId!.Value;
            await ExecAsync(connection, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'; GRANT USAGE ON SCHEMA config TO {roleName};", ct);
            try
            {
                var asMcp = new NpgsqlConnectionStringBuilder(connection.ConnectionString) { Username = roleName, Password = rolePassword }.ConnectionString;
                await using var source = NpgsqlDataSource.Create(asMcp);
                var mcp = new InvestigationRequestStore(source, 30, () => _now);

                /* Before the grants the role can do nothing: the 42501 proves the grants below are what lets it in. */
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => mcp.ClaimAsync(id, "agent-a", ct))).SqlState);

                foreach (var grant in grants)
                {
                    await ExecAsync(connection, grant, ct);
                }

                var first = await mcp.ClaimAsync(id, "agent-a", ct);
                Assert.Equal(InvestigationClaimOutcome.Claimed, first.Outcome);
                Assert.True(await mcp.SetBriefAsync(id, first.ClaimId!.Value, 1, "{\"brief_format\":1}", ct));
                Assert.True(await mcp.ReleaseAsync(id, first.ClaimId!.Value, ct));
                var second = await mcp.ClaimAsync(id, "agent-a", ct);
                Assert.Equal(InvestigationClaimOutcome.Claimed, second.Outcome);
                Assert.Equal(InvestigationSubmitOutcome.Reported, await mcp.SubmitAsync(Report(id, second.ClaimId!.Value), ct));
                Assert.Equal("reported|1", await ScalarAsync(connection,
                    $"SELECT state || '|' || (SELECT count(*) FROM config.investigation_reports) FROM config.investigation_requests WHERE request_id = '{id}'", ct));

                await using var direct = await source.OpenConnectionAsync(ct);
                await ExecAsync(direct, $"INSERT INTO config.investigation_notes (server_id, note_text, author, source_request_id) VALUES (7, 'a proposal', 'agent:agent-a', '{id}')", ct);
                Assert.Equal("pending", await ScalarAsync(connection, "SELECT state FROM config.investigation_notes", ct));
                foreach (var refused in new[]
                {
                    "INSERT INTO config.investigation_notes (note_text, author, state) VALUES ('self-approved', 'agent:agent-a', 'approved')",
                    "UPDATE config.investigation_notes SET state = 'approved'",
                    $"INSERT INTO config.investigation_request_alerts (request_id, alert_time, server_id, metric_name) VALUES ('{id}', now(), 7, 'Blocking')",
                    "UPDATE config.investigation_requests SET closed_reason = 'mine'",
                    "DELETE FROM config.investigation_reports",
                    "UPDATE config.agent_environment_profiles SET profile_text = 'x'",
                })
                {
                    Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(direct, refused, ct))).SqlState);
                }
            }
            finally
            {
                await ExecAsync(connection, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", ct);
            }
        });
    }

    /// <summary>
    /// <c>provision-roles.sql</c> runs clean on a store one rung below V184 (#5632 review round 1, L5): every block that names
    /// a V184 column or table (the route carves of steps 2b and 2c, and step 3i) runs on the store wound back by
    /// <see cref="RungRewindArtifacts.DropV184SentinelAsync"/>, carves the V181 route columns and skips the agent ones; on the
    /// full store the same blocks grant the agent columns. The blocks are retargeted at per-run roles created inside one
    /// transaction that is rolled back, so no role or grant outlives the test and the fixed role names are never written.
    /// </summary>
    [Fact]
    public async Task TheByoScriptsV184Blocks_GrantTheAgentColumnsAtV184_AndRunCleanOneRungBelow()
    {
        var script = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql");
        var blocks = Regex.Matches(script, @"(?s)DO \$\$\r?\nBEGIN\r?\n   IF to_regclass\('config\.config_notification_routes'\) IS NOT NULL THEN.*?END \$\$;")
            .Select(m => m.Value)
            .Concat(Regex.Matches(script, @"(?s)DO \$do\$\r?\nBEGIN\r?\n.*?END \$do\$;")
                .Select(m => m.Value)
                /* #5918: step 3j now also names the table (the practice INSERT grant on a V191 column); it is V191's block, not V184's. */
                .Where(block => block.Contains("config.investigation_requests", StringComparison.Ordinal)
                                && !block.Contains("practice_of", StringComparison.Ordinal)))
            .ToList();

        /* The viewer carve, the mcp carve, and step 3i, retargeted at per-run roles. */
        Assert.Equal(3, blocks.Count);
        var token = Guid.NewGuid().ToString("N")[..8];
        var (admin, viewer, mcp) = ("inv_a_" + token, "inv_v_" + token, "inv_m_" + token);
        blocks = blocks
            .Select(block => Regex.Replace(block, @"\b(admin|viewer|mcp)\b", m => m.Value switch
            {
                "admin" => admin,
                "viewer" => viewer,
                _ => mcp,
            }))
            .ToList();

        await RunAsync(async (store, connection, ct) =>
        {
            var privileges = $@"
SELECT has_table_privilege('{viewer}', 'config.config_notification_routes', 'SELECT')
       || '|' || has_column_privilege('{viewer}', 'config.config_notification_routes', 'scope_tag_id', 'SELECT')
       || '|' || has_column_privilege('{mcp}', 'config.config_notification_routes', 'scope_tag_id', 'SELECT')
       || '|' || coalesce(has_table_privilege('{mcp}', to_regclass('config.investigation_requests'), 'SELECT'), false)
       || '|' || EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = 'config.config_notification_routes'::regclass AND attname = 'agent_level'
                           AND NOT attisdropped
                           AND has_column_privilege('{mcp}', attrelid, attnum, 'SELECT'))";
            var roles = $@"
CREATE ROLE {admin} NOLOGIN;
CREATE ROLE {viewer} NOLOGIN;
CREATE ROLE {mcp} NOLOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA config TO {viewer}, {mcp};";

            Assert.Equal("false|true|true|true|true", await RunBlocksRolledBackAsync(connection, roles, blocks, privileges, ct));

            await RungRewindArtifacts.DropV191SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV190SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV189SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV188SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV187SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV186SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV185SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV184SentinelAsync(connection, ct);
            Assert.Equal("false|true|true|false|false", await RunBlocksRolledBackAsync(connection, roles, blocks, privileges, ct));
        });
    }

    private static async Task<string?> RunBlocksRolledBackAsync(NpgsqlConnection connection, string roles,
        System.Collections.Generic.IEnumerable<string> blocks, string probe, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await ExecAsync(connection, roles, ct);
        foreach (var block in blocks)
        {
            await ExecAsync(connection, block, ct);
        }

        var result = await ScalarAsync(connection, probe, ct);
        await transaction.RollbackAsync(ct);
        return result;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>#5914: a report's links are link_recorded journal rows, one per request and URL: a URL given twice (as two
    /// kinds) writes one row, the first kind kept; the read hands them back on the report, and a report without links reads
    /// with none and answers with no links key.</summary>
    [Fact]
    public async Task AReportsLinks_AreJournaledOncePerUrl_AndReadBackOnTheReport()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(id, "agent-a", ct);
            var links = new[]
            {
                new InvestigationReportLink("jira", "https://jira.example.invalid/browse/OPS-1", "OPS-1"),
                new InvestigationReportLink("bug_report", "https://jira.example.invalid/browse/OPS-1", null),
                new InvestigationReportLink("pull_request", "https://git.example.invalid/pm/pull/42", "#42"),
            };
            Assert.Equal(InvestigationSubmitOutcome.Reported,
                await store.SubmitAsync(Report(id, claim.ClaimId!.Value) with { Links = links }, ct));

            Assert.Equal("2|agent:agent-a", await ScalarAsync(connection, $@"
SELECT count(*)::text || '|' || min(actor) FROM config.investigation_journal
WHERE request_id = '{id}' AND event = 'link_recorded'", ct));

            var read = await store.ReadReportsAsync(id, null, T0.AddDays(-1), 10, 200, ct);
            var report = Assert.Single(read.Reports);
            Assert.Equal(new[] { "jira OPS-1 https://jira.example.invalid/browse/OPS-1", "pull_request #42 https://git.example.invalid/pm/pull/42" },
                report.Links!.Select(l => l.Kind + " " + l.Key + " " + l.Url));

            /* A second request, reported with no links: no Links, and no links key anywhere in the answer. */
            _now = T0.AddMinutes(1);
            var other = (await store.OpenOrJoinAsync(Alert(T0.AddMinutes(1), family: "jobs"), ct)).RequestId!.Value;
            var otherClaim = await store.ClaimAsync(other, "agent-b", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(Report(other, otherClaim.ClaimId!.Value), ct));
            var bare = await store.ReadReportsAsync(other, null, T0.AddDays(-1), 10, 200, ct);
            Assert.Null(Assert.Single(bare.Reports).Links);
            Assert.DoesNotContain("\"links", InvestigationToolCore.ReportsAnswer(bare, null, McpDataDetail.Full), StringComparison.Ordinal);
        });
    }

    /// <summary>#5914 review r1: link_recorded rows are served whole on each report, so the capped newest-first journal page
    /// leaves them out. A link shares its report's event_time and sorts above it, so left in, 50 reported requests with two
    /// links each (250 rows: opened, claimed, reported and two links apiece) would overflow the 200-row page and drop the
    /// oldest requests' opened and claimed rows; now the 150 lifecycle rows fit, nothing is truncated, and every report still
    /// carries both its links.</summary>
    [Fact]
    public async Task ReportLinks_DoNotCompeteForTheJournalPage_WithTheLifecycleRows()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            const int requests = 50;
            var ids = new System.Collections.Generic.List<Guid>();
            for (var i = 0; i < requests; i++)
            {
                _now = T0.AddSeconds(i * 10);
                var id = (await store.OpenOrJoinAsync(Alert(_now, serverId: 100 + i, maxPerHour: 1000, maxPerDay: 1000), ct)).RequestId!.Value;
                var claim = await store.ClaimAsync(id, "agent-a", ct);
                var links = new[]
                {
                    new InvestigationReportLink("jira", "https://jira.example.invalid/browse/OPS-" + i, "OPS-" + i),
                    new InvestigationReportLink("pull_request", "https://git.example.invalid/pm/pull/" + i, "#" + i),
                };
                Assert.Equal(InvestigationSubmitOutcome.Reported,
                    await store.SubmitAsync(Report(id, claim.ClaimId!.Value) with { Links = links }, ct));
                ids.Add(id);
            }

            Assert.Equal(requests * 2, int.Parse((await ScalarAsync(connection,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'link_recorded'", ct))!, CultureInfo.InvariantCulture));

            var read = await store.ReadReportsAsync(null, null, T0.AddDays(-1), 100, 200, ct);
            Assert.Equal(requests, read.Reports.Count);
            Assert.All(read.Reports, r => Assert.Equal(2, r.Links!.Count));
            Assert.DoesNotContain(read.Journal, j => j.Event == InvestigationReportLinks.JournalEvent);
            Assert.False(read.JournalTruncated);
            foreach (var evt in new[] { "opened", "claimed", "reported" })
            {
                Assert.Equal(requests, read.Journal.Count(j => j.Event == evt));
            }
        });
    }

    /// <summary>#5914: the store refuses a link the tool rule refuses (a caller that skipped the tool), before any write.</summary>
    [Fact]
    public async Task AnHttpLink_IsRefusedByTheStore_AndNothingIsWritten()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(id, "agent-a", ct);
            await Assert.ThrowsAsync<ArgumentException>(async () => await store.SubmitAsync(Report(id, claim.ClaimId!.Value) with
            {
                Links = new[] { new InvestigationReportLink("jira", "http://jira.example.invalid/browse/OPS-1", null) },
            }, ct));
            Assert.Equal("claimed|0", await ScalarAsync(connection, $@"
SELECT r.state || '|' || (SELECT count(*) FROM config.investigation_reports WHERE request_id = r.request_id)
FROM config.investigation_requests AS r WHERE r.request_id = '{id}'", ct));
        });
    }

    /// <summary>#5914: rendering re-checks the URL. A UNC and a file link planted straight into the journal (no submit
    /// checked them) read back as stored, the viewer shows each as text with no target, and the answer still masks them as
    /// free text.</summary>
    [Fact]
    public async Task AUncOrFileLinkPlantedInTheStore_ReadsBack_AndRendersAsText()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(id, "agent-a", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(Report(id, claim.ClaimId!.Value), ct));
            await ExecAsync(connection, $@"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail)
VALUES (now(), '{id}', 7, 'link_recorded', 'agent:agent-a', 'jira', '{{""kind"":""jira"",""url"":""\\\\attacker.example.invalid\\share""}}'::jsonb),
       (now(), '{id}', 7, 'link_recorded', 'agent:agent-a', 'bug_report', '{{""kind"":""bug_report"",""url"":""file://attacker.example.invalid/share/x""}}'::jsonb)", ct);

            var report = Assert.Single((await store.ReadReportsAsync(id, null, T0.AddDays(-1), 10, 200, ct)).Reports);
            Assert.Equal(new[] { "\\\\attacker.example.invalid\\share", "file://attacker.example.invalid/share/x" }, report.Links!.Select(l => l.Url));
            foreach (var link in report.Links!)
            {
                Assert.False(InvestigationReportLinks.IsSafeUrl(link.Url));
                var (label, target, refused) = PerformanceMonitor.Darling.Viewer.InvestigationsWindow.LinkItem(link);
                Assert.Null(target);
                Assert.NotNull(refused);
                Assert.Contains(link.Url, label, StringComparison.Ordinal);
            }
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation store live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, connection, ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
