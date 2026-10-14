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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
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

    /// <summary>The store bounds what a token holder can write (#5632 review round 1, L3): an agent id over 64 characters
    /// is refused before anything is read or written; a request takes at most 200 agent-written journal rows, after which
    /// every agent call on it is refused and one <c>journal_capped</c> row records that; and claims of unknown ids journal
    /// once per hour, not once per guessed id.</summary>
    [Fact]
    public async Task ALongAgentId_IsRefused_AndARequestsAgentJournal_IsCappedAt200_AndUnknownIdsJournalHourly()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0), ct)).RequestId!.Value;
            var tooLong = new string('a', InvestigationRequestStore.MaxAgentIdLength + 1);
            var longest = new string('b', InvestigationRequestStore.MaxAgentIdLength);
            const string AgentRows = "SELECT count(*) FROM config.investigation_journal WHERE actor LIKE 'agent:%'";

            var refused = await store.ClaimAsync(id, tooLong, ct);
            Assert.Equal(InvestigationClaimOutcome.AgentIdTooLong, refused.Outcome);
            Assert.Equal("agent_id_too_long", refused.Reason);
            Assert.Equal("0", await ScalarAsync(connection, AgentRows, ct));

            var claim = await store.ClaimAsync(id, longest, ct);
            Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
            Assert.Equal(InvestigationSubmitOutcome.AgentIdTooLong,
                await store.SubmitAsync(Report(id, claim.ClaimId!.Value) with { AgentId = tooLong }, ct));
            Assert.Equal("1", await ScalarAsync(connection, AgentRows, ct));

            /* 199 agent rows; one more refused submit under a guessed claim id is the 200th. */
            await ExecAsync(connection, $@"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason)
SELECT '2026-10-14 12:00:00+00', '{id}', 7, 'refused', 'agent:seed', 'submit:lease_lost' FROM generate_series(1, 198)", ct);
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
                && statement.Contains(" config.investigation_", StringComparison.Ordinal))
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

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
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
