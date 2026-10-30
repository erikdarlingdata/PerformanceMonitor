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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The investigation MCP tools on a live store (#5632 Phase A, L6): a claim composes the brief, stores it under the claim
/// and returns it; a compose failure releases the claim (still counted) and journals once; two concurrent claims have one
/// winner; a lapsed claim id claims afresh with a new id, while one superseded by another agent's live lease answers
/// <c>already_claimed</c> and its report <c>lease_lost</c> (ruling 2); the list writes nothing and the web
/// <c>/api/read</c> serves it; a proposed note lands pending.
/// </summary>
public sealed class InvestigationToolsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\",\"basis\":\"verified\"}]";

    /* The store's clock: the list tool reads with the real clock, so the requests open at the real now. */
    private DateTime _now = DateTime.UtcNow.AddSeconds(-5);

    private InvestigationAlert Alert(string family = "blocking") =>
        new(1, 7, "srv-7", family, "Blocking", _now, new[] { "key-" + family }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    private sealed class FailingReader : IInvestigationBriefSourceReader
    {
        public Task<InvestigationBriefSources?> ReadAsync(Guid requestId, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("brief source read failed");
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public async Task Claim_ComposesTheBrief_StoresItUnderTheClaim_AndReturnsIt()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);

            var answer = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, openedId.ToString(), "agent-a", null, null, ct));

            Assert.Equal("claimed", answer.GetProperty("status").GetString());
            var brief = answer.GetProperty("brief").GetRawText();
            var claimId = answer.GetProperty("claim_id").GetString();
            Assert.Equal(InvestigationFormats.BriefFormat, answer.GetProperty("brief_format").GetInt16());
            Assert.Equal("true", await ScalarAsync(connection,
                $"SELECT (brief = '{brief.Replace("'", "''", StringComparison.Ordinal)}'::jsonb AND brief_format = 1 AND claim_id = '{claimId}')::text " +
                $"FROM config.investigation_requests WHERE request_id = '{openedId}'", ct));
        });
    }

    [Fact]
    public async Task ComposeFailure_ReleasesTheClaim_KeepsItCounted_AndJournalsOnce()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var composer = new InvestigationBriefComposer(new FailingReader(), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);

            for (var attempt = 0; attempt < 2; attempt++)
            {
                var answer = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, openedId.ToString(), "agent-a", null, null, ct));
                Assert.Equal("error", answer.GetProperty("status").GetString());
                Assert.Contains("released", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            Assert.Equal("open|2", await ScalarAsync(connection,
                $"SELECT state || '|' || claim_count FROM config.investigation_requests WHERE request_id = '{openedId}'", ct));
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{openedId}' AND reason = 'claim:brief_failed'", ct));
        });
    }

    [Fact]
    public async Task TwoConcurrentClaims_HaveOneWinner()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);

            var answers = await Task.WhenAll(
                Task.Run(() => DarlingMcpInvestigationTools.ClaimAsync(store, composer, openedId.ToString(), "agent-a", null, null, ct), ct),
                Task.Run(() => DarlingMcpInvestigationTools.ClaimAsync(store, composer, openedId.ToString(), "agent-b", null, null, ct), ct));

            var statuses = answers.Select(a => Root(a).GetProperty("status").GetString()).OrderBy(s => s, StringComparer.Ordinal).ToArray();
            Assert.Equal(new[] { "already_claimed", "claimed" }, statuses);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT claim_count FROM config.investigation_requests WHERE request_id = '{openedId}'", ct));
        });
    }

    /// <summary>Ruling 2. A lapsed claim id (nobody else holds the request) claims afresh with a NEW claim id and counts;
    /// a claim id superseded by another agent's LIVE lease answers already_claimed and never takes the request, and its
    /// report answers lease_lost. The clock is moved by the store's lease column.</summary>
    [Fact]
    public async Task ALapsedClaimId_ClaimsAfresh_ASupersededOne_IsAlreadyClaimed_AndItsReportLeaseLost()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var id = openedId.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var lapse = $"UPDATE config.investigation_requests SET lease_until = now() - interval '1 minute' WHERE request_id = '{id}'";

            var first = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct));
            var firstClaim = first.GetProperty("claim_id").GetString()!;

            /* A live lease renews under the same id. */
            var renewed = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", firstClaim, null, ct));
            Assert.Equal("renewed", renewed.GetProperty("status").GetString());
            Assert.Equal(firstClaim, renewed.GetProperty("claim_id").GetString());

            /* Lapsed, nobody else holds it: a new claim id, counted. */
            await ExecAsync(connection, lapse, ct);
            var afresh = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", firstClaim, null, ct));
            Assert.Equal("claimed", afresh.GetProperty("status").GetString());
            var secondClaim = afresh.GetProperty("claim_id").GetString()!;
            Assert.NotEqual(firstClaim, secondClaim);
            Assert.Equal(2, afresh.GetProperty("claim_count").GetInt32());

            /* Lapsed again, agent-b takes a live lease: agent-a's id is superseded. */
            await ExecAsync(connection, lapse, ct);
            var other = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-b", null, null, ct));
            Assert.Equal("claimed", other.GetProperty("status").GetString());
            var otherClaim = other.GetProperty("claim_id").GetString()!;

            var superseded = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", secondClaim, null, ct));
            Assert.Equal("already_claimed", superseded.GetProperty("status").GetString());
            Assert.Equal($"{otherClaim}|3", await ScalarAsync(connection,
                $"SELECT claim_id || '|' || claim_count FROM config.investigation_requests WHERE request_id = '{id}'", ct));

            var late = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, id, secondClaim, 1, "real", "a blocking chain",
                Evidence, "summary", null, null, ct));
            Assert.Equal("lease_lost", late.GetProperty("status").GetString());
        });
    }

    /// <summary>#6576, #6586: through the tool on a named agent token, the agent that lost its claim_id claims again and gets status
    /// renewed, reclaimed true and the live claim_id, with the lease moved on; another agent_id is refused. On the legacy token
    /// (agent_id is free text there, and claimed_by is published by list_investigation_requests) the same call is already_claimed
    /// and no claim_id comes back.</summary>
    [Fact]
    public async Task AReclaim_ReturnsTheLiveClaimId_OnlyOnANamedToken_AndTheLegacyTokenIsRefused()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var id = openedId.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);

            var first = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct, "agent-a"));
            var claimId = first.GetProperty("claim_id").GetString()!;
            await ExecAsync(connection,
                $"UPDATE config.investigation_requests SET lease_until = now() + interval '1 minute' WHERE request_id = '{id}'", ct);
            var before = await ScalarAsync(connection,
                $"SELECT lease_until::text FROM config.investigation_requests WHERE request_id = '{id}'", ct);

            /* The legacy token: the claimed_by name is public, so naming it must not return the claim_id. */
            var legacy = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct));
            Assert.Equal("already_claimed", legacy.GetProperty("status").GetString());
            Assert.False(legacy.TryGetProperty("claim_id", out _));
            Assert.Equal("false", await ScalarAsync(connection,
                $"SELECT (lease_until > '{before}'::timestamptz)::text FROM config.investigation_requests WHERE request_id = '{id}'", ct));

            var again = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct, "agent-a"));
            Assert.Equal("renewed", again.GetProperty("status").GetString());
            Assert.True(again.GetProperty("reclaimed").GetBoolean());
            Assert.Equal(claimId, again.GetProperty("claim_id").GetString());
            Assert.Equal("true", await ScalarAsync(connection,
                $"SELECT (lease_until > '{before}'::timestamptz)::text FROM config.investigation_requests WHERE request_id = '{id}'", ct));

            var other = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-b", null, null, ct, "agent-b"));
            Assert.Equal("already_claimed", other.GetProperty("status").GetString());
            Assert.False(other.TryGetProperty("claim_id", out _));
            Assert.Equal($"{claimId}|1", await ScalarAsync(connection,
                $"SELECT claim_id || '|' || claim_count FROM config.investigation_requests WHERE request_id = '{id}'", ct));
        });
    }

    [Fact]
    public async Task List_WritesNothing_AndTheWebReadServesTheSameAnswer()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            await store.OpenOrJoinAsync(Alert("resources"), ct);
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            await DarlingMcpInvestigationTools.ClaimAsync(store, composer, openedId.ToString(), "agent-a", null, null, ct);
            /* A lapsed lease reads open without a write: the row versions and the journal must not move. */
            await ExecAsync(connection,
                $"UPDATE config.investigation_requests SET lease_until = now() - interval '1 minute' WHERE request_id = '{openedId}'", ct);

            const string Versions =
                "SELECT (SELECT string_agg(xmin::text, ',' ORDER BY request_id) FROM config.investigation_requests) || '|' || " +
                "(SELECT count(*) FROM config.investigation_journal)";
            var before = await ScalarAsync(connection, Versions, ct);

            var tool = await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, cancellationToken: ct);
            var context = new DefaultHttpContext();
            context.Request.QueryString = new QueryString("?limit=20");
            var web = await DarlingWebEndpoints.BuildReadDispatch()["list_investigation_requests"](context, dataSource, null!);

            Assert.Equal(before, await ScalarAsync(connection, Versions, ct));
            Assert.Equal(tool, web);

            var root = Root(tool);
            Assert.Equal("ok", root.GetProperty("status").GetString());
            var states = root.GetProperty("requests").EnumerateArray()
                .Select(r => r.GetProperty("state").GetString()).ToArray();
            Assert.Equal(new[] { "open", "open" }, states);
        });
    }

    /// <summary>#5904: the two reads the Investigations page makes carry the fields it draws, in the MCP order (requests
    /// <c>opened_at DESC</c>, journal <c>event_time DESC</c>), and the report read the web serves is the full report and
    /// brief an operator sees (the same answer the tool gives at the full level).</summary>
    [Fact]
    public async Task TheWebReads_CarryTheFieldsThePageDraws_InTheMcpOrder_AndTheReportIsServedInFull()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var older = (await store.OpenOrJoinAsync(Alert("resources"), ct)).RequestId!.Value;
            _now = _now.AddSeconds(2);
            var newer = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var id = newer.ToString();
            var claim = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct));
            await DarlingMcpInvestigationTools.SubmitAsync(store, id, claim.GetProperty("claim_id").GetString()!, 1,
                "needs_person", "could not tell", Evidence, "see the table SecretOrders for the cause", null, null, ct);

            async Task<JsonElement> WebRead(string tool, string query)
            {
                var context = new DefaultHttpContext();
                context.Request.QueryString = new QueryString(query);
                return Root(await DarlingWebEndpoints.BuildReadDispatch()[tool](context, dataSource, null!));
            }

            var list = await WebRead("list_investigation_requests", "?limit=100");
            var requests = list.GetProperty("requests").EnumerateArray().ToArray();
            Assert.Equal(new[] { newer, older }, requests.Select(r => r.GetProperty("request_id").GetGuid()));
            foreach (var key in new[] { "request_id", "opened_at", "state", "server_name", "first_metric", "family", "alert_count",
                         "claimed_by", "lease_until", "reported_at", "route_id" })
                Assert.True(requests[0].TryGetProperty(key, out _), "the list row carries " + key);
            Assert.Equal("reported", requests[0].GetProperty("state").GetString());
            Assert.Equal("agent-a", requests[0].GetProperty("claimed_by").GetString());
            Assert.Equal("open", requests[1].GetProperty("state").GetString());

            var full = await WebRead("get_investigation_reports", "?request_id=" + id);
            var report = Assert.Single(full.GetProperty("reports").EnumerateArray().ToArray());
            foreach (var key in new[] { "agent_id", "submitted_at", "verdict", "root_cause_found", "root_cause", "summary", "evidence_verified", "evidence_inferred" })
                Assert.True(report.TryGetProperty(key, out _), "the report carries " + key);
            Assert.Equal("see the table SecretOrders for the cause", report.GetProperty("summary").GetString());
            Assert.True(full.GetProperty("brief").ValueKind == JsonValueKind.Object);
            var journal = full.GetProperty("journal").EnumerateArray().ToArray();
            Assert.Contains(journal, j => j.GetProperty("event").GetString() == "claimed");
            Assert.Contains(journal, j => j.GetProperty("event").GetString() == "reported");
            var times = journal.Select(j => j.GetProperty("event_time").GetDateTime()).ToArray();
            Assert.Equal(times.OrderByDescending(t => t), times);
            foreach (var key in new[] { "event_time", "event", "actor", "reason", "request_id" })
                Assert.True(journal[0].TryGetProperty(key, out _), "the journal row carries " + key);

            /* The web read is the tool's full-level answer: an operator sees the whole report and brief. */
            var tool = Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, id, 24, ct));
            Assert.Equal(tool.GetProperty("reports").GetRawText(), full.GetProperty("reports").GetRawText());
            Assert.Equal(tool.GetProperty("brief").GetRawText(), full.GetProperty("brief").GetRawText());
        });
    }

    [Fact]
    public async Task Submit_AProposedNote_LandsPending_AndTheReportIsStored()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var id = openedId.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var claim = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct));

            var answer = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, id, claim.GetProperty("claim_id").GetString()!, 1,
                "needs_person", "could not tell", Evidence, "summary", "  check the nightly index job  ", null, ct));

            Assert.Equal("reported", answer.GetProperty("status").GetString());
            Assert.Equal("pending", answer.GetProperty("proposed_note").GetString());
            Assert.Equal("pending|check the nightly index job|true", await ScalarAsync(connection,
                "SELECT state || '|' || note_text || '|' || (strpos(author, 'agent-a') > 0)::text " +
                $"FROM config.investigation_notes WHERE source_request_id = '{id}'", ct));
            /* Ruling 3: root_cause_found is false only for needs_person. */
            Assert.Equal("false", await ScalarAsync(connection,
                $"SELECT root_cause_found::text FROM config.investigation_reports WHERE request_id = '{id}'", ct));
        });
    }

    /// <summary>#5914: submit refuses a UNC link as invalid, naming links and the rule, and stores nothing; the same report
    /// with https links is reported, the repeated URL counted once in links_recorded and journaled once; a report without
    /// links answers with no links_recorded key.</summary>
    [Fact]
    public async Task Submit_Links_RefusesAUncUrl_AndRecordsEachUrlOnce()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var claimId = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct))
                .GetProperty("claim_id").GetString()!;

            var refused = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, id, claimId, 1, "real", "a blocking chain", Evidence,
                "summary", null, null, ct, "[{\"kind\":\"jira\",\"url\":\"\\\\\\\\attacker.example.invalid\\\\share\"}]"));
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
            Assert.Contains("links: item 0 url: a UNC path", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*)::text FROM config.investigation_reports WHERE request_id = '{id}'", ct));

            const string Links = "[{\"kind\":\"jira\",\"url\":\"https://jira.example.invalid/browse/OPS-7\",\"key\":\"OPS-7\"}," +
                "{\"kind\":\"bug_report\",\"url\":\"https://jira.example.invalid/browse/OPS-7\"}," +
                "{\"kind\":\"pull_request\",\"url\":\"https://git.example.invalid/pm/pull/9\",\"key\":\"#9\"}]";
            var answer = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, id, claimId, 1, "real", "a blocking chain", Evidence,
                "summary", null, null, ct, Links));
            Assert.Equal("reported", answer.GetProperty("status").GetString());
            Assert.Equal(2, answer.GetProperty("links_recorded").GetInt32());
            Assert.Equal("2", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_journal WHERE request_id = '{id}' AND event = 'link_recorded'", ct));

            var read = Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, id, 24, ct));
            var links = read.GetProperty("reports")[0].GetProperty("links");
            Assert.Equal(2, links.GetArrayLength());
            Assert.Equal("https://jira.example.invalid/browse/OPS-7", links[0].GetProperty("url").GetString());
        });
    }

    /// <summary>#5632 acceptance test, claim side: a brief_format this Darling does not compose is refused as
    /// <c>unsupported_format</c>, the message names the format sent and the one composed, the refusal is journaled once for
    /// the request, and the request is left open and unclaimed. Format 1 and an omitted format still claim.</summary>
    [Theory]
    [InlineData(2)]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Claim_AnUnsupportedBriefFormat_IsRefused_NamingBothFormats_AndJournaledOnce(int sent)
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);

            for (var attempt = 0; attempt < 2; attempt++)
            {
                var refused = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, sent, ct));
                Assert.Equal("unsupported_format", refused.GetProperty("status").GetString());
                var message = refused.GetProperty("message").GetString()!;
                Assert.Contains("brief_format " + sent.ToString(System.Globalization.CultureInfo.InvariantCulture) + " is not supported", message, StringComparison.Ordinal);
                Assert.Contains("brief_format " + InvestigationFormats.BriefFormat.ToString(System.Globalization.CultureInfo.InvariantCulture), message.Substring(message.IndexOf("Darling composes", StringComparison.Ordinal)), StringComparison.Ordinal);
            }

            /* Nothing was claimed, and the refusal is one journal row however often the agent retries. */
            Assert.Equal("open|0", await ScalarAsync(connection,
                $"SELECT state || '|' || claim_count FROM config.investigation_requests WHERE request_id = '{id}'", ct));
            Assert.Equal("1|agent:agent-a", await ScalarAsync(connection,
                $"SELECT count(*) || '|' || min(actor) FROM config.investigation_journal WHERE request_id = '{id}' AND event = 'refused' AND reason = 'claim:unsupported_format'", ct));

            /* The format the agent was built for still claims, and so does leaving it out. */
            var claimed = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, InvestigationFormats.BriefFormat, ct));
            Assert.Equal("claimed", claimed.GetProperty("status").GetString());
            var omitted = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", claimed.GetProperty("claim_id").GetString(), null, ct));
            Assert.Contains(omitted.GetProperty("status").GetString(), new[] { "claimed", "renewed" });
        });
    }

    /// <summary>#5632 acceptance test, report side: format 1 (the previous format, once a format 2 exists) is accepted; a
    /// format this Darling does not accept is refused as <c>unsupported_format</c>, naming the format sent and every accepted
    /// format, stores nothing, and is journaled once under the claiming agent. A number outside the 16-bit range is refused
    /// too, not wrapped into an accepted format (65537 would wrap to 1).</summary>
    [Theory]
    [InlineData(2)]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(65537)]
    public async Task Submit_AnUnsupportedReportFormat_IsRefused_NamingBothFormats_AndJournaledOnce(int sent)
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var claimId = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct))
                .GetProperty("claim_id").GetString()!;

            for (var attempt = 0; attempt < 2; attempt++)
            {
                var refused = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, id, claimId, sent, "real", "a blocking chain", Evidence,
                    "summary", null, null, ct));
                Assert.Equal("unsupported_format", refused.GetProperty("status").GetString());
                var message = refused.GetProperty("message").GetString()!;
                /* The format sent (a number past the 16-bit range is shown at its clamp) and every format accepted. */
                var shown = Math.Clamp(sent, short.MinValue, short.MaxValue).ToString(System.Globalization.CultureInfo.InvariantCulture);
                Assert.Contains("report_format " + shown + " is not supported", message, StringComparison.Ordinal);
                Assert.Contains("accepts report_format " + string.Join(", ", InvestigationFormats.AcceptedReportFormats), message, StringComparison.Ordinal);
            }

            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*)::text FROM config.investigation_reports WHERE request_id = '{id}'", ct));
            Assert.Equal("1|agent:agent-a", await ScalarAsync(connection,
                $"SELECT count(*) || '|' || min(actor) FROM config.investigation_journal WHERE request_id = '{id}' AND event = 'refused' AND reason = 'submit:unsupported_format'", ct));

            /* The same claim then reports in format 1, so the refusal cost the agent nothing. */
            var accepted = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, id, claimId, 1, "real", "a blocking chain", Evidence,
                "summary", null, null, ct));
            Assert.Equal("reported", accepted.GetProperty("status").GetString());
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_reports WHERE request_id = '{id}' AND report_format = 1", ct));
        });
    }

    /// <summary>#6004: a family_of that is not a full id (the 8-character short form) or names no request is refused as invalid,
    /// and nothing is stored. The second refusal names the id. One that
    /// names a request is stored and served back, with the queries run.</summary>
    [Fact]
    public async Task Submit_FamilyOf_MustNameARequest_AndIsServedBack()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var first = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var second = (await store.OpenOrJoinAsync(Alert("resources"), ct)).RequestId!.Value.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var claimId = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, second, "agent-a", null, null, ct))
                .GetProperty("claim_id").GetString()!;

            var shortForm = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, second, claimId, 1, "real", "a blocking chain", Evidence,
                "summary", null, null, ct, family_of: first.ToString()[..8]));
            Assert.Equal("invalid", shortForm.GetProperty("status").GetString());
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*)::text FROM config.investigation_reports WHERE request_id = '{second}'", ct));

            var missing = Guid.NewGuid();
            var refused = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, second, claimId, 1, "real", "a blocking chain", Evidence,
                "summary", null, null, ct, family_of: missing.ToString()));
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
            Assert.Contains($"family_of: no investigation request has id {missing}", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*)::text FROM config.investigation_reports WHERE request_id = '{second}'", ct));

            var answer = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, second, claimId, 1, "real", "a blocking chain", Evidence,
                "summary", null, null, ct, family_of: first.ToString(), queries_run: "[\"SELECT 1\"]"));
            Assert.Equal("reported", answer.GetProperty("status").GetString());

            var report = Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, second, 24, ct))
                .GetProperty("reports")[0];
            Assert.Equal(first.ToString(), report.GetProperty("family_of").GetString());
            Assert.Equal("SELECT 1", report.GetProperty("queries_run")[0].GetString());
            Assert.Equal(1, report.GetProperty("evidence_verified").GetArrayLength());
        });
    }

    /// <summary>#6004: a commit link and a release link are stored as link_recorded rows and read back with their kinds.</summary>
    [Fact]
    public async Task Submit_CommitAndReleaseLinks_AreStored_AndReadBack()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var claimId = Root(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, id, "agent-a", null, null, ct))
                .GetProperty("claim_id").GetString()!;

            const string Links = "[{\"kind\":\"commit\",\"url\":\"https://git.example.invalid/pm/commit/abc123\",\"key\":\"abc123\"}," +
                "{\"kind\":\"release\",\"url\":\"https://git.example.invalid/pm/releases/tag/v3.11.0\"}]";
            var answer = Root(await DarlingMcpInvestigationTools.SubmitAsync(store, id, claimId, 1, "real", "a blocking chain", Evidence,
                "summary", null, null, ct, Links));
            Assert.Equal("reported", answer.GetProperty("status").GetString());
            Assert.Equal(2, answer.GetProperty("links_recorded").GetInt32());

            var links = Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, id, 24, ct))
                .GetProperty("reports")[0].GetProperty("links");
            Assert.Equal(new[] { "commit", "release" }, links.EnumerateArray().Select(l => l.GetProperty("kind").GetString()).ToArray());
        });
    }

    /// <summary>#5822: the long poll's catch-up read answers the requests opened after a time, on the named routes, oldest
    /// first, and the wait tool answers a request opened after its cursor through it.</summary>
    [Fact]
    public async Task OpenedSince_AnswersRequestsOpenedAfterATime_OnTheNamedRoutes_AndTheWaitToolAnswersThroughIt()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var signal = new InvestigationOpenSignal();
            var start = _now;
            var cursor = Root(await DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, null, null, 0, ct))
                .GetProperty("cursor").GetString();

            _now = _now.AddSeconds(1);
            var first = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            _now = _now.AddSeconds(1);
            var second = (await store.OpenOrJoinAsync(
                new InvestigationAlert(2, 8, "srv-8", "resources", "High CPU", _now, new[] { "key-cpu" }, null, 20, 100,
                    LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;

            var all = await store.OpenedSinceAsync(start, null, 10, ct);
            Assert.Equal(new[] { first, second }, all.Select(r => r.RequestId));
            var routeTwo = Assert.Single(await store.OpenedSinceAsync(start, new[] { 2 }, 10, ct));
            Assert.Equal((second, 2, "srv-8", "resources"), (routeTwo.RequestId, routeTwo.RouteId, routeTwo.ServerName, routeTwo.Family));
            Assert.Empty(await store.OpenedSinceAsync(all[1].OpenedAtUtc, null, 10, ct));
            Assert.Single(await store.OpenedSinceAsync(start, null, 1, ct));

            var answer = Root(await DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, new[] { 1 }, cursor, 0, ct));
            Assert.Equal("ok", answer.GetProperty("status").GetString());
            var row = Assert.Single(answer.GetProperty("requests").EnumerateArray().ToArray());
            Assert.Equal(first, row.GetProperty("request_id").GetGuid());
            Assert.Equal("srv-7", row.GetProperty("server_name").GetString());
        });
    }

    /// <summary>#6551: a change-watch request has no alert row in the history, so the list and the wait answer carry its detail
    /// (what changed, the database of each query measure, which measure moved) as <c>detail_text</c>; an alert's request carries none.</summary>
    [Fact]
    public async Task ListAndWait_CarryAChangeWatchRequestsDetail_AndAnAlertRequestsCarryNone()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var signal = new InvestigationOpenSignal();
            var start = _now;
            var cursor = Root(await DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, null, null, 0, ct))
                .GetProperty("cursor").GetString();
            const string detail = "After a recorded change: 10:00 plan 9 of query 5 in Sales: forced; query_avg_duration_ms:0xABC before 10 after 40 (read from the plan cache, query_hash 0xABC in database Sales)";
            _now = _now.AddSeconds(1);
            var watch = (await store.OpenOrJoinAsync(
                new InvestigationAlert(1, 7, "srv-7", "query_performance", InvestigationTriggerKinds.ChangeWatchMetric, _now, new[] { "change-watch:1" }, null, 20, 100,
                    LeaseMinutes: 15, ExpiryMinutes: 240, Facts: new InvestigationAlertFacts(1, 1, false, detail, null))
                { TriggerKind = InvestigationTriggerKinds.ChangeWatch }, ct)).RequestId!.Value;
            _now = _now.AddSeconds(1);
            var alert = (await store.OpenOrJoinAsync(
                new InvestigationAlert(2, 8, "srv-8", "resources", "High CPU", _now, new[] { "key-cpu" }, null, 20, 100,
                    LeaseMinutes: 15, ExpiryMinutes: 240, Facts: new InvestigationAlertFacts(95, 90, false, "CPU is high", null)), ct)).RequestId!.Value;

            var listed = Root(await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, cancellationToken: ct))
                .GetProperty("requests").EnumerateArray().ToDictionary(r => r.GetProperty("request_id").GetGuid());
            Assert.Equal(detail, listed[watch].GetProperty("detail_text").GetString());
            Assert.False(listed[alert].TryGetProperty("detail_text", out _));

            var rows = (await store.OpenedSinceAsync(start, null, 10, ct)).ToDictionary(r => r.RequestId);
            Assert.Equal(detail, rows[watch].DetailText);
            Assert.Null(rows[alert].DetailText);
            var answer = Root(await DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, null, cursor, 0, ct));
            var byId = answer.GetProperty("requests").EnumerateArray().ToDictionary(r => r.GetProperty("request_id").GetGuid());
            Assert.Equal(detail, byId[watch].GetProperty("detail_text").GetString());
            Assert.False(byId[alert].TryGetProperty("detail_text", out _));
        });
    }

    /* #5834: both reads are pages, and each says when the window held more than the page (the #3699 shape). At the bound:
       101 requests answer 100 returned and truncated; 100 answer 100 and not truncated, on the reports read and the list
       alike; and 201 journal rows on the page's requests answer 200 and journal_truncated, 200 answer 200 and not. */
    [Fact]
    public async Task ReportsAndList_SayWhenTheWindowHeldMoreThanThePage()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            await ExecAsync(connection, @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until,
     opened_at, expires_at)
SELECT gen_random_uuid(), 1, 7, 'srv-7', 'blocking', 'Blocking', now() - make_interval(secs => i), now() - make_interval(secs => i),
       now(), now() - make_interval(secs => i), now() + interval '4 hours'
FROM generate_series(1, " + (InvestigationToolCore.MaxLimit + 1) + ") AS i", ct);
            var newest = await ScalarAsync(connection,
                "SELECT request_id FROM config.investigation_requests ORDER BY opened_at DESC LIMIT 1", ct);

            async Task<(JsonElement Reports, JsonElement List)> ReadAsync() => (
                Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, null, 24, ct)),
                Root(await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, limit: InvestigationToolCore.MaxLimit, cancellationToken: ct)));

            async Task JournalRowsAsync(int count) => await ExecAsync(connection,
                "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor) " +
                $"SELECT now() - make_interval(secs => i), '{newest}', 7, 'opened', 'service' FROM generate_series(1, {count}) AS i", ct);

            var (reports, list) = await ReadAsync();
            Assert.Equal(InvestigationToolCore.MaxLimit, reports.GetProperty("requests_returned").GetInt32());
            Assert.Equal(InvestigationToolCore.MaxLimit, reports.GetProperty("requests").GetArrayLength());
            Assert.True(reports.GetProperty("truncated").GetBoolean());
            Assert.Equal(InvestigationToolCore.MaxLimit, list.GetProperty("requests_returned").GetInt32());
            Assert.Equal(InvestigationToolCore.MaxLimit, list.GetProperty("requests").GetArrayLength());
            Assert.True(list.GetProperty("truncated").GetBoolean());

            await ExecAsync(connection,
                "DELETE FROM config.investigation_requests WHERE request_id = (SELECT request_id FROM config.investigation_requests ORDER BY opened_at LIMIT 1)", ct);
            await JournalRowsAsync(InvestigationToolCore.JournalLimit);
            (reports, list) = await ReadAsync();
            Assert.Equal(InvestigationToolCore.MaxLimit, reports.GetProperty("requests_returned").GetInt32());
            Assert.False(reports.GetProperty("truncated").GetBoolean());
            Assert.Equal(InvestigationToolCore.JournalLimit, reports.GetProperty("journal").GetArrayLength());
            Assert.False(reports.GetProperty("journal_truncated").GetBoolean());
            Assert.Equal(InvestigationToolCore.MaxLimit, list.GetProperty("requests_returned").GetInt32());
            Assert.False(list.GetProperty("truncated").GetBoolean());

            await JournalRowsAsync(1);
            (reports, _) = await ReadAsync();
            Assert.Equal(InvestigationToolCore.JournalLimit, reports.GetProperty("journal").GetArrayLength());
            Assert.True(reports.GetProperty("journal_truncated").GetBoolean());
        });
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation tools live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, dataSource, connection, ct);
    }
}
