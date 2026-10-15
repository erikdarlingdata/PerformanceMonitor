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

    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\"}]";

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
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full));

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
            var composer = new InvestigationBriefComposer(new FailingReader(), new McpDataDetailSetting(McpDataDetail.Full));

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
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full));

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
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full));
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

    [Fact]
    public async Task List_WritesNothing_AndTheWebReadServesTheSameAnswer()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            await store.OpenOrJoinAsync(Alert("resources"), ct);
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full));
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

    [Fact]
    public async Task Submit_AProposedNote_LandsPending_AndTheReportIsStored()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var openedId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var id = openedId.ToString();
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(dataSource), new McpDataDetailSetting(McpDataDetail.Full));
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
FROM generate_series(1, " + (DarlingMcpInvestigationTools.MaxLimit + 1) + ") AS i", ct);
            var newest = await ScalarAsync(connection,
                "SELECT request_id FROM config.investigation_requests ORDER BY opened_at DESC LIMIT 1", ct);

            async Task<(JsonElement Reports, JsonElement List)> ReadAsync() => (
                Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, null, 24, ct)),
                Root(await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, limit: DarlingMcpInvestigationTools.MaxLimit, cancellationToken: ct)));

            async Task JournalRowsAsync(int count) => await ExecAsync(connection,
                "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor) " +
                $"SELECT now() - make_interval(secs => i), '{newest}', 7, 'opened', 'service' FROM generate_series(1, {count}) AS i", ct);

            var (reports, list) = await ReadAsync();
            Assert.Equal(DarlingMcpInvestigationTools.MaxLimit, reports.GetProperty("requests_returned").GetInt32());
            Assert.Equal(DarlingMcpInvestigationTools.MaxLimit, reports.GetProperty("requests").GetArrayLength());
            Assert.True(reports.GetProperty("truncated").GetBoolean());
            Assert.Equal(DarlingMcpInvestigationTools.MaxLimit, list.GetProperty("requests_returned").GetInt32());
            Assert.Equal(DarlingMcpInvestigationTools.MaxLimit, list.GetProperty("requests").GetArrayLength());
            Assert.True(list.GetProperty("truncated").GetBoolean());

            await ExecAsync(connection,
                "DELETE FROM config.investigation_requests WHERE request_id = (SELECT request_id FROM config.investigation_requests ORDER BY opened_at LIMIT 1)", ct);
            await JournalRowsAsync(DarlingMcpInvestigationTools.JournalLimit);
            (reports, list) = await ReadAsync();
            Assert.Equal(DarlingMcpInvestigationTools.MaxLimit, reports.GetProperty("requests_returned").GetInt32());
            Assert.False(reports.GetProperty("truncated").GetBoolean());
            Assert.Equal(DarlingMcpInvestigationTools.JournalLimit, reports.GetProperty("journal").GetArrayLength());
            Assert.False(reports.GetProperty("journal_truncated").GetBoolean());
            Assert.Equal(DarlingMcpInvestigationTools.MaxLimit, list.GetProperty("requests_returned").GetInt32());
            Assert.False(list.GetProperty("truncated").GetBoolean());

            await JournalRowsAsync(1);
            (reports, _) = await ReadAsync();
            Assert.Equal(DarlingMcpInvestigationTools.JournalLimit, reports.GetProperty("journal").GetArrayLength());
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
