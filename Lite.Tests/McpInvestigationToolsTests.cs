using System;
using System.Collections.Generic;
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
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's four investigation MCP tools on a throwaway DuckDB with the real <see cref="DuckDbInvestigationStore"/> (#5820):
/// the facts Darling's <c>InvestigationToolsLiveTests</c> and <c>DarlingMcpInvestigationToolsTests</c> hold for the same tools, under
/// the same names. A claim composes the brief, stores it under the claim and returns it; a compose failure releases the claim
/// (still counted) and journals once; two concurrent claims have one winner; a lapsed claim id claims afresh with a new id,
/// while one superseded by another agent's live lease answers <c>already_claimed</c> and its report <c>lease_lost</c>; the list
/// writes nothing; a proposed note lands pending; the window and journal truncation words are honest; the unsupported-format
/// refusals name Lite. (The OpenedSince and wait facts are not ported: Lite has no wait tool in this slice.)
/// </summary>
public sealed class McpInvestigationToolsTests : IDisposable
{
    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\"}]";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly DuckDbInvestigationStore _store;
    private readonly ServerManager _servers;
    private readonly DateTime _now = DateTime.UtcNow.AddSeconds(-5);

    public McpInvestigationToolsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpInvestigationToolsTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _db = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _db.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbInvestigationStore(_db);
        _servers = new ServerManager(_tempDir);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best effort */ }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private InvestigationAlert Alert(string family = "blocking") =>
        new(1, 7, "srv-7", family, "Blocking", _now, new[] { "key-" + family }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    private sealed class FailingReader : IInvestigationBriefSourceReader
    {
        public Task<InvestigationBriefSources?> ReadAsync(Guid requestId, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("brief source read failed");
    }

    private InvestigationBriefComposer Composer() => new(
        new DuckDbInvestigationBriefSourceReader(_db), new McpDataDetailSetting(McpDataDetail.Full), LiteInvestigationBriefTools.Tools);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

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

    private void Lapse(string id) =>
        Exec($"UPDATE investigation_requests SET lease_until = TIMESTAMP '2000-01-01 00:00:00' WHERE request_id = '{id}'");

    [Fact]
    public async Task Claim_ComposesTheBrief_StoresItUnderTheClaim_AndReturnsIt()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;

        var answer = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "agent-a", null, null, Ct));

        Assert.Equal("claimed", answer.GetProperty("status").GetString());
        var claimId = answer.GetProperty("claim_id").GetString();
        Assert.Equal(InvestigationFormats.BriefFormat, answer.GetProperty("brief_format").GetInt16());
        var returned = answer.GetProperty("brief");
        Assert.Equal(JsonValueKind.Object, returned.ValueKind);

        var stored = Root(Scalar($"SELECT brief FROM investigation_requests WHERE request_id = '{openedId}'")!);
        Assert.Equal(returned.GetRawText(), stored.GetRawText());
        Assert.Equal($"1|{claimId}", Scalar(
            $"SELECT concat_ws('|', brief_format, claim_id) FROM investigation_requests WHERE request_id = '{openedId}'"));
    }

    [Fact]
    public async Task ComposeFailure_ReleasesTheClaim_KeepsItCounted_AndJournalsOnce()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var composer = new InvestigationBriefComposer(new FailingReader(), new McpDataDetailSetting(McpDataDetail.Full), LiteInvestigationBriefTools.Tools);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var answer = Root(await McpInvestigationTools.ClaimAsync(_store, composer, openedId.ToString(), "agent-a", null, null, Ct));
            Assert.Equal("error", answer.GetProperty("status").GetString());
            Assert.Contains("released", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        }

        Assert.Equal("open|2", Scalar($"SELECT state || '|' || claim_count FROM investigation_requests WHERE request_id = '{openedId}'"));
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{openedId}' AND reason = 'claim:brief_failed'"));
    }

    [Fact]
    public async Task TwoConcurrentClaims_HaveOneWinner()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var composer = Composer();

        var answers = await Task.WhenAll(
            Task.Run(() => McpInvestigationTools.ClaimAsync(_store, composer, openedId.ToString(), "agent-a", null, null, Ct), Ct),
            Task.Run(() => McpInvestigationTools.ClaimAsync(_store, composer, openedId.ToString(), "agent-b", null, null, Ct), Ct));

        var statuses = answers.Select(a => Root(a).GetProperty("status").GetString()).OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "already_claimed", "claimed" }, statuses);
        Assert.Equal("1", Scalar($"SELECT claim_count FROM investigation_requests WHERE request_id = '{openedId}'"));
    }

    /// <summary>A lapsed claim id (nobody else holds the request) claims afresh with a NEW claim id and counts; a claim id
    /// superseded by another agent's LIVE lease answers already_claimed and never takes the request, and its report answers
    /// lease_lost. The clock is moved by the store's lease column.</summary>
    [Fact]
    public async Task ALapsedClaimId_ClaimsAfresh_ASupersededOne_IsAlreadyClaimed_AndItsReportLeaseLost()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var id = openedId.ToString();
        var composer = Composer();

        var first = Root(await McpInvestigationTools.ClaimAsync(_store, composer, id, "agent-a", null, null, Ct));
        var firstClaim = first.GetProperty("claim_id").GetString()!;

        /* A live lease renews under the same id. */
        var renewed = Root(await McpInvestigationTools.ClaimAsync(_store, composer, id, "agent-a", firstClaim, null, Ct));
        Assert.Equal("renewed", renewed.GetProperty("status").GetString());
        Assert.Equal(firstClaim, renewed.GetProperty("claim_id").GetString());

        /* Lapsed, nobody else holds it: a new claim id, counted. */
        Lapse(id);
        var afresh = Root(await McpInvestigationTools.ClaimAsync(_store, composer, id, "agent-a", firstClaim, null, Ct));
        Assert.Equal("claimed", afresh.GetProperty("status").GetString());
        var secondClaim = afresh.GetProperty("claim_id").GetString()!;
        Assert.NotEqual(firstClaim, secondClaim);
        Assert.Equal(2, afresh.GetProperty("claim_count").GetInt32());

        /* Lapsed again, agent-b takes a live lease: agent-a's id is superseded. */
        Lapse(id);
        var other = Root(await McpInvestigationTools.ClaimAsync(_store, composer, id, "agent-b", null, null, Ct));
        Assert.Equal("claimed", other.GetProperty("status").GetString());
        var otherClaim = other.GetProperty("claim_id").GetString()!;

        var superseded = Root(await McpInvestigationTools.ClaimAsync(_store, composer, id, "agent-a", secondClaim, null, Ct));
        Assert.Equal("already_claimed", superseded.GetProperty("status").GetString());
        Assert.Equal($"{otherClaim}|3", Scalar($"SELECT concat_ws('|', claim_id, claim_count) FROM investigation_requests WHERE request_id = '{id}'"));

        var late = Root(await McpInvestigationTools.SubmitAsync(_store, id, secondClaim, 1, "real", "a blocking chain", Evidence, "summary", null, null, Ct));
        Assert.Equal("lease_lost", late.GetProperty("status").GetString());
    }

    [Fact]
    public async Task List_WritesNothing_AndAnswersTheComputedState()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        await _store.OpenOrJoinAsync(Alert("resources"), Ct);
        await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "agent-a", null, null, Ct);
        /* A lapsed lease reads open without a write: the rows and the journal must not move. */
        Lapse(openedId.ToString());

        const string Snapshot =
            "SELECT (SELECT string_agg(concat_ws(',', request_id, state, claim_count, lease_until), ';' ORDER BY request_id) FROM investigation_requests)" +
            " || '|' || (SELECT count(*) FROM investigation_journal)";
        var before = Scalar(Snapshot);

        var tool = await McpInvestigationTools.ListInvestigationRequests(_servers, _store, cancellationToken: Ct);

        Assert.Equal(before, Scalar(Snapshot));
        var root = Root(tool);
        Assert.Equal("ok", root.GetProperty("status").GetString());
        Assert.Equal(new[] { "open", "open" }, root.GetProperty("requests").EnumerateArray().Select(r => r.GetProperty("state").GetString()).ToArray());
    }

    [Fact]
    public async Task Submit_AProposedNote_LandsPending_AndTheReportIsStored()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var id = openedId.ToString();
        var claim = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), id, "agent-a", null, null, Ct));

        var answer = Root(await McpInvestigationTools.SubmitAsync(_store, id, claim.GetProperty("claim_id").GetString()!, 1,
            "needs_person", "could not tell", Evidence, "summary", "  check the nightly index job  ", null, Ct));

        Assert.Equal("reported", answer.GetProperty("status").GetString());
        Assert.Equal("pending", answer.GetProperty("proposed_note").GetString());
        Assert.Equal("pending|check the nightly index job|true", Scalar(
            "SELECT concat_ws('|', state, note_text, strpos(author, 'agent-a') > 0) " +
            $"FROM investigation_notes WHERE source_request_id = '{id}'"));
        /* root_cause_found is false only for needs_person. */
        Assert.Equal("false", Scalar($"SELECT root_cause_found::VARCHAR FROM investigation_reports WHERE request_id = '{id}'"));
    }

    /* The window and the journal say when they held more than the page: 101 requests in the window is truncated, 100 is
       not; 201 journal rows on the page's requests is journal_truncated, 200 is not. */
    [Fact]
    public async Task ReportsAndList_SayWhenTheWindowHeldMoreThanThePage()
    {
        var baseTime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        Exec($@"
INSERT INTO investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, opened_at, expires_at)
SELECT uuid(), 1, 7, 'srv-7', 'blocking', 'Blocking', TIMESTAMP '{baseTime}' - to_seconds(i), TIMESTAMP '{baseTime}' - to_seconds(i),
       TIMESTAMP '{baseTime}', TIMESTAMP '{baseTime}' - to_seconds(i), TIMESTAMP '{baseTime}' + INTERVAL 4 HOUR
FROM generate_series(1, {InvestigationToolCore.MaxLimit + 1}) AS t(i)");
        var newest = Scalar("SELECT request_id FROM investigation_requests ORDER BY opened_at DESC LIMIT 1");

        async Task<(JsonElement Reports, JsonElement List)> ReadAsync() => (
            Root(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, null, 24, Ct)),
            Root(await McpInvestigationTools.ListInvestigationRequests(_servers, _store, limit: InvestigationToolCore.MaxLimit, cancellationToken: Ct)));

        void JournalRows(int count, int firstId) => Exec(
            "INSERT INTO investigation_journal (journal_id, event_time, request_id, server_id, event, actor) " +
            $"SELECT {firstId} + i, TIMESTAMP '{baseTime}' - to_seconds(i), '{newest}', 7, 'opened', 'service' " +
            $"FROM generate_series(1, {count}) AS t(i)");

        var (reports, list) = await ReadAsync();
        Assert.Equal(InvestigationToolCore.MaxLimit, reports.GetProperty("requests_returned").GetInt32());
        Assert.Equal(InvestigationToolCore.MaxLimit, reports.GetProperty("requests").GetArrayLength());
        Assert.True(reports.GetProperty("truncated").GetBoolean());
        Assert.Equal(InvestigationToolCore.MaxLimit, list.GetProperty("requests_returned").GetInt32());
        Assert.Equal(InvestigationToolCore.MaxLimit, list.GetProperty("requests").GetArrayLength());
        Assert.True(list.GetProperty("truncated").GetBoolean());

        Exec("DELETE FROM investigation_requests WHERE request_id = (SELECT request_id FROM investigation_requests ORDER BY opened_at LIMIT 1)");
        JournalRows(InvestigationToolCore.JournalLimit, 1000000);
        (reports, list) = await ReadAsync();
        Assert.Equal(InvestigationToolCore.MaxLimit, reports.GetProperty("requests_returned").GetInt32());
        Assert.False(reports.GetProperty("truncated").GetBoolean());
        Assert.Equal(InvestigationToolCore.JournalLimit, reports.GetProperty("journal").GetArrayLength());
        Assert.False(reports.GetProperty("journal_truncated").GetBoolean());
        Assert.Equal(InvestigationToolCore.MaxLimit, list.GetProperty("requests_returned").GetInt32());
        Assert.False(list.GetProperty("truncated").GetBoolean());

        JournalRows(1, 2000000);
        (reports, _) = await ReadAsync();
        Assert.Equal(InvestigationToolCore.JournalLimit, reports.GetProperty("journal").GetArrayLength());
        Assert.True(reports.GetProperty("journal_truncated").GetBoolean());
    }

    /* ---- answer shapes: the refusals that never reach the store, the status words, the per-product message ---- */

    [Fact]
    public async Task Submit_AnInvalidReport_IsTheSharedRefusal_NamingTheFirstField_BeforeAnyStoreCall()
    {
        /* Neither id parses, so the tool never reaches the store: the null store is never touched. */
        var answer = await McpInvestigationTools.SubmitAsync(null!, "not-a-guid", "", 1, "real", "", Evidence, "", null, null, Ct);

        using var document = JsonDocument.Parse(answer);
        Assert.Equal("invalid", document.RootElement.GetProperty("status").GetString());
        Assert.Equal("request_id", document.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        var message = document.RootElement.GetProperty("message").GetString()!;
        foreach (var field in new[] { "request_id:", "claim_id:", "root_cause:", "summary:" })
            Assert.Contains(field, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Submit_OverACap_IsRefused_NamingTheFieldAndTheCap_BeforeAnyStoreCall()
    {
        var answer = await McpInvestigationTools.SubmitAsync(null!, "not-a-guid", "", 1, "real", new string('r', 8001), Evidence, "s", null, null, Ct);

        using var document = JsonDocument.Parse(answer);
        Assert.Equal("refused", document.RootElement.GetProperty("status").GetString());
        Assert.Equal("root_cause", document.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Equal(8000, document.RootElement.GetProperty("hints").GetProperty("cap").GetInt32());
        Assert.Contains("8001 characters, over 8000", document.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnsupportedFormats_NameLite_AndAreJournaled_WhileDarlingsMessageStillNamesDarling()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var id = openedId.ToString();

        var brief = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), id, "agent-a", null, 2, Ct));
        Assert.Equal("unsupported_format", brief.GetProperty("status").GetString());
        Assert.Equal("brief_format 2 is not supported. Lite composes brief_format 1.", brief.GetProperty("message").GetString());

        var claim = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), id, "agent-a", null, null, Ct));
        var report = Root(await McpInvestigationTools.SubmitAsync(_store, id, claim.GetProperty("claim_id").GetString()!, 2, "real", "cause", Evidence, "s", null, null, Ct));
        Assert.Equal("unsupported_format", report.GetProperty("status").GetString());
        Assert.Equal("report_format 2 is not supported. Lite accepts report_format 1. The brief you claimed is brief_format 1.",
            report.GetProperty("message").GetString());
        Assert.Equal("2", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{id}' AND reason LIKE '%:unsupported_format'"));

        Assert.Equal("report_format 0 is not supported. Darling accepts report_format 1. The brief you claimed is brief_format 1.",
            InvestigationFormats.UnsupportedReportFormatMessage(0));
    }

    [Fact]
    public async Task Claim_AnUnknownRequest_AndABadAgentId_AreRefusedWithTheirStatusWords()
    {
        var unknown = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), Guid.NewGuid().ToString(), "agent-a", null, null, Ct));
        Assert.Equal("not_found", unknown.GetProperty("status").GetString());

        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var bad = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "bad agent!", null, null, Ct));
        Assert.Equal("invalid", bad.GetProperty("status").GetString());
        Assert.Equal("agent_id", bad.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Equal("open|0", Scalar($"SELECT state || '|' || claim_count FROM investigation_requests WHERE request_id = '{openedId}'"));
    }

    [Fact]
    public async Task Reports_ByRequestId_ReturnTheBriefAndReport_AndAnUnknownIdIsNotFound()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var id = openedId.ToString();
        var claim = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), id, "agent-a", null, null, Ct));
        await McpInvestigationTools.SubmitAsync(_store, id, claim.GetProperty("claim_id").GetString()!, 1, "real", "a blocking chain", Evidence, "summary", null, null, Ct);

        var one = Root(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, id, 24, Ct));
        Assert.Equal("ok", one.GetProperty("status").GetString());
        Assert.Equal(1, one.GetProperty("requests_returned").GetInt32());

        var missing = Root(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, Guid.NewGuid().ToString(), 24, Ct));
        Assert.Equal("not_found", missing.GetProperty("status").GetString());

        var badWindow = Root(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, null, 0, Ct));
        Assert.Equal("invalid", badWindow.GetProperty("status").GetString());
    }

    /* The cross-app contract: Darling tool class is held to the same table by Darling.Tests InvestigationToolContractTests. */
    [Fact]
    public void LitesInvestigationTools_KeepTheSharedContractWithDarling() =>
        Assert.Empty(Darling.Tests.InvestigationToolContract.Differences(typeof(McpInvestigationTools)));

    [Fact]
    public void TheFourToolNames_AreTheOnesTheGateServes()
    {
        var declared = typeof(McpInvestigationTools).GetMethods()
            .Select(m => m.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false)
                .Cast<ModelContextProtocol.Server.McpServerToolAttribute>().SingleOrDefault()?.Name)
            .Where(n => n is not null)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(McpInvestigationPolicy.InvestigationTools.OrderBy(n => n, StringComparer.Ordinal), declared);
    }
}
