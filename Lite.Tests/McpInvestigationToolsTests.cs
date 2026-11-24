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
[Collection("app-logger-statics")]
public sealed class McpInvestigationToolsTests : IDisposable
{
    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\",\"basis\":\"verified\"}]";

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
        _store = new DuckDbInvestigationStore(_db, lockBudget: DuckDbInvestigationStoreTests.TestLockBudget);
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

    /// <summary>The real store, except that a test can make the brief write, the release or the refusal write throw, or cancel a token
    /// when the brief write starts (#9511: a lock timeout on any of them is the field failure).</summary>
    private sealed class FaultingStore(DuckDbInvestigationStore inner) : IInvestigationRequestStore
    {
        public Exception? SetBriefThrows { get; set; }
        public bool SetBriefReturnsFalse { get; set; }
        public Exception? ReleaseThrows { get; set; }
        public Exception? RefusalThrows { get; set; }
        public CancellationTokenSource? CancelOnSetBrief { get; set; }
        public int Releases { get; private set; }

        public Task<bool> SetBriefAsync(Guid requestId, Guid claimId, short briefFormat, string briefJson, CancellationToken cancellationToken = default)
        {
            CancelOnSetBrief?.Cancel();
            if (SetBriefThrows is not null) throw SetBriefThrows;
            if (SetBriefReturnsFalse) return Task.FromResult(false);
            cancellationToken.ThrowIfCancellationRequested();
            return inner.SetBriefAsync(requestId, claimId, briefFormat, briefJson, cancellationToken);
        }

        public Task<bool> ReleaseAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default)
        {
            Releases++;
            if (ReleaseThrows is not null) throw ReleaseThrows;
            return inner.ReleaseAsync(requestId, claimId, cancellationToken);
        }

        public Task RecordRefusalAsync(Guid requestId, string agentId, string operation, string reason, string? detailJson, CancellationToken cancellationToken = default)
        {
            if (RefusalThrows is not null) throw RefusalThrows;
            return inner.RecordRefusalAsync(requestId, agentId, operation, reason, detailJson, cancellationToken);
        }

        public Task<InvestigationOpenResult> OpenOrJoinAsync(InvestigationAlert alert, CancellationToken cancellationToken = default) => inner.OpenOrJoinAsync(alert, cancellationToken);
        public Task<InvestigationClaimResult> ClaimAsync(Guid requestId, string agentId, CancellationToken cancellationToken = default) => inner.ClaimAsync(requestId, agentId, cancellationToken);
        public Task<InvestigationSubmitOutcome> SubmitAsync(InvestigationReportSubmission report, CancellationToken cancellationToken = default) => inner.SubmitAsync(report, cancellationToken);
        public Task<InvestigationRenewResult> RenewAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default) => inner.RenewAsync(requestId, claimId, cancellationToken);
        public Task<string?> ClaimHolderAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default) => inner.ClaimHolderAsync(requestId, claimId, cancellationToken);
        public Task<IReadOnlyDictionary<int, (int LastHour, int LastDay)>> RouteUseAsync(CancellationToken cancellationToken = default) => inner.RouteUseAsync(cancellationToken);
        public Task<bool> RequestExistsAsync(Guid requestId, CancellationToken cancellationToken = default) => inner.RequestExistsAsync(requestId, cancellationToken);
        public Task<(InvestigationRequestRow Row, string? Brief)?> ReadRequestAsync(Guid requestId, CancellationToken cancellationToken = default) => inner.ReadRequestAsync(requestId, cancellationToken);
        public Task<InvestigationReportsRead> ReadReportsAsync(Guid? requestId, int? serverId, DateTime sinceUtc, int limit, int journalLimit, CancellationToken cancellationToken = default) => inner.ReadReportsAsync(requestId, serverId, sinceUtc, limit, journalLimit, cancellationToken);
        public Task<IReadOnlyList<InvestigationRequestRow>> ListAsync(string? state, int? serverId, string? family, int limit, CancellationToken cancellationToken = default) => inner.ListAsync(state, serverId, family, limit, cancellationToken);
        public Task<IReadOnlyList<InvestigationOpenedRow>> OpenedSinceAsync(DateTime sinceUtc, IReadOnlyCollection<int>? routeIds, int limit, CancellationToken cancellationToken = default) => inner.OpenedSinceAsync(sinceUtc, routeIds, limit, cancellationToken);
        public Task<InvestigationSweepResult> SweepAsync(CancellationToken cancellationToken = default) => inner.SweepAsync(cancellationToken);
    }

    private static List<string> ErrorLines(Guid requestId) =>
        AppLogger.DrainBufferedLines()
            .Where(line => line.Contains("ERROR", StringComparison.Ordinal) && line.Contains(requestId.ToString(), StringComparison.Ordinal))
            .ToList();

    private static StoreLockTimeoutException LockTimeout() => new(TimeSpan.FromSeconds(5), [], "test");

    /// <summary>#9511: the brief write times out on the store lock after the claim row is written. The agent gets an error with no
    /// claim_id, so the claim is released and journaled, or a re-claim would answer already_claimed for the whole lease.</summary>
    [Fact]
    public async Task BriefWriteFailure_ReleasesTheClaim_JournalsAnswerFailed_LogsTheStep_AndAReclaimSucceeds()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var faulting = new FaultingStore(_store) { SetBriefThrows = LockTimeout() };
        AppLogger.DrainBufferedLines();

        var failed = Root(await McpInvestigationTools.ClaimAsync(faulting, Composer(), openedId.ToString(), "agent-a", null, null, Ct));

        Assert.Equal("error", failed.GetProperty("status").GetString());
        Assert.Contains("released", failed.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("open", Scalar($"SELECT state FROM investigation_requests WHERE request_id = '{openedId}'"));
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{openedId}' AND reason = 'claim:answer_failed'"));
        Assert.Single(ErrorLines(openedId), l => l.Contains("step store_brief", StringComparison.Ordinal));

        var again = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "agent-a", null, null, Ct));
        Assert.Equal("claimed", again.GetProperty("status").GetString());
    }

    /// <summary>#9511: the store_brief branch, where the write returns false because the claim no longer holds the request. The claim is
    /// released and journaled as brief_failed, and the step is logged.</summary>
    [Fact]
    public async Task BriefNotStored_ReleasesTheClaim_JournalsBriefFailed_LogsTheStep_AndAReclaimSucceeds()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var faulting = new FaultingStore(_store) { SetBriefReturnsFalse = true };
        AppLogger.DrainBufferedLines();

        var failed = Root(await McpInvestigationTools.ClaimAsync(faulting, Composer(), openedId.ToString(), "agent-a", null, null, Ct));

        Assert.Equal("error", failed.GetProperty("status").GetString());
        Assert.Contains("released", failed.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("open", Scalar($"SELECT state FROM investigation_requests WHERE request_id = '{openedId}'"));
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{openedId}' AND reason = 'claim:brief_failed'"));
        Assert.Single(ErrorLines(openedId), l => l.Contains("step store_brief", StringComparison.Ordinal));

        var again = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "agent-a", null, null, Ct));
        Assert.Equal("claimed", again.GetProperty("status").GetString());
    }

    [Fact]
    public async Task AnswerBuildFailure_AfterTheBriefIsStored_ReleasesTheClaim_JournalsAnswerFailed_AndAReclaimSucceeds()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        AppLogger.DrainBufferedLines();

        var failed = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "agent-a", null, null, Ct,
            parseBrief: _ => throw new InvalidOperationException("answer build failed")));

        Assert.Equal("error", failed.GetProperty("status").GetString());
        Assert.Contains("released", failed.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("open", Scalar($"SELECT state FROM investigation_requests WHERE request_id = '{openedId}'"));
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{openedId}' AND reason = 'claim:answer_failed'"));
        Assert.Single(ErrorLines(openedId), l => l.Contains("step build_answer", StringComparison.Ordinal));

        var again = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "agent-a", null, null, Ct));
        Assert.Equal("claimed", again.GetProperty("status").GetString());
    }

    [Fact]
    public async Task ACancelAfterTheClaim_ReleasesIt_AndIsNotLoggedAsAnError()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var faulting = new FaultingStore(_store) { CancelOnSetBrief = cts };
        AppLogger.DrainBufferedLines();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            McpInvestigationTools.ClaimAsync(faulting, Composer(), openedId.ToString(), "agent-a", null, null, cts.Token));

        Assert.Equal("open", Scalar($"SELECT state FROM investigation_requests WHERE request_id = '{openedId}'"));
        Assert.Empty(ErrorLines(openedId));
        Assert.Equal("0", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{openedId}' AND reason LIKE 'claim:%failed'"));

        var again = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), "agent-a", null, null, Ct));
        Assert.Equal("claimed", again.GetProperty("status").GetString());
    }

    /// <summary>The release takes the same write lock and can time out the same way. The agent still sees the first error, and both are logged.</summary>
    [Fact]
    public async Task AFailedRelease_AfterABriefWriteFailure_KeepsTheFirstError_AndLogsBoth()
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;
        var faulting = new FaultingStore(_store)
        {
            SetBriefThrows = new InvalidOperationException("brief write failed first"),
            ReleaseThrows = new InvalidOperationException("release failed second"),
        };
        AppLogger.DrainBufferedLines();

        var failed = Root(await McpInvestigationTools.ClaimAsync(faulting, Composer(), openedId.ToString(), "agent-a", null, null, Ct));

        Assert.Equal("error", failed.GetProperty("status").GetString());
        var message = failed.GetProperty("message").GetString()!;
        Assert.Contains("brief write failed first", message, StringComparison.Ordinal);
        Assert.DoesNotContain("release failed second", message, StringComparison.Ordinal);
        Assert.DoesNotContain("the claim was released", message, StringComparison.Ordinal);
        Assert.Equal(1, faulting.Releases);

        var lines = ErrorLines(openedId);
        Assert.Contains(lines, l => l.Contains("brief write failed first", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("release failed second", StringComparison.Ordinal));
        /* The refusal is still journaled after the failed release. */
        Assert.Equal("1", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{openedId}' AND reason = 'claim:answer_failed'"));
    }

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

    /// <summary>#8611: Lite journals its own refusals under invalid and unclaimed (and Darling its no-claim writes under legacy-token), so a claim as any of the three would make the claim's rows read
    /// as a refusal's. Darling refuses both (case-insensitively); so does Lite, and no claim is made.</summary>
    [Theory]
    [InlineData("invalid")]
    [InlineData("unclaimed")]
    [InlineData("Invalid")]
    [InlineData("UNCLAIMED")]
    [InlineData("legacy-token")]
    [InlineData("Legacy-Token")]
    public async Task Claim_AsAReservedAgentName_IsRefused_AndMakesNoClaim(string agentId)
    {
        var openedId = (await _store.OpenOrJoinAsync(Alert(), Ct)).RequestId!.Value;

        var answer = Root(await McpInvestigationTools.ClaimAsync(_store, Composer(), openedId.ToString(), agentId, null, null, Ct));

        Assert.NotEqual("claimed", answer.GetProperty("status").GetString());
        Assert.Equal("agent_id_is_named_token", answer.GetProperty("status").GetString());
        Assert.Equal("", Scalar($"SELECT coalesce(CAST(claim_id AS VARCHAR), '') FROM investigation_requests WHERE request_id = '{openedId}'"));
    }

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

    /* #8983: the report read keeps the newest requests that fit the response budget, each whole with its report and journal rows,
       and says how many it left out; a window that fits is the same reply as the read without a budget. */
    [Fact]
    public async Task GetInvestigationReports_OverTheBudget_KeepsTheNewestRequestsWhole_AndSaysWhatItLeftOut()
    {
        const int total = 45;
        var composer = Composer();
        var ids = new List<Guid>();
        for (var i = 0; i < total; i++)
        {
            var id = (await _store.OpenOrJoinAsync(
                new InvestigationAlert(1, 100 + i, "srv-" + i.ToString(CultureInfo.InvariantCulture), "blocking", "Blocking", _now,
                    new[] { "key-" + i.ToString(CultureInfo.InvariantCulture) }, null, 1000, 10000, LeaseMinutes: 15, ExpiryMinutes: 240), Ct)).RequestId!.Value;
            Exec($"UPDATE investigation_requests SET opened_at = TIMESTAMP '{DateTime.UtcNow.AddMinutes(i - total).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}' WHERE request_id = '{id}'");
            var claim = Root(await McpInvestigationTools.ClaimAsync(_store, composer, id.ToString(), "agent-a", null, null, Ct));
            var submitted = Root(await McpInvestigationTools.SubmitAsync(_store, id.ToString(), claim.GetProperty("claim_id").GetString()!, 1,
                "real", $"case-{i:D3} " + new string('x', 1500), Evidence, "summary", null, null, Ct));
            Assert.Equal("reported", submitted.GetProperty("status").GetString());
            ids.Add(id);
        }

        var json = await McpInvestigationTools.GetInvestigationReports(_servers, _store, composer, null, null, 24, Ct);
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        var root = Root(json);
        var kept = root.GetProperty("requests_returned").GetInt32();
        Assert.InRange(kept, 1, total - 1);
        var newestFirst = ids.AsEnumerable().Reverse().Take(kept).ToList();
        Assert.Equal(newestFirst, root.GetProperty("requests").EnumerateArray().Select(r => r.GetProperty("request_id").GetGuid()).ToList());
        Assert.Equal(newestFirst.OrderBy(g => g).ToList(), root.GetProperty("reports").EnumerateArray().Select(r => r.GetProperty("request_id").GetGuid()).OrderBy(g => g).ToList());
        var journal = root.GetProperty("journal").EnumerateArray().ToList();
        foreach (var id in newestFirst)
            Assert.Equal(Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{id}'"),
                journal.Count(j => j.GetProperty("request_id").GetString() == id.ToString()).ToString(CultureInfo.InvariantCulture));
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        var note = root.GetProperty("budget_note").GetString()!;
        Assert.Contains($"{total - kept:#,0} were left out", note, StringComparison.Ordinal);
        Assert.Contains("server_name or request_id, or a smaller hours_back", note, StringComparison.Ordinal);

        /* A window that fits is the same reply as the read without a budget, with no budget fields. */
        var small = await McpInvestigationTools.GetInvestigationReports(_servers, _store, composer, null, ids[total - 1].ToString(), 24, Ct);
        Assert.Equal(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, ids[total - 1].ToString(), 24, Ct), small);
        Assert.False(Root(small).TryGetProperty("budget_note", out _));
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

    /// <summary>#8461: of the four tools the class declares, the gate serves the two that change investigation state, and the two
    /// reads are on the read-only list and served in every configuration, as Darling serves them.</summary>
    [Fact]
    public void TheDeclaredStateChangingTools_AreTheOnesTheGateServes_AndTheDeclaredReads_AreOnTheReadOnlyList()
    {
        var declared = typeof(McpInvestigationTools).GetMethods()
            .Select(m => m.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false)
                .Cast<ModelContextProtocol.Server.McpServerToolAttribute>().SingleOrDefault()?.Name)
            .Where(n => n is not null)
            .Select(n => n!)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        var reads = declared.Where(McpToolClassification.ReadOnlyTools.Contains).ToArray();
        var stateChanging = declared.Except(reads).ToArray();

        Assert.Equal(new[] { "get_investigation_reports", "list_investigation_requests" }, reads);
        Assert.Equal(new[] { "claim_investigation_request", "submit_investigation_report" }, stateChanging);
        Assert.Equal(stateChanging, McpInvestigationPolicy.InvestigationTools.OrderBy(n => n, StringComparer.Ordinal));
    }
}
