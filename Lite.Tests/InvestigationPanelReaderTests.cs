/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The investigations panel's read side (#5904) on a throwaway DuckDB: the list comes in the MCP list order
/// (<c>opened_at DESC, request_id</c>) with the alert count each request carries, the operator's panel shows the report and
/// brief in full (mcp_data_detail masks agents over MCP only), and a request's reports and journal render under it, row for row.
/// </summary>
public sealed class InvestigationPanelReaderTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);
    private const string Literal = "Smith-Secret-4471";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly DuckDbInvestigationStore _store;
    private DateTime _now = T0;

    public InvestigationPanelReaderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "InvestigationPanelReaderTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _db = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _db.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbInvestigationStore(_db, () => _now);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best effort */ }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private InvestigationPanelReader Reader() => new(_store, () => _now);

    private static InvestigationAlert Alert(DateTime at, int serverId) =>
        new(1, serverId, "srv-" + serverId, "blocking", "Blocking", at, Array.Empty<string>(), null,
            MaxPerHour: 50, MaxPerDay: 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    private async Task<Guid> OpenAsync(int serverId) => (await _store.OpenOrJoinAsync(Alert(_now, serverId), Ct)).RequestId!.Value;

    [Fact]
    public async Task TheListIsInTheMcpListOrderNewestOpenedFirstThenRequestId()
    {
        var oldest = await OpenAsync(1);
        _now = T0.AddMinutes(10);
        var tieA = await OpenAsync(2);
        var tieB = await OpenAsync(3);
        _now = T0.AddMinutes(20);
        var newest = await OpenAsync(4);

        var (requests, truncated) = await Reader().ListAsync(null, null, Ct);

        Assert.False(truncated);
        var ties = new[] { tieA, tieB }.OrderBy(g => g.ToString("D"), StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { newest, ties[0], ties[1], oldest }, requests.Select(r => r.RequestId).ToArray());
        var mcp = await _store.ListAsync(null, null, null, 100, Ct);
        Assert.Equal(mcp.Select(r => r.RequestId), requests.Select(r => r.RequestId));
    }

    [Fact]
    public async Task TheListCarriesTheAlertCountOfEachRequest()
    {
        var joined = await OpenAsync(1);
        _now = T0.AddMinutes(1);
        Assert.Equal(joined, await OpenAsync(1));
        var single = await OpenAsync(2);

        var (requests, _) = await Reader().ListAsync(null, null, Ct);

        Assert.Equal(2, requests.Single(r => r.RequestId == joined).AlertCount);
        Assert.Equal(1, requests.Single(r => r.RequestId == single).AlertCount);
    }

    [Fact]
    public async Task TheListFiltersByStateAndFamily()
    {
        var open = await OpenAsync(1);
        var claimedId = await OpenAsync(2);
        await _store.ClaimAsync(claimedId, "agent-a", Ct);

        var (claimedOnly, _) = await Reader().ListAsync("claimed", null, Ct);
        Assert.Equal(new[] { claimedId }, claimedOnly.Select(r => r.RequestId).ToArray());
        Assert.Equal("agent-a", claimedOnly[0].Agent);
        var (none, _) = await Reader().ListAsync(null, "jobs", Ct);
        Assert.Empty(none);
        Assert.Contains(open, (await Reader().ListAsync("", "", Ct)).Requests.Select(r => r.RequestId));
    }

    [Fact]
    public async Task ThePanelShowsSummaryVerdictRootCauseAndBriefInFull_WhileTheMcpHostRunsAtMetadataOnly()
    {
        var id = await OpenAsync(1);
        _now = T0.AddMinutes(2);
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        var brief = "{\"brief_format\":1,\"data_detail\":\"full\",\"findings\":[{\"story\":\"blocked by login " + Literal + "\"}]}";
        Assert.True(await _store.SetBriefAsync(id, claim.ClaimId!.Value, 1, brief, Ct));
        _now = T0.AddMinutes(5);
        await _store.SubmitAsync(new InvestigationReportSubmission(
            id, claim.ClaimId!.Value, "agent-a", 1, "real", "root cause " + Literal, true, "summary for " + Literal, "[]"), Ct);

        /* What the MCP host serves an agent at metadataOnly: the report is withheld, so the agent half stays masked. */
        var read = await _store.ReadReportsAsync(id, null, _now, 1, InvestigationToolCore.JournalLimit, Ct);
        var agentAnswer = InvestigationToolCore.ReportsAnswer(read, null, McpDataDetail.MetadataOnly);
        Assert.DoesNotContain(Literal, agentAnswer, StringComparison.Ordinal);

        /* The panel has no data-detail input at all: the operator's own window reads the same request in full. */
        var detail = await Reader().ReadDetailAsync(id, Ct);
        var report = Assert.Single(detail!.Reports);
        Assert.Equal("real", report.Verdict);
        Assert.Equal("root cause " + Literal, report.RootCause);
        Assert.Equal("summary for " + Literal, report.Summary);
        Assert.Contains(Literal, detail.Brief, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReportAndItsJournalComeBackUnderTheRequestWithTheReportTextInFull()
    {
        var id = await OpenAsync(1);
        _now = T0.AddMinutes(2);
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        _now = T0.AddMinutes(5);
        await _store.SubmitAsync(new InvestigationReportSubmission(
            id, claim.ClaimId!.Value, "agent-a", 1, "real", "root cause " + Literal, true, "summary for " + Literal,
            "[{\"tool\":\"get_blocking\",\"key_numbers\":\"" + Literal + "\"}]"), Ct);

        var detail = await Reader().ReadDetailAsync(id, Ct);

        Assert.NotNull(detail);
        Assert.Equal("reported", detail!.Request.State);
        var report = Assert.Single(detail.Reports);
        Assert.Equal(("agent-a", "real", true), (report.AgentId, report.Verdict, report.RootCauseFound));
        Assert.Contains(Literal, report.Summary + report.RootCause + report.Evidence, StringComparison.Ordinal);
        Assert.Equal(new[] { "reported", "claimed", "opened" }, detail.Journal.Select(j => j.Event).ToArray());
        Assert.Equal(InvestigationPanelReader.LocalText(T0.AddMinutes(2)), detail.Claimed);

        Assert.Null(await Reader().ReadDetailAsync(Guid.NewGuid(), Ct));
    }

    [Fact]
    public void ARequestWithTwoReportsAndThreeJournalRowsRendersAllUnderIt()
    {
        var id = Guid.NewGuid();
        var request = new InvestigationRequestRow(id, 1, 7, "srv-7", "blocking", "Blocking", T0, T0, 1, "reported",
            T0, T0.AddHours(4), "agent-b", null, 2, T0.AddMinutes(30));
        var reports = new[]
        {
            new InvestigationReportRow(id, 1, "agent-b", T0.AddMinutes(30), "real", "second root cause", true, "second summary", "[]"),
            new InvestigationReportRow(id, 1, "agent-a", T0.AddMinutes(10), "needs_person", "first root cause", false, "first summary", "[{\"tool\":\"x\"}]"),
        };
        var journal = new[]
        {
            new InvestigationJournalRow(T0.AddMinutes(30), id, 7, "reported", "agent:agent-b", null, null),
            new InvestigationJournalRow(T0.AddMinutes(20), id, 7, "claimed", "agent:agent-b", null, null),
            new InvestigationJournalRow(T0.AddMinutes(10), id, 7, "claimed", "agent:agent-a", "lease_expired", null),
        };

        var detail = InvestigationPanelReader.Detail(
            new InvestigationReportsRead(new[] { request }, reports, journal, null), McpDataDetail.Full);

        Assert.Equal(new[] { "second summary", "first summary" }, detail.Reports.Select(r => r.Summary).ToArray());
        Assert.Equal(new[] { "agent-b", "agent-a" }, detail.Reports.Select(r => r.AgentId).ToArray());
        Assert.Equal(3, detail.Journal.Count);
        Assert.Equal(new[] { "reported", "claimed", "claimed" }, detail.Journal.Select(j => j.Event).ToArray());
        Assert.Equal("lease_expired", detail.Journal[2].Reason);
        Assert.Equal(InvestigationPanelReader.LocalText(T0.AddMinutes(20)), detail.Claimed);
        Assert.Null(detail.Brief);
    }
}
