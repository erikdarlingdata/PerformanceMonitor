/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// A claim's later calls must come from the token that claimed it (#6542). With named agent tokens served, a second named token that
/// presents the first token's claim_id is refused on submit, renew and evidence, and the refusal is journaled under the second token's
/// name. The holder still can, and the legacy single token (no token name) behaves as before. A legacy claim whose agent_id is a served
/// named agent token's name is refused; any other name claims.
/// </summary>
public sealed class InvestigationClaimTokenLiveTests
{
    private const string Holder = "triage-bot";
    private const string Second = "other-bot";
    private const int ServerId = 7_654_301;

    private const string Evidence =
        "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"412 s\",\"basis\":\"verified\"}]";

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, InvestigationRequestStore Store, Guid RequestId,
        Guid ClaimId, CancellationToken Ct);

    private static async Task RunAsync(Func<Ctx, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the claim token tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "ClaimTokenSrv", ct);

        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(source, 30, () => DateTime.UtcNow);
        var alert = new InvestigationAlert(1, ServerId, "ClaimTokenSrv", "blocking", "Blocking", DateTime.UtcNow.AddSeconds(-5),
            new[] { "claim-token" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);
        var requestId = (await store.OpenOrJoinAsync(alert, ct)).RequestId!.Value;
        /* The holder claims as its named token does: its own name, reclaim allowed. */
        var claim = await store.ClaimAsync(requestId, Holder, allowReclaim: true, ct);
        await body(new Ctx(source, connection, store, requestId, claim.ClaimId!.Value, ct));
    }

    /// <summary>An open request on a second server, so it neither joins nor shares a claim with the first.</summary>
    private static async Task<Guid> OpenOtherAsync(Ctx c)
    {
        await DarlingMcpTestData.RegisterServerAsync(c.Connection, ServerId + 1, "ClaimTokenSrv2", c.Ct);
        var alert = new InvestigationAlert(1, ServerId + 1, "ClaimTokenSrv2", "blocking", "Blocking", DateTime.UtcNow.AddSeconds(-5),
            new[] { "claim-token-2" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);
        return (await c.Store.OpenOrJoinAsync(alert, c.Ct)).RequestId!.Value;
    }

    private static string StatusOf(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>How many refused rows carry <paramref name="reason"/> (as "operation:code"), per actor, as "holder|second|invalid".</summary>
    private static Task<string?> RefusedAsync(Ctx c, string reason) =>
        ScalarAsync(c.Connection,
            "SELECT count(*) FILTER (WHERE actor = 'agent:" + Holder + "')::text || '|' || count(*) FILTER (WHERE actor = 'agent:" + Second
            + "')::text || '|' || count(*) FILTER (WHERE actor = 'agent:invalid')::text "
            + $"FROM config.investigation_journal WHERE event = 'refused' AND reason = '{reason}'", c.Ct);

    private static Task<string> SubmitAsync(Ctx c, string? tokenName) =>
        DarlingMcpInvestigationTools.SubmitAsync(c.Store, c.RequestId.ToString(), c.ClaimId.ToString(), 1, "real", "a cause", Evidence,
            "a summary", null, null, c.Ct, tokenName: tokenName);

    [Fact]
    public async Task ASecondNamedToken_CannotSubmitOnTheFirstTokensClaim_AndTheRefusalJournalsTheSecond()
    {
        await RunAsync(async c =>
        {
            var refused = await SubmitAsync(c, Second);

            Assert.Equal(DarlingMcpInvestigationTools.ClaimNotToken, StatusOf(refused));
            Assert.Equal("0|1|0", await RefusedAsync(c, "submit:" + DarlingMcpInvestigationTools.ClaimNotToken));
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_reports", c.Ct));

            /* The holder still can. */
            Assert.Equal("reported", StatusOf(await SubmitAsync(c, Holder)));
            Assert.Equal("1", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_reports", c.Ct));
        });
    }

    [Fact]
    public async Task ANamedTokens_OtherSubmitRefusals_AreJournaledAsItself_NotAsUnclaimed()
    {
        await RunAsync(async c =>
        {
            /* A claim_id that holds nothing: the refusal used to be journaled as "unclaimed". */
            var answer = await DarlingMcpInvestigationTools.SubmitAsync(c.Store, c.RequestId.ToString(), Guid.NewGuid().ToString(), 99, "real",
                "a cause", Evidence, "a summary", null, null, c.Ct, tokenName: Second);

            Assert.Equal("unsupported_format", StatusOf(answer));
            Assert.Equal("0|1|0", await RefusedAsync(c, "submit:unsupported_format"));
        });
    }

    [Fact]
    public async Task TheHoldersOwnRefusals_StillJournalAsTheHolder()
    {
        await RunAsync(async c =>
        {
            var answer = await DarlingMcpInvestigationTools.SubmitAsync(c.Store, c.RequestId.ToString(), c.ClaimId.ToString(), 99, "real", "a cause",
                Evidence, "a summary", null, null, c.Ct, tokenName: Holder);

            Assert.Equal("unsupported_format", StatusOf(answer));
            Assert.Equal("1|0|0", await RefusedAsync(c, "submit:unsupported_format"));
        });
    }

    [Fact]
    public async Task OnTheLegacyToken_SubmitOnAnyClaimIdStillWorks()
    {
        await RunAsync(async c =>
        {
            Assert.Equal("reported", StatusOf(await SubmitAsync(c, tokenName: null)));
        });
    }

    [Fact]
    public async Task ASecondNamedToken_CannotRenewTheFirstTokensClaim_ButTheHolderCan()
    {
        await RunAsync(async c =>
        {
            /* The composer is never reached: a refusal and a renewal both answer before a brief is composed. */
            var refused = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, null!, c.RequestId.ToString(), Second, c.ClaimId.ToString(),
                null, c.Ct, Second);
            Assert.Equal(DarlingMcpInvestigationTools.ClaimNotToken, StatusOf(refused));
            Assert.Equal("0|1|0", await RefusedAsync(c, "claim:" + DarlingMcpInvestigationTools.ClaimNotToken));

            var renewed = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, null!, c.RequestId.ToString(), Holder, c.ClaimId.ToString(),
                null, c.Ct, Holder);
            Assert.Equal("renewed", StatusOf(renewed));
        });
    }

    [Fact]
    public async Task OnTheLegacyToken_ARenewalByClaimIdStillWorks()
    {
        await RunAsync(async c =>
        {
            var renewed = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, null!, c.RequestId.ToString(), "any-agent", c.ClaimId.ToString(),
                null, c.Ct);
            Assert.Equal("renewed", StatusOf(renewed));
        });
    }

    [Fact]
    public async Task ASecondNamedToken_CannotReadEvidenceOnTheFirstTokensClaim_ButTheHolderCan()
    {
        await RunAsync(async c =>
        {
            var refused = await DarlingMcpInvestigationTools.EvidenceAsync(c.Store, McpDataDetail.Full, c.RequestId.ToString(), null,
                c.ClaimId.ToString(), c.Ct, tokenName: Second);
            Assert.Equal(DarlingMcpInvestigationTools.ClaimNotToken, StatusOf(refused));
            Assert.Equal("0|1|0", await RefusedAsync(c, "evidence:" + DarlingMcpInvestigationTools.ClaimNotToken));

            Assert.NotEqual(DarlingMcpInvestigationTools.ClaimNotToken, StatusOf(await DarlingMcpInvestigationTools.EvidenceAsync(
                c.Store, McpDataDetail.Full, c.RequestId.ToString(), null, c.ClaimId.ToString(), c.Ct, tokenName: Holder)));
            Assert.NotEqual("already_claimed", StatusOf(await DarlingMcpInvestigationTools.EvidenceAsync(
                c.Store, McpDataDetail.Full, c.RequestId.ToString(), null, c.ClaimId.ToString(), c.Ct, tokenName: Holder)));

            /* The legacy token (no name) keeps today's rule: the claim_id opens the read. */
            Assert.NotEqual("already_claimed", StatusOf(await DarlingMcpInvestigationTools.EvidenceAsync(
                c.Store, McpDataDetail.Full, c.RequestId.ToString(), null, c.ClaimId.ToString(), c.Ct)));
        });
    }

    [Fact]
    public async Task ALegacyClaim_UnderAServedNamedTokensName_IsRefused_AndOtherNamesClaim()
    {
        await RunAsync(async c =>
        {
            /* A second, unclaimed request: the legacy token claims on it. */
            var other = await OpenOtherAsync(c);
            var named = new HashSet<string>(StringComparer.Ordinal) { Holder, Second };
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(c.Source), new McpDataDetailSetting(McpDataDetail.Full),
                DarlingInvestigationBriefTools.Tools);

            var refused = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, composer, other.ToString(), Second, null, null, c.Ct,
                tokenName: null, namedAgentNames: named);
            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, StatusOf(refused));
            Assert.Contains("different agent_id", refused, StringComparison.Ordinal);
            Assert.Equal("", await ScalarAsync(c.Connection, $"SELECT coalesce(claimed_by, '') FROM config.investigation_requests WHERE request_id = '{other}'", c.Ct));
            Assert.Equal("0|0|1", await RefusedAsync(c, "claim:" + DarlingMcpInvestigationTools.AgentIdIsNamedToken));

            var claimed = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, composer, other.ToString(), "legacy-agent", null, null, c.Ct,
                tokenName: null, namedAgentNames: named);
            Assert.Equal("claimed", StatusOf(claimed));
        });
    }

    private static readonly HashSet<string> BothNames = new(StringComparer.Ordinal) { Holder, Second };

    [Fact]
    public async Task TheLegacyToken_CannotRenewSubmitOrReadEvidence_OnAServedNamedTokensClaim_AndTheRowsAreNotTheNamedAgents()
    {
        await RunAsync(async c =>
        {
            /* The claim was made by the named token (or before the upgrade); the legacy token holds its claim_id. */
            var renew = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, null!, c.RequestId.ToString(), "legacy-agent", c.ClaimId.ToString(),
                null, c.Ct, tokenName: null, namedAgentNames: BothNames);
            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, StatusOf(renew));

            var submit = await DarlingMcpInvestigationTools.SubmitAsync(c.Store, c.RequestId.ToString(), c.ClaimId.ToString(), 1, "real", "a cause",
                Evidence, "a summary", null, null, c.Ct, tokenName: null, namedAgentNames: BothNames);
            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, StatusOf(submit));
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_reports", c.Ct));

            var evidence = await DarlingMcpInvestigationTools.EvidenceAsync(c.Store, McpDataDetail.Full, c.RequestId.ToString(), null,
                c.ClaimId.ToString(), c.Ct, tokenName: null, namedAgentNames: BothNames);
            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, StatusOf(evidence));

            /* Each refusal is journaled as the placeholder, never as the named agent whose claim it was. */
            Assert.Equal("0|0|1", await RefusedAsync(c, "claim:" + DarlingMcpInvestigationTools.AgentIdIsNamedToken));
            Assert.Equal("0|0|1", await RefusedAsync(c, "submit:" + DarlingMcpInvestigationTools.AgentIdIsNamedToken));
            Assert.Equal("0|0|1", await RefusedAsync(c, "evidence:" + DarlingMcpInvestigationTools.AgentIdIsNamedToken));
        });
    }

    [Fact]
    public async Task TheNamedToken_CanTakeTheClaimOver_ByReclaimingUnderItsOwnName()
    {
        await RunAsync(async c =>
        {
            var reclaimed = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, null!, c.RequestId.ToString(), Holder, null, null, c.Ct,
                tokenName: Holder, namedAgentNames: BothNames);
            Assert.Equal("renewed", StatusOf(reclaimed));
            Assert.Contains(c.ClaimId.ToString(), reclaimed, StringComparison.Ordinal);

            Assert.Equal("reported", StatusOf(await DarlingMcpInvestigationTools.SubmitAsync(c.Store, c.RequestId.ToString(), c.ClaimId.ToString(), 1,
                "real", "a cause", Evidence, "a summary", null, null, c.Ct, tokenName: Holder, namedAgentNames: BothNames)));
        });
    }

    [Fact]
    public async Task AStoreLevelSubmitRefusal_IsJournaledAsTheCaller_NotAsUnclaimed()
    {
        await RunAsync(async c =>
        {
            /* A claim_id nothing holds: the store answers lease_lost, and the row names the token that asked. */
            var lost = await DarlingMcpInvestigationTools.SubmitAsync(c.Store, c.RequestId.ToString(), Guid.NewGuid().ToString(), 1, "real",
                "a cause", Evidence, "a summary", null, null, c.Ct, tokenName: Second);
            Assert.Equal("lease_lost", StatusOf(lost));
            Assert.Equal("0|1|0", await RefusedAsync(c, "submit:lease_lost"));
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE actor = 'agent:unclaimed'", c.Ct));
        });
    }

    [Fact]
    public async Task ALegacyClaim_WithNoNamedTokensServed_AcceptsAnyValidName()
    {
        await RunAsync(async c =>
        {
            var other = await OpenOtherAsync(c);
            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(c.Source), new McpDataDetailSetting(McpDataDetail.Full),
                DarlingInvestigationBriefTools.Tools);

            var claimed = await DarlingMcpInvestigationTools.ClaimAsync(c.Store, composer, other.ToString(), Second, null, null, c.Ct);
            Assert.Equal("claimed", StatusOf(claimed));
        });
    }
}
