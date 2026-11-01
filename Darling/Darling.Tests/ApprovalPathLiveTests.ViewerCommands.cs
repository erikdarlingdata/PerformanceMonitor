/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The Viewer's proposal commands (#7109): the list, the first press and the confirm, run through the command plane's own code on a
/// single-person route, and refused on a two-person route (the Viewer is an administrator path with no person).</summary>
public sealed partial class ApprovalPathLiveTests
{
    private static ClaimedCommand Command(string type, string? args) => new(1, type, null, args, "alice");

    [Fact]
    public async Task TheViewerCommands_ListFirstPressAndConfirm_OnASinglePersonRoute_EndTheBackend()
    {
        await RunAsync(singlePerson: true, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var args = $"{{\"proposal_id\":\"{proposalId:D}\"}}";
            foreach (var type in new[] { ProposalCommands.ReadType, ProposalCommands.FirstPressType, ProposalCommands.ConfirmType })
                Assert.Equal(CommandKind.ProposalCommand, DarlingCommandExecutor.ResolvePlan(Command(type, type == ProposalCommands.ReadType ? null : args)).Kind);

            var list = await ProposalCommands.ReadAsync(c.Source, NullLogger.Instance, null, null, McpDataDetail.Full, "https://dash.example", c.Ct);
            var row = Assert.Single(JsonNode.Parse(list.ResultJson!)!["proposals"]!.AsArray())!;
            Assert.Equal(proposalId.ToString("D"), (string?)row["proposal_id"]);
            Assert.Equal("kill a session", (string?)row["kind"]);
            Assert.Equal("pending", (string?)row["state"]);
            Assert.Equal("the blocker holds a transaction open", (string?)row["reason"]);
            Assert.True((bool)row["can_approve_here"]!);
            Assert.False((bool)row["nobody_can_approve"]!);

            /* The server filter is part of the store query: another server's id lists nothing, this one's lists the proposal. */
            var serverId = (int)row["server_id"]!;
            var otherServer = await ProposalCommands.ReadAsync(c.Source, NullLogger.Instance, null, serverId + 1, McpDataDetail.Full, null, c.Ct);
            Assert.Empty(JsonNode.Parse(otherServer.ResultJson!)!["proposals"]!.AsArray());
            var sameServer = await ProposalCommands.ReadAsync(c.Source, NullLogger.Instance, null, serverId, McpDataDetail.Full, null, c.Ct);
            Assert.Single(JsonNode.Parse(sameServer.ResultJson!)!["proposals"]!.AsArray());

            /* The agent's reason is free text under the same masker the web page uses, at each mcp.dataDetail level. */
            foreach (var mode in new[] { McpDataDetail.Full, McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly })
            {
                var masked = await ProposalCommands.ReadAsync(c.Source, NullLogger.Instance, null, null, mode, null, c.Ct);
                var maskedRow = Assert.Single(JsonNode.Parse(masked.ResultJson!)!["proposals"]!.AsArray())!;
                Assert.Equal(McpDataDetailMasker.ForAnswer(mode).FreeText("the blocker holds a transaction open"), (string?)maskedRow["reason"]);
            }

            var first = await ProposalCommands.PressAsync(c.Source, NullLogger.Instance, ProposalCommands.FirstPressType, proposalId, McpDataDetail.Full, c.Ct);
            Assert.True(first.Success);
            Assert.Equal("first_pressed", (string?)JsonNode.Parse(first.ResultJson!)!["status"]);
            Assert.Equal(pid, (int?)JsonNode.Parse(first.ResultJson!)!["facts"]!["pid"]);

            var confirm = await ProposalCommands.PressAsync(c.Source, NullLogger.Instance, ProposalCommands.ConfirmType, proposalId, McpDataDetail.Full, c.Ct);
            Assert.True(confirm.Success);
            Assert.Equal("run", (string?)JsonNode.Parse(confirm.ResultJson!)!["status"]);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal("run", await c.StateAsync(proposalId));
        });
    }

    [Fact]
    public async Task TheViewerCommands_OnATwoPersonRoute_ListTheWebLinkAndTheSentence_AndRefuseBothPresses()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;

            var list = await ProposalCommands.ReadAsync(c.Source, NullLogger.Instance, null, null, McpDataDetail.Full, "https://dash.example", c.Ct);
            var row = Assert.Single(JsonNode.Parse(list.ResultJson!)!["proposals"]!.AsArray())!;
            Assert.False((bool)row["can_approve_here"]!);
            Assert.True((bool)row["nobody_can_approve"]!);
            Assert.Equal($"https://dash.example/#/proposals?proposal={proposalId:D}", (string?)row["link"]);

            foreach (var type in new[] { ProposalCommands.FirstPressType, ProposalCommands.ConfirmType })
            {
                var outcome = await ProposalCommands.PressAsync(c.Source, NullLogger.Instance, type, proposalId, McpDataDetail.Full, c.Ct);
                Assert.False(outcome.Success);
                Assert.Equal(InvestigationRequestStore.ProposalPressCodes.AdministratorNeedsSinglePerson, outcome.ResultStatus);
            }

            Assert.True(await c.AliveAsync(pid, polls: 5));
            Assert.Equal("pending", await c.StateAsync(proposalId));
        });
    }

    [Fact]
    public void AProposalCommandWithBadArgs_IsFailedBeforeItRuns()
    {
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command(ProposalCommands.FirstPressType, "{}")).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command(ProposalCommands.ConfirmType, "{\"proposal_id\":\"nope\"}")).Kind);
    }

    [Fact]
    public void EveryStatementClassValueInAProposalsTarget_FollowsMcpDataDetail()
    {
        static JsonNode Target() => JsonNode.Parse("{\"statement\":\"SELECT 'alice'\",\"module_or_batch\":\"SELECT 'alice'\",\"hints\":\"OPTION (OPTIMIZE FOR (@a = 'alice'))\",\"definition\":\"SELECT 'alice'\",\"plan_id\":5}")!;
        string[] statementKeys = ["statement", "module_or_batch", "hints", "definition"];

        var full = ProposalCommands.MaskedJson(Target(), McpDataDetailMasker.ForAnswer(McpDataDetail.Full))!.AsObject();
        foreach (var key in statementKeys)
            Assert.Contains("alice", (string?)full[key], StringComparison.Ordinal);

        /* maskLiterals: every statement-class value is still there but its literal is replaced. */
        var literals = ProposalCommands.MaskedJson(Target(), McpDataDetailMasker.ForAnswer(McpDataDetail.MaskLiterals))!.AsObject();
        foreach (var key in statementKeys)
        {
            Assert.True(literals.ContainsKey(key), key);
            Assert.DoesNotContain("alice", (string?)literals[key], StringComparison.Ordinal);
        }

        Assert.Equal("SELECT '?'", (string?)literals["statement"]);
        Assert.Equal(5, (int?)literals["plan_id"]);

        /* metadataOnly: no statement-class value carries the literal, and the plan id stays. */
        var withheld = ProposalCommands.MaskedJson(Target(), McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly))!.AsObject();
        Assert.DoesNotContain("alice", withheld.ToJsonString(), StringComparison.Ordinal);
        Assert.Equal(5, (int?)withheld["plan_id"]);
    }
}
