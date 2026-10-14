/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 Phase A, the investigation MCP tools without a store: the <c>mcp.allowInvestigations</c> gate (ruling R2) and
/// its one Critical line, the annotation hints, the classification of the four tools, the report validation
/// (every bad field at once, action proposals refused at Level 1, the note and summary lengths), the formats, and the
/// agent-route refusal on the two route writes (ruling 1: agent_level 1 or more only). The store paths are
/// <see cref="InvestigationToolsLiveTests"/>.
/// </summary>
public sealed class DarlingMcpInvestigationToolsTests
{
    private const string Token = "investigation-gate-token-value-0123456789";

    private const string Evidence = "[{\"tool\":\"get_wait_stats\",\"arguments\":{\"server_name\":\"s1\"},\"key_numbers\":\"LCK_M_X 82%\"}]";

    [Fact]
    public void ResolveForStart_WritesOff_InvestigationsOn_WithAToken_ServesThem()
    {
        var effective = DarlingMcpWritePolicy.ResolveForStart(new McpConfig { AllowInvestigations = true }, Token, out var problem);

        Assert.Null(problem);
        Assert.True(effective.Investigations);
        Assert.False(effective.Writes);
        Assert.True(effective.Offers("claim_investigation_request"));
        Assert.True(effective.Offers("submit_investigation_report"));
        Assert.False(effective.Offers("create_mute_rule"));
    }

    [Fact]
    public void ResolveForStart_BothOn_KeepsTheInvestigationToolsOff_AndServesTheWrites()
    {
        var mcp = new McpConfig { AllowInvestigations = true, AllowWrites = true };
        var effective = DarlingMcpWritePolicy.ResolveForStart(mcp, Token, out _);

        Assert.True(DarlingMcpWritePolicy.InvestigationsConflict(mcp));
        Assert.False(effective.Investigations);
        Assert.True(effective.Writes);
        Assert.False(effective.Offers("claim_investigation_request"));
        Assert.False(effective.Offers("submit_investigation_report"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("short")]
    public void ResolveForStart_NoUsableToken_KeepsThemOff(string token)
    {
        var effective = DarlingMcpWritePolicy.ResolveForStart(new McpConfig { AllowInvestigations = true }, token, out var problem);

        Assert.False(effective.Investigations);
        Assert.Equal(token.Length > 0, problem is not null);
    }

    [Fact]
    public void ResolveForStart_Default_ServesNeitherInvestigationTool_ButBothReads()
    {
        var effective = DarlingMcpWritePolicy.ResolveForStart(new McpConfig(), Token, out _);

        Assert.Equal(DarlingMcpWritePolicy.Effective.Off, effective);
        Assert.False(effective.Offers("claim_investigation_request"));
        Assert.True(effective.Offers("list_investigation_requests"));
        Assert.True(effective.Offers("get_investigation_reports"));
    }

    [Fact]
    public void Start_BothOn_LogsTheConflictLine_ExactlyOnce_AsTheOnlyCritical()
    {
        var mcp = new McpConfig { AllowInvestigations = true, AllowWrites = true };
        var logger = new CapturingTestLogger();

        DarlingMcpHostService.LogInvestigationsGate(logger, mcp, DarlingMcpWritePolicy.ResolveForStart(mcp, Token, out var problem), problem);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
        Assert.Single(logger.Lines);
        Assert.Contains(DarlingMcpWritePolicy.InvestigationConflictLine, logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_Served_WarnsOnce_NoToken_IsCritical_Off_LogsNothing()
    {
        var on = new McpConfig { AllowInvestigations = true };

        var served = new CapturingTestLogger();
        DarlingMcpHostService.LogInvestigationsGate(served, on, DarlingMcpWritePolicy.ResolveForStart(on, Token, out var p1), p1);
        Assert.Equal(1, served.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, served.CountAtLevel(LogLevel.Critical));

        var noToken = new CapturingTestLogger();
        DarlingMcpHostService.LogInvestigationsGate(noToken, on, DarlingMcpWritePolicy.ResolveForStart(on, "", out var p2), p2);
        Assert.Equal(1, noToken.CountAtLevel(LogLevel.Critical));
        Assert.Contains("investigation tools stay off", noToken.Joined, StringComparison.Ordinal);

        var off = new CapturingTestLogger();
        DarlingMcpHostService.LogInvestigationsGate(off, new McpConfig(), DarlingMcpWritePolicy.Effective.Off, null);
        Assert.Empty(off.Lines);
    }

    [Fact]
    public void Annotations_ClaimAndSubmit_AreNeitherReadOnlyNorDestructive_TheReadsAreReadOnly()
    {
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in McpToolsListBudgetTests.BuildServedTools().Tools)
            all.Add(tool);

        DarlingMcpWritePolicy.Apply(all, new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true));

        var byName = all.ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool.Annotations!, StringComparer.Ordinal);
        foreach (var name in DarlingMcpWritePolicy.InvestigationTools)
        {
            Assert.False(byName[name].ReadOnlyHint, $"{name} writes investigation state");
            Assert.False(byName[name].DestructiveHint, $"{name} changes no configuration and no collected data");
        }

        foreach (var name in new[] { "list_investigation_requests", "get_investigation_reports" })
        {
            Assert.True(byName[name].ReadOnlyHint);
            Assert.False(byName[name].DestructiveHint);
        }

        Assert.DoesNotContain(byName.Keys, DarlingMcpWritePolicy.IsWrite);
    }

    [Fact]
    public void Classification_TheReadsAreReadOnly_ClaimAndSubmitAreTheThirdSet_NeverWrites()
    {
        Assert.Contains("list_investigation_requests", DarlingMcpToolClassification.ReadOnlyTools);
        Assert.Contains("get_investigation_reports", DarlingMcpToolClassification.ReadOnlyTools);
        Assert.Equal(
            new[] { "claim_investigation_request", "submit_investigation_report" },
            DarlingMcpWritePolicy.InvestigationTools.OrderBy(n => n, StringComparer.Ordinal));
        Assert.Empty(DarlingMcpWritePolicy.InvestigationTools.Intersect(DarlingMcpWritePolicy.WriteTools));
        Assert.Empty(DarlingMcpWritePolicy.InvestigationTools.Intersect(DarlingMcpToolClassification.ReadOnlyTools));
    }

    [Fact]
    public void Instructions_NameTheInvestigationSwitch_AndTheWritesOffCondition()
    {
        Assert.Contains(
            "Investigation tools (claim_investigation_request, submit_investigation_report) write only investigation state and are " +
            "served when darling.json sets mcp.allowInvestigations with a token and mcp.allowWrites is off.",
            DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void Formats_ReportFormat0_IsRefused_1_IsAccepted_TheBriefIs1()
    {
        Assert.False(InvestigationFormats.IsAcceptedReportFormat(0));
        Assert.True(InvestigationFormats.IsAcceptedReportFormat(1));
        Assert.Equal(1, InvestigationFormats.BriefFormat);
    }

    [Fact]
    public void ValidateReport_AnEmptyReport_ListsEveryMissingField_AtOnce()
    {
        var problems = DarlingMcpInvestigationTools.ValidateReport(
            null, null, null, null, null, null, null, null, out _, out _, out _);

        Assert.Equal(
            new[] { "request_id", "claim_id", "verdict", "root_cause", "summary", "evidence" },
            problems.Select(p => p.Split(':')[0]));

        var message = DarlingMcpInvestigationTools.InvalidReportMessage(problems);
        foreach (var problem in problems)
            Assert.Contains(problem, message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateReport_AValidReport_HasNoProblem_AndNormalizesTheEvidence()
    {
        var problems = DarlingMcpInvestigationTools.ValidateReport(
            Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "real", "a blocking chain", Evidence, "summary",
            new string('n', DarlingMcpInvestigationTools.MaxNoteChars), "[]", out _, out _, out var evidenceJson);

        Assert.Empty(problems);
        Assert.NotNull(evidenceJson);
    }

    [Theory]
    [InlineData("[{\"kind\":\"kill_session\"}]")]
    [InlineData("anything at all")]
    public void ValidateReport_AnyActionProposal_IsRefusedAtLevel1(string proposals)
    {
        var problems = DarlingMcpInvestigationTools.ValidateReport(
            Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "real", "cause", Evidence, "summary", null, proposals,
            out _, out _, out _);

        var only = Assert.Single(problems);
        Assert.StartsWith("action_proposals:", only, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateReport_ANoteOverItsCap_AndAWrongVerdict_AndEvidenceWithoutNumbers_AreEachNamed()
    {
        var problems = DarlingMcpInvestigationTools.ValidateReport(
            Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "maybe", "cause",
            "[{\"tool\":\"get_wait_stats\",\"arguments\":{}}]", "summary",
            new string('n', DarlingMcpInvestigationTools.MaxNoteChars + 1), null, out _, out _, out _);

        Assert.Equal(new[] { "verdict", "proposed_note", "evidence" }, problems.Select(p => p.Split(':')[0]));
        Assert.Contains("key_numbers", problems[2], StringComparison.Ordinal);
    }

    [Fact]
    public async Task Submit_AnInvalidReport_IsTheSharedRefusal_NamingTheFirstField_BeforeAnyStoreCall()
    {
        /* Neither id parses, so the tool never reaches the store: the null store is never touched. */
        var answer = await DarlingMcpInvestigationTools.SubmitAsync(
            null!, "not-a-guid", "", 1, "real", "", Evidence, "", null, null, default);

        using var document = JsonDocument.Parse(answer);
        Assert.Equal("invalid", document.RootElement.GetProperty("status").GetString());
        Assert.Equal("request_id", document.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        var message = document.RootElement.GetProperty("message").GetString()!;
        foreach (var field in new[] { "request_id:", "claim_id:", "root_cause:", "summary:" })
            Assert.Contains(field, message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData(0, false)]
    [InlineData(1, true)]
    public void RouteWrites_RefuseOnlyAnAgentLevelOfOneOrMore(int? agentLevel, bool refused)
    {
        var route = new PgNotificationRouteStore.RouteSummary(
            7, "*", null, ["email"], "", true, DateTime.UtcNow) { AgentLevel = agentLevel };

        var answer = DarlingMcpAlertTools.AgentRouteRefusal(route, "delete_notification_route");

        Assert.Equal(refused, answer is not null);
        if (answer is not null)
        {
            using var document = JsonDocument.Parse(answer);
            Assert.Equal("refused", document.RootElement.GetProperty("status").GetString());
            Assert.Contains("admin notification routes page", answer, StringComparison.Ordinal);
        }
    }
}
