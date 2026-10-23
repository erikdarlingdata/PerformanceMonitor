// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6455: wording and validation of the older MCP write tools, run through the code that builds each answer. None of these needs a store:
/// each refusal or status is decided before any query runs.
/// </summary>
public sealed class McpWriteToolWordingTests
{
    [Theory]
    [InlineData(1, 1, 1, "dismissed")]
    [InlineData(2, 2, 1, "dismissed")]
    [InlineData(1, 0, 0, "not_found")]
    [InlineData(3, 2, 0, "unchanged")]
    public void DismissAlertHistory_OnlyAnswersDismissedWhenItDismissedARow(int requested, int known, int dismissed, string expected)
    {
        Assert.Equal(expected, DarlingMcpAlertTools.DismissStatus(new AlertDismissOutcome(requested, known, dismissed)));
    }

    [Theory]
    [InlineData(1, 1, 1, "restored")]
    [InlineData(1, 0, 0, "not_found")]
    [InlineData(2, 2, 0, "unchanged")]
    public void RestoreAlertHistory_OnlyAnswersRestoredWhenItRestoredARow(int requested, int known, int restored, string expected)
    {
        Assert.Equal(expected, DarlingMcpAlertTools.RestoreStatus(new AlertDismissOutcome(requested, known, restored)));
    }

    [Fact]
    public void UpdateAlertSettings_NamesTheCooldownAsTheCallAndTheReadDo()
    {
        Assert.Equal("delivery.cooldown_minutes", DarlingMcpAlertTools.UpdatedFieldName(DarlingMcpAlertTools.DeliveryCooldownColumn));
        Assert.Equal("blocking_threshold_seconds", DarlingMcpAlertTools.UpdatedFieldName("blocking_threshold_seconds"));
    }

    [Theory]
    [InlineData("0xG6G6G6G6G6G6G6G6")]
    [InlineData("0x")]
    [InlineData("hello")]
    [InlineData("0x1A2B3C4D5E6F70811")]
    [InlineData("12.5")]
    [InlineData("-5")]
    [InlineData("99999999999999999999999")]
    [InlineData("0")]
    [InlineData("0x0000000000000000")]
    public async Task AssignQueryTag_RefusesAHashThatIsNotHexadecimal(string hash)
    {
        /* The refusal comes before the first store call, so a data source that cannot connect is enough. */
        await using var source = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");
        var answer = JsonDocument.Parse(await DarlingMcpQueryTagTools.AssignQueryTag(source, 1, "hash", hash)).RootElement;

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Contains("hexadecimal", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("0x1A2B3C4D5E6F7081")]
    [InlineData("0xa1b2")]
    [InlineData("0X00000000000000FF")]
    [InlineData("12345678901234567890")]
    [InlineData("42")]
    public void AHashTheTraceOrTheDmvsStore_IsAccepted(string hash)
    {
        Assert.True(PerformanceMonitor.Common.QueryHashIdentity.IsAssignableHash(hash));
    }

    [Fact]
    public void ADryRunRefusedForNoAgentAction_SaysWhyItNeedsOne()
    {
        var refusal = new AgentActionRefusal(AgentActionCodes.Required, "agent_action is required.");

        var dry = JsonDocument.Parse(DarlingMcpPlanForceTools.GateRefusalAnswer(refusal, dryRun: true)).RootElement;
        var write = JsonDocument.Parse(DarlingMcpPlanForceTools.GateRefusalAnswer(refusal, dryRun: false)).RootElement;
        var other = JsonDocument.Parse(DarlingMcpPlanForceTools.GateRefusalAnswer(
            new AgentActionRefusal(AgentActionCodes.ActionsOff, "off."), dryRun: true)).RootElement;

        Assert.Equal("agent_action_required", dry.GetProperty("reason").GetString());
        Assert.Contains("A dry run needs it too", dry.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("agent_action is required.", write.GetProperty("message").GetString());
        Assert.Equal("off.", other.GetProperty("message").GetString());
    }
}
