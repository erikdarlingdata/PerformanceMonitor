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
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9072: the settings paragraph of the instructions says which tools that change state a client is served. Each case starts the
/// real host (<see cref="DarlingMcpWritePolicyTests.BuildServer"/>) under one policy, reads the token's own <c>tools/list</c> and
/// the <c>instructions</c> its <c>initialize</c> returns, and compares the paragraph with the served set: the investigation
/// tools, the server admin tools, the write tools, and under the investigations arm the agent action tools and, while
/// <c>mcp.allowAgentMuteRules</c> is on, the mute rule tools called with no <c>agent_action</c>. Where the legacy token is not
/// served the investigations arm its paragraph names each group with the setting that serves it, so a refused client knows what
/// to ask for, and it is held to naming every group served and to describing the arm in full. Under the arm, and for each named
/// token's role, the paragraph is that policy's own, so it is also held to naming no tool the policy does not serve. Every
/// rendering is held to the instructions' size budget.
/// </summary>
public sealed class DarlingMcpInstructionsServedGroupsTests
{
    private const string LegacyToken = "0123456789abcdef0123456789abcdef";
    private const string OperatorToken = "operator-token-value-aaaaaaaaaaaa-1";
    private const string AgentToken = "agent-token-value-bbbbbbbbbbbbbbb-2";
    private const string ReaderToken = "reader-token-value-ccccccccccccc-3";

    /// <summary>What one token is served: its <c>tools/list</c> names, split into the groups that change state, and the
    /// settings paragraph of its instructions.</summary>
    private sealed record Served(HashSet<string> Tools, string Instructions, string Paragraph)
    {
        internal string[] Investigation => Tools.Where(DarlingMcpWritePolicy.IsInvestigation).ToArray();

        internal string[] ServerAdmin => Tools.Where(DarlingMcpWritePolicy.IsServerAdmin).ToArray();

        /// <summary>True when a write tool outside the server admin, agent action and change window (#9485, served to an agent token too) sets is served: the writes arm, or an operator token.</summary>
        internal bool WritesArm => Tools.Any(n => DarlingMcpWritePolicy.IsWrite(n) && !DarlingMcpWritePolicy.IsServerAdmin(n)
            && !DarlingMcpWritePolicy.AgentActionTools.Contains(n) && !DarlingMcpWritePolicy.ChangeWindowTools.Contains(n));

        /// <summary>The agent action tools served as such: write tools from the action set, served while the other write tools are not.</summary>
        internal string[] AgentActions => WritesArm
            ? Array.Empty<string>()
            : Tools.Where(n => DarlingMcpWritePolicy.IsWrite(n) && DarlingMcpWritePolicy.AgentActionTools.Contains(n)).ToArray();

        /// <summary>Every registered tool that changes state and is absent from this token's <c>tools/list</c>.</summary>
        internal string[] Unserved => DarlingMcpWritePolicy.WriteTools.Concat(DarlingMcpWritePolicy.InvestigationTools)
            .Where(n => !Tools.Contains(n)).ToArray();
    }

    private static async Task<Served> ServedAsync(TestServer server, string bearer)
    {
        var (status, body) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, bearer);
        Assert.Equal(StatusCodes.Status200OK, status);
        var tools = DarlingMcpWritePolicyTests.ToolsOf(body).Keys.ToHashSet(StringComparer.Ordinal);
        var instructions = await DarlingMcpNamedTokenTests.InstructionsAsync(server, bearer);
        Assert.True(instructions.Length <= DarlingMcpInstructionsBudgetTests.InstructionsBudget,
            $"the served instructions are {instructions.Length} characters; the budget is {DarlingMcpInstructionsBudgetTests.InstructionsBudget}");
        return new Served(tools, instructions, SettingsParagraph(instructions));
    }

    /// <summary>The settings paragraph: the last paragraph of the instructions' first section, which runs from the first
    /// <c>## </c> heading to the next one.</summary>
    private static string SettingsParagraph(string instructions)
    {
        var start = instructions.IndexOf("## ", StringComparison.Ordinal);
        Assert.True(start >= 0, "the instructions carry no section heading");
        var end = instructions.IndexOf("\n## ", start, StringComparison.Ordinal);
        Assert.True(end > start, "the instructions carry one section only");
        var paragraph = instructions[start..end].Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)[^1];
        Assert.False(paragraph.StartsWith("## ", StringComparison.Ordinal), "the first section carries no paragraph");
        return paragraph;
    }

    /// <summary>True when <paramref name="text"/> carries <paramref name="tool"/> as a whole snake_case word.</summary>
    private static bool Names(string text, string tool) =>
        Regex.IsMatch(text, "(?<![a-z0-9_])" + Regex.Escape(tool) + "(?![a-z0-9_])", RegexOptions.CultureInvariant);

    /// <summary>The sentences of a paragraph. A setting or tool name carries no period followed by a space, so none is split.</summary>
    private static string[] Sentences(string paragraph) => paragraph.Split(". ", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>How a paragraph names the agent action tools: as a counted family.</summary>
    private static string CountedActions(int count) => count.ToString(CultureInfo.InvariantCulture) + " agent action tools";

    /// <summary>The agent action tools the investigations arm offers, from the policy's own sets.</summary>
    private static string[] ArmActionTools => DarlingMcpWritePolicy.AgentActionTools
        .Where(n => !DarlingMcpWritePolicy.NeverAgentTools.Contains(n)).Order(StringComparer.Ordinal).ToArray();

    private static McpTokenEntry Entry(string name, string token, string role) => new() { Name = name, Token = token, Role = role };

    private static McpConfig ThreeRoles(bool agentMuteRules, bool writes = false, bool serverAdmin = false) => new()
    {
        AllowWrites = writes,
        AllowServerAdmin = serverAdmin,
        AllowAgentMuteRules = agentMuteRules,
        Tokens = new List<McpTokenEntry>
        {
            Entry("ops-lead", OperatorToken, McpTokenEntry.OperatorRole),
            Entry("triage-bot", AgentToken, McpTokenEntry.AgentRole),
            Entry("dev-a", ReaderToken, McpTokenEntry.ReaderRole),
        },
    };

    private static DarlingMcpTokenSet SetFor(McpConfig mcp) =>
        DarlingMcpTokenSet.Resolve(mcp, "", DarlingMcpWritePolicy.ResolveForStart(mcp, "", out _), null);

    /// <summary>Every group the legacy token is served is in its paragraph: each served investigation and server admin tool by
    /// name, and the setting that serves each group.</summary>
    private static void AssertTheLegacyParagraphNamesEveryGroupServed(Served served)
    {
        Assert.All(served.Investigation, tool => Assert.True(Names(served.Paragraph, tool), $"{tool} is served and not named: {served.Paragraph}"));
        Assert.All(served.ServerAdmin, tool => Assert.True(Names(served.Paragraph, tool), $"{tool} is served and not named: {served.Paragraph}"));
        if (served.WritesArm)
            Assert.Contains(DarlingMcpWritePolicy.WritesSetting, served.Paragraph, StringComparison.Ordinal);
        if (served.ServerAdmin.Length > 0)
            Assert.Contains(DarlingMcpWritePolicy.ServerAdminSetting, served.Paragraph, StringComparison.Ordinal);
        if (served.Investigation.Length > 0)
            Assert.Contains(DarlingMcpWritePolicy.InvestigationsSetting, served.Paragraph, StringComparison.Ordinal);
        if (served.AgentActions.Length > 0)
        {
            Assert.Contains(CountedActions(served.AgentActions.Length), served.Paragraph, StringComparison.Ordinal);
            Assert.Contains("agent_action", served.Paragraph, StringComparison.Ordinal);
        }
    }

    /// <summary>The legacy token's settings paragraph says the mute rule tools write with no <c>agent_action</c> inside the sentence
    /// that names <c>mcp.allowInvestigations</c>, tied to it with the setting, never as a sentence of its own that a client holding
    /// the writes arm could read as describing its session. While <c>mcp.allowAgentMuteRules</c> is off no sentence names it.</summary>
    private static void AssertTheMuteClaimIsTiedToTheInvestigationsSentence(Served served, bool agentMuteRules)
    {
        var naming = Sentences(served.Paragraph).Where(s => s.Contains(DarlingMcpAgentMuteDirect.SettingName, StringComparison.Ordinal)).ToArray();
        if (!agentMuteRules)
        {
            Assert.Empty(naming);
            Assert.DoesNotContain("without an agent_action", served.Paragraph, StringComparison.Ordinal);
            return;
        }

        var sentence = Assert.Single(naming);
        Assert.Contains(DarlingMcpWritePolicy.InvestigationsSetting, sentence, StringComparison.Ordinal);
        Assert.Contains("while " + DarlingMcpAgentMuteDirect.SettingName + " is on, the mute rule tools among them also write mute and digest rules without an agent_action", sentence, StringComparison.Ordinal);
        Assert.Single(Sentences(served.Paragraph), s => s.Contains("without an agent_action", StringComparison.Ordinal));
    }

    /// <summary>Both settings off: no tool that changes state is served, so the paragraph names each setting and what it serves,
    /// for a refused client to ask for. What it says <c>mcp.allowInvestigations</c> serves is the whole arm: the investigation
    /// tools by name, the agent action tools counted from the policy's set, and the mute rule tools with no <c>agent_action</c>
    /// while <c>mcp.allowAgentMuteRules</c> is on.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BothSettingsOff_TheParagraphSaysEverythingEachSettingServes(bool agentMuteRules)
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig { AllowAgentMuteRules = agentMuteRules }, LegacyToken);
        var served = await ServedAsync(server, LegacyToken);

        Assert.Empty(served.Investigation);
        Assert.Empty(served.ServerAdmin);
        Assert.Empty(served.AgentActions);
        Assert.False(served.WritesArm);

        Assert.Contains(DarlingMcpWritePolicy.WritesSetting, served.Paragraph, StringComparison.Ordinal);
        Assert.Contains(DarlingMcpWritePolicy.ServerAdminSetting, served.Paragraph, StringComparison.Ordinal);
        Assert.All(DarlingMcpWritePolicy.ServerAdminTools, tool => Assert.True(Names(served.Paragraph, tool), $"{tool} is not named: {served.Paragraph}"));

        Assert.Contains(DarlingMcpWritePolicy.InvestigationsSetting, served.Paragraph, StringComparison.Ordinal);
        Assert.All(DarlingMcpWritePolicy.InvestigationTools, tool => Assert.True(Names(served.Paragraph, tool), $"{tool} is not named: {served.Paragraph}"));
        Assert.NotEmpty(ArmActionTools);
        Assert.Contains(CountedActions(ArmActionTools.Length), served.Paragraph, StringComparison.Ordinal);
        Assert.Contains("agent_action", served.Paragraph, StringComparison.Ordinal);
        Assert.Equal(agentMuteRules, served.Instructions.Contains(DarlingMcpAgentMuteDirect.SettingName, StringComparison.Ordinal));
        AssertTheMuteClaimIsTiedToTheInvestigationsSentence(served, agentMuteRules);
    }

    /// <summary>The investigations arm serves three groups that change state, and the paragraph names all three: the investigation
    /// tools by name, the agent action tools as a family counted from the served set and called with an <c>agent_action</c>, and
    /// the mute rule tools that write without one because <c>mcp.allowAgentMuteRules</c> is on.</summary>
    [Fact]
    public async Task TheInvestigationsArm_NamesTheInvestigationTools_TheAgentActionTools_AndTheMuteRuleToolsWithoutAnAgentAction()
    {
        var mcp = new McpConfig { AllowInvestigations = true };
        Assert.True(mcp.AllowAgentMuteRules, "mcp.allowAgentMuteRules defaults on");
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, LegacyToken);
        var served = await ServedAsync(server, LegacyToken);

        Assert.Equal(DarlingMcpWritePolicy.InvestigationTools.Order(StringComparer.Ordinal), served.Investigation.Order(StringComparer.Ordinal));
        Assert.Equal(ArmActionTools, served.AgentActions.Order(StringComparer.Ordinal));
        Assert.NotEmpty(served.AgentActions);
        Assert.False(served.WritesArm);
        Assert.Empty(served.ServerAdmin);

        AssertTheLegacyParagraphNamesEveryGroupServed(served);
        Assert.Contains(DarlingMcpAgentMuteDirect.SettingName, served.Paragraph, StringComparison.Ordinal);

        /* The arm's paragraph is its own: no tool that changes state and is absent from the arm's tools/list is named. */
        Assert.NotEmpty(served.Unserved);
        Assert.All(served.Unserved, tool => Assert.False(Names(served.Paragraph, tool), $"{tool} is not served and is named: {served.Paragraph}"));
    }

    /// <summary><c>mcp.allowAgentMuteRules</c> off: every mute rule call with no <c>agent_action</c> is refused, so the sentence
    /// that offers one is absent, and the other two groups are still named.</summary>
    [Fact]
    public async Task TheInvestigationsArm_WithAgentMuteRulesOff_DoesNotSayTheMuteRuleToolsWriteWithoutAnAgentAction()
    {
        using var on = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig { AllowInvestigations = true }, LegacyToken);
        using var off = await DarlingMcpWritePolicyTests.BuildServer(
            new McpConfig { AllowInvestigations = true, AllowAgentMuteRules = false }, LegacyToken);
        var servedOn = await ServedAsync(on, LegacyToken);
        var servedOff = await ServedAsync(off, LegacyToken);

        Assert.Equal(servedOn.Tools.Order(StringComparer.Ordinal), servedOff.Tools.Order(StringComparer.Ordinal));
        AssertTheLegacyParagraphNamesEveryGroupServed(servedOff);
        Assert.DoesNotContain(DarlingMcpAgentMuteDirect.SettingName, servedOff.Instructions, StringComparison.Ordinal);

        /* The two paragraphs differ by one sentence: the one that names the setting and says the mute rule tools need no agent_action. */
        Assert.Empty(Sentences(servedOff.Paragraph).Except(Sentences(servedOn.Paragraph), StringComparer.Ordinal));
        var sentence = Assert.Single(Sentences(servedOn.Paragraph).Except(Sentences(servedOff.Paragraph), StringComparer.Ordinal));
        Assert.Contains(DarlingMcpAgentMuteDirect.SettingName, sentence, StringComparison.Ordinal);
        Assert.Contains("agent_action", sentence, StringComparison.Ordinal);
        Assert.Contains("mute", sentence, StringComparison.Ordinal);
    }

    /// <summary>The writes arm serves the write tools, the server admin tools with their own setting, and no investigation tool. Its
    /// paragraph says nothing of the mute rule tools outside the sentence that describes the investigations arm it does not hold.</summary>
    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    public async Task TheWritesArm_NamesTheWriteSetting_AndEveryServerAdminToolItServes(bool serverAdmin, bool agentMuteRules)
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(
            new McpConfig { AllowWrites = true, AllowServerAdmin = serverAdmin, AllowAgentMuteRules = agentMuteRules }, LegacyToken);
        var served = await ServedAsync(server, LegacyToken);

        Assert.True(served.WritesArm);
        Assert.Empty(served.Investigation);
        Assert.Empty(served.AgentActions);
        Assert.Equal(serverAdmin ? DarlingMcpWritePolicy.ServerAdminTools.Count : 0, served.ServerAdmin.Length);

        AssertTheLegacyParagraphNamesEveryGroupServed(served);
        AssertTheMuteClaimIsTiedToTheInvestigationsSentence(served, agentMuteRules);
    }

    /// <summary>Each named token's paragraph is its role's own: it names every group that role is served and no tool that
    /// changes state and is absent from that token's <c>tools/list</c>. Only the agent's says the mute rule tools write with no
    /// <c>agent_action</c>, and only while <c>mcp.allowAgentMuteRules</c> is on.</summary>
    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, true)]
    public async Task EachNamedRole_IsToldEveryGroupItIsServed_AndNoToolItIsNot(bool agentMuteRules, bool writes, bool serverAdmin)
    {
        var mcp = ThreeRoles(agentMuteRules, writes, serverAdmin);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));
        var agent = await ServedAsync(server, AgentToken);
        var operatorRole = await ServedAsync(server, OperatorToken);
        var reader = await ServedAsync(server, ReaderToken);

        foreach (var role in new[] { agent, operatorRole, reader })
        {
            Assert.All(role.Unserved, tool => Assert.False(Names(role.Paragraph, tool), $"{tool} is not served and is named: {role.Paragraph}"));
            Assert.DoesNotContain(DarlingMcpWritePolicy.InvestigationsSetting, role.Paragraph, StringComparison.Ordinal);
        }

        /* The agent: the investigation tools by name, the agent action tools as a counted family, the mute rule sentence with the setting. */
        Assert.Equal(DarlingMcpWritePolicy.InvestigationTools.Order(StringComparer.Ordinal), agent.Investigation.Order(StringComparer.Ordinal));
        Assert.NotEmpty(agent.AgentActions);
        Assert.Empty(agent.ServerAdmin);
        Assert.All(agent.Investigation, tool => Assert.True(Names(agent.Paragraph, tool), $"{tool} is served and not named: {agent.Paragraph}"));
        Assert.Contains(CountedActions(agent.AgentActions.Length), agent.Paragraph, StringComparison.Ordinal);
        Assert.Contains("agent_action", agent.Paragraph, StringComparison.Ordinal);
        Assert.Equal(agentMuteRules, agent.Paragraph.Contains(DarlingMcpAgentMuteDirect.SettingName, StringComparison.Ordinal));
        Assert.Equal(agentMuteRules, agent.Instructions.Contains(DarlingMcpAgentMuteDirect.SettingName, StringComparison.Ordinal));
        Assert.DoesNotContain(DarlingMcpWritePolicy.WritesSetting, agent.Paragraph, StringComparison.Ordinal);

        /* The operator: the write tools as plain writes, so no agent_action, no mute rule sentence and no investigation tool. */
        Assert.True(operatorRole.WritesArm);
        Assert.Empty(operatorRole.Investigation);
        Assert.Equal(writes && serverAdmin ? DarlingMcpWritePolicy.ServerAdminTools.Count : 0, operatorRole.ServerAdmin.Length);
        Assert.Contains(DarlingMcpWritePolicy.ServerAdminSetting, operatorRole.Paragraph, StringComparison.Ordinal);
        Assert.DoesNotContain("agent_action", operatorRole.Paragraph, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingMcpAgentMuteDirect.SettingName, operatorRole.Instructions, StringComparison.Ordinal);

        /* The reader: no tool that changes state, so no group is offered. */
        Assert.DoesNotContain(reader.Tools, n => DarlingMcpWritePolicy.IsWrite(n) || DarlingMcpWritePolicy.IsInvestigation(n));
        Assert.DoesNotContain("agent_action", reader.Paragraph, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingMcpAgentMuteDirect.SettingName, reader.Instructions, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingMcpWritePolicy.WritesSetting, reader.Paragraph, StringComparison.Ordinal);
    }

    /// <summary>The start's line for the arm names both groups it serves, and says the mute rule tools write with no
    /// <c>agent_action</c> only while <c>mcp.allowAgentMuteRules</c> is on.</summary>
    [Fact]
    public void TheStartLine_NamesBothGroups_AndTheMuteRuleToolsOnlyWhileTheSettingIsOn()
    {
        string LineFor(bool agentMuteRules)
        {
            var mcp = new McpConfig { AllowInvestigations = true, AllowAgentMuteRules = agentMuteRules };
            var logger = new CapturingTestLogger();
            DarlingMcpHostService.LogInvestigationsGate(logger, mcp, DarlingMcpWritePolicy.ResolveForStart(mcp, LegacyToken, out var problem), problem);
            return Assert.Single(logger.Lines);
        }

        var on = LineFor(true);
        var off = LineFor(false);

        foreach (var line in new[] { on, off })
        {
            Assert.All(DarlingMcpWritePolicy.InvestigationTools, tool => Assert.True(Names(line, tool), $"{tool} is not named: {line}"));
            Assert.Contains(CountedActions(ArmActionTools.Length), line, StringComparison.Ordinal);
            Assert.Contains("agent_action", line, StringComparison.Ordinal);
        }

        Assert.Contains("mute", on, StringComparison.Ordinal);
        Assert.DoesNotContain("mute", off, StringComparison.Ordinal);
    }
}
