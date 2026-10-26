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
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Named MCP bearer tokens with roles (#6542, part 1). The host cases drive the real pipeline through a TestServer with one
/// operator token and one agent token: each token's <c>tools/list</c> is its own set, and a call outside the set fails as an unknown
/// tool. The unit cases cover the start checks, the constant-time match, the legacy single token and the log identity.
/// </summary>
public sealed class DarlingMcpNamedTokenTests
{
    private const string OperatorToken = "operator-token-value-aaaaaaaaaaaa-1";
    private const string AgentToken = "agent-token-value-bbbbbbbbbbbbbbb-2";

    private static McpTokenEntry Entry(string? name, string? token, string? role) =>
        new() { Name = name, Token = token, Role = role };

    private static McpConfig TwoTokens(bool serverAdmin = false, bool writes = false) => new()
    {
        AllowServerAdmin = serverAdmin,
        AllowWrites = writes,
        Tokens = new List<McpTokenEntry>
        {
            Entry("ops-lead", OperatorToken, "operator"),
            Entry("triage-bot", AgentToken, "agent"),
        },
    };

    private static DarlingMcpTokenSet SetFor(McpConfig mcp, string legacyToken = "")
    {
        var legacy = DarlingMcpWritePolicy.ResolveForStart(mcp, legacyToken, out _);
        return DarlingMcpTokenSet.Resolve(mcp, legacyToken, legacy, null);
    }

    private static async Task<HashSet<string>> ToolNamesAsync(Microsoft.AspNetCore.TestHost.TestServer server, string? bearer)
    {
        var (status, body) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, bearer);
        Assert.Equal(StatusCodes.Status200OK, status);
        return DarlingMcpWritePolicyTests.ToolsOf(body).Keys.ToHashSet(StringComparer.Ordinal);
    }

    /// <summary>The <c>instructions</c> an <c>initialize</c> request returns to the token presented; the transport may frame the
    /// reply as an SSE <c>data:</c> line or as bare JSON.</summary>
    private static async Task<string> InstructionsAsync(Microsoft.AspNetCore.TestHost.TestServer server, string bearer)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(
                "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\"," +
                "\"capabilities\":{},\"clientInfo\":{\"name\":\"test\",\"version\":\"1\"}}}");
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
            c.Request.Headers.Authorization = "Bearer " + bearer;
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        return document.RootElement.GetProperty("result").GetProperty("instructions").GetString() ?? "";
    }

    /// <summary>Every registered tool whose name the text carries, backticked or not (a tool name is a whole snake_case word).</summary>
    private static List<string> ToolsNamedIn(string text, IEnumerable<string> catalog) =>
        catalog.Where(n => Regex.IsMatch(text, @"(?<![A-Za-z0-9_])" + Regex.Escape(n) + @"(?![A-Za-z0-9_])", RegexOptions.CultureInvariant))
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

    /// <summary>The tools the text points a client at: backticked names, the convention #5789's
    /// <c>RenderedInstructions_NameOnlyServedTools</c> checks. The legacy settings paragraph
    /// (<c>DarlingMcpWritePolicy.InstructionsLine</c>) names the claim and submit tools bare, to say which switch serves
    /// them, and that is not pointing at them.</summary>
    private static List<string> ToolsPointedAtIn(string text, IEnumerable<string> catalog) =>
        catalog.Where(n => text.Contains("`" + n + "`", StringComparison.Ordinal)).OrderBy(n => n, StringComparer.Ordinal).ToList();

    private static readonly string[] ServerAdminToolNames = { "add_servers", "edit_server", "remove_server", "set_server_action" };

    [Fact]
    public void ServerAdminToolsList_IsTheFourToolsTheDocsName()
    {
        Assert.Equal(ServerAdminToolNames.OrderBy(n => n, StringComparer.Ordinal), DarlingMcpWritePolicy.ServerAdminTools.OrderBy(n => n, StringComparer.Ordinal));
    }

    /// <summary>Review round 2, finding 2 of 2: <c>mcp.allowServerAdmin</c> does nothing unless <c>mcp.allowWrites</c> is on
    /// (<see cref="DarlingMcpWritePolicy.Resolve"/>). An operator token keeps that precondition, so adding <c>mcp.tokens</c> to a
    /// service with <c>allowServerAdmin: true, allowWrites: false</c> does not turn on the four server admin tools.</summary>
    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, true)]
    public async Task AnOperatorToken_SeesTheServerAdminTools_OnlyWithBothAllowWritesAndAllowServerAdmin(bool writes, bool serverAdmin, bool expected)
    {
        var mcp = TwoTokens(serverAdmin: serverAdmin, writes: writes);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));

        var operatorTools = await ToolNamesAsync(server, OperatorToken);
        var agentTools = await ToolNamesAsync(server, AgentToken);

        foreach (var tool in ServerAdminToolNames)
        {
            Assert.Equal(expected, operatorTools.Contains(tool));
            Assert.DoesNotContain(tool, agentTools);
        }

        /* The ordinary write tools come from the role either way, as documented. */
        Assert.Contains("create_mute_rule", operatorTools);
        Assert.Contains("update_alert_settings", operatorTools);
    }

    /// <summary>Review round 2, finding 1 of 2: the instructions follow the token. Each token's <c>initialize</c> instructions name
    /// no tool its own <c>tools/list</c> lacks (#5789), and the write-policy paragraph names the role, not the settings that govern
    /// the legacy token.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    public async Task EachTokensInstructions_NameNoToolItsOwnToolsListLacks(bool writes, bool serverAdmin)
    {
        var mcp = TwoTokens(serverAdmin: serverAdmin, writes: writes);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));
        var catalog = DarlingMcpWritePolicy.ToolNames(McpToolsListBudgetTests.BuildServedTools().Tools);

        var operatorTools = await ToolNamesAsync(server, OperatorToken);
        var agentTools = await ToolNamesAsync(server, AgentToken);
        var operatorText = await InstructionsAsync(server, OperatorToken);
        var agentText = await InstructionsAsync(server, AgentToken);

        Assert.DoesNotContain(ToolsNamedIn(operatorText, catalog), n => !operatorTools.Contains(n));
        Assert.DoesNotContain(ToolsNamedIn(agentText, catalog), n => !agentTools.Contains(n));

        /* The case the review found: the mute step is in NeverAgentTools, so the agent's Workflow line must not name it. */
        Assert.DoesNotContain("mute_analysis_finding", agentTools);
        Assert.DoesNotContain("mute_analysis_finding", agentText, StringComparison.Ordinal);
        Assert.Contains("mute_analysis_finding", operatorText, StringComparison.Ordinal);

        /* The census counts what each token is served, and the sentences name the role. */
        Assert.Contains($"exposes {agentTools.Count} of its {catalog.Count} tools", agentText, StringComparison.Ordinal);
        Assert.Contains($"exposes {operatorTools.Count} of its {catalog.Count} tools", operatorText, StringComparison.Ordinal);
        Assert.Contains("agent role", agentText, StringComparison.Ordinal);
        Assert.Contains("operator role", operatorText, StringComparison.Ordinal);
        foreach (var text in new[] { operatorText, agentText })
        {
            Assert.DoesNotContain("are served when darling.json sets mcp.allowWrites", text, StringComparison.Ordinal);
            Assert.DoesNotContain("mcp.allowInvestigations", text, StringComparison.Ordinal);
        }

        /* The two roles are told different things, and the unnamed start-wide union text is neither. */
        Assert.NotEqual(operatorText, agentText);
        Assert.DoesNotContain("claim_investigation_request", operatorText, StringComparison.Ordinal);
        Assert.Contains("claim_investigation_request", agentText, StringComparison.Ordinal);
    }

    /// <summary>With a legacy token beside the named ones, the legacy token's instructions are the ones its own settings give it.</summary>
    [Fact]
    public async Task ALegacyTokenBesideNamedTokens_GetsTheInstructionsItsSettingsGiveIt()
    {
        const string legacy = "legacy-single-token-value-cccccc-3";
        var mcp = TwoTokens(serverAdmin: true, writes: true);
        var catalog = DarlingMcpWritePolicy.ToolNames(McpToolsListBudgetTests.BuildServedTools().Tools);
        using var plain = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig { AllowWrites = true, AllowServerAdmin = true }, legacy);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, legacy, tokens: SetFor(mcp, legacy));

        var expected = await InstructionsAsync(plain, legacy);
        var actual = await InstructionsAsync(server, legacy);

        Assert.Equal(expected, actual);
        var served = await ToolNamesAsync(server, legacy);
        Assert.DoesNotContain(ToolsPointedAtIn(actual, catalog), n => !served.Contains(n));
    }

    [Fact]
    public async Task AnAgentToken_ListsNoRouteAdminTool_AndACallToOneIsRefusedAsUnknown()
    {
        var mcp = TwoTokens();
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));

        var agentTools = await ToolNamesAsync(server, AgentToken);
        foreach (var route in new[] { "create_notification_route", "update_notification_route", "delete_notification_route", "set_notification_route_enabled", "set_notification_route_fallback" })
            Assert.DoesNotContain(route, agentTools);

        /* Hidden is not enough: the call itself is refused, with no tool body run. */
        var (_, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(server, "create_notification_route", "{}", AgentToken);
        Assert.Contains("Unknown tool", body, StringComparison.OrdinalIgnoreCase);

        /* The operator token does see them, so the absence above is the role and not an unserved tool. */
        Assert.Contains("create_notification_route", await ToolNamesAsync(server, OperatorToken));
    }

    [Fact]
    public async Task AnAgentToken_ReachesNoNeverAgentTool_AndNoWriteBeyondTheLevel2ActionTools()
    {
        var mcp = TwoTokens(serverAdmin: true);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));

        var agentTools = await ToolNamesAsync(server, AgentToken);

        Assert.DoesNotContain(agentTools, DarlingMcpWritePolicy.NeverAgentTools.Contains);
        Assert.All(agentTools.Where(DarlingMcpWritePolicy.IsWrite), t => Assert.Contains(t, DarlingMcpWritePolicy.AgentActionTools));
        Assert.DoesNotContain(agentTools, DarlingMcpWritePolicy.IsServerAdmin);
        Assert.Contains("list_servers", agentTools);
    }

    [Fact]
    public async Task AnOperatorToken_CannotClaimOrSubmit_TheToolsAreAbsentAndTheCallIsRefused()
    {
        var mcp = TwoTokens();
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));

        var operatorTools = await ToolNamesAsync(server, OperatorToken);
        Assert.DoesNotContain("claim_investigation_request", operatorTools);
        Assert.DoesNotContain("submit_investigation_report", operatorTools);

        var (_, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
            server, "claim_investigation_request",
            "{\"request_id\":\"" + Guid.NewGuid() + "\",\"agent_id\":\"ops-lead\"}", OperatorToken);
        Assert.Contains("Unknown tool", body, StringComparison.OrdinalIgnoreCase);

        /* And the agent token does claim, so the tool is served to someone. */
        var agentTools = await ToolNamesAsync(server, AgentToken);
        Assert.Contains("claim_investigation_request", agentTools);
        Assert.Contains("submit_investigation_report", agentTools);
    }

    [Fact]
    public async Task OneService_AnOperatorTokenAndAnAgentToken_EachSeeTheirOwnToolSet_AndAnUnknownTokenGets401()
    {
        var mcp = TwoTokens();
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));

        var operatorTools = await ToolNamesAsync(server, OperatorToken);
        var agentTools = await ToolNamesAsync(server, AgentToken);

        var set = SetFor(mcp);
        var operatorPolicy = set.PolicyFor(set.Named.Single(n => n.Name == "ops-lead"));
        var agentPolicy = set.PolicyFor(set.Named.Single(n => n.Name == "triage-bot"));
        Assert.All(operatorTools, n => Assert.True(operatorPolicy.Offers(n), n + " is not in the operator role's set"));
        Assert.All(agentTools, n => Assert.True(agentPolicy.Offers(n), n + " is not in the agent role's set"));
        Assert.NotEqual(operatorTools.Count, agentTools.Count);
        Assert.Contains("create_mute_rule", operatorTools);
        Assert.Contains("create_mute_rule", agentTools);
        Assert.Contains("update_alert_settings", operatorTools);
        Assert.DoesNotContain("update_alert_settings", agentTools);

        var (unknown, _) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, "not-a-served-token-value-0123456789");
        var (none, _) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, null);
        Assert.Equal(StatusCodes.Status401Unauthorized, unknown);
        Assert.Equal(StatusCodes.Status401Unauthorized, none);
    }

    [Fact]
    public async Task NamedTokens_GateTheListenerEvenWhenTheCallerAsksForNoGate()
    {
        var mcp = TwoTokens();
        var set = SetFor(mcp);
        Assert.True(DarlingMcpHostService.RequireTokenWhenLoopbackOnly(false, "", set));
        Assert.False(DarlingMcpHostService.RequireTokenWhenLoopbackOnly(false, "", SetFor(new McpConfig())));

        /* The pipeline is built with requireTokenWhenLoopbackOnly false, as a caller that forgot it would: the named tokens still gate. */
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: set, requireToken: false);
        var (status, _) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, bearer: null);
        Assert.Equal(StatusCodes.Status401Unauthorized, status);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task ALegacySingleTokenSetup_ServesExactlyWhatItServedBefore_ForEveryWritesAndInvestigationsSetting(bool writes, bool investigations)
    {
        const string legacy = "legacy-single-token-value-cccccc-3";
        var mcp = new McpConfig { AllowWrites = writes, AllowInvestigations = investigations };
        var resolved = DarlingMcpWritePolicy.ResolveForStart(mcp, legacy, out _);
        var expectedClaim = investigations && !writes;

        using var plain = await DarlingMcpWritePolicyTests.BuildServer(mcp, legacy);
        using var withSet = await DarlingMcpWritePolicyTests.BuildServer(mcp, legacy, tokens: SetFor(mcp, legacy));

        var before = await ToolNamesAsync(plain, legacy);
        var after = await ToolNamesAsync(withSet, legacy);

        Assert.All(before, n => Assert.True(resolved.Offers(n), n + " is not in the legacy token's set"));
        Assert.Equal(expectedClaim, before.Contains("claim_investigation_request"));
        Assert.Equal(writes, before.Contains("create_mute_rule") && before.Contains("update_alert_settings"));
        Assert.Equal(before.OrderBy(n => n, StringComparer.Ordinal), after.OrderBy(n => n, StringComparer.Ordinal));
        Assert.Equal(StatusCodes.Status401Unauthorized, (await DarlingMcpWritePolicyTests.ToolsListAsync(withSet, "wrong-token-value-0123456789abcdef")).StatusCode);
    }

    [Fact]
    public void Match_FindsEachServedToken_AndNothingForAnUnknownOrEmptyOne()
    {
        const string legacy = "legacy-single-token-value-cccccc-3";
        var mcp = TwoTokens();
        var set = SetFor(mcp, legacy);

        Assert.Equal("ops-lead", set.Match(OperatorToken)!.Name);
        Assert.True(set.Match(OperatorToken)!.Policy.Writes);
        Assert.False(set.Match(OperatorToken)!.Policy.Investigations);
        Assert.Equal("triage-bot", set.Match(AgentToken)!.Name);
        Assert.True(set.Match(AgentToken)!.Policy.Investigations);
        Assert.False(set.Match(AgentToken)!.Policy.Writes);
        Assert.Null(set.Match(legacy)!.Name);
        Assert.Null(set.Match("an-unserved-token-value-0123456789"));
        Assert.Null(set.Match(""));
        Assert.Null(set.Match(null));
        /* A prefix or an extension of a served token is not that token. */
        Assert.Null(set.Match(OperatorToken + "x"));
        Assert.Null(set.Match(OperatorToken[..^1]));
    }

    [Fact]
    public void Match_ComparesEveryServedToken_EvenAfterTheFirstOneMatches()
    {
        var comparisons = new List<string>();
        var set = new DarlingMcpTokenSet(
            "", DarlingMcpWritePolicy.Effective.Off,
            new[]
            {
                new DarlingMcpTokenSet.Entry("first", "first-token-value-0123456789abcdef", "operator"),
                new DarlingMcpTokenSet.Entry("second", "second-token-value-0123456789abcde", "agent"),
                new DarlingMcpTokenSet.Entry("third", "third-token-value-0123456789abcdef", "agent"),
            },
            serverAdmin: false)
        {
            Compare = (presented, expected) =>
            {
                comparisons.Add(expected is null ? string.Empty : expected[..expected.IndexOf('-', StringComparison.Ordinal)]);
                return DarlingHostBinding.FixedTimeTokenEquals(presented, expected);
            },
        };

        /* The first token matches, and the second and third are still compared. */
        Assert.Equal("first", set.Match("first-token-value-0123456789abcdef")!.Name);
        Assert.Equal(new[] { "first", "second", "third" }, comparisons);

        /* No match: every token is compared as well. */
        comparisons.Clear();
        Assert.Null(set.Match("no-such-token-value-0123456789abcd"));
        Assert.Equal(new[] { "first", "second", "third" }, comparisons);
    }

    public static IEnumerable<object?[]> BadEntries()
    {
        yield return new object?[] { "a name with spaces", "valid-token-value-0123456789abcdef", "agent", "no valid name" };
        yield return new object?[] { "", "valid-token-value-0123456789abcdef", "agent", "no valid name" };
        yield return new object?[] { new string('a', 65), "valid-token-value-0123456789abcdef", "agent", "no valid name" };
        yield return new object?[] { "invalid", "valid-token-value-0123456789abcdef", "agent", "reserved" };
        yield return new object?[] { "Unclaimed", "valid-token-value-0123456789abcdef", "operator", "reserved" };
        yield return new object?[] { "bad-role", "valid-token-value-0123456789abcdef", "admin", "no valid role" };
        yield return new object?[] { "no-role", "valid-token-value-0123456789abcdef", null, "no valid role" };
        yield return new object?[] { "short-one", "tooshort", "agent", "shorter than" };
        yield return new object?[] { "no-value", "", "agent", "could not be read" };
    }

    [Theory]
    [MemberData(nameof(BadEntries))]
    public void Check_RefusesABadEntry_NamesItAndNeverItsValue(string? name, string? token, string? role, string expectedPhrase)
    {
        var mcp = new McpConfig
        {
            Tokens = new List<McpTokenEntry> { Entry("good-agent", "good-token-value-0123456789abcdef", "agent"), Entry(name, token, role) },
        };

        var problems = DarlingMcpTokenSet.Check(mcp, "", DarlingMcpTokenSet.ResolveEntry, out var usable);

        Assert.Equal("good-agent", Assert.Single(usable).Name);
        var problem = Assert.Single(problems);
        Assert.Contains(expectedPhrase, problem, StringComparison.Ordinal);
        Assert.Contains("mcp.tokens[1]", problem, StringComparison.Ordinal);
        if (!string.IsNullOrEmpty(name) && DarlingMcpTokenSet.IsValidName(name))
            Assert.Contains(name, problem, StringComparison.Ordinal);
        if (!string.IsNullOrEmpty(token))
            Assert.DoesNotContain(token, problem, StringComparison.Ordinal);
    }

    /// <summary>#6542 part 2 (item 10 of #6556's security review): a named token whose reference cannot be read says which entry it is,
    /// <c>mcp.tokens[name].token</c>, and never names the legacy setting <c>mcp.network.token</c>.</summary>
    [Fact]
    public void AnUnreadableNamedToken_NamesItsOwnSetting_NotTheLegacyOne()
    {
        var entry = Entry("ops-lead", "env:DARLING_TEST_6542_TOKEN_THAT_IS_NOT_SET", "operator");

        var (token, unreadable) = DarlingMcpTokenSet.ResolveEntry(entry);

        Assert.Equal("", token);
        Assert.NotNull(unreadable);
        Assert.Contains("mcp.tokens[ops-lead].token", unreadable, StringComparison.Ordinal);
        Assert.DoesNotContain("mcp.network", unreadable, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesTwoTokensWithTheSameName_AndNeitherIsServed()
    {
        var mcp = new McpConfig
        {
            Tokens = new List<McpTokenEntry>
            {
                Entry("twin", "first-token-value-0123456789abcdef", "agent"),
                Entry("twin", "other-token-value-0123456789abcdef", "operator"),
                Entry("solo", "solo-token-value-0123456789abcdef", "agent"),
            },
        };

        var problems = DarlingMcpTokenSet.Check(mcp, "", DarlingMcpTokenSet.ResolveEntry, out var usable);

        Assert.Equal("solo", Assert.Single(usable).Name);
        var problem = Assert.Single(problems);
        Assert.Contains("twin", problem, StringComparison.Ordinal);
        Assert.DoesNotContain("token-value", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesTwoTokensWithTheSameValue_AndTheLegacyValue_NamingThemButNotTheValue()
    {
        const string shared = "shared-token-value-0123456789abcdef";
        const string legacy = "legacy-token-value-0123456789abcdef";
        var mcp = new McpConfig
        {
            Tokens = new List<McpTokenEntry>
            {
                Entry("one", shared, "operator"),
                Entry("two", shared, "agent"),
                Entry("three", legacy, "agent"),
                Entry("fine", "fine-token-value-0123456789abcdef", "agent"),
            },
        };

        var problems = DarlingMcpTokenSet.Check(mcp, legacy, DarlingMcpTokenSet.ResolveEntry, out var usable);

        Assert.Equal("fine", Assert.Single(usable).Name);
        var sameValue = Assert.Single(problems, p => p.Contains("share a token value", StringComparison.Ordinal));
        Assert.Contains("one", sameValue, StringComparison.Ordinal);
        Assert.Contains("two", sameValue, StringComparison.Ordinal);
        Assert.Contains("three", sameValue, StringComparison.Ordinal);
        Assert.All(problems, p =>
        {
            Assert.DoesNotContain(shared, p, StringComparison.Ordinal);
            Assert.DoesNotContain(legacy, p, StringComparison.Ordinal);
        });
        Assert.Contains(problems, p => p.Contains("legacy", StringComparison.Ordinal));
    }

    [Fact]
    public void Check_WithNoNamedTokens_SaysNothingAndServesNothingExtra()
    {
        var problems = DarlingMcpTokenSet.Check(new McpConfig(), "legacy-token-value-0123456789abcdef", DarlingMcpTokenSet.ResolveEntry, out var usable);

        Assert.Empty(problems);
        Assert.Empty(usable);
        Assert.False(SetFor(new McpConfig(), "legacy-token-value-0123456789abcdef").HasNamed);
    }

    [Fact]
    public void ANamedTokenThatCannotBeServed_StillCountsAsNamedTokensAsked_SoTheStartDoesNotGoOpen()
    {
        var mcp = new McpConfig { Tokens = new List<McpTokenEntry> { Entry("broken", "tooshort", "agent") } };

        Assert.True(mcp.HasNamedTokens);
        Assert.False(SetFor(mcp).HasAnyToken);
    }

    /// <summary>Review round 2: a legacy token beside <c>mcp.tokens</c> is a supported setup, so its advice logs at Warning. A token
    /// that is not served logs at Critical.</summary>
    [Fact]
    public void Resolve_LogsTheLegacyBesideNamedAdviceAtWarning_AndAnUnservedTokenAtCritical()
    {
        const string legacy = "legacy-single-token-value-cccccc-3";
        var advice = new CapturingTestLogger();
        DarlingMcpTokenSet.Resolve(TwoTokens(), legacy, DarlingMcpWritePolicy.Effective.Off, advice);
        Assert.Equal(1, advice.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, advice.CountAtLevel(LogLevel.Critical));

        var unserved = new CapturingTestLogger();
        var broken = new McpConfig { Tokens = new List<McpTokenEntry> { Entry("ops-lead", OperatorToken, "operator"), Entry("bad", "tooshort", "agent") } };
        DarlingMcpTokenSet.Resolve(broken, legacy, DarlingMcpWritePolicy.Effective.Off, unserved);
        Assert.Equal(1, unserved.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1, unserved.CountAtLevel(LogLevel.Critical));
    }

    [Fact]
    public void AnAgentTokenName_IsTheAgentIdTheToolsHoldTheCallerTo_AndAnOperatorTokenHasNone()
    {
        var set = SetFor(TwoTokens());

        Assert.Equal("triage-bot", set.PolicyFor(set.Named.Single(n => n.Name == "triage-bot")).AgentTokenName);
        Assert.Null(set.PolicyFor(set.Named.Single(n => n.Name == "ops-lead")).AgentTokenName);
        Assert.Null(new DarlingMcpWritePolicy.Effective(false, false, true).AgentTokenName);
    }

    [Fact]
    public void TheLogIdentity_IsTheTokensName_AndOnlyTheLegacyTokenShowsAFingerprint()
    {
        Assert.Equal("named token triage-bot", ReadCaller.OfMcpIdentity("triage-bot", AgentToken));
        Assert.DoesNotContain(AgentToken, ReadCaller.OfMcpIdentity("triage-bot", AgentToken), StringComparison.Ordinal);
        Assert.Equal(ReadCaller.OfMcpToken("legacy-single-token-value-cccccc-3"), ReadCaller.OfMcpIdentity(null, "legacy-single-token-value-cccccc-3"));
        Assert.Equal("no token", ReadCaller.OfMcpIdentity(null, null));
    }

    [Fact]
    public async Task ValidateConfig_PrintsTheNamedTokenFindings_WheneverTokensAreSet_EvenWithInvestigationsOff()
    {
        const string shared = "shared-token-value-0123456789abcdef";
        var config = new DarlingConfig
        {
            Mcp = new McpConfig { Tokens = new List<McpTokenEntry> { Entry("one", shared, "operator"), Entry("two", shared, "agent"), Entry("bad name", shared, "agent") } },
        };
        var output = new StringWriter();

        await DarlingCliCommands.ReportAgentDispatchAsync(config, output, TestContext.Current.CancellationToken);

        var text = output.ToString();
        Assert.Contains("WARNING: ", text, StringComparison.Ordinal);
        Assert.Contains("share a token value", text, StringComparison.Ordinal);
        Assert.Contains("no valid name", text, StringComparison.Ordinal);
        Assert.DoesNotContain(shared, text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateConfig_SaysNothing_WhenNoNamedTokensAreSet_AndInvestigationsAreOff()
    {
        var output = new StringWriter();

        await DarlingCliCommands.ReportAgentDispatchAsync(new DarlingConfig(), output, TestContext.Current.CancellationToken);

        Assert.Equal("", output.ToString());
    }

    [Fact]
    public void NamedTokenFindings_WarnWhenNoAgentTokenIsServedWhileARouteHasAnAgentLevel_AndOnlyThen()
    {
        var operatorOnly = new McpConfig { Tokens = new List<McpTokenEntry> { Entry("ops-lead", OperatorToken, "operator") } };
        Assert.Contains(DarlingAgentDispatchChecks.NoAgentTokenMessage, DarlingAgentDispatchChecks.NamedTokenFindings(operatorOnly, "", agentRouteCount: 2));
        Assert.DoesNotContain(DarlingAgentDispatchChecks.NoAgentTokenMessage, DarlingAgentDispatchChecks.NamedTokenFindings(operatorOnly, "", agentRouteCount: 0));
        Assert.DoesNotContain(DarlingAgentDispatchChecks.NoAgentTokenMessage, DarlingAgentDispatchChecks.NamedTokenFindings(operatorOnly, "", agentRouteCount: null));
        Assert.DoesNotContain(DarlingAgentDispatchChecks.NoAgentTokenMessage, DarlingAgentDispatchChecks.NamedTokenFindings(TwoTokens(), "", agentRouteCount: 2));
    }
}
