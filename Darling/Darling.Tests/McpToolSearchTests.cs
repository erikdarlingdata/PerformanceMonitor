/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9388 <c>find_tools</c> on Darling. The match, rank and render rules run on the shared <see cref="McpToolSearch"/> over small
/// hand-built tools. The served-set rule runs through the host's own transport: a request is searched over the tools ITS
/// <c>tools/list</c> serves, so a tool that a token's policy or a write switch leaves out is never named. Lite's twin is
/// <c>Lite.Tests.McpToolSearchTests</c>.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpToolSearchTests
{
    private static McpServerTool Tool(string name, string description) =>
        McpServerTool.Create(static (string server_name) => server_name, new McpServerToolCreateOptions { Name = name, Description = description, ReadOnly = true });

    private static JsonDocument Answer(IEnumerable<McpServerTool> served, string? query, string? detail = null, int? limit = null) =>
        JsonDocument.Parse(McpToolSearch.Render(served, query, detail, limit));

    private static string[] NamesOf(JsonDocument answer) =>
        answer.RootElement.GetProperty("tools").EnumerateArray()
            .Select(t => t.ValueKind == JsonValueKind.String ? t.GetString()! : t.GetProperty("name").GetString()!)
            .ToArray();

    private static string[] Notes(JsonDocument answer) =>
        answer.RootElement.TryGetProperty("notes", out var notes) ? notes.EnumerateArray().Select(n => n.GetString()!).ToArray() : Array.Empty<string>();

    [Fact]
    public void Rank_MoreWordsInTheNameFirst_ThenMoreInTheHead_ThenNameOrder()
    {
        var served = new[]
        {
            Tool("get_waiting_tasks", "Tasks waiting now."),
            Tool("get_memory_stats", "Memory stats. Includes a wait column."),
            Tool("get_wait_stats", "Wait totals."),
            Tool("get_cpu_stats", "CPU stats."),
            Tool("get_cpu_stats_b", "CPU stats."),
            Tool("list_servers", "Lists servers."),
        };

        using var answer = Answer(served, "wait stats", "names");

        /* get_wait_stats has both words in its name. get_memory_stats has one in its name and both in its head. The two cpu tools and
           get_waiting_tasks each have one word in the name and one in the head, so the name decides. list_servers has neither. */
        Assert.Equal(new[] { "get_wait_stats", "get_memory_stats", "get_cpu_stats", "get_cpu_stats_b", "get_waiting_tasks" }, NamesOf(answer));
        Assert.Equal(5, answer.RootElement.GetProperty("matched").GetInt32());
    }

    [Fact]
    public void AToolMatchesOnAWordInItsDescription_WhenItsNameHasNone()
    {
        using var answer = Answer(new[] { Tool("get_x", "Reads the DEADLOCK graph."), Tool("get_y", "Reads something else.") }, "deadlock", "names");

        Assert.Equal(new[] { "get_x" }, NamesOf(answer));
    }

    [Fact]
    public void TheSearchToolIsNeverInItsOwnResults()
    {
        using var answer = Answer(new[] { Tool(McpToolSearch.ToolName, "Searches tools."), Tool("get_tools_used", "Tools used.") }, "tools", "names");

        Assert.Equal(new[] { "get_tools_used" }, NamesOf(answer));
    }

    [Fact]
    public void Names_Summaries_AndFull_CarryWhatTheyPromise()
    {
        var served = new[]
        {
            Tool("get_wait_stats", "Wait totals by type. A second sentence that a summary leaves out." + McpToolGuide.GuidePointer),
            Tool("get_guideless_wait", "Wait without a guide."),
            Tool("get_tool_guide", "Guides."),
        };

        using var names = Answer(served, "wait", "names");
        Assert.All(names.RootElement.GetProperty("tools").EnumerateArray(), t => Assert.Equal(JsonValueKind.String, t.ValueKind));

        using var summaries = Answer(served, "wait");
        Assert.Equal("summaries", summaries.RootElement.GetProperty("detail").GetString());
        var first = summaries.RootElement.GetProperty("tools").EnumerateArray().First(t => t.GetProperty("name").GetString() == "get_wait_stats");
        Assert.Equal("Wait totals by type.", first.GetProperty("summary").GetString());

        using var full = Answer(served, "wait", "full");
        var entries = full.RootElement.GetProperty("tools").EnumerateArray().ToDictionary(t => t.GetProperty("name").GetString()!);
        var guided = entries["get_wait_stats"];
        /* The head, without the pointer the served description adds, and the registered tool's own schema and hints. */
        Assert.Equal("Wait totals by type. A second sentence that a summary leaves out.", guided.GetProperty("description").GetString());
        Assert.True(guided.GetProperty("input_schema").GetProperty("properties").TryGetProperty("server_name", out _));
        Assert.True(guided.GetProperty("annotations").GetProperty("readOnlyHint").GetBoolean());
        Assert.True(guided.GetProperty("has_reading_guide").GetBoolean());
        Assert.False(entries["get_guideless_wait"].TryGetProperty("has_reading_guide", out _));
    }

    [Fact]
    public void Limit_DefaultsTo20_AndAValueOutOfRangeIsCutAndSaid()
    {
        var served = Enumerable.Range(0, 70).Select(i => Tool($"get_probe_{i:D2}", "A probe.")).ToArray();

        using var byDefault = Answer(served, "probe", "names");
        Assert.Equal(McpToolSearch.DefaultLimit, NamesOf(byDefault).Length);
        Assert.Equal(70, byDefault.RootElement.GetProperty("matched").GetInt32());

        using var tooMany = Answer(served, "probe", "names", 500);
        Assert.Equal(McpToolSearch.MaxLimit, NamesOf(tooMany).Length);
        Assert.NotEmpty(Notes(tooMany));

        using var tooManyFull = Answer(served, "probe", "full", 99);
        Assert.Equal(McpToolSearch.MaxFullLimit, NamesOf(tooManyFull).Length);
        Assert.NotEmpty(Notes(tooManyFull));

        using var fullByDefault = Answer(served, "probe", "full");
        Assert.Equal(McpToolSearch.MaxFullLimit, NamesOf(fullByDefault).Length);

        using var none = Answer(served, "probe", "names", 0);
        Assert.Single(NamesOf(none));
        Assert.NotEmpty(Notes(none));

        using var exact = Answer(served, "probe", "names", 7);
        Assert.Equal(7, NamesOf(exact).Length);
    }

    [Theory]
    [InlineData("names", 51)]
    [InlineData("full", 11)]
    [InlineData("summaries", 0)]
    public void ALimitOutOfRange_IsRefusedByTheTool_WithTheSharedEnvelope(string detail, int limit)
    {
        /* The refusal comes before the request's server is read, so no host is needed. */
        using var answer = JsonDocument.Parse(DarlingMcpToolSearchTools.FindTools(null!, "wait", detail, limit));

        Assert.Equal("limit", answer.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
    }

    [Fact]
    public void ANoMatchAndAnEmptyQuery_AreAnsweredNotThrown()
    {
        var served = new[] { Tool("get_wait_stats", "Wait totals.") };

        using var noMatch = Answer(served, "zzzz");
        Assert.Equal("no_match", noMatch.RootElement.GetProperty("status").GetString());
        Assert.Equal(0, noMatch.RootElement.GetProperty("matched").GetInt32());
        Assert.False(string.IsNullOrWhiteSpace(noMatch.RootElement.GetProperty("message").GetString()));

        foreach (var query in new string?[] { null, "", "  ,  " })
        {
            using var empty = Answer(served, query);
            Assert.Equal("error", empty.RootElement.GetProperty("status").GetString());
        }

        using var unknownDetail = Answer(served, "wait", "everything");
        Assert.Equal("summaries", unknownDetail.RootElement.GetProperty("detail").GetString());
        Assert.NotEmpty(Notes(unknownDetail));
    }

    /// <summary>#9584: stateless HTTP gives the service no session to push <c>tools/list_changed</c> on, so a client that connected
    /// before an upgrade keeps its old list. Every answer that names tools, or says none match, carries one plain note that a tool
    /// named here which the client cannot call needs a reconnect. The shared note names no SKU's way round it. The empty-query error
    /// names no tools and carries no note.</summary>
    [Theory]
    [InlineData("names")]
    [InlineData("summaries")]
    [InlineData("full")]
    public void EveryAnswer_WithAMatchOrWithNone_CarriesOneReconnectNote_AtEveryDetailLevel(string detail)
    {
        var served = new[] { Tool("get_wait_stats", "Wait totals.") };

        foreach (var query in new[] { "wait", "zzzz" })
        {
            using var answer = Answer(served, query, detail);

            var note = Assert.Single(Notes(answer), n => n.Contains("reconnect", StringComparison.OrdinalIgnoreCase));
            Assert.Contains("connects", note, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("call_tool", note, StringComparison.Ordinal);
        }

        using var empty = Answer(served, "  ,  ", detail);
        Assert.Equal("error", empty.RootElement.GetProperty("status").GetString());
        Assert.Empty(Notes(empty));
    }

    [Fact]
    public void Full_StaysInsideTheResponseBudget_AndNamesWhatDidNotFit()
    {
        var big = new string('x', 6_000);
        var served = Enumerable.Range(0, 10).Select(i => Tool($"get_big_{i}", $"Big tool {i}. {big}")).ToArray();

        var text = McpToolSearch.Render(served, "big", "full", 10, DarlingMcpToolSearchTools.LeanReconnectSentence);

        Assert.True(Encoding.UTF8.GetByteCount(text) <= McpResponseBudget.DefaultBytes, "the answer is over the response budget");
        using var answer = JsonDocument.Parse(text);
        var returned = NamesOf(answer);
        var deferred = answer.RootElement.GetProperty("deferred").EnumerateArray().Select(d => d.GetString()!).ToArray();
        Assert.NotEmpty(returned);
        Assert.NotEmpty(deferred);
        Assert.Equal(10, returned.Length + deferred.Length);
    }

    /// <summary>The worst case on the real tools: <c>full</c> at its cap, for every registered tool's own name and for words that match
    /// many tools, stays inside the budget the read tools keep to.</summary>
    [Fact]
    public void Full_AtItsCap_OverTheRealRegisteredTools_StaysInsideTheResponseBudget()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        Assert.True(tools.Count > 200, "the registered tool set should be the full Darling surface");

        var queries = tools.Select(t => t.ProtocolTool.Name).Concat(new[] { "get", "stats", "wait", "query", "server", "e" }).Distinct().ToArray();
        var largest = 0;
        foreach (var query in queries)
        {
            var bytes = Encoding.UTF8.GetByteCount(McpToolSearch.Render(tools, query, "full", McpToolSearch.MaxFullLimit, DarlingMcpToolSearchTools.LeanReconnectSentence));
            largest = Math.Max(largest, bytes);
            Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"find_tools query \"{query}\" at full answers {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0} budget");
        }

        Assert.True(largest > 4_000, "the largest full answer is small, so the registered tools did not carry their schemas");
    }

    /// <summary>D6 lockstep, on the registered tool object (no source text): the name, description and parameters come from the shared
    /// constants, and the request's <c>McpServer</c> is not a parameter a client sees. Lite's twin test asserts the same values.</summary>
    [Fact]
    public void TheRegisteredTool_HasTheSharedNameDescriptionAndParameters()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var tool = tools.Single(t => t.ProtocolTool.Name == McpToolSearch.ToolName).ProtocolTool;

        Assert.Equal(McpToolSearchToolText.Description, tool.Description);
        var properties = tool.InputSchema.GetProperty("properties");
        Assert.Equal(new[] { "detail", "limit", "query" }, properties.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(McpToolSearchToolText.QueryParameter, properties.GetProperty("query").GetProperty("description").GetString());
        Assert.Equal(McpToolSearchToolText.DetailParameter, properties.GetProperty("detail").GetProperty("description").GetString());
        Assert.Equal(McpToolSearchToolText.LimitParameter, properties.GetProperty("limit").GetProperty("description").GetString());
        Assert.Equal(new[] { "query" }, tool.InputSchema.GetProperty("required").EnumerateArray().Select(r => r.GetString()).ToArray());
    }

    /* ----------------- the served-set rule, through the host's own transport ----------------- */

    /// <summary>The tool names the <c>find_tools</c> answer for one query names, for the token presented.</summary>
    private static async Task<string[]> FoundAsync(Microsoft.AspNetCore.TestHost.TestServer server, string? bearer, string query, string path = "/")
    {
        var (status, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
            server, McpToolSearch.ToolName, JsonSerializer.Serialize(new { query, detail = "names", limit = 50 }), bearer, path);
        Assert.Equal(200, status);
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var response = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var result = response.RootElement.GetProperty("result");
        Assert.False(result.TryGetProperty("isError", out var isError) && isError.GetBoolean(), "find_tools answered as an error: " + body);
        using var answer = JsonDocument.Parse(result.GetProperty("content")[0].GetProperty("text").GetString()!);
        return answer.RootElement.TryGetProperty("tools", out var found)
            ? found.EnumerateArray().Select(t => t.GetString()!).ToArray()
            : Array.Empty<string>();
    }

    [Fact]
    public async Task ANamedToken_NeverSeesAToolItsPolicyDrops_ThroughFindTools()
    {
        const string operatorToken = "operator-token-value-aaaaaaaaaaaa-1";
        const string agentToken = "agent-token-value-bbbbbbbbbbbbbbb-2";
        var mcp = new McpConfig
        {
            AllowWrites = true,
            Tokens = new List<McpTokenEntry>
            {
                new() { Name = "ops-lead", Token = operatorToken, Role = "operator" },
                new() { Name = "triage-bot", Token = agentToken, Role = "agent" },
            },
        };
        var legacy = DarlingMcpWritePolicy.ResolveForStart(mcp, "", out _);
        var tokens = DarlingMcpTokenSet.Resolve(mcp, "", legacy, null);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: tokens);

        var (_, operatorList) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, operatorToken);
        var (_, agentList) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, agentToken);
        var operatorTools = DarlingMcpWritePolicyTests.ToolsOf(operatorList).Keys.ToHashSet(StringComparer.Ordinal);
        var agentTools = DarlingMcpWritePolicyTests.ToolsOf(agentList).Keys.ToHashSet(StringComparer.Ordinal);
        var dropped = operatorTools.Except(agentTools).OrderBy(n => n, StringComparer.Ordinal).Take(6).ToArray();
        Assert.NotEmpty(dropped);

        foreach (var tool in dropped)
        {
            Assert.Contains(tool, await FoundAsync(server, operatorToken, tool));
            Assert.DoesNotContain(tool, await FoundAsync(server, agentToken, tool));
        }

        /* Every name an answer carries is a tool that caller's own tools/list serves. */
        foreach (var query in new[] { "create", "delete", "update", "get", "alert" })
        {
            Assert.All(await FoundAsync(server, agentToken, query), name => Assert.Contains(name, agentTools));
            Assert.All(await FoundAsync(server, operatorToken, query), name => Assert.Contains(name, operatorTools));
        }
    }

    [Fact]
    public async Task WithTheWriteSwitchesOff_NoWriteToolIsNamed_AndTheReadsAre()
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig(), "");

        var writes = DarlingMcpWritePolicy.WriteTools.OrderBy(n => n, StringComparer.Ordinal).Take(8).ToArray();
        Assert.NotEmpty(writes);
        foreach (var write in writes)
        {
            Assert.DoesNotContain(write, await FoundAsync(server, null, write));
        }

        Assert.Contains("list_servers", await FoundAsync(server, null, "list_servers"));
        Assert.DoesNotContain(McpToolSearch.ToolName, await FoundAsync(server, null, McpToolSearch.ToolName));
    }

    [Fact]
    public async Task WithWritesOn_TheWriteToolsAreNamed()
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig { AllowWrites = true }, DarlingMcpWritePolicyTests.Token);

        Assert.Contains("create_mute_rule", await FoundAsync(server, DarlingMcpWritePolicyTests.Token, "create_mute_rule"));
    }

    /// <summary>The notes the <c>find_tools</c> answer for one query carries, through the host's own transport at the path given.</summary>
    private static async Task<string[]> NotesFromAsync(Microsoft.AspNetCore.TestHost.TestServer server, string query, string path)
    {
        var (status, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
            server, McpToolSearch.ToolName, JsonSerializer.Serialize(new { query, detail = "names", limit = 50 }), null, path);
        Assert.Equal(200, status);
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var response = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var result = response.RootElement.GetProperty("result");
        Assert.False(result.TryGetProperty("isError", out var isError) && isError.GetBoolean(), "find_tools answered as an error: " + body);
        using var answer = JsonDocument.Parse(result.GetProperty("content")[0].GetProperty("text").GetString()!);
        return Notes(answer);
    }

    /// <summary>#9584: on <c>/lean</c>, the one path that serves <c>call_tool</c>, Darling's note adds the way round a stale tool list that
    /// path has: <c>call_tool</c> runs a read tool without a reconnect, and a tool that writes, which <c>call_tool</c> refuses, still needs
    /// one. On <c>/</c>, where <c>call_tool</c> is not served, the note does not offer it. <c>/core</c> serves no <c>find_tools</c>. With a match
    /// and with none.</summary>
    [Theory]
    [InlineData("list_servers", "/", false)]
    [InlineData("zzzzqq", "/", false)]
    [InlineData("list_servers", "/lean", true)]
    [InlineData("zzzzqq", "/lean", true)]
    public async Task TheAnswer_CarriesOneReconnectNote_ThatOffersCallToolOnlyWhereItIsServed(string query, string path, bool servesCallTool)
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig(), "");

        var notes = await NotesFromAsync(server, query, path);

        var note = Assert.Single(notes, n => n.Contains("reconnect", StringComparison.OrdinalIgnoreCase));
        if (servesCallTool)
        {
            Assert.Contains("call_tool runs a read tool", note, StringComparison.Ordinal);
            Assert.Contains("A tool that writes is not run there and still needs the client to reconnect", note, StringComparison.Ordinal);
        }
        else
        {
            Assert.DoesNotContain("call_tool", note, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task TheTool_IsServedWithTheReadOnlyHint()
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig(), "");

        var (_, body) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, null);
        var tool = DarlingMcpWritePolicyTests.ToolsOf(body)[McpToolSearch.ToolName];

        Assert.True(tool.GetProperty("annotations").GetProperty("readOnlyHint").GetBoolean());
        Assert.False(tool.GetProperty("inputSchema").GetProperty("properties").TryGetProperty("server", out _));
    }
}
