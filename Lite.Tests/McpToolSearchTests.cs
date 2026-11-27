/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9388 <c>find_tools</c> on Lite, the twin of <c>Darling.Tests.McpToolSearchTests</c>. The match, rank and render rules are the
/// shared <see cref="McpToolSearch"/> and are tested there. This class checks what is Lite's own: the registered tool, the served-set
/// rule through the host's own registration, and the size of its answer over Lite's real tools.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpToolSearchTests
{
    private static async Task<string[]> FoundAsync(Microsoft.AspNetCore.TestHost.TestServer server, string query)
    {
        var body = await McpWritePolicyTests.PostAsync(server,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + McpToolSearch.ToolName + "\",\"arguments\":" +
            JsonSerializer.Serialize(new { query, detail = "names", limit = 50 }) + "}}");
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var response = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var result = response.RootElement.GetProperty("result");
        Assert.False(result.TryGetProperty("isError", out var isError) && isError.GetBoolean(), "find_tools answered as an error: " + body);
        using var answer = JsonDocument.Parse(result.GetProperty("content")[0].GetProperty("text").GetString()!);
        return answer.RootElement.TryGetProperty("tools", out var found)
            ? found.EnumerateArray().Select(t => t.GetString()!).ToArray()
            : Array.Empty<string>();
    }

    /// <summary>D6 lockstep, on the registered tool object (no source text): the same shared name, description and parameters as
    /// Darling's registered tool, which its own test asserts against the same constants.</summary>
    [Fact]
    public void TheRegisteredTool_HasTheSharedNameDescriptionAndParameters()
    {
        var tool = McpToolsListBudgetTests.BuildServedTools().Tools
            .Single(t => t.ProtocolTool.Name == McpToolSearch.ToolName).ProtocolTool;

        Assert.Equal(McpToolSearchToolText.Description, tool.Description);
        var properties = tool.InputSchema.GetProperty("properties");
        Assert.Equal(new[] { "detail", "limit", "query" }, properties.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(McpToolSearchToolText.QueryParameter, properties.GetProperty("query").GetProperty("description").GetString());
        Assert.Equal(McpToolSearchToolText.DetailParameter, properties.GetProperty("detail").GetProperty("description").GetString());
        Assert.Equal(McpToolSearchToolText.LimitParameter, properties.GetProperty("limit").GetProperty("description").GetString());
        Assert.Equal(new[] { "query" }, tool.InputSchema.GetProperty("required").EnumerateArray().Select(r => r.GetString()).ToArray());
    }

    [Fact]
    public async Task WithWritesOff_NoWriteToolOrInvestigationToolIsNamed_AndTheReadsAre()
    {
        using var server = await McpWritePolicyTests.BuildServer(allowWrites: false);

        foreach (var hidden in new[] { "mute_analysis_finding", "unmute_analysis_finding", "claim_investigation_request", "submit_investigation_report" })
        {
            Assert.DoesNotContain(hidden, await FoundAsync(server, hidden));
        }

        Assert.Contains("list_servers", await FoundAsync(server, "list_servers"));
        Assert.DoesNotContain(McpToolSearch.ToolName, await FoundAsync(server, McpToolSearch.ToolName));
    }

    /// <summary>The notes the <c>find_tools</c> answer for one query carries.</summary>
    private static async Task<string[]> NotesFromAsync(Microsoft.AspNetCore.TestHost.TestServer server, string query)
    {
        var body = await McpWritePolicyTests.PostAsync(server,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + McpToolSearch.ToolName + "\",\"arguments\":" +
            JsonSerializer.Serialize(new { query, detail = "names", limit = 50 }) + "}}");
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var response = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var result = response.RootElement.GetProperty("result");
        Assert.False(result.TryGetProperty("isError", out var isError) && isError.GetBoolean(), "find_tools answered as an error: " + body);
        using var answer = JsonDocument.Parse(result.GetProperty("content")[0].GetProperty("text").GetString()!);
        return answer.RootElement.TryGetProperty("notes", out var notes)
            ? notes.EnumerateArray().Select(n => n.GetString()!).ToArray()
            : Array.Empty<string>();
    }

    /// <summary>#9584: Lite serves MCP over stateless HTTP too, so a client that connected before an upgrade keeps its old tool list. The
    /// answer carries one plain note that a tool named here which the client cannot call needs a reconnect, with a match and with none.
    /// Lite has no <c>call_tool</c>, so its note does not name one.</summary>
    [Theory]
    [InlineData("list_servers")]
    [InlineData("zzzzqq")]
    public async Task TheAnswer_CarriesOneReconnectNote_ThatNamesNoCallTool(string query)
    {
        using var server = await McpWritePolicyTests.BuildServer(allowWrites: false);

        var notes = await NotesFromAsync(server, query);

        var note = Assert.Single(notes, n => n.Contains("reconnect", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("connects", note, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("call_tool", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WithWritesOn_TheWriteToolsAreNamed()
    {
        using var server = await McpWritePolicyTests.BuildServer(allowWrites: true);

        Assert.Contains("mute_analysis_finding", await FoundAsync(server, "mute_analysis_finding"));
    }

    /// <summary>The worst case on Lite's real tools: <c>full</c> at its cap, for every tool's own name and for words that match many
    /// tools, stays inside the budget the read tools keep to.</summary>
    [Fact]
    public void Full_AtItsCap_OverTheRealRegisteredTools_StaysInsideTheResponseBudget()
    {
        var tools = McpToolsListBudgetTests.BuildServedTools().Tools;
        Assert.True(tools.Count > 80, "the registered tool set should be the full Lite surface");

        var queries = tools.Select(t => t.ProtocolTool.Name).Concat(new[] { "get", "stats", "wait", "query", "server", "e" }).Distinct().ToArray();
        var largest = 0;
        foreach (var query in queries)
        {
            var bytes = Encoding.UTF8.GetByteCount(McpToolSearch.Render(tools, query, "full", McpToolSearch.MaxFullLimit));
            largest = Math.Max(largest, bytes);
            Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"find_tools query \"{query}\" at full answers {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0} budget");
        }

        Assert.True(largest > 4_000, "the largest full answer is small, so the registered tools did not carry their schemas");
    }
}
