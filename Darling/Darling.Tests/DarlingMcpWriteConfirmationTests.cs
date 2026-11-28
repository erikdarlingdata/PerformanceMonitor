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
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live cases mint their own scratch database through ScratchPostgres and touch nothing on the shared one. */

/// <summary>
/// The destructive write tools ask the user to confirm before they run (#9562), through the real MCP host (the builder, the filter
/// pipeline and the stateless endpoint production runs). A client that negotiated the 2026-07-28 multi round-trip flow gets one
/// question and nothing changes until the call comes back with an answer. Every other client runs as before.
/// </summary>
public sealed class DarlingMcpWriteConfirmationTests
{
    private const string Token = DarlingMcpWritePolicyTests.Token;

    private static readonly McpConfig Config = new() { AllowWrites = true, AllowServerAdmin = true };

    /// <summary>One row per listed tool except <c>purge_now</c> (its question reads the store, so the live cases cover it):
    /// the tool, a call that would change something, and what its question names.</summary>
    public static IEnumerable<object[]> ListedTools() =>
    [
        ["remove_server", "{\"server_name\":\"sql-prod-7\"}", "sql-prod-7"],
        ["remove_webhook_signing_key", "{\"route_id\":41}", "route 41"],
        ["remove_webhook_signing_key", "{}", "default webhook destination"],
        ["delete_custom_alert_rule", "{\"rule_id\":55}", "custom alert rule 55"],
        ["delete_custom_view", "{\"view_id\":66}", "custom view 66"],
        ["delete_known_pattern", "{\"id\":77}", "known pattern 77"],
        ["delete_mute_rule", "{\"rule_id\":\"mute-88\"}", "mute-88"],
        ["delete_notification_route", "{\"route_id\":99}", "notification route 99"],
        ["delete_query_tag", "{\"tag_id\":12}", "query tag 12"],
        ["delete_query_tag_rule", "{\"rule_id\":13}", "query tag rule 13"],
        ["delete_server_tag", "{\"tag_id\":14}", "server tag 14"],
        ["drop_plan_guide", "{\"database_name\":\"Sales\",\"name\":\"PG_orders\",\"server_name\":\"sql-prod-7\",\"dry_run\":false}", "PG_orders"],
        ["purge_expired_mute_rules", "{}", "expired"],
        ["reset_collector_schedules", "{\"server_name\":\"sql-prod-7\"}", "sql-prod-7"],
        ["reset_collector_schedules", "{}", "fleet-wide"],
        ["clear_change_window", "{\"window_id\":21,\"reason\":\"cancelled\"}", "change window 21"],
        ["clear_quiet_window", "{\"rule_id\":\"quiet-31\"}", "quiet-31"],
    ];

    [Theory]
    [MemberData(nameof(ListedTools))]
    public async Task ANegotiatingClient_IsAskedOneQuestion_ThatNamesWhatWillChange(string tool, string arguments, string named)
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(Config, Token);

        var answer = await CallAsync(server, tool, arguments, negotiate: true);

        var question = QuestionOf(answer);
        Assert.NotNull(question);
        Assert.Contains(named, question, StringComparison.Ordinal);
        Assert.Equal("elicitation/create", MethodOf(answer));
    }

    [Fact]
    public async Task EveryListedTool_IsAToolTheHostServes()
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(Config, Token);
        var list = await DarlingMcpWritePolicyTests.ToolsListAsync(server, Token);
        var served = DarlingMcpWritePolicyTests.ToolsOf(list.Body).Keys.ToHashSet(StringComparer.Ordinal);

        var missing = DarlingMcpWriteConfirmation.ToolNames.Where(n => !served.Contains(n)).ToList();

        Assert.True(missing.Count == 0, "Listed for confirmation but not served: " + string.Join(", ", missing));
    }

    [Theory]
    [InlineData("decline")]
    [InlineData("cancel")]
    public async Task ADeclinedOrDismissedQuestion_AnswersDeclined_AsANormalResult(string action)
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(Config, Token);

        var answer = await CallAsync(server, "delete_known_pattern", "{\"id\":77}", negotiate: true, inputResponse: $"{{\"action\":\"{action}\"}}");

        var result = ResultOf(answer);
        Assert.False(result.TryGetProperty("isError", out var isError) && isError.GetBoolean());
        using var body = JsonDocument.Parse(result.GetProperty("content")[0].GetProperty("text").GetString()!);
        Assert.Equal("declined", body.RootElement.GetProperty("status").GetString());
        Assert.False(body.RootElement.GetProperty("changed").GetBoolean());
    }

    [Fact]
    public async Task AnAcceptWithTheBoxCleared_IsNo()
    {
        using var server = await DarlingMcpWritePolicyTests.BuildServer(Config, Token);

        var answer = await CallAsync(
            server, "delete_known_pattern", "{\"id\":77}", negotiate: true, inputResponse: "{\"action\":\"accept\",\"content\":{\"confirm\":false}}");

        using var body = JsonDocument.Parse(ResultOf(answer).GetProperty("content")[0].GetProperty("text").GetString()!);
        Assert.Equal("declined", body.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task AToolThatIsNotServed_IsNeverAskedAbout()
    {
        /* Writes off: the tool is unknown to the caller, so the call ends in the SDK's unknown-tool error and no question is sent. */
        using var server = await DarlingMcpWritePolicyTests.BuildServer(new McpConfig(), Token);

        var answer = await CallAsync(server, "delete_known_pattern", "{\"id\":77}", negotiate: true);

        Assert.Null(QuestionOf(answer));
    }

    /// <summary>
    /// A tools/call the way a 2026-07-28 client sends one: the protocol version and the client's capabilities travel in the request's own
    /// <c>_meta</c> (the stateless endpoint has no session to hold them). <paramref name="negotiate"/> false sends a bare call, as an
    /// older client does. <paramref name="inputResponse"/> is the answer a client sends back when it asks again.
    /// </summary>
    internal static async Task<string> CallAsync(
        TestServer server, string tool, string argumentsJson, bool negotiate, string? inputResponse = null, bool elicitation = true)
    {
        var meta = negotiate
            ? "\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
              + "\"io.modelcontextprotocol/clientInfo\":{\"name\":\"confirm-test\",\"version\":\"1\"},"
              + "\"io.modelcontextprotocol/clientCapabilities\":" + (elicitation ? "{\"elicitation\":{\"form\":{}}}" : "{}") + "},"
            : string.Empty;
        var responses = inputResponse is null ? string.Empty : "\"inputResponses\":{\"" + DarlingMcpWriteConfirmation.InputKey + "\":" + inputResponse + "},";
        var json = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{" + meta + responses
            + "\"name\":\"" + tool + "\",\"arguments\":" + argumentsJson + "}}";

        var context = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            if (negotiate)
            {
                c.Request.Headers["MCP-Protocol-Version"] = "2026-07-28";
                c.Request.Headers["Mcp-Method"] = "tools/call";
                c.Request.Headers["Mcp-Name"] = tool;
            }

            var bytes = Encoding.UTF8.GetBytes(json);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
            c.Request.Headers.Authorization = $"Bearer {Token}";
        });

        using var reader = new System.IO.StreamReader(context.Response.Body);
        return await reader.ReadToEndAsync();
    }

    private static JsonElement ResultOf(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var root = document.RootElement;
        Assert.True(root.TryGetProperty("result", out var result), "No result in: " + body);
        return result.Clone();
    }

    private static JsonElement? InputRequestOf(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        if (!document.RootElement.TryGetProperty("result", out var result)
            || !result.TryGetProperty("inputRequests", out var requests)
            || !requests.TryGetProperty(DarlingMcpWriteConfirmation.InputKey, out var request))
        {
            return null;
        }

        return request.Clone();
    }

    internal static string? QuestionOf(string body) =>
        InputRequestOf(body) is { } request ? request.GetProperty("params").GetProperty("message").GetString() : null;

    internal static string? MethodOf(string body) =>
        InputRequestOf(body) is { } request ? request.GetProperty("method").GetString() : null;
}
