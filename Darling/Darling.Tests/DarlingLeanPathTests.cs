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
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9388: the <c>/lean</c> path and <c>call_tool</c>, on the real MCP host behind an in-memory server. The point of the class is the
/// risk in <c>call_tool</c>: a call through it must pass every call-tool filter a direct call passes, so the same arguments, the same
/// output filter and the same latency record apply.
/// </summary>
[Collection("DarlingOutputFormatEnv")]
public sealed class DarlingLeanPathTests
{
    private static readonly string[] Leans = ["list_servers", "get_fleet_overview", "analyze_server", "get_tool_guide"];

    /// <summary>A read tool that is not on /lean's list and needs no store.</summary>
    private const string UnlistedRead = "describe_custom_view_catalog";

    private static async Task<(bool IsError, string Text)> AnswerAsync(
        TestServer server, string path, string tool, JsonObject arguments)
    {
        try
        {
            var (text, isError) = await StatementFilterCensus.CallRealToolAsync(server, tool, arguments, path);
            return (isError, text);
        }
        catch (InvalidOperationException ex)
        {
            return (true, ex.Message);
        }
    }

    private static JsonObject ViaCallTool(string name, JsonObject? arguments) =>
        new() { ["name"] = name, ["arguments"] = arguments };

    [Fact]
    public async Task LeanListsTheEntryToolsAndCallToolOnly_AndTheOtherPathsDoNotListCallTool()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();

        var lean = await StatementFilterCensus.ListToolNamesAsync(server, "/lean");
        var root = await StatementFilterCensus.ListToolNamesAsync(server, "/");
        var core = await StatementFilterCensus.ListToolNamesAsync(server, "/core");

        var expected = Leans.Append("call_tool").ToList();
        if (root.Contains("find_tools"))
            expected.Add("find_tools");
        Assert.Equal(expected.Order(StringComparer.Ordinal), lean.Order(StringComparer.Ordinal));
        Assert.DoesNotContain("call_tool", root);
        Assert.DoesNotContain("call_tool", core);
        Assert.True(root.Count > 50, "the full path still lists its whole set");
    }

    [Theory]
    [InlineData("/")]
    [InlineData("/core")]
    public async Task CallToolOnTheOtherPaths_GetsTheUnknownToolAnswer(string path)
    {
        using var server = await StatementFilterCensus.BuildHostAsync();

        var (isError, text) = await AnswerAsync(server, path, "call_tool", ViaCallTool("list_servers", new JsonObject()));
        var (_, unknown) = await AnswerAsync(server, path, "no_such_tool_9388", new JsonObject());

        Assert.True(isError);
        Assert.Contains("Unknown tool", text);
        Assert.Equal(unknown.Replace("no_such_tool_9388", "call_tool"), text);
    }

    [Fact]
    public async Task CallToolRunsAReadToolThatLeanDoesNotList_AndAnswersAsTheDirectCallDoes()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();
        Assert.DoesNotContain(UnlistedRead, await StatementFilterCensus.ListToolNamesAsync(server, "/lean"));

        var direct = await AnswerAsync(server, "/", UnlistedRead, new JsonObject());
        var viaCall = await AnswerAsync(server, "/lean", "call_tool", ViaCallTool(UnlistedRead, new JsonObject()));
        var viaCallNoArguments = await AnswerAsync(server, "/lean", "call_tool", ViaCallTool(UnlistedRead, null));
        var directOnLean = await AnswerAsync(server, "/lean", UnlistedRead, new JsonObject());

        Assert.False(direct.IsError);
        Assert.Equal(direct, viaCall);
        Assert.Equal(direct, viaCallNoArguments);
        Assert.Equal(direct, directOnLean);
    }

    [Fact]
    public async Task LeanFindToolsNamesAToolLeanDoesNotList_AndCallToolRunsIt()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();
        Assert.DoesNotContain(UnlistedRead, await StatementFilterCensus.ListToolNamesAsync(server, "/lean"));

        var found = await AnswerAsync(server, "/lean", "find_tools", new JsonObject { ["query"] = UnlistedRead, ["detail"] = "names" });
        var ran = await AnswerAsync(server, "/lean", "call_tool", ViaCallTool(UnlistedRead, new JsonObject()));

        Assert.False(found.IsError);
        Assert.Contains(UnlistedRead, found.Text);
        Assert.False(ran.IsError);
        Assert.Equal(await AnswerAsync(server, "/", UnlistedRead, new JsonObject()), ran);
    }

    [Fact]
    public async Task AnUnknownArgumentThroughCallTool_GetsTheAnswerADirectCallGets()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();
        var bogus = new JsonObject { ["no_such_argument_9388"] = 1 };

        var direct = await AnswerAsync(server, "/", UnlistedRead, bogus.DeepClone().AsObject());
        var viaCall = await AnswerAsync(server, "/lean", "call_tool", ViaCallTool(UnlistedRead, bogus.DeepClone().AsObject()));

        Assert.Contains("no_such_argument_9388", direct.Text);
        Assert.Equal(direct, viaCall);
    }

    [Fact]
    public async Task TheStatementFilterSweepsAToolsOutputThroughCallTool_AsItDoesADirectCall()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();
        var rows = new JsonArray
        {
            new JsonObject { ["query_text"] = StatementScrubCanary.CanaryStatement, ["executions"] = 1 },
            new JsonObject { ["query_text"] = StatementScrubCanary.PlainStatement, ["executions"] = 2 },
        };
        string raw = new JsonObject { ["rows"] = rows }.ToJsonString();
        StatementFilterCensus.AssertRawHoldsTheCanary(raw);

        var direct = await StatementFilterCensus.FilterThroughHostAsync(server, raw);
        var viaCall = await StatementFilterCensus.FilterThroughHostAsync(server, raw, viaCallTool: true);

        Assert.DoesNotContain("S3cret-canary-ssf", direct.Text);
        Assert.Equal(direct, viaCall);
    }

    [Fact]
    public async Task EveryWriteToolIsRefusedThroughCallTool_EvenWhereTheTokenCanCallItOnTheFullPath()
    {
        var union = DarlingMcpWritePolicy.WriteTools
            .Concat(DarlingMcpWritePolicy.InvestigationTools)
            .Concat(DarlingMcpWritePolicy.ServerAdminTools)
            .Concat(DarlingMcpWritePolicy.AgentActionTools)
            .ToHashSet(StringComparer.Ordinal);
        var exercised = new HashSet<string>(StringComparer.Ordinal);

        foreach (var writes in new[]
        {
            new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: true),
            new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true),
        })
        {
            using var server = await StatementFilterCensus.BuildHostAsync(writes: writes);
            var served = await StatementFilterCensus.ListToolNamesAsync(server, "/");
            foreach (var name in union.Where(served.Contains))
            {
                exercised.Add(name);
                var (isError, text) = await AnswerAsync(server, "/lean", "call_tool", ViaCallTool(name, new JsonObject()));
                Assert.True(isError, name);
                Assert.Contains("read tools only", text);
                Assert.Contains("full path", text);
            }
        }

        Assert.Equal(union.Order(StringComparer.Ordinal), exercised.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task AToolThePolicyDrops_AndCallToolNamingItself_AreRefused()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();

        Assert.DoesNotContain("add_servers", await StatementFilterCensus.ListToolNamesAsync(server, "/"));
        var (_, unknown) = await AnswerAsync(server, "/lean", "no_such_tool_9388", new JsonObject());
        var dropped = await AnswerAsync(server, "/lean", "call_tool", ViaCallTool("add_servers", new JsonObject()));
        var self = await AnswerAsync(server, "/lean", "call_tool", ViaCallTool("call_tool", new JsonObject()));
        var notAnObject = await AnswerAsync(server, "/lean", "call_tool", new JsonObject { ["name"] = "list_servers", ["arguments"] = "x" });

        Assert.True(dropped.IsError);
        Assert.Equal(unknown.Replace("no_such_tool_9388", "add_servers"), dropped.Text);
        Assert.True(self.IsError);
        Assert.Contains("does not run itself", self.Text);
        Assert.True(notAnObject.IsError);
        Assert.Contains("JSON object", notAnObject.Text);
    }

    [Fact]
    public async Task TheLatencyRecordOfACallThroughCallTool_NamesTheInnerTool()
    {
        var latency = new ReadLatencyAccumulator();
        using var server = await StatementFilterCensus.BuildHostAsync(readLatency: latency);

        await AnswerAsync(server, "/lean", "call_tool", ViaCallTool(UnlistedRead, new JsonObject()));

        var routes = latency.Snapshot().Select(r => r.Route).ToList();
        Assert.Contains(UnlistedRead, routes);
        Assert.DoesNotContain("call_tool", routes);
    }

    [Fact]
    public async Task LeanIsRefusedByTheHostTokenAndJsonGuards_AsTheFullPathIs()
    {
        using var server = await StatementFilterCensus.BuildHostAsync(requireTokenWhenLoopbackOnly: true);
        const string list = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}";

        async Task<int> StatusAsync(string path, string host, string? token, string contentType)
        {
            var ctx = await server.SendAsync(c =>
            {
                c.Request.Method = "POST";
                c.Request.Path = path;
                c.Request.Headers.Host = host;
                c.Request.Headers.Accept = "application/json, text/event-stream";
                if (token is not null)
                    c.Request.Headers.Authorization = "Bearer " + token;
                c.Request.ContentType = contentType;
                var bytes = Encoding.UTF8.GetBytes(list);
                c.Request.Body = new System.IO.MemoryStream(bytes);
                c.Request.ContentLength = bytes.Length;
                c.Connection.RemoteIpAddress = IPAddress.Loopback;
            });
            return ctx.Response.StatusCode;
        }

        (string Host, string? Token, string Type, int Expected)[] cases =
        [
            ("localhost", null, "application/json", 401),
            ("localhost", "wrong-token", "application/json", 401),
            ("evil.example", StatementFilterCensus.CensusToken, "application/json", 400),
            ("localhost", StatementFilterCensus.CensusToken, "text/plain", 415),
            ("localhost", StatementFilterCensus.CensusToken, "application/json", 200),
        ];
        foreach (var (host, token, type, expected) in cases)
        {
            var full = await StatusAsync("/", host, token, type);
            var lean = await StatusAsync("/lean", host, token, type);
            Assert.Equal(expected, full);
            Assert.Equal(full, lean);
        }
    }
}
