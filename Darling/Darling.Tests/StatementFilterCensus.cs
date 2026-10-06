/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348: the read-time census harness. A read's census plants <see cref="StatementScrubCanary"/> values in the
/// read's source rows, runs the read RAW (the tool method alone, which must still HOLD the canary: the control that
/// proves the plant reached the output) and FILTERED (the same text sent through the host's REAL registered
/// call-tool filter list, with or without <c>DARLING_OUTPUT_FORMAT=gcf</c>), and then asserts what the filtered
/// text may hold. The harness is read-only for its users: a per-read test calls <see cref="FilterThroughHostAsync"/>
/// (or <see cref="CallRealToolAsync"/> for a tool that needs no store) and the <c>Assert*</c> helpers, and builds
/// no host of its own.
///
/// <para>The host is the real <c>DarlingMcpHostService.ConfigureMcpServices</c> plus <c>ConfigurePipeline</c>
/// pair behind an in-memory server, with one test-only tool (<see cref="ProbeTools"/>) added after it. The probe
/// answers whatever text a test registered under a key, so the text a real read produced reaches the registered
/// filters exactly as a tool's return value does. Tests that call it and change the environment variable belong to
/// the <c>DarlingOutputFormatEnv</c> collection.</para>
/// </summary>
internal static class StatementFilterCensus
{
    /// <summary>The statement filter's marker, the only thing a withheld value may be replaced by.</summary>
    public const string Marker = SensitiveStatements.PlaceholderText;

    private const string ProbeName = "ssf_census_probe";

    private static readonly ConcurrentDictionary<string, (string Text, bool IsError)> s_payloads = new();

    /// <summary>The one test-only tool, registered ONLY on a census host (never in production's
    /// <c>ConfigureMcpServices</c>). It answers the payload registered under <c>key</c>, as a text block, the way a
    /// tool that returned that string would, error flag included.</summary>
    [McpServerToolType]
    public sealed class ProbeTools
    {
        [McpServerTool(Name = ProbeName), Description("Test-only: answers the text a census test registered.")]
        public static CallToolResult Probe([Description("The payload key.")] string key)
        {
            var (text, isError) = s_payloads[key];
            return new CallToolResult
            {
                Content = new List<ContentBlock> { new TextContentBlock { Text = text } },
                IsError = isError ? true : null,
            };
        }
    }

    /// <summary>A real host: the production tool and filter registration plus the probe tool, behind an in-memory
    /// server. Dispose it with the test.</summary>
    public static async Task<TestServer> BuildHostAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        var sharedBaselines = new BaselineCache();
        builder.Services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: sharedBaselines));
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, new ReadLatencyAccumulator(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, null);

        // Chains onto the SAME builder ConfigureMcpServices created, so every real tool and every real filter stays.
        builder.Services.AddMcpServer().WithTools<ProbeTools>();

        var app = builder.Build();
        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());
        host.ConfigurePipeline(
            app, networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), bearerToken: "ssf-census-token");

        await app.StartAsync();
        return app.GetTestServer();
    }

    /// <summary>Sends <paramref name="rawToolText"/> through the host's registered filters as a tool's result and
    /// returns what a client would receive.</summary>
    public static async Task<(string Text, bool IsError)> FilterThroughHostAsync(
        TestServer server, string rawToolText, bool isError = false, string path = "/")
    {
        string key = Guid.NewGuid().ToString("N");
        s_payloads[key] = (rawToolText, isError);
        try
        {
            return await CallRealToolAsync(server, ProbeName, new JsonObject { ["key"] = key }, path);
        }
        finally
        {
            s_payloads.TryRemove(key, out _);
        }
    }

    /// <summary>A <c>tools/call</c> for a real tool, through the host's registered filters. Returns the first text
    /// block and the error flag; throws when the server answered a protocol error, so a test fails loudly instead of
    /// reading an empty answer as a clean one.</summary>
    public static async Task<(string Text, bool IsError)> CallRealToolAsync(
        TestServer server, string toolName, JsonObject arguments, string path = "/")
    {
        string body = new JsonObject
        {
            ["jsonrpc"] = "2.0",
            ["id"] = 1,
            ["method"] = "tools/call",
            ["params"] = new JsonObject { ["name"] = toolName, ["arguments"] = arguments },
        }.ToJsonString();

        JsonElement root = (await RpcAsync(server, path, body)).RootElement;
        if (root.TryGetProperty("error", out var error))
        {
            throw new InvalidOperationException("The server answered a protocol error: " + error.GetRawText());
        }

        var result = root.GetProperty("result");
        bool isError = result.TryGetProperty("isError", out var flag) && flag.ValueKind == JsonValueKind.True;
        string text = result.GetProperty("content")[0].GetProperty("text").GetString() ?? "";
        return (text, isError);
    }

    /// <summary>The names <c>tools/list</c> advertises on <paramref name="path"/>.</summary>
    public static async Task<IReadOnlyList<string>> ListToolNamesAsync(TestServer server, string path = "/")
    {
        using var doc = await RpcAsync(server, path, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}");
        return doc.RootElement.GetProperty("result").GetProperty("tools").EnumerateArray()
            .Select(t => t.GetProperty("name").GetString()!).ToList();
    }

    private static async Task<JsonDocument> RpcAsync(TestServer server, string path, string requestBody)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = path;
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(requestBody);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        string raw = await reader.ReadToEndAsync();
        Assert.Equal(200, ctx.Response.StatusCode);

        // A stateless answer is one SSE event ("data: {json}") or a bare JSON body, depending on the Accept match.
        string json = raw.TrimStart().StartsWith('{')
            ? raw
            : string.Join("\n", raw.Split('\n').Where(l => l.StartsWith("data:", StringComparison.Ordinal))
                .Select(l => l.Substring(5).Trim()));
        return JsonDocument.Parse(json);
    }

    // ── the census assertions ──

    /// <summary>The control: the RAW read holds the canary, so a filtered answer that does not proves the filter and
    /// not an empty plant.</summary>
    public static void AssertRawHoldsTheCanary(string raw)
    {
        Assert.Contains("S3cret-canary-ssf", raw);
    }

    /// <summary>The FILTERED answer of a read: no secret needle, the marker present, and every kept needle the raw
    /// text held still there, unchanged.</summary>
    public static void AssertFilteredWithholdsTheCanary(string raw, string filtered)
    {
        foreach (string needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, filtered);
        }

        Assert.Contains(Marker, filtered);
        foreach (string kept in StatementScrubCanary.KeptNeedles.Where(k => raw.Contains(k, StringComparison.Ordinal)))
        {
            Assert.Contains(kept, filtered);
        }
    }

    /// <summary>The filtered text of a plan read in XML form: it still parses, statement 1 is the marker, and
    /// statements 2 and 3 keep their text, operators and parameters.</summary>
    public static void AssertPlanFilteredKeepsTheRest(string rawPlan, string filteredPlan)
    {
        AssertFilteredWithholdsTheCanary(rawPlan, filteredPlan);
        var statements = XDocument.Parse(filteredPlan).Descendants().Where(e => e.Name.LocalName == "StmtSimple")
            .Select(e => (string?)e.Attribute("StatementText")).ToList();
        Assert.Equal(Marker, statements[0]);
        Assert.Equal(StatementScrubCanary.PlainStatement, statements[1]);
        Assert.Equal(StatementScrubCanary.AutoParamStatement, statements[2]);
        Assert.Contains(
            XDocument.Parse(filteredPlan).Descendants(),
            e => (string?)e.Attribute("ParameterCompiledValue") == "N'param-canary-ssf'");
    }
}
