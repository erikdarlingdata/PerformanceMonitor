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
using ModelContextProtocol;
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

    /// <summary>Test-only: a tool that THROWS an <c>McpException</c> carrying <c>message</c>, the way a tool that
    /// reports a failure through the SDK's exception does. Its text reaches the client through the SDK's own error
    /// result, so a filter has to see the exception to sweep it.</summary>
    [McpServerToolType]
    public sealed class ThrowProbeTools
    {
        [McpServerTool(Name = ThrowProbeName), Description("Test-only: throws an McpException with the given message.")]
        public static CallToolResult Throw([Description("The exception message.")] string message) =>
            throw new McpException(message);
    }

    public const string ThrowProbeName = "ssf_census_throw";

    /// <summary>A real host: the production tool and filter registration plus the probe tool, behind an in-memory
    /// server. Dispose it with the test.
    /// <para><paramref name="gcf"/> pins the host's GCF format (#5320): <c>false</c> (the default) is plain output and
    /// <c>true</c> is GCF on. Neither reads <c>DARLING_OUTPUT_FORMAT</c>, so a class in the <c>DarlingOutputFormatEnv</c>
    /// collection that sets it cannot reach this host. <c>null</c> keeps the production default (the environment variable,
    /// read on each call), for the test that pins the real wiring.</para>
    /// <para><paramref name="pseudonymKeys"/> (#6066) is the key source the host registers the way production does: through
    /// <c>DarlingMcpHostService.RegisterToolServices</c>'s own parameter, not a hand-written registration. Null registers none.</para></summary>
    public static async Task<TestServer> BuildHostAsync(bool? gcf = false, IPseudonymKeySource? pseudonymKeys = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        // #5632: the mcp.dataDetail level the host serves, as the service registers it; FilterThroughHostAsync sets it per call.
        builder.Services.AddSingleton(new McpDataDetailSetting(McpDataDetail.Full));
        if (gcf is bool pinned)
        {
            builder.Services.AddSingleton<IGcfOutputFormat>(new FixedGcfOutputFormat(pinned));
        }

        var sharedBaselines = new BaselineCache();
        builder.Services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: sharedBaselines));
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        if (pseudonymKeys is not null)
        {
            DarlingMcpHostService.RegisterToolServices(
                builder.Services, postgres, new MonitoredServerRegistryState(), new PostgresConfig(), analyzer: null,
                planFetcher: null, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, sharedBaselines,
                ownerSource: null, pseudonymKeys: pseudonymKeys);
        }

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, new ReadLatencyAccumulator(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, null);

        // Chains onto the SAME builder ConfigureMcpServices created, so every real tool and every real filter stays.
        builder.Services.AddMcpServer().WithTools<ProbeTools>().WithTools<ThrowProbeTools>();

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

    /// <summary>The bearer token <see cref="BuildLiveHostAsync"/> gates on: long enough to clear the network bind's floor,
    /// so the investigations arm resolves on.</summary>
    public const string LiveHostToken = "l8-contract-token-5632-0123456789abcdef0123456789abcdef";

    /// <summary>
    /// The REAL MCP host over a live store (#5632 Phase A, L8; review round 1 asked for it, as <see cref="BuildHostAsync"/>
    /// holds a dummy data source and no write policy). It registers <paramref name="storeConnectionString"/> as the store,
    /// serves <paramref name="dataDetail"/>, resolves the write policy as a start does from <c>mcp.allowInvestigations</c>
    /// on and <c>mcp.allowWrites</c> off with <paramref name="bearerToken"/>, and gates every request on that token
    /// (loopback included, as a start that serves the investigation tools does). Call it with
    /// <see cref="CallRealToolAsync"/>'s <c>bearerToken</c>. Dispose it with the test.
    /// </summary>
    public static async Task<TestServer> BuildLiveHostAsync(
        string storeConnectionString, McpDataDetail dataDetail, string bearerToken = LiveHostToken)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create(storeConnectionString);
        builder.Services.AddSingleton(postgres);
        builder.Services.AddSingleton(new McpDataDetailSetting(dataDetail));
        builder.Services.AddSingleton<IGcfOutputFormat>(new FixedGcfOutputFormat(false));
        var sharedBaselines = new BaselineCache();
        builder.Services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(
            postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: sharedBaselines));
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        var writes = DarlingMcpWritePolicy.ResolveForStart(
            new McpConfig { AllowInvestigations = true, AllowWrites = false }, bearerToken, out var tokenProblem);
        Assert.Null(tokenProblem);
        Assert.True(writes.Investigations && !writes.Writes && !writes.ServerAdmin, "The live host must serve the investigation arm only.");

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, new ReadLatencyAccumulator(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, null, writes);

        var app = builder.Build();
        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());
        host.ConfigurePipeline(
            app, networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), bearerToken: bearerToken, requireTokenWhenLoopbackOnly: true);

        await app.StartAsync();
        return app.GetTestServer();
    }

    /// <summary>The HTTP status of a <c>tools/call</c> sent with <paramref name="bearerToken"/> (none when null), for the
    /// gate checks on <see cref="BuildLiveHostAsync"/>.</summary>
    public static async Task<int> CallStatusAsync(TestServer server, string toolName, string? bearerToken)
    {
        string body = new JsonObject
        {
            ["jsonrpc"] = "2.0",
            ["id"] = 1,
            ["method"] = "tools/call",
            ["params"] = new JsonObject { ["name"] = toolName, ["arguments"] = new JsonObject() },
        }.ToJsonString();
        var ctx = await SendAsync(server, "/", body, bearerToken);
        return ctx.Response.StatusCode;
    }

    /// <summary>Sends <paramref name="rawToolText"/> through the host's registered filters as a tool's result and
    /// returns what a client would receive. <paramref name="dataDetail"/> is the <c>mcp.dataDetail</c> level the host
    /// serves for this call (#5632), set back to <c>full</c> afterwards; one host serves one call at a time here.</summary>
    public static async Task<(string Text, bool IsError)> FilterThroughHostAsync(
        TestServer server, string rawToolText, bool isError = false, string path = "/",
        McpDataDetail dataDetail = McpDataDetail.Full, string? bearerToken = null)
    {
        string key = Guid.NewGuid().ToString("N");
        s_payloads[key] = (rawToolText, isError);
        var setting = server.Services.GetRequiredService<McpDataDetailSetting>();
        setting.Mode = dataDetail;
        try
        {
            return await CallRealToolAsync(server, ProbeName, new JsonObject { ["key"] = key }, path, bearerToken);
        }
        finally
        {
            setting.Mode = McpDataDetail.Full;
            s_payloads.TryRemove(key, out _);
        }
    }

    /// <summary>A <c>tools/call</c> for a real tool, through the host's registered filters. Returns the first text
    /// block and the error flag; throws when the server answered a protocol error, so a test fails loudly instead of
    /// reading an empty answer as a clean one.</summary>
    public static async Task<(string Text, bool IsError)> CallRealToolAsync(
        TestServer server, string toolName, JsonObject arguments, string path = "/", string? bearerToken = null)
    {
        string body = new JsonObject
        {
            ["jsonrpc"] = "2.0",
            ["id"] = 1,
            ["method"] = "tools/call",
            ["params"] = new JsonObject { ["name"] = toolName, ["arguments"] = arguments },
        }.ToJsonString();

        JsonElement root = (await RpcAsync(server, path, body, bearerToken)).RootElement;
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
    public static async Task<IReadOnlyList<string>> ListToolNamesAsync(TestServer server, string path = "/", string? bearerToken = null)
    {
        using var doc = await RpcAsync(server, path, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}", bearerToken);
        return doc.RootElement.GetProperty("result").GetProperty("tools").EnumerateArray()
            .Select(t => t.GetProperty("name").GetString()!).ToList();
    }

    private static Task<HttpContext> SendAsync(TestServer server, string path, string requestBody, string? bearerToken) =>
        server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = path;
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            if (bearerToken is not null)
            {
                c.Request.Headers.Authorization = "Bearer " + bearerToken;
            }

            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(requestBody);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

    private static async Task<JsonDocument> RpcAsync(TestServer server, string path, string requestBody, string? bearerToken = null)
    {
        var ctx = await SendAsync(server, path, requestBody, bearerToken);

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

    // ── the keyed mcp.dataDetail census (#5632, R3) ──

    /// <summary>A string literal the census plants: unquoted, because a quote is escaped in JSON and XML.</summary>
    public const string LiteralCanary = "zq-lit-5632";

    /// <summary>A NUMERIC literal the census plants: its digits.</summary>
    public const string NumericCanary = "86753091";

    /// <summary>A login the census plants.</summary>
    public const string LoginCanary = "zq_login_5632";

    /// <summary>A client host name the census plants.</summary>
    public const string HostCanary = "zq-host-5632";

    /// <summary>A statement the statement filter keeps, holding both literal canaries: what <c>maskLiterals</c> must
    /// mask and <c>metadataOnly</c> must withhold.</summary>
    public const string MaskStatement =
        "SELECT zq_census_col FROM dbo.t WHERE c = N'" + LiteralCanary + "' AND n = " + NumericCanary;

    /// <summary>The literal canaries: the two above plus the literals the statement-filter harness already plants in
    /// statements it keeps (a plan's <c>ParameterCompiledValue</c> and a plan warning's predicate).</summary>
    public static readonly string[] LiteralCanaries = { LiteralCanary, NumericCanary, "param-canary-ssf", "kept-predicate-ssf" };

    /// <summary>Every canary the census looks for.</summary>
    public static readonly string[] AllCanaries = LiteralCanaries.Concat(new[] { LoginCanary, HostCanary }).ToArray();

    /// <summary>
    /// The keyed census of one read (#5632, R3): <paramref name="raw"/> (the read's answer, the control) goes through
    /// the host's registered filters under <c>maskLiterals</c> and under <c>metadataOnly</c>, and any field of the
    /// answer that still carries a planted canary fails the test with the tool and the field named: a JSON key, an
    /// alert context label's value, or an XML element or attribute inside a value. A literal canary may survive
    /// neither level; a login or host canary may not survive <c>metadataOnly</c>. A field that fails belongs in
    /// <see cref="McpDataDetailKeys"/> (as statement, parameter value, login or host). Returns the canaries the raw
    /// answer held, so a class can prove its plants reached the output.
    /// </summary>
    public static async Task<IReadOnlySet<string>> AssertKeyedMaskingAsync(TestServer server, string tool, string raw)
    {
        var planted = AllCanaries.Where(c => raw.Contains(c, StringComparison.Ordinal)).ToHashSet(StringComparer.Ordinal);
        if (planted.Count == 0) return planted;

        foreach (var mode in new[] { McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly })
        {
            var (filtered, _) = await FilterThroughHostAsync(server, raw, dataDetail: mode);
            var watched = mode == McpDataDetail.MetadataOnly
                ? planted.ToArray()
                : planted.Where(c => LiteralCanaries.Contains(c, StringComparer.Ordinal)).ToArray();
            var leaks = new List<string>();
            FindLeaks(filtered, "(answer)", watched, leaks);
            Assert.True(leaks.Count == 0,
                $"{tool} under mcp.dataDetail={mode}: {string.Join("; ", leaks)}. "
                + "List each field in McpDataDetailKeys (statement, parameter value, login or host).");
        }

        return planted;
    }

    /// <summary>The control for a class: the union of what its reads' raw answers held covers every canary it
    /// planted.</summary>
    public static void AssertPlanted(IEnumerable<string> found, params string[] expected)
    {
        var have = found.ToHashSet(StringComparer.Ordinal);
        foreach (string canary in expected)
        {
            Assert.True(have.Contains(canary), $"No raw answer held the canary {canary}: the plant did not reach a tool.");
        }
    }

    private static void FindLeaks(string text, string path, string[] canaries, List<string> leaks)
    {
        string? hit = canaries.FirstOrDefault(c => text.Contains(c, StringComparison.Ordinal));
        if (hit is null) return;

        string trimmed = text.TrimStart();
        if (trimmed.StartsWith('{') || trimmed.StartsWith('['))
        {
            JsonNode? node = null;
            try { node = JsonNode.Parse(text); } catch (JsonException) { }
            if (node is not null)
            {
                FindLeaks(node, path, canaries, leaks);
                return;
            }
        }

        if (trimmed.StartsWith('<'))
        {
            XDocument? doc = null;
            try { doc = XDocument.Parse(text); } catch (System.Xml.XmlException) { }
            if (doc?.Root is { } root)
            {
                foreach (var element in root.DescendantsAndSelf())
                {
                    foreach (var attribute in element.Attributes())
                    {
                        string? a = canaries.FirstOrDefault(c => attribute.Value.Contains(c, StringComparison.Ordinal));
                        if (a is not null) leaks.Add($"{path} <{element.Name.LocalName} {attribute.Name.LocalName}=> carries '{a}'");
                    }

                    foreach (var textNode in element.Nodes().OfType<XText>())
                    {
                        string? t = canaries.FirstOrDefault(c => textNode.Value.Contains(c, StringComparison.Ordinal));
                        if (t is not null) leaks.Add($"{path} <{element.Name.LocalName}> text carries '{t}'");
                    }
                }

                return;
            }
        }

        leaks.Add($"key '{path}' carries '{hit}'");
    }

    private static void FindLeaks(JsonNode node, string path, string[] canaries, List<string> leaks)
    {
        switch (node)
        {
            case JsonObject obj:
                // An alert context field ({"label": ..., "value": ...}) is keyed by its label, as the masker reads it.
                string? label = obj["label"] is JsonValue lv && lv.TryGetValue(out string? l) ? l : null;
                foreach (var (key, child) in obj)
                {
                    if (child is null) continue;
                    string childPath = label is not null && key == "value" ? $"label '{label}'" : key;
                    FindLeaks(child, childPath, canaries, leaks);
                }

                break;
            case JsonArray array:
                foreach (var child in array)
                {
                    if (child is not null) FindLeaks(child, path, canaries, leaks);
                }

                break;
            case JsonValue value:
                FindLeaks(value.TryGetValue(out string? sv) ? sv : value.ToJsonString(), path, canaries, leaks);
                break;
        }
    }
}
