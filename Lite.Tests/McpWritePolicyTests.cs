/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The MCP write switch (<see cref="McpWritePolicy"/>): with the default settings no tool that changes what Lite
/// stores is served, <c>mcp_allow_writes</c> serves the analysis mute and its undo, every served tool carries the
/// read-only or destructive annotation hint, and the instructions no longer call the server read-only. The pure
/// cases run over the REAL registered tool set; the wired cases go through the SDK's own options, which is where
/// the host applies the policy.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpWritePolicyTests
{
    private static McpServerPrimitiveCollection<McpServerTool> RegisteredTools()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var collection = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in tools)
        {
            collection.Add(tool);
        }

        return collection;
    }

    private static string[] Names(IEnumerable<McpServerTool> tools) =>
        tools.Select(t => t.ProtocolTool.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>The SDK's own options for the analysis tool class, with the policy registered the way the host
    /// registers it. The service-typed parameters are never resolved: nothing here calls a tool.</summary>
    private static string[] ToolsServedByTheSdk(bool allowWrites)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        foreach (var serviceType in typeof(McpAnalysisTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(McpServedSchema.IsServiceParameter)
            .Where(t => t != typeof(McpToolGuideCatalog))
            .Distinct())
        {
            services.AddSingleton(serviceType, _ => null!);
        }

        services.AddMcpWritePolicy(allowWrites);
        services.AddMcpServer().WithGeminiCompatibleTools<McpAnalysisTools>();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<McpServerOptions>>().Value;
        return Names(options.ToolCollection!);
    }

    [Fact]
    public void EveryRegisteredTool_IsEitherOnTheReadOnlyList_OrInTheWriteSet()
    {
        var registered = Names(RegisteredTools());
        Assert.True(registered.Length > 80, "the registered tool set should be the full Lite surface");

        var readOnly = McpToolClassification.ReadOnlyTools;
        var writes = McpWritePolicy.WriteTools;

        /* #5820: claim and submit are the third set, as on Darling: they change only investigation state, so they are neither
           reads nor WriteTools (which mcp_allow_writes serves). McpInvestigationPolicy serves them by name and McpWritePolicy
           stamps them neither read-only nor destructive. */
        var stateOnly = McpInvestigationPolicy.StateChangingTools;

        var unclassified = registered.Where(n => !readOnly.Contains(n) && !writes.Contains(n) && !stateOnly.Contains(n)).ToArray();
        var both = readOnly.Intersect(writes).Concat(stateOnly.Intersect(readOnly)).Concat(stateOnly.Intersect(writes)).ToArray();
        var stale = readOnly.Concat(writes).Concat(stateOnly).Except(registered).ToArray();

        Assert.True(
            unclassified.Length == 0,
            "a registered tool is on neither the read-only list (McpToolClassification) nor McpWritePolicy.WriteTools; "
            + $"decide which it is and add it: [{string.Join(", ", unclassified)}]");
        Assert.True(both.Length == 0, $"a tool is on the read-only list AND in the write set: [{string.Join(", ", both)}]");
        Assert.True(stale.Length == 0, $"a classified name is not a registered tool: [{string.Join(", ", stale)}]");
    }

    [Fact]
    public void EveryTool_IsStampedByTheWriteSet_ReadOnlyForReads_NotReadOnlyAndDestructiveForWrites()
    {
        var all = RegisteredTools();
        McpWritePolicy.Apply(all, allowWrites: true);

        foreach (var tool in all)
        {
            var isWrite = McpWritePolicy.IsWrite(tool.ProtocolTool.Name);
            Assert.NotNull(tool.ProtocolTool.Annotations);
            /* #5820: claim and submit change only investigation state: neither read-only nor destructive. */
            Assert.Equal(!isWrite && !McpInvestigationPolicy.IsStateChanging(tool.ProtocolTool.Name), tool.ProtocolTool.Annotations!.ReadOnlyHint);
            Assert.Equal(isWrite, tool.ProtocolTool.Annotations.DestructiveHint);
        }
    }

    /// <summary>The host's own registration (<see cref="McpHostService.RegisterMcpServer"/>) and JSON guard behind a
    /// test web server: what a client reaches when it calls a tool on the real endpoint.</summary>
    private static async Task<TestServer> BuildServer(bool allowWrites)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        foreach (var serviceType in typeof(McpAnalysisTools)
            .Assembly.GetTypes()
            .Where(t => t.Name.StartsWith("Mcp", StringComparison.Ordinal) && t.Name.EndsWith("Tools", StringComparison.Ordinal))
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(McpServedSchema.IsServiceParameter)
            .Where(t => t != typeof(McpToolGuideCatalog))
            .Distinct())
        {
            builder.Services.AddSingleton(serviceType, _ => null!);
        }

        McpHostService.RegisterMcpServer(builder.Services, allowWrites);

        var app = builder.Build();
        McpHostService.UseJsonPostGuard(app);
        app.MapMcp();
        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<string> PostAsync(TestServer server, string json)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = System.Text.Encoding.UTF8.GetBytes(json);
            c.Request.Body = new MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = System.Net.IPAddress.Loopback;
        });

        using var reader = new StreamReader(ctx.Response.Body);
        return await reader.ReadToEndAsync();
    }

    [Theory]
    [InlineData("mute_analysis_finding", "{\"finding_hash\":\"abc\"}")]
    [InlineData("unmute_analysis_finding", "{\"finding_hash\":\"abc\"}")]
    public async Task WritesOff_CallingAWriteTool_IsRefusedAsAnUnknownTool(string tool, string arguments)
    {
        using var server = await BuildServer(allowWrites: false);

        var body = await PostAsync(server,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + tool + "\",\"arguments\":" + arguments + "}}");

        Assert.Contains("Unknown tool", body, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WritesOn_TheWriteToolsAreListed_WithTheDestructiveHint()
    {
        using var server = await BuildServer(allowWrites: true);

        var body = await PostAsync(server, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");

        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var tools = document.RootElement.GetProperty("result").GetProperty("tools").EnumerateArray()
            .ToDictionary(t => t.GetProperty("name").GetString() ?? "", t => t.Clone(), StringComparer.Ordinal);
        Assert.True(tools["mute_analysis_finding"].GetProperty("annotations").GetProperty("destructiveHint").GetBoolean());
        Assert.False(tools["mute_analysis_finding"].GetProperty("annotations").GetProperty("readOnlyHint").GetBoolean());
        Assert.True(tools["analyze_server"].GetProperty("annotations").GetProperty("readOnlyHint").GetBoolean());
    }

    [Fact]
    public void Default_ServesNoWriteTool_AndEveryRead()
    {
        var all = RegisteredTools();
        var served = McpWritePolicy.FilterToolCollection(all, allowWrites: false);

        var names = Names(served);
        Assert.DoesNotContain("mute_analysis_finding", names);
        Assert.DoesNotContain("unmute_analysis_finding", names);
        Assert.DoesNotContain(names, McpWritePolicy.IsWrite);
        Assert.Equal(all.Count - McpWritePolicy.WriteTools.Count, served.Count);
        Assert.Contains("list_servers", names);
        Assert.Contains("analyze_server", names);
    }

    /* #5719: Lite's instructions said "90 tools" while tools/list returned 88 with mcp_allow_writes off (the default).
       The sentence now carries the registered total, the default count and the two tools the setting adds; this pins
       them to the registered tools and the policy's write set. Lite has no bearer token, so the setting is the whole
       condition (unlike Darling's "with a token"). */
    [Fact]
    public void Instructions_ToolCounts_MatchTheRegisteredTools_UnderEachSwitchState()
    {
        var all = RegisteredTools();
        var defaultServed = McpWritePolicy.FilterToolCollection(all, allowWrites: false);
        var writesServed = McpWritePolicy.FilterToolCollection(all, allowWrites: true);

        Assert.Equal(all.Count, writesServed.Count);
        Assert.Equal(McpWritePolicy.WriteTools.Count, all.Count - defaultServed.Count);

        /* #5820: the four investigation tools are served only by their own setting (McpInvestigationPolicy), so the default
           count leaves them out too. */
        var defaultCount = defaultServed.Count - McpInvestigationPolicy.InvestigationTools.Count;

        var text = McpInstructions.Text;
        Assert.Contains($"{all.Count} tools ({defaultCount} by default; `{McpWritePolicy.WritesSetting}` adds ", text, StringComparison.Ordinal);
        foreach (var name in McpWritePolicy.WriteTools)
        {
            Assert.Contains($"`{name}`", text, StringComparison.Ordinal);
        }
    }

    /* #5789 (Darling's twin): mcp_allow_writes hides the write tools, and the census was the fixed catalog line
       "93 tools (89 by default; ...)" whichever way the switch was set. The host now renders it from the names
       before and after McpWritePolicy.Apply; these pin that rendering for each switch state. */
    [Fact]
    public void ServedCensus_WritesOff_CountsTheServedTools_AndSaysTheSwitchIsOff()
    {
        var all = RegisteredTools();
        var catalog = McpWritePolicy.ToolNames(all);
        var served = McpWritePolicy.ToolNames(McpWritePolicy.FilterToolCollection(all, allowWrites: false));

        var text = McpInstructions.Build(catalog, served);
        Assert.Contains(
            $"## Tools\n\n{served.Count} tools ({catalog.Count} in the catalog; `{McpWritePolicy.WritesSetting}` is off, so the " +
            $"{catalog.Count - served.Count} write tools are not served): discovery/health",
            text, StringComparison.Ordinal);
        Assert.Equal(McpWritePolicy.WriteTools.Count, catalog.Count - served.Count);
        Assert.DoesNotContain($"{catalog.Count} tools (", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ServedCensus_WritesOn_CountsEveryTool_AndSaysTheSwitchIsOn()
    {
        var all = RegisteredTools();
        var catalog = McpWritePolicy.ToolNames(all);
        var served = McpWritePolicy.ToolNames(McpWritePolicy.FilterToolCollection(all, allowWrites: true));

        var text = McpInstructions.Build(catalog, served);
        Assert.Contains(
            $"## Tools\n\n{catalog.Count} tools (the {McpWritePolicy.WriteTools.Count} write tools included: " +
            $"`{McpWritePolicy.WritesSetting}` is on): discovery/health",
            text, StringComparison.Ordinal);
        Assert.DoesNotContain("in the catalog", text, StringComparison.Ordinal);
    }

    /* #5789 end to end: through the host's own registration, the count initialize's instructions state is the
       length of tools/list on the same server, with the writes off and on. */
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InitializeInstructions_CountTheToolsThatToolsListServes(bool allowWrites)
    {
        using var server = await BuildServer(allowWrites);

        var listBody = await PostAsync(server, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
        var initBody = await PostAsync(server,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\"," +
            "\"capabilities\":{},\"clientInfo\":{\"name\":\"census-test\",\"version\":\"1.0\"}}}");

        var served = ResultOf(listBody).GetProperty("tools").GetArrayLength();
        var instructions = ResultOf(initBody).GetProperty("instructions").GetString() ?? "";

        var census = System.Text.RegularExpressions.Regex.Match(instructions, @"## Tools\s+(\d+) tools ");
        Assert.True(census.Success, "the instructions carry no '## Tools / N tools' census");
        Assert.Equal(served, int.Parse(census.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(allowWrites, !instructions.Contains("in the catalog", StringComparison.Ordinal));
    }

    /// <summary>#5789 correction: no sentence of the rendered instructions points a client at a tool this server
    /// does not serve. Every backticked name that is a Lite tool must be in the served set, writes off and on.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RenderedInstructions_NameOnlyServedTools(bool allowWrites)
    {
        var all = RegisteredTools();
        var catalog = McpWritePolicy.ToolNames(all);
        var served = McpWritePolicy.ToolNames(McpWritePolicy.FilterToolCollection(all, allowWrites));
        var text = McpInstructions.Build(catalog, served);

        var named = System.Text.RegularExpressions.Regex.Matches(text, "`([a-z][a-z0-9_]*)`")
            .Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        var unserved = named.Where(n => catalog.Contains(n) && !served.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(named.Count(catalog.Contains) > 3, "the scan found almost no tool names; the backtick pattern no longer matches the text");
        Assert.True(unserved.Count == 0, "the instructions name tools this configuration does not serve: " + string.Join(", ", unserved));
    }

    /// <summary>#5820 L5c: the investigation paragraph appears exactly when the investigation tools are served (the setting
    /// on and the writes off), and with them off the rendered text names none of the four tools, in the paragraph or the census.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RenderedInstructions_CarryTheInvestigationParagraph_OnlyWhenTheInvestigationToolsAreServed(bool investigationsServed)
    {
        var all = RegisteredTools();
        var catalog = McpWritePolicy.ToolNames(all).Where(n => investigationsServed || !McpInvestigationPolicy.IsInvestigationTool(n)).ToList();
        var served = catalog.Where(n => !McpWritePolicy.IsWrite(n)).ToList();
        var text = McpInstructions.Build(catalog, served);

        Assert.Equal(investigationsServed, text.Contains("### Investigation requests", StringComparison.Ordinal));
        Assert.Equal(investigationsServed, text.Contains("mcp_allow_investigations", StringComparison.Ordinal));
        foreach (var name in McpInvestigationPolicy.InvestigationTools)
        {
            Assert.Equal(investigationsServed, text.Contains($"`{name}`", StringComparison.Ordinal));
        }
    }

    private static JsonElement ResultOf(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        return document.RootElement.GetProperty("result").Clone();
    }

    [Fact]
    public void WritesOn_ServesTheMuteAndItsUndo()
    {
        var all = RegisteredTools();
        var served = McpWritePolicy.FilterToolCollection(all, allowWrites: true);

        Assert.Equal(all.Count, served.Count);
        Assert.Contains("mute_analysis_finding", Names(served));
        Assert.Contains("unmute_analysis_finding", Names(served));
    }

    [Fact]
    public void Apply_RemovesRefusedWritesInPlace_AndStampsTheHints()
    {
        var narrowed = RegisteredTools();
        var before = narrowed.Count;
        McpWritePolicy.Apply(narrowed, allowWrites: false);

        Assert.Equal(before - McpWritePolicy.WriteTools.Count, narrowed.Count);
        Assert.All(narrowed, tool =>
        {
            Assert.NotNull(tool.ProtocolTool.Annotations);
            Assert.Equal(!McpInvestigationPolicy.IsStateChanging(tool.ProtocolTool.Name), tool.ProtocolTool.Annotations!.ReadOnlyHint);
            Assert.False(tool.ProtocolTool.Annotations.DestructiveHint, $"{tool.ProtocolTool.Name} should not carry destructiveHint");
        });

        var full = RegisteredTools();
        McpWritePolicy.Apply(full, allowWrites: true);
        Assert.Equal(before, full.Count);
        foreach (var tool in full.Where(t => McpWritePolicy.IsWrite(t.ProtocolTool.Name)))
        {
            Assert.True(tool.ProtocolTool.Annotations!.DestructiveHint, $"{tool.ProtocolTool.Name} should carry destructiveHint");
            Assert.False(tool.ProtocolTool.Annotations.ReadOnlyHint, $"{tool.ProtocolTool.Name} should not carry readOnlyHint");
        }

        McpWritePolicy.Apply(null, allowWrites: true);
    }

    [Fact]
    public void ThroughTheSdksOptions_TheMuteIsNotServedByDefault_AndIsServedWhenAllowed()
    {
        var byDefault = ToolsServedByTheSdk(allowWrites: false);
        Assert.NotEmpty(byDefault);
        Assert.DoesNotContain("mute_analysis_finding", byDefault);
        Assert.DoesNotContain("unmute_analysis_finding", byDefault);
        Assert.Contains("analyze_server", byDefault);

        var allowed = ToolsServedByTheSdk(allowWrites: true);
        Assert.Contains("mute_analysis_finding", allowed);
        Assert.Contains("unmute_analysis_finding", allowed);
        Assert.Equal(byDefault.Length + 2, allowed.Length);
    }

    [Fact]
    public void Setting_DefaultsOff_IsReadFromSettingsJson_AndAWrongKindIsLoud()
    {
        Assert.False(new McpSettings().AllowWrites);
        Assert.Equal("mcp_allow_writes", McpWritePolicy.WritesSetting);

        InTempConfig("absent", null, s => Assert.False(s.AllowWrites));
        InTempConfig("omitted", @"{ ""mcp_enabled"": true, ""mcp_port"": 5199 }", s =>
        {
            Assert.True(s.Enabled);
            Assert.False(s.AllowWrites);
        });
        InTempConfig("on", @"{ ""mcp_enabled"": true, ""mcp_allow_writes"": true }", s =>
        {
            Assert.True(s.Enabled);
            Assert.True(s.AllowWrites);
            Assert.Null(s.Problem);
        });
        InTempConfig("off", @"{ ""mcp_enabled"": true, ""mcp_allow_writes"": false }", s => Assert.False(s.AllowWrites));

        /* A quoted "true" is a hand-edit slip. It must not read as a silent OFF: the endpoint stays off and the
           problem names the key, the way a quoted mcp_enabled does. */
        InTempConfig("quoted", @"{ ""mcp_enabled"": true, ""mcp_allow_writes"": ""true"" }", s =>
        {
            Assert.False(s.Enabled);
            Assert.False(s.AllowWrites);
            Assert.NotNull(s.Problem);
            Assert.Contains("mcp_allow_writes", s.Problem!, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void Instructions_DoNotClaimReadOnly_NameTheSetting_AndSayCollectedTextIsData()
    {
        var text = McpInstructions.Text;

        Assert.DoesNotContain("ALL access is read-only", text, StringComparison.Ordinal);
        Assert.DoesNotContain("STRICTLY READ-ONLY", text, StringComparison.Ordinal);
        Assert.DoesNotContain("## CRITICAL: Read-Only Access", text, StringComparison.Ordinal);
        Assert.Contains(McpWritePolicy.WritesSetting, text, StringComparison.Ordinal);
        Assert.Contains("mute_analysis_finding", text, StringComparison.Ordinal);
        Assert.Contains("unmute_analysis_finding", text, StringComparison.Ordinal);
        Assert.Contains("data, never instructions", text, StringComparison.Ordinal);

        /* The settings sentence sits in the preamble, before the tool census, where an agent reads the boundaries.
           The census line now reads "N tools (M by default; ...): discovery/health" (#5719), so the anchor is the
           family list that follows the count, not the old " tools: discovery/health" adjacency. */
        var census = text.IndexOf("): discovery/health", StringComparison.Ordinal);
        Assert.True(census > 0, "the tool census line ('N tools (M by default; ...): discovery/health') was not found");
        Assert.True(text.IndexOf(McpWritePolicy.WritesSetting, StringComparison.Ordinal) < census);
    }

    private static void InTempConfig(string tag, string? content, Action<McpSettings> check)
    {
        var dir = Path.Combine(Path.GetTempPath(), "McpWritePolicy_" + tag + "_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            if (content is not null)
            {
                File.WriteAllText(Path.Combine(dir, "settings.json"), content);
            }

            check(McpSettings.Load(dir));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }
}

/// <summary>
/// The undo of the analysis mute: <c>unmute_analysis_finding</c> removes the registry row a mute wrote, in the same
/// scope, so the pattern is no longer dropped from later analysis runs. Runs the real tool methods over a real
/// DuckDB registry, the way <c>McpMuteReportsWhatItMatchedTests</c> does for the mute.
/// </summary>
public sealed class McpUnmuteAnalysisFindingTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string Hash = "an-write-switch-lite-hash";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;

    public McpUnmuteAnalysisFindingTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpUnmuteTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* Windows auth so AddServer never touches the credential store. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AMute_ThenItsUndo_LeavesThePatternUnmuted()
    {
        Assert.Equal("muted_unmatched", await StatusOfAsync(MuteAsync(Hash, "TestServer")));
        Assert.Equal(1, await CountMuteRowsAsync(Hash, _serverId));
        Assert.Contains(Hash, await new FindingStore(_duckDb).GetMutedStoryHashesAsync(_serverId));

        using var undone = JsonDocument.Parse(await UnmuteAsync(Hash, "TestServer"));
        Assert.Equal("unmuted", undone.RootElement.GetProperty("status").GetString());
        Assert.Equal(1, undone.RootElement.GetProperty("removed").GetInt32());
        Assert.Equal("TestServer", undone.RootElement.GetProperty("server").GetString());

        Assert.Equal(0, await CountMuteRowsAsync(Hash, _serverId));
        Assert.DoesNotContain(Hash, await new FindingStore(_duckDb).GetMutedStoryHashesAsync(_serverId));

        /* The registry really lost the row: the same mute registers again instead of reporting already_muted. */
        using var again = JsonDocument.Parse(await MuteAsync(Hash, "TestServer"));
        Assert.True(again.RootElement.GetProperty("registered").GetBoolean());
    }

    [Fact]
    public async Task TheUndo_IsPerScope_AnAllServersMuteSurvivesANamedServerUndo_AndTheReverse()
    {
        await MuteAsync(Hash, serverName: null);
        await MuteAsync(Hash, "TestServer");
        Assert.Equal(1, await CountMuteRowsAsync(Hash, serverId: null));
        Assert.Equal(1, await CountMuteRowsAsync(Hash, _serverId));

        using var named = JsonDocument.Parse(await UnmuteAsync(Hash, "TestServer"));
        Assert.Equal("unmuted", named.RootElement.GetProperty("status").GetString());
        Assert.Equal(1, named.RootElement.GetProperty("removed").GetInt32());
        Assert.Equal(0, await CountMuteRowsAsync(Hash, _serverId));
        Assert.Equal(1, await CountMuteRowsAsync(Hash, serverId: null));

        using var namedAgain = JsonDocument.Parse(await UnmuteAsync(Hash, "TestServer"));
        Assert.Equal("not_muted", namedAgain.RootElement.GetProperty("status").GetString());
        Assert.Equal(0, namedAgain.RootElement.GetProperty("removed").GetInt32());
        Assert.Equal(1, await CountMuteRowsAsync(Hash, serverId: null));

        using var global = JsonDocument.Parse(await UnmuteAsync(Hash, serverName: null));
        Assert.Equal("unmuted", global.RootElement.GetProperty("status").GetString());
        Assert.Equal("(all servers)", global.RootElement.GetProperty("server").GetString());
        Assert.Equal(0, await CountMuteRowsAsync(Hash, serverId: null));
    }

    [Fact]
    public async Task TheUndoOfAHashThatWasNeverMuted_SaysNotMuted_AndChangesNothing()
    {
        await MuteAsync("an-other-hash", "TestServer");

        using var doc = JsonDocument.Parse(await UnmuteAsync(Hash, "TestServer"));
        Assert.Equal("not_muted", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(0, doc.RootElement.GetProperty("removed").GetInt32());
        Assert.Equal(1, await CountMuteRowsAsync("an-other-hash", _serverId));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ABlankHash_IsInvalid_AndRemovesNothing(string hash)
    {
        await MuteAsync("an-other-hash", "TestServer");

        using var doc = JsonDocument.Parse(await UnmuteAsync(hash, "TestServer"));
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(1, await CountMuteRowsAsync("an-other-hash", _serverId));
    }

    [Fact]
    public void TheTool_IsAWriteTool_AndTheDescriptionSaysWhatServesIt()
    {
        var method = typeof(McpAnalysisTools).GetMethod(nameof(McpAnalysisTools.UnmuteAnalysisFinding))!;
        var tool = method.GetCustomAttribute<McpServerToolAttribute>()!;
        Assert.Equal("unmute_analysis_finding", tool.Name);
        Assert.True(McpWritePolicy.IsWrite(tool.Name!));

        var description = method.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.StartsWith("Write:", description, StringComparison.Ordinal);
        Assert.Contains(McpWritePolicy.WritesSetting, description, StringComparison.Ordinal);
    }

    private AnalysisService CreateTestService() => new(_duckDb) { MinimumDataHours = 0 };

    private Task<string> MuteAsync(string hash, string? serverName) =>
        McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, hash, serverName, "test");

    private Task<string> UnmuteAsync(string hash, string? serverName) =>
        McpAnalysisTools.UnmuteAnalysisFinding(CreateTestService(), _serverManager, hash, serverName);

    private static async Task<string?> StatusOfAsync(Task<string> call)
    {
        using var doc = JsonDocument.Parse(await call);
        return doc.RootElement.GetProperty("status").GetString();
    }

    /// <summary>Registry rows for a hash under one server, or the all-servers rows (server_id IS NULL) when
    /// <paramref name="serverId"/> is null; the store writes the all-servers mute as NULL.</summary>
    private async Task<int> CountMuteRowsAsync(string storyPathHash, int? serverId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = serverId is null
            ? "SELECT COUNT(*) FROM analysis_muted WHERE story_path_hash = $1 AND server_id IS NULL"
            : "SELECT COUNT(*) FROM analysis_muted WHERE story_path_hash = $1 AND server_id = $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        if (serverId is not null)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId.Value });
        }

        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
    }
}
