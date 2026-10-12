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
    public void WriteToolSet_IsExactlyTheRegisteredToolsWithAWriteVerb()
    {
        var registered = Names(RegisteredTools());
        Assert.True(registered.Length > 80, "the registered tool set should be the full Lite surface");

        var byVerb = registered
            .Where(n => McpWritePolicy.WriteVerbPrefixes.Any(p => n.StartsWith(p, StringComparison.Ordinal)))
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(
            byVerb.SetEquals(McpWritePolicy.WriteTools),
            "every registered tool whose name starts with a write verb must be in WriteTools, and every WriteTools name must be registered; "
            + $"by verb but unlisted: [{string.Join(", ", byVerb.Except(McpWritePolicy.WriteTools))}], "
            + $"listed but not registered by verb: [{string.Join(", ", McpWritePolicy.WriteTools.Except(byVerb))}]");
        Assert.Equal(2, McpWritePolicy.WriteTools.Count);
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
            Assert.True(tool.ProtocolTool.Annotations!.ReadOnlyHint, $"{tool.ProtocolTool.Name} should carry readOnlyHint");
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
    public void Host_RegistersThePolicyWithTheSetting_AndTheMainWindowPassesTheSetting()
    {
        var host = ParitySource.ReadFile("Lite/Mcp/McpHostService.cs");
        Assert.Contains("builder.Services.AddMcpWritePolicy(_allowWrites);", host, StringComparison.Ordinal);
        Assert.Contains("_allowWrites = allowWrites;", host, StringComparison.Ordinal);
        Assert.Contains("bool allowWrites = false", host, StringComparison.Ordinal);

        var main = ParitySource.ReadFile("Lite/MainWindow.xaml.cs");
        Assert.Contains("mcpSettings.Port, _scheduleManager, mcpSettings.AllowWrites)", main, StringComparison.Ordinal);
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

        /* The settings sentence sits in the preamble, before the tool census, where an agent reads the boundaries. */
        Assert.True(
            text.IndexOf(McpWritePolicy.WritesSetting, StringComparison.Ordinal)
                < text.IndexOf(" tools: discovery/health", StringComparison.Ordinal));
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
