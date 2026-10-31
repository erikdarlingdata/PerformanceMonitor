/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>reader</c> role and a token's own <c>dataDetail</c> (#7023). The tool-set cases drive the real pipeline through a TestServer
/// with every write, server admin and investigation setting ON: a reader is served the reads and nothing else, whatever the settings
/// say. The detail cases send one answer through the real call filter, and read the injected setting in a tool, as each token asks.
/// </summary>
[Collection("DarlingOutputFormatEnv")]
public sealed class DarlingMcpReaderRoleTests
{
    private const string OperatorToken = "operator-token-value-aaaaaaaaaaaa-1";
    private const string AgentToken = "agent-token-value-bbbbbbbbbbbbbbb-2";
    private const string ReaderToken = "reader-token-value-cccccccccccccc-3";
    private const string MaskedReaderToken = "masked-reader-value-dddddddddddd-4";
    private const string FullEntryToken = "full-entry-token-value-eeeeeeeeee-5";

    private static McpTokenEntry Entry(string name, string token, string role, string? dataDetail = null) =>
        new() { Name = name, Token = token, Role = role, DataDetail = dataDetail };

    private static DarlingMcpTokenSet SetFor(McpConfig mcp)
    {
        var legacy = DarlingMcpWritePolicy.ResolveForStart(mcp, "", out _);
        return DarlingMcpTokenSet.Resolve(mcp, "", legacy, null);
    }

    private static McpConfig EverythingOn() => new()
    {
        AllowWrites = true,
        AllowServerAdmin = true,
        AllowInvestigations = true,
        Tokens = new List<McpTokenEntry>
        {
            Entry("ops-lead", OperatorToken, "operator"),
            Entry("triage-bot", AgentToken, "agent"),
            Entry("dev-a", ReaderToken, "reader"),
        },
    };

    private static async Task<HashSet<string>> ToolNamesAsync(TestServer server, string bearer)
    {
        var (_, body) = await DarlingMcpWritePolicyTests.ToolsListAsync(server, bearer);
        return DarlingMcpWritePolicyTests.ToolsOf(body).Keys.ToHashSet(StringComparer.Ordinal);
    }

    [Fact]
    public async Task AReaderToken_SeesTheReadsOnly_WithEveryWriteServerAdminAndInvestigationSettingOn()
    {
        var mcp = EverythingOn();
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));

        var reader = await ToolNamesAsync(server, ReaderToken);
        var operatorTools = await ToolNamesAsync(server, OperatorToken);
        var agent = await ToolNamesAsync(server, AgentToken);

        Assert.NotEmpty(reader);
        Assert.Empty(reader.Where(DarlingMcpWritePolicy.IsWrite));
        Assert.Empty(reader.Where(DarlingMcpWritePolicy.IsInvestigation));
        Assert.Empty(reader.Where(DarlingMcpWritePolicy.IsServerAdmin));
        Assert.Empty(reader.Where(DarlingMcpWritePolicy.AgentActionTools.Contains));

        /* Controls: the same settings do serve the operator its writes and the agent its investigation tools, so the reader's absence is the role's. */
        Assert.Contains("create_mute_rule", operatorTools);
        Assert.Contains("add_servers", operatorTools);
        Assert.Contains("claim_investigation_request", agent);
        Assert.True(reader.IsSubsetOf(operatorTools));
    }

    [Fact]
    public async Task AReaderToken_IsRefusedAWriteCallAndAClaimCall_AsUnknownTools()
    {
        var mcp = EverythingOn();
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", tokens: SetFor(mcp));

        var (_, write) = await DarlingMcpWritePolicyTests.ToolsCallAsync(server, "create_mute_rule", "{}", ReaderToken);
        Assert.Contains("Unknown tool", write, StringComparison.OrdinalIgnoreCase);

        var (_, claim) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
            server, "claim_investigation_request", "{\"request_id\":\"" + Guid.NewGuid() + "\",\"agent_id\":\"dev-a\"}", ReaderToken);
        Assert.Contains("Unknown tool", claim, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AReader_IsNotAnAgent_NoAgentIdentityNoOperatorActor()
    {
        var set = SetFor(EverythingOn());

        Assert.Equal(new[] { "triage-bot" }, set.AgentNames.ToArray());

        var entry = set.Named.Single(e => e.Name == "dev-a");
        var policy = set.PolicyFor(entry);
        Assert.Null(policy.AgentTokenName);
        Assert.Null(policy.OperatorActor);
        Assert.False(policy.IsOperator);
        Assert.False(policy.Writes || policy.ServerAdmin || policy.Investigations);

        /* The union a start serves is unchanged by a reader: only the operator and the agent widen it. */
        var withoutReader = new DarlingMcpTokenSet("", default, set.Named.Where(e => e.Name != "dev-a").ToList(), serverAdmin: true);
        Assert.Equal(withoutReader.Union, set.Union);
    }

    [Fact]
    public void Check_AcceptsTheReaderRole_AndABadRoleNamesAllThree()
    {
        var mcp = new McpConfig { Tokens = new List<McpTokenEntry> { Entry("dev-a", ReaderToken, "Reader"), Entry("odd", AgentToken, "viewer") } };

        var problems = DarlingMcpTokenSet.Check(mcp, "", DarlingMcpTokenSet.ResolveEntry, out var usable);

        Assert.Equal(new[] { "dev-a" }, usable.Select(u => u.Name).ToArray());
        var bad = Assert.Single(problems);
        Assert.Contains("operator, agent or reader", bad, StringComparison.Ordinal);
        Assert.DoesNotContain(AgentToken, bad, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_AnUnrecognizedDataDetail_LeavesTheEntryUnserved_AndNamesTheSetting()
    {
        var mcp = new McpConfig
        {
            Tokens = new List<McpTokenEntry>
            {
                Entry("dev-a", ReaderToken, "reader", "maskLiteral"),
                Entry("dev-b", MaskedReaderToken, "reader", "maskLiterals"),
            },
        };

        var problems = DarlingMcpTokenSet.Check(mcp, "", DarlingMcpTokenSet.ResolveEntry, out var usable);

        Assert.Equal(new[] { "dev-b" }, usable.Select(u => u.Name).ToArray());
        Assert.Equal(McpDataDetail.MaskLiterals, usable.Single().DataDetail);
        var problem = Assert.Single(problems);
        Assert.Contains("mcp.tokens[dev-a].dataDetail", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(McpDataDetail.Full, null, McpDataDetail.Full)]
    [InlineData(McpDataDetail.Full, McpDataDetail.MaskLiterals, McpDataDetail.MaskLiterals)]
    [InlineData(McpDataDetail.Full, McpDataDetail.MetadataOnly, McpDataDetail.MetadataOnly)]
    [InlineData(McpDataDetail.MaskLiterals, McpDataDetail.Full, McpDataDetail.MaskLiterals)]
    [InlineData(McpDataDetail.MaskLiterals, null, McpDataDetail.MaskLiterals)]
    [InlineData(McpDataDetail.MetadataOnly, McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly)]
    public void ATokensDataDetail_OnlyNarrowsTheServiceLevel(McpDataDetail service, McpDataDetail? own, McpDataDetail expected)
    {
        var caller = new McpCaller("t", default, own);

        Assert.Equal(expected, DarlingMcpTokenSet.DetailFor(service, caller));
        Assert.Equal(service, DarlingMcpTokenSet.DetailFor(service, null));
    }

    /// <summary>A tool that takes the setting the way the real tools do (<c>McpDataDetailSetting? maskSetting = null</c>), and answers its level.</summary>
    [McpServerToolType]
    public sealed class SettingProbeTools
    {
        [McpServerTool(Name = "setting_probe"), Description("Test-only: answers the data detail level the call was served.")]
        public static string Probe(McpDataDetailSetting? maskSetting = null) => McpDataDetailModes.Word(maskSetting?.Mode ?? McpDataDetail.Full);

        [McpServerTool(Name = "composer_probe"), Description("Test-only: answers the level the investigation brief composer composes at.")]
        public static string Composer(InvestigationBriefComposer composer) => McpDataDetailModes.Word(composer.Mode);
    }

    private static async Task<TestServer> BuildDetailServerAsync(McpDataDetail service)
    {
        var mcp = new McpConfig
        {
            Tokens = new List<McpTokenEntry>
            {
                Entry("dba", OperatorToken, "operator"),
                Entry("dev-a", MaskedReaderToken, "reader", "maskLiterals"),
                Entry("dev-b", FullEntryToken, "reader", "full"),
                Entry("dev-c", ReaderToken, "reader"),
            },
        };
        var tokens = SetFor(mcp);

        var builder = DarlingMcpHostService.CreateHostBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
        builder.Services.AddSingleton<IGcfOutputFormat>(new FixedGcfOutputFormat(false));
        DarlingMcpHostService.RegisterDataDetail(builder.Services, service);
        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, writes: tokens.Union, tokens: tokens);
        builder.Services.AddMcpServer().WithTools<StatementFilterCensus.ProbeTools>().WithTools<SettingProbeTools>();

        var app = builder.Build();
        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance,
            new McpRuntimeState(),
            new MonitoredServerRegistryState());
        host.ConfigurePipeline(
            app, networkMode: false, networkListenIp: null, allowedCidr: System.Net.IPNetwork.Parse("192.168.1.0/24"), bearerToken: "",
            requireTokenWhenLoopbackOnly: true, tokens: tokens);
        await app.StartAsync();
        return app.GetTestServer();
    }

    private const string Raw = "{\"status\":\"error\",\"message\":\"Bad value where b = 'zq-lit-7023'\"}";

    [Fact]
    public async Task AMaskLiteralsReader_UnderAFullService_IsServedMaskedAnswers_AndTheInjectedSettingSaysSo()
    {
        using var server = await BuildDetailServerAsync(McpDataDetail.Full);

        var (masked, _) = await StatementFilterCensus.FilterThroughHostAsync(server, Raw, bearerToken: MaskedReaderToken);
        Assert.DoesNotContain("zq-lit-7023", masked, StringComparison.Ordinal);

        /* The controls: the operator, a reader with no dataDetail and a reader that asks for full are all served the service's level, full. */
        foreach (var bearer in new[] { OperatorToken, ReaderToken, FullEntryToken })
        {
            var (open, _) = await StatementFilterCensus.FilterThroughHostAsync(server, Raw, bearerToken: bearer);
            Assert.Contains("zq-lit-7023", open, StringComparison.Ordinal);
        }

        var (level, _) = await StatementFilterCensus.CallRealToolAsync(server, "setting_probe", new JsonObject(), bearerToken: MaskedReaderToken);
        Assert.Equal("maskLiterals", level);
        var (operatorLevel, _) = await StatementFilterCensus.CallRealToolAsync(server, "setting_probe", new JsonObject(), bearerToken: OperatorToken);
        Assert.Equal("full", operatorLevel);
    }

    [Fact]
    public async Task AFullEntry_UnderAMaskLiteralsService_StaysMasked_ForTheFilterAndTheInjectedSetting()
    {
        using var server = await BuildDetailServerAsync(McpDataDetail.MaskLiterals);

        foreach (var bearer in new[] { OperatorToken, ReaderToken, FullEntryToken, MaskedReaderToken })
        {
            var (text, _) = await StatementFilterCensus.FilterThroughHostAsync(server, Raw, bearerToken: bearer);
            Assert.DoesNotContain("zq-lit-7023", text, StringComparison.Ordinal);
            var (level, _) = await StatementFilterCensus.CallRealToolAsync(server, "setting_probe", new JsonObject(), bearerToken: bearer);
            Assert.Equal("maskLiterals", level);
        }
    }

    [Fact]
    public async Task TheInvestigationBriefComposer_ComposesAtTheCallersLevel_NotTheFirstCallersLevel()
    {
        using var server = await BuildDetailServerAsync(McpDataDetail.Full);

        /* A masked caller first, then the operator, then the masked caller again: a composer built once would answer the first level to all. */
        foreach (var (bearer, expected) in new[] { (MaskedReaderToken, "maskLiterals"), (OperatorToken, "full"), (MaskedReaderToken, "maskLiterals") })
        {
            var (level, _) = await StatementFilterCensus.CallRealToolAsync(server, "composer_probe", new JsonObject(), bearerToken: bearer);
            Assert.Equal(expected, level);
        }
    }
}
