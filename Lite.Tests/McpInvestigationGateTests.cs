/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5820 slice L4: the two Lite settings <c>mcp_allow_investigations</c> and <c>mcp_data_detail</c>, the gate that serves
/// the four investigation tools BY NAME (on AND writes off, no token), and the data-detail masking through the host's REAL
/// registration (<see cref="McpHostService.RegisterMcpServer"/> and <see cref="McpHostService.AddCallToolFilters"/>). The
/// four tool classes come later, so a stand-in class carries the four names plus one that must never be gated.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpInvestigationGateTests
{
    private const string Secret = "hunter2-literal";

    /// <summary>Stand-ins with the names the gate acts on, and one tool that returns statement text.</summary>
    [McpServerToolType]
    public sealed class StandInTools
    {
        [McpServerTool(Name = "list_investigation_requests"), Description("stand-in")]
        public static string List() => "listed";

        [McpServerTool(Name = "claim_investigation_request"), Description("stand-in")]
        public static string Claim() => "claimed";

        [McpServerTool(Name = "submit_investigation_report"), Description("stand-in")]
        public static string Submit() => "submitted";

        [McpServerTool(Name = "get_investigation_reports"), Description("stand-in")]
        public static string Get() => "reports";

        [McpServerTool(Name = "wait_for_investigation_requests"), Description("stand-in")]
        public static string Wait() => "waited";

        [McpServerTool(Name = "mute_analysis_finding"), Description("stand-in")]
        public static string Mute() => "muted";

        [McpServerTool(Name = "stand_in_login"), Description("stand-in")]
        public static string Login() => "{\"rows\":[{\"login_name\":\"CORP\\\\Alice\",\"duration_ms\":5}]}";

        [McpServerTool(Name = "stand_in_statement"), Description("stand-in")]
        public static string Statement() =>
            "{\"rows\":[{\"query_text\":\"SELECT * FROM dbo.t WHERE name = '" + Secret + "'\",\"duration_ms\":5}]}";
    }

    private static readonly string[] InvestigationToolNames =
    {
        "list_investigation_requests", "claim_investigation_request", "submit_investigation_report", "get_investigation_reports",
    };

    private static async Task<TestServer> BuildServer(bool allowWrites, bool allowInvestigations, McpDataDetail dataDetail,
        IPseudonymKeySource? pseudonymKeys = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        McpHostService.RegisterMcpServer(builder.Services, allowWrites, allowInvestigations, dataDetail, pseudonymKeys);
        builder.Services.AddMcpServer().WithTools<StandInTools>();

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

    private static JsonElement ResultOf(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        return document.RootElement.Clone();
    }

    private static async Task<string[]> ListedAsync(TestServer server)
    {
        var body = await PostAsync(server, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
        return ResultOf(body).GetProperty("result").GetProperty("tools").EnumerateArray()
            .Select(t => t.GetProperty("name").GetString() ?? "").ToArray();
    }

    private static Task<string> CallAsync(TestServer server, string tool) =>
        PostAsync(server,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"" + tool + "\",\"arguments\":{}}}");

    // ---- the gate: on AND writes off ----

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, true)]
    [InlineData(false, true, false)]
    [InlineData(true, true, false)]
    public void Served_IsInvestigationsOn_AndWritesOff(bool investigations, bool writes, bool expected)
    {
        Assert.Equal(expected, McpInvestigationPolicy.Served(investigations, writes));
        Assert.Equal(expected, McpInvestigationPolicy.Served(
            new McpSettings { AllowInvestigations = investigations, AllowWrites = writes }));
    }

    [Fact]
    public void TheGatedSet_IsTheFourAgentTools_AndNotTheWaitTool()
    {
        Assert.Equal(InvestigationToolNames.OrderBy(n => n), McpInvestigationPolicy.InvestigationTools.OrderBy(n => n));
        Assert.DoesNotContain("wait_for_investigation_requests", McpInvestigationPolicy.InvestigationTools);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Gate_ServesTheFourTools_OnlyWhenInvestigationsAreOnAndWritesAreOff(bool investigations, bool writes)
    {
        var served = investigations && !writes;
        using var server = await BuildServer(writes, investigations, McpDataDetail.Full);

        var listed = await ListedAsync(server);
        foreach (var name in InvestigationToolNames)
        {
            Assert.Equal(served, listed.Contains(name));

            var body = await CallAsync(server, name);
            if (served)
            {
                Assert.DoesNotContain("Unknown tool", body, StringComparison.OrdinalIgnoreCase);
            }
            else
            {
                Assert.Contains("Unknown tool", body, StringComparison.OrdinalIgnoreCase);
            }
        }

        /* The wait tool is never gated, and the write tool follows the write switch alone. */
        Assert.Contains("wait_for_investigation_requests", listed);
        Assert.Equal(writes, listed.Contains("mute_analysis_finding"));
    }

    [Fact]
    public async Task ServedAgentTools_CarryTheirOwnHints_NotTheDestructiveOne()
    {
        using var server = await BuildServer(allowWrites: false, allowInvestigations: true, McpDataDetail.Full);
        var body = await PostAsync(server, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
        var tools = ResultOf(body).GetProperty("result").GetProperty("tools").EnumerateArray()
            .ToDictionary(t => t.GetProperty("name").GetString() ?? "", t => t.GetProperty("annotations"));

        Assert.True(tools["list_investigation_requests"].GetProperty("readOnlyHint").GetBoolean());
        Assert.True(tools["get_investigation_reports"].GetProperty("readOnlyHint").GetBoolean());
        foreach (var name in new[] { "claim_investigation_request", "submit_investigation_report" })
        {
            Assert.False(tools[name].GetProperty("readOnlyHint").GetBoolean());
            Assert.False(tools[name].GetProperty("destructiveHint").GetBoolean());
        }
    }

    [Fact]
    public void StartupLine_IsOneInformationLine_WhenServed_OneCriticalLine_WhenBothOn_AndNothingWhenOff()
    {
        Assert.Null(McpInvestigationPolicy.StartupLine(allowInvestigations: false, allowWrites: false, tokenUsable: true));
        Assert.Null(McpInvestigationPolicy.StartupLine(allowInvestigations: false, allowWrites: true, tokenUsable: true));

        var served = McpInvestigationPolicy.StartupLine(allowInvestigations: true, allowWrites: false, tokenUsable: true);
        Assert.Equal(LogLevel.Information, served!.Value.Level);
        foreach (var name in InvestigationToolNames)
        {
            Assert.Contains(name, served.Value.Message, StringComparison.Ordinal);
        }

        var noToken = McpInvestigationPolicy.StartupLine(allowInvestigations: true, allowWrites: false, tokenUsable: false);
        Assert.Equal(LogLevel.Critical, noToken!.Value.Level);
        Assert.Contains("OFF", noToken.Value.Message, StringComparison.Ordinal);

        var conflict = McpInvestigationPolicy.StartupLine(allowInvestigations: true, allowWrites: true, tokenUsable: true);
        Assert.Equal(LogLevel.Critical, conflict!.Value.Level);
        Assert.Contains("OFF", conflict.Value.Message, StringComparison.Ordinal);
        Assert.Contains("mcp_allow_investigations", conflict.Value.Message, StringComparison.Ordinal);
        Assert.Contains("mcp_allow_writes", conflict.Value.Message, StringComparison.Ordinal);
    }

    // ---- masking through the real filter list ----

    [Fact]
    public async Task DataDetailFull_ReturnsTheStatementText()
    {
        using var server = await BuildServer(allowWrites: false, allowInvestigations: false, McpDataDetail.Full);
        var body = await CallAsync(server, "stand_in_statement");
        Assert.Contains(Secret, body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DataDetailMetadataOnly_WithholdsTheStatementText_ButKeepsTheDuration()
    {
        using var server = await BuildServer(allowWrites: false, allowInvestigations: false, McpDataDetail.MetadataOnly);
        var body = await CallAsync(server, "stand_in_statement");
        Assert.DoesNotContain(Secret, body, StringComparison.Ordinal);
        Assert.Contains("duration_ms", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DataDetailMaskLiterals_ReplacesTheLiteral_ButKeepsTheStatementShape()
    {
        using var server = await BuildServer(allowWrites: false, allowInvestigations: false, McpDataDetail.MaskLiterals);
        var body = await CallAsync(server, "stand_in_statement");
        Assert.DoesNotContain(Secret, body, StringComparison.Ordinal);
        Assert.Contains("dbo.t", body, StringComparison.Ordinal);
    }

    /* #6066 rule 9, end to end: a real tools/call through the real MCP pipeline and the production filter, with the key
       source registered by RegisterMcpServer's own parameter. The other key tests call Mask(result, mode, key) directly. */
    private static string LoginOf(string body)
    {
        using var result = JsonDocument.Parse(ResultOf(body).GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!);
        return result.RootElement.GetProperty("rows")[0].GetProperty("login_name").GetString()!;
    }

    [Fact]
    public async Task DataDetailMetadataOnly_ShowsTheKeyedPseudonym_WhenTheSourceIsRegistered()
    {
        var key = new PseudonymKey(Enumerable.Range(0, PseudonymKey.Length).Select(i => (byte)i).ToArray());
        using var server = await BuildServer(false, false, McpDataDetail.MetadataOnly, new FixedPseudonymKeySource(key));
        string login = LoginOf(await CallAsync(server, "stand_in_login"));
        Assert.Equal(key.Pseudonym("login", "CORP\\Alice"), login);
        Assert.Equal("login#5f484746", login);
    }

    [Fact]
    public async Task DataDetailMetadataOnly_NumbersPerAnswer_WhenNoSourceIsRegistered()
    {
        using var server = await BuildServer(false, false, McpDataDetail.MetadataOnly);
        Assert.Equal("login#1", LoginOf(await CallAsync(server, "stand_in_login")));
    }

    // ---- the two settings ----

    private static McpSettings Load(string? json)
    {
        var dir = Path.Combine(Path.GetTempPath(), "lite-mcp-l4-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            if (json is not null)
            {
                File.WriteAllText(Path.Combine(dir, "settings.json"), json);
            }

            return McpSettings.Load(dir);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Settings_DefaultToInvestigationsOff_AndFullDetail()
    {
        foreach (var settings in new[] { Load(null), Load("{\"mcp_enabled\":true}") })
        {
            Assert.Null(settings.Problem);
            Assert.False(settings.AllowInvestigations);
            Assert.Equal(McpDataDetail.Full, settings.DataDetail);
        }
    }

    [Theory]
    [InlineData("full", McpDataDetail.Full)]
    [InlineData("maskLiterals", McpDataDetail.MaskLiterals)]
    [InlineData("metadataonly", McpDataDetail.MetadataOnly)]
    [InlineData("", McpDataDetail.Full)]
    public void Settings_ReadBothKeys(string word, McpDataDetail expected)
    {
        var settings = Load("{\"mcp_enabled\":true,\"mcp_allow_investigations\":true,\"mcp_data_detail\":\"" + word + "\"}");

        Assert.Null(settings.Problem);
        Assert.True(settings.Enabled);
        Assert.True(settings.AllowInvestigations);
        Assert.Equal(expected, settings.DataDetail);
    }

    [Theory]
    [InlineData("{\"mcp_enabled\":true,\"mcp_allow_investigations\":\"true\"}", "mcp_allow_investigations")]
    [InlineData("{\"mcp_enabled\":true,\"mcp_data_detail\":2}", "mcp_data_detail")]
    [InlineData("{\"mcp_enabled\":true,\"mcp_data_detail\":true}", "mcp_data_detail")]
    public void Settings_AWrongKind_LosesTheEndpoint_AndNamesTheKey(string json, string key)
    {
        var settings = Load(json);

        Assert.NotNull(settings.Problem);
        Assert.Contains(key, settings.Problem, StringComparison.Ordinal);
        Assert.False(settings.Enabled);
    }

    [Fact]
    public void Settings_AnUnknownDataDetailWord_LosesTheEndpoint_AndNamesTheAcceptedWords()
    {
        var settings = Load("{\"mcp_enabled\":true,\"mcp_data_detail\":\"masked\"}");

        Assert.NotNull(settings.Problem);
        Assert.False(settings.Enabled);
        Assert.Contains("masked", settings.Problem, StringComparison.Ordinal);
        Assert.Contains("full", settings.Problem, StringComparison.Ordinal);
        Assert.Contains("maskLiterals", settings.Problem, StringComparison.Ordinal);
        Assert.Contains("metadataOnly", settings.Problem, StringComparison.Ordinal);
    }
}
