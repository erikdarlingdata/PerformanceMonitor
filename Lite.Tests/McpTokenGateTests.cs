/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's MCP server uses a token once it is turned on, like Darling's: a request with no token or a wrong one is refused
/// 401 before the handler, the right token is served, <c>mcp_allow_tokenless_loopback</c> serves without one, and the first
/// start with MCP on and no stored token makes one and keeps it.
/// </summary>
public sealed class McpTokenGateTests
{
    private const string Token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    private static async Task<(int Status, int Reached, string? Challenge)> SendAsync(string? gateToken, string? authorization)
    {
        var reached = 0;
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        await using var app = builder.Build();
        McpHostService.UseBearerTokenGate(app, gateToken);
        app.Run(context =>
        {
            reached++;
            context.Response.StatusCode = StatusCodes.Status200OK;
            return Task.CompletedTask;
        });
        await app.StartAsync();

        using var client = app.GetTestClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/");
        if (authorization is not null)
        {
            request.Headers.TryAddWithoutValidation("Authorization", authorization);
        }

        using var response = await client.SendAsync(request);
        string? challenge = response.Headers.WwwAuthenticate.Count > 0 ? response.Headers.WwwAuthenticate.ToString() : null;
        return ((int)response.StatusCode, reached, challenge);
    }

    [Fact]
    public async Task NoToken_IsRefused401_AndNeverReachesTheHandler()
    {
        var (status, reached, challenge) = await SendAsync(Token, authorization: null);

        Assert.Equal(StatusCodes.Status401Unauthorized, status);
        Assert.Equal(0, reached);
        Assert.Equal("Bearer", challenge);
    }

    [Theory]
    [InlineData("Bearer 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdee")]
    [InlineData("Bearer ")]
    [InlineData("Bearer")]
    [InlineData("Basic 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")]
    [InlineData("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")]
    [InlineData("Bearer 0123456789abcdef")]
    public async Task AWrongOrMalformedToken_IsRefused401_AndNeverReachesTheHandler(string authorization)
    {
        var (status, reached, _) = await SendAsync(Token, authorization);

        Assert.Equal(StatusCodes.Status401Unauthorized, status);
        Assert.Equal(0, reached);
    }

    [Theory]
    [InlineData("Bearer 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")]
    [InlineData("bearer 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")]
    public async Task TheRightToken_IsServed(string authorization)
    {
        var (status, reached, _) = await SendAsync(Token, authorization);

        Assert.Equal(StatusCodes.Status200OK, status);
        Assert.Equal(1, reached);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task TheOptOut_ServesWithNoToken(string? gateToken)
    {
        var (status, reached, _) = await SendAsync(gateToken, authorization: null);

        Assert.Equal(StatusCodes.Status200OK, status);
        Assert.Equal(1, reached);
    }

    // ---- the token store ----

    private sealed class FakeVault : IMcpTokenVault
    {
        public string? Stored;
        public CredentialReadOutcome ReadOutcome = CredentialReadOutcome.NotFound;
        public string? WriteFailure;
        public int Writes;

        public CredentialReadOutcome Read(out string? token, out string? failure)
        {
            token = Stored;
            failure = ReadOutcome == CredentialReadOutcome.Unreadable ? "vault is locked" : null;
            return Stored is not null && ReadOutcome == CredentialReadOutcome.NotFound ? CredentialReadOutcome.Found : ReadOutcome;
        }

        public string? Write(string token)
        {
            if (WriteFailure is not null) return WriteFailure;
            Writes++;
            Stored = token;
            return null;
        }
    }

    [Fact]
    public void FirstStartWithMcpOn_AndNoToken_MakesOne_AndKeepsIt()
    {
        var vault = new FakeVault();
        var store = new McpTokenStore(vault);

        var first = store.Resolve(new McpSettings { Enabled = true }, out var failure, out var created);

        Assert.Null(failure);
        Assert.True(created);
        Assert.NotNull(first);
        Assert.Matches(new Regex("^[0-9a-f]{64}$"), first);
        Assert.Equal(first, vault.Stored);

        var second = store.Resolve(new McpSettings { Enabled = true }, out failure, out created);

        Assert.Null(failure);
        Assert.False(created);
        Assert.Equal(first, second);
        Assert.Equal(1, vault.Writes);
    }

    [Fact]
    public void TwoTokensMadeAreNotTheSame()
        => Assert.NotEqual(McpTokenStore.Generate(), McpTokenStore.Generate());

    [Fact]
    public void TheOptOut_MakesNoToken_AndWritesNothing()
    {
        var vault = new FakeVault();

        var token = new McpTokenStore(vault).Resolve(
            new McpSettings { Enabled = true, AllowTokenlessLoopback = true }, out var failure, out var created);

        Assert.Null(token);
        Assert.Null(failure);
        Assert.False(created);
        Assert.Equal(0, vault.Writes);
    }

    [Fact]
    public void AnEntryThatCannotBeRead_IsNeverReplaced_AndRefusesTheStart()
    {
        var vault = new FakeVault { ReadOutcome = CredentialReadOutcome.Unreadable, Stored = "old" };

        var token = new McpTokenStore(vault).Resolve(new McpSettings { Enabled = true }, out var failure, out _);

        Assert.Null(token);
        Assert.Contains("vault is locked", failure, StringComparison.Ordinal);
        Assert.Equal(0, vault.Writes);
        Assert.Equal("old", vault.Stored);
    }

    [Fact]
    public void ATokenThatCannotBeSaved_RefusesTheStart()
    {
        var vault = new FakeVault { WriteFailure = "access denied" };

        var token = new McpTokenStore(vault).Resolve(new McpSettings { Enabled = true }, out var failure, out var created);

        Assert.Null(token);
        Assert.False(created);
        Assert.Contains("access denied", failure, StringComparison.Ordinal);
    }

    [Fact]
    public void TheClientCommand_CarriesTheTokenHeader()
        => Assert.Equal(
            "claude mcp add --transport http --scope user sql-monitor http://localhost:5151/ --header \"Authorization: Bearer " + Token + "\"",
            McpTokenStore.ClientCommand(5151, Token));

    // ---- the setting ----

    private static McpSettings LoadWith(string json)
    {
        var dir = Path.Combine(Path.GetTempPath(), "lite-mcp-token-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "settings.json"), json);
            return McpSettings.Load(dir);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void TheTokenIsRequiredByDefault()
    {
        Assert.False(new McpSettings().AllowTokenlessLoopback);
        Assert.False(LoadWith("{ \"mcp_enabled\": true }").AllowTokenlessLoopback);
    }

    [Fact]
    public void TheOptOutIsReadFromSettingsJson()
        => Assert.True(LoadWith("{ \"mcp_enabled\": true, \"mcp_allow_tokenless_loopback\": true }").AllowTokenlessLoopback);

    [Fact]
    public void AQuotedOptOut_KeepsTheEndpointOff_AndNamesTheKey()
    {
        var settings = LoadWith("{ \"mcp_enabled\": true, \"mcp_allow_tokenless_loopback\": \"true\" }");

        Assert.False(settings.Enabled);
        Assert.Contains("mcp_allow_tokenless_loopback", settings.Problem, StringComparison.Ordinal);
    }
}
