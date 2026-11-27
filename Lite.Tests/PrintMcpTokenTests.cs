/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9185: <c>--print-mcp-token</c> gives a headless Lite's token to the Windows user that runs it. It prints the stored token and
/// exits 0. It never makes or changes one. A missing token, an unreadable entry and an empty entry print nothing to standard
/// output and exit with a code of their own. The token reaches standard output and nothing else, and it is the token the MCP gate
/// accepts. The vault is a fake, so no test reads or writes Credential Manager.
/// </summary>
public sealed class PrintMcpTokenTests
{
    private const string Token = "9f8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c5b4a39281706f5e4d3c2b1a0";

    private sealed class FakeVault : IMcpTokenVault
    {
        public CredentialReadOutcome Outcome = CredentialReadOutcome.NotFound;
        public string? Stored;
        public string? ReadFailure;
        public int Writes;
        public int Reads;

        public CredentialReadOutcome Read(out string? token, out string? failure)
        {
            Reads++;
            token = Stored;
            failure = ReadFailure;
            return Outcome;
        }

        public string? Write(string token)
        {
            Writes++;
            return null;
        }
    }

    private static (int Code, string Out, string Err) RunWith(FakeVault vault)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var code = PrintMcpTokenCommand.Run(new McpTokenStore(vault), output, error);
        return (code, output.ToString(), error.ToString());
    }

    private static FakeVault Holding(string token) => new() { Outcome = CredentialReadOutcome.Found, Stored = token };

    [Fact]
    public void AStoredToken_IsPrintedWithANewline_AndExitsZero_AndNothingIsWritten()
    {
        var vault = Holding(Token);

        var (code, output, error) = RunWith(vault);

        Assert.Equal(PrintMcpTokenCommand.ExitPrinted, code);
        Assert.Equal(0, code);
        Assert.Equal(Token + Environment.NewLine, output);
        Assert.Equal(string.Empty, error);
        Assert.Equal(0, vault.Writes);
    }

    [Fact]
    public void NoStoredToken_PrintsNothingToStdout_SaysSoOnStderr_ExitsNonZero_AndMakesNoToken()
    {
        var vault = new FakeVault { Outcome = CredentialReadOutcome.NotFound };

        var (code, output, error) = RunWith(vault);

        Assert.Equal(PrintMcpTokenCommand.ExitNoToken, code);
        Assert.NotEqual(0, code);
        Assert.Equal(string.Empty, output);
        Assert.Single(error.Split('\n', StringSplitOptions.RemoveEmptyEntries));
        Assert.Equal(0, vault.Writes);
        Assert.Null(vault.Stored);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AnUnreadableOrEmptyEntry_PrintsNothingToStdout_UsesTheOtherCode_AndWritesNothing(bool unreadable)
    {
        var vault = unreadable
            ? new FakeVault { Outcome = CredentialReadOutcome.Unreadable, ReadFailure = "Windows returned error 5", Stored = Token }
            : new FakeVault { Outcome = CredentialReadOutcome.Found, Stored = "  " };

        var (code, output, error) = RunWith(vault);

        Assert.Equal(PrintMcpTokenCommand.ExitReadFailed, code);
        Assert.NotEqual(PrintMcpTokenCommand.ExitNoToken, code);
        Assert.NotEqual(0, code);
        Assert.Equal(string.Empty, output);
        Assert.NotEqual(string.Empty, error);
        Assert.DoesNotContain(Token, error, StringComparison.Ordinal);
        Assert.Equal(0, vault.Writes);
        if (unreadable)
        {
            Assert.Contains("Windows returned error 5", error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheTokenReachesStdoutOnly_NotStderrAndNotTheLog()
    {
        AppLogger.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
        AppLogger.DrainBufferedLines();
        try
        {
            var vault = Holding(Token);
            var store = new McpTokenStore(vault);
            var output = new StringWriter();
            var error = new StringWriter();

            PrintMcpTokenCommand.Run(store, output, error);

            Assert.Contains(Token, output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain(Token, error.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain(AppLogger.DrainBufferedLines(), line => line.Contains(Token, StringComparison.Ordinal));
        }
        finally
        {
            AppLogger.SetMinimumLevel(AppLogger.DefaultMinimumLevel);
        }
    }

    [Fact]
    public void TheArgument_PlansAStartThatTakesNoLockShowsNothingAndStartsNothing()
    {
        string[][] argSets = [["--print-mcp-token"], ["--PRINT-MCP-TOKEN"], ["--headless", "--print-mcp-token"]];
        foreach (var args in argSets)
        {
            var plan = LiteStartupPlan.FromArgs(args);

            Assert.True(plan.PrintMcpToken);
            Assert.False(plan.StartsServices);
            Assert.False(plan.TakesInstanceLock);
            Assert.False(plan.ShowsMainWindow);
            Assert.False(plan.CreatesTrayIcon);
        }

        var ordinary = LiteStartupPlan.FromArgs([]);
        Assert.False(ordinary.PrintMcpToken);
        Assert.True(ordinary.StartsServices);
        Assert.True(ordinary.TakesInstanceLock);
    }

    [Fact]
    public void ThePrintedToken_IsTheOneTheHostResolves_AndTheOneTheGateAccepts()
    {
        var vault = Holding(Token);
        var store = new McpTokenStore(vault);

        var (_, output, _) = RunWith(vault);
        var printed = output.Trim();
        var hostToken = store.Resolve(new McpSettings { Enabled = true }, out var failure, out var created);

        Assert.Null(failure);
        Assert.False(created);
        Assert.Equal(hostToken, printed);
        Assert.True(McpTokenStore.IsAuthorized("Bearer " + printed, hostToken!));
        Assert.False(McpTokenStore.IsAuthorized(null, hostToken!));
        Assert.False(McpTokenStore.IsAuthorized("Bearer " + printed[..^1] + "1", hostToken!));
    }

    [Fact]
    public async Task TheGate_ServesARequestCarryingThePrintedToken_AndRefusesOneWithoutIt()
    {
        var vault = Holding(Token);
        var printed = RunWith(vault).Out.Trim();

        var (withToken, reachedWith) = await SendAsync(printed, "Bearer " + printed);
        var (without, reachedWithout) = await SendAsync(printed, authorization: null);

        Assert.Equal(StatusCodes.Status200OK, withToken);
        Assert.Equal(1, reachedWith);
        Assert.Equal(StatusCodes.Status401Unauthorized, without);
        Assert.Equal(0, reachedWithout);
    }

    private static async Task<(int Status, int Reached)> SendAsync(string gateToken, string? authorization)
    {
        var reached = 0;
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        await using var app = builder.Build();
        McpHostService.UseBearerTokenGate(app, gateToken, log: _ => { });
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
        return ((int)response.StatusCode, reached);
    }
}
