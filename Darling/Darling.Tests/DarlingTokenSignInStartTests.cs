/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The start-up side of <c>web.network.oidc.tokenSignIn</c> (#8735): the bind ladder, the rule that stops a start that
/// cannot honor the setting, and the lines a start logs for it. All three decisions are PURE.
/// </summary>
public sealed class DarlingTokenSignInStartTests
{
    private static WebConfig Web(string? tokenSignIn, bool withOidc = true, bool withToken = false)
    {
        var oidc = withOidc
            ? $"\"oidc\": {{ \"authority\": \"https://login.example.com\", \"clientId\": \"client-1\"{(tokenSignIn is null ? "" : $", \"tokenSignIn\": \"{tokenSignIn}\"")} }},"
            : "";
        var token = withToken ? "\"token\": \"a-long-enough-token-value-1234567890\"," : "";
        var json = $"{{ \"enabled\": true, \"network\": {{ {oidc} {token} \"listen\": \"192.168.1.5\", \"allowFrom\": \"192.168.1.0/24\" }} }}";
        return JsonSerializer.Deserialize<WebConfig>(json)!;
    }

    [Fact]
    public void ResolveWebBind_Off_WithOidc_NoToken_IsExposed()
    {
        var bind = DarlingWebHostService.ResolveWebBind(Web("off"), managed: true, inContainer: false);

        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, bind.Mode);
    }

    [Theory]
    [InlineData("loopback")]
    [InlineData(null)]
    [InlineData("bogus")]
    public void ResolveWebBind_NotOff_NoToken_StaysTokenMissing(string? mode)
    {
        var bind = DarlingWebHostService.ResolveWebBind(Web(mode), managed: true, inContainer: false);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, bind.Mode);
    }

    [Fact]
    public void ResolveWebBind_OffWithoutOidcConfigured_StaysTokenMissing()
    {
        var bind = DarlingWebHostService.ResolveWebBind(Web("off", withOidc: false), managed: true, inContainer: false);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, bind.Mode);
    }

    [Fact]
    public void ResolveWebBind_Off_WithOidcAndAToken_IsExposed()
    {
        var bind = DarlingWebHostService.ResolveWebBind(Web("off", withToken: true), managed: true, inContainer: false);

        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, bind.Mode);
    }

    [Fact]
    public void StartProblem_UnknownValue_QuotesItAndNamesTheThreeWords()
    {
        var line = DarlingWebHostService.TokenSignInStartProblem("bogus", true, true, true);

        Assert.NotNull(line);
        Assert.Contains("bogus", line, StringComparison.Ordinal);
        Assert.Contains("'on'", line, StringComparison.Ordinal);
        Assert.Contains("'loopback'", line, StringComparison.Ordinal);
        Assert.Contains("'off'", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("off", true, true, true, false)]
    [InlineData("OFF ", true, true, true, false)]
    [InlineData("off", true, true, false, true)]
    [InlineData("off", true, false, true, true)]
    [InlineData("off", false, false, false, true)]
    [InlineData("loopback", true, false, false, false)]
    [InlineData("on", false, false, false, false)]
    [InlineData(null, false, false, false, false)]
    [InlineData("", false, false, false, false)]
    public void StartProblem_Cases(string? raw, bool oidcConfigured, bool networkMode, bool oidcClientPresent, bool expectProblem)
    {
        var line = DarlingWebHostService.TokenSignInStartProblem(raw, oidcConfigured, networkMode, oidcClientPresent);

        Assert.Equal(expectProblem, line is not null);
    }

    [Theory]
    [InlineData(false, false, "OIDC is not configured")]
    [InlineData(true, false, "OIDC was refused")]
    public void StartProblem_Off_NamesWhatIsMissing(bool oidcConfigured, bool oidcClientPresent, string expected)
    {
        var line = DarlingWebHostService.TokenSignInStartProblem("off", oidcConfigured, true, oidcClientPresent);

        Assert.Contains(expected, line, StringComparison.Ordinal);
        Assert.Contains("'loopback'", line, StringComparison.Ordinal);
    }

    [Fact]
    public void StartProblem_Off_NotExposed_NamesTheDashboard()
    {
        var line = DarlingWebHostService.TokenSignInStartProblem("off", true, false, true);

        Assert.Contains("not exposed", line, StringComparison.Ordinal);
    }

    [Fact]
    public void StartLines_On_IsEmpty()
    {
        Assert.Empty(DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.On, hasTrustedProxies: false));
    }

    [Fact]
    public void StartLines_Loopback_WithoutTrustedProxies_WarnsNamingEnvoyAndLinkerd()
    {
        var lines = DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.Loopback, hasTrustedProxies: false);

        var warning = Assert.Single(lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("Envoy", warning.Message, StringComparison.Ordinal);
        Assert.Contains("Linkerd", warning.Message, StringComparison.Ordinal);
        Assert.Single(lines, l => l.Level == LogLevel.Information);
    }

    [Fact]
    public void StartLines_Loopback_WithTrustedProxies_HasNoWarning()
    {
        var lines = DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.Loopback, hasTrustedProxies: true);

        Assert.DoesNotContain(lines, l => l.Level == LogLevel.Warning);
        Assert.Single(lines);
    }

    [Fact]
    public void StartLines_Off_IsOneInformationLine()
    {
        var lines = DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.Off, hasTrustedProxies: false);

        var line = Assert.Single(lines);
        Assert.Equal(LogLevel.Information, line.Level);
    }

    [Fact]
    public void TryStartServerAsync_Wiring()
    {
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs");

        Assert.Contains("TokenSignInStartProblem(", web, StringComparison.Ordinal);
        Assert.Contains("TokenSignInStartLines(", web, StringComparison.Ordinal);
        Assert.Contains("tokenSignIn: tokenSignIn", web, StringComparison.Ordinal);

        /* The check runs after the last degrade and before the pipeline is configured. */
        var check = web.IndexOf("var tokenSignInProblem = TokenSignInStartProblem(", StringComparison.Ordinal);
        var tls = web.IndexOf("ExposesWithoutItsCertificate)", StringComparison.Ordinal);
        var pipeline = web.IndexOf("ConfigurePipeline(_app,", StringComparison.Ordinal);
        Assert.True(tls >= 0 && check > tls && pipeline > check);
    }
}
