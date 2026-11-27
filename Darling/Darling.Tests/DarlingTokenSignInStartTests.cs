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
    public void StartProblem_Off_NamesWhatIsMissing(bool oidcConfigured, bool oidcClientPresent, string expected)
    {
        var line = DarlingWebHostService.TokenSignInStartProblem("off", oidcConfigured, true, oidcClientPresent);

        Assert.Contains(expected, line, StringComparison.Ordinal);
        Assert.Contains("'loopback'", line, StringComparison.Ordinal);
    }

    [Fact]
    public void StartProblem_Off_OidcRefused_NamesTheRefusal()
    {
        var line = DarlingWebHostService.TokenSignInStartProblem("off", true, true, false);

        Assert.Contains("OIDC was refused", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void StartProblem_Off_NotExposed_NamesTheDashboard_NotAnOidcRefusal(bool oidcClientPresent)
    {
        var line = DarlingWebHostService.TokenSignInStartProblem("off", true, false, oidcClientPresent);

        Assert.Contains("not exposed", line, StringComparison.Ordinal);
        Assert.DoesNotContain("refused at start", line, StringComparison.Ordinal);
    }

    [Fact]
    public void StartLines_On_IsEmpty()
    {
        Assert.Empty(DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.On, networkMode: true, hasTrustedProxies: false, oidcClientPresent: true));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StartLines_Loopback_Exposed_WarnsNamingEnvoyLinkerdAndOff_WhetherOrNotProxiesAreListed(bool hasTrustedProxies)
    {
        var lines = DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.Loopback, networkMode: true, hasTrustedProxies, oidcClientPresent: true);

        var warning = Assert.Single(lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("Envoy", warning.Message, StringComparison.Ordinal);
        Assert.Contains("Linkerd", warning.Message, StringComparison.Ordinal);
        Assert.Contains("'off'", warning.Message, StringComparison.Ordinal);
        Assert.Contains("X-Forwarded-For", warning.Message, StringComparison.Ordinal);
        Assert.Single(lines, l => l.Level == LogLevel.Information);
    }

    [Fact]
    public void StartLines_Loopback_LoopbackOnlyMode_HasNoWarning()
    {
        var lines = DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.Loopback, networkMode: false, hasTrustedProxies: false, oidcClientPresent: true);

        Assert.DoesNotContain(lines, l => l.Level == LogLevel.Warning);
        Assert.Single(lines);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public void StartLines_Loopback_InformationLine_MentionsOidcOnlyWhenThereIsAClient(bool oidcClientPresent, bool mentionsOidc)
    {
        var lines = DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.Loopback, networkMode: false, hasTrustedProxies: false, oidcClientPresent);

        var info = Assert.Single(lines, l => l.Level == LogLevel.Information);
        Assert.Equal(mentionsOidc, info.Message.Contains("OIDC", StringComparison.Ordinal));
    }

    [Fact]
    public void StartLines_Off_IsOneInformationLine()
    {
        var lines = DarlingWebHostService.TokenSignInStartLines(TokenSignInMode.Off, networkMode: true, hasTrustedProxies: false, oidcClientPresent: true);

        var line = Assert.Single(lines);
        Assert.Equal(LogLevel.Information, line.Level);
    }

    private static TokenSignInMode Mode(string raw)
    {
        Assert.True(DarlingTokenSignIn.TryParse(raw, out var mode));
        return mode;
    }

    [Theory]
    [InlineData("on", " The shared token keeps working beside OIDC as the scripted-caller/break-glass path.")]
    [InlineData("loopback", " The shared token signs in from this machine only.")]
    [InlineData("off", "")]
    public void OidcStartLineTokenNote_FollowsTheMode(string rawMode, string expected)
    {
        Assert.Equal(expected, DarlingWebHostService.OidcStartLineTokenNote(Mode(rawMode)));
    }

    [Theory]
    [InlineData("on", "This token gates web dashboard network access.")]
    [InlineData("loopback", "This token gates web dashboard network access.")]
    [InlineData("off", "web.network.oidc.tokenSignIn is 'off', so this token signs nobody in.")]
    public void PlaintextTokenWarning_FollowsTheMode(string rawMode, string expectedTail)
    {
        var text = DarlingWebHostService.PlaintextTokenWarning(Mode(rawMode));

        Assert.StartsWith("web.network.token is set in plaintext (dev convenience): prefer web.network.encryptedToken (produced by --encrypt-password). ", text, StringComparison.Ordinal);
        Assert.EndsWith(expectedTail, text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("on", "the shared token remains the only web credential.")]
    [InlineData("loopback", "the shared token remains the only web credential, from this machine.")]
    [InlineData("off", "web.network.oidc.tokenSignIn is 'off', so no token signs in and the dashboard is not started.")]
    public void OidcRefusedConsequence_FollowsTheMode(string rawMode, string expected)
    {
        Assert.Equal(expected, DarlingWebHostService.OidcRefusedConsequence(Mode(rawMode)));
    }
}
