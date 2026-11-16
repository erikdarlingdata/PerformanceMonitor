/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8735: the command line's view of <c>web.network.oidc.tokenSignIn</c>: the <c>--validate-config</c> findings and exit code, and
/// <c>--enable-web</c>'s token note.
/// </summary>
public sealed class DarlingTokenSignInCliTests
{
    private const string Oidc = "\"authority\":\"https://login.example.com\",\"clientId\":\"cid\",\"clientSecret\":\"secret\"";

    private static DarlingConfig WebConfig(string networkBody, bool managed = true, bool enabled = true) =>
        DarlingConfig.Parse("{\"postgres\":{\"managed\":" + (managed ? "true" : "false") + "},\"web\":{\"enabled\":" + (enabled ? "true" : "false") + ",\"network\":{" + networkBody + "}}}");

    private static DarlingConfig WithOidc(string listen, string signIn, string extra = "") =>
        WebConfig(
            $"\"listen\":\"{listen}\",\"allowFrom\":\"10.0.0.0/8\"{extra},\"oidc\":{{{Oidc}"
            + (signIn.Length == 0 ? "" : $",\"tokenSignIn\":\"{signIn}\"") + "}");

    [Theory]
    [InlineData("")]
    [InlineData("on")]
    [InlineData("  ON ")]
    public void Findings_OnOrMissing_Empty(string signIn)
    {
        Assert.Empty(DarlingCliCommands.TokenSignInFindings(WithOidc("0.0.0.0", signIn)));
    }

    [Fact]
    public void Findings_NoOidcBlock_Empty()
    {
        Assert.Empty(DarlingCliCommands.TokenSignInFindings(WebConfig("\"listen\":\"0.0.0.0\"")));
    }

    [Fact]
    public void Findings_Unknown_IsError()
    {
        var line = Assert.Single(DarlingCliCommands.TokenSignInFindings(WithOidc("0.0.0.0", "bogus")));

        Assert.StartsWith("ERROR:", line, StringComparison.Ordinal);
        Assert.Contains("tokenSignIn", line, StringComparison.Ordinal);
        Assert.Contains("bogus", line, StringComparison.Ordinal);
    }

    [Fact]
    public void Findings_OffWithoutOidc_IsError()
    {
        var config = WebConfig("\"listen\":\"0.0.0.0\",\"allowFrom\":\"10.0.0.0/8\",\"oidc\":{\"tokenSignIn\":\"off\"}");

        var line = Assert.Single(DarlingCliCommands.TokenSignInFindings(config));
        Assert.StartsWith("ERROR:", line, StringComparison.Ordinal);
        Assert.Contains("OIDC is not configured", line, StringComparison.Ordinal);
    }

    [Fact]
    public void Findings_OffWithOidcNotExposed_IsError()
    {
        var line = Assert.Single(DarlingCliCommands.TokenSignInFindings(WithOidc("127.0.0.1", "off")));

        Assert.StartsWith("ERROR:", line, StringComparison.Ordinal);
        Assert.Contains("not exposed", line, StringComparison.Ordinal);
    }

    [Fact]
    public void Findings_OffWithOidcExposed_IsInfoOnly()
    {
        var line = Assert.Single(DarlingCliCommands.TokenSignInFindings(WithOidc("0.0.0.0", "off")));

        Assert.StartsWith("INFO:", line, StringComparison.Ordinal);
        Assert.Contains("'off'", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(",\"token\":\"t\"")]
    [InlineData(",\"token\":\"t\",\"trustedProxies\":[\"10.1.2.3\"]")]
    public void Findings_LoopbackExposed_HasNoteNamingEnvoyLinkerdAndOff_WhetherOrNotProxiesAreListed(string extra)
    {
        var findings = DarlingCliCommands.TokenSignInFindings(WithOidc("0.0.0.0", "loopback", extra));

        Assert.Equal(2, findings.Count);
        Assert.StartsWith("INFO:", findings[0], StringComparison.Ordinal);
        Assert.StartsWith("NOTE:", findings[1], StringComparison.Ordinal);
        Assert.Contains("Envoy", findings[1], StringComparison.Ordinal);
        Assert.Contains("Linkerd", findings[1], StringComparison.Ordinal);
        Assert.Contains("'off'", findings[1], StringComparison.Ordinal);
    }

    [Fact]
    public void Findings_OffWithOidcAndExposedListen_ButNoAllowFrom_IsError()
    {
        var config = WebConfig($"\"listen\":\"0.0.0.0\",\"oidc\":{{{Oidc},\"tokenSignIn\":\"off\"}}");

        var line = Assert.Single(DarlingCliCommands.TokenSignInFindings(config));
        Assert.StartsWith("ERROR:", line, StringComparison.Ordinal);
        Assert.Contains("not exposed", line, StringComparison.Ordinal);
    }

    [Fact]
    public void Findings_OffWithOidcAndExposedListen_ButAnUnmanagedStore_IsError()
    {
        var config = WebConfig(
            $"\"listen\":\"0.0.0.0\",\"allowFrom\":\"10.0.0.0/8\",\"oidc\":{{{Oidc},\"tokenSignIn\":\"off\"}}", managed: false);

        var line = Assert.Single(DarlingCliCommands.TokenSignInFindings(config, inContainer: false));
        Assert.StartsWith("ERROR:", line, StringComparison.Ordinal);
        Assert.Contains("not exposed", line, StringComparison.Ordinal);

        /* The same configuration in a container is exposure-capable, as the service sees it. */
        Assert.StartsWith("INFO:", Assert.Single(DarlingCliCommands.TokenSignInFindings(config, inContainer: true)), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("off")]
    [InlineData("bogus")]
    [InlineData("loopback")]
    public void Findings_DashboardDisabled_IsOneInfoLine_WhateverTheSetting(string signIn)
    {
        var findings = DarlingCliCommands.TokenSignInFindings(
            WebConfig($"\"oidc\":{{{Oidc},\"tokenSignIn\":\"{signIn}\"}}", enabled: false));

        var line = Assert.Single(findings);
        Assert.StartsWith("INFO:", line, StringComparison.Ordinal);
        Assert.Contains("not checked here", line, StringComparison.Ordinal);
        Assert.Contains("--enable-web", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateConfig_DashboardDisabledWithALeftoverOffAndNoListen_ExitsZero()
    {
        var dir = Directory.CreateTempSubdirectory("token-sign-in-validate-").FullName;
        try
        {
            var path = Path.Combine(dir, "darling.json");
            File.WriteAllText(
                path,
                "{\"postgres\":{\"managed\":true,\"dataDirectory\":" + JsonSerializer.Serialize(Path.Combine(dir, "pgdata")) + "},"
                + "\"web\":{\"enabled\":false,\"network\":{\"oidc\":{" + Oidc + ",\"tokenSignIn\":\"off\"}}}}");

            using var output = new StringWriter();
            using var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(path, output, error, _ => null, TestContext.Current.CancellationToken);

            Assert.Equal(0, exit);
            Assert.Contains("INFO: web.enabled is false", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("ERROR:", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("NOTE: web.network.oidc.tokenSignIn", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Findings_LoopbackNotExposed_IsInfoOnly()
    {
        var findings = DarlingCliCommands.TokenSignInFindings(WithOidc("127.0.0.1", "loopback"));

        var line = Assert.Single(findings);
        Assert.StartsWith("INFO:", line, StringComparison.Ordinal);
    }

    [Fact]
    public void EnableTokenNote_OffWithOidc_NoToken_IsNull()
    {
        Assert.Null(DarlingCliCommands.EnableTokenNote(mcp: false, WithOidc("0.0.0.0", "off")));
    }

    [Fact]
    public void EnableTokenNote_LoopbackWithOidc_NoToken_StillWarns()
    {
        var note = DarlingCliCommands.EnableTokenNote(mcp: false, WithOidc("0.0.0.0", "loopback"));

        Assert.NotNull(note);
        Assert.Contains("no token", note, StringComparison.Ordinal);
    }

    [Fact]
    public void EnableTokenNote_OffWithoutOidc_NoToken_StillWarns()
    {
        Assert.NotNull(DarlingCliCommands.EnableTokenNote(
            mcp: false, WebConfig("\"listen\":\"0.0.0.0\",\"oidc\":{\"tokenSignIn\":\"off\"}")));
    }

    [Fact]
    public void DisabledExposure_DropsTheOidcBlockAndWithItTokenSignIn()
    {
        var original = "{\"web\":{\"network\":{\"listen\":\"0.0.0.0\",\"allowFrom\":\"10.0.0.0/8\",\"encryptedToken\":\"x\",\"oidc\":{"
            + Oidc + ",\"tokenSignIn\":\"off\"}}}}";

        var reparsed = DarlingConfig.Parse(DarlingCliCommands.DisabledExposureText(original).NewText);

        Assert.Null(reparsed.Web.Network?.Oidc);
    }

    [Fact]
    public async Task ValidateConfig_ExitsNonZero_OnTokenSignInError()
    {
        var dir = Directory.CreateTempSubdirectory("token-sign-in-validate-").FullName;
        try
        {
            var path = WriteConfig(dir, "\"encryptedToken\":\"x\",", "bogus");

            using var output = new StringWriter();
            using var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(path, output, error, _ => null, TestContext.Current.CancellationToken);

            Assert.Equal(1, exit);
            var line = output.ToString().Split('\n').Single(l => l.StartsWith("ERROR:", StringComparison.Ordinal));
            Assert.Contains("tokenSignIn", line, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ValidateConfig_OffWithExposedListenButNoAllowFrom_ExitsNonZero()
    {
        var dir = Directory.CreateTempSubdirectory("token-sign-in-validate-").FullName;
        try
        {
            var path = WriteConfig(dir, "", "off", allowFrom: false);

            using var output = new StringWriter();
            using var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(path, output, error, _ => null, TestContext.Current.CancellationToken);

            Assert.Equal(1, exit);
            Assert.Contains("ERROR: web.network.oidc.tokenSignIn is 'off'", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ValidateConfig_ANoteOnlyConfiguration_ExitsZero()
    {
        var dir = Directory.CreateTempSubdirectory("token-sign-in-validate-").FullName;
        try
        {
            var path = WriteConfig(dir, "\"token\":\"t\",", "loopback");

            using var output = new StringWriter();
            using var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(path, output, error, _ => null, TestContext.Current.CancellationToken);

            Assert.Equal(0, exit);
            Assert.Contains("NOTE: web.network.oidc.tokenSignIn is 'loopback'", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("ERROR:", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("off", false)]
    [InlineData("on", true)]
    public async Task ValidateConfig_WebTokenEqualToMcpToken_WarnsExceptUnderOff(string signIn, bool warns)
    {
        var dir = Directory.CreateTempSubdirectory("token-sign-in-validate-").FullName;
        try
        {
            var path = WriteConfig(dir, "\"token\":\"" + SharedValue + "\",", signIn, mcpToken: SharedValue);

            using var output = new StringWriter();
            using var error = new StringWriter();
            await DarlingCliCommands.ValidateConfigAsync(path, output, error, _ => null, TestContext.Current.CancellationToken);

            Assert.Equal(warns, output.ToString().Contains(DarlingAgentDispatchChecks.WebTokenSameAsMcpMessage, StringComparison.Ordinal));
            Assert.DoesNotContain(SharedValue, output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ValidateConfig_AValidTokenSignIn_PrintsInfoAndDoesNotFail()
    {
        var dir = Directory.CreateTempSubdirectory("token-sign-in-validate-").FullName;
        try
        {
            var path = WriteConfig(dir, "", "off");

            using var output = new StringWriter();
            using var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(path, output, error, _ => null, TestContext.Current.CancellationToken);

            Assert.Equal(0, exit);
            Assert.Contains("INFO: web.network.oidc.tokenSignIn is 'off'", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("ERROR:", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>A darling.json whose managed store has no stored credential under the temp directory (so no store is probed and no servers are listed) and whose web block holds OIDC with the given mode.</summary>
    private const string SharedValue = "a-shared-token-value-0123456789abcdef";

    private static string WriteConfig(string dir, string tokenMember, string signIn, bool allowFrom = true, string? mcpToken = null)
    {
        var path = Path.Combine(dir, "darling.json");
        File.WriteAllText(
            path,
            "{\"postgres\":{\"managed\":true,\"dataDirectory\":" + JsonSerializer.Serialize(Path.Combine(dir, "pgdata")) + "},"
            + (mcpToken is null ? "" : "\"mcp\":{\"network\":{\"token\":\"" + mcpToken + "\"}},")
            + "\"web\":{\"enabled\":true,\"network\":{\"listen\":\"0.0.0.0\"," + (allowFrom ? "\"allowFrom\":\"10.0.0.0/8\"," : "") + tokenMember
            + "\"oidc\":{" + Oidc + ",\"tokenSignIn\":\"" + signIn + "\"}}}}");
        return path;
    }
}
