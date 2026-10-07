/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5307: darling.json names each key under <c>web.network</c> and <c>mcp.network</c> (their <c>tls</c> blocks and
/// <c>web.network.oidc</c> included) that no config class declares, with its full path and never its value.
/// </summary>
public sealed class DarlingUnknownNetworkKeyTests
{
    private const string Secret = "hunter2-not-a-real-secret";

    private static IReadOnlyList<string> WarningsFor(string json) =>
        DarlingWorker.GetUnknownNetworkKeyWarnings(DarlingConfig.Parse(json));

    [Theory]
    [InlineData("mcp", "mcp.network.ssl")]
    [InlineData("web", "web.network.ssl")]
    public void ASslBlockWrittenWhereTlsWasMeant_GivesOneWarningNamingItsPath(string section, string path)
    {
        var warnings = WarningsFor("{ \"" + section + "\": { \"network\": { \"listen\": \"192.0.2.10\", \"ssl\": { \"pfxPath\": \"x.pfx\" } } } }");

        var warning = Assert.Single(warnings);
        Assert.Contains($"'{path}'", warning, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{ "mcp": { "network": { "tls": { "pfxPath": "x.pfx", "pfxPasswrd": "p" } } } }""", "mcp.network.tls.pfxPasswrd")]
    [InlineData("""{ "web": { "network": { "tls": { "certPath": "c.pem", "keyFile": "k.pem" } } } }""", "web.network.tls.keyFile")]
    [InlineData("""{ "web": { "network": { "oidc": { "authority": "https://idp.example", "clientID2": "c" } } } }""", "web.network.oidc.clientID2")]
    public void AnUnknownKeyInsideATlsOrOidcBlock_IsNamedWithItsFullPath(string json, string path)
    {
        var warning = Assert.Single(WarningsFor(json));
        Assert.Contains($"'{path}'", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryUnknownKeyGetsItsOwnWarning_InFileOrder()
    {
        var warnings = WarningsFor("""
            { "mcp": { "network": { "ssl": 1, "tls": { "oops": 2 } } },
              "web": { "network": { "tokn": 3, "oidc": { "nope": 4 } } } }
            """);

        Assert.Equal(4, warnings.Count);
        Assert.Contains("'mcp.network.ssl'", warnings[0], StringComparison.Ordinal);
        Assert.Contains("'mcp.network.tls.oops'", warnings[1], StringComparison.Ordinal);
        Assert.Contains("'web.network.tokn'", warnings[2], StringComparison.Ordinal);
        Assert.Contains("'web.network.oidc.nope'", warnings[3], StringComparison.Ordinal);
    }

    [Fact]
    public void AFileWithOnlyKnownKeys_LogsNoUnknownKeyWarning()
    {
        /* Every declared key of every checked object, spelled in a different case on purpose: the loader matches
           names ignoring case, so the walk must too. */
        var warnings = WarningsFor("""
            { "mcp": { "Network": { "Listen": "192.0.2.10", "allowFrom": "192.0.2.0/24", "encryptedToken": "e", "token": "t",
                        "hostName": "mcp.example", "TLS": { "pfxPath": "x.pfx", "encryptedPfxPassword": "e", "pfxPassword": "p",
                        "certPath": "c", "keyPath": "k" } } },
              "web": { "network": { "listen": "192.0.2.10", "allowFrom": "192.0.2.0/24", "encryptedToken": "e", "token": "t",
                        "tls": { "pfxPath": "x.pfx" },
                        "oidc": { "authority": "a", "clientId": "c", "encryptedClientSecret": "e", "clientSecret": "s",
                        "scopes": "openid", "subjectClaim": "sub", "roleClaim": "r", "adminRoles": ["a"], "viewerRoles": ["v"] } } } }
            """);

        Assert.Empty(warnings);
        Assert.Empty(WarningsFor("{}"));
        Assert.Empty(WarningsFor("""{ "postgres": { "network": { "somethingElse": 1 } } }"""));
    }

    [Fact]
    public void TheWarningsAndADiagnosticsBundleBuiltFromThatConfig_NeverCarryTheKeysValue()
    {
        var config = DarlingConfig.Parse("""
            { "mcp": { "network": { "ssl": "hunter2-not-a-real-secret", "tls": { "pfxPasswrd": "hunter2-not-a-real-secret" } } },
              "web": { "network": { "oidc": { "clientSecrt": "hunter2-not-a-real-secret" } } } }
            """);

        Assert.Equal(3, config.UnknownNetworkKeys.Count);
        Assert.DoesNotContain(DarlingWorker.GetUnknownNetworkKeyWarnings(config), w => w.Contains(Secret, StringComparison.Ordinal));
        Assert.DoesNotContain(Secret, DiagnosticsBundle.BuildConfigShape(config).ToJsonString(), StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, JsonSerializer.Serialize(config), StringComparison.Ordinal);
        Assert.DoesNotContain("UnknownNetworkKeys", JsonSerializer.Serialize(config), StringComparison.Ordinal);
    }

    /// <summary>
    /// The wizard's owned-key lists are meant to be PARTIAL (everything it does not own is kept, so <c>tls</c>,
    /// <c>oidc</c> and later keys are not listed on purpose): each must be a subset of the matching class's JSON
    /// names, so a renamed key cannot leave the wizard writing one the loader no longer reads.
    /// </summary>
    [Theory]
    [InlineData(nameof(DarlingNetworkConfigEditor.McpNetworkOwnedKeys), typeof(McpNetworkConfig))]
    [InlineData(nameof(DarlingNetworkConfigEditor.WebNetworkOwnedKeys), typeof(WebNetworkConfig))]
    [InlineData(nameof(DarlingNetworkConfigEditor.StoreNetworkOwnedKeys), typeof(PostgresNetworkConfig))]
    public void TheWizardsOwnedKeyList_IsASubsetOfTheClassesJsonNames(string listName, Type configType)
    {
        var list = (IReadOnlyList<string>)typeof(DarlingNetworkConfigEditor)
            .GetField(listName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        var jsonNames = configType.GetProperties()
            .Select(p => p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal);

        Assert.NotEmpty(list);
        Assert.All(list, key => Assert.Contains(key, jsonNames));
    }
}
