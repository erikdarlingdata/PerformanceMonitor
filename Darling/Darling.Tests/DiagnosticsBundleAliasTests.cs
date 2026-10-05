/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the bundle's name aliasing and its verifier. Names are synthetic slugs.</summary>
public sealed class DiagnosticsBundleAliasTests
{
    private static BundleAliaser Seeded()
    {
        var a = new BundleAliaser();
        a.AddServer(11, "zeta-07");
        a.AddName(AliasKind.Host, @"zeta-07.example.test\INST");
        a.AddName(AliasKind.Database, "DeltaLedgerDb");
        a.AddName(AliasKind.Database, "gamma_orders");
        return a;
    }

    [Fact]
    public void LongestFirst_FqdnIsReplacedWhole_AndNoAliasIsRescanned()
    {
        var a = Seeded();
        var text = a.Alias("connect to zeta-07.example.test failed; zeta-07 down; example.test unreachable");
        Assert.DoesNotContain("zeta", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("example.test", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("server-1", text, StringComparison.Ordinal);
        Assert.Contains("domain-", text, StringComparison.Ordinal);
        /* A second pass over already-aliased text changes nothing. */
        Assert.Equal(text, a.Alias(text));
    }

    [Fact]
    public void Matching_IgnoresCase()
    {
        var a = Seeded();
        Assert.DoesNotContain("deltaledgerdb", a.Alias("DELTALEDGERDB and deltaLedgerDb"), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void WordBoundary_ShortNameDoesNotMatchInsideALongerWord()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Database, "sales");
        Assert.Equal("wholesales and db-1", a.Alias("wholesales and sales"));
    }

    [Theory]
    [InlineData(@"host\qxinst", "host", "qxinst")]
    [InlineData("alpha-host,1433", "alpha-host", null)]
    [InlineData("alpha-host:5432", "alpha-host", null)]
    [InlineData("[alpha-host]", "alpha-host", null)]
    public void Splitting_AddsTheHostAndInstanceAsTokens(string raw, string host, string? instance)
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, raw);
        Assert.DoesNotContain(host, a.Alias("on " + host + " now"), StringComparison.OrdinalIgnoreCase);
        if (instance is not null)
        {
            Assert.DoesNotContain(instance, a.Alias("named " + instance), StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Fqdn_AddsFirstLabelAndDomainSuffix()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Host, "beta-node.corp.example.test");
        var text = a.Alias("beta-node and corp.example.test and beta-node.corp.example.test");
        Assert.DoesNotContain("beta-node", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("corp.example.test", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IpLiterals_BecomeIpAliases_AndAVersionNumberIsKept()
    {
        var a = new BundleAliaser();
        var text = a.Alias("peer 203.0.113.40 and 2001:db8::1 and again 203.0.113.40");
        Assert.DoesNotContain("203.0.113.40", text, StringComparison.Ordinal);
        Assert.DoesNotContain("2001:db8", text, StringComparison.Ordinal);
        Assert.Equal(2, text.Split("ip-1").Length - 1);
        var tree = a.AliasTree(JsonNode.Parse("""{"product_version":"1.5.0.0","note":"v 1.5.0.0"}"""));
        Assert.Equal("1.5.0.0", tree!["product_version"]!.GetValue<string>());
    }

    [Fact]
    public void Keys_AreNeverRewritten_AndSystemDatabasesAreKept()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Database, "tempdb");
        a.AddName(AliasKind.Database, "database_name");
        var tree = (JsonObject)a.AliasTree(JsonNode.Parse("""{"database_name":"tempdb","master":"x","msg":"in master and tempdb"}"""))!;
        Assert.Equal("tempdb", tree["database_name"]!.GetValue<string>());
        Assert.True(tree.ContainsKey("master"));
        Assert.Equal("in master and tempdb", tree["msg"]!.GetValue<string>());
    }

    [Fact]
    public void ServerId_IsRemappedToServerAlias()
    {
        var a = Seeded();
        var tree = (JsonObject)a.AliasTree(JsonNode.Parse("""{"server_id":11,"arguments":{"server_id":11}}"""))!;
        Assert.False(tree.ContainsKey("server_id"));
        Assert.Equal("server-1", tree["server_alias"]!.GetValue<string>());
        Assert.Equal("server-1", tree["arguments"]!["server_alias"]!.GetValue<string>());
    }

    [Fact]
    public void Aliases_AreStableAcrossTwoRuns()
    {
        string One()
        {
            var a = Seeded();
            return a.Alias("zeta-07 gamma_orders DeltaLedgerDb 198.51.100.7");
        }

        Assert.Equal(One(), One());
    }

    [Fact]
    public void Harvest_PicksUpAnIdentifierKeyedValueNobodyRegistered()
    {
        var a = new BundleAliaser();
        var tree = JsonNode.Parse("""{"rows":[{"client_hostname":"omega-ws9","note":"omega-ws9 timed out"}]}""");
        a.Harvest(tree);
        Assert.DoesNotContain("omega-ws9", a.AliasTree(tree)!.ToJsonString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Verifier_CatchesAGluedName_AndForcesTheNoBoundaryPass()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, "zetanode-07");
        var root = (JsonObject)a.AliasTree(JsonNode.Parse("""{"sections":{"x":{"msg":"zetanode-07x failed"}}}"""))!;
        Assert.Contains("zetanode-07x", root.ToJsonString(), StringComparison.Ordinal);
        var (leaks, text) = a.Finish(root, DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);
        Assert.DoesNotContain("zetanode", text!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Verifier_ChecksTheJsonUnescapedText()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, "omega-store");
        /* Built so the name appears only after a \u escape is decoded: the tree holds the decoded string. */
        var root = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["msg"] = "omega\u002Dstore up" } } };
        var (leaks, text) = a.Finish(root, DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);
        Assert.DoesNotContain("omega-store", text!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void KeptNames_ArePinned()
    {
        Assert.Equal(
            new[] { "master", "model", "msdb", "tempdb", "distribution", "rdsadmin", "postgres", "template0", "template1" },
            BundleAliaser.KeptNames.Take(9).ToArray());
        Assert.True(BundleAliaser.IsKept("MSDB"));
    }

    [Fact]
    public void ProductRoles_AreKept_AndOtherRolesAreAliased()
    {
        var a = new BundleAliaser();
        var tree = JsonNode.Parse("""{"r":[{"role_name":"viewer"},{"role_name":"betaowner"}]}""");
        a.Harvest(tree);
        var text = a.AliasTree(tree)!.ToJsonString();
        Assert.Contains("viewer", text, StringComparison.Ordinal);
        Assert.DoesNotContain("betaowner", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AliasMap_IsOnlyAvailableThroughItsAccessor()
    {
        var a = Seeded();
        Assert.Contains(a.AliasMapLines(), l => l.StartsWith("server-1\t", StringComparison.Ordinal));
        var (_, text) = a.Finish(new JsonObject { ["sections"] = new JsonObject { ["x"] = a.Alias("zeta-07") } }, DiagnosticsBundle.WriteOptions);
        Assert.DoesNotContain("\t", text!.Replace("\\t", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
    }
}
