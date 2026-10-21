/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the name-set holes the first review found, each pinned on the aliaser alone. Names are synthetic.</summary>
public sealed class DiagnosticsBundleAliasHardeningTests
{
    [Fact]
    public void StoreLoginKeyedRole_IsHarvested_AndProductRolesAreKept()
    {
        var a = new BundleAliaser();
        var tree = JsonNode.Parse("""{"by_role":[{"role":"betaowner"},{"role":"viewer"}],"statements":[{"role":"betaowner","query":"select 1"}]}""");
        a.Harvest(tree);
        var text = a.AliasTree(tree)!.ToJsonString();
        Assert.DoesNotContain("betaowner", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("viewer", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("tcp:omega-sql,1433")]
    [InlineData("tcp:omega-sql.example.test,1433")]
    [InlineData(@"np:\\omega-sql\pipe\sql\query")]
    [InlineData("admin:omega-sql")]
    [InlineData("lpc:omega-sql")]
    [InlineData("TCP:omega-sql:1433")]
    public void ProtocolPrefixedHost_IsRegisteredAsTheBareHost(string raw)
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, raw);
        Assert.DoesNotContain("omega-sql", a.Alias("driver says omega-sql refused"), StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("omega-sql", a.Alias("driver says tcp:omega-sql,1433 refused"), StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(a.AliasMapLines(), l => l.Contains("tcp:", StringComparison.OrdinalIgnoreCase) || l.Contains("np:", StringComparison.OrdinalIgnoreCase));
        if (raw.Contains("example.test", StringComparison.Ordinal))
        {
            Assert.DoesNotContain("example.test", a.Alias("on omega-sql.example.test and example.test"), StringComparison.OrdinalIgnoreCase);
        }
    }

    [Theory]
    [InlineData("Login failed for user 'QXCORP\\svc_zeta'.")]
    [InlineData("Login failed for user \"QXCORP\\svc_zeta\".")]
    public void QuotedDomainBackslashLogin_AliasesBothHalves(string message)
    {
        var a = new BundleAliaser();
        var text = a.Alias(message);
        Assert.DoesNotContain("QXCORP", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("svc_zeta", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("domain-1", text, StringComparison.Ordinal);
        Assert.Contains("login-1", text, StringComparison.Ordinal);
        /* The same pair seen in a later, unquoted spelling is already in the name set. */
        Assert.DoesNotContain("svc_zeta", a.Alias("account QXCORP\\svc_zeta expired"), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void QuotedPair_IsRegisteredByTheHarvestPass_BeforeAnyTextIsAliased()
    {
        var a = new BundleAliaser();
        var tree = JsonNode.Parse("""{"a":{"msg":"first mentions svc_zeta alone"},"b":{"msg":"Login failed for user 'QXCORP\\svc_zeta'."}}""");
        a.Harvest(tree);
        Assert.DoesNotContain("svc_zeta", a.AliasTree(tree)!.ToJsonString(), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("::ffff:203.0.113.77")]
    [InlineData("::FFFF:203.0.113.77")]
    [InlineData("0:0:0:0:0:ffff:203.0.113.77")]
    [InlineData("64:ff9b::203.0.113.77")]
    public void Ipv4MappedIpv6_AliasesTheWholeAddress(string address)
    {
        var text = new BundleAliaser().Alias("host=" + address + " refused");
        Assert.DoesNotContain("203.0.113", text, StringComparison.Ordinal);
        Assert.DoesNotContain("ffff", text, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("host=ip-1 refused", text);
    }

    [Fact]
    public void DottedName_UnderAKnownDomain_BecomesAHostAlias()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Domain, "example.test");
        var text = a.Alias("replica sql02.example.test and again SQL02.EXAMPLE.TEST, also sql03.corp.example.test");
        Assert.DoesNotContain("sql02", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sql03", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("host-", text, StringComparison.Ordinal);
        Assert.Equal(2, text.Split("host-1").Length - 1);
    }

    [Fact]
    public void ShortConfigSecret_IsReplacedOnWordBoundaries_NotInsideOtherWords()
    {
        var a = new BundleAliaser();
        a.AddSecret("ab");
        Assert.Equal("pw [redacted] and abc and cab", a.Alias("pw ab and abc and cab"));
    }

    [Fact]
    public void LongSecret_IsStillReplacedAnywhere_AndVerified()
    {
        var a = new BundleAliaser();
        a.AddSecret("Sup3rS3cret!");
        Assert.DoesNotContain("Sup3rS3cret!", a.Alias("xSup3rS3cret!x"), StringComparison.Ordinal);
    }

    [Fact]
    public void AliasTextCheck_IsNarrow_ADatabaseCalledMain_IsStillVerified()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Database, "main");
        var root = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["msg"] = "main rows remaining" } } };
        var (leaks, text) = a.Finish(root, DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);

        /* #6256: the standalone name is still verified and replaced; the same letters inside "remaining" are not a name. */
        Assert.DoesNotContain("main rows", text!, StringComparison.Ordinal);
        Assert.Contains("remaining", text!, StringComparison.Ordinal);
    }

    [Fact]
    public void ARealServerNamedLikeAnAlias_CannotCollideWithTheAliasSpace()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, "server-1");
        a.AddName(AliasKind.Server, "zeta-07");
        var text = a.Alias("server-1 and zeta-07");
        var parts = text.Split(" and ");
        Assert.NotEqual(parts[0], parts[1]);
        Assert.NotEqual("server-1", parts[0]);
        Assert.NotEqual("server-1", parts[1]);
        Assert.DoesNotContain(a.AliasMapLines(), l => l.StartsWith("server-1\t", StringComparison.Ordinal));
    }

    [Fact]
    public void Verifier_KeyExemption_AppliesOnlyWhenEveryOccurrenceIsExactlyAKey()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, "zeta-07");
        a.AddName(AliasKind.Database, "status_db");

        /* A name that is a data key plus a different spelling elsewhere is a leak, and no text comes back. */
        var keyed = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["zeta-07 stats"] = 1 } } };
        var (leaks, text) = a.Finish(keyed, DiagnosticsBundle.WriteOptions);
        Assert.NotEmpty(leaks);
        Assert.Null(text);

        /* A token that appears exactly as a key and nowhere else is the product's own vocabulary and is exempt. */
        var vocabulary = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["status_db"] = 1 } } };
        var (okLeaks, okText) = a.Finish(vocabulary, DiagnosticsBundle.WriteOptions);
        Assert.Empty(okLeaks);
        Assert.NotNull(okText);

        /* The same token as a key AND as a value is a leak in the value (the alias pass fixes the value first). */
        var both = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["status_db"] = a.Alias("status_db") } } };
        var (bothLeaks, bothText) = a.Finish(both, DiagnosticsBundle.WriteOptions);
        Assert.Empty(bothLeaks);
        Assert.NotNull(bothText);
    }

    /* ── #6256: a leftover name is replaced when it is a whole sub-token of its identifier, not inside a plain letter run ── */

    private static JsonObject OneValue(string text) =>
        new() { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["msg"] = text } } };

    private static string Message(string serialized) =>
        JsonNode.Parse(serialized)!["sections"]!["x"]!["msg"]!.GetValue<string>();

    [Theory]
    [InlineData("sales", "wholesales report")]
    [InlineData("data", "the database is up")]
    [InlineData("admin", "an administrator signed in")]
    public void ANameInsideAnOrdinaryWord_IsNeitherReplacedNorCountedAsALeftover(string name, string input)
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Login, name);

        var (leaks, text) = a.Finish(OneValue(input), DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);
        Assert.Equal(input, Message(text!));
    }

    [Theory]
    [InlineData("sqlprod", "restore SQLPROD_Full.bak now")]
    [InlineData("sqlprod", "job DBA_SQLPROD_Backup ran")]
    [InlineData("prod", "see prod01 and prod-02")]
    [InlineData("sqlprod", "see SQLPRODBackup today")]
    [InlineData("sales", "open SalesReport now")]
    [InlineData("sales", "open the sales_report.csv")]
    [InlineData("prod", "path C:\\backups\\prod\\full.bak")]
    public void AnIdentifierShapedLeftover_IsStillReplaced(string name, string input)
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, name);

        var (leaks, text) = a.Finish(OneValue(input), DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);
        Assert.DoesNotContain(name, Message(text!), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("server-", Message(text!), StringComparison.Ordinal);
    }

    [Fact]
    public void ASubTokenMatch_ReplacesOnlyTheOccurrencesThatQualify()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, "sales");

        var (leaks, text) = a.Finish(OneValue("wholesales and SalesReport and sales and unsalesly"), DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);
        var message = Message(text!);
        Assert.StartsWith("wholesales and ", message, StringComparison.Ordinal);
        Assert.EndsWith(" and unsalesly", message, StringComparison.Ordinal);
        Assert.Matches(@"^wholesales and server-\d+Report and server-\d+ and unsalesly$", message);
    }

    [Fact]
    public void TheLongestLeftoverName_IsReplacedFirst_AndAliasTextIsNeverRewritten()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Server, "prod");
        a.AddName(AliasKind.Server, "prod_two");
        var alias = a.Alias("prod_two");

        var (leaks, text) = a.Finish(OneValue("prod_two and prod and " + alias), DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);
        var message = Message(text!);
        Assert.DoesNotContain("prod", message, StringComparison.OrdinalIgnoreCase);
        Assert.Matches(@"^server-\d+ and server-\d+ and server-\d+$", message);
        Assert.EndsWith(" and " + alias, message, StringComparison.Ordinal);
    }

    [Fact]
    public void AStandaloneLeftoverName_IsStillCaught_InAValueAndInAKey()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Login, "apps");

        var (valueLeaks, valueText) = a.Finish(OneValue("apps is down, appsettings is up"), DiagnosticsBundle.WriteOptions);
        Assert.Empty(valueLeaks);
        Assert.DoesNotContain("apps is down", valueText!, StringComparison.Ordinal);
        Assert.Contains("appsettings is up", valueText!, StringComparison.Ordinal);

        /* A data-shaped key is never rewritten, so a name in one is reported and no text comes back. */
        var keyed = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["apps node"] = 1 } } };
        var (keyLeaks, keyText) = a.Finish(keyed, DiagnosticsBundle.WriteOptions);
        Assert.NotEmpty(keyLeaks);
        Assert.Null(keyText);

        /* The serialized bytes hold the apostrophe as an escape; the name after it is still standalone. */
        var escaped = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["'apps' node"] = 1 } } };
        var (escapedLeaks, escapedText) = a.Finish(escaped, DiagnosticsBundle.WriteOptions);
        Assert.NotEmpty(escapedLeaks);
        Assert.Null(escapedText);

        /* A name inside a plain word in a key is no leftover. */
        var word = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["appsettings node"] = 1 } } };
        var (wordLeaks, wordText) = a.Finish(word, DiagnosticsBundle.WriteOptions);
        Assert.Empty(wordLeaks);
        Assert.NotNull(wordText);
    }

    [Fact]
    public void AShortLoginNameInsideALongerWord_IsLeftAlone_AndPunctuationNeighboursAreAliased()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Login, "app");

        var scanned = a.Alias("login app ran application and appsettings and app_pool");
        Assert.DoesNotContain("login app ", scanned, StringComparison.Ordinal);
        Assert.Contains("application and appsettings and app_pool", scanned, StringComparison.Ordinal);

        foreach (var input in new[] { "user 'app' failed", "user app. Then", "user [app] failed", "user app\\ failed", "DOMAIN\\app failed", "user=app;pwd" })
        {
            Assert.DoesNotContain("app", a.Alias(input), StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void ADottedHostName_IsStillAliasedAsAWhole()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Host, "db01.example.internal");

        var scanned = a.Alias("connect to db01.example.internal failed");
        Assert.DoesNotContain("db01", scanned, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("example", scanned, StringComparison.OrdinalIgnoreCase);

        var (leaks, text) = a.Finish(OneValue("connect to db01.example.internal failed"), DiagnosticsBundle.WriteOptions);
        Assert.Empty(leaks);
        Assert.DoesNotContain("db01", text!, StringComparison.OrdinalIgnoreCase);
    }
}
