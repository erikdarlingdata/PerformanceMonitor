/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: secrets never reach a bundle. Passwords here are synthetic.</summary>
public sealed class DiagnosticsBundleSecretTests
{
    [Fact]
    public void ConnectionStringInALogLine_HasAnAliasedHost_AndARedactedPassword()
    {
        var a = new BundleAliaser();
        a.AddName(AliasKind.Host, "zeta-07.example.test");
        var text = a.Alias("connect failed Server=zeta-07.example.test;Database=x;User ID=u;Password=hunter2;Timeout=5");
        Assert.DoesNotContain("hunter2", text, StringComparison.Ordinal);
        Assert.DoesNotContain("zeta", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Password=[redacted]", text, StringComparison.Ordinal);
        Assert.Contains("Timeout=5", text, StringComparison.Ordinal);
    }

    [Fact]
    public void UrlCredential_IsRedacted()
    {
        var text = new BundleAliaser().Alias("dsn postgres://appuser:s3cr3tvalue@db.example.test/main ok");
        Assert.DoesNotContain("s3cr3tvalue", text, StringComparison.Ordinal);
        Assert.DoesNotContain("appuser", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BearerAndTokenAssignments_AreRedacted()
    {
        var text = new BundleAliaser().Alias("Authorization: Bearer abc123def456 and token=zz99yy88 sent");
        Assert.DoesNotContain("abc123def456", text, StringComparison.Ordinal);
        Assert.DoesNotContain("zz99yy88", text, StringComparison.Ordinal);
    }

    [Fact]
    public void SecretKeyAtAnyDepth_HasItsValueRedacted()
    {
        var tree = (JsonObject)new BundleAliaser().AliasTree(JsonNode.Parse("""{"a":{"b":[{"api_key":"k-1234567","note":"fine"}]}}"""))!;
        var item = tree["a"]!["b"]![0]!;
        Assert.Equal("[redacted]", item["api_key"]!.GetValue<string>());
        Assert.Equal("fine", item["note"]!.GetValue<string>());
    }

    [Fact]
    public void KnownSecretValue_IsRemovedFromAnyText_AndTheVerifierBlocksOne_ThatSurvives()
    {
        var a = new BundleAliaser();
        a.AddSecret("Sup3rS3cret!");
        Assert.DoesNotContain("Sup3rS3cret!", a.Alias("the value Sup3rS3cret! leaked"), StringComparison.Ordinal);

        /* A secret written into the tree after aliasing (a stand-in for a path the pass missed) is a leak: no text. */
        var root = new JsonObject { ["sections"] = new JsonObject { ["x"] = new JsonObject { ["msg"] = "Sup3rS3cret!" } } };
        var (leaks, text) = a.Finish(root, DiagnosticsBundle.WriteOptions);
        Assert.Null(text);
        var leak = Assert.Single(leaks);
        Assert.Equal("secret", leak.Class);
        Assert.Equal("x", leak.Section);
        Assert.Equal("msg", leak.Field);
    }

    [Fact]
    public void SecretTextGuard_IsTheOnePolicy_ForSlowReadsAndTheBundle()
    {
        Assert.True(SecretTextGuard.IsSecretKey("Api_Key"));
        Assert.False(SecretTextGuard.IsSecretKey("dedup_key"));
        Assert.True(SecretTextGuard.ValueLooksSecret("password=abc"));
        Assert.True(SecretTextGuard.ValueLooksSecret("see https://u:p@host/"));
        Assert.False(SecretTextGuard.ValueLooksSecret("select 1"));
        Assert.Equal(SlowReadLog.IsSecretKey("webhook_url"), SecretTextGuard.IsSecretKey("webhook_url"));
    }

    [Fact]
    public void ConfigShape_EveryConfigPropertyIsProjectedOrExplicitlyExcluded()
    {
        var known = DiagnosticsBundle.ConfigShapeProjected.Concat(DiagnosticsBundle.ConfigShapeExcluded).ToHashSet(StringComparer.Ordinal);
        var unclassified = new[] { typeof(DarlingConfig), typeof(PostgresConfig), typeof(McpConfig), typeof(WebConfig), typeof(MonitoredServer) }
            .SelectMany(t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && p.GetCustomAttribute<System.Text.Json.Serialization.JsonIgnoreAttribute>() is null)
                .Select(p => t.Name + "." + p.Name))
            .Where(n => !known.Contains(n))
            .ToList();
        Assert.True(unclassified.Count == 0, "Classify these in DiagnosticsBundle.ConfigShapeProjected (a scalar that identifies nothing, such as a flag or a number) or ConfigShapeExcluded (anything that holds a name, host, path, URL or secret, with a reason): " + string.Join(", ", unclassified));
    }

    [Theory]
    [InlineData(null, "off")]
    [InlineData("off", "off")]
    [InlineData("shadow", "shadow")]
    [InlineData(" ON ", "on")]
    [InlineData("secret-host.example.test", "off")]
    public void ConfigShape_ProjectsTheProcedureStatsPlanFetchAsItsParsedMode(string? raw, string expected)
    {
        var shape = DiagnosticsBundle.BuildConfigShape(new DarlingConfig { ProcedureStatsDeferredPlanFetch = raw! });
        Assert.Equal(expected, shape["procedure_stats_deferred_plan_fetch"]!.GetValue<string>());
        Assert.DoesNotContain("secret-host", shape.ToJsonString());
    }

    [Fact]
    public void ConfigShape_ProjectsTheDeferredPlanFetchFlag()
    {
        Assert.True(DiagnosticsBundle.BuildConfigShape(new DarlingConfig())["query_stats_deferred_plan_fetch"]!.GetValue<bool>());
        Assert.False(DiagnosticsBundle.BuildConfigShape(new DarlingConfig { QueryStatsDeferredPlanFetch = false })["query_stats_deferred_plan_fetch"]!.GetValue<bool>());
    }

    [Fact]
    public void ConfigShape_CarriesNoStringFromTheConfig()
    {
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig { ConnectionString = "Host=alpha-pg.example.test;Password=hunter2" },
            Servers = { new MonitoredServer { Name = "beta-01", Host = "beta-01.example.test", Username = "betaowner", Password = "pw-1234" } },
        };
        var text = DiagnosticsBundle.BuildConfigShape(config).ToJsonString();
        foreach (var forbidden in new[] { "alpha-pg", "hunter2", "beta-01", "betaowner", "pw-1234", "example.test" })
        {
            Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void SeedFromConfig_AddsTheStorePasswordAsASecret()
    {
        var a = new BundleAliaser();
        DiagnosticsBundle.SeedFromConnectionString(a, "Host=alpha-pg.example.test,gamma-pg;Database=deltadb;Username=betaowner;Password=Qx7!longpass");
        var text = a.Alias("x Qx7!longpass alpha-pg.example.test gamma-pg deltadb betaowner");
        foreach (var forbidden in new[] { "Qx7!longpass", "alpha-pg", "gamma-pg", "deltadb", "betaowner" })
        {
            Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
        }
    }
}
