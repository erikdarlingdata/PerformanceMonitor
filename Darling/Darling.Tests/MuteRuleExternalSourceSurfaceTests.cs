/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The external alert source on a mute rule (#5654), as the MCP tools and the web mute routes carry it (D2):
/// <c>get_mute_rules</c> reports it, <c>create_mute_rule</c> and <c>update_mute_rule</c> write it behind the
/// registry's name check, the two hand-written rule comparers treat it as part of the rule, and
/// <c>get_alert_history</c> reports the source an alert came from. Update's rule for the field is the tool's rule for
/// every other optional dimension: a key left out leaves it, an explicit null clears it, a blank string is refused.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class MuteRuleExternalSourceSurfaceTests
{
    private static JsonNode Payload(string json) =>
        JsonNode.Parse(json)!["mute_rule"] ?? throw new InvalidOperationException($"no mute_rule in {json}");

    [Theory]
    [InlineData("nagios", true)]
    [InlineData("ab", true)]
    [InlineData("a_1", true)]
    [InlineData("a", false)]
    [InlineData("Nagios", false)]
    [InlineData("1nagios", false)]
    [InlineData("na gios", false)]
    [InlineData("na-gios", false)]
    [InlineData("n123456789012345678901234567890x", false)]
    [InlineData("n1234567890123456789012345678901", false)]
    [InlineData("n123456789012345678901234567890", true)]
    [InlineData("", false)]
    [InlineData("nagios\n", false)]
    [InlineData("nagios\r\n", false)]
    public void TheSourceNameCheckIsTheRegistrysShape(string name, bool valid) =>
        Assert.Equal(valid, MuteRule.IsValidExternalSource(name));

    [Fact]
    public async Task CreateOverMcp_StoresTheSource_AndEchoesItOnTheRule()
    {
        var store = new FakeMuteRuleStore();

        var json = await DarlingMcpAlertTools.CreateMuteRuleOver(store, "SQL01", null, null, null, null, null, null, null,
            external_source: "  nagios ");

        Assert.Equal("created", DarlingMcpTestData.StatusOf(json));
        Assert.Equal("nagios", Payload(json)["external_source"]!.GetValue<string>());
        Assert.Contains("from nagios", Payload(json)["match_summary"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Nagios")]
    [InlineData("n")]
    [InlineData("no spaces")]
    [InlineData("1abc")]
    [InlineData("nagios\n")]
    [InlineData("na\ngios")]
    public async Task CreateOverMcp_RefusesAnInvalidSource_AndWritesNothing(string source)
    {
        var store = new FakeMuteRuleStore();

        var json = await DarlingMcpAlertTools.CreateMuteRuleOver(store, "SQL01", null, null, null, null, null, null, null,
            external_source: source);

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(json));
        Assert.Contains("external_source", JsonNode.Parse(json)!["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(0, store.Count);
    }

    /// <summary>
    /// #5654: a source that is empty after trimming, or holds only control characters, is refused with <c>invalid</c> on every
    /// entry point (MCP create, the web create body, and the update parser MCP update and web PATCH share), and no rule is
    /// written or changed: it is never read as "any source". Omitting the field on create still means any source.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\n")]
    [InlineData("\t")]
    [InlineData("\r\n")]
    [InlineData("\u0001")]
    [InlineData(" \u001f ")]
    public async Task ABlankOrControlOnlySource_IsRefused_OnEveryEntryPoint(string source)
    {
        var mcpStore = new FakeMuteRuleStore();
        var mcp = await DarlingMcpAlertTools.CreateMuteRuleOver(mcpStore, "SQL01", null, null, null, null, null, null, null,
            external_source: source);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(mcp));
        Assert.Contains("external_source", JsonNode.Parse(mcp)!["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(0, mcpStore.Count);

        var body = new JsonObject { ["server_name"] = "SQL01", ["external_source"] = source }.ToJsonString();
        var webStore = new FakeMuteRuleStore();
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.CreateMuteRuleCore(webStore, body)));
        Assert.Equal(0, webStore.Count);

        var seeded = Seeded("nagios");
        var update = new JsonObject { ["external_source"] = source }.ToJsonString();
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateMuteRuleCore(seeded, "r1", update)));
        Assert.Equal("nagios", seeded.Row("r1")!.ExternalSource);
    }

    [Fact]
    public async Task CreateOverMcp_WithTheSourceOmitted_MutesAnySource()
    {
        var store = new FakeMuteRuleStore();

        var json = await DarlingMcpAlertTools.CreateMuteRuleOver(store, "SQL01", null, null, null, null, null, null, null,
            external_source: null);

        Assert.Equal("created", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(JsonValueKind.Null, JsonDocument.Parse(json).RootElement.GetProperty("mute_rule").GetProperty("external_source").ValueKind);
    }

    [Fact]
    public async Task CreateViaTheWebBody_StoresAndValidatesTheSource()
    {
        var store = new FakeMuteRuleStore();

        var ok = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"external_source\":\"zabbix\",\"reason\":\"noisy\"}");
        Assert.Equal("created", DarlingMcpTestData.StatusOf(ok));
        Assert.Equal("zabbix", Payload(ok)["external_source"]!.GetValue<string>());

        var bad = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"external_source\":\"Zabbix\"}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(bad));
        Assert.Contains("'external_source'", JsonNode.Parse(bad)!["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task TwoRulesThatDifferOnlyBySource_AreDifferentRules_OnCreate()
    {
        var store = new FakeMuteRuleStore();

        var first = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"server_name\":\"SQL01\",\"external_source\":\"nagios\"}");
        var other = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"server_name\":\"SQL01\",\"external_source\":\"zabbix\"}");
        var none = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"server_name\":\"SQL01\"}");
        var repeat = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"server_name\":\"SQL01\",\"external_source\":\"nagios\"}");

        Assert.Equal("created", DarlingMcpTestData.StatusOf(first));
        Assert.Equal("created", DarlingMcpTestData.StatusOf(other));
        Assert.Equal("created", DarlingMcpTestData.StatusOf(none));
        Assert.Equal("already_exists", DarlingMcpTestData.StatusOf(repeat));
        Assert.Equal(3, store.Count);
    }

    private static FakeMuteRuleStore Seeded(string? source) =>
        new FakeMuteRuleStore().Seed(new MuteRule { Id = "r1", ServerName = "SQL01", Reason = "x", ExternalSource = source });

    /// <summary>#5654: a trailing line feed is refused on the web body's create and on the shared update parser (MCP and web
    /// PATCH), not trimmed into a valid name.</summary>
    [Fact]
    public async Task ATrailingLineFeed_IsRefused_OnTheWebCreate_AndOnUpdate()
    {
        var store = Seeded(null);

        var created = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"external_source\":\"nagios\\n\"}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(created));

        var updated = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":\"nagios\\n\"}");
        Assert.NotEqual("updated", DarlingMcpTestData.StatusOf(updated));
        Assert.Null(store.Row("r1")!.ExternalSource);
    }

    [Fact]
    public async Task Update_SetsTheSource_AndAnAbsentKeyLeavesIt()
    {
        var store = Seeded(null);

        var set = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":\"nagios\"}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(set));
        Assert.Equal("nagios", store.Row("r1")!.ExternalSource);

        var other = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"reason\":\"edited\"}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(other));
        Assert.Equal("nagios", store.Row("r1")!.ExternalSource);
    }

    [Fact]
    public async Task Update_AnExplicitNullClears_ABlankIsRefused_AnInvalidNameIsRefused()
    {
        var store = Seeded("nagios");

        var blank = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":\"  \"}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(blank));
        var bad = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":\"NAGIOS\"}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(bad));
        var notText = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":5}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(notText));
        Assert.Equal("nagios", store.Row("r1")!.ExternalSource);

        var cleared = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":null}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(cleared));
        Assert.Null(store.Row("r1")!.ExternalSource);
    }

    /// <summary>The 'unchanged' comparer (SameEditableFields): a source edit on a rule that differs ONLY by source is
    /// an edit, and the same value again is not.</summary>
    [Fact]
    public async Task Update_TheEditableFieldsComparer_SeesASourceOnlyDifference()
    {
        var store = Seeded("nagios");

        Assert.Equal("unchanged", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":\"nagios\"}")));
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":\"zabbix\"}")));
        Assert.Equal("zabbix", store.Row("r1")!.ExternalSource);
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"external_source\":null}")));
    }

    [Fact]
    public void TheDuplicateComparer_SeesASourceOnlyDifference()
    {
        var withSource = new MuteRule { ServerName = "SQL01", ExternalSource = "nagios" };
        var without = new MuteRule { ServerName = "SQL01" };
        var rules = new[] { withSource };

        Assert.Null(DarlingMcpAlertTools.FindDuplicate(rules, without, DateTime.UtcNow));
        Assert.Null(DarlingMcpAlertTools.FindDuplicate(rules, new MuteRule { ServerName = "SQL01", ExternalSource = "zabbix" }, DateTime.UtcNow));
        Assert.Same(withSource, DarlingMcpAlertTools.FindDuplicate(rules, new MuteRule { ServerName = "SQL01", ExternalSource = "nagios" }, DateTime.UtcNow));
    }

    [Fact]
    public void BothComparersNameTheSource_AndThePayloadAndHistoryCarryIt()
    {
        var tools = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs").ReplaceLineEndings("\n");

        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(tools,
            @"string\.Equals\(a\.ExternalSource, b\.ExternalSource, StringComparison\.Ordinal\)").Count);
        Assert.Contains(typeof(PerformanceMonitor.Darling.Storage.MuteRuleImage).GetProperties(),
            p => System.Reflection.CustomAttributeExtensions.GetCustomAttribute<System.Text.Json.Serialization.JsonPropertyNameAttribute>(p)!.Name == "external_source");
        Assert.Contains("external_source = AlertMuteContext.ExternalSourceOf(r.MetricName),", tools, StringComparison.Ordinal);
    }

    [Fact]
    public void LiteReturnsTheSameKeys_AlwaysNull()
    {
        var lite = ReadRepoFile("Lite", "Mcp", "McpAlertTools.cs").ReplaceLineEndings("\n");

        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(lite, @"external_source = \(string\?\)null,").Count);
    }

    [Fact]
    public void TheFleetSilenceBell_IgnoresARuleLimitedToASource()
    {
        Assert.Contains("m.external_source IS NULL", DarlingFleetReader.FleetServersSql, StringComparison.Ordinal);
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "fleet.js");
        Assert.Contains("rule.external_source == null", js, StringComparison.Ordinal);
    }

    /// <summary>The MCP create and update tools through the real store: the source is written, read back by
    /// <c>get_mute_rules</c>, refused when invalid, and cleared by an explicit null.</summary>
    [Fact]
    public async Task TheMcpToolsRoundTripTheSourceThroughTheLiveStore()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-source tool test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var seed = new NpgsqlConnection(scratch.ConnectionString))
        {
            await seed.OpenAsync(ct);
            await PgMigrations.MigrateAsync(seed, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var created = await DarlingMcpAlertTools.CreateMuteRule(postgres, server_name: "SQL01", reason: "noisy", external_source: "nagios");
            Assert.Equal("created", DarlingMcpTestData.StatusOf(created));
            var id = Payload(created)["id"]!.GetValue<string>();

            var refused = await DarlingMcpAlertTools.CreateMuteRule(postgres, server_name: "SQL02", external_source: "Bad Name");
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(refused));

            var listed = JsonNode.Parse(await DarlingMcpAlertTools.GetMuteRules(postgres, enabled_only: false, cancellationToken: ct))!;
            var rows = listed["mute_rules"]!.AsArray();
            Assert.Single(rows);
            Assert.Equal("nagios", rows[0]!["external_source"]!.GetValue<string>());

            var updated = await DarlingMcpAlertTools.UpdateMuteRule(postgres, id, "{\"external_source\":\"zabbix\"}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(updated));
            Assert.Equal("zabbix", Payload(updated)["external_source"]!.GetValue<string>());

            var cleared = await DarlingMcpAlertTools.UpdateMuteRule(postgres, id, "{\"external_source\":null}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(cleared));
            var after = JsonNode.Parse(await DarlingMcpAlertTools.GetMuteRules(postgres, enabled_only: false, cancellationToken: ct))!;
            Assert.Null(after["mute_rules"]!.AsArray()[0]!["external_source"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
