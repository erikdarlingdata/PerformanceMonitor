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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5640: a mute rule can match by the alert's client application (so SSMS cancels can be muted while an
/// application's own timeouts still alert). The pattern is a case-insensitive Contains like every other text
/// pattern, a rule that carries one never matches an alert with no client application, both stores and the
/// viewer's SQL carry the column, the MCP create/update verbs take and report it, and a rule narrowed ONLY by
/// it is not mistaken for a whole-server silence.
/// </summary>
[Collection("live-postgres")]
public sealed class MuteRuleClientAppTests
{
    private static readonly DateTime Now = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);

    private static AlertMuteContext Ctx(string? app) =>
        new() { ServerName = "srv", MetricName = "Client Timeouts", ClientAppName = app };

    [Theory]
    [InlineData("Management Studio", "Microsoft SQL Server Management Studio - Query", true)]
    [InlineData("management studio", "Microsoft SQL Server Management Studio - Query", true)]
    [InlineData("MANAGEMENT STUDIO", "microsoft sql server management studio", true)]
    [InlineData("Management Studio", "OrdersApi", false)]
    [InlineData("Management Studio", null, false)]
    public void ClientAppPattern_IsACaseInsensitiveContains_AndNeverMatchesAMissingApp(string pattern, string? app, bool expected)
    {
        var rule = new MuteRule { ClientAppPattern = pattern };

        Assert.Equal(expected, rule.MatchesAt(Ctx(app), Now));
    }

    [Fact]
    public void ARuleWithoutAClientAppPattern_MatchesWithOrWithoutAClientApp()
    {
        var rule = new MuteRule { MetricName = "Client Timeouts" };

        Assert.True(rule.MatchesAt(Ctx(null), Now));
        Assert.True(rule.MatchesAt(Ctx("OrdersApi"), Now));
    }

    [Fact]
    public void ClientAppPattern_NarrowsAnotherDimension_BothMustMatch()
    {
        var rule = new MuteRule { MetricName = "Client Timeouts", DatabasePattern = "Sales", ClientAppPattern = "Management Studio" };

        Assert.True(rule.MatchesAt(new AlertMuteContext { ServerName = "srv", MetricName = "Client Timeouts", DatabaseName = "SalesDb", ClientAppName = "SQL Server Management Studio" }, Now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "srv", MetricName = "Client Timeouts", DatabaseName = "Other", ClientAppName = "SQL Server Management Studio" }, Now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "srv", MetricName = "Client Timeouts", DatabaseName = "SalesDb", ClientAppName = "OrdersApi" }, Now));
    }

    [Fact]
    public void ClientAppPattern_ShowsInTheSummary_IsKeptByClone_AndIsNotAMatchEverythingRule()
    {
        var rule = new MuteRule { ClientAppPattern = "ssms" };

        Assert.Contains("app≈ssms", rule.Summary, StringComparison.Ordinal);
        Assert.False(rule.MatchesEveryAlert);
        Assert.Equal("ssms", rule.Clone().ClientAppPattern);
    }

    [Fact]
    public void ARuleNarrowedOnlyByClientApp_IsNotAWholeServerSilence()
    {
        var onlyApp = new MuteRule { ServerId = 7, ServerName = "srv", ClientAppPattern = "ssms" };
        var silence = ViewerDataService.BuildServerSilenceRule(7, "srv");

        Assert.False(ViewerDataService.IsWholeServerSilence(onlyApp, 7, "srv"));
        Assert.True(ViewerDataService.IsWholeServerSilence(silence, 7, "srv"));
    }

    [Fact]
    public void TheStoresViewerAndFleetSql_CarryTheClientAppColumnThroughSelectInsertAndUpdate()
    {
        /* The viewer's constants. */
        Assert.Contains("client_app_pattern", ViewerDataService.MuteRulesSelectSql, StringComparison.Ordinal);
        Assert.Contains("client_app_pattern", ViewerDataService.MuteRuleInsertSql, StringComparison.Ordinal);
        Assert.Contains("client_app_pattern = $12", ViewerDataService.MuteRuleUpdateSql, StringComparison.Ordinal);

        /* The service store: the same three statements, with the external_source column (V180, #5654) after the client
           application column: 14 bound insert values and 13 update values. */
        var store = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "PgMuteRuleStore.cs");
        Assert.Contains("job_name_pattern, server_id, client_app_pattern, external_source, starts_at_utc,\n       recur_days, recur_start_time, recur_end_time, recur_time_zone\nFROM config_mute_rules", store, StringComparison.Ordinal);
        Assert.Contains("job_name_pattern, server_id, client_app_pattern, external_source, starts_at_utc,\n     recur_days, recur_start_time, recur_end_time, recur_time_zone)\nVALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)", store, StringComparison.Ordinal);
        Assert.Contains("server_id = $11, client_app_pattern = $12,\n    external_source = $13, starts_at_utc = $14,\n    recur_days = $15, recur_start_time = $16, recur_end_time = $17, recur_time_zone = $18", store, StringComparison.Ordinal);
        Assert.Contains("ClientAppPattern = reader.IsDBNull(12)", store, StringComparison.Ordinal);
        Assert.Contains("ExternalSource = reader.IsDBNull(13)", store, StringComparison.Ordinal);

        /* A rule narrowed only by client application is not the whole-server silence the fleet bell reads. */
        var fleet = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs");
        Assert.Contains("m.client_app_pattern IS NULL", fleet, StringComparison.Ordinal);
        var fleetJs = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "fleet.js");
        Assert.Contains("rule.client_app_pattern == null", fleetJs, StringComparison.Ordinal);
        var muteJs = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "mute-rules.js");
        Assert.Contains("key: \"client_app_pattern\"", muteJs, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEditorsReadAndWriteTheClientAppBox_AndSeedItFromTheAlertContext()
    {
        foreach (var (xaml, code) in new[]
        {
            (RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Viewer", "MuteRuleEditDialog.xaml"),
                RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Viewer", "MuteRuleEditDialog.xaml.cs")),
            (RepoFile.ReadRepoFileLf("Lite", "Windows", "MuteRuleDialog.xaml"),
                RepoFile.ReadRepoFileLf("Lite", "Windows", "MuteRuleDialog.xaml.cs")),
        })
        {
            Assert.Contains("x:Name=\"ClientAppPatternBox\"", xaml, StringComparison.Ordinal);
            Assert.Contains("x:Name=\"ClientAppLabel\"", xaml, StringComparison.Ordinal);
            Assert.Contains("ClientAppPatternBox.Text = context.ClientAppName;", code, StringComparison.Ordinal);
            Assert.Contains("ClientAppPatternBox.Text = rule.ClientAppPattern ?? \"\";", code, StringComparison.Ordinal);
            Assert.Contains("Rule.ClientAppPattern = ClientAppPatternBox.Visibility == Visibility.Visible", code, StringComparison.Ordinal);
            /* A rule with only a client-app pattern is a filtered rule, so the save does not ask "mute ALL alerts?". */
            Assert.Contains("Rule.JobNamePattern == null\n            && Rule.ClientAppPattern == null)", code, StringComparison.Ordinal);
            Assert.Contains("showClientApp", code, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task CreateMuteRule_TakesAndEchoesClientAppPattern_ViaTheToolAndTheWebBody()
    {
        var viaTool = new FakeMuteRuleStore();
        var created = await DarlingMcpAlertTools.CreateMuteRuleOver(viaTool, "srv", "Client Timeouts", null, null, null, null,
            "mute ssms", null, client_app_pattern: "  Management Studio ");
        var toolId = (string)JsonNode.Parse(created)!["mute_rule"]!["id"]!;
        Assert.Equal("Management Studio", viaTool.Row(toolId)!.ClientAppPattern);
        Assert.Equal("Management Studio", (string?)JsonNode.Parse(created)!["mute_rule"]!["client_app_pattern"]);

        var viaWeb = new FakeMuteRuleStore();
        var webResult = await DarlingMcpAlertTools.CreateMuteRuleCore(viaWeb,
            "{\"server_name\":\"srv\",\"metric_name\":\"Client Timeouts\",\"client_app_pattern\":\"Management Studio\"}");
        var webId = (string)JsonNode.Parse(webResult)!["mute_rule"]!["id"]!;
        Assert.Equal("Management Studio", viaWeb.Row(webId)!.ClientAppPattern);
    }

    [Fact]
    public async Task CreateMuteRule_RulesThatDifferOnlyByClientApp_AreNotRepeats()
    {
        var store = new FakeMuteRuleStore();

        var first = await DarlingMcpAlertTools.CreateMuteRuleOver(store, "srv", "Client Timeouts", null, null, null, null, null, null, client_app_pattern: "ssms");
        var second = await DarlingMcpAlertTools.CreateMuteRuleOver(store, "srv", "Client Timeouts", null, null, null, null, null, null, client_app_pattern: "OrdersApi");
        var repeat = await DarlingMcpAlertTools.CreateMuteRuleOver(store, "srv", "Client Timeouts", null, null, null, null, null, null, client_app_pattern: "ssms");

        Assert.Equal("created", DarlingMcpTestData.StatusOf(first));
        Assert.Equal("created", DarlingMcpTestData.StatusOf(second));
        Assert.Equal("already_exists", DarlingMcpTestData.StatusOf(repeat));
        Assert.Equal(2, store.Count);
    }

    [Fact]
    public async Task UpdateMuteRule_SetsAndClearsClientAppPattern_AndReportsNoChangeWhenRepeated()
    {
        var store = new FakeMuteRuleStore().Seed(new MuteRule { Id = "r1", ServerName = "srv", MetricName = "Client Timeouts" });

        var set = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"client_app_pattern\":\"ssms\"}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(set));
        Assert.Equal("ssms", store.Row("r1")!.ClientAppPattern);

        var again = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"client_app_pattern\":\"ssms\"}");
        Assert.Equal("unchanged", DarlingMcpTestData.StatusOf(again));

        var cleared = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"client_app_pattern\":null}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(cleared));
        Assert.Null(store.Row("r1")!.ClientAppPattern);
    }

    [Fact]
    public void TheMcpToolsNameTheClientAppParameter_InCreateAndTheUpdateFieldList()
    {
        var mcp = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        /* The create tool's parameter list runs client_app_pattern, then external_source (V180, #5654), then the Level 2
           agent_action (#5915). */
        Assert.Contains("string? client_app_pattern = null,\n", mcp, StringComparison.Ordinal);
        Assert.Contains("string? external_source = null,\n", mcp, StringComparison.Ordinal);
        Assert.Contains(typeof(PerformanceMonitor.Darling.Storage.MuteRuleImage).GetProperties(),
            p => System.Reflection.CustomAttributeExtensions.GetCustomAttribute<System.Text.Json.Serialization.JsonPropertyNameAttribute>(p)!.Name == "client_app_pattern");
        Assert.Contains("metric_name, external_source, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, client_app_pattern, reason, \" +", mcp, StringComparison.Ordinal);
    }

    /// <summary>Live round trip over the real store: the column V179 adds is written by insert, rewritten by update,
    /// and read back; a rule that never set it reads back null. Skipped without DARLING_TEST_PG.</summary>
    [Fact]
    public async Task PgMuteRuleStore_RoundTripsClientAppPattern_AndNullForALegacyRule()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-rule client_app_pattern round trip.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgMuteRuleStore(postgres);
        var withApp = new MuteRule { Id = "mute-app-" + Guid.NewGuid().ToString("N"), ServerName = "label-only", ClientAppPattern = "ssms", Reason = "app round-trip" };
        var without = new MuteRule { Id = "mute-app-" + Guid.NewGuid().ToString("N"), ServerName = "label-only", Reason = "app round-trip" };

        var bodySucceeded = false;
        try
        {
            await store.InsertAsync(withApp);
            await store.InsertAsync(without);

            var all = await store.LoadAllAsync(ct);
            Assert.Equal("ssms", all.Single(r => r.Id == withApp.Id).ClientAppPattern);
            Assert.Null(all.Single(r => r.Id == without.Id).ClientAppPattern);

            withApp.ClientAppPattern = "OrdersApi";
            without.ClientAppPattern = "ssms";
            await store.UpdateAsync(withApp);
            await store.UpdateAsync(without);
            all = await store.LoadAllAsync(ct);
            Assert.Equal("OrdersApi", all.Single(r => r.Id == withApp.Id).ClientAppPattern);
            Assert.Equal("ssms", all.Single(r => r.Id == without.Id).ClientAppPattern);

            withApp.ClientAppPattern = null;
            await store.UpdateAsync(withApp);
            Assert.Null((await store.LoadAllAsync(ct)).Single(r => r.Id == withApp.Id).ClientAppPattern);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await store.DeleteAsync(withApp.Id);
                await store.DeleteAsync(without.Id);
            });
        }
    }
}
