/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6390 S4: the Viewer manages custom alert rules with the web's own store and validation. The pure half: the edit form builds a
/// definition the web validator accepts, keeps what it does not own, and says the validator's own text when it refuses.
/// </summary>
public sealed class ViewerAlertRulesTests
{
    private static AlertRuleForm Form(string measureKey = "wait_time_ms", string op = "ge", string warn = "5", string critical = "") => new(
        measureKey, "sum", "ms", "0.25", op, warn, critical, "", "", "", "", "3", "2", AlertRuleScopeKind.All, Array.Empty<string>(), null);

    [Fact]
    public void TheBadDefinition_GivesTheViewerAndTheWebValidatorTheSameText()
    {
        // /api/alerts/validate returns CustomAlertRuleDefinition.TryParse's error; Validate must be that string, not a paraphrase.
        foreach (var bad in new[]
        {
            "not json",
            "{}",
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":99},\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}",
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1},\"predicate\":{\"op\":\"zz\"}}",
        })
        {
            var (parsed, webError) = CustomAlertRuleDefinition.TryParse(bad);
            Assert.Null(parsed);
            Assert.False(string.IsNullOrEmpty(webError));
            Assert.Equal(webError, AlertRuleEditorModel.Validate(bad));
        }
    }

    [Fact]
    public void EveryTemplate_LoadsIntoTheFormAndBuildsBackToADefinitionTheValidatorAccepts()
    {
        foreach (var template in CustomAlertTemplates.All)
        {
            var form = AlertRuleEditorModel.Load(template.DefinitionJson);
            Assert.NotNull(form);
            Assert.Null(AlertRuleEditorModel.TryBuild(template.DefinitionJson, form!, out var built));

            var (original, _) = CustomAlertRuleDefinition.TryParse(template.DefinitionJson);
            var (rebuilt, error) = CustomAlertRuleDefinition.TryParse(built);
            Assert.Null(error);
            Assert.Equal(original!.Plan.Measure.Key, rebuilt!.Plan.Measure.Key);
            Assert.Equal(original.Op, rebuilt.Op);
            Assert.Equal(original.WarnThreshold, rebuilt.WarnThreshold);
            Assert.Equal(original.CriticalThreshold, rebuilt.CriticalThreshold);
            Assert.Equal(original.WindowHours, rebuilt.WindowHours);
            Assert.Equal(original.BreachSamples, rebuilt.BreachSamples);
            Assert.Equal(original.ClearSamples, rebuilt.ClearSamples);
        }
    }

    [Fact]
    public void AnEdit_KeepsTheKeysTheFormDoesNotOwn()
    {
        var existing = "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"unit\":\"ms\",\"hours\":1,"
            + "\"filters\":[{\"dimension\":\"wait_type\",\"op\":\"eq\",\"value\":\"CXPACKET\"}]},"
            + "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1},\"evaluationIntervalSeconds\":120}";
        Assert.Null(AlertRuleEditorModel.Validate(existing));

        Assert.Null(AlertRuleEditorModel.TryBuild(existing, Form(warn: "9"), out var built));
        var root = JsonNode.Parse(built)!.AsObject();
        Assert.Equal(120, (int)root["evaluationIntervalSeconds"]!);
        Assert.NotNull(root["metric"]!["filters"]);
        Assert.Equal(9, (double)root["predicate"]!["warnThreshold"]!);
    }

    [Fact]
    public void ABoxThatIsNotANumber_IsRefusedByName_AndTheValidatorsRefusalPassesThroughUnchanged()
    {
        Assert.Equal("Warning threshold must be a number.", AlertRuleEditorModel.TryBuild(null, Form(warn: "lots"), out _));

        // 'lt' on a count is a validator rule, not the form's: the form shows that text as it is.
        var refusal = AlertRuleEditorModel.TryBuild(null, Form(op: "lt") with { Aggregate = "count", Unit = "count" }, out var json);
        Assert.Equal(AlertRuleEditorModel.Validate(json), refusal);
        Assert.NotNull(refusal);
    }

    [Fact]
    public void ARangeRule_BuildsBoundsAndNoThresholds()
    {
        var form = Form(op: "outside", warn: "") with { Lower = "10", Upper = "90" };
        Assert.Null(AlertRuleEditorModel.TryBuild(null, form, out var built));
        var predicate = JsonNode.Parse(built)!["predicate"]!.AsObject();
        Assert.Equal(10, (double)predicate["lowerBound"]!);
        Assert.Null(predicate["warnThreshold"]);
    }

    [Fact]
    public void TheExpiryIsTheWebsRule_CreateRefusesThePast_EditRefusesOnlyAChange()
    {
        var past = DateTime.UtcNow.AddDays(-2);
        Assert.NotNull(AlertRuleEditorModel.ResolveExpiry(AlertRuleEditorModel.ExpiryText(past), isEdit: false, current: null).Error);
        Assert.NotNull(AlertRuleEditorModel.ResolveExpiry(AlertRuleEditorModel.ExpiryText(past), isEdit: true, current: DateTime.UtcNow.AddDays(1)).Error);
        // Re-saving a rule that has already expired sends its own expiry back and must still save.
        var stored = new DateTime(past.Ticks - (past.Ticks % 10), DateTimeKind.Utc);
        Assert.Null(AlertRuleEditorModel.ResolveExpiry(AlertRuleEditorModel.ExpiryText(stored), isEdit: true, current: stored).Error);
        Assert.Equal((null, null), AlertRuleEditorModel.ResolveExpiry("", isEdit: true, current: stored));
    }

    [Fact]
    public void TheRuleList_SaysPausedExpiredFiringAndQuiet()
    {
        var now = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
        static CustomAlertRuleSummary Rule(bool enabled, DateTime? expires) =>
            new(1, "r", null, enabled, 3, new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc), "erik", null, expires);

        Assert.Equal("Paused", AlertRuleRow.From(Rule(false, null), "All servers", 2, now).Status);
        Assert.Equal("Expired", AlertRuleRow.From(Rule(true, now.AddHours(-1)), "All servers", 0, now).Status);
        Assert.Equal("Firing on 2", AlertRuleRow.From(Rule(true, null), "All servers", 2, now).StateText);
        Assert.Equal("Quiet", AlertRuleRow.From(Rule(true, now.AddDays(1)), "All servers", 0, now).StateText);
        Assert.Equal("Never", AlertRuleRow.From(Rule(true, null), "All servers", 0, now).ExpiryText);
    }
}

/// <summary>
/// #6390 S4 live (DARLING_TEST_PG): what the Viewer saves is the row the web reads, the store's stale-version and cap refusals reach the
/// Viewer unchanged, Delete closes the open incident, and the write gate follows the login's own grant on the rules table.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerAlertRulesLiveTests
{
    private const string Definition =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":0.25},\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the Viewer alert-rule live tests.");
        return new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = "collect,config,public" }.ConnectionString;
    }

    private static async Task MigrateAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        // The teardown's recovery row goes through this provisioning function, which a migration does not create.
        await using var function = new NpgsqlCommand(DarlingManagedRoles.BuildCustomAlertResolveFunctionSql("config"), connection);
        await function.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRulesLikeAsync(NpgsqlConnection cleanup, string prefix, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM config.custom_alert_rules WHERE name LIKE $1", cleanup);
        command.Parameters.AddWithValue(prefix + "%");
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task ARuleTheViewerSaves_IsReadBackByTheStoreAndTheWebsRuleNodeWithTheSameFields()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await MigrateAsync(cs, ct);
        var prefix = "vw_" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(cs);
            var expiry = new DateTime(DateTime.UtcNow.AddDays(3).Ticks / 10 * 10, DateTimeKind.Utc);
            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await viewer.CreateCustomAlertRuleAsync(prefix, "from the viewer", Definition, true, expiry, ct));

            await using var dataSource = NpgsqlDataSource.Create(cs);
            var store = new CustomAlertRuleStore(dataSource, StorageCommandDeadlines.McpReadSeconds);
            var fromStore = Assert.IsType<CustomAlertRuleResult.Ok>(await store.GetAsync(created.Rule!.Id, ct)).Rule!;
            var fromViewer = Assert.IsType<CustomAlertRuleResult.Ok>(await viewer.GetCustomAlertRuleAsync(created.Rule.Id, ct)).Rule!;

            Assert.Equal(fromStore, fromViewer);
            Assert.Equal(Environment.UserName, fromStore.UpdatedBy);
            Assert.Equal(expiry, fromStore.ExpiresAtUtc);

            var node = DarlingWebEndpoints.BuildFullRuleNode(fromStore);
            Assert.Equal(prefix, (string)node["name"]!);
            Assert.Equal("from the viewer", (string)node["description"]!);
            Assert.True((bool)node["enabled"]!);
            Assert.Equal(1, (int)node["version"]!);
            Assert.Equal(Environment.UserName, (string)node["updated_by"]!);
            Assert.Equal(CustomAlertExpiry.Format(expiry), (string)node["expires_at_utc"]!);
            Assert.Equal(DarlingWebEndpoints.BuildFullRuleNode(fromViewer).ToJsonString(), node.ToJsonString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, (cleanup, cleanupCt) => DeleteRulesLikeAsync(cleanup, prefix, cleanupCt));
        }
    }

    [Fact]
    public async Task AStaleVersion_IsRefusedWithTheStoresOwnText_AndPauseCarriesTheVersionItLoaded()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await MigrateAsync(cs, ct);
        var prefix = "vs_" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(cs);
            var rule = Assert.IsType<CustomAlertRuleResult.Ok>(await viewer.CreateCustomAlertRuleAsync(prefix, null, Definition, true, null, ct)).Rule!;

            // A first save moves the rule to version 2; a second save still holding version 1 is stale.
            Assert.IsType<CustomAlertRuleResult.Ok>(
                await viewer.UpdateCustomAlertRuleAsync(rule.Id, prefix, "x", Definition, true, rule.Version, null, ct));
            var stale = await viewer.UpdateCustomAlertRuleAsync(rule.Id, prefix, "y", Definition, true, rule.Version, null, ct);
            var conflict = Assert.IsType<CustomAlertRuleResult.Conflict>(stale);

            await using var dataSource = NpgsqlDataSource.Create(cs);
            var direct = await new CustomAlertRuleStore(dataSource, StorageCommandDeadlines.McpReadSeconds)
                .UpdateAsync(rule.Id, prefix, "z", Definition, true, rule.Version, "t", null, ct);
            Assert.Equal(conflict.Message, Assert.IsType<CustomAlertRuleResult.Conflict>(direct).Message);
            Assert.Equal(conflict.Message, ViewerDataService.CustomAlertRuleRefusal(stale));

            // Pause under the version the list showed (2) works and bumps to 3; under the old one it is refused.
            Assert.IsType<CustomAlertRuleResult.Conflict>(await viewer.SetCustomAlertRuleEnabledAsync(rule.Id, false, rule.Version, ct));
            var paused = Assert.IsType<CustomAlertRuleResult.Ok>(await viewer.SetCustomAlertRuleEnabledAsync(rule.Id, false, 2, ct)).Rule!;
            Assert.False(paused.Enabled);
            Assert.Equal(3, paused.Version);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, (cleanup, cleanupCt) => DeleteRulesLikeAsync(cleanup, prefix, cleanupCt));
        }
    }

    [Fact]
    public async Task TheEnabledRuleCap_HoldsThroughTheViewer_WithTheStoresText()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await MigrateAsync(cs, ct);
        var prefix = "vc_" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(cs);
            CustomAlertRuleResult? refused = null;
            for (var i = 0; i <= CustomAlertRuleStore.EnabledRuleCap && refused is null; i++)
            {
                var result = await viewer.CreateCustomAlertRuleAsync($"{prefix}_{i}", null, Definition, true, null, ct);
                refused = result is CustomAlertRuleResult.Ok ? null : result;
            }

            var invalid = Assert.IsType<CustomAlertRuleResult.Invalid>(refused);
            Assert.Equal(CustomAlertRuleStore.EnabledCapCreateMessage, invalid.Message);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, (cleanup, cleanupCt) => DeleteRulesLikeAsync(cleanup, prefix, cleanupCt));
        }
    }

    [Fact]
    public async Task Delete_ClosesTheOpenIncident_BeforeTheRuleGoes()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await MigrateAsync(cs, ct);
        var name = "vd_" + Guid.NewGuid().ToString("N");
        const int serverId = 990301;
        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(cs);
            await using var dataSource = NpgsqlDataSource.Create(cs);
            var rule = Assert.IsType<CustomAlertRuleResult.Ok>(await viewer.CreateCustomAlertRuleAsync(name, null, Definition, true, null, ct)).Rule!;
            var state = new CustomAlertStateStore(dataSource, StorageCommandDeadlines.McpReadSeconds);
            var at = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
            await state.SaveAsync(rule.Id, serverId, new CustomAlertRuleState(
                new PerformanceMonitor.Alerting.PersistenceState(ConsecutiveBreaches: 3, ConsecutiveClears: 0, Firing: true),
                rule.Version, "Critical", at, at.AddSeconds(60)), ct);
            Assert.Equal(1, (await viewer.GetCustomAlertFiringCountsAsync(ct))[rule.Id]);

            Assert.IsType<CustomAlertRuleResult.Ok>(await viewer.DeleteCustomAlertRuleAsync(rule.Id, ct));

            await using var count = dataSource.CreateCommand("SELECT COUNT(*) FROM config_alert_log WHERE metric_name = $1 AND server_id = $2");
            count.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name + " Resolved" });
            count.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            Assert.Equal(1, Convert.ToInt32(await count.ExecuteScalarAsync(ct)));
            Assert.Empty(await state.ListForRuleAsync(rule.Id, ct));
            Assert.IsType<CustomAlertRuleResult.NotFound>(await viewer.GetCustomAlertRuleAsync(rule.Id, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRulesLikeAsync(cleanup, name, cleanupCt);
                await using var log = new NpgsqlCommand("DELETE FROM config.config_alert_log WHERE server_id = $1", cleanup);
                log.Parameters.AddWithValue(serverId);
                await log.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public async Task TheWriteGate_FollowsTheLoginsOwnGrant_NotTheReadOnlyProbe()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await MigrateAsync(cs, ct);
        var role = "vw_gate_" + Guid.NewGuid().ToString("N")[..12];
        var bodySucceeded = false;
        try
        {
            await using (var admin = new NpgsqlConnection(cs))
            {
                await admin.OpenAsync(ct);
                foreach (var sql in new[]
                {
                    $"CREATE ROLE {role} LOGIN",
                    $"GRANT USAGE ON SCHEMA config TO {role}",
                    $"GRANT SELECT ON config.custom_alert_rules TO {role}",
                    // Holds what the mute-rule and dismiss probes ask for, but not the rules table's write grants.
                    $"GRANT INSERT, UPDATE ON config.config_alert_log TO {role}",
                })
                {
                    await using var command = new NpgsqlCommand(sql, admin);
                    await command.ExecuteNonQueryAsync(ct);
                }
            }

            var roleCs = new NpgsqlConnectionStringBuilder(cs) { Username = role, Password = null }.ConnectionString;
            await using (var viewer = new ViewerDataService(roleCs))
            {
                Assert.False(await viewer.DetectReadOnlyAsync(ct), "the mute-rule probe says writable");
                Assert.False(await viewer.CanWriteCustomAlertRulesAsync(ct));
                await Assert.ThrowsAsync<ViewerReadOnlyException>(() => viewer.CreateCustomAlertRuleAsync("denied", null, Definition, false, null, ct));
            }

            await using (var admin = new NpgsqlConnection(cs))
            {
                await admin.OpenAsync(ct);
                await using var grant = new NpgsqlCommand($"GRANT INSERT, UPDATE, DELETE ON config.custom_alert_rules TO {role}", admin);
                await grant.ExecuteNonQueryAsync(ct);
            }

            await using (var viewer = new ViewerDataService(roleCs))
            {
                Assert.True(await viewer.CanWriteCustomAlertRulesAsync(ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
