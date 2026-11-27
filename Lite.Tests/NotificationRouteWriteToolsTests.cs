/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9639: Lite's <c>create_notification_route</c> and <c>update_notification_route</c>, the twins of Darling's. Each call is read
/// back through the store the Notification Routes window reads, the destinations are never in an answer, and every validation rule
/// refuses in words that name the argument and changes nothing. Credential Manager is an in-memory pair here.
/// </summary>
[Collection("app-alert-statics")]
public sealed class NotificationRouteWriteToolsTests : IDisposable
{
    private const string TeamsUrl = "https://teams.w9639-secret.test/hook/abc123";
    private const string SlackUrl = "https://hooks.slack.w9639-secret.test/T0/B0/xyz";
    private const string NewSlackUrl = "https://hooks.slack.w9639-secret.test/T1/B1/new";
    private const string PagerKey = "W9639PAGERDUTYROUTINGKEY0123456789";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly FakeRouteSecrets _secrets = new();
    private readonly List<ServerConnection> _servers;
    private readonly int _serverA;
    private readonly DuckDbNotificationRouteStore _store;
    private readonly LocalDataService _data;

    public NotificationRouteWriteToolsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteWriteToolsTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _data = new LocalDataService(_initializer);
        _servers = [new ServerConnection { ServerName = "SQLA", DisplayName = "Alpha" }];
        var ids = _servers.Select(s => RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s))).ToArray();
        _serverA = ids[0];
        _store = new DuckDbNotificationRouteStore(_initializer, () => ids, _secrets.Access);
    }

    public void Dispose()
    {
        NotificationRouteCache.Configure(null);
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private async Task<JsonDocument> CreateAsync(
        string? match, bool enabled = true, int? server = null, int? tag = null, McpAlertTools.RouteDestinationArgs? dest = null) =>
        JsonDocument.Parse(await McpAlertTools.CreateRouteAsync(
            _store, _servers, await _data.GetServerTagsAsync(), match, enabled, server, tag, dest ?? McpAlertTools.RouteDestinationArgs.None));

    private async Task<JsonDocument> UpdateAsync(
        int id, string? match = null, bool? enabled = null, int? server = null, int? tag = null, bool? fleetWide = null,
        McpAlertTools.RouteDestinationArgs? dest = null) =>
        JsonDocument.Parse(await McpAlertTools.UpdateRouteAsync(
            _store, _servers, await _data.GetServerTagsAsync(), id, match, enabled, server, tag, fleetWide, dest ?? McpAlertTools.RouteDestinationArgs.None));

    private static string Status(JsonDocument doc) => doc.RootElement.GetProperty("status").GetString()!;

    private static NotificationRoute Seed(string match = AlertFamily.Performance) =>
        new(0, match, TeamsUrl, SlackUrl, "", PagerKey, "ops@w9639.test", true);

    [Fact]
    public async Task Create_StoresTheRouteAndItsDestinations_AsTheWindowSaveDoes_AndAnswersWithoutAnyDestination()
    {
        var tag = await _data.CreateServerTagAsync("Prod", null);

        using var answer = await CreateAsync(
            AlertFamily.Performance, enabled: false, tag: tag,
            dest: new McpAlertTools.RouteDestinationArgs(TeamsUrl, SlackUrl, null, PagerKey, "ops@w9639.test, dba@w9639.test"));

        Assert.Equal("created", Status(answer));
        var id = answer.RootElement.GetProperty("route").GetProperty("route_id").GetInt32();
        var stored = await _store.GetAsync(id);
        Assert.NotNull(stored);
        Assert.Equal(AlertFamily.Performance, stored.MetricMatch);
        Assert.False(stored.Enabled);
        Assert.Equal(tag, stored.ScopeTagId);
        Assert.Null(stored.ScopeServerId);
        Assert.Equal(TeamsUrl, stored.TeamsUrl);
        Assert.Equal(SlackUrl, stored.SlackUrl);
        Assert.Equal("", stored.GenericUrl);
        Assert.Equal(PagerKey, stored.PagerDutyRoutingKey);
        Assert.Equal("ops@w9639.test, dba@w9639.test", stored.SmtpRecipients);
        var json = answer.RootElement.GetRawText();
        Assert.DoesNotContain("w9639-secret", json, StringComparison.Ordinal);
        Assert.DoesNotContain(PagerKey, json, StringComparison.Ordinal);
        Assert.Equal("delete_notification_route", answer.RootElement.GetProperty("undo").GetProperty("tool").GetString());
        Assert.Equal(id, answer.RootElement.GetProperty("undo").GetProperty("arguments").GetProperty("route_id").GetInt32());
        Assert.Equal("saved", answer.RootElement.GetProperty("destinations").GetProperty("now").GetProperty("teams").GetString());
    }

    [Fact]
    public async Task Create_WithNoDestination_IsFleetWideAndEnabled_AndCarriesNoDestinationBlock()
    {
        using var answer = await CreateAsync("REPORTS");

        Assert.Equal("created", Status(answer));
        var stored = await _store.GetAsync(answer.RootElement.GetProperty("route").GetProperty("route_id").GetInt32());
        Assert.Equal(AlertFamily.Reports, stored!.MetricMatch);
        Assert.True(stored.Enabled);
        Assert.False(stored.IsScoped);
        Assert.Equal(JsonValueKind.Null, answer.RootElement.GetProperty("destinations").ValueKind);
    }

    [Theory]
    [InlineData(null, null, null, null, "metric_match")]
    [InlineData("  ", null, null, null, "metric_match")]
    [InlineData("*", null, null, null, "scope_server_id or scope_tag_id")]
    [InlineData("performance", 1, 1, null, "scope_server_id or scope_tag_id")]
    [InlineData("performance", 424242, null, null, "scope_server_id")]
    [InlineData("performance", null, 424242, null, "scope_tag_id")]
    [InlineData("performance", null, null, "not a url secret-ish", "teams_webhook_url")]
    [InlineData("performance", null, null, "ftp://hooks.w9639-secret.test/x", "teams_webhook_url")]
    [InlineData("performance", null, null, "-", "teams_webhook_url")]
    public async Task Create_RefusesEachBadArgument_NamingIt_AndStoresNothing(string? match, int? server, int? tag, string? teams, string argument)
    {
        using var answer = await CreateAsync(match, server: server, tag: tag, dest: new McpAlertTools.RouteDestinationArgs(teams, null, null, null, null));

        Assert.Equal("invalid", Status(answer));
        var message = answer.RootElement.GetProperty("message").GetString()!;
        Assert.Contains(argument, message, StringComparison.Ordinal);
        Assert.DoesNotContain("w9639-secret", message, StringComparison.Ordinal);
        Assert.DoesNotContain("secret-ish", message, StringComparison.Ordinal);
        Assert.Empty(await _store.ListAsync());
    }

    [Fact]
    public async Task Create_AServerScope_UsesTheIdTheRoutesReadReports()
    {
        using var answer = await CreateAsync("*", server: _serverA);

        Assert.Equal("created", Status(answer));
        var stored = await _store.GetAsync(answer.RootElement.GetProperty("route").GetProperty("route_id").GetInt32());
        Assert.Equal(_serverA, stored!.ScopeServerId);
        Assert.Equal("Alpha", answer.RootElement.GetProperty("route").GetProperty("scope_server_name").GetString());
    }

    [Fact]
    public async Task Update_ChangesOnlyWhatIsPassed_KeepsEveryDestination_AndUndoPutsTheRouteBack()
    {
        var id = await _store.InsertAsync(Seed());

        using var answer = await UpdateAsync(id, match: "Blocking", enabled: false, server: _serverA);

        Assert.Equal("updated", Status(answer));
        var stored = (await _store.GetAsync(id))!;
        Assert.Equal("Blocking", stored.MetricMatch);
        Assert.False(stored.Enabled);
        Assert.Equal(_serverA, stored.ScopeServerId);
        Assert.Equal(TeamsUrl, stored.TeamsUrl);
        Assert.Equal(SlackUrl, stored.SlackUrl);
        Assert.Equal(PagerKey, stored.PagerDutyRoutingKey);
        Assert.Equal("ops@w9639.test", stored.SmtpRecipients);
        Assert.DoesNotContain("w9639-secret", answer.RootElement.GetRawText(), StringComparison.Ordinal);

        var undo = answer.RootElement.GetProperty("undo");
        Assert.Equal("update_notification_route", undo.GetProperty("tool").GetString());
        var args = undo.GetProperty("arguments");
        using var back = await UpdateAsync(
            args.GetProperty("route_id").GetInt32(), args.GetProperty("metric_match").GetString(), args.GetProperty("enabled").GetBoolean(),
            fleetWide: args.GetProperty("fleet_wide").GetBoolean());
        Assert.Equal("updated", Status(back));
        var restored = (await _store.GetAsync(id))!;
        Assert.Equal(AlertFamily.Performance, restored.MetricMatch);
        Assert.True(restored.Enabled);
        Assert.False(restored.IsScoped);
    }

    [Fact]
    public async Task Update_ReplacesRemovesAndKeepsDestinations_ByArgument()
    {
        var id = await _store.InsertAsync(Seed());

        using var answer = await UpdateAsync(id, dest: new McpAlertTools.RouteDestinationArgs(
            Teams: "-", Slack: NewSlackUrl, Generic: null, PagerDuty: null, Recipients: "new@w9639.test"));

        Assert.Equal("updated", Status(answer));
        var stored = (await _store.GetAsync(id))!;
        Assert.Equal("", stored.TeamsUrl);
        Assert.Equal(NewSlackUrl, stored.SlackUrl);
        Assert.Equal(PagerKey, stored.PagerDutyRoutingKey);
        Assert.Equal("new@w9639.test", stored.SmtpRecipients);
        var destinations = answer.RootElement.GetProperty("destinations");
        Assert.Equal("none", destinations.GetProperty("now").GetProperty("teams").GetString());
        Assert.Equal("saved", destinations.GetProperty("previous").GetProperty("teams").GetString());
        Assert.Equal("ops@w9639.test", destinations.GetProperty("previous").GetProperty("email").GetString());
        Assert.Equal("ops@w9639.test", answer.RootElement.GetProperty("undo").GetProperty("arguments").GetProperty("email_recipients").GetString());
        Assert.DoesNotContain("w9639-secret", answer.RootElement.GetRawText(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Update_ThatChangesNothing_IsUnchanged_AndNotFoundForAnUnknownRoute()
    {
        var id = await _store.InsertAsync(Seed());

        using (var same = await UpdateAsync(id, match: "PERFORMANCE", enabled: true))
        {
            Assert.Equal("unchanged", Status(same));
        }

        using (var missing = await UpdateAsync(999, enabled: false))
        {
            Assert.Equal("not_found", Status(missing));
        }
    }

    [Fact]
    public async Task Update_RefusesAnAgentRoute_AndKeepsAnAgentOffRoutesAgentSettings()
    {
        var agent = await _store.InsertAsync(Seed() with { AgentLevel = NotificationRoute.AgentLevelInvestigate });
        var off = await _store.InsertAsync(Seed("Blocking") with { AgentLevel = NotificationRoute.AgentLevelOff, AgentMaxPerHour = 7 });

        using (var refused = await UpdateAsync(agent, enabled: false))
        {
            Assert.Equal("refused", Status(refused));
        }

        Assert.True((await _store.GetAsync(agent))!.Enabled);

        using (var ok = await UpdateAsync(off, enabled: false))
        {
            Assert.Equal("updated", Status(ok));
        }

        var stored = (await _store.GetAsync(off))!;
        Assert.False(stored.Enabled);
        Assert.Equal(NotificationRoute.AgentLevelOff, stored.AgentLevel);
        Assert.Equal(7, stored.AgentMaxPerHour);
    }

    [Theory]
    [InlineData("*", null, null, null, null, "scope_server_id or scope_tag_id")]
    [InlineData(null, 1, 1, null, null, "not both")]
    [InlineData(null, 1, null, true, null, "fleet_wide")]
    [InlineData(null, 424242, null, null, null, "scope_server_id")]
    [InlineData(null, null, 424242, null, null, "scope_tag_id")]
    [InlineData(null, null, null, null, "gopher://x.w9639-secret.test", "slack_webhook_url")]
    public async Task Update_RefusesEachBadArgument_NamingIt_AndChangesNothing(
        string? match, int? server, int? tag, bool? fleetWide, string? slack, string argument)
    {
        var id = await _store.InsertAsync(Seed());

        using var answer = await UpdateAsync(id, match, server: server, tag: tag, fleetWide: fleetWide,
            dest: new McpAlertTools.RouteDestinationArgs(null, slack, null, null, null));

        Assert.Equal("invalid", Status(answer));
        var message = answer.RootElement.GetProperty("message").GetString()!;
        Assert.Contains(argument, message, StringComparison.Ordinal);
        Assert.DoesNotContain("w9639-secret", message, StringComparison.Ordinal);
        var stored = (await _store.GetAsync(id))!;
        Assert.Equal(AlertFamily.Performance, stored.MetricMatch);
        Assert.Equal(SlackUrl, stored.SlackUrl);
        Assert.False(stored.IsScoped);
    }

    [Fact]
    public async Task BothTools_AreRefused_WhileARestoreOfTheTableIsPending_AndChangeNothing()
    {
        var id = await _store.InsertAsync(Seed());
        File.WriteAllText(
            Path.Combine(_initializer.ArchivePath, PreservedTableRestore.RestoreMarkerFileName),
            "pm_preserve_w9639\n" + DuckDbNotificationRouteStore.TableName + "\n");

        using (var create = await CreateAsync("reports"))
        {
            Assert.Equal("unavailable", Status(create));
        }

        using (var update = await UpdateAsync(id, enabled: false))
        {
            Assert.Equal("unavailable", Status(update));
        }

        Assert.True((await _store.GetAsync(id))!.Enabled);
        Assert.Single(await _store.ListAsync());
    }

    [Fact]
    public void BothTools_AreWrites_ServedOnlyWithTheWriteSwitch()
    {
        Assert.True(McpWritePolicy.IsWrite("create_notification_route"));
        Assert.True(McpWritePolicy.IsWrite("update_notification_route"));
    }
}
