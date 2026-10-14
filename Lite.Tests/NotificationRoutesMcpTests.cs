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
using System.Text.RegularExpressions;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5750 lane R6: Lite's <c>get_notification_routes</c> / <c>set_notification_route_enabled</c> /
/// <c>delete_notification_route</c>. The read shows fleet-wide, server-scoped, tag-scoped and orphaned routes with the SAME
/// field names as Darling's twin and never a URL, key or host; the two writes are gated like the analysis mute and refuse
/// while a saved-settings restore of the table is pending. Credential Manager is an in-memory pair here.
/// </summary>
[Trait("Reads", "Darling")]
[Collection("app-alert-statics")]
public sealed class NotificationRoutesMcpTests : IDisposable
{
    private const string TeamsUrl = "https://teams.r6-secret.test/hook/abc123";
    private const string SlackUrl = "https://hooks.slack.r6-secret.test/T0/B0/xyz";
    private const string GenericUrl = "https://generic.r6-secret.test/in";
    private const string PagerKey = "R6PAGERDUTYROUTINGKEY0123456789ab";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly FakeRouteSecrets _secrets = new();
    private readonly List<ServerConnection> _servers;
    private readonly int _serverA;
    private readonly int _serverB;
    private readonly DuckDbNotificationRouteStore _store;
    private readonly LocalDataService _data;

    public NotificationRoutesMcpTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRoutesMcpTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _data = new LocalDataService(_initializer);

        _servers =
        [
            new ServerConnection { ServerName = "SQLA", DisplayName = "Alpha" },
            new ServerConnection { ServerName = "SQLB", DisplayName = "Beta" },
        ];
        var ids = _servers.Select(s => RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s))).ToArray();
        _serverA = ids[0];
        _serverB = ids[1];

        _store = new DuckDbNotificationRouteStore(
            _initializer,
            () => ids,
            _secrets.Access);
    }

    public void Dispose()
    {
        NotificationRouteCache.Configure(null);
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static NotificationRoute Route(string match, int? server = null, int? tag = null, string teams = "", string slack = "",
        string generic = "", string pager = "", string smtp = "") =>
        new(0, match, teams, slack, generic, pager, smtp, true) { ScopeServerId = server, ScopeTagId = tag };

    private async Task<string> ReadAsync() =>
        await McpAlertTools.ReadNotificationRoutesAsync(_store, _servers, await _data.GetServerTagsAsync());

    private async Task<(int Fleet, int Server, int Tag, int Orphan, int Root, int Child)> SeedAsync()
    {
        var root = await _data.CreateServerTagAsync("Prod", null);
        var child = await _data.CreateServerTagAsync("Payments", root);
        await _data.AssignServerTagAsync(new[] { _serverA }, child);
        await _data.AssignServerTagAsync(new[] { _serverB }, root);

        var fleet = await _store.InsertAsync(Route("Performance", teams: TeamsUrl, smtp: "ops@r6.test"));
        var server = await _store.InsertAsync(Route("*", server: _serverA, slack: SlackUrl, pager: PagerKey));
        var tag = await _store.InsertAsync(Route("High CPU", tag: root, generic: GenericUrl));
        var orphan = await _store.InsertAsync(Route("Blocking", server: 424242, teams: TeamsUrl));
        return (fleet, server, tag, orphan, root, child);
    }

    [Fact]
    public async Task TheRead_ShowsFleetServerTagAndOrphanedRoutes_AndNeverAUrlKeyOrHost()
    {
        var ids = await SeedAsync();
        var json = await ReadAsync();

        foreach (var secret in new[] { TeamsUrl, SlackUrl, GenericUrl, PagerKey, "r6-secret", "teams.", "hooks.slack", "https://", "http://" })
        {
            Assert.DoesNotContain(secret, json, StringComparison.OrdinalIgnoreCase);
        }

        using var doc = JsonDocument.Parse(json);
        var routes = doc.RootElement.GetProperty("routes").EnumerateArray().ToDictionary(r => r.GetProperty("route_id").GetInt32());
        Assert.Equal(4, doc.RootElement.GetProperty("route_count").GetInt32());
        Assert.Equal(4, doc.RootElement.GetProperty("enabled_route_count").GetInt32());

        var fleet = routes[ids.Fleet];
        Assert.Equal(JsonValueKind.Null, fleet.GetProperty("scope_server_id").ValueKind);
        Assert.Equal(JsonValueKind.Null, fleet.GetProperty("scope_tag_id").ValueKind);
        Assert.Equal(JsonValueKind.Null, fleet.GetProperty("expanded_server_count").ValueKind);
        Assert.False(fleet.GetProperty("orphaned").GetBoolean());
        Assert.False(fleet.GetProperty("scope_unreadable").GetBoolean());
        Assert.Equal(new[] { "Teams", "Email" }, fleet.GetProperty("configured_channels").EnumerateArray().Select(c => c.GetString()).ToArray());
        Assert.Equal("ops@r6.test", fleet.GetProperty("smtp_recipients").GetString());
        Assert.Equal("family", fleet.GetProperty("match_kind").GetString());

        var server = routes[ids.Server];
        Assert.Equal(_serverA, server.GetProperty("scope_server_id").GetInt32());
        Assert.Equal("Alpha", server.GetProperty("scope_server_name").GetString());
        Assert.Equal(1, server.GetProperty("expanded_server_count").GetInt32());
        Assert.False(server.GetProperty("orphaned").GetBoolean());
        Assert.Equal("*", server.GetProperty("metric_match").GetString());
        /* A "*" route is the least specific route, not an exact performance metric (it used to read exact_metric / performance). */
        Assert.Equal("wildcard", server.GetProperty("match_kind").GetString());
        Assert.Equal(JsonValueKind.Null, server.GetProperty("family").ValueKind);
        Assert.Equal(new[] { "Slack", "PagerDuty" }, server.GetProperty("configured_channels").EnumerateArray().Select(c => c.GetString()).ToArray());

        var tag = routes[ids.Tag];
        Assert.Equal(ids.Root, tag.GetProperty("scope_tag_id").GetInt32());
        Assert.Equal("Prod", tag.GetProperty("scope_tag_name").GetString());
        Assert.Equal(2, tag.GetProperty("expanded_server_count").GetInt32());
        Assert.Equal("exact_metric", tag.GetProperty("match_kind").GetString());

        var orphan = routes[ids.Orphan];
        Assert.True(orphan.GetProperty("orphaned").GetBoolean());
        Assert.Equal(424242, orphan.GetProperty("scope_server_id").GetInt32());
        Assert.Equal(JsonValueKind.Null, orphan.GetProperty("scope_server_name").ValueKind);
        Assert.Equal(0, orphan.GetProperty("expanded_server_count").GetInt32());
    }

    [Fact]
    public async Task AnUnreadableScope_IsReportedAsScopeUnreadable_NotShownAsAnOrdinaryActiveRoute()
    {
        var ids = await SeedAsync();
        var tags = await _data.GetServerTagsAsync();
        using (var conn = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "lite.duckdb")}"))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            using var drop = conn.CreateCommand();
            drop.CommandText = "DROP TABLE server_tag_map";
            await drop.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        using var doc = JsonDocument.Parse(await McpAlertTools.ReadNotificationRoutesAsync(_store, _servers, tags));
        var routes = doc.RootElement.GetProperty("routes").EnumerateArray().ToDictionary(r => r.GetProperty("route_id").GetInt32());

        /* The tag route cannot be expanded: it matches nothing, is not orphaned, and says so. Same rule as Darling's loader (#5750). */
        var tag = routes[ids.Tag];
        Assert.True(tag.GetProperty("scope_unreadable").GetBoolean());
        Assert.False(tag.GetProperty("orphaned").GetBoolean());
        Assert.Equal(JsonValueKind.Null, tag.GetProperty("expanded_server_count").ValueKind);

        /* The server-scoped, orphaned and fleet-wide routes are unaffected. */
        Assert.False(routes[ids.Server].GetProperty("scope_unreadable").GetBoolean());
        Assert.Equal(1, routes[ids.Server].GetProperty("expanded_server_count").GetInt32());
        Assert.True(routes[ids.Orphan].GetProperty("orphaned").GetBoolean());
        Assert.False(routes[ids.Orphan].GetProperty("scope_unreadable").GetBoolean());
        Assert.False(routes[ids.Fleet].GetProperty("scope_unreadable").GetBoolean());
    }

    [Fact]
    public async Task ADeletedTag_LeavesItsRouteInTheRead_AsOrphaned()
    {
        var ids = await SeedAsync();
        await _data.DeleteServerTagAsync(ids.Root);

        using var doc = JsonDocument.Parse(await ReadAsync());
        var tag = doc.RootElement.GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == ids.Tag);
        Assert.True(tag.GetProperty("orphaned").GetBoolean());
        Assert.Equal(JsonValueKind.Null, tag.GetProperty("scope_tag_name").ValueKind);
    }

    [Fact]
    public async Task ZeroRoutes_IsAnEmptyListWithCountZero_NotAnError()
    {
        using var doc = JsonDocument.Parse(await ReadAsync());
        Assert.Equal(0, doc.RootElement.GetProperty("route_count").GetInt32());
        Assert.Empty(doc.RootElement.GetProperty("routes").EnumerateArray());
        Assert.False(doc.RootElement.TryGetProperty("routes_supported", out _));
    }

    [Fact]
    public async Task SetEnabled_UpdatesThenReportsUnchanged_AndNotFoundForAnUnknownRoute()
    {
        var ids = await SeedAsync();
        var tags = await _data.GetServerTagsAsync();

        using (var off = JsonDocument.Parse(await McpAlertTools.SetRouteEnabledAsync(_store, _servers, tags, ids.Fleet, false)))
        {
            Assert.Equal("updated", off.RootElement.GetProperty("status").GetString());
            Assert.False(off.RootElement.GetProperty("route").GetProperty("enabled").GetBoolean());
        }

        Assert.False((await _store.GetAsync(ids.Fleet))!.Enabled);

        using (var again = JsonDocument.Parse(await McpAlertTools.SetRouteEnabledAsync(_store, _servers, tags, ids.Fleet, false)))
        {
            Assert.Equal("unchanged", again.RootElement.GetProperty("status").GetString());
        }

        using (var missing = JsonDocument.Parse(await McpAlertTools.SetRouteEnabledAsync(_store, _servers, tags, 999, true)))
        {
            Assert.Equal("not_found", missing.RootElement.GetProperty("status").GetString());
        }
    }

    [Fact]
    public async Task Delete_RemovesTheRouteAndItsCredentials_AndNotFoundTheSecondTime()
    {
        var ids = await SeedAsync();
        Assert.True(_secrets.Has(ids.Fleet, NotificationRouter.TeamsChannel));

        using (var gone = JsonDocument.Parse(await McpAlertTools.DeleteRouteAsync(_store, ids.Fleet)))
        {
            Assert.Equal("deleted", gone.RootElement.GetProperty("status").GetString());
        }

        Assert.Null(await _store.GetAsync(ids.Fleet));
        Assert.False(_secrets.Has(ids.Fleet, NotificationRouter.TeamsChannel));

        using var second = JsonDocument.Parse(await McpAlertTools.DeleteRouteAsync(_store, ids.Fleet));
        Assert.Equal("not_found", second.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task BothWrites_AreRefused_WhileARestoreOfTheTableIsPending_AndChangeNothing()
    {
        var ids = await SeedAsync();
        var tags = await _data.GetServerTagsAsync();
        File.WriteAllText(
            Path.Combine(_initializer.ArchivePath, PreservedTableRestore.RestoreMarkerFileName),
            "pm_preserve_r6\n" + DuckDbNotificationRouteStore.TableName + "\n");

        using (var set = JsonDocument.Parse(await McpAlertTools.SetRouteEnabledAsync(_store, _servers, tags, ids.Fleet, false)))
        {
            Assert.Equal("unavailable", set.RootElement.GetProperty("status").GetString());
            Assert.Contains("until Lite restarts", set.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }

        using (var del = JsonDocument.Parse(await McpAlertTools.DeleteRouteAsync(_store, ids.Fleet)))
        {
            Assert.Equal("unavailable", del.RootElement.GetProperty("status").GetString());
        }

        Assert.True((await _store.GetAsync(ids.Fleet))!.Enabled);
    }

    [Fact]
    public void TheTwoWrites_AreInTheWriteSet_AndTheReadIsNot()
    {
        Assert.True(McpWritePolicy.IsWrite("set_notification_route_enabled"));
        Assert.True(McpWritePolicy.IsWrite("delete_notification_route"));
        Assert.False(McpWritePolicy.IsWrite("get_notification_routes"));
    }

    // ---- field-name parity with Darling's twin ---------------------------------------------------

    private static IReadOnlyList<string> DarlingRouteFields(string section)
    {
        var source = Lite.Tests.ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs");
        var start = source.IndexOf(section, StringComparison.Ordinal);
        Assert.True(start >= 0, "Darling source lost: " + section);
        var open = source.IndexOf("=> new", start, StringComparison.Ordinal);
        var braceOpen = source.IndexOf('{', open);
        var depth = 0;
        var end = braceOpen;
        for (; end < source.Length; end++)
        {
            if (source[end] == '{') depth++;
            else if (source[end] == '}' && --depth == 0) break;
        }

        var body = source[braceOpen..end];
        /* Only the object's own members: drop nested initializers and comments, then take "name =" at the start of a member. */
        body = Regex.Replace(body, @"/\*.*?\*/", "", RegexOptions.Singleline);
        return Regex.Matches(body, @"(?m)^\s{8}([a-z_]+)\s*=").Select(m => m.Groups[1].Value).ToList();
    }

    [Fact]
    public async Task EveryRouteField_IsNamedTheSameInLiteAndDarling_SoOneSideAddingOrRenamingOneFails()
    {
        await SeedAsync();
        using var doc = JsonDocument.Parse(await ReadAsync());
        var lite = doc.RootElement.GetProperty("routes").EnumerateArray().First().EnumerateObject().Select(p => p.Name).ToList();

        /* Darling has ONE route projection: get_notification_routes lists it and set_notification_route_enabled returns it. */
        var darling = DarlingRouteFields("internal static object RoutePayload(PgNotificationRouteStore.RouteSummary r) => new");
        Assert.NotEmpty(darling);
        /* #5632: the agent channel is Darling's alone (Lite has no investigation requests), so its five fields are the one
           named exception; every one of them must still be in Darling's projection. */
        var darlingOnly = new[] { "agent_level", "agent_max_per_hour", "agent_max_per_day", "agent_lease_minutes", "agent_expiry_minutes" };
        Assert.All(darlingOnly, f => Assert.Contains(f, darling));
        Assert.Equal(darling.Except(darlingOnly).OrderBy(n => n, StringComparer.Ordinal), lite.OrderBy(n => n, StringComparer.Ordinal));
        var source = Lite.Tests.ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs");
        Assert.Contains("routes = routes.Select(RoutePayload)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("routes.Select(r => new", source, StringComparison.Ordinal);
    }

    [Fact]
    public void BothProducts_SpellTheMatchKindAndFamilyThroughTheOneSharedHelper()
    {
        foreach (var path in new[] { "Lite/Mcp/McpAlertTools.cs", "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs" })
        {
            var source = Lite.Tests.ParitySource.ReadFile(path);
            Assert.Contains("NotificationRouteKinds.MatchKind(r.MetricMatch)", source, StringComparison.Ordinal);
            Assert.Contains("NotificationRouteKinds.FamilyOf(r.MetricMatch)", source, StringComparison.Ordinal);
        }

        Assert.Equal("wildcard", NotificationRouteKinds.MatchKind("*"));
        Assert.Null(NotificationRouteKinds.FamilyOf("*"));
        Assert.Equal("family", NotificationRouteKinds.MatchKind("Performance"));
        Assert.Equal("performance", NotificationRouteKinds.FamilyOf("Performance"));
        Assert.Equal("exact_metric", NotificationRouteKinds.MatchKind("High CPU"));
    }

    [Fact]
    public async Task EveryTopLevelField_IsNamedTheSameInLiteAndDarling()
    {
        using var doc = JsonDocument.Parse(await ReadAsync());
        var lite = doc.RootElement.EnumerateObject().Select(p => p.Name).ToList();

        var source = Lite.Tests.ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs");
        var start = source.IndexOf("return JsonSerializer.Serialize(new\r\n            {\r\n                /* The taxonomy first", StringComparison.Ordinal);
        if (start < 0) start = source.IndexOf("return JsonSerializer.Serialize(new\n            {\n                /* The taxonomy first", StringComparison.Ordinal);
        Assert.True(start >= 0, "Darling get_notification_routes payload moved");
        var end = source.IndexOf("}, McpHelpers.JsonOptions);", start, StringComparison.Ordinal);
        var body = Regex.Replace(source[start..end], @"/\*.*?\*/", "", RegexOptions.Singleline);
        var darling = Regex.Matches(body, @"(?m)^ {16}([a-z_]+)\s*=").Select(m => m.Groups[1].Value).ToList();

        Assert.Equal(darling.OrderBy(n => n, StringComparer.Ordinal), lite.OrderBy(n => n, StringComparer.Ordinal));
    }
}
