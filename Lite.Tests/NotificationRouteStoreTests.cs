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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5750, the Lite half: the <c>notification_routes</c> table (schema v69), the store that keeps its destinations in
/// Credential Manager (an in-memory pair here, never the real one), the scope expansion through the tag tables, and the
/// cache the alert path reads. In the <c>app-alert-statics</c> collection because the cache is static and one test reads
/// the app-wide webhook statics through <see cref="AppAlertSettings"/>.
/// </summary>
[Collection("app-alert-statics")]
public sealed class NotificationRouteStoreTests : IDisposable
{
    private const string TeamsUrl = "https://teams.r3-secret.test/hook/abc123";
    private const string SlackUrl = "https://hooks.slack.r3-secret.test/T0/B0/xyz";
    private const string GenericUrl = "https://generic.r3-secret.test/in";
    private const string PagerKey = "R3PAGERDUTYROUTINGKEY0123456789ab";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _initializer;
    private readonly FakeRouteSecrets _secrets = new();
    private IReadOnlyCollection<int> _knownServers = new[] { 7101, 7102, 7103 };
    private readonly DuckDbNotificationRouteStore _store;

    public NotificationRouteStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteStoreTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "lite.duckdb");
        _initializer = new DuckDbInitializer(_dbPath);
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbNotificationRouteStore(
            _initializer,
            () => _knownServers,
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

    private static NotificationRoute Route(string match, string teams = "", int? server = null, int? tag = null,
        string slack = "", string generic = "", string pager = "", string smtp = "", bool enabled = true) =>
        new(0, match, teams, slack, generic, pager, smtp, enabled) { ScopeServerId = server, ScopeTagId = tag };

    // ---- schema v69 ---------------------------------------------------------------------------

    [Fact]
    public async Task AFreshStore_HasTheTable_AtTheCurrentVersion_AndTheV69BlockIsNonFatal()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 69);
        var source = Lite.Tests.ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs");
        var start = source.IndexOf("if (fromVersion < 69)", StringComparison.Ordinal);
        Assert.True(start >= 0, "DuckDbInitializer has no v69 block");
        Assert.Contains("Schema.CreateNotificationRoutesTable", source[start..], StringComparison.Ordinal);
        Assert.Contains("#5750", source[start..], StringComparison.Ordinal);

        Assert.Equal(1L, await ScalarLongAsync("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'notification_routes'"));
        Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await ScalarLongAsync("SELECT MAX(version) FROM schema_version"));
    }

    [Fact]
    public async Task AV68Database_ClimbsToV69_AndGainsTheEmptyTable()
    {
        var dbPath = Path.Combine(_tempDir, "v68.duckdb");
        using (var first = new DuckDbInitializer(dbPath))
        {
            await first.InitializeAsync();
        }

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            await ExecAsync(conn, "DROP TABLE notification_routes");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (68)");
        }

        using (var upgraded = new DuckDbInitializer(dbPath))
        {
            await upgraded.InitializeAsync();
        }

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM notification_routes")));
        }
    }

    [Fact]
    public async Task TheTableItself_RefusesBothScopes_AndAFleetWideStar_AndTheStoreRefusesThemFirst()
    {
        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();

        /* DuckDB enforces both CHECKs at INSERT (not just the store's validation). */
        await Assert.ThrowsAnyAsync<Exception>(() => ExecAsync(conn,
            "INSERT INTO notification_routes (route_id, metric_match, scope_server_id, scope_tag_id) VALUES (1, 'High CPU', 5, 6)"));
        await Assert.ThrowsAnyAsync<Exception>(() => ExecAsync(conn,
            "INSERT INTO notification_routes (route_id, metric_match) VALUES (2, '*')"));
        await ExecAsync(conn, "INSERT INTO notification_routes (route_id, metric_match, scope_server_id) VALUES (3, '*', 5)");
        await ExecAsync(conn, "INSERT INTO notification_routes (route_id, metric_match, scope_tag_id) VALUES (4, '*', 6)");
        conn.Close();

        await Assert.ThrowsAsync<ArgumentException>(() => _store.InsertAsync(Route("High CPU", server: 7101, tag: 1)));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.InsertAsync(Route("*")));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.InsertAsync(Route("  *  ")));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.InsertAsync(Route("", server: 7101)));
        Assert.Equal(2L, await ScalarLongAsync("SELECT COUNT(*) FROM notification_routes"));
    }

    // ---- store round trip, secrets ----------------------------------------------------------------

    [Fact]
    public async Task InsertGetUpdateSetEnabledDelete_RoundTrip()
    {
        var id1 = await _store.InsertAsync(Route("High CPU", TeamsUrl, server: 7101, slack: SlackUrl, generic: GenericUrl, pager: PagerKey, smtp: "a@x.test, b@x.test"));
        var id2 = await _store.InsertAsync(Route("*", tag: null, server: 7102));
        Assert.Equal(1, id1);
        Assert.Equal(2, id2);

        var got = await _store.GetAsync(id1);
        Assert.NotNull(got);
        Assert.Equal("High CPU", got!.MetricMatch);
        Assert.Equal(TeamsUrl, got.TeamsUrl);
        Assert.Equal(SlackUrl, got.SlackUrl);
        Assert.Equal(GenericUrl, got.GenericUrl);
        Assert.Equal(PagerKey, got.PagerDutyRoutingKey);
        Assert.Equal("a@x.test, b@x.test", got.SmtpRecipients);
        Assert.Equal(7101, got.ScopeServerId);
        Assert.Null(got.ScopeTagId);
        Assert.True(got.Enabled);
        Assert.Null(got.OffChannels);
        Assert.Equal(new[] { 7101 }, got.ExpandedServerIds!.ToArray());

        Assert.True(await _store.UpdateAsync(got with { MetricMatch = "Blocking", SlackUrl = "", Enabled = false }));
        var updated = (await _store.GetAsync(id1))!;
        Assert.Equal("Blocking", updated.MetricMatch);
        Assert.Equal("", updated.SlackUrl);
        Assert.False(updated.Enabled);
        Assert.False(_secrets.Has(id1, "Slack"), "clearing a channel removes its credential");

        Assert.True(await _store.SetEnabledAsync(id1, true));
        Assert.True((await _store.GetAsync(id1))!.Enabled);
        Assert.False(await _store.SetEnabledAsync(999, true));
        Assert.False(await _store.UpdateAsync(Route("Blocking", server: 7101) with { RouteId = 999 }));

        Assert.Equal(2, (await _store.ListAsync()).Count);
        Assert.True(await _store.DeleteAsync(id2));
        Assert.False(await _store.DeleteAsync(id2));
        Assert.Single(await _store.ListAsync());
    }

    [Fact]
    public async Task NoUrlOrKey_IsEverWrittenToDuckDb_InAnyColumn()
    {
        var id = await _store.InsertAsync(Route("High CPU", TeamsUrl, server: 7101, slack: SlackUrl, generic: GenericUrl, pager: PagerKey, smtp: "ops@x.test"));
        await _store.UpdateAsync((await _store.GetAsync(id))! with { TeamsUrl = TeamsUrl + "/v2" });

        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        var columns = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT column_name FROM information_schema.columns WHERE table_name = 'notification_routes'";
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) columns.Add(reader.GetString(0));
        }

        Assert.Equal(14, columns.Count);
        var needles = new[] { "r3-secret", "R3PAGERDUTY", "hooks.slack", "https://", "hook/abc123" };
        foreach (var column in columns)
        {
            foreach (var needle in needles)
            {
                Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(conn,
                    $"SELECT COUNT(*) FROM notification_routes WHERE CAST({column} AS VARCHAR) ILIKE '%{needle}%'")));
            }
        }

        /* And the destinations did land in the credential seam, under the documented keys. */
        Assert.Equal(TeamsUrl + "/v2", _secrets.ValueOf(id, "Teams"));
        Assert.Equal(PagerKey, _secrets.ValueOf(id, "PagerDuty"));
    }

    [Fact]
    public async Task DeletingARoute_RemovesItsFourCredentials_AndOnlyItsOwn()
    {
        var keep = await _store.InsertAsync(Route("High CPU", TeamsUrl, server: 7101));
        var doomed = await _store.InsertAsync(Route("High CPU", TeamsUrl, server: 7102, slack: SlackUrl, generic: GenericUrl, pager: PagerKey));
        Assert.Equal(5, _secrets.Count);

        Assert.True(await _store.DeleteAsync(doomed));

        Assert.Single(_secrets.Keys);
        Assert.True(_secrets.Has(keep, "Teams"));
    }

    /* ACredentialThatCannotBeRead_TurnsThatChannelOff_InsteadOfInheriting lives in NotificationRouteTruthfulnessTests: reading a lost
       secret logs a warning into the shared buffer, and that class (collection app-logger-statics) counts those warnings while
       this one (app-alert-statics) runs beside it. */

    // ---- scope expansion through the store ----------------------------------------------------------

    private async Task<(int Root, int Child)> SeedTagsAsync(LocalDataService tags)
    {
        var root = await tags.CreateServerTagAsync("Prod", null);
        var child = await tags.CreateServerTagAsync("Payments", root);
        await tags.AssignServerTagAsync(new[] { 7101 }, child);
        await tags.AssignServerTagAsync(new[] { 7102 }, root);
        return (root, child);
    }

    [Fact]
    public async Task ATagScope_CoversItsSubtree_AndAChildTagOnlyItsOwn_WithTheDepthForTheTieRule()
    {
        var tags = new LocalDataService(_initializer);
        var (root, child) = await SeedTagsAsync(tags);
        var rootRoute = await _store.InsertAsync(Route("*", tag: root));
        var childRoute = await _store.InsertAsync(Route("*", tag: child));

        var list = await _store.ListAsync();

        Assert.Equal(new[] { 7101, 7102 }, list.Single(r => r.RouteId == rootRoute).ExpandedServerIds!.OrderBy(i => i).ToArray());
        Assert.Equal(new[] { 7101 }, list.Single(r => r.RouteId == childRoute).ExpandedServerIds!.ToArray());
        Assert.Equal(0, list.Single(r => r.RouteId == rootRoute).ScopeTagDepth);
        Assert.Equal(1, list.Single(r => r.RouteId == childRoute).ScopeTagDepth);
        Assert.All(list, r => Assert.False(r.Orphaned));
    }

    [Fact]
    public async Task ADeletedTag_LeavesTheRouteRow_AndMarksItOrphaned_MatchingNothing()
    {
        var tags = new LocalDataService(_initializer);
        var (_, child) = await SeedTagsAsync(tags);
        var id = await _store.InsertAsync(Route("*", TeamsUrl, tag: child));

        await tags.DeleteServerTagAsync(child);

        Assert.Equal(1L, await ScalarLongAsync("SELECT COUNT(*) FROM notification_routes"));
        var route = (await _store.GetAsync(id))!;
        Assert.True(route.Orphaned);
        Assert.False(route.CanMatch);
        Assert.Empty(route.ExpandedServerIds!);

        /* The deleted tag's id is not reused while a route still names it, so the orphan cannot attach to a new tag. */
        var next = await tags.CreateServerTagAsync("Other", null);
        Assert.True(next > child);
        Assert.True((await _store.GetAsync(id))!.Orphaned);
    }

    [Fact]
    public async Task ADeletedServer_LeavesTheRouteRow_AndMarksItOrphaned()
    {
        var id = await _store.InsertAsync(Route("High CPU", TeamsUrl, server: 7103));
        Assert.False((await _store.GetAsync(id))!.Orphaned);

        _knownServers = new[] { 7101, 7102 };

        Assert.Equal(1L, await ScalarLongAsync("SELECT COUNT(*) FROM notification_routes"));
        var route = (await _store.GetAsync(id))!;
        Assert.True(route.Orphaned);
        Assert.False(route.CanMatch);
    }

    [Fact]
    public async Task AFailedTagRead_FailsClosedForTagRoutesOnly_ServerAndFleetWideRoutesStay()
    {
        /* Darling's loader behaves this way (StoreConfigProvider.ExpandRoutes keeps a tag-scoped route unexpanded and expands
           the rest): a server route's ids come from the server list, not the tag tables (#5750 parity ruling). */
        var scoped = await _store.InsertAsync(Route("High CPU", TeamsUrl, tag: 1));
        var serverScoped = await _store.InsertAsync(Route("High CPU", TeamsUrl, server: 7101));
        var fleet = await _store.InsertAsync(Route("High CPU", slack: SlackUrl));
        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();
            await ExecAsync(conn, "DROP TABLE server_tag_map");
        }

        var inputs = await _store.GatherScopeInputsAsync();
        var list = await _store.ListAsync();

        Assert.False(inputs.TagsReadable);
        Assert.False(list.Single(r => r.RouteId == scoped).CanMatch);
        Assert.True(list.Single(r => r.RouteId == scoped).ScopeUnexpanded);
        Assert.True(list.Single(r => r.RouteId == serverScoped).CanMatch);
        Assert.Equal(new[] { 7101 }, list.Single(r => r.RouteId == serverScoped).ExpandedServerIds!.ToArray());
        Assert.True(list.Single(r => r.RouteId == fleet).CanMatch);
    }

    [Fact]
    public async Task GatherScopeInputs_ReturnsTheKnownServers_TheTagParents_AndTheServerTags()
    {
        var tags = new LocalDataService(_initializer);
        var (root, child) = await SeedTagsAsync(tags);

        var inputs = await _store.GatherScopeInputsAsync();

        Assert.Equal(new[] { 7101, 7102, 7103 }, inputs.KnownServerIds.OrderBy(i => i).ToArray());
        Assert.Null(inputs.TagParents[root]);
        Assert.Equal(root, inputs.TagParents[child]);
        Assert.Equal(new[] { child }, inputs.ServerTags[7101].ToArray());
        Assert.Equal(new[] { root }, inputs.ServerTags[7102].ToArray());
        Assert.False(inputs.ServerTags.ContainsKey(7103));
    }

    // ---- the cache ------------------------------------------------------------------------------

    private void ConfigureCache() =>
        NotificationRouteCache.Configure(() => Task.Run(() => _store.ListAsync()).GetAwaiter().GetResult());

    [Fact]
    public async Task TheCache_ReloadsAfterATagEdit_AndAfterARouteWrite()
    {
        var tags = new LocalDataService(_initializer);
        var root = await tags.CreateServerTagAsync("Prod", null);
        await tags.AssignServerTagAsync(new[] { 7101 }, root);
        var id = await _store.InsertAsync(Route("*", TeamsUrl, tag: root));
        ConfigureCache();

        Assert.Equal(new[] { 7101 }, NotificationRouteCache.Current.Single().ExpandedServerIds!.ToArray());

        await tags.AssignServerTagAsync(new[] { 7102 }, root);
        Assert.Equal(new[] { 7101, 7102 }, NotificationRouteCache.Current.Single().ExpandedServerIds!.OrderBy(i => i).ToArray());

        await tags.UnassignServerTagAsync(new[] { 7101 }, root);
        Assert.Equal(new[] { 7102 }, NotificationRouteCache.Current.Single().ExpandedServerIds!.ToArray());

        var second = await tags.CreateServerTagAsync("Dev", null);
        await tags.ReparentServerTagAsync(root, second);
        Assert.Equal(1, NotificationRouteCache.Current.Single().ScopeTagDepth);

        await tags.DeleteServerTagAsync(root);
        Assert.True(NotificationRouteCache.Current.Single().Orphaned);

        await _store.SetEnabledAsync(id, false);
        Assert.False(NotificationRouteCache.Current.Single().Enabled);

        await _store.DeleteAsync(id);
        Assert.Empty(NotificationRouteCache.Current);
    }

    [Fact]
    public async Task TheCache_ReloadsAfterAServerAdd_AndAServerSettingsEdit()
    {
        /* The loader names the server state it SAW in the route it returns, and the test asserts on that, never on how many
           times the loader ran. The cache is a process-wide static that every ServerManager and tag writer invalidates, and
           test classes outside this serialized collection write servers in parallel: a count of loads includes their
           invalidations (CI saw "Expected: 3, Actual: 4"). A foreign invalidation only makes the next read load the same state again. */
        const int BeforeAdd = 1, AfterAdd = 2, AfterEdit = 3;
        var configDir = Path.Combine(_tempDir, "cfg");
        Directory.CreateDirectory(configDir);
        var manager = new ServerManager(configDir);
        var server = new PerformanceMonitorLite.Models.ServerConnection { ServerName = "R3-SRV", DisplayName = "R3" };
        NotificationRouteCache.Configure(() =>
        {
            var seen = manager.GetAllServers().FirstOrDefault(s => s.Id == server.Id);
            var state = seen is null ? BeforeAdd : seen.DisplayName == "R3 edited" ? AfterEdit : AfterAdd;
            return new[] { new NotificationRoute(state, "High CPU", "https://x.test/h", "", "", "", "", true) };
        });
        Assert.Equal(BeforeAdd, NotificationRouteCache.Current.Single().RouteId);

        manager.AddServer(server);
        Assert.Equal(AfterAdd, NotificationRouteCache.Current.Single().RouteId);
        Assert.Contains(
            RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server)),
            manager.GetKnownServerIds());

        server.DisplayName = "R3 edited";
        manager.UpdateServerSettings(server);
        Assert.Equal(AfterEdit, NotificationRouteCache.Current.Single().RouteId);

        await Task.CompletedTask;
    }

    [Fact]
    public void TheCache_KeepsTheRoutesInForce_WhenAReloadFails()
    {
        var good = new[] { new NotificationRoute(1, "High CPU", "https://x.test/h", "", "", "", "", true) };
        var fail = false;
        NotificationRouteCache.Configure(() => fail ? throw new InvalidOperationException("boom") : good);
        Assert.Single(NotificationRouteCache.Current);

        fail = true;
        NotificationRouteCache.Invalidate();

        Assert.Single(NotificationRouteCache.Current);
    }

    // ---- Resolve through AppAlertSettings -----------------------------------------------------------

    [Fact]
    public async Task AScopedRoute_SendsItsServersAlert_ToTheRouteUrl_AndAnotherServersAlert_ToTheDefault()
    {
        const string defaultTeams = "https://teams.default.r3.test/hook";
        var savedEnabled = App.TeamsWebhookEnabled;
        var savedUrl = App.TeamsWebhookUrl;
        try
        {
            App.TeamsWebhookEnabled = true;
            App.TeamsWebhookUrl = defaultTeams;
            await _store.InsertAsync(Route("High CPU", TeamsUrl, server: 7101));
            ConfigureCache();
            IAlertSettings settings = new AppAlertSettings();

            var scoped = NotificationRouter.Resolve("High CPU", settings.NotificationRoutes, settings, 7101);
            var other = NotificationRouter.Resolve("High CPU", settings.NotificationRoutes, settings, 7102);
            var noServer = NotificationRouter.Resolve("High CPU", settings.NotificationRoutes, settings);

            Assert.Equal(TeamsUrl, scoped.Teams.Destination);
            Assert.Equal(RouteScopeTier.Server, scoped.Teams.Tier);
            Assert.Equal(defaultTeams, other.Teams.Destination);
            Assert.Null(other.Teams.RouteId);
            Assert.Equal(defaultTeams, noServer.Teams.Destination);
        }
        finally
        {
            App.TeamsWebhookEnabled = savedEnabled;
            App.TeamsWebhookUrl = savedUrl;
        }
    }

    [Fact]
    public void WithNoLoaderInstalled_TheAdapterServesNoRoutes()
    {
        NotificationRouteCache.Configure(null);

        Assert.Empty(new AppAlertSettings().NotificationRoutes);
    }

    private async Task<long> ScalarLongAsync(string sql)
    {
        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        return Convert.ToInt64(await ScalarAsync(conn, sql));
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
