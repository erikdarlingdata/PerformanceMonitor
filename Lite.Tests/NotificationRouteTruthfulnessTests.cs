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
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5750: what Lite says about routes that cannot work. (a) A saved destination that Credential Manager cannot give back is
/// logged once per route and channel, never with the destination. (b) A scoped route the loader could not expand because the
/// tag read failed is "not active", not "OK". In <c>app-logger-statics</c> because the log lines are read from the shared buffer.
/// </summary>
[Collection("app-logger-statics")]
public sealed class NotificationRouteTruthfulnessTests : IDisposable
{
    private const string TeamsUrl = "https://teams.r7-secret.test/hook/abc123";
    private const string PagerKey = "R7PAGERDUTYROUTINGKEY0123456789ab";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _initializer;
    private readonly FakeRouteSecrets _secrets = new();
    private readonly DuckDbNotificationRouteStore _store;

    public NotificationRouteTruthfulnessTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteTruthfulnessTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "lite.duckdb");
        _initializer = new DuckDbInitializer(_dbPath);
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbNotificationRouteStore(
            _initializer,
            () => new[] { 7101, 7102 },
            _secrets.Access);
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static NotificationRoute Route(string match, string teams = "", string pager = "", int? server = null, int? tag = null) =>
        new(0, match, teams, "", "", pager, "", true) { ScopeServerId = server, ScopeTagId = tag };

    // ---- (a) an unreadable saved secret is logged once ------------------------------------------

    [Fact]
    public async Task ASavedSecretCredentialManagerCannotRead_IsLoggedOncePerRouteAndChannel_NeverWithTheUrl()
    {
        var id = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl, pager: PagerKey));
        _secrets.Lose(id, NotificationRouter.TeamsChannel);
        _secrets.Lose(id, NotificationRouter.PagerDutyChannel);

        AppLogger.DrainBufferedLines();
        await _store.ListAsync();
        await _store.ListAsync();
        await _store.GetAsync(id);
        /* #6113: this test loses Teams and PagerDuty only. A route-store test in another collection that loses the generic channel
           logs the same sentence for its own route 1, so the count takes the two channels' lines and no other channel's. */
        var lines = AppLogger.DrainBufferedLines().Where(l => l.Contains("[NotificationRoutes]", StringComparison.Ordinal)
            && l.Contains("could not be read from Credential Manager", StringComparison.Ordinal)
            && (l.Contains($" {NotificationRouter.TeamsChannel} destination", StringComparison.Ordinal)
                || l.Contains($" {NotificationRouter.PagerDutyChannel} destination", StringComparison.Ordinal))).ToList();

        /* Two channels, three reloads: two warnings, one each. */
        Assert.Equal(2, lines.Count);
        Assert.Single(lines, l => l.Contains($"route {id} ", StringComparison.Ordinal) && l.Contains(NotificationRouter.TeamsChannel, StringComparison.Ordinal));
        Assert.Single(lines, l => l.Contains($"route {id} ", StringComparison.Ordinal) && l.Contains(NotificationRouter.PagerDutyChannel, StringComparison.Ordinal));
        Assert.All(lines, l => Assert.Contains("[WARN ]", l, StringComparison.Ordinal));
        Assert.All(lines, l => Assert.DoesNotContain("r7-secret", l, StringComparison.Ordinal));
        Assert.All(lines, l => Assert.DoesNotContain(PagerKey, l, StringComparison.Ordinal));

        /* The channel is still off for the route, as before. */
        var route = (await _store.GetAsync(id))!;
        Assert.True(route.IsOffFor(NotificationRouter.TeamsChannel));
    }

    [Fact]
    public async Task ACredentialThatCannotBeRead_TurnsThatChannelOff_InsteadOfInheriting()
    {
        const string slackUrl = "https://hooks.slack.r7-secret.test/T0/B0/xyz";
        var id = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl, server: 7101) with { SlackUrl = slackUrl });
        _secrets.Lose(id, NotificationRouter.TeamsChannel);

        var route = (await _store.GetAsync(id))!;

        Assert.Equal("", route.TeamsUrl);
        Assert.True(route.IsOffFor(NotificationRouter.TeamsChannel));
        Assert.False(route.IsOffFor(NotificationRouter.SlackChannel));
        Assert.Equal(slackUrl, route.SlackUrl);
    }

    [Fact]
    public async Task AReadableSecret_LogsNothing()
    {
        await _store.InsertAsync(Route("High CPU", teams: TeamsUrl));

        AppLogger.DrainBufferedLines();
        await _store.ListAsync();

        Assert.DoesNotContain(AppLogger.DrainBufferedLines(), l => l.Contains("[NotificationRoutes]", StringComparison.Ordinal) && l.Contains("could not be read from Credential Manager", StringComparison.Ordinal));
    }

    // ---- (b) a failed tag read is "not active", not "OK" ---------------------------------------

    [Fact]
    public async Task WhenTheTagReadFails_TheTagRouteIsNotActive_NotOk_AndTheServerAndFleetWideRoutesAre()
    {
        var tagRoute = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl, tag: 1));
        var serverRoute = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl, server: 7101));
        var fleet = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl));
        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            using var drop = conn.CreateCommand();
            drop.CommandText = "DROP TABLE server_tag_map";
            await drop.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var list = await _store.ListAsync();
        var scoped = list.Single(r => r.RouteId == tagRoute);

        Assert.True(scoped.ScopeUnexpanded);
        Assert.False(scoped.CanMatch);
        Assert.False(scoped.Orphaned);
        /* The Darling viewer's texts, word for word (ViewerNotificationRouteScope). */
        Assert.Equal("Not active: tag data unreadable", NotificationRouteUi.StatusText(scoped));
        /* Darling's loader still expands a server-scoped route when only the tag read fails (its ids come from the server
           list), so Lite does too: the route is active, not "scope data unreadable". */
        var byServer = list.Single(r => r.RouteId == serverRoute);
        Assert.False(byServer.ScopeUnexpanded);
        Assert.True(byServer.CanMatch);
        Assert.Equal("OK", NotificationRouteUi.StatusText(byServer));
        /* The text for a server-scoped route that is unexpanded for any other reason stays the Darling viewer's, word for word. */
        Assert.Equal("Not active: scope data unreadable", NotificationRouteUi.StatusText(byServer with { ExpandedServerIds = null }));

        var fleetRoute = list.Single(r => r.RouteId == fleet);
        Assert.False(fleetRoute.ScopeUnexpanded);
        Assert.Equal("OK", NotificationRouteUi.StatusText(fleetRoute));
    }

    [Fact]
    public async Task WhenTheTagsReadFine_NoRouteIsMarkedNotActive_AnOrphanKeepsItsOwnStatus()
    {
        var tags = new LocalDataService(_initializer);
        var tag = await tags.CreateServerTagAsync("Prod", null);
        var healthy = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl, tag: tag));
        var ghostTag = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl, tag: tag + 500));
        var ghostServer = await _store.InsertAsync(Route("High CPU", teams: TeamsUrl, server: 9999));

        var list = await _store.ListAsync();

        Assert.All(list, r => Assert.False(r.ScopeUnexpanded));
        Assert.Equal("OK", NotificationRouteUi.StatusText(list.Single(r => r.RouteId == healthy)));
        Assert.Equal(NotificationRouteUi.OrphanedTagStatus, NotificationRouteUi.StatusText(list.Single(r => r.RouteId == ghostTag)));
        Assert.Equal(NotificationRouteUi.OrphanedServerStatus, NotificationRouteUi.StatusText(list.Single(r => r.RouteId == ghostServer)));
    }

    [Fact]
    public void ARouteNeverExpanded_IsNotActive_AFleetWideOrAlreadyOrphanedOneIsNot()
    {
        Assert.True(Route("High CPU", teams: TeamsUrl, server: 7101).ScopeUnexpanded);
        Assert.True(Route("High CPU", teams: TeamsUrl, tag: 3).ScopeUnexpanded);
        Assert.False(Route("High CPU", teams: TeamsUrl).ScopeUnexpanded);
        Assert.False((Route("High CPU", teams: TeamsUrl, tag: 3) with { Orphaned = true }).ScopeUnexpanded);
        Assert.False((Route("High CPU", teams: TeamsUrl, tag: 3) with { ExpandedServerIds = new HashSet<int>() }).ScopeUnexpanded);
    }
}
