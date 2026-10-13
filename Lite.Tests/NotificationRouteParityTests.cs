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
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's half of the scoped-route parity pin (#5750): <see cref="NotificationRouteParityFixture"/>'s fleet is built in a
/// DuckDB store, read back through <see cref="DuckDbNotificationRouteStore.GatherScopeInputsAsync"/> and expanded, and the
/// result must equal the fixture's one expected table. Darling.Tests asserts the same table against PostgreSQL
/// (<c>NotificationRouteScopeParityLiveTests</c>), so the two loaders expand a scope to the same servers. Lite's server id is
/// the hash of the name (the id its alert path and <c>server_tag_map</c> use), so ids are mapped back to names through the
/// name-to-hash table below before they are compared.
/// </summary>
[Collection("app-alert-statics")]
public sealed class NotificationRouteParityTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly FakeRouteSecrets _secrets = new();
    private readonly HashSet<string> _registry = new(NotificationRouteParityFixture.Servers, StringComparer.Ordinal);

    public NotificationRouteParityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteParityTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _initializer.InitializeAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>Lite's server id for a name: the hash the alert path and the tag map key on.</summary>
    private static int IdOf(string serverName) => RemoteCollectorService.GetDeterministicHashCode(serverName);

    private static NotificationRoute RouteFor(NotificationRouteParityFixture.Case c, IReadOnlyDictionary<string, int> tagIds) =>
        new(0, "High CPU", "https://teams.parity.test/hook", "", "", "", "", true)
        {
            ScopeServerId = c.Kind == NotificationRouteParityFixture.Kind.Server ? IdOf(c.Target!) : null,
            ScopeTagId = c.Kind == NotificationRouteParityFixture.Kind.Tag ? tagIds[c.Target!] : null
        };

    [Fact]
    public async Task LitesLoader_ExpandsTheSharedFleet_ToTheSharedExpectedTable()
    {
        var fixture = NotificationRouteParityFixture.Cases;
        var store = new DuckDbNotificationRouteStore(
            _initializer,
            () => _registry.Select(IdOf).ToArray(),
            _secrets.Access);
        var tags = new LocalDataService(_initializer);

        /* The tag tree, in creation order, so a parent exists before its child. */
        var tagIds = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var (name, parent) in NotificationRouteParityFixture.Tags)
        {
            tagIds[name] = await tags.CreateServerTagAsync(name, parent is null ? null : tagIds[parent]);
        }

        foreach (var (server, tag) in NotificationRouteParityFixture.Assignments)
        {
            await tags.AssignServerTagAsync(new[] { IdOf(server) }, tagIds[tag]);
        }

        /* Store one route per case, then remove the server and the tag a case names so they are orphans. */
        var caseByRouteId = new Dictionary<int, NotificationRouteParityFixture.Case>();
        foreach (var c in fixture)
        {
            caseByRouteId[await store.InsertAsync(RouteFor(c, tagIds))] = c;
        }

        _registry.Remove(NotificationRouteParityFixture.RemovedServer);
        await tags.DeleteServerTagAsync(tagIds[NotificationRouteParityFixture.DeletedTag]);

        /* Hash ids back to names: the one place Lite's ids and Darling's meet. */
        var nameById = NotificationRouteParityFixture.Servers.ToDictionary(IdOf, n => n);
        var expected = NotificationRouteParityFixture.ExpectedLines();

        /* (1) The gather method, then the shared expander on routes built from the fixture alone. */
        var inputs = await store.GatherScopeInputsAsync();
        var bare = fixture.Select(c => RouteFor(c, tagIds)).ToList();
        var viaGather = NotificationRouteScopeExpander.Expand(bare, inputs.KnownServerIds, inputs.TagParents, inputs.ServerTags);
        Assert.Equal(expected, fixture.Select((c, i) => NotificationRouteParityFixture.Describe(
            c.Name, viaGather[i].ExpandedServerIds, viaGather[i].ScopeTagDepth, viaGather[i].Orphaned, nameById)).ToList());

        /* (2) The store's own read, which is what the alert path gets. */
        var listed = (await store.ListAsync()).ToDictionary(r => r.RouteId);
        Assert.Equal(expected, caseByRouteId.OrderBy(p => p.Key).Select(p => NotificationRouteParityFixture.Describe(
            p.Value.Name, listed[p.Key].ExpandedServerIds, listed[p.Key].ScopeTagDepth, listed[p.Key].Orphaned, nameById)).ToList());
    }

    [Fact]
    public void TheSharedFixture_IsWellFormed_ThreeDeepTreeATwoTagServerAnEmptyTagAndAnOrphanOfEachKind()
    {
        var tags = NotificationRouteParityFixture.Tags;
        string? Parent(string name) => tags.Single(t => t.Name == name).Parent;
        Assert.Equal("east", Parent("rack1"));
        Assert.Equal("prod", Parent("east"));
        Assert.Null(Parent("prod"));
        Assert.Equal(2, NotificationRouteParityFixture.Assignments.Count(a => a.Server == "delta"));
        Assert.DoesNotContain(NotificationRouteParityFixture.Assignments, a => a.Tag == "spare");
        Assert.Contains(NotificationRouteParityFixture.Cases, c => c.ExpectedOrphaned && c.Kind == NotificationRouteParityFixture.Kind.Server);
        Assert.Contains(NotificationRouteParityFixture.Cases, c => c.ExpectedOrphaned && c.Kind == NotificationRouteParityFixture.Kind.Tag);
        Assert.Equal(new[] { 0, 1, 2 }, NotificationRouteParityFixture.Cases
            .Where(c => c.Target is "prod" or "east" or "rack1").Select(c => c.ExpectedDepth).ToArray());
    }
}
