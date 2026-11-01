/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch database through ScratchPostgres and never touch another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that scopes a notification route to a server or a tag (V181, #5750): its place in the ladder, the shape of its
/// script (two nullable columns, two guarded CHECKs, the reload triggers on the two tag tables, no foreign key, no GRANT), the
/// probe's sentinel, and on a live store: both scopes refused, a fleet-wide <c>*</c> refused, a scoped <c>*</c> accepted, a stored row
/// untouched, the loader's expansion of a tag subtree, an orphan kept as written, a tag edit reloading, a failed tag read closing, and
/// the MCP read's scope fields.
/// </summary>
public sealed class NotificationRouteScopeRungTests
{
    private const int RungVersion = 181;
    private const string Table = "config_notification_routes";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegisteredOnce_BelowTheTopOfTheLadder()
    {
        Assert.Equal("notification-route-scope", Rung.Name);
        Assert.Single(PgMigrations.Scripts, m => m.Version == RungVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        /* V182 (#5506) took the top of the ladder; this rung stays registered below it. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(PgMigrations.Scripts.Select(s => s.Version).Distinct().OrderBy(v => v), PgMigrations.Scripts.Select(s => s.Version));
    }

    [Fact]
    public void TheScript_AddsTwoNullableColumnsAndTwoChecks_WithNoForeignKeyNoCascadeNoGrant_AndTheTagReloadTriggers()
    {
        var sql = Rung.Sql;
        Assert.Contains("ADD COLUMN IF NOT EXISTS scope_server_id integer,", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS scope_tag_id integer;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("integer NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("CHECK (scope_server_id IS NULL OR scope_tag_id IS NULL)", sql, StringComparison.Ordinal);
        Assert.Contains("CHECK (btrim(metric_match) <> '*' OR scope_server_id IS NOT NULL OR scope_tag_id IS NOT NULL) NOT VALID", sql, StringComparison.Ordinal);

        /* Ruling 10: an orphan stays as written. A reference would delete it (CASCADE) or widen it to fleet-wide (SET NULL). */
        Assert.DoesNotContain("REFERENCES", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ON DELETE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("GRANT", sql, StringComparison.Ordinal);

        /* V32 gave the tag tables no reload beacon, and a scoped route's server set depends on them. */
        foreach (var tagTable in new[] { "server_tags", "server_tag_map" })
        {
            Assert.Contains($"AFTER INSERT OR UPDATE OR DELETE ON config.{tagTable}", sql, StringComparison.Ordinal);
        }

        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "EXECUTE FUNCTION config.config_bump_version\\(\\)").Count);

        /* The V131 table's own CHECK and trigger are not edited by this rung. */
        Assert.DoesNotContain("CREATE TABLE", sql, StringComparison.Ordinal);
    }

    /// <summary>The viewer and mcp roles read the two scope columns: they are on the non-secret list (an upgraded managed store gets the
    /// grant when provisioning re-runs after migration), and the MCP read selects them.</summary>
    [Fact]
    public void TheScopeColumns_AreOnTheNonSecretCarve_AndTheMcpSummaryReadsThem()
    {
        var acl = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(a => a.Table == Table);
        Assert.Contains("scope_server_id", acl.NonSecretColumns);
        Assert.Contains("scope_tag_id", acl.NonSecretColumns);
        Assert.DoesNotContain("scope_server_id", acl.SecretColumns);
        Assert.DoesNotContain("scope_tag_id", acl.SecretColumns);
        Assert.Contains("scope_server_id, scope_tag_id", PgNotificationRouteStore.SelectSummariesSql, StringComparison.Ordinal);
    }

    /// <summary>A failed tag read must not widen a route: the tag route stays unexpanded (matches nothing), a server route still expands.
    /// Pure, so it runs everywhere; the live twin below drives the same method against a store with an unreadable tag table.</summary>
    [Fact]
    public void ExpandRoutes_WhenTheTagReadFailed_LeavesATagRouteUnexpanded_AndStillExpandsAServerRoute()
    {
        var routes = new List<NotificationRoute>
        {
            new(1, "*", "", "https://hooks.example.invalid/t", "", "", "", true) { ScopeTagId = 7 },
            new(2, "*", "", "https://hooks.example.invalid/s", "", "", "", true) { ScopeServerId = 3 },
            new(3, "Deadlocks Detected", "", "https://hooks.example.invalid/f", "", "", "", true),
        };
        var failed = new StoreConfigProvider.RouteScopeInputs(
            new HashSet<int> { 3, 4 }, new Dictionary<int, int?>(), new Dictionary<int, IReadOnlyCollection<int>>(), TagsReadable: false);

        var expanded = StoreConfigProvider.ExpandRoutes(routes, failed);

        Assert.Same(routes[0], expanded[0]);
        Assert.False(expanded[0].Orphaned);
        Assert.False(expanded[0].CanMatch);
        Assert.Equal(new[] { 3 }, expanded[1].ExpandedServerIds!.OrderBy(i => i));
        Assert.True(expanded[1].AppliesTo(3));
        Assert.Same(routes[2], expanded[2]);

        var settings = new DarlingAlertSettings(new DarlingConfig());
        var decision = NotificationRouter.Resolve("Deadlocks Detected", expanded, settings, 4);
        Assert.NotEqual("https://hooks.example.invalid/t", decision.Slack.Destination);
        Assert.NotEqual("https://hooks.example.invalid/s", decision.Slack.Destination);

        /* The same inputs with the tags readable: the tag exists nowhere, so the route is orphaned and still matches nothing. */
        var readable = failed with { TagsReadable = true };
        Assert.True(StoreConfigProvider.ExpandRoutes(routes, readable)[0].Orphaned);
    }

    [Fact]
    public async Task OnALiveStore_TheChecksHold_ALegacyRowIsUntouched_AndTheTagTablesBumpTheBeacon()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V181 rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);

        var bodySucceeded = false;
        try
        {
            /* A store one rung behind: V181's columns gone, a legacy fleet-wide row and a legacy unscoped '*' row stored. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 181", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url) VALUES ('Deadlocks Detected', 'sealed-a'), ('*', 'sealed-b')", ct);
            var before = await ScalarAsync(connection,
                "SELECT string_agg(route_id || '|' || metric_match || '|' || slack_url || '|' || enabled, ';' ORDER BY route_id) FROM config.config_notification_routes", ct);

            Assert.Equal(180, await ProbedVersionAsync(connection, ct));
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            /* The stored rows are exactly as they were; both scopes read NULL (fleet-wide). A legacy '*' survives the migration (NOT VALID). */
            Assert.Equal(before, await ScalarAsync(connection,
                "SELECT string_agg(route_id || '|' || metric_match || '|' || slack_url || '|' || enabled, ';' ORDER BY route_id) FROM config.config_notification_routes", ct));
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM config.config_notification_routes WHERE scope_server_id IS NOT NULL OR scope_tag_id IS NOT NULL", ct));
            await ExecAsync(connection, "DELETE FROM config.config_notification_routes", ct);

            /* Both scopes: refused. Fleet-wide '*' (also with padding): refused. Scoped '*': accepted, either scope. */
            var both = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id, scope_tag_id) VALUES ('Deadlocks Detected', 'x', 1, 1)", ct));
            Assert.Equal("23514", both.SqlState);
            var wildcard = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url) VALUES ('*', 'x')", ct));
            Assert.Equal("23514", wildcard.SqlState);
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url) VALUES (' * ', 'x')", ct));
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id) VALUES ('*', 'x', 11)", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_tag_id) VALUES ('*', 'y', 12)", ct);
            Assert.Equal(2L, await ScalarAsync(connection, "SELECT count(*) FROM config.config_notification_routes WHERE metric_match = '*'", ct));

            /* An UPDATE that would strip the last scope off a '*' route is refused too. */
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "UPDATE config.config_notification_routes SET scope_server_id = NULL WHERE scope_server_id = 11", ct));

            /* No reference: a scope may name a server or tag that does not exist (ruling 10), and removing it later cannot touch the row. */
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config.config_notification_routes'::regclass AND contype = 'f'", ct));

            /* The rung applied again changes nothing. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 181", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(2L, await ScalarAsync(connection, "SELECT count(*) FROM config.config_notification_routes", ct));

            /* The reload beacon: every kind of write to either tag table bumps it (V32 gave them none). */
            foreach (var statement in new[]
            {
                "INSERT INTO config.server_tags (name) VALUES ('beacon-a')",
                "UPDATE config.server_tags SET name = 'beacon-b' WHERE name = 'beacon-a'",
                "INSERT INTO config.server_tag_map (server_id, tag_id) SELECT 9, id FROM config.server_tags WHERE name = 'beacon-b'",
                "DELETE FROM config.server_tag_map WHERE server_id = 9",
                "DELETE FROM config.server_tags WHERE name = 'beacon-b'",
            })
            {
                var beacon = await BeaconAsync(connection, ct);
                await ExecAsync(connection, statement, ct);
                Assert.True(await BeaconAsync(connection, ct) > beacon, "config_version must bump on: " + statement);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheLoader_ExpandsATagSubtree_KeepsAnOrphan_ReloadsOnATagEdit_AndFailsClosedOnAnUnreadableTagTable_AndTheMcpReadShowsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the scoped-route loader's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
        await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* Servers 1..4. Tags: West (root) > Prod > Edge; East (root). Server 1 on Prod, 2 on Edge, 3 on East, 4 on none. */
            await ExecAsync(connection,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (1, 'sql-a', 'sql-a', true), (2, 'sql-b', 'sql-b', true), (3, 'sql-c', 'sql-c', true), (4, 'sql-d', 'sql-d', true)", ct);
            var west = await TagAsync(connection, "West", null, ct);
            var prod = await TagAsync(connection, "Prod", west, ct);
            var edge = await TagAsync(connection, "Edge", prod, ct);
            var east = await TagAsync(connection, "East", null, ct);
            await ExecAsync(connection, FormattableString.Invariant(
                $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (1, {prod}), (2, {edge}), (3, {east})"), ct);

            const string Url = "https://hooks.example.invalid/";
            await ExecAsync(connection, FormattableString.Invariant($@"
INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_tag_id) VALUES ('*', '{Url}west', {west});
INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id) VALUES ('*', '{Url}d', 4);
INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id) VALUES ('*', '{Url}ghost-server', 99);
INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_tag_id) VALUES ('*', '{Url}ghost-tag', 9999);
INSERT INTO config.config_notification_routes (metric_match, slack_url) VALUES ('Deadlocks Detected', '{Url}fleet');"), ct);

            /* The gathering method, on its own: the three inputs a parity pin compares with Lite's. */
            var inputs = await StoreConfigProvider.ReadRouteScopeInputsAsync(connection, NullLogger.Instance, ct);
            Assert.True(inputs.TagsReadable);
            Assert.Equal(new[] { 1, 2, 3, 4 }, inputs.KnownServerIds.OrderBy(i => i));
            Assert.Equal(4, inputs.TagParents.Count);
            Assert.Equal((int?)west, inputs.TagParents[prod]);
            Assert.Null(inputs.TagParents[west]);
            Assert.Equal(new[] { edge }, inputs.ServerTags[2]);
            Assert.False(inputs.ServerTags.ContainsKey(4));

            /* The service's read: the subtree West covers servers 1 and 2; an orphan stays as written and matches nothing. */
            var provider = new StoreConfigProvider(dataSource);
            var routes = (await provider.LoadViewAsync(new DarlingConfig(), ct))!.NotificationRoutes;
            Assert.Equal(5, routes.Count);
            var westRoute = routes.Single(r => r.ScopeTagId == west);
            Assert.Equal(new[] { 1, 2 }, westRoute.ExpandedServerIds!.OrderBy(i => i));
            Assert.Equal(0, westRoute.ScopeTagDepth);
            Assert.Equal(new[] { 4 }, routes.Single(r => r.ScopeServerId == 4).ExpandedServerIds!.OrderBy(i => i));
            Assert.True(routes.Single(r => r.ScopeServerId == 99).Orphaned);
            Assert.True(routes.Single(r => r.ScopeTagId == 9999).Orphaned);
            Assert.All(routes.Where(r => r.Orphaned), r => Assert.False(r.CanMatch && r.AppliesTo(99)));
            Assert.Null(routes.Single(r => r.ScopeTagId is null && r.ScopeServerId is null).ExpandedServerIds);

            var settings = new DarlingAlertSettings(new DarlingConfig());
            Assert.Equal(Url + "west", NotificationRouter.Resolve("Deadlocks Detected", routes, settings, 2).Slack.Destination);
            Assert.Equal(Url + "d", NotificationRouter.Resolve("Server Unreachable", routes, settings, 4).Slack.Destination);
            Assert.Equal(Url + "fleet", NotificationRouter.Resolve("Deadlocks Detected", routes, settings, 3).Slack.Destination);

            /* A tag edit reloads: the beacon moves, and the next load puts server 3 under West (the server set changed, no route row did). */
            var beacon = await BeaconAsync(connection, ct);
            await ExecAsync(connection, FormattableString.Invariant($"UPDATE config.server_tags SET parent_id = {west} WHERE id = {east}"), ct);
            Assert.True(await BeaconAsync(connection, ct) > beacon, "a tag edit must bump the beacon the route cache watches");
            routes = (await provider.LoadViewAsync(new DarlingConfig(), ct))!.NotificationRoutes;
            Assert.Equal(new[] { 1, 2, 3 }, routes.Single(r => r.ScopeTagId == west).ExpandedServerIds!.OrderBy(i => i));
            var settingsAfter = new DarlingAlertSettings(Configured(routes));
            Assert.Equal(Url + "west", NotificationRouter.Resolve("Deadlocks Detected", settingsAfter.NotificationRoutes, settings, 3).Slack.Destination);

            /* The MCP read: scope ids, names, the expanded count and orphaned; no destination. */
            using (var json = JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(dataSource, ct)))
            {
                var rows = json.RootElement.GetProperty("routes").EnumerateArray().ToList();
                Assert.Equal(5, rows.Count);
                var tagRow = rows.Single(r => r.GetProperty("scope_tag_id").ValueKind == JsonValueKind.Number && r.GetProperty("scope_tag_id").GetInt32() == west);
                Assert.Equal("West", tagRow.GetProperty("scope_tag_name").GetString());
                Assert.Equal(3, tagRow.GetProperty("expanded_server_count").GetInt32());
                Assert.False(tagRow.GetProperty("orphaned").GetBoolean());
                Assert.False(tagRow.GetProperty("scope_unreadable").GetBoolean());
                var serverRow = rows.Single(r => r.GetProperty("scope_server_id").ValueKind == JsonValueKind.Number && r.GetProperty("scope_server_id").GetInt32() == 4);
                Assert.Equal("sql-d", serverRow.GetProperty("scope_server_name").GetString());
                Assert.Equal(1, serverRow.GetProperty("expanded_server_count").GetInt32());
                var ghost = rows.Single(r => r.GetProperty("scope_server_id").ValueKind == JsonValueKind.Number && r.GetProperty("scope_server_id").GetInt32() == 99);
                Assert.True(ghost.GetProperty("orphaned").GetBoolean());
                Assert.Equal(JsonValueKind.Null, ghost.GetProperty("scope_server_name").ValueKind);
                Assert.Equal(0, ghost.GetProperty("expanded_server_count").GetInt32());
                var fleet = rows.Single(r => r.GetProperty("scope_server_id").ValueKind == JsonValueKind.Null && r.GetProperty("scope_tag_id").ValueKind == JsonValueKind.Null);
                Assert.Equal(JsonValueKind.Null, fleet.GetProperty("expanded_server_count").ValueKind);
                Assert.False(fleet.GetProperty("orphaned").GetBoolean());
                Assert.DoesNotContain("hooks.example.invalid", json.RootElement.GetRawText(), StringComparison.Ordinal);

                /* The wildcard route reads wildcard with no family (it used to read exact_metric / performance). */
                Assert.Equal("wildcard", tagRow.GetProperty("match_kind").GetString());
                Assert.Equal(JsonValueKind.Null, tagRow.GetProperty("family").ValueKind);
            }

            /* set_notification_route_enabled returns the same projection, scope included (#5750 review): the tag route, as stored. */
            var westRouteId = Convert.ToInt32(await ScalarAsync(connection, FormattableString.Invariant($"SELECT route_id FROM config.config_notification_routes WHERE scope_tag_id = {west}"), ct), CultureInfo.InvariantCulture);
            using (var set = JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteEnabled(dataSource, westRouteId, false)))
            {
                Assert.Equal("updated", set.RootElement.GetProperty("status").GetString());
                var route = set.RootElement.GetProperty("route");
                Assert.False(route.GetProperty("enabled").GetBoolean());
                Assert.Equal(west, route.GetProperty("scope_tag_id").GetInt32());
                Assert.Equal("West", route.GetProperty("scope_tag_name").GetString());
                Assert.Equal(3, route.GetProperty("expanded_server_count").GetInt32());
                Assert.False(route.GetProperty("orphaned").GetBoolean());
                Assert.False(route.GetProperty("scope_unreadable").GetBoolean());
                Assert.Equal("wildcard", route.GetProperty("match_kind").GetString());
            }

            await ExecAsync(connection, FormattableString.Invariant($"UPDATE config.config_notification_routes SET enabled = true WHERE route_id = {westRouteId}"), ct);

            /* A deleted tag and a deleted server leave the route rows, now orphaned. The cascade from the tag delete takes its subtree. */
            await ExecAsync(connection, FormattableString.Invariant($"DELETE FROM config.server_tags WHERE id = {west}"), ct);
            await ExecAsync(connection, "DELETE FROM config.config_monitored_servers WHERE server_id = 4", ct);
            Assert.Equal(5L, await ScalarAsync(connection, "SELECT count(*) FROM config.config_notification_routes", ct));
            routes = (await provider.LoadViewAsync(new DarlingConfig(), ct))!.NotificationRoutes;
            Assert.True(routes.Single(r => r.ScopeTagId == west).Orphaned);
            Assert.True(routes.Single(r => r.ScopeServerId == 4).Orphaned);
            Assert.Equal(Url + "fleet", NotificationRouter.Resolve("Deadlocks Detected", routes, settings, 2).Slack.Destination);

            /* Fail closed: with the tag table unreadable the tag route matches nothing (and is not marked orphaned); the server route still works. */
            await ExecAsync(connection, "INSERT INTO config.server_tags (name) VALUES ('Again')", ct);
            var again = Convert.ToInt32(await ScalarAsync(connection, "SELECT id FROM config.server_tags WHERE name = 'Again'", ct), CultureInfo.InvariantCulture);
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (5, 'sql-e', 'sql-e', true)", ct);
            await ExecAsync(connection, FormattableString.Invariant($@"
INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_tag_id) VALUES ('*', '{Url}again', {again});
INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id) VALUES ('*', '{Url}e', 5);"), ct);
            await ExecAsync(connection, "ALTER TABLE config.server_tag_map RENAME TO server_tag_map_hidden", ct);
            try
            {
                routes = (await provider.LoadViewAsync(new DarlingConfig(), ct))!.NotificationRoutes;
                var tagRoute = routes.Single(r => r.ScopeTagId == again);
                Assert.False(tagRoute.CanMatch);
                Assert.False(tagRoute.Orphaned);
                Assert.Equal(new[] { 5 }, routes.Single(r => r.ScopeServerId == 5).ExpandedServerIds!.OrderBy(i => i));

                /* The MCP read says so too: the tag route is flagged scope_unreadable (not orphaned, no count), the server route
                   is an ordinary active one (#5750). */
                using var unreadable = JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(dataSource, ct));
                var mcpRows = unreadable.RootElement.GetProperty("routes").EnumerateArray().ToList();
                var mcpTag = mcpRows.Single(r => r.GetProperty("scope_tag_id").ValueKind == JsonValueKind.Number && r.GetProperty("scope_tag_id").GetInt32() == again);
                Assert.True(mcpTag.GetProperty("scope_unreadable").GetBoolean());
                Assert.False(mcpTag.GetProperty("orphaned").GetBoolean());
                Assert.Equal(JsonValueKind.Null, mcpTag.GetProperty("expanded_server_count").ValueKind);
                var mcpServer = mcpRows.Single(r => r.GetProperty("scope_server_id").ValueKind == JsonValueKind.Number && r.GetProperty("scope_server_id").GetInt32() == 5);
                Assert.False(mcpServer.GetProperty("scope_unreadable").GetBoolean());
                Assert.Equal(1, mcpServer.GetProperty("expanded_server_count").GetInt32());
            }
            finally
            {
                await ExecAsync(connection, "ALTER TABLE config.server_tag_map_hidden RENAME TO server_tag_map", ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static DarlingConfig Configured(IReadOnlyList<NotificationRoute> routes)
    {
        var config = new DarlingConfig();
        config.NotificationRoutes = routes.ToList();
        return config;
    }

    private static async Task<int> TagAsync(NpgsqlConnection connection, string name, int? parent, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("INSERT INTO config.server_tags (name, parent_id) VALUES ($1, $2) RETURNING id", connection);
        command.Parameters.AddWithValue(name);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)parent ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<long> BeaconAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, "SELECT config_version FROM config.config_service WHERE id = 1", ct), CultureInfo.InvariantCulture);

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
