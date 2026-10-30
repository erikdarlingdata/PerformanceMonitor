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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one. The two cluster-wide roles it needs (mcp and viewer) are created only when absent and dropped again by
   this class when it made them, so it runs in the collection for classes that change a cluster-wide object under a
   fixed name. It reads no wall clock. */

/// <summary>
/// #6281: the MCP tag tools may not change which servers an agent route covers. Each tool runs through its real body over a
/// store whose connections log in as <c>mcp</c> (<c>SET SESSION AUTHORIZATION</c>, which the test owner may do), so the
/// refusal is the store's own trigger and not a check the tool makes. Tags: <c>Root</c> holds an agent route, <c>Child</c> and
/// <c>Grand</c> are under it; <c>Plain</c> holds a route with no agent setting and <c>PlainChild</c> is under it; <c>Free</c> has
/// no route.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class DarlingMcpAgentRouteTagGuardLiveTests
{
    private sealed class Fixture : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlDataSource Owner { get; init; }
        public required NpgsqlDataSource Mcp { get; init; }
        public required NpgsqlDataSource Viewer { get; init; }
        public string OwnerConnectionString { get; init; } = "";
        public required List<string> MadeRoles { get; init; }
        public required int Root { get; init; }
        public required int Child { get; init; }
        public required int Grand { get; init; }
        public required int Plain { get; init; }
        public required int PlainChild { get; init; }
        public required int Free { get; init; }
        public required int AgentRoute { get; init; }
        public string BaseConnectionString { get; init; } = "";

        public ServerTagStore McpStore => new(Mcp, 30);
        public ServerTagStore ViewerStore => new(Viewer, 30);

        public async ValueTask DisposeAsync()
        {
            await Mcp.DisposeAsync();
            await Viewer.DisposeAsync();
            await Owner.DisposeAsync();
            await Scratch.DisposeAsync();
            if (MadeRoles.Count > 0)
            {
                await using var connection = new NpgsqlConnection(BaseConnectionString);
                await connection.OpenAsync();
                foreach (var role in MadeRoles)
                {
                    await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {role}", connection);
                    await drop.ExecuteNonQueryAsync();
                }
            }
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private static NpgsqlDataSource LoginAs(string connectionString, string role)
    {
        var builder = new NpgsqlDataSourceBuilder(connectionString);
        builder.UsePhysicalConnectionInitializer(
            connection =>
            {
                using var command = new NpgsqlCommand($"SET SESSION AUTHORIZATION {role}", connection);
                command.ExecuteNonQuery();
            },
            async connection =>
            {
                await using var command = new NpgsqlCommand($"SET SESSION AUTHORIZATION {role}", connection);
                await command.ExecuteNonQueryAsync();
            });
        return builder.Build();
    }

    private static async Task<Fixture?> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live agent-route tag guard test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var made = new List<string>();
        await using (var connection = new NpgsqlConnection(baseConnectionString))
        {
            await connection.OpenAsync(ct);
            foreach (var role in new[] { "mcp", "viewer" })
            {
                await using var exists = new NpgsqlCommand("SELECT count(*) FROM pg_roles WHERE rolname = $1", connection);
                exists.Parameters.AddWithValue(role);
                if (Convert.ToInt64(await exists.ExecuteScalarAsync(ct)) == 0)
                {
                    await using var create = new NpgsqlCommand($"CREATE ROLE {role} NOLOGIN", connection);
                    await create.ExecuteNonQueryAsync(ct);
                    made.Add(role);
                }
            }
        }

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var owner = NpgsqlDataSource.Create(connectionString);
        /* The grants the real roles hold on the tag tables (and the DELETE remove_server needs to clear a server's collector schedule rows, #6454), plus read on the rest of what the store's snapshot reads. The
           route table is NOT granted to mcp: the guard reads it as the owner. */
        await ExecAsync(owner, @"
GRANT USAGE ON SCHEMA config, collect TO mcp, viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA config TO mcp, viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO mcp, viewer;
REVOKE SELECT ON config.config_notification_routes FROM mcp;
GRANT INSERT, UPDATE, DELETE ON config.server_tags, config.server_tag_map TO mcp, viewer;
GRANT DELETE ON config.config_monitored_servers, config.query_tag_assignments, config.query_tag_rules, config.query_tag_map TO mcp;
GRANT DELETE ON config.config_collector_schedules, config.config_collector_run_times TO mcp;
GRANT UPDATE (config_version, updated_at) ON config.config_service TO mcp, viewer;");

        await ExecAsync(owner, @"
INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'alpha-sql-01', 'alpha-sql-01'), (8, 'alpha-sql-02', 'alpha-sql-02');
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (7, 'alpha-sql-01', 'alpha-sql-01', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'),
       (8, 'alpha-sql-02', 'alpha-sql-02', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'),
       (9, 'alpha-sql-09', 'alpha-sql-09', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC');");

        var store = new ServerTagStore(owner, 30);
        var root = ((ServerTagWriteResult.Ok)await store.CreateAsync("Root", null, null, ct)).Tag!.Id;
        var child = ((ServerTagWriteResult.Ok)await store.CreateAsync("Child", root, null, ct)).Tag!.Id;
        var grand = ((ServerTagWriteResult.Ok)await store.CreateAsync("Grand", child, null, ct)).Tag!.Id;
        var plain = ((ServerTagWriteResult.Ok)await store.CreateAsync("Plain", null, null, ct)).Tag!.Id;
        var plainChild = ((ServerTagWriteResult.Ok)await store.CreateAsync("PlainChild", plain, null, ct)).Tag!.Id;
        var free = ((ServerTagWriteResult.Ok)await store.CreateAsync("Free", null, null, ct)).Tag!.Id;

        await using (var insert = owner.CreateCommand(
            "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_tag_id) VALUES ('Blocking', 1, $1) RETURNING route_id"))
        {
            insert.Parameters.AddWithValue(root);
            var agentRoute = Convert.ToInt32(await insert.ExecuteScalarAsync(ct));
            await ExecAsync(owner, $"INSERT INTO config.config_notification_routes (metric_match, scope_tag_id) VALUES ('Blocking', {plain})");

            return new Fixture
            {
                Scratch = scratch,
                Owner = owner,
                Mcp = LoginAs(connectionString, "mcp"),
                Viewer = LoginAs(connectionString, "viewer"),
                OwnerConnectionString = connectionString,
                MadeRoles = made,
                BaseConnectionString = baseConnectionString!,
                Root = root, Child = child, Grand = grand, Plain = plain, PlainChild = plainChild, Free = free,
                AgentRoute = agentRoute,
            };
        }
    }

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    private static void AssertRefused(string json, int routeId)
    {
        var node = Parse(json);
        Assert.Equal("refused", (string?)node["status"]);
        Assert.Equal(DarlingMcpServerTagTools.AgentRouteRefusalCode, (string?)node["refusal"]);
        Assert.Contains($"Route {routeId} ", (string?)node["message"], StringComparison.Ordinal);
    }

    [Fact]
    public async Task Assign_ToATagOnOrUnderAnAgentRoute_IsRefusedAndWritesNothing_ElsewhereItWorks()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        var store = f.McpStore;

        foreach (var tag in new[] { f.Root, f.Child, f.Grand })
        {
            AssertRefused(await DarlingMcpServerTagTools.AssignServerTagCore(store, tag, new[] { 7 }, ct), f.AgentRoute);
        }

        Assert.Equal(0, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));

        Assert.Equal("assigned", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, f.Plain, new[] { 7 }, ct))["status"]);
        Assert.Equal("assigned", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, f.PlainChild, new[] { 7 }, ct))["status"]);
        Assert.Equal("assigned", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, f.Free, new[] { 7, 8 }, ct))["status"]);
        Assert.Equal(4, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));
    }

    [Fact]
    public async Task Unassign_FromATagOnOrUnderAnAgentRoute_IsRefusedAndWritesNothing_ElsewhereItWorks()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        await ExecAsync(f.Owner, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, {f.Root}), (7, {f.Grand}), (7, {f.PlainChild}), (7, {f.Free})");
        var store = f.McpStore;

        AssertRefused(await DarlingMcpServerTagTools.UnassignServerTagCore(store, f.Root, new[] { 7 }, ct), f.AgentRoute);
        AssertRefused(await DarlingMcpServerTagTools.UnassignServerTagCore(store, f.Grand, new[] { 7 }, ct), f.AgentRoute);
        Assert.Equal(4, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));

        Assert.Equal("unassigned", (string?)Parse(await DarlingMcpServerTagTools.UnassignServerTagCore(store, f.PlainChild, new[] { 7 }, ct))["status"]);
        Assert.Equal("unassigned", (string?)Parse(await DarlingMcpServerTagTools.UnassignServerTagCore(store, f.Free, new[] { 7 }, ct))["status"]);
        Assert.Equal(2, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));
    }

    [Fact]
    public async Task Delete_OfATagOnOrUnderAnAgentRoute_IsRefusedAndWritesNothing_ElsewhereItWorks()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        await ExecAsync(f.Owner, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, {f.Grand})");
        var store = f.McpStore;

        /* The route's own tag, a tag under it, and a tag above nothing but holding a descendant under it are all refused; the
           subtree delete cannot take a covered tag with it. */
        foreach (var tag in new[] { f.Root, f.Child, f.Grand })
        {
            AssertRefused(await DarlingMcpServerTagTools.DeleteServerTagCore(store, tag, true, ct), f.AgentRoute);
        }

        Assert.Equal(6, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tags"));
        Assert.Equal(1, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));

        Assert.Equal("deleted", (string?)Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, f.PlainChild, true, ct))["status"]);
        Assert.Equal("deleted", (string?)Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, f.Free, true, ct))["status"]);
        Assert.Equal(4, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tags"));
    }

    [Fact]
    public async Task Reparent_OutOfOrIntoAnAgentRoutesTags_IsRefusedAndWritesNothing_ElsewhereItWorks_AndARenameIsNotHeld()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        var store = f.McpStore;

        AssertRefused(await DarlingMcpServerTagTools.UpdateServerTagCore(store, f.Grand, $"{{\"parent_id\":{f.Plain}}}", ct), f.AgentRoute);
        AssertRefused(await DarlingMcpServerTagTools.UpdateServerTagCore(store, f.Free, $"{{\"parent_id\":{f.Child}}}", ct), f.AgentRoute);
        Assert.Equal(f.Child, await CountAsync(f.Owner, $"SELECT parent_id FROM config.server_tags WHERE id = {f.Grand}"));
        Assert.Equal(0, await CountAsync(f.Owner, $"SELECT count(*) FROM config.server_tags WHERE id = {f.Free} AND parent_id IS NOT NULL"));

        Assert.Equal("updated", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, f.Free, $"{{\"parent_id\":{f.PlainChild}}}", ct))["status"]);
        Assert.Equal("updated", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, f.PlainChild, "{\"parent_id\":null}", ct))["status"]);

        /* A rename and a colour change do not touch coverage, even on the route's own tag. */
        Assert.Equal("updated", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, f.Root, "{\"name\":\"Root renamed\",\"colour\":\"#112233\"}", ct))["status"]);
    }

    [Fact]
    public async Task ADirectWriteAsMcp_IsRefusedByTheStore_AndRemovingAServerCoveredByAnAgentRouteIsRefusedWithItsTagsAndRegistration()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;

        var ex = await Assert.ThrowsAsync<PostgresException>(() =>
            ExecAsync(f.Mcp, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, {f.Grand})"));
        Assert.Equal("DL403", ex.SqlState);
        Assert.Contains($"Route {f.AgentRoute} ", ex.MessageText, StringComparison.Ordinal);

        /* remove_server deletes the registry row and then clears the server's map rows in one transaction. A server mapped under an
           agent route's tag is no longer exempt (its tag rows would come back with it when it is added again, because the id is a
           hash): the tool answers a refusal, the server stays defined and its tag rows stay. */
        await ExecAsync(f.Owner, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (8, {f.Grand})");
        var refused = Parse(await DarlingMcpServerAdminTools.RemoveServer(f.Mcp, "alpha-sql-02"));
        Assert.Equal("refused", (string?)refused["status"]);
        Assert.Contains($"Route {f.AgentRoute} ", (string?)refused["message"], StringComparison.Ordinal);
        Assert.Contains("not removed", (string?)refused["message"], StringComparison.Ordinal);
        Assert.Equal(1, await CountAsync(f.Owner, "SELECT count(*) FROM config.config_monitored_servers WHERE server_id = 8"));
        Assert.Equal(1, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map WHERE server_id = 8"));

        /* A server mapped only under tags with no agent route is removed, and its tag rows go with it. */
        await ExecAsync(f.Owner, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, {f.Free}), (7, {f.PlainChild})");
        Assert.Equal("removed", (string?)Parse(await DarlingMcpServerAdminTools.RemoveServer(f.Mcp, "alpha-sql-01"))["status"]);
        Assert.Equal(0, await CountAsync(f.Owner, "SELECT count(*) FROM config.config_monitored_servers WHERE server_id = 7"));
        Assert.Equal(0, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map WHERE server_id = 7"));
    }

    [Fact]
    public async Task AnOffRoute_IsHeldLikeAnyAgentRoute_ForAssignUnassignDeleteAndMove()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        var off = await TagAsync(f, "Off tag", null);
        var offChild = await TagAsync(f, "Off child", off);
        var elsewhere = await TagAsync(f, "Elsewhere", null);
        var offRoute = await RouteAsync(f, off, 0);
        await ExecAsync(f.Owner, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, {offChild})");
        var store = f.McpStore;

        AssertRefused(await DarlingMcpServerTagTools.AssignServerTagCore(store, off, new[] { 8 }, ct), offRoute);
        AssertRefused(await DarlingMcpServerTagTools.AssignServerTagCore(store, offChild, new[] { 8 }, ct), offRoute);
        AssertRefused(await DarlingMcpServerTagTools.UnassignServerTagCore(store, offChild, new[] { 7 }, ct), offRoute);
        AssertRefused(await DarlingMcpServerTagTools.UpdateServerTagCore(store, offChild, $"{{\"parent_id\":{elsewhere}}}", ct), offRoute);
        AssertRefused(await DarlingMcpServerTagTools.DeleteServerTagCore(store, off, true, ct), offRoute);

        Assert.Equal(1, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));
        Assert.Equal(off, await CountAsync(f.Owner, $"SELECT parent_id FROM config.server_tags WHERE id = {offChild}"));
    }

    [Fact]
    public async Task MovingATagAbove_OrDeletingATagAbove_AnAgentRoutesTag_IsRefused()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        var top = await TagAsync(f, "Top", null);
        var mid = await TagAsync(f, "Mid", top);
        var leaf = await TagAsync(f, "Leaf", mid);
        var other = await TagAsync(f, "Other", null);
        var route = await RouteAsync(f, leaf, 0);
        var store = f.McpStore;

        /* A move of an ancestor changes how deep the route's tag sits, which decides which route wins for a server both cover. */
        AssertRefused(await DarlingMcpServerTagTools.UpdateServerTagCore(store, mid, "{\"parent_id\":null}", ct), route);
        AssertRefused(await DarlingMcpServerTagTools.UpdateServerTagCore(store, top, $"{{\"parent_id\":{other}}}", ct), route);
        Assert.Equal(top, await CountAsync(f.Owner, $"SELECT parent_id FROM config.server_tags WHERE id = {mid}"));
        Assert.Equal(0, await CountAsync(f.Owner, $"SELECT count(*) FROM config.server_tags WHERE id = {top} AND parent_id IS NOT NULL"));

        /* A delete of a tag above takes the route's tag with it. */
        AssertRefused(await DarlingMcpServerTagTools.DeleteServerTagCore(store, top, true, ct), route);
        AssertRefused(await DarlingMcpServerTagTools.DeleteServerTagCore(store, mid, true, ct), route);
        Assert.Equal(1, await CountAsync(f.Owner, $"SELECT count(*) FROM config.server_tags WHERE id = {leaf}"));
    }

    [Fact]
    public async Task ATagAddedUnderAnAgentRoutesTag_CannotThenBeAssigned()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        var store = f.McpStore;

        var created = Parse(await DarlingMcpServerTagTools.CreateServerTagCore(store, "New kid", f.Child, null, ct));
        Assert.Equal("created", (string?)created["status"]);
        var kid = Convert.ToInt32(await CountAsync(f.Owner, "SELECT id FROM config.server_tags WHERE name = 'New kid'"));

        AssertRefused(await DarlingMcpServerTagTools.AssignServerTagCore(store, kid, new[] { 7 }, ct), f.AgentRoute);
        Assert.Equal(0, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));
    }

    [Fact]
    public async Task AServerThatIsNotRegistered_CannotBeStagedOntoAnAgentRoutesTag()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;

        /* Server 9 has a collection row and no registration (a removed server stays in the collection table). Its id is a hash of
           its connection, so a later add brings its tag rows back. */
        AssertRefused(await DarlingMcpServerTagTools.AssignServerTagCore(f.McpStore, f.Grand, new[] { 9 }, ct), f.AgentRoute);
        Assert.Equal(0, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));

        Assert.Equal("assigned", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTagCore(f.McpStore, f.Free, new[] { 9 }, ct))["status"]);
    }

    /* postgres.mcpConnectionString unset: the MCP host connects with the store's own login. Each test below hands the OWNER's data
       source to a real MCP tool, so the only thing that can refuse the write is the marker the tool's transaction sets itself
       (session_user is the owner, not mcp, and the connection string carries no Options). */

    [Fact]
    public async Task AssignServerTag_AsTheOwner_IsRefusedUnderAnAgentRoute_AndWritesNothing()
    {
        await using var f = (await OpenAsync())!;

        AssertRefused(await DarlingMcpServerTagTools.AssignServerTag(f.Owner, f.Grand, new[] { 7 }, TestContext.Current.CancellationToken), f.AgentRoute);
        Assert.Equal(0, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map"));
        Assert.Equal("assigned", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTag(f.Owner, f.Free, new[] { 7 }, TestContext.Current.CancellationToken))["status"]);
    }

    [Fact]
    public async Task UnassignServerTag_AsTheOwner_IsRefusedUnderAnAgentRoute_AndKeepsTheRow()
    {
        await using var f = (await OpenAsync())!;
        await ExecAsync(f.Owner, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, {f.Grand})");

        AssertRefused(await DarlingMcpServerTagTools.UnassignServerTag(f.Owner, f.Grand, new[] { 7 }, TestContext.Current.CancellationToken), f.AgentRoute);
        Assert.Equal(1, await CountAsync(f.Owner, $"SELECT count(*) FROM config.server_tag_map WHERE server_id = 7 AND tag_id = {f.Grand}"));
    }

    [Fact]
    public async Task UpdateServerTag_AsTheOwner_RefusesAReparentOutOfAnAgentRoute_ButARenameWorks()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;

        AssertRefused(await DarlingMcpServerTagTools.UpdateServerTag(f.Owner, f.Grand, $"{{\"parent_id\":{f.Plain}}}", ct), f.AgentRoute);
        Assert.Equal(1, await CountAsync(f.Owner, $"SELECT count(*) FROM config.server_tags WHERE id = {f.Grand} AND parent_id = {f.Child}"));
        Assert.Equal("updated", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTag(f.Owner, f.Grand, "{\"name\":\"Grand2\"}", ct))["status"]);
    }

    [Fact]
    public async Task DeleteServerTag_AsTheOwner_IsRefusedOnAnAgentRoutesTag_AndKeepsTheTree()
    {
        await using var f = (await OpenAsync())!;

        AssertRefused(await DarlingMcpServerTagTools.DeleteServerTag(f.Owner, f.Root, true, TestContext.Current.CancellationToken), f.AgentRoute);
        Assert.Equal(6, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tags"));
    }

    [Fact]
    public async Task RemoveServer_AsTheOwner_IsRefusedForAServerUnderAnAgentRoute_AndKeepsTheServerAndItsTags()
    {
        await using var f = (await OpenAsync())!;
        await ExecAsync(f.Owner, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (8, {f.Grand})");

        var refused = Parse(await DarlingMcpServerAdminTools.RemoveServer(f.Owner, "alpha-sql-02"));
        Assert.Equal("refused", (string?)refused["status"]);
        Assert.Contains($"Route {f.AgentRoute} ", (string?)refused["message"], StringComparison.Ordinal);
        Assert.Equal(1, await CountAsync(f.Owner, "SELECT count(*) FROM config.config_monitored_servers WHERE server_id = 8"));
        Assert.Equal(1, await CountAsync(f.Owner, "SELECT count(*) FROM config.server_tag_map WHERE server_id = 8"));
    }

    [Fact]
    public async Task AViewerSessionAsTheOwner_StillEditsAnAgentRoutesTags()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;

        /* The Viewer and the web build the store without the marker; the same login edits the same tags. */
        var viewerAsOwner = new ServerTagStore(f.Owner, 30);
        Assert.Equal(new[] { 7 }, (await viewerAsOwner.AssignAsync(f.Grand, new[] { 7 }, ct)).ToArray());
        Assert.Equal(new[] { 7 }, (await viewerAsOwner.UnassignAsync(f.Grand, new[] { 7 }, ct)).ToArray());
        Assert.IsType<ServerTagWriteResult.Ok>(await viewerAsOwner.UpdateAsync(f.Grand, new ServerTagEdit(false, null, false, null, true, f.Plain), ct));
        Assert.IsType<ServerTagWriteResult.Ok>(await viewerAsOwner.DeleteAsync(f.Root, true, ct));
        Assert.Equal(0, await CountAsync(f.Owner, $"SELECT count(*) FROM config.server_tags WHERE id IN ({f.Root}, {f.Child})"));
    }

    [Fact]
    public async Task TheMcpDataSource_SendsNoStartupOptions_AndTheMarkerIsGoneOnceTheWriteCommits()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;

        var connectionString = DarlingStoreConnection.PinSessionTimeZoneUtc(
            DarlingStoreConnection.WithApplicationName(f.OwnerConnectionString, DarlingManagedPostgres.McpApplicationName));
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        Assert.True(string.IsNullOrEmpty(builder.Options), $"the MCP store connection string must not carry Options, got '{builder.Options}'");
        Assert.DoesNotContain("darling.surface", connectionString, StringComparison.Ordinal);

        /* One physical connection, reused: a marker set for the session (not the transaction) would still be there after the write. */
        builder.MaxPoolSize = 1;
        await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
        Assert.Equal("assigned", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTag(source, f.Free, new[] { 7 }, ct))["status"]);
        Assert.Equal("", await TextAsync(source, "SELECT coalesce(current_setting('darling.surface', true), '')"));
    }

    /// <summary>The rollback path of the transaction-local marker (#6281): a write the guard REFUSES ends its transaction with a rollback,
    /// and the one pooled connection it ran on must not keep the marker, or the next use of that connection (a read, or a write that is
    /// not held) would run as an MCP surface it never was.</summary>
    [Fact]
    public async Task TheMcpDataSource_TheMarkerIsGoneAfterARefusedWrite_OnOneConnection()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;

        var builder = new NpgsqlConnectionStringBuilder(DarlingStoreConnection.PinSessionTimeZoneUtc(
            DarlingStoreConnection.WithApplicationName(f.OwnerConnectionString, DarlingManagedPostgres.McpApplicationName)))
        {
            MaxPoolSize = 1,
        };
        await using var source = NpgsqlDataSource.Create(builder.ConnectionString);

        AssertRefused(await DarlingMcpServerTagTools.AssignServerTag(source, f.Grand, new[] { 7 }, ct), f.AgentRoute);
        Assert.Equal("", await TextAsync(source, "SELECT coalesce(current_setting('darling.surface', true), '')"));
        Assert.Equal(0, await CountAsync(source, "SELECT count(*) FROM config.server_tag_map"));

        /* The same connection still writes where nothing is held, and the marker is gone again afterwards. */
        Assert.Equal("assigned", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTag(source, f.Free, new[] { 7 }, ct))["status"]);
        Assert.Equal("", await TextAsync(source, "SELECT coalesce(current_setting('darling.surface', true), '')"));
    }

    private static async Task<string> TextAsync(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken)) ?? "";
    }

    private static async Task<int> TagAsync(Fixture f, string name, int? parent) =>
        ((ServerTagWriteResult.Ok)await new ServerTagStore(f.Owner, 30).CreateAsync(name, parent, null, TestContext.Current.CancellationToken)).Tag!.Id;

    private static async Task<int> RouteAsync(Fixture f, int tag, int level)
    {
        await using var insert = f.Owner.CreateCommand(
            "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_tag_id) VALUES ('Blocking', $1, $2) RETURNING route_id");
        insert.Parameters.AddWithValue(level);
        insert.Parameters.AddWithValue(tag);
        return Convert.ToInt32(await insert.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TheViewerRole_StillEditsAnAgentRoutesTags()
    {
        await using var f = (await OpenAsync())!;
        var ct = TestContext.Current.CancellationToken;
        var store = f.ViewerStore;

        Assert.Equal(new[] { 7 }, (await store.AssignAsync(f.Grand, new[] { 7 }, ct)).ToArray());
        Assert.Equal(new[] { 7 }, (await store.UnassignAsync(f.Grand, new[] { 7 }, ct)).ToArray());
        Assert.IsType<ServerTagWriteResult.Ok>(await store.UpdateAsync(f.Grand, new ServerTagEdit(false, null, false, null, true, f.Plain), ct));
        Assert.IsType<ServerTagWriteResult.Ok>(await store.DeleteAsync(f.Root, true, ct));
        Assert.Equal(0, await CountAsync(f.Owner, $"SELECT count(*) FROM config.server_tags WHERE id IN ({f.Root}, {f.Child})"));
    }
}
