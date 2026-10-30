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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6926, a threshold override held for a server tag. The precedence (database, then server, then tag, then global), the tag rule
/// (the notification routes' own: the deepest tag a server is under, the lowest row id on a same-depth tie, a deleted tag matching
/// nothing) and the whole path through a live store: <c>set_alert_threshold_override</c> with a tag, <c>get_alert_settings</c>
/// naming the tag, the list marking an orphan, and V227 on a store that already holds server overrides.
/// </summary>
public sealed class ThresholdOverrideTagScopeTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingMcpWritePolicy.Effective WritesArm = new(true, false);
    private static readonly DarlingMcpWritePolicy.Effective AgentArm = new(false, false, Investigations: true);

    /* Tags: 1 prod (root) > 2 east > 3 rack1; 4 dev (root); 5 and 6 are two roots a server can sit under at once. */
    private static readonly Dictionary<int, int?> Parents = new()
    {
        [1] = null, [2] = 1, [3] = 2, [4] = null, [5] = null, [6] = null,
    };

    private static StoreConfigProvider.RouteScopeInputs Inputs(bool readable = true) => new(
        new HashSet<int> { 7, 8, 9, 10 },
        Parents,
        new Dictionary<int, IReadOnlyCollection<int>>
        {
            [7] = new[] { 3 },          /* under rack1: prod > east > rack1 */
            [8] = new[] { 1 },          /* directly under prod */
            [9] = new[] { 4 },          /* dev */
            [10] = new[] { 5, 6 },      /* two roots at the same depth */
        },
        readable);

    private static DarlingThresholdOverrides Load(
        (int Server, string? Database, string Key, double Value)[] direct,
        (long RowId, int Tag, string Key, double Value)[] tags,
        bool readable = true)
    {
        var overrides = new DarlingThresholdOverrides();
        overrides.Replace(direct, tags.Select(t => (t.RowId, t.Tag, t.Key, t.Value)), Inputs(readable));
        return overrides;
    }

    [Fact]
    public void Precedence_DatabaseBeatsServerBeatsTagBeatsGlobal()
    {
        const string Key = "pvs.threshold_percent";
        var o = Load(
            [(7, "shop", Key, 60), (7, null, Key, 55)],
            [(1, 1, Key, 50)]);

        Assert.Equal(60, o.ResolveWire(Key, "7", "shop", 40));    /* the database override */
        Assert.Equal(55, o.ResolveWire(Key, "7", "ledger", 40));  /* no override for that database: the server's */
        Assert.Equal(55, o.ResolveWire(Key, "7", null, 40));
        Assert.Equal(50, o.ResolveWire(Key, "8", null, 40));      /* no server override: the tag's */
        Assert.Equal(50, o.ResolveWire(Key, "8", "shop", 40));
        Assert.Equal(40, o.ResolveWire(Key, "9", null, 40));      /* a server outside the tag: global */
        Assert.Equal(40, o.ResolveWire("cpu.threshold_percent", "8", null, 40));
    }

    [Fact]
    public void TagRule_DeepestTagWins_AndASameDepthTieGoesToTheLowestRowId()
    {
        const string Key = "cpu.threshold_percent";

        /* Server 7 sits under rack1 (2 ancestors); prod (0 ancestors) also covers it. The deeper tag wins whatever the row ids. */
        var nested = Load([], [(1, 1, Key, 80), (2, 3, Key, 70), (3, 2, Key, 75)]);
        Assert.Equal(70, nested.ResolveWire(Key, "7", null, 90));
        Assert.Equal(80, nested.ResolveWire(Key, "8", null, 90));   /* server 8 is only under prod */

        /* Server 10 is under two roots of the same depth: the lower row id wins, the way the router takes the lower route id. */
        var tie = Load([], [(20, 5, Key, 65), (10, 6, Key, 85)]);
        Assert.Equal(85, tie.ResolveWire(Key, "10", null, 90));
        var reversed = Load([], [(10, 5, Key, 65), (20, 6, Key, 85)]);
        Assert.Equal(65, reversed.ResolveWire(Key, "10", null, 90));
    }

    [Fact]
    public void ADeletedTag_MatchesNothing_AndUnreadableTagTablesMatchNothing()
    {
        const string Key = "cpu.threshold_percent";
        var orphan = Load([], [(1, 99, Key, 70)]);   /* tag 99 is not in the tree */
        Assert.Equal(90, orphan.ResolveWire(Key, "7", null, 90));
        Assert.Equal(0, orphan.Count);

        var unreadable = Load([], [(1, 1, Key, 70)], readable: false);
        Assert.Equal(90, unreadable.ResolveWire(Key, "8", null, 90));
    }

    [Fact]
    public void ATagRowWithAnInvalidValue_IsSkipped_AndTheShallowerTagStillApplies()
    {
        const string Key = "cpu.threshold_percent";
        var o = Load([], [(1, 1, Key, 80), (2, 3, Key, 5000)]);   /* 5000 is out of bounds for a percent */
        Assert.Equal(80, o.ResolveWire(Key, "7", null, 90));
    }

    [Fact]
    public async Task Tool_SetsAndClearsATagOverride_TheLoaderAndTheServerViewNameTheTag()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            var set = await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 95, tag: "Production", setBy: "erik");
            Assert.Equal("ok", Status(set));
            Assert.Equal("Production", JsonNode.Parse(set)!["tag"]!.GetValue<string>());
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides WHERE scope_tag_id = 1 AND server_id IS NULL", ct));
            Assert.Equal("erik", await ScalarAsync(connection, "SELECT set_by FROM config.config_threshold_overrides WHERE scope_tag_id = 1", ct));

            /* A second call updates the same row (by tag id this time); the answer carries the undo. */
            var again = await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 92, tag: "1");
            Assert.Equal(95, JsonNode.Parse(again)!["previous_override"]!.GetValue<double>());
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides WHERE scope_tag_id = 1", ct));

            /* A nested tag with its own value: the deeper tag wins for the server under it. */
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 88, tag: "east")));

            var loader = new DarlingThresholdOverrides();
            await loader.LoadAsync(ds, ct);
            Assert.Equal(88, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "7", null, 80));   /* srv-7 is under east */
            Assert.Equal(92, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "8", null, 80));   /* srv-8 is directly under Production */
            Assert.Equal(80, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "9", null, 80));

            /* A server override beats the tag in the loader and in the view. */
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-7", "cpu.threshold_percent", 60)));
            await loader.LoadAsync(ds, ct);
            Assert.Equal(60, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "7", null, 80));

            await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            JsonNode? Row(JsonNode view, string key) => view["thresholds"]!.AsArray().First(n => n!["key"]!.GetValue<string>() == key);

            var seven = JsonNode.Parse(await DarlingMcpAlertTools.GetAlertSettings(ds, "srv-7", ct))!["server_view"]!;
            Assert.Equal("server_override", Row(seven, "cpu.threshold_percent")!["source"]!.GetValue<string>());

            var eight = JsonNode.Parse(await DarlingMcpAlertTools.GetAlertSettings(ds, "srv-8", ct))!["server_view"]!;
            var cpu = Row(eight, "cpu.threshold_percent")!;
            Assert.Equal("tag_override", cpu["source"]!.GetValue<string>());
            Assert.Equal("Production", cpu["tag"]!.GetValue<string>());
            Assert.Equal(92, cpu["effective_value"]!.GetValue<int>());
            Assert.Equal("global", Row(eight, "blocking.count_threshold")!["source"]!.GetValue<string>());
            Assert.Single(eight["tag_overrides"]!.AsArray());

            var nine = JsonNode.Parse(await DarlingMcpAlertTools.GetAlertSettings(ds, "srv-9", ct))!["server_view"]!;
            Assert.Equal("global", Row(nine, "cpu.threshold_percent")!["source"]!.GetValue<string>());

            /* Clear works the same way, by name or id. */
            Assert.Equal("cleared", Status(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", null, tag: "east", clear: true)));
            Assert.Equal("cleared", Status(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", null, tag: "1", clear: true)));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides WHERE scope_tag_id IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task Tool_RefusesTagMisuse_AndLeavesTheTableUntouched()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            Assert.Equal("scope_conflict", Reason(await Set(ds, store, WritesArm, "srv-7", "cpu.threshold_percent", 90, tag: "Production")));
            Assert.Equal("scope_required", Reason(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 90)));
            Assert.Equal("database_scope", Reason(await Set(ds, store, WritesArm, null, "pvs.threshold_percent", 70, database: "shop", tag: "Production")));
            Assert.Equal("tag_not_found", Reason(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 90, tag: "nope")));
            Assert.Equal("tag_writes_arm_only", Reason(await Set(ds, store, AgentArm, null, "cpu.threshold_percent", 90, tag: "Production")));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides", ct));

            /* A name two tags share is refused until the id is used. */
            await ExecAsync(connection, "INSERT INTO config.server_tags (name, parent_id) VALUES ('Shared', 1), ('Shared', 4)", ct);
            Assert.Equal("tag_not_found", Reason(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 90, tag: "shared")));
        });
    }

    [Fact]
    public async Task ADeletedTag_ListsAsOrphaned_MatchesNothing_AndCanStillBeCleared()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 95, tag: "dev")));
            await ExecAsync(connection, "DELETE FROM config.server_tags WHERE id = 4", ct);

            var rows = await ThresholdOverrideStore.ListAsync(ds, k => null, ct);
            var row = Assert.Single(rows);
            Assert.True(row.IsTag);
            Assert.True(row.Orphaned);
            Assert.Equal(4, row.TagId);

            var loader = new DarlingThresholdOverrides();
            await loader.LoadAsync(ds, ct);
            Assert.Equal(80, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "9", null, 80));

            Assert.Equal("cleared", Status(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", null, tag: "4", clear: true)));
            Assert.Empty(await ThresholdOverrideStore.ListAsync(ds, k => null, ct));
        });
    }

    [Fact]
    public async Task AFailedTagTableRead_KeepsTheSnapshotInForce_NotAGlobalOne()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 92, tag: "Production")));
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-9", "cpu.threshold_percent", 70)));

            var loader = new DarlingThresholdOverrides();
            await loader.LoadAsync(ds, ct);
            Assert.Equal(92, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "8", null, 80));
            Assert.Equal(3, loader.Count);   /* srv-9's own row, and the Production override for srv-7 and srv-8 */

            /* The server-to-tag map cannot be read (a reset or a timeout stands in as a missing table), and a server override changes. */
            await ExecAsync(connection, "ALTER TABLE config.server_tag_map RENAME TO server_tag_map_away", ct);
            await ExecAsync(connection, "UPDATE config.config_threshold_overrides SET value = 71 WHERE server_id = 9", ct);
            await Assert.ThrowsAnyAsync<Exception>(() => loader.LoadAsync(ds, ct));

            /* Nothing was replaced: the tag override still applies and the changed server row is not half-loaded. */
            Assert.Equal(92, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "8", null, 80));
            Assert.Equal(70, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "9", null, 80));
            Assert.Equal(3, loader.Count);   /* srv-9's own row, and the Production override for srv-7 and srv-8 */

            await ExecAsync(connection, "ALTER TABLE config.server_tag_map_away RENAME TO server_tag_map", ct);
            await loader.LoadAsync(ds, ct);
            Assert.Equal(71, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "9", null, 80));
            Assert.Equal(92, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "8", null, 80));
        });
    }

    [Fact]
    public async Task TheServerView_SkipsATagRowOutsideTheBounds_AsTheLoaderDoes()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            /* srv-7 is under east (2) under Production (1). The deeper tag holds a value outside a percent's bounds (written by direct SQL). */
            await ExecAsync(connection, "INSERT INTO config.config_threshold_overrides (scope_tag_id, threshold_key, value, set_by) VALUES "
                + "(1, 'cpu.threshold_percent', 85, 'erik'), (2, 'cpu.threshold_percent', 5000, 'erik')", ct);
            await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);

            var loader = new DarlingThresholdOverrides();
            await loader.LoadAsync(ds, ct);
            Assert.Equal(85, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "7", null, 80));

            var view = JsonNode.Parse(await DarlingMcpAlertTools.GetAlertSettings(ds, "srv-7", ct))!["server_view"]!;
            var cpu = view["thresholds"]!.AsArray().First(n => n!["key"]!.GetValue<string>() == "cpu.threshold_percent")!;
            Assert.Equal("tag_override", cpu["source"]!.GetValue<string>());
            Assert.Equal("Production", cpu["tag"]!.GetValue<string>());
            Assert.Equal(85, cpu["effective_value"]!.GetValue<int>());
        });
    }

    [Fact]
    public async Task ListShowsTagRowsWithTheirTagName()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, null, "cpu.threshold_percent", 95, tag: "Production")));
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-7", "cpu.threshold_percent", 90)));
            var rows = await ThresholdOverrideStore.ListAsync(ds, k => null, ct);
            Assert.Equal(2, rows.Count);
            var tag = rows.Single(r => r.IsTag);
            Assert.Equal("Production", tag.ServerName);
            Assert.False(tag.Orphaned);
            Assert.Equal("srv-7", rows.Single(r => !r.IsTag).ServerName);

            var body = await DarlingWebThresholdOverrides.ListBodyAsync(ds, ct);
            Assert.Equal(2, body["overrides"]!.AsArray().Count);
            Assert.Contains(body["tags"]!.AsArray(), t => t!["tag_name"]!.GetValue<string>() == "Production");
        });
    }

    [Fact]
    public async Task V227_OnAStoreThatHoldsServerOverrides_KeepsThem_AndTakesTagRows()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the threshold override live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var store = new NpgsqlConnection(scratch.ConnectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);

        var rung = PgMigrations.Scripts.Single(m => m.Name == "threshold-override-tag-scope").Version;
        Assert.Equal(227, rung);
        await RungRewindArtifacts.DropSentinelsAboveAsync(store, rung - 1, ct);
        await ExecAsync(store, "DELETE FROM darling_schema_version WHERE version >= " + rung.ToString(CultureInfo.InvariantCulture), ct);
        await ExecAsync(store, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7')", ct);
        await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES "
            + "(7, NULL, 'cpu.threshold_percent', 91, 'erik'), (7, 'shop', 'pvs.threshold_percent', 66, 'erik')", ct);
        Assert.Equal("False", await ScalarAsync(store,
            "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'config.config_threshold_overrides'::regclass AND attname = 'scope_tag_id' AND NOT attisdropped)", ct));

        await PgMigrations.MigrateAsync(store, ct);
        Assert.Equal(StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture), await ScalarAsync(store, "SELECT max(version) FROM darling_schema_version", ct));
        Assert.Equal("2", await ScalarAsync(store, "SELECT count(*) FROM config.config_threshold_overrides WHERE server_id = 7 AND scope_tag_id IS NULL", ct));
        Assert.Equal("91", await ScalarAsync(store, "SELECT value FROM config.config_threshold_overrides WHERE database_name IS NULL", ct));

        /* A tag row is accepted; a row with neither scope, both scopes or a database on a tag is not; one value per tag and key. */
        await ExecAsync(store, "INSERT INTO config.server_tags (name) VALUES ('Production')", ct);
        await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (scope_tag_id, threshold_key, value, set_by) VALUES (1, 'cpu.threshold_percent', 80, 'erik')", ct);
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store,
            "INSERT INTO config.config_threshold_overrides (scope_tag_id, threshold_key, value, set_by) VALUES (1, 'cpu.threshold_percent', 70, 'erik')", ct));
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store,
            "INSERT INTO config.config_threshold_overrides (threshold_key, value, set_by) VALUES ('cpu.threshold_percent', 70, 'erik')", ct));
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store,
            "INSERT INTO config.config_threshold_overrides (server_id, scope_tag_id, threshold_key, value, set_by) VALUES (7, 1, 'blocking.count_threshold', 70, 'erik')", ct));
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store,
            "INSERT INTO config.config_threshold_overrides (scope_tag_id, database_name, threshold_key, value, set_by) VALUES (1, 'shop', 'pvs.threshold_percent', 70, 'erik')", ct));

        /* A second run changes nothing, and the version stamp is not repeated. */
        await ExecAsync(store, "DELETE FROM darling_schema_version WHERE version >= " + rung.ToString(CultureInfo.InvariantCulture), ct);
        await PgMigrations.MigrateAsync(store, ct);
        Assert.Equal("3", await ScalarAsync(store, "SELECT count(*) FROM config.config_threshold_overrides", ct));
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, InvestigationRequestStore, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the threshold override live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7'), (8, 'srv-8', 'h8'), (9, 'srv-9', 'h9')", ct);
        await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
            + "VALUES (7, 'srv-7', 'srv-7', TRUE, 15, now(), now()), (8, 'srv-8', 'srv-8', TRUE, 15, now(), now()), (9, 'srv-9', 'srv-9', TRUE, 15, now(), now())", ct);
        /* Tag ids 1 Production, 2 east (under Production), 3 rack1 (under east), 4 dev; srv-7 is under east, srv-8 under Production, srv-9 under dev. */
        await ExecAsync(connection, "INSERT INTO config.server_tags (name, parent_id) VALUES ('Production', NULL), ('east', 1), ('rack1', 2), ('dev', NULL)", ct);
        await ExecAsync(connection, "INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, 2), (8, 1), (9, 4)", ct);

        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => new DateTime(2026, 10, 29, 12, 0, 0, DateTimeKind.Utc));
        await body(dataSource, connection, store, ct);
    }

    private static Task<string> Set(NpgsqlDataSource ds, InvestigationRequestStore store, DarlingMcpWritePolicy.Effective policy,
        string? server, string key, int? value, string? database = null, bool clear = false, string? setBy = null, string? tag = null) =>
        DarlingMcpThresholdOverrideTools.SetAsync(ds, policy, store, NullLogger.Instance, server, key, value, database, clear, setBy, null,
            TestContext.Current.CancellationToken, null, tag);

    private static string Status(string json) => JsonNode.Parse(json)!["status"]!.GetValue<string>();

    private static string Reason(string json) => JsonNode.Parse(json)!["reason"]!.GetValue<string>();

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? "";
    }
}
