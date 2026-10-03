/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
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
/// Pins the Darling rung that lets a mute rule name a server by its store id (<c>config.config_mute_rules.server_id</c>,
/// nullable integer). A display name is not an identity: a blank name falls back to the host, so two registrations on
/// one logical server shared a silence key. NULL stays a legacy name-keyed rule, matched exactly as before, so no stored
/// rule changes effect. The rung is one catalog-only ALTER; the table-level grants already cover a new column and the
/// V117 reload beacon fires on any change to the table.
///
/// <para>Every fact finds the rung by NAME, so a renumber is one edit to the registration, the constant and the viewer's
/// <c>return</c>.</para>
/// </summary>
public sealed class MuteRuleServerIdRungTests
{
    public const string RungName = "mute-rule-server-id";

    /// <summary>This rung's sentinel ordinal in the probe. No longer the last argument, since V158 appended its own.</summary>
    private const int ProbeOrdinal = 132;

    public static int RungVersion => Rung.Version;

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    [Fact]
    public void TheRungIsRegisteredAtVersion157_InADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(157, Rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        /* No longer the top rung: V158 (the collector run time) landed above it. */
        Assert.True(Rung.Version < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    [Fact]
    public void TheRungAddsOneNullableServerIdColumn_IdempotentlyAndSchemaQualified()
    {
        var sql = Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains("ALTER TABLE config.config_mute_rules ADD COLUMN IF NOT EXISTS server_id integer;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeCarriesTheColumn_AndAStoreThatStopsHereMapsToThisRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = "table_schema = 'config' AND table_name = 'config_mute_rules' AND column_name = 'server_id'";
        Assert.Contains(arm, probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1);
        Assert.Equal("hasMuteRuleServerId", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(157, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(156, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasMuteRuleServerId)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasCheckpointLongestSync)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "no sentinel arm: a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "this arm sits below the previous rung's, so a current store maps one rung short");
    }

    [Fact]
    public void PgMuteRuleStoreSql_CarriesServerIdInSelectInsertAndUpdate()
    {
        var source = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "PgMuteRuleStore.cs");
        var select = source[source.IndexOf("SELECT id, enabled", StringComparison.Ordinal)..source.IndexOf("FROM config_mute_rules", StringComparison.Ordinal)];
        var insert = source[source.IndexOf("INSERT INTO config_mute_rules", StringComparison.Ordinal)..source.IndexOf("VALUES (", StringComparison.Ordinal)];
        var update = source[source.IndexOf("UPDATE config_mute_rules SET\n", StringComparison.Ordinal)..];
        update = update[..update.IndexOf("WHERE id = $1", StringComparison.Ordinal)];

        Assert.Contains("server_id", select, StringComparison.Ordinal);
        Assert.Contains("server_id", insert, StringComparison.Ordinal);
        Assert.Contains("server_id = $", update, StringComparison.Ordinal);
        Assert.Contains("$12", source, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerMuteRuleSql_CarriesServerIdInSelectInsertAndUpdate()
    {
        Assert.Contains("server_id", ViewerDataService.MuteRulesSelectSql, StringComparison.Ordinal);
        Assert.Contains("server_id", ViewerDataService.MuteRuleInsertSql, StringComparison.Ordinal);
        Assert.Contains("$12", ViewerDataService.MuteRuleInsertSql, StringComparison.Ordinal);
        Assert.Contains("server_id = $", ViewerDataService.MuteRuleUpdateSql, StringComparison.Ordinal);
    }
}

/// <summary>The live half of <see cref="MuteRuleServerIdRungTests"/>, split out so only this one test serializes
/// with the other classes that write the shared store: the production mute-rule store round-trips the column.</summary>
[Collection("live-postgres")]
public sealed class MuteRuleServerIdLivePostgresTests
{
    /// <summary>Live round trip through the production store: an id-keyed rule reads back its id, a name-keyed rule
    /// reads back null, and an UPDATE can set and clear it.</summary>
    [Fact]
    public async Task PgMuteRuleStore_RoundTripsServerId_AndNullForALegacyRule()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-rule server_id round trip.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgMuteRuleStore(postgres);

        var keyed = new MuteRule { Id = "mute-sid-" + Guid.NewGuid().ToString("N"), ServerId = 42, ServerName = "label-only", Reason = "sid round-trip" };
        var legacy = new MuteRule { Id = "mute-sid-" + Guid.NewGuid().ToString("N"), ServerName = "label-only", Reason = "sid round-trip" };

        var bodySucceeded = false;
        try
        {
            await store.InsertAsync(keyed);
            await store.InsertAsync(legacy);

            var loaded = await store.LoadAllAsync();
            Assert.Equal(42, loaded.Single(r => r.Id == keyed.Id).ServerId);
            Assert.Null(loaded.Single(r => r.Id == legacy.Id).ServerId);

            keyed.ServerId = null;
            legacy.ServerId = 7;
            await store.UpdateAsync(keyed);
            await store.UpdateAsync(legacy);
            loaded = await store.LoadAllAsync();
            Assert.Null(loaded.Single(r => r.Id == keyed.Id).ServerId);
            Assert.Equal(7, loaded.Single(r => r.Id == legacy.Id).ServerId);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await store.DeleteAsync(keyed.Id);
                await store.DeleteAsync(legacy.Id);
            });
        }
    }
}

/// <summary>The MCP surface of the id-keyed mute rule: the payload carries <c>server_id</c>, create takes an optional
/// one that must be a monitored server's id, and update can move it.</summary>
public sealed class MuteRuleServerIdMcpTests
{
    private static Task<string?> Known(int id) => Task.FromResult<string?>(id == 7 ? "Display Seven" : null);

    private static Task<string> Create(FakeMuteRuleStore store, string? serverName, int? serverId) =>
        DarlingMcpAlertTools.CreateMuteRuleOver(store, serverName, null, null, null, null, null, "r", null, serverId, Known);

    [Fact]
    public async Task Create_WithAKnownServerId_StoresIt_AndLabelsTheRuleWithTheDisplayName()
    {
        var store = new FakeMuteRuleStore();
        var result = await Create(store, null, 7);

        Assert.Equal("created", DarlingMcpTestData.StatusOf(result));
        var payload = JsonNode.Parse(result)!["mute_rule"]!;
        Assert.Equal(7, (int)payload["server_id"]!);
        Assert.Equal("Display Seven", (string)payload["server_name"]!);
        Assert.Equal(7, store.Row((string)payload["id"]!)!.ServerId);
    }

    [Fact]
    public async Task Create_WithAServerIdAndAName_KeepsTheCallersNameAsTheLabel()
    {
        var store = new FakeMuteRuleStore();
        var result = await Create(store, "my label", 7);

        var payload = JsonNode.Parse(result)!["mute_rule"]!;
        Assert.Equal(7, (int)payload["server_id"]!);
        Assert.Equal("my label", (string)payload["server_name"]!);
    }

    [Fact]
    public async Task Create_WithAnUnknownServerId_IsRefused_AndWritesNothing()
    {
        var store = new FakeMuteRuleStore();
        var result = await Create(store, null, 999);

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task Create_WithoutAServerId_LeavesItNull_AndIsStillNameKeyed()
    {
        var store = new FakeMuteRuleStore();
        var result = await Create(store, "legacy-name", null);

        var payload = JsonNode.Parse(result)!["mute_rule"]!;
        Assert.Null(payload["server_id"]);
        Assert.Equal("legacy-name", (string)payload["server_name"]!);
    }

    [Fact]
    public async Task Update_CanSetAndClearServerId()
    {
        var seed = new MuteRule { Id = "r1", ServerName = "n", CreatedAtUtc = DateTime.UtcNow };
        var store = new FakeMuteRuleStore().Seed(seed);

        var set = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"server_id\":7}", Known);
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(set));
        Assert.Equal(7, store.Row("r1")!.ServerId);

        var same = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"server_id\":7}", Known);
        Assert.Equal("unchanged", DarlingMcpTestData.StatusOf(same));

        var cleared = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"server_id\":null}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(cleared));
        Assert.Null(store.Row("r1")!.ServerId);

        var bad = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"server_id\":\"seven\"}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(bad));
    }

    [Fact]
    public async Task UpdateCore_AnUnknownServerId_IsRefused_AndTheRuleIsUnchanged()
    {
        var seed = new MuteRule { Id = "r1", ServerName = "n", CreatedAtUtc = DateTime.UtcNow };
        var store = new FakeMuteRuleStore().Seed(seed);

        var result = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"server_id\":99}", Known);

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
        Assert.Null(store.Row("r1")!.ServerId);
    }

    [Fact]
    public async Task CreateCore_TheWebBody_RefusesAnUnknownServerId_AndLabelsAKnownOneFromTheRegistry()
    {
        /* The web route's create runs this Core over a JSON body, so a server_id there is held to the same
           rule as the MCP tool's: a rule keyed on an id no server has would mute nothing. */
        var store = new FakeMuteRuleStore();

        var unknown = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"server_id\":99}", Known);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(unknown));
        Assert.Equal(0, store.Count);

        var known = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"server_id\":7}", Known);
        Assert.Equal("created", DarlingMcpTestData.StatusOf(known));
        var payload = JsonNode.Parse(known)!["mute_rule"]!;
        var row = store.Row((string)payload["id"]!)!;
        Assert.Equal(7, row.ServerId);
        Assert.Equal("Display Seven", row.ServerName);
    }

    [Fact]
    public void TheWebRoutesAndTheUpdateTool_CheckAServerIdAgainstTheRegistry()
    {
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Contains("CreateMuteRuleCore(store, await ReadBodyAsync(context), serverNameLookup)", web, StringComparison.Ordinal);
        Assert.Contains("UpdateMuteRuleCore(store, id, await ReadBodyAsync(context), serverNameLookup)", web, StringComparison.Ordinal);

        var mcp = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        Assert.Contains("UpdateMuteRuleCore(new PgMuteRuleStore(postgres), rule_id, changes_json, id => MonitoredServerDisplayNameAsync(postgres, id))", mcp, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARepeatedCreate_WithADifferentServerId_IsADifferentRule()
    {
        var store = new FakeMuteRuleStore();
        await DarlingMcpAlertTools.CreateMuteRuleOver(store, "same", null, null, null, null, null, "r", null, 7, Known);
        var other = await DarlingMcpAlertTools.CreateMuteRuleOver(store, "same", null, null, null, null, null, "r", null, null, Known);

        Assert.Equal("created", DarlingMcpTestData.StatusOf(other));
        Assert.Equal(2, store.Count);
    }

    [Fact]
    public void TheCreateToolOffersServerId_AndTheUpdateTextNamesItAsEditable()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        Assert.Contains("int? server_id = null", source, StringComparison.Ordinal);
        Assert.Contains("Editable fields: server_name, server_id,", source, StringComparison.Ordinal);
    }
}
