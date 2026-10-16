/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// The query-tag MCP tools (#5981, wave 2 of #5971) called through their tool methods against a real store: tags, hand
/// assignments, the refusals the shared tag rules give, the confirm-before-delete shape, the unknown-server refusal, the state bump
/// and what <c>list_query_tags</c> reports. One test per behaviour. The write gate (<c>mcp.allowWrites</c>) is pinned in
/// <c>DarlingMcpWritePolicyTests</c>, which drives the real tool-serving pipeline.
/// </summary>
public sealed class DarlingMcpQueryTagToolsLiveTests
{
    private const int ServerA = 7301;
    private const int ServerB = 7302;
    private const string Hash = "0xA1B2C3D4E5F60718";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the query-tag tool pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        await ExecAsync(owner, $"INSERT INTO servers (server_id, server_name) VALUES ({ServerA}, 'qt-alpha'), ({ServerB}, 'qt-beta')", ct);
        await using var source = NpgsqlDataSource.Create(connectionString);

        var bodySucceeded = false;
        try
        {
            await body(source, owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    private static async Task<int> CreateAsync(NpgsqlDataSource source, string name, int? parentId, CancellationToken ct)
    {
        var result = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, name, parentId, null, ct));
        Assert.Equal("created", (string?)result["status"]);
        return (int)result["tag"]!["tag_id"]!;
    }

    private static Task<string> AssignHashAsync(NpgsqlDataSource source, int tagId, int? serverId, CancellationToken ct, string hash = Hash) =>
        DarlingMcpQueryTagTools.AssignQueryTag(source, tagId, "hash", hash, server_id: serverId, cancellationToken: ct);

    [Fact]
    public async Task ACreateWithABadName_ABadColour_AFifthLevel_OrADuplicateSibling_IsRefusedWithTheSharedCode_AndWritesNothing()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var root = await CreateAsync(source, "Reports", null, ct);
            var l1 = await CreateAsync(source, "L1", root, ct);
            var l2 = await CreateAsync(source, "L2", l1, ct);
            var l3 = await CreateAsync(source, "L3", l2, ct);
            var tagsBefore = await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags", ct);

            var blank = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "   ", null, null, ct));
            var colour = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "Pretty", null, "red", ct));
            var fifth = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "L4", l3, null, ct));
            var duplicate = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "REPORTS", null, null, ct));

            Assert.Equal("bad_name", (string?)blank["refusal"]);
            Assert.Equal("bad_colour", (string?)colour["refusal"]);
            Assert.Equal("depth_limit", (string?)fifth["refusal"]);
            Assert.Equal("duplicate_name", (string?)duplicate["refusal"]);
            Assert.Equal("conflict", (string?)duplicate["status"]);
            Assert.Equal(tagsBefore, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags", ct));
        });
    }

    [Fact]
    public async Task TheSameNameUnderTwoDifferentParents_IsLegal()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var a = await CreateAsync(source, "Team A", null, ct);
            var b = await CreateAsync(source, "Team B", null, ct);
            await CreateAsync(source, "Nightly", a, ct);
            await CreateAsync(source, "nightly", b, ct);

            Assert.Equal(2, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags WHERE lower(name) = 'nightly'", ct));
        });
    }

    [Fact]
    public async Task AMoveUnderADescendant_IsRefusedWithCycle_AndNothingMoves()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var root = await CreateAsync(source, "Reports", null, ct);
            var child = await CreateAsync(source, "Nightly", root, ct);

            var moved = Parse(await DarlingMcpQueryTagTools.UpdateQueryTag(source, root, $"{{\"parent_id\":{child}}}", ct));

            Assert.Equal("cycle", (string?)moved["refusal"]);
            Assert.Equal(0, await ScalarAsync(owner, $"SELECT count(*) FROM config.query_tags WHERE id = {root} AND parent_id IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task AssignByHash_ThenList_ShowsTheAssignmentUnderItsTag()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var tag = await CreateAsync(source, "Reports", null, ct);
            var other = await CreateAsync(source, "Etl", null, ct);

            var assigned = Parse(await AssignHashAsync(source, tag, null, ct));
            var listed = Parse(await DarlingMcpQueryTagTools.ListQueryTags(source, ct));

            Assert.Equal("assigned", (string?)assigned["status"]);
            var tags = listed["tags"]!.AsArray();
            var mine = tags.Single(t => (int)t!["tag_id"]! == tag)!;
            var theirs = tags.Single(t => (int)t!["tag_id"]! == other)!;
            var row = Assert.Single(mine["assignments"]!.AsArray())!;
            Assert.Equal("hash", (string?)row["kind"]);
            Assert.Equal(Hash, (string?)row["query_hash"]);
            Assert.Null(row["server_id"]);
            Assert.Empty(theirs["assignments"]!.AsArray());
        });
    }

    [Theory]
    [InlineData(null, "dbo", "usp_Report", "database_name")]
    [InlineData("Sales", null, "usp_Report", "schema_name")]
    [InlineData("Sales", "dbo", null, "object_name")]
    public async Task AssignByObject_WithAMissingPart_IsRefused_AndWritesNothing(string? database, string? schema, string? obj, string missing)
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var tag = await CreateAsync(source, "Reports", null, ct);

            var refused = Parse(await DarlingMcpQueryTagTools.AssignQueryTag(source, tag, "object", null, database, schema, obj, cancellationToken: ct));

            Assert.Equal("invalid", (string?)refused["status"]);
            Assert.Contains(missing, (string?)refused["message"], StringComparison.Ordinal);
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_assignments", ct));
        });
    }

    [Fact]
    public async Task AssignByObject_WithAllThreeParts_IsStoredAndListed()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var tag = await CreateAsync(source, "Reports", null, ct);

            var assigned = Parse(await DarlingMcpQueryTagTools.AssignQueryTag(source, tag, "object", null, "Sales", "dbo", "usp_Report", cancellationToken: ct));
            var listed = Parse(await DarlingMcpQueryTagTools.ListQueryTags(source, ct));

            Assert.Equal("assigned", (string?)assigned["status"]);
            var row = Assert.Single(listed["tags"]![0]!["assignments"]!.AsArray())!;
            Assert.Equal("hand_object", (string?)row["source"]);
            Assert.Equal("usp_Report", (string?)row["object_name"]);
        });
    }

    [Fact]
    public async Task AssigningTheSameKeyAgain_MovesItToTheNewTag_AndSaysSo()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var first = await CreateAsync(source, "Reports", null, ct);
            var second = await CreateAsync(source, "Etl", null, ct);
            await AssignHashAsync(source, first, null, ct);

            var moved = Parse(await AssignHashAsync(source, second, null, ct));

            Assert.Equal("moved", (string?)moved["status"]);
            Assert.Equal(first, (int)moved["replaced_tag"]!["tag_id"]!);
            Assert.Contains("one tag per key", (string?)moved["message"], StringComparison.Ordinal);
            Assert.Contains("next map pass", (string?)moved["note"], StringComparison.Ordinal);
            Assert.Equal(1, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_assignments", ct));
            Assert.Equal(second, await ScalarAsync(owner, "SELECT tag_id FROM config.query_tag_assignments", ct));
        });
    }

    [Fact]
    public async Task AKeyOnOneServer_AndTheSameKeyOnEveryServer_AreTwoAssignments_AndUnassignAddressesOneScope()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var tag = await CreateAsync(source, "Reports", null, ct);
            await AssignHashAsync(source, tag, ServerA, ct);
            await AssignHashAsync(source, tag, null, ct);
            Assert.Equal(2, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_assignments", ct));

            var removed = Parse(await DarlingMcpQueryTagTools.UnassignQueryTag(source, "hash", Hash, server_id: ServerA, cancellationToken: ct));

            Assert.Equal("unassigned", (string?)removed["status"]);
            Assert.Equal(ServerA, (int)removed["removed_assignment"]!["server_id"]!);
            Assert.Equal(1, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_assignments WHERE server_id IS NULL", ct));
            var again = Parse(await DarlingMcpQueryTagTools.UnassignQueryTag(source, "hash", Hash, server_id: ServerA, cancellationToken: ct));
            Assert.Equal("unchanged", (string?)again["status"]);
        });
    }

    [Fact]
    public async Task AnAssignmentToAServerTheRegistryDoesNotHold_IsRefused_AndWritesNothing()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var tag = await CreateAsync(source, "Reports", null, ct);

            var refused = Parse(await AssignHashAsync(source, tag, 999_999, ct));

            Assert.Equal("not_found", (string?)refused["status"]);
            Assert.Equal("unknown_server", (string?)refused["refusal"]);
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_assignments", ct));
        });
    }

    [Fact]
    public async Task DeletingATagWithAssignments_WithoutConfirm_ChangesNothing_AndAnswersConfirmRequiredWithTheCounts()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var parent = await CreateAsync(source, "Reports", null, ct);
            var child = await CreateAsync(source, "Nightly", parent, ct);
            await AssignHashAsync(source, parent, null, ct);
            await AssignHashAsync(source, child, ServerA, ct, "0xFFEEDDCCBBAA9988");
            await ExecAsync(owner, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({child}, 'host', 'web*', 'test')", ct);
            var version = await ScalarAsync(owner, "SELECT rules_version FROM config.query_tag_state", ct);

            var refused = Parse(await DarlingMcpQueryTagTools.DeleteQueryTag(source, parent, false, ct));

            Assert.Equal("confirm_required", (string?)refused["status"]);
            Assert.Equal(1, (int)refused["child_tags"]!);
            Assert.Equal(2, (int)refused["assignments"]!);
            Assert.Equal(1, (int)refused["rules"]!);
            Assert.Equal(2, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags", ct));
            Assert.Equal(2, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_assignments", ct));
            Assert.Equal(version, await ScalarAsync(owner, "SELECT rules_version FROM config.query_tag_state", ct));

            var deleted = Parse(await DarlingMcpQueryTagTools.DeleteQueryTag(source, parent, true, ct));

            Assert.Equal("deleted", (string?)deleted["status"]);
            Assert.Equal(2, (int)deleted["removed_assignments"]!);
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags", ct));
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_assignments", ct));
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_rules", ct));
        });
    }

    [Fact]
    public async Task DeletingAnEmptyTag_NeedsNoConfirm()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var tag = await CreateAsync(source, "Scratch", null, ct);

            var deleted = Parse(await DarlingMcpQueryTagTools.DeleteQueryTag(source, tag, false, ct));

            Assert.Equal("deleted", (string?)deleted["status"]);
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags", ct));
        });
    }

    [Fact]
    public async Task EveryWrite_BumpsTheRulesVersion_LeavesTheMapUnbuilt_AndSaysTheMapTakesEffectOnTheNextPass()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            async Task<long> VersionAsync() => await ScalarAsync(owner, "SELECT rules_version FROM config.query_tag_state", ct);
            var built = await ScalarAsync(owner, "SELECT built_version FROM config.query_tag_state", ct);

            var before = await VersionAsync();
            var created = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "Reports", null, null, ct));
            var tag = (int)created["tag"]!["tag_id"]!;
            Assert.True(await VersionAsync() > before, "create_query_tag bumps rules_version");
            Assert.Contains("next map pass", (string?)created["note"], StringComparison.Ordinal);

            before = await VersionAsync();
            var updated = Parse(await DarlingMcpQueryTagTools.UpdateQueryTag(source, tag, "{\"name\":\"Reporting\"}", ct));
            Assert.True(await VersionAsync() > before, "update_query_tag bumps rules_version");
            Assert.Contains("next map pass", (string?)updated["note"], StringComparison.Ordinal);

            before = await VersionAsync();
            var assigned = Parse(await AssignHashAsync(source, tag, null, ct));
            Assert.True(await VersionAsync() > before, "assign_query_tag bumps rules_version");
            Assert.Contains("next map pass", (string?)assigned["note"], StringComparison.Ordinal);

            before = await VersionAsync();
            var unassigned = Parse(await DarlingMcpQueryTagTools.UnassignQueryTag(source, "hash", Hash, cancellationToken: ct));
            Assert.True(await VersionAsync() > before, "unassign_query_tag bumps rules_version");
            Assert.Contains("next map pass", (string?)unassigned["note"], StringComparison.Ordinal);

            before = await VersionAsync();
            var deleted = Parse(await DarlingMcpQueryTagTools.DeleteQueryTag(source, tag, false, ct));
            Assert.True(await VersionAsync() > before, "delete_query_tag bumps rules_version");
            Assert.Contains("next map pass", (string?)deleted["note"], StringComparison.Ordinal);

            Assert.Equal(built, await ScalarAsync(owner, "SELECT built_version FROM config.query_tag_state", ct));
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tag_map", ct));
            var listed = Parse(await DarlingMcpQueryTagTools.ListQueryTags(source, ct));
            Assert.False((bool)listed["map"]!["current"]!);
        });
    }

    [Fact]
    public async Task List_ReportsAHandAssignmentAsBeatingARuleForTheSameQuery_AndTheRuleCountOnTheRulesTag()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var handTag = await CreateAsync(source, "Hand", null, ct);
            var ruleTag = await CreateAsync(source, "ByRule", null, ct);
            await ExecAsync(owner, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({ruleTag}, 'application', 'Report*', 'test')", ct);
            await AssignHashAsync(source, handTag, ServerA, ct);
            await DarlingMcpQueryTagTools.AssignQueryTag(source, handTag, "object", null, "Sales", "dbo", "usp_Report", cancellationToken: ct);
            await AssignHashAsync(source, handTag, null, ct, "0x1111111111111111");

            var listed = Parse(await DarlingMcpQueryTagTools.ListQueryTags(source, ct));

            var tags = listed["tags"]!.AsArray();
            var hand = tags.Single(t => (int)t!["tag_id"]! == handTag)!;
            var byRule = tags.Single(t => (int)t!["tag_id"]! == ruleTag)!;
            Assert.Equal(1, (int)byRule["rule_count"]!);
            Assert.Equal(0, (int)hand["rule_count"]!);
            var ranks = hand["assignments"]!.AsArray().ToDictionary(a => (string)a!["source"]! + (a["server_id"] is null ? "-all" : "-one"), a => (int)a!["precedence_rank"]!);
            Assert.Equal(1, ranks["hand_hash-one"]);
            Assert.Equal(2, ranks["hand_hash-all"]);
            Assert.Equal(3, ranks["hand_object-all"]);
            Assert.All(hand["assignments"]!.AsArray(), a => Assert.True((bool)a!["beats_rules"]!));
            var order = listed["precedence"]!.AsArray().Select(p => (string)p!).ToArray();
            Assert.StartsWith("hand assignment by hash for the server", order[0], StringComparison.Ordinal);
            Assert.StartsWith("rule", order[^1], StringComparison.Ordinal);
            Assert.Equal("qt-alpha", (string?)hand["assignments"]!.AsArray().Single(a => a!["server_id"] is not null)!["server_name"]);
        });
    }

    [Fact]
    public async Task UpdateQueryTag_RenamesRecolours_AndMovesATag_AndAnEditThatChangesNothingSaysUnchanged()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var a = await CreateAsync(source, "Reports", null, ct);
            var b = await CreateAsync(source, "Etl", null, ct);

            var edited = Parse(await DarlingMcpQueryTagTools.UpdateQueryTag(source, b, $"{{\"name\":\"Loads\",\"colour\":\"#112233\",\"parent_id\":{a}}}", ct));
            var again = Parse(await DarlingMcpQueryTagTools.UpdateQueryTag(source, b, "{\"name\":\"Loads\"}", ct));

            Assert.Equal("updated", (string?)edited["status"]);
            Assert.Equal("Reports / Loads", (string?)edited["tag"]!["path"]);
            Assert.Equal("#112233", (string?)edited["tag"]!["colour"]);
            Assert.Equal("unchanged", (string?)again["status"]);
        });
    }

    /// <summary>The writes run as the <c>mcp</c> role, so a missing grant would pass every owner-run test above and fail only in a deployment with a 42501.
    /// This runs the five writes and the list as a NOSUPERUSER role holding only the grants <see cref="DarlingManagedRoles.BuildProvisioningSql"/>
    /// gives <c>mcp</c> for the query-tag tables (taken from its output, role name swapped), so dropping a grant fails here.</summary>
    [Fact]
    public async Task TheMcpRoleGrants_LetTheQueryTagToolsCreateAssignMoveUnassignDeleteAndList()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the mcp-role query-tag pin.");
        var ct = TestContext.Current.CancellationToken;
        var roleName = "qtag_mcp_" + Guid.NewGuid().ToString("N")[..8];
        var rolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = new NpgsqlConnection(ownerString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        await ExecAsync(owner, $"INSERT INTO servers (server_id, server_name) VALUES ({ServerA}, 'qt-alpha')", ct);

        var grants = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp)
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("GRANT ", StringComparison.Ordinal)
                && line.EndsWith(" TO mcp;", StringComparison.Ordinal)
                && (line.Contains("config.query_tags ", StringComparison.Ordinal) || line.Contains("config.query_tag_assignments ", StringComparison.Ordinal)))
            .Select(line => line[..^" TO mcp;".Length] + " TO " + roleName + ";")
            .ToList();
        Assert.Equal(2, grants.Count);

        await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'", ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"GRANT USAGE ON SCHEMA config, collect TO {roleName}; GRANT SELECT ON ALL TABLES IN SCHEMA config TO {roleName}; GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {roleName};", ct);
            await ExecAsync(owner, $"GRANT SELECT ON servers TO {roleName};", ct);
            foreach (var grant in grants)
            {
                await ExecAsync(owner, grant, ct);
            }

            var asMcp = new NpgsqlConnectionStringBuilder(ownerString) { Username = roleName, Password = rolePassword }.ConnectionString;
            await using var source = NpgsqlDataSource.Create(asMcp);

            var first = await CreateAsync(source, "Reports", null, ct);
            var second = await CreateAsync(source, "Etl", null, ct);
            Assert.Equal("assigned", (string?)Parse(await AssignHashAsync(source, first, ServerA, ct))["status"]);
            Assert.Equal("moved", (string?)Parse(await AssignHashAsync(source, second, ServerA, ct))["status"]);
            Assert.Equal("ok", (string?)Parse(await DarlingMcpQueryTagTools.ListQueryTags(source, ct))["status"]);
            Assert.Equal("unassigned", (string?)Parse(await DarlingMcpQueryTagTools.UnassignQueryTag(source, "hash", Hash, server_id: ServerA, cancellationToken: ct))["status"]);
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpQueryTagTools.DeleteQueryTag(source, second, false, ct))["status"]);
            Assert.Equal(1, await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, token) =>
            {
                await ExecAsync(cleanup, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", token);
            });
        }
    }
}
