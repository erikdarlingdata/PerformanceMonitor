/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// #6398: the known-pattern write tools as the provisioned, NOSUPERUSER <c>mcp</c> role (the shipped provisioning batch, run whole), so a missing
/// EXECUTE grant fails here. Each write is compared with what the web route writes for the same input (<see cref="PgKnownPatternStore.BuildInputAsync"/>
/// then the store call), each refusal writes nothing, each undo call replays and restores the row, and the role still has no direct write on the table.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class DarlingMcpKnownPatternWriteToolsLiveTests
{
    private const int ServerA = 7611;

    /// <summary>Has a definition (<c>config.config_monitored_servers</c>, what the editors' pickers list) and no <c>servers</c> row: added, not yet connected.</summary>
    private const int ServerDefinedOnly = 7612;

    /// <summary>Has a <c>servers</c> row and no definition: removed, its history kept.</summary>
    private const int ServerRegistryOnly = 7613;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonNode Parse(string json) => JsonNode.Parse(json)!;

    private const string RowSql =
        "SELECT server_id, metric_name, time_of_day::text, time_zone, window_minutes, note, expires_at_utc::text FROM config.config_known_patterns WHERE id = {0}";

    [Fact]
    public async Task CreateUpdateDelete_WriteWhatTheWebRouteWrites_AndEachUndoReplaysAndRestoresTheRow()
    {
        await RunAsync(async (mcp, owner, ct) =>
        {
            /* Create: the tool and the web route's path (BuildInputAsync, then the store insert) for the same input store the same row. */
            var created = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(
                mcp, "Backup job runs here", ServerA, "High CPU", "02:00", "America/New_York", 30, "2099-12-31T00:00:00Z", cancellationToken: ct));
            Assert.Equal("created", (string)created["status"]!);
            var id = (long)created["id"]!;
            Assert.Null(created["before"]);
            Assert.Equal("Backup job runs here", (string)created["after"]!["note"]!);

            var (input, refusal) = await PgKnownPatternStore.BuildInputAsync(
                ServerA, "High CPU", "02:00", "America/New_York", 30, "Backup job runs here", "2099-12-31T00:00:00Z",
                i => PgKnownPatternStore.MonitoredServerNameAsync(mcp, i));
            Assert.Null(refusal);
            var webId = await new PgKnownPatternStore(mcp).InsertAsync(input!, "web", ct);
            Assert.Equal(await RowAsync(owner, webId, ct), await RowAsync(owner, id, ct));
            Assert.StartsWith("mcp:", await ScalarAsync(owner, $"SELECT created_by FROM config.config_known_patterns WHERE id = {id}", ct));

            /* Update replaces the whole entry, as the web PUT does; the answer carries both images and an undo that puts the old values back. */
            var updated = Parse(await DarlingMcpKnownPatternWriteTools.UpdateKnownPattern(mcp, id, "Now a quiet pattern", metric_name: "Memory", cancellationToken: ct));
            Assert.Equal("updated", (string)updated["status"]!);
            Assert.Equal("Backup job runs here", (string)updated["before"]!["note"]!);
            Assert.Equal("Now a quiet pattern", (string)updated["after"]!["note"]!);
            Assert.Null(updated["after"]!["server_id"]);
            Assert.Equal("update_known_pattern", (string)updated["undo"]!["tool"]!);

            var webInput = (await PgKnownPatternStore.BuildInputAsync(
                null, "Memory", null, null, null, "Now a quiet pattern", null, i => PgKnownPatternStore.MonitoredServerNameAsync(mcp, i))).Input!;
            Assert.True(await new PgKnownPatternStore(mcp).UpdateAsync(webId, webInput, "web", ct));
            Assert.Equal(await RowAsync(owner, webId, ct), await RowAsync(owner, id, ct));

            var original = await RowAsync(owner, webId, ct);
            await ReplayAsync(mcp, updated["undo"]!, ct);
            Assert.Equal("Backup job runs here", (await RowAsync(owner, id, ct)).Split('|')[5]);
            Assert.Equal(ServerA.ToString(System.Globalization.CultureInfo.InvariantCulture), (await RowAsync(owner, id, ct)).Split('|')[0]);
            Assert.Equal("America/New_York", (await RowAsync(owner, id, ct)).Split('|')[3]);
            Assert.NotEqual(original, await RowAsync(owner, id, ct));

            /* Delete: the answer has the row as it was; its undo creates it again under a new id with the same fields. */
            var beforeDelete = await RowAsync(owner, id, ct);
            var deleted = Parse(await DarlingMcpKnownPatternWriteTools.DeleteKnownPattern(mcp, id, cancellationToken: ct));
            Assert.Equal("deleted", (string)deleted["status"]!);
            Assert.Equal("Backup job runs here", (string)deleted["before"]!["note"]!);
            Assert.Null(deleted["after"]);
            Assert.Equal("", await RowAsync(owner, id, ct));
            var recreated = await ReplayAsync(mcp, deleted["undo"]!, ct);
            Assert.NotEqual(id, (long)recreated["id"]!);
            Assert.Equal(beforeDelete, await RowAsync(owner, (long)recreated["id"]!, ct));

            /* Create's undo deletes the row it made. */
            var fresh = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "undo me", cancellationToken: ct));
            var freshId = (long)fresh["id"]!;
            Assert.NotEqual("", await RowAsync(owner, freshId, ct));
            var undoCreate = await ReplayAsync(mcp, fresh["undo"]!, ct);
            Assert.Equal("deleted", (string)undoCreate["status"]!);
            Assert.Equal("", await RowAsync(owner, freshId, ct));
        });
    }

    public static IEnumerable<object[]> Refusals() =>
    [
        [string.Empty, null!, null!, null!, null!, null!, "note"],
        [new string('x', 501), null!, null!, null!, null!, null!, "note"],
        ["ok", null!, null!, "Not/A_Zone", null!, null!, "time_zone"],
        ["ok", null!, "25:99", null!, null!, null!, "time_of_day"],
        ["ok", null!, null!, null!, null!, "not a date", "expires_at_utc"],
        ["ok", null!, null!, null!, 99999, null!, "window_minutes"],
        ["ok", 424242, null!, null!, null!, null!, "server_id"],
    ];

    [Theory]
    [MemberData(nameof(Refusals))]
    public async Task EveryRefusalTheWebRouteHas_IsRefusedWithTheSameSentence_AndWritesNothing(
        string note, int? serverId, string? timeOfDay, string? zone, int? window, string? expires, string parameter)
    {
        await RunAsync(async (mcp, owner, ct) =>
        {
            var seeded = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "seed", cancellationToken: ct));
            var seedId = (long)seeded["id"]!;
            var rows = await ScalarAsync(owner, "SELECT count(*) FROM config.config_known_patterns", ct);
            var seedRow = await RowAsync(owner, seedId, ct);

            var (_, webRefusal) = await PgKnownPatternStore.BuildInputAsync(
                serverId, null, timeOfDay, zone, window, note, expires, i => PgKnownPatternStore.MonitoredServerNameAsync(mcp, i));
            Assert.NotNull(webRefusal);

            var create = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, note, serverId, null, timeOfDay, zone, window, expires, cancellationToken: ct));
            var update = Parse(await DarlingMcpKnownPatternWriteTools.UpdateKnownPattern(mcp, seedId, note, serverId, null, timeOfDay, zone, window, expires, cancellationToken: ct));
            foreach (var answer in new[] { create, update })
            {
                Assert.Equal("invalid", (string)answer["status"]!);
                Assert.Equal(parameter, (string)answer["hints"]!["parameter"]!);
                Assert.StartsWith(webRefusal!, (string)answer["message"]!, StringComparison.Ordinal);
            }

            Assert.Equal(rows, await ScalarAsync(owner, "SELECT count(*) FROM config.config_known_patterns", ct));
            Assert.Equal(seedRow, await RowAsync(owner, seedId, ct));
        });
    }

    [Fact]
    public async Task ACreatePastTheCap_IsRefusedWithTheCountAndTheCap_AndWritesNothing_While499Succeeds()
    {
        await RunAsync(async (mcp, owner, ct) =>
        {
            const int cap = PerformanceMonitor.Common.KnownPatterns.KnownPatternMatcher.MaxEntries;
            await using (var fill = new NpgsqlCommand($"INSERT INTO config.config_known_patterns (note) SELECT 'bulk ' || g FROM generate_series(1, {cap - 1}) g", owner))
            {
                await fill.ExecuteNonQueryAsync(ct);
            }

            /* 499 rows: the create succeeds and makes the 500th. */
            var last = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "the 500th", cancellationToken: ct));
            Assert.Equal("created", (string)last["status"]!);
            Assert.Equal(cap.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(owner, "SELECT count(*) FROM config.config_known_patterns", ct));

            /* 500 rows: refused in plain words, nothing written. */
            var refused = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "the 501st", cancellationToken: ct));
            Assert.Equal("invalid", (string)refused["status"]!);
            var message = (string)refused["message"]!;
            /* The store's whole sentence plus the tool's tail: no ".NET (Parameter 'input')" wedged between them. */
            Assert.Equal("Known patterns are capped at 500 entries and 500 exist. Delete entries you no longer need to make room. Nothing was changed.", message);
            /* The hint comes from the store's DL409, not from the sentence's first words. */
            Assert.Equal("known_patterns", (string)refused["hints"]!["parameter"]!);
            Assert.Equal(cap.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(owner, "SELECT count(*) FROM config.config_known_patterns", ct));

            /* Edit and delete still work at the cap, and the delete makes room for one create. */
            var id = (long)last["id"]!;
            Assert.Equal("updated", (string)Parse(await DarlingMcpKnownPatternWriteTools.UpdateKnownPattern(mcp, id, "edited at the cap", cancellationToken: ct))["status"]!);
            Assert.Equal("deleted", (string)Parse(await DarlingMcpKnownPatternWriteTools.DeleteKnownPattern(mcp, id, cancellationToken: ct))["status"]!);
            Assert.Equal("created", (string)Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "room again", cancellationToken: ct))["status"]!);
        });
    }

    [Fact]
    public async Task ADefinedServerThatNeverConnected_CanBeScopedTo_AndAServerWithOnlyARegistryRowCannot_WithTheServerIdHint()
    {
        await RunAsync(async (mcp, owner, ct) =>
        {
            /* The editors' pickers list config_monitored_servers; a server added a moment ago has no servers row yet (it is written on the first connect). */
            var created = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "new server", ServerDefinedOnly, cancellationToken: ct));
            Assert.Equal("created", (string)created["status"]!);
            var id = (long)created["id"]!;
            Assert.Equal(ServerDefinedOnly, (int)created["after"]!["server_id"]!);
            Assert.Equal("updated", (string)Parse(await DarlingMcpKnownPatternWriteTools.UpdateKnownPattern(mcp, id, "new server, edited", ServerDefinedOnly, cancellationToken: ct))["status"]!);

            /* A removed server keeps its servers row but no editor lists it: refused in the store's registry's words, naming server_id, writing nothing. */
            var rows = await ScalarAsync(owner, "SELECT count(*) FROM config.config_known_patterns", ct);
            foreach (var answer in new[]
            {
                Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "removed server", ServerRegistryOnly, cancellationToken: ct)),
                Parse(await DarlingMcpKnownPatternWriteTools.UpdateKnownPattern(mcp, id, "removed server", ServerRegistryOnly, cancellationToken: ct)),
            })
            {
                Assert.Equal("invalid", (string)answer["status"]!);
                Assert.Equal("server_id", (string)answer["hints"]!["parameter"]!);
                Assert.StartsWith($"server_id {ServerRegistryOnly} is not a registered server.", (string)answer["message"]!, StringComparison.Ordinal);
            }

            Assert.Equal(rows, await ScalarAsync(owner, "SELECT count(*) FROM config.config_known_patterns", ct));
            Assert.Equal("new server, edited", (await RowAsync(owner, id, ct)).Split('|')[5]);
        });
    }

    [Fact]
    public async Task AStoreRefusal_IsHintedFromItsSqlState_NotFromItsWording()
    {
        await RunAsync(async (mcp, owner, ct) =>
        {
            /* The store's own DL404 (a server removed between the tool's check and the write), reached through the real function. */
            var store = new PgKnownPatternStore(mcp);
            var seeded = await store.InsertAsync(new PerformanceMonitor.Common.KnownPatterns.KnownPatternInput(null, null, null, null, null, "keep", null), "seed", ct);
            var ghost = new PerformanceMonitor.Common.KnownPatterns.KnownPatternInput(ServerRegistryOnly, null, null, null, null, "ghost", null);

            foreach (var refusal in new[]
            {
                (await DarlingMcpKnownPatternWriteTools.Guarded(() => store.InsertAsync(ghost, "x", ct))).Refusal,
                (await DarlingMcpKnownPatternWriteTools.Guarded(() => store.UpdateAsync(seeded, ghost, "x", ct))).Refusal,
            })
            {
                var answer = Parse(DarlingMcpKnownPatternWriteTools.Refused(refusal!.Value.Sentence, refusal.Value.Parameter));
                Assert.Equal("invalid", (string)answer["status"]!);
                Assert.Equal("server_id", (string)answer["hints"]!["parameter"]!);
                Assert.Equal("No monitored server with that id. Nothing was changed.", (string)answer["message"]!);
            }

            Assert.Equal("keep", (await RowAsync(owner, seeded, ct)).Split('|')[5]);
        });
    }

    [Fact]
    public async Task AnIdThatMatchesNoEntry_IsNotFound_ForUpdateAndDelete_AndChangesNothing()
    {
        await RunAsync(async (mcp, owner, ct) =>
        {
            Assert.Equal("not_found", (string)Parse(await DarlingMcpKnownPatternWriteTools.UpdateKnownPattern(mcp, 987654, "x", cancellationToken: ct))["status"]!);
            Assert.Equal("not_found", (string)Parse(await DarlingMcpKnownPatternWriteTools.DeleteKnownPattern(mcp, 987654, cancellationToken: ct))["status"]!);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_known_patterns", ct));
        });
    }

    [Fact]
    public async Task TheMcpRole_StillHasNoDirectInsertUpdateOrDeleteOnTheTable()
    {
        await RunAsync(async (mcp, owner, ct) =>
        {
            var created = Parse(await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(mcp, "seed", cancellationToken: ct));
            var id = (long)created["id"]!;
            foreach (var sql in new[]
            {
                "INSERT INTO config.config_known_patterns (note) VALUES ('direct')",
                $"UPDATE config.config_known_patterns SET note = 'direct' WHERE id = {id}",
                $"DELETE FROM config.config_known_patterns WHERE id = {id}",
            })
            {
                await using var command = mcp.CreateCommand(sql);
                var ex = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync(ct));
                Assert.Equal("42501", ex.SqlState);
            }

            Assert.Equal("seed", (await RowAsync(owner, id, ct)).Split('|')[5]);
        });
    }

    /// <summary>Replays an answer's <c>undo</c> call through the tool it names, the way a client would.</summary>
    private static async Task<JsonNode> ReplayAsync(NpgsqlDataSource mcp, JsonNode undo, CancellationToken ct)
    {
        var a = undo["arguments"]!;
        string? S(string name) => a[name]?.GetValue<string>();
        int? I(string name) => a[name]?.GetValue<int>();
        var json = (string)undo["tool"]! switch
        {
            "delete_known_pattern" => await DarlingMcpKnownPatternWriteTools.DeleteKnownPattern(mcp, a["id"]!.GetValue<long>(), cancellationToken: ct),
            "update_known_pattern" => await DarlingMcpKnownPatternWriteTools.UpdateKnownPattern(
                mcp, a["id"]!.GetValue<long>(), S("note")!, I("server_id"), S("metric_name"), S("time_of_day"), S("time_zone"), I("window_minutes"), S("expires_at_utc"), cancellationToken: ct),
            "create_known_pattern" => await DarlingMcpKnownPatternWriteTools.CreateKnownPattern(
                mcp, S("note")!, I("server_id"), S("metric_name"), S("time_of_day"), S("time_zone"), I("window_minutes"), S("expires_at_utc"), cancellationToken: ct),
            var other => throw new InvalidOperationException("Unexpected undo tool " + other),
        };
        return Parse(json);
    }

    /// <summary>The columns a write sets, as one text line; empty when the row does not exist.</summary>
    private static async Task<string> RowAsync(NpgsqlConnection owner, long id, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(string.Format(System.Globalization.CultureInfo.InvariantCulture, RowSql, id), owner);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return string.Empty;
        }

        var parts = new string[reader.FieldCount];
        for (var i = 0; i < parts.Length; i++)
        {
            parts[i] = reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!;
        }

        return string.Join('|', parts);
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection owner, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, owner);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the known-pattern write tool tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs to something else. */
            await using var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner);
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!, "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using var ownerConnection = new NpgsqlConnection(scratch.ConnectionString);
            await ownerConnection.OpenAsync(ct);
            var ownerRole = new NpgsqlConnectionStringBuilder(ownerConnection.ConnectionString).Username ?? "darling";

            // The shipped batch, run whole: every grant the managed mcp role really holds.
            var batch = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
            await using (var provision = new NpgsqlCommand(batch, ownerConnection))
            {
                await provision.ExecuteNonQueryAsync(ct);
            }

            await using (var seed = new NpgsqlCommand(
                $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({ServerA}, 'srv{ServerA}', 'srv{ServerA}', TRUE, 16, now(), now()); " +
                $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerA}, 'kp-a', 'kp-a', TRUE) ON CONFLICT DO NOTHING; " +
                $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerDefinedOnly}, 'kp-b', 'kp-b', TRUE) ON CONFLICT DO NOTHING; " +
                $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({ServerRegistryOnly}, 'srv{ServerRegistryOnly}', 'srv{ServerRegistryOnly}', FALSE, 16, now(), now())", ownerConnection))
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);

            await body(mcp, ownerConnection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, web, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
