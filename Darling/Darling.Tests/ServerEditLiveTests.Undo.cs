/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #6403 server-admin group: add_servers, edit_server and remove_server answer with the previous value and an undo call in the
/// {tool, arguments} shape. Every fact calls the tool, REPLAYS the returned undo through the real tool, and reads the row back.
/// </summary>
public sealed partial class ServerEditLiveTests
{
    private const string DefinitionSignatureSql = @"SELECT concat_ws('|', name, host, port, database, read_only_intent, auth, username, encrypt_mode,
        trust_server_certificate, multi_subnet_failover, monthly_cost_usd::bigint, engine) FROM config_monitored_servers WHERE server_id = ";

    /// <summary>Runs an answer's undo through the real tool it names.</summary>
    private static async Task<string> ReplayEditAsync(NpgsqlDataSource mcp, JsonNode? undo, CancellationToken ct)
    {
        Assert.NotNull(undo);
        Assert.Equal("edit_server", (string)undo!["tool"]!);
        var arguments = undo["arguments"]!.AsObject();
        return await Edit.EditServerByNameAsync(mcp, (string)arguments["server_name"]!, (string)arguments["changes_json"]!, Reachable, TestKeyRings.Healthy, null, ct);
    }

    [Fact]
    public async Task EditServer_AnswersWithPreviousAndAnUndo_ThatPutsTheChangedFieldsBack_AndASecondReplayIsAConflict()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5301, "zeta-01", "zeta-01.example.test", "integrated", null, null, ct);
            var before = await ScalarAsync<string>(rig.Owner, DefinitionSignatureSql + 5301, ct);

            var edited = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "zeta-01.example.test",
                "{\"display_name\":\"Orders\",\"monthly_cost_usd\":120,\"host\":\"zeta-09.example.test\",\"trust_server_certificate\":true}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", (string)edited["status"]!);
            Assert.NotEqual(before, await ScalarAsync<string>(rig.Owner, DefinitionSignatureSql + 5301, ct));
            Assert.Equal("zeta-01", (string)edited["previous"]!["display_name"]!);
            Assert.Equal("zeta-01.example.test", (string)edited["previous"]!["host"]!);
            Assert.False((bool)edited["previous"]!["trust_server_certificate"]!);
            Assert.Null(edited["undo_note"]);

            var undone = Parse(await ReplayEditAsync(rig.Mcp, edited["undo"], ct));
            Assert.Equal("updated", (string)undone["status"]!);
            Assert.Equal(before, await ScalarAsync<string>(rig.Owner, DefinitionSignatureSql + 5301, ct));

            /* The undo carries the modified_at the edit returned, so a change made after the edit makes the replay a conflict, not an overwrite. */
            var second = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "zeta-01.example.test", "{\"display_name\":\"Second\"}", Reachable, TestKeyRings.Healthy, null, ct));
            var later = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "zeta-01.example.test", "{\"monthly_cost_usd\":9}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", (string)later["status"]!);
            var stale = Parse(await ReplayEditAsync(rig.Mcp, second["undo"], ct));
            Assert.Equal("conflict", (string)stale["status"]!);
            Assert.Equal("Second", await ScalarAsync<string>(rig.Owner, "SELECT name FROM config_monitored_servers WHERE server_id = 5301", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task EditServer_APasswordChange_IsNeverInPreviousOrTheUndo_AndTheNoteSaysSo()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5302, "zeta-02", "zeta-02.example.test", "sql", "monitor", "rem-blob-b2", ct);

            var passwordOnly = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "zeta-02.example.test", "{\"password\":\"Synth-Undo-Pw-2\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", (string)passwordOnly["status"]!);
            Assert.Null(passwordOnly["undo"]);
            Assert.Contains("password", (string)passwordOnly["undo_note"]!, StringComparison.Ordinal);
            Assert.DoesNotContain("Synth-Undo-Pw-2", passwordOnly.ToJsonString(), StringComparison.Ordinal);

            var mixed = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "zeta-02.example.test",
                "{\"display_name\":\"Renamed\",\"password\":\"Synth-Undo-Pw-3\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", (string)mixed["status"]!);
            var arguments = mixed["undo"]!["arguments"]!.AsObject();
            var changes = JsonNode.Parse((string)arguments["changes_json"]!)!.AsObject();
            Assert.Equal("zeta-02", (string)changes["display_name"]!);
            Assert.False(changes.ContainsKey("password"));
            Assert.DoesNotContain("Synth-Undo-Pw", mixed.ToJsonString(), StringComparison.Ordinal);
            Assert.NotNull(mixed["undo_note"]);

            Assert.Equal("updated", (string)Parse(await ReplayEditAsync(rig.Mcp, mixed["undo"], ct))["status"]!);
            Assert.Equal("zeta-02", await ScalarAsync<string>(rig.Owner, "SELECT name FROM config_monitored_servers WHERE server_id = 5302", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task EditServer_ClearingAPortSavedBeforeTheRule_SaysItsUndoWillBeRefused_AndTheReplayWritesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5305, "zeta-05", "zeta-05.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner, "UPDATE config_monitored_servers SET port = 1444 WHERE server_id = 5305", ct);

            var cleared = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "zeta-05.example.test", "{\"port\":0}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", (string)cleared["status"]!);
            Assert.Equal(1444, (int)cleared["previous"]!["port"]!);
            Assert.Contains("refused", (string)cleared["undo_note"]!, StringComparison.Ordinal);
            Assert.Contains("1444", (string)cleared["undo_note"]!, StringComparison.Ordinal);

            var signature = await ScalarAsync<string>(rig.Owner, DefinitionSignatureSql + 5305, ct);
            var replay = Parse(await ReplayEditAsync(rig.Mcp, cleared["undo"], ct));
            Assert.Equal("invalid", (string)replay["status"]!);
            Assert.Equal(signature, await ScalarAsync<string>(rig.Owner, DefinitionSignatureSql + 5305, ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AddServers_EachAddedEntryCarriesTheRemoveUndo_AndASkippedEntryCarriesNone()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5303, "zeta-03", "zeta-03.example.test", "integrated", null, null, ct);
            var answer = Parse(await Edit.AddServersAsync(rig.Owner,
                "[{\"host\":\"zeta-10.example.test\"},{\"host\":\"zeta-03.example.test\"}]", Reachable, ct, TestKeyRings.Healthy));
            Assert.Equal(1, (int)answer["added"]!);
            var results = answer["results"]!.AsArray();
            Assert.Equal("added", (string)results[0]!["status"]!);
            Assert.Equal("duplicate", (string)results[1]!["status"]!);
            Assert.Null(results[1]!["undo"]);

            var undo = results[0]!["undo"];
            Assert.NotNull(undo);
            Assert.Equal("remove_server", (string)undo!["tool"]!);
            Assert.Equal(2L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers", ct));

            var removed = Parse(await Edit.RemoveServer(rig.Owner, (string)undo["arguments"]!["server_name"]!));
            Assert.Equal("removed", (string)removed["status"]!);
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers", ct));
            Assert.Equal(5303L, await ScalarAsync<long>(rig.Owner, "SELECT server_id FROM config_monitored_servers", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task RemoveServer_AnswersWithThePreviousDefinition_AndTheUndoReAddsIt_WithoutTheTagsItDeleted()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5304, "zeta-04", "zeta-04.example.test", "sql", "monitor", "rem-blob-b4", ct);
            await ExecAsync(rig.Owner, "UPDATE config_monitored_servers SET trust_server_certificate = TRUE, encrypt_mode = 'Strict', read_only_intent = TRUE WHERE server_id = 5304", ct);
            var tags = new ServerTagStore(rig.Owner, 30);
            var tag = Assert.IsType<ServerTagWriteResult.Ok>(await tags.CreateAsync("East", null, null, ct)).Tag!;
            await tags.AssignAsync(tag.Id, [5304], ct);

            var removed = Parse(await Edit.RemoveServer(rig.Owner, "zeta-04"));
            Assert.Equal("removed", (string)removed["status"]!);
            Assert.Equal(0L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers", ct));
            var previous = removed["previous"]!;
            Assert.Equal("zeta-04.example.test", (string)previous["host"]!);
            Assert.Equal("Strict", (string)previous["encrypt_mode"]!);
            Assert.Equal(Edit.PasswordPlaceholder, (string)previous["password"]!);
            Assert.DoesNotContain("rem-blob-b4", removed.ToJsonString(), StringComparison.Ordinal);
            Assert.Contains("tag assignments", (string)removed["undo_note"]!, StringComparison.Ordinal);

            var undo = removed["undo"]!;
            Assert.Equal("add_servers", (string)undo["tool"]!);
            var serversJson = ((string)undo["arguments"]!["servers_json"]!).Replace(Edit.PasswordPlaceholder, "Synth-Undo-Pw-4", StringComparison.Ordinal);
            var replay = Parse(await Edit.AddServersAsync(rig.Owner, serversJson, Reachable, ct, TestKeyRings.Healthy));
            Assert.Equal(1, (int)replay["added"]!);

            /* The definition is back (same id: it is derived from the connection); the tag assignment the removal deleted is not. */
            Assert.Equal("zeta-04|zeta-04.example.test|sql|monitor|Strict|t|t",
                await ScalarAsync<string>(rig.Owner, "SELECT concat_ws('|', name, host, auth, username, encrypt_mode, trust_server_certificate, read_only_intent) FROM config_monitored_servers", ct));
            Assert.Equal(0L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM server_tag_map", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }
}
