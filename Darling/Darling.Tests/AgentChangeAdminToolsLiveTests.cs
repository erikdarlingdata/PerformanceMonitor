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
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Tools = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpAgentChangeAdminTools;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database and role through ScratchPostgres and never touches another test's rows.
   The role it connects as is created for the fact and dropped in its cleanup; the grant replay names cluster-wide objects, so the class
   runs in pg-cluster-roles like the other mcp-role live classes. */

/// <summary>
/// #6401: <c>set_agent_actions_enabled</c> and <c>undo_agent_change</c> against a live store, connected as the <c>mcp</c> role with the
/// grants <see cref="DarlingManagedRoles.BuildProvisioningSql"/> hands it, so removing a grant a tool needs makes these fail with 42501.
/// The journal rows are written directly (the tools that make the changes are other slices).
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class AgentChangeAdminToolsLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
    private const string Actor = "mcp:token abc12345, client says: test-client 1.0";

    private sealed record Rig(ScratchPostgres Scratch, NpgsqlDataSource Owner, NpgsqlDataSource Mcp, string Role) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Mcp.DisposeAsync();
            await Owner.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static List<string> McpStatements(string roleName, string database)
    {
        var provisioning = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var statements = new List<string>();
        var uncommented = string.Join('\n', provisioning.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));
        foreach (var raw in uncommented.Split(';'))
        {
            var statement = raw.Trim();
            var isGrant = statement.StartsWith("GRANT ", StringComparison.Ordinal);
            if (!isGrant && !statement.StartsWith("REVOKE ", StringComparison.Ordinal))
            {
                continue;
            }

            var marker = isGrant ? " TO " : " FROM ";
            var at = statement.LastIndexOf(marker, StringComparison.Ordinal);
            if (at < 0 || ViewerGrantReplay.CannotReplayOnScratchStore(statement))
            {
                continue;
            }

            if (!statement[(at + marker.Length)..].Split(',').Select(t => t.Trim()).Contains("mcp", StringComparer.Ordinal))
            {
                continue;
            }

            statements.Add(ViewerGrantReplay.OnDatabase(statement[..at], database) + marker + roleName);
        }

        return statements;
    }

    private static async Task<Rig> OpenAsync(CancellationToken ct, bool seedServiceRow = true)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the agent change admin tool live tests (each mints its own scratch database and role).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var owner = NpgsqlDataSource.Create(ownerString);
        var role = "agent_admin_" + Guid.NewGuid().ToString("N")[..8];
        await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        foreach (var statement in McpStatements(role, scratch.DatabaseName))
        {
            await ExecAsync(owner, statement, ct);
        }

        if (seedServiceRow)
        {
            await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        }

        var mcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(ownerString) { Username = role, Password = RolePassword }.ConnectionString);
        return new Rig(scratch, owner, mcp, role);
    }

    private static async Task DropRoleAsync(Rig rig, bool bodySucceeded) =>
        await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
            ExecAsync(rig.Owner, $"DROP OWNED BY {rig.Role}; DROP ROLE IF EXISTS {rig.Role};", CancellationToken.None));

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static async Task<long> JournalAsync(NpgsqlDataSource owner, string journalEvent, Guid request, int serverId, string detail, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(
            "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, detail) VALUES (now() - interval '1 hour', $1, $2, $3, 'agent:a', $4::jsonb) RETURNING journal_id");
        command.Parameters.AddWithValue(request);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(journalEvent);
        command.Parameters.AddWithValue(detail);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>An agent change as the Level 2 tools journal it: the called row with the before image, and a done row with the after.</summary>
    private static async Task<long> ChangeAsync(NpgsqlDataSource owner, string kind, string targetId, int serverId, JsonNode? before, JsonNode? after, CancellationToken ct)
    {
        var request = Guid.NewGuid();
        var called = await JournalAsync(owner, "action_called", request, serverId, new JsonObject
        {
            ["tool"] = kind + "_tool", ["kind"] = kind, ["claim_no"] = 1,
            ["target"] = new JsonObject { ["id"] = targetId, ["server_id"] = serverId, ["database"] = null },
            ["evidence"] = new JsonArray(), ["before"] = before,
        }.ToJsonString(), ct);
        await JournalAsync(owner, "action_done", request, serverId,
            new JsonObject { ["called_id"] = called, ["after"] = after }.ToJsonString(), ct);
        return called;
    }

    private static JsonObject MuteImage(string reason, bool enabled) => new()
    {
        ["enabled"] = enabled, ["reason"] = reason, ["metric_name"] = "Blocking", ["server_id"] = 7,
        ["expires_at_utc"] = null, ["server_name"] = null, ["database_pattern"] = null, ["query_text_pattern"] = null,
        ["wait_type_pattern"] = null, ["job_name_pattern"] = null, ["client_app_pattern"] = null,
    };

    private static async Task<int> CountAsync(NpgsqlDataSource owner, string where, CancellationToken ct) =>
        int.Parse((await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE " + where, ct))!, CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheSwitch_TurnsOff_AsTheMcpRole_AndJournalsTheCaller_AndTurningOnIsRefusedWritingNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var off = Parse(await Tools.SwitchAsync(rig.Mcp, null, false, Actor, ct));
            Assert.Equal("off", (string?)off["status"]);
            Assert.True((bool?)off["changed"]);
            Assert.True((bool?)off["before"]!["enabled"]);
            Assert.False((bool?)off["after"]!["enabled"]);
            /* What the web route writes: the service row, and one agent_actions_switched journal row by the caller with {enabled}. */
            Assert.Equal("false|" + Actor, await ScalarAsync(rig.Owner, "SELECT agent_actions_enabled || '|' || updated_by FROM config.config_service WHERE id = 1", ct));
            Assert.Equal(Actor + "|false", await ScalarAsync(rig.Owner,
                "SELECT actor || '|' || (detail->>'enabled') FROM config.investigation_journal WHERE event = 'agent_actions_switched'", ct));

            /* MCP never turns agent actions back on (#6401): refused, and the switch, who set it and the journal are as they were. */
            var on = Parse(await Tools.SwitchAsync(rig.Mcp, null, true, Actor + "-again", ct));
            Assert.Equal("refused", (string?)on["status"]);
            Assert.Equal("on_is_a_persons_act", (string?)on["code"]);
            Assert.False((bool?)on["changed"]);
            Assert.Equal(Tools.TurnOnRefusalMessage, (string?)on["message"]);
            Assert.Equal("false|" + Actor, await ScalarAsync(rig.Owner, "SELECT agent_actions_enabled || '|' || updated_by FROM config.config_service WHERE id = 1", ct));
            Assert.Equal(1, await CountAsync(rig.Owner, "event = 'agent_actions_switched'", ct));

            /* A person's switch through the web and Viewer path still turns it on and off, and writes the same row shape. */
            Assert.True(await new AgentChangeUndo(rig.Owner, 30).SetEnabledAsync(true, "erik", ct));
            Assert.Equal("true|person:erik", await ScalarAsync(rig.Owner, "SELECT agent_actions_enabled || '|' || updated_by FROM config.config_service WHERE id = 1", ct));
            Assert.True(await new AgentChangeUndo(rig.Owner, 30).SetEnabledAsync(false, "erik", ct));
            Assert.Equal("false|person:erik", await ScalarAsync(rig.Owner, "SELECT agent_actions_enabled || '|' || updated_by FROM config.config_service WHERE id = 1", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task TheSwitch_WithNoServiceRow_IsRefusedForTheMcpToolAndTheSharedMethod_AndCreatesNoRow()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct, seedServiceRow: false);
        var ok = false;
        try
        {
            var refused = Parse(await Tools.SwitchAsync(rig.Mcp, null, false, Actor, ct));
            Assert.Equal("refused", (string?)refused["status"]);
            Assert.Equal("store_not_set_up", (string?)refused["code"]);
            Assert.Equal(AgentChangeUndo.SwitchStoreNotSetUpMessage, (string?)refused["message"]);

            /* The Viewer and the web call this method: false, and the store is as it was. */
            Assert.False(await new AgentChangeUndo(rig.Owner, 30).SetEnabledAsync(false, "erik", ct));
            Assert.False(await new AgentChangeUndo(rig.Owner, 30).SetEnabledAsync(true, "erik", ct));
            Assert.Equal("0", await ScalarAsync(rig.Owner, "SELECT count(*) FROM config.config_service", ct));
            Assert.Equal(0, await CountAsync(rig.Owner, "event = 'agent_actions_switched'", ct));

            /* With the row there, the same calls update it. */
            await ExecAsync(rig.Owner, "INSERT INTO config.config_service (id) VALUES (1)", ct);
            Assert.Equal("off", (string?)Parse(await Tools.SwitchAsync(rig.Mcp, null, false, Actor, ct))["status"]);
            Assert.Equal("false|" + Actor, await ScalarAsync(rig.Owner, "SELECT agent_actions_enabled || '|' || updated_by FROM config.config_service WHERE id = 1", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task UndoingAMuteChange_RestoresWhatTheWebUndoRestores_AndAnAlreadyUndoneOneIsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await ExecAsync(rig.Owner, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) VALUES ('m1', true, now() at time zone 'utc', 'agent says', 'Blocking', 7)", ct);
            var called = await ChangeAsync(rig.Owner, "mute", "m1", 7, MuteImage("old", false), new JsonObject { ["status"] = "updated", ["mute_rule"] = MuteImage("agent says", true) }, ct);

            var done = Parse(await Tools.UndoAsync(rig.Mcp, null, called, Actor, ct));
            Assert.Equal("undone", (string?)done["status"]);
            Assert.False((bool?)done["before"]!["undone"]);
            Assert.True((bool?)done["after"]!["undone"]);
            Assert.Equal("old|false", await ScalarAsync(rig.Owner, "SELECT reason || '|' || enabled FROM config.config_mute_rules WHERE id = 'm1'", ct));
            Assert.Equal(Actor, await ScalarAsync(rig.Owner, $"SELECT actor FROM config.investigation_journal WHERE event = 'action_undone' AND detail->>'called_id' = '{called}'", ct));

            var again = Parse(await Tools.UndoAsync(rig.Mcp, null, called, Actor, ct));
            Assert.Equal("refused", (string?)again["status"]);
            Assert.Equal("already_undone", (string?)again["code"]);
            Assert.False((bool?)again["changed"]);
            Assert.Equal(1, await CountAsync(rig.Owner, "event = 'action_undone'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task UndoingAThresholdOverrideChange_PutsTheValueBack_ByTheCaller()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await ExecAsync(rig.Owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h')", ct);
            await ExecAsync(rig.Owner, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (7, NULL, 'blocking_sessions', 10, 'agent:a')", ct);
            var called = await ChangeAsync(rig.Owner, "threshold_override", "blocking_sessions", 7, new JsonObject { ["value"] = 5.0 }, new JsonObject { ["value"] = 10.0 }, ct);

            var done = Parse(await Tools.UndoAsync(rig.Mcp, null, called, Actor, ct));
            Assert.Equal("undone", (string?)done["status"]);
            Assert.Equal("5|" + Actor, await ScalarAsync(rig.Owner, "SELECT value || '|' || set_by FROM config.config_threshold_overrides WHERE server_id = 7 AND database_name IS NULL", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task UndoRefusals_AnUnknownId_APlanChange_AndABadId_WriteNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var unknown = Parse(await Tools.UndoAsync(rig.Mcp, null, 987654, Actor, ct));
            Assert.Equal("refused", (string?)unknown["status"]);
            Assert.Equal("not_found", (string?)unknown["code"]);

            var plan = await ChangeAsync(rig.Owner, AgentChangeKinds.PlanForce, "q1", 7, null, new JsonObject { ["status"] = "forced" }, ct);
            var refused = Parse(await Tools.UndoAsync(rig.Mcp, null, plan, Actor, ct));
            Assert.Equal("refused", (string?)refused["status"]);
            Assert.Equal("plan_change", (string?)refused["code"]);
            Assert.Contains("undo_plan_change", (string?)refused["message"], StringComparison.Ordinal);

            Assert.Equal("invalid", (string?)Parse(await Tools.UndoAsync(rig.Mcp, null, 0, Actor, ct))["status"]);
            Assert.Equal(0, await CountAsync(rig.Owner, "event = 'action_undone'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task TheMcpRole_GotExactlyTheGrantsTheTwoCallsNeed_AndNoMore()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            foreach (var sql in new[]
            {
                "UPDATE config.config_service SET paused = TRUE WHERE id = 1",
                "UPDATE config.config_service SET mcp_port = 1 WHERE id = 1", /* #6401 merge: dev now grants mcp capture_plans (update_service_flags), so a column it still lacks stands in */
                "UPDATE config.investigation_journal SET reason = 'x'",
                "DELETE FROM config.investigation_journal",
                "UPDATE config.investigation_signature_merges SET reason = 'x'",
            })
            {
                var denied = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(rig.Mcp, sql, ct));
                Assert.Equal("42501", denied.SqlState);
            }

            /* The service creates the row, never the MCP login (#6401): no INSERT, so a switch cannot make a store look set up. */
            var noInsert = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(rig.Mcp, "INSERT INTO config.config_service (id, agent_actions_enabled) VALUES (2, FALSE)", ct));
            Assert.Equal("42501", noInsert.SqlState);

            /* What the two calls need: the switch columns, and the undone stamp on a merge row. */
            await ExecAsync(rig.Mcp, "UPDATE config.config_service SET agent_actions_enabled = FALSE, updated_by = 'mcp:x' WHERE id = 1", ct);
            await ExecAsync(rig.Owner, "INSERT INTO config.investigation_signature_merges (server_id, signature, merged_into, reason, merged_by) VALUES (7, 'a', 'b', 'r', 'agent')", ct);
            await ExecAsync(rig.Mcp, "UPDATE config.investigation_signature_merges SET undone_at = now(), undone_by = 'mcp:x'", ct);
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }
}
