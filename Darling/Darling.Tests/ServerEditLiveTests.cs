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
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another
   one. The role it connects as is created for the fact and dropped in its cleanup. */

/// <summary>
/// <c>edit_server</c> (#5240) against a store, connected as the <c>mcp</c> role: the statements are TAKEN FROM
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/> with the role name swapped, so removing a grant the edit
/// needs makes these fail with 42501. The owner-side reads prove what the write left alone.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class ServerEditLiveTests : IDisposable
{
    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public ServerEditLiveTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private const string SecretRef = "Synth-Edit-Live-Pw-1";

    private static readonly Edit.ServerProbe Reachable = (_, _) => Task.FromResult(
        new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null));

    private sealed record Rig(ScratchPostgres Scratch, NpgsqlDataSource Owner, NpgsqlDataSource Mcp, string Role) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Mcp.DisposeAsync();
            await Owner.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static List<string> McpStatements(string roleName)
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
            if (at < 0 || statement.Contains("EXECUTE ON FUNCTION", StringComparison.Ordinal))
            {
                continue;
            }

            if (!statement[(at + marker.Length)..].Split(',').Select(t => t.Trim()).Contains("mcp", StringComparer.Ordinal))
            {
                continue;
            }

            statements.Add(statement[..at] + marker + roleName);
        }

        return statements;
    }

    private static async Task<Rig> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live edit_server tests (each mints its own scratch database and role).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var owner = NpgsqlDataSource.Create(ownerString);
        var role = "srv_edit_" + Guid.NewGuid().ToString("N")[..8];
        await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        foreach (var statement in McpStatements(role))
        {
            await ExecAsync(owner, statement, ct);
        }

        /* The edit write is a function (#5240) the provisioning batch creates and grants: the real one, EXECUTE for this role. */
        await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config"), ct);
        await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerSignature}) TO {role}", ct);
        await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
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

    private static async Task<T> ScalarAsync<T>(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync(ct))!, typeof(T));
    }

    private static Task SeedServerAsync(
        NpgsqlDataSource owner, int id, string name, string host, string auth, string? username, string? blob, CancellationToken ct) =>
        ExecAsync(owner, $@"INSERT INTO config_monitored_servers
            (server_id, name, host, auth, username, encrypted_password, excluded_databases, capture_plans, is_enabled,
             alert_delivery_mode_override, plan_force_bot_enabled, remediation_username, remediation_encrypted_password, monthly_cost_usd)
            VALUES ({id}, '{name}', '{host}', '{auth}', {(username is null ? "NULL" : "'" + username + "'")}, {(blob is null ? "NULL" : "'" + blob + "'")},
                    ARRAY['tempdb','model'], TRUE, FALSE, 'PerEvent', TRUE, 'rem-user', 'rem-blob', 7)", ct);

    private static async Task<string> RowSignatureAsync(NpgsqlDataSource owner, int id, CancellationToken ct) =>
        await ScalarAsync<string>(owner, $@"SELECT concat_ws('|', is_enabled, array_to_string(excluded_databases, ','), capture_plans,
            alert_delivery_mode_override, plan_force_bot_enabled, remediation_username, remediation_encrypted_password, engine, created_at)
            FROM config_monitored_servers WHERE server_id = {id}", ct);

    private static JsonNode Parse(string answer) => JsonNode.Parse(answer)!;

    [Fact]
    public async Task AsTheMcpRole_AnEditOfNameCostAndAddress_KeepsTheIdTagsAndSettings_AndBumpsTheBeaconOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5101, "alpha-01", "alpha-01.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 5102, "alpha-02", "alpha-02.example.test", "integrated", null, null, ct);
            var tags = new ServerTagStore(rig.Owner, 30);
            var tag = Assert.IsType<ServerTagWriteResult.Ok>(await tags.CreateAsync("East", null, null, ct)).Tag!;
            await tags.AssignAsync(tag.Id, [5101, 5102], ct);
            var before = await RowSignatureAsync(rig.Owner, 5101, ct);
            var versionBefore = await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct);
            var modifiedBefore = await ScalarAsync<string>(rig.Owner, "SELECT modified_at::text FROM config_monitored_servers WHERE server_id = 5101", ct);

            var name = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-01.example.test", "{\"display_name\":\"Orders\",\"monthly_cost_usd\":120}", Reachable, true, null, ct));
            Assert.Equal("updated", name["status"]!.GetValue<string>());
            Assert.Equal(5101, name["server_id"]!.GetValue<int>());
            Assert.Equal(versionBefore + 1, await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct));

            var address = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "Orders", "{\"host\":\"alpha-09.example.test\"}", Reachable, true, null, ct));
            Assert.Equal("updated", address["status"]!.GetValue<string>());
            Assert.Equal(5101, address["server_id"]!.GetValue<int>());
            Assert.Contains("keeps its id", address["note"]!.GetValue<string>(), StringComparison.Ordinal);

            Assert.Equal("Orders", await ScalarAsync<string>(rig.Owner, "SELECT name FROM config_monitored_servers WHERE server_id = 5101", ct));
            Assert.Equal("alpha-09.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 5101", ct));
            Assert.Equal(120L, await ScalarAsync<long>(rig.Owner, "SELECT monthly_cost_usd::bigint FROM config_monitored_servers WHERE server_id = 5101", ct));
            Assert.NotEqual(modifiedBefore, await ScalarAsync<string>(rig.Owner, "SELECT modified_at::text FROM config_monitored_servers WHERE server_id = 5101", ct));
            Assert.Equal(before, await RowSignatureAsync(rig.Owner, 5101, ct));

            /* The tags stayed, on the same id, for the edited server and its neighbour. */
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM server_tag_map WHERE server_id = 5101", ct));
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM server_tag_map WHERE server_id = 5102", ct));
            Assert.Equal(2L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers", ct));

            /* A request equal to the stored values writes nothing: no statement, so no beacon bump. */
            var versionMid = await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct);
            var same = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "Orders", "{\"display_name\":\"Orders\"}", Reachable, true, null, ct));
            Assert.Equal("unchanged", same["status"]!.GetValue<string>());
            Assert.Equal(versionMid, await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AStaleToken_IsAConflict_AndALostRace_IsAConflict_WithNothingWritten()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5111, "alpha-11", "alpha-11.example.test", "integrated", null, null, ct);
            var first = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", "{\"monthly_cost_usd\":1}", Reachable, true, null, ct));
            var token = first["modified_at"]!.GetValue<string>();

            /* The token an earlier answer returned is accepted once, then it is stale. */
            var good = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", $"{{\"monthly_cost_usd\":2,\"expected_modified_at\":\"{token}\"}}", Reachable, true, null, ct));
            Assert.Equal("updated", good["status"]!.GetValue<string>());
            Assert.NotEqual(token, good["modified_at"]!.GetValue<string>());
            var stale = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", $"{{\"monthly_cost_usd\":3,\"expected_modified_at\":\"{token}\"}}", Reachable, true, null, ct));
            Assert.Equal("conflict", stale["status"]!.GetValue<string>());
            Assert.Equal(2L, await ScalarAsync<long>(rig.Owner, "SELECT monthly_cost_usd::bigint FROM config_monitored_servers WHERE server_id = 5111", ct));

            /* The race: a write lands between the core's read and its UPDATE. The probe stands in the gap. */
            Edit.ServerProbe racing = async (_, c) =>
            {
                await ExecAsync(rig.Owner, "UPDATE config_monitored_servers SET modified_at = modified_at + interval '1 second' WHERE server_id = 5111", c);
                return new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null);
            };
            var lost = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", "{\"host\":\"alpha-12.example.test\"}", racing, true, null, ct));
            Assert.Equal("conflict", lost["status"]!.GetValue<string>());
            Assert.Equal("alpha-11", lost["current"]!["display_name"]!.GetValue<string>());
            Assert.Equal("alpha-11.example.test", lost["current"]!["host"]!.GetValue<string>());
            Assert.Equal("alpha-11.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 5111", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AnAddressAnotherServerHolds_Collides_AndNothingIsWritten()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5121, "alpha-21", "alpha-21.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 5122, "alpha-22", "alpha-22.example.test", "integrated", null, null, ct);

            var answer = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-21", "{\"host\":\"ALPHA-22.example.test\"}", Reachable, true, null, ct));

            Assert.Equal("collides", answer["status"]!.GetValue<string>());
            Assert.Equal("occupied", answer["reason"]!.GetValue<string>());
            Assert.Equal("alpha-21.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 5121", ct));

            var ambiguous = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-2", "{\"monthly_cost_usd\":1}", Reachable, true, null, ct));
            Assert.Equal("ambiguous", ambiguous["status"]!.GetValue<string>());
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task ASecretEdit_StoresTheProtectedSecret_AndTheMcpRoleStillCannotReadItBack()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5131, "alpha-31", "alpha-31.example.test", "sql", "monitor", "env:OLD_REF", ct);

            var refused = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", "{\"host\":\"alpha-32.example.test\"}", Reachable, true, null, ct));
            Assert.Equal("invalid", refused["status"]!.GetValue<string>());
            Assert.Equal("env:OLD_REF", await ScalarAsync<string>(rig.Owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5131", ct));

            var saved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", $"{{\"host\":\"alpha-32.example.test\",\"password\":\"{SecretRef}\"}}", Reachable, true, null, ct));
            Assert.Equal("updated", saved["status"]!.GetValue<string>());
            Assert.True(saved["tested"]!.GetValue<bool>());
            Assert.DoesNotContain(SecretRef, saved.ToJsonString(), StringComparison.Ordinal);
            var storedAfterSave = await ScalarAsync<string>(rig.Owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5131", ct);
            Assert.Equal(SecretRef, DarlingSecrets.Unprotect(storedAfterSave));

            var denied = await Assert.ThrowsAsync<PostgresException>(async () => await ScalarAsync<string>(rig.Mcp, "SELECT encrypted_password FROM config_monitored_servers", ct));
            Assert.Equal("42501", denied.SqlState);

            /* A probe that throws with the submitted secret in its message: the outer catch redacts it. */
            Edit.ServerProbe throwing = (_, _) => throw new InvalidOperationException("driver said: " + SecretRef);
            var thrown = await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", $"{{\"host\":\"alpha-33.example.test\",\"password\":\"{SecretRef}\"}}", throwing, true, null, ct);
            Assert.DoesNotContain(SecretRef, thrown, StringComparison.Ordinal);

            /* A name-only edit leaves the stored secret alone, with no password in the request. */
            var rename = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", "{\"display_name\":\"Beta\"}", Reachable, true, null, ct));
            Assert.Equal("updated", rename["status"]!.GetValue<string>());
            Assert.Equal(storedAfterSave, await ScalarAsync<string>(rig.Owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5131", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /* ---------------- the probe, against a real SQL Server ---------------- */

    [Fact]
    public async Task AgainstARealSqlServer_ACorrectSecretSaves_AndAWrongOneIsConnectionFailed_WithNoWrite()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(host), "Set DARLING_TEST_SQL (and _USER / _PASSWORD) to run the live probe check.");
        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER") ?? "sa";
        var password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD");
        Assert.SkipWhen(string.IsNullOrEmpty(password), "Set DARLING_TEST_SQL_PASSWORD to run the live probe check.");

        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var wrong = "not-the-password-" + Guid.NewGuid().ToString("N")[..6];
            Environment.SetEnvironmentVariable("SYNTH_EDIT_LIVE_GOOD", password);
            await SeedServerAsync(rig.Owner, 5141, "alpha-41", host!, "sql", user, "env:SYNTH_EDIT_LIVE_GOOD", ct);
            await ExecAsync(rig.Owner, "UPDATE config_monitored_servers SET trust_server_certificate = TRUE WHERE server_id = 5141", ct);
            var before = await ScalarAsync<string>(rig.Owner, "SELECT concat_ws('|', database, encrypted_password, modified_at::text) FROM config_monitored_servers WHERE server_id = 5141", ct);

            Edit.ServerProbe real = (config, c) => DarlingServerConnector.ProbeAsync(config, null, c);

            var bad = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-41", "{\"database\":\"master\",\"password\":\"" + wrong + "\"}", real, true, null, ct));
            Assert.Equal("connection_failed", bad["status"]!.GetValue<string>());
            Assert.DoesNotContain(wrong, bad.ToJsonString(), StringComparison.Ordinal);
            Assert.Equal(before, await ScalarAsync<string>(rig.Owner, "SELECT concat_ws('|', database, encrypted_password, modified_at::text) FROM config_monitored_servers WHERE server_id = 5141", ct));

            var good = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-41", "{\"database\":\"master\",\"password\":\"" + password + "\"}", real, true, null, ct));
            Assert.Equal("updated", good["status"]!.GetValue<string>());
            Assert.True(good["tested"]!.GetValue<bool>());
            Assert.Equal("master", await ScalarAsync<string>(rig.Owner, "SELECT database FROM config_monitored_servers WHERE server_id = 5141", ct));
            ok = true;
        }
        finally
        {
            Environment.SetEnvironmentVariable("SYNTH_EDIT_LIVE_GOOD", null);
            await DropRoleAsync(rig, ok);
        }
    }
}
