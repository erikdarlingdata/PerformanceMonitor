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
using PerformanceMonitor.Common;
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
public sealed partial class ServerEditLiveTests : IDisposable
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
        foreach (var statement in McpStatements(role, scratch.DatabaseName))
        {
            await ExecAsync(owner, statement, ct);
        }

        /* The edit write is a function (#5240) the provisioning batch creates and grants: the real one, EXECUTE for this role. */
        await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config"), ct);
        await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerSeventeenWrapperSql("config"), ct);
        await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerLegacyWrapperSql("config"), ct);
        await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerSignature}) TO {role}", ct);
        await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerSeventeenSignature}) TO {role}", ct);
        await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerLegacySignature}) TO {role}", ct);
        await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        var mcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(ownerString) { Username = role, Password = RolePassword }.ConnectionString);
        return new Rig(scratch, owner, mcp, role);
    }

    /// <summary>Drops the rig's role, and any login role the test created itself (<paramref name="extraRoles"/>): a role is cluster-wide, so
    /// one left behind outlives the scratch database (#6340 round 2, N5).</summary>
    private static async Task DropRoleAsync(Rig rig, bool bodySucceeded, params string[] extraRoles) =>
        await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
        {
            foreach (var role in extraRoles.Append(rig.Role))
            {
                await ExecAsync(rig.Owner, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};", CancellationToken.None);
            }
        });

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        var value = await command.ExecuteScalarAsync(ct);
        /* A SQL NULL is DBNull, which Convert.ChangeType turns into "" for a string, so a NULL column would read as empty text. */
        if (value is null or DBNull)
        {
            return default!;
        }

        return (T)Convert.ChangeType(value, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T));
    }

    private static Task SeedServerAsync(
        NpgsqlDataSource owner, int id, string name, string host, string auth, string? username, string? blob, CancellationToken ct,
        string? remediationBlob = null) =>
        ExecAsync(owner, $@"INSERT INTO config_monitored_servers
            (server_id, name, host, auth, username, encrypted_password, excluded_databases, capture_plans, is_enabled,
             alert_delivery_mode_override, plan_force_bot_enabled, remediation_username, remediation_encrypted_password, monthly_cost_usd)
            VALUES ({id}, '{name}', '{host}', '{auth}', {(username is null ? "NULL" : "'" + username + "'")}, {(blob is null ? "NULL" : "'" + blob + "'")},
                    ARRAY['tempdb','model'], TRUE, FALSE, 'PerEvent', TRUE, 'rem-user', {(remediationBlob is null ? "NULL" : "'" + remediationBlob + "'")}, 7)", ct);

    private static async Task<string> RowSignatureAsync(NpgsqlDataSource owner, int id, CancellationToken ct) =>
        await ScalarAsync<string>(owner, $@"SELECT concat_ws('|', is_enabled, array_to_string(excluded_databases, ','), capture_plans,
            alert_delivery_mode_override, plan_force_bot_enabled, remediation_username, remediation_encrypted_password, engine, created_at)
            FROM config_monitored_servers WHERE server_id = {id}", ct);

    private static JsonNode Parse(string answer) => JsonNode.Parse(answer)!;

    /// <summary>Opens the stored value of a row with <paramref name="ring"/>, through the connection settings the row holds.</summary>
    private static async Task<string> OpenStoredAsync(NpgsqlDataSource owner, int id, IPasswordKeyRing ring, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(
            "SELECT " + ServerConnectionIdentity.StoredColumns + ", encrypted_password FROM config_monitored_servers WHERE server_id = " + id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var identity = ServerConnectionIdentity.FromStoredColumns(
            reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt32(1),
            reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3),
            !reader.IsDBNull(4) && reader.GetBoolean(4), reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
            !reader.IsDBNull(8) && reader.GetBoolean(8), !reader.IsDBNull(9) && reader.GetBoolean(9), reader.IsDBNull(10) ? null : reader.GetString(10));
        return ring.Open(reader.GetString(11), PasswordBinding.ForServer(identity));
    }

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

            var name = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-01.example.test", "{\"display_name\":\"Orders\",\"monthly_cost_usd\":120}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", name["status"]!.GetValue<string>());
            Assert.Equal(5101, name["server_id"]!.GetValue<int>());
            Assert.Equal(versionBefore + 1, await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct));

            var address = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "Orders", "{\"host\":\"alpha-09.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct));
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
            var same = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "Orders", "{\"display_name\":\"Orders\"}", Reachable, TestKeyRings.Healthy, null, ct));
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
    public async Task GetServerSettings_ShowsTheFieldsEditServerChanges_BeforeAndAfterAnEdit_AndNeverASecret()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5171, "alpha-71", "alpha-71.example.test", "sql", "monitor", "sealed-password-blob", ct);

            /* #6402: the read, through the mcp role's own data source (the role the tool runs as). */
            var before = Parse(await DarlingMcpCollectionStateTools.GetServerSettings(rig.Mcp, "alpha-71", ct));
            Assert.Equal("ok", before["status"]!.GetValue<string>());
            var settingsBefore = before["settings"]!.AsObject();
            Assert.Equal(5171, settingsBefore["server_id"]!.GetValue<int>());
            Assert.Equal("alpha-71.example.test", settingsBefore["host"]!.GetValue<string>());
            Assert.Equal("SQL", settingsBefore["auth"]!.GetValue<string>());
            Assert.Equal("monitor", settingsBefore["username"]!.GetValue<string>());
            Assert.Equal(7m, settingsBefore["monthly_cost_usd"]!.GetValue<decimal>());
            Assert.DoesNotContain("sealed-password-blob", before.ToJsonString(), StringComparison.Ordinal);
            Assert.False(settingsBefore.ContainsKey("password"));
            Assert.False(settingsBefore.ContainsKey("encrypted_password"));

            var edited = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-71",
                "{\"display_name\":\"Billing\",\"monthly_cost_usd\":250.5,\"expected_modified_at\":\"" + settingsBefore["modified_at"]!.GetValue<string>() + "\"}",
                Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", edited["status"]!.GetValue<string>());

            var after = Parse(await DarlingMcpCollectionStateTools.GetServerSettings(rig.Mcp, "Billing", ct))["settings"]!.AsObject();
            Assert.Equal("Billing", after["display_name"]!.GetValue<string>());
            Assert.Equal(250.5m, after["monthly_cost_usd"]!.GetValue<decimal>());
            Assert.False(after["trust_server_certificate"]!.GetValue<bool>());
            Assert.NotEqual(settingsBefore["modified_at"]!.GetValue<string>(), after["modified_at"]!.GetValue<string>());

            /* The same cost shows in get_collection_state. */
            var state = Parse(await DarlingMcpCollectionStateTools.GetCollectionState(rig.Mcp, ct));
            var listed = state["servers"]!.AsArray().Single(x => x!["server_id"]!.GetValue<int>() == 5171)!;
            Assert.Equal(250.5m, listed["monthly_cost_usd"]!.GetValue<decimal>());

            var missing = Parse(await DarlingMcpCollectionStateTools.GetServerSettings(rig.Mcp, "no-such-server", ct));
            Assert.Equal("refused", missing["status"]!.GetValue<string>());
            Assert.Equal("not_found", missing["reason"]!.GetValue<string>());
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task ListServers_ShowsTheMonthlyCostOnTheDefinition_AndZeroWhereNoneIsSet()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            /* #6402: list_servers lists only an enabled definition that has a registry row. */
            await SeedServerAsync(rig.Owner, 5181, "alpha-81", "alpha-81.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 5182, "alpha-82", "alpha-82.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner, "UPDATE config_monitored_servers SET is_enabled = TRUE, monthly_cost_usd = CASE server_id WHEN 5181 THEN 99.5 ELSE 0 END WHERE server_id IN (5181, 5182)", ct);
            foreach (var (id, name) in new[] { (5181, "alpha-81"), (5182, "alpha-82") })
            {
                await DarlingObservability.UpsertServerAsync(rig.Owner, new ServerRuntime
                {
                    Config = new MonitoredServer { Name = name, Host = name + ".example.test" },
                    ConnectionString = "Server=" + name,
                    Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                    StorageName = name,
                    ServerId = id,
                    EngineEdition = 3,
                }, null, ct);
            }

            var list = Parse(await DarlingMcpDataTools.ListServers(rig.Mcp, ct));
            var costs = list["servers"]!.AsArray().ToDictionary(x => x!["server_name"]!.GetValue<string>(), x => x!["monthly_cost_usd"]!.GetValue<decimal>());
            Assert.Equal(99.5m, costs["alpha-81"]);
            Assert.Equal(0m, costs["alpha-82"]);
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task TheEditServerTool_PassesItsPolicyToTheScreen_SoWindowsAuthOverUnverifiedTlsIsRefused_AndNothingIsWritten()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5161, "alpha-61", "alpha-61.example.test", "integrated", null, null, ct);
            var before = await RowSignatureAsync(rig.Owner, 5161, ct);
            var versionBefore = await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct);
            var modifiedBefore = await ScalarAsync<string>(rig.Owner, "SELECT modified_at::text FROM config_monitored_servers WHERE server_id = 5161", ct);
            var trustBefore = await ScalarAsync<string>(rig.Owner, "SELECT trust_server_certificate::text FROM config_monitored_servers WHERE server_id = 5161", ct);

            /* The tool method itself, with the host's default policy, not the core: dropping the policy argument
               on the way from the tool to the screen (EditServer, or EditServerByNameAsync) makes this edit reach
               the probe and the write. The screen answers first, so no connection is attempted. */
            var refused = Parse(await Edit.EditServer(rig.Mcp, "alpha-61.example.test", "{\"trust_server_certificate\":true}",
                DarlingMcpAddServersPolicy.Default, null, ct));

            Assert.Equal("invalid", refused["status"]!.GetValue<string>());
            Assert.StartsWith("Not saved:", refused["message"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal(before, await RowSignatureAsync(rig.Owner, 5161, ct));
            Assert.Equal(trustBefore, await ScalarAsync<string>(rig.Owner, "SELECT trust_server_certificate::text FROM config_monitored_servers WHERE server_id = 5161", ct));
            Assert.Equal(modifiedBefore, await ScalarAsync<string>(rig.Owner, "SELECT modified_at::text FROM config_monitored_servers WHERE server_id = 5161", ct));
            Assert.Equal(versionBefore, await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AsTheMcpRole_AnAddressEditOnARowWithARemediationLogin_IsRefused_AndWritesNothing_WhileANameEditSaves()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5191, "alpha-91", "alpha-91.example.test", "integrated", null, null, ct, remediationBlob: "rem-blob");
            var before = await RowSignatureAsync(rig.Owner, 5191, ct);
            var versionBefore = await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct);

            var moved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-91.example.test", "{\"host\":\"alpha-99.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("invalid", moved["status"]!.GetValue<string>());
            Assert.Equal(Edit.EditRemediationKeptText, moved["message"]!.GetValue<string>());
            Assert.Equal("alpha-91.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 5191", ct));
            Assert.Equal(before, await RowSignatureAsync(rig.Owner, 5191, ct));
            Assert.Equal(versionBefore, await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct));

            var renamed = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-91.example.test", "{\"display_name\":\"Renamed\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", renamed["status"]!.GetValue<string>());
            Assert.Equal(before, await RowSignatureAsync(rig.Owner, 5191, ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AsTheMcpRole_RoleAndExternalIdEditsGoThroughTheEditFunction_AndEveryOtherColumnStaysUpdatable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5192, "alpha-92", "alpha-92.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner,
                "UPDATE config_monitored_servers SET engine = 'postgres', port = 5432, aws_role_arn = 'arn:aws:iam::123456789012:role/a', aws_external_id = 'tenant-1234' WHERE server_id = 5192", ct);

            /* A direct UPDATE of the role or the external ID is not allowed for this login; the edit function is the way. */
            var roleUpdate = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(rig.Mcp, "UPDATE config_monitored_servers SET aws_role_arn = 'arn:aws:iam::123456789012:role/b' WHERE server_id = 5192", ct));
            Assert.Equal("42501", roleUpdate.SqlState);
            var externalUpdate = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(rig.Mcp, "UPDATE config_monitored_servers SET aws_external_id = 'tenant-5678' WHERE server_id = 5192", ct));
            Assert.Equal("42501", externalUpdate.SqlState);
            Assert.Equal("arn:aws:iam::123456789012:role/a|tenant-1234",
                await ScalarAsync<string>(rig.Owner, "SELECT concat_ws('|', aws_role_arn, aws_external_id) FROM config_monitored_servers WHERE server_id = 5192", ct));

            /* Every other column is still updatable: the grant names each column of the table but the three AWS ones and the two hypothetical-index login ones (#6170, set on the service host). */
            await ExecAsync(rig.Mcp, "UPDATE config_monitored_servers SET name = 'alpha-92b', is_enabled = TRUE, excluded_databases = ARRAY['model'] WHERE server_id = 5192", ct);
            var columns = await ScalarAsync<string>(rig.Owner,
                "SELECT string_agg(attname, ',' ORDER BY attname) FROM pg_attribute WHERE attrelid = 'config.config_monitored_servers'::regclass "
                + "AND attnum > 0 AND NOT attisdropped AND attname NOT IN ('aws_role_arn', 'aws_external_id', 'aws_external_id_set', 'hypothetical_index_username', 'hypothetical_index_encrypted_password', 'query_store_replica')", ct);
            Assert.Equal(
                string.Join(',', DarlingManagedRoles.McpMonitoredServerUpdateColumns.Split(',', StringSplitOptions.TrimEntries).Order(StringComparer.Ordinal)),
                columns);

            /* An add still names the role, and the edit function still changes it, with the external ID sent or cleared. */
            await ExecAsync(rig.Mcp,
                "INSERT INTO config_monitored_servers (server_id, name, host, auth, engine, port, aws_role_arn, aws_external_id) "
                + "VALUES (5193, 'alpha-93', 'alpha-93.example.test', 'sql', 'postgres', 5432, 'arn:aws:iam::123456789012:role/a', 'tenant-1234')", ct);
            var token = await ScalarAsync<string>(rig.Owner, "SELECT to_char(modified_at, 'YYYY-MM-DD HH24:MI:SS.US') FROM config_monitored_servers WHERE server_id = 5192", ct);
            var outcome = await ScalarAsync<string>(rig.Mcp,
                $"SELECT outcome FROM config.edit_monitored_server(5192, '{token}'::timestamp, ARRAY['aws_role_arn','aws_external_id']::text[], "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'arn:aws:iam::123456789012:role/b', NULL)", ct);
            Assert.Equal("saved", outcome);
            Assert.Equal("arn:aws:iam::123456789012:role/b|",
                await ScalarAsync<string>(rig.Owner, "SELECT concat_ws('|', aws_role_arn, aws_external_id, '') FROM config_monitored_servers WHERE server_id = 5192", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AsTheMcpRole_EmptyRequiredFields_AnswerInvalidValue_OnBothSignatures_AndWriteNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5194, "alpha-94", "alpha-94.example.test", "sql", "monitor", "blob-94", ct);
            await ExecAsync(rig.Owner,
                "UPDATE config_monitored_servers SET engine = 'postgres', port = 5432, aws_role_arn = 'arn:aws:iam::123456789012:role/a', aws_external_id = 'tenant-1234' WHERE server_id = 5194", ct);
            var before = await ScalarAsync<string>(rig.Owner,
                "SELECT concat_ws('|', name, host, port, auth, encrypted_password, aws_role_arn, aws_external_id, modified_at) FROM config_monitored_servers WHERE server_id = 5194", ct);

            foreach (var columns in new[] { "'name'", "'host','encrypted_password'", "'port'", "'auth'", "'encrypt_mode'", "'read_only_intent'", "'monthly_cost_usd'" })
            {
                var token = await ScalarAsync<string>(rig.Owner, "SELECT to_char(modified_at, 'YYYY-MM-DD HH24:MI:SS.US') FROM config_monitored_servers WHERE server_id = 5194", ct);
                var seventeen = await ScalarAsync<string>(rig.Mcp,
                    $"SELECT outcome FROM config.edit_monitored_server(5194, '{token}'::timestamp, ARRAY[{columns}]::text[], "
                    + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'typed-secret', NULL, NULL, NULL, NULL, NULL, NULL)", ct);
                Assert.Equal("invalid_value", seventeen);
                var fifteen = await ScalarAsync<string>(rig.Mcp,
                    $"SELECT outcome FROM config.edit_monitored_server(5194, '{token}'::timestamp, ARRAY[{columns}]::text[], "
                    + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'typed-secret', NULL, NULL, NULL, NULL)", ct);
                Assert.Equal("invalid_value", fifteen);
            }

            /* A role the table's format check does not accept is the same outcome, and nothing changed. */
            var roleToken = await ScalarAsync<string>(rig.Owner, "SELECT to_char(modified_at, 'YYYY-MM-DD HH24:MI:SS.US') FROM config_monitored_servers WHERE server_id = 5194", ct);
            Assert.Equal("invalid_value", await ScalarAsync<string>(rig.Mcp,
                $"SELECT outcome FROM config.edit_monitored_server(5194, '{roleToken}'::timestamp, ARRAY['aws_role_arn','aws_external_id']::text[], "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'not-an-arn', 'tenant-1234')", ct));

            Assert.Equal(before, await ScalarAsync<string>(rig.Owner,
                "SELECT concat_ws('|', name, host, port, auth, encrypted_password, aws_role_arn, aws_external_id, modified_at) FROM config_monitored_servers WHERE server_id = 5194", ct));
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
            var first = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", "{\"monthly_cost_usd\":1}", Reachable, TestKeyRings.Healthy, null, ct));
            var token = first["modified_at"]!.GetValue<string>();

            /* The token an earlier answer returned is accepted once, then it is stale. */
            var good = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", $"{{\"monthly_cost_usd\":2,\"expected_modified_at\":\"{token}\"}}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", good["status"]!.GetValue<string>());
            Assert.NotEqual(token, good["modified_at"]!.GetValue<string>());
            var stale = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", $"{{\"monthly_cost_usd\":3,\"expected_modified_at\":\"{token}\"}}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("conflict", stale["status"]!.GetValue<string>());
            Assert.Equal(2L, await ScalarAsync<long>(rig.Owner, "SELECT monthly_cost_usd::bigint FROM config_monitored_servers WHERE server_id = 5111", ct));

            /* The race: a write lands between the core's read and its UPDATE. The probe stands in the gap. */
            Edit.ServerProbe racing = async (_, c) =>
            {
                await ExecAsync(rig.Owner, "UPDATE config_monitored_servers SET modified_at = modified_at + interval '1 second' WHERE server_id = 5111", c);
                return new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null);
            };
            var lost = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-11", "{\"host\":\"alpha-12.example.test\"}", racing, TestKeyRings.Healthy, null, ct));
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

            var answer = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-21", "{\"host\":\"ALPHA-22.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct));

            Assert.Equal("collides", answer["status"]!.GetValue<string>());
            Assert.Equal("occupied", answer["reason"]!.GetValue<string>());
            Assert.Equal("alpha-21.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 5121", ct));

            var ambiguous = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-2", "{\"monthly_cost_usd\":1}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("ambiguous", ambiguous["status"]!.GetValue<string>());
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AName_ThatIsOneServersStorageNameAndAnotherServersDisplayName_IsAmbiguous_AndNothingIsWritten()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            /* Server 5131's storage name is the text server 5132 shows as its display name. */
            await SeedServerAsync(rig.Owner, 5131, "ident-a", "shared-ident.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 5132, "shared-ident.example.test", "ident-b.example.test", "integrated", null, null, ct);

            var ambiguous = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "shared-ident.example.test", "{\"monthly_cost_usd\":1}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("ambiguous", ambiguous["status"]!.GetValue<string>());
            var listed = ambiguous["candidates"]!.AsArray().Select(c => c!["server"]!.GetValue<string>()).OrderBy(n => n, StringComparer.Ordinal).ToArray();
            Assert.Equal(new[] { "ident-b.example.test", "shared-ident.example.test" }, listed);
            Assert.Equal(2L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE server_id IN (5131, 5132) AND monthly_cost_usd = 7", ct));

            /* An exact single match still resolves: the other server's own storage name picks it. */
            var edited = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "ident-b.example.test", "{\"monthly_cost_usd\":1}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.NotEqual("ambiguous", edited["status"]!.GetValue<string>());
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 5132 AND monthly_cost_usd = 1", ct));
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 5131 AND monthly_cost_usd = 7", ct));
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

            var refused = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", "{\"host\":\"alpha-32.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("invalid", refused["status"]!.GetValue<string>());
            Assert.Equal("env:OLD_REF", await ScalarAsync<string>(rig.Owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5131", ct));

            var saved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", $"{{\"host\":\"alpha-32.example.test\",\"password\":\"{SecretRef}\"}}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", saved["status"]!.GetValue<string>());
            Assert.True(saved["tested"]!.GetValue<bool>());
            Assert.DoesNotContain(SecretRef, saved.ToJsonString(), StringComparison.Ordinal);
            var storedAfterSave = await ScalarAsync<string>(rig.Owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5131", ct);
            /* The value is sealed for the row's own connection settings: it opens with the key through them. */
            Assert.Equal(SecretRef, await OpenStoredAsync(rig.Owner, 5131, TestKeyRings.Healthy, ct));

            var denied = await Assert.ThrowsAsync<PostgresException>(async () => await ScalarAsync<string>(rig.Mcp, "SELECT encrypted_password FROM config_monitored_servers", ct));
            Assert.Equal("42501", denied.SqlState);

            /* A probe that throws with the submitted secret in its message: the outer catch redacts it. */
            Edit.ServerProbe throwing = (_, _) => throw new InvalidOperationException("driver said: " + SecretRef);
            var thrown = await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", $"{{\"host\":\"alpha-33.example.test\",\"password\":\"{SecretRef}\"}}", throwing, TestKeyRings.Healthy, null, ct);
            Assert.DoesNotContain(SecretRef, thrown, StringComparison.Ordinal);

            /* A name-only edit leaves the stored secret alone, with no password in the request. */
            var rename = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-31", "{\"display_name\":\"Beta\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", rename["status"]!.GetValue<string>());
            Assert.Equal(storedAfterSave, await ScalarAsync<string>(rig.Owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5131", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /* ---------------- sealing at add and edit, read back the way the service reads it (#5366) ---------------- */

    private const string TypedPassword = "p@ss-not-real";
    private const string RetypedPassword = "p@ss-not-real-2";

    /// <summary>The enabled rows, read the way the service reads them at startup.</summary>
    private static async Task<IReadOnlyList<MonitoredServer>> ReadServersAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        await using var connection = await owner.OpenConnectionAsync(ct);
        return await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct);
    }

    [Fact]
    public async Task AnAdd_WithALiteralPassword_StoresASealedValue_ThatTheResolverOpensForTheRowsOwnSettings()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var answer = await Edit.AddServersAsync(rig.Mcp,
                $"[{{\"host\":\"alpha-41.example.test\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"{TypedPassword}\"}}]",
                Reachable, ct, TestKeyRings.Healthy);
            Assert.Contains("\"added\"", answer, StringComparison.Ordinal);
            Assert.DoesNotContain(TypedPassword, answer, StringComparison.Ordinal);

            var server = Assert.Single(await ReadServersAsync(rig.Owner, ct), s => s.Host == "alpha-41.example.test");
            Assert.StartsWith("sealed:", server.EncryptedPassword, StringComparison.Ordinal);
            Assert.DoesNotContain(TypedPassword, server.EncryptedPassword, StringComparison.Ordinal);

            /* The service's own resolver opens it with the key, through the settings the row stores. */
            Assert.Equal(TypedPassword, DarlingSecrets.ResolvePassword(server, out var usedPlaintext, TestKeyRings.Healthy));
            Assert.False(usedPlaintext);

            /* A ring that does not hold the key cannot open it, and the same value does not open for another host. */
            Assert.ThrowsAny<Exception>(() => DarlingSecrets.ResolvePassword(
                server, out _, DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate())));
            var otherHost = server.ConnectionIdentity with { Host = "alpha-43.example.test" };
            Assert.Throws<PasswordSealException>(() => TestKeyRings.Healthy.Open(server.EncryptedPassword!, PasswordBinding.ForServer(otherHost)));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AnEdit_WithARetypedPasswordAndANewHost_OpensWithTheNewBinding_AndTheOldSealedValueNoLongerOpensForTheNewHost()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await Edit.AddServersAsync(rig.Mcp,
                $"[{{\"host\":\"alpha-51.example.test\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"{TypedPassword}\"}}]",
                Reachable, ct, TestKeyRings.Healthy);
            var before = Assert.Single(await ReadServersAsync(rig.Owner, ct), s => s.Host == "alpha-51.example.test");
            var oldSealed = before.EncryptedPassword;
            Assert.Equal(TypedPassword, DarlingSecrets.ResolvePassword(before, out _, TestKeyRings.Healthy));

            /* The host moves and the password is not typed again: refused, nothing written (the old value is bound to the old host). */
            var refused = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-51.example.test", "{\"host\":\"alpha-52.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("invalid", refused["status"]!.GetValue<string>());
            Assert.Equal(oldSealed, (await ReadServersAsync(rig.Owner, ct)).Single(s => s.ServerId == before.ServerId).EncryptedPassword);

            var saved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-51.example.test",
                $"{{\"host\":\"alpha-52.example.test\",\"password\":\"{RetypedPassword}\"}}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", saved["status"]!.GetValue<string>());

            var after = Assert.Single(await ReadServersAsync(rig.Owner, ct), s => s.ServerId == before.ServerId);
            Assert.Equal("alpha-52.example.test", after.Host);
            Assert.NotEqual(oldSealed, after.EncryptedPassword);
            Assert.Equal(RetypedPassword, DarlingSecrets.ResolvePassword(after, out _, TestKeyRings.Healthy));

            /* The value sealed for the old host does not open for the new one. */
            after.EncryptedPassword = oldSealed;
            Assert.ThrowsAny<Exception>(() => DarlingSecrets.ResolvePassword(after, out _, TestKeyRings.Healthy));
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

            var bad = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-41", "{\"database\":\"master\",\"password\":\"" + wrong + "\"}", real, TestKeyRings.Healthy, null, ct));
            Assert.Equal("connection_failed", bad["status"]!.GetValue<string>());
            Assert.DoesNotContain(wrong, bad.ToJsonString(), StringComparison.Ordinal);
            Assert.Equal(before, await ScalarAsync<string>(rig.Owner, "SELECT concat_ws('|', database, encrypted_password, modified_at::text) FROM config_monitored_servers WHERE server_id = 5141", ct));

            var good = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-41", "{\"database\":\"master\",\"password\":\"" + password + "\"}", real, TestKeyRings.Healthy, null, ct));
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

    /// <summary>#6281: over MCP, where a registration connects is not changed while a route with an agent setting applies to the server. An
    /// Off route counts, and so does one scoped to a tag above the server's own tag. The display name stays editable.</summary>
    [Fact]
    public async Task AsTheMcpRole_AnAgentCoveredServer_CannotBePointedElsewhere_ButItsDisplayNameChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7201, "alpha-71", "alpha-71.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7202, "alpha-72", "alpha-72.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner,
                "INSERT INTO config.server_tags (id, name, parent_id) OVERRIDING SYSTEM VALUE VALUES (7301, 'fleet-a', NULL), (7302, 'prod-a', 7301);" +
                "INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7202, 7302);" +
                "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7201);" +
                "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_tag_id) VALUES ('Blocking', 2, 7301);", ct);

            /* A server-scoped Off route covers 7201. */
            var offMove = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-71", "{\"host\":\"alpha-79.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", offMove["status"]!.GetValue<string>());
            var message = offMove["message"]!.GetValue<string>();
            Assert.Contains("Viewer", message, StringComparison.Ordinal);
            Assert.Contains("web dashboard", message, StringComparison.Ordinal);
            Assert.Equal("alpha-71.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7201", ct));

            /* An agent route on a tag ABOVE 7202's own tag covers 7202; a database change is a change of where it connects too. */
            var tagMove = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-72", "{\"database\":\"orders\"}", Reachable, TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", tagMove["status"]!.GetValue<string>());
            Assert.Equal(0, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE database IS NOT NULL", ct));

            /* The display name is not where it connects. */
            var renamed = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-71", "{\"display_name\":\"Orders\"}", Reachable, TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("updated", renamed["status"]!.GetValue<string>());
            Assert.Equal("Orders", await ScalarAsync<string>(rig.Owner, "SELECT name FROM config_monitored_servers WHERE server_id = 7201", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6281: a server no route with an agent setting is scoped to (a route with no agent setting, or an agent route on another server or
    /// tag) is still pointed elsewhere over MCP; and a route with no scope at all, which applies to every server, does not hold it either.</summary>
    [Fact]
    public async Task AsTheMcpRole_AnUnscopedServer_StillMoves_EvenWithAnAgentRouteThatHasNoScope()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7211, "alpha-73", "alpha-73.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7212, "alpha-74", "alpha-74.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner,
                "INSERT INTO config.server_tags (id, name, parent_id) OVERRIDING SYSTEM VALUE VALUES (7311, 'other-a', NULL);" +
                "INSERT INTO config.config_notification_routes (metric_match, scope_server_id) VALUES ('Blocking', 7211);" +
                "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 1, 7212);" +
                "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_tag_id) VALUES ('Blocking', 2, 7311);", ct);

            var moved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-73", "{\"host\":\"alpha-78.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("updated", moved["status"]!.GetValue<string>());
            Assert.Equal("alpha-78.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7211", ct));

            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Deadlocks', 0)", ct);
            var movedAgain = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-78", "{\"host\":\"alpha-80.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("updated", movedAgain["status"]!.GetValue<string>());
            Assert.Equal("alpha-80.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7211", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6281: over MCP another database or read-only intent on a registered machine is added as usual, unless a route with an agent
    /// setting is scoped to the registration that holds the machine (its id, or a tag or a tag above one), a removed registration included. A route
    /// with no scope never holds an add back. The removed registration's own name is re-used.</summary>
    [Fact]
    public async Task TheAddServersTool_RefusesASecondRegistrationOfAMachineOnlyWhileAScopedAgentRouteCoversTheFirst()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7221, "alpha-75", "alpha-75.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7222, "alpha-78", "alpha-78.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7223, "alpha-79", "alpha-79.example.test", "integrated", null, null, ct);
            var removedId = ServerIdHelper.GetDeterministicHashCode("alpha-76.example.test:orders");
            var removedPlainId = ServerIdHelper.GetDeterministicHashCode("alpha-81.example.test:orders");
            await ExecAsync(rig.Owner,
                "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) " +
                $"VALUES ({removedId}, 'alpha-76.example.test:orders', 'Removed Orders', FALSE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')," +
                $"({removedPlainId}, 'alpha-81.example.test:orders', 'Removed Plain', FALSE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC');" +
                "INSERT INTO config.server_tags (id, name, parent_id) OVERRIDING SYSTEM VALUE VALUES (7321, 'fleet-b', NULL), (7322, 'prod-b', 7321);" +
                "INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7223, 7322);", ct);

            async Task<(string Status, string Detail)> AddAsync(string json)
            {
                var answer = Parse(await Edit.AddServersAsync(rig.Mcp, json, Reachable, ct, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default));
                var row = answer["results"]!.AsArray().Single()!;
                return (row["status"]!.GetValue<string>(), row["detail"]!.GetValue<string>());
            }

            /* Only a route with no scope (it applies to every server): the machine takes another database and another intent. */
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Deadlocks', 0)", ct);
            Assert.Equal("added", (await AddAsync("[{\"host\":\"alpha-75.example.test\",\"database\":\"billing\"}]")).Status);
            Assert.Equal("added", (await AddAsync("[{\"host\":\"ALPHA-75.example.test\",\"read_only_intent\":true}]")).Status);
            Assert.Equal("added", (await AddAsync("[{\"host\":\"alpha-81.example.test\",\"database\":\"billing\"}]")).Status);

            /* An Off route scoped to the registration holds the machine, whatever the database or intent. */
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7222)", ct);
            var scoped = await AddAsync("[{\"host\":\"alpha-78.example.test\",\"database\":\"billing\"}]");
            Assert.Equal("collides", scoped.Status);
            Assert.Contains("alpha-78", scoped.Detail, StringComparison.Ordinal);
            Assert.Equal("collides", (await AddAsync("[{\"host\":\"alpha-78.example.test\",\"read_only_intent\":true}]")).Status);

            /* A route on a tag above the registration's own tag holds it too. */
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_tag_id) VALUES ('Blocking', 2, 7321)", ct);
            Assert.Equal("collides", (await AddAsync("[{\"host\":\"alpha-79.example.test\",\"database\":\"billing\"}]")).Status);

            /* A removed registration holds its machine only while a route is scoped to its id. */
            await ExecAsync(rig.Owner, $"INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 1, {removedId})", ct);
            var removed = await AddAsync("[{\"host\":\"alpha-76.example.test\",\"database\":\"billing\"}]");
            Assert.Equal("collides", removed.Status);
            Assert.Contains("Removed Orders", removed.Detail, StringComparison.Ordinal);
            Assert.Equal("added", (await AddAsync("[{\"host\":\"alpha-81.example.test\",\"database\":\"audit\"}]")).Status);

            Assert.Equal(3 + 4, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers", ct));

            /* The removed registration's own name is re-used, and a fresh machine is added. */
            var again = await AddAsync("[{\"host\":\"alpha-76.example.test\",\"database\":\"orders\"}]");
            Assert.Equal("added", again.Status);
            Assert.Equal("added", (await AddAsync("[{\"host\":\"alpha-77.example.test\"}]")).Status);
            Assert.Equal(removedId, await ScalarAsync<int>(rig.Owner, "SELECT server_id FROM config_monitored_servers WHERE host = 'alpha-76.example.test'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6281: several databases of one logical server go in one call; none of them has a route yet, so none holds another back.</summary>
    [Fact]
    public async Task TheAddServersTool_TakesSeveralDatabasesOfOneMachineInOneCall()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var answer = Parse(await Edit.AddServersAsync(rig.Mcp,
                "[{\"host\":\"alpha-90.example.test\",\"database\":\"one\"},{\"host\":\"alpha-90.example.test\",\"database\":\"two\"}]",
                Reachable, ct, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default));
            Assert.All(answer["results"]!.AsArray(), row => Assert.Equal("added", row!["status"]!.GetValue<string>()));
            Assert.Equal(2, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = 'alpha-90.example.test'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6281: over MCP, a registration no route is scoped to is not pointed at a machine whose registration a route with an agent setting is
    /// scoped to, in any database or intent and in any spelling of the host: the same rule add_servers applies. A machine nothing holds, and the
    /// registration's other settings, still change.</summary>
    [Fact]
    public async Task AsTheMcpRole_AnUnscopedServer_CannotBePointedAtAMachineAScopedRegistrationHolds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7401, "alpha-p1", "alpha-p1.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7402, "alpha-b1", "alpha-b1.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7401)", ct);

            async Task<JsonNode> EditAsync(string changes) =>
                Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-b1", changes, Reachable, TestKeyRings.Healthy, null, ct, policy: DarlingMcpAddServersPolicy.Default));

            foreach (var changes in new[]
            {
                "{\"host\":\"alpha-p1.example.test\",\"database\":\"master\"}",
                "{\"host\":\"alpha-p1.example.test\",\"read_only_intent\":true}",
                "{\"host\":\"ALPHA-P1.example.test.\",\"database\":\"orders\"}",
                "{\"host\":\"tcp:alpha-p1.example.test,1433\",\"database\":\"orders\"}",
                "{\"host\":\"alpha-p1.example.test\\\\MSSQLSERVER\",\"database\":\"orders\"}",
            })
            {
                var refused = await EditAsync(changes);
                Assert.Equal("refused", refused["status"]!.GetValue<string>());
                Assert.Contains("alpha-p1", refused["message"]!.GetValue<string>(), StringComparison.Ordinal);
                Assert.Equal("alpha-b1.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7402", ct));
            }

            /* A machine no route holds, and the display name, still change. */
            Assert.Equal("updated", (await EditAsync("{\"host\":\"alpha-q1.example.test\"}"))["status"]!.GetValue<string>());
            Assert.Equal("updated", (await EditAsync("{\"display_name\":\"Billing\"}"))["status"]!.GetValue<string>());
            Assert.Equal("alpha-q1.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7402", ct));

            /* Without a policy (the web dashboard, the operator) the same move is allowed. */
            var web = Parse(await Edit.EditServerCoreAsync(new Edit.PostgresServerEditStore(rig.Owner), 7402, "{\"host\":\"alpha-p1.example.test\",\"database\":\"master\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", web["status"]!.GetValue<string>());
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6254 (security round 1, M1): the Query Store replica is a second machine the service connects to for a registration, so over MCP it
    /// is held like the host. A server a route with an agent setting covers keeps its replica; a server no route covers is not given a replica on a
    /// machine a covered registration holds (any spelling); a free machine, and the web dashboard (no policy), still take it.</summary>
    [Fact]
    public async Task AsTheMcpRole_TheQueryStoreReplica_IsHeldLikeTheHost_ForACoveredServerAndAHeldMachine()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7501, "rep-covered", "rep-covered.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7502, "rep-free", "rep-free.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7503, "rep-holder", "rep-holder.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner,
                "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7501);" +
                "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 1, 7503)", ct);

            async Task<JsonNode> EditAsync(string name, string changes) =>
                Parse(await Edit.EditServerByNameAsync(rig.Mcp, name, changes, Reachable, TestKeyRings.Healthy, null, ct, policy: DarlingMcpAddServersPolicy.Default));
            Task<string> ReplicaOfAsync(int id) =>
                ScalarAsync<string>(rig.Owner, $"SELECT coalesce(query_store_replica, '') FROM config_monitored_servers WHERE server_id = {id}", ct);

            /* A covered server: the replica cannot be set, and the refusal names it and where it is done. */
            var covered = await EditAsync("rep-covered", "{\"query_store_replica\":\"rep-elsewhere.example.test\"}");
            Assert.Equal("refused", covered["status"]!.GetValue<string>());
            Assert.Contains("Query Store replica", covered["message"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Contains("Viewer", covered["message"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal("", await ReplicaOfAsync(7501));

            /* A free server: not pointed at a machine a covered registration holds, in any spelling of the host. */
            foreach (var spelled in new[] { "rep-holder.example.test", "REP-HOLDER.example.test.", "rep-holder.example.test,1433" })
            {
                var held = await EditAsync("rep-free", $"{{\"query_store_replica\":\"{spelled}\"}}");
                Assert.Equal("refused", held["status"]!.GetValue<string>());
                Assert.Contains("rep-holder", held["message"]!.GetValue<string>(), StringComparison.Ordinal);
                Assert.Equal("", await ReplicaOfAsync(7502));
            }

            /* A machine nothing holds is taken, and a later change of another setting on the same server is not held by the replica it keeps. */
            Assert.Equal("updated", (await EditAsync("rep-free", "{\"query_store_replica\":\"rep-listener.example.test\"}"))["status"]!.GetValue<string>());
            Assert.Equal("rep-listener.example.test", await ReplicaOfAsync(7502));
            Assert.Equal("updated", (await EditAsync("rep-free", "{\"display_name\":\"Free\"}"))["status"]!.GetValue<string>());

            /* Without a policy (the web dashboard, the operator) the covered server takes a replica. */
            var web = Parse(await Edit.EditServerCoreAsync(new Edit.PostgresServerEditStore(rig.Owner), 7501, "{\"query_store_replica\":\"rep-elsewhere.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", web["status"]!.GetValue<string>());
            Assert.Equal("rep-elsewhere.example.test", await ReplicaOfAsync(7501));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6254 (security round 1, M1): add_servers runs the replica host through the held-machine check the host goes through, so a new
    /// registration is not given a replica on a machine a covered registration holds; and a replica with a protocol prefix (L3) is refused.</summary>
    [Fact]
    public async Task AsTheMcpRole_AddServers_HoldsTheReplicaHost_AndRefusesAProtocolPrefix()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7511, "rep-holder-2", "rep-holder-2.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7511)", ct);

            async Task<(string Status, string Detail)> AddAsync(string json)
            {
                var answer = Parse(await Edit.AddServersAsync(rig.Mcp, json, Reachable, ct, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default));
                var row = answer["results"]!.AsArray().Single()!;
                return (row["status"]!.GetValue<string>(), row["detail"]!.GetValue<string>());
            }

            var held = await AddAsync("[{\"host\":\"rep-new-1.example.test\",\"query_store_replica\":\"Rep-Holder-2.example.test\"}]");
            Assert.Equal("collides", held.Status);
            Assert.Contains("rep-holder-2", held.Detail, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(0, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = 'rep-new-1.example.test'", ct));

            Assert.Equal("added", (await AddAsync("[{\"host\":\"rep-new-2.example.test\",\"query_store_replica\":\"rep-free-listener.example.test\"}]")).Status);
            Assert.Equal("rep-free-listener.example.test",
                await ScalarAsync<string>(rig.Owner, "SELECT query_store_replica FROM config_monitored_servers WHERE host = 'rep-new-2.example.test'", ct));

            foreach (var replica in new[] { "np:\\\\attacker\\pipe\\sql\\query", "lpc:attacker", "admin:attacker", "tcp:tcp:attacker", "\\\\attacker\\share" })
            {
                var json = "[{\"host\":\"rep-new-3.example.test\",\"query_store_replica\":" + JsonValue.Create(replica)!.ToJsonString() + "}]";
                var refused = await AddAsync(json);
                Assert.NotEqual("added", refused.Status);
                Assert.Equal(0, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = 'rep-new-3.example.test'", ct));
            }

            /* The edit refuses the same values, with the same sentence. */
            await SeedServerAsync(rig.Owner, 7512, "rep-edit-3", "rep-edit-3.example.test", "integrated", null, null, ct);
            var edit = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "rep-edit-3",
                "{\"query_store_replica\":" + JsonValue.Create("np:\\\\attacker\\pipe\\sql\\query")!.ToJsonString() + "}", Reachable,
                TestKeyRings.Healthy, null, ct, policy: DarlingMcpAddServersPolicy.Default));
            Assert.NotEqual("updated", edit["status"]!.GetValue<string>());
            Assert.Equal("", await ScalarAsync<string>(rig.Owner, "SELECT coalesce(query_store_replica, '') FROM config_monitored_servers WHERE server_id = 7512", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6281: a route set up while the connection test runs is seen by the write itself, which checks inside its own transaction: the edit
    /// is refused and nothing is saved, whether the route holds the machine the registration moves to or the registration itself; and an add whose
    /// machine gets a route during its test is refused the same way.</summary>
    [Fact]
    public async Task ARouteSetUpDuringTheConnectionTest_RefusesTheEditAndTheAdd_AndWritesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7411, "alpha-r1", "alpha-r1.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7412, "alpha-r2", "alpha-r2.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7413, "alpha-r3", "alpha-r3.example.test", "integrated", null, null, ct);

            Edit.ServerProbe RoutedDuringProbe(int scopedServerId) => async (_, c) =>
            {
                await ExecAsync(rig.Owner, $"INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 1, {scopedServerId})", c);
                return new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null);
            };

            /* The machine the edit moves to gets a route during the test. */
            var moved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-r2", "{\"host\":\"alpha-r1.example.test\",\"database\":\"orders\"}", RoutedDuringProbe(7411), TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", moved["status"]!.GetValue<string>());
            Assert.Equal("alpha-r2.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7412", ct));

            /* The registration being edited gets a route during the test. */
            var own = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-r3", "{\"host\":\"alpha-r9.example.test\"}", RoutedDuringProbe(7413), TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", own["status"]!.GetValue<string>());
            Assert.Equal("alpha-r3.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7413", ct));

            /* #6254 (security round 3, L1): a replica-only edit moves the connection too, so a route set up on the registration during its
               test is seen by the write's own check, and the replica is not saved. */
            await SeedServerAsync(rig.Owner, 7415, "alpha-r5", "alpha-r5.example.test", "integrated", null, null, ct);
            var replicaOnly = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-r5", "{\"query_store_replica\":\"alpha-r5-listener.example.test\"}", RoutedDuringProbe(7415), TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", replicaOnly["status"]!.GetValue<string>());
            Assert.Equal("", await ScalarAsync<string>(rig.Owner, "SELECT coalesce(query_store_replica, '') FROM config_monitored_servers WHERE server_id = 7415", ct));

            /* An add of another database of a machine that gets a route during the test. */
            await SeedServerAsync(rig.Owner, 7414, "alpha-r4", "alpha-r4.example.test", "integrated", null, null, ct);
            var added = Parse(await Edit.AddServersAsync(rig.Mcp, "[{\"host\":\"alpha-r4.example.test\",\"database\":\"billing\"}]", RoutedDuringProbe(7414), ct,
                ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("collides", added["results"]!.AsArray().Single()!["status"]!.GetValue<string>());
            Assert.Equal(1, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = 'alpha-r4.example.test'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6281: the machine a registration holds is told by its host with the spelling folded away: case, a trailing dot, a tcp: prefix,
    /// host,port, host\MSSQLSERVER and an explicit default port are the same machine, so an add in any of them is refused; a named instance and another
    /// port are other machines.</summary>
    [Fact]
    public async Task TheAddServersTool_RefusesEverySpellingOfAHeldMachine_AndAddsAnotherInstanceOrPort()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7431, "alpha-s1", "alpha-s1.example.test", "integrated", null, null, ct);
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7431)", ct);

            async Task<string> AddAsync(string json) =>
                Parse(await Edit.AddServersAsync(rig.Mcp, json, Reachable, ct, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default))["results"]!.AsArray().Single()!["status"]!.GetValue<string>();

            foreach (var json in new[]
            {
                "[{\"host\":\"ALPHA-S1.EXAMPLE.TEST\",\"database\":\"d1\"}]",
                "[{\"host\":\"alpha-s1.example.test.\",\"database\":\"d2\"}]",
                "[{\"host\":\"tcp:alpha-s1.example.test\",\"database\":\"d3\"}]",
                "[{\"host\":\"alpha-s1.example.test,1433\",\"database\":\"d4\"}]",
                "[{\"host\":\"tcp:alpha-s1.example.test,1433\",\"database\":\"d5\"}]",
                "[{\"host\":\"alpha-s1.example.test\\\\MSSQLSERVER\",\"database\":\"d6\"}]",
            })
            {
                Assert.Equal("collides", await AddAsync(json));
            }

            /* #6583: the port field on a SQL Server entry is refused, not folded into the machine's identity; the port goes in host, as the cases above spell it. */
            Assert.Equal("invalid", await AddAsync("[{\"host\":\"alpha-s1.example.test\",\"port\":1433,\"database\":\"d7\"}]"));

            Assert.Equal(1, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers", ct));

            /* Another port, and a named instance, are other engine instances. */
            Assert.Equal("added", await AddAsync("[{\"host\":\"alpha-s1.example.test,1444\",\"database\":\"d8\"}]"));
            Assert.Equal("added", await AddAsync("[{\"host\":\"alpha-s1.example.test\\\\REPORTS\",\"database\":\"d9\"}]"));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    private static Edit.ServerProbe ReportsIdentity(string identity) => (_, _) => Task.FromResult(
        new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null, MachineIdentity: identity));

    /// <summary>What the connect path writes (V212): the registry row of a connected server carries the name it reported.</summary>
    private static Task SeedIdentityAsync(NpgsqlDataSource owner, int id, string name, string? identity, CancellationToken ct) =>
        ExecAsync(owner, $@"INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, created_date, modified_date, machine_identity)
            VALUES ({id}, '{name}', '{name}', TRUE, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC', {(identity is null ? "NULL" : "'" + identity + "'")})
            ON CONFLICT (server_id) DO UPDATE SET machine_identity = EXCLUDED.machine_identity", ct);

    private static async Task<(string Status, string Detail)> AddOneAsync(Rig rig, string host, Edit.ServerProbe probe, CancellationToken ct)
    {
        var answer = Parse(await Edit.AddServersAsync(rig.Mcp, $"[{{\"host\":\"{host}\"}}]", probe, ct,
            ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default));
        var row = answer["results"]!.AsArray().Single()!;
        return (row["status"]!.GetValue<string>(), row["detail"]!.GetValue<string>());
    }

    /// <summary>#6285, #6339: the identity the last connect wrote to <c>collect.servers.machine_identity</c> is what the server the add reached
    /// is compared with, over MCP, while a route with an agent setting is scoped to the registration. The newest connect's name counts, an
    /// Azure SQL Database (none stored) and a registration with none are held to the host text alone, and a registration whose host text
    /// equals another machine's reported name does not stand in for that machine. Emptying <c>collect.server_properties</c>, as a retention
    /// purge does, leaves the identity in place.</summary>
    [Fact]
    public async Task TheAddServersTool_RefusesAnotherNameOfAMachineWhoseRegistryIdentityMatches_AndAPurgeLeavesTheIdentity()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            /* 7231 is registered under its address and reports 'alpha-91'; 7234 is registered under the TEXT 'alpha-97' and reports it. */
            await SeedServerAsync(rig.Owner, 7231, "alpha-91", "10.9.9.91", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7232, "alpha-92", "alpha-92", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7233, "azure-93", "azure-93.database.windows.net", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7234, "alpha-97", "alpha-97", "integrated", null, null, ct);
            await SeedIdentityAsync(rig.Owner, 7231, "10.9.9.91", "oldname", ct);
            await SeedIdentityAsync(rig.Owner, 7231, "10.9.9.91", "alpha-91", ct);
            await SeedIdentityAsync(rig.Owner, 7232, "alpha-92", null, ct);
            await SeedIdentityAsync(rig.Owner, 7233, "azure-93", null, ct);
            await SeedIdentityAsync(rig.Owner, 7234, "alpha-97", "alpha-97", ct);
            await ExecAsync(rig.Owner,
                "INSERT INTO collect.server_properties (collection_id, collection_time, server_id, server_name, engine_edition) VALUES " +
                "(1, now() AT TIME ZONE 'UTC' - interval '1 hour', 7231, '10.9.9.91', 3);" +
                "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7231), ('Blocking', 0, 7232), ('Blocking', 0, 7233), ('Blocking', 0, 7234);", ct);

            /* The name the server reported on its last connect names the machine: another name for it is refused, and nothing is written. */
            var held = await AddOneAsync(rig, "alpha-91.example.test", ReportsIdentity("ALPHA-91"), ct);
            Assert.Equal("collides", held.Status);
            Assert.Contains("'alpha-91'", held.Detail, StringComparison.Ordinal);
            Assert.Contains("(the server reports the same name for itself)", held.Detail, StringComparison.Ordinal);
            Assert.Equal(0, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = 'alpha-91.example.test'", ct));

            /* A purge that empties the fact table leaves the registry's identity: the guard still refuses. */
            await ExecAsync(rig.Owner, "DELETE FROM collect.server_properties", ct);
            Assert.Equal("collides", (await AddOneAsync(rig, "alpha-91.example.test", ReportsIdentity("alpha-91"), ct)).Status);

            /* The older connect's name is not the machine's any more; a registration with none stored and an Azure SQL Database's node
               name are held to the host text alone. */
            Assert.Equal("added", (await AddOneAsync(rig, "10.1.2.4", ReportsIdentity("oldname"), ct)).Status);
            Assert.Equal("added", (await AddOneAsync(rig, "10.1.2.5", ReportsIdentity("alpha-92"), ct)).Status);
            Assert.Equal("added", (await AddOneAsync(rig, "azure-93b.database.windows.net", ReportsIdentity("a1b2c3d4e5f6"), ct)).Status);

            /* The text rule still answers for its own spelling. */
            Assert.Equal("collides", (await AddOneAsync(rig, "ALPHA-97.", ReportsIdentity("unrelated"), ct)).Status);
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6285 (M2): a removed covered registration's storage name is skipped only when the add's own storage key makes the same id.
    /// Another spelling of it makes a new id outside the route and is refused; the exact key re-uses the id, so the route follows.</summary>
    [Fact]
    public async Task TheAddServersTool_RefusesAnotherSpellingOfARemovedCoveredKey_AndReusesTheExactKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var id = PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode("SQLPROD01");
            await SeedIdentityAsync(rig.Owner, id, "SQLPROD01", null, ct);
            await ExecAsync(rig.Owner, $"INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, {id});", ct);

            var other = await AddOneAsync(rig, "sqlprod01.", ReportsIdentity("none-such"), ct);
            Assert.Equal("collides", other.Status);
            Assert.Contains("a removed registration", other.Detail, StringComparison.Ordinal);

            Assert.Equal("collides", (await AddOneAsync(rig, "sqlprod01", ReportsIdentity("none-such"), ct)).Status);

            Assert.Equal("added", (await AddOneAsync(rig, "SQLPROD01", ReportsIdentity("none-such"), ct)).Status);
            Assert.Equal(id, await ScalarAsync<int>(rig.Owner, "SELECT server_id FROM config_monitored_servers WHERE host = 'SQLPROD01'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6285 (L3): the recheck inside the add's write transaction. The registrations were loaded before the connection test; the
    /// held registration's identity reaches the registry while the probe runs, so the checks made around the probe (on the loaded list) pass
    /// and only the check in the write transaction, given the probed identity, can refuse. Dropping the identity from that check turns this
    /// red.</summary>
    [Fact]
    public async Task TheAddServersTool_RechecksTheIdentityInsideTheWriteTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7251, "alpha-71", "alpha-71", "integrated", null, null, ct);
            await SeedIdentityAsync(rig.Owner, 7251, "alpha-71", null, ct);
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7251);", ct);

            Edit.ServerProbe slowToReport = async (_, token) =>
            {
                await SeedIdentityAsync(rig.Owner, 7251, "alpha-71", "alpha-71", token);
                return new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null, MachineIdentity: "alpha-71");
            };

            var refused = await AddOneAsync(rig, "10.7.1.1", slowToReport, ct);
            Assert.Equal("collides", refused.Status);
            Assert.Equal(0, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = '10.7.1.1'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6285: edit_server applies the identity check to the machine a registration is moved to, and in the write's own transaction
    /// too: the registration being edited is not held against itself, and a move to a removed covered registration's own key is refused
    /// (M2), because an edit keeps its own id and the key is not re-use. A change to another connection field (L1) is held to the
    /// identity as well.</summary>
    [Fact]
    public async Task TheEditServerTool_RefusesAMoveToAnotherNameOfAHeldMachine_AndToARemovedCoveredKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7241, "alpha-95", "alpha-95", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7242, "alpha-96", "alpha-96.example.test", "integrated", null, null, ct);
            var removedId = PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode("10.1.2.8");
            await SeedIdentityAsync(rig.Owner, 7241, "alpha-95", "alpha-95", ct);
            await SeedIdentityAsync(rig.Owner, 7242, "alpha-96", "alpha-96", ct);
            await SeedIdentityAsync(rig.Owner, removedId, "10.1.2.8", "alpha-98", ct);
            await ExecAsync(rig.Owner,
                $"INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7241), ('Blocking', 0, {removedId});", ct);

            var refused = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-96", "{\"host\":\"10.1.2.6\"}", ReportsIdentity("alpha-95"), TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", refused["status"]!.GetValue<string>());
            Assert.Contains("alpha-95", refused["message"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal("alpha-96.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7242", ct));

            /* M2: the exact key of a removed covered registration is not skipped for an edit. */
            var toRemoved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-96", "{\"host\":\"10.1.2.8\"}", ReportsIdentity("unrelated"), TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", toRemoved["status"]!.GetValue<string>());
            Assert.Contains("a removed registration", toRemoved["message"]!.GetValue<string>(), StringComparison.Ordinal);

            /* L1: another connection field, host unchanged, reaching a held machine's identity is refused too. */
            var subnet = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-96", "{\"multi_subnet_failover\":true}", ReportsIdentity("alpha-95"), TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", subnet["status"]!.GetValue<string>());

            /* Its own collected name is not held against it, and a machine nothing holds is a plain move. */
            var moved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-96", "{\"host\":\"10.1.2.7\"}", ReportsIdentity("alpha-96"), TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("updated", moved["status"]!.GetValue<string>());
            Assert.Equal("10.1.2.7", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7242", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6285 (L3): the recheck inside the edit's write transaction. The list the edit loads around the probe is stale (it holds no
    /// identity for the held registration), while the write transaction reloads the registry, where the identity now is: only the in-transaction
    /// check, given the probed identity, refuses.</summary>
    [Fact]
    public async Task TheEditServerTool_RechecksTheIdentityInsideTheWriteTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7261, "alpha-81", "alpha-81", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 7262, "alpha-82", "alpha-82.example.test", "integrated", null, null, ct);
            await SeedIdentityAsync(rig.Owner, 7261, "alpha-81", null, ct);
            await ExecAsync(rig.Owner, "INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, 7261);", ct);

            Edit.ServerProbe slowToReport = async (_, token) =>
            {
                await SeedIdentityAsync(rig.Owner, 7261, "alpha-81", "alpha-81", token);
                return new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null, MachineIdentity: "alpha-81");
            };
            var stale = new List<Edit.RegisteredTarget>
            {
                new(7261, "alpha-81", "alpha-81", "sqlserver", 0, "alpha-81", false),
                new(7262, "alpha-82", "alpha-82.example.test", "sqlserver", 0, "alpha-82", false),
            };
            var (changes, error) = Edit.ParseEditChanges("{\"host\":\"10.7.2.2\"}");
            Assert.Null(error);

            var refused = Parse(await Edit.EditServerCoreAsync(new Edit.PostgresServerEditStore(rig.Mcp), 7262, changes!, slowToReport, TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default,
                agentRouteCovering: (id, token) => Edit.ScopedAgentRouteAsync(rig.Mcp, id, token),
                loadTargets: _ => Task.FromResult(stale)));

            Assert.Equal("refused", refused["status"]!.GetValue<string>());
            Assert.Equal("alpha-82.example.test", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7262", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6340 round 2 (N2): moving a registration to another address clears the machine identity its last connect stored, since that
    /// value names the machine at the old address; an edit that leaves the address alone keeps it. The write is the function the edit calls,
    /// so the Viewer and the web route get the same behaviour as the MCP tool.</summary>
    [Fact]
    public async Task AnEditThatMovesTheAddress_ClearsTheStoredIdentity_AndAnEditThatDoesNotMoveItKeepsIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 7311, "alpha-311", "alpha-311.example.test", "integrated", null, null, ct);
            await SeedIdentityAsync(rig.Owner, 7311, "alpha-311.example.test", "alpha-311", ct);

            var cost = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-311", "{\"monthly_cost_usd\":99}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", cost["status"]!.GetValue<string>());
            Assert.Equal("alpha-311", await ScalarAsync<string>(rig.Owner, "SELECT machine_identity FROM collect.servers WHERE server_id = 7311", ct));

            var moved = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-311", "{\"host\":\"alpha-312.example.test\"}", Reachable, TestKeyRings.Healthy, null, ct));
            Assert.Equal("updated", moved["status"]!.GetValue<string>());
            Assert.Null(await ScalarAsync<string?>(rig.Owner, "SELECT machine_identity FROM collect.servers WHERE server_id = 7311", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>#6340 round 2 (N4): an Aurora cluster gives no machine identity, since nothing shows its <c>system_identifier</c> to be unique per
    /// cluster; the same session on any other PostgreSQL reads one.</summary>
    [Fact]
    public async Task ThePostgresIdentityProbe_ReadsAnIdentity_ButNoneOnAnAuroraCluster()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await using var connection = await rig.Owner.OpenConnectionAsync(ct);
            Assert.NotNull(await DarlingServerConnector.ProbePostgresIdentityAsync(connection, null, ct));
            Assert.Null(await DarlingServerConnector.ProbePostgresIdentityAsync(connection, null, ct, isAurora: true));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>Registers <paramref name="config"/>'s server the way the service does on a connect: the real connect, then the registry write.
    /// Returns the runtime, and the identity the registry holds for it.</summary>
    private static async Task<(ServerRuntime Runtime, string? Stored)> ConnectAndRegisterAsync(Rig rig, MonitoredServer config, CancellationToken ct)
    {
        var runtime = await DarlingServerConnector.ConnectAsync(config, null, ct);
        await DarlingObservability.UpsertServerAsync(rig.Owner, runtime, null, ct);
        var stored = await ScalarAsync<string?>(rig.Owner, $"SELECT machine_identity FROM collect.servers WHERE server_id = {runtime.ServerId}", ct);
        return (runtime, stored);
    }

    private static Edit.ServerProbe RealProbe => (server, token) => DarlingServerConnector.ProbeAsync(server, null, token);

    /// <summary>#6339, through the real connect path: a PostgreSQL connect reads the cluster's identifier and port, the registry write keeps
    /// them in <c>collect.servers.machine_identity</c>, and the same cluster reached under another host name is refused by add_servers while a
    /// route with an agent setting is scoped to the first registration. A role that may not call <c>pg_control_system()</c> connects all the
    /// same and stores none.</summary>
    [Fact]
    public async Task ThroughTheRealConnect_APostgresClusterUnderASecondHostNameIsRefused_AndARoleThatCannotReadTheIdentityKeepsTheStoredOne()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        string? role = null;
        try
        {
            var admin = new NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!);
            MonitoredServer Target(string host, string user, string name) => new()
            {
                Name = name, Host = host, Port = admin.Port, Database = rig.Scratch.DatabaseName, Engine = "postgres", Auth = "sql",
                Username = user, Password = "unused", EncryptMode = "Optional",
            };

            var (runtime, stored) = await ConnectAndRegisterAsync(rig, Target("127.0.0.1", admin.Username!, "pg-6339-first"), ct);
            var expected = await ScalarAsync<string>(rig.Owner, "SELECT s.system_identifier::text || ':' || current_setting('port') FROM pg_control_system() AS s", ct);
            Assert.Equal(expected, runtime.MachineIdentity);
            Assert.Equal(expected, stored);

            /* The configured registration, and a route with an agent setting scoped to it. */
            await SeedServerAsync(rig.Owner, runtime.ServerId, "pg-6339-first", "127.0.0.1", "sql", admin.Username, null, ct);
            await ExecAsync(rig.Owner, $"UPDATE config_monitored_servers SET engine = 'postgres', port = {admin.Port} WHERE server_id = {runtime.ServerId}", ct);
            await ExecAsync(rig.Owner, $"INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, {runtime.ServerId});", ct);

            /* The same cluster under another host name: refused, and nothing is written. */
            var json = System.Text.Json.JsonSerializer.Serialize(new[]
            {
                new { host = "localhost", engine = "postgres", port = admin.Port, database = rig.Scratch.DatabaseName, auth = "sql", username = admin.Username, password = "unused", encrypt_mode = "Optional" },
            });
            var answer = Parse(await Edit.AddServersAsync(rig.Mcp, json, RealProbe, ct, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default));
            var row = answer["results"]!.AsArray().Single()!;
            Assert.Equal("collides", row["status"]!.GetValue<string>());
            Assert.Contains("the server reports the same name for itself", row["detail"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal(0, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = 'localhost'", ct));

            /* A role that may not call pg_control_system(): the connect works and reads no identity. The registry keeps what the same
               registration stored before, rather than writing NULL over it (#6340 round 2, N1): a swap of the login must not blank it. */
            role = "noctl_" + Guid.NewGuid().ToString("N")[..8];
            await ExecAsync(rig.Owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER", ct);
            await ExecAsync(rig.Owner, "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC", ct);
            var (blind, blindStored) = await ConnectAndRegisterAsync(rig, Target("127.0.0.1", role, "pg-6339-blind"), ct);
            Assert.Null(blind.MachineIdentity);
            Assert.Equal(expected, blindStored);
            Assert.True(blind.Target.PostgresMajorVersion > 0);

            /* N1: over MCP, the same cluster under a second host name through that login reads no identity, and a covered PostgreSQL
               registration holds one: refused (not matched by host text alone), and the message says what grant fixes it. */
            var blindJson = System.Text.Json.JsonSerializer.Serialize(new[]
            {
                new { host = "localhost", engine = "postgres", port = admin.Port, database = rig.Scratch.DatabaseName, auth = "sql", username = role, password = "unused", encrypt_mode = "Optional" },
            });
            var blindAdd = Parse(await Edit.AddServersAsync(rig.Mcp, blindJson, RealProbe, ct, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default))["results"]!.AsArray().Single()!;
            Assert.Equal("collides", blindAdd["status"]!.GetValue<string>());
            Assert.Contains("GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system()", blindAdd["detail"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal(0, await ScalarAsync<int>(rig.Owner, "SELECT count(*)::int FROM config_monitored_servers WHERE host = 'localhost'", ct));

            /* The same for an edit that moves another registration to that address. */
            await SeedServerAsync(rig.Owner, 7301, "pg-6340-mover", "198.51.100.9", "sql", role, "blob-7301", ct);
            await ExecAsync(rig.Owner, $"UPDATE config_monitored_servers SET engine = 'postgres', port = {admin.Port}, database = '{rig.Scratch.DatabaseName}', encrypt_mode = 'Optional' WHERE server_id = 7301", ct);
            var blindEdit = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "pg-6340-mover", "{\"host\":\"localhost\",\"password\":\"unused\"}", RealProbe, TestKeyRings.Healthy, null, ct,
                policy: DarlingMcpAddServersPolicy.Default));
            Assert.Equal("refused", blindEdit["status"]!.GetValue<string>());
            Assert.Contains("pg_control_system()", blindEdit["message"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal("198.51.100.9", await ScalarAsync<string>(rig.Owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7301", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok, role is null ? Array.Empty<string>() : new[] { role });
        }
    }

    /// <summary>#6285, through the real connect path against a SQL Server (sql2019 on the dev box): the connect reads
    /// <c>SERVERPROPERTY('ServerName')</c>, the registry write keeps it in <c>collect.servers.machine_identity</c> in lower case, and the same
    /// server reached under a second name is refused by add_servers while a route with an agent setting is scoped to the first registration.
    /// Read only against the SQL Server: the connection test's SELECTs.</summary>
    [Fact]
    public async Task ThroughTheRealConnect_ASqlServerUnderASecondNameIsRefused()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(host), "Set DARLING_TEST_SQL (and _USER / _PASSWORD for SQL auth) to run the live connect check.");
        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD");
        var ct = TestContext.Current.CancellationToken;
        var other = string.IsNullOrEmpty(user)
            ? (await System.Net.Dns.GetHostEntryAsync(host!, ct)).HostName
            : (await System.Net.Dns.GetHostAddressesAsync(host!, ct)).FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)?.ToString();
        Assert.SkipWhen(string.IsNullOrEmpty(other) || string.Equals(other, host, StringComparison.OrdinalIgnoreCase), "The host has no second name to reach it by.");

        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var first = new MonitoredServer
            {
                Name = "sql-6285-first", Host = host!, Auth = string.IsNullOrEmpty(user) ? "integrated" : "sql", Username = user, Password = password,
                TrustServerCertificate = true,
            };
            var (runtime, stored) = await ConnectAndRegisterAsync(rig, first, ct);
            Assert.False(string.IsNullOrWhiteSpace(runtime.MachineIdentity));
            Assert.Equal(runtime.MachineIdentity!.Trim().ToLowerInvariant(), stored);

            await SeedServerAsync(rig.Owner, runtime.ServerId, "sql-6285-first", host!, "integrated", null, null, ct);
            await ExecAsync(rig.Owner, $"INSERT INTO config.config_notification_routes (metric_match, agent_level, scope_server_id) VALUES ('Blocking', 0, {runtime.ServerId});", ct);

            var json = System.Text.Json.JsonSerializer.Serialize(new[]
            {
                string.IsNullOrEmpty(user)
                    ? (object)new { host = other, trust_server_certificate = true }
                    : new { host = other, auth = "sql", username = user, password, trust_server_certificate = true },
            });
            var answer = Parse(await Edit.AddServersAsync(rig.Mcp, json, RealProbe, ct, ring: TestKeyRings.Healthy, policy: DarlingMcpAddServersPolicy.Default));
            var row = answer["results"]!.AsArray().Single()!;
            Assert.Equal("collides", row["status"]!.GetValue<string>());
            Assert.Contains("the server reports the same name for itself", row["detail"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal(0, await ScalarAsync<int>(rig.Owner, $"SELECT count(*)::int FROM config_monitored_servers WHERE host = '{other}'", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }
}
