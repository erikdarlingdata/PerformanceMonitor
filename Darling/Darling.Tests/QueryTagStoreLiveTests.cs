/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// The query-tag store tables (V192, #5977, wave 1 of #5971) on a real store: the migration, the constraints, the version
/// bump, the cascades, server removal on both removal paths and retention. One test per behaviour.
/// </summary>
public sealed class QueryTagStoreLiveTests
{
    private const int ServerA = 7201;
    private const int ServerB = 7202;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, string, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the query-tag store pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var store = new NpgsqlConnection(connectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);

        var bodySucceeded = false;
        try
        {
            await body(store, connectionString, ct);
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

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture)!;
    }

    private static Task<string> RulesVersionAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync(connection, "SELECT rules_version FROM config.query_tag_state WHERE id = 1", ct);

    private static async Task<int> NewTagAsync(NpgsqlConnection connection, string name, CancellationToken ct) =>
        int.Parse(await ScalarAsync(connection, $"INSERT INTO config.query_tags (name) VALUES ('{name}') RETURNING id", ct), CultureInfo.InvariantCulture);

    private static Task AddHashAssignmentAsync(NpgsqlConnection connection, int tagId, string? serverId, string hash, CancellationToken ct) =>
        ExecAsync(connection,
            $"INSERT INTO config.query_tag_assignments (tag_id, kind, server_id, query_hash, created_by) VALUES ({tagId}, 'hash', {serverId ?? "NULL"}, '{hash}', 'test')", ct);

    private static async Task AssertSqlStateAsync(NpgsqlConnection connection, string sql, string sqlState, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, sql, ct));
        Assert.Equal(sqlState, ex.SqlState);
    }

    [Fact]
    public async Task TheMigration_AppliesOnAFreshStore_AndOnAStoreAtThePreviousVersion_AndASecondRunChangesNothing()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            const string Tables = "config.query_tags,config.query_tag_assignments,config.query_tag_rules,collect.query_tag_sightings,config.query_tag_map,config.query_tag_state";
            async Task<string> PresentAsync() => await ScalarAsync(store,
                "SELECT string_agg(t, ',' ORDER BY t) FROM unnest(string_to_array('" + Tables + "', ',')) AS t WHERE to_regclass(t) IS NOT NULL", ct);
            var expected = string.Join(",", Tables.Split(',').OrderBy(t => t, StringComparer.Ordinal));

            /* A fresh store holds all six tables and a state row at version 1, nothing built. */
            Assert.Equal(expected, await PresentAsync());
            Assert.Equal("1|0", await ScalarAsync(store, "SELECT rules_version || '|' || built_version FROM config.query_tag_state", ct));

            /* A store at the previous version: the rung's objects and its version row gone, then migrated. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(store, 191, ct);
            await ExecAsync(store, "DELETE FROM darling_schema_version WHERE version >= 192", ct);
            Assert.Equal(string.Empty, await PresentAsync());
            await PgMigrations.MigrateAsync(store, ct);
            Assert.Equal(expected, await PresentAsync());
            Assert.Equal(StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture), await ScalarAsync(store, "SELECT max(version) FROM darling_schema_version", ct));

            /* A second run of the rung's SQL changes nothing: the rows stay, the state row is neither reset nor bumped. */
            var tag = await NewTagAsync(store, "Reports", ct);
            await AddHashAssignmentAsync(store, tag, null, "0xAB", ct);
            var before = await RulesVersionAsync(store, ct);
            await ExecAsync(store, QueryTagSchema.CreateSql, ct);
            await ExecAsync(store, QueryTagSchema.CreateSql, ct);
            Assert.Equal(before, await RulesVersionAsync(store, ct));
            Assert.Equal("1|1", await ScalarAsync(store, "SELECT (SELECT count(*) FROM config.query_tags) || '|' || (SELECT count(*) FROM config.query_tag_assignments)", ct));
        });
    }

    [Fact]
    public async Task AHashAssignmentWithoutAHash_IsRefused_ByTheCheck()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            var tag = await NewTagAsync(store, "Reports", ct);
            await AssertSqlStateAsync(store,
                $"INSERT INTO config.query_tag_assignments (tag_id, kind, created_by) VALUES ({tag}, 'hash', 'test')", "23514", ct);
            await AssertSqlStateAsync(store,
                $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, object_name, created_by) VALUES ({tag}, 'hash', '0x1', 'p', 'test')", "23514", ct);
        });
    }

    [Fact]
    public async Task AnObjectAssignmentWithoutAllThreeParts_IsRefused_ByTheCheck()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            var tag = await NewTagAsync(store, "Reports", ct);
            foreach (var parts in new[] { "NULL, 'dbo', 'p'", "'db', NULL, 'p'", "'db', 'dbo', NULL", "'db', 'dbo', ''" })
            {
                await AssertSqlStateAsync(store,
                    $"INSERT INTO config.query_tag_assignments (tag_id, kind, database_name, schema_name, object_name, created_by) VALUES ({tag}, 'object', {parts}, 'test')", "23514", ct);
            }

            await ExecAsync(store,
                $"INSERT INTO config.query_tag_assignments (tag_id, kind, database_name, schema_name, object_name, created_by) VALUES ({tag}, 'object', 'db', 'dbo', 'p', 'test')", ct);
        });
    }

    [Fact]
    public async Task TwoHandAssignmentsForOneKeyAndServerScope_AreRefused_AndAnotherScopeIsNot()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            var tagA = await NewTagAsync(store, "A", ct);
            var tagB = await NewTagAsync(store, "B", ct);

            await AddHashAssignmentAsync(store, tagA, ServerA.ToString(CultureInfo.InvariantCulture), "0x01", ct);
            await Assert.ThrowsAsync<PostgresException>(() => AddHashAssignmentAsync(store, tagB, ServerA.ToString(CultureInfo.InvariantCulture), "0x01", ct));

            /* The same key on another server, and on every server (NULL), is a different scope. */
            await AddHashAssignmentAsync(store, tagB, ServerB.ToString(CultureInfo.InvariantCulture), "0x01", ct);
            await AddHashAssignmentAsync(store, tagB, null, "0x01", ct);
            await AssertSqlStateAsync(store,
                $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({tagA}, 'hash', '0x01', 'test')", "23505", ct);

            /* The same holds for an object key. */
            const string Object = "kind, server_id, database_name, schema_name, object_name, created_by) VALUES ({0}, 'object', {1}, 'db', 'dbo', 'p', 'test')";
            await ExecAsync(store, "INSERT INTO config.query_tag_assignments (tag_id, " + string.Format(CultureInfo.InvariantCulture, Object, tagA, ServerA), ct);
            await AssertSqlStateAsync(store, "INSERT INTO config.query_tag_assignments (tag_id, " + string.Format(CultureInfo.InvariantCulture, Object, tagB, ServerA), "23505", ct);
        });
    }

    [Fact]
    public async Task AWriteToTagsAssignmentsOrRules_BumpsTheRulesVersion_AndTheMapDoesNot()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            var version = long.Parse(await RulesVersionAsync(store, ct), CultureInfo.InvariantCulture);

            async Task ExpectBumpAsync(string sql)
            {
                await ExecAsync(store, sql, ct);
                var now = long.Parse(await RulesVersionAsync(store, ct), CultureInfo.InvariantCulture);
                Assert.True(now > version, sql);
                version = now;
            }

            await ExpectBumpAsync("INSERT INTO config.query_tags (name) VALUES ('Reports')");
            var tag = int.Parse(await ScalarAsync(store, "SELECT id FROM config.query_tags", ct), CultureInfo.InvariantCulture);
            await ExpectBumpAsync("UPDATE config.query_tags SET colour = '#112233'");
            await ExpectBumpAsync($"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({tag}, 'hash', '0x01', 'test')");
            await ExpectBumpAsync("DELETE FROM config.query_tag_assignments");
            await ExpectBumpAsync($"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({tag}, 'application', 'SSMS%', 'test')");
            await ExpectBumpAsync("UPDATE config.query_tag_rules SET enabled = false");
            await ExpectBumpAsync("DELETE FROM config.query_tag_rules");

            /* The map is the build's output, not a rule input: writing it does not ask for another build. */
            await ExecAsync(store, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ({ServerA}, 'hash', '0x01', {tag}, 'rule')", ct);
            Assert.Equal(version.ToString(CultureInfo.InvariantCulture), await RulesVersionAsync(store, ct));
        });
    }

    [Fact]
    public async Task DeletingATag_RemovesItsAssignmentsRulesAndMapRows_AndDeletingARuleLeavesItsMapRows()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            var gone = await NewTagAsync(store, "Gone", ct);
            var kept = await NewTagAsync(store, "Kept", ct);
            foreach (var tag in new[] { gone, kept })
            {
                await AddHashAssignmentAsync(store, tag, ServerA.ToString(CultureInfo.InvariantCulture), "0x0" + tag, ct);
                await ExecAsync(store, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({tag}, 'login', 'svc%', 'test')", ct);
                await ExecAsync(store, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source, rule_id) VALUES ({ServerA}, 'hash', '0x0{tag}', {tag}, 'rule', (SELECT max(id) FROM config.query_tag_rules WHERE tag_id = {tag}))", ct);
            }

            /* A rule deleted on its own leaves the map row it produced until the next rebuild. */
            await ExecAsync(store, $"DELETE FROM config.query_tag_rules WHERE tag_id = {kept}", ct);
            Assert.Equal("1", await ScalarAsync(store, $"SELECT count(*) FROM config.query_tag_map WHERE tag_id = {kept}", ct));

            await ExecAsync(store, $"DELETE FROM config.query_tags WHERE id = {gone}", ct);
            Assert.Equal("0|0|0", await ScalarAsync(store,
                $"SELECT (SELECT count(*) FROM config.query_tag_assignments WHERE tag_id = {gone}) || '|' || (SELECT count(*) FROM config.query_tag_rules WHERE tag_id = {gone}) || '|' || (SELECT count(*) FROM config.query_tag_map WHERE tag_id = {gone})", ct));
            Assert.Equal("1|1", await ScalarAsync(store,
                $"SELECT (SELECT count(*) FROM config.query_tag_assignments WHERE tag_id = {kept}) || '|' || (SELECT count(*) FROM config.query_tag_map WHERE tag_id = {kept})", ct));
        });
    }

    [Fact]
    public async Task TheMap_HoldsOneTagPerKey()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            var a = await NewTagAsync(store, "A", ct);
            var b = await NewTagAsync(store, "B", ct);
            await ExecAsync(store, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ({ServerA}, 'hash', '0x01', {a}, 'hand_hash')", ct);
            await AssertSqlStateAsync(store, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ({ServerA}, 'hash', '0x01', {b}, 'rule')", "23505", ct);
            await ExecAsync(store, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ({ServerB}, 'hash', '0x01', {b}, 'rule')", ct);
        });
    }

    [Fact]
    public async Task ASightingHoldsNoMoreThanTheTextSampleCap_AndASecondSightingOfOneKeyIsOneRow()
    {
        await WithStoreAsync(async (store, _, ct) =>
        {
            var atCap = new string('x', QueryTagSchema.TextSampleMaxLength);
            await ExecAsync(store, $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, text_sample) VALUES ({ServerA}, 'hash', '0x01', '{atCap}')", ct);
            await AssertSqlStateAsync(store,
                $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, text_sample) VALUES ({ServerA}, 'hash', '0x02', '{atCap}y')", "23514", ct);

            /* NULL caller fields fold to an empty string in the unique key: the second sighting conflicts, so the collector upserts. */
            await AssertSqlStateAsync(store,
                $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, application_name) VALUES ({ServerA}, 'hash', '0x01', '')", "23505", ct);
            await ExecAsync(store,
                $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, application_name) VALUES ({ServerA}, 'hash', '0x01', 'SSMS')", ct);
        });
    }

    [Fact]
    public async Task TheMcpRemoveServerTool_ClearsTheServersQueryTagRows_AndNoOtherServers()
    {
        await WithStoreAsync(async (store, connectionString, ct) =>
        {
            await SeedTwoServersAsync(store, ct);
            await using var source = NpgsqlDataSource.Create(connectionString);

            using var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(source, "tagdrop-host"));
            Assert.Equal("removed", doc.RootElement.GetProperty("status").GetString());

            await AssertOnlyTheKeptServerRemainsAsync(store, ct);
        });
    }

    [Fact]
    public async Task TheViewersRemove_ClearsTheServersQueryTagRows_AndNoOtherServers()
    {
        await WithStoreAsync(async (store, connectionString, ct) =>
        {
            await SeedTwoServersAsync(store, ct);

            await using (var viewer = new ViewerDataService(connectionString))
            {
                await viewer.DeleteMonitoredServerAsync(ServerB, ct);
            }

            await AssertOnlyTheKeptServerRemainsAsync(store, ct);
        });
    }

    private static async Task SeedTwoServersAsync(NpgsqlConnection store, CancellationToken ct)
    {
        await ExecAsync(store, $@"INSERT INTO config_monitored_servers (server_id, name, host) VALUES
            ({ServerA}, 'tagkeep-host', 'tagkeep-host'), ({ServerB}, 'tagdrop-host', 'tagdrop-host')", ct);
        var tag = await NewTagAsync(store, "Reports", ct);
        foreach (var server in new[] { ServerA, ServerB })
        {
            await AddHashAssignmentAsync(store, tag, server.ToString(CultureInfo.InvariantCulture), "0x01", ct);
            await ExecAsync(store, $"INSERT INTO config.query_tag_rules (tag_id, server_id, match_field, pattern, created_by) VALUES ({tag}, {server}, 'host', 'web%', 'test')", ct);
            await ExecAsync(store, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ({server}, 'hash', '0x01', {tag}, 'hand_hash')", ct);
            await ExecAsync(store, $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key) VALUES ({server}, 'hash', '0x01')", ct);
        }

        /* An all-server assignment and rule belong to no server: they stay. */
        await AddHashAssignmentAsync(store, tag, null, "0x02", ct);
        await ExecAsync(store, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({tag}, 'host', 'all%', 'test')", ct);
    }

    private static async Task AssertOnlyTheKeptServerRemainsAsync(NpgsqlConnection store, CancellationToken ct)
    {
        string[] perServer = ["config.query_tag_assignments", "config.query_tag_rules", "config.query_tag_map"];
        foreach (var table in perServer)
        {
            Assert.Equal("0", await ScalarAsync(store, $"SELECT count(*) FROM {table} WHERE server_id = {ServerB}", ct));
            Assert.Equal("1", await ScalarAsync(store, $"SELECT count(*) FROM {table} WHERE server_id = {ServerA}", ct));
        }

        /* The sightings are collect history: a removal leaves them to retention, like every other collect table. */
        Assert.Equal("1", await ScalarAsync(store, $"SELECT count(*) FROM collect.query_tag_sightings WHERE server_id = {ServerB}", ct));

        Assert.Equal("1|1|1", await ScalarAsync(store,
            "SELECT (SELECT count(*) FROM config.query_tags) || '|' || (SELECT count(*) FROM config.query_tag_assignments WHERE server_id IS NULL) || '|' || (SELECT count(*) FROM config.query_tag_rules WHERE server_id IS NULL)", ct));
        Assert.Equal("0", await ScalarAsync(store, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {ServerB}", ct));
    }

    [Fact]
    public async Task Retention_PrunesAgedSightings_KeepsRecentOnes_AndNeverTouchesTheTagRows()
    {
        await WithStoreAsync(async (store, connectionString, ct) =>
        {
            var tag = await NewTagAsync(store, "Old", ct);
            await ExecAsync(store, "UPDATE config.query_tags SET created_at = now() - interval '3650 days'", ct);
            await ExecAsync(store, $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by, created_at) VALUES ({tag}, 'hash', '0x01', 'test', now() - interval '3650 days')", ct);
            await ExecAsync(store, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by, created_at) VALUES ({tag}, 'login', 'svc%', 'test', now() - interval '3650 days')", ct);
            await ExecAsync(store, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source, built_at) VALUES ({ServerA}, 'hash', '0x01', {tag}, 'rule', now() - interval '3650 days')", ct);
            await ExecAsync(store,
                $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, first_seen, last_seen) VALUES ({ServerA}, 'hash', 'aged', now() - interval '3650 days', now() - interval '3650 days'), ({ServerA}, 'hash', 'recent', now() - interval '3650 days', now() - interval '1 day')", ct);

            await using var source = NpgsqlDataSource.Create(connectionString);
            await DarlingRetention.PurgeAsync(source, timescaleAvailable: false, logger: null, ct);

            Assert.Equal("recent", await ScalarAsync(store, "SELECT string_agg(key, ',') FROM collect.query_tag_sightings", ct));
            Assert.Equal("1|1|1|1", await ScalarAsync(store,
                "SELECT (SELECT count(*) FROM config.query_tags) || '|' || (SELECT count(*) FROM config.query_tag_assignments) || '|' || (SELECT count(*) FROM config.query_tag_rules) || '|' || (SELECT count(*) FROM config.query_tag_map)", ct));
        });
    }
}

/// <summary>
/// The grants behind the query-tag tables (V192, #5977) as real logins, in both grant sources: the managed batch and the
/// bring-your-own <c>provision-roles.sql</c>. The fixed-name <c>admin</c>, <c>viewer</c> and <c>mcp</c> roles are cluster-wide, so
/// the class is in the <c>pg-cluster-roles</c> collection. The store owner is the service: it writes the sightings, the map and
/// the state row, and no provisioned role holds those writes.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class QueryTagStoreGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/mcp roles
       are why the class sits in the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    private static async Task<NpgsqlConnection> LoginAsync(ScratchPostgres scratch, string role, string password, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = role,
            Password = password,
            Pooling = false,
        }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    private static async Task RunAsync(bool managed, Func<NpgsqlConnection, ScratchPostgres, string, string, string, string, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string, owner/superuser, to run the query-tag grant live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            var exist = await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
            Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the grants would adopt it.");

            string adminPassword, viewerPassword, webPassword, mcpPassword;
            if (managed)
            {
                var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
                await ExecAsync(owner,
                    DarlingManagedRoles.BuildProvisioningSql(
                        ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                        15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName), ProvisioningTestSecrets.Web), ct);
                (adminPassword, viewerPassword, webPassword, mcpPassword) = (ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.WebPassword, ProvisioningTestSecrets.McpPassword);
            }
            else
            {
                Assert.SkipUnless(
                    await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct) == "True",
                    "The script names the store owner 'darling', and this rig has no such role.");
                (adminPassword, viewerPassword, webPassword, mcpPassword) = ("ScriptAdmin5977", "ScriptViewer5977", "ScriptWeb5977", "ScriptMcp5977");
                var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                    .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                    .Replace("CHANGE_ME_ADMIN_PASSWORD", adminPassword, StringComparison.Ordinal)
                    .Replace("CHANGE_ME_VIEWER_PASSWORD", viewerPassword, StringComparison.Ordinal)
                    .Replace("CHANGE_ME_WEB_PASSWORD", webPassword, StringComparison.Ordinal)
                    .Replace("CHANGE_ME_MCP_PASSWORD", mcpPassword, StringComparison.Ordinal);
                await ExecAsync(owner, script, ct);
            }

            await body(owner, scratch, adminPassword, viewerPassword, webPassword, mcpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "qtstore5977none", cleanupCt));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheServiceWritesSightingsAndTheMap_TheMcpRoleEditsTagsRulesAndAssignments_AndNeitherRoleWritesWhatIsNotItsOwn(bool managed)
    {
        await RunAsync(managed, async (owner, scratch, _, viewerPassword, webPassword, mcpPassword, ct) =>
        {
            /* The service, as the store owner, writes the sightings, the map and the state row. */
            await ExecAsync(owner, "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key) VALUES (1, 'hash', '0x01'), (2, 'hash', '0x01')", ct);
            await ExecAsync(owner, "UPDATE config.query_tag_state SET built_version = rules_version", ct);

            await using var mcp = await LoginAsync(scratch, "mcp", mcpPassword, ct);
            /* The tag writes also fire the rules_version trigger as mcp, which holds no grant on the state row. */
            var tag = int.Parse(await ScalarAsync(mcp, "INSERT INTO config.query_tags (name) VALUES ('Reports') RETURNING id", ct), CultureInfo.InvariantCulture);
            await ExecAsync(mcp, $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({tag}, 'hash', '0x01', 'mcp')", ct);
            await ExecAsync(mcp, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({tag}, 'login', 'svc%', 'mcp')", ct);
            await ExecAsync(mcp, "UPDATE config.query_tags SET colour = '#112233'", ct);
            Assert.True(long.Parse(await ScalarAsync(owner, "SELECT rules_version - built_version FROM config.query_tag_state", ct), CultureInfo.InvariantCulture) >= 4);

            await ExecAsync(owner, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES (1, 'hash', '0x01', {tag}, 'hand_hash')", ct);
            Assert.Equal("1", await ScalarAsync(mcp, "SELECT count(*) FROM config.query_tag_map", ct));
            Assert.Equal("2", await ScalarAsync(mcp, "SELECT count(*) FROM collect.query_tag_sightings", ct));

            await AssertDeniedAsync(mcp, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES (9, 'hash', '0x09', {tag}, 'rule')", ct);
            await AssertDeniedAsync(mcp, "UPDATE config.query_tag_map SET source = 'rule'", ct);
            await AssertDeniedAsync(mcp, "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key) VALUES (9, 'hash', '0x09')", ct);
            await AssertDeniedAsync(mcp, "UPDATE collect.query_tag_sightings SET seen_count = 5", ct);
            await AssertDeniedAsync(mcp, "DELETE FROM collect.query_tag_sightings", ct);
            await AssertDeniedAsync(mcp, "UPDATE config.query_tag_state SET rules_version = 0", ct);

            await using var viewer = await LoginAsync(scratch, "viewer", viewerPassword, ct);
            Assert.Equal("Reports", await ScalarAsync(viewer, "SELECT name FROM config.query_tags", ct));
            Assert.Equal("1", await ScalarAsync(viewer, "SELECT count(*) FROM config.query_tag_map", ct));
            /* The viewer login is read-only: it holds none of the three writes mcp does. */
            await AssertDeniedAsync(viewer, "INSERT INTO config.query_tags (name) VALUES ('FromViewer')", ct);
            await AssertDeniedAsync(viewer, "UPDATE config.query_tags SET colour = '#445566'", ct);
            await AssertDeniedAsync(viewer, $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({tag}, 'hash', '0x05', 'web')", ct);
            await AssertDeniedAsync(viewer, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({tag}, 'login', 'x', 'web')", ct);
            await AssertDeniedAsync(viewer, "DELETE FROM config.query_tags", ct);

            /* #5986: the web page that manages query tags runs the MCP cores as web, so web holds the same three writes mcp does. */
            await using var web = await LoginAsync(scratch, "web", webPassword, ct);
            var webTag = int.Parse(await ScalarAsync(web, "INSERT INTO config.query_tags (name) VALUES ('FromWeb') RETURNING id", ct), CultureInfo.InvariantCulture);
            await ExecAsync(web, "UPDATE config.query_tags SET colour = '#445566' WHERE name = 'FromWeb'", ct);
            await ExecAsync(web, $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({webTag}, 'hash', '0x05', 'web')", ct);
            await ExecAsync(web, "UPDATE config.query_tag_assignments SET created_by = 'web' WHERE query_hash = '0x05'", ct);
            await ExecAsync(web, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({webTag}, 'login', 'x', 'web')", ct);
            await ExecAsync(web, "UPDATE config.query_tag_rules SET enabled = false WHERE pattern = 'x'", ct);
            await ExecAsync(web, "DELETE FROM config.query_tags WHERE name = 'FromWeb'", ct);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.query_tags WHERE name = 'FromWeb'", ct));
            await AssertDeniedAsync(viewer, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES (9, 'hash', '0x09', {tag}, 'rule')", ct);
            await AssertDeniedAsync(viewer, "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key) VALUES (9, 'hash', '0x09')", ct);
            await AssertDeniedAsync(viewer, "DELETE FROM collect.query_tag_sightings", ct);
            await AssertDeniedAsync(viewer, "UPDATE config.query_tag_state SET rules_version = 0", ct);
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ServerRemoval_RunsAsEachRole_ThroughTheDeleteGrants_AndTheVersionBumpStillWorks(bool managed)
    {
        await RunAsync(managed, async (owner, scratch, adminPassword, viewerPassword, webPassword, mcpPassword, ct) =>
        {
            var tag = int.Parse(await ScalarAsync(owner, "INSERT INTO config.query_tags (name) VALUES ('Reports') RETURNING id", ct), CultureInfo.InvariantCulture);
            foreach (var (role, password, server) in new[] { ("web", webPassword, 11), ("mcp", mcpPassword, 12), ("admin", adminPassword, 13) })
            {
                await ExecAsync(owner, $"INSERT INTO config.query_tag_assignments (tag_id, kind, server_id, query_hash, created_by) VALUES ({tag}, 'hash', {server}, '0x01', 'test')", ct);
                await ExecAsync(owner, $"INSERT INTO config.query_tag_rules (tag_id, server_id, match_field, pattern, created_by) VALUES ({tag}, {server}, 'host', 'x', 'test')", ct);
                await ExecAsync(owner, $"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ({server}, 'hash', '0x01', {tag}, 'hand_hash')", ct);
                await ExecAsync(owner, $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key) VALUES ({server}, 'hash', '0x01')", ct);

                var version = await ScalarAsync(owner, "SELECT rules_version FROM config.query_tag_state", ct);
                await using var login = await LoginAsync(scratch, role, password, ct);
                foreach (var sql in QueryTagSchema.ClearForServerSqls)
                {
                    await using var command = new NpgsqlCommand(sql.Replace("$1", server.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal), login);
                    await command.ExecuteNonQueryAsync(ct);
                }

                Assert.Equal("0", await ScalarAsync(owner,
                    $"SELECT (SELECT count(*) FROM config.query_tag_assignments WHERE server_id = {server}) + (SELECT count(*) FROM config.query_tag_rules WHERE server_id = {server}) + (SELECT count(*) FROM config.query_tag_map WHERE server_id = {server})", ct));
                Assert.NotEqual(version, await ScalarAsync(owner, "SELECT rules_version FROM config.query_tag_state", ct));
            }
        });
    }
}
