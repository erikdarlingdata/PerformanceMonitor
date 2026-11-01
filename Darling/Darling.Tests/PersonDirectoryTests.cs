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
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>#7107: the people directory's command plane and web entry points, without a store.</summary>
public sealed class PersonDirectoryCommandTests
{
    private static ClaimedCommand Command(string type, string? args, string? requestedBy = "alice") => new(1, type, null, args, requestedBy);

    [Theory]
    [InlineData("add_person", """{"display_name":"Ann"}""")]
    [InlineData("rename_person", """{"person_id":3,"display_name":"Ann B"}""")]
    [InlineData("set_person_enabled", """{"person_id":3,"enabled":false}""")]
    [InlineData("add_person_identity", """{"person_id":3,"kind":"slack","external_id":"T1:U2"}""")]
    [InlineData("remove_person_identity", """{"identity_id":9}""")]
    [InlineData("add_person_grant", """{"person_id":3,"capability":"approve","kinds":["kill_session"],"scope_server_id":7}""")]
    [InlineData("add_person_grant", """{"person_id":3,"capability":"ask","scope_tag_id":null}""")]
    [InlineData("remove_person_grant", """{"grant_id":4}""")]
    public void EachWrite_IsACommandOfItsOwn_RunByTheService(string type, string args)
    {
        Assert.Equal(CommandKind.PersonDirectoryWrite, DarlingCommandExecutor.ResolvePlan(Command(type, args)).Kind);
    }

    [Theory]
    [InlineData("add_person", """{}""")]
    [InlineData("add_person", """{"display_name":7}""")]
    [InlineData("rename_person", """{"display_name":"x"}""")]
    [InlineData("set_person_enabled", """{"person_id":3,"enabled":"no"}""")]
    [InlineData("add_person_identity", """{"person_id":0,"kind":"web","external_id":"x"}""")]
    [InlineData("add_person_grant", """{"person_id":3,"capability":"approve","kinds":"kill_session"}""")]
    [InlineData("add_person_grant", """{"person_id":3,"capability":"ask","scope_server_id":"7"}""")]
    [InlineData("remove_person_grant", "not json")]
    [InlineData("remove_person_grant", null)]
    public void BadArguments_FailBeforeAnythingRuns(string type, string? args)
    {
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command(type, args)).Kind);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("bad‮name")]
    public void AWriteWithNoValidAdministratorName_FailsBeforeAnythingRuns(string? requestedBy)
    {
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("add_person", """{"display_name":"Ann"}""", requestedBy)).Kind);
    }

    /// <summary>The web identity of a seat is its owner key, the hash of the issuer and sub, never the display subject, and a seat with no owner key
    /// (the shared token) has none.</summary>
    [Fact]
    public void AWebIdentity_IsTheSeatsOwnerKey_NeverItsSubject()
    {
        var key = DarlingWebSeat.ComputeOwnerKey("https://issuer.example.test", "sub-123")!;
        var seat = new DarlingWebSeat("ann@example.test", true, key);
        Assert.Equal(key, PersonDirectoryCommands.WebIdentityOf(seat));
        Assert.True(PersonDirectory.IsValidExternalId(PersonIdentityKinds.Web, key));
        Assert.False(PersonDirectory.IsValidExternalId(PersonIdentityKinds.Web, seat.Subject));
        Assert.Null(PersonDirectoryCommands.WebIdentityOf(DarlingWebSeat.SharedToken));
        Assert.True(PersonDirectory.IsValidExternalId(PersonIdentityKinds.Slack, "T0123ABCD:U0456EFGH"));
        Assert.False(PersonDirectory.IsValidExternalId(PersonIdentityKinds.Slack, key));
    }

    [Fact]
    public async Task TheWebEntryPoint_RefusesASeatThatIsNotASignedInAdmin_BeforeItTouchesTheStore()
    {
        /* A data source that would fail to connect: the refusal comes first, so it is never opened. */
        await using var owner = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Timeout=1");
        var context = new DefaultHttpContext();
        var answer = await PersonDirectoryCommands.RunForWebSeatAsync(context, owner, "add_person", """{"display_name":"Ann"}""", TestContext.Current.CancellationToken);
        Assert.Equal("sign_in_required", answer.Code);

        context.Items[DarlingWebSeat.HttpContextItemKey] = DarlingWebSeat.ReadsOnly;
        answer = await PersonDirectoryCommands.RunForWebSeatAsync(context, owner, "add_person", """{"display_name":"Ann"}""", TestContext.Current.CancellationToken);
        Assert.Equal("sign_in_required", answer.Code);

        context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat("ann", true, null);
        answer = await PersonDirectoryCommands.RunForWebSeatAsync(context, owner, "add_person", "{}", TestContext.Current.CancellationToken);
        Assert.Equal("invalid_args", answer.Code);
    }

    [Fact]
    public void TheRungIsTheLadderTop_AndTheProbeMapsIt()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Name == "people-identities-grants");
        Assert.Equal(237, rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var own = Array.FindIndex(parameters, p => p.Name == "hasPeopleGrants");
        Assert.Equal(parameters.Length - 1, own);
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(237, (int)method.Invoke(null, all)!);
        all[own] = false;
        Assert.Equal(234, (int)method.Invoke(null, all)!);
    }
}

/// <summary>
/// #7107 on a live store: every write lands with its journal row naming the administrator, identities are unique for each kind, a web identity
/// is an owner key, the MCP read shows all of it, and the rung rewinds and re-runs.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class PersonDirectoryLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the people directory live tests.");
        return connectionString!;
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task EveryWrite_JournalsItsEvent_WithTheAdministratorAsActor_InTheSameTransaction()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'host-7')", ct);
            await ExecAsync(connection, "INSERT INTO config.server_tags (name) VALUES ('prod')", ct);
            var tagId = int.Parse(await ScalarAsync(connection, "SELECT id FROM config.server_tags WHERE name = 'prod'", ct), CultureInfo.InvariantCulture);
            const string Admin = "person:alice";

            var added = await PersonDirectory.AddPersonAsync(owner, "  Ann  ", Admin, ServerActionSurfaces.Viewer, ct);
            Assert.True(added.Succeeded, added.Message);
            var ann = added.PersonId!.Value;
            Assert.Equal("Ann", await ScalarAsync(connection, $"SELECT display_name FROM config.person WHERE person_id = {ann}", ct));

            Assert.Equal("renamed", (await PersonDirectory.RenamePersonAsync(owner, ann, "Ann B", Admin, ServerActionSurfaces.Web, ct)).Status);
            Assert.Equal("unchanged", (await PersonDirectory.RenamePersonAsync(owner, ann, "Ann B", Admin, ServerActionSurfaces.Web, ct)).Status);
            Assert.Equal("disabled", (await PersonDirectory.SetPersonEnabledAsync(owner, ann, false, Admin, ServerActionSurfaces.Viewer, ct)).Status);
            Assert.Equal("enabled", (await PersonDirectory.SetPersonEnabledAsync(owner, ann, true, Admin, ServerActionSurfaces.Viewer, ct)).Status);

            var key = DarlingWebSeat.ComputeOwnerKey("https://issuer.example.test", "sub-ann")!;
            var web = await PersonDirectory.AddIdentityAsync(owner, ann, "web", key, Admin, ServerActionSurfaces.Web, ct);
            Assert.True(web.Succeeded, web.Message);
            var slack = await PersonDirectory.AddIdentityAsync(owner, ann, "slack", "T01:U02", Admin, ServerActionSurfaces.Web, ct);
            Assert.True(slack.Succeeded, slack.Message);
            Assert.Equal(key, await ScalarAsync(connection, $"SELECT external_id FROM config.person_identity WHERE person_id = {ann} AND kind = 'web'", ct));

            var approve = await PersonDirectory.AddGrantAsync(owner, ann, 7, null, "approve", ["kill_session", "force_plan", "kill_session"], Admin, ServerActionSurfaces.Viewer, ct);
            Assert.True(approve.Succeeded, approve.Message);
            Assert.Equal("{force_plan,kill_session}", await ScalarAsync(connection, $"SELECT kinds::text FROM config.person_grant WHERE grant_id = {approve.GrantId}", ct));
            Assert.True((await PersonDirectory.AddGrantAsync(owner, ann, null, tagId, "ask", null, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.True((await PersonDirectory.AddGrantAsync(owner, ann, null, null, "decide_note", [], Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);

            Assert.Equal("removed", (await PersonDirectory.RemoveIdentityAsync(owner, slack.IdentityId!.Value, Admin, ServerActionSurfaces.Viewer, ct)).Status);
            Assert.Equal("removed", (await PersonDirectory.RemoveGrantAsync(owner, approve.GrantId!.Value, Admin, ServerActionSurfaces.Viewer, ct)).Status);

            /* One journal row per change (the unchanged rename wrote none), every one by the administrator, and each event kind present. */
            Assert.Equal("11", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'person%'", ct));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'person%' AND actor <> 'person:alice'", ct));
            foreach (var kind in PersonEvents.OwnerOnly)
            {
                Assert.NotEqual("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE event = '{kind}'", ct));
            }

            Assert.Equal("viewer", await ScalarAsync(connection, "SELECT detail->>'surface' FROM config.investigation_journal WHERE event = 'person_added'", ct));
            Assert.Equal("7", await ScalarAsync(connection, "SELECT server_id FROM config.investigation_journal WHERE event = 'person_grant_added' AND detail->>'capability' = 'approve'", ct));

            /* The refusals change nothing and journal nothing. */
            Assert.Equal("grant_exists", (await PersonDirectory.AddGrantAsync(owner, ann, null, null, "decide_note", null, Admin, ServerActionSurfaces.Viewer, ct)).Code);
            Assert.Equal("kinds_required", (await PersonDirectory.AddGrantAsync(owner, ann, 7, null, "request_level_3", null, Admin, ServerActionSurfaces.Viewer, ct)).Code);
            Assert.Equal("kinds_not_taken", (await PersonDirectory.AddGrantAsync(owner, ann, 7, null, "undo", ["force_plan"], Admin, ServerActionSurfaces.Viewer, ct)).Code);
            Assert.Equal("unknown_server", (await PersonDirectory.AddGrantAsync(owner, ann, 99, null, "undo", null, Admin, ServerActionSurfaces.Viewer, ct)).Code);
            Assert.Equal("invalid_scope", (await PersonDirectory.AddGrantAsync(owner, ann, 7, tagId, "undo", null, Admin, ServerActionSurfaces.Viewer, ct)).Code);
            Assert.Equal("unknown_person", (await PersonDirectory.RenamePersonAsync(owner, 999_999, "x", Admin, ServerActionSurfaces.Viewer, ct)).Code);
            Assert.Equal("11", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'person%'", ct));
        });
    }

    [Fact]
    public async Task AnIdentity_IsUniqueForItsKind_AndAWebIdentityMustBeAnOwnerKey()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            var ann = (await PersonDirectory.AddPersonAsync(owner, "Ann", "person:alice", ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            var bob = (await PersonDirectory.AddPersonAsync(owner, "Bob", "person:alice", ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            var key = DarlingWebSeat.ComputeOwnerKey("https://issuer.example.test", "sub-ann")!;

            Assert.True((await PersonDirectory.AddIdentityAsync(owner, ann, "web", key, "person:alice", ServerActionSurfaces.Web, ct)).Succeeded);
            Assert.Equal("unchanged", (await PersonDirectory.AddIdentityAsync(owner, ann, "web", key, "person:alice", ServerActionSurfaces.Web, ct)).Status);
            Assert.Equal("identity_taken", (await PersonDirectory.AddIdentityAsync(owner, bob, "web", key, "person:alice", ServerActionSurfaces.Web, ct)).Code);
            /* A display name is never a web identity. */
            Assert.Equal("invalid_identity", (await PersonDirectory.AddIdentityAsync(owner, bob, "web", "ann@example.test", "person:alice", ServerActionSurfaces.Web, ct)).Code);
            Assert.Equal("unknown_kind", (await PersonDirectory.AddIdentityAsync(owner, bob, "email", "ann@example.test", "person:alice", ServerActionSurfaces.Web, ct)).Code);

            /* The table holds the same rules for the owner itself: the unique key per kind, and the shape of each kind. */
            var duplicate = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(connection, $"INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES ({bob}, 'web', '{key}', 'x')", ct));
            Assert.Equal(PostgresErrorCodes.UniqueViolation, duplicate.SqlState);
            var shape = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(connection, $"INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES ({bob}, 'web', 'ann', 'x')", ct));
            Assert.Equal(PostgresErrorCodes.CheckViolation, shape.SqlState);
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.person_identity", ct));
        });
    }

    [Fact]
    public async Task TheMcpRead_ShowsPeopleIdentitiesAndGrants()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'host-7')", ct);
            var ann = (await PersonDirectory.AddPersonAsync(owner, "Ann", "person:alice", ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            await PersonDirectory.AddPersonAsync(owner, "bob", "person:alice", ServerActionSurfaces.Viewer, ct);
            await PersonDirectory.AddIdentityAsync(owner, ann, "slack", "T01:U02", "person:alice", ServerActionSurfaces.Viewer, ct);
            await PersonDirectory.AddGrantAsync(owner, ann, 7, null, "request_level_3", ["evict_plan"], "person:alice", ServerActionSurfaces.Viewer, ct);

            var json = JsonNode.Parse(await DarlingMcpPeopleTools.GetPeople(owner, ct))!;
            Assert.Equal(2, (int)json["people_returned"]!);
            var first = json["people"]![0]!;
            Assert.Equal("Ann", (string)first["display_name"]!);
            Assert.Equal("T01:U02", (string)first["identities"]![0]!["external_id"]!);
            var grant = first["grants"]![0]!;
            Assert.Equal("request_level_3", (string)grant["capability"]!);
            Assert.Equal("server", (string)grant["scope"]!);
            Assert.Equal("srv-7", (string)grant["scope_server_name"]!);
            Assert.Equal("evict_plan", (string)grant["kinds"]![0]!);
            Assert.Equal("bob", (string)json["people"]![1]!["display_name"]!);
        });
    }

    /// <summary>The rung rewinds (its rewind artifact drops the three tables) and a re-run creates them again; the probe reads the store as the top.</summary>
    [Fact]
    public async Task TheRung_Rewinds_AndReRuns()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 234, ct);
            Assert.Equal("False", await ScalarAsync(connection, "SELECT to_regclass('config.person') IS NOT NULL", ct));
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 237", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.person_grant') IS NOT NULL", ct));
            Assert.True((await PersonDirectory.AddPersonAsync(owner, "Ann", "person:alice", ServerActionSurfaces.Viewer, ct)).Succeeded);
        });
    }
}

/// <summary>
/// #7107: only the store owner writes the people tables and journals the people events, in both grant sources (the managed batch and the
/// bring-your-own <c>provision-roles.sql</c> step 3j3), as the real <c>admin</c> and <c>mcp</c> logins. The fixed-name roles are cluster-wide,
/// so the class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PersonDirectoryGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/mcp roles are why the class
       sits in the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];
    private static readonly string[] s_tables = ["config.person", "config.person_identity", "config.person_grant"];

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the people directory grant live tests.");
        return connectionString!;
    }

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
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    /// <summary>Every role reads the three tables and none writes them, and neither login can write a person, an identity, a grant or any people event.</summary>
    private static async Task AssertOwnerOnlyAsync(NpgsqlConnection owner, ScratchPostgres scratch, string adminPassword, string mcpPassword, CancellationToken ct)
    {
        foreach (var role in s_cleanupRoles)
        {
            foreach (var table in s_tables)
            {
                var held = string.Join(",", new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" }
                    .Where(verb => ScalarAsync(owner, $"SELECT has_table_privilege('{role}', '{table}', '{verb}')", ct).GetAwaiter().GetResult() == "True"));
                Assert.Equal("SELECT", held);
            }
        }

        var ann = (await PersonDirectory.AddPersonAsync(NpgsqlDataSource.Create(scratch.ConnectionString), "Ann", "person:alice", ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
        foreach (var (role, password) in new[] { ("admin", adminPassword), ("mcp", mcpPassword) })
        {
            await using var login = await LoginAsync(scratch, role, password, ct);
            Assert.Equal("1", await ScalarAsync(login, "SELECT count(*) FROM config.person", ct));
            await AssertDeniedAsync(login, "INSERT INTO config.person (display_name, created_by, updated_by) VALUES ('Eve', 'x', 'x')", ct);
            await AssertDeniedAsync(login, "UPDATE config.person SET enabled = false", ct);
            await AssertDeniedAsync(login, "DELETE FROM config.person", ct);
            await AssertDeniedAsync(login, $"INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES ({ann}, 'slack', 'T1:U1', 'x')", ct);
            await AssertDeniedAsync(login, "DELETE FROM config.person_identity", ct);
            await AssertDeniedAsync(login, $"INSERT INTO config.person_grant (person_id, capability, created_by) VALUES ({ann}, 'undo', 'x')", ct);
            await AssertDeniedAsync(login, "DELETE FROM config.person_grant", ct);

            /* The journal trigger keeps every people event to the owner, so neither login can write a false record of who mapped or granted somebody. */
            foreach (var kind in PersonEvents.OwnerOnly)
            {
                var journal = await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(login, $"INSERT INTO config.investigation_journal (event, actor, detail) VALUES ('{kind}', 'person:eve', '{{}}'::jsonb)", ct));
                Assert.Equal("42501", journal.SqlState);
                Assert.Contains("Only the Darling service journals the request event " + kind, journal.MessageText, StringComparison.Ordinal);
            }

            /* A write through the people directory on the login's own data source is refused too, and changes nothing. */
            await using var source = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString);
            var refused = await Assert.ThrowsAsync<PostgresException>(() => PersonDirectory.AddPersonAsync(source, "Eve", "person:eve", ServerActionSurfaces.Viewer, ct));
            Assert.Equal("42501", refused.SqlState);
        }

        Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.person", ct));
        Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'person%'", ct));
    }

    private static async Task<bool> SkipIfRolesExistAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var exist = await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
        Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");
        return exist;
    }

    private static async Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "people7107none", cleanupCt));

    [Fact]
    public async Task TheManagedBatch_LeavesThePeopleTablesToTheOwner()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await SkipIfRolesExistAsync(owner, ct);
            var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            await ExecAsync(owner,
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)), ct);
            await AssertOwnerOnlyAsync(owner, scratch, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheBringYourOwnScript_LeavesThePeopleTablesToTheOwner_AndSkipsOnAStoreBehindTheRung()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await SkipIfRolesExistAsync(owner, ct);
            Assert.SkipUnless(await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct) == "True",
                "The script names the store owner 'darling', and this cluster has no such role.");

            const string AdminPassword = "ScriptAdmin7107", ViewerPassword = "ScriptViewer7107", WebPassword = "ScriptWeb7107", McpPassword = "ScriptMcp7107";
            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", AdminPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", ViewerPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", WebPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", McpPassword, StringComparison.Ordinal);

            /* Behind the rung the script warns, skips step 3j3 and still runs to the end. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 234, ct);
            await ExecAsync(owner, script, ct);
            Assert.Equal("4", await ScalarAsync(owner, "SELECT count(*) FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp')", ct));

            await ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version >= 237", ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await ExecAsync(owner, script, ct);
            await AssertOwnerOnlyAsync(owner, scratch, AdminPassword, McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }
}
