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
    [InlineData("delete_person", """{"person_id":3}""")]
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
    [InlineData("delete_person", """{}""")]
    [InlineData("delete_person", """{"person_id":0}""")]
    [InlineData("delete_person", """{"person_id":"3"}""")]
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

    private static PostgresException ForeignKeyViolation(string constraint) =>
        new("violates foreign key constraint", "ERROR", "ERROR", PostgresErrorCodes.ForeignKeyViolation, constraintName: constraint);

    [Theory]
    [InlineData("fk_person_grant_server", "unknown_server")]
    [InlineData("fk_person_grant_tag", "unknown_tag")]
    [InlineData("fk_person_grant_person", "unknown_person")]
    [InlineData("fk_person_identity_person", "unknown_person")]
    public void AForeignKeyLostToARemoval_IsRefusedWithTheCodeTheExistenceCheckGives(string constraint, string code)
    {
        var refusal = PersonDirectory.RefusalForForeignKey(ForeignKeyViolation(constraint).ConstraintName);
        Assert.NotNull(refusal);
        Assert.False(refusal.Succeeded);
        Assert.Equal("refused", refusal.Status);
        Assert.Equal(code, refusal.Code);
    }

    [Fact]
    public void AnUnrelatedForeignKey_IsNotRefused_SoItStillThrows()
    {
        Assert.Null(PersonDirectory.RefusalForForeignKey(ForeignKeyViolation("fk_something_else").ConstraintName));
        Assert.Null(PersonDirectory.RefusalForForeignKey(null));
    }

    /// <summary>#8777: a sign-in or grant foreign key that refuses a person's delete answers as the check does, never as a raw error.</summary>
    [Theory]
    [InlineData("fk_person_grant_person")]
    [InlineData("fk_person_identity_person")]
    public void AForeignKeyThatRefusesADelete_IsTheSignInsAndGrantsRefusal(string constraint)
    {
        var refusal = PersonDirectory.RefusalForDeleteForeignKey(ForeignKeyViolation(constraint).ConstraintName, 4);
        Assert.NotNull(refusal);
        Assert.False(refusal.Succeeded);
        Assert.Equal("refused", refusal.Status);
        Assert.Equal("person_in_use", refusal.Code);
        Assert.Equal("Remove their sign-ins and grants first.", refusal.Message);
        Assert.Equal(4L, refusal.PersonId);
    }

    [Fact]
    public void AnUnrelatedForeignKeyOnADelete_IsNotRefused_SoItStillThrows()
    {
        Assert.Null(PersonDirectory.RefusalForDeleteForeignKey(ForeignKeyViolation("fk_person_grant_server").ConstraintName, 4));
        Assert.Null(PersonDirectory.RefusalForDeleteForeignKey(null, 4));
    }

    /// <summary>#8777: the web list says who has history only when it was read. Unread, a person carries no <c>has_history</c> at all.</summary>
    [Fact]
    public void TheWebList_MarksWhoHasHistory_OnlyWhenTheHistoryWasRead()
    {
        var at = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        PersonRow[] people =
        [
            new(1, "Ann", true, at, "person:alice", at, "person:alice", [], []),
            new(2, "Bob", true, at, "person:alice", at, "person:alice", [], []),
        ];

        var read = DarlingMcpPeopleTools.ToJson(people, includeExternalIds: true);
        DarlingWebEndpoints.MarkHistory(read, new HashSet<long> { 2 });
        Assert.Equal([false, true], read["people"]!.AsArray().Select(p => (bool)p!["has_history"]!));

        var unread = DarlingMcpPeopleTools.ToJson(people, includeExternalIds: true);
        DarlingWebEndpoints.MarkHistory(unread, null);
        Assert.All(unread["people"]!.AsArray(), p => Assert.False(p!.AsObject().ContainsKey("has_history")));
    }

    [Fact]
    public void TheRungIsV237_AndTheProbeMapsIt()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Name == "people-identities-grants");
        Assert.Equal(237, rung.Version);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var own = Array.FindIndex(parameters, p => p.Name == "hasPeopleGrants");
        Assert.True(own >= 0);
        /* Every sentinel up to this rung's own: the rungs above it are off, so the map reads this rung. */
        var all = parameters.Select((_, i) => (object)(i <= own)).ToArray();
        Assert.Equal(237, (int)method.Invoke(null, all)!);
        all[own] = false;
        Assert.Equal(236, (int)method.Invoke(null, all)!);
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

            var spare = (await PersonDirectory.AddPersonAsync(owner, "Spare", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            Assert.Equal("deleted", (await PersonDirectory.DeletePersonAsync(owner, spare, Admin, ServerActionSurfaces.Viewer, ct)).Status);

            /* One journal row per change (the unchanged rename wrote none), every one by the administrator, and each event kind present. */
            Assert.Equal("13", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'person%'", ct));
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
            Assert.Equal("13", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'person%'", ct));
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
            /* MCP tokens, agent tokens included, see the kind and never the Slack ids or a web sign-in key (#7107). */
            Assert.Equal("slack", (string)first["identities"]![0]!["kind"]!);
            Assert.False(first["identities"]![0]!.AsObject().ContainsKey("external_id"));
            Assert.DoesNotContain("T01:U02", json.ToJsonString(), StringComparison.Ordinal);
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
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 236, ct);
            Assert.Equal("False", await ScalarAsync(connection, "SELECT to_regclass('config.person') IS NOT NULL", ct));
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 237", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.person_grant') IS NOT NULL", ct));
            Assert.True((await PersonDirectory.AddPersonAsync(owner, "Ann", "person:alice", ServerActionSurfaces.Viewer, ct)).Succeeded);
        });
    }

    [Fact]
    public async Task TheSameIdentityAddedAtOnce_IsRefusedAsAConflict_AndChangesNothing()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            const string Admin = "person:alice";
            var ann = (await PersonDirectory.AddPersonAsync(owner, "Ann", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            var bob = (await PersonDirectory.AddPersonAsync(owner, "Bob", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            var journalBefore = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct);

            /* Another administrator's transaction has inserted the identity and not committed: Ann's existence check cannot see it, her insert waits on
               the unique index, and when the other transaction commits the insert meets the duplicate. */
            await using var other = await owner.OpenConnectionAsync(ct);
            await using var otherTransaction = await other.BeginTransactionAsync(ct);
            await using (var insert = new NpgsqlCommand(
                "INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES (@p, 'slack', 'T01:U02', 'person:bob')", other, otherTransaction))
            {
                insert.Parameters.AddWithValue("p", bob);
                await insert.ExecuteNonQueryAsync(ct);
            }

            var racing = PersonDirectory.AddIdentityAsync(owner, ann, "slack", "T01:U02", Admin, ServerActionSurfaces.Web, ct);
            for (var i = 0; i < 100 && await ScalarAsync(connection, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()", ct) == "0"; i++)
                await Task.Delay(100, ct);
            Assert.False(racing.IsCompleted);
            await otherTransaction.CommitAsync(ct);

            var result = await racing;
            Assert.False(result.Succeeded);
            Assert.Equal("refused", result.Status);
            Assert.Equal("conflict", result.Code);
            Assert.Equal(bob.ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT person_id FROM config.person_identity WHERE external_id = 'T01:U02'", ct));
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.person_identity", ct));
            Assert.Equal(journalBefore, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct));
        });
    }

    private static readonly Guid s_historyRequest = Guid.Parse("87770000-0000-4000-8000-000000000001");

    /// <summary>The request the history rows of the delete tests (#8777) hang from.</summary>
    internal static Task AddHistoryRequestAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ActionProposalStoreLiveTests.AddRequestAsync(connection, s_historyRequest, "claimed", ct);

    /// <summary>Makes the person an asker of the history request: history whose foreign key cascades with the person.</summary>
    internal static async Task AddAskerAsync(NpgsqlConnection connection, long personId, CancellationToken ct, NpgsqlTransaction? transaction = null)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.investigation_request_askers (request_id, person_id, identity, request_level) "
            + $"VALUES ('{s_historyRequest}', {personId}, 'slack:T01:U{personId}', 1)", connection, transaction);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Gives the person one message in the history request's thread: history whose foreign key sets the person to null.</summary>
    internal static Task AddMessageAsync(NpgsqlConnection connection, long personId, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO config.investigation_person_messages (channel_id, ts, thread_ts, thread_request_id, person_id, identity, message_text, state) "
            + $"VALUES ('C8777', '1700000000.{personId:D6}', '1700000000.000100', '{s_historyRequest}', {personId}, 'slack:T01:U{personId}', 'is it still blocked?', 'waiting')", ct);

    private static async Task<long> AddAsync(NpgsqlDataSource owner, string name, CancellationToken ct) =>
        (await PersonDirectory.AddPersonAsync(owner, name, "person:alice", ServerActionSurfaces.Viewer, ct)).PersonId!.Value;

    [Fact]
    public async Task APersonWithNothingAttached_IsDeleted_AndJournaledUnderTheAdministrator_WithTheIdAndName()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            const string Admin = "person:alice";
            var ann = await AddAsync(owner, "Ann", ct);
            await AddAsync(owner, "Bob", ct);

            var deleted = await PersonDirectory.DeletePersonAsync(owner, ann, Admin, ServerActionSurfaces.Web, ct);
            Assert.True(deleted.Succeeded, deleted.Message);
            Assert.Equal("deleted", deleted.Status);
            Assert.Null(deleted.Code);
            Assert.Equal("Deleted Ann.", deleted.Message);
            Assert.Equal(ann, deleted.PersonId);
            Assert.Equal("Bob", await ScalarAsync(connection, "SELECT string_agg(display_name, ',') FROM config.person", ct));

            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'person_deleted'", ct));
            Assert.Equal($"{deleted.JournalId}|person:alice|web|{ann}|Ann", await ScalarAsync(connection,
                "SELECT concat_ws('|', journal_id, actor, detail->>'surface', detail->>'person_id', detail->>'display_name') FROM config.investigation_journal WHERE event = 'person_deleted'", ct));

            /* A person who is gone is the unknown person every other people write answers, and journals nothing. */
            var journalBefore = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct);
            var again = await PersonDirectory.DeletePersonAsync(owner, ann, Admin, ServerActionSurfaces.Web, ct);
            Assert.False(again.Succeeded);
            Assert.Equal("refused", again.Status);
            Assert.Equal("unknown_person", again.Code);
            Assert.Equal(journalBefore, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct));
        });
    }

    [Fact]
    public async Task APersonWithASignInOrAGrant_IsRefused_UntilBothAreRemoved()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            const string Admin = "person:alice";
            var ann = await AddAsync(owner, "Ann", ct);
            var identity = await PersonDirectory.AddIdentityAsync(owner, ann, "slack", "T01:U02", Admin, ServerActionSurfaces.Viewer, ct);
            Assert.True(identity.Succeeded, identity.Message);
            var grant = await PersonDirectory.AddGrantAsync(owner, ann, null, null, "ask", null, Admin, ServerActionSurfaces.Viewer, ct);
            Assert.True(grant.Succeeded, grant.Message);

            async Task AssertInUseAsync(string identities, string grants)
            {
                var journalBefore = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct);
                var refused = await PersonDirectory.DeletePersonAsync(owner, ann, Admin, ServerActionSurfaces.Web, ct);
                Assert.False(refused.Succeeded);
                Assert.Equal("refused", refused.Status);
                Assert.Equal(PersonDirectory.PersonInUseCode, refused.Code);
                Assert.Equal("Remove their sign-ins and grants first.", refused.Message);
                Assert.Equal(ann, refused.PersonId);
                Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.person", ct));
                Assert.Equal(identities, await ScalarAsync(connection, "SELECT count(*) FROM config.person_identity", ct));
                Assert.Equal(grants, await ScalarAsync(connection, "SELECT count(*) FROM config.person_grant", ct));
                Assert.Equal(journalBefore, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct));
            }

            /* Both, then the grant alone, then a sign-in alone: each one refuses the delete on its own. */
            await AssertInUseAsync("1", "1");
            Assert.Equal("removed", (await PersonDirectory.RemoveIdentityAsync(owner, identity.IdentityId!.Value, Admin, ServerActionSurfaces.Viewer, ct)).Status);
            await AssertInUseAsync("0", "1");
            Assert.Equal("removed", (await PersonDirectory.RemoveGrantAsync(owner, grant.GrantId!.Value, Admin, ServerActionSurfaces.Viewer, ct)).Status);
            var second = await PersonDirectory.AddIdentityAsync(owner, ann, "slack", "T01:U03", Admin, ServerActionSurfaces.Viewer, ct);
            Assert.True(second.Succeeded, second.Message);
            await AssertInUseAsync("1", "0");
            Assert.Equal("removed", (await PersonDirectory.RemoveIdentityAsync(owner, second.IdentityId!.Value, Admin, ServerActionSurfaces.Viewer, ct)).Status);

            Assert.Equal("deleted", (await PersonDirectory.DeletePersonAsync(owner, ann, Admin, ServerActionSurfaces.Web, ct)).Status);
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.person", ct));
        });
    }

    [Fact]
    public async Task APersonWithHistory_IsRefused_AndTheHistoryIsLeftAsItWas()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            const string Admin = "person:alice";
            var ann = await AddAsync(owner, "Ann", ct);
            var bob = await AddAsync(owner, "Bob", ct);
            var cat = await AddAsync(owner, "Cat", ct);
            var dee = await AddAsync(owner, "Dee", ct);
            await AddHistoryRequestAsync(connection, ct);
            await AddAskerAsync(connection, ann, ct);
            await AddMessageAsync(connection, bob, ct);
            /* Cat has history and a grant: the history refusal is the one she gets, because removing the grant would not make her deletable. */
            await AddAskerAsync(connection, cat, ct);
            Assert.True((await PersonDirectory.AddGrantAsync(owner, cat, null, null, "ask", null, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            var journalBefore = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct);

            Assert.Equal([ann, bob, cat], (await PersonDirectory.PeopleWithHistoryAsync(owner, ct))!.OrderBy(id => id));

            foreach (var person in new[] { ann, bob, cat })
            {
                var refused = await PersonDirectory.DeletePersonAsync(owner, person, Admin, ServerActionSurfaces.Viewer, ct);
                Assert.False(refused.Succeeded);
                Assert.Equal("refused", refused.Status);
                Assert.Equal(PersonDirectory.PersonHasHistoryCode, refused.Code);
                Assert.Equal("This person has history, so disable them instead.", refused.Message);
                Assert.Equal(person, refused.PersonId);
            }

            Assert.Equal("4", await ScalarAsync(connection, "SELECT count(*) FROM config.person", ct));
            Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_request_askers", ct));
            Assert.Equal(bob.ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT person_id FROM config.investigation_person_messages", ct));
            Assert.Equal(journalBefore, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct));

            /* Disabling is what the refusal points at, and it is open to a person with history. */
            Assert.Equal("disabled", (await PersonDirectory.SetPersonEnabledAsync(owner, ann, false, Admin, ServerActionSurfaces.Viewer, ct)).Status);

            /* A person with no history deletes, and the others' history stays. */
            Assert.Equal("deleted", (await PersonDirectory.DeletePersonAsync(owner, dee, Admin, ServerActionSurfaces.Viewer, ct)).Status);
            Assert.Equal([ann, bob, cat], (await PersonDirectory.PeopleWithHistoryAsync(owner, ct))!.OrderBy(id => id));
        });
    }

    [Fact]
    public async Task ASignInOrHistoryAddedAtTheSameMoment_MakesTheDeleteWait_ThenRefuseIt_AndNothingIsOrphaned()
    {
        await WithStoreAsync(async (connection, owner, ct) =>
        {
            const string Admin = "person:alice";
            var ann = await AddAsync(owner, "Ann", ct);
            var bob = await AddAsync(owner, "Bob", ct);
            await AddHistoryRequestAsync(connection, ct);
            var journalBefore = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct);

            async Task<PersonWriteResult> DeleteWhileAsync(long person, Func<NpgsqlConnection, NpgsqlTransaction, Task> insert)
            {
                /* Another transaction has inserted a row that references the person and not committed: the delete's checks cannot see it, so the
                   delete waits on the person's row, and when the other transaction commits the checks read the row. */
                await using var other = await owner.OpenConnectionAsync(ct);
                await using var otherTransaction = await other.BeginTransactionAsync(ct);
                await insert(other, otherTransaction);

                var racing = PersonDirectory.DeletePersonAsync(owner, person, Admin, ServerActionSurfaces.Web, ct);
                for (var i = 0; i < 100 && await ScalarAsync(connection, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()", ct) == "0"; i++)
                    await Task.Delay(100, ct);
                Assert.False(racing.IsCompleted);
                await otherTransaction.CommitAsync(ct);
                return await racing;
            }

            var signIn = await DeleteWhileAsync(ann, async (other, transaction) =>
            {
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES (@p, 'slack', 'T01:U02', 'person:bob')", other, transaction);
                insert.Parameters.AddWithValue("p", ann);
                await insert.ExecuteNonQueryAsync(ct);
            });
            Assert.False(signIn.Succeeded);
            Assert.Equal(PersonDirectory.PersonInUseCode, signIn.Code);

            var history = await DeleteWhileAsync(bob, (other, transaction) => AddAskerAsync(other, bob, ct, transaction));
            Assert.False(history.Succeeded);
            Assert.Equal(PersonDirectory.PersonHasHistoryCode, history.Code);

            Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.person", ct));
            Assert.Equal(ann.ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT person_id FROM config.person_identity", ct));
            Assert.Equal(bob.ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT person_id FROM config.investigation_request_askers", ct));
            Assert.Equal(journalBefore, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal", ct));
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
    private static readonly (string Table, string Column)[] s_sequences =
        [("config.person", "person_id"), ("config.person_identity", "identity_id"), ("config.person_grant", "grant_id")];

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

            /* A table's REVOKE does not reach its identity sequence: no role keeps USAGE (nextval), SELECT or UPDATE on one. */
            foreach (var sequence in s_sequences)
            {
                foreach (var verb in new[] { "USAGE", "SELECT", "UPDATE" })
                    Assert.Equal("False", await ScalarAsync(owner, $"SELECT has_sequence_privilege('{role}', pg_get_serial_sequence('{sequence.Table}', '{sequence.Column}'), '{verb}')", ct));
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
            await AssertDeniedAsync(login, "UPDATE config.person_identity SET kind = kind", ct);
            await AssertDeniedAsync(login, "DELETE FROM config.person_identity", ct);
            await AssertDeniedAsync(login, $"INSERT INTO config.person_grant (person_id, capability, created_by) VALUES ({ann}, 'undo', 'x')", ct);
            await AssertDeniedAsync(login, "UPDATE config.person_grant SET capability = capability", ct);
            await AssertDeniedAsync(login, "DELETE FROM config.person_grant", ct);
            foreach (var sequence in s_sequences)
                await AssertDeniedAsync(login, $"SELECT nextval(pg_get_serial_sequence('{sequence.Table}', '{sequence.Column}'))", ct);

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

            /* #8777: who has history is read by the admin login, which holds SELECT on the two history tables. The mcp login holds none, and
               the read answers that it does not know. */
            var history = await PersonDirectory.PeopleWithHistoryAsync(source, ct);
            if (role == "admin")
                Assert.Empty(Assert.IsAssignableFrom<IReadOnlySet<long>>(history));
            else
                Assert.Null(history);
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
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 236, ct);
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

    /// <summary>
    /// The rung takes the writes back itself (#7107): on a store whose roles and default privileges exist but whose provisioning has not run
    /// again since V237, <c>admin</c> holds SELECT only on the three tables and nothing on their sequences, and every write as <c>admin</c>
    /// or <c>mcp</c> is refused. A grant on a server goes when <c>admin</c> removes the server, so adding the same server back (the same id)
    /// does not bring it back. The same for a tag.
    /// </summary>
    [Fact]
    public async Task TheRung_TakesTheWritesBack_BeforeProvisioningRunsAgain_AndARemovedServerTakesItsGrants()
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

            /* Back behind the rung, then the upgrade runs V237 alone, with the default privileges in place and no provisioning after it. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 236, ct);
            await ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version >= 237", ct);
            Assert.Equal("True", await ScalarAsync(owner,
                "SELECT EXISTS (SELECT 1 FROM pg_default_acl AS d JOIN pg_namespace AS n ON n.oid = d.defaclnamespace WHERE n.nspname = 'config' AND d.defaclobjtype = 'r' AND array_to_string(d.defaclacl, ',') LIKE '%admin=a%')", ct));
            await PgMigrations.MigrateAsync(owner, ct);

            /* No role holds more than SELECT on a table (mcp holds nothing until provisioning grants its read), and none holds anything on a
               sequence. As admin, an INSERT, UPDATE and DELETE on each table and a nextval on each sequence are refused. */
            foreach (var role in s_cleanupRoles)
            {
                foreach (var table in s_tables)
                {
                    foreach (var verb in new[] { "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" })
                        Assert.Equal("False", await ScalarAsync(owner, $"SELECT has_table_privilege('{role}', '{table}', '{verb}')", ct));
                }

                foreach (var sequence in s_sequences)
                {
                    foreach (var verb in new[] { "USAGE", "SELECT", "UPDATE" })
                        Assert.Equal("False", await ScalarAsync(owner, $"SELECT has_sequence_privilege('{role}', pg_get_serial_sequence('{sequence.Table}', '{sequence.Column}'), '{verb}')", ct));
                }
            }

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var ann = (await PersonDirectory.AddPersonAsync(source, "Ann", "person:alice", ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            await using (var admin = await LoginAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, ct))
            {
                Assert.Equal("1", await ScalarAsync(admin, "SELECT count(*) FROM config.person", ct));
                await AssertDeniedAsync(admin, "INSERT INTO config.person (display_name, created_by, updated_by) VALUES ('Eve', 'x', 'x')", ct);
                await AssertDeniedAsync(admin, "UPDATE config.person SET enabled = false", ct);
                await AssertDeniedAsync(admin, "DELETE FROM config.person", ct);
                await AssertDeniedAsync(admin, $"INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES ({ann}, 'slack', 'T1:U1', 'x')", ct);
                await AssertDeniedAsync(admin, "UPDATE config.person_identity SET kind = kind", ct);
                await AssertDeniedAsync(admin, "DELETE FROM config.person_identity", ct);
                await AssertDeniedAsync(admin, $"INSERT INTO config.person_grant (person_id, capability, created_by) VALUES ({ann}, 'undo', 'x')", ct);
                await AssertDeniedAsync(admin, "UPDATE config.person_grant SET capability = capability", ct);
                await AssertDeniedAsync(admin, "DELETE FROM config.person_grant", ct);
                foreach (var sequence in s_sequences)
                    await AssertDeniedAsync(admin, $"SELECT nextval(pg_get_serial_sequence('{sequence.Table}', '{sequence.Column}'))", ct);
            }

            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'host-7')", ct);
            var tagId = int.Parse(await ScalarAsync(owner, "INSERT INTO config.server_tags (name) VALUES ('prod') RETURNING id", ct), CultureInfo.InvariantCulture);
            Assert.Equal("added", (await PersonDirectory.AddGrantAsync(source, ann, 7, null, "undo", [], "person:alice", ServerActionSurfaces.Viewer, ct)).Status);
            Assert.Equal("added", (await PersonDirectory.AddGrantAsync(source, ann, null, tagId, "undo", [], "person:alice", ServerActionSurfaces.Viewer, ct)).Status);

            /* admin removes the server and the tag as the Viewer's Remove does. The cascade runs as the grant table's owner. */
            await using (var admin = await LoginAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, ct))
            {
                await ExecAsync(admin, "SET search_path = config, public", ct);
                await ExecAsync(admin, ViewerDataService.MonitoredServerDeleteSql.Replace("$1", "7", StringComparison.Ordinal), ct);
                await ExecAsync(admin, $"DELETE FROM config.server_tags WHERE id = {tagId}", ct);
            }

            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'host-7')", ct);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.person_grant", ct));
            Assert.Empty((await PersonDirectory.ListAsync(source, ct)).Single().Grants);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }
}
