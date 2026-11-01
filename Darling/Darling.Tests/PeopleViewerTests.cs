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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>The Viewer's People screen (#7107): the command sends against a fake command plane, the store rung check, the rows and the dialogs' checks.</summary>
public sealed class PeopleViewerTests
{
    private static readonly string s_webKey = new('a', 43);

    private sealed class FakePlane
    {
        public List<(string Type, string Args, string Login)> Sent { get; } = new();

        public CommandResult? Answer { get; set; }

        public Task<CommandResult?> RunAsync(string type, string args, string login, CancellationToken ct)
        {
            Sent.Add((type, args, login));
            return Task.FromResult(Answer);
        }
    }

    private static CommandResult Done(string json) => new("succeeded", "ok", json);

    [Fact]
    public void EachCommand_CarriesTheTypeAndArgsTheServiceReads()
    {
        Assert.Equal("""{"display_name":"Ann"}""", ViewerPersonCommands.AddPersonArgs("Ann"));
        Assert.Equal("""{"person_id":4,"display_name":"Anna"}""", ViewerPersonCommands.RenamePersonArgs(4, "Anna"));
        Assert.Equal("""{"person_id":4,"enabled":false}""", ViewerPersonCommands.SetPersonEnabledArgs(4, false));
        Assert.Equal($$"""{"person_id":4,"kind":"web","external_id":"{{s_webKey}}"}""", ViewerPersonCommands.AddIdentityArgs(4, "web", s_webKey));
        Assert.Equal("""{"identity_id":9}""", ViewerPersonCommands.RemoveIdentityArgs(9));
        Assert.Equal("""{"grant_id":3}""", ViewerPersonCommands.RemoveGrantArgs(3));
        /* The types are the ones the service executor resolves. */
        Assert.Equal(PersonDirectoryCommands.Types.OrderBy(t => t, StringComparer.Ordinal),
            new[]
            {
                ViewerPersonCommands.AddPerson, ViewerPersonCommands.RenamePerson, ViewerPersonCommands.SetPersonEnabled, ViewerPersonCommands.AddIdentity,
                ViewerPersonCommands.RemoveIdentity, ViewerPersonCommands.AddGrant, ViewerPersonCommands.RemoveGrant,
            }.OrderBy(t => t, StringComparer.Ordinal));
    }

    [Fact]
    public void AGrant_CarriesKindsOnlyWhereTheCapabilityTakesThem_AndOneScope()
    {
        using (var doc = JsonDocument.Parse(ViewerPersonCommands.AddGrantArgs(4, "approve", ["kill_session", "force_plan"], 7, 2)))
        {
            var root = doc.RootElement;
            Assert.Equal(["force_plan", "kill_session"], root.GetProperty("kinds").EnumerateArray().Select(e => e.GetString()));
            Assert.Equal(7, root.GetProperty("scope_server_id").GetInt32());
            Assert.False(root.TryGetProperty("scope_tag_id", out _));
        }

        using (var doc = JsonDocument.Parse(ViewerPersonCommands.AddGrantArgs(4, "undo", ["force_plan"], null, 2)))
        {
            Assert.False(doc.RootElement.TryGetProperty("kinds", out _));
            Assert.Equal(2, doc.RootElement.GetProperty("scope_tag_id").GetInt32());
            Assert.False(doc.RootElement.TryGetProperty("scope_server_id", out _));
        }

        using var fleet = JsonDocument.Parse(ViewerPersonCommands.AddGrantArgs(4, "ask", null, null, null));
        Assert.Equal(2, fleet.RootElement.EnumerateObject().Count());
    }

    [Fact]
    public async Task ASend_GoesToThePlaneAsTheLogin_AndReadsTheAnswer()
    {
        var plane = new FakePlane { Answer = Done("""{"status":"added","message":"Added Ann.","person_id":12,"journal_id":5}""") };
        var answer = await ViewerPersonCommands.SendAsync(plane.RunAsync, ViewerPersonCommands.AddPerson, ViewerPersonCommands.AddPersonArgs("Ann"), "alice",
            TestContext.Current.CancellationToken);
        Assert.True(answer.Succeeded);
        Assert.Equal("added", answer.Status);
        Assert.Equal("Added Ann.", answer.Message);
        Assert.Equal(12, answer.PersonId);
        var sent = Assert.Single(plane.Sent);
        Assert.Equal(("add_person", """{"display_name":"Ann"}""", "alice"), sent);
    }

    [Fact]
    public async Task ARefusal_ShowsTheServicesOwnSentence_AndTheCode()
    {
        var plane = new FakePlane { Answer = Done("""{"status":"refused","code":"identity_taken","message":"That identity already belongs to Bob."}""") };
        var answer = await ViewerPersonCommands.SendAsync(plane.RunAsync, ViewerPersonCommands.AddIdentity, "{}", "alice", TestContext.Current.CancellationToken);
        Assert.False(answer.Succeeded);
        Assert.Equal("identity_taken", answer.Code);
        Assert.Equal("That identity already belongs to Bob.", answer.Message);
    }

    [Theory]
    [InlineData("""{"status":"unchanged","message":"Ann already has that name. Nothing was changed."}""", true, "unchanged")]
    [InlineData("""{"error":"people directory write failed"}""", false, "failed")]
    [InlineData("not json", false, "failed")]
    [InlineData(null, false, "failed")]
    public void TheAnswer_IsReadAsItCame_AndAnythingElseIsAFailure(string? json, bool succeeded, string status)
    {
        var answer = PersonCommandAnswer.Read(json, null);
        Assert.Equal(succeeded, answer.Succeeded);
        Assert.Equal(status, answer.Status);
        Assert.False(string.IsNullOrWhiteSpace(answer.Message));
    }

    [Fact]
    public async Task ACommandStillRunning_SaysToReadTheListBeforeTryingTwice()
    {
        var plane = new FakePlane { Answer = null };
        var answer = await ViewerPersonCommands.SendAsync(plane.RunAsync, ViewerPersonCommands.RemoveGrant, ViewerPersonCommands.RemoveGrantArgs(3), "alice",
            TestContext.Current.CancellationToken);
        Assert.False(answer.Succeeded);
        Assert.Equal("timed_out", answer.Status);
        Assert.Contains("Read the list again", answer.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ALoginTheServiceWouldRefuse_IsNotSent()
    {
        var plane = new FakePlane { Answer = Done("""{"status":"added","message":"x"}""") };
        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            ViewerPersonCommands.SendAsync(plane.RunAsync, ViewerPersonCommands.AddPerson, "{}", "", TestContext.Current.CancellationToken));
        Assert.Empty(plane.Sent);
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(236, false)]
    [InlineData(200, false)]
    [InlineData(237, true)]
    [InlineData(300, true)]
    public void AStoreBelowV237_NeedsAnUpgrade_AndAProbeThatCannotTellReadsTheTables(int? version, bool hasPeople)
    {
        Assert.Equal(hasPeople, PeopleText.StoreHasPeople(version));
        Assert.Equal(237, ViewerDataService.PeopleStoreSchemaVersion);
        Assert.True(StorageVersion.SchemaVersion >= ViewerDataService.PeopleStoreSchemaVersion);
    }

    [Fact]
    public void ThePeopleRows_AreInNameOrder_WithPlainStateAndScopeText()
    {
        var at = new DateTime(2026, 11, 1, 12, 0, 0, DateTimeKind.Utc);
        var grants = new[]
        {
            new PersonGrantRow(1, null, null, null, null, "ask", [], at, "person:alice"),
            new PersonGrantRow(2, 7, "orders-prod", null, null, "approve", ["kill_session"], at, "person:alice"),
            new PersonGrantRow(3, 99, null, null, null, "undo", [], at, "person:alice"),
            new PersonGrantRow(4, null, null, 5, "Production / East", "feedback", [], at, "person:alice"),
        };
        var identities = new[] { new PersonIdentityRow(1, "web", s_webKey, at, "person:alice"), new PersonIdentityRow(2, "slack", "T01:U02", at, "person:alice") };
        var rows = PersonViewRow.From(
        [
            new PersonRow(2, "bob", false, at, "person:alice", at, "person:alice", [], []),
            new PersonRow(1, "Ann", true, at, "person:alice", at, "person:alice", identities, grants),
        ]);

        Assert.Equal(["Ann", "bob"], rows.Select(r => r.DisplayName));
        Assert.Equal(("Enabled", "2", "4", "alice"), (rows[0].StateDisplay, rows[0].IdentitiesDisplay, rows[0].GrantsDisplay, rows[0].UpdatedBy));
        Assert.Equal(("Disabled", "none", "none"), (rows[1].StateDisplay, rows[1].IdentitiesDisplay, rows[1].GrantsDisplay));
        Assert.Equal(["Web sign-in", "Slack"], rows[0].Identities.Select(i => i.KindDisplay));
        Assert.Equal(["The whole fleet", "Server: orders-prod", "Server: (removed, id 99)", "Tag: Production / East"], rows[0].Grants.Select(g => g.ScopeDisplay));
        Assert.Equal("Kill sessions", rows[0].Grants[1].KindsDisplay);
        Assert.Equal("Approve server actions", rows[0].Grants[1].CapabilityDisplay);
    }

    [Fact]
    public void EveryCapability_HasAPlainLabel()
    {
        foreach (var capability in PersonCapabilities.All)
            Assert.NotEqual(capability, PeopleText.CapabilityLabel(capability));
    }

    [Theory]
    [InlineData("slack", "t01abc", " u02def ", null, "T01ABC:U02DEF")]
    [InlineData("web", null, null, " aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    public void AnIdentity_IsBuiltAsTheStoreReadsIt(string kind, string? team, string? user, string? key, string expected)
    {
        Assert.Equal(expected, PersonIdentityEditor.TryBuild(kind, team, user, key, out var error));
        Assert.Null(error);
    }

    [Theory]
    [InlineData("slack", "", "U02", null)]
    [InlineData("slack", "T01", "U 02", null)]
    [InlineData("web", null, null, "ann@example.test")]
    [InlineData("web", null, null, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    [InlineData("email", null, null, null)]
    public void AnIdentityTheStoreWouldRefuse_IsRefusedHereWithAReason(string kind, string? team, string? user, string? key)
    {
        Assert.Null(PersonIdentityEditor.TryBuild(kind, team, user, key, out var error));
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public void AGrant_NeedsKindsOnlyForLevel3AndApprove()
    {
        Assert.NotNull(PersonGrantEditor.Error("request_level_3", []));
        Assert.NotNull(PersonGrantEditor.Error("approve", []));
        Assert.Null(PersonGrantEditor.Error("approve", ["force_plan"]));
        Assert.Null(PersonGrantEditor.Error("undo", []));
        Assert.NotNull(PersonGrantEditor.Error(null, []));
        Assert.True(PersonGrantEditor.ShowsKinds("approve"));
        Assert.False(PersonGrantEditor.ShowsKinds("ask"));
    }
}

/// <summary>
/// The Viewer's people read and command round trip on a real store (#7107). A loop plays the service: it claims each pending people command, runs the
/// same parse and write the service executor runs, and writes the answer back, so the Viewer's enqueue, poll and delete are the real ones. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class PeopleViewerLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the people Viewer live tests.");
        return connectionString!;
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task PlayServiceAsync(NpgsqlDataSource owner, CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            await using var claim = owner.CreateCommand(
                "UPDATE config.config_command SET status = 'running' WHERE command_id = (SELECT command_id FROM config.config_command WHERE status = 'pending' ORDER BY command_id LIMIT 1) " +
                "RETURNING command_id, command_type, args_json::text, requested_by");
            await using var reader = await claim.ExecuteReaderAsync(CancellationToken.None);
            if (!await reader.ReadAsync(CancellationToken.None))
            {
                await Task.Delay(50, CancellationToken.None);
                continue;
            }

            var id = reader.GetInt64(0);
            var type = reader.GetString(1);
            var args = reader.IsDBNull(2) ? null : reader.GetString(2);
            var by = reader.GetString(3);
            await reader.DisposeAsync();
            PersonDirectoryCommands.TryParse(type, args, out var write, out var usage);
            var result = await write(owner, "person:" + by, ServerActionSurfaces.Viewer, CancellationToken.None);
            await using var done = owner.CreateCommand("UPDATE config.config_command SET status = 'succeeded', result_status = $2, result_json = $3::jsonb, completed_at = now() WHERE command_id = $1");
            done.Parameters.AddWithValue(id);
            done.Parameters.AddWithValue(result.Status);
            done.Parameters.AddWithValue(result.ToJson().ToJsonString());
            await done.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task TheViewer_AddsMapsAndGrants_ThroughTheCommandPlane_AndReadsItBack()
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
            using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var service = Task.Run(() => PlayServiceAsync(owner, stop.Token), CancellationToken.None);
            try
            {
                await using var viewer = new ViewerDataService(scratch.ConnectionString);
                Assert.False((await viewer.ReadPeopleAsync(ct)).StoreNeedsUpgrade);
                Assert.Empty((await viewer.ReadPeopleAsync(ct)).People);

                var added = await viewer.AddPersonAsync("Ann", "alice", ct);
                Assert.True(added.Succeeded, added.Message);
                var ann = added.PersonId!.Value;
                Assert.True((await viewer.RenamePersonAsync(ann, "Ann Smith", "alice", ct)).Succeeded);
                var key = new string('b', 43);
                var identity = await viewer.AddPersonIdentityAsync(ann, "web", key, "alice", ct);
                Assert.True(identity.Succeeded, identity.Message);
                var taken = await viewer.AddPersonIdentityAsync((await viewer.AddPersonAsync("Bob", "alice", ct)).PersonId!.Value, "web", key, "alice", ct);
                Assert.False(taken.Succeeded);
                Assert.Equal("identity_taken", taken.Code);
                var grant = await viewer.AddPersonGrantAsync(ann, "approve", ["kill_session"], null, null, "alice", ct);
                Assert.True(grant.Succeeded, grant.Message);
                var noKinds = await viewer.AddPersonGrantAsync(ann, "approve", [], null, null, "alice", ct);
                Assert.Equal("kinds_required", noKinds.Code);
                Assert.True((await viewer.SetPersonEnabledAsync(ann, false, "alice", ct)).Succeeded);

                var people = (await viewer.ReadPeopleAsync(ct)).People;
                var row = Assert.Single(people, p => p.PersonId == ann);
                Assert.Equal(("Ann Smith", false), (row.DisplayName, row.Enabled));
                Assert.Equal(key, Assert.Single(row.Identities).ExternalId);
                Assert.Equal(["kill_session"], Assert.Single(row.Grants).Kinds);
                Assert.Equal("person:alice", row.UpdatedBy);

                Assert.True((await viewer.RemovePersonGrantAsync(grant.GrantId!.Value, "alice", ct)).Succeeded);
                Assert.True((await viewer.RemovePersonIdentityAsync(identity.IdentityId!.Value, "alice", ct)).Succeeded);
                /* The Viewer deletes each finished command row. */
                Assert.Equal(0, await ScalarAsync(connection, "SELECT count(*) FROM config.config_command", ct));
            }
            finally
            {
                await stop.CancelAsync();
                await service;
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreBelowV237_HasNoTables_AndTheViewerSaysItNeedsAnUpgradeInsteadOfFailing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            /* The proposals table (V238, #7109) is the newest probe sentinel, so a store that still has it reads as V238 whatever else is
               gone. A store below V237 has none of the four. */
            await using (var drop = new NpgsqlCommand("DROP TABLE config.agent_action_proposals; DROP TABLE config.person_grant; DROP TABLE config.person_identity; DROP TABLE config.person", connection))
            {
                await drop.ExecuteNonQueryAsync(ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var below = await viewer.ReadPeopleAsync(ct);
            Assert.True(below.StoreNeedsUpgrade);
            Assert.Empty(below.People);
            /* The route editor's nobody-can-approve read (#7109) queries the same tables, so it also answers without querying them. */
            Assert.False(await viewer.NobodyCanApproveAnywhereAsync(ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
