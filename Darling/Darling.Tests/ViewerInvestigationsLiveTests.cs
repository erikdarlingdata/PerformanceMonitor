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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The viewer's investigation partial on a live V184 store (#5632 Phase A), run as a NOSUPERUSER role holding exactly the
/// grants provisioning gives the managed role the seat connects as (<see cref="ViewerGrantReplay"/>, retargeted): an
/// <c>admin</c> seat reads a request the store opened, closes it with a reason (journaled <c>closed</c> by
/// <c>person:&lt;login&gt;</c>), approves an agent's pending note, writes an approved note that expires in 90 days, and saves,
/// replaces and removes the profile of each scope; a <c>viewer</c> seat reads the same rows and every write refuses with
/// <see cref="ViewerReadOnlyException"/>. Removing one of the admin grants from the product's SQL fails the first fact.
/// </summary>
public sealed class ViewerInvestigationsLiveTests
{
    private const string RolePassword = "inv-viewer-live-pw";

    private static InvestigationAlert Alert(DateTime at) =>
        new(1, 7, "srv-7", "blocking", "Blocking", at, new[] { "key-1" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    [Fact]
    public async Task AnAdminSeat_ClosesARequest_DecidesAndWritesNotes_AndSavesAProfilePerScope()
    {
        await RunAsRoleAsync("admin", async (viewer, owner, requestId, ct) =>
        {
            var open = Assert.Single(await viewer.GetInvestigationRequestsAsync("open", ct));
            Assert.Equal(requestId, open.RequestId);
            Assert.Equal("srv-7", open.ServerName);
            Assert.Equal(1, open.AlertCount);
            Assert.True(open.IsClosable);

            Assert.True(await viewer.CloseInvestigationRequestAsync(requestId, "tester", "known nightly job", ct));
            Assert.False(await viewer.CloseInvestigationRequestAsync(requestId, "tester", "again", ct));
            Assert.Empty(await viewer.GetInvestigationRequestsAsync("open", ct));
            var closed = Assert.Single(await viewer.GetInvestigationRequestsAsync("closed", ct));
            Assert.Equal("person:tester", closed.ClosedBy);
            Assert.Equal("known nightly job", closed.ClosedReason);

            var detail = await viewer.GetInvestigationRequestDetailAsync(requestId, ct);
            Assert.Equal("Blocking", Assert.Single(detail.Alerts).MetricName);
            Assert.Empty(detail.Reports);
            Assert.Null(detail.BriefJson);
            var closedRow = Assert.Single(detail.Journal, j => j.Event == "closed");
            Assert.Equal("person:tester", closedRow.Actor);
            Assert.Equal("known nightly job", closedRow.Reason);
            Assert.Contains(detail.Journal, j => j.Event == "opened");

            /* An agent's proposal, as the mcp role inserts it: pending until a person decides. */
            var proposed = await ScalarAsync(owner, $@"
INSERT INTO config.investigation_notes (server_id, note_text, author, source_request_id)
VALUES (7, 'the 02:00 ETL blocks for ten minutes', 'agent:agent-a', '{requestId}') RETURNING note_id", ct);
            var noteId = long.Parse(proposed!, CultureInfo.InvariantCulture);
            var pending = Assert.Single(await viewer.GetInvestigationNotesAsync("pending", ct));
            Assert.Equal("srv-7", pending.ServerName);
            Assert.True(await viewer.DecideInvestigationNoteAsync(noteId, approve: true, "tester", ct));
            Assert.False(await viewer.DecideInvestigationNoteAsync(noteId, approve: false, "tester", ct));
            Assert.Equal("approved|person:tester", await ScalarAsync(owner,
                $"SELECT state || '|' || decided_by FROM config.investigation_notes WHERE note_id = {noteId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'note_approved' AND request_id = '{requestId}'", ct));

            var written = await viewer.WriteInvestigationNoteAsync(7, "Blocking", "page the DBA only after 03:00", "tester", cancellationToken: ct);
            Assert.Equal("approved|person:tester|90", await ScalarAsync(owner, $@"
SELECT state || '|' || author || '|' || round(extract(epoch FROM expires_at - created_at) / 86400)
FROM config.investigation_notes WHERE note_id = {written}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'note_written' AND actor = 'person:tester'", ct));

            await viewer.SaveInvestigationProfileAsync(null, null, "fleet: OLTP, business hours 08-18", "tester", ct);
            await viewer.SaveInvestigationProfileAsync(7, null, "srv-7: reporting replica", "tester", ct);
            await viewer.SaveInvestigationProfileAsync(null, 3, "tag 3: dev servers", "tester", ct);
            await viewer.SaveInvestigationProfileAsync(null, null, "fleet: OLTP, 24x7", "tester", ct);
            var profiles = await viewer.GetInvestigationProfilesAsync(ct);
            Assert.Equal(3, profiles.Count);
            Assert.Equal("fleet: OLTP, 24x7", profiles[0].ProfileText);
            Assert.Null(profiles[0].ScopeServerId);
            Assert.Null(profiles[0].ScopeTagId);
            Assert.Equal("person:tester", profiles[0].ModifiedBy);
            Assert.Contains(profiles, p => p.ScopeServerId == 7 && p.ProfileText == "srv-7: reporting replica");
            Assert.Contains(profiles, p => p.ScopeTagId == 3 && p.ProfileText == "tag 3: dev servers");

            await viewer.SaveInvestigationProfileAsync(7, null, "  ", "tester", ct);
            Assert.DoesNotContain(await viewer.GetInvestigationProfilesAsync(ct), p => p.ScopeServerId == 7);
        });
    }

    [Fact]
    public async Task AViewerSeat_ReadsEverything_AndEveryWriteRefusesAsReadOnly()
    {
        await RunAsRoleAsync("viewer", async (viewer, owner, requestId, ct) =>
        {
            await ExecAsync(owner, $@"
INSERT INTO config.investigation_notes (server_id, note_text, author, source_request_id) VALUES (7, 'proposal', 'agent:a', '{requestId}');
INSERT INTO config.agent_environment_profiles (profile_text, modified_by) VALUES ('fleet', 'person:x');", ct);

            Assert.Single(await viewer.GetInvestigationRequestsAsync(null, ct));
            Assert.Contains((await viewer.GetInvestigationRequestDetailAsync(requestId, ct)).Journal, j => j.Event == "opened");
            var note = Assert.Single(await viewer.GetInvestigationNotesAsync(null, ct));
            Assert.Single(await viewer.GetInvestigationProfilesAsync(ct));

            await Assert.ThrowsAsync<ViewerReadOnlyException>(() => viewer.CloseInvestigationRequestAsync(requestId, "tester", "no", ct));
            await Assert.ThrowsAsync<ViewerReadOnlyException>(() => viewer.DecideInvestigationNoteAsync(note.NoteId, true, "tester", ct));
            await Assert.ThrowsAsync<ViewerReadOnlyException>(() => viewer.WriteInvestigationNoteAsync(null, null, "note", "tester", cancellationToken: ct));
            await Assert.ThrowsAsync<ViewerReadOnlyException>(() => viewer.SaveInvestigationProfileAsync(null, null, "x", "tester", ct));
            Assert.Equal("open|pending|fleet", await ScalarAsync(owner, @"
SELECT (SELECT state FROM config.investigation_requests) || '|' || (SELECT state FROM config.investigation_notes)
       || '|' || (SELECT profile_text FROM config.agent_environment_profiles)", ct));
        });
    }

    private static async Task RunAsRoleAsync(
        string managedRole, Func<ViewerDataService, NpgsqlDataSource, Guid, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the viewer investigation live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "inv_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = NpgsqlDataSource.Create(ownerString);
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, 7, "srv-7", ct);
        }

        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var requestId = (await store.OpenOrJoinAsync(Alert(DateTime.UtcNow), ct)).RequestId!.Value;

        var statements = ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, managedRole);
        Assert.Contains(statements, s => s.Contains("config.investigation_requests", StringComparison.Ordinal));
        await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        try
        {
            foreach (var statement in statements)
            {
                await ExecAsync(owner, statement, ct);
            }

            var asSeat = new NpgsqlConnectionStringBuilder(ownerString) { Username = roleName, Password = RolePassword }.ConnectionString;
            await using var viewer = new ViewerDataService(asSeat);
            await body(viewer, owner, requestId, ct);
        }
        finally
        {
            NpgsqlConnection.ClearAllPools();
            await ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", ct);
        }
    }

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
}
