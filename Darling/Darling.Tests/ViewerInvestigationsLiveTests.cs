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
using PerformanceMonitor.Common;
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

            async Task<DateTime?> ShownAsync(int? server, int? tag) =>
                (await viewer.GetInvestigationProfilesAsync(ct)).FirstOrDefault(p => p.ScopeServerId == server && p.ScopeTagId == tag)?.ModifiedAtUtc;

            await viewer.SaveInvestigationProfileAsync(null, null, "fleet: OLTP, business hours 08-18", "tester", null, ct);
            /* #6400: a server profile needs the server in the registry (config_monitored_servers), not only in collect.servers. */
            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'srv-7')", ct);
            await viewer.SaveInvestigationProfileAsync(7, null, "srv-7: reporting replica", "tester", null, ct);
            /* #6400: a tag profile needs the tag to exist. */
            await ExecAsync(owner, "INSERT INTO config.server_tags (id, name) OVERRIDING SYSTEM VALUE VALUES (3, 'dev')", ct);
            await viewer.SaveInvestigationProfileAsync(null, 3, "tag 3: dev servers", "tester", null, ct);
            var staleFleet = await ShownAsync(null, null);

            /* #6274: the window showed the fleet profile, someone else saved it, and this save is refused: the text stays theirs. */
            await viewer.SaveInvestigationProfileAsync(null, null, "fleet: OLTP, 24x7", "tester", staleFleet, ct);
            await Assert.ThrowsAsync<InvestigationProfileConflictException>(
                () => viewer.SaveInvestigationProfileAsync(null, null, "fleet: an out-of-date edit", "tester", staleFleet, ct));
            var profiles = await viewer.GetInvestigationProfilesAsync(ct);
            Assert.Equal(3, profiles.Count);
            Assert.Equal("fleet: OLTP, 24x7", profiles[0].ProfileText);
            Assert.Null(profiles[0].ScopeServerId);
            Assert.Null(profiles[0].ScopeTagId);
            Assert.Equal("person:tester", profiles[0].ModifiedBy);
            Assert.Contains(profiles, p => p.ScopeServerId == 7 && p.ProfileText == "srv-7: reporting replica");
            Assert.Contains(profiles, p => p.ScopeTagId == 3 && p.ProfileText == "tag 3: dev servers");

            await viewer.SaveInvestigationProfileAsync(7, null, "  ", "tester", await ShownAsync(7, null), ct);
            Assert.DoesNotContain(await viewer.GetInvestigationProfilesAsync(ct), p => p.ScopeServerId == 7);
            await viewer.SaveInvestigationProfileAsync(7, null, "", "tester", null, ct);

            /* #5632 review L2: every save and the blank save are journaled in order (request NULL, server = the scope's
               server, NULL for the fleet and a tag); the second blank save removed nothing and journals nothing. */
            Assert.Equal(
                "profile_saved||fleet profile;profile_saved|7|server 7 profile;profile_saved||tag 3 profile;"
                + "profile_saved||fleet profile;profile_removed|7|server 7 profile",
                await ScalarAsync(owner, @"
SELECT string_agg(event || '|' || coalesce(server_id::text, '') || '|' || reason, ';' ORDER BY journal_id)
FROM config.investigation_journal
WHERE event LIKE 'profile%' AND request_id IS NULL AND actor = 'person:tester'", ct));
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
            await Assert.ThrowsAsync<ViewerReadOnlyException>(() => viewer.SaveInvestigationProfileAsync(null, null, "x", "tester", null, ct));
            Assert.Equal("open|pending|fleet", await ScalarAsync(owner, @"
SELECT (SELECT state FROM config.investigation_requests) || '|' || (SELECT state FROM config.investigation_notes)
       || '|' || (SELECT profile_text FROM config.agent_environment_profiles)", ct));
        });
    }

    /* #5913: the request an agent claims and reports, so there is a report to give feedback on. */
    private static async Task ReportAsync(NpgsqlDataSource owner, Guid requestId, string verdict, CancellationToken ct)
    {
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var claim = await store.ClaimAsync(requestId, "agent-a", ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
            requestId, claim.ClaimId!.Value, "agent-a", 1, verdict, "root cause", true, "summary", "[{\"tool\":\"get_blocking\"}]"), ct));
    }

    [Fact]
    public async Task AnAdminSeat_GivesFeedbackOnAReport_ReplacesItOnASecondSave_AndSeesItInTheListAndTheCounts()
    {
        await RunAsRoleAsync("admin", async (viewer, owner, requestId, ct) =>
        {
            var noReport = await viewer.RecordInvestigationFeedbackAsync(requestId, "wrong", null, null, null, "tester", ct);
            Assert.Equal(InvestigationFeedbackOutcome.NoReport, noReport.Outcome);
            Assert.Empty((await viewer.GetInvestigationFeedbackListAsync(30, ct)).Rows);

            /* A window outside 1 to 365 days is refused, never shortened or lengthened (#5913, REVIEW-TRAPS: refuse, don't clamp). */
            foreach (var days in new[] { -1, 0, 366, 4000 })
            {
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => viewer.GetInvestigationFeedbackListAsync(days, ct));
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => viewer.GetInvestigationFamilyCountsAsync(days, ct));
            }

            Assert.Empty((await viewer.GetInvestigationFeedbackListAsync(365, ct)).Rows);
            Assert.Empty((await viewer.GetInvestigationFeedbackListAsync(1, ct)).Rows);

            await ReportAsync(owner, requestId, "real", ct);
            var first = await viewer.RecordInvestigationFeedbackAsync(
                requestId, "wrong", "false_positive", "look at the job history", "it is the nightly job", "DOMAIN\\Tester", ct);
            Assert.Equal(InvestigationFeedbackOutcome.Recorded, first.Outcome);
            var replaced = await viewer.RecordInvestigationFeedbackAsync(
                requestId, "partly", "false_positive", null, "mostly the nightly job", "domain\\tester", ct);
            Assert.Equal(InvestigationFeedbackOutcome.Replaced, replaced.Outcome);
            var other = await viewer.RecordInvestigationFeedbackAsync(requestId, "right", null, null, null, "dana", ct);
            Assert.Equal(InvestigationFeedbackOutcome.Recorded, other.Outcome);

            var (rows, truncated) = await viewer.GetInvestigationFeedbackListAsync(30, ct);
            Assert.False(truncated);
            var listed = Assert.Single(rows);
            Assert.Equal(requestId, listed.RequestId);
            Assert.Equal(2, listed.Feedback.Count);
            var mine = Assert.Single(listed.Feedback, f => f.Author.StartsWith("domain_tester", StringComparison.OrdinalIgnoreCase));
            Assert.Equal("partly", mine.Verdict);
            Assert.Equal("mostly the nightly job", mine.Comment);
            Assert.Equal(InvestigationFeedbackRules.AuthorFromIdentity("domain\\tester"), mine.Author);

            /* dana's later record has no corrected verdict, so the newest correction (the first login's) settles it. */
            var family = Assert.Single(await viewer.GetInvestigationFamilyCountsAsync(30, ct));
            Assert.Equal("blocking", family.Family);
            Assert.Equal(1, family.Reported);
            Assert.Equal(1, family.WithFeedback);
            Assert.Equal(0, family.AgentFalsePositives);
            Assert.Equal(1, family.FalsePositives);

            var refused = await Assert.ThrowsAsync<ArgumentException>(
                () => viewer.RecordInvestigationFeedbackAsync(requestId, "fine", null, null, null, "tester", ct));
            Assert.Contains("verdict", refused.Message, StringComparison.Ordinal);
            await Assert.ThrowsAsync<ArgumentException>(
                () => viewer.RecordInvestigationFeedbackAsync(requestId, "wrong", null, null, null, "  ", ct));
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));
        });
    }

    [Fact]
    public async Task AViewerSeat_ReadsTheFeedbackList_AndASaveRefusesAsReadOnly_WritingNothing()
    {
        await RunAsRoleAsync("viewer", async (viewer, owner, requestId, ct) =>
        {
            await ReportAsync(owner, requestId, "real", ct);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(
                new InvestigationFeedbackSubmission(requestId, "right", null, null, null, "dana"), ct);

            var listed = Assert.Single((await viewer.GetInvestigationFeedbackListAsync(30, ct)).Rows);
            Assert.Equal("dana", Assert.Single(listed.Feedback).Author);
            Assert.Single(await viewer.GetInvestigationFamilyCountsAsync(30, ct));

            await Assert.ThrowsAsync<ViewerReadOnlyException>(
                () => viewer.RecordInvestigationFeedbackAsync(requestId, "wrong", null, null, null, "tester", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));
        });
    }

    [Fact]
    public async Task TheRequestsTab_KeepsItsV184Floor_ListingAStoreBeforeV191AsBefore_WithNoPracticeColumn()
    {
        await RunAsRoleAsync("viewer", async (viewer, owner, requestId, ct) =>
        {
            /* #5918: practice_of arrives with V191. A store before it (no feedback table, the V191 sentinel, and no column) lists as it
               always did, not as a raw 42703 that empties the tab. */
            await ExecAsync(owner, "DROP TABLE config.investigation_feedback CASCADE; ALTER TABLE config.investigation_requests DROP COLUMN practice_of CASCADE", ct);
            /* The probe answers the NEWEST sentinel present, and a real store before V191 has no V192 query-tag tables either (#5977). */
            await using (var ownerConnection = await owner.OpenConnectionAsync(ct))
            {
                await RungRewindArtifacts.DropSentinelsAboveAsync(ownerConnection, 191, ct);
            }
            Assert.True(await viewer.GetStoreSchemaVersionAsync(ct) < ViewerDataService.PracticeStoreSchemaVersion);

            var row = Assert.Single(await viewer.GetInvestigationRequestsAsync(null, ct));
            Assert.Equal(requestId, row.RequestId);
            Assert.Null(row.PracticeOf);
            Assert.Equal("", row.PracticeDisplay);
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
