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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's agent-note approve screen store (#5903) on a throwaway DuckDB, the twin of Darling's
/// <c>InvestigationNoteDecisionLiveTests</c> store fact: a pending note is absent from the next brief, approving it puts it
/// in, rejecting it keeps it out and keeps the row with its verdict, who decided and when are stamped, a decided note cannot
/// be decided again, and every decision writes its journal row.
/// </summary>
public sealed class DuckDbInvestigationNoteStoreTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly DuckDbInvestigationStore _store;
    private DateTime _now = T0;

    public DuckDbInvestigationNoteStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DuckDbInvestigationNoteStoreTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _db = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _db.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbInvestigationStore(_db, () => _now);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static InvestigationAlert Alert(DateTime at, string family) =>
        new(1, 7, "srv-7", family, "Blocking", at, new[] { "7:" + family }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    /// <summary>Opens a request of <paramref name="family"/>, has agent-a report it and propose <paramref name="note"/>, and
    /// returns the request id. The moved clock keeps proposals in the order they were made.</summary>
    private async Task<Guid> ProposeAsync(string family, string note, DateTime at)
    {
        _now = at;
        var id = (await _store.OpenOrJoinAsync(Alert(at, family), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await _store.SubmitAsync(
            new InvestigationReportSubmission(id, claim.ClaimId!.Value, "agent-a", 1, "real", "a long blocking chain", true,
                "summary", "[{\"tool\":\"get_blocking\"}]") with { ProposedNote = note }, Ct));
        return id;
    }

    private string? Scalar(string sql)
    {
        using var connection = _db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task APendingNote_IsAbsentFromTheBriefRead_UntilApproved_AndARejectedNoteStaysStoredButOut()
    {
        var approvedRequest = await ProposeAsync("blocking", "approve me: the nightly load blocks for a minute", T0.AddMinutes(-60));
        var rejectedRequest = await ProposeAsync("resources", "reject me: a wrong guess", T0.AddMinutes(-30));
        _now = T0;
        var reader = new DuckDbInvestigationBriefSourceReader(_db, () => T0);

        /* Both wait, oldest first, with the request's server, family and metric and the agent that proposed them. */
        var pending = await _store.ListPendingNotesAsync(200, Ct);
        Assert.Equal(2, pending.Count);
        Assert.Equal("approve me: the nightly load blocks for a minute", pending[0].NoteText);
        Assert.Equal("srv-7", pending[0].ServerName);
        Assert.Equal(7, pending[0].ServerId);
        Assert.Equal("blocking", pending[0].Family);
        Assert.Equal("Blocking", pending[0].MetricName);
        Assert.Equal("agent:agent-a", pending[0].Author);
        Assert.Equal(approvedRequest, pending[0].SourceRequestId);
        Assert.Equal(rejectedRequest, pending[1].SourceRequestId);
        Assert.Equal(T0.AddMinutes(-60), pending[0].ProposedAtUtc);

        /* The composer's approved read does not carry a pending note. */
        var next = (await _store.OpenOrJoinAsync(Alert(T0, "jobs"), Ct)).RequestId!.Value;
        Assert.Empty((await reader.ReadAsync(next, Ct))!.Notes);

        /* Approve: the note joins the brief read, stamped with the approver, and leaves the pending list. */
        Assert.True(await _store.ApproveNoteAsync(pending[0].NoteId, "person:tester", Ct));
        var carried = Assert.Single((await reader.ReadAsync(next, Ct))!.Notes);
        Assert.Equal("approve me: the nightly load blocks for a minute", carried.Text);
        Assert.Equal("person:tester", carried.DecidedBy);
        Assert.Equal("approved|person:tester|2026-10-15 12:00:00", Scalar($@"
SELECT concat_ws('|', state, decided_by, strftime(decided_at, '%Y-%m-%d %H:%M:%S'))
FROM investigation_notes WHERE note_id = {pending[0].NoteId}"));
        Assert.Equal("1", Scalar($@"
SELECT count(*) FROM investigation_journal
WHERE event = 'note_approved' AND actor = 'person:tester' AND request_id = '{approvedRequest}' AND server_id = 7"));
        Assert.Equal(pending[1].NoteId, Assert.Single(await _store.ListPendingNotesAsync(200, Ct)).NoteId);

        /* Reject: the note stays out of the brief read and stays stored, with its verdict, who and when. */
        _now = T0.AddMinutes(5);
        Assert.True(await _store.RejectNoteAsync(pending[1].NoteId, "person:tester", Ct));
        var notes = (await reader.ReadAsync(next, Ct))!.Notes;
        Assert.Single(notes);
        Assert.DoesNotContain(notes, n => n.Text.StartsWith("reject me", StringComparison.Ordinal));
        Assert.Empty(await _store.ListPendingNotesAsync(200, Ct));
        Assert.Equal("rejected|person:tester|reject me: a wrong guess|2026-10-15 12:05:00", Scalar($@"
SELECT concat_ws('|', state, decided_by, note_text, strftime(decided_at, '%Y-%m-%d %H:%M:%S'))
FROM investigation_notes WHERE note_id = {pending[1].NoteId}"));
        Assert.Equal("1", Scalar($@"
SELECT count(*) FROM investigation_journal
WHERE event = 'note_rejected' AND actor = 'person:tester' AND request_id = '{rejectedRequest}'"));

        /* A decided note cannot be decided again either way, and an unknown id changes nothing. */
        Assert.False(await _store.ApproveNoteAsync(pending[1].NoteId, "person:other", Ct));
        Assert.False(await _store.RejectNoteAsync(pending[0].NoteId, "person:other", Ct));
        Assert.False(await _store.ApproveNoteAsync(long.MaxValue, "person:other", Ct));
        Assert.Equal("approved|person:tester,rejected|person:tester", Scalar(
            "SELECT string_agg(state || '|' || decided_by, ',' ORDER BY note_id) FROM investigation_notes"));
        Assert.Equal("2", Scalar("SELECT count(*) FROM investigation_journal WHERE event IN ('note_approved', 'note_rejected')"));
    }

    [Fact]
    public async Task TheLimit_IsClamped_AndABlankDecider_Throws()
    {
        await ProposeAsync("blocking", "one", T0.AddMinutes(-10));
        await ProposeAsync("resources", "two", T0.AddMinutes(-5));
        Assert.Single(await _store.ListPendingNotesAsync(1, Ct));
        Assert.Single(await _store.ListPendingNotesAsync(0, Ct));
        var note = (await _store.ListPendingNotesAsync(10, Ct)).First();
        await Assert.ThrowsAsync<ArgumentException>(() => _store.ApproveNoteAsync(note.NoteId, " ", Ct));
        Assert.Equal(2, (await _store.ListPendingNotesAsync(10, Ct)).Count);
    }

    /* #5903 round 1: a note is born with a 90-day expires_at and the brief read drops one past it, so approving an expired
       pending note would do nothing. The list leaves it out and shows the expiry of the ones it does list. */
    [Fact]
    public async Task AnExpiredPendingNote_IsNotListed_AndAListedNoteShowsItsExpiry()
    {
        await ProposeAsync("blocking", "stale: proposed 100 days ago", T0.AddDays(-100));
        await ProposeAsync("resources", "edge: expires a minute from now", T0.AddDays(-90).AddMinutes(1));
        await ProposeAsync("jobs", "fresh", T0.AddMinutes(-5));
        _now = T0;

        var pending = await _store.ListPendingNotesAsync(200, Ct);
        Assert.Equal(new[] { "edge: expires a minute from now", "fresh" }, pending.Select(n => n.NoteText).ToArray());
        Assert.Equal(T0.AddMinutes(1), pending[0].ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, pending[0].ExpiresAtUtc.Kind);
        Assert.Equal(T0.AddMinutes(-5).AddDays(90), pending[1].ExpiresAtUtc);

        /* The same clock decides the edge: a minute on, the edge note is expired and gone from the list. */
        _now = T0.AddMinutes(1);
        Assert.Equal("fresh", Assert.Single(await _store.ListPendingNotesAsync(200, Ct)).NoteText);
    }

    /* #5903 round 1: one decided_by rule (1 to 128 characters once trimmed) for every surface. */
    [Fact]
    public async Task ADecidedByOutsideOneTo128Characters_IsRefused_AndNothingChanges()
    {
        await ProposeAsync("blocking", "one", T0.AddMinutes(-10));
        _now = T0;
        var note = Assert.Single(await _store.ListPendingNotesAsync(10, Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.ApproveNoteAsync(note.NoteId, new string('x', 129), Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.RejectNoteAsync(note.NoteId, "", Ct));
        Assert.Equal("0", Scalar("SELECT count(*) FROM investigation_journal WHERE event IN ('note_approved', 'note_rejected')"));

        Assert.True(await _store.ApproveNoteAsync(note.NoteId, "  " + new string('x', 128) + "  ", Ct));
        Assert.Equal(new string('x', 128), Scalar($"SELECT decided_by FROM investigation_notes WHERE note_id = {note.NoteId}"));
    }
}
