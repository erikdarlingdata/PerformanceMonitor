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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's reads and writes of the Level 1 investigation tables (V184, #5632 Phase A): the requests an alert opened
/// for the operator's own agent, the reports agents returned, the journal, the notes a brief may carry and the environment
/// profiles. Same discipline as the other write partials: public-const SQL, bound <c>$N</c> parameters, a 42501 on a
/// read-only seat becomes <see cref="ViewerReadOnlyException"/> and a store below V184 becomes
/// <see cref="ViewerSchemaSkewException"/>.
///
/// <para><b>What the viewer may write</b> is what provisioning grants the <c>admin</c> role (DarlingManagedRoles section
/// 10d): it closes a request (the four close columns), journals what it did, writes, decides or deletes a note, and writes
/// the environment profiles. The <c>viewer</c> role holds SELECT only, so a read-only seat reads everything here and every
/// write refuses. Each write and its journal row run in one transaction, so a refused journal row rolls the write back.</para>
///
/// <para><b>The state shown</b> is computed the way the store's list computes it (<c>InvestigationRequestStore.ListSql</c>),
/// without writing: a claim whose lease lapsed reads as open, and a request past its expiry or out of claims reads as
/// expired before the sweep marks it.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The longest note a person may write; the V184 CHECK's bound.</summary>
    public const int InvestigationNoteMaxLength = InvestigationPersonWrites.NoteMaxLength;

    /// <summary>The longest environment profile; the V184 CHECK's bound.</summary>
    public const int InvestigationProfileMaxLength = InvestigationProfiles.MaxLength;

    /// <summary>The longest close reason the viewer accepts.</summary>
    public const int InvestigationCloseReasonMaxLength = InvestigationPersonWrites.CloseReasonMaxLength;

    /// <summary>A written note's default lifetime in days (decision h).</summary>
    public const int InvestigationNoteDefaultDays = InvestigationPersonWrites.NoteDefaultDays;

    /// <summary>A written note's longest lifetime in days (decision h).</summary>
    public const int InvestigationNoteMaxDays = InvestigationPersonWrites.NoteMaxDays;

    /// <summary>The first store version whose requests table has <c>practice_of</c> (V191, #5918).</summary>
    public const int PracticeStoreSchemaVersion = 191;

    /// <summary>The requests, newest first, with the state computed as the store's list computes it. $1 is the state
    /// filter (NULL for every state). <paramref name="withPractice"/> adds the <c>practice_of</c> column, which only a store at
    /// <see cref="PracticeStoreSchemaVersion"/> or later has: the Requests tab keeps its V184 floor and asks for it only then.</summary>
    public static string InvestigationRequestsSelectSql(bool withPractice) => @"
SELECT request_id, server_id, server_name, family, first_metric, opened_at, alert_count, effective_state,
       claimed_by, lease_until, claim_count, expires_at, reported_at, closed_at, closed_by, closed_reason,
       (SELECT p.verdict FROM config.investigation_reports AS p WHERE p.request_id = s.request_id) AS verdict" +
        (withPractice ? ", practice_of" : "") + @"
FROM (
    SELECT r.*,
           CASE
               WHEN r.state IN ('open', 'claimed') AND r.expires_at <= now() THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < now() AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'open' AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < now() THEN 'open'
               ELSE r.state
           END AS effective_state
    FROM config.investigation_requests AS r
) AS s
WHERE ($1::text IS NULL OR s.effective_state = $1)
ORDER BY s.opened_at DESC, s.request_id
LIMIT 500";

    /// <summary>The brief composed at claim, for the detail pane (NULL until an agent claims the request).</summary>
    public const string InvestigationBriefSelectSql =
        "SELECT brief_format, brief::text FROM config.investigation_requests WHERE request_id = $1";

    /// <summary>The alerts that opened or joined one request, oldest first.</summary>
    public const string InvestigationRequestAlertsSelectSql = @"
SELECT alert_time, metric_name, dedup_key
FROM config.investigation_request_alerts
WHERE request_id = $1
ORDER BY alert_time, request_alert_id
LIMIT 200";

    /// <summary>The report an agent returned for one request (at most one, the V184 unique key).</summary>
    public const string InvestigationReportsSelectSql = @"
SELECT report_id, submitted_at, agent_id, report_format, verdict, root_cause, root_cause_found, summary, evidence::text
FROM config.investigation_reports
WHERE request_id = $1
ORDER BY submitted_at DESC";

    /// <summary>One request's report links (#5914): its link_recorded journal rows, oldest first. The viewer re-checks each
    /// URL with <see cref="InvestigationReportLinks.UrlProblem"/> before it shows one as a link.</summary>
    public const string InvestigationLinksSelectSql = @"
SELECT detail::text
FROM config.investigation_journal
WHERE request_id = $1
  AND event = 'link_recorded'
ORDER BY journal_id";

    /// <summary>One request's journal, oldest first.</summary>
    public const string InvestigationJournalSelectSql = @"
SELECT event_time, event, actor, reason
FROM config.investigation_journal
WHERE request_id = $1
ORDER BY event_time, journal_id
LIMIT 500";

    /// <summary>The notes, pending first, then newest first. $1 is the state filter (NULL for every state).</summary>
    public const string InvestigationNotesSelectSql = @"
SELECT n.note_id, n.server_id, s.server_name, n.metric_name, n.note_text, n.author, n.source_request_id, n.state,
       n.created_at, n.expires_at, n.decided_at, n.decided_by
FROM config.investigation_notes AS n
LEFT JOIN servers AS s ON s.server_id = n.server_id
WHERE ($1::text IS NULL OR n.state = $1)
ORDER BY (n.state = 'pending') DESC, n.created_at DESC, n.note_id DESC
LIMIT 500";

    /// <summary>Every environment profile: the fleet's (both scopes NULL), then per server, then per tag (#6264: the one copy is
    /// <see cref="InvestigationProfiles.SelectSql"/>, which the web page shares).</summary>
    public const string InvestigationProfilesSelectSql = InvestigationProfiles.SelectSql;

    /// <summary>The journal row a person's action writes. $1 request (or NULL), $2 server, $3 event, $4 actor, $5 reason.</summary>
    public const string InvestigationJournalInsertSql = @"
INSERT INTO config.investigation_journal (request_id, server_id, event, actor, reason)
VALUES ($1, $2, $3, $4, $5)";

    /// <summary>Writes the one profile of a scope. $1 server, $2 tag (both NULL = fleet), $3 text, $4 actor.</summary>
    public const string InvestigationProfileUpsertSql = InvestigationProfiles.UpsertSql;

    /// <summary>Removes the profile of a scope (a profile saved blank). $1 server, $2 tag (both NULL = fleet).</summary>
    public const string InvestigationProfileDeleteSql = InvestigationProfiles.DeleteSql;

    /// <summary>The journal actor for a person: <c>person:&lt;login&gt;</c>, as the V184 journal records people.</summary>
    public static string InvestigationPersonActor(string login) => InvestigationPersonWrites.PersonActor(login);

    /// <summary>The requests, newest first. <paramref name="state"/> is one of the five states, or null for all.</summary>
    public async Task<IReadOnlyList<InvestigationRequestViewRow>> GetInvestigationRequestsAsync(
        string? state, CancellationToken cancellationToken = default)
    {
        /* #5918: practice_of exists from V191. An older store keeps listing as before, with no practice column; a store whose
           probe could not answer reads the older shape too. The read at V191 or later goes through SkewAsync like the other
           V191 reads, so a column the store lacks is the house schema-skew message. */
        var storeVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        var withPractice = storeVersion is int version && version >= PracticeStoreSchemaVersion;
        return withPractice
            ? await SkewAsync(() => ReadInvestigationRequestsAsync(state, true, cancellationToken))
            : await ReadInvestigationRequestsAsync(state, false, cancellationToken);
    }

    private async Task<IReadOnlyList<InvestigationRequestViewRow>> ReadInvestigationRequestsAsync(
        string? state, bool withPractice, CancellationToken cancellationToken)
    {
        var rows = new List<InvestigationRequestViewRow>();
        await using var command = _dataSource.CreateCommand(InvestigationRequestsSelectSql(withPractice));
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddNullableText(command, string.IsNullOrWhiteSpace(state) ? null : state.Trim());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationRequestViewRow
            {
                RequestId = reader.GetGuid(0),
                ServerId = reader.GetInt32(1),
                ServerName = reader.GetString(2),
                Family = reader.GetString(3),
                FirstMetric = reader.GetString(4),
                OpenedAtUtc = reader.GetDateTime(5),
                AlertCount = reader.GetInt32(6),
                State = reader.GetString(7),
                ClaimedBy = reader.IsDBNull(8) ? null : reader.GetString(8),
                LeaseUntilUtc = reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                ClaimCount = reader.GetInt32(10),
                ExpiresAtUtc = reader.GetDateTime(11),
                ReportedAtUtc = reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                ClosedAtUtc = reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                ClosedBy = reader.IsDBNull(14) ? null : reader.GetString(14),
                ClosedReason = reader.IsDBNull(15) ? null : reader.GetString(15),
                Verdict = reader.IsDBNull(16) ? null : reader.GetString(16),
                PracticeOf = withPractice && !reader.IsDBNull(17) ? reader.GetGuid(17) : null,
            });
        }

        return rows;
    }

    /// <summary>One request's brief, alerts, report and journal, for the detail pane.</summary>
    public async Task<InvestigationRequestDetail> GetInvestigationRequestDetailAsync(
        Guid requestId, CancellationToken cancellationToken = default)
    {
        var detail = new InvestigationRequestDetail { RequestId = requestId };
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);

        await using (var brief = Command(connection, InvestigationBriefSelectSql, requestId))
        await using (var reader = await brief.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                detail.BriefFormat = reader.IsDBNull(0) ? null : reader.GetInt16(0);
                detail.BriefJson = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }

        await using (var alerts = Command(connection, InvestigationRequestAlertsSelectSql, requestId))
        await using (var reader = await alerts.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                detail.Alerts.Add(new InvestigationAlertViewRow(
                    reader.GetDateTime(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }
        }

        await using (var reports = Command(connection, InvestigationReportsSelectSql, requestId))
        await using (var reader = await reports.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                detail.Reports.Add(new InvestigationReportViewRow
                {
                    ReportId = reader.GetInt64(0),
                    SubmittedAtUtc = reader.GetDateTime(1),
                    AgentId = reader.GetString(2),
                    ReportFormat = reader.GetInt16(3),
                    Verdict = reader.GetString(4),
                    RootCause = reader.GetString(5),
                    RootCauseFound = reader.GetBoolean(6),
                    Summary = reader.IsDBNull(7) ? null : reader.GetString(7),
                    EvidenceJson = reader.GetString(8),
                });
            }
        }

        if (detail.Reports.Count > 0)
        {
            await using var links = Command(connection, InvestigationLinksSelectSql, requestId);
            await using var reader = await links.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (InvestigationReportLinks.FromDetail(reader.IsDBNull(0) ? null : reader.GetString(0)) is { } link)
                    detail.Reports[0].Links.Add(link);
            }
        }

        await using (var journal = Command(connection, InvestigationJournalSelectSql, requestId))
        await using (var reader = await journal.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                detail.Journal.Add(new InvestigationJournalViewRow(
                    reader.GetDateTime(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3)));
            }
        }

        return detail;
    }

    /// <summary>The notes, pending first. <paramref name="state"/> is pending, approved or rejected, or null for all.</summary>
    public async Task<IReadOnlyList<InvestigationNoteViewRow>> GetInvestigationNotesAsync(
        string? state, CancellationToken cancellationToken = default)
    {
        var rows = new List<InvestigationNoteViewRow>();
        await using var command = _dataSource.CreateCommand(InvestigationNotesSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddNullableText(command, string.IsNullOrWhiteSpace(state) ? null : state.Trim());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationNoteViewRow
            {
                NoteId = reader.GetInt64(0),
                ServerId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                ServerName = reader.IsDBNull(2) ? null : reader.GetString(2),
                MetricName = reader.IsDBNull(3) ? null : reader.GetString(3),
                NoteText = reader.GetString(4),
                Author = reader.GetString(5),
                SourceRequestId = reader.IsDBNull(6) ? null : reader.GetGuid(6),
                State = reader.GetString(7),
                CreatedAtUtc = reader.GetDateTime(8),
                ExpiresAtUtc = reader.GetDateTime(9),
                DecidedAtUtc = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                DecidedBy = reader.IsDBNull(11) ? null : reader.GetString(11),
            });
        }

        return rows;
    }

    /// <summary>Every environment profile: fleet first, then per server, then per tag.</summary>
    public async Task<IReadOnlyList<InvestigationProfileViewRow>> GetInvestigationProfilesAsync(CancellationToken cancellationToken = default)
    {
        var rows = await new InvestigationRequestStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds).ListProfilesAsync(cancellationToken);
        return rows.Select(r => new InvestigationProfileViewRow
        {
            ProfileId = r.ProfileId,
            ScopeServerId = r.ScopeServerId,
            ScopeTagId = r.ScopeTagId,
            ProfileText = r.ProfileText,
            ModifiedAtUtc = r.ModifiedAtUtc,
            ModifiedBy = r.ModifiedBy,
        }).ToList();
    }

    /// <summary>
    /// Closes a request with a reason and journals <c>closed</c>, actor <c>person:&lt;login&gt;</c>, in one transaction.
    /// False when the request is gone, already closed or expired (nothing is written then).
    /// </summary>
    public async Task<bool> CloseInvestigationRequestAsync(
        Guid requestId, string login, string reason, CancellationToken cancellationToken = default)
    {
        if (ValidateInvestigationLogin(login) is { } loginError)
        {
            throw new ArgumentException(loginError, nameof(login));
        }

        if (ValidateInvestigationCloseReason(reason) is { } reasonError)
        {
            throw new ArgumentException(reasonError, nameof(reason));
        }

        /* #6265: the close statement, its journal row and the clock are the storage store's, the one the web route shares. */
        try
        {
            return await new InvestigationRequestStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds)
                .CloseRequestAsync(requestId, InvestigationPersonActor(login), reason, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>
    /// Approves or rejects a pending note and journals <c>note_approved</c> or <c>note_rejected</c> against the request that
    /// proposed it, in one transaction. False when the note is gone or already decided.
    /// </summary>
    public async Task<bool> DecideInvestigationNoteAsync(
        long noteId, bool approve, string login, CancellationToken cancellationToken = default)
    {
        var error = ValidateInvestigationLogin(login);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(login));
        }

        /* #5903: the decide statement, its journal row, the clock and the person guard are the storage
           store's, the one the web routes and Lite's twin share; the Viewer only adds its two translations. */
        var actor = InvestigationPersonActor(login);
        try
        {
            return await new InvestigationRequestStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds)
                .DecideNoteAsync(noteId, approve, actor, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>
    /// Writes a note as a person: approved as written, author <c>person:&lt;login&gt;</c>, expiring after
    /// <paramref name="days"/> (90 by default, 365 at most). Journals <c>note_written</c>. Returns the new note's id.
    /// </summary>
    public async Task<long> WriteInvestigationNoteAsync(
        int? serverId, string? metricName, string noteText, string login, int days = InvestigationNoteDefaultDays,
        CancellationToken cancellationToken = default)
    {
        var error = ValidateInvestigationLogin(login) ?? ValidateInvestigationNote(noteText, days, metricName);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(noteText));
        }

        try
        {
            return await new InvestigationRequestStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds)
                .WriteNoteAsync(serverId, metricName, noteText, InvestigationPersonActor(login), days, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>
    /// Saves the environment profile of one scope: the fleet (both null), one server or one tag. One row per scope; a
    /// blank text removes the scope's profile. <c>modified_by</c> is <c>person:&lt;login&gt;</c>. The write and its journal
    /// row (<c>profile_saved</c>, or <c>profile_removed</c> when a blank save removed a profile; request NULL, server = the
    /// scope's server or NULL for the fleet and tag scopes, reason naming the scope) run in one transaction, as close,
    /// decide and note write do: the profile goes into every brief of its scope, so a change to what agents are told
    /// leaves a trace (#5632 review L2). A blank save on a scope with no profile changes nothing and journals nothing.
    /// </summary>
    public async Task SaveInvestigationProfileAsync(
        int? scopeServerId, int? scopeTagId, string? profileText, string login, DateTime? expectedModifiedAt,
        CancellationToken cancellationToken = default)
    {
        /* #6264: the check, the write and the journal row are InvestigationRequestStore.SaveProfileAsync, the copy the web page shares.
           #6274: expectedModifiedAt is the modified_at the window showed for the scope (null = it showed none); a profile changed or
           removed since throws InvestigationProfileConflictException and nothing is written. */
        try
        {
            await new InvestigationRequestStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds)
                .SaveProfileAsync(scopeServerId, scopeTagId, profileText, login, expectedModifiedAt, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>The journal reason of a profile change, naming its scope: <c>fleet profile</c>, <c>server 7 profile</c> or
    /// <c>tag 3 profile</c>.</summary>
    public static string InvestigationProfileScopeReason(int? scopeServerId, int? scopeTagId) =>
        InvestigationProfiles.ScopeReason(scopeServerId, scopeTagId);

    /// <summary>How many requests the feedback list shows at most (the web page's list holds the same number).</summary>
    public const int InvestigationFeedbackListLimit = 200;

    /// <summary>The longest feedback window, in days; a longer one is refused, not shortened.</summary>
    public const int InvestigationFeedbackMaxDays = 365;

    /// <summary>
    /// The reported requests opened in the last <paramref name="daysBack"/> days, newest report first, each with the feedback
    /// people gave it (#5913). <paramref name="daysBack"/> is 1 to <see cref="InvestigationFeedbackMaxDays"/>; a value outside
    /// that is an <see cref="ArgumentOutOfRangeException"/>, never a shorter or longer window than the one asked for. Reads
    /// through <see cref="InvestigationFeedbackStore"/>; a store below V191 is a <see cref="ViewerSchemaSkewException"/>. The
    /// second value says the window held more than the limit.
    /// </summary>
    public async Task<(IReadOnlyList<InvestigationFeedbackListRow> Rows, bool Truncated)> GetInvestigationFeedbackListAsync(
        int daysBack, CancellationToken cancellationToken = default)
    {
        ValidateFeedbackDays(daysBack);
        var store = FeedbackStore();
        return await SkewAsync(() => store.ListReportedAsync(
            DateTime.UtcNow.AddDays(-daysBack), null, InvestigationFeedbackListLimit, cancellationToken));
    }

    /// <summary>The false-positive counts per alert family over the same window as the feedback list (#5913); the window is
    /// validated as <see cref="GetInvestigationFeedbackListAsync"/> does.</summary>
    public async Task<IReadOnlyList<InvestigationFamilyCounts>> GetInvestigationFamilyCountsAsync(
        int daysBack, CancellationToken cancellationToken = default)
    {
        ValidateFeedbackDays(daysBack);
        var store = FeedbackStore();
        return await SkewAsync(() => store.FamilyCountsAsync(
            DateTime.UtcNow.AddDays(-daysBack), null, cancellationToken));
    }

    private static void ValidateFeedbackDays(int daysBack)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(daysBack, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(daysBack, InvestigationFeedbackMaxDays);
    }

    /// <summary>
    /// Records the signed-in person's feedback on a report (#5913): their verdict, the corrected verdict if they disagree, the
    /// step the agent missed and a comment. The author is the login shaped by
    /// <see cref="InvestigationFeedbackRules.AuthorFromIdentity"/>; a second record by the same login replaces the first. A
    /// value the rules refuse is an <see cref="ArgumentException"/> carrying the rules' sentence, and nothing is written. An
    /// unknown request or one with no report comes back as the result's outcome and message. A read-only seat's 42501 is
    /// <see cref="ViewerReadOnlyException"/>. No journal row (the journal's event domain is not widened for feedback).
    /// </summary>
    public async Task<InvestigationFeedbackResult> RecordInvestigationFeedbackAsync(
        Guid requestId, string verdict, string? correctedVerdict, string? missedStep, string? comment, string login,
        CancellationToken cancellationToken = default)
    {
        var author = InvestigationFeedbackRules.AuthorFromIdentity(login);
        if (author is null)
        {
            throw new ArgumentException(ValidateInvestigationLogin(login) ?? "The viewer could not tell who you are, so it cannot record the change.", nameof(login));
        }

        var store = FeedbackStore();
        InvestigationFeedbackResult result;
        try
        {
            result = await SkewAsync(() => store.RecordAsync(
                new InvestigationFeedbackSubmission(requestId, verdict, correctedVerdict, missedStep, comment, author), cancellationToken));
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }

        return result.Outcome == InvestigationFeedbackOutcome.Refused
            ? throw new ArgumentException(result.Message, nameof(verdict))
            : result;
    }

    private InvestigationFeedbackStore FeedbackStore() =>
        new(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);

    /// <summary>Runs a feedback store call; a store without the V191 table or a column is the house schema-skew message.</summary>
    private static async Task<T> SkewAsync<T>(Func<Task<T>> call)
    {
        try
        {
            return await call();
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>
    /// The text a window shows for an exception from an investigation write (#7077). An <see cref="ArgumentException"/>
    /// carrying a refusal sentence ends its <see cref="Exception.Message"/> with " (Parameter 'login')", a code name the
    /// person cannot act on, so that suffix is removed. Any other exception's message comes back unchanged.
    /// </summary>
    internal static string InputErrorText(Exception ex)
    {
        var message = ex.Message;
        if (ex is ArgumentException { ParamName: { Length: > 0 } name })
        {
            message = message.Replace($" (Parameter '{name}')", "", StringComparison.Ordinal);
        }

        return message;
    }

    /// <summary>The login the actor names: not blank, and the undo path's person shape (1 to 256 characters: letters, digits, single spaces, a few punctuation marks).</summary>
    public static string? ValidateInvestigationLogin(string? login) => InvestigationPersonWrites.ValidateLogin(login);

    /// <summary>A close reason: not blank, at most 500 characters.</summary>
    public static string? ValidateInvestigationCloseReason(string? reason) => InvestigationPersonWrites.ValidateCloseReason(reason);

    /// <summary>A note: not blank, at most 2,000 characters, kept 1 to 365 days (the V184 CHECK's twin, said in words).</summary>
    public static string? ValidateInvestigationNote(string? noteText, int days, string? metricName = null) => InvestigationPersonWrites.ValidateNote(noteText, days, metricName);

    /// <summary>A profile: one scope at most (fleet, a server or a tag), at most 4,000 characters.</summary>
    public static string? ValidateInvestigationProfile(int? scopeServerId, int? scopeTagId, string? profileText) =>
        InvestigationProfiles.Validate(scopeServerId, scopeTagId, profileText);

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, Guid requestId)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = requestId });
        return command;
    }

    private static async Task JournalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid? requestId,
        int? serverId, string journalEvent, string actor, string? reason, CancellationToken cancellationToken)
    {
        await using var journal = new NpgsqlCommand(InvestigationJournalInsertSql, connection, transaction);
        journal.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        journal.Parameters.Add(requestId.HasValue
            ? new NpgsqlParameter<Guid> { TypedValue = requestId.Value }
            : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Uuid });
        AddNullableInt(journal, serverId);
        journal.Parameters.Add(new NpgsqlParameter<string> { TypedValue = journalEvent });
        journal.Parameters.Add(new NpgsqlParameter<string> { TypedValue = actor });
        AddNullableText(journal, reason);
        await journal.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Runs a write and its journal row in one transaction, with the house translation of 42501 and a missing
    /// V184 table.</summary>
    private async Task<T> InTransactionAsync<T>(
        Func<NpgsqlConnection, NpgsqlTransaction, Task<T>> body, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            var result = await body(connection, transaction);
            await transaction.CommitAsync(cancellationToken);
            return result;
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }
}

/// <summary>One investigation request as the Investigations window lists it (#5632).</summary>
public sealed class InvestigationRequestViewRow
{
    public Guid RequestId { get; init; }
    public int ServerId { get; init; }
    public string ServerName { get; init; } = "";
    public string Family { get; init; } = "";
    public string FirstMetric { get; init; } = "";
    public DateTime OpenedAtUtc { get; init; }
    public int AlertCount { get; init; }
    public string State { get; init; } = "";
    public string? ClaimedBy { get; init; }
    public DateTime? LeaseUntilUtc { get; init; }
    public int ClaimCount { get; init; }
    public DateTime ExpiresAtUtc { get; init; }
    public DateTime? ReportedAtUtc { get; init; }
    public DateTime? ClosedAtUtc { get; init; }
    public string? ClosedBy { get; init; }
    public string? ClosedReason { get; init; }
    public string? Verdict { get; init; }

    /// <summary>The request a practice request replays (#5918); null for every other request.</summary>
    public Guid? PracticeOf { get; init; }

    /// <summary>"Practice" for a practice request, empty otherwise: the Requests tab's marker column.</summary>
    public string PracticeDisplay => PracticeOf is null ? "" : "Practice";

    /// <summary>True when a person may still close it: open, claimed or reported.</summary>
    public bool IsClosable => State is "open" or "claimed" or "reported";

    public string OpenedDisplay => OpenedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);
    public string ExpiresDisplay => ExpiresAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);
    public string LeaseDisplay => LeaseUntilUtc is { } lease ? lease.ToLocalTime().ToString("HH:mm", CultureInfo.CurrentCulture) : "";
    public string ClaimsDisplay => ClaimCount.ToString(CultureInfo.InvariantCulture) + " of 3";
}

/// <summary>One request's detail: the brief, the alerts, the report and the journal.</summary>
public sealed class InvestigationRequestDetail
{
    public Guid RequestId { get; init; }
    public short? BriefFormat { get; set; }
    public string? BriefJson { get; set; }
    public List<InvestigationAlertViewRow> Alerts { get; } = new();
    public List<InvestigationReportViewRow> Reports { get; } = new();
    public List<InvestigationJournalViewRow> Journal { get; } = new();
}

/// <summary>An alert that opened or joined a request.</summary>
public sealed record InvestigationAlertViewRow(DateTime AlertTimeUtc, string MetricName, string? DedupKey)
{
    public string TimeDisplay => AlertTimeUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.CurrentCulture);
}

/// <summary>A journal row of a request.</summary>
public sealed record InvestigationJournalViewRow(DateTime EventTimeUtc, string Event, string Actor, string? Reason)
{
    public string TimeDisplay => EventTimeUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.CurrentCulture);
}

/// <summary>The report an agent returned.</summary>
public sealed class InvestigationReportViewRow
{
    public long ReportId { get; init; }
    public DateTime SubmittedAtUtc { get; init; }
    public string AgentId { get; init; } = "";
    public short ReportFormat { get; init; }
    public string Verdict { get; init; } = "";
    public string RootCause { get; init; } = "";
    public bool RootCauseFound { get; init; }
    public string? Summary { get; init; }
    public string EvidenceJson { get; init; } = "[]";

    /// <summary>The links the report recorded (#5914), as stored: render each through
    /// <see cref="InvestigationReportLinks.IsSafeUrl"/>, never straight.</summary>
    public List<InvestigationReportLink> Links { get; } = new();
}

/// <summary>A note as the Notes tab lists it.</summary>
public sealed class InvestigationNoteViewRow
{
    public long NoteId { get; init; }
    public int? ServerId { get; init; }
    public string? ServerName { get; init; }
    public string? MetricName { get; init; }
    public string NoteText { get; init; } = "";
    public string Author { get; init; } = "";
    public Guid? SourceRequestId { get; init; }
    public string State { get; init; } = "";
    public DateTime CreatedAtUtc { get; init; }
    public DateTime ExpiresAtUtc { get; init; }
    public DateTime? DecidedAtUtc { get; init; }
    public string? DecidedBy { get; init; }

    public bool IsPending => State == "pending";
    public string ScopeDisplay => ServerName ?? (ServerId is { } id ? "server " + id.ToString(CultureInfo.InvariantCulture) : "Fleet");
    public string CreatedDisplay => CreatedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);
    public string ExpiresDisplay => ExpiresAtUtc.ToLocalTime().ToString("yyyy-MM-dd", CultureInfo.CurrentCulture);
}

/// <summary>An environment profile as the Profiles tab lists it.</summary>
public sealed class InvestigationProfileViewRow
{
    public int ProfileId { get; init; }
    public int? ScopeServerId { get; init; }
    public int? ScopeTagId { get; init; }
    public string ProfileText { get; init; } = "";
    public DateTime ModifiedAtUtc { get; init; }
    public string? ModifiedBy { get; init; }
}
