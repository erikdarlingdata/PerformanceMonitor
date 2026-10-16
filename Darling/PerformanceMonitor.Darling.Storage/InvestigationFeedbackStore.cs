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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store write and reads of a person's feedback on an investigation report (#5913, #5632 Phase B slice F), on
/// <c>config.investigation_feedback</c> (V191). Kept apart from <see cref="InvestigationRequestStore"/> and its shared
/// contract because Lite has no feedback table.
///
/// <para><b>One record per author per request.</b> <see cref="RecordAsync"/> is an upsert on V191's unique index
/// <c>(request_id, lower(author))</c>: a second record by the same author, in any letter case, replaces the first in one
/// statement, so two clicks racing leave one row. Rules (<see cref="InvestigationFeedbackRules"/>) run first, so a verdict
/// outside right, partly or wrong, or text over 4000 characters, is refused in plain words and never reaches the CHECKs.
/// A request with no report is refused too: there is nothing to give feedback on.</para>
///
/// <para><b>Who may write.</b> Admin only (V191's grants); the viewer and mcp roles hold SELECT. A write from a viewer-role
/// login throws <see cref="PostgresException"/> with SQLSTATE 42501, which the web and the Viewer each translate to their own
/// wording. The reads need only SELECT.</para>
///
/// <para><b>Deleting a request deletes its feedback</b> (V191's <c>ON DELETE CASCADE</c>); nothing here deletes.</para>
/// </summary>
public sealed class InvestigationFeedbackStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;

    /// <summary>One feedback record in, one row out; the xmax trick tells an insert (0) from a replacement. $1 request, $2 verdict,
    /// $3 corrected verdict, $4 missed step, $5 comment, $6 author. No row when the request holds no report.</summary>
    public const string RecordSql = @"
INSERT INTO config.investigation_feedback (request_id, verdict, corrected_verdict, missed_step, comment, author)
SELECT $1, $2, $3, $4, $5, $6
WHERE EXISTS (SELECT 1 FROM config.investigation_reports AS p WHERE p.request_id = $1)
ON CONFLICT (request_id, lower(author)) DO UPDATE
SET verdict = EXCLUDED.verdict,
    corrected_verdict = EXCLUDED.corrected_verdict,
    missed_step = EXCLUDED.missed_step,
    comment = EXCLUDED.comment,
    author = EXCLUDED.author,
    created_at = now()
RETURNING (xmax = 0)";

    /// <summary>Whether a request exists, to tell a missing request from one with no report. $1 request.</summary>
    public const string RequestExistsSql = "SELECT EXISTS (SELECT 1 FROM config.investigation_requests WHERE request_id = $1)";

    /// <summary>The feedback of the requests in $1, newest first.</summary>
    public const string FeedbackSql = @"
SELECT request_id, verdict, corrected_verdict, missed_step, comment, author, created_at
FROM config.investigation_feedback
WHERE request_id = ANY($1)
ORDER BY created_at DESC, id DESC";

    /// <summary>Reported requests opened since $1, optionally on server $2, newest report first, at most $3.</summary>
    public const string ReportedSql = @"
SELECT r.request_id, r.server_name, r.family, r.first_metric, r.opened_at, p.submitted_at, p.agent_id, p.verdict,
       p.root_cause, p.summary
FROM config.investigation_requests AS r
JOIN config.investigation_reports AS p ON p.request_id = r.request_id
WHERE r.opened_at >= $1 AND ($2::integer IS NULL OR r.server_id = $2)
ORDER BY p.submitted_at DESC, r.request_id
LIMIT $3";

    /// <summary>
    /// The false-positive counts per family over the reported requests opened since $1, optionally on server $2. The settled
    /// verdict of a request is the corrected verdict of its newest feedback that gave one, else the report's verdict.
    /// </summary>
    public const string FamilyCountsSql = @"
WITH rep AS (
    SELECT r.request_id, r.family, p.verdict AS report_verdict
    FROM config.investigation_requests AS r
    JOIN config.investigation_reports AS p ON p.request_id = r.request_id
    WHERE r.opened_at >= $1 AND ($2::integer IS NULL OR r.server_id = $2)
), newest AS (
    SELECT DISTINCT ON (f.request_id) f.request_id, f.verdict
    FROM config.investigation_feedback AS f
    JOIN rep ON rep.request_id = f.request_id
    ORDER BY f.request_id, f.created_at DESC, f.id DESC
), newest_corrected AS (
    SELECT DISTINCT ON (f.request_id) f.request_id, f.corrected_verdict
    FROM config.investigation_feedback AS f
    JOIN rep ON rep.request_id = f.request_id
    WHERE f.corrected_verdict IS NOT NULL
    ORDER BY f.request_id, f.created_at DESC, f.id DESC
)
SELECT rep.family,
       count(*)::integer,
       count(newest.request_id)::integer,
       (count(*) FILTER (WHERE newest.verdict = 'wrong'))::integer,
       (count(*) FILTER (WHERE rep.report_verdict = 'false_positive'))::integer,
       (count(*) FILTER (WHERE coalesce(newest_corrected.corrected_verdict, rep.report_verdict) = 'false_positive'))::integer
FROM rep
LEFT JOIN newest ON newest.request_id = rep.request_id
LEFT JOIN newest_corrected ON newest_corrected.request_id = rep.request_id
GROUP BY rep.family
ORDER BY rep.family";

    /// <summary><paramref name="dataSource"/> must connect as a role that may write the feedback table to record (admin) and
    /// may SELECT it to read (admin, viewer or mcp).</summary>
    public InvestigationFeedbackStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    /// <summary>
    /// Records <paramref name="feedback"/>: a first record by this author on the request, or a replacement of that author's
    /// earlier one. Blank text is stored as null. A rule failure is returned as <see cref="InvestigationFeedbackOutcome.Refused"/>
    /// with plain words and writes nothing. A login that may not write throws <see cref="PostgresException"/> (SQLSTATE 42501).
    /// </summary>
    public async Task<InvestigationFeedbackResult> RecordAsync(InvestigationFeedbackSubmission feedback, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(feedback);
        var cleaned = feedback with
        {
            CorrectedVerdict = Blank(feedback.CorrectedVerdict),
            MissedStep = Blank(feedback.MissedStep),
            Comment = Blank(feedback.Comment),
        };
        var problems = InvestigationFeedbackRules.Validate(cleaned);
        if (problems.Count > 0)
        {
            return new InvestigationFeedbackResult(InvestigationFeedbackOutcome.Refused, InvestigationFeedbackRules.RefusalMessage(problems));
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using (var command = Command(connection, RecordSql,
            new NpgsqlParameter<Guid> { TypedValue = cleaned.RequestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            Text(cleaned.Verdict), Text(cleaned.CorrectedVerdict), Text(cleaned.MissedStep), Text(cleaned.Comment), Text(cleaned.Author)))
        {
            var inserted = await command.ExecuteScalarAsync(cancellationToken);
            if (inserted is bool wasInsert)
            {
                return new InvestigationFeedbackResult(wasInsert ? InvestigationFeedbackOutcome.Recorded : InvestigationFeedbackOutcome.Replaced, null);
            }
        }

        await using var exists = Command(connection, RequestExistsSql,
            new NpgsqlParameter<Guid> { TypedValue = cleaned.RequestId, NpgsqlDbType = NpgsqlDbType.Uuid });
        return await exists.ExecuteScalarAsync(cancellationToken) is true
            ? new InvestigationFeedbackResult(InvestigationFeedbackOutcome.NoReport,
                "That request has no report yet, so there is nothing to give feedback on.")
            : new InvestigationFeedbackResult(InvestigationFeedbackOutcome.NotFound, "No investigation request has that id.");
    }

    /// <summary>The feedback of <paramref name="requestIds"/>, newest first. Writes nothing.</summary>
    public async Task<IReadOnlyList<InvestigationFeedbackRow>> ReadAsync(
        IReadOnlyCollection<Guid> requestIds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requestIds);
        var rows = new List<InvestigationFeedbackRow>();
        if (requestIds.Count == 0)
        {
            return rows;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, FeedbackSql,
            new NpgsqlParameter<Guid[]> { TypedValue = requestIds.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationFeedbackRow(
                reader.GetGuid(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetString(5), reader.GetFieldValue<DateTime>(6)));
        }

        return rows;
    }

    /// <summary>
    /// The reported requests opened since <paramref name="sinceUtc"/> (optionally on one server), newest report first, each
    /// with its feedback, at most <paramref name="limit"/>. The read goes one row past the limit to say whether the window held
    /// more. Writes nothing.
    /// </summary>
    public async Task<(IReadOnlyList<InvestigationFeedbackListRow> Rows, bool Truncated)> ListReportedAsync(
        DateTime sinceUtc, int? serverId, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        var heads = new List<(Guid Id, string Server, string Family, string Metric, DateTime Opened, DateTime Reported, string Agent, string Verdict, string Root, string? Summary)>();
        await using (var connection = await _dataSource.OpenConnectionAsync(cancellationToken))
        await using (var command = Command(connection, ReportedSql, Utc(sinceUtc),
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            new NpgsqlParameter<int> { TypedValue = limit + 1 }))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                heads.Add((reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                    reader.GetFieldValue<DateTime>(4), reader.GetFieldValue<DateTime>(5), reader.GetString(6), reader.GetString(7),
                    reader.GetString(8), reader.IsDBNull(9) ? null : reader.GetString(9)));
            }
        }

        var truncated = heads.Count > limit;
        if (truncated)
        {
            heads.RemoveRange(limit, heads.Count - limit);
        }

        var feedback = (await ReadAsync(heads.Select(h => h.Id).ToArray(), cancellationToken))
            .ToLookup(f => f.RequestId);
        var rows = heads.Select(h => new InvestigationFeedbackListRow(
            h.Id, h.Server, h.Family, h.Metric, h.Opened, h.Reported, h.Agent, h.Verdict, h.Root, h.Summary,
            feedback[h.Id].ToList())).ToList();
        return (rows, truncated);
    }

    /// <summary>
    /// The false-positive counts per alert family over the reported requests opened since <paramref name="sinceUtc"/>
    /// (optionally on one server), families in name order. Writes nothing.
    /// </summary>
    public async Task<IReadOnlyList<InvestigationFamilyCounts>> FamilyCountsAsync(
        DateTime sinceUtc, int? serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<InvestigationFamilyCounts>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, FamilyCountsSql, Utc(sinceUtc),
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationFamilyCounts(
                reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4), reader.GetInt32(5)));
        }

        return rows;
    }

    private NpgsqlCommand Command(NpgsqlConnection connection, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }

    private static string? Blank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    private static NpgsqlParameter<string?> Text(string? value) =>
        new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };

    private static NpgsqlParameter<DateTime> Utc(DateTime value) =>
        new() { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz };
}
