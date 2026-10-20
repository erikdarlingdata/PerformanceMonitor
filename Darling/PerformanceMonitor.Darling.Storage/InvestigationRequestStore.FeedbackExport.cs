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

/// <summary>One investigation case that a person has given feedback on (#6236): the request (the alert), its stored brief (the
/// alert and the evidence the agent was given), the report the agent wrote, and the people's feedback on it.</summary>
public sealed record InvestigationFeedbackCase(
    InvestigationRequestRow Request, string? Brief, InvestigationReportRow Report, IReadOnlyList<InvestigationFeedbackRow> Feedback);

public sealed partial class InvestigationRequestStore
{
    /// <summary>The most cases one export reads (#6236); a window with more says so and the operator narrows it.</summary>
    public const int FeedbackExportLimit = 2000;

    /// <summary>The cases one page of an export read holds (#6236): the export holds one page at a time, never the whole window.</summary>
    internal const int FeedbackExportPageSize = 50;

    /// <summary>
    /// The requests that have a report and at least one feedback row (#6236), newest first, one page. $1 now (the state is computed
    /// as the other reads compute it), $2 opened at or after (null: no lower bound), $3 opened before (null: no upper bound), $4
    /// server (null: all), $5 a verdict a person recorded (null: any), $6 page size, $7 and $8 the last request of the previous page
    /// (opened_at and id; null for the first page). A practice request is not real traffic (#5918) and is left out. A request
    /// matches the verdict when ANY of its feedback rows holds it; the case then carries all its feedback.
    /// </summary>
    internal static string FeedbackCasesRequestsSql => ReportsRequestsSelect + @"
WHERE r.practice_of IS NULL
  AND ($2::timestamptz IS NULL OR r.opened_at >= $2)
  AND ($3::timestamptz IS NULL OR r.opened_at < $3)
  AND ($4::integer IS NULL OR r.server_id = $4)
  AND ($7::timestamptz IS NULL OR r.opened_at < $7 OR (r.opened_at = $7 AND r.request_id > $8::uuid))
  AND EXISTS (SELECT 1 FROM config.investigation_reports AS p WHERE p.request_id = r.request_id)
  AND EXISTS (SELECT 1 FROM config.investigation_feedback AS f
              WHERE f.request_id = r.request_id AND ($5::text IS NULL OR f.verdict = $5))
ORDER BY r.opened_at DESC, r.request_id
LIMIT $6";

    /// <summary>
    /// Hands <paramref name="onCase"/> every request that has feedback inside the filter, with its brief, report and all of its
    /// feedback, newest first, one case at a time (#6236): the test set an operator replays their own agent on. The store is read a
    /// page of <see cref="FeedbackExportPageSize"/> at a time, so memory holds a page however large the window is. Returns whether
    /// the filter held more than <paramref name="limit"/> cases. Reads only; the text is as stored, and the caller masks it as the
    /// report reads do.
    /// </summary>
    public async Task<bool> ReadFeedbackCasesAsync(
        DateTime? openedFromUtc, DateTime? openedBeforeUtc, int? serverId, string? feedbackVerdict, int limit,
        Func<InvestigationFeedbackCase, Task> onCase, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentNullException.ThrowIfNull(onCase);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var feedbackStore = new InvestigationFeedbackStore(_dataSource, _commandTimeoutSeconds);
        var seen = 0;
        DateTime? cursorOpened = null;
        Guid? cursorId = null;
        while (seen <= limit)
        {
            var pageSize = Math.Min(FeedbackExportPageSize, limit + 1 - seen);
            var requests = new List<InvestigationRequestRow>();
            var briefs = new Dictionary<Guid, string?>();
            await using (var command = Command(connection, null, FeedbackCasesRequestsSql,
                P(Now()), NullableTime(openedFromUtc), NullableTime(openedBeforeUtc),
                new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
                new NpgsqlParameter<string?> { TypedValue = feedbackVerdict, NpgsqlDbType = NpgsqlDbType.Text }, P(pageSize),
                NullableTime(cursorOpened),
                new NpgsqlParameter<Guid?> { TypedValue = cursorId, NpgsqlDbType = NpgsqlDbType.Uuid }))
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var row = ReportsRequestRow(reader);
                    requests.Add(row);
                    briefs[row.RequestId] = reader.IsDBNull(16) ? null : reader.GetString(16);
                }
            }

            if (requests.Count == 0)
                return false;

            var ids = requests.Select(r => r.RequestId).ToArray();
            var reports = (await ReadReportsForAsync(connection, ids, cancellationToken)).ToDictionary(r => r.RequestId);
            var feedback = (await feedbackStore.ReadAsync(connection, ids, cancellationToken))
                .GroupBy(f => f.RequestId).ToDictionary(g => g.Key, g => (IReadOnlyList<InvestigationFeedbackRow>)g.ToList());
            foreach (var request in requests)
            {
                if (seen == limit)
                    return true;
                seen++;
                if (reports.TryGetValue(request.RequestId, out var report) && feedback.TryGetValue(request.RequestId, out var rows))
                    await onCase(new InvestigationFeedbackCase(request, briefs[request.RequestId], report, rows));
            }

            if (requests.Count < pageSize)
                return false;
            cursorOpened = requests[^1].OpenedAtUtc;
            cursorId = requests[^1].RequestId;
        }

        return false;
    }

    private static NpgsqlParameter<DateTime?> NullableTime(DateTime? value) =>
        new() { TypedValue = value is { } v ? DateTime.SpecifyKind(v, DateTimeKind.Utc) : null, NpgsqlDbType = NpgsqlDbType.TimestampTz };
}
