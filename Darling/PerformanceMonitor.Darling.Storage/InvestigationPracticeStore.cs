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
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>How opening a practice request came out (#5918).</summary>
public enum InvestigationPracticeOutcome
{
    /// <summary>The practice request was opened.</summary>
    Opened,

    /// <summary>No investigation request has that id.</summary>
    NotFound,

    /// <summary>The request is itself a practice request; only a live case is replayed.</summary>
    IsPractice,

    /// <summary>The request holds no report, so it was never judged.</summary>
    NoReport,

    /// <summary>Nobody gave the report feedback, so there is nothing to score a replay against.</summary>
    NoFeedback,

    /// <summary>A practice request of this case is still open or claimed; finish or let it expire before replaying again.</summary>
    AlreadyLive,
}

/// <summary>The result of opening a practice request; <see cref="RequestId"/> is the new request (or the live one for
/// <see cref="InvestigationPracticeOutcome.AlreadyLive"/>).</summary>
public sealed record InvestigationPracticeResult(InvestigationPracticeOutcome Outcome, Guid? RequestId, string? Message);

/// <summary>
/// One agent's practice score (#5918). <see cref="Cases"/> are the replayed cases the agent reported on (its newest practice
/// report per case); <see cref="Scored"/> those with a known answer; <see cref="Unscored"/> those without one (feedback says
/// wrong with no corrected verdict, or the replayed case is gone). <see cref="Possible"/> is the credit the scored cases carry,
/// <see cref="Points"/> what the agent earned, <see cref="Percent"/> their ratio (null when nothing was scored).
/// <see cref="MissedStepCases"/> counts the cases whose feedback names a missed step, for a person to read the practice report
/// against. Advisory: nothing reads it to turn a route on or off.
/// </summary>
public sealed record InvestigationPracticeScore(
    string AgentId, int Cases, int Scored, int Unscored, int Matched, double Points, double Possible, double? Percent, int MissedStepCases);

/// <summary>One practice report with what its replayed case settled to, for the score and for a list.</summary>
public sealed record InvestigationPracticeRun(
    Guid PracticeRequestId, Guid SourceRequestId, string ServerName, string Family, string AgentId, string PracticeVerdict,
    DateTime ReportedAtUtc, string? ExpectedVerdict, double Credit, double Points, bool? Matched, bool HasMissedStep);

/// <summary>
/// The pure scoring of a practice report against the feedback on the case it replays (#5918). What the feedback says, newest
/// record first: a corrected verdict, when any record gave one, is the answer and carries full credit; else a verdict of
/// <c>right</c> makes the original report's verdict the answer at full credit, and <c>partly</c> at half credit (the report
/// was only partly right, so matching it earns half); <c>wrong</c> with no corrected verdict names no answer, so the case
/// cannot be scored. A practice report earns the credit when its verdict equals the answer and nothing otherwise.
/// </summary>
public static class InvestigationPracticeScoring
{
    /// <summary>Credit a <c>partly</c> verdict carries.</summary>
    public const double PartlyCredit = 0.5;

    /// <summary>The answer the feedback settles on and the credit a match earns; a null answer is a case that cannot be scored.</summary>
    public static (string? Expected, double Credit) Settle(string? originalReportVerdict, IReadOnlyList<InvestigationFeedbackRow> feedback)
    {
        ArgumentNullException.ThrowIfNull(feedback);
        var newestFirst = feedback.OrderByDescending(f => f.CreatedAtUtc).ToList();
        if (newestFirst.Count == 0)
        {
            return (null, 0);
        }

        var corrected = newestFirst.FirstOrDefault(f => !string.IsNullOrWhiteSpace(f.CorrectedVerdict));
        if (corrected is not null)
        {
            return (corrected.CorrectedVerdict, 1.0);
        }

        return newestFirst[0].Verdict switch
        {
            "right" when originalReportVerdict is not null => (originalReportVerdict, 1.0),
            "partly" when originalReportVerdict is not null => (originalReportVerdict, PartlyCredit),
            _ => (null, 0),
        };
    }

    /// <summary>Scores one practice report.</summary>
    public static InvestigationPracticeRun Run(
        Guid practiceRequestId, Guid sourceRequestId, string serverName, string family, string agentId, string practiceVerdict,
        DateTime reportedAtUtc, string? originalReportVerdict, IReadOnlyList<InvestigationFeedbackRow> feedback)
    {
        var (expected, credit) = Settle(originalReportVerdict, feedback);
        bool? matched = expected is null ? null : string.Equals(expected, practiceVerdict, StringComparison.Ordinal);
        return new InvestigationPracticeRun(practiceRequestId, sourceRequestId, serverName, family, agentId, practiceVerdict,
            reportedAtUtc, expected, credit, matched == true ? credit : 0, matched,
            feedback.Any(f => !string.IsNullOrWhiteSpace(f.MissedStep)));
    }

    /// <summary>The score per agent: each agent's newest run per replayed case counts; agents in name order.</summary>
    public static IReadOnlyList<InvestigationPracticeScore> ByAgent(IEnumerable<InvestigationPracticeRun> runs)
    {
        ArgumentNullException.ThrowIfNull(runs);
        return runs
            .GroupBy(r => (r.AgentId, r.SourceRequestId))
            .Select(g => g.OrderByDescending(r => r.ReportedAtUtc).First())
            .GroupBy(r => r.AgentId, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g =>
            {
                var scored = g.Where(r => r.Matched is not null).ToList();
                var possible = scored.Sum(r => r.Credit);
                var points = scored.Sum(r => r.Points);
                return new InvestigationPracticeScore(g.Key, g.Count(), scored.Count, g.Count() - scored.Count,
                    scored.Count(r => r.Matched == true), points, possible, possible > 0 ? points / possible : null,
                    g.Count(r => r.HasMissedStep));
            })
            .ToList();
    }
}

/// <summary>
/// Opens practice requests and reads their runs (#5918, #5632 Phase B). A practice request replays a reported case that has
/// feedback: the new request copies the case's route, server, family, first metric, story path, alert times, alert count and the alert
/// rows' values (so the brief's alerts section reads what the original brief read), and carries <c>practice_of</c>. It copies
/// no report, verdict, feedback or note. It is stamped level 1 like any request; the gate refuses every action on it with
/// <c>practice</c> whatever its level, and a live alert never joins it nor counts it against a route's limits.
///
/// <para><b>Who may write.</b> The desktop Viewer's admin role (provisioning's INSERT grants on the request and alert-link
/// tables). A viewer-role login throws <see cref="PostgresException"/> with SQLSTATE 42501.</para>
/// </summary>
public sealed class InvestigationPracticeStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;
    private readonly Func<DateTime> _utcNow;

    /// <summary>
    /// The case to replay. <c>FOR SHARE OF r</c> locks the case's REQUEST row, so the case cannot be deleted under the copy (its
    /// reports, feedback and alert rows reference it ON DELETE CASCADE). It does not lock the feedback table: a direct delete of
    /// the case's feedback rows can still commit between this read and the insert, and the copy reads no feedback. $1 request.
    /// </summary>
    internal const string SourceSql = @"
SELECT r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.story_path_hash, r.first_alert_at, r.last_alert_at,
       r.window_until, r.lease_minutes, r.expires_at - r.opened_at, r.practice_of, r.alert_count,
       EXISTS (SELECT 1 FROM config.investigation_reports AS p WHERE p.request_id = r.request_id),
       EXISTS (SELECT 1 FROM config.investigation_feedback AS f WHERE f.request_id = r.request_id)
FROM config.investigation_requests AS r
WHERE r.request_id = $1
FOR SHARE OF r";

    /// <summary>
    /// A LIVE practice request of the case: the same computed state (<see cref="InvestigationRequestStore.LivePracticeRequestsSql"/>)
    /// that hides the case's answers, so a practice that is dead (claimed three times, lease lapsed) no longer blocks a fresh
    /// replay. $1 case, $2 now.
    /// </summary>
    internal static readonly string LiveSql =
        "SELECT lp.request_id FROM (" + InvestigationRequestStore.LivePracticeRequestsSql("$2") + ") AS lp "
        + "WHERE lp.practice_of = $1 ORDER BY lp.opened_at DESC LIMIT 1";

    internal const string OpenSql = @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
     window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, practice_of)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11 + $12, $13, 1, $14, $15)";

    /// <summary>The case's alert rows, copied without the dedup key. $1 new request, $2 case.</summary>
    internal const string CopyAlertsSql = @"
INSERT INTO config.investigation_request_alerts
    (request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json, severity)
SELECT $1, alert_time, server_id, metric_name, NULL, current_value, threshold_value, muted, detail_text, context_json, severity
FROM config.investigation_request_alerts
WHERE request_id = $2";

    /// <summary>Reports of practice requests opened since $1, with the case's own report verdict (none when the case is deleted).</summary>
    internal const string RunsSql = @"
SELECT p.request_id, p.practice_of, p.server_name, p.family, rp.agent_id, rp.verdict, rp.submitted_at, src.verdict
FROM config.investigation_requests AS p
JOIN config.investigation_reports AS rp ON rp.request_id = p.request_id
LEFT JOIN config.investigation_reports AS src ON src.request_id = p.practice_of
WHERE p.practice_of IS NOT NULL
  AND p.opened_at >= $1
ORDER BY rp.submitted_at DESC, p.request_id";

    /// <summary><paramref name="dataSource"/> must connect as admin to open, and may be viewer or mcp to read.</summary>
    public InvestigationPracticeStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds, Func<DateTime>? utcNow = null)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>
    /// Opens a practice request replaying <paramref name="sourceRequestId"/>, with an <c>opened</c> journal row naming
    /// <paramref name="actor"/>. Refused, writing nothing, for an unknown case, a practice request, a case with no report or
    /// no feedback, or a case that already has a live practice request. A login that may not write throws
    /// <see cref="PostgresException"/> (42501).
    /// </summary>
    public async Task<InvestigationPracticeResult> OpenAsync(Guid sourceRequestId, string actor, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        if (InvestigationPersonWrites.ValidateActor(actor) is { } actorError)
        {
            throw new ArgumentException(actorError, nameof(actor));
        }

        var now = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        int routeId, serverId, leaseMinutes, alertCount;
        string serverName, family, firstMetric;
        string? storyPath;
        DateTime firstAlert, lastAlert, windowUntil;
        TimeSpan life;
        bool isPractice, hasReport, hasFeedback;
        await using (var command = Command(connection, transaction, SourceSql, Uuid(sourceRequestId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                return new InvestigationPracticeResult(InvestigationPracticeOutcome.NotFound, null, "No investigation request has that id.");
            }

            routeId = reader.GetInt32(0);
            serverId = reader.GetInt32(1);
            serverName = reader.GetString(2);
            family = reader.GetString(3);
            firstMetric = reader.GetString(4);
            storyPath = reader.IsDBNull(5) ? null : reader.GetString(5);
            firstAlert = reader.GetFieldValue<DateTime>(6);
            lastAlert = reader.GetFieldValue<DateTime>(7);
            windowUntil = reader.GetFieldValue<DateTime>(8);
            leaseMinutes = reader.GetInt32(9);
            life = reader.GetFieldValue<TimeSpan>(10);
            isPractice = !reader.IsDBNull(11);
            alertCount = reader.GetInt32(12);
            hasReport = reader.GetBoolean(13);
            hasFeedback = reader.GetBoolean(14);
        }

        if (isPractice)
        {
            return new InvestigationPracticeResult(InvestigationPracticeOutcome.IsPractice, null, "That is a practice request; only a live case is replayed.");
        }

        if (!hasReport)
        {
            return new InvestigationPracticeResult(InvestigationPracticeOutcome.NoReport, null, "That request has no report, so it was never judged and cannot be replayed.");
        }

        if (!hasFeedback)
        {
            return new InvestigationPracticeResult(InvestigationPracticeOutcome.NoFeedback, null,
                "Nobody has given feedback on that report, so there is nothing to score a replay against. Give feedback first.");
        }

        await using (var live = Command(connection, transaction, LiveSql, Uuid(sourceRequestId), Ts(now)))
        {
            if (await live.ExecuteScalarAsync(cancellationToken) is Guid existing)
            {
                return new InvestigationPracticeResult(InvestigationPracticeOutcome.AlreadyLive, existing,
                    "That case already has a practice request that is open or claimed.");
            }
        }

        var practiceId = Guid.NewGuid();
        await using (var insert = Command(connection, transaction, OpenSql, Uuid(practiceId), Int(routeId), Int(serverId), Text(serverName),
            Text(family), Text(firstMetric), Text(storyPath), Ts(firstAlert), Ts(lastAlert), Ts(windowUntil), Ts(now),
            new NpgsqlParameter<TimeSpan> { TypedValue = life, NpgsqlDbType = NpgsqlDbType.Interval }, Int(leaseMinutes), Int(alertCount), Uuid(sourceRequestId)))
        {
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var copy = Command(connection, transaction, CopyAlertsSql, Uuid(practiceId), Uuid(sourceRequestId)))
        {
            await copy.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var journal = Command(connection, transaction, InvestigationRequestStore.AgentActionJournalSql,
            Ts(now), Uuid(practiceId), Int(serverId), Text("opened"), Text(actor),
            Text("practice of " + sourceRequestId.ToString("D", CultureInfo.InvariantCulture)),
            new NpgsqlParameter<string?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.Text }))
        {
            await journal.ExecuteScalarAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return new InvestigationPracticeResult(InvestigationPracticeOutcome.Opened, practiceId, null);
    }

    /// <summary>
    /// The practice reports of requests opened since <paramref name="sinceUtc"/>, newest report first: each report's verdict
    /// against what the feedback on its case settled to (<see cref="InvestigationPracticeScoring"/>). Writes nothing.
    /// </summary>
    public async Task<IReadOnlyList<InvestigationPracticeRun>> ReadRunsAsync(DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var heads = new List<(Guid Practice, Guid Source, string Server, string Family, string Agent, string Verdict, DateTime At, string? SourceVerdict)>();
        await using (var command = Command(connection, null, RunsSql, Ts(sinceUtc)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                heads.Add((reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2), reader.GetString(3), reader.GetString(4),
                    reader.GetString(5), reader.GetFieldValue<DateTime>(6), reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        var feedback = (await new InvestigationFeedbackStore(_dataSource, _commandTimeoutSeconds)
            .ReadAsync(connection, heads.Select(h => h.Source).Distinct().ToArray(), cancellationToken)).ToLookup(f => f.RequestId);
        return heads.Select(h => InvestigationPracticeScoring.Run(h.Practice, h.Source, h.Server, h.Family, h.Agent, h.Verdict, h.At,
            h.SourceVerdict, feedback[h.Source].ToList())).ToList();
    }

    /// <summary>The score per agent over the practice requests opened since <paramref name="sinceUtc"/>.</summary>
    public async Task<IReadOnlyList<InvestigationPracticeScore>> ReadScoresAsync(DateTime sinceUtc, CancellationToken cancellationToken = default) =>
        InvestigationPracticeScoring.ByAgent(await ReadRunsAsync(sinceUtc, cancellationToken));

    private NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }

    private static NpgsqlParameter<Guid> Uuid(Guid value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Uuid };

    private static NpgsqlParameter<int> Int(int value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Integer };

    private static NpgsqlParameter<string?> Text(string? value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };

    private static NpgsqlParameter<DateTime> Ts(DateTime value) =>
        new() { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz };
}
