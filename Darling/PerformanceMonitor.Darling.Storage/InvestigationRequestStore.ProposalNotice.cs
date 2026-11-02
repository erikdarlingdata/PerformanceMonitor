/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7109: a pending proposal whose creation is journaled and that has no notice mark yet. It carries only what the notice may say: the
/// proposal, its request and route, the server and the tool. The agent's reason, the target and the statement are not read.
/// </summary>
public sealed record ProposalNoticeCandidate(Guid ProposalId, Guid RequestId, int RouteId, int ServerId, string ServerName, string Tool);

public sealed partial class InvestigationRequestStore
{
    /// <summary>The journal row that marks a proposal's notice as handled (once key <see cref="ProposalNoticeOnceKey"/>).</summary>
    public const string ProposalNoticeSent = "proposal_notice_sent";

    /// <summary>The once key of a proposal's notice: one per proposal, so a proposal is announced at most once whatever replays the pass.</summary>
    public static string ProposalNoticeOnceKey(Guid proposalId) => "proposal_notice:" + proposalId.ToString("D");

    /* The notice is driven by the `proposal_created` journal row. CreateProposalAsync writes that row only for a call that inserted the
       proposal, and a retry that met the live row writes none, so a retry never has a candidate. The window runs on the journal row's
       time (ix_investigation_journal_time). Only a pending proposal is announced: one that ended before the pass has nothing to approve. */
    internal const string PendingProposalNoticesSql = @"
SELECT p.proposal_id, p.request_id, r.route_id, p.server_id, COALESCE(sv.display_name, sv.server_name, r.server_name), p.tool
FROM config.investigation_journal AS j
JOIN config.agent_action_proposals AS p ON p.proposal_id = (j.detail->>'proposal_id')::uuid
JOIN config.investigation_requests AS r ON r.request_id = p.request_id
LEFT JOIN collect.servers AS sv ON sv.server_id = p.server_id
WHERE j.event = 'proposal_created'
  AND j.event_time > $1
  AND p.state = 'pending'
  AND r.practice_of IS NULL
  AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS m
                  WHERE m.once_key = 'proposal_notice:' || p.proposal_id::text)
ORDER BY j.journal_id
LIMIT $2";

    /// <summary>Up to <paramref name="limit"/> proposals created after <paramref name="sinceUtc"/> that are still pending and have no notice
    /// mark, oldest first. Writes nothing.</summary>
    public async Task<IReadOnlyList<ProposalNoticeCandidate>> PendingProposalNoticesAsync(
        DateTime sinceUtc, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, PendingProposalNoticesSql, P(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc)), P(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<ProposalNoticeCandidate>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ProposalNoticeCandidate(reader.GetGuid(0), reader.GetGuid(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetString(4),
                reader.GetString(5)));
        }

        return rows;
    }

    /// <summary>Writes the proposal's notice mark with <paramref name="reason"/> (the channels that took it, or why none was sent) and answers
    /// whether THIS call wrote it. The caller sends only on true.</summary>
    public Task<bool> MarkProposalNoticeAsync(ProposalNoticeCandidate candidate, string reason, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(candidate);
        return MarkOnceAsync(candidate.RequestId, candidate.RouteId, candidate.ServerId, ProposalNoticeSent, reason,
            ProposalNoticeOnceKey(candidate.ProposalId), cancellationToken);
    }

    /// <summary>An enabled person with at least one identity holding any <c>approve</c> grant, whatever its kind or scope.</summary>
    internal const string AnyApproverAnywhereSql = @"
SELECT EXISTS (
    SELECT 1
    FROM config.person AS p
    JOIN config.person_grant AS g ON g.person_id = p.person_id
    WHERE p.enabled
      AND g.capability = 'approve'
      AND EXISTS (SELECT 1 FROM config.person_identity AS i WHERE i.person_id = p.person_id))";

    /// <summary>
    /// True when no enabled mapped person holds any <c>approve</c> grant at all (#7109), so no proposal on any server can be approved by a
    /// person. A route that needs a second person then has nobody to approve for it: the route page says so (<see cref="ActionApprovalRules.NobodyCanApproveMessage"/>).
    /// A per-server, per-kind answer is <see cref="NobodyCanApproveAsync"/>.
    /// </summary>
    public async Task<bool> NobodyCanApproveAnywhereAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, AnyApproverAnywhereSql);
        return await command.ExecuteScalarAsync(cancellationToken) is not true;
    }

    /// <summary>
    /// What the request's live routes say about approving its proposals: whether they need a person and whether a single person may approve
    /// (<see cref="ActionApprovalRules.ForRequest"/> over the routes the request's metrics resolve to now). Null when the request is gone.
    /// <paramref name="loadRoutes"/> is the service's route hook. Writes nothing.
    /// </summary>
    public async Task<ActionApprovalState?> RequestApprovalAsync(Guid requestId,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> loadRoutes,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(loadRoutes);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var request = await ReadAgentActionRequestAsync(connection, transaction, requestId, cancellationToken);
        if (request is null)
            return null;
        var view = await ResolveRequestRoutesAsync(connection, transaction, request, loadRoutes, 0, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return view.Approval;
    }
}
