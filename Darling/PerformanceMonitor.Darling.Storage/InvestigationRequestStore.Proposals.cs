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
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The journal events of an action proposal (#7109). Only the store owner journals them: the proposal store writes each in the transaction
/// of its change, and the journal's owner-only trigger refuses every one of them from the other logins, so no other login can write a row
/// that says a person approved an action.
/// </summary>
public static class ProposalEvents
{
    public const string Created = "proposal_created";
    public const string FirstPress = "proposal_first_press";
    public const string Approved = "proposal_approved";
    public const string Run = "proposal_run";
    public const string Failed = "proposal_failed";
    public const string Unknown = "proposal_unknown";
    public const string Refused = "proposal_refused";
    public const string Expired = "proposal_expired";
    public const string Superseded = "proposal_superseded";
    public const string Voided = "proposal_voided";

    /// <summary>Every event above, for the journal trigger's owner-only list.</summary>
    public static IReadOnlyList<string> OwnerOnly { get; } =
        [Created, FirstPress, Approved, Run, Failed, Unknown, Refused, Expired, Superseded, Voided];

    /// <summary>The event that records a move to <paramref name="state"/>.</summary>
    public static string ForState(string state) => state switch
    {
        ProposalStates.Approved => Approved,
        ProposalStates.Run => Run,
        ProposalStates.Failed => Failed,
        ProposalStates.Unknown => Unknown,
        ProposalStates.Refused => Refused,
        ProposalStates.Expired => Expired,
        ProposalStates.Superseded => Superseded,
        ProposalStates.Voided => Voided,
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "A proposal moves to this state only by creation."),
    };
}

/// <summary>The states of an action proposal (#7109). <see cref="Pending"/> and <see cref="Approved"/> are live, the rest are final.</summary>
public static class ProposalStates
{
    public const string Pending = "pending";
    public const string Approved = "approved";
    public const string Run = "run";
    public const string Failed = "failed";
    public const string Unknown = "unknown";
    public const string Refused = "refused";
    public const string Expired = "expired";
    public const string Superseded = "superseded";
    public const string Voided = "voided";

    public static IReadOnlyList<string> All { get; } = [Pending, Approved, Run, Failed, Unknown, Refused, Expired, Superseded, Voided];

    /// <summary>The final states an approved proposal moves to: the write ran, failed, lost its reply, or its re-check refused it.</summary>
    public static IReadOnlyList<string> AfterApproved { get; } = [Run, Failed, Unknown, Refused];
}

/// <summary>The time rules of an action proposal (#7109, open rule 2 of #6980).</summary>
public static class ProposalRules
{
    /// <summary>A pending proposal expires this long after it was made. A constant: the two presses and the binding catch changed state.</summary>
    public const int ExpiryMinutes = 60;

    /// <summary>An approved proposal with no outcome this long after its approval becomes <c>unknown</c>: the service stopped between the
    /// approval and the end of the write.</summary>
    public const int ApprovedOutcomeMinutes = 10;
}

/// <summary>A proposal to store (#7109): the request, its claim number and agent, the tool, the server, the parsed target fields, the binding
/// and the agent's reason. No <c>agent_action</c> and no claim id.</summary>
public sealed record ActionProposalDraft(Guid RequestId, int ClaimNo, string Agent, string Tool, int ServerId, JsonObject Target, JsonObject Binding,
    string? Reason);

/// <summary>The answer to a create: the proposal's id, and whether this call made it (false: a live proposal for the same request, tool and
/// target already had it).</summary>
public sealed record ActionProposalCreated(Guid ProposalId, bool Created);

/// <summary>One stored proposal (#7109).</summary>
public sealed record ActionProposalRow(Guid ProposalId, Guid RequestId, int ClaimNo, string Agent, string Tool, int ServerId, string TargetJson,
    string BindingJson, string? Reason, DateTime ProposedAt, DateTime ExpiresAt, string State, string? FirstPressBy, DateTime? FirstPressAt,
    string? FirstPressFactsJson, string? DecidedBy, DateTime? DecidedAt, DateTime? FinishedAt, string? OutcomeJson);

public sealed partial class InvestigationRequestStore
{
    /* #7109: one live proposal for each request, tool and target (uq_agent_action_proposals_live). A retry meets the conflict and reads
       the live row's id, in the same transaction. The request's route and server come from the request row, so the journal row names
       them as the rest of the request's rows do. */
    internal const string ProposalInsertSql = @"
INSERT INTO config.agent_action_proposals
    (proposal_id, request_id, claim_no, agent, tool, server_id, target, target_key, binding, reason, proposed_at, expires_at, state)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, $10, $11, $12, 'pending')
ON CONFLICT (request_id, tool, target_key) WHERE state IN ('pending', 'approved') DO NOTHING
RETURNING proposal_id";

    internal const string ProposalLiveIdSql = @"
SELECT proposal_id
FROM config.agent_action_proposals
WHERE request_id = $1 AND tool = $2 AND target_key = $3 AND state IN ('pending', 'approved')";

    /* The journal row of a proposal change, written in the statement that makes the change: no row moved, no row journaled. */
    private const string ProposalJournalInsert = @"
INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail)
SELECT $now, m.request_id, r.route_id, m.server_id, $event, $actor, $reason,
       jsonb_build_object('proposal_id', m.proposal_id, 'tool', m.tool, 'state', m.state) || coalesce($detail::jsonb, '{}'::jsonb)
FROM moved AS m
JOIN config.investigation_requests AS r ON r.request_id = m.request_id
RETURNING journal_id";

    internal static string ProposalCreatedJournalSql { get; } = ProposalJournalSql(
        "SELECT p.proposal_id, p.request_id, p.server_id, p.tool, p.state FROM config.agent_action_proposals AS p WHERE p.proposal_id = $1");

    /* The first of a person's two presses. A later first press, by the same person or another, replaces it: the confirming press must
       match first_press_by (ProposalApproveSql). */
    internal static string ProposalFirstPressSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals
SET first_press_by = $2, first_press_at = $now, first_press_facts = $3::jsonb
WHERE proposal_id = $1 AND state = 'pending' AND expires_at > $now
RETURNING proposal_id, request_id, server_id, tool, state");

    /* The confirming press: pending, not expired, and the person of the first press. */
    internal static string ProposalApproveSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals
SET state = 'approved', decided_by = $2, decided_at = $now
WHERE proposal_id = $1 AND state = 'pending' AND expires_at > $now AND first_press_by = $2
RETURNING proposal_id, request_id, server_id, tool, state");

    /* Fresh facts differ from the binding, at either press. */
    internal static string ProposalSupersedeSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals
SET state = 'superseded', decided_by = $2, decided_at = $now, finished_at = $now, outcome = $3::jsonb
WHERE proposal_id = $1 AND state = 'pending'
RETURNING proposal_id, request_id, server_id, tool, state");

    /* The write's end: run, failed, unknown or refused, only from approved. */
    internal static string ProposalFinishSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals
SET state = $2, finished_at = $now, outcome = $3::jsonb
WHERE proposal_id = $1 AND state = 'approved'
RETURNING proposal_id, request_id, server_id, tool, state");

    /* The sweep's four moves (#7109). Each is guarded by the from-state, so a press that won the row first keeps it. */
    internal static string ProposalSweepVoidClosedSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals AS p
SET state = 'voided', finished_at = $now, outcome = '{""reason"":""request_closed""}'::jsonb
FROM config.investigation_requests AS r
WHERE r.request_id = p.request_id AND p.state = 'pending' AND r.state = 'closed'
RETURNING p.proposal_id, p.request_id, p.server_id, p.tool, p.state");

    /* A kill is about the session now, so an alert request's pending kill is void once its alert resolves. Other proposals stay until they
       expire: a bad plan can still be there after the alert clears. A follow-up is an alert request too, as the join reads it. */
    internal static string ProposalSweepVoidResolvedKillSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals AS p
SET state = 'voided', finished_at = $now, outcome = '{""reason"":""alert_resolved""}'::jsonb
FROM config.investigation_requests AS r
WHERE r.request_id = p.request_id
  AND p.state = 'pending'
  AND p.tool = 'kill_session'
  AND r.trigger_kind = 'alert'
  AND EXISTS (SELECT 1 FROM config.investigation_journal AS j WHERE j.request_id = r.request_id AND j.event = 'resolved')
RETURNING p.proposal_id, p.request_id, p.server_id, p.tool, p.state");

    internal static string ProposalSweepExpireSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals
SET state = 'expired', finished_at = $now
WHERE state = 'pending' AND expires_at <= $now
RETURNING proposal_id, request_id, server_id, tool, state");

    internal static string ProposalSweepUnknownSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals
SET state = 'unknown', finished_at = $now, outcome = '{""reason"":""no_outcome""}'::jsonb
WHERE state = 'approved' AND decided_at <= $now - make_interval(mins => " + ProposalRules.ApprovedOutcomeMinutes.ToString(CultureInfo.InvariantCulture) + @")
RETURNING proposal_id, request_id, server_id, tool, state");

    private const string ProposalSelect = @"
SELECT proposal_id, request_id, claim_no, agent, tool, server_id, target::text, binding::text, reason, proposed_at, expires_at, state,
       first_press_by, first_press_at, first_press_facts::text, decided_by, decided_at, finished_at, outcome::text
FROM config.agent_action_proposals";

    /// <summary>
    /// Wraps a change that RETURNs (proposal_id, request_id, server_id, tool, state) with its journal insert. The named slots become
    /// positions after the change's own: <c>$now</c>, <c>$event</c>, <c>$actor</c>, <c>$reason</c>, <c>$detail</c> are always the last five.
    /// </summary>
    private static string ProposalJournalSql(string change) => "WITH moved AS (" + change + ")" + ProposalJournalInsert;

    /// <summary>
    /// Stores a proposal (#7109), or answers the id of the live one (pending or approved) for the same request, tool and parsed target, so a
    /// retry adds nothing. A new plan id is a new target, so a new proposal. Expires <see cref="ProposalRules.ExpiryMinutes"/> after now.
    /// Journals <see cref="ProposalEvents.Created"/> with the agent as the actor, in the same transaction. Owner connection only.
    /// </summary>
    public async Task<ActionProposalCreated> CreateProposalAsync(ActionProposalDraft draft, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var created = await CreateProposalAsync(connection, transaction, draft, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return created;
    }

    /// <summary>The create, inside the caller's owner transaction.</summary>
    public async Task<ActionProposalCreated> CreateProposalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, ActionProposalDraft draft,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(draft);
        var now = Now();
        var target = CanonicalJson(draft.Target);
        var key = TargetKey(target);
        var binding = CanonicalJson(draft.Binding);

        /* Twice at most: the live row a conflict met can end (expire, finish) between the insert and the read; the second insert then wins. */
        for (var attempt = 0; attempt < 2; attempt++)
        {
            object? inserted;
            await using (var insert = Command(connection, transaction, ProposalInsertSql,
                P(Guid.NewGuid()), P(draft.RequestId), P((short)draft.ClaimNo), P(draft.Agent), P(draft.Tool), P(draft.ServerId), P(target), P(key),
                P(binding), PNullable(draft.Reason), P(now), P(now.AddMinutes(ProposalRules.ExpiryMinutes))))
            {
                inserted = await insert.ExecuteScalarAsync(cancellationToken);
            }

            if (inserted is Guid id)
            {
                await using var journal = ProposalCommand(connection, transaction, ProposalCreatedJournalSql, now, ProposalEvents.Created, draft.Agent,
                    null, null, P(id));
                await journal.ExecuteScalarAsync(cancellationToken);
                return new ActionProposalCreated(id, true);
            }

            await using var live = Command(connection, transaction, ProposalLiveIdSql, P(draft.RequestId), P(draft.Tool), P(key));
            if (await live.ExecuteScalarAsync(cancellationToken) is Guid existing)
            {
                return new ActionProposalCreated(existing, false);
            }
        }

        throw new InvalidOperationException("The proposal could neither be stored nor found.");
    }

    /// <summary>
    /// Records the first of a person's two presses (#7109): who (<c>person:&lt;name&gt;</c>), when, and the facts the fresh dry run showed. Only
    /// a pending proposal before its expiry. A later first press replaces it. Journals <see cref="ProposalEvents.FirstPress"/> with the person
    /// as the actor and <paramref name="detail"/> (the surface and identity used). False when the proposal is not pending.
    /// </summary>
    public Task<bool> RecordFirstPressAsync(Guid proposalId, string person, JsonObject facts, JsonObject? detail = null,
        CancellationToken cancellationToken = default) =>
        InOwnTransactionAsync((c, t) => RecordFirstPressAsync(c, t, proposalId, person, facts, detail, cancellationToken), cancellationToken);

    public Task<bool> RecordFirstPressAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid proposalId, string person, JsonObject facts,
        JsonObject? detail = null, CancellationToken cancellationToken = default) =>
        ProposalChangeAsync(connection, transaction, ProposalFirstPressSql, ProposalEvents.FirstPress, person, null, detail, cancellationToken,
            P(proposalId), P(person), P(CanonicalJson(facts)));

    /// <summary>
    /// The confirming press (#7109): pending to approved, only before the expiry and only by the person of the first press. Journals
    /// <see cref="ProposalEvents.Approved"/>. False when any of those does not hold, so a double press or a replay changes nothing.
    /// </summary>
    public Task<bool> ApproveProposalAsync(Guid proposalId, string person, JsonObject? detail = null, CancellationToken cancellationToken = default) =>
        InOwnTransactionAsync((c, t) => ApproveProposalAsync(c, t, proposalId, person, detail, cancellationToken), cancellationToken);

    public Task<bool> ApproveProposalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid proposalId, string person,
        JsonObject? detail = null, CancellationToken cancellationToken = default) =>
        ProposalChangeAsync(connection, transaction, ProposalApproveSql, ProposalEvents.Approved, person, null, detail, cancellationToken,
            P(proposalId), P(person));

    /// <summary>
    /// Pending to superseded (#7109): the fresh facts at a press differ from the binding. <paramref name="outcome"/> says what changed.
    /// Journals <see cref="ProposalEvents.Superseded"/> with <paramref name="actor"/>, the person who pressed.
    /// </summary>
    public Task<bool> SupersedeProposalAsync(Guid proposalId, string actor, JsonObject outcome, CancellationToken cancellationToken = default) =>
        InOwnTransactionAsync((c, t) => SupersedeProposalAsync(c, t, proposalId, actor, outcome, cancellationToken), cancellationToken);

    public Task<bool> SupersedeProposalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid proposalId, string actor, JsonObject outcome,
        CancellationToken cancellationToken = default)
    {
        var outcomeJson = CanonicalJson(outcome);
        return ProposalChangeAsync(connection, transaction, ProposalSupersedeSql, ProposalEvents.Superseded, actor, null, outcome, cancellationToken,
            P(proposalId), P(actor), P(outcomeJson));
    }

    /// <summary>
    /// Approved to one of <see cref="ProposalStates.AfterApproved"/> (#7109): the write ran, failed, lost its reply, or its re-check refused it.
    /// Journals the state's event with <paramref name="actor"/> and the outcome. False when the proposal is not approved.
    /// </summary>
    public Task<bool> FinishProposalAsync(Guid proposalId, string state, string actor, JsonObject? outcome, CancellationToken cancellationToken = default) =>
        InOwnTransactionAsync((c, t) => FinishProposalAsync(c, t, proposalId, state, actor, outcome, cancellationToken), cancellationToken);

    public Task<bool> FinishProposalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid proposalId, string state, string actor,
        JsonObject? outcome, CancellationToken cancellationToken = default)
    {
        if (!ProposalStates.AfterApproved.Contains(state, StringComparer.Ordinal))
        {
            throw new ArgumentOutOfRangeException(nameof(state), state, "An approved proposal ends as run, failed, unknown or refused.");
        }

        var outcomeJson = outcome is null ? null : CanonicalJson(outcome);
        return ProposalChangeAsync(connection, transaction, ProposalFinishSql, ProposalEvents.ForState(state), actor, null, outcome, cancellationToken,
            P(proposalId), P(state), PNullable(outcomeJson));
    }

    /// <summary>One proposal by id, or null.</summary>
    public async Task<ActionProposalRow?> GetProposalAsync(Guid proposalId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, ProposalSelect + " WHERE proposal_id = $1", P(proposalId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadProposal(reader) : null;
    }

    /// <summary>
    /// Proposals, newest first: of one request when <paramref name="requestId"/> is set, in one state when <paramref name="state"/> is set, on one
    /// server when <paramref name="serverId"/> is set. All three filters are part of the query, so the limit counts the matching rows only.
    /// </summary>
    public Task<IReadOnlyList<ActionProposalRow>> ListProposalsAsync(Guid? requestId, string? state, int limit, CancellationToken cancellationToken = default) =>
        ListProposalsAsync(requestId, state, limit, null, cancellationToken);

    /// <inheritdoc cref="ListProposalsAsync(Guid?, string?, int, CancellationToken)"/>
    public async Task<IReadOnlyList<ActionProposalRow>> ListProposalsAsync(Guid? requestId, string? state, int limit, int? serverId,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<ActionProposalRow>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null,
            ProposalSelect + " WHERE ($1::uuid IS NULL OR request_id = $1) AND ($2::text IS NULL OR state = $2) AND ($4::int IS NULL OR server_id = $4) ORDER BY proposed_at DESC, proposal_id LIMIT $3",
            new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid }, PNullable(state), P(Math.Clamp(limit, 1, 500)),
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadProposal(reader));
        }

        return rows;
    }

    /// <summary>
    /// The proposal sweep (#7109), in the caller's transaction: pending to voided on a closed request; an alert request's pending
    /// <c>kill_session</c> to voided once its alert resolved; pending to expired past its expiry; approved to unknown
    /// <see cref="ProposalRules.ApprovedOutcomeMinutes"/> after its approval with no outcome. Each move journals its event as the service.
    /// Returns the proposals moved.
    /// </summary>
    private async Task<int> SweepProposalsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now, CancellationToken cancellationToken)
    {
        var moved = 0;
        foreach (var (sql, journalEvent) in new[]
                 {
                     (ProposalSweepVoidClosedSql, ProposalEvents.Voided),
                     (ProposalSweepVoidResolvedKillSql, ProposalEvents.Voided),
                     (ProposalSweepExpireSql, ProposalEvents.Expired),
                     (ProposalSweepUnknownSql, ProposalEvents.Unknown),
                 })
        {
            await using var command = ProposalCommand(connection, transaction, sql, now, journalEvent, "service", null, null);
            moved += await command.ExecuteNonQueryAsync(cancellationToken);
        }

        return moved;
    }

    private async Task<bool> ProposalChangeAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, string journalEvent, string actor,
        string? reason, JsonObject? detail, CancellationToken cancellationToken, params NpgsqlParameter[] leading)
    {
        await using var command = ProposalCommand(connection, transaction, sql, Now(), journalEvent, actor, reason, detail, leading);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>Numbers the five named slots after the change's own parameters.</summary>
    private NpgsqlCommand ProposalCommand(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, DateTime now, string journalEvent,
        string actor, string? reason, JsonObject? detail, params NpgsqlParameter[] leading)
    {
        var n = leading.Length;
        var text = sql
            .Replace("$now", "$" + (n + 1).ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("$event", "$" + (n + 2).ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("$actor", "$" + (n + 3).ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("$reason", "$" + (n + 4).ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("$detail", "$" + (n + 5).ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
        return Command(connection, transaction, text,
            [.. leading, P(now), P(journalEvent), P(actor), PNullable(reason), PNullable(detail is null ? null : CanonicalJson(detail))]);
    }

    private async Task<bool> InOwnTransactionAsync(Func<NpgsqlConnection, NpgsqlTransaction, Task<bool>> change, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var changed = await change(connection, transaction);
        await transaction.CommitAsync(cancellationToken);
        return changed;
    }

    private static ActionProposalRow ReadProposal(NpgsqlDataReader reader)
    {
        static string? Text(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);
        static DateTime? Time(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : DateTime.SpecifyKind(r.GetDateTime(i), DateTimeKind.Utc);
        return new ActionProposalRow(reader.GetGuid(0), reader.GetGuid(1), reader.GetInt16(2), reader.GetString(3), reader.GetString(4),
            reader.GetInt32(5), reader.GetString(6), reader.GetString(7), Text(reader, 8), Time(reader, 9)!.Value, Time(reader, 10)!.Value,
            reader.GetString(11), Text(reader, 12), Time(reader, 13), Text(reader, 14), Text(reader, 15), Time(reader, 16), Time(reader, 17),
            Text(reader, 18));
    }

    /// <summary>The JSON text of <paramref name="node"/> with every object's keys in ordinal order, so the same target always has the same
    /// text and so the same key.</summary>
    internal static string CanonicalJson(JsonNode? node) => Canonical(node)?.ToJsonString() ?? "null";

    private static JsonNode? Canonical(JsonNode? node) => node switch
    {
        JsonObject o => new JsonObject(o.OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => KeyValuePair.Create(p.Key, Canonical(p.Value)))),
        JsonArray a => new JsonArray(a.Select(Canonical).ToArray()),
        null => null,
        _ => JsonNode.Parse(node.ToJsonString()),
    };

    /// <summary>The SHA-256 of the canonical target text, lower-case hex: the target's part of the live proposal's unique key.</summary>
    internal static string TargetKey(string canonicalTarget) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(canonicalTarget)));
}
