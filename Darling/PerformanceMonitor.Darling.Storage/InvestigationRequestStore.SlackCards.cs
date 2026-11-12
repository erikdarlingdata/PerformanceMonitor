/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The states of a Slack card (#7658): <see cref="Live"/> while its buttons act, <see cref="Decided"/> once a press ended it.</summary>
public static class SlackCardStates
{
    public const string Live = "live";
    public const string Decided = "decided";
}

/// <summary>
/// #7658: one card the bot posted with buttons (V253 <c>config.investigation_slack_posts</c>): the request and post key it was posted under,
/// the channel and <c>ts</c> Slack gave it, the kind of record its buttons act on (<c>proposal</c>, <c>change</c>, <c>report</c>, <c>note</c>),
/// that record's id as text, the server the request is about, and the card's state. A press names a card by its channel and ts, and the
/// service acts only on a card it posted.
/// </summary>
public sealed record SlackCard(Guid RequestId, string PostKey, string ChannelId, string MessageTs, string Kind, string RecordId, int ServerId,
    string State = SlackCardStates.Live, string? DecidedBy = null, DateTime? DecidedAtUtc = null);

/// <summary>#7658: where the bot's cards live. The poster saves one after a delivered reply that carries buttons; the press handler finds it.</summary>
public interface ISlackCardStore
{
    /// <summary>Saves a delivered card. A card already saved under the same request and post key, or the same channel and ts, stays: the first writer wins.</summary>
    Task SaveCardAsync(SlackCard card, CancellationToken cancellationToken);

    /// <summary>The card at <paramref name="channelId"/> and <paramref name="messageTs"/>, or null when this store posted no card there.</summary>
    Task<SlackCard?> FindCardAsync(string channelId, string messageTs, CancellationToken cancellationToken);

    /// <summary>Marks a live card decided by <paramref name="decidedBy"/>. False when the card is gone or already decided.</summary>
    Task<bool> MarkCardDecidedAsync(string channelId, string messageTs, string decidedBy, CancellationToken cancellationToken);
}

/// <summary>#7658: the person behind a Slack identity: the person, the identity row the press used, the display name, and whether the person is enabled.</summary>
public sealed record SlackPerson(long PersonId, long IdentityId, string DisplayName, bool Enabled);

/// <summary>#7658: the owner reads and writes a Slack press needs besides the record's own command: who pressed, what they hold, and the press's journal row.</summary>
public interface ISlackPressStore
{
    /// <summary>The person mapped to the Slack identity <paramref name="teamId"/>:<paramref name="userId"/>, or null when nobody is.</summary>
    Task<SlackPerson?> FindSlackPersonAsync(string teamId, string userId, CancellationToken cancellationToken);

    /// <summary>Whether an enabled <paramref name="personId"/> holds <paramref name="capability"/> for <paramref name="serverId"/>: a fleet grant, a grant
    /// on the server, or a grant on one of its tags. With no server only a fleet grant counts.</summary>
    Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken cancellationToken);

    /// <summary>Journals one press (<see cref="InvestigationRequestStore.SlackPressed"/>) with the actor, the action and <paramref name="detail"/>.</summary>
    Task JournalSlackPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken cancellationToken);
}

public sealed partial class InvestigationRequestStore : ISlackCardStore, ISlackPressStore
{
    /// <summary>#7658: the journal row of one Slack button press: the person (or the unmapped identity) as the actor, the action as the reason,
    /// and the identity, record and outcome in the detail. Owner-only, as the proposal events are.</summary>
    public const string SlackPressed = "slack_press";

    /// <summary>The post key of a pending note's card: <c>note:&lt;note_id&gt;</c>.</summary>
    public const string NotePostKeyPrefix = "note:";

    /// <summary>The event the thread pass gives a pending note's card. Not a journal kind: the row is the note itself.</summary>
    public const string NotePending = "note_pending";

    /* First writer wins on both keys: two services delivering the same reply both saved only what Slack took, and a ts is Slack's own. */
    internal const string SaveSlackCardSql = @"
INSERT INTO config.investigation_slack_posts (request_id, post_key, channel_id, message_ts, kind, record_id, server_id, state, posted_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'live', now() AT TIME ZONE 'UTC')
ON CONFLICT DO NOTHING";

    internal const string FindSlackCardSql = @"
SELECT request_id, post_key, kind, record_id, server_id, state, decided_by, decided_at
FROM config.investigation_slack_posts
WHERE channel_id = $1 AND message_ts = $2";

    internal const string DecideSlackCardSql = @"
UPDATE config.investigation_slack_posts
SET state = 'decided', decided_by = $3, decided_at = now() AT TIME ZONE 'UTC'
WHERE channel_id = $1 AND message_ts = $2 AND state = 'live'";

    /// <summary>The Slack identity as <c>config.person_identity</c> stores it: the team id and the user id joined by a colon (#7107).</summary>
    public static string SlackExternalId(string teamId, string userId) => (teamId ?? "").Trim() + ":" + (userId ?? "").Trim();

    internal const string FindSlackPersonSql = @"
SELECT p.person_id, i.identity_id, p.display_name, p.enabled
FROM config.person_identity AS i
JOIN config.person AS p ON p.person_id = i.person_id
WHERE i.kind = 'slack' AND i.external_id = $1";

    /* The same scope rule as the approver check (ApproverCheckSql): the fleet, the server, or one of its tags. A capability without kinds. */
    internal const string PersonGrantSql = @"
SELECT EXISTS (
    SELECT 1
    FROM config.person_grant AS g
    JOIN config.person AS p ON p.person_id = g.person_id
    WHERE g.person_id = $1
      AND p.enabled
      AND g.capability = $2
      AND (   (g.scope_server_id IS NULL AND g.scope_tag_id IS NULL)
           OR ($3::integer IS NOT NULL AND g.scope_server_id = $3)
           OR ($3::integer IS NOT NULL AND g.scope_tag_id IN (SELECT m.tag_id FROM config.server_tag_map AS m WHERE m.server_id = $3))))";

    /* A person's refusal of a pending proposal (#7658): pending to refused, with who and why, journaled as proposal_refused with the person as the actor. */
    internal static string ProposalRefuseByPersonSql { get; } = ProposalJournalSql(@"
UPDATE config.agent_action_proposals
SET state = 'refused', decided_by = $2, decided_at = $now, finished_at = $now, outcome = $3::jsonb
WHERE proposal_id = $1 AND state = 'pending'
RETURNING proposal_id, request_id, server_id, tool, state");

    /// <inheritdoc />
    public async Task SaveCardAsync(SlackCard card, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(card);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, SaveSlackCardSql,
            P(card.RequestId), P(card.PostKey), P(card.ChannelId), P(card.MessageTs), P(card.Kind), P(card.RecordId ?? ""), P(card.ServerId));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<SlackCard?> FindCardAsync(string channelId, string messageTs, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(channelId) || string.IsNullOrWhiteSpace(messageTs))
        {
            return null;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, FindSlackCardSql, P(channelId.Trim()), P(messageTs.Trim()));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new SlackCard(reader.GetGuid(0), reader.GetString(1), channelId.Trim(), messageTs.Trim(), reader.GetString(2), reader.GetString(3),
            reader.GetInt32(4), reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc));
    }

    /// <inheritdoc />
    public async Task<bool> MarkCardDecidedAsync(string channelId, string messageTs, string decidedBy, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channelId);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageTs);
        ArgumentException.ThrowIfNullOrWhiteSpace(decidedBy);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, DecideSlackCardSql, P(channelId.Trim()), P(messageTs.Trim()), P(decidedBy));
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    /// <inheritdoc />
    public async Task<SlackPerson?> FindSlackPersonAsync(string teamId, string userId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(teamId) || string.IsNullOrWhiteSpace(userId))
        {
            return null;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, FindSlackPersonSql, P(SlackExternalId(teamId, userId)));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new SlackPerson(reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2), reader.GetBoolean(3))
            : null;
    }

    /// <inheritdoc />
    public async Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(capability);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, PersonGrantSql, P(personId), PText(capability),
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <inheritdoc />
    public async Task JournalSlackPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        ArgumentException.ThrowIfNullOrWhiteSpace(action);
        ArgumentNullException.ThrowIfNull(detail);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await JournalAsync(connection, transaction, Now(), requestId, null, serverId, SlackPressed, actor, action, detail.ToJsonString(), null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// A person's refusal of a proposal (#7658): the checks of a press (pending, not expired, the shared conditions and the approver's grant), then
    /// pending to refused with the person as <c>decided_by</c>, journaled as <see cref="ProposalEvents.Refused"/> with the person as the actor. The
    /// refusal, when the checks fail; null when the proposal was refused.
    /// </summary>
    public async Task<AgentActionRefusal?> RefuseProposalAsync(Guid proposalId, ProposalApprover approver, ApprovedActionHooks hooks, string? reason,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(approver);
        ArgumentNullException.ThrowIfNull(hooks);
        var now = Now();
        await using var connection = await AgentActionSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var (refusal, _, _) = await PressChecksAsync(connection, transaction, proposalId, approver, hooks, forUpdate: true, now, cancellationToken);
        if (refusal is not null)
        {
            await transaction.RollbackAsync(cancellationToken);
            return refusal;
        }

        if (!await RefuseProposalRowAsync(connection, transaction, proposalId, approver, reason, cancellationToken))
        {
            await transaction.RollbackAsync(cancellationToken);
            return new AgentActionRefusal(ProposalPressCodes.NotPending, "This proposal is no longer waiting for a person. Nothing was run.");
        }

        await transaction.CommitAsync(cancellationToken);
        return null;
    }

    /// <summary>The refusal's one statement, in the caller's owner transaction: pending to refused by <paramref name="approver"/>. False when not pending.</summary>
    public Task<bool> RefuseProposalRowAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid proposalId, ProposalApprover approver, string? reason,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(approver);
        var outcome = new JsonObject { ["reason"] = "refused_by_person", ["message"] = reason };
        var detail = new JsonObject { ["approver"] = approver.ToJson() };
        return ProposalChangeAsync(connection, transaction, ProposalRefuseByPersonSql, ProposalEvents.Refused, approver.Key, reason, detail, cancellationToken,
            P(proposalId), P(approver.Key), P(CanonicalJson(outcome)));
    }
}
