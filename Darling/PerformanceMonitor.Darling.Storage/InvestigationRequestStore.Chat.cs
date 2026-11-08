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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>#7659: the limits of the chat requests (design section 6), constants in the first build.</summary>
public static class SlackChatRules
{
    /// <summary>A mention on a thread whose first request opened longer ago than this opens nothing.</summary>
    public static readonly TimeSpan ThreadAgeLimit = TimeSpan.FromDays(7);

    /// <summary>The most chat requests one thread opens in an hour. Over it, the message waits.</summary>
    public const int PerThreadPerHour = 6;

    /// <summary>How long an unclaimed chat request stands before the bot says the agent may be offline.</summary>
    public static readonly TimeSpan OfflineAfter = TimeSpan.FromMinutes(5);

    /// <summary>The most characters of a person's message kept, and of an agent's message posted.</summary>
    public const int TextLimit = 4000;

    /// <summary>The most messages one thread holds waiting for the agent. Past it a mention is refused with one sentence.</summary>
    public const int WaitingPerThread = 20;

    /// <summary>The most messages one person holds waiting in one thread. Past it their mention is refused with one sentence.</summary>
    public const int WaitingPerPerson = 5;

    /// <summary>The most characters of people's text one claim answer carries in <c>person_requests</c>: the newest messages are kept and the
    /// answer says how many were left out.</summary>
    public const int PersonRequestsCharLimit = 16000;

    /// <summary>The most message rows one claim answer reads before the character ceiling.</summary>
    public const int PersonRequestsRowLimit = 50;

    /// <summary>PURE: <paramref name="text"/> held to <paramref name="limit"/> UTF-16 units, never cut between the two halves of a surrogate pair.</summary>
    public static string Bound(string? text, int limit = TextLimit)
    {
        var value = text ?? "";
        if (value.Length <= limit)
        {
            return value;
        }

        var cut = limit;
        if (cut > 0 && char.IsHighSurrogate(value[cut - 1]))
        {
            cut--;
        }

        return value[..cut];
    }
}

/// <summary>#7659: the request thread a mention landed in, as this store knows it: the thread's first request (the one whose parent post opened
/// the thread), its route and server, the channel and parent ts, and when that first request opened.</summary>
public sealed record SlackChatThread(Guid ThreadRequestId, int RouteId, int ServerId, string ChannelId, string ThreadTs, DateTime OpenedAtUtc);

/// <summary>#7659: the newest request of a thread: the thread's first request, a follow-up that replied in it, or a chat request linked to it.</summary>
public sealed record SlackChatNewest(Guid RequestId, string State, string TriggerKind, DateTime OpenedAtUtc);

/// <summary>#7659: the states of a person's message: <see cref="Waiting"/> for a request to end, <see cref="Joined"/> into an unclaimed chat
/// request, <see cref="Opened"/> as part of a chat request's open, <see cref="Refused"/> by the sweep (the thread past its age limit, or the
/// asker's live level 0 by the time the request ended) and never handed to a chat request.</summary>
public static class SlackPersonMessageStates
{
    public const string Waiting = "waiting";
    public const string Joined = "joined";
    public const string Opened = "opened";
    public const string Refused = "refused";
}

/// <summary>
/// #7659: one person's message (V254 <c>config.investigation_person_messages</c>): an audit row, not a journal event. Its natural key is the
/// channel and ts; <see cref="ThreadRequestId"/> is the thread's first request; <see cref="RequestId"/> the chat request it joined or opened,
/// null while it waits; <see cref="Text"/> the message with every mention of the bot removed, at most <see cref="SlackChatRules.TextLimit"/>
/// characters.
/// </summary>
public sealed record SlackPersonMessage(string ChannelId, string Ts, string ThreadTs, Guid ThreadRequestId, long? PersonId, string Identity, string Text,
    string State = SlackPersonMessageStates.Waiting, Guid? RequestId = null, DateTime? ReceivedAtUtc = null);

/// <summary>#7659: how saving a person's message ended.</summary>
public enum SlackPersonMessageSaved
{
    /// <summary>Saved as waiting.</summary>
    Saved,

    /// <summary>A message with that channel and ts is already saved: the same mention arrived again.</summary>
    Duplicate,

    /// <summary>The thread already holds <see cref="SlackChatRules.WaitingPerThread"/> waiting messages.</summary>
    ThreadFull,

    /// <summary>The person already holds <see cref="SlackChatRules.WaitingPerPerson"/> waiting messages in the thread.</summary>
    PersonFull,
}

/// <summary>#7659: a person who asked in a chat request: the person, the Slack identity used, and the request level their grants give for the
/// request's server (1 <c>ask</c>, 2 <c>request_level_2</c>, 3 <c>request_level_3</c>).</summary>
public sealed record SlackAsker(long PersonId, string Identity, int RequestLevel);

/// <summary>#7659: how an attempt to open a chat request ended.</summary>
public enum SlackChatOpenOutcome
{
    /// <summary>Opened; <see cref="SlackChatOpenResult.RequestId"/> names it.</summary>
    Opened,

    /// <summary>The thread opened <see cref="SlackChatRules.PerThreadPerHour"/> chat requests this hour: the messages wait.</summary>
    ThreadCap,

    /// <summary>The route opened its <c>agent_max_per_hour</c> chat requests this hour: the messages wait.</summary>
    RouteCap,

    /// <summary>The parent request is gone.</summary>
    ParentGone,

    /// <summary>None of the messages handed over was still waiting: another open or join took them first, so nothing opened.</summary>
    NothingWaiting,
}

/// <summary>#7659: the answer of a chat request open.</summary>
public sealed record SlackChatOpenResult(SlackChatOpenOutcome Outcome, Guid? RequestId);

/// <summary>#7659: the chat requests opened in the last hour: by this thread, and by this route.</summary>
public sealed record SlackChatCounts(long ThreadLastHour, long RouteLastHour);

/// <summary>#7659: a thread with waiting messages, for the sweep; <see cref="OpenedAtUtc"/> is when its first request opened, for the age limit.</summary>
public sealed record SlackWaitingThread(Guid ThreadRequestId, int RouteId, int ServerId, string ChannelId, string ThreadTs, int Messages, DateTime OpenedAtUtc);

/// <summary>#7659: a chat request no agent has claimed, with its thread, for the offline post.</summary>
public sealed record SlackUnclaimedChat(Guid RequestId, Guid ThreadRequestId, int RouteId, int ServerId, string ChannelId, string ThreadTs, DateTime OpenedAtUtc);

/// <summary>#7659: one entry of the claim answer's <c>person_requests</c>: the person's name, their grants for the request's server, and their text.</summary>
public sealed record PersonRequest(string Name, IReadOnlyList<string> Grants, string Text);

/// <summary>#7659: the <c>person_requests</c> of one claim answer: the newest messages inside the character ceiling, oldest first, and how many older
/// ones were left out.</summary>
public sealed record PersonRequestsPage(IReadOnlyList<PersonRequest> Items, int Omitted);

/// <summary>#7659: how <see cref="InvestigationRequestStore.PostAgentMessageAsync"/> ended.</summary>
public enum AgentMessageOutcome
{
    Posted,
    NotFound,
    LeaseLost,
    NotOpen,
    JournalCapped,
}

/// <summary>#7659: the answer of an agent's message: the outcome and the journal id when posted.</summary>
public sealed record AgentMessageResult(AgentMessageOutcome Outcome, long? JournalId);

/// <summary>
/// #7659: the owner reads and writes a mention needs, behind one seam. The store implements them on the owner connection; a test stands a fake in.
/// </summary>
public interface ISlackChatStore
{
    /// <summary>The request thread at <paramref name="channelId"/> and <paramref name="threadTs"/>, or null when this store opened no thread there.</summary>
    Task<SlackChatThread?> FindThreadAsync(string channelId, string threadTs, CancellationToken cancellationToken);

    /// <summary>The newest request of <paramref name="thread"/>: its first request, a follow-up in it, or a chat request linked to it.</summary>
    Task<SlackChatNewest?> NewestRequestAsync(SlackChatThread thread, CancellationToken cancellationToken);

    /// <summary>The person mapped to the Slack identity, or null.</summary>
    Task<SlackPerson?> FindSlackPersonAsync(string teamId, string userId, CancellationToken cancellationToken);

    /// <summary>The request level an enabled person's grants give for <paramref name="serverId"/>: 3, 2, 1, or 0 with no <c>ask</c>.</summary>
    Task<int> RequestLevelAsync(long personId, int serverId, CancellationToken cancellationToken);

    /// <summary>Saves a message as waiting, under the thread's and the person's waiting caps; a message with that channel and ts already saved is a duplicate.</summary>
    Task<SlackPersonMessageSaved> SaveMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken);

    /// <summary>Joins a saved message and its asker into the unclaimed chat request <paramref name="requestId"/>, lowering its stamp to the asker's
    /// level. False when that request is no longer an open chat request: the message stays waiting.</summary>
    Task<bool> JoinChatRequestAsync(Guid requestId, SlackAsker asker, SlackPersonMessage message, CancellationToken cancellationToken);

    /// <summary>The chat requests opened in the last hour by the thread and by the route.</summary>
    Task<SlackChatCounts> ChatCountsAsync(Guid threadRequestId, int routeId, DateTime nowUtc, CancellationToken cancellationToken);

    /// <summary>Opens one chat request linked to <paramref name="thread"/>, copying <paramref name="parentRequestId"/>'s alert rows, with
    /// <paramref name="askers"/> and <paramref name="messages"/>, under the thread cap and <paramref name="routeMaxPerHour"/>.</summary>
    Task<SlackChatOpenResult> OpenChatRequestAsync(SlackChatThread thread, Guid parentRequestId, IReadOnlyList<SlackAsker> askers,
        IReadOnlyList<SlackPersonMessage> messages, int routeMaxPerHour, DateTime nowUtc, CancellationToken cancellationToken);

    /// <summary>The no-text journal row of a mention that opened nothing (<see cref="InvestigationRequestStore.AskerJournalEvent"/>), once per <paramref name="onceKey"/>.</summary>
    Task JournalPersonMessageAsync(Guid? requestId, int? routeId, int? serverId, string actor, string outcome, JsonObject detail, string onceKey, CancellationToken cancellationToken);

    /// <summary>Writes the once mark of a bot answer and says whether this call wrote it. The caller posts only on true.</summary>
    Task<bool> MarkChatAnswerOnceAsync(Guid? requestId, int? routeId, int? serverId, string answerKey, string onceKey, CancellationToken cancellationToken);

    /// <summary>The threads with waiting messages, oldest message first.</summary>
    Task<IReadOnlyList<SlackWaitingThread>> WaitingThreadsAsync(int limit, CancellationToken cancellationToken);

    /// <summary>The waiting messages of one thread, oldest first.</summary>
    Task<IReadOnlyList<SlackPersonMessage>> WaitingMessagesAsync(Guid threadRequestId, CancellationToken cancellationToken);

    /// <summary>Marks a waiting message <see cref="SlackPersonMessageStates.Refused"/>: it is handed to no chat request. A message no longer waiting is left as it is.</summary>
    Task RefuseMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken);

    /// <summary>The open chat requests no agent has claimed that opened at or before <paramref name="openedBeforeUtc"/> and have no offline post yet.</summary>
    Task<IReadOnlyList<SlackUnclaimedChat>> UnclaimedChatRequestsAsync(DateTime openedBeforeUtc, CancellationToken cancellationToken);
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>#7659: the journal row of an agent's message to the thread. Owner-only: only <see cref="PostAgentMessageAsync"/>, which checks the
    /// claim, writes it, so no other login can post as the agent.</summary>
    public const string AgentMessage = "agent_message";

    /// <summary>The post key of a bot answer in a thread, by its kind: <c>chat_answer:&lt;kind&gt;</c>.</summary>
    public static string ChatAnswerKey(string kind) => "chat_answer:" + kind;

    /// <summary>The once key of a person's message journal row: one per channel and ts, so the same mention arriving twice journals once.</summary>
    public static string PersonMessageOnceKey(string channelId, string ts) => "person_message:" + channelId + ":" + ts;

    /* The thread's first request: of the requests that share the channel and parent ts (a follow-up and a chat request save the thread as their own
       once their request post replied in it), the one that opened first. The reverse index on (channel_id, thread_ts) serves the lookup. */
    internal const string FindChatThreadSql = @"
SELECT t.request_id, r.route_id, r.server_id, r.opened_at
FROM config.investigation_request_threads AS t
JOIN config.investigation_requests AS r ON r.request_id = t.request_id
WHERE t.channel_id = $1 AND t.thread_ts = $2
ORDER BY r.opened_at, t.posted_at, t.request_id
LIMIT 1";

    /* The newest request of the thread: the first request itself, any request whose thread row is the same channel and ts (a follow-up chain
       that replied in it), and any chat request linked to the first request, which has no thread row until its own request post delivers. */
    internal const string NewestThreadRequestSql = @"
SELECT r.request_id, r.state, r.trigger_kind, r.opened_at
FROM config.investigation_requests AS r
WHERE r.practice_of IS NULL
  AND (r.request_id = $1
       OR r.chat_thread_of = $1
       OR r.request_id IN (SELECT t.request_id FROM config.investigation_request_threads AS t WHERE t.channel_id = $2 AND t.thread_ts = $3))
ORDER BY r.opened_at DESC, r.request_id DESC
LIMIT 1";

    /* The highest request level among an enabled person's grants that reach the server: the fleet, the server, or one of its tags (the scope rule
       of PersonGrantSql). A Level 3 grant includes Level 2, as a Level 3 route does. */
    internal const string RequestLevelSql = @"
SELECT COALESCE(max(CASE g.capability WHEN 'request_level_3' THEN 3 WHEN 'request_level_2' THEN 2 WHEN 'ask' THEN 1 ELSE 0 END), 0)
FROM config.person_grant AS g
JOIN config.person AS p ON p.person_id = g.person_id
WHERE g.person_id = $1
  AND p.enabled
  AND (   (g.scope_server_id IS NULL AND g.scope_tag_id IS NULL)
       OR g.scope_server_id = $2
       OR g.scope_tag_id IN (SELECT m.tag_id FROM config.server_tag_map AS m WHERE m.server_id = $2))";

    /* The waiting rows of a thread, and of one person in it, read before a save: the caps hold a thread to WaitingPerThread and a person to WaitingPerPerson. */
    internal const string WaitingCountsSql = @"
SELECT count(*), count(*) FILTER (WHERE person_id = $2)
FROM config.investigation_person_messages
WHERE thread_request_id = $1 AND state = 'waiting'";

    /* The message's natural key is the channel and ts: the same mention arriving as app_mention and as message, or on both connections, inserts once. */
    internal const string SavePersonMessageSql = @"
INSERT INTO config.investigation_person_messages (channel_id, ts, thread_ts, thread_request_id, person_id, identity, message_text, state, received_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'waiting', $8)
ON CONFLICT (channel_id, ts) DO NOTHING";

    /* A join lowers an unclaimed chat request to the joining asker's level, as JoinSql lowers an alert request's level. No row: it moved on. */
    internal const string JoinChatRequestSql = @"
UPDATE config.investigation_requests
SET autonomy_level = least(autonomy_level, $2::smallint)
WHERE request_id = $1 AND state = 'open' AND trigger_kind = 'chat'
RETURNING route_id, server_id";

    internal const string AddAskerSql = @"
INSERT INTO config.investigation_request_askers (request_id, person_id, identity, request_level, joined_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (request_id, person_id) DO UPDATE SET request_level = least(config.investigation_request_askers.request_level, EXCLUDED.request_level)";

    /* A message joins a chat request only while it still waits: one already taken by an open or a join is left where it is, and the join is nothing. */
    internal const string AttachPersonMessageSql = @"
UPDATE config.investigation_person_messages
SET request_id = $3, state = $4
WHERE channel_id = $1 AND ts = $2 AND state = 'waiting'";

    /* An open takes its messages first, under the route lock, each only while it still waits: the handler's open and the sweep's for the same
       message, in turn, cannot both take it, so one message opens one chat request. The request row does not exist yet (its id is a foreign key),
       so the link is set by LinkPersonMessageSql once it does. */
    internal const string TakePersonMessageSql = @"
UPDATE config.investigation_person_messages
SET state = 'opened'
WHERE channel_id = $1 AND ts = $2 AND state = 'waiting'";

    internal const string LinkPersonMessageSql = @"
UPDATE config.investigation_person_messages
SET request_id = $3
WHERE channel_id = $1 AND ts = $2 AND state = 'opened' AND request_id IS NULL";

    /* The chat requests of the last hour, by thread (the link column) and by route, apart from alerts (CountRouteSql leaves them out). */
    internal const string ChatCountsSql = @"
SELECT count(*) FILTER (WHERE chat_thread_of = $1), count(*) FILTER (WHERE route_id = $2)
FROM config.investigation_requests
WHERE trigger_kind = 'chat'
  AND practice_of IS NULL
  AND opened_at > $3 - interval '1 hour'";

    /* The chat request copies its parent as OpenFollowUpSql copies a follow-up, with trigger_kind chat and the thread's first request in
       chat_thread_of, stamped at the lowest of the parent's stamp ($4 the level the live routes give the parent's metrics, $5 the lowest asker
       level), never below 1 (the column's floor; Level 1 is reads alone). $1 new id, $2 parent, $3 now, $6 the thread's first request. */
    internal const string OpenChatRequestSql = @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
     window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, max_severity, trigger_kind, chat_thread_of)
SELECT $1, r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.story_path_hash, r.first_alert_at, r.last_alert_at,
       r.window_until, $3, $3 + (r.expires_at - r.opened_at), r.lease_minutes, greatest(1, least(r.autonomy_level, $4::smallint, $5::smallint)), r.alert_count,
       r.max_severity, 'chat', $6
FROM config.investigation_requests AS r
WHERE r.request_id = $2
RETURNING route_id, server_id, first_metric";

    /* A follow-up and a force check of a chat request copy its askers (an alert request has none, so the copy is empty). $1 new, $2 source. */
    internal const string CopyAskersSql = @"
INSERT INTO config.investigation_request_askers (request_id, person_id, identity, request_level, joined_at)
SELECT $1, person_id, identity, request_level, joined_at
FROM config.investigation_request_askers
WHERE request_id = $2
ON CONFLICT (request_id, person_id) DO NOTHING";

    /* The threads with waiting messages, with when each thread's first request opened, so the sweep applies the age limit as the handler does. */
    internal const string WaitingThreadsSql = @"
SELECT m.thread_request_id, r.route_id, r.server_id, m.channel_id, m.thread_ts, count(*), r.opened_at
FROM config.investigation_person_messages AS m
JOIN config.investigation_requests AS r ON r.request_id = m.thread_request_id
WHERE m.state = 'waiting'
GROUP BY m.thread_request_id, r.route_id, r.server_id, m.channel_id, m.thread_ts, r.opened_at
ORDER BY min(m.received_at)
LIMIT $1";

    /* A waiting message the sweep refuses: it keeps its text as the audit row it is, and no chat request ever reads it. */
    internal const string RefusePersonMessageSql = @"
UPDATE config.investigation_person_messages
SET state = 'refused'
WHERE channel_id = $1 AND ts = $2 AND state = 'waiting'";

    /* The oldest waiting messages of a thread, at most the thread cap: rows past it wait for the next open. */
    internal const string WaitingMessagesSql = @"
SELECT channel_id, ts, thread_ts, thread_request_id, person_id, identity, message_text, state, request_id, received_at
FROM config.investigation_person_messages
WHERE thread_request_id = $1 AND state = 'waiting'
ORDER BY received_at, ts
LIMIT $2";

    /* The open chat requests older than $1 with no offline answer yet, with the thread they belong to. A day bounds the read. */
    internal const string UnclaimedChatRequestsSql = @"
SELECT r.request_id, r.chat_thread_of, r.route_id, r.server_id, t.channel_id, t.thread_ts, r.opened_at
FROM config.investigation_requests AS r
JOIN config.investigation_request_threads AS t ON t.request_id = r.chat_thread_of
WHERE r.trigger_kind = 'chat'
  AND r.state = 'open'
  AND r.practice_of IS NULL
  AND r.opened_at <= $1
  AND r.opened_at > $1 - interval '1 day'
  AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS j WHERE j.once_key = 'slack_thread:' || r.request_id::text || ':chat_answer:offline')
ORDER BY r.opened_at";

    /* The newest messages of a request first, at most the row limit, with the request's whole count: the claim answer keeps the newest inside its
       character ceiling and says how many it left out. Only a message from an enabled person holding ask, request_level_2 or request_level_3 that
       reaches the request's server (the scope rule of RequestLevelSql) is read: the read itself keeps a disabled or ungranted person's text from
       the agent, whatever row another path attached. */
    internal const string PersonRequestsSql = @"
SELECT COALESCE(p.display_name, m.identity), m.person_id, m.message_text, r.server_id,
       count(*) OVER () AS total
FROM config.investigation_person_messages AS m
JOIN config.investigation_requests AS r ON r.request_id = m.request_id
JOIN config.person AS p ON p.person_id = m.person_id AND p.enabled
WHERE m.request_id = $1
  AND EXISTS (SELECT 1
              FROM config.person_grant AS g
              WHERE g.person_id = p.person_id
                AND g.capability IN ('ask', 'request_level_2', 'request_level_3')
                AND (   (g.scope_server_id IS NULL AND g.scope_tag_id IS NULL)
                     OR g.scope_server_id = r.server_id
                     OR g.scope_tag_id IN (SELECT t.tag_id FROM config.server_tag_map AS t WHERE t.server_id = r.server_id)))
ORDER BY m.received_at DESC, m.ts DESC
LIMIT $2";

    internal const string PersonRequestGrantsSql = @"
SELECT g.capability, g.kinds
FROM config.person_grant AS g
WHERE g.person_id = $1
  AND g.capability IN ('ask', 'request_level_2', 'request_level_3')
  AND (   (g.scope_server_id IS NULL AND g.scope_tag_id IS NULL)
       OR g.scope_server_id = $2
       OR g.scope_tag_id IN (SELECT m.tag_id FROM config.server_tag_map AS m WHERE m.server_id = $2))
ORDER BY g.capability";

    internal const string AgentMessageLockSql = @"
SELECT state, claim_id, lease_until, server_id
FROM config.investigation_requests
WHERE request_id = $1
FOR UPDATE";

    /// <inheritdoc cref="ISlackChatStore.FindThreadAsync" />
    public async Task<SlackChatThread?> FindChatThreadAsync(string channelId, string threadTs, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(channelId) || string.IsNullOrWhiteSpace(threadTs))
        {
            return null;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, FindChatThreadSql, P(channelId.Trim()), P(threadTs.Trim()));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new SlackChatThread(reader.GetGuid(0), reader.GetInt32(1), reader.GetInt32(2), channelId.Trim(), threadTs.Trim(), Utc(reader.GetDateTime(3)))
            : null;
    }

    /// <inheritdoc cref="ISlackChatStore.NewestRequestAsync" />
    public async Task<SlackChatNewest?> NewestThreadRequestAsync(SlackChatThread thread, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(thread);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, NewestThreadRequestSql, P(thread.ThreadRequestId), P(thread.ChannelId), P(thread.ThreadTs));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new SlackChatNewest(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), Utc(reader.GetDateTime(3)))
            : null;
    }

    /// <inheritdoc cref="ISlackChatStore.RequestLevelAsync" />
    public async Task<int> RequestLevelAsync(long personId, int serverId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, RequestLevelSql, P(personId), P(serverId));
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    /// <inheritdoc cref="ISlackChatStore.SaveMessageAsync" />
    public async Task<SlackPersonMessageSaved> SavePersonMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        /* The caps, read before the insert: a thread holds WaitingPerThread waiting messages and a person WaitingPerPerson of them, so one asker cannot grow
           a chat request or the claim answer without bound while a request is in play. A count and an insert, not a lock: two mentions in the same
           instant can pass together, and the read limit bounds what reaches the agent either way. A duplicate is still answered as one. */
        await using (var counts = Command(connection, null, WaitingCountsSql, P(message.ThreadRequestId),
            new NpgsqlParameter<long?> { TypedValue = message.PersonId, NpgsqlDbType = NpgsqlDbType.Bigint }))
        await using (var reader = await counts.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            var thread = reader.GetInt64(0);
            var person = reader.GetInt64(1);
            if (thread >= SlackChatRules.WaitingPerThread || person >= SlackChatRules.WaitingPerPerson)
            {
                await reader.CloseAsync();
                await using var seen = Command(connection, null, "SELECT EXISTS (SELECT 1 FROM config.investigation_person_messages WHERE channel_id = $1 AND ts = $2)", P(message.ChannelId), P(message.Ts));
                if (await seen.ExecuteScalarAsync(cancellationToken) is true)
                    return SlackPersonMessageSaved.Duplicate;
                return person >= SlackChatRules.WaitingPerPerson ? SlackPersonMessageSaved.PersonFull : SlackPersonMessageSaved.ThreadFull;
            }
        }

        await using var command = Command(connection, null, SavePersonMessageSql,
            P(message.ChannelId), P(message.Ts), P(message.ThreadTs), P(message.ThreadRequestId),
            new NpgsqlParameter<long?> { TypedValue = message.PersonId, NpgsqlDbType = NpgsqlDbType.Bigint },
            P(message.Identity), P(SlackChatRules.Bound(message.Text)), P(message.ReceivedAtUtc ?? Now()));
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1 ? SlackPersonMessageSaved.Saved : SlackPersonMessageSaved.Duplicate;
    }

    /// <inheritdoc cref="ISlackChatStore.JoinChatRequestAsync" />
    public async Task<bool> JoinChatRequestAsync(Guid requestId, SlackAsker asker, SlackPersonMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(asker);
        ArgumentNullException.ThrowIfNull(message);
        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        /* The message first, only while it still waits: one an open or another join already took joins nothing, and the stamp is untouched. */
        if (await ExecAsync(connection, transaction, AttachPersonMessageSql, cancellationToken, P(message.ChannelId), P(message.Ts), P(requestId), P(SlackPersonMessageStates.Joined)) == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        int routeId = 0, serverId = 0;
        var joined = false;
        await using (var join = Command(connection, transaction, JoinChatRequestSql, P(requestId), P((short)asker.RequestLevel)))
        await using (var returned = await join.ExecuteReaderAsync(cancellationToken))
        {
            if (await returned.ReadAsync(cancellationToken))
            {
                joined = true;
                routeId = returned.GetInt32(0);
                serverId = returned.GetInt32(1);
            }
        }

        if (!joined)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        await ExecAsync(connection, transaction, AddAskerSql, cancellationToken, P(requestId), P(asker.PersonId), P(asker.Identity), P((short)asker.RequestLevel), P(now));
        await JournalAskerAsync(connection, transaction, now, requestId, routeId, serverId, asker, message, SlackPersonMessageStates.Joined, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <inheritdoc cref="ISlackChatStore.ChatCountsAsync" />
    public async Task<SlackChatCounts> ChatCountsAsync(Guid threadRequestId, int routeId, DateTime nowUtc, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await ChatCountsAsync(connection, null, threadRequestId, routeId, nowUtc, cancellationToken);
    }

    private async Task<SlackChatCounts> ChatCountsAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, Guid threadRequestId, int routeId, DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, ChatCountsSql, P(threadRequestId), P(routeId), P(nowUtc));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new SlackChatCounts(reader.GetInt64(0), reader.GetInt64(1));
    }

    /// <summary>
    /// Opens a chat request (<see cref="ISlackChatStore.OpenChatRequestAsync"/>), under the route's advisory lock so two opens on one route count
    /// in turn: the thread cap and <paramref name="routeMaxPerHour"/> first, then the messages taken while they still wait (none taken of those handed
    /// over: <see cref="SlackChatOpenOutcome.NothingWaiting"/>, nothing opens), then the copy of the parent with <c>trigger_kind</c> chat and the
    /// thread link, the parent's alert rows, the askers whose messages were taken, one <c>opened</c> row and one <c>person_message</c> row per message taken.
    /// <paramref name="loadRoutes"/> gives the live routes, so the stamp is the lowest of the parent's stamp, the level the routes give the parent's
    /// metrics (a metric whose route is gone reads 0, fail closed, as the gate reads it), and the lowest asker level.
    /// </summary>
    public async Task<SlackChatOpenResult> OpenChatRequestAsync(SlackChatThread thread, Guid parentRequestId, IReadOnlyList<SlackAsker> askers,
        IReadOnlyList<SlackPersonMessage> messages, int routeMaxPerHour, DateTime nowUtc,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> loadRoutes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(thread);
        ArgumentNullException.ThrowIfNull(askers);
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(loadRoutes);
        /* Every message names one of the askers: a message handed over with no asker would be journaled as nobody's, so it is refused before any write. */
        foreach (var message in messages)
        {
            if (!askers.Any(a => a.PersonId == message.PersonId))
            {
                throw new ArgumentException("every message of a chat request open must belong to one of its askers; the message at " + message.Ts + " belongs to none", nameof(messages));
            }
        }

        var now = DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await ExecAsync(connection, transaction, RouteLockSql, cancellationToken, P(RouteLockKey), P(thread.RouteId));
        var counts = await ChatCountsAsync(connection, transaction, thread.ThreadRequestId, thread.RouteId, now, cancellationToken);
        if (counts.ThreadLastHour >= SlackChatRules.PerThreadPerHour)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new SlackChatOpenResult(SlackChatOpenOutcome.ThreadCap, null);
        }

        if (counts.RouteLastHour >= routeMaxPerHour)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new SlackChatOpenResult(SlackChatOpenOutcome.RouteCap, null);
        }

        /* The messages are taken under the lock, each only while it still waits. None taken out of some handed over: another open or join took
           them first, and nothing opens, so the hour's count and the agents' long poll are left alone. The askers are then those whose messages
           were taken (a list with no messages, a follow-up's shape, keeps the askers it was given). */
        var taken = new List<SlackPersonMessage>(messages.Count);
        foreach (var message in messages)
        {
            if (await ExecAsync(connection, transaction, TakePersonMessageSql, cancellationToken, P(message.ChannelId), P(message.Ts)) == 1)
            {
                taken.Add(message);
            }
        }

        if (messages.Count > 0 && taken.Count == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new SlackChatOpenResult(SlackChatOpenOutcome.NothingWaiting, null);
        }

        if (messages.Count > 0)
        {
            askers = askers.Where(a => taken.Any(m => m.PersonId == a.PersonId)).ToList();
        }

        /* The level the live routes give the parent's metrics: the lowest wins, a metric with no route reads 0. A parent with no alert rows lowers nothing. */
        var routeLevel = 3;
        var resolve = await loadRoutes(connection, transaction, cancellationToken);
        var metrics = new List<(string Metric, AlertSeverityLevel? Severity)>();
        await using (var read = Command(connection, transaction, AgentActionMetricsSql, P(parentRequestId)))
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var severity = reader.IsDBNull(1) ? null
                    : string.Equals(reader.GetString(1), InvestigationEvents.Critical, StringComparison.Ordinal) ? AlertSeverityLevel.Critical
                    : (AlertSeverityLevel?)AlertSeverityLevel.Warning;
                metrics.Add((reader.GetString(0), severity));
            }
        }

        foreach (var (metric, severity) in metrics)
        {
            routeLevel = Math.Min(routeLevel, resolve(metric, thread.ServerId, severity)?.AgentLevel ?? 0);
        }

        var askerLevel = askers.Count == 0 ? 3 : askers.Min(a => a.RequestLevel);
        var requestId = Guid.NewGuid();
        int routeId = 0, serverId = 0;
        string? firstMetric = null;
        await using (var insert = Command(connection, transaction, OpenChatRequestSql, P(requestId), P(parentRequestId), P(now),
            P((short)routeLevel), P((short)askerLevel), P(thread.ThreadRequestId)))
        await using (var returned = await insert.ExecuteReaderAsync(cancellationToken))
        {
            if (await returned.ReadAsync(cancellationToken))
            {
                routeId = returned.GetInt32(0);
                serverId = returned.GetInt32(1);
                firstMetric = returned.GetString(2);
            }
        }

        if (firstMetric is null)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new SlackChatOpenResult(SlackChatOpenOutcome.ParentGone, null);
        }

        await ExecAsync(connection, transaction, CopyFollowUpAlertsSql, cancellationToken, P(requestId), P(parentRequestId));
        foreach (var asker in askers)
        {
            await ExecAsync(connection, transaction, AddAskerSql, cancellationToken, P(requestId), P(asker.PersonId), P(asker.Identity), P((short)asker.RequestLevel), P(now));
        }

        var detail = new JsonObject
        {
            ["chat_thread_of"] = thread.ThreadRequestId.ToString("D"),
            ["parent"] = parentRequestId.ToString("D"),
            ["askers"] = askers.Count,
            ["messages"] = taken.Count,
        };
        await JournalAsync(connection, transaction, now, requestId, routeId, serverId, InvestigationEvents.Opened, "service", firstMetric, detail.ToJsonString(), null, cancellationToken);
        foreach (var message in taken)
        {
            await ExecAsync(connection, transaction, LinkPersonMessageSql, cancellationToken, P(message.ChannelId), P(message.Ts), P(requestId));
            await JournalAskerAsync(connection, transaction, now, requestId, routeId, serverId, askers.First(a => a.PersonId == message.PersonId), message, SlackPersonMessageStates.Opened, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return new SlackChatOpenResult(SlackChatOpenOutcome.Opened, requestId);
    }

    /* The asker's journal row (AskerJournalEvent): the person as the actor and in detail.person_id, which the approver check reads, the identity used,
       the message's channel and ts, and no text. One per channel and ts. */
    private Task<int> JournalAskerAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now, Guid requestId, int routeId, int serverId,
        SlackAsker asker, SlackPersonMessage message, string outcome, CancellationToken cancellationToken)
    {
        var detail = new JsonObject
        {
            ["person_id"] = asker.PersonId,
            ["identity"] = asker.Identity,
            ["request_level"] = asker.RequestLevel,
            ["channel"] = message.ChannelId,
            ["ts"] = message.Ts,
            ["outcome"] = outcome,
        };
        return JournalAsync(connection, transaction, now, requestId, routeId, serverId, AskerJournalEvent, "person:" + asker.PersonId.ToString(CultureInfo.InvariantCulture),
            outcome, detail.ToJsonString(), PersonMessageOnceKey(message.ChannelId, message.Ts), cancellationToken);
    }

    /// <inheritdoc cref="ISlackChatStore.JournalPersonMessageAsync" />
    public async Task JournalPersonMessageAsync(Guid? requestId, int? routeId, int? serverId, string actor, string outcome, JsonObject detail, string onceKey,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        ArgumentException.ThrowIfNullOrWhiteSpace(outcome);
        ArgumentNullException.ThrowIfNull(detail);
        ArgumentException.ThrowIfNullOrWhiteSpace(onceKey);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await JournalAsync(connection, transaction, Now(), requestId, routeId, serverId, AskerJournalEvent, actor, outcome, detail.ToJsonString(), onceKey, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <inheritdoc cref="ISlackChatStore.MarkChatAnswerOnceAsync" />
    public async Task<bool> MarkChatAnswerOnceAsync(Guid? requestId, int? routeId, int? serverId, string answerKey, string onceKey, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(answerKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(onceKey);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var written = await JournalAsync(connection, transaction, Now(), requestId, routeId, serverId, SlackThreadPosted, "service", answerKey, null, onceKey, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return written == 1;
    }

    /// <inheritdoc cref="ISlackChatStore.WaitingThreadsAsync" />
    public async Task<IReadOnlyList<SlackWaitingThread>> WaitingThreadsAsync(int limit, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, WaitingThreadsSql, P(Math.Clamp(limit, 1, 1000)));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<SlackWaitingThread>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SlackWaitingThread(reader.GetGuid(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3), reader.GetString(4), (int)reader.GetInt64(5), Utc(reader.GetDateTime(6))));
        }

        return rows;
    }

    /// <inheritdoc cref="ISlackChatStore.RefuseMessageAsync" />
    public async Task RefusePersonMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, RefusePersonMessageSql, P(message.ChannelId), P(message.Ts));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc cref="ISlackChatStore.WaitingMessagesAsync" />
    public async Task<IReadOnlyList<SlackPersonMessage>> WaitingMessagesAsync(Guid threadRequestId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, WaitingMessagesSql, P(threadRequestId), P(SlackChatRules.WaitingPerThread));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<SlackPersonMessage>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SlackPersonMessage(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetGuid(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4), reader.GetString(5), reader.GetString(6), reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetGuid(8), Utc(reader.GetDateTime(9))));
        }

        return rows;
    }

    /// <inheritdoc cref="ISlackChatStore.UnclaimedChatRequestsAsync" />
    public async Task<IReadOnlyList<SlackUnclaimedChat>> UnclaimedChatRequestsAsync(DateTime openedBeforeUtc, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, UnclaimedChatRequestsSql, P(openedBeforeUtc));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<SlackUnclaimedChat>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SlackUnclaimedChat(reader.GetGuid(0), reader.GetGuid(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetString(4), reader.GetString(5), Utc(reader.GetDateTime(6))));
        }

        return rows;
    }

    /// <summary>The messages of a chat request for the claim answer's <c>person_requests</c>: each person's name, grants for the request's server
    /// (<c>ask</c>, <c>request_level_2</c>, <c>request_level_3:&lt;kinds&gt;</c>) and text, oldest first, from enabled people whose grants reach the
    /// server alone. Empty for an alert request.</summary>
    public async Task<PersonRequestsPage> ListPersonRequestsAsync(Guid requestId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        /* Newest first, at most the row limit; the character ceiling then keeps the newest that fit, and the rest of the request's count is what was left out. */
        var rows = new List<(string Name, long? PersonId, string Text, int ServerId)>();
        long total = 0;
        var characters = 0;
        await using (var command = Command(connection, null, PersonRequestsSql, P(requestId), P(SlackChatRules.PersonRequestsRowLimit)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                total = reader.GetInt64(4);
                var text = reader.GetString(2);
                if (characters + text.Length > SlackChatRules.PersonRequestsCharLimit && rows.Count > 0)
                    break;
                characters += text.Length;
                rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), text, reader.GetInt32(3)));
            }
        }

        rows.Reverse();

        var grants = new Dictionary<long, IReadOnlyList<string>>();
        var result = new List<PersonRequest>(rows.Count);
        foreach (var (name, personId, text, serverId) in rows)
        {
            IReadOnlyList<string> held = [];
            if (personId is long person)
            {
                if (!grants.TryGetValue(person, out held!))
                {
                    var list = new List<string>();
                    await using var command = Command(connection, null, PersonRequestGrantsSql, P(person), P(serverId));
                    await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        var capability = reader.GetString(0);
                        var kinds = reader.IsDBNull(1) ? [] : reader.GetFieldValue<string[]>(1);
                        list.Add(kinds.Length == 0 ? capability : capability + ":" + string.Join(",", kinds));
                    }

                    held = list;
                    grants[person] = held;
                }
            }

            result.Add(new PersonRequest(name, held, text));
        }

        return new PersonRequestsPage(result, (int)Math.Max(0, total - rows.Count));
    }

    /// <summary>
    /// An agent's message to the thread (<c>post_investigation_message</c>): while <paramref name="claimId"/> is the live claim of
    /// <paramref name="requestId"/> (the row held <c>FOR UPDATE</c>), journals <see cref="AgentMessage"/> as <c>agent:&lt;agentId&gt;</c> with the
    /// text bounded to <see cref="SlackChatRules.TextLimit"/>, under the request's agent journal cap (<see cref="InvestigationRules.MaxAgentJournalRows"/>).
    /// The row is the owner's alone (the journal's trigger refuses it from any other login), so the whole post runs on <see cref="AgentActionSource"/>,
    /// as the agent action rows do: the tool's own data source is the mcp login.
    /// </summary>
    public async Task<AgentMessageResult> PostAgentMessageAsync(Guid requestId, Guid claimId, string agentId, string text, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(agentId);
        var now = Now();
        var actor = "agent:" + agentId;
        await using var connection = await AgentActionSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        string? state = null;
        Guid? currentClaim = null;
        DateTime? leaseUntil = null;
        var serverId = 0;
        await using (var command = Command(connection, transaction, AgentMessageLockSql, P(requestId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                state = reader.GetString(0);
                currentClaim = reader.IsDBNull(1) ? null : reader.GetGuid(1);
                leaseUntil = reader.IsDBNull(2) ? null : Utc(reader.GetDateTime(2));
                serverId = reader.GetInt32(3);
            }
        }

        if (state is null)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new AgentMessageResult(AgentMessageOutcome.NotFound, null);
        }

        if (state is "reported" or "closed")
        {
            await transaction.RollbackAsync(cancellationToken);
            return new AgentMessageResult(AgentMessageOutcome.NotOpen, null);
        }

        if (state != "claimed" || currentClaim != claimId || leaseUntil is not DateTime lease || lease <= now)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new AgentMessageResult(AgentMessageOutcome.LeaseLost, null);
        }

        if (await JournalCappedAsync(connection, transaction, now, requestId, actor, "message", cancellationToken))
        {
            await transaction.CommitAsync(cancellationToken);
            return new AgentMessageResult(AgentMessageOutcome.JournalCapped, null);
        }

        var detail = new JsonObject { ["text"] = SlackChatRules.Bound(text) };
        long journalId;
        await using (var insert = Command(connection, transaction, AgentActionJournalSql, P(now), P(requestId), P(serverId), P(AgentMessage), P(actor),
            PNullable(null), P(detail.ToJsonString())))
        {
            journalId = (long)(await insert.ExecuteScalarAsync(cancellationToken))!;
        }

        await transaction.CommitAsync(cancellationToken);
        return new AgentMessageResult(AgentMessageOutcome.Posted, journalId);
    }

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
