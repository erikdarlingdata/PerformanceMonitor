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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>#7656: a request's Slack thread (V252 <c>config.investigation_request_threads</c>): the channel the parent post went to and the parent's <c>ts</c>.</summary>
public sealed record SlackRequestThread(Guid RequestId, string ChannelId, string ThreadTs);

/// <summary>
/// #7656: where request threads and post marks live. The store writes the thread row when the parent post delivers, finds it for
/// every later post, and writes each post's once mark (a journal row under the <c>slack_thread:</c> once key) before the send.
/// </summary>
public interface ISlackRequestThreadStore
{
    /// <summary>The thread of <paramref name="threadRequestId"/>, or null when it has none yet.</summary>
    Task<SlackRequestThread?> FindAsync(Guid threadRequestId, CancellationToken cancellationToken);

    /// <summary>Saves a request's thread after its parent post delivered. A thread already saved for the request stays: the first writer wins.</summary>
    Task SaveAsync(SlackRequestThread thread, CancellationToken cancellationToken);

    /// <summary>Writes the once mark of one post and answers whether THIS call wrote it. The poster sends only on true.</summary>
    Task<bool> MarkOnceAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, CancellationToken cancellationToken);

    /// <summary>Writes the once mark of a post that is not sent, with <paramref name="reason"/> saying why, so the pass does not read the row
    /// again. The mark row is the same kind and key a sent post gets; only its reason differs.</summary>
    Task<bool> MarkSkippedAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, string reason, CancellationToken cancellationToken);

    /// <summary>Takes back the once mark of one post after Slack answered 429: the post did not go out, so the next pass sends it.</summary>
    Task ReleaseMarkAsync(string onceKey, CancellationToken cancellationToken);
}

/// <summary>
/// #7656: one journal row the thread pass posts, with the request it belongs to. <see cref="PostKey"/> is <c>request</c> for the
/// <c>opened</c> row, <c>action_unknown:&lt;called_id&gt;</c> for a change with no result, else <c>&lt;event&gt;:&lt;journal_id&gt;</c>.
/// <see cref="CalledDetailJson"/> is the <c>action_called</c> row's detail for a change's result row, <see cref="ClaimNo"/> the claim's
/// number among the request's claims, <see cref="Verdict"/> and <see cref="RootCause"/> the report's for the <c>reported</c> row, and
/// <see cref="OpenedAtUtc"/> when the request opened, which the pass holds against the route's bot channel stamp.
/// </summary>
public sealed record SlackThreadJournalRow(
    long JournalId, string Event, DateTime EventTimeUtc, string Actor, string? Reason, string? DetailJson, string PostKey,
    Guid RequestId, int RouteId, int ServerId, string ServerName, string Metric, string Severity, string Trigger, Guid? FollowUpOf,
    string? CalledDetailJson, int ClaimNo, string? Verdict, string? RootCause, DateTime OpenedAtUtc, Guid? ChatThreadOf = null);

/// <summary>#7656: where the thread pass reads the rows to post. The store implements it; a test stands its own in.</summary>
public interface ISlackThreadRowSource
{
    /// <summary>Up to <paramref name="limit"/> journal rows of the kinds in <paramref name="events"/> from <paramref name="sinceUtc"/> on
    /// with no post mark yet, on a live request of an enabled route with a bot channel, in journal order, plus each <c>action_called</c>
    /// row from that window and before <paramref name="unknownBeforeUtc"/> whose result never came.</summary>
    Task<IReadOnlyList<SlackThreadJournalRow>> PendingSlackThreadRowsAsync(IReadOnlyList<string> events, DateTime sinceUtc, DateTime unknownBeforeUtc, int limit, CancellationToken cancellationToken);

    /// <summary>The agent changes of one request, oldest first, for the report post.</summary>
    Task<IReadOnlyList<RequestChangeRow>> ChangesForRequestAsync(Guid requestId, CancellationToken cancellationToken = default);
}

public sealed partial class InvestigationRequestStore : ISlackRequestThreadStore, ISlackThreadRowSource
{
    /// <summary>#7656: the journal row that marks one Slack thread post as sent (once key <c>slack_thread:&lt;request&gt;:&lt;post&gt;</c>). Owner-only,
    /// as the notice marks are: another login that wrote it first would silence the post.</summary>
    public const string SlackThreadPosted = "slack_thread_posted";

    internal const string FindSlackThreadSql = @"
SELECT channel_id, thread_ts
FROM config.investigation_request_threads
WHERE request_id = $1";

    /* First writer wins: two services posting the same parent both take the once mark race, and only one delivers, so only one saves. */
    internal const string SaveSlackThreadSql = @"
INSERT INTO config.investigation_request_threads (request_id, channel_id, thread_ts, posted_at)
VALUES ($1, $2, $3, now() AT TIME ZONE 'UTC')
ON CONFLICT (request_id) DO NOTHING";

    /// <inheritdoc />
    public async Task<SlackRequestThread?> FindAsync(Guid threadRequestId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(FindSlackThreadSql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = threadRequestId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new SlackRequestThread(threadRequestId, reader.GetString(0), reader.GetString(1))
            : null;
    }

    /// <inheritdoc />
    public async Task SaveAsync(SlackRequestThread thread, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(thread);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(SaveSlackThreadSql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = thread.RequestId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = thread.ChannelId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = thread.ThreadTs });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> MarkOnceAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, CancellationToken cancellationToken) =>
        MarkOnceAsync(requestId, routeId, serverId, SlackThreadPosted, postKey, onceKey, cancellationToken);

    /// <inheritdoc />
    public Task<bool> MarkSkippedAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, string reason, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return MarkOnceAsync(requestId, routeId, serverId, SlackThreadPosted, postKey + ": " + reason, onceKey, cancellationToken);
    }

    /* Apart from retention's age-out (DarlingRetention), this is the only delete on the journal. It exists because a 429 means the
       post did not go out, so its mark must go for the next pass to post it. The mark row is the service's own: the journal's
       owner-row trigger guards inserts, and the service owns the table, so its delete needs no grant. Only a slack_thread_posted
       row goes, never a row of another kind that shares the once key's shape. */
    internal const string ReleaseSlackMarkSql = @"
DELETE FROM config.investigation_journal
WHERE once_key = $1
  AND event = 'slack_thread_posted'";

    /// <inheritdoc />
    public async Task ReleaseMarkAsync(string onceKey, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(onceKey);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(ReleaseSlackMarkSql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = onceKey });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /* The rows to post: a mark says what was posted, and the window from $4 bounds what is read. Both arms read the journal through its
       event_time index from $4 on, so a pass costs the window's rows and not the journal's; a row older than the window is neither read
       nor marked. The first arm is every journal row of a post kind; the second is each action_called row older than $2 with no done,
       failed or undone row naming it, posted once as result unknown under its own key. The third arm (#7658) is each pending note written
       from a request on a server, as a card with its decision buttons under the key note:<note_id>; a fleet note has no server and is not
       posted. A request is left out while it is a drill, while
       its route is disabled or has no bot channel, and a row once its mark exists. The called row's detail rides along for a change's
       result row, the claim's number for a claim, the report's verdict and root cause for the reported row, and the request's opened
       time for the pass's gate. The called row is joined by its bigint journal id (the result row's called_id, when it is a number),
       so the join is a primary key lookup per returned row and not a walk of the journal. */
    internal const string PendingSlackThreadRowsSql = @"
WITH posts AS (
    SELECT j.journal_id, j.event, j.event_time, j.actor, j.reason, j.detail, j.request_id,
           CASE WHEN j.event = 'opened' THEN 'request' ELSE j.event || ':' || j.journal_id::text END AS post_key,
           CASE WHEN j.event IN ('action_done', 'action_failed', 'action_undone') AND (j.detail ->> 'called_id') ~ '^[0-9]{1,18}$'
                THEN (j.detail ->> 'called_id')::bigint END AS called_id
    FROM config.investigation_journal AS j
    WHERE j.event = ANY ($1)
      AND j.request_id IS NOT NULL
      AND j.event_time >= $4
    UNION ALL
    SELECT c.journal_id, 'action_unknown', c.event_time, c.actor, c.reason, c.detail, c.request_id,
           'action_unknown:' || c.journal_id::text, c.journal_id
    FROM config.investigation_journal AS c
    WHERE c.event = 'action_called'
      AND c.request_id IS NOT NULL
      AND c.event_time >= $4
      AND c.event_time <= $2
      AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS x
                      WHERE x.request_id = c.request_id
                        AND x.event IN ('action_done', 'action_failed', 'action_undone')
                        AND x.detail ->> 'called_id' = c.journal_id::text)
    UNION ALL
    SELECT n.note_id, 'note_pending', n.created_at, n.author, n.note_text, jsonb_build_object('note_id', n.note_id, 'metric', n.metric_name), n.source_request_id,
           'note:' || n.note_id::text, NULL::bigint
    FROM config.investigation_notes AS n
    WHERE n.state = 'pending'
      AND n.source_request_id IS NOT NULL
      AND n.server_id IS NOT NULL
      AND n.created_at >= $4
      AND n.expires_at > $2
)
SELECT p.journal_id, p.event, p.event_time, p.actor, p.reason, p.detail::text, p.post_key,
       r.request_id, r.route_id, r.server_id, COALESCE(sv.display_name, sv.server_name, r.server_name), r.first_metric,
       COALESCE(r.max_severity, ''), COALESCE(r.trigger_kind, ''), r.follow_up_of,
       c.detail::text,
       CASE WHEN p.event = 'claimed'
            THEN (SELECT count(*) FROM config.investigation_journal AS k
                  WHERE k.request_id = p.request_id AND k.event = 'claimed' AND k.journal_id <= p.journal_id)
            ELSE 0 END,
       rp.verdict, rp.root_cause, r.opened_at
FROM posts AS p
JOIN config.investigation_requests AS r ON r.request_id = p.request_id
JOIN config.config_notification_routes AS rt ON rt.route_id = r.route_id
LEFT JOIN collect.servers AS sv ON sv.server_id = r.server_id
LEFT JOIN config.investigation_journal AS c ON c.journal_id = p.called_id
LEFT JOIN config.investigation_reports AS rp ON p.event = 'reported' AND rp.request_id = r.request_id
WHERE r.practice_of IS NULL
  AND rt.enabled
  AND rt.slack_bot_channel <> ''
  AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS m
                  WHERE m.once_key = 'slack_thread:' || r.request_id::text || ':' || p.post_key)
ORDER BY p.journal_id
LIMIT $3";

    /// <inheritdoc />
    public async Task<IReadOnlyList<SlackThreadJournalRow>> PendingSlackThreadRowsAsync(IReadOnlyList<string> events, DateTime sinceUtc, DateTime unknownBeforeUtc, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(events);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, PendingSlackThreadRowsSql,
            new NpgsqlParameter<string[]> { TypedValue = [.. events], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text },
            P(DateTime.SpecifyKind(unknownBeforeUtc, DateTimeKind.Utc)),
            P(limit),
            P(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc)));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<SlackThreadJournalRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SlackThreadJournalRow(
                reader.GetInt64(0), reader.GetString(1), reader.GetFieldValue<DateTime>(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetString(6),
                reader.GetGuid(7), reader.GetInt32(8), reader.GetInt32(9), reader.GetString(10), reader.GetString(11),
                reader.GetString(12), reader.GetString(13), reader.IsDBNull(14) ? null : reader.GetGuid(14),
                reader.IsDBNull(15) ? null : reader.GetString(15), (int)reader.GetInt64(16),
                reader.IsDBNull(17) ? null : reader.GetString(17), reader.IsDBNull(18) ? null : reader.GetString(18),
                reader.GetFieldValue<DateTime>(19)));
        }

        return rows;
    }
}
