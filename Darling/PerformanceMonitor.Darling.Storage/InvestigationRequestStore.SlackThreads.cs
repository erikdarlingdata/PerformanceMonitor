/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

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
}

public sealed partial class InvestigationRequestStore : ISlackRequestThreadStore
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
}
