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
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7656: a request's Slack thread: the channel the parent was posted to and the parent's <c>ts</c>.</summary>
public sealed record SlackRequestThread(Guid RequestId, string ChannelId, string ThreadTs);

/// <summary>
/// #7656: where request threads and post marks live. The store writes the thread row when the parent post delivers, finds it for
/// every later post, and writes each post's once mark (a journal row under <see cref="SlackThreadPosts.OnceKey"/>) before the send.
/// </summary>
public interface ISlackRequestThreadStore
{
    /// <summary>The thread of <paramref name="threadRequestId"/>, or null when it has none yet.</summary>
    Task<SlackRequestThread?> FindAsync(Guid threadRequestId, CancellationToken cancellationToken);

    /// <summary>Saves a request's thread after its parent post delivered.</summary>
    Task SaveAsync(SlackRequestThread thread, CancellationToken cancellationToken);

    /// <summary>Writes the once mark of one post and answers whether THIS call wrote it. The poster sends only on true.</summary>
    Task<bool> MarkOnceAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, CancellationToken cancellationToken);
}

/// <summary>
/// #7656: which request a post belongs to and where its thread is. <see cref="ThreadRequestId"/> is the request whose thread the post
/// goes into: the request itself, or, for a linked request, the thread's first request. <see cref="BotChannel"/> is the route's bot
/// channel id, null or blank when the route has none. <see cref="Practice"/> is a drill and gets no thread.
/// </summary>
public sealed record SlackThreadTarget(Guid RequestId, Guid ThreadRequestId, int RouteId, int ServerId, string? BotChannel, bool Practice);

/// <summary>#7656: how one thread post ended.</summary>
public enum SlackThreadPostOutcome
{
    /// <summary>Delivered.</summary>
    Posted,

    /// <summary>A practice request: nothing written, nothing sent.</summary>
    Practice,

    /// <summary>The route has no bot channel: nothing written, nothing sent.</summary>
    NoChannel,

    /// <summary>A reply with no thread to join: the parent post has not delivered. Nothing written, nothing sent.</summary>
    NoThread,

    /// <summary>The once mark was already there: another pass or service posted it.</summary>
    AlreadyPosted,

    /// <summary>The mark was written and the send failed. Not retried: the mark stays, as the report notice's does.</summary>
    Failed,
}

/// <summary>
/// #7656: posts into one Slack thread per request. The request post is the thread parent: it delivers to the route's bot channel and
/// its <c>ts</c> is saved as the thread. Every other post of the request, and every post of a request linked to it, replies in that
/// thread. The once mark is written before the send, so a replayed pass and a second service on the same store post each thing once
/// between them. A practice request gets no thread, and a route with no bot channel posts nothing.
/// </summary>
public sealed class SlackThreadPoster
{
    private readonly ISlackRequestThreadStore _store;
    private readonly ISlackBotPosts _posts;
    private readonly ILogger<SlackThreadPoster> _logger;

    public SlackThreadPoster(ISlackRequestThreadStore store, ISlackBotPosts posts, ILogger<SlackThreadPoster> logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _posts = posts ?? throw new ArgumentNullException(nameof(posts));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>Posts <paramref name="content"/> for <paramref name="target"/> under <paramref name="postKey"/> (one of the
    /// <see cref="SlackThreadPosts"/> keys). The request post (<see cref="SlackThreadPosts.RequestKey"/>) opens the thread when the
    /// target has none; every other key replies in the thread and posts nothing without one.</summary>
    public async Task<SlackThreadPostOutcome> PostAsync(SlackThreadTarget target, string postKey, SlackThreadPostContent content, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentException.ThrowIfNullOrWhiteSpace(postKey);
        ArgumentNullException.ThrowIfNull(content);
        if (target.Practice)
        {
            return SlackThreadPostOutcome.Practice;
        }

        if (string.IsNullOrWhiteSpace(target.BotChannel))
        {
            return SlackThreadPostOutcome.NoChannel;
        }

        var thread = await _store.FindAsync(target.ThreadRequestId, cancellationToken);
        var opensThread = thread is null && string.Equals(postKey, SlackThreadPosts.RequestKey, StringComparison.Ordinal);
        if (thread is null && !opensThread)
        {
            return SlackThreadPostOutcome.NoThread;
        }

        var onceKey = SlackThreadPosts.OnceKey(target.RequestId, postKey);
        if (!await _store.MarkOnceAsync(target.RequestId, target.RouteId, target.ServerId, postKey, onceKey, cancellationToken))
        {
            return SlackThreadPostOutcome.AlreadyPosted;
        }

        var channel = thread?.ChannelId ?? target.BotChannel.Trim();
        var result = await _posts.PostAsync(channel, content.Text, content.Blocks, thread?.ThreadTs, cancellationToken);
        if (!result.Ok)
        {
            /* The error is Slack's word or an HTTP status, never the token: the client redacts it. */
            _logger.LogWarning("Slack thread post {PostKey} for investigation request {RequestId} was not delivered: {Error}",
                postKey, target.RequestId, result.Error);
            return SlackThreadPostOutcome.Failed;
        }

        if (opensThread)
        {
            if (string.IsNullOrWhiteSpace(result.Ts))
            {
                _logger.LogWarning("Slack thread parent for investigation request {RequestId} delivered without a ts; later posts cannot join it.", target.RequestId);
                return SlackThreadPostOutcome.Failed;
            }

            await _store.SaveAsync(new SlackRequestThread(target.ThreadRequestId, result.Channel ?? channel, result.Ts), cancellationToken);
        }

        return SlackThreadPostOutcome.Posted;
    }
}
