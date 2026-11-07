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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Chat;

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

    /// <summary>Slack answered 429: the post did not go out. Its mark is released, and the channel waits for Retry-After.</summary>
    RateLimited,
}

/// <summary>
/// #7656: the channels Slack has rate-limited, each with the time its Retry-After ends. <c>chat.postMessage</c> allows about one post
/// a second per channel with short bursts, and an alert storm opens many requests at once, so a 429 is expected there. Shared by the
/// poster, which records a 429, and the pass, which posts nothing to a blocked channel before its time.
/// </summary>
public sealed class SlackPostThrottle
{
    private readonly Dictionary<string, DateTime> _blockedUntil = new(StringComparer.Ordinal);
    private readonly object _lock = new();

    /// <summary>Blocks <paramref name="channel"/> until <paramref name="untilUtc"/>. A later time replaces an earlier one, never the reverse.</summary>
    public void Block(string channel, DateTime untilUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        lock (_lock)
        {
            if (!_blockedUntil.TryGetValue(channel, out var current) || current < untilUtc)
            {
                _blockedUntil[channel] = untilUtc;
            }
        }
    }

    /// <summary>When the block on <paramref name="channel"/> ends, or null when the channel is open at <paramref name="nowUtc"/>.</summary>
    public DateTime? BlockedUntil(string channel, DateTime nowUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        lock (_lock)
        {
            if (!_blockedUntil.TryGetValue(channel, out var until))
            {
                return null;
            }

            if (until > nowUtc)
            {
                return until;
            }

            _blockedUntil.Remove(channel);
            return null;
        }
    }
}

/// <summary>
/// #7656: posts into one Slack thread per request. The request post is the thread parent: it delivers to the route's bot channel and
/// its <c>ts</c> is saved as the thread. Every other post of the request, and every post of a request linked to it, replies in that
/// thread. The once mark is written before the send, so a replayed pass and a second service on the same store post each thing once
/// between them. A practice request gets no thread, and a route with no bot channel posts nothing. A 429 is the one failure whose mark
/// is released: Slack did not take the post, so the next pass sends it; the channel is blocked in the <see cref="SlackPostThrottle"/>
/// until Retry-After ends. Every other failure keeps its mark: a timeout may have posted, and <c>channel_not_found</c> or
/// <c>not_in_channel</c> do not get better on a retry.
/// </summary>
public sealed class SlackThreadPoster
{
    /// <summary>How long a channel waits after a 429 that carries no <c>Retry-After</c>. Slack sends the header on every 429, so this
    /// stands in only for an answer with it stripped.</summary>
    public static readonly TimeSpan DefaultRetryAfter = TimeSpan.FromSeconds(60);

    private readonly ISlackRequestThreadStore _store;
    private readonly ISlackBotPosts _posts;
    private readonly ILogger<SlackThreadPoster> _logger;
    private readonly SlackPostThrottle _throttle;
    private readonly Func<DateTime> _utcNow;

    /// <param name="throttle">Where a 429's block is recorded; the pass that drives this poster shares it. A new one when null.</param>
    /// <param name="utcNow">The clock the block's end is measured from.</param>
    public SlackThreadPoster(ISlackRequestThreadStore store, ISlackBotPosts posts, ILogger<SlackThreadPoster> logger,
        SlackPostThrottle? throttle = null, Func<DateTime>? utcNow = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _posts = posts ?? throw new ArgumentNullException(nameof(posts));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _throttle = throttle ?? new SlackPostThrottle();
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>The throttle this poster records a 429 in.</summary>
    public SlackPostThrottle Throttle => _throttle;

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
        if (!result.Ok && result.RateLimited)
        {
            var until = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc) + (result.RetryAfter ?? DefaultRetryAfter);
            _throttle.Block(channel, until);
            await _store.ReleaseMarkAsync(onceKey, cancellationToken);
            _logger.LogWarning("Slack rate-limited the thread post {PostKey} for investigation request {RequestId}; its mark is released and the channel waits until {Until:O}",
                postKey, target.RequestId, until);
            return SlackThreadPostOutcome.RateLimited;
        }

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
        else if (thread is not null && target.ThreadRequestId != target.RequestId
            && string.Equals(postKey, SlackThreadPosts.RequestKey, StringComparison.Ordinal))
        {
            /* A follow-up's request post replied in its parent's thread: the thread is saved as the follow-up's own, so a follow-up of
               the follow-up finds it with one lookup and the chain lands in the root's thread. */
            await _store.SaveAsync(new SlackRequestThread(target.RequestId, thread.ChannelId, thread.ThreadTs), cancellationToken);
        }

        return SlackThreadPostOutcome.Posted;
    }
}
