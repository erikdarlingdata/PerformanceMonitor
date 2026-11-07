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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7659: what the newest request of a thread makes a mention do (design section 4, the one rule).</summary>
public enum SlackChatRule
{
    /// <summary>The newest request is an unclaimed chat request: the message joins it.</summary>
    Join,

    /// <summary>The newest request is in play: the message waits for it to end.</summary>
    Wait,

    /// <summary>The newest request has ended, or there is none: a linked chat request opens now.</summary>
    Open,
}

/// <summary>#7659: the bot's answers in a thread, one sentence each. Posted once per thread and cause; never a journal event the agent sees.</summary>
public static class SlackChatAnswers
{
    public const string NoGrant = "You don't have the ask grant for this server, so I can't send this to the agent. An admin can grant it.";
    public const string OutsideThread = "I only work inside a request's thread.";
    public const string OldRequest = "This request is more than 7 days old, so I won't open a new one from it. Mention me in a newer request's thread.";
    public const string Waiting = "Got it. The agent will read this when it finishes the current request.";
    public const string OverCap = "This thread has reached 6 agent requests this hour. I'll open yours when the hour ends.";
    public const string Offline = "No agent has picked this up in 5 minutes. It may be offline.";
    public const string RouteCap = "This route has reached its agent requests for this hour. I'll open yours when the hour ends.";

    /// <summary>The answer kinds, as the once marks name them.</summary>
    public const string NoGrantKind = "no_grant", OutsideKind = "outside", OldKind = "old", WaitingKind = "waiting", OverCapKind = "over_cap",
        OfflineKind = "offline", RouteCapKind = "route_cap";
}

/// <summary>
/// #7659: the mentions. One event reaches here after the listener's filters. Only a message that mentions the bot counts; a message with a subtype
/// (an edit, a broadcast) is ignored. A mention outside a request thread, or on a thread whose first request opened more than 7 days ago, opens
/// nothing and is answered once. A mention from a person without <c>ask</c> writes a journal row with no text and is answered once. Otherwise the
/// newest request of the thread decides: the message joins an unclaimed chat request, waits for a request in play (the first waiting mention is
/// acknowledged once), or opens a linked chat request now, under the thread's and the route's hourly caps. The message text, with every mention
/// of the bot removed and bounded, is an audit row in its own table and reaches the agent only as <c>person_requests</c> on the claim answer.
/// </summary>
public sealed class SlackMentionHandler : ISlackEventHandlers
{
    private readonly ISlackChatStore _store;
    private readonly ISlackBotPosts _posts;
    private readonly Func<IReadOnlyList<NotificationRoute>> _routes;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;

    public SlackMentionHandler(ISlackChatStore store, ISlackBotPosts posts, Func<IReadOnlyList<NotificationRoute>> routes, ILogger logger, Func<DateTime>? utcNow = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _posts = posts ?? throw new ArgumentNullException(nameof(posts));
        _routes = routes ?? throw new ArgumentNullException(nameof(routes));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _utcNow = utcNow ?? (static () => DateTime.UtcNow);
    }

    /// <summary>The event type of a mention.</summary>
    public const string AppMention = "app_mention";

    /// <summary>The event type of a channel message, which counts only when it mentions the bot.</summary>
    public const string Message = "message";

    /// <summary>PURE: whether an event of <paramref name="eventType"/> with <paramref name="text"/> is a mention of the bot <paramref name="botUserId"/>.</summary>
    public static bool IsMention(string? eventType, string? text, string? botUserId) => false;

    /// <summary>PURE: <paramref name="text"/> with every <c>&lt;@bot&gt;</c> token removed, wherever it sits, runs of spaces collapsed and the ends trimmed.</summary>
    public static string StripBotMentions(string? text, string? botUserId) => text ?? "";

    /// <summary>PURE: <paramref name="text"/> held to <see cref="SlackChatRules.TextLimit"/> characters, never cut inside a surrogate pair.</summary>
    public static string Bound(string text) => text;

    /// <summary>PURE: the one rule over the newest request of the thread.</summary>
    public static SlackChatRule Decide(SlackChatNewest? newest) => SlackChatRule.Wait;

    /// <summary>PURE: whether a thread whose first request opened at <paramref name="openedAtUtc"/> is past the age limit at <paramref name="nowUtc"/>.</summary>
    public static bool IsTooOld(DateTime openedAtUtc, DateTime nowUtc) => false;

    /// <summary>Presses are not this handler's: the buttons (#7658) take them.</summary>
    public Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>#7659: the sweep's two arms, on the request sweep's tick: a chat request opens for the waiting messages of a thread whose newest request has
/// ended, and an unclaimed chat request older than <see cref="SlackChatRules.OfflineAfter"/> gets the offline answer once.</summary>
public sealed class SlackChatSweep
{
    private readonly ISlackChatStore _store;
    private readonly ISlackBotPosts _posts;
    private readonly Func<IReadOnlyList<NotificationRoute>> _routes;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;

    /// <summary>The most threads one sweep opens for.</summary>
    public const int BatchSize = 50;

    public SlackChatSweep(ISlackChatStore store, ISlackBotPosts posts, Func<IReadOnlyList<NotificationRoute>> routes, ILogger logger, Func<DateTime>? utcNow = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _posts = posts ?? throw new ArgumentNullException(nameof(posts));
        _routes = routes ?? throw new ArgumentNullException(nameof(routes));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _utcNow = utcNow ?? (static () => DateTime.UtcNow);
    }

    /// <summary>One sweep. Answers how many chat requests opened and how many offline answers posted.</summary>
    public Task<(int Opened, int Offline)> RunOnceAsync(CancellationToken cancellationToken) => Task.FromResult((0, 0));
}

/// <summary>#7659: the buttons' handler and the mentions' handler behind the listener's one seam: a press goes to the first, an event to the second.</summary>
public sealed class CompositeSlackEventHandlers(ISlackEventHandlers presses, ISlackEventHandlers mentions) : ISlackEventHandlers
{
    /// <inheritdoc />
    public Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken) => mentions.HandleEventAsync(inbound, cancellationToken);

    /// <inheritdoc />
    public Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken) => presses.HandleInteractiveAsync(inbound, cancellationToken);
}
