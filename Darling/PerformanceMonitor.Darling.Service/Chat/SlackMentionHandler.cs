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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
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

    /// <summary>PURE: whether an event of <paramref name="eventType"/> with <paramref name="text"/> is a mention of the bot <paramref name="botUserId"/>.
    /// An <c>app_mention</c> is one by its type; a <c>message</c> is one only when its text carries the bot's token.</summary>
    public static bool IsMention(string? eventType, string? text, string? botUserId) =>
        string.Equals(eventType, AppMention, StringComparison.Ordinal)
        || (string.Equals(eventType, Message, StringComparison.Ordinal) && !string.IsNullOrWhiteSpace(botUserId) && BotToken(botUserId).IsMatch(text ?? ""));

    /// <summary>PURE: <paramref name="text"/> with every <c>&lt;@bot&gt;</c> token removed, wherever it sits, runs of spaces collapsed and the ends trimmed.</summary>
    public static string StripBotMentions(string? text, string? botUserId)
    {
        var value = text ?? "";
        if (string.IsNullOrWhiteSpace(botUserId))
        {
            return value.Trim();
        }

        var stripped = BotToken(botUserId).Replace(value, " ");
        return s_spaces.Replace(stripped, " ").Trim();
    }

    /// <summary>PURE: <paramref name="text"/> held to <see cref="SlackChatRules.TextLimit"/> characters, never cut inside a surrogate pair.</summary>
    public static string Bound(string text) => SlackChatRules.Bound(text);

    /// <summary>PURE: the one rule over the newest request of the thread.</summary>
    public static SlackChatRule Decide(SlackChatNewest? newest)
    {
        if (newest is null)
        {
            return SlackChatRule.Open;
        }

        if (newest.State == "open")
        {
            return newest.TriggerKind == InvestigationTriggerKinds.Chat ? SlackChatRule.Join : SlackChatRule.Wait;
        }

        return newest.State == "claimed" ? SlackChatRule.Wait : SlackChatRule.Open;
    }

    /// <summary>PURE: whether a thread whose first request opened at <paramref name="openedAtUtc"/> is past the age limit at <paramref name="nowUtc"/>.</summary>
    public static bool IsTooOld(DateTime openedAtUtc, DateTime nowUtc) => nowUtc - openedAtUtc > SlackChatRules.ThreadAgeLimit;

    private static readonly Regex s_spaces = new(@"[ \t]{2,}", RegexOptions.Compiled);

    /* The bot's mention as Slack writes it in message text: <@U123> or <@U123|name>. The id is escaped, so a token for another user never matches. */
    private static Regex BotToken(string botUserId) => new("<@" + Regex.Escape(botUserId.Trim()) + @"(\|[^>]*)?>", RegexOptions.CultureInvariant);

    /// <summary>Presses are not this handler's: the buttons (#7658) take them.</summary>
    public Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public async Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(inbound);
        var text = Text(inbound.Event, "text");
        if (Text(inbound.Event, "subtype") is not null || string.IsNullOrWhiteSpace(inbound.UserId) || !IsMention(inbound.EventType, text, inbound.BotUserId))
        {
            return;
        }

        /* The listener's channel rule, checked again here: a thread this store does not own stays silent. */
        var route = _routes().FirstOrDefault(r => r.Enabled && string.Equals((r.SlackBotChannel ?? "").Trim(), inbound.Channel, StringComparison.Ordinal));
        if (route is null)
        {
            _logger.LogDebug("Slack mention in a channel no route of this store names; nothing is done");
            return;
        }

        var now = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        var identity = "slack:" + inbound.TeamId + ":" + inbound.UserId;
        var threadTs = inbound.ThreadTs ?? inbound.Ts;
        var thread = inbound.ThreadTs is null ? null : await _store.FindThreadAsync(inbound.Channel, inbound.ThreadTs, cancellationToken);
        if (thread is null)
        {
            /* Outside a request thread: answered once per thread (a top-level mention threads under itself), journaled with no text and no request. */
            await SlackChatAnswering.AnswerOnceAsync(_store, _posts, _logger, null, route.RouteId, null, SlackChatAnswers.OutsideKind, SlackChatAnswers.OutsideThread,
                inbound.Channel, threadTs, "slack_thread:none:" + inbound.Channel + ":" + threadTs + ":" + InvestigationRequestStore.ChatAnswerKey(SlackChatAnswers.OutsideKind), cancellationToken);
            await JournalAsync(null, route.RouteId, null, identity, SlackChatAnswers.OutsideKind, inbound, identity, cancellationToken);
            return;
        }

        if (IsTooOld(thread.OpenedAtUtc, now))
        {
            await AnswerOnceAsync(thread, SlackChatAnswers.OldKind, SlackChatAnswers.OldRequest, InvestigationRequestStore.ChatAnswerKey(SlackChatAnswers.OldKind), cancellationToken);
            await JournalAsync(thread.ThreadRequestId, thread.RouteId, thread.ServerId, identity, SlackChatAnswers.OldKind, inbound, identity, cancellationToken);
            return;
        }

        var person = await _store.FindSlackPersonAsync(inbound.TeamId, inbound.UserId, cancellationToken);
        var level = person is { Enabled: true } ? await _store.RequestLevelAsync(person.PersonId, thread.ServerId, cancellationToken) : 0;
        if (person is null || level < 1)
        {
            /* No ask grant: one answer per thread and identity, and a journal row with no text that names nobody as an asker. */
            var actor = person is null ? identity : "person:" + person.PersonId.ToString(CultureInfo.InvariantCulture);
            await AnswerOnceAsync(thread, SlackChatAnswers.NoGrantKind, SlackChatAnswers.NoGrant, InvestigationRequestStore.ChatAnswerKey(SlackChatAnswers.NoGrantKind + ":" + identity), cancellationToken);
            await JournalAsync(thread.ThreadRequestId, thread.RouteId, thread.ServerId, actor, SlackChatAnswers.NoGrantKind, inbound, identity, cancellationToken);
            return;
        }

        var newest = await _store.NewestRequestAsync(thread, cancellationToken);
        var message = new SlackPersonMessage(inbound.Channel, inbound.Ts, inbound.ThreadTs!, thread.ThreadRequestId, person.PersonId, identity,
            Bound(StripBotMentions(text, inbound.BotUserId)));
        if (!await _store.SaveMessageAsync(message, cancellationToken))
        {
            /* The same mention again (app_mention and message, or Slack's retry on the other connection): the first one acted. */
            return;
        }

        var asker = new SlackAsker(person.PersonId, identity, level);
        switch (Decide(newest))
        {
            case SlackChatRule.Join:
                if (await _store.JoinChatRequestAsync(newest!.RequestId, asker, message, cancellationToken))
                {
                    return;
                }

                /* The chat request was claimed between the read and the join: the message waits for it, and the sweep opens the next one. */
                await AnswerOnceAsync(thread, SlackChatAnswers.WaitingKind, SlackChatAnswers.Waiting, WaitingKey(newest.RequestId), cancellationToken);
                return;
            case SlackChatRule.Wait:
                await AnswerOnceAsync(thread, SlackChatAnswers.WaitingKind, SlackChatAnswers.Waiting, WaitingKey(newest!.RequestId), cancellationToken);
                return;
            default:
                await SlackChatAnswering.OpenAsync(_store, _posts, _logger, route, thread, newest?.RequestId ?? thread.ThreadRequestId, [asker], [message], now,
                    answerCaps: true, cancellationToken);
                return;
        }
    }

    /// <summary>The acknowledgement's key: once per request in play, so a later request in the same thread gets its own first acknowledgement.</summary>
    private static string WaitingKey(Guid inPlay) => InvestigationRequestStore.ChatAnswerKey(SlackChatAnswers.WaitingKind + ":" + inPlay.ToString("D"));

    private Task<bool> AnswerOnceAsync(SlackChatThread thread, string kind, string text, string answerKey, CancellationToken cancellationToken) =>
        SlackChatAnswering.AnswerOnceAsync(_store, _posts, _logger, thread.ThreadRequestId, thread.RouteId, thread.ServerId, kind, text, thread.ChannelId, thread.ThreadTs,
            SlackThreadPosts.OnceKey(thread.ThreadRequestId, answerKey), cancellationToken, answerKey);

    /* The no-text journal row of a mention that opened nothing: the identity, where it was, and why; never the text, and never a person_id, since
       the asker check reads that key. */
    private Task JournalAsync(Guid? requestId, int? routeId, int? serverId, string actor, string outcome, SlackInboundEvent inbound, string identity, CancellationToken cancellationToken) =>
        _store.JournalPersonMessageAsync(requestId, routeId, serverId, actor, outcome, new JsonObject
        {
            ["identity"] = identity,
            ["channel"] = inbound.Channel,
            ["ts"] = inbound.Ts,
            ["thread_ts"] = inbound.ThreadTs,
            ["event"] = inbound.EventType,
            ["outcome"] = outcome,
        }, InvestigationRequestStore.PersonMessageOnceKey(inbound.Channel, inbound.Ts), cancellationToken);

    private static string? Text(JsonObject node, string name) =>
        node[name] is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;
}

/// <summary>#7659: what the mention handler and the sweep share: a once-marked answer in a thread, and the open of a chat request under the caps.</summary>
internal static class SlackChatAnswering
{
    /// <summary>Marks the answer once (the mark is written before the send, as every bot post is) and posts it as a reply in the thread. False when
    /// another pass or service already answered.</summary>
    public static async Task<bool> AnswerOnceAsync(ISlackChatStore store, ISlackBotPosts posts, ILogger logger, Guid? requestId, int? routeId, int? serverId,
        string kind, string text, string channel, string threadTs, string onceKey, CancellationToken cancellationToken, string? answerKey = null)
    {
        answerKey ??= InvestigationRequestStore.ChatAnswerKey(kind);
        if (!await store.MarkChatAnswerOnceAsync(requestId, routeId, serverId, answerKey, onceKey, cancellationToken))
        {
            return false;
        }

        var result = await posts.PostAsync(channel, text, [new { type = "section", text = new { type = "mrkdwn", text } }], threadTs, cancellationToken);
        if (!result.Ok)
        {
            logger.LogWarning("Slack chat answer {Kind} was not delivered: {Error}", kind, result.Error);
        }

        return true;
    }

    /// <summary>Opens a chat request for <paramref name="messages"/>: the caps first (over one, the messages wait and, with <paramref name="answerCaps"/>,
    /// the bot says so once an hour), then the store's open under the route lock, which checks them again. An open wakes the agents' long poll.</summary>
    public static async Task<SlackChatOpenOutcome> OpenAsync(ISlackChatStore store, ISlackBotPosts posts, ILogger logger, NotificationRoute route, SlackChatThread thread,
        Guid parentRequestId, IReadOnlyList<SlackAsker> askers, IReadOnlyList<SlackPersonMessage> messages, DateTime now, bool answerCaps, CancellationToken cancellationToken)
    {
        var counts = await store.ChatCountsAsync(thread.ThreadRequestId, thread.RouteId, now, cancellationToken);
        var outcome = counts.ThreadLastHour >= SlackChatRules.PerThreadPerHour ? SlackChatOpenOutcome.ThreadCap
            : counts.RouteLastHour >= route.AgentMaxPerHour ? SlackChatOpenOutcome.RouteCap
            : (SlackChatOpenOutcome?)null;
        if (outcome is null)
        {
            var result = await store.OpenChatRequestAsync(thread, parentRequestId, askers, messages, route.AgentMaxPerHour, now, cancellationToken);
            outcome = result.Outcome;
            if (outcome == SlackChatOpenOutcome.Opened)
            {
                InvestigationOpenSignal.Shared.WakeAll();
                logger.LogInformation("Slack mention opened chat request {RequestId} in the thread of request {ThreadRequestId} with {Askers} asker(s) and {Messages} message(s)",
                    result.RequestId, thread.ThreadRequestId, askers.Count, messages.Count);
            }
            else if (outcome == SlackChatOpenOutcome.ParentGone)
            {
                logger.LogWarning("Slack mention could not open a chat request: the parent request {Parent} is gone", parentRequestId);
            }
        }

        if (answerCaps && outcome is SlackChatOpenOutcome.ThreadCap or SlackChatOpenOutcome.RouteCap)
        {
            var kind = outcome == SlackChatOpenOutcome.ThreadCap ? SlackChatAnswers.OverCapKind : SlackChatAnswers.RouteCapKind;
            var text = outcome == SlackChatOpenOutcome.ThreadCap ? SlackChatAnswers.OverCap : SlackChatAnswers.RouteCap;
            var answerKey = InvestigationRequestStore.ChatAnswerKey(kind + ":" + now.ToString("yyyyMMddHH", CultureInfo.InvariantCulture));
            await AnswerOnceAsync(store, posts, logger, thread.ThreadRequestId, thread.RouteId, thread.ServerId, kind, text, thread.ChannelId, thread.ThreadTs,
                SlackThreadPosts.OnceKey(thread.ThreadRequestId, answerKey), cancellationToken, answerKey);
        }

        return outcome.Value;
    }
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

    /// <summary>One sweep. Answers how many chat requests opened and how many offline answers posted. Throws what the store throws.</summary>
    public async Task<(int Opened, int Offline)> RunOnceAsync(CancellationToken cancellationToken)
    {
        var now = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        var routes = _routes();
        var opened = 0;
        foreach (var waiting in await _store.WaitingThreadsAsync(BatchSize, cancellationToken))
        {
            var route = routes.FirstOrDefault(r => r.RouteId == waiting.RouteId);
            if (route is null || !route.Enabled)
            {
                continue;
            }

            var thread = new SlackChatThread(waiting.ThreadRequestId, waiting.RouteId, waiting.ServerId, waiting.ChannelId, waiting.ThreadTs, now);
            var newest = await _store.NewestRequestAsync(thread, cancellationToken);
            var rule = SlackMentionHandler.Decide(newest);
            if (rule == SlackChatRule.Wait)
            {
                continue;
            }

            var messages = await _store.WaitingMessagesAsync(thread.ThreadRequestId, cancellationToken);
            if (messages.Count == 0)
            {
                continue;
            }

            /* Each asker's level is read now: a grant removed while the message waited counts, as a join reads the live grant. */
            var askers = new List<SlackAsker>();
            foreach (var person in messages.Where(m => m.PersonId is not null).Select(m => (Id: m.PersonId!.Value, m.Identity)).DistinctBy(p => p.Id))
            {
                askers.Add(new SlackAsker(person.Id, person.Identity, await _store.RequestLevelAsync(person.Id, thread.ServerId, cancellationToken)));
            }

            if (rule == SlackChatRule.Join)
            {
                foreach (var message in messages)
                {
                    var asker = askers.FirstOrDefault(a => a.PersonId == message.PersonId);
                    if (asker is not null)
                    {
                        await _store.JoinChatRequestAsync(newest!.RequestId, asker, message, cancellationToken);
                    }
                }

                continue;
            }

            if (await SlackChatAnswering.OpenAsync(_store, _posts, _logger, route, thread, newest?.RequestId ?? thread.ThreadRequestId, askers, messages, now,
                    answerCaps: false, cancellationToken) == SlackChatOpenOutcome.Opened)
            {
                opened++;
            }
        }

        var offline = 0;
        foreach (var chat in await _store.UnclaimedChatRequestsAsync(now - SlackChatRules.OfflineAfter, cancellationToken))
        {
            if (await SlackChatAnswering.AnswerOnceAsync(_store, _posts, _logger, chat.RequestId, chat.RouteId, chat.ServerId, SlackChatAnswers.OfflineKind, SlackChatAnswers.Offline,
                    chat.ChannelId, chat.ThreadTs, SlackThreadPosts.OnceKey(chat.RequestId, InvestigationRequestStore.ChatAnswerKey(SlackChatAnswers.OfflineKind)), cancellationToken))
            {
                offline++;
            }
        }

        return (opened, offline);
    }
}

/// <summary>#7659: the buttons' handler and the mentions' handler behind the listener's one seam: a press goes to the first, an event to the second.</summary>
public sealed class CompositeSlackEventHandlers(ISlackEventHandlers presses, ISlackEventHandlers mentions) : ISlackEventHandlers
{
    /// <inheritdoc />
    public Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken) => mentions.HandleEventAsync(inbound, cancellationToken);

    /// <inheritdoc />
    public Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken) => presses.HandleInteractiveAsync(inbound, cancellationToken);
}
