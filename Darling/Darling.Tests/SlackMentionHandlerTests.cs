/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7659: the mention handler over fake events, a fake chat store, recorded posts and a fixed clock, and the chat sweep over the same fakes. Only a
/// message that mentions the bot counts; the one rule (join, wait, open) runs over the newest request of the thread; every refusal and
/// acknowledgement is one sentence, posted once; the message text reaches the store with every bot token removed and bounded; a foreign workspace
/// is dropped by the listener's filter before any handler; a channel no route of this store names stays silent.
/// </summary>
public sealed class SlackMentionHandlerTests
{
    private const string Team = "T0TEAM";
    private const string App = "A0APP";
    private const string Bot = "U0BOT";
    private const string User = "U0PERSON";
    private const string Channel = "C0BOT";
    private const string ThreadTs = "1700000000.000100";
    private const int RouteId = 7;
    private const int ServerId = 3;
    private static readonly Guid First = Guid.Parse("76590000-0000-0000-0000-0000000000a1");
    private static readonly Guid Newest = Guid.Parse("76590000-0000-0000-0000-0000000000a2");
    private static readonly Guid Opened = Guid.Parse("76590000-0000-0000-0000-0000000000a3");
    private static readonly DateTime Now = new(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);
    private static readonly SlackPerson Erik = new(7, 70, "Erik", true);

    private sealed class FakeChatStore : ISlackChatStore
    {
        public Dictionary<string, SlackChatThread> Threads { get; } = new(StringComparer.Ordinal);
        public SlackChatNewest? Newest { get; set; }
        public Dictionary<string, SlackPerson> People { get; } = new(StringComparer.Ordinal);
        public Dictionary<long, int> Levels { get; } = [];
        public SlackChatCounts Counts { get; set; } = new(0, 0);
        public HashSet<string> Saved { get; } = new(StringComparer.Ordinal);
        public List<SlackPersonMessage> Messages { get; } = [];
        public List<(Guid Request, SlackAsker Asker, SlackPersonMessage Message)> Joins { get; } = [];
        public bool JoinAnswer { get; set; } = true;
        public List<(SlackChatThread Thread, Guid Parent, IReadOnlyList<SlackAsker> Askers, IReadOnlyList<SlackPersonMessage> Messages, int RouteMax, DateTime Now)> Opens { get; } = [];
        public SlackChatOpenOutcome OpenOutcome { get; set; } = SlackChatOpenOutcome.Opened;
        public List<(Guid? Request, int? Route, int? Server, string Actor, string Outcome, JsonObject Detail, string OnceKey)> Journal { get; } = [];
        public HashSet<string> Marks { get; } = new(StringComparer.Ordinal);
        public List<(Guid? Request, string AnswerKey)> MarkCalls { get; } = [];
        public List<SlackWaitingThread> Waiting { get; } = [];
        public Dictionary<Guid, List<SlackPersonMessage>> WaitingMessages { get; } = [];
        public List<SlackUnclaimedChat> Unclaimed { get; } = [];
        public List<DateTime> UnclaimedAskedBefore { get; } = [];
        public List<SlackPersonMessage> Refused { get; } = [];
        public int Calls { get; private set; }

        public Task<SlackChatThread?> FindThreadAsync(string channelId, string threadTs, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(Threads.TryGetValue(channelId + "|" + threadTs, out var t) ? t : null);
        }

        public Task<SlackChatNewest?> NewestRequestAsync(SlackChatThread thread, CancellationToken ct) { Calls++; return Task.FromResult(Newest); }

        public Task<SlackPerson?> FindSlackPersonAsync(string teamId, string userId, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(People.TryGetValue(teamId + ":" + userId, out var p) ? p : null);
        }

        public Task<int> RequestLevelAsync(long personId, int serverId, CancellationToken ct) { Calls++; return Task.FromResult(Levels.TryGetValue(personId, out var l) ? l : 0); }

        public SlackPersonMessageSaved? SaveAnswer { get; set; }

        public Task<SlackPersonMessageSaved> SaveMessageAsync(SlackPersonMessage message, CancellationToken ct)
        {
            Calls++;
            if (SaveAnswer is { } forced)
                return Task.FromResult(forced);
            if (!Saved.Add(message.ChannelId + "|" + message.Ts))
                return Task.FromResult(SlackPersonMessageSaved.Duplicate);
            Messages.Add(message);
            return Task.FromResult(SlackPersonMessageSaved.Saved);
        }

        public Task<bool> JoinChatRequestAsync(Guid requestId, SlackAsker asker, SlackPersonMessage message, CancellationToken ct)
        {
            Calls++;
            Joins.Add((requestId, asker, message));
            return Task.FromResult(JoinAnswer);
        }

        public Task<SlackChatCounts> ChatCountsAsync(Guid threadRequestId, int routeId, DateTime nowUtc, CancellationToken ct) { Calls++; return Task.FromResult(Counts); }

        public Task<SlackChatOpenResult> OpenChatRequestAsync(SlackChatThread thread, Guid parentRequestId, IReadOnlyList<SlackAsker> askers,
            IReadOnlyList<SlackPersonMessage> messages, int routeMaxPerHour, DateTime nowUtc, CancellationToken ct)
        {
            Calls++;
            Opens.Add((thread, parentRequestId, askers, messages, routeMaxPerHour, nowUtc));
            return Task.FromResult(new SlackChatOpenResult(OpenOutcome, OpenOutcome == SlackChatOpenOutcome.Opened ? Opened : null));
        }

        public Task JournalPersonMessageAsync(Guid? requestId, int? routeId, int? serverId, string actor, string outcome, JsonObject detail, string onceKey, CancellationToken ct)
        {
            Calls++;
            if (Journal.All(j => j.OnceKey != onceKey))
                Journal.Add((requestId, routeId, serverId, actor, outcome, detail, onceKey));
            return Task.CompletedTask;
        }

        public Task<bool> MarkChatAnswerOnceAsync(Guid? requestId, int? routeId, int? serverId, string answerKey, string onceKey, CancellationToken ct)
        {
            Calls++;
            MarkCalls.Add((requestId, answerKey));
            return Task.FromResult(Marks.Add(onceKey));
        }

        public Task<IReadOnlyList<SlackWaitingThread>> WaitingThreadsAsync(int limit, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<SlackWaitingThread>>(Waiting.Take(limit).ToList());

        public Task<IReadOnlyList<SlackPersonMessage>> WaitingMessagesAsync(Guid threadRequestId, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<SlackPersonMessage>>(WaitingMessages.TryGetValue(threadRequestId, out var m) ? m.ToList() : []);

        public Task RefuseMessageAsync(SlackPersonMessage message, CancellationToken ct)
        {
            Calls++;
            Refused.Add(message);
            /* A refused message no longer waits: the next pass's reads leave it, and a thread with nothing left waiting, out. */
            if (WaitingMessages.TryGetValue(message.ThreadRequestId, out var list))
            {
                list.RemoveAll(m => m.Ts == message.Ts);
                if (list.Count == 0)
                    Waiting.RemoveAll(w => w.ThreadRequestId == message.ThreadRequestId);
            }

            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<SlackUnclaimedChat>> UnclaimedChatRequestsAsync(DateTime openedBeforeUtc, CancellationToken ct)
        {
            UnclaimedAskedBefore.Add(openedBeforeUtc);
            return Task.FromResult<IReadOnlyList<SlackUnclaimedChat>>(Unclaimed.Where(u => u.OpenedAtUtc <= openedBeforeUtc).ToList());
        }
    }

    private sealed class RecordingPosts : ISlackBotPosts
    {
        public List<(string Channel, string Text, string? ThreadTs)> Posts { get; } = [];

        public Task<SlackPostResult> PostAsync(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs, CancellationToken ct)
        {
            Posts.Add((channel, text, threadTs));
            return Task.FromResult(new SlackPostResult(true, "1700000000.000900", channel, null));
        }
    }

    private static IReadOnlyList<NotificationRoute> Routes(int maxPerHour = 10) =>
        [new NotificationRoute(RouteId, "Blocking", "", "", "", "", "", Enabled: true) { SlackBotChannel = Channel, AgentMaxPerHour = maxPerHour }];

    private static SlackChatThread Thread(DateTime? openedAt = null) => new(First, RouteId, ServerId, Channel, ThreadTs, openedAt ?? Now.AddHours(-2));

    private static SlackInboundEvent Event(string type, string text, string ts = "1700000000.000200", string? threadTs = ThreadTs, string channel = Channel,
        string user = User, string? subtype = null, string? botUserId = Bot)
    {
        var ev = new JsonObject { ["type"] = type, ["text"] = text, ["user"] = user, ["ts"] = ts, ["channel"] = channel };
        if (threadTs is not null)
            ev["thread_ts"] = threadTs;
        if (subtype is not null)
            ev["subtype"] = subtype;
        var payload = new JsonObject { ["team_id"] = Team, ["api_app_id"] = App, ["event"] = ev.DeepClone() };
        return new SlackInboundEvent(Team, App, type, channel, ts, threadTs, user, ev, payload) { BotUserId = botUserId };
    }

    private static (SlackMentionHandler Handler, FakeChatStore Store, RecordingPosts Posts) Handler(int maxPerHour = 10)
    {
        var store = new FakeChatStore();
        var posts = new RecordingPosts();
        store.People[Team + ":" + User] = Erik;
        store.Levels[Erik.PersonId] = 1;
        return (new SlackMentionHandler(store, posts, () => Routes(maxPerHour), NullLogger.Instance, () => Now), store, posts);
    }

    /* ───────── pure ───────── */

    [Fact]
    public void StripBotMentions_RemovesEveryTokenOfTheBot_WhereverItSits_AndKeepsOtherPeoples()
    {
        Assert.Equal("please look at this cc", SlackMentionHandler.StripBotMentions("<@U0BOT> please look at this cc <@U0BOT|darling>", Bot));
        Assert.Equal("look at <@U0OTHER> and this", SlackMentionHandler.StripBotMentions("look at <@U0OTHER> and <@U0BOT> this", Bot));
        Assert.Equal("the plan 'x' is forced", SlackMentionHandler.StripBotMentions("  the plan <@U0BOT> 'x' is forced  ", Bot));
        Assert.Equal("", SlackMentionHandler.StripBotMentions(null, Bot));
        Assert.Equal("", SlackMentionHandler.StripBotMentions("<@U0BOT>", Bot));
        Assert.Equal("<@U0BOT> kept", SlackMentionHandler.StripBotMentions("<@U0BOT> kept", null));
    }

    [Fact]
    public void Bound_HoldsTheTextTo4000Characters_AndNeverCutsASurrogatePair()
    {
        var long1 = new string('a', 4500);
        Assert.Equal(4000, SlackMentionHandler.Bound(long1).Length);
        var pair = new string('a', 3999) + "\U0001F600" + new string('b', 10);
        var bounded = SlackMentionHandler.Bound(pair);
        Assert.Equal(3999, bounded.Length);
        Assert.Equal(new string('a', 10), SlackMentionHandler.Bound(new string('a', 10)));
        Assert.Equal(SlackChatRules.TextLimit, 4000);
    }

    [Fact]
    public void IsMention_IsAnAppMention_OrAMessageThatNamesTheBot()
    {
        Assert.True(SlackMentionHandler.IsMention(SlackMentionHandler.AppMention, "anything", Bot));
        Assert.True(SlackMentionHandler.IsMention(SlackMentionHandler.AppMention, "anything", null));
        Assert.True(SlackMentionHandler.IsMention(SlackMentionHandler.Message, "cc <@U0BOT>", Bot));
        Assert.True(SlackMentionHandler.IsMention(SlackMentionHandler.Message, "<@U0BOT|darling> hi", Bot));
        Assert.False(SlackMentionHandler.IsMention(SlackMentionHandler.Message, "thanks everyone", Bot));
        Assert.False(SlackMentionHandler.IsMention(SlackMentionHandler.Message, "<@U0OTHER> look", Bot));
        Assert.False(SlackMentionHandler.IsMention(SlackMentionHandler.Message, "<@U0BOT> hi", null));
        Assert.False(SlackMentionHandler.IsMention("reaction_added", "<@U0BOT>", Bot));
    }

    [Fact]
    public void Decide_FollowsTheNewestRequestOfTheThread()
    {
        Assert.Equal(SlackChatRule.Open, SlackMentionHandler.Decide(null));
        Assert.Equal(SlackChatRule.Join, SlackMentionHandler.Decide(new SlackChatNewest(Newest, "open", InvestigationTriggerKinds.Chat, Now)));
        Assert.Equal(SlackChatRule.Wait, SlackMentionHandler.Decide(new SlackChatNewest(Newest, "claimed", InvestigationTriggerKinds.Chat, Now)));
        Assert.Equal(SlackChatRule.Wait, SlackMentionHandler.Decide(new SlackChatNewest(Newest, "open", InvestigationTriggerKinds.Alert, Now)));
        Assert.Equal(SlackChatRule.Wait, SlackMentionHandler.Decide(new SlackChatNewest(Newest, "claimed", InvestigationTriggerKinds.Alert, Now)));
        Assert.Equal(SlackChatRule.Open, SlackMentionHandler.Decide(new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now)));
        Assert.Equal(SlackChatRule.Open, SlackMentionHandler.Decide(new SlackChatNewest(Newest, "closed", InvestigationTriggerKinds.Chat, Now)));
        Assert.Equal(SlackChatRule.Open, SlackMentionHandler.Decide(new SlackChatNewest(Newest, "expired", InvestigationTriggerKinds.Alert, Now)));
    }

    [Fact]
    public void IsTooOld_IsPastSevenDays()
    {
        Assert.False(SlackMentionHandler.IsTooOld(Now.AddDays(-6).AddHours(-23), Now));
        Assert.False(SlackMentionHandler.IsTooOld(Now.AddDays(-7), Now));
        Assert.True(SlackMentionHandler.IsTooOld(Now.AddDays(-7).AddSeconds(-1), Now));
        Assert.Equal(TimeSpan.FromDays(7), SlackChatRules.ThreadAgeLimit);
    }

    /* ───────── the handler ───────── */

    [Fact]
    public async Task AMentionWithTheAskGrant_WhoseThreadsNewestRequestHasEnded_OpensALinkedChatRequest_WithTheStrippedText()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddHours(-1));

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> why is CPU high on this one? cc <@U0BOT>"), CancellationToken.None);

        var open = Assert.Single(store.Opens);
        Assert.Equal((First, Newest, 10, Now), (open.Thread.ThreadRequestId, open.Parent, open.RouteMax, open.Now));
        var asker = Assert.Single(open.Askers);
        Assert.Equal((Erik.PersonId, "slack:" + Team + ":" + User, 1), (asker.PersonId, asker.Identity, asker.RequestLevel));
        var message = Assert.Single(open.Messages);
        Assert.Equal("why is CPU high on this one? cc", message.Text);
        Assert.Equal((Channel, "1700000000.000200", ThreadTs, First, Erik.PersonId), (message.ChannelId, message.Ts, message.ThreadTs, message.ThreadRequestId, message.PersonId));
        Assert.Single(store.Messages);
        Assert.Empty(store.Joins);
        Assert.Empty(posts.Posts);
    }

    [Fact]
    public async Task AMention_WhileTheNewestRequestIsAnUnclaimedChatRequest_JoinsIt_AsAnAskerAtTheirLevel()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = new SlackChatNewest(Newest, "open", InvestigationTriggerKinds.Chat, Now.AddMinutes(-1));
        store.Levels[Erik.PersonId] = 2;

        await handler.HandleEventAsync(Event(SlackMentionHandler.Message, "<@U0BOT> and the blocking too"), CancellationToken.None);

        var join = Assert.Single(store.Joins);
        Assert.Equal((Newest, Erik.PersonId, 2, "and the blocking too"), (join.Request, join.Asker.PersonId, join.Asker.RequestLevel, join.Message.Text));
        Assert.Empty(store.Opens);
        Assert.Empty(posts.Posts);
    }

    [Fact]
    public async Task AMentionFromAPersonWithoutTheGrant_IsAnsweredOnce_AndJournaledWithNoTextAndNoAsker()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddHours(-1));
        store.Levels[Erik.PersonId] = 0;

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> the password is hunter2", ts: "1700000000.000201"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hello again", ts: "1700000000.000202"), CancellationToken.None);

        var post = Assert.Single(posts.Posts);
        Assert.Equal((Channel, SlackChatAnswers.NoGrant, ThreadTs), post);
        /* The second mention within the hour from the same person is a repeat: no second answer and no second journal row. */
        Assert.Single(store.Journal);
        foreach (var row in store.Journal)
        {
            Assert.Equal((First, RouteId, ServerId, "person:7", SlackChatAnswers.NoGrantKind), (row.Request, row.Route, row.Server, row.Actor, row.Outcome));
            Assert.Equal("slack:" + Team + ":" + User, row.Detail["identity"]!.ToString());
            Assert.Null(row.Detail["text"]);
            Assert.Null(row.Detail["person_id"]);
            Assert.DoesNotContain("hunter2", row.Detail.ToJsonString(), StringComparison.Ordinal);
        }

        Assert.Empty(store.Messages);
        Assert.Empty(store.Opens);
        Assert.Empty(store.Joins);
    }

    [Fact]
    public async Task AMentionFromAnUnmappedOrDisabledPerson_IsAnsweredAsNoGrant_WithTheIdentityAsTheActor()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.People.Clear();

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi"), CancellationToken.None);
        Assert.Equal(SlackChatAnswers.NoGrant, Assert.Single(posts.Posts).Text);
        Assert.Equal("slack:" + Team + ":" + User, Assert.Single(store.Journal).Actor);

        store.People[Team + ":" + User] = Erik with { Enabled = false };
        store.Levels[Erik.PersonId] = 3;
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", ts: "1700000000.000203", channel: Channel), CancellationToken.None);
        Assert.Single(posts.Posts);
        Assert.Single(store.Journal);
        Assert.Empty(store.Opens);
    }

    [Fact]
    public async Task AMentionOutsideARequestThread_IsAnsweredOnce_InThatThread_AndOpensNothing()
    {
        var (handler, store, posts) = Handler();

        /* A top-level mention: the answer threads under the message itself. */
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", ts: "1700000000.000300", threadTs: null), CancellationToken.None);
        Assert.Equal((Channel, SlackChatAnswers.OutsideThread, "1700000000.000300"), Assert.Single(posts.Posts));
        var row = Assert.Single(store.Journal);
        Assert.Equal((null, SlackChatAnswers.OutsideKind), (row.Request, row.Outcome));
        Assert.Null(row.Detail["text"]);

        /* More mentions outside request threads from the same person within the hour, top-level or in a thread this store did not open: no second
           answer and no second journal row, so a channel member cannot drive posts or journal growth by repeating. */
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", ts: "1700000000.000401", threadTs: "1700000000.000400"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi again", ts: "1700000000.000402", threadTs: "1700000000.000400"), CancellationToken.None);
        for (var i = 0; i < 5; i++)
            await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> spam", ts: "1700000000.00050" + i, threadTs: null), CancellationToken.None);
        Assert.Single(posts.Posts);
        Assert.Single(store.Journal);
        Assert.Empty(store.Messages);
        Assert.Empty(store.Opens);

        /* Another person is answered on their own account, and the next hour starts the first person's count again. */
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", ts: "1700000000.000600", threadTs: null, user: "U0OTHER"), CancellationToken.None);
        Assert.Equal(2, posts.Posts.Count);
        Assert.Equal(2, store.Journal.Count);
        var later = new SlackMentionHandler(store, posts, () => Routes(), NullLogger.Instance, () => Now.AddHours(1));
        await later.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", ts: "1700000000.000700", threadTs: null), CancellationToken.None);
        Assert.Equal(3, posts.Posts.Count);
        Assert.Equal(3, store.Journal.Count);
    }

    [Fact]
    public async Task AMentionOnAThreadWhoseFirstRequestIsOlderThanSevenDays_IsAnsweredOnce_AndOpensNothing()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread(Now.AddDays(-8));
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddDays(-8));

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", ts: "1700000000.000501"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", ts: "1700000000.000502"), CancellationToken.None);

        Assert.Equal((Channel, SlackChatAnswers.OldRequest, ThreadTs), Assert.Single(posts.Posts));
        var old = Assert.Single(store.Journal);
        Assert.Equal((First, SlackChatAnswers.OldKind), (old.Request, old.Outcome));
        Assert.Empty(store.Messages);
        Assert.Empty(store.Opens);
        Assert.Empty(store.Joins);
    }

    [Fact]
    public async Task WhileTheNewestRequestIsInPlay_TheMessageWaits_AndOnlyTheFirstWaitingMentionIsAcknowledged()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = new SlackChatNewest(Newest, "claimed", InvestigationTriggerKinds.Alert, Now.AddMinutes(-3));

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> first", ts: "1700000000.000601"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> second", ts: "1700000000.000602"), CancellationToken.None);

        Assert.Equal((Channel, SlackChatAnswers.Waiting, ThreadTs), Assert.Single(posts.Posts));
        Assert.Equal(2, store.Messages.Count);
        Assert.All(store.Messages, m => Assert.Equal(SlackPersonMessageStates.Waiting, m.State));
        Assert.Equal(new[] { "first", "second" }, store.Messages.Select(m => m.Text).ToArray());
        Assert.Empty(store.Opens);
        Assert.Empty(store.Joins);
        /* The acknowledgement is marked under the request in play, so a later request in the same thread gets its own first acknowledgement. */
        Assert.Contains(store.MarkCalls, m => m.Request == First && m.AnswerKey.Contains(Newest.ToString("D"), StringComparison.Ordinal));
    }

    [Fact]
    public async Task OverTheThreadCapOfSixAnHour_TheMessageWaits_AndTheBotSaysSoOnce()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Chat, Now.AddMinutes(-3));
        store.Counts = new SlackChatCounts(SlackChatRules.PerThreadPerHour, 6);

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> one more", ts: "1700000000.000701"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> and another", ts: "1700000000.000702"), CancellationToken.None);

        Assert.Equal((Channel, SlackChatAnswers.OverCap, ThreadTs), Assert.Single(posts.Posts));
        Assert.Equal(2, store.Messages.Count);
        Assert.All(store.Messages, m => Assert.Equal(SlackPersonMessageStates.Waiting, m.State));
        Assert.Empty(store.Opens);
        Assert.Equal(6, SlackChatRules.PerThreadPerHour);
    }

    [Fact]
    public async Task OverTheRoutesHourlyCap_TheMessageWaits_AndTheBotSaysSoOnce()
    {
        var (handler, store, posts) = Handler(maxPerHour: 4);
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = new SlackChatNewest(Newest, "expired", InvestigationTriggerKinds.Alert, Now.AddMinutes(-30));
        store.Counts = new SlackChatCounts(1, 4);

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> please", ts: "1700000000.000801"), CancellationToken.None);

        Assert.Equal((Channel, SlackChatAnswers.RouteCap, ThreadTs), Assert.Single(posts.Posts));
        Assert.Equal(SlackPersonMessageStates.Waiting, Assert.Single(store.Messages).State);
        Assert.Empty(store.Opens);
    }

    [Fact]
    public async Task OverTheWaitingCaps_TheMentionIsRefusedWithOneSentenceOnceAnHour_AndNothingIsOpenedOrJoined()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = new SlackChatNewest(Newest, "claimed", InvestigationTriggerKinds.Alert, Now.AddMinutes(-3));
        store.SaveAnswer = SlackPersonMessageSaved.PersonFull;

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> sixth", ts: "1700000000.000801"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> seventh", ts: "1700000000.000802"), CancellationToken.None);
        store.SaveAnswer = SlackPersonMessageSaved.ThreadFull;
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> eighth", ts: "1700000000.000803"), CancellationToken.None);

        Assert.Equal((Channel, SlackChatAnswers.WaitingFull, ThreadTs), Assert.Single(posts.Posts));
        var row = Assert.Single(store.Journal);
        Assert.Equal((First, SlackChatAnswers.WaitingFullKind), (row.Request, row.Outcome));
        Assert.Null(row.Detail["text"]);
        Assert.Empty(store.Messages);
        Assert.Empty(store.Opens);
        Assert.Empty(store.Joins);
        Assert.Equal((20, 5), (SlackChatRules.WaitingPerThread, SlackChatRules.WaitingPerPerson));
    }

    [Fact]
    public async Task TheSameMentionArrivingTwice_AsAnAppMentionAndAsAMessage_SavesOneMessageAndOpensOnce()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = null;

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> same", ts: "1700000000.000901"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.Message, "<@U0BOT> same", ts: "1700000000.000901"), CancellationToken.None);

        Assert.Single(store.Messages);
        Assert.Single(store.Opens);
        Assert.Empty(posts.Posts);
    }

    [Fact]
    public async Task AMessageWithASubtype_OrWithoutTheBotsToken_OrWithoutAUser_IsIgnored()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();

        await handler.HandleEventAsync(Event(SlackMentionHandler.Message, "<@U0BOT> edited", subtype: "message_changed"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.Message, "thanks all"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.Message, "<@U0OTHER> can you look"), CancellationToken.None);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> broadcast", subtype: "thread_broadcast"), CancellationToken.None);
        await handler.HandleEventAsync(Event("reaction_added", "<@U0BOT>"), CancellationToken.None);
        var noUser = Event(SlackMentionHandler.AppMention, "<@U0BOT> hi");
        await handler.HandleEventAsync(noUser with { UserId = null }, CancellationToken.None);

        Assert.Equal(0, store.Calls);
        Assert.Empty(posts.Posts);
    }

    [Fact]
    public async Task AMentionInAChannelNoRouteOfThisStoreNames_StaysSilent()
    {
        var (handler, store, posts) = Handler();
        store.Threads["C0OTHER|" + ThreadTs] = Thread();

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi", channel: "C0OTHER"), CancellationToken.None);

        Assert.Equal(0, store.Calls);
        Assert.Empty(posts.Posts);
    }

    [Fact]
    public void AnEventFromAnotherWorkspace_IsDroppedByTheListenersFilter_BeforeAnyHandler()
    {
        var payload = new JsonObject
        {
            ["team_id"] = "T0FOREIGN",
            ["api_app_id"] = App,
            ["event"] = new JsonObject { ["type"] = SlackMentionHandler.AppMention, ["text"] = "<@U0BOT> hi", ["user"] = User, ["user_team"] = Team, ["ts"] = "1.2", ["thread_ts"] = ThreadTs, ["channel"] = Channel },
        };
        var verdict = SlackEnvelopeFilter.Judge(SlackEnvelopeFilter.EventsApiType, payload, Team, App, _ => true, out var inbound, out var interaction);
        Assert.Equal(SlackEnvelopeVerdict.DropTeam, verdict);
        Assert.Null(inbound);
        Assert.Null(interaction);

        payload["team_id"] = Team;
        Assert.Equal(SlackEnvelopeVerdict.Accept, SlackEnvelopeFilter.Judge(SlackEnvelopeFilter.EventsApiType, payload, Team, App, _ => true, out inbound, out _));
        Assert.Equal(ThreadTs, inbound!.ThreadTs);
    }

    [Fact]
    public async Task AParentThatIsGone_OpensNothing_AndAnswersNothing()
    {
        var (handler, store, posts) = Handler();
        store.Threads[Channel + "|" + ThreadTs] = Thread();
        store.Newest = null;
        store.OpenOutcome = SlackChatOpenOutcome.ParentGone;

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> gone"), CancellationToken.None);

        Assert.Single(store.Opens);
        Assert.Empty(posts.Posts);
        /* The message is refused, not left waiting for a sweep that could never open it. */
        Assert.Equal("1700000000.000200", Assert.Single(store.Refused).Ts);
        Assert.Empty(store.Journal);
    }

    /// <summary>Routes may share a bot channel; the channel's first enabled route is only the ownership check. Once the thread is found, its own
    /// route gives the cap the open is counted against, and a thread whose own route is gone or disabled stays silent.</summary>
    [Fact]
    public async Task AMention_InAChannelTwoRoutesShare_UsesTheThreadsOwnRoute_ForTheCapAndTheOpen()
    {
        var store = new FakeChatStore();
        var posts = new RecordingPosts();
        store.People[Team + ":" + User] = Erik;
        store.Levels[Erik.PersonId] = 1;
        var routes = new List<NotificationRoute>
        {
            new(RouteId, "Blocking", "", "", "", "", "", Enabled: true) { SlackBotChannel = Channel, AgentMaxPerHour = 10 },
            new(8, "CPU", "", "", "", "", "", Enabled: true) { SlackBotChannel = Channel, AgentMaxPerHour = 4 },
        };
        var handler = new SlackMentionHandler(store, posts, () => routes, NullLogger.Instance, () => Now);
        store.Threads[Channel + "|" + ThreadTs] = new SlackChatThread(First, 8, ServerId, Channel, ThreadTs, Now.AddHours(-2));
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddHours(-1));

        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> hi"), CancellationToken.None);
        Assert.Equal(4, Assert.Single(store.Opens).RouteMax);

        /* The thread route's cap, counted on the thread's route: at it, the route-cap answer and no open. */
        store.Opens.Clear();
        store.Counts = new SlackChatCounts(1, 4);
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> again", ts: "1700000000.000201"), CancellationToken.None);
        Assert.Empty(store.Opens);
        Assert.Equal(SlackChatAnswers.RouteCap, Assert.Single(posts.Posts).Text);

        /* The thread's own route disabled: silent, nothing saved, while the channel's other route still owns the channel. */
        routes[1] = routes[1] with { Enabled = false };
        await handler.HandleEventAsync(Event(SlackMentionHandler.AppMention, "<@U0BOT> third", ts: "1700000000.000202"), CancellationToken.None);
        Assert.Empty(store.Opens);
        Assert.Single(posts.Posts);
        Assert.Equal(2, store.Messages.Count);
    }

    /* ───────── the sweep ───────── */

    private static (SlackChatSweep Sweep, FakeChatStore Store, RecordingPosts Posts) Sweep()
    {
        var store = new FakeChatStore();
        var posts = new RecordingPosts();
        return (new SlackChatSweep(store, posts, () => Routes(), NullLogger.Instance, () => Now), store, posts);
    }

    private static SlackPersonMessage Waiting(string ts, long personId, string text) =>
        new(Channel, ts, ThreadTs, First, personId, "slack:" + Team + ":U" + personId, text, SlackPersonMessageStates.Waiting);

    [Fact]
    public async Task TheSweep_OpensOneChatRequestForTheWaitingMessagesOfAThread_WhoseNewestRequestHasEnded_WithEveryAsker()
    {
        var (sweep, store, posts) = Sweep();
        store.Waiting.Add(new SlackWaitingThread(First, RouteId, ServerId, Channel, ThreadTs, 2, Now.AddHours(-2)));
        store.WaitingMessages[First] = [Waiting("1700000000.000601", 7, "first"), Waiting("1700000000.000602", 8, "second"), Waiting("1700000000.000603", 7, "third")];
        store.Levels[7] = 1;
        store.Levels[8] = 3;
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddMinutes(-10));

        Assert.Equal((1, 0), await sweep.RunOnceAsync(CancellationToken.None));

        var open = Assert.Single(store.Opens);
        Assert.Equal((First, Newest, 10), (open.Thread.ThreadRequestId, open.Parent, open.RouteMax));
        Assert.Equal(3, open.Messages.Count);
        Assert.Equal(new[] { (7L, 1), (8L, 3) }, open.Askers.Select(a => (a.PersonId, a.RequestLevel)).OrderBy(a => a.PersonId).ToArray());
        Assert.Empty(posts.Posts);
    }

    [Fact]
    public async Task TheSweep_LeavesWaitingMessagesAlone_WhileTheNewestRequestIsInPlay_OrOverTheCaps()
    {
        var (sweep, store, _) = Sweep();
        store.Waiting.Add(new SlackWaitingThread(First, RouteId, ServerId, Channel, ThreadTs, 1, Now.AddHours(-2)));
        store.WaitingMessages[First] = [Waiting("1700000000.000601", 7, "first")];
        store.Levels[7] = 1;
        store.Newest = new SlackChatNewest(Newest, "claimed", InvestigationTriggerKinds.Chat, Now.AddMinutes(-1));
        Assert.Equal((0, 0), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Empty(store.Opens);

        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Chat, Now.AddMinutes(-1));
        store.Counts = new SlackChatCounts(6, 0);
        Assert.Equal((0, 0), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Empty(store.Opens);
        Assert.Empty(store.Refused);
    }

    /// <summary>The sweep applies the handler's grant rule before it opens or joins: a waiting message whose asker's live level is 0 by the time the
    /// request ends (the grant removed, or the person disabled) is refused as the handler refuses a mention without the grant, answered once an hour
    /// and journaled without text or person_id, and never opens or joins a chat request at level 0. The other askers' messages go on as before.</summary>
    [Fact]
    public async Task TheSweep_RefusesAWaitingMessageWhoseAskersLiveLevelIsZero_OnceAnHour_AndOpensOrJoinsForTheRest()
    {
        var (sweep, store, posts) = Sweep();
        store.Waiting.Add(new SlackWaitingThread(First, RouteId, ServerId, Channel, ThreadTs, 3, Now.AddHours(-2)));
        store.WaitingMessages[First] = [Waiting("1700000000.000601", 7, "first"), Waiting("1700000000.000602", 8, "the password is hunter2"), Waiting("1700000000.000603", 8, "again")];
        store.Levels[7] = 1;
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddMinutes(-10));

        /* The open arm: person 8 has no live level, so the chat request opens for person 7's message alone. */
        Assert.Equal((1, 0), await sweep.RunOnceAsync(CancellationToken.None));
        var open = Assert.Single(store.Opens);
        Assert.Equal(new[] { "first" }, open.Messages.Select(m => m.Text).ToArray());
        Assert.Equal((7L, 1), (Assert.Single(open.Askers).PersonId, Assert.Single(open.Askers).RequestLevel));
        Assert.Equal(new[] { "1700000000.000602", "1700000000.000603" }, store.Refused.Select(m => m.Ts).ToArray());
        Assert.Equal((Channel, SlackChatAnswers.NoGrant, ThreadTs), Assert.Single(posts.Posts));
        var row = Assert.Single(store.Journal);
        Assert.Equal((First, RouteId, ServerId, "person:8", SlackChatAnswers.NoGrantKind), (row.Request, row.Route, row.Server, row.Actor, row.Outcome));
        Assert.Null(row.Detail["text"]);
        Assert.Null(row.Detail["person_id"]);
        Assert.DoesNotContain("hunter2", row.Detail.ToJsonString(), StringComparison.Ordinal);

        /* The join arm: person 7 joins the unclaimed chat request at their level; person 8's message is refused, never joined at level 0, and the
           same person within the hour gets no second answer. */
        store.Opens.Clear();
        store.Refused.Clear();
        store.WaitingMessages[First] = [Waiting("1700000000.000604", 7, "join me"), Waiting("1700000000.000605", 8, "still revoked")];
        store.Newest = new SlackChatNewest(Newest, "open", InvestigationTriggerKinds.Chat, Now.AddMinutes(-1));
        Assert.Equal((0, 0), await sweep.RunOnceAsync(CancellationToken.None));
        var join = Assert.Single(store.Joins);
        Assert.Equal((Newest, 7L, 1, "join me"), (join.Request, join.Asker.PersonId, join.Asker.RequestLevel, join.Message.Text));
        Assert.Equal("1700000000.000605", Assert.Single(store.Refused).Ts);
        Assert.Empty(store.Opens);
        Assert.Single(posts.Posts);
        Assert.Single(store.Journal);

        /* Nobody granted: nothing opens, and every message is refused. */
        store.Refused.Clear();
        store.Joins.Clear();
        store.WaitingMessages[First] = [Waiting("1700000000.000606", 8, "alone")];
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Chat, Now.AddMinutes(-1));
        Assert.Equal((0, 0), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Empty(store.Opens);
        Assert.Empty(store.Joins);
        Assert.Single(store.Refused);
    }

    /// <summary>A thread whose route is gone or disabled cannot open, so its waiting messages are refused without a post or a journal row instead of
    /// holding a batch slot for ever: with a batch of them ahead, a live thread behind them is still reached on the next pass.</summary>
    [Fact]
    public async Task TheSweep_RefusesTheWaitingMessagesOfAThreadWhoseRouteIsGoneOrDisabled_WithoutAPost_SoALiveThreadBehindABatchOfThemIsReached()
    {
        var store = new FakeChatStore();
        var posts = new RecordingPosts();
        var routes = new List<NotificationRoute>
        {
            new(RouteId, "Blocking", "", "", "", "", "", Enabled: true) { SlackBotChannel = Channel, AgentMaxPerHour = 10 },
            new(8, "CPU", "", "", "", "", "", Enabled: false) { SlackBotChannel = Channel, AgentMaxPerHour = 10 },
        };
        var sweep = new SlackChatSweep(store, posts, () => routes, NullLogger.Instance, () => Now);
        store.Levels[7] = 1;
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddMinutes(-10));
        for (var i = 1; i <= SlackChatSweep.BatchSize + 1; i++)
        {
            /* Odd threads on the disabled route, even ones on a route no longer configured; every one older than the live thread. */
            var dead = Guid.Parse("76590000-0000-0000-0000-" + i.ToString("x12"));
            var ts = "1700000000." + i.ToString("D6");
            store.Waiting.Add(new SlackWaitingThread(dead, i % 2 == 1 ? 8 : 99, ServerId, Channel, ts, 1, Now.AddHours(-3)));
            store.WaitingMessages[dead] = [new SlackPersonMessage(Channel, ts + "1", ts, dead, 7, "slack:" + Team + ":U7", "dead " + i, SlackPersonMessageStates.Waiting)];
        }

        store.Waiting.Add(new SlackWaitingThread(First, RouteId, ServerId, Channel, ThreadTs, 1, Now.AddHours(-2)));
        store.WaitingMessages[First] = [Waiting("1700000000.000601", 7, "live")];

        /* The first pass clears a batch of dead threads; the second clears the last one and reaches the live thread. */
        Assert.Equal((0, 0), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Equal(SlackChatSweep.BatchSize, store.Refused.Count);
        Assert.Equal((1, 0), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Equal(SlackChatSweep.BatchSize + 1, store.Refused.Count);
        Assert.All(store.Refused, m => Assert.StartsWith("dead ", m.Text, StringComparison.Ordinal));
        Assert.Equal("live", Assert.Single(Assert.Single(store.Opens).Messages).Text);
        Assert.Empty(posts.Posts);
        Assert.Empty(store.Journal);
    }

    /// <summary>The sweep applies the handler's age limit: a thread whose first request opened more than seven days ago opens nothing, and every
    /// waiting message of it is refused as the handler refuses a mention there, each person answered once.</summary>
    [Fact]
    public async Task TheSweep_RefusesEveryWaitingMessageOfAThreadPastSevenDays_AndOpensNothing()
    {
        var (sweep, store, posts) = Sweep();
        store.Waiting.Add(new SlackWaitingThread(First, RouteId, ServerId, Channel, ThreadTs, 2, Now.AddDays(-7).AddSeconds(-1)));
        store.WaitingMessages[First] = [Waiting("1700000000.000601", 7, "late"), Waiting("1700000000.000602", 8, "later"), Waiting("1700000000.000603", 7, "latest")];
        store.Levels[7] = 1;
        store.Levels[8] = 3;
        store.Newest = new SlackChatNewest(Newest, "reported", InvestigationTriggerKinds.Alert, Now.AddDays(-8));

        Assert.Equal((0, 0), await sweep.RunOnceAsync(CancellationToken.None));

        Assert.Empty(store.Opens);
        Assert.Empty(store.Joins);
        Assert.Equal(3, store.Refused.Count);
        Assert.Equal(2, posts.Posts.Count);
        Assert.All(posts.Posts, p => Assert.Equal((Channel, SlackChatAnswers.OldRequest, ThreadTs), p));
        Assert.Equal(2, store.Journal.Count);
        Assert.All(store.Journal, r =>
        {
            Assert.Equal((First, SlackChatAnswers.OldKind), (r.Request, r.Outcome));
            Assert.Null(r.Detail["text"]);
            Assert.Null(r.Detail["person_id"]);
        });

        /* A thread at the limit, not past it, still opens. */
        store.Waiting.Clear();
        store.Waiting.Add(new SlackWaitingThread(First, RouteId, ServerId, Channel, ThreadTs, 1, Now.AddDays(-7)));
        store.Refused.Clear();
        store.WaitingMessages[First] = [Waiting("1700000000.000604", 7, "in time")];
        Assert.Equal((1, 0), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Single(store.Opens);
        Assert.Empty(store.Refused);
    }

    [Fact]
    public async Task TheSweep_SaysTheAgentMayBeOffline_OnceOnly_FiveMinutesAfterAnUnclaimedChatRequestOpened()
    {
        var (sweep, store, posts) = Sweep();
        store.Unclaimed.Add(new SlackUnclaimedChat(Opened, First, RouteId, ServerId, Channel, ThreadTs, Now.AddMinutes(-5)));
        store.Unclaimed.Add(new SlackUnclaimedChat(Newest, First, RouteId, ServerId, Channel, "1700000000.000800", Now.AddMinutes(-4)));

        Assert.Equal((0, 1), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Equal((Channel, SlackChatAnswers.Offline, ThreadTs), Assert.Single(posts.Posts));
        Assert.Equal(Now - SlackChatRules.OfflineAfter, Assert.Single(store.UnclaimedAskedBefore));
        Assert.Contains(store.MarkCalls, m => m.Request == Opened && m.AnswerKey == InvestigationRequestStore.ChatAnswerKey(SlackChatAnswers.OfflineKind));

        Assert.Equal((0, 0), await sweep.RunOnceAsync(CancellationToken.None));
        Assert.Single(posts.Posts);
        Assert.Equal(TimeSpan.FromMinutes(5), SlackChatRules.OfflineAfter);
    }

    /* ───────── the agent's message in the thread ───────── */

    [Fact]
    public void AnAgentsMessage_PostsAsTheAgents_AtTheStoresDataDetail()
    {
        var full = SlackThreadPosts.AgentMessagePost("agent-a", "The plan for 'orders' is forced now.", McpDataDetail.Full);
        Assert.Contains("agent-a", full.Text, StringComparison.Ordinal);
        /* The serializer writes an apostrophe as an escape, so the literal is checked by its word. */
        Assert.Contains("orders", JsonSerializer.Serialize(full.Blocks), StringComparison.Ordinal);
        Assert.Contains("is forced now.", JsonSerializer.Serialize(full.Blocks), StringComparison.Ordinal);

        var masked = SlackThreadPosts.AgentMessagePost("agent-a", "The plan for 'orders' is forced now.", McpDataDetail.MaskLiterals);
        Assert.DoesNotContain("orders", JsonSerializer.Serialize(masked.Blocks), StringComparison.Ordinal);

        var withheld = SlackThreadPosts.AgentMessagePost("agent-a", "The plan for 'orders' is forced now.", McpDataDetail.MetadataOnly);
        Assert.DoesNotContain("orders", JsonSerializer.Serialize(withheld.Blocks), StringComparison.Ordinal);
        Assert.Contains("withheld", JsonSerializer.Serialize(withheld.Blocks), StringComparison.OrdinalIgnoreCase);

        /* The text is escaped and bounded like every collected value: a mention cannot ride in the agent's message. */
        var escaped = SlackThreadPosts.AgentMessagePost("agent-a", "<!channel> look " + new string('x', 5000), McpDataDetail.Full);
        var section = JsonSerializer.SerializeToNode(escaped.Blocks)![0]!["text"]!["text"]!.GetValue<string>();
        Assert.DoesNotContain("<!channel>", section, StringComparison.Ordinal);
        Assert.Contains("&lt;!channel&gt;", section, StringComparison.Ordinal);
        Assert.True(section.Length < 3000, section.Length.ToString());
    }

    [Fact]
    public void TheThreadPass_PostsAgentMessages_FromTheirJournalRow()
    {
        Assert.Contains(InvestigationRequestStore.AgentMessage, SlackThreadPass.PostedEvents);
        var row = new SlackThreadJournalRow(900, InvestigationRequestStore.AgentMessage, Now, "agent:agent-a", null, "{\"text\":\"Looking at the plan now.\"}",
            SlackThreadPosts.EventKey(InvestigationRequestStore.AgentMessage, 900), Opened, RouteId, ServerId, "prod-sql-01", "CPU", "critical",
            InvestigationTriggerKinds.Chat, null, null, 0, null, null, Now.AddMinutes(-5), ChatThreadOf: First);
        var content = SlackThreadPass.Content(row, [], McpDataDetail.Full, "");
        Assert.Contains("Looking at the plan now.", JsonSerializer.Serialize(content.Blocks), StringComparison.Ordinal);
        Assert.Contains("agent-a", content.Text, StringComparison.Ordinal);
        Assert.Equal(First, row.ChatThreadOf);
    }
}
