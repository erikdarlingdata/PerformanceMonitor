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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7656: the thread pass. It reads each request's journal rows in journal order, posts the parent first and every later row as a
/// reply, skips a request's later posts when its parent post failed, posts nothing from the do-not-post list, lets a follow-up
/// inherit its parent's thread one level deep, and on a 429 releases the mark, stops the pass and posts nothing to that channel
/// before Retry-After, while every other error keeps its mark.
/// </summary>
public sealed class SlackThreadPassTests
{
    private const string Channel = "C0BOT";
    private const int RouteId = 7;
    private static readonly DateTime Now = new(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);
    private static readonly Guid R1 = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid R2 = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly Guid R3 = Guid.Parse("33333333-3333-3333-3333-333333333333");

    /// <summary>The thread store and the row source in memory: rows the test journals, marks as the store writes them, and every release.</summary>
    private sealed class MemoryStore : ISlackRequestThreadStore, ISlackThreadRowSource
    {
        public readonly Dictionary<Guid, SlackRequestThread> Threads = new();
        public readonly HashSet<string> Marks = new(StringComparer.Ordinal);
        public readonly Dictionary<string, string> SkipReasons = new(StringComparer.Ordinal);
        public readonly List<string> Released = new();
        public readonly List<SlackThreadJournalRow> Rows = new();
        public readonly Dictionary<Guid, List<RequestChangeRow>> Changes = new();
        public IReadOnlyList<string>? EventsAsked;

        public Task<SlackRequestThread?> FindAsync(Guid threadRequestId, CancellationToken cancellationToken) =>
            Task.FromResult(Threads.TryGetValue(threadRequestId, out var t) ? t : null);

        public Task SaveAsync(SlackRequestThread thread, CancellationToken cancellationToken)
        {
            Threads.TryAdd(thread.RequestId, thread);
            return Task.CompletedTask;
        }

        public Task<bool> MarkOnceAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, CancellationToken cancellationToken) =>
            Task.FromResult(Marks.Add(onceKey));

        public Task<bool> MarkSkippedAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, string reason, CancellationToken cancellationToken)
        {
            SkipReasons[onceKey] = reason;
            return Task.FromResult(Marks.Add(onceKey));
        }

        public Task ReleaseMarkAsync(string onceKey, CancellationToken cancellationToken)
        {
            Released.Add(onceKey);
            Marks.Remove(onceKey);
            return Task.CompletedTask;
        }

        /* Every unmarked row inside the window comes back whatever the kinds asked for, so a test can hand the pass a row from the
           do-not-post list and see the pass itself refuse it. The kinds asked for are kept for the test that reads them. */
        public Task<IReadOnlyList<SlackThreadJournalRow>> PendingSlackThreadRowsAsync(IReadOnlyList<string> events, DateTime sinceUtc, DateTime unknownBeforeUtc, int limit, CancellationToken cancellationToken)
        {
            EventsAsked = events;
            IReadOnlyList<SlackThreadJournalRow> pending = Rows
                .Where(r => r.EventTimeUtc >= sinceUtc && !Marks.Contains(SlackThreadPosts.OnceKey(r.RequestId, r.PostKey)))
                .OrderBy(r => r.JournalId)
                .Take(limit)
                .ToList();
            return Task.FromResult(pending);
        }

        public Task<IReadOnlyList<RequestChangeRow>> ChangesForRequestAsync(Guid requestId, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<RequestChangeRow>>(Changes.TryGetValue(requestId, out var c) ? c : []);
    }

    private sealed class QueuePosts : ISlackBotPosts
    {
        public readonly List<(string Channel, string Text, string? ThreadTs)> Posts = new();
        public readonly Queue<SlackPostResult> Results = new();

        public Task<SlackPostResult> PostAsync(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs, CancellationToken cancellationToken)
        {
            Posts.Add((channel, text, threadTs));
            return Task.FromResult(Results.Count > 0 ? Results.Dequeue() : new SlackPostResult(true, "1700000000." + Posts.Count.ToString("D6"), channel, null));
        }
    }

    /// <summary>A row at Now less 30 minutes plus its journal id in seconds. Its request opened at <paramref name="openedAt"/>, else at the
    /// row's own time, which is the opened row's time for the request's first row and within seconds of it for the rest.</summary>
    private static SlackThreadJournalRow Row(long journalId, string eventKind, Guid requestId, string actor = "agent:a", string? reason = null,
        string? detailJson = null, Guid? followUpOf = null, int claimNo = 0, string? calledDetailJson = null, string? verdict = null, string? rootCause = null,
        DateTime? openedAt = null) =>
        new(journalId, eventKind, Now.AddMinutes(-30).AddSeconds(journalId), actor, reason, detailJson,
            eventKind == "opened" ? SlackThreadPosts.RequestKey : eventKind == "action_unknown" ? SlackThreadPosts.UnknownKey(journalId) : SlackThreadPosts.EventKey(eventKind, journalId),
            requestId, RouteId, ServerId: 3, "prod-sql-01", "CPU", "critical", "alert", followUpOf, calledDetailJson, claimNo, verdict, rootCause,
            openedAt ?? Now.AddMinutes(-30).AddSeconds(journalId));

    /// <summary>Keeps every line the pass logs, with its level, so a test can say a pass warned nothing.</summary>
    private sealed class ListLogger : ILogger
    {
        public readonly List<(LogLevel Level, string Message)> Lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add((logLevel, formatter(state, exception)));
    }

    /// <summary>Sets when the route got its bot channel. By reflection, so the test names the member it needs when it is missing.</summary>
    private static void Stamp(NotificationRoute route, DateTime setAtUtc)
    {
        var property = typeof(NotificationRoute).GetProperty("SlackBotChannelSetAt");
        Assert.True(property is not null, "NotificationRoute has no SlackBotChannelSetAt: the time the route got its bot channel, which gates the parent post (#7656).");
        property!.SetValue(route, setAtUtc);
    }

    private static (SlackThreadPass Pass, SlackPostThrottle Throttle) Pass(MemoryStore store, QueuePosts posts, Func<DateTime> now, string channel = Channel, DateTime? channelSetAt = null, ILogger? logger = null)
    {
        var throttle = new SlackPostThrottle();
        var poster = new SlackThreadPoster(store, posts, NullLogger<SlackThreadPoster>.Instance, throttle, now);
        var route = new NotificationRoute(RouteId, "Blocking", "", "", "", "", "", Enabled: true) { SlackBotChannel = channel };
        if (channelSetAt is DateTime setAt)
        {
            Stamp(route, setAt);
        }

        var routes = new List<NotificationRoute> { route };
        var pass = new SlackThreadPass(store, store, poster, throttle, () => routes, logger ?? NullLogger.Instance, McpDataDetail.Full, () => "https://pm.example", now);
        return (pass, throttle);
    }

    [Fact]
    public async Task ThePass_PostsInJournalOrder_TheParentFirst_AndEveryLaterRowRepliesInItsThread()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(3, "claimed", R1, actor: "agent:atlas", claimNo: 1));
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(5, "reported", R1, actor: "agent:atlas", reason: "needs_person", verdict: "needs_person", rootCause: "A plan regressed.\nMore."));
        store.Rows.Add(Row(8, "resolved", R1, actor: "service", reason: "CPU"));
        var (pass, _) = Pass(store, posts, () => Now);

        var posted = await pass.RunOnceAsync(CancellationToken.None);

        Assert.Equal(4, posted);
        Assert.Equal(4, posts.Posts.Count);
        Assert.Null(posts.Posts[0].ThreadTs);
        Assert.Contains("Investigation request", posts.Posts[0].Text);
        var ts = store.Threads[R1].ThreadTs;
        Assert.Equal("1700000000.000001", ts);
        Assert.All(posts.Posts.Skip(1), p => Assert.Equal(ts, p.ThreadTs));
        Assert.All(posts.Posts, p => Assert.Equal(Channel, p.Channel));
        Assert.Contains("agent:atlas", posts.Posts[1].Text);
        Assert.Contains("claim 1", posts.Posts[1].Text);
        Assert.Contains("needs_person", posts.Posts[2].Text);
        Assert.StartsWith("Resolved", posts.Posts[3].Text, StringComparison.Ordinal);
        Assert.Equal(
            new HashSet<string>([SlackThreadPosts.OnceKey(R1, "request"), SlackThreadPosts.OnceKey(R1, "claimed:3"), SlackThreadPosts.OnceKey(R1, "reported:5"), SlackThreadPosts.OnceKey(R1, "resolved:8")], StringComparer.Ordinal),
            store.Marks);

        /* A second pass finds every row marked and posts nothing. */
        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Equal(4, posts.Posts.Count);
    }

    [Fact]
    public async Task AFailedParent_KeepsItsMark_AndItsLaterPostsAreSkipped_NotPostedAtTheTopLevel()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(2, "claimed", R1, claimNo: 1));
        store.Rows.Add(Row(3, "resolved", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(4, "opened", R2, actor: "service", reason: "Blocking"));
        posts.Results.Enqueue(new SlackPostResult(false, null, null, "channel_not_found"));
        var (pass, _) = Pass(store, posts, () => Now);

        var posted = await pass.RunOnceAsync(CancellationToken.None);

        /* One attempt for R1's parent, none for its later rows, and R2's own parent still posts. */
        Assert.Equal(1, posted);
        Assert.Equal(2, posts.Posts.Count);
        Assert.All(posts.Posts, p => Assert.Null(p.ThreadTs));
        Assert.False(store.Threads.ContainsKey(R1));
        Assert.Contains(SlackThreadPosts.OnceKey(R1, "request"), store.Marks);
        Assert.Empty(store.Released);

        /* The next pass does not try the parent again, and still posts none of R1's later rows at the top level. */
        await pass.RunOnceAsync(CancellationToken.None);
        Assert.Equal(2, posts.Posts.Count);
    }

    [Fact]
    public async Task TheDoNotPostList_PostsNothing_AndWritesNoMark()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Threads[R1] = new SlackRequestThread(R1, Channel, "1700000000.000001");
        string[] silent =
        [
            "joined", "joined_quiet", "evidence_ready", "deadlock_digest", "event_delivered", "fallback_sent", "report_notice_sent",
            "proposal_notice_sent", "proposal_first_press", "proposal_approved", "refused", "journal_capped", "action_called", "note_written",
            "link_recorded", "follow_up", "slack_thread_posted",
        ];
        var id = 10L;
        foreach (var kind in silent)
        {
            store.Rows.Add(Row(id++, kind, R1));
        }

        var (pass, _) = Pass(store, posts, () => Now);

        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Empty(posts.Posts);
        Assert.Empty(store.Marks);

        /* The kinds the store is asked for are the post list and nothing from the silent list. */
        Assert.NotNull(store.EventsAsked);
        Assert.Equal(SlackThreadPass.PostedEvents, store.EventsAsked);
        Assert.All(silent, kind => Assert.DoesNotContain(kind, SlackThreadPass.PostedEvents));
        string[] posted =
        [
            "opened", "claimed", "reported", "action_done", "action_failed", "action_undone", "proposal_created", "proposal_run", "proposal_failed",
            "proposal_refused", "proposal_expired", "proposal_superseded", "proposal_voided", "proposal_unknown", "resolved", "closed", "expired", "claims_exhausted",
        ];
        Assert.All(posted, kind => Assert.Contains(kind, SlackThreadPass.PostedEvents));
    }

    [Fact]
    public async Task EachPostedKind_RendersItsRow_WithTheDecidingPersonAndTheResult()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Threads[R1] = new SlackRequestThread(R1, Channel, "1700000000.000001");
        var called = """{"tool":"kill_session","kind":"kill","target":{"id":"session 87","server_id":3,"database":"Sales"}}""";
        store.Rows.Add(Row(2, "action_done", R1, detailJson: """{"called_id":1}""", calledDetailJson: called));
        store.Rows.Add(Row(3, "action_failed", R1, reason: "error", detailJson: """{"called_id":1,"error":"timeout"}""", calledDetailJson: called));
        store.Rows.Add(Row(4, "action_undone", R1, actor: "person:erik", detailJson: """{"called_id":1}""", calledDetailJson: called));
        store.Rows.Add(Row(5, "action_unknown", R1, detailJson: called, calledDetailJson: called));
        store.Rows.Add(Row(6, "proposal_created", R1, detailJson: """{"proposal_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeffff0001","tool":"kill_session","kind":"kill"}"""));
        store.Rows.Add(Row(7, "proposal_run", R1, actor: "person:erik", detailJson: """{"proposal_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeffff0001","tool":"kill_session","by":"person:erik"}"""));
        store.Rows.Add(Row(8, "proposal_superseded", R1, actor: "service", reason: "the plan changed", detailJson: """{"proposal_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeffff0001","tool":"kill_session"}"""));
        store.Rows.Add(Row(9, "closed", R1, actor: "person:erik", reason: "handled by hand"));
        store.Rows.Add(Row(10, "expired", R1, actor: "service", detailJson: """{"unclaimed":true}"""));
        store.Rows.Add(Row(11, "claims_exhausted", R1, actor: "service"));
        var (pass, _) = Pass(store, posts, () => Now);

        Assert.Equal(10, await pass.RunOnceAsync(CancellationToken.None));

        Assert.All(posts.Posts, p => Assert.Equal("1700000000.000001", p.ThreadTs));
        Assert.Contains("kill_session", posts.Posts[0].Text);
        Assert.Contains("done", posts.Posts[0].Text);
        Assert.Contains("failed", posts.Posts[1].Text);
        Assert.Contains("undone", posts.Posts[2].Text);
        Assert.Contains("result unknown", posts.Posts[3].Text);
        Assert.Contains("created", posts.Posts[4].Text);
        Assert.Contains("run", posts.Posts[5].Text);
        Assert.Contains("person:erik", posts.Posts[5].Text);
        Assert.Contains("superseded", posts.Posts[6].Text);
        Assert.Contains("the plan changed", posts.Posts[6].Text);
        Assert.Contains("Closed", posts.Posts[7].Text);
        Assert.Contains("person:erik", posts.Posts[7].Text);
        Assert.Contains("handled by hand", posts.Posts[7].Text);
        Assert.Contains("Expired", posts.Posts[8].Text);
        Assert.Contains("no agent", posts.Posts[8].Text);
        Assert.Contains("Claims exhausted", posts.Posts[9].Text);
        Assert.Contains(SlackThreadPosts.OnceKey(R1, "action_unknown:5"), store.Marks);
    }

    [Fact]
    public async Task AFollowUp_RepliesInItsParentsThread_AndInheritsIt_SoTheNextFollowUpLandsThereWithOneLookup()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Threads[R1] = new SlackRequestThread(R1, Channel, "1700000000.000001");
        store.Rows.Add(Row(20, "opened", R2, actor: "service", reason: "CPU", followUpOf: R1));
        store.Rows.Add(Row(21, "claimed", R2, claimNo: 1));
        store.Rows.Add(Row(30, "opened", R3, actor: "service", reason: "CPU", followUpOf: R2));
        var (pass, _) = Pass(store, posts, () => Now);

        Assert.Equal(3, await pass.RunOnceAsync(CancellationToken.None));

        Assert.All(posts.Posts, p => Assert.Equal("1700000000.000001", p.ThreadTs));
        Assert.All(posts.Posts, p => Assert.Equal(Channel, p.Channel));
        Assert.Equal("1700000000.000001", store.Threads[R2].ThreadTs);
        Assert.Equal("1700000000.000001", store.Threads[R3].ThreadTs);
    }

    [Fact]
    public async Task AFollowUp_StartsANewThread_WhenItsParentHasNone_OrTheParentsChannelIsNotTheRoutesChannelNow()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        /* R1 has no thread; R3's parent R2 has one in a channel the route no longer uses. */
        store.Threads[R2] = new SlackRequestThread(R2, "C0OLD", "1600000000.000009");
        store.Rows.Add(Row(40, "opened", R1, actor: "service", reason: "CPU", followUpOf: Guid.Parse("99999999-9999-9999-9999-999999999999")));
        store.Rows.Add(Row(41, "opened", R3, actor: "service", reason: "CPU", followUpOf: R2));
        store.Rows.Add(Row(42, "claimed", R3, claimNo: 1));
        var (pass, _) = Pass(store, posts, () => Now);

        Assert.Equal(3, await pass.RunOnceAsync(CancellationToken.None));

        Assert.Null(posts.Posts[0].ThreadTs);
        Assert.Null(posts.Posts[1].ThreadTs);
        Assert.Equal(Channel, posts.Posts[1].Channel);
        Assert.Equal("1700000000.000002", store.Threads[R3].ThreadTs);
        Assert.Equal(Channel, store.Threads[R3].ChannelId);
        Assert.Equal("1700000000.000002", posts.Posts[2].ThreadTs);
        Assert.Equal("C0OLD", store.Threads[R2].ChannelId);
    }

    /// <summary>A request that opened before the route got its bot channel gets no thread: its parent is not posted, and its rows are
    /// marked so the pass does not read them again. The opened row's time is the request's opened time (Now less 30 minutes, plus its
    /// journal id in seconds); the route's stamp is later.</summary>
    [Fact]
    public async Task ARequestOpenedBeforeTheRouteGotItsChannel_GetsNoParentPost_AndItsRowsAreMarked()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(3, "claimed", R1, actor: "agent:atlas", claimNo: 1));
        store.Rows.Add(Row(5, "resolved", R1, actor: "service", reason: "CPU"));
        var (pass, _) = Pass(store, posts, () => Now, channelSetAt: Now.AddMinutes(-20));

        var posted = await pass.RunOnceAsync(CancellationToken.None);

        Assert.Equal(0, posted);
        Assert.Empty(posts.Posts);
        Assert.Empty(store.Threads);
        Assert.Contains(SlackThreadPosts.OnceKey(R1, SlackThreadPosts.RequestKey), store.Marks);
        Assert.Contains(SlackThreadPosts.OnceKey(R1, SlackThreadPosts.EventKey("claimed", 3)), store.Marks);
        Assert.Contains(SlackThreadPosts.OnceKey(R1, SlackThreadPosts.EventKey("resolved", 5)), store.Marks);
        Assert.All(store.Marks, key => Assert.Equal(SlackThreadPass.OpenedBeforeChannel, store.SkipReasons[key]));
        Assert.Empty(await store.PendingSlackThreadRowsAsync(SlackThreadPass.PostedEvents, Now - SlackThreadPass.ReadWindow, Now, 100, CancellationToken.None));
        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Empty(posts.Posts);
    }

    /// <summary>The gate holds across passes. A gated request's rows are read 100 at a time across requests, so its opened row and its
    /// later rows often land in different passes: here the request has 130 rows, the first pass gates the parent and marks the 99 rows
    /// behind it, and the second pass reads the other 30 with the parent's mark already on disk. Every row is marked with the gate as
    /// the reason, nothing is posted, and no pass warns that a parent post was not delivered.</summary>
    [Fact]
    public async Task AGatedRequestWhoseRowsSpanTwoPasses_MarksEveryRowWithTheGateReason_AndWarnsNothing()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        var logger = new ListLogger();
        var openedAt = Now.AddMinutes(-30).AddSeconds(1);
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU", openedAt: openedAt));
        for (var id = 2L; id <= 130; id++)
        {
            store.Rows.Add(Row(id, "claimed", R1, actor: "agent:atlas", claimNo: (int)id - 1, openedAt: openedAt));
        }

        Assert.True(store.Rows.Count > SlackThreadPass.BatchSize, "the rows must span two passes");
        var (pass, _) = Pass(store, posts, () => Now, channelSetAt: Now.AddMinutes(-20), logger: logger);

        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Equal(SlackThreadPass.BatchSize, store.Marks.Count);
        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));

        Assert.Empty(posts.Posts);
        Assert.Empty(store.Threads);
        Assert.Equal(130, store.Marks.Count);
        Assert.All(store.Rows, row => Assert.Contains(SlackThreadPosts.OnceKey(R1, row.PostKey), store.Marks));
        Assert.All(store.Marks, key => Assert.Equal(SlackThreadPass.OpenedBeforeChannel, store.SkipReasons[key]));
        Assert.DoesNotContain(logger.Lines, line => line.Level >= LogLevel.Warning);
        Assert.Equal(2, logger.Lines.Count(line => line.Message.Contains("before the route's bot channel was set", StringComparison.Ordinal)));

        Assert.Empty(await store.PendingSlackThreadRowsAsync(SlackThreadPass.PostedEvents, Now - SlackThreadPass.ReadWindow, Now, 100, CancellationToken.None));
        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Empty(posts.Posts);
    }

    /// <summary>A request that opened at the stamp posts: the gate is "at or after".</summary>
    [Fact]
    public async Task ARequestOpenedAtOrAfterTheRouteGotItsChannel_Posts()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(3, "claimed", R1, actor: "agent:atlas", claimNo: 1));
        var (pass, _) = Pass(store, posts, () => Now, channelSetAt: Now.AddMinutes(-30).AddSeconds(1));

        var posted = await pass.RunOnceAsync(CancellationToken.None);

        Assert.Equal(2, posted);
        Assert.Equal(2, posts.Posts.Count);
        Assert.Null(posts.Posts[0].ThreadTs);
        Assert.Equal(store.Threads[R1].ThreadTs, posts.Posts[1].ThreadTs);
    }

    /// <summary>A request with a stored thread keeps posting its later rows into it after the channel changed, so a request in flight is
    /// not cut off: the stamp gates the parent post alone. The thread sits in the earlier channel, and the replies go there.</summary>
    [Fact]
    public async Task ARequestWithAStoredThread_FromBeforeTheChannelChanged_KeepsPostingItsLaterRowsIntoThatThread()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Threads[R1] = new SlackRequestThread(R1, "C0OLD", "1700000000.000777");
        store.Marks.Add(SlackThreadPosts.OnceKey(R1, SlackThreadPosts.RequestKey));
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(3, "claimed", R1, actor: "agent:atlas", claimNo: 1));
        store.Rows.Add(Row(8, "resolved", R1, actor: "service", reason: "CPU"));
        var (pass, _) = Pass(store, posts, () => Now, channelSetAt: Now.AddMinutes(-20));

        var posted = await pass.RunOnceAsync(CancellationToken.None);

        Assert.Equal(2, posted);
        Assert.Equal(2, posts.Posts.Count);
        Assert.All(posts.Posts, p => Assert.Equal("1700000000.000777", p.ThreadTs));
        Assert.All(posts.Posts, p => Assert.Equal("C0OLD", p.Channel));
        Assert.Equal("1700000000.000777", store.Threads[R1].ThreadTs);
    }

    /// <summary>A follow-up of a request the gate skipped has a parent with no stored thread, so it starts its own, under the follow-up rule.</summary>
    [Fact]
    public async Task AFollowUpOfARequestTheGateSkipped_StartsItsOwnThread()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(3, "claimed", R1, actor: "agent:atlas", claimNo: 1));
        store.Rows.Add(Row(9, "opened", R2, actor: "service", reason: "CPU", followUpOf: R1));
        store.Rows.Add(Row(11, "claimed", R2, actor: "agent:atlas", claimNo: 1));
        var (pass, _) = Pass(store, posts, () => Now, channelSetAt: Now.AddMinutes(-30).AddSeconds(5));

        var posted = await pass.RunOnceAsync(CancellationToken.None);

        Assert.Equal(2, posted);
        Assert.Equal(2, posts.Posts.Count);
        Assert.Null(posts.Posts[0].ThreadTs);
        Assert.Equal(store.Threads[R2].ThreadTs, posts.Posts[1].ThreadTs);
        Assert.False(store.Threads.ContainsKey(R1));
        Assert.Contains(SlackThreadPosts.OnceKey(R1, SlackThreadPosts.RequestKey), store.Marks);
        Assert.Contains(SlackThreadPosts.OnceKey(R1, SlackThreadPosts.EventKey("claimed", 3)), store.Marks);
    }

    /// <summary>The proposal card says when the proposal expires: the row's time plus the one proposal lifetime, with no second read.</summary>
    [Fact]
    public void TheProposalCard_SaysWhenTheProposalExpires()
    {
        var row = Row(21, "proposal_created", R1, actor: "agent:atlas", reason: "slow", detailJson: """{"tool":"kill_session","proposal_id":"aaaaaaaa-1111-2222-3333-abcdefabcdef"}""");

        var content = SlackThreadPass.Content(row, [], McpDataDetail.Full, "https://pm.example");

        var rendered = content.Text + "\n" + string.Join("\n", content.Blocks.Select(b => JsonSerializer.Serialize(b)));
        Assert.Contains("Expires", rendered);
        Assert.Contains(row.EventTimeUtc.AddMinutes(ProposalRules.ExpiryMinutes).ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", rendered);
    }

    [Fact]
    public async Task A429_ReleasesTheMark_StopsThePass_AndNothingPostsToTheChannelBeforeRetryAfter()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        store.Rows.Add(Row(2, "claimed", R1, claimNo: 1));
        store.Rows.Add(Row(3, "opened", R2, actor: "service", reason: "Blocking"));
        posts.Results.Enqueue(new SlackPostResult(false, null, null, "HTTP 429", RateLimited: true, RetryAfter: TimeSpan.FromSeconds(30)));
        var now = Now;
        var (pass, throttle) = Pass(store, posts, () => now);

        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));

        /* One attempt, then the pass stopped: the mark is released, nothing else was marked or sent. */
        Assert.Single(posts.Posts);
        Assert.Equal([SlackThreadPosts.OnceKey(R1, "request")], store.Released);
        Assert.Empty(store.Marks);
        Assert.Empty(store.Threads);
        Assert.Equal(Now.AddSeconds(30), throttle.BlockedUntil(Channel, Now));

        /* Inside Retry-After, a pass posts nothing to that channel and marks nothing. */
        now = Now.AddSeconds(10);
        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Single(posts.Posts);
        Assert.Empty(store.Marks);

        /* After it, the next pass posts the released parent and everything behind it, in order. */
        now = Now.AddSeconds(31);
        Assert.Equal(3, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Equal(4, posts.Posts.Count);
        Assert.Null(posts.Posts[1].ThreadTs);
        Assert.Equal(store.Threads[R1].ThreadTs, posts.Posts[2].ThreadTs);
        Assert.Null(posts.Posts[3].ThreadTs);
        Assert.Null(throttle.BlockedUntil(Channel, now));
    }

    [Fact]
    public async Task A429_WithNoRetryAfter_WaitsTheDefault()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        posts.Results.Enqueue(new SlackPostResult(false, null, null, "HTTP 429", RateLimited: true));
        var (pass, throttle) = Pass(store, posts, () => Now);

        await pass.RunOnceAsync(CancellationToken.None);

        Assert.Equal(Now + SlackThreadPoster.DefaultRetryAfter, throttle.BlockedUntil(Channel, Now));
        Assert.Equal(TimeSpan.FromSeconds(60), SlackThreadPoster.DefaultRetryAfter);
    }

    [Fact]
    public async Task AnotherError_KeepsItsMark_AndIsNotTriedAgain()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Threads[R1] = new SlackRequestThread(R1, Channel, "1700000000.000001");
        store.Rows.Add(Row(2, "claimed", R1, claimNo: 1));
        store.Rows.Add(Row(3, "resolved", R1, actor: "service", reason: "CPU"));
        posts.Results.Enqueue(new SlackPostResult(false, null, null, "HTTP 503"));
        var (pass, throttle) = Pass(store, posts, () => Now);

        Assert.Equal(1, await pass.RunOnceAsync(CancellationToken.None));

        Assert.Equal(2, posts.Posts.Count);
        Assert.Contains(SlackThreadPosts.OnceKey(R1, "claimed:2"), store.Marks);
        Assert.Empty(store.Released);
        Assert.Null(throttle.BlockedUntil(Channel, Now));

        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Equal(2, posts.Posts.Count);
    }

    [Fact]
    public async Task ARouteWithNoBotChannelOrNoToken_PostsNothing_AndMarksNothing()
    {
        var store = new MemoryStore();
        var posts = new QueuePosts();
        store.Rows.Add(Row(1, "opened", R1, actor: "service", reason: "CPU"));
        var (pass, _) = Pass(store, posts, () => Now, channel: "");

        Assert.Equal(0, await pass.RunOnceAsync(CancellationToken.None));
        Assert.Empty(posts.Posts);
        Assert.Empty(store.Marks);

        var throttle = new SlackPostThrottle();
        var poster = new SlackThreadPoster(store, posts, NullLogger<SlackThreadPoster>.Instance, throttle, () => Now);
        var routes = new List<NotificationRoute> { new(RouteId, "Blocking", "", "", "", "", "", Enabled: true) { SlackBotChannel = Channel } };
        var noToken = new SlackThreadPass(store, store, poster, throttle, () => routes, NullLogger.Instance, McpDataDetail.Full, () => "", () => Now, ready: () => false);

        Assert.Equal(0, await noToken.RunOnceAsync(CancellationToken.None));
        Assert.Empty(posts.Posts);
        Assert.Empty(store.Marks);
    }

    /// <summary>A handler that answers every request with one status and, when given, a Retry-After in seconds.</summary>
    private sealed class StatusHandler(HttpStatusCode status, int? retryAfterSeconds) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(status) { Content = new StringContent("{\"ok\":false,\"error\":\"ratelimited\"}") };
            if (retryAfterSeconds is int seconds)
            {
                response.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(seconds));
            }

            return Task.FromResult(response);
        }
    }

    [Fact]
    public async Task TheClient_ReportsA429_AsRateLimited_WithSlacksRetryAfter()
    {
        var limited = new SlackBotClient("xoxb-test-token", "http://127.0.0.1:9/api/", new StatusHandler(HttpStatusCode.TooManyRequests, 12));
        var result = await limited.PostAsync(Channel, "hello", null, null, CancellationToken.None);
        Assert.False(result.Ok);
        Assert.True(result.RateLimited);
        Assert.Equal(TimeSpan.FromSeconds(12), result.RetryAfter);
        Assert.DoesNotContain("xoxb-test-token", result.Error ?? "", StringComparison.Ordinal);

        var noHeader = new SlackBotClient("xoxb-test-token", "http://127.0.0.1:9/api/", new StatusHandler(HttpStatusCode.TooManyRequests, null));
        var bare = await noHeader.PostAsync(Channel, "hello", null, null, CancellationToken.None);
        Assert.True(bare.RateLimited);
        Assert.Null(bare.RetryAfter);

        var unavailable = new SlackBotClient("xoxb-test-token", "http://127.0.0.1:9/api/", new StatusHandler(HttpStatusCode.ServiceUnavailable, 5));
        var other = await unavailable.PostAsync(Channel, "hello", null, null, CancellationToken.None);
        Assert.False(other.Ok);
        Assert.False(other.RateLimited);
    }
}
