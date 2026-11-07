/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
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
}

/// <summary>#7659: the request thread a mention landed in, as this store knows it: the thread's first request (the one whose parent post opened
/// the thread), its route and server, the channel and parent ts, and when that first request opened.</summary>
public sealed record SlackChatThread(Guid ThreadRequestId, int RouteId, int ServerId, string ChannelId, string ThreadTs, DateTime OpenedAtUtc);

/// <summary>#7659: the newest request of a thread: the thread's first request, a follow-up that replied in it, or a chat request linked to it.</summary>
public sealed record SlackChatNewest(Guid RequestId, string State, string TriggerKind, DateTime OpenedAtUtc);

/// <summary>#7659: the states of a person's message: <see cref="Waiting"/> for a request to end, <see cref="Joined"/> into an unclaimed chat
/// request, <see cref="Opened"/> as part of a chat request's open.</summary>
public static class SlackPersonMessageStates
{
    public const string Waiting = "waiting";
    public const string Joined = "joined";
    public const string Opened = "opened";
}

/// <summary>
/// #7659: one person's message (V254 <c>config.investigation_person_messages</c>): an audit row, not a journal event. Its natural key is the
/// channel and ts; <see cref="ThreadRequestId"/> is the thread's first request; <see cref="RequestId"/> the chat request it joined or opened,
/// null while it waits; <see cref="Text"/> the message with every mention of the bot removed, at most <see cref="SlackChatRules.TextLimit"/>
/// characters.
/// </summary>
public sealed record SlackPersonMessage(string ChannelId, string Ts, string ThreadTs, Guid ThreadRequestId, long? PersonId, string Identity, string Text,
    string State = SlackPersonMessageStates.Waiting, Guid? RequestId = null, DateTime? ReceivedAtUtc = null);

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
}

/// <summary>#7659: the answer of a chat request open.</summary>
public sealed record SlackChatOpenResult(SlackChatOpenOutcome Outcome, Guid? RequestId);

/// <summary>#7659: the chat requests opened in the last hour: by this thread, and by this route.</summary>
public sealed record SlackChatCounts(long ThreadLastHour, long RouteLastHour);

/// <summary>#7659: a thread with waiting messages, for the sweep.</summary>
public sealed record SlackWaitingThread(Guid ThreadRequestId, int RouteId, int ServerId, string ChannelId, string ThreadTs, int Messages);

/// <summary>#7659: a chat request no agent has claimed, with its thread, for the offline post.</summary>
public sealed record SlackUnclaimedChat(Guid RequestId, Guid ThreadRequestId, int RouteId, int ServerId, string ChannelId, string ThreadTs, DateTime OpenedAtUtc);

/// <summary>#7659: one entry of the claim answer's <c>person_requests</c>: the person's name, their grants for the request's server, and their text.</summary>
public sealed record PersonRequest(string Name, IReadOnlyList<string> Grants, string Text);

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

    /// <summary>Saves a message as waiting. False when a message with that channel and ts is already saved: the same mention arrived again.</summary>
    Task<bool> SaveMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken);

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

    /// <inheritdoc cref="ISlackChatStore.FindThreadAsync" />
    public Task<SlackChatThread?> FindChatThreadAsync(string channelId, string threadTs, CancellationToken cancellationToken) =>
        Task.FromResult<SlackChatThread?>(null);

    /// <inheritdoc cref="ISlackChatStore.NewestRequestAsync" />
    public Task<SlackChatNewest?> NewestThreadRequestAsync(SlackChatThread thread, CancellationToken cancellationToken) =>
        Task.FromResult<SlackChatNewest?>(null);

    /// <inheritdoc cref="ISlackChatStore.RequestLevelAsync" />
    public Task<int> RequestLevelAsync(long personId, int serverId, CancellationToken cancellationToken) => Task.FromResult(0);

    /// <inheritdoc cref="ISlackChatStore.SaveMessageAsync" />
    public Task<bool> SavePersonMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken) => Task.FromResult(false);

    /// <inheritdoc cref="ISlackChatStore.JoinChatRequestAsync" />
    public Task<bool> JoinChatRequestAsync(Guid requestId, SlackAsker asker, SlackPersonMessage message, CancellationToken cancellationToken) =>
        Task.FromResult(false);

    /// <inheritdoc cref="ISlackChatStore.ChatCountsAsync" />
    public Task<SlackChatCounts> ChatCountsAsync(Guid threadRequestId, int routeId, DateTime nowUtc, CancellationToken cancellationToken) =>
        Task.FromResult(new SlackChatCounts(0, 0));

    /// <summary>Opens a chat request (<see cref="ISlackChatStore.OpenChatRequestAsync"/>). <paramref name="loadRoutes"/> gives the live routes, so the
    /// stamp is the lowest of the parent's stamp, the level the routes give the parent's metrics, and the lowest asker level.</summary>
    public Task<SlackChatOpenResult> OpenChatRequestAsync(SlackChatThread thread, Guid parentRequestId, IReadOnlyList<SlackAsker> askers,
        IReadOnlyList<SlackPersonMessage> messages, int routeMaxPerHour, DateTime nowUtc,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> loadRoutes,
        CancellationToken cancellationToken) =>
        Task.FromResult(new SlackChatOpenResult(SlackChatOpenOutcome.ParentGone, null));

    /// <inheritdoc cref="ISlackChatStore.JournalPersonMessageAsync" />
    public Task JournalPersonMessageAsync(Guid? requestId, int? routeId, int? serverId, string actor, string outcome, JsonObject detail, string onceKey,
        CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc cref="ISlackChatStore.MarkChatAnswerOnceAsync" />
    public Task<bool> MarkChatAnswerOnceAsync(Guid? requestId, int? routeId, int? serverId, string answerKey, string onceKey, CancellationToken cancellationToken) =>
        Task.FromResult(false);

    /// <inheritdoc cref="ISlackChatStore.WaitingThreadsAsync" />
    public Task<IReadOnlyList<SlackWaitingThread>> WaitingThreadsAsync(int limit, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<SlackWaitingThread>>([]);

    /// <inheritdoc cref="ISlackChatStore.WaitingMessagesAsync" />
    public Task<IReadOnlyList<SlackPersonMessage>> WaitingMessagesAsync(Guid threadRequestId, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<SlackPersonMessage>>([]);

    /// <inheritdoc cref="ISlackChatStore.UnclaimedChatRequestsAsync" />
    public Task<IReadOnlyList<SlackUnclaimedChat>> UnclaimedChatRequestsAsync(DateTime openedBeforeUtc, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<SlackUnclaimedChat>>([]);

    /// <summary>The messages of a chat request for the claim answer's <c>person_requests</c>: each person's name, grants for the request's server and text.</summary>
    public Task<IReadOnlyList<PersonRequest>> ListPersonRequestsAsync(Guid requestId, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<PersonRequest>>([]);

    /// <summary>An agent's message to the thread (<c>post_investigation_message</c>): while <paramref name="claimId"/> is the live claim of
    /// <paramref name="requestId"/>, journals <see cref="AgentMessage"/> as <c>agent:&lt;agentId&gt;</c> with the text, bounded, under the request's
    /// agent journal cap.</summary>
    public Task<AgentMessageResult> PostAgentMessageAsync(Guid requestId, Guid claimId, string agentId, string text, CancellationToken cancellationToken) =>
        Task.FromResult(new AgentMessageResult(AgentMessageOutcome.NotFound, null));
}
