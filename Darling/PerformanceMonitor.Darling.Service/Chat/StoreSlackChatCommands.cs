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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>
/// #7659: the chat store on the store owner's connection. Every call forwards to <see cref="InvestigationRequestStore"/>; the open binds the live
/// route reader (<see cref="DarlingAgentActionGate.LoadRoutesAsync"/>), so a chat request is stamped at the level the routes give its parent's
/// metrics, as the gate reads them.
/// </summary>
public sealed class StoreSlackChatCommands(InvestigationRequestStore store, ILogger logger) : ISlackChatStore
{
    private readonly InvestigationRequestStore _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly ILogger _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public Task<SlackChatThread?> FindThreadAsync(string channelId, string threadTs, CancellationToken cancellationToken) =>
        _store.FindChatThreadAsync(channelId, threadTs, cancellationToken);

    /// <inheritdoc />
    public Task<SlackChatNewest?> NewestRequestAsync(SlackChatThread thread, CancellationToken cancellationToken) =>
        _store.NewestThreadRequestAsync(thread, cancellationToken);

    /// <inheritdoc />
    public Task<SlackPerson?> FindSlackPersonAsync(string teamId, string userId, CancellationToken cancellationToken) =>
        _store.FindSlackPersonAsync(teamId, userId, cancellationToken);

    /// <inheritdoc />
    public Task<int> RequestLevelAsync(long personId, int serverId, CancellationToken cancellationToken) =>
        _store.RequestLevelAsync(personId, serverId, cancellationToken);

    /// <inheritdoc />
    public Task<SlackPersonMessageSaved> SaveMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken) =>
        _store.SavePersonMessageAsync(message, cancellationToken);

    /// <inheritdoc />
    public Task<bool> JoinChatRequestAsync(Guid requestId, SlackAsker asker, SlackPersonMessage message, CancellationToken cancellationToken) =>
        _store.JoinChatRequestAsync(requestId, asker, message, cancellationToken);

    /// <inheritdoc />
    public Task<SlackChatCounts> ChatCountsAsync(Guid threadRequestId, int routeId, DateTime nowUtc, CancellationToken cancellationToken) =>
        _store.ChatCountsAsync(threadRequestId, routeId, nowUtc, cancellationToken);

    /// <inheritdoc />
    public Task<SlackChatOpenResult> OpenChatRequestAsync(SlackChatThread thread, Guid parentRequestId, IReadOnlyList<SlackAsker> askers,
        IReadOnlyList<SlackPersonMessage> messages, int routeMaxPerHour, DateTime nowUtc, CancellationToken cancellationToken) =>
        _store.OpenChatRequestAsync(thread, parentRequestId, askers, messages, routeMaxPerHour, nowUtc,
            (connection, _, token) => DarlingAgentActionGate.LoadRoutesAsync(connection, _logger, token), cancellationToken);

    /// <inheritdoc />
    public Task JournalPersonMessageAsync(Guid? requestId, int? routeId, int? serverId, string actor, string outcome, JsonObject detail, string onceKey,
        CancellationToken cancellationToken) =>
        _store.JournalPersonMessageAsync(requestId, routeId, serverId, actor, outcome, detail, onceKey, cancellationToken);

    /// <inheritdoc />
    public Task<bool> MarkChatAnswerOnceAsync(Guid? requestId, int? routeId, int? serverId, string answerKey, string onceKey, CancellationToken cancellationToken) =>
        _store.MarkChatAnswerOnceAsync(requestId, routeId, serverId, answerKey, onceKey, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<SlackWaitingThread>> WaitingThreadsAsync(int limit, CancellationToken cancellationToken) =>
        _store.WaitingThreadsAsync(limit, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<SlackPersonMessage>> WaitingMessagesAsync(Guid threadRequestId, CancellationToken cancellationToken) =>
        _store.WaitingMessagesAsync(threadRequestId, cancellationToken);

    /// <inheritdoc />
    public Task RefuseMessageAsync(SlackPersonMessage message, CancellationToken cancellationToken) =>
        _store.RefusePersonMessageAsync(message, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<SlackUnclaimedChat>> UnclaimedChatRequestsAsync(DateTime openedBeforeUtc, CancellationToken cancellationToken) =>
        _store.UnclaimedChatRequestsAsync(openedBeforeUtc, cancellationToken);
}
