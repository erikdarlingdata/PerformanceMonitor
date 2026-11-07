/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7657: one Events API event that passed every listener filter. The ids are verified: <see cref="TeamId"/> is the
/// workspace the listener learned at connect, <see cref="AppId"/> the app the connection belongs to, and <see cref="Channel"/> a bot
/// channel of this store's own routes. <see cref="Event"/> is the inner <c>event</c> object and <see cref="Payload"/> the whole
/// Events API payload, both as Slack sent them.</summary>
public sealed record SlackInboundEvent(
    string TeamId,
    string AppId,
    string EventType,
    string Channel,
    string Ts,
    string? ThreadTs,
    string? UserId,
    JsonObject Event,
    JsonObject Payload);

/// <summary>#7657: one interactive payload (a button press) that passed every listener filter. <see cref="UserTeamId"/> equals
/// <see cref="TeamId"/>: a press from another workspace in a shared channel is dropped before this record exists. <see cref="Channel"/>
/// and <see cref="MessageTs"/> name the card the press came from, as the container says.</summary>
public sealed record SlackInboundInteraction(
    string TeamId,
    string AppId,
    string UserId,
    string UserTeamId,
    string Channel,
    string MessageTs,
    JsonObject Payload);

/// <summary>#7657: the seam the Socket Mode listener hands verified payloads to. The buttons (#7658) and the mentions (#7659) plug in
/// here; the listener itself only acknowledges, filters and dispatches. A handler that throws is logged and the listener goes on.</summary>
public interface ISlackEventHandlers
{
    /// <summary>An Events API event (<c>message</c>, <c>app_mention</c>) from a channel this store owns.</summary>
    Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken);

    /// <summary>An interactive payload (<c>block_actions</c>) from a card in a channel this store owns.</summary>
    Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken);
}

/// <summary>#7657: the handlers of the listener alone: each verified payload is logged at Debug by its type and dropped. Nothing
/// is stored and no agent is woken until a child adds a handler.</summary>
public sealed class LoggingSlackEventHandlers(ILogger logger) : ISlackEventHandlers
{
    private long _events;
    private long _interactions;

    /// <summary>How many events reached this handler.</summary>
    public long Events => Interlocked.Read(ref _events);

    /// <summary>How many interactive payloads reached this handler.</summary>
    public long Interactions => Interlocked.Read(ref _interactions);

    /// <inheritdoc />
    public Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _events);
        logger.LogDebug("Slack listener received a {EventType} event in a channel of this store; no handler takes it yet, so it is dropped", inbound.EventType);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _interactions);
        logger.LogDebug("Slack listener received an interactive payload from a card of this store; no handler takes it yet, so it is dropped");
        return Task.CompletedTask;
    }
}

/// <summary>#7657: one open Socket Mode connection, as the listener sees it. The product wraps a <c>ClientWebSocket</c>; a test hands
/// the listener an in-memory socket, so every ordering and reconnect rule is checked without a network.</summary>
public interface ISlackSocket : IDisposable
{
    /// <summary>The next whole text frame, or null once the peer has closed the socket.</summary>
    Task<string?> ReceiveAsync(CancellationToken cancellationToken);

    /// <summary>Sends one text frame (an acknowledgement).</summary>
    Task SendAsync(string text, CancellationToken cancellationToken);

    /// <summary>Closes the socket from this side. Never throws for a socket the peer already closed.</summary>
    Task CloseAsync(CancellationToken cancellationToken);
}

/// <summary>#7657: Slack's answer to <c>apps.connections.open</c>: the <c>wss://</c> URL of a fresh connection, or the error word.
/// <see cref="Error"/> never carries a token.</summary>
public sealed record SlackConnectionOpen(bool Ok, Uri? Url, string? Error);

/// <summary>#7657: Slack's answer to <c>auth.test</c> on the bot token: the workspace (<see cref="TeamId"/>), the bot's user id and
/// its bot id, or the error word. <see cref="Error"/> never carries a token.</summary>
public sealed record SlackAuthTest(bool Ok, string? TeamId, string? UserId, string? BotId, string? Error);

/// <summary>#7657: the three network calls the listener makes, behind one seam so a test replaces the network with scripted answers.
/// Each method takes the token it needs and never keeps it.</summary>
public interface ISlackSocketModeTransport
{
    /// <summary><c>auth.test</c> with the bot token: learns the workspace id the listener enforces on every payload.</summary>
    Task<SlackAuthTest> AuthTestAsync(string botToken, CancellationToken cancellationToken);

    /// <summary><c>apps.connections.open</c> with the app-level token: a one-use <c>wss://</c> URL.</summary>
    Task<SlackConnectionOpen> OpenConnectionAsync(string appToken, CancellationToken cancellationToken);

    /// <summary>Opens the WebSocket at <paramref name="url"/>.</summary>
    Task<ISlackSocket> ConnectAsync(Uri url, CancellationToken cancellationToken);
}
