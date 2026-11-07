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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7657: the Socket Mode listener. Not built yet: it opens its connections and reads every frame without acknowledging,
/// filtering or dispatching anything.</summary>
public sealed class SlackSocketModeListener
{
    public const int DefaultConnections = 2;
    public const int DefaultQueueCapacity = 1000;
    public static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(60);

    private readonly ISlackSocketModeTransport _transport;
    private readonly Func<string> _appToken;

    public SlackSocketModeListener(
        ISlackSocketModeTransport transport,
        Func<string> appToken,
        Func<string> botToken,
        Func<IReadOnlyList<NotificationRoute>> routes,
        ISlackEventHandlers handlers,
        SlackListenerStatus status,
        ILogger logger,
        int connections = DefaultConnections,
        int queueCapacity = DefaultQueueCapacity,
        Func<int, TimeSpan>? backoff = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _transport = transport;
        _appToken = appToken;
        Connections = connections;
    }

    public int Connections { get; }
    public string? TeamId => null;
    public string? AppId => null;
    public string? BotUserId => null;
    public string? BotId => null;

    public static TimeSpan DefaultBackoff(int attempt) => TimeSpan.Zero;

    public static string AckFrame(string envelopeId) => "";

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            var loops = Enumerable.Range(0, Connections).Select(_ => ReadAsync(cancellationToken)).ToArray();
            await Task.WhenAll(loops);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ReadAsync(CancellationToken cancellationToken)
    {
        var open = await _transport.OpenConnectionAsync(_appToken(), cancellationToken);
        if (!open.Ok || open.Url is null)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return;
        }

        var socket = await _transport.ConnectAsync(open.Url, cancellationToken);
        while (await socket.ReceiveAsync(cancellationToken) is not null)
        {
        }
    }
}
