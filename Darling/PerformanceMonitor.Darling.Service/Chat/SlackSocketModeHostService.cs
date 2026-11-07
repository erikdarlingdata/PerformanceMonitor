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
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7657: what the worker publishes for the listener's host.</summary>
public sealed class SlackListenerRuntimeState
{
    public sealed record Published(Func<string> AppToken, Func<string> BotToken, Func<IReadOnlyList<NotificationRoute>> Routes);

    private volatile Published? _current;

    public void Publish(Func<string> appToken, Func<string> botToken, Func<IReadOnlyList<NotificationRoute>> routes) =>
        _current = new Published(appToken, botToken, routes);

    public Published? Read() => _current;
}

/// <summary>#7657: the listener's host. Not built yet: it never starts a listener.</summary>
public sealed class SlackSocketModeHostService : BackgroundService
{
    public enum SupervisorAction { None, Start, Stop, Restart }

    public static readonly TimeSpan SupervisorPollInterval = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan StatusLogInterval = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);

    public SlackSocketModeHostService(
        ILogger<SlackSocketModeHostService> logger,
        SlackListenerRuntimeState state,
        ISlackSocketModeTransport? transport = null,
        string? statusDirectory = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<DateTime>? nowUtc = null)
    {
    }

    public bool IsRunning => false;

    public SlackListenerStatus? CurrentStatus => null;

    public static SupervisorAction DecideAction(bool running, string? tokenInUse, string? token) => SupervisorAction.None;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;

    public Task TickAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
