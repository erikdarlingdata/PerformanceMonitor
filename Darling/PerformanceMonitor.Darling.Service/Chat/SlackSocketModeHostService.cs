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

/// <summary>
/// #7657: what the worker publishes for the Socket Mode listener: how to read the opened app-level token, the opened bot token and
/// the live routes. The worker owns the settings (it opens the sealed tokens and reloads them from the store), and the listener's host
/// is a separate hosted service, so the readers cross here as one immutable record. Null until the worker has opened the settings.
/// </summary>
public sealed class SlackListenerRuntimeState
{
    /// <summary>The published readers; each is read when needed, so a token saved later is seen without a restart.</summary>
    public sealed record Published(Func<string> AppToken, Func<string> BotToken, Func<IReadOnlyList<NotificationRoute>> Routes);

    private volatile Published? _current;

    /// <summary>Publishes the readers (worker only).</summary>
    public void Publish(Func<string> appToken, Func<string> botToken, Func<IReadOnlyList<NotificationRoute>> routes)
    {
        ArgumentNullException.ThrowIfNull(appToken);
        ArgumentNullException.ThrowIfNull(botToken);
        ArgumentNullException.ThrowIfNull(routes);
        _current = new Published(appToken, botToken, routes);
    }

    /// <summary>The latest publication, or null before the first.</summary>
    public Published? Read() => _current;
}

/// <summary>
/// #7657: the hosted service that owns the Socket Mode listener's lifetime. The app-level token alone turns the listener on: every
/// <see cref="SupervisorPollInterval"/> the supervisor reads the opened token the worker published, starts the listener when a token
/// is set and none runs, stops it when the token is cleared, and restarts it when either token changed (<see cref="DecideAction"/>). A
/// blank token means off; there is no other setting. The supervisor also logs the listener's counters and writes them to the status
/// file the diagnostics bundle reads (<see cref="SlackListenerStatusFile"/>). Registered always and self-gating, as the other hosts are.
/// </summary>
public sealed class SlackSocketModeHostService : BackgroundService
{
    /// <summary>The supervisor's per-tick verdict, pure over (running, token in use, token now).</summary>
    public enum SupervisorAction
    {
        /// <summary>Nothing to do.</summary>
        None,

        /// <summary>A token is set and no listener runs.</summary>
        Start,

        /// <summary>The token was cleared while a listener runs.</summary>
        Stop,

        /// <summary>The token changed while a listener runs.</summary>
        Restart,
    }

    /// <summary>How often the supervisor reads the published token and the counters.</summary>
    public static readonly TimeSpan SupervisorPollInterval = TimeSpan.FromSeconds(5);

    /// <summary>How often a running listener's counters are logged at Information.</summary>
    public static readonly TimeSpan StatusLogInterval = TimeSpan.FromMinutes(10);

    /// <summary>How long a stop waits for the listener's tasks to end before the host moves on.</summary>
    public static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);

    private readonly ILogger<SlackSocketModeHostService> _logger;
    private readonly SlackListenerRuntimeState _state;
    private readonly ISlackSocketModeTransport _transport;
    private readonly string _statusDirectory;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<DateTime> _now;
    private SlackSocketModeListener? _listener;
    private SlackListenerStatus? _status;
    private CancellationTokenSource? _listenerStop;
    private Task? _run;
    private string _tokenInUse = "";
    private string _botTokenInUse = "";
    private string? _lastWritten;
    private DateTime _lastStatusLog = DateTime.MinValue;

    /// <param name="logger">The log.</param>
    /// <param name="state">The worker's publication.</param>
    /// <param name="transport">The network; the product's Web API client by default, a scripted one in a test.</param>
    /// <param name="statusDirectory">Where the status file goes; the service's log directory by default.</param>
    /// <param name="delay">How a poll waits; a fake clock in a test.</param>
    /// <param name="nowUtc">The clock for the status file's stamp.</param>
    public SlackSocketModeHostService(
        ILogger<SlackSocketModeHostService> logger,
        SlackListenerRuntimeState state,
        ISlackSocketModeTransport? transport = null,
        string? statusDirectory = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<DateTime>? nowUtc = null)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(state);
        _logger = logger;
        _state = state;
        _transport = transport ?? new SlackSocketModeWebApi();
        _statusDirectory = string.IsNullOrWhiteSpace(statusDirectory) ? DarlingFileLoggerProvider.DefaultLogDirectory() : statusDirectory;
        _delay = delay ?? (static (wait, ct) => Task.Delay(wait, ct));
        _now = nowUtc ?? (static () => DateTime.UtcNow);
    }

    /// <summary>Whether a listener runs now.</summary>
    public bool IsRunning => _run is not null;

    /// <summary>The running listener's counters, or null when none runs.</summary>
    public SlackListenerStatus? CurrentStatus => _status;

    /// <summary>PURE: the supervisor's verdict. A blank app-level <paramref name="token"/> means off. A change to either token while
    /// the listener runs restarts it, so the workspace and app ids are learned again with the new bot token.</summary>
    public static SupervisorAction DecideAction(bool running, string? tokenInUse, string? botTokenInUse, string? token, string? botToken)
    {
        var wanted = (token ?? "").Trim();
        if (!running)
        {
            return wanted.Length == 0 ? SupervisorAction.None : SupervisorAction.Start;
        }

        if (wanted.Length == 0)
        {
            return SupervisorAction.Stop;
        }

        var same = string.Equals(wanted, (tokenInUse ?? "").Trim(), StringComparison.Ordinal)
            && string.Equals((botToken ?? "").Trim(), (botTokenInUse ?? "").Trim(), StringComparison.Ordinal);
        return same ? SupervisorAction.None : SupervisorAction.Restart;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await TickAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Slack listener supervisor tick failed; the next tick runs as usual");
                }

                try
                {
                    await _delay(SupervisorPollInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        finally
        {
            await StopListenerAsync();
        }
    }

    /// <summary>One supervisor tick: the verdict, then the counters. Public so a test drives ticks without the poll.</summary>
    public async Task TickAsync(CancellationToken cancellationToken)
    {
        var published = _state.Read();
        var token = "";
        var botToken = "";
        if (published is not null)
        {
            try
            {
                token = (published.AppToken() ?? "").Trim();
                botToken = (published.BotToken() ?? "").Trim();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Slack listener supervisor could not read the tokens; treated as blank");
            }
        }

        switch (DecideAction(IsRunning, _tokenInUse, _botTokenInUse, token, botToken))
        {
            case SupervisorAction.Start:
                StartListener(published!, token, botToken, cancellationToken);
                break;
            case SupervisorAction.Stop:
                _logger.LogInformation("Slack listener stopping: the app-level token was cleared");
                await StopListenerAsync();
                break;
            case SupervisorAction.Restart:
                _logger.LogInformation("Slack listener restarting: a token changed, so the workspace and app ids are learned again");
                await StopListenerAsync();
                StartListener(published!, token, botToken, cancellationToken);
                break;
        }

        if (_run is { IsCompleted: true })
        {
            /* The listener's run ended on its own (it should not: it ends only on cancel). Clear it so the next tick restarts it. */
            _logger.LogWarning("Slack listener run ended unexpectedly; it restarts on the next tick");
            await StopListenerAsync();
        }

        PublishStatus();
    }

    private void StartListener(SlackListenerRuntimeState.Published published, string token, string botToken, CancellationToken cancellationToken)
    {
        _status = new SlackListenerStatus(_now);
        _listener = new SlackSocketModeListener(
            _transport, published.AppToken, published.BotToken, published.Routes,
            new LoggingSlackEventHandlers(_logger), _status, _logger, nowUtc: _now);
        _listenerStop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _tokenInUse = token;
        _botTokenInUse = botToken;
        _lastStatusLog = _now();
        _run = _listener.RunAsync(_listenerStop.Token);
    }

    private async Task StopListenerAsync()
    {
        var run = _run;
        var stop = _listenerStop;
        if (run is null || stop is null)
        {
            return;
        }

        try
        {
            stop.Cancel();
            await run.WaitAsync(StopTimeout);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Slack listener did not stop within {Timeout}; the host moves on", StopTimeout);
        }
        catch (OperationCanceledException)
        {
            /* Stopped. */
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Slack listener run ended with an error");
        }
        finally
        {
            stop.Dispose();
            _run = null;
            _listenerStop = null;
            _listener = null;
            _tokenInUse = "";
            _botTokenInUse = "";
            _status?.Stopped();
            PublishStatus();
        }
    }

    /// <summary>Writes the counters to the status file when they changed, and logs them on <see cref="StatusLogInterval"/>.</summary>
    private void PublishStatus()
    {
        var status = _status;
        if (status is null)
        {
            return;
        }

        var snapshot = status.Read();
        var json = SlackListenerStatus.ToJson(snapshot).ToJsonString();
        if (!string.Equals(json, _lastWritten, StringComparison.Ordinal))
        {
            var failure = SlackListenerStatusFile.Write(_statusDirectory, snapshot, _now());
            if (failure is not null)
            {
                _logger.LogDebug("Slack listener status file was not written: {Failure}", failure);
            }

            _lastWritten = json;
        }

        if (snapshot.Running && _now() - _lastStatusLog >= StatusLogInterval)
        {
            _lastStatusLog = _now();
            _logger.LogInformation("Slack listener: {Status}", SlackListenerStatus.Describe(snapshot));
        }
    }
}
