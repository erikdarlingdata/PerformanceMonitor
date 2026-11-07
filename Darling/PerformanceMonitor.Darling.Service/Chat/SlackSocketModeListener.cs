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
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>
/// #7657: the Socket Mode listener. It holds <see cref="Connections"/> outbound WebSocket connections to Slack (two, as the design
/// fixes it: Slack sends each event to one of them, and a reconnect on one leaves the other listening), so the store host needs no
/// inbound port. Each connection's receive loop does three things with an envelope and nothing else: it acknowledges it on the same
/// socket as soon as it arrives, before parsing the payload, so Slack never retries for a slow handler (the prototype's late
/// acknowledgements cost 457 retries in a day); it puts the payload on the bounded queue; and it reads the next frame. One consumer
/// drains the queue, runs the filters (<see cref="SlackEnvelopeFilter"/>: the <c>bot_id</c> drop first, then the workspace and app
/// ids learned at connect, then the channel-ownership rule, then the duplicate check by channel and ts) and hands what passes to
/// <see cref="ISlackEventHandlers"/>.
///
/// <para>The ids come from the connect: <c>auth.test</c> on the bot token gives the workspace id (<c>team_id</c>, the one workspace;
/// never <c>enterprise.id</c>, which would admit every workspace of an org) and the bot's user id; Slack's <c>hello</c> on each
/// connection gives the app id. Until both are known every payload fails closed.</para>
///
/// <para>Slack sends <c>disconnect</c> (reason <c>warning</c>) about every five hours on each connection; the loop closes that socket
/// and opens a new one at once. A connection that fails or closes without a <c>disconnect</c> is reopened after a backoff that grows
/// to a minute. The queue drops its oldest item when full and counts the drop: a blocked receive loop would recreate the slow
/// acknowledgement, and Slack's natural keys (channel and ts; <c>WHERE state = 'pending'</c>) make a lost event safe. Cancellation
/// closes every socket and ends the consumer; nothing is persisted, so a restart loses only what was queued, by design.</para>
/// </summary>
public sealed class SlackSocketModeListener
{
    /// <summary>How many connections the listener holds. The design fixes it; there is no setting.</summary>
    public const int DefaultConnections = 2;

    /// <summary>How many acknowledged payloads wait for the consumer before the oldest is dropped.</summary>
    public const int DefaultQueueCapacity = 1000;

    /// <summary>The longest wait between two connect attempts.</summary>
    public static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(60);

    private readonly record struct QueuedEnvelope(string Type, JsonObject Payload, int Connection);

    private readonly ISlackSocketModeTransport _transport;
    private readonly Func<string> _appToken;
    private readonly Func<string> _botToken;
    private readonly Func<IReadOnlyList<NotificationRoute>> _routes;
    private readonly ISlackEventHandlers _handlers;
    private readonly SlackListenerStatus _status;
    private readonly ILogger _logger;
    private readonly Func<int, TimeSpan> _backoff;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Channel<QueuedEnvelope> _queue;
    private readonly SlackEventDedupe _dedupe = new();
    private readonly object _idsLock = new();
    private Task<bool>? _idsInFlight;
    private readonly bool[] _open;
    private readonly HashSet<string> _warnedDrops = new(StringComparer.Ordinal);
    private readonly object _warnLock = new();
    private string? _teamId;
    private string? _appId;
    private readonly Func<DateTime> _now;
    private long _unownedSinceWarning;
    private DateTime? _lastUnownedWarningUtc;

    /// <summary>How often the listener warns that events keep arriving for channels no route of this store owns. The likely cause is
    /// a second store listening on the same Slack app: Socket Mode hands each event to one connection, so both stores acknowledge and
    /// lose each other's events. One line per window names it; the drops themselves are counted in the status.</summary>
    public static readonly TimeSpan UnownedChannelWarningWindow = TimeSpan.FromMinutes(10);

    /// <param name="transport">The network: the product's <see cref="SlackSocketModeWebApi"/>, or a test's scripted one.</param>
    /// <param name="appToken">The opened app-level token, read at each connect.</param>
    /// <param name="botToken">The opened bot token, read for <c>auth.test</c>.</param>
    /// <param name="routes">The live routes: a channel is owned when a route's <see cref="NotificationRoute.SlackBotChannel"/> names it.</param>
    /// <param name="handlers">Where verified payloads go.</param>
    /// <param name="status">The counters the host logs and writes.</param>
    /// <param name="logger">The log.</param>
    /// <param name="connections">How many connections to hold; <see cref="DefaultConnections"/> in the product.</param>
    /// <param name="queueCapacity">The queue's bound; <see cref="DefaultQueueCapacity"/> in the product.</param>
    /// <param name="backoff">The wait before connect attempt <c>n</c> (1-based) after a failure; doubling to <see cref="MaxBackoff"/> by default.</param>
    /// <param name="delay">How the wait is taken; <see cref="Task.Delay(TimeSpan, CancellationToken)"/> in the product, a fake clock in a test.</param>
    /// <param name="nowUtc">The clock for the rate-limited warning; a fake clock in a test.</param>
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
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<DateTime>? nowUtc = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(appToken);
        ArgumentNullException.ThrowIfNull(botToken);
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(handlers);
        ArgumentNullException.ThrowIfNull(status);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThan(connections, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(queueCapacity, 1);
        _transport = transport;
        _appToken = appToken;
        _botToken = botToken;
        _routes = routes;
        _handlers = handlers;
        _status = status;
        _logger = logger;
        Connections = connections;
        _open = new bool[connections];
        _backoff = backoff ?? DefaultBackoff;
        _delay = delay ?? (static (wait, ct) => Task.Delay(wait, ct));
        _now = nowUtc ?? (static () => DateTime.UtcNow);
        _queue = Channel.CreateBounded<QueuedEnvelope>(
            new BoundedChannelOptions(queueCapacity) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true, SingleWriter = false },
            _ => _status.QueueDropped());
        _status.QueueDepthSource = () => _queue.Reader.Count;
    }

    /// <summary>How many connections this listener holds.</summary>
    public int Connections { get; }

    /// <summary>The workspace id learned from <c>auth.test</c>; null until the connect learned it.</summary>
    public string? TeamId => Volatile.Read(ref _teamId);

    /// <summary>The app id learned from the first <c>hello</c>; null until a connection opened.</summary>
    public string? AppId => Volatile.Read(ref _appId);

    /// <summary>The bot's user id from <c>auth.test</c>, for the mention detection a later child adds.</summary>
    public string? BotUserId { get; private set; }

    /// <summary>The bot's bot id from <c>auth.test</c>.</summary>
    public string? BotId { get; private set; }

    /// <summary>PURE: the default wait before connect attempt <paramref name="attempt"/>: 2, 4, 8, 16, 32 seconds, then <see cref="MaxBackoff"/>.</summary>
    public static TimeSpan DefaultBackoff(int attempt)
    {
        var n = attempt < 1 ? 1 : attempt;
        var seconds = Math.Pow(2, Math.Min(n, 6));
        return seconds >= MaxBackoff.TotalSeconds ? MaxBackoff : TimeSpan.FromSeconds(seconds);
    }

    /// <summary>PURE: the acknowledgement frame for <paramref name="envelopeId"/>.</summary>
    public static string AckFrame(string envelopeId) =>
        JsonSerializer.Serialize(new Dictionary<string, string>(StringComparer.Ordinal) { ["envelope_id"] = envelopeId });

    /// <summary>Runs the connections and the consumer until <paramref name="cancellationToken"/> is cancelled, then closes every
    /// socket and returns. Never throws for a cancel.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _status.Started(Connections);
        _logger.LogInformation("Slack listener starting with {Connections} Socket Mode connections", Connections);
        var consumer = ConsumeAsync(cancellationToken);
        var loops = Enumerable.Range(0, Connections).Select(i => ConnectionLoopAsync(i, cancellationToken)).ToArray();
        try
        {
            await Task.WhenAll(loops);
        }
        finally
        {
            _queue.Writer.TryComplete();
            try
            {
                await consumer;
            }
            catch (OperationCanceledException)
            {
                /* The consumer ends with the token. */
            }

            _status.Stopped();
            _logger.LogInformation("Slack listener stopped: {Status}", SlackListenerStatus.Describe(_status.Read()));
        }
    }

    private async Task ConnectionLoopAsync(int index, CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            ISlackSocket? socket = null;
            try
            {
                if (!await EnsureIdsAsync(cancellationToken))
                {
                    attempt++;
                    await _delay(_backoff(attempt), cancellationToken);
                    continue;
                }

                var open = await _transport.OpenConnectionAsync(_appToken(), cancellationToken);
                if (!open.Ok || open.Url is null)
                {
                    var error = open.Error ?? "apps.connections.open failed";
                    _status.ConnectFailed(error);
                    attempt++;
                    _logger.LogWarning("Slack listener connection {Index} could not open: {Error}; next attempt in {Wait}", index, error, _backoff(attempt));
                    await _delay(_backoff(attempt), cancellationToken);
                    continue;
                }

                socket = await _transport.ConnectAsync(open.Url, cancellationToken);
                var reason = await ReceiveLoopAsync(socket, index, cancellationToken);
                var wasOpen = CloseAccounting(index, reason);
                if (wasOpen)
                {
                    /* The connection was healthy (it received hello), so the next retry starts the backoff over. */
                    attempt = 0;
                }

                if (reason.IsDisconnect)
                {
                    /* Slack's own disconnect: reconnect at once. */
                    attempt = 0;
                    _logger.LogInformation("Slack listener connection {Index} received disconnect ({Reason}); reconnecting", index, reason.Text);
                    continue;
                }

                attempt++;
                _logger.LogWarning("Slack listener connection {Index} closed ({Reason}, was open: {WasOpen}); next attempt in {Wait}", index, reason.Text, wasOpen, _backoff(attempt));
                await _delay(_backoff(attempt), cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                CloseAccounting(index, new CloseReason("stopped", false));
                break;
            }
            catch (Exception ex)
            {
                var error = SlackBotClient.Redact(SlackBotClient.Redact(ex.GetBaseException().Message, _appToken() ?? ""), _botToken() ?? "");
                if (!CloseAccounting(index, new CloseReason(error, false)))
                {
                    _status.ConnectFailed(error);
                }

                attempt++;
                _logger.LogWarning("Slack listener connection {Index} failed: {Error}; next attempt in {Wait}", index, error, _backoff(attempt));
                try
                {
                    await _delay(_backoff(attempt), cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
            finally
            {
                if (socket is not null)
                {
                    await CloseQuietlyAsync(socket);
                    socket.Dispose();
                }
            }
        }
    }

    private readonly record struct CloseReason(string Text, bool IsDisconnect);

    /// <summary>Marks connection <paramref name="index"/> closed in the status when it was open. Returns whether it was.</summary>
    private bool CloseAccounting(int index, CloseReason reason)
    {
        lock (_open)
        {
            if (!_open[index])
            {
                return false;
            }

            _open[index] = false;
        }

        _status.ConnectionClosed(reason.Text);
        return true;
    }

    private async Task<bool> EnsureIdsAsync(CancellationToken cancellationToken)
    {
        if (TeamId is not null)
        {
            return true;
        }

        /* One auth.test at a time: the connection loops share the call in flight rather than each making its own. */
        Task<bool> inFlight;
        lock (_idsLock)
        {
            _idsInFlight ??= LearnIdsAsync(cancellationToken);
            inFlight = _idsInFlight;
        }

        try
        {
            return await inFlight.WaitAsync(cancellationToken);
        }
        finally
        {
            lock (_idsLock)
            {
                if (ReferenceEquals(_idsInFlight, inFlight) && inFlight.IsCompleted)
                {
                    _idsInFlight = null;
                }
            }
        }
    }

    private async Task<bool> LearnIdsAsync(CancellationToken cancellationToken)
    {
        var auth = await _transport.AuthTestAsync(_botToken(), cancellationToken);
        if (!auth.Ok || string.IsNullOrWhiteSpace(auth.TeamId))
        {
            var error = "auth.test: " + (auth.Error ?? (auth.Ok ? "no team_id in the answer" : "not ok"));
            _status.ConnectFailed(error);
            _logger.LogWarning("Slack listener could not learn the workspace id ({Error}); no connection opens until it can", error);
            return false;
        }

        BotUserId = auth.UserId;
        BotId = auth.BotId;
        Volatile.Write(ref _teamId, auth.TeamId);
        if (AppId is not null)
        {
            _status.IdsLearned();
        }

        _logger.LogInformation("Slack listener learned the workspace id from auth.test");
        return true;
    }

    private async Task<CloseReason> ReceiveLoopAsync(ISlackSocket socket, int index, CancellationToken cancellationToken)
    {
        while (true)
        {
            var text = await socket.ReceiveAsync(cancellationToken);
            if (text is null)
            {
                return new CloseReason("closed by peer", false);
            }

            _status.Received();
            JsonObject? envelope;
            try
            {
                envelope = JsonNode.Parse(text) as JsonObject;
            }
            catch (JsonException)
            {
                envelope = null;
            }

            if (envelope is null)
            {
                _status.Dropped(SlackEnvelopeFilter.Name(SlackEnvelopeVerdict.DropMalformed));
                continue;
            }

            var type = Text(envelope, "type");
            if (string.Equals(type, "hello", StringComparison.Ordinal))
            {
                LearnAppId(envelope);
                MarkOpen(index);
                continue;
            }

            if (string.Equals(type, "disconnect", StringComparison.Ordinal))
            {
                return new CloseReason(Text(envelope, "reason") ?? "disconnect", true);
            }

            /* The acknowledgement goes first, before the payload is looked at. */
            var envelopeId = Text(envelope, "envelope_id");
            if (!string.IsNullOrEmpty(envelopeId))
            {
                await socket.SendAsync(AckFrame(envelopeId), cancellationToken);
                _status.Acked();
            }

            if (envelope["payload"] is not JsonObject payload)
            {
                _status.Dropped(SlackEnvelopeFilter.Name(SlackEnvelopeVerdict.DropMalformed));
                continue;
            }

            _queue.Writer.TryWrite(new QueuedEnvelope(type ?? "", payload, index));
        }
    }

    private void MarkOpen(int index)
    {
        lock (_open)
        {
            if (_open[index])
            {
                return;
            }

            _open[index] = true;
        }

        _status.ConnectionOpened();
        _logger.LogInformation("Slack listener connection {Index} is open", index);
    }

    private void LearnAppId(JsonObject hello)
    {
        var appId = hello["connection_info"] is JsonObject info ? Text(info, "app_id") : null;
        if (string.IsNullOrWhiteSpace(appId))
        {
            return;
        }

        var known = AppId;
        if (known is null)
        {
            Volatile.Write(ref _appId, appId);
            if (TeamId is not null)
            {
                _status.IdsLearned();
            }

            return;
        }

        if (!string.Equals(known, appId, StringComparison.Ordinal))
        {
            _logger.LogWarning("Slack listener: a connection's hello named a different app id than the first; the first is kept and payloads of the other are dropped");
        }
    }

    private async Task ConsumeAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _queue.Reader.ReadAllAsync(cancellationToken))
            {
                var verdict = SlackEnvelopeFilter.Judge(item.Type, item.Payload, TeamId, AppId, ChannelOwned, out var inboundEvent, out var interaction);
                if (verdict == SlackEnvelopeVerdict.Accept && inboundEvent is not null && _dedupe.Seen(SlackEventDedupe.KeyOf(inboundEvent)))
                {
                    verdict = SlackEnvelopeVerdict.DropDuplicate;
                }

                if (verdict != SlackEnvelopeVerdict.Accept)
                {
                    var name = SlackEnvelopeFilter.Name(verdict);
                    _status.Dropped(name);
                    LogDrop(verdict, name, item.Connection);
                    continue;
                }

                _status.Dispatched();
                try
                {
                    if (inboundEvent is not null)
                    {
                        await _handlers.HandleEventAsync(inboundEvent, cancellationToken);
                    }
                    else if (interaction is not null)
                    {
                        await _handlers.HandleInteractiveAsync(interaction, cancellationToken);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Slack listener: a handler failed; the payload is dropped");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Stopped. */
        }
    }

    private void LogDrop(SlackEnvelopeVerdict verdict, string name, int connection)
    {
        if (verdict == SlackEnvelopeVerdict.DropChannel)
        {
            NoteUnownedChannel();
        }

        if (verdict is SlackEnvelopeVerdict.DropTeam or SlackEnvelopeVerdict.DropApp or SlackEnvelopeVerdict.DropUserTeam)
        {
            bool first;
            lock (_warnLock)
            {
                first = _warnedDrops.Add(name);
            }

            if (first)
            {
                _logger.LogWarning("Slack listener dropped a payload on connection {Index} by the {Filter} filter; later drops of this kind log at Debug", connection, name);
                return;
            }
        }

        _logger.LogDebug("Slack listener dropped a payload on connection {Index} by the {Filter} filter", connection, name);
    }

    /// <summary>One warning per <see cref="UnownedChannelWarningWindow"/> when events arrive for channels no route of this store owns,
    /// with the count since the last line. The window is measured on the injected clock, so a test pins it.</summary>
    private void NoteUnownedChannel()
    {
        long count;
        lock (_warnLock)
        {
            _unownedSinceWarning++;
            var now = _now();
            if (_lastUnownedWarningUtc is DateTime last && now - last < UnownedChannelWarningWindow)
            {
                return;
            }

            count = _unownedSinceWarning;
            _unownedSinceWarning = 0;
            _lastUnownedWarningUtc = now;
        }

        _logger.LogWarning(
            "Slack listener dropped {Count} event(s) from channels no route of this store owns. The likely cause is another store sharing this Slack app with its app-level token set: Socket Mode hands each event to one connection, so both stores acknowledge and lose each other's events. Set the app-level token on one store per app. The next line of this kind comes after {Window}",
            count, UnownedChannelWarningWindow);
    }

    private bool ChannelOwned(string channel)
    {
        try
        {
            var routes = _routes();
            return routes is not null && routes.Any(r => string.Equals((r.SlackBotChannel ?? "").Trim(), channel, StringComparison.Ordinal));
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Slack listener could not read the routes; the channel is treated as not owned");
            return false;
        }
    }

    private static async Task CloseQuietlyAsync(ISlackSocket socket)
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await socket.CloseAsync(timeout.Token);
        }
        catch (Exception)
        {
            /* The socket is disposed next either way. */
        }
    }

    private static string? Text(JsonObject node, string name) =>
        node[name] is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;
}
