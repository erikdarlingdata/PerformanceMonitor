/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An in-memory Socket Mode connection (#7657): the test feeds frames in, the listener's acknowledgements come out, and an event
/// log records the order in which frames were handed out and acknowledgements sent. Nothing waits on a clock: a receive with no
/// frame pending waits for the next one, or for the test to close the socket.
/// </summary>
internal sealed class FakeSlackSocket : ISlackSocket
{
    private readonly Channel<string?> _frames = Channel.CreateUnbounded<string?>();
    private readonly ConcurrentQueue<string> _sent = new();
    private readonly ConcurrentQueue<string> _events = new();
    private readonly Counter _sends = new();
    private int _handedOut;

    public int Index { get; init; }

    public bool Closed { get; private set; }

    public IReadOnlyList<string> Sent => _sent.ToArray();

    /// <summary><c>recv:&lt;n&gt;</c> when the n-th frame is handed to the listener, <c>send:&lt;envelope id&gt;</c> when an acknowledgement is sent.</summary>
    public IReadOnlyList<string> Events => _events.ToArray();

    public void Feed(string frame) => _frames.Writer.TryWrite(frame);

    /// <summary>The peer closes the socket: the next receive answers null.</summary>
    public void CloseFromPeer() => _frames.Writer.TryWrite(null);

    public Task WhenSent(int atLeast) => _sends.WhenAtLeast(atLeast);

    public async Task<string?> ReceiveAsync(CancellationToken cancellationToken)
    {
        var frame = await _frames.Reader.ReadAsync(cancellationToken);
        if (frame is not null)
        {
            _events.Enqueue("recv:" + Interlocked.Increment(ref _handedOut));
        }

        return frame;
    }

    public Task SendAsync(string text, CancellationToken cancellationToken)
    {
        _sent.Enqueue(text);
        var id = JsonNode.Parse(text)?["envelope_id"]?.GetValue<string>() ?? "?";
        _events.Enqueue("send:" + id);
        _sends.Increment();
        return Task.CompletedTask;
    }

    public Task CloseAsync(CancellationToken cancellationToken)
    {
        Closed = true;
        return Task.CompletedTask;
    }

    public void Dispose() { }
}

/// <summary>The scripted network (#7657): <c>auth.test</c> and <c>apps.connections.open</c> answers, and one <see cref="FakeSlackSocket"/>
/// per connect, in order.</summary>
internal sealed class FakeSocketModeTransport : ISlackSocketModeTransport
{
    private readonly ConcurrentQueue<FakeSlackSocket> _sockets = new();
    private readonly Counter _connects = new();
    private int _created;

    public SlackAuthTest Auth { get; set; } = new(true, "T0TEAM", "U0BOT", "B0BOT", null);

    public SlackConnectionOpen Open { get; set; } = new(true, new Uri("wss://socket.example/link/1"), null);

    public int AuthCalls { get; private set; }

    public int OpenCalls { get; private set; }

    public ConcurrentQueue<string> AppTokensSeen { get; } = new();

    public ConcurrentQueue<string> BotTokensSeen { get; } = new();

    public IReadOnlyList<FakeSlackSocket> Sockets => _sockets.ToArray();

    public Task WhenConnected(int atLeast) => _connects.WhenAtLeast(atLeast);

    public Task<SlackAuthTest> AuthTestAsync(string botToken, CancellationToken cancellationToken)
    {
        AuthCalls++;
        BotTokensSeen.Enqueue(botToken);
        return Task.FromResult(Auth);
    }

    public Task<SlackConnectionOpen> OpenConnectionAsync(string appToken, CancellationToken cancellationToken)
    {
        OpenCalls++;
        AppTokensSeen.Enqueue(appToken);
        return Task.FromResult(Open);
    }

    public Task<ISlackSocket> ConnectAsync(Uri url, CancellationToken cancellationToken)
    {
        var socket = new FakeSlackSocket { Index = Interlocked.Increment(ref _created) };
        _sockets.Enqueue(socket);
        _connects.Increment();
        return Task.FromResult<ISlackSocket>(socket);
    }
}

/// <summary>Handlers that record what reached them and, while <see cref="Block"/> is set, hold the first call until the test releases it.</summary>
internal sealed class RecordingSlackHandlers : ISlackEventHandlers
{
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Counter _handled = new();

    public bool Block { get; init; }

    public ConcurrentQueue<SlackInboundEvent> Events { get; } = new();

    public ConcurrentQueue<SlackInboundInteraction> Interactions { get; } = new();

    public int Handled => _handled.Value;

    /// <summary>Completes when the first blocked call has entered the handler.</summary>
    public Task Entered => _entered.Task;

    public void Release() => _release.TrySetResult();

    public Task WhenHandled(int atLeast) => _handled.WhenAtLeast(atLeast);

    public async Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken)
    {
        Events.Enqueue(inbound);
        await GateAsync(cancellationToken);
        _handled.Increment();
    }

    public async Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken)
    {
        Interactions.Enqueue(inbound);
        await GateAsync(cancellationToken);
        _handled.Increment();
    }

    private async Task GateAsync(CancellationToken cancellationToken)
    {
        if (!Block)
        {
            return;
        }

        _entered.TrySetResult();
        await _release.Task.WaitAsync(cancellationToken);
    }
}

/// <summary>A counter a test can await: <see cref="WhenAtLeast"/> completes once the count reaches the target. No polling and no clock.</summary>
internal sealed class Counter
{
    private readonly object _lock = new();
    private readonly List<(int Target, TaskCompletionSource Done)> _waiters = new();
    private int _value;

    public int Value
    {
        get
        {
            lock (_lock)
            {
                return _value;
            }
        }
    }

    public void Increment()
    {
        List<TaskCompletionSource>? ready = null;
        lock (_lock)
        {
            _value++;
            for (var i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Target <= _value)
                {
                    (ready ??= new()).Add(_waiters[i].Done);
                    _waiters.RemoveAt(i);
                }
            }
        }

        ready?.ForEach(w => w.TrySetResult());
    }

    public Task WhenAtLeast(int target)
    {
        lock (_lock)
        {
            if (_value >= target)
            {
                return Task.CompletedTask;
            }

            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((target, done));
            return done.Task;
        }
    }
}

/// <summary>The listener's backoff clock as a recorder (#7657): every wait is noted and none is taken. A wait blocks until the
/// listener is cancelled, so a failing connect loop does not spin; the test reads the waits it asked for.</summary>
internal sealed class FakeBackoffClock
{
    private readonly ConcurrentQueue<TaskCompletionSource> _pending = new();

    public ConcurrentQueue<TimeSpan> Waits { get; } = new();

    public Counter Calls { get; } = new();

    public Task DelayAsync(TimeSpan wait, CancellationToken cancellationToken)
    {
        Waits.Enqueue(wait);
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cancellationToken.Register(() => blocked.TrySetCanceled(cancellationToken));
        _pending.Enqueue(blocked);
        Calls.Increment();
        return blocked.Task;
    }

    /// <summary>Lets the oldest pending wait end, as the clock reaching it would.</summary>
    public void ReleaseOne()
    {
        if (_pending.TryDequeue(out var waiter))
        {
            waiter.TrySetResult();
        }
    }
}

/// <summary>A logger that keeps every line, so a test counts the warnings the listener writes.</summary>
internal sealed class RecordingLogger : ILogger
{
    public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

    public IEnumerable<string> Warnings => Entries.Where(e => e.Level == LogLevel.Warning).Select(e => e.Message);

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        Entries.Enqueue((logLevel, formatter(state, exception)));
}

public sealed class SlackSocketModeListenerTests
{
    private const string Team = "T0TEAM";
    private const string App = "A0APP";
    private const string OwnedChannel = "C0OWNED";
    private const string OtherChannel = "C0OTHER";
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);

    private static List<NotificationRoute> Routes(params string[] channels) =>
        channels.Select((c, i) => new NotificationRoute(100 + i, "Route " + i, "", "", "", "", "", Enabled: true) { SlackBotChannel = c }).ToList();

    private static (SlackSocketModeListener Listener, FakeSocketModeTransport Transport, RecordingSlackHandlers Handlers, SlackListenerStatus Status, FakeBackoffClock Clock) Build(
        bool block = false, int connections = 2, int queueCapacity = SlackSocketModeListener.DefaultQueueCapacity, FakeSocketModeTransport? transport = null,
        bool defaultBackoff = false, Func<DateTime>? nowUtc = null, ILogger? logger = null, params string[] owned)
    {
        transport ??= new FakeSocketModeTransport();
        nowUtc ??= static () => new DateTime(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);
        var handlers = new RecordingSlackHandlers { Block = block };
        var status = new SlackListenerStatus(nowUtc);
        var clock = new FakeBackoffClock();
        var routes = Routes(owned.Length == 0 ? new[] { OwnedChannel } : owned);
        var listener = new SlackSocketModeListener(
            transport, () => "app-token-a", () => "bot-token-a", () => routes, handlers, status, logger ?? NullLogger.Instance,
            connections, queueCapacity, backoff: defaultBackoff ? null : attempt => TimeSpan.FromSeconds(attempt), delay: clock.DelayAsync, nowUtc: nowUtc);
        return (listener, transport, handlers, status, clock);
    }

    private static string Hello(string appId = App) =>
        new JsonObject { ["type"] = "hello", ["num_connections"] = 1, ["connection_info"] = new JsonObject { ["app_id"] = appId } }.ToJsonString();

    private static string Disconnect(string reason = "warning") =>
        new JsonObject { ["type"] = "disconnect", ["reason"] = reason, ["debug_info"] = new JsonObject { ["host"] = "applink-1" } }.ToJsonString();

    private static string MessageEnvelope(string envelopeId, string channel = OwnedChannel, string ts = "1700000000.000100", string teamId = Team, string appId = App, string? botId = null, string? subtype = null, string eventType = "message", string user = "U0PERSON", string? authorTeam = Team, string? userTeam = null)
    {
        var ev = new JsonObject
        {
            ["type"] = eventType,
            ["channel"] = channel,
            ["ts"] = ts,
            ["user"] = user,
            ["text"] = "hello there",
            ["thread_ts"] = "1699999999.000001",
        };
        if (authorTeam is not null)
        {
            ev["team"] = authorTeam;
        }

        if (userTeam is not null)
        {
            ev["user_team"] = userTeam;
        }

        if (botId is not null)
        {
            ev["bot_id"] = botId;
        }

        if (subtype is not null)
        {
            ev["subtype"] = subtype;
        }

        var payload = new JsonObject { ["team_id"] = teamId, ["api_app_id"] = appId, ["event"] = ev, ["type"] = "event_callback" };
        return new JsonObject { ["type"] = "events_api", ["envelope_id"] = envelopeId, ["payload"] = payload, ["accepts_response_payload"] = false }.ToJsonString();
    }

    private static string PressEnvelope(string envelopeId, string channel = OwnedChannel, string teamId = Team, string appId = App, string userTeamId = Team)
    {
        var payload = new JsonObject
        {
            ["type"] = "block_actions",
            ["api_app_id"] = appId,
            ["team"] = new JsonObject { ["id"] = teamId, ["domain"] = "example" },
            ["user"] = new JsonObject { ["id"] = "U0PERSON", ["team_id"] = userTeamId, ["username"] = "person" },
            ["container"] = new JsonObject { ["type"] = "message", ["channel_id"] = channel, ["message_ts"] = "1700000000.000200" },
            ["actions"] = new JsonArray(new JsonObject { ["action_id"] = "proposal_approve_1", ["value"] = "42" }),
        };
        return new JsonObject { ["type"] = "interactive", ["envelope_id"] = envelopeId, ["payload"] = payload }.ToJsonString();
    }

    private static async Task<(Task Run, CancellationTokenSource Stop)> StartAsync(SlackSocketModeListener listener, FakeSocketModeTransport transport, int connections = 2)
    {
        var stop = new CancellationTokenSource();
        var run = listener.RunAsync(stop.Token);
        await transport.WhenConnected(connections).WaitAsync(Guard);
        foreach (var socket in transport.Sockets)
        {
            socket.Feed(Hello());
        }

        return (run, stop);
    }

    private static async Task StopAsync(Task run, CancellationTokenSource stop)
    {
        stop.Cancel();
        await run.WaitAsync(Guard);
        stop.Dispose();
    }

    [Fact]
    public async Task Acknowledges_every_envelope_before_any_handler_runs()
    {
        var (listener, transport, handlers, status, _) = Build(block: true);
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        for (var i = 1; i <= 100; i++)
        {
            socket.Feed(MessageEnvelope("env-" + i, ts: "1700000000." + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture)));
        }

        await socket.WhenSent(100).WaitAsync(Guard);
        await handlers.Entered.WaitAsync(Guard);

        /* Every acknowledgement went out while the one handler call is still blocked. */
        Assert.Equal(100, socket.Sent.Count);
        Assert.Equal(0, handlers.Handled);
        Assert.Single(handlers.Events);
        Assert.Equal(100, status.Read().Acked);

        /* Each frame's acknowledgement is sent before the next frame is read. */
        var events = socket.Events.Where(e => e != "recv:1").ToList();   /* the hello is the first frame handed out and has no ack */
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal("recv:" + (i + 2), events[2 * i]);
            Assert.Equal("send:env-" + (i + 1), events[2 * i + 1]);
        }

        handlers.Release();
        await handlers.WhenHandled(100).WaitAsync(Guard);
        Assert.Equal(100, handlers.Handled);
        await StopAsync(run, stop);
    }

    [Fact]
    public async Task Drops_bot_events_first_and_counts_them()
    {
        var (listener, transport, handlers, status, _) = Build();
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        /* The alert webhook's post: subtype bot_message with a bot_id. The bot's own post: a bot_id and the app's id. A bot post from the
           wrong team is still a bot drop, because that filter runs before the team check. */
        socket.Feed(MessageEnvelope("e1", botId: "B0HOOK", subtype: "bot_message", user: ""));
        socket.Feed(MessageEnvelope("e2", botId: "B0BOT", ts: "1700000000.000101"));
        socket.Feed(MessageEnvelope("e3", botId: "B0HOOK", teamId: "T9OTHER", ts: "1700000000.000102"));
        socket.Feed(MessageEnvelope("e4", ts: "1700000000.000103"));
        await handlers.WhenHandled(1).WaitAsync(Guard);
        await StopAsync(run, stop);

        var snapshot = status.Read();
        Assert.Equal(3, snapshot.DroppedByReason["bot"]);
        Assert.Equal(3, snapshot.DroppedByFilter);
        Assert.Equal(4, snapshot.Acked);
        Assert.Single(handlers.Events);
        Assert.Equal("U0PERSON", handlers.Events.Single().UserId);
    }

    [Fact]
    public async Task Drops_a_payload_from_another_workspace_or_another_app()
    {
        var (listener, transport, handlers, status, _) = Build();
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        socket.Feed(MessageEnvelope("e1", teamId: "T9OTHER"));
        socket.Feed(MessageEnvelope("e2", appId: "A9OTHER", ts: "1700000000.000101"));
        socket.Feed(PressEnvelope("e3", teamId: "T9OTHER"));
        socket.Feed(PressEnvelope("e4", appId: "A9OTHER"));
        socket.Feed(MessageEnvelope("e5", ts: "1700000000.000102"));
        await handlers.WhenHandled(1).WaitAsync(Guard);
        await StopAsync(run, stop);

        var snapshot = status.Read();
        Assert.Equal(2, snapshot.DroppedByReason["team"]);
        Assert.Equal(2, snapshot.DroppedByReason["app"]);
        Assert.Equal(Team, listener.TeamId);
        Assert.Equal(App, listener.AppId);
        Assert.True(snapshot.IdsLearned);
        Assert.Empty(handlers.Interactions);
        Assert.Single(handlers.Events);
    }

    [Fact]
    public async Task Drops_a_mention_by_a_person_of_another_workspace_in_an_owned_channel()
    {
        var (listener, transport, handlers, status, _) = Build();
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        /* A Slack Connect member's mention arrives under the host workspace's team_id, with the author's own workspace in user_team. */
        socket.Feed(MessageEnvelope("e1", eventType: "app_mention", userTeam: "T9SHARED"));
        socket.Feed(MessageEnvelope("e2", ts: "1700000000.000101", authorTeam: "T9SHARED"));
        socket.Feed(MessageEnvelope("e3", ts: "1700000000.000102", authorTeam: null));
        socket.Feed(MessageEnvelope("e4", ts: "1700000000.000103", botId: "B0HOOK", userTeam: "T9SHARED"));
        socket.Feed(MessageEnvelope("e5", eventType: "app_mention", ts: "1700000000.000104", userTeam: Team, authorTeam: "T9HOST"));
        await handlers.WhenHandled(1).WaitAsync(Guard);
        await StopAsync(run, stop);

        var snapshot = status.Read();
        Assert.Equal(2, snapshot.DroppedByReason["user_team"]);
        Assert.Equal(1, snapshot.DroppedByReason["malformed"]);
        Assert.Equal(1, snapshot.DroppedByReason["bot"]);
        Assert.Equal(5, snapshot.Acked);
        var inbound = Assert.Single(handlers.Events);
        Assert.Equal("1700000000.000104", inbound.Ts);
    }

    [Fact]
    public async Task Drops_a_press_from_a_person_of_another_workspace()
    {
        var (listener, transport, handlers, status, _) = Build();
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        socket.Feed(PressEnvelope("e1", userTeamId: "T9SHARED"));
        socket.Feed(PressEnvelope("e2"));
        await handlers.WhenHandled(1).WaitAsync(Guard);
        await StopAsync(run, stop);

        Assert.Equal(1, status.Read().DroppedByReason["user_team"]);
        var press = Assert.Single(handlers.Interactions);
        Assert.Equal(Team, press.UserTeamId);
        Assert.Equal(OwnedChannel, press.Channel);
        Assert.Equal("1700000000.000200", press.MessageTs);
        Assert.Equal("U0PERSON", press.UserId);
    }

    [Fact]
    public async Task Acts_only_on_channels_set_on_its_own_routes()
    {
        var (listener, transport, handlers, status, _) = Build();
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        socket.Feed(MessageEnvelope("e1", channel: OtherChannel));
        socket.Feed(PressEnvelope("e2", channel: OtherChannel));
        socket.Feed(MessageEnvelope("e3", eventType: "app_mention"));
        await handlers.WhenHandled(1).WaitAsync(Guard);
        await StopAsync(run, stop);

        Assert.Equal(2, status.Read().DroppedByReason["channel"]);
        var inbound = Assert.Single(handlers.Events);
        Assert.Equal("app_mention", inbound.EventType);
        Assert.Equal(OwnedChannel, inbound.Channel);
        Assert.Equal("1699999999.000001", inbound.ThreadTs);
    }

    [Fact]
    public async Task Dispatches_a_retried_event_once_by_channel_and_ts()
    {
        var (listener, transport, handlers, status, _) = Build();
        var (run, stop) = await StartAsync(listener, transport);
        /* The same event, three envelope ids, on both connections: Slack's retry shape. */
        transport.Sockets[0].Feed(MessageEnvelope("e1"));
        transport.Sockets[1].Feed(MessageEnvelope("e2"));
        transport.Sockets[0].Feed(MessageEnvelope("e3"));
        /* A different ts is a different event. */
        transport.Sockets[1].Feed(MessageEnvelope("e4", ts: "1700000000.000999"));
        await handlers.WhenHandled(2).WaitAsync(Guard);
        await transport.Sockets[0].WhenSent(2).WaitAsync(Guard);
        await transport.Sockets[1].WhenSent(2).WaitAsync(Guard);
        await StopAsync(run, stop);

        var snapshot = status.Read();
        Assert.Equal(2, snapshot.DroppedByReason["duplicate"]);
        Assert.Equal(4, snapshot.Acked);
        Assert.Equal(2, snapshot.Dispatched);
        Assert.Equal(2, handlers.Events.Count);
    }

    [Fact]
    public async Task Reconnects_at_once_on_disconnect_and_keeps_the_queue()
    {
        var (listener, transport, handlers, status, clock) = Build(block: true);
        var (run, stop) = await StartAsync(listener, transport);
        var first = transport.Sockets[0];
        first.Feed(MessageEnvelope("e1"));
        first.Feed(MessageEnvelope("e2", ts: "1700000000.000101"));
        await first.WhenSent(2).WaitAsync(Guard);
        first.Feed(Disconnect());

        await transport.WhenConnected(3).WaitAsync(Guard);
        var replacement = transport.Sockets[2];
        replacement.Feed(Hello());
        replacement.Feed(MessageEnvelope("e3", ts: "1700000000.000102"));
        await replacement.WhenSent(1).WaitAsync(Guard);

        Assert.True(first.Closed);
        Assert.Empty(clock.Waits);                 /* a disconnect reconnects without a backoff */
        Assert.Equal(3, transport.OpenCalls);
        Assert.Equal(1, transport.AuthCalls);      /* the ids are learned once */

        handlers.Release();
        await handlers.WhenHandled(3).WaitAsync(Guard);
        Assert.Equal(new[] { "1700000000.000100", "1700000000.000101", "1700000000.000102" }, handlers.Events.Select(e => e.Ts).OrderBy(t => t, StringComparer.Ordinal));
        var snapshot = status.Read();
        Assert.Equal("warning", snapshot.LastDisconnectReason);
        Assert.NotNull(snapshot.LastDisconnectUtc);
        Assert.Equal(2, snapshot.ConnectionsOpen);
        await StopAsync(run, stop);
    }

    [Fact]
    public async Task Holds_two_connections_and_takes_events_from_either()
    {
        var (listener, transport, handlers, status, _) = Build();
        var (run, stop) = await StartAsync(listener, transport);
        Assert.Equal(2, transport.Sockets.Count);
        Assert.Equal(2, listener.Connections);
        transport.Sockets[0].Feed(MessageEnvelope("e1"));
        transport.Sockets[1].Feed(MessageEnvelope("e2", ts: "1700000000.000101"));
        await handlers.WhenHandled(2).WaitAsync(Guard);
        var snapshot = status.Read();
        Assert.Equal(2, snapshot.ConnectionsOpen);
        Assert.Equal(2, snapshot.ConnectionsWanted);
        Assert.NotNull(snapshot.LastHelloUtc);
        Assert.Equal(2, transport.OpenCalls);
        Assert.Equal(new[] { "app-token-a", "app-token-a" }, transport.AppTokensSeen.ToArray());
        Assert.Equal(new[] { "bot-token-a" }, transport.BotTokensSeen.ToArray());
        await StopAsync(run, stop);
    }

    [Fact]
    public async Task Drops_the_oldest_queued_payload_when_the_queue_is_full()
    {
        var (listener, transport, handlers, status, _) = Build(block: true, queueCapacity: 3);
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        socket.Feed(MessageEnvelope("e1", ts: "1700000000.000001"));
        await handlers.Entered.WaitAsync(Guard);   /* the consumer holds item 1 */
        for (var i = 2; i <= 6; i++)
        {
            socket.Feed(MessageEnvelope("e" + i, ts: "1700000000.00000" + i));
        }

        await socket.WhenSent(6).WaitAsync(Guard);  /* every one was acknowledged, dropped or not */
        var full = status.Read();
        Assert.Equal(2, full.QueueDropped);
        Assert.Equal(3, full.QueueDepth);
        Assert.Equal(6, full.Acked);

        handlers.Release();
        await handlers.WhenHandled(4).WaitAsync(Guard);
        await StopAsync(run, stop);
        Assert.Equal(new[] { "1700000000.000001", "1700000000.000004", "1700000000.000005", "1700000000.000006" }, handlers.Events.Select(e => e.Ts).ToArray());
    }

    [Fact]
    public async Task Stops_cleanly_and_closes_every_socket()
    {
        var (listener, transport, handlers, status, _) = Build(block: true);
        var (run, stop) = await StartAsync(listener, transport);
        transport.Sockets[0].Feed(MessageEnvelope("e1"));
        await handlers.Entered.WaitAsync(Guard);
        Assert.True(status.Read().Running);

        stop.Cancel();
        await run.WaitAsync(Guard);
        Assert.True(run.IsCompletedSuccessfully);
        Assert.All(transport.Sockets, s => Assert.True(s.Closed));
        var snapshot = status.Read();
        Assert.False(snapshot.Running);
        Assert.Equal(0, snapshot.ConnectionsOpen);
        stop.Dispose();
    }

    [Fact]
    public async Task Opens_no_connection_until_auth_test_names_the_workspace()
    {
        var transport = new FakeSocketModeTransport { Auth = new SlackAuthTest(false, null, null, null, "invalid_auth") };
        var (listener, _, _, status, clock) = Build(transport: transport);
        using var stop = new CancellationTokenSource();
        var run = listener.RunAsync(stop.Token);
        await clock.Calls.WhenAtLeast(2).WaitAsync(Guard);   /* both connection loops backed off */
        Assert.Equal(0, transport.OpenCalls);
        Assert.Empty(transport.Sockets);
        Assert.Null(listener.TeamId);
        var snapshot = status.Read();
        Assert.False(snapshot.IdsLearned);
        Assert.Equal("auth.test: invalid_auth", snapshot.LastError);
        Assert.NotNull(snapshot.LastErrorUtc);
        Assert.Equal(TimeSpan.FromSeconds(1), clock.Waits.First());
        stop.Cancel();
        await run.WaitAsync(Guard);
    }

    [Fact]
    public async Task Backs_off_when_the_connection_cannot_open()
    {
        var transport = new FakeSocketModeTransport { Open = new SlackConnectionOpen(false, null, "HTTP 503") };
        var (listener, _, _, status, clock) = Build(transport: transport);
        using var stop = new CancellationTokenSource();
        var run = listener.RunAsync(stop.Token);
        await clock.Calls.WhenAtLeast(2).WaitAsync(Guard);
        Assert.Equal(2, transport.OpenCalls);
        Assert.Empty(transport.Sockets);
        Assert.Equal("HTTP 503", status.Read().LastError);
        stop.Cancel();
        await run.WaitAsync(Guard);
    }

    [Fact]
    public async Task Warns_once_per_window_while_events_keep_arriving_for_channels_no_route_owns()
    {
        var now = new DateTime(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);
        var log = new RecordingLogger();
        var (listener, transport, handlers, status, _) = Build(nowUtc: () => now, logger: log);
        var (run, stop) = await StartAsync(listener, transport);
        var socket = transport.Sockets[0];
        /* Two unowned events, then an owned one: the consumer is sequential, so once the owned one is handled both drops are judged. */
        socket.Feed(MessageEnvelope("e1", channel: OtherChannel, ts: "1700000000.000001"));
        socket.Feed(MessageEnvelope("e2", channel: OtherChannel, ts: "1700000000.000002"));
        socket.Feed(MessageEnvelope("e3", ts: "1700000000.000003"));
        await handlers.WhenHandled(1).WaitAsync(Guard);
        var first = Assert.Single(log.Warnings);
        Assert.Contains("another store sharing this Slack app", first, StringComparison.Ordinal);
        Assert.Contains("dropped 1 event(s)", first, StringComparison.Ordinal);

        /* Inside the window: more drops, no new line. */
        now = now.AddMinutes(9);
        socket.Feed(MessageEnvelope("e4", channel: OtherChannel, ts: "1700000000.000004"));
        socket.Feed(MessageEnvelope("e5", ts: "1700000000.000005"));
        await handlers.WhenHandled(2).WaitAsync(Guard);
        Assert.Single(log.Warnings);

        /* After the window: one more line, carrying the drops since the last one. */
        now = now.AddMinutes(2);
        socket.Feed(MessageEnvelope("e6", channel: OtherChannel, ts: "1700000000.000006"));
        socket.Feed(MessageEnvelope("e7", ts: "1700000000.000007"));
        await handlers.WhenHandled(3).WaitAsync(Guard);
        await StopAsync(run, stop);
        Assert.Equal(2, log.Warnings.Count());
        Assert.Contains("dropped 3 event(s)", log.Warnings.Last(), StringComparison.Ordinal);
        Assert.Equal(4, status.Read().DroppedByReason["channel"]);
    }

    [Fact]
    public async Task Starts_the_backoff_over_after_a_healthy_hello()
    {
        var (listener, transport, handlers, _, clock) = Build(defaultBackoff: true);
        var (run, stop) = await StartAsync(listener, transport);
        var first = transport.Sockets[0];
        first.CloseFromPeer();
        await clock.Calls.WhenAtLeast(1).WaitAsync(Guard);
        Assert.Equal(TimeSpan.FromSeconds(2), clock.Waits.ElementAt(0));

        /* The clock reaches the wait; the connection reopens, says hello and carries an event, then closes again. */
        clock.ReleaseOne();
        await transport.WhenConnected(3).WaitAsync(Guard);
        var second = transport.Sockets[2];
        second.Feed(Hello());
        second.Feed(MessageEnvelope("e1"));
        await handlers.WhenHandled(1).WaitAsync(Guard);
        second.CloseFromPeer();
        await clock.Calls.WhenAtLeast(2).WaitAsync(Guard);

        /* A healthy connection resets the count: the second retry waits 2 seconds again, not 4. */
        Assert.Equal(TimeSpan.FromSeconds(2), clock.Waits.ElementAt(1));
        await StopAsync(run, stop);
    }

    [Fact]
    public void Default_backoff_doubles_to_a_minute()
    {
        Assert.Equal(TimeSpan.FromSeconds(2), SlackSocketModeListener.DefaultBackoff(1));
        Assert.Equal(TimeSpan.FromSeconds(4), SlackSocketModeListener.DefaultBackoff(2));
        Assert.Equal(TimeSpan.FromSeconds(32), SlackSocketModeListener.DefaultBackoff(5));
        Assert.Equal(SlackSocketModeListener.MaxBackoff, SlackSocketModeListener.DefaultBackoff(6));
        Assert.Equal(SlackSocketModeListener.MaxBackoff, SlackSocketModeListener.DefaultBackoff(60));
        Assert.Equal("""{"envelope_id":"abc"}""", SlackSocketModeListener.AckFrame("abc"));
    }
}

public sealed class SlackEnvelopeFilterTests
{
    private static bool Owned(string channel) => channel == "C0OWNED";

    private static JsonObject Event(string? teamId = "T1", string? appId = "A1", string? channel = "C0OWNED", string? ts = "1.2", string? botId = null, string? type = "message", string? team = "T1", string? userTeam = null)
    {
        var ev = new JsonObject();
        if (type is not null) ev["type"] = type;
        if (channel is not null) ev["channel"] = channel;
        if (ts is not null) ev["ts"] = ts;
        if (botId is not null) ev["bot_id"] = botId;
        if (team is not null) ev["team"] = team;
        if (userTeam is not null) ev["user_team"] = userTeam;
        ev["user"] = "U1";
        var payload = new JsonObject { ["event"] = ev };
        if (teamId is not null) payload["team_id"] = teamId;
        if (appId is not null) payload["api_app_id"] = appId;
        return payload;
    }

    [Fact]
    public void Judges_each_filter_in_the_designed_order()
    {
        Assert.Equal(SlackEnvelopeVerdict.Accept, SlackEnvelopeFilter.Judge("events_api", Event(), "T1", "A1", Owned, out var ev, out var press));
        Assert.NotNull(ev);
        Assert.Null(press);
        Assert.Equal("T1", ev!.TeamId);
        Assert.Equal("A1", ev.AppId);
        Assert.Equal("1.2", ev.Ts);

        Assert.Equal(SlackEnvelopeVerdict.DropBot, SlackEnvelopeFilter.Judge("events_api", Event(botId: "B1", teamId: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropTeam, SlackEnvelopeFilter.Judge("events_api", Event(teamId: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropApp, SlackEnvelopeFilter.Judge("events_api", Event(appId: "A9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropChannel, SlackEnvelopeFilter.Judge("events_api", Event(channel: "C9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropMalformed, SlackEnvelopeFilter.Judge("events_api", Event(ts: null), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropMalformed, SlackEnvelopeFilter.Judge("events_api", new JsonObject { ["team_id"] = "T1" }, "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropMalformed, SlackEnvelopeFilter.Judge("events_api", null, "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropUnhandledType, SlackEnvelopeFilter.Judge("slash_commands", Event(), "T1", "A1", Owned, out _, out _));
    }

    [Fact]
    public void Judges_an_event_by_its_author_workspace()
    {
        /* user_team names a Slack Connect author's own workspace; team is the host's. user_team wins when both are there. */
        Assert.Equal(SlackEnvelopeVerdict.DropUserTeam, SlackEnvelopeFilter.Judge("events_api", Event(type: "app_mention", userTeam: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropUserTeam, SlackEnvelopeFilter.Judge("events_api", Event(team: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropUserTeam, SlackEnvelopeFilter.Judge("events_api", Event(team: null, userTeam: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.Accept, SlackEnvelopeFilter.Judge("events_api", Event(team: "T9", userTeam: "T1"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.Accept, SlackEnvelopeFilter.Judge("events_api", Event(team: null, userTeam: "T1"), "T1", "A1", Owned, out _, out _));
        /* Neither field: the author's workspace cannot be checked, so the event is not accepted. */
        Assert.Equal(SlackEnvelopeVerdict.DropMalformed, SlackEnvelopeFilter.Judge("events_api", Event(team: null), "T1", "A1", Owned, out _, out _));
        /* The bot filter still runs first, and the author check runs before the channel check. */
        Assert.Equal(SlackEnvelopeVerdict.DropBot, SlackEnvelopeFilter.Judge("events_api", Event(botId: "B1", userTeam: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropUserTeam, SlackEnvelopeFilter.Judge("events_api", Event(channel: "C9", userTeam: "T9"), "T1", "A1", Owned, out _, out _));
    }

    [Fact]
    public void Fails_closed_before_the_ids_are_learned()
    {
        Assert.Equal(SlackEnvelopeVerdict.DropTeam, SlackEnvelopeFilter.Judge("events_api", Event(), null, "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropApp, SlackEnvelopeFilter.Judge("events_api", Event(), "T1", null, Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropTeam, SlackEnvelopeFilter.Judge("events_api", Event(), "", "A1", Owned, out _, out _));
    }

    [Fact]
    public void Judges_a_press_by_team_app_user_team_and_card_channel()
    {
        static JsonObject Press(string team = "T1", string app = "A1", string userTeam = "T1", string? channel = "C0OWNED", string? messageTs = "9.9")
        {
            var container = new JsonObject { ["type"] = "message" };
            if (channel is not null) container["channel_id"] = channel;
            if (messageTs is not null) container["message_ts"] = messageTs;
            return new JsonObject
            {
                ["type"] = "block_actions",
                ["api_app_id"] = app,
                ["team"] = new JsonObject { ["id"] = team },
                ["user"] = new JsonObject { ["id"] = "U7", ["team_id"] = userTeam },
                ["container"] = container,
            };
        }

        Assert.Equal(SlackEnvelopeVerdict.Accept, SlackEnvelopeFilter.Judge("interactive", Press(), "T1", "A1", Owned, out var ev, out var press));
        Assert.Null(ev);
        Assert.NotNull(press);
        Assert.Equal("U7", press!.UserId);
        Assert.Equal("9.9", press.MessageTs);
        Assert.Equal(SlackEnvelopeVerdict.DropTeam, SlackEnvelopeFilter.Judge("interactive", Press(team: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropApp, SlackEnvelopeFilter.Judge("interactive", Press(app: "A9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropUserTeam, SlackEnvelopeFilter.Judge("interactive", Press(userTeam: "T9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropChannel, SlackEnvelopeFilter.Judge("interactive", Press(channel: "C9"), "T1", "A1", Owned, out _, out _));
        Assert.Equal(SlackEnvelopeVerdict.DropMalformed, SlackEnvelopeFilter.Judge("interactive", Press(messageTs: null), "T1", "A1", Owned, out _, out _));
    }

    [Fact]
    public void Dedupe_remembers_a_bounded_number_of_keys()
    {
        var dedupe = new SlackEventDedupe(capacity: 3);
        Assert.False(dedupe.Seen("a"));
        Assert.True(dedupe.Seen("a"));
        Assert.False(dedupe.Seen("b"));
        Assert.False(dedupe.Seen("c"));
        Assert.False(dedupe.Seen("d"));   /* a is forgotten */
        Assert.Equal(3, dedupe.Count);
        Assert.False(dedupe.Seen("a"));
        var inbound = new SlackInboundEvent("T", "A", "message", "C1", "1.5", null, null, new JsonObject(), new JsonObject());
        Assert.Equal("message|C1|1.5", SlackEventDedupe.KeyOf(inbound));
        Assert.Equal("duplicate", SlackEnvelopeFilter.Name(SlackEnvelopeVerdict.DropDuplicate));
        Assert.Equal("user_team", SlackEnvelopeFilter.Name(SlackEnvelopeVerdict.DropUserTeam));
    }
}

public sealed class SlackSocketModeHostTests
{
    [Fact]
    public void A_blank_app_token_means_off_and_either_token_changing_restarts()
    {
        const string Bot = "bot-token-a";
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.None, SlackSocketModeHostService.DecideAction(false, "", "", "", Bot));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.None, SlackSocketModeHostService.DecideAction(false, "", "", "   ", Bot));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.None, SlackSocketModeHostService.DecideAction(false, "", "", null, null));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.Start, SlackSocketModeHostService.DecideAction(false, "", "", "app-token-a", Bot));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.Start, SlackSocketModeHostService.DecideAction(false, "", "", "app-token-a", ""));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.Stop, SlackSocketModeHostService.DecideAction(true, "app-token-a", Bot, "", Bot));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.None, SlackSocketModeHostService.DecideAction(true, "app-token-a", Bot, " app-token-a ", Bot));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.Restart, SlackSocketModeHostService.DecideAction(true, "app-token-a", Bot, "app-token-b", Bot));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.Restart, SlackSocketModeHostService.DecideAction(true, "app-token-a", Bot, "app-token-a", "bot-token-b"));
        Assert.Equal(SlackSocketModeHostService.SupervisorAction.Restart, SlackSocketModeHostService.DecideAction(true, "app-token-a", Bot, "app-token-a", ""));
    }

    [Fact]
    public async Task The_host_restarts_the_listener_when_the_bot_token_changes_and_learns_the_ids_again()
    {
        var directory = Path.Combine(Path.GetTempPath(), "darling-slack-listener-" + Guid.NewGuid().ToString("N"));
        try
        {
            var transport = new FakeSocketModeTransport();
            var state = new SlackListenerRuntimeState();
            var bot = "bot-token-a";
            var routes = new List<NotificationRoute>();
            var host = new SlackSocketModeHostService(NullLogger<SlackSocketModeHostService>.Instance, state, transport, directory,
                delay: static (_, _) => Task.CompletedTask, nowUtc: () => new DateTime(2026, 11, 6, 13, 0, 0, DateTimeKind.Utc));
            state.Publish(() => "app-token-a", () => bot, () => routes);
            await host.TickAsync(CancellationToken.None);
            Assert.True(host.IsRunning);
            await transport.WhenConnected(2).WaitAsync(TimeSpan.FromSeconds(30));
            var firstStatus = host.CurrentStatus;

            bot = "bot-token-b";
            await host.TickAsync(CancellationToken.None);
            Assert.True(host.IsRunning);
            await transport.WhenConnected(4).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.NotSame(firstStatus, host.CurrentStatus);
            Assert.Equal(2, transport.AuthCalls);
            Assert.Equal(new[] { "bot-token-a", "bot-token-b" }, transport.BotTokensSeen.ToArray());
            Assert.All(transport.Sockets.Take(2), s => Assert.True(s.Closed));

            /* The same tokens again: nothing restarts. */
            await host.TickAsync(CancellationToken.None);
            Assert.Equal(2, transport.AuthCalls);
            Assert.Equal(4, transport.Sockets.Count);
            host.Dispose();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task The_host_starts_on_a_token_stops_when_it_clears_and_writes_the_status_file()
    {
        var directory = Path.Combine(Path.GetTempPath(), "darling-slack-listener-" + Guid.NewGuid().ToString("N"));
        try
        {
            var transport = new FakeSocketModeTransport();
            var state = new SlackListenerRuntimeState();
            var token = "";
            var routes = new List<NotificationRoute>();
            var host = new SlackSocketModeHostService(NullLogger<SlackSocketModeHostService>.Instance, state, transport, directory,
                delay: static (_, _) => Task.CompletedTask, nowUtc: () => new DateTime(2026, 11, 6, 13, 0, 0, DateTimeKind.Utc));

            /* Nothing published yet, then a blank token: no listener. */
            await host.TickAsync(CancellationToken.None);
            Assert.False(host.IsRunning);
            state.Publish(() => token, () => "bot-token-a", () => routes);
            await host.TickAsync(CancellationToken.None);
            Assert.False(host.IsRunning);
            Assert.Equal(0, transport.OpenCalls);
            Assert.False(File.Exists(SlackListenerStatusFile.PathUnder(directory)));

            /* A token: the listener starts and opens its two connections. */
            token = "app-token-a";
            await host.TickAsync(CancellationToken.None);
            Assert.True(host.IsRunning);
            await transport.WhenConnected(2).WaitAsync(TimeSpan.FromSeconds(30));
            foreach (var socket in transport.Sockets)
            {
                socket.Feed("""{"type":"hello","connection_info":{"app_id":"A0APP"}}""");
            }

            await host.TickAsync(CancellationToken.None);
            var section = SlackListenerStatusFile.ReadSection(directory);
            Assert.Equal("ok", section["status"]!.GetValue<string>());
            Assert.True(section["running"]!.GetValue<bool>());
            Assert.Equal(2, section["connections_wanted"]!.GetValue<int>());
            Assert.Equal("2026-11-06T13:00:00Z", section["written_at"]!.GetValue<string>());

            /* The token clears: the listener stops, its sockets close, and the file says so. */
            token = "";
            await host.TickAsync(CancellationToken.None);
            Assert.False(host.IsRunning);
            Assert.All(transport.Sockets, s => Assert.True(s.Closed));
            section = SlackListenerStatusFile.ReadSection(directory);
            Assert.False(section["running"]!.GetValue<bool>());
            Assert.Equal(0, section["connections_open"]!.GetValue<int>());
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public void The_diagnostics_block_carries_the_counters_and_no_identifiers()
    {
        var now = new DateTime(2026, 11, 6, 14, 30, 0, DateTimeKind.Utc);
        var status = new SlackListenerStatus(() => now);
        status.Started(2);
        status.IdsLearned();
        status.ConnectionOpened();
        status.ConnectionOpened();
        status.Received();
        status.Received();
        status.Acked();
        status.Acked();
        status.Dropped("bot");
        status.Dropped("bot");
        status.Dropped("team");
        status.Dispatched();
        status.QueueDropped();
        status.QueueDepthSource = () => 7;
        now = now.AddMinutes(5);
        status.ConnectionClosed("warning");
        status.ConnectFailed("HTTP 503");

        var snapshot = status.Read();
        var json = SlackListenerStatus.ToJson(snapshot);
        Assert.Equal(1, json["connections_open"]!.GetValue<int>());
        Assert.Equal(2, json["connections_wanted"]!.GetValue<int>());
        Assert.Equal("warning", json["last_disconnect_reason"]!.GetValue<string>());
        Assert.Equal("2026-11-06T14:35:00Z", json["last_disconnect_at"]!.GetValue<string>());
        Assert.Equal(3, json["dropped_by_filter"]!.GetValue<long>());
        Assert.Equal(2, json["dropped_by_reason"]!["bot"]!.GetValue<long>());
        Assert.Equal(1, json["dropped_by_reason"]!["team"]!.GetValue<long>());
        Assert.Equal(7, json["queue_depth"]!.GetValue<int>());
        Assert.Equal(1, json["queue_dropped"]!.GetValue<long>());
        Assert.Equal("2026-11-06T14:30:00Z", json["last_event_at"]!.GetValue<string>());
        Assert.Equal("2026-11-06T14:30:00Z", json["last_hello_at"]!.GetValue<string>());
        Assert.Equal("HTTP 503", json["last_error"]!.GetValue<string>());
        Assert.Equal(2, json["received"]!.GetValue<long>());
        Assert.Equal(2, json["acked"]!.GetValue<long>());
        Assert.Equal(1, json["dispatched"]!.GetValue<long>());
        Assert.True(json["ids_learned"]!.GetValue<bool>());

        /* No key names a channel, a user, a team or a token: the block is counts, reasons and times. */
        var keys = json.Select(p => p.Key).ToList();
        Assert.DoesNotContain(keys, k => k.Contains("channel", StringComparison.OrdinalIgnoreCase) || k.Contains("token", StringComparison.OrdinalIgnoreCase) || k.Contains("user", StringComparison.OrdinalIgnoreCase));
        var line = SlackListenerStatus.Describe(snapshot);
        Assert.Contains("connections 1/2", line, StringComparison.Ordinal);
        Assert.Contains("dropped by filter 3", line, StringComparison.Ordinal);
        Assert.Contains("queue depth 7", line, StringComparison.Ordinal);
        Assert.Contains("last disconnect warning at 2026-11-06T14:35:00Z", line, StringComparison.Ordinal);
    }

    [Fact]
    public void The_status_file_round_trips_and_an_absent_file_reads_as_absent()
    {
        var directory = Path.Combine(Path.GetTempPath(), "darling-slack-status-" + Guid.NewGuid().ToString("N"));
        try
        {
            Assert.Equal("absent", SlackListenerStatusFile.ReadSection(directory)["status"]!.GetValue<string>());
            var status = new SlackListenerStatus(() => new DateTime(2026, 11, 6, 15, 0, 0, DateTimeKind.Utc));
            status.Started(2);
            status.ConnectionOpened();
            status.Dropped("channel");
            Assert.Null(SlackListenerStatusFile.Write(directory, status.Read(), new DateTime(2026, 11, 6, 15, 1, 0, DateTimeKind.Utc)));
            var section = SlackListenerStatusFile.ReadSection(directory);
            Assert.Equal("ok", section["status"]!.GetValue<string>());
            Assert.Equal(1, section["connections_open"]!.GetValue<int>());
            Assert.Equal(1, section["dropped_by_reason"]!["channel"]!.GetValue<long>());
            Assert.Equal("2026-11-06T15:01:00Z", section["written_at"]!.GetValue<string>());
            Assert.Equal("slack_listener", SlackListenerStatusFile.SectionName);
            Assert.False(File.Exists(SlackListenerStatusFile.PathUnder(directory) + ".tmp"));

            File.WriteAllText(SlackListenerStatusFile.PathUnder(directory), "not json");
            Assert.Equal("error", SlackListenerStatusFile.ReadSection(directory)["status"]!.GetValue<string>());
            SlackListenerStatusFile.Delete(directory);
            Assert.Equal("absent", SlackListenerStatusFile.ReadSection(directory)["status"]!.GetValue<string>());
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }
}

public sealed class SlackSocketModeWebApiTests
{
    [Fact]
    public async Task Auth_test_carries_the_bot_token_and_learns_the_ids()
    {
        using var api = new FakeSlackApi((200, """{"ok":true,"team_id":"T0TEAM","user_id":"U0BOT","bot_id":"B0BOT","team":"example"}"""));
        var transport = new SlackSocketModeWebApi(api.ApiBase);
        var auth = await transport.AuthTestAsync("bot-token-a", CancellationToken.None);
        Assert.True(auth.Ok);
        Assert.Equal("T0TEAM", auth.TeamId);
        Assert.Equal("U0BOT", auth.UserId);
        Assert.Equal("B0BOT", auth.BotId);
        var request = Assert.Single(api.Requests);
        Assert.Equal("/api/auth.test", request.Path);
        Assert.Equal("Bearer bot-token-a", request.Authorization);
    }

    [Fact]
    public async Task Connections_open_carries_the_app_token_and_returns_the_socket_url()
    {
        using var api = new FakeSlackApi((200, """{"ok":true,"url":"wss://wss-primary.example/link/?ticket=abc&app_id=A0APP"}"""));
        var transport = new SlackSocketModeWebApi(api.ApiBase);
        var open = await transport.OpenConnectionAsync("app-token-a", CancellationToken.None);
        Assert.True(open.Ok);
        Assert.Equal("wss", open.Url!.Scheme);
        Assert.Equal("wss-primary.example", open.Url.Host);
        var request = Assert.Single(api.Requests);
        Assert.Equal("/api/apps.connections.open", request.Path);
        Assert.Equal("Bearer app-token-a", request.Authorization);
    }

    [Fact]
    public async Task Errors_name_the_cause_and_never_the_token()
    {
        using var api = new FakeSlackApi(
            (200, """{"ok":false,"error":"invalid_auth for app-token-a"}"""),
            (200, """{"ok":false,"error":"not_authed bot-token-a"}"""),
            (503, "busy"),
            (200, "not json"),
            (200, """{"ok":true}"""));
        var transport = new SlackSocketModeWebApi(api.ApiBase);
        var open = await transport.OpenConnectionAsync("app-token-a", CancellationToken.None);
        Assert.False(open.Ok);
        Assert.Equal("invalid_auth for " + SlackBotClient.RedactedToken, open.Error);
        var auth = await transport.AuthTestAsync("bot-token-a", CancellationToken.None);
        Assert.False(auth.Ok);
        Assert.Equal("not_authed " + SlackBotClient.RedactedToken, auth.Error);
        Assert.Equal("HTTP 503", (await transport.OpenConnectionAsync("app-token-a", CancellationToken.None)).Error);
        Assert.Equal("HTTP 200 with a body that is not JSON", (await transport.AuthTestAsync("bot-token-a", CancellationToken.None)).Error);
        Assert.Equal("ok without a WebSocket url", (await transport.OpenConnectionAsync("app-token-a", CancellationToken.None)).Error);
        Assert.Equal("no app token", (await transport.OpenConnectionAsync("  ", CancellationToken.None)).Error);
        Assert.Equal("no bot token", (await transport.AuthTestAsync("", CancellationToken.None)).Error);
        Assert.DoesNotContain("token-a", transport.ToString(), StringComparison.Ordinal);
    }
}
