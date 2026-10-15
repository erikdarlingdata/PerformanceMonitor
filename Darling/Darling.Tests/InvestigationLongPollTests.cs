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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The investigation long poll (#5822) end to end in process: the dispatcher opens or joins a request in an in-memory store,
/// fires the signal on an open, and <c>wait_for_investigation_requests</c> (through
/// <see cref="DarlingMcpInvestigationTools.WaitAsync"/>, the tool's body) wakes, reads the store past its cursor and answers.
/// The store's own catch-up SQL is <c>InvestigationToolsLiveTests</c>.
/// </summary>
public sealed class InvestigationLongPollTests
{
    private const int BlockingRoute = 5;
    private const int CpuRoute = 6;

    /// <summary>An in-memory request store: the dispatcher's open-or-join seam and the tool's catch-up read over one list.</summary>
    private sealed class FakeStore
    {
        private readonly object _gate = new();
        private readonly List<InvestigationOpenedRow> _rows = [];

        public DateTime Clock { get; set; } = new(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc);

        public InvestigationOpenOutcome Next { get; set; } = InvestigationOpenOutcome.Opened;

        public Task<InvestigationOpenResult> OpenOrJoinAsync(InvestigationAlert alert, CancellationToken cancellationToken)
        {
            if (Next != InvestigationOpenOutcome.Opened)
            {
                return Task.FromResult(new InvestigationOpenResult(Next, Next == InvestigationOpenOutcome.Joined ? Guid.NewGuid() : null));
            }

            Clock = Clock.AddSeconds(1);
            var row = new InvestigationOpenedRow(Guid.NewGuid(), alert.RouteId, alert.ServerName, alert.Family, Clock);
            Add(row);
            return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, row.RequestId));
        }

        public void Add(InvestigationOpenedRow row)
        {
            lock (_gate)
            {
                _rows.Add(row);
            }
        }

        public Task<IReadOnlyList<InvestigationOpenedRow>> ReadAsync(
            DateTime sinceUtc, IReadOnlyCollection<int>? routeIds, int limit, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                IReadOnlyList<InvestigationOpenedRow> rows = _rows
                    .Where(r => r.OpenedAtUtc > sinceUtc && (routeIds is null || routeIds.Contains(r.RouteId)))
                    .OrderBy(r => r.OpenedAtUtc).ThenBy(r => r.RequestId)
                    .Take(limit)
                    .ToList();
                return Task.FromResult(rows);
            }
        }
    }

    private sealed class Rig
    {
        public FakeStore Store { get; } = new();

        public InvestigationOpenSignal Signal { get; } = new();

        public InvestigationDispatcher Dispatcher { get; }

        public Rig()
        {
            var config = new DarlingConfig
            {
                NotificationRoutes =
                [
                    AgentRoute(BlockingRoute, "Deadlocks Detected"),
                    AgentRoute(CpuRoute, "High CPU"),
                ],
            };
            Dispatcher = new InvestigationDispatcher(Store.OpenOrJoinAsync, new DarlingAlertSettings(config), NullLogger.Instance, openSignal: Signal);
        }

        public Task<string> WaitAsync(int[]? routes, string? after, int timeoutSeconds, CancellationToken cancellationToken = default) =>
            DarlingMcpInvestigationTools.WaitAsync(Store.ReadAsync, Signal, () => Store.Clock, routes, after, timeoutSeconds,
                cancellationToken);

        public Task<InvestigationOpenOutcome?> AlertAsync(string metric) =>
            Dispatcher.DispatchAsync(new AlertHistoryRecord("42", "srv", metric, "3", "1", 3, 1,
                AlertDelivery.RoutedToDigest(), false, null, null), null);

        /// <summary>A cursor at "now", from a call that does not wait.</summary>
        public async Task<string> CursorAsync(int[]? routes = null) =>
            (string)JsonNode.Parse(await WaitAsync(routes, null, 0))!["cursor"]!;

        /// <summary>Waits until <paramref name="count"/> calls are registered, so an open fired next is one they wait on.</summary>
        public async Task WaitersAsync(int count)
        {
            var until = DateTime.UtcNow.AddSeconds(10);
            while (Signal.WaiterCount < count && DateTime.UtcNow < until)
            {
                await Task.Delay(10, TestContext.Current.CancellationToken);
            }

            Assert.Equal(count, Signal.WaiterCount);
        }
    }

    private static NotificationRoute AgentRoute(int routeId, string match) =>
        new(routeId, match, "", "", "", "", "", Enabled: true)
        {
            AgentLevel = NotificationRoute.AgentLevelInvestigate, AgentMaxPerHour = 50, AgentMaxPerDay = 500,
        };

    private static JsonObject Answer(string json) => JsonNode.Parse(json)!.AsObject();

    private static JsonArray Requests(string json) => Answer(json)["requests"]!.AsArray();

    [Fact]
    public async Task AWait_WakesWhenARequestOpensOnItsRoute_AndAnswersOnlyTheIdServerFamilyRouteAndTime()
    {
        var rig = new Rig();
        var cursor = await rig.CursorAsync();

        var wait = rig.WaitAsync([BlockingRoute], cursor, 20, TestContext.Current.CancellationToken);
        await rig.WaitersAsync(1);
        Assert.Equal(InvestigationOpenOutcome.Opened, await rig.AlertAsync("Deadlocks Detected"));

        var answer = Answer(await wait.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal("ok", (string)answer["status"]!);
        var row = Assert.Single(answer["requests"]!.AsArray())!.AsObject();
        Assert.Equal(
            new[] { "family", "opened_at", "request_id", "route_id", "server_name" },
            row.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(BlockingRoute, (int)row["route_id"]!);
        Assert.Equal("srv", (string)row["server_name"]!);
        Assert.Equal("blocking", (string)row["family"]!);
        Assert.Equal(0, rig.Signal.WaiterCount);
    }

    [Fact]
    public async Task AWait_DoesNotWake_ForAnotherRoute_NorForAJoin_AndTimesOutEmptyWithItsCursorUnchanged()
    {
        var rig = new Rig();
        var cursor = await rig.CursorAsync([BlockingRoute]);

        var wait = rig.WaitAsync([BlockingRoute], cursor, 2, TestContext.Current.CancellationToken);
        await rig.WaitersAsync(1);
        Assert.Equal(InvestigationOpenOutcome.Opened, await rig.AlertAsync("High CPU"));
        rig.Store.Next = InvestigationOpenOutcome.Joined;
        Assert.Equal(InvestigationOpenOutcome.Joined, await rig.AlertAsync("Deadlocks Detected"));

        var answer = Answer(await wait.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal("empty", (string)answer["status"]!);
        Assert.Empty(answer["requests"]!.AsArray());
        Assert.Equal(cursor, (string)answer["cursor"]!);
        Assert.Equal(0, rig.Signal.WaiterCount);
    }

    [Fact]
    public async Task ARequestOpenedBetweenTwoCalls_IsAnsweredByTheNextCallsCursor_Once()
    {
        var rig = new Rig();
        var cursor = await rig.CursorAsync();

        Assert.Equal(InvestigationOpenOutcome.Opened, await rig.AlertAsync("Deadlocks Detected"));
        Assert.Equal(0, rig.Signal.WaiterCount);

        var first = await rig.WaitAsync(null, cursor, 0);
        Assert.Equal("ok", (string)Answer(first)["status"]!);
        Assert.Single(Requests(first));

        var second = await rig.WaitAsync(null, (string)Answer(first)["cursor"]!, 0);
        Assert.Equal("empty", (string)Answer(second)["status"]!);
    }

    [Fact]
    public async Task ARequestThatCommitsAfterANewerOneWasAnswered_IsStillAnswered_AndNeitherIsAnsweredTwice()
    {
        var rig = new Rig();
        var cursor = await rig.CursorAsync();

        Assert.Equal(InvestigationOpenOutcome.Opened, await rig.AlertAsync("Deadlocks Detected"));
        var first = await rig.WaitAsync(null, cursor, 0);
        var newer = Assert.Single(Requests(first))!;

        /* A request whose opened_at was taken before the answered one's, committing only now (it waited on a lock). */
        var late = new InvestigationOpenedRow(Guid.NewGuid(), CpuRoute, "srv2", "resources", rig.Store.Clock.AddMilliseconds(-500));
        rig.Store.Add(late);

        var second = await rig.WaitAsync(null, (string)Answer(first)["cursor"]!, 0);
        var row = Assert.Single(Requests(second))!;
        Assert.Equal(late.RequestId, Guid.Parse((string)row["request_id"]!));
        Assert.NotEqual((string)newer["request_id"]!, (string)row["request_id"]!);

        var third = await rig.WaitAsync(null, (string)Answer(second)["cursor"]!, 0);
        Assert.Equal("empty", (string)Answer(third)["status"]!);
    }

    [Fact]
    public async Task WithoutACursor_ARequestAlreadyOpen_IsNotAnswered()
    {
        var rig = new Rig();
        Assert.Equal(InvestigationOpenOutcome.Opened, await rig.AlertAsync("Deadlocks Detected"));

        var answer = await rig.WaitAsync(null, null, 0);
        Assert.Equal("empty", (string)Answer(answer)["status"]!);
    }

    [Fact]
    public async Task TwoWaitersOnOneRoute_BothWake_OnOneOpen()
    {
        var rig = new Rig();
        var cursor = await rig.CursorAsync();

        var a = rig.WaitAsync([BlockingRoute], cursor, 20, TestContext.Current.CancellationToken);
        var b = rig.WaitAsync(null, cursor, 20, TestContext.Current.CancellationToken);
        await rig.WaitersAsync(2);
        Assert.Equal(InvestigationOpenOutcome.Opened, await rig.AlertAsync("Deadlocks Detected"));

        var answers = await Task.WhenAll(a, b).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.All(answers, json => Assert.Single(Requests(json)));
        Assert.Equal((string)Requests(answers[0])[0]!["request_id"]!, (string)Requests(answers[1])[0]!["request_id"]!);
        Assert.Equal(0, rig.Signal.WaiterCount);
    }

    [Fact]
    public async Task ACancelledWait_Throws_AndLeavesNoWaiter()
    {
        var rig = new Rig();
        var cursor = await rig.CursorAsync();
        using var cts = new CancellationTokenSource();

        var wait = rig.WaitAsync([BlockingRoute], cursor, 20, cts.Token);
        await rig.WaitersAsync(1);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(0, rig.Signal.WaiterCount);
    }

    [Theory]
    [InlineData(-1, null, "timeout_seconds")]
    [InlineData(26, null, "timeout_seconds")]
    [InlineData(5, "not-a-cursor", "after")]
    [InlineData(5, "v1.123.nothex", "after")]
    [InlineData(5, "v1.0.", "after")]
    [InlineData(5, "v1.2999999999.", "after")]
    [InlineData(5, "v1.3000000000.", "after")]
    public async Task ABadTimeoutOrCursor_IsRefused_WithoutWaiting(int timeoutSeconds, string? after, string parameter)
    {
        // The last three parse as cursors but sit at or under the five-minute overlap (3,000,000,000 ticks), which every
        // read subtracts: before #5856 review round 1 they answered status = error from the subtraction, not this refusal.
        var rig = new Rig();
        var answer = Answer(await rig.WaitAsync(null, after, timeoutSeconds)
            .WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));

        Assert.Equal("invalid", (string?)answer["status"]);
        Assert.Equal(parameter, (string?)answer["hints"]?["parameter"]);
        Assert.Equal(0, rig.Signal.WaiterCount);
    }

    [Fact]
    public async Task ACursorJustPastTheOverlap_IsAccepted_AndAnswersFromItsWatermark()
    {
        var rig = new Rig();
        var answer = Answer(await rig.WaitAsync(null, "v1.3000000001.", 0, TestContext.Current.CancellationToken));

        Assert.Equal("empty", (string?)answer["status"]);
        Assert.StartsWith("v1.3000000001.", (string)answer["cursor"]!, StringComparison.Ordinal);
    }
}
