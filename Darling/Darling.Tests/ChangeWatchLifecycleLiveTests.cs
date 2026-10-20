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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.ChangeWatch;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.ChangeWatchDetectionLiveTests;
using static Darling.Tests.ChangeWatchLiveTests;

namespace Darling.Tests;

/// <summary>
/// #6234, third round: the watch's lifecycle (one claim, one request, a window that ends is evaluated and never extended, a span cap, an agent's own
/// change opens no further request) and the detection reads that decide what a watch sees (trace-flag empty runs, a database missing from the baseline capture,
/// a plan unforced and re-forced). Each fact runs on its own scratch database.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync (ScratchPostgres), so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class ChangeWatchLifecycleLiveTests
{
    private sealed class ListLogger : ILogger
    {
        public List<string> Messages { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Add(logLevel + ": " + formatter(state, exception));
    }

    private static DarlingAlertSettings AgentRoute()
    {
        var config = new DarlingConfig
        {
            NotificationRoutes = new[]
            {
                new NotificationRoute(1, InvestigationTriggerKinds.ChangeWatchMetric, "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelInvestigate },
            },
        };
        return new DarlingAlertSettings(config);
    }

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, c);
        foreach (var value in values)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task TwoPassesOverOneDueWatch_OpenOneRequest()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);

            /* Two service instances over one store read the same due watch at the same time. */
            await Task.WhenAll(h.NewPass().EvaluateDueAsync(ct), h.NewPass().EvaluateDueAsync(ct), h.NewPass().EvaluateDueAsync(ct));

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task AWatchWhoseClaimerDiedAfterTheRequestOpened_IsClosedOnTheRequest_NotGivenASecond()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);
            await h.NewPass().EvaluateDueAsync(ct);
            var requestId = await TextAsync(h.Connection, "SELECT request_id::text FROM config.change_watches", ct);
            Assert.NotNull(requestId);

            /* The claimer opened the request and died before it closed the watch: the watch is claimed, with a claim older than the stale limit. */
            await ExecAsync(h.Connection, "UPDATE config.change_watches SET state = 'evaluating', claimed_at = $1, closed_at = NULL, result = NULL, request_id = NULL", ct,
                h.Now.AddMinutes(-30));
            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            Assert.Equal("closed", await TextAsync(h.Connection, "SELECT state FROM config.change_watches", ct));
            Assert.Equal(requestId, await TextAsync(h.Connection, "SELECT request_id::text FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task AFailedRequestOpen_LeavesTheWatchToBeTriedAgain_AndTheRetryOpensExactlyOne()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            var clock = h.Now;
            var store = new ChangeWatchStore(h.Source, 30, () => clock);
            await store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);

            var requests = new InvestigationRequestStore(h.Source, 30, () => h.Now);
            var failing = true;
            var dispatcher = new InvestigationDispatcher(
                (alert, token) => failing ? throw new InvalidOperationException("the store is unavailable") : requests.OpenOrJoinAsync(alert, token),
                AgentRoute(), NullLogger.Instance);
            ChangeWatchPass Pass() => new(h.Source, store, dispatcher.Core, NullLogger.Instance, 30, () => clock);

            await Pass().EvaluateDueAsync(ct);

            /* A failure is not "no route": the watch is not closed as moved_no_request. */
            Assert.NotEqual("closed", await TextAsync(h.Connection, "SELECT state FROM config.change_watches", ct));
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));

            failing = false;
            clock = h.Now.AddMinutes(20);
            await Pass().EvaluateDueAsync(ct);
            await Pass().EvaluateDueAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task AChangeAfterAWatchWindowEnded_IsNotAddedToIt_AndRunsAsAWatchOfItsOwn()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var old = h.Now.AddHours(-3);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(old, "max server memory"), ct);

            /* The service was down for hours: the old watch ended, and a change 30 minutes ago is found at the pass that wakes up. */
            var recent = h.Now.AddMinutes(-30);
            var result = await h.Store.RecordAsync(ServerId, ServerName, await OneChange(recent, "cost threshold"), ct);

            Assert.Null(result.WatchId);
            Assert.True(result.Deferred);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(old.AddMinutes(60), DateTime.SpecifyKind(
                (DateTime)(await ScalarAsync(h.Connection, "SELECT window_ends_at FROM config.change_watches", ct))!, DateTimeKind.Utc));

            /* A pass evaluates the old watch first, so the new change then opens its own. */
            var clock = h.Now;
            await ServerConfigAsync(h.Connection, recent.AddMinutes(-20), "cost threshold for parallelism", 5, ct);
            await ServerConfigAsync(h.Connection, recent, "cost threshold for parallelism", 50, ct);
            await h.Store.SetMarksAsync(ServerId, ChangeWatchStore.MarkSources.ToDictionary(s => s, _ => recent.AddMinutes(-10)), ct);
            await h.NewPassAt(() => clock).RunOnceAsync(ct);

            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE state = 'closed'", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE state = 'open'", ct));
        });
    }

    [Fact]
    public async Task AWatchIsNotExtendedPastTheSpanCap_AndALaterChangeWaitsForANewWatch()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-200);
            /* Each change falls inside the window the one before left: 150, 100 and 50 minutes ago keep one watch open. */
            foreach (var minutes in new[] { 200, 150, 100, 50 })
            {
                await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(h.Now.AddMinutes(-minutes), "change " + minutes), ct);
            }

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.Equal(first.AddMinutes(180), DateTime.SpecifyKind(
                (DateTime)(await ScalarAsync(h.Connection, "SELECT window_ends_at FROM config.change_watches", ct))!, DateTimeKind.Utc));

            /* A change 10 minutes ago is after the capped window: it is not added, and nothing stretches the watch. */
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(h.Now.AddMinutes(-10), "change 10"), ct);
            Assert.Equal(4, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(first.AddMinutes(180), DateTime.SpecifyKind(
                (DateTime)(await ScalarAsync(h.Connection, "SELECT window_ends_at FROM config.change_watches", ct))!, DateTimeKind.Utc));
        });
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, c);
        return await command.ExecuteScalarAsync(ct);
    }

    [Fact]
    public async Task AnAgentsPlanChangeUnderAChangeWatchRequest_IsWatched_AndOpensNoFurtherRequest()
    {
        await RunAsync(true, async (h, ct) =>
        {
            /* First watch: a person's change, a regression, one request: the agent's work item. */
            var first = h.Now.AddMinutes(-300);
            await SeedWaitsAsync(h.Connection, first.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, first, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(first, "max degree of parallelism"), ct);
            await h.NewPass().EvaluateDueAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));

            /* The agent handling that request forces a plan; the next forced-plan capture sees it, and the waits move again. */
            var forced = h.Now.AddMinutes(-150);
            await ExecAsync(h.Connection, @"
INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail)
SELECT $1, request_id, route_id, server_id, 'action_called', 'agent:tester', 'force the plan', '{""kind"":""plan_force""}'::jsonb
FROM config.investigation_requests", ct, forced.AddMinutes(-3));
            await SeedWaitsAsync(h.Connection, forced.AddMinutes(-60), 200, ct);
            await SeedWaitsAsync(h.Connection, forced, 2000, ct);
            var agentChange = new ChangeWatchChange("plan_force:snapshot:Sales:1:10:x", "plan_force", forced, "plan 10 of query 1 in Sales: forced", "Sales", 1);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[] { agentChange }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            /* It is recorded and measured, and it opened nothing. */
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE result = 'moved_agent_change' AND result_json IS NOT NULL", ct));

            /* The same plan change with no agent action behind it is a person's: it opens a request. */
            /* The route's hourly limit counts the requests of the last hour: the two are a few hours apart in the story. */
            await ExecAsync(h.Connection, "UPDATE config.investigation_requests SET opened_at = opened_at - interval '3 hours'", ct);
            var person = h.Now.AddMinutes(-70);
            await SeedWaitsAsync(h.Connection, person.AddMinutes(-60), 300, ct);
            await SeedWaitsAsync(h.Connection, person, 8000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[]
            {
                new ChangeWatchChange("plan_force:snapshot:Sales:2:20:x", "plan_force", person, "plan 20 of query 2 in Sales: forced", "Sales", 2),
            }, ct);
            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
        });
    }

    [Fact]
    public async Task ATraceFlagTurnedOffAndBackOn_BetweenTheBaselineAndTheMark_IsSeenWhenItReturns()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var mark = h.Now.AddMinutes(-30);
            /* t0 has flag 4199, t1 is an empty run (turned off, seen by an earlier pass), t2 has it again, after the mark. */
            await TraceFlagAsync(h.Connection, mark.AddMinutes(-20), 4199, ct);
            await CollectorRunAsync(h.Connection, mark.AddMinutes(-10), "trace_flags", "SUCCESS", 0, ct);
            await TraceFlagAsync(h.Connection, mark.AddMinutes(10), 4199, ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, mark, h.Now, ct);

            var change = Assert.Single(found, c => c.Kind == "trace_flag");
            Assert.Equal("trace_flag:4199:" + Stamp(mark.AddMinutes(10)), change.Key);
            Assert.Contains("enabled", change.Description, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public async Task ATraceFlagThatWasAlreadyOff_IsNotReportedOffAgain_WhenAnotherFlagAppears()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var mark = h.Now.AddMinutes(-30);
            await TraceFlagAsync(h.Connection, mark.AddMinutes(-20), 4199, ct);
            await CollectorRunAsync(h.Connection, mark.AddMinutes(-10), "trace_flags", "SUCCESS", 0, ct);
            await TraceFlagAsync(h.Connection, mark.AddMinutes(10), 2371, ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, mark, h.Now, ct);

            /* Only the new flag: 4199 went off before the mark and is not a change at t2. */
            var change = Assert.Single(found, c => c.Kind == "trace_flag");
            Assert.Equal("trace_flag:2371:" + Stamp(mark.AddMinutes(10)), change.Key);
        });
    }

    [Fact]
    public async Task ADatabaseMissingFromTheBaselineCapture_StillShowsItsFirstChangeInTheWindow()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var mark = h.Now.AddMinutes(-30);
            /* Orders was offline when the capture at the mark connected: that capture holds Sales only. Its own previous capture is older. */
            await ScopedConfigAsync(h.Connection, mark.AddMinutes(-20), "Sales", "MAXDOP", "0", ct);
            await ScopedConfigAsync(h.Connection, mark.AddMinutes(-20), "Orders", "MAXDOP", "0", ct);
            await ScopedConfigAsync(h.Connection, mark, "Sales", "MAXDOP", "0", ct);
            await ScopedConfigAsync(h.Connection, mark.AddMinutes(10), "Sales", "MAXDOP", "0", ct);
            await ScopedConfigAsync(h.Connection, mark.AddMinutes(10), "Orders", "MAXDOP", "4", ct);
            await DatabaseConfigAsync(h.Connection, mark.AddMinutes(-20), "Sales", 150, "FULL", ct);
            await DatabaseConfigAsync(h.Connection, mark.AddMinutes(-20), "Orders", 150, "FULL", ct);
            await DatabaseConfigAsync(h.Connection, mark, "Sales", 150, "FULL", ct);
            await DatabaseConfigAsync(h.Connection, mark.AddMinutes(10), "Sales", 150, "FULL", ct);
            await DatabaseConfigAsync(h.Connection, mark.AddMinutes(10), "Orders", 160, "FULL", ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, mark, h.Now, ct);

            Assert.Contains("database_scoped_config:Orders:MAXDOP:" + Stamp(mark.AddMinutes(10)), Keys(found));
            Assert.Contains("database_config:Orders:compatibility_level:" + Stamp(mark.AddMinutes(10)), Keys(found));
            Assert.DoesNotContain(found, c => c.DatabaseName == "Sales");
        });
    }

    [Fact]
    public async Task APlanUnforcedAndForcedAgainWithinADay_IsBothChanges_AndAFirstCapturedDatabaseIsABaseline()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var t1 = h.Now.AddMinutes(-50);
            var t2 = h.Now.AddMinutes(-40);
            var t3 = h.Now.AddMinutes(-30);
            await ForcedPlanCaptureAsync(h.Connection, t1, "Sales", ct, (1, 10));
            await ForcedPlanCaptureAsync(h.Connection, t2, "Sales", ct);
            await ForcedPlanCaptureAsync(h.Connection, t3, "Sales", ct, (1, 10));
            /* Orders was not captured before t3 (Query Store was off); its forced plans at its first capture are the baseline. */
            await ForcedPlanCaptureAsync(h.Connection, t3, "Orders", ct, (2, 20), (3, 30));

            var found = await h.NewPass().ReadChangesAsync(ServerId, t1.AddMinutes(-1), h.Now, ct);

            Assert.Equal(
                new[] { "plan_force:snapshot:Sales:1:10:" + Stamp(t3), "plan_force:unforced:Sales:1:10:" + Stamp(t2) },
                Keys(found.Where(c => c.Kind == "plan_force")));
        });
    }

    [Fact]
    public async Task DetectionAfterAnOutageLongerThanTheLookback_LogsTheSpanItSkipped()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            await h.Store.SetMarksAsync(ServerId, ChangeWatchStore.MarkSources.ToDictionary(s => s, _ => h.Now.AddHours(-5)), ct);
            var logger = new ListLogger();
            var dispatcher = new InvestigationDispatcher(new InvestigationRequestStore(h.Source, 30, () => h.Now), AgentRoute(), NullLogger.Instance);
            var pass = new ChangeWatchPass(h.Source, h.Store, dispatcher.Core, logger, 30, () => h.Now);

            await pass.DetectAsync(ct);

            Assert.Contains(logger.Messages, m => m.StartsWith("Warning: ", StringComparison.Ordinal) && m.Contains("no pass from", StringComparison.Ordinal));
        });
    }
}
