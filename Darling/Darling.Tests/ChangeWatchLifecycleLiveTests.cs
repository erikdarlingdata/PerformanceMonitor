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
SELECT $1, request_id, route_id, server_id, 'action_called', 'agent:tester', 'force the plan', '{""kind"":""plan_force"",""target"":{""id"":""10"",""database"":""Sales""}}'::jsonb
FROM config.investigation_requests", ct, forced.AddMinutes(-3));
            await SeedWaitsAsync(h.Connection, forced.AddMinutes(-60), 200, ct);
            await SeedWaitsAsync(h.Connection, forced, 2000, ct);
            var agentChange = new ChangeWatchChange("plan_force:snapshot:Sales:1:10:x", "plan_force", forced, "plan 10 of query 1 in Sales: forced", "Sales", 1, 10);
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
                new ChangeWatchChange("plan_force:snapshot:Sales:2:20:x", "plan_force", person, "plan 20 of query 2 in Sales: forced", "Sales", 2, 20),
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

    /* A first watch whose request an agent works: its journal row says it forced Sales plan 10 at <agentForcedAt>. The request is then moved three hours back, because
       the route's hourly limit counts the requests of the last hour and the watches under test come later in the story. */
    private static async Task OpenARequestAndJournalAnAgentForceAsync(Harness h, DateTime agentForcedAt, CancellationToken ct)
    {
        var first = h.Now.AddMinutes(-300);
        await SeedWaitsAsync(h.Connection, first.AddMinutes(-60), 100, ct);
        await SeedWaitsAsync(h.Connection, first, 1000, ct);
        await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(first, "max degree of parallelism"), ct);
        await h.NewPass().EvaluateDueAsync(ct);
        Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
        await ExecAsync(h.Connection, @"
INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail)
SELECT $1, request_id, route_id, server_id, 'action_called', 'agent:tester', 'force the plan', '{""kind"":""plan_force"",""target"":{""id"":""10"",""database"":""Sales""}}'::jsonb
FROM config.investigation_requests", ct, agentForcedAt);
        await ExecAsync(h.Connection, "UPDATE config.investigation_requests SET opened_at = opened_at - interval '3 hours'", ct);
    }

    [Fact]
    public async Task APersonsPlanForce_WithinThirtyMinutesOfAnAgentsUnrelatedForce_StillOpensARequest()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var person = h.Now.AddMinutes(-70);
            /* The agent forced Sales plan 10 ten minutes before a person forced Orders plan 20: different database, different plan. */
            await OpenARequestAndJournalAnAgentForceAsync(h, person.AddMinutes(-10), ct);
            await SeedWaitsAsync(h.Connection, person.AddMinutes(-60), 300, ct);
            await SeedWaitsAsync(h.Connection, person, 8000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[]
            {
                new ChangeWatchChange("plan_force:snapshot:Orders:2:20:x", "plan_force", person, "plan 20 of query 2 in Orders: forced", "Orders", 2, 20),
            }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE result = 'request_opened' AND first_change_at > now() - interval '3 hours'", ct));
        });
    }

    /// <summary>#6550: a watch of automatic plan correction's forces and reverts is recorded and measured, and opens no request.</summary>
    [Fact]
    public async Task AWatchOfOnlyAutomaticPlanCorrection_OpensNoRequest_ButAWatchWithAManualForceDoes()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var at = h.Now.AddMinutes(-70);
            await SeedWaitsAsync(h.Connection, at.AddMinutes(-60), 300, ct);
            await SeedWaitsAsync(h.Connection, at, 8000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[]
            {
                new ChangeWatchChange("plan_force:snapshot:Sales:1:10:x", "plan_force", at,
                    "plan 10 of query 1 in Sales: forced" + ChangeWatchPlanForcing.Suffix("AUTO"), "Sales", 1, 10),
                new ChangeWatchChange("plan_force:unforced:Sales:1:10:y", "plan_force", at.AddMinutes(5),
                    "plan 10 of query 1 in Sales: unforced" + ChangeWatchPlanForcing.Suffix("AUTO"), "Sales", 1, 10),
            }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE result = 'moved_auto_plan_correction'", ct));

            /* One manual force in the window is somebody's change: the request opens as before. */
            var later = h.Now.AddMinutes(-65).AddHours(-3);
            await SeedWaitsAsync(h.Connection, later.AddMinutes(-60), 300, ct);
            await SeedWaitsAsync(h.Connection, later, 8000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[]
            {
                new ChangeWatchChange("plan_force:snapshot:Sales:2:20:x", "plan_force", later,
                    "plan 20 of query 2 in Sales: forced" + ChangeWatchPlanForcing.Suffix("AUTO"), "Sales", 2, 20),
                new ChangeWatchChange("plan_force:snapshot:Sales:3:30:x", "plan_force", later,
                    "plan 30 of query 3 in Sales: forced" + ChangeWatchPlanForcing.Suffix("MANUAL"), "Sales", 3, 30),
            }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE result = 'request_opened'", ct));
        });
    }

    [Fact]
    public async Task AtASixtyMinuteFrequency_AnAgentsForceFiftyMinutesBeforeTheCaptureThatSawIt_IsTheAgentsChange()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            await SetForcedPlansFrequencyAsync(h, 60, ct);
            var at = h.Now.AddMinutes(-70);
            /* The forced-plans collector runs hourly, so the capture that sees the agent's force is up to an hour after it: the journal row reaches back that far. */
            await OpenARequestAndJournalAnAgentForceAsync(h, at.AddMinutes(-50), ct);
            await SeedWaitsAsync(h.Connection, at.AddMinutes(-60), 300, ct);
            await SeedWaitsAsync(h.Connection, at, 8000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[]
            {
                new ChangeWatchChange("plan_force:snapshot:Sales:1:10:x", "plan_force", at, "plan 10 of query 1 in Sales: forced", "Sales", 1, 10),
            }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE result = 'moved_agent_change'", ct));
        });
    }

    [Fact]
    public async Task AWatchHoldingBothAnAgentsForceAndAPersonsForce_OpensARequest()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var at = h.Now.AddMinutes(-70);
            await OpenARequestAndJournalAnAgentForceAsync(h, at.AddMinutes(-3), ct);
            await SeedWaitsAsync(h.Connection, at.AddMinutes(-60), 300, ct);
            await SeedWaitsAsync(h.Connection, at, 8000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, new[]
            {
                new ChangeWatchChange("plan_force:snapshot:Sales:1:10:x", "plan_force", at, "plan 10 of query 1 in Sales: forced", "Sales", 1, 10),
                new ChangeWatchChange("plan_force:snapshot:Orders:2:20:x", "plan_force", at, "plan 20 of query 2 in Orders: forced", "Orders", 2, 20),
            }, ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
        });
    }

    [Fact]
    public async Task AnEvaluationThatAlwaysThrows_EndsFailedAfterFiveAttempts_AndIsPrunedLikeAClosedWatch()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);
            /* Every evaluation read fails, for good: the view it reads is gone. */
            await ExecAsync(h.Connection, "DROP VIEW v_wait_stats", ct);

            var clock = h.Now;
            var store = new ChangeWatchStore(h.Source, 30, () => clock);
            var dispatcher = new InvestigationDispatcher(new InvestigationRequestStore(h.Source, 30, () => clock), AgentRoute(), NullLogger.Instance);
            ChangeWatchPass Pass() => new(h.Source, store, dispatcher.Core, NullLogger.Instance, 30, () => clock);

            for (var attempt = 1; attempt <= 4; attempt++)
            {
                /* Each pass is past the stale claim of the one before. */
                clock = h.Now.AddMinutes(11 * attempt);
                await Pass().EvaluateDueAsync(ct);
                Assert.Equal("evaluating", await TextAsync(h.Connection, "SELECT state FROM config.change_watches", ct));
                Assert.Equal(attempt, await CountAsync(h.Connection, "SELECT attempts FROM config.change_watches", ct));
            }

            clock = h.Now.AddMinutes(55);
            await Pass().EvaluateDueAsync(ct);

            Assert.Equal("failed", await TextAsync(h.Connection, "SELECT state FROM config.change_watches", ct));
            Assert.Equal("failed", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
            Assert.Equal(5, await CountAsync(h.Connection, "SELECT attempts FROM config.change_watches", ct));
            Assert.Contains("v_wait_stats", await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct), StringComparison.Ordinal);

            /* It is not tried again. */
            clock = h.Now.AddMinutes(70);
            await Pass().EvaluateDueAsync(ct);
            Assert.Equal(5, await CountAsync(h.Connection, "SELECT attempts FROM config.change_watches", ct));

            /* A failed watch is kept for the closed retention and then pruned with its changes, by the same prune a close runs. */
            await ExecAsync(h.Connection, "UPDATE config.change_watches SET closed_at = $1", ct, h.Now.AddDays(-100));
            await store.CloseAsync(Guid.NewGuid(), ChangeWatchStore.ResultFlat, null, "{}", ct);
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
        });
    }

    [Fact]
    public async Task APlanForcedByTheLedgerBeforeTheDatabasesPreviousCapture_IsNotExplainedByThatLedgerAction_ButOneInTheGapIs()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var t1 = h.Now.AddMinutes(-60);
            var t2 = h.Now.AddMinutes(-35);
            /* Plans 10 and 11 are in the capture at t2 and not in the one at t1. The ledger forced 11 inside the gap (after t1, up to t2). It forced 10 at
               t2 - 28 minutes, which is BEFORE t1: that action belongs to the interval before the previous capture, not to this one. */
            await ForcedPlanCaptureAsync(h.Connection, t1, "Sales", ct);
            await ForcedPlanCaptureAsync(h.Connection, t2, "Sales", ct, (1, 10), (2, 11));
            await LedgerAsync(h.Connection, t2.AddMinutes(-28), "Sales", 1, 10, "force", ct);
            await LedgerAsync(h.Connection, t1.AddMinutes(5), "Sales", 2, 11, "force", ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, t1.AddMinutes(-1), h.Now, ct);

            var snapshot = Assert.Single(found, c => c.Key.StartsWith("plan_force:snapshot:", StringComparison.Ordinal));
            Assert.Equal(10, snapshot.PlanId);
        });
    }

    /* #6234: the forced-plans collector's frequency is per server and settable, so the longest gap between two captures that is still diffed follows it
       (the larger of 30 minutes and twice the frequency plus 5 minutes), not a fixed 30 minutes. */
    private static Task SetForcedPlansFrequencyAsync(Harness h, int minutes, CancellationToken ct) =>
        ExecAsync(h.Connection, "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES ($1, 'query_store_forced_plans', $2)", ct, ServerId, minutes);

    [Fact]
    public async Task AtASixtyMinuteFrequency_APlanForcedBetweenTwoCapturesAnHourApart_IsDetected()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            await SetForcedPlansFrequencyAsync(h, 60, ct);
            var first = h.Now.AddMinutes(-130);
            var second = h.Now.AddMinutes(-70);
            await ForcedPlanCaptureAsync(h.Connection, first, "Sales", ct, (1, 10));
            await ForcedPlanCaptureAsync(h.Connection, second, "Sales", ct, (1, 10), (2, 20));

            Assert.Equal(60, await h.NewPass().ForcedPlansFrequencyMinutesAsync(ServerId, ct));
            var found = await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct);

            var change = Assert.Single(found, c => c.Kind == "plan_force");
            Assert.Equal(20, change.PlanId);
            Assert.Equal("plan_force:snapshot:Sales:2:20:" + Stamp(second), change.Key);
        });
    }

    [Fact]
    public async Task AtTheDefaultFrequency_APlanForcedBetweenTwoCapturesTwentyFiveMinutesApart_IsStillDetected()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            /* No override row: the forced-plans collector runs at its 5-minute default, where twice the frequency plus 5 minutes is 15. A capture delayed by load
               25 minutes after the last is still diffed, because the gap limit never drops below 30 minutes. */
            var first = h.Now.AddMinutes(-60);
            var second = h.Now.AddMinutes(-35);
            await ForcedPlanCaptureAsync(h.Connection, first, "Sales", ct, (1, 10));
            await ForcedPlanCaptureAsync(h.Connection, second, "Sales", ct, (1, 10), (2, 20));

            var found = await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct);

            var change = Assert.Single(found, c => c.Kind == "plan_force");
            Assert.Equal(20, change.PlanId);
            Assert.Equal(TimeSpan.FromMinutes(30), ChangeWatchPass.CaptureGapLimit(5));
        });
    }

    [Fact]
    public async Task AtAFifteenMinuteFrequency_ACaptureThreeHoursAfterThePreviousOne_IsStillANewBaseline()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            await SetForcedPlansFrequencyAsync(h, 15, ct);
            var first = h.Now.AddMinutes(-230);
            var second = h.Now.AddMinutes(-50);
            await ForcedPlanCaptureAsync(h.Connection, first, "Sales", ct, (1, 10));
            await ForcedPlanCaptureAsync(h.Connection, second, "Sales", ct, (1, 10), (2, 20));

            var found = await h.NewPass().ReadChangesAsync(ServerId, first.AddMinutes(-1), h.Now, ct);

            Assert.DoesNotContain(found, c => c.Kind == "plan_force");
        });
    }

    [Fact]
    public async Task ADatabaseCapturedAgainAfterALongGap_HasThatCaptureAsANewBaseline_AndTheNextCaptureIsDiffedAgainstIt()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var before = h.Now.AddMinutes(-150);
            var back = h.Now.AddMinutes(-40);
            var next = h.Now.AddMinutes(-35);
            /* Sales was not captured for nearly two hours (a failback, Query Store read-only). Plan 20 was forced somewhere in that gap: it cannot be dated, so it is
               no change at the capture that finds it. Plan 30 is forced between the return capture and the next. */
            await ForcedPlanCaptureAsync(h.Connection, before, "Sales", ct, (1, 10));
            await ForcedPlanCaptureAsync(h.Connection, back, "Sales", ct, (1, 10), (2, 20));
            await ForcedPlanCaptureAsync(h.Connection, next, "Sales", ct, (1, 10), (2, 20), (3, 30));

            var found = await h.NewPass().ReadChangesAsync(ServerId, before.AddMinutes(-1), h.Now, ct);

            var change = Assert.Single(found, c => c.Kind == "plan_force");
            Assert.Equal("plan_force:snapshot:Sales:3:30:" + Stamp(next), change.Key);
            Assert.Equal(30, change.PlanId);
        });
    }

    [Fact]
    public async Task PlanDetection_ReadsTheCapturesAfterTheMark_NotADayOfForcedPlanRowsPerPass()
    {
        await RunAsync(true, async (h, ct) =>
        {
            /* Four Query Store databases, a capture every five minutes for a day, 20 forced plans each: the marker row and the plans of every capture. */
            await ExecAsync(h.Connection, @"
INSERT INTO collect.query_store_forced_plans (collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id)
SELECT row_number() OVER (), c.t, $1, 'x', d.name, p.n, p.n * 10
FROM generate_series($2::timestamp - interval '24 hours', $2::timestamp, interval '5 minutes') AS c(t)
CROSS JOIN (VALUES ('db1'), ('db2'), ('db3'), ('db4')) AS d(name)
CROSS JOIN LATERAL (SELECT NULL::bigint AS n UNION ALL SELECT generate_series(1, 20)) AS p(n)", ct, ServerId, Naive(h.Now));
            /* One plan is forced in db1 at the newest capture. */
            await ForcedPlanAsync(h.Connection, h.Now, "db1", 999, 21, ct);
            await ExecAsync(h.Connection, "ANALYZE collect.query_store_forced_plans", ct);
            var total = await CountAsync(h.Connection, "SELECT count(*) FROM collect.query_store_forced_plans", ct);
            var mark = h.Now.AddMinutes(-30);

            var found = await h.NewPass().ReadChangesAsync(ServerId, mark, h.Now, ct);
            var change = Assert.Single(found, c => c.Kind == "plan_force");
            Assert.Equal(21, change.PlanId);
            Assert.Equal("db1", change.DatabaseName);

            /* Rows the statement takes out of the table (the rows its scans emit, over every loop), against the day the old read sorted. */
            await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + ChangeWatchPass.PlanSnapshotChangesSql, h.Connection);
            explain.Parameters.Add(new NpgsqlParameter { Value = ServerId });
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = Naive(mark) });
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = Naive(h.Now) });
            ChangeWatchPass.WithGapLimit(explain, ChangeWatchPass.CaptureGapLimit(5));
            var json = (string)(await explain.ExecuteScalarAsync(ct))!;
            using var document = System.Text.Json.JsonDocument.Parse(json);
            var emitted = RowsEmittedFromTable(document.RootElement[0].GetProperty("Plan"), "query_store_forced_plans");

            Assert.True(emitted < total / 4, $"the statement took {emitted} rows out of {total} forced-plan rows (a day); it must read the captures after the mark only: {json}");
        });
    }

    [Fact]
    public async Task PlanDetection_AfterADayOfMissedPasses_ReadsTheMarkersWithoutTheSkipScanFailure()
    {
        await RunAsync(true, async (h, ct) =>
        {
            /* #6234: the marker read as SELECT DISTINCT over idx_query_store_forced_plans_markers failed on TimescaleDB 2.30 with
               XX000 "ScanKey for skip qual not found" (Custom Scan (SkipScan) on the index, skip qual database_name > NULL), so no plan change was ever read.
               The shape the planner picks SkipScan for, and the one a production store was in: a mark almost a day old (change detection that had not
               completed for a day: the service stopped, or an earlier pass failed, and a failed pass does not move the mark) over a table where the markers are
               about half of one percent of the rows (two databases, a capture every ten minutes, 200 forced plans each). The older test above has a 30-minute window and
               one marker row in 21, so it never got that plan; and a scratch store has no TimescaleDB extension until it is enabled, the plain-PostgreSQL planner never
               has a SkipScan to pick, so the extension is enabled here the way the product enables it (the table stays the plain table V195 makes). */
            var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(h.Connection.ConnectionString, ct);
            Assert.True(timescaleEnabled, "TimescaleDB must be available on CI: the failure only exists under its planner");
            await ExecAsync(h.Connection, @"
INSERT INTO collect.query_store_forced_plans (collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id)
SELECT row_number() OVER (), c.t, $1, 'x', d.name, p.n, p.n * 10
FROM generate_series($2::timestamp - interval '24 hours', $2::timestamp, interval '10 minutes') AS c(t)
CROSS JOIN (VALUES ('db1'), ('db2')) AS d(name)
CROSS JOIN LATERAL (SELECT NULL::bigint AS n UNION ALL SELECT generate_series(1, 200)) AS p(n)", ct, ServerId, Naive(h.Now));
            /* One plan is forced in db1 at the newest capture. */
            await ForcedPlanAsync(h.Connection, h.Now, "db1", 999, 201, ct);
            await ExecAsync(h.Connection, "ANALYZE collect.query_store_forced_plans", ct);

            var found = await h.NewPass().ReadChangesAsync(ServerId, h.Now.AddHours(-23.5), h.Now, ct);

            var change = Assert.Single(found, c => c.Kind == "plan_force");
            Assert.Equal(201, change.PlanId);
            Assert.Equal("db1", change.DatabaseName);
        });
    }

    private static double RowsEmittedFromTable(System.Text.Json.JsonElement node, string table)
    {
        var rows = 0d;
        if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == table)
        {
            rows += node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble();
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                rows += RowsEmittedFromTable(child, table);
            }
        }

        return rows;
    }
}
