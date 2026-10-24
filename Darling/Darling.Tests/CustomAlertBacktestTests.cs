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
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5911: the pure half of <c>backtest_custom_alert_rule</c>. The step, each cap (span, servers, queries, time), the
/// unavailable answer for a source with no samples, and the parity of its fire and clear points with the live
/// engine's on the same samples. The live half (the panel run on a real store, at the caps) is
/// <see cref="CustomAlertBacktestLiveTests"/>.
/// </summary>
public sealed class CustomAlertBacktestTests
{
    // Warning at >= 25, critical at >= 40; fires after 2 breaching samples in a row, clears after 3 clear ones.
    private const string DefinitionJson =
        "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
        "\"hysteresis\":{\"breachSamples\":2,\"clearSamples\":3}," +
        "\"scope\":{\"mode\":\"servers\",\"servers\":[\"alpha-01\"]}}";

    private static readonly DateTime End = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

    private static CustomAlertRuleDefinition Definition()
    {
        var (definition, error) = CustomAlertRuleDefinition.TryParse(DefinitionJson);
        Assert.True(definition is not null, error);
        return definition!;
    }

    private static CustomAlertBacktest.ServerReplay Replay(string name, int id, TimeSpan span, TimeSpan step) =>
        new(Definition(), new CustomAlertBacktest.Target(id, name, name), step, CustomAlertBacktest.StepTimes(End, span, step));

    // ---- The step ---------------------------------------------------------------------------------------------

    [Fact]
    public void TheStep_IsSpanOver288_WhenTheCadenceIsFiner()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), CustomAlertBacktest.StepFor(TimeSpan.FromHours(24), TimeSpan.FromMinutes(1)));
        Assert.Equal(TimeSpan.FromMinutes(35), CustomAlertBacktest.StepFor(TimeSpan.FromDays(7), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void TheStep_IsTheCadence_WhenTheCadenceIsCoarser()
    {
        Assert.Equal(TimeSpan.FromMinutes(15), CustomAlertBacktest.StepFor(TimeSpan.FromHours(24), TimeSpan.FromMinutes(15)));
        Assert.Equal(TimeSpan.FromDays(1), CustomAlertBacktest.StepFor(TimeSpan.FromDays(7), TimeSpan.FromDays(1)));
    }

    [Fact]
    public void TheStepTimes_EndAtTheEnd_AndNeverPass288()
    {
        var times = CustomAlertBacktest.StepTimes(End, TimeSpan.FromHours(24), TimeSpan.FromMinutes(5));
        Assert.Equal(CustomAlertBacktest.StepsPerSpan, times.Count);
        Assert.Equal(End, times[^1]);
        Assert.Equal(End.AddHours(-24).AddMinutes(5), times[0]);

        var coarse = CustomAlertBacktest.StepTimes(End, TimeSpan.FromHours(24), TimeSpan.FromDays(1));
        Assert.Equal(End, Assert.Single(coarse));
    }

    [Fact]
    public void TheCadence_IsTheSchedulersResolution_OverrideThenFleetThenDefault()
    {
        var shipped = CustomAlertBacktest.CadenceFor("cpu_utilization_stats", 7, Array.Empty<ScheduleOverride>());
        Assert.True(shipped > TimeSpan.Zero);

        var fleet = new ScheduleOverride(null, "cpu_utilization", 10, null, null);
        Assert.Equal(TimeSpan.FromMinutes(10), CustomAlertBacktest.CadenceFor("cpu_utilization_stats", 7, new[] { fleet }));

        var own = new ScheduleOverride(7, "cpu_utilization", 20, null, null);
        Assert.Equal(TimeSpan.FromMinutes(20), CustomAlertBacktest.CadenceFor("cpu_utilization_stats", 7, new[] { fleet, own }));
        Assert.Equal(TimeSpan.FromMinutes(10), CustomAlertBacktest.CadenceFor("cpu_utilization_stats", 8, new[] { fleet, own }));

        Assert.Equal(CustomAlertBacktest.FallbackCadence, CustomAlertBacktest.CadenceFor("no_such_table", 7, Array.Empty<ScheduleOverride>()));
    }

    /// <summary>Two rows for one collector at one level (the unique indexes are on the raw name, the matchers compare
    /// case-insensitively, so a case variant is storable): the last one wins, as in the scheduler's one row picker.</summary>
    [Fact]
    public void TheCadence_PicksDuplicateRowsTheWayTheSchedulerDoes_TheLastOneWins()
    {
        var first = new ScheduleOverride(null, "cpu_utilization", 10, null, null);
        var last = new ScheduleOverride(null, "CPU_UTILIZATION", 30, null, null);
        Assert.Equal(TimeSpan.FromMinutes(30), CustomAlertBacktest.CadenceFor("cpu_utilization_stats", 7, new[] { first, last }));

        var ownFirst = new ScheduleOverride(7, "cpu_utilization", 15, null, null);
        var ownLast = new ScheduleOverride(7, "Cpu_Utilization", 45, null, null);
        Assert.Equal(TimeSpan.FromMinutes(45), CustomAlertBacktest.CadenceFor("cpu_utilization_stats", 7, new[] { ownFirst, first, ownLast, last }));
    }

    // ---- Parity with the live engine ---------------------------------------------------------------------------

    /// <summary>The same samples fed to the replay and, pass by pass, to the live engine's decision half
    /// (<see cref="CustomAlertEvaluator.ApplyValueAsync"/>, with a no-data pass skipped the way
    /// <c>EvaluateRuleForServerAsync</c> skips it) fire and clear at the same points.</summary>
    [Fact]
    public async Task TheReplay_FiresAndClears_WhereTheLiveEngineDoes_OnTheSameSamples()
    {
        double?[] values =
        [
            10, 30, null, 31, 45, 50, 10, null, 12, 30, 11, 12, 13, 26, 27, 28, 29, 5, 5, null, 5, 41, 42, 1, 1, 1,
        ];
        var times = Enumerable.Range(0, values.Length).Select(i => End.AddMinutes(i - values.Length + 1)).ToArray();

        var replay = new CustomAlertBacktest.ServerReplay(
            Definition(), new CustomAlertBacktest.Target(7, "alpha-01", "alpha-01"), TimeSpan.FromMinutes(1), times);
        for (var i = 0; i < values.Length; i++)
        {
            replay.Observe(times[i], values[i]);
        }

        await using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");
        var deliverer = new RecordingDeliverer();
        var evaluator = new CustomAlertEvaluator(
            new CustomAlertRuleStore(dataSource, StorageCommandDeadlines.McpReadSeconds), new CustomAlertStateStore(dataSource, StorageCommandDeadlines.McpReadSeconds), dataSource, deliverer,
            isAlertMuted: _ => false, alertsEnabled: static () => true, new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds),
            defaultIntervalSeconds: 0, cacheTtl: TimeSpan.Zero, new CapturingTestLogger());
        var row = new CustomAlertRule(5911, "backtest parity", DefinitionJson, null, true, 1, End, End, null);
        var state = CustomAlertRuleState.Fresh(1);
        var liveFires = new List<DateTime>();
        var liveClears = new List<DateTime>();
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] is not double v)
            {
                continue; // the live engine's no-data arm: the streak is frozen and the gate is not called
            }

            var wasFiring = state.Persistence.Firing;
            state = await evaluator.ApplyValueAsync(
                row, Definition(), 7, "alpha-01", times[i], times[i].AddSeconds(30), v, state, TestContext.Current.CancellationToken);
            if (!wasFiring && state.Persistence.Firing)
            {
                liveFires.Add(times[i]);
            }
            else if (wasFiring && !state.Persistence.Firing)
            {
                liveClears.Add(times[i]);
            }
        }

        Assert.NotEmpty(liveFires);
        Assert.NotEmpty(liveClears);
        Assert.Equal(liveFires, replay.Fires.Select(f => f.AtUtc));
        Assert.Equal(liveClears, replay.Clears.Select(c => c.AtUtc));
        Assert.Equal(state.Persistence.Firing, replay.FiringAtEnd);
        Assert.Equal(values.Count(v => v is null), replay.NoDataSteps);
        Assert.Equal(values.Count(v => v is not null), replay.SamplesEvaluated);
    }

    // ---- The caps ----------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheQueryCap_StopsTheReplay_AsPartial_WithStoppedAtAndWhatWasCovered()
    {
        var servers = new[]
        {
            Replay("alpha-01", 1, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
            Replay("beta-02", 2, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
        };

        var run = await CustomAlertBacktest.ReplayAsync(
            servers, (_, _, _) => Task.FromResult<double?>(30), maxQueries: 5, TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        Assert.Equal(CustomAlertBacktest.StopCap.Queries, run.StoppedBy);
        Assert.Equal(5, run.QueriesRun);
        Assert.Equal(3, servers[0].StepsRun); // round-robin: alpha 3, beta 2
        Assert.Equal(2, servers[1].StepsRun);
        Assert.Equal(servers[1].Steps[2], run.StoppedAtUtc);

        var answer = Parse(DarlingMcpCustomAlertTools.FormatBacktest(Definition(), null, null, TimeSpan.FromHours(1), End, run, Array.Empty<string>()));
        Assert.Equal("partial", (string?)answer["status"]);
        Assert.Equal("queries", (string?)answer["stopped_at"]!["cap"]);
        Assert.NotNull((string?)answer["stopped_at"]!["at_utc"]);
        Assert.Equal(5, (int)answer["queries_run"]!);
        var alpha = (JsonObject)answer["servers"]![0]!;
        Assert.Equal("partial", (string?)alpha["status"]);
        Assert.NotNull((string?)alpha["covered_to_utc"]);
    }

    [Fact]
    public async Task TheTimeCap_StopsTheReplay_AsPartial_EvenMidQuery()
    {
        var servers = new[] { Replay("alpha-01", 1, TimeSpan.FromHours(24), TimeSpan.FromMinutes(5)) };
        var calls = 0;

        var run = await CustomAlertBacktest.ReplayAsync(
            servers,
            async (_, _, ct) =>
            {
                if (++calls < 3)
                {
                    return 10;
                }

                await Task.Delay(Timeout.Infinite, ct); // a query the time cap must cancel
                return 10;
            },
            CustomAlertBacktest.MaxQueries,
            TimeSpan.FromMilliseconds(300),
            TestContext.Current.CancellationToken);

        Assert.Equal(CustomAlertBacktest.StopCap.Time, run.StoppedBy);
        Assert.Equal(2, run.QueriesRun); // the cancelled query is not counted as run
        Assert.Equal(servers[0].Steps[2], run.StoppedAtUtc);
        Assert.InRange(run.ElapsedMilliseconds, 250, 10_000);

        var answer = Parse(DarlingMcpCustomAlertTools.FormatBacktest(Definition(), null, null, TimeSpan.FromHours(24), End, run, Array.Empty<string>()));
        Assert.Equal("partial", (string?)answer["status"]);
        Assert.Equal("time", (string?)answer["stopped_at"]!["cap"]);
    }

    /// <summary>One read that throws something other than the time cap's cancellation (a statement_timeout, a 57014, a
    /// dropped connection) ends that server's replay only: the steps already replayed stay, the other server runs every
    /// step, and the answer is partial with the error's reason rather than the whole call's error envelope.</summary>
    [Fact]
    public async Task AReadThatThrowsOnTheNthStep_KeepsTheStepsAlreadyReplayed_AndAnswersThatServerPartial()
    {
        var servers = new[]
        {
            Replay("alpha-01", 1, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
            Replay("beta-02", 2, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
        };
        var alphaReads = 0;

        var run = await CustomAlertBacktest.ReplayAsync(
            servers,
            (t, _, _) =>
            {
                if (t.ServerId == 1 && ++alphaReads == 4)
                {
                    throw new InvalidOperationException("canceling statement due to statement timeout");
                }

                return Task.FromResult<double?>(30);
            },
            CustomAlertBacktest.MaxQueries, TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        Assert.Equal(3, servers[0].StepsRun); // the three steps before the failed one stay
        Assert.True(servers[0].Failed);
        Assert.Equal(servers[0].Steps[3], servers[0].FailedAtUtc);
        Assert.Equal(servers[1].Steps.Count, servers[1].StepsRun); // the other server carried on
        Assert.Equal(CustomAlertBacktest.StopCap.None, run.StoppedBy);

        var answer = Parse(DarlingMcpCustomAlertTools.FormatBacktest(Definition(), null, null, TimeSpan.FromHours(1), End, run, Array.Empty<string>()));
        Assert.Equal("partial", (string?)answer["status"]);
        Assert.Equal("error", (string?)answer["stopped_at"]!["cap"]);
        var alpha = (JsonObject)answer["servers"]![0]!;
        Assert.Equal("partial", (string?)alpha["status"]);
        Assert.Equal(3, (int)alpha["steps_run"]!);
        Assert.Contains("statement timeout", (string?)alpha["reason"], StringComparison.Ordinal);
        Assert.NotNull(alpha["fire_count"]); // what was replayed is still an answer
        Assert.Equal("ok", (string?)answer["servers"]![1]!["status"]);
    }

    [Fact]
    public async Task AReadThatThrowsOnTheFirstStep_AnswersThatServerUnavailable_WithTheErrorAsTheReason()
    {
        var servers = new[] { Replay("alpha-01", 1, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)) };
        var run = await CustomAlertBacktest.ReplayAsync(
            servers, (_, _, _) => throw new TimeoutException("connection dropped"), CustomAlertBacktest.MaxQueries, TimeSpan.FromSeconds(60),
            TestContext.Current.CancellationToken);

        Assert.Equal(1, run.QueriesRun); // the server stopped at its first failure, not 12 more
        var answer = Parse(DarlingMcpCustomAlertTools.FormatBacktest(Definition(), null, null, TimeSpan.FromHours(1), End, run, Array.Empty<string>()));
        Assert.Equal("unavailable", (string?)answer["status"]);
        var alpha = (JsonObject)answer["servers"]![0]!;
        Assert.Equal("unavailable", (string?)alpha["status"]);
        Assert.Contains("connection dropped", (string?)alpha["reason"], StringComparison.Ordinal);
        Assert.Null(alpha["fire_count"]);
    }

    [Fact]
    public async Task ACallerCancellation_StillEndsTheCall_NotJustOneServer()
    {
        var servers = new[] { Replay("alpha-01", 1, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)) };
        using var caller = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => CustomAlertBacktest.ReplayAsync(
            servers,
            (_, _, _) =>
            {
                caller.Cancel();
                throw new OperationCanceledException(caller.Token);
            },
            CustomAlertBacktest.MaxQueries, TimeSpan.FromSeconds(60), caller.Token));
    }

    [Fact]
    public void TheCapsAreTheIssuesNumbers()
    {
        Assert.Equal(TimeSpan.FromHours(24), CustomAlertBacktest.DefaultSpan);
        Assert.Equal(TimeSpan.FromDays(7), CustomAlertBacktest.MaxSpan);
        Assert.Equal(10, CustomAlertBacktest.MaxServers);
        Assert.Equal(2_880, CustomAlertBacktest.MaxQueries);
        Assert.Equal(TimeSpan.FromSeconds(60), CustomAlertBacktest.MaxElapsed);
        Assert.Equal(2_880, CustomAlertBacktest.MaxServers * CustomAlertBacktest.StepsPerSpan); // a full call fits the query cap
    }

    [Fact]
    public async Task TheServersCap_AnswersPartial_AndNamesTheServersNotCovered()
    {
        var servers = new[] { Replay("alpha-01", 1, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)) };
        var run = await CustomAlertBacktest.ReplayAsync(
            servers, (_, _, _) => Task.FromResult<double?>(10), CustomAlertBacktest.MaxQueries, TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
        Assert.Equal(CustomAlertBacktest.StopCap.None, run.StoppedBy);

        var answer = Parse(DarlingMcpCustomAlertTools.FormatBacktest(Definition(), null, null, TimeSpan.FromHours(1), End, run, new[] { "zulu-11" }));
        Assert.Equal("partial", (string?)answer["status"]);
        Assert.Equal("servers", (string?)answer["stopped_at"]!["cap"]);
        Assert.Equal("zulu-11", (string?)answer["servers_not_covered"]![0]);
        Assert.Equal("ok", (string?)answer["servers"]![0]!["status"]); // the covered server ran every step
    }

    [Fact]
    public async Task ASpanOver7Days_IsRefused_BeforeAnyStoreRead()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");
        var answer = Parse(await DarlingMcpCustomAlertTools.BacktestCustomAlertRule(
            dataSource, rule_id: null, definition: DefinitionJson, hours: 168.5, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("invalid", (string?)answer["status"]);
        Assert.Contains("168-hour (7-day) cap", (string?)answer["message"], StringComparison.Ordinal);

        // TimeSpan.FromHours throws on NaN and past ~2.56e8; both must be refused as invalid, not escape as an error.
        foreach (var bad in new[] { double.NaN, 1_000_000_000d, double.PositiveInfinity, -1d, 0d })
        {
            var refused = Parse(await DarlingMcpCustomAlertTools.BacktestCustomAlertRule(
                dataSource, rule_id: null, definition: DefinitionJson, hours: bad, cancellationToken: TestContext.Current.CancellationToken));
            Assert.Equal("invalid", (string?)refused["status"]);
        }

        var both = Parse(await DarlingMcpCustomAlertTools.BacktestCustomAlertRule(
            dataSource, rule_id: 1, definition: DefinitionJson, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("invalid", (string?)both["status"]);
    }

    // ---- A source with no samples ------------------------------------------------------------------------------

    [Fact]
    public async Task ASourceWithNoSamples_IsUnavailable_NeverZeroFires()
    {
        var servers = new[]
        {
            Replay("alpha-01", 1, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
            Replay("beta-02", 2, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
        };
        var run = await CustomAlertBacktest.ReplayAsync(
            servers, (_, _, _) => Task.FromResult<double?>(null), CustomAlertBacktest.MaxQueries, TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        var answer = Parse(DarlingMcpCustomAlertTools.FormatBacktest(Definition(), null, null, TimeSpan.FromHours(1), End, run, Array.Empty<string>()));
        Assert.Equal("unavailable", (string?)answer["status"]);
        var alpha = (JsonObject)answer["servers"]![0]!;
        Assert.Equal("unavailable", (string?)alpha["status"]);
        Assert.False(string.IsNullOrEmpty((string?)alpha["reason"]));
        Assert.Null(alpha["fire_count"]); // no answer, not 0
        Assert.Null(alpha["fires"]);
    }

    [Fact]
    public async Task OneServerWithSamples_KeepsTheCallOk_AndTheOtherUnavailable()
    {
        var servers = new[]
        {
            Replay("alpha-01", 1, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
            Replay("beta-02", 2, TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)),
        };
        var run = await CustomAlertBacktest.ReplayAsync(
            servers, (t, _, _) => Task.FromResult<double?>(t.ServerId == 1 ? 30 : null), CustomAlertBacktest.MaxQueries, TimeSpan.FromSeconds(60),
            TestContext.Current.CancellationToken);

        var answer = Parse(DarlingMcpCustomAlertTools.FormatBacktest(Definition(), null, null, TimeSpan.FromHours(1), End, run, Array.Empty<string>()));
        Assert.Equal("ok", (string?)answer["status"]);
        Assert.Equal(1, (int)answer["servers"]![0]!["fire_count"]!);
        Assert.Equal("Warning", (string?)answer["servers"]![0]!["fires"]![0]!["severity"]);
        Assert.Equal("unavailable", (string?)answer["servers"]![1]!["status"]);
    }

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default) =>
            Task.FromResult<AlertDelivery?>(null);
    }
}
