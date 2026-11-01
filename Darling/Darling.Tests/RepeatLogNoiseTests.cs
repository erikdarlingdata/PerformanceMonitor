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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7067: the service log repeated a connect-failure line on every retry and a permanent precondition warning on every
/// collection cycle. These drive the real logging methods on a stepped clock and count the lines.
/// </summary>
public sealed class RepeatLogNoiseTests
{
    private static readonly DateTime T0 = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);

    private sealed class Lines : ILogger<DarlingWorker>
    {
        public List<(LogLevel Level, string Text)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));

        public int Count(string contains) => Entries.Count(e => e.Text.Contains(contains, StringComparison.Ordinal));
    }

    private sealed class Clock
    {
        public DateTime Now = T0;
    }

    private static (DarlingWorker Worker, Lines Log, Clock Clock) NewWorker()
    {
        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        var log = new Lines();
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(worker, log);
        var clock = new Clock();
        worker.RepeatLog.Clock = () => clock.Now;
        return (worker, log, clock);
    }

    private static DarlingWorker.ServerLoopState NewServer(string name = "srv-a") => new()
    {
        Config = new MonitoredServer { Name = name, Host = name },
    };

    [Fact]
    public void AServerDownFor25Hours_LogsTheFirstFailure_AnHourlyReminder_AndOneRecovery()
    {
        var (worker, log, clock) = NewWorker();
        var server = NewServer();
        var cause = new InvalidOperationException("connection refused");

        var attempts = 0;
        for (var minute = 0; minute <= 25 * 60; minute += 5)
        {
            clock.Now = T0.AddMinutes(minute);
            worker.LogConnectFailure(server, cause, 240);
            attempts++;
        }

        Assert.Equal(1, log.Count("Connect failed, retrying"));
        Assert.Equal(25, log.Count("Connect still failing"));
        /* The reminder carries the attempts so far and the time down: the last one, at hour 25, is attempt 301. */
        Assert.Contains(log.Entries, e => e.Text.Contains("still failing after 301 attempts over 1d 1h", StringComparison.Ordinal));

        var note = worker.TakeConnectRecoveryNote(server);
        Assert.Contains($"{attempts} failed attempt(s) over 1d 1h", note, StringComparison.Ordinal);
        Assert.Null(server.ConnectDownSinceUtc);
        Assert.Equal(0, server.ConnectAttemptsSinceDown);
    }

    [Fact]
    public void ACauseThatChanges_IsLoggedAtOnce_EvenInsideTheHour()
    {
        var (worker, log, clock) = NewWorker();
        var server = NewServer();

        worker.LogConnectFailure(server, new InvalidOperationException("connection refused"), 60);
        clock.Now = T0.AddMinutes(5);
        worker.LogConnectFailure(server, new InvalidOperationException("connection refused"), 60);
        clock.Now = T0.AddMinutes(10);
        worker.LogConnectFailure(server, new InvalidOperationException("login failed for user"), 60);

        Assert.Equal(2, log.Count("Connect failed, retrying"));
        Assert.Equal(1, log.Count("login failed for user"));
        Assert.Equal(0, log.Count("Connect still failing"));
    }

    [Fact]
    public void AServerThatConnectsFirstTime_HasNoRecoveryNote_AndAFreshFailureAfterwardsLogsInFull()
    {
        var (worker, log, clock) = NewWorker();
        var server = NewServer();

        Assert.Equal(string.Empty, worker.TakeConnectRecoveryNote(server));

        worker.LogConnectFailure(server, new InvalidOperationException("refused"), 60);
        worker.TakeConnectRecoveryNote(server);
        server.LastConnectFailureLogged = null; // what TryConnectAsync does on success
        clock.Now = T0.AddMinutes(1);
        worker.LogConnectFailure(server, new InvalidOperationException("refused"), 60);

        Assert.Equal(2, log.Count("Connect failed, retrying"));
        Assert.Equal(0, log.Count("Connect still failing"));
    }

    [Fact]
    public void APreconditionWarning_LogsOnceThenOncePerDay_AndAgainAtOnceWhenItChangesOrAfterItClears()
    {
        var (worker, log, clock) = NewWorker();

        void Cycle(string explanation = "extension pg_buffer_usage is not created") =>
            worker.LogPostgresFault("pg-a", "pg_buffer_usage", "EXTENSION_MISSING", "42P01", explanation);

        for (var minute = 0; minute < 23 * 60; minute++)
        {
            clock.Now = T0.AddMinutes(minute);
            Cycle();
        }

        Assert.Equal(1, log.Count("pg_buffer_usage => EXTENSION_MISSING"));

        clock.Now = T0.AddHours(24);
        Cycle();
        Assert.Equal(2, log.Count("pg_buffer_usage => EXTENSION_MISSING"));

        clock.Now = T0.AddHours(25);
        Cycle("a different explanation");
        Assert.Equal(3, log.Count("pg_buffer_usage => EXTENSION_MISSING"));

        /* The same text on another collector and on another server are their own sightings. */
        worker.LogPostgresFault("pg-a", "pg_kernel_stats", "EXTENSION_MISSING", "42P01", "x");
        worker.LogPostgresFault("pg-b", "pg_buffer_usage", "EXTENSION_MISSING", "42P01", "a different explanation");
        Assert.Equal(5, log.Count("EXTENSION_MISSING"));

        /* The collector succeeds (RunOneAsync clears the memory), then the same failure comes back: logged at once. */
        Cycle("a different explanation");
        Assert.Equal(5, log.Count("EXTENSION_MISSING"));
        worker.RepeatLog.Clear("pg-a", "pg_buffer_usage");
        Cycle("a different explanation");
        Assert.Equal(6, log.Count("EXTENSION_MISSING"));
    }

    [Fact]
    public void ACauseThatFlipsBack_IsLoggedEachTime_ButTheSameCauseTwiceInADayIsLoggedOnce()
    {
        var (worker, log, clock) = NewWorker();

        void Cycle(string explanation) =>
            worker.LogPostgresFault("pg-a", "pg_buffer_usage", "EXTENSION_MISSING", "42P01", explanation);

        /* A, B, A ten minutes apart: the second A follows a different message, so it is a new sighting. */
        Cycle("cause A");
        clock.Now = T0.AddMinutes(10);
        Cycle("cause B");
        clock.Now = T0.AddMinutes(20);
        Cycle("cause A");
        Assert.Equal(3, log.Count("pg_buffer_usage => EXTENSION_MISSING"));

        /* A, A within the day: the repeat is silent. */
        clock.Now = T0.AddMinutes(30);
        Cycle("cause A");
        clock.Now = T0.AddHours(5);
        Cycle("cause A");
        Assert.Equal(3, log.Count("pg_buffer_usage => EXTENSION_MISSING"));
    }

    private static DarlingCollectorRunner NewRunner(CapturingTestLogger logger, RepeatLogGate gate) => new(
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x"),
        new CollectorDeltaCalculator(),
        logger,
        databaseScope: (_, _) => new List<string>(),
        separatelyMonitoredDatabases: _ => new List<string>(),
        installId: () => "x")
    {
        RepeatLog = gate,
    };

    private static ServerRuntime RuntimeFor(string name, CollectorTargetEngine engine) => new()
    {
        Config = new MonitoredServer { Name = name, Host = name },
        ConnectionString = $"Host={name};Port=5432",
        Target = new CollectorTargetInfo { Engine = engine },
        StorageName = name,
        ServerId = 7,
    };

    private static int Surfaced(CapturingTestLogger logger) =>
        logger.Lines.Count(l => l.Contains("surfacing the first failure", StringComparison.Ordinal));

    [Fact]
    public void ThePostgresSurfacingLine_FollowsTheWarningItExplains_WhenThatWarningIsGated()
    {
        var logger = new CapturingTestLogger();
        var gate = new RepeatLogGate();
        var now = T0;
        gate.Clock = () => now;
        var runner = NewRunner(logger, gate);
        var server = RuntimeFor("pg-a", CollectorTargetEngine.PostgreSql);
        /* An undefined table: the worker records it as a precondition (not ERROR), and logs it once a day. */
        var fault = new PostgresException("relation does not exist", "ERROR", "ERROR", "42P01");

        for (var hour = 0; hour < 24; hour++)
        {
            now = T0.AddHours(hour);
            runner.LogFailedInAllDatabases(server, "pg_predicate_stats", 3, fault);
        }

        Assert.Equal(1, Surfaced(logger));

        now = T0.AddHours(24);
        runner.LogFailedInAllDatabases(server, "pg_predicate_stats", 3, fault);
        runner.LogFailedInAllDatabases(server, "pg_predicate_stats", 4, fault);
        Assert.Equal(3, Surfaced(logger));
    }

    [Fact]
    public void TheSurfacingLine_IsLoggedEveryCycle_WhenTheWorkersWarningForTheFaultIsNotGated()
    {
        var logger = new CapturingTestLogger();
        var gate = new RepeatLogGate();
        var now = T0;
        gate.Clock = () => now;
        var runner = NewRunner(logger, gate);

        /* SQL Server: a fault surfaced from every database lands in RunOneAsync's permissions or general ERROR arm,
           and those warn on every cycle. The line that explains them must not go quiet while they keep printing. */
        var sqlServer = RuntimeFor("sql-a", CollectorTargetEngine.SqlServer);
        for (var cycle = 0; cycle < 5; cycle++)
        {
            now = T0.AddMinutes(cycle);
            runner.LogFailedInAllDatabases(sqlServer, "query_store", 2, new InvalidOperationException("denied"));
        }

        Assert.Equal(5, Surfaced(logger));

        /* A PostgreSQL fault that the worker classifies ERROR is logged every cycle by its general arm too. */
        var pg = RuntimeFor("pg-a", CollectorTargetEngine.PostgreSql);
        var internalError = new PostgresException("boom", "ERROR", "ERROR", "XX000");
        for (var cycle = 0; cycle < 3; cycle++)
        {
            now = T0.AddMinutes(10 + cycle);
            runner.LogFailedInAllDatabases(pg, "pg_predicate_stats", 3, internalError);
        }

        Assert.Equal(8, Surfaced(logger));
    }

    private static (DarlingWorker Worker, Lines Log, Clock Clock) NewRealWorker()
    {
        var log = new Lines();
        var worker = new DarlingWorker(
            log,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            /* A store nothing listens on: the collection_log write fails and is swallowed, as it is for any observability write. */
            StoreForTests = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1"),
        };
        /* The failed-connect path reports the outage to the self-alerts; these fakes take it and deliver nothing. */
        worker.SelfAlertsForTests = new DarlingSelfAlertEvaluator(
            new DarlingSelfAlertTests.FakeSettings(), new DarlingSelfAlertTests.RecordingDeliverer(),
            new DarlingSelfAlertTests.FakeHistoryStore(), _ => false);
        var clock = new Clock();
        worker.RepeatLog.Clock = () => clock.Now;
        return (worker, log, clock);
    }

    [Fact]
    public async Task ASuccessfulRun_ThroughRunOneAsync_ClearsTheGate_SoTheSameFailureIsLoggedAtOnce()
    {
        var (worker, log, _) = NewRealWorker();
        var runtime = RuntimeFor("pg-a", CollectorTargetEngine.PostgreSql);
        var server = new DarlingWorker.ServerLoopState { Config = runtime.Config, Runtime = runtime };
        var runner = NewRunner(new CapturingTestLogger(), worker.RepeatLog);
        const string Collector = "pg_buffer_usage";

        worker.DispatchOverrideForTests = (_, _, _) => Task.FromResult(new CollectorRunResult(3, 5, 7, []));

        worker.LogPostgresFault("pg-a", Collector, "EXTENSION_MISSING", "42P01", "x");
        worker.LogPostgresFault("pg-a", Collector, "EXTENSION_MISSING", "42P01", "x");
        Assert.Equal(1, log.Count("EXTENSION_MISSING"));

        var runOne = typeof(DarlingWorker).GetMethod("RunOneAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var rows = await (Task<int>)runOne.Invoke(
            worker, [server, runner, Collector, null, TestContext.Current.CancellationToken, false, null, null])!;
        Assert.Equal(3, rows);

        /* The run succeeded, so the same failure coming back within the day is a new sighting. */
        worker.LogPostgresFault("pg-a", Collector, "EXTENSION_MISSING", "42P01", "x");
        Assert.Equal(2, log.Count("EXTENSION_MISSING"));
    }

    [Fact]
    public async Task TheConnectPath_CountsFailedAttempts_AndASuccessfulConnectTakesTheRecoveryBookkeeping()
    {
        var (worker, log, clock) = NewRealWorker();
        var config = new MonitoredServer { Name = "srv-a", Host = "srv-a", StoredServerId = 7 };
        var server = new DarlingWorker.ServerLoopState { Config = config };
        var tryConnect = typeof(DarlingWorker).GetMethod("TryConnectAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var ct = TestContext.Current.CancellationToken;

        Task Connect(ConnectAttempt attempt) =>
            (Task)tryConnect.Invoke(worker, [server, null, new DarlingConfig(), attempt, ct])!;

        /* Three failed attempts, five minutes apart, through the connect call site: one full line, no repeats. */
        for (var attempt = 0; attempt < 3; attempt++)
        {
            clock.Now = T0.AddMinutes(attempt * 5);
            await Connect(new ConnectAttempt(config, null, new InvalidOperationException("connection refused")));
        }

        Assert.Equal(3, server.ConnectAttemptsSinceDown);
        Assert.Equal(T0, server.ConnectDownSinceUtc);
        Assert.Equal(1, log.Count("Connect failed, retrying"));
        Assert.Equal(0, log.Count("Connect still failing"));

        /* The server is retired so the connect stops right after the install bookkeeping and writes nothing to the store. */
        server.Retired = true;
        clock.Now = T0.AddMinutes(15);
        await Connect(new ConnectAttempt(config, RuntimeFor("srv-a", CollectorTargetEngine.SqlServer), null));

        Assert.Null(server.ConnectDownSinceUtc);
        Assert.Equal(0, server.ConnectAttemptsSinceDown);
        Assert.Equal(default, server.LastConnectFailureLineUtc);
    }
}
