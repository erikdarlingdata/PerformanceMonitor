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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
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
    public void TheSurfacingLine_FollowsTheSameRule()
    {
        var logger = new CapturingTestLogger();
        var gate = new RepeatLogGate();
        var now = T0;
        gate.Clock = () => now;
        var runner = new DarlingCollectorRunner(
            NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x"),
            new CollectorDeltaCalculator(),
            logger,
            databaseScope: (_, _) => new List<string>(),
            separatelyMonitoredDatabases: _ => new List<string>(),
            installId: () => "x")
        {
            RepeatLog = gate,
        };

        for (var hour = 0; hour < 24; hour++)
        {
            now = T0.AddHours(hour);
            runner.LogFailedInAllDatabases("pg_predicate_stats", 3, "pg-a");
        }

        Assert.Equal(1, logger.Lines.Count(l => l.Contains("surfacing the first failure", StringComparison.Ordinal)));

        now = T0.AddHours(24);
        runner.LogFailedInAllDatabases("pg_predicate_stats", 3, "pg-a");
        runner.LogFailedInAllDatabases("pg_predicate_stats", 4, "pg-a");
        Assert.Equal(3, logger.Lines.Count(l => l.Contains("surfacing the first failure", StringComparison.Ordinal)));
    }
}
