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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8528: a restart or a failover of the monitored instance leaves the capture sessions stopped. The sweep that sees the identity
/// move reads them before anything starts them, so the run records SESSION_MISSING, and the Capture Down self-alert fired on that
/// row although the service itself started the sessions seconds later. These tests pin the three parts of the repair: the
/// self-alert is told what the service's own ensure found (a session it started and whose collector has not read since is not a
/// capture that is down; a session it could not start is, on that first sweep), and the detail says what the ensure actually
/// found instead of claiming a create failed. The live class pins the run order: after an identity change the capture run
/// ensures the sessions before its read.
/// </summary>
public sealed class CaptureDownAfterFailoverTests
{
    private const int ServerId = 852800;
    private const string Name = "FAILOVER-SRV";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AFailoverWhoseEnsureRestartedTheSession_FiresNoCaptureDown()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        /* The newest attention row is SESSION_MISSING from before the start; the service started the session and the collector has not read since. */
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Client timeouts" },
            CaptureDownSeams.Report(restarted: new[] { "attention" }), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* The read that follows succeeds: nothing was active, so nothing is restored either. */
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, Array.Empty<string>(), CaptureDownSeams.Report(), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task AnActiveCaptureDown_NeitherRepeatsNorRestores_WhileTheRestartedSessionAwaitsItsRead()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" }, CaptureDownSeams.Report(), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* The hourly ensure started the stopped session; the verdict waits for the collector's next row. */
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" },
            CaptureDownSeams.Report(restarted: new[] { "deadlocks" }), Ct);

        Assert.Single(h.Deliverer.Outcomes);
        Assert.DoesNotContain(h.History.Records, r => r.MetricName == "Capture Restored");

        /* The read succeeded: the capture is restored once, as before. */
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, Array.Empty<string>(), CaptureDownSeams.Report(), Ct);

        Assert.Single(h.Deliverer.Outcomes);
        Assert.Single(h.History.Records, r => r.MetricName == "Capture Restored");
    }

    [Fact]
    public async Task NoIdentityChange_ASessionFoundStopped_FiresOnTheFirstSweep_AndSaysWhatTheServiceDoesNext()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        /* Someone stopped the session by hand: the collector's read failed, and the service's last check of the sessions found nothing to fix. */
        await e.ApplyCaptureDownAsync(ServerId, Name, new[] { "Client timeouts" }, Array.Empty<string>(), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Contains("The Client timeouts Extended Events session is missing", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("found the Client timeouts session stopped, absent or unreadable", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("next check", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("could not be created", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("usually a permissions problem", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AMixedSweep_FiresOnlyForTheCaptureStillDown()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Blocking", "Client timeouts" },
            CaptureDownSeams.Report(restarted: new[] { "attention" }), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Blocking", fired.CurrentValue);
        Assert.Contains("The Blocking Extended Events session is missing", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Client timeouts", fired.DetailText, StringComparison.Ordinal);
    }
}

/// <summary>
/// #8528: the run side. After an identity change the capture collectors ensure the sessions before they read them, and a run
/// that writes its row ends the "started, awaiting its read" state of its session. Needs a store for the run's row.
/// </summary>
[Collection("live-postgres")]
public sealed class CaptureDownAfterFailoverLiveTests
{
    private const int EnsureServerId = -852801;
    private const int EnsuredServerId = -852802;
    private const int MarkServerId = -852803;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AfterAnIdentityChange_TheAttentionRunEnsuresTheSessionsBeforeItsRead()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live capture run-order test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, EnsureServerId, "failover-ensure", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var fixture = Fixture.Build(postgres, EnsureServerId, "failover-ensure");

            /* The sweep's wait_stats run saw the identity move and cleared the ensure stamp (ForgetLongQueryTraceLatch). */
            fixture.Server.XeSessionsEnsuredAtUtc = null;

            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "attention", EnsureServerId, fixture.Effective("attention"), ct);

            Assert.Equal(new[] { "ensure", "read" }, fixture.Order);
            Assert.NotNull(fixture.Server.XeSessionsEnsuredAtUtc);
            var row = Assert.Single(await ReadRowsAsync(connection, EnsureServerId, "attention", ct));
            Assert.Equal("SUCCESS", row.Status);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task WithTheSessionsEnsuredThisHour_TheAttentionRunReadsWithoutEnsuringAgain()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live capture run-order test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, EnsuredServerId, "failover-ensured", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var fixture = Fixture.Build(postgres, EnsuredServerId, "failover-ensured");
            var stamp = DateTime.UtcNow.AddMinutes(-5);
            fixture.Server.XeSessionsEnsuredAtUtc = stamp;

            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "attention", EnsuredServerId, fixture.Effective("attention"), ct);

            Assert.Equal(new[] { "read" }, fixture.Order);
            Assert.Equal(stamp, fixture.Server.XeSessionsEnsuredAtUtc);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ARunThatWritesItsRow_EndsTheSessionsAwaitingReadState()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live capture run-order test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, MarkServerId, "failover-mark", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var fixture = Fixture.Build(postgres, MarkServerId, "failover-mark");
            fixture.Server.XeSessionsEnsuredAtUtc = DateTime.UtcNow;
            CaptureDownSeams.MarkRestarted(fixture.Server, "deadlocks");

            /* Before the run: the session was started and not read since, so the self-alert holds its verdict. */
            var before = new DarlingSelfAlertTests.Harness();
            await CaptureDownSeams.ApplyAsync(before.Build(), MarkServerId, "failover-mark", new[] { "Deadlock" }, CaptureDownSeams.ReportOf(fixture.Server), ct);
            Assert.Empty(before.Deliverer.Outcomes);

            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "deadlocks", MarkServerId, fixture.Effective("deadlocks"), ct);

            /* After the run wrote its row, the newest row is the read's own, and a SESSION_MISSING there is a capture that is down. */
            var after = new DarlingSelfAlertTests.Harness();
            await CaptureDownSeams.ApplyAsync(after.Build(), MarkServerId, "failover-mark", new[] { "Deadlock" }, CaptureDownSeams.ReportOf(fixture.Server), ct);
            Assert.Single(after.Deliverer.Outcomes);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private sealed class Fixture
    {
        public required DarlingWorker Worker { get; init; }
        public required DarlingCollectorRunner Runner { get; init; }
        public required DarlingWorker.ServerLoopState Server { get; init; }
        public List<string> Order { get; } = new();

        public EffectiveSchedule Effective(string collector) => StoreConfigProvider.ResolveSchedule(collector, Server.Config.ServerId, Worker.ScheduleOverridesForTest);

        public static Fixture Build(NpgsqlDataSource postgres, int serverId, string host)
        {
            var config = new MonitoredServer { Name = host, Host = host };
            var runtime = new ServerRuntime
            {
                Config = config,
                ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
                Target = new CollectorTargetInfo(),
                StorageName = host,
                ServerId = serverId,
            };
            var server = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
            var runner = new DarlingCollectorRunner(
                postgres,
                new CollectorDeltaCalculator(),
                databaseScope: (_, _) => new List<string>(),
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => "0a1b2c3d");
            var worker = new DarlingWorker(
                NullLogger<DarlingWorker>.Instance,
                NullLoggerFactory.Instance,
                new McpRuntimeState(),
                new WebRuntimeState(),
                new MonitoredServerRegistryState(),
                new CollectorRuntimeState(),
                new WebTlsCertificateState(),
                new BaselineCache(),
                new ReadLatencyAccumulator())
            {
                StoreForTests = postgres,
            };

            /* One list records both: the ensure the runner's seam stands in for, and the read the dispatch seam stands in for. */
            var fixture = new Fixture { Worker = worker, Runner = runner, Server = server };
            runner.XeEnsureOverrideForTests = (_, _) =>
            {
                fixture.Order.Add("ensure");
                return Task.CompletedTask;
            };
            worker.DispatchOverrideForTests = (_, _, _) =>
            {
                fixture.Order.Add("read");
                return Task.FromResult(new CollectorRunResult(0, 0, 0, Array.Empty<CollectorMeasurement>()));
            };
            return fixture;
        }
    }

    private static async Task<List<(string Status, string? Message)>> ReadRowsAsync(NpgsqlConnection connection, int serverId, string collector, CancellationToken ct)
    {
        var rows = new List<(string, string?)>();
        await using var command = new NpgsqlCommand(
            "SELECT status, error_message FROM collection_log WHERE server_id = $1 AND collector_name = $2 ORDER BY collection_time", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(collector);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return rows;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { EnsureServerId, EnsuredServerId, MarkServerId })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", id);
        }
    }
}

/// <summary>
/// #8528: the seams these tests reach by name, so the tests compile against a service without them and fail at their asserts there:
/// the report of what the service's own ensure found for a server, the loop state's view of it, and the self-alert arm that takes it.
/// </summary>
internal static class CaptureDownSeams
{
    private const BindingFlags Any = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;

    private static Type ReportType()
    {
        var type = typeof(DarlingSelfAlertEvaluator).Assembly.GetType("PerformanceMonitor.Darling.Service.CaptureSessionEnsureReport");
        Assert.True(type is not null, "The service has no CaptureSessionEnsureReport: the Capture Down self-alert is not told what the service's own ensure of the capture sessions found.");
        return type!;
    }

    /// <summary>A report with no ensure failure: the collectors whose session the ensure started and that have not read since.</summary>
    public static object Report(string[]? restarted = null)
    {
        var map = new Dictionary<string, DarlingXeSessions.XeEnsureFailure>(StringComparer.OrdinalIgnoreCase);

        var type = ReportType();
        var ctor = type.GetConstructors(Any).FirstOrDefault(c => c.GetParameters().Length == 2);
        Assert.True(ctor is not null, "CaptureSessionEnsureReport takes the restarted collectors and the ensure failures.");
        return ctor!.Invoke(new object[] { (IReadOnlyCollection<string>)(restarted ?? Array.Empty<string>()), (IReadOnlyDictionary<string, DarlingXeSessions.XeEnsureFailure>)map });
    }

    /// <summary>The loop state's own report, as the fleet pass hands it to the self-alert.</summary>
    public static object ReportOf(DarlingWorker.ServerLoopState server)
    {
        var method = typeof(DarlingWorker.ServerLoopState).GetMethod("CaptureSessionReport", Any);
        Assert.True(method is not null, "ServerLoopState has no CaptureSessionReport(): the fleet pass cannot tell the self-alert what the ensure found.");
        return method!.Invoke(server, Array.Empty<object>())!;
    }

    /// <summary>Marks a collector's session as started by the ensure and not read since, as EnsureAlwaysOnXeSessionsAsync does.</summary>
    public static void MarkRestarted(DarlingWorker.ServerLoopState server, string collector)
    {
        var property = typeof(DarlingWorker.ServerLoopState).GetProperty("CaptureSessionRestarts", Any);
        Assert.True(property is not null, "ServerLoopState has no CaptureSessionRestarts: a session the ensure started is not remembered until its collector reads it.");
        var marks = property!.GetValue(server)!;
        var mark = marks.GetType().GetMethod("Mark", Any);
        Assert.True(mark is not null, "CaptureSessionRestarts has no Mark(collector).");
        mark!.Invoke(marks, new object[] { collector });
    }

    /// <summary>The Capture Down arm with the ensure report, through its optional parameter.</summary>
    public static Task ApplyAsync(DarlingSelfAlertEvaluator evaluator, int serverId, string serverName, IReadOnlyList<string> missing, object report, CancellationToken ct)
    {
        var method = typeof(DarlingSelfAlertEvaluator).GetMethod("ApplyCaptureDownAsync", Any);
        Assert.True(method is not null, "The evaluator has no ApplyCaptureDownAsync.");
        var parameters = method!.GetParameters();
        var ensure = Array.FindIndex(parameters, p => p.ParameterType == report.GetType());
        Assert.True(ensure >= 0, "ApplyCaptureDownAsync takes no CaptureSessionEnsureReport: a session the service's own ensure started still fires Capture Down.");

        var args = new object?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            args[i] = parameters[i].HasDefaultValue ? parameters[i].DefaultValue : null;
        }

        args[0] = serverId;
        args[1] = serverName;
        args[2] = missing;
        args[3] = Array.Empty<string>();
        args[4] = ct;
        args[ensure] = report;
        return (Task)method.Invoke(evaluator, args)!;
    }
}
