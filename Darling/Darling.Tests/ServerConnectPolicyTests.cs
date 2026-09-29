/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4710: unreachable servers must not use up the fleet's collection slots, and a command timeout must not
/// force a reconnect while the connection is still good.
/// </summary>
public sealed class ServerConnectBackoffTests
{
    [Fact]
    public void Delay_GrowsFromSixtySecondsToTheCap_AndHolds()
    {
        var seconds = Enumerable.Range(1, 8)
            .Select(failures => ServerConnectBackoff.NextDelay(failures, 0.5).TotalSeconds)
            .ToArray();

        Assert.Equal(new double[] { 60, 120, 240, 240, 240, 240, 240, 240 }, seconds);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(50)]
    public void Delay_JitterStaysInsideTheBand_AndTheLongestWaitIsUnderFiveMinutes(int failures)
    {
        var low = ServerConnectBackoff.NextDelay(failures, 0.0);
        var high = ServerConnectBackoff.NextDelay(failures, 1.0);
        var middle = ServerConnectBackoff.NextDelay(failures, 0.5);

        Assert.True(low < middle && middle < high, "jitter must spread the delay");
        Assert.Equal(middle.TotalSeconds * 0.8, low.TotalSeconds, 6);
        Assert.Equal(middle.TotalSeconds * 1.2, high.TotalSeconds, 6);
        Assert.True(high < TimeSpan.FromMinutes(5), "a 10-minute-class wait would delay recovery detection too long");
        Assert.True(middle >= TimeSpan.FromSeconds(60), "never faster than the old fixed retry");
    }

    [Fact]
    public void ADefinitionEdit_ResetsTheBackoff()
    {
        var loopState = typeof(DarlingWorker).GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;
        var state = NewLoopState(loopState, new MonitoredServer { Name = "s", Host = "old.invalid", StoredServerId = 7 });
        loopState.GetProperty("ConsecutiveConnectFailures")!.SetValue(state, 5);
        loopState.GetProperty("NextConnectAttempt")!.SetValue(state, DateTime.UtcNow.AddMinutes(5));

        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);

        var list = Activator.CreateInstance(typeof(List<>).MakeGenericType(loopState))!;
        list.GetType().GetMethod("Add")!.Invoke(list, new[] { state });
        var edited = new MonitoredServer { Name = "s", Host = "new.invalid", StoredServerId = 7 };
        typeof(DarlingWorker).GetMethod("ReconcileServers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(worker, new object[] { list, new List<MonitoredServer> { edited } });

        Assert.Equal(0, (int)loopState.GetProperty("ConsecutiveConnectFailures")!.GetValue(state)!);
        Assert.Equal(DateTime.MinValue, (DateTime)loopState.GetProperty("NextConnectAttempt")!.GetValue(state)!);
    }

    [Fact]
    public void TheWorker_BacksOffOnFailure_AndResetsOnSuccess()
    {
        var source = ReadWorkerSource();

        Assert.Contains("ServerConnectBackoff.NextDelay(server.ConsecutiveConnectFailures", source, StringComparison.Ordinal);
        Assert.Contains("server.ConsecutiveConnectFailures = 0;", source, StringComparison.Ordinal);
        Assert.DoesNotContain("server.NextConnectAttempt = DateTime.UtcNow.AddSeconds(60);\r\n            /* #2255", source, StringComparison.Ordinal);
    }

    internal static object NewLoopState(Type loopState, MonitoredServer config)
    {
        var state = Activator.CreateInstance(loopState)!;
        loopState.GetProperty("Config")!.SetValue(state, config);
        return state;
    }

    internal static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}

public sealed class ServerConnectGateOccupancyTests
{
    [Fact]
    public async Task DeadServers_DoNotHoldFleetPermits_WhileTheirConnectsAreInFlight()
    {
        const int dead = 30;
        using var fleetGate = new SemaphoreSlim(4, 4);
        using var probeGate = new SemaphoreSlim(ServerConnectProbe.GateWidth, ServerConnectProbe.GateWidth);
        using var cts = new CancellationTokenSource();
        var inFlight = 0;

        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);
        typeof(DarlingWorker).GetField("_connectProbeGate", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, probeGate);
        /* A dead server: the connect parks (SqlClient's 15 s) and never answers. */
        worker.ConnectOverride = async (_, token) =>
        {
            Interlocked.Increment(ref inFlight);
            await Task.Delay(Timeout.Infinite, token);
            throw new InvalidOperationException("unreachable");
        };

        var loopState = typeof(DarlingWorker).GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;
        var process = typeof(DarlingWorker).GetMethod("ProcessServerSweepAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var bodies = Enumerable.Range(0, dead).Select(i =>
        {
            var state = ServerConnectBackoffTests.NewLoopState(loopState, new MonitoredServer { Name = $"dead-{i}", Host = $"dead-{i}.invalid" });
            return (Task)process.Invoke(worker, new object?[] { state, null, null, null, null, new DarlingConfig(), fleetGate, cts.Token })!;
        }).ToArray();

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (Volatile.Read(ref inFlight) < ServerConnectProbe.GateWidth && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }

        /* The connect gate is full of dead servers' attempts and the rest are queued behind it, yet not one
           of them holds, or is queued for, a fleet permit: healthy servers still get all four. */
        Assert.Equal(ServerConnectProbe.GateWidth, Volatile.Read(ref inFlight));
        Assert.Equal(4, fleetGate.CurrentCount);
        Assert.True(await fleetGate.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
        fleetGate.Release();

        await cts.CancelAsync();
        foreach (var body in bodies)
        {
            try
            {
                await body;
            }
            catch (OperationCanceledException)
            {
                /* Shutdown unwinding, as intended. */
            }
        }

        Assert.Equal(4, fleetGate.CurrentCount);
    }

    [Fact]
    public void TheWorker_ConnectsBeforeItTakesTheFleetPermit()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        var body = source.IndexOf("private async Task ProcessServerSweepAsync(", StringComparison.Ordinal);
        var attempt = source.IndexOf("ServerConnectProbe.AttemptAsync(", body, StringComparison.Ordinal);
        var fleetWait = source.IndexOf("await gate.WaitAsync(stoppingToken);", body, StringComparison.Ordinal);

        Assert.True(body > 0 && attempt > body, "the body must run the connect attempt through the connect gate");
        Assert.True(attempt < fleetWait, "the connect attempt must come before the fleet gate wait");
        Assert.DoesNotContain("DarlingServerConnector.ConnectAsync(server.Config, _logger, cancellationToken)", source, StringComparison.Ordinal);
    }
}

public sealed class ConnectionFaultDispositionTests
{
    private static ServerRuntime SqlRuntime() => new()
    {
        Config = new MonitoredServer { Name = "s", Host = "s.invalid" },
        ConnectionString = "Server=s.invalid;Integrated Security=true",
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
        StorageName = "s",
        ServerId = 1,
    };

    [Fact]
    public async Task CommandTimeout_WithAHealthyProbe_KeepsTheRuntime()
    {
        var probes = 0;
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(-2, 11), SqlRuntime(),
            (_, _) => { probes++; return Task.FromResult(true); }, TestContext.Current.CancellationToken);

        Assert.False(drop);
        Assert.Equal(1, probes);
    }

    [Fact]
    public async Task CommandTimeout_WithAFailedProbe_DropsTheRuntime()
    {
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(-2, 11), SqlRuntime(),
            (_, _) => Task.FromResult(false), TestContext.Current.CancellationToken);

        Assert.True(drop);
    }

    [Fact]
    public async Task FatalClass_DropsAtOnce_WithoutProbing()
    {
        var probes = 0;
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(4060, 20), SqlRuntime(),
            (_, _) => { probes++; return Task.FromResult(true); }, TestContext.Current.CancellationToken);

        Assert.True(drop);
        Assert.Equal(0, probes);
    }

    [Fact]
    public async Task OrdinaryQueryError_KeepsTheRuntime_WithoutProbing()
    {
        var probes = 0;
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(8134, 16), SqlRuntime(),
            (_, _) => { probes++; return Task.FromResult(true); }, TestContext.Current.CancellationToken);

        Assert.False(drop);
        Assert.Equal(0, probes);
    }

    [Fact]
    public void TheWorker_SettlesATimeoutWithAProbe_NotADrop()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();

        Assert.Contains("ConnectionFaultDisposition.ShouldDropRuntimeAsync(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("sqlEx.Number == -2", source, StringComparison.Ordinal);
    }

    /// <summary>SqlException has no public constructor; this builds one through the driver's internals.</summary>
    private static class SqlExceptionFactory
    {
        public static SqlException Create(int number, byte errorClass)
        {
            const BindingFlags all = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static;
            var errorCtor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .OrderByDescending(c => c.GetParameters().Length).First();
            var ints = new Queue<object>(new object[] { number, 0, 0, 0 });
            var bytes = new Queue<object>(new object[] { (byte)0, errorClass, (byte)0, (byte)0 });
            var strings = new Queue<object>(new object[] { "server", "message", "procedure", "extra", "extra" });
            var args = errorCtor.GetParameters().Select(p =>
                p.ParameterType == typeof(int) ? ints.Dequeue()
                : p.ParameterType == typeof(byte) ? bytes.Dequeue()
                : p.ParameterType == typeof(string) ? strings.Dequeue()
                : p.ParameterType == typeof(uint) ? (object)0u
                : p.ParameterType == typeof(Exception) ? null!
                : p.HasDefaultValue ? p.DefaultValue! : throw new InvalidOperationException($"unmapped SqlError ctor parameter {p.ParameterType}")).ToArray();
            var error = errorCtor.Invoke(args);

            var collection = Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
            typeof(SqlErrorCollection).GetMethod("Add", all, null, new[] { typeof(SqlError) }, null)!.Invoke(collection, new[] { error });

            var create = typeof(SqlException).GetMethods(all)
                .Where(m => m.Name == "CreateException" && m.GetParameters().Length >= 2
                    && m.GetParameters()[0].ParameterType == typeof(SqlErrorCollection)
                    && m.GetParameters()[1].ParameterType == typeof(string))
                .OrderBy(m => m.GetParameters().Length).First();
            var createArgs = create.GetParameters().Select((p, i) =>
                i == 0 ? collection
                : i == 1 ? "11.0"
                : p.ParameterType == typeof(Guid) ? Guid.Empty
                : p.HasDefaultValue ? p.DefaultValue! : null!).ToArray();
            return (SqlException)create.Invoke(null, createArgs)!;
        }
    }
}
