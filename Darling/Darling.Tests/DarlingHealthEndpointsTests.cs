/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#8184: the health port's answer table and its vocabulary shared with <c>/api/ping</c>.</summary>
public sealed class DarlingHealthEndpointsTests
{
    private static CollectorRuntimeState.Snapshot? Snap(int? phase) =>
        phase is null
            ? null
            : new CollectorRuntimeState.Snapshot((CollectorRuntimeState.CollectorPhase)phase.Value, null, null, 0, 0, DateTime.UtcNow);

    private const int Retrying = (int)CollectorRuntimeState.CollectorPhase.Retrying;
    private const int Stopped = (int)CollectorRuntimeState.CollectorPhase.Stopped;
    private const int Collecting = (int)CollectorRuntimeState.CollectorPhase.Collecting;

    public static IEnumerable<object?[]> Table() => new[]
    {
        new object?[] { null, 200, "starting", 503, "starting" },
        new object?[] { Retrying, 200, "degraded", 503, "degraded" },
        new object?[] { Stopped, 200, "stopped", 503, "stopped" },
        new object?[] { Collecting, 200, "ok", 200, "ok" },
        new object?[] { 99, 200, "stopped", 503, "stopped" },
    };

    [Theory]
    [MemberData(nameof(Table))]
    public void Answer_CoversTheWholeTable(int? phase, int liveStatus, string liveWord, int readyStatus, string readyWord)
    {
        var s = Snap(phase);
        Assert.Equal(new HealthAnswer(liveStatus, liveWord), DarlingHealthEndpoints.Answer(HealthProbe.Live, s));
        Assert.Equal(new HealthAnswer(readyStatus, readyWord), DarlingHealthEndpoints.Answer(HealthProbe.Ready, s));
    }

    [Theory]
    [MemberData(nameof(Table))]
    public void Answer_WordAlwaysMatchesPing(int? phase, int liveStatus, string liveWord, int readyStatus, string readyWord)
    {
        Assert.NotEmpty(liveWord + readyWord);
        Assert.NotEqual(0, liveStatus + readyStatus);
        var s = Snap(phase);
        var ping = DarlingWebEndpoints.DescribePing(s).Status;
        Assert.Equal(ping, DarlingHealthEndpoints.Answer(HealthProbe.Live, s).Word);
        Assert.Equal(ping, DarlingHealthEndpoints.Answer(HealthProbe.Ready, s).Word);
    }

    private static FrontDoorStoreState Store(FrontDoorStoreHealth health, string? error) =>
        new(new FrontDoorStore("east", new Uri("http://east.example.invalid:5152/"), null, null), health, Array.Empty<RegisteredServer>(), null, error);

    [Fact]
    public void AnswerFrontDoor_Cases()
    {
        var none = Array.Empty<FrontDoorStoreState>();
        var allAuthFailed = new[] { Store(FrontDoorStoreHealth.AuthFailed, "401"), Store(FrontDoorStoreHealth.AuthFailed, null) };
        var notYetTried = new[] { Store(FrontDoorStoreHealth.Unreachable, null) };
        var stopped = Snap(Stopped);

        Assert.Equal(new HealthAnswer(503, "stopped"), DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Ready, stopped, none));
        Assert.Equal(new HealthAnswer(503, "degraded"), DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Ready, null, allAuthFailed));
        Assert.Equal(new HealthAnswer(200, "ok"), DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Ready, null, none));
        Assert.Equal(new HealthAnswer(200, "ok"), DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Ready, null, notYetTried));

        Assert.Equal(200, DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Live, stopped, none).StatusCode);
        Assert.Equal(200, DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Live, null, allAuthFailed).StatusCode);
        Assert.Equal(200, DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Live, null, none).StatusCode);
        Assert.Equal(200, DarlingHealthEndpoints.AnswerFrontDoor(HealthProbe.Live, null, notYetTried).StatusCode);
    }
}
