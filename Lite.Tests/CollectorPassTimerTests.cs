/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: the fact pass awaits its collectors one after another, and a field log could only say the pass was slow. Each collector logs its own
/// elapsed time at Debug, and a pass over the slow-pass threshold logs one Info line naming its three slowest collectors.
///
/// <para>Collection <c>app-logger-statics</c>: the assertions drain the process-wide log buffer and change its level.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class CollectorPassTimerTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly LogLevel _levelBefore = AppLogger.MinimumLevel;

    public CollectorPassTimerTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => AppLogger.SetMinimumLevel(_levelBefore);

    private static List<string> LinesFor(string serverName) =>
        AppLogger.DrainBufferedLines().Where(l => l.Contains(serverName, StringComparison.Ordinal)).ToList();

    [Fact]
    public async Task ASlowPass_LogsEachCollectorsTime_AndNamesTheThreeSlowest()
    {
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        const string server = "timer-slow-9195";
        AppLogger.DrainBufferedLines();

        var timer = new CollectorPassTimer(server, 7, TimeSpan.FromMilliseconds(100));
        await timer.RunAsync("FastCollector", () => Task.CompletedTask);
        await timer.RunAsync("SlowestCollector", () => Task.Delay(400, TestContext.Current.CancellationToken));
        await timer.RunAsync("SlowCollector", () => Task.Delay(250, TestContext.Current.CancellationToken));
        await timer.RunAsync("SlowerCollector", () => Task.Delay(320, TestContext.Current.CancellationToken));
        timer.Finish();

        var lines = LinesFor(server);
        Assert.Equal(4, lines.Count(l => l.Contains("[DEBUG]", StringComparison.Ordinal)));
        var slowestDebug = Assert.Single(lines, l => l.Contains("SlowestCollector took", StringComparison.Ordinal));
        var ms = int.Parse(slowestDebug.Split(" took ")[1].Split(' ')[0], System.Globalization.CultureInfo.InvariantCulture);
        Assert.InRange(ms, 350, 5_000);

        var info = Assert.Single(lines, l => l.Contains("[INFO ]", StringComparison.Ordinal));
        Assert.Contains("slowest: SlowestCollector", info, StringComparison.Ordinal);
        Assert.True(
            info.IndexOf("SlowestCollector", StringComparison.Ordinal) < info.IndexOf("SlowerCollector", StringComparison.Ordinal)
            && info.IndexOf("SlowerCollector", StringComparison.Ordinal) < info.IndexOf("SlowCollector ", StringComparison.Ordinal),
            "slowest first");
        Assert.DoesNotContain("FastCollector", info, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFastPass_LogsNoInfoLine()
    {
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        const string server = "timer-fast-9195";
        AppLogger.DrainBufferedLines();

        var timer = new CollectorPassTimer(server, 8);
        await timer.RunAsync("FastCollector", () => Task.CompletedTask);
        timer.Finish();

        var lines = LinesFor(server);
        Assert.Single(lines);
        Assert.DoesNotContain(lines, l => l.Contains("[INFO ]", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ARealFactPass_LogsOneTimedLinePerCollector_ByName()
    {
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        const string server = "timer-real-9195";
        var end = DateTime.UtcNow;
        var context = new AnalysisContext { ServerId = -919_501, ServerName = server, TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };
        AppLogger.DrainBufferedLines();

        await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);

        var timed = LinesFor(server).Where(l => l.Contains(" took ", StringComparison.Ordinal)).ToList();
        Assert.True(timed.Count >= 30, $"every collector logs its time, saw {timed.Count}");
        Assert.Contains(timed, l => l.Contains("CollectObservedCoverageAsync took", StringComparison.Ordinal));
        Assert.Contains(timed, l => l.Contains("CollectWaitStatsFactsAsync took", StringComparison.Ordinal));
    }
}
