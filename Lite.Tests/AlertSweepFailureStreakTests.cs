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
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The alert sweep's catch-all writes the exception once per streak of failed sweeps, per server, and again on the
/// first failure after a sweep that completed. The sweep is <c>MainWindow</c> code and the window cannot be built in a
/// test, so this drives the streak the sweep keeps (<see cref="FailureStreakLog{TKey}"/> over the server id) through the
/// exact calls the sweep makes: <c>Failed("Alerts", serverId, message, ex)</c> from its catch-all, and
/// <c>Succeeded(serverId)</c> after <c>EvaluateServerAsync</c> returns.
///
/// <para>The line that carries the exception is the one AppLogger renders as <c>message | Type: Message</c>, written
/// in the same branch as the stack, so that suffix on the line naming this test's server is the marker.</para>
///
/// <para>Collection <c>app-logger-statics</c>: the log assertions drain the process-wide buffer, which the other tests
/// that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class AlertSweepFailureStreakTests
{
    private const string PlantedMessage = "planted sweep fault";

    [Fact]
    public void AFailedSweep_ThenAGoodOne_ThenAFailure_WritesTheExceptionTwice()
    {
        var sweepFailures = new FailureStreakLog<int>();
        var server = "streak-" + Guid.NewGuid().ToString("N")[..8];
        var ex = new InvalidOperationException(PlantedMessage);
        AppLogger.DrainBufferedLines();

        sweepFailures.Failed("Alerts", 7, $"Alert sweep failed for {server}: {ex.Message}", ex);
        sweepFailures.Succeeded(7);
        sweepFailures.Failed("Alerts", 7, $"Alert sweep failed for {server}: {ex.Message}", ex);

        var lines = FailureLines(AppLogger.DrainBufferedLines(), server);
        Assert.Equal(2, lines.Count);
        Assert.All(lines, line => Assert.True(CarriesTheException(line), line));
    }

    [Fact]
    public void TwoFailedSweepsInARow_WriteTheExceptionOnce_AndAnotherServerItsOwn()
    {
        var sweepFailures = new FailureStreakLog<int>();
        var server = "streak-" + Guid.NewGuid().ToString("N")[..8];
        var ex = new InvalidOperationException(PlantedMessage);
        AppLogger.DrainBufferedLines();

        sweepFailures.Failed("Alerts", 7, $"Alert sweep failed for {server}-a: {ex.Message}", ex);
        sweepFailures.Failed("Alerts", 7, $"Alert sweep failed for {server}-a: {ex.Message}", ex);
        sweepFailures.Failed("Alerts", 8, $"Alert sweep failed for {server}-b: {ex.Message}", ex);

        var lines = FailureLines(AppLogger.DrainBufferedLines(), server);
        Assert.Equal(3, lines.Count);
        /* Both failure lines still carry the message, and the second server's first failure carries the exception. */
        Assert.All(lines, line => Assert.Contains(PlantedMessage, line, StringComparison.Ordinal));
        Assert.Single(lines.Where(line => line.Contains($"{server}-a:", StringComparison.Ordinal)), CarriesTheException);
        Assert.True(CarriesTheException(Assert.Single(lines, line => line.Contains($"{server}-b:", StringComparison.Ordinal))));
    }

    private static List<string> FailureLines(IEnumerable<string> log, string server) =>
        log.Where(line => line.Contains("[Alerts]", StringComparison.Ordinal)
            && line.Contains($"Alert sweep failed for {server}", StringComparison.Ordinal)).ToList();

    private static bool CarriesTheException(string line) =>
        line.Contains($"| InvalidOperationException: {PlantedMessage}", StringComparison.Ordinal);
}
