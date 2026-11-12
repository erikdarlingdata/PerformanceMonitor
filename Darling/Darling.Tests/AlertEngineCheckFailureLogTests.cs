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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The engine's per-check log-and-skip arms write the exception once per streak of failures, per (server, check): on
/// the first failure, and again on the first failure after the check ran to its end. The failures in between write the
/// message only, and every hundredth says how long the streak is. Both hosts hand the engine a logger that renders the
/// exception in full (Lite's AppLogger adapter writes the stack and every inner exception, the service's console
/// logger the same), and the arms run for every server on every sweep, so a store fault that persists would otherwise
/// write about fifteen stacks per server per sweep for as long as it lasts.
///
/// <para>The failure is planted in the blocking read of the harness's fake adapter, so the blocking arm is the one that
/// catches, and nothing else about the sweep differs. Clearing it is the recovery.</para>
/// </summary>
public sealed class AlertEngineCheckFailureLogTests
{
    private const string FailureText = "blocking read failed";

    /// <summary>
    /// How often a continuing streak says its length. Stated here as the number the behaviour is pinned to, the same
    /// interval the collector's reminder for its own logging failures uses.
    /// </summary>
    private const int CountEvery = 100;

    [Fact]
    public async Task ARepeatedCheckFailure_WritesTheExceptionOnce_AndAgainAfterTheCheckSucceeds()
    {
        var log = new DarlingSelfAlertTests.CapturingLogger();
        var h = new AlertEngineTests.Harness { Logger = log };
        h.Settings.BlockingEnabled = true;
        h.Adapter.BlockingThrows = true;
        var engine = h.Build();

        /* Two failures in a row: two lines, each carrying the message text, and one of them the exception. */
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var firstStreak = BlockingFailures(log);
        Assert.Equal(2, firstStreak.Count);
        Assert.All(firstStreak, e => Assert.Contains(FailureText, e.Message, StringComparison.Ordinal));
        Assert.Single(firstStreak, e => e.Exception is InvalidOperationException);
        log.Entries.Clear();

        /* A sweep whose blocking read answers: no failure line, and the streak is over. */
        h.Adapter.BlockingThrows = false;
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(BlockingFailures(log));

        /* The first failure after that carries the exception again. */
        h.Adapter.BlockingThrows = true;
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var again = Assert.Single(BlockingFailures(log));
        Assert.IsType<InvalidOperationException>(again.Exception);
    }

    [Fact]
    public async Task EachServer_HasItsOwnStreak()
    {
        var log = new DarlingSelfAlertTests.CapturingLogger();
        var h = new AlertEngineTests.Harness { Logger = log };
        h.Settings.BlockingEnabled = true;
        h.Adapter.BlockingThrows = true;
        var engine = h.Build();

        /* The second server's first failure is the first of ITS streak, however many the first server has had. */
        await engine.EvaluateServerAsync(Snapshot("101", "SRV-A"));
        await engine.EvaluateServerAsync(Snapshot("101", "SRV-A"));
        await engine.EvaluateServerAsync(Snapshot("202", "SRV-B"));

        var failures = BlockingFailures(log);
        Assert.Equal(3, failures.Count);
        Assert.Single(failures.Where(e => e.Message.Contains("SRV-A", StringComparison.Ordinal)), e => e.Exception is not null);
        var b = Assert.Single(failures, e => e.Message.Contains("SRV-B", StringComparison.Ordinal));
        Assert.IsType<InvalidOperationException>(b.Exception);
    }

    [Fact]
    public async Task EveryHundredthRepeat_SaysHowLongTheStreakIs()
    {
        var log = new DarlingSelfAlertTests.CapturingLogger();
        var h = new AlertEngineTests.Harness { Logger = log };
        h.Settings.BlockingEnabled = true;
        h.Adapter.BlockingThrows = true;
        var engine = h.Build();

        for (var i = 0; i < CountEvery; i++)
        {
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        var failures = BlockingFailures(log);
        Assert.Equal(CountEvery, failures.Count);
        Assert.EndsWith($"({CountEvery} consecutive failures)", failures[^1].Message, StringComparison.Ordinal);
        /* The lines before it say nothing about the count, and only the first carries the exception. */
        Assert.All(failures.Take(CountEvery - 1), e => Assert.DoesNotContain("consecutive failures", e.Message, StringComparison.Ordinal));
        Assert.Single(failures, e => e.Exception is not null);
    }

    private static AlertServerSnapshot Snapshot(string key, string name) =>
        new(key, name, true, 70, 90, false, false, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

    /* Only the blocking arm's lines: the harness's other reads answer, so nothing else fails. */
    private static List<(LogLevel Level, string Message, Exception? Exception)> BlockingFailures(DarlingSelfAlertTests.CapturingLogger log) =>
        log.Entries.Where(e => e.Level == LogLevel.Error && e.Message.StartsWith("Failed to check blocking for ", StringComparison.Ordinal)).ToList();
}
