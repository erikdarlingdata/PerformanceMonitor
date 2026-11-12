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
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The analysis pass's catch-all writes the exception once per streak of failures, per server: on the first
/// failure, and again on the first failure after a pass that completed. The failures in between write the message
/// only. <c>AppLogger.Error</c> given an exception writes the stack and every inner exception after the line, so a
/// fault that persists in the store would otherwise write every server's stack on every scheduled pass for as long
/// as the fault lasts, into a log kept for seven days with no size cap.
///
/// <para>The failure is planted as an <see cref="AnalysisService.AnalysisCompleted"/> subscriber that throws. The
/// pass raises that event inside its try, after every store read and write succeeded, so the throw lands in the
/// catch-all and nothing else about the pass differs. Removing the subscriber is the recovery.</para>
///
/// <para>The line that carries the exception is the one AppLogger renders as <c>message | Type: Message</c>, written
/// in the same branch as the stack, so that suffix on the line naming this test's server is the marker. The stack
/// lines themselves name no server, and the buffer is process-wide.</para>
///
/// <para>Collection <c>app-logger-statics</c>: the log assertions drain the process-wide buffer, which the other tests
/// that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class AnalysisFailureLogThrottleTests : IClassFixture<SharedDuckDbFixture>
{
    private const string PlantedMessage = "planted analysis fault";

    private readonly DuckDbInitializer _duckDb;

    public AnalysisFailureLogThrottleTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ARepeatedFailure_WritesTheExceptionOnce_AndAgainAfterAPassCompletes()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();

        var serverName = "throttle-" + Guid.NewGuid().ToString("N")[..8];
        var service = new AnalysisService(_duckDb) { MinimumDataHours = 0 };
        EventHandler<AnalysisCompletedEventArgs> planted = (_, _) => throw new InvalidOperationException(PlantedMessage);

        AppLogger.DrainBufferedLines();

        /* Two failures in a row: two lines name the server, and one of them carries the exception. */
        service.AnalysisCompleted += planted;
        Assert.Empty(await service.AnalyzeAsync(Context(serverName)));
        Assert.Empty(await service.AnalyzeAsync(Context(serverName)));

        var firstStreak = FailureLines(AppLogger.DrainBufferedLines(), serverName);
        Assert.Equal(2, firstStreak.Count);
        Assert.Single(firstStreak, CarriesTheException);

        /* A pass that completes: no failure line, and the completion line proves the pass ran to its end. */
        service.AnalysisCompleted -= planted;
        await service.AnalyzeAsync(Context(serverName));

        var recovery = AppLogger.DrainBufferedLines();
        Assert.Empty(FailureLines(recovery, serverName));
        Assert.Contains(recovery, line => line.Contains($"Analysis complete for {serverName}:", StringComparison.Ordinal));

        /* The first failure after that completion carries the exception again. */
        service.AnalysisCompleted += planted;
        Assert.Empty(await service.AnalyzeAsync(Context(serverName)));

        var secondStreak = FailureLines(AppLogger.DrainBufferedLines(), serverName);
        var line = Assert.Single(secondStreak);
        Assert.True(CarriesTheException(line), line);
    }

    /// <summary>The seeded server's window, under a name no other test's lines carry.</summary>
    private static AnalysisContext Context(string serverName)
    {
        var context = TestDataSeeder.CreateTestContext();
        context.ServerName = serverName;
        return context;
    }

    /* Only the catch-all's lines for this test's server: the buffer is process-wide, so another server's failed
       pass in another test class is not this test's line. */
    private static List<string> FailureLines(IEnumerable<string> log, string serverName) =>
        log.Where(line => line.Contains("[AnalysisService]", StringComparison.Ordinal)
            && line.Contains($"Analysis failed for {serverName}:", StringComparison.Ordinal)).ToList();

    private static bool CarriesTheException(string line) =>
        line.Contains($"| InvalidOperationException: {PlantedMessage}", StringComparison.Ordinal);
}
