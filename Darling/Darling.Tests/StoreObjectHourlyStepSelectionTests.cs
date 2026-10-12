/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The hourly convergence pass runs a step's <c>HourlyEnsureAsync</c> when it has one and the step's
/// <c>EnsureAsync</c> otherwise; the start path always runs <c>EnsureAsync</c>. Each call runs its own
/// delegate and not the other.
/// </summary>
public sealed class StoreObjectHourlyStepSelectionTests
{
    private static DarlingWorker.StoreObjectConvergenceStep Step(List<string> ran, bool withHourly) =>
        new("fake ensure", DarlingWorker.StoreObjectConvergenceStage.Tuning, DarlingWorker.StoreObjectChangeSignal.InPlace,
            (_, _, _) => { ran.Add("start"); return Task.FromResult(0); },
            withHourly ? (_, _, _) => { ran.Add("hourly"); return Task.FromResult(0); } : null);

    [Fact]
    public async Task EachPass_RunsItsOwnDelegate_AndNotTheOther()
    {
        var ran = new List<string>();
        var step = Step(ran, withHourly: true);
        var logger = new CapturingTestLogger();
        var ct = TestContext.Current.CancellationToken;

        await DarlingWorker.RunStoreObjectConvergenceStepAsync(null!, step, new DarlingWorker.StoreObjectConvergenceTally(), logger, ct, hourly: true);
        Assert.Equal(new[] { "hourly" }, ran);

        ran.Clear();
        await DarlingWorker.RunStoreObjectConvergenceStepAsync(null!, step, new DarlingWorker.StoreObjectConvergenceTally(), logger, ct);
        Assert.Equal(new[] { "start" }, ran);
    }

    [Fact]
    public async Task AStepWithNoHourlyDelegate_RunsItsEnsureOnTheHourlyPass()
    {
        var ran = new List<string>();
        await DarlingWorker.RunStoreObjectConvergenceStepAsync(
            null!, Step(ran, withHourly: false), new DarlingWorker.StoreObjectConvergenceTally(),
            new CapturingTestLogger(), TestContext.Current.CancellationToken, hourly: true);
        Assert.Equal(new[] { "start" }, ran);
    }

    [Fact]
    public void TheHourlyTickPassesHourlyTrue_AndThePerformanceTuningStepCarriesAnHourlyDelegate()
    {
        var src = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs")
            .ReplaceLineEndings("\n");

        var start = src.IndexOf("private async Task ConvergeStoreObjectsAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = src.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        var body = src.Substring(start, end - start);
        Assert.Contains("RunStoreObjectConvergenceStepAsync(connection, step, tally, _logger, budget.Token, hourly: true)", body, StringComparison.Ordinal);

        var entry = src.IndexOf("new(\"composer performance tuning\"", StringComparison.Ordinal);
        Assert.True(entry >= 0);
        var entryEnd = src.IndexOf("\n    };", entry, StringComparison.Ordinal);
        Assert.Contains("PgTableTuning.ApplyAsync(connection, logger, hourly: true, ct)", src.Substring(entry, entryEnd - entry), StringComparison.Ordinal);
    }
}
