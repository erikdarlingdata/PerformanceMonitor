/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4964: a collector whose Extended Events session cannot be ensured (the long-query, deadlock and blocked-process
/// collectors) fails on every sweep, and each failed run logs its missing-session line. The first failing run on a
/// server logs it at Warning. The runs after it log the same line at Debug, until a run of that collector on that server
/// succeeds. The state is kept per server and per collector, in memory. The fault each run records does not change.
/// </summary>
public sealed class XeSessionMissingWarningTests
{
    private const string Deadlocks = "deadlocks";
    private const string BlockedProcesses = "blocked_process_reports";

    private static DarlingWorker.ServerLoopState NewServer(string name) =>
        new() { Config = new MonitoredServer { Name = name, Host = name } };

    [Fact]
    public void TheFirstFailureOfACollector_LogsAtWarning_AndItsRepeatsLogAtDebug()
    {
        var warnings = NewServer("warn-first").XeSessionMissingWarnings;

        Assert.True(warnings.TryMarkWarned(Deadlocks), "The first failing run logs at Warning.");

        Assert.False(warnings.TryMarkWarned(Deadlocks), "The second failing run logs at Debug.");
        Assert.False(warnings.TryMarkWarned(Deadlocks), "Every later failing run logs at Debug too.");
    }

    [Fact]
    public void ASuccessfulRun_ClearsTheCollector_SoItsNextFailureWarnsAgain()
    {
        var warnings = NewServer("warn-cleared").XeSessionMissingWarnings;

        /* A success for a collector that never failed is not an error and changes nothing. */
        warnings.Clear(Deadlocks);

        Assert.True(warnings.TryMarkWarned(Deadlocks));
        Assert.False(warnings.TryMarkWarned(Deadlocks));

        warnings.Clear(Deadlocks);

        Assert.True(warnings.TryMarkWarned(Deadlocks), "The failure after a success is a new one, so it warns.");
        Assert.False(warnings.TryMarkWarned(Deadlocks), "Its repeats are at Debug again.");
    }

    [Fact]
    public void EachCollector_KeepsItsOwnState()
    {
        var warnings = NewServer("warn-collectors").XeSessionMissingWarnings;

        Assert.True(warnings.TryMarkWarned(Deadlocks));
        Assert.True(warnings.TryMarkWarned(BlockedProcesses), "One collector's warning does not quiet another's first failure.");

        Assert.False(warnings.TryMarkWarned(Deadlocks));
        Assert.False(warnings.TryMarkWarned(BlockedProcesses));

        /* A success for one collector re-arms that collector alone. */
        warnings.Clear(Deadlocks);

        Assert.True(warnings.TryMarkWarned(Deadlocks));
        Assert.False(warnings.TryMarkWarned(BlockedProcesses), "The other collector's run of failures goes on.");
    }

    [Fact]
    public void EachServer_KeepsItsOwnState()
    {
        var first = NewServer("warn-first-server");
        var second = NewServer("warn-second-server");

        Assert.NotSame(first.XeSessionMissingWarnings, second.XeSessionMissingWarnings);

        Assert.True(first.XeSessionMissingWarnings.TryMarkWarned(Deadlocks));
        Assert.True(second.XeSessionMissingWarnings.TryMarkWarned(Deadlocks), "Another server's first failure still warns.");

        Assert.False(first.XeSessionMissingWarnings.TryMarkWarned(Deadlocks));
        Assert.False(second.XeSessionMissingWarnings.TryMarkWarned(Deadlocks));
    }

    [Fact]
    public async Task ConcurrentFirstFailures_OfOneCollector_WarnExactlyOnce()
    {
        var warnings = NewServer("warn-concurrent").XeSessionMissingWarnings;
        using var gate = new ManualResetEventSlim(initialState: false);

        var runs = Enumerable.Range(0, 64)
            .Select(_ => Task.Run(() =>
            {
                gate.Wait();
                return warnings.TryMarkWarned(Deadlocks);
            }))
            .ToArray();

        gate.Set();
        var results = await Task.WhenAll(runs);

        Assert.Equal(1, results.Count(warned => warned));
    }

    [Fact]
    public void TheCollectorRun_ClearsTheCollectorsWarning_WhenItsRunSucceeds()
    {
        var body = RunOneAsyncBody();

        var run = body.IndexOf("var result = await run(runner, runtime, cancellationToken);", StringComparison.Ordinal);
        var clear = body.IndexOf("server.XeSessionMissingWarnings.Clear(collectorName);", StringComparison.Ordinal);
        var catchArm = body.IndexOf("catch (DarlingXeSessionMissingException ex)", StringComparison.Ordinal);

        Assert.True(run >= 0, "The collector's run must be awaited in RunOneAsync.");
        Assert.True(clear >= 0, "RunOneAsync must clear the collector's warning when its run succeeds.");
        Assert.True(clear > run, "The clear belongs after the run returned, so a run that threw does not clear it.");
        Assert.True(catchArm >= 0 && clear < catchArm, "The clear belongs on the try's success path, not in a catch arm.");
    }

    [Fact]
    public void TheMissingSessionLine_LogsAtTheLevelTheWarningsStateChooses_AndTheRunStillRecordsTheFault()
    {
        var body = RunOneAsyncBody();

        var start = body.IndexOf("catch (DarlingXeSessionMissingException ex)", StringComparison.Ordinal);
        Assert.True(start >= 0, "RunOneAsync must catch the missing-session exception.");
        /* The next catch arm starts a line: the arm's own comment mentions "catch (" in prose. */
        var next = body.IndexOf("\n        catch (", start + 1, StringComparison.Ordinal);
        Assert.True(next > start, "The missing-session arm must be followed by another arm.");
        var arm = body[start..next];

        Assert.True(
            arm.Contains("server.XeSessionMissingWarnings.TryMarkWarned(collectorName)", StringComparison.Ordinal),
            "The line's level comes from the server's warnings state, keyed by the collector.");
        Assert.True(
            arm.Contains("LogLevel.Warning", StringComparison.Ordinal) && arm.Contains("LogLevel.Debug", StringComparison.Ordinal),
            "The first failing run keeps Warning and the repeats drop to Debug.");
        Assert.False(
            arm.Contains("_logger.LogWarning(", StringComparison.Ordinal),
            "The arm no longer logs its line at Warning on every sweep.");

        /* What each run records is the same on every sweep. */
        Assert.True(
            arm.Contains("\"SESSION_MISSING\"", StringComparison.Ordinal),
            "Every failing run still records SESSION_MISSING.");
    }

    private static string RunOneAsyncBody()
    {
        var worker = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        var run = worker.IndexOf("private async Task<int> RunOneAsync(", StringComparison.Ordinal);
        Assert.True(run >= 0, "RunOneAsync must exist.");
        var end = worker.IndexOf("\n    }\n", run, StringComparison.Ordinal);
        Assert.True(end > run, "RunOneAsync must end.");
        return worker[run..end];
    }
}
