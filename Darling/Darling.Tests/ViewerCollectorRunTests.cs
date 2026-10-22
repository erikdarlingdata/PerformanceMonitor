/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6382: the Viewer's Collection Health "Run all collectors now" and per-collector "Run now" send the web's command (<c>snapshot_now</c>
/// with the optional collectors list, #5941) and say the web's words about its result. These drive the Viewer's argument builder through
/// the service's own parser and executor plan, and its result text over the JSON shape the service writes.
/// </summary>
public sealed class ViewerCollectorRunTests
{
    [Fact]
    public void RunAll_SendsNoArguments()
    {
        Assert.Null(ViewerCollectorRun.BuildArgs(null));
        Assert.Null(ViewerCollectorRun.BuildArgs([]));
    }

    [Fact]
    public void RunOne_SendsAListTheServiceAcceptsAsTheRunOneForm()
    {
        var name = CollectorScheduleDefaults.All.Keys.First();

        var args = ViewerCollectorRun.BuildArgs([name]);

        Assert.True(SnapshotNowArgs.TryParse(args, out var parsed, out var error), error);
        Assert.Equal([name], parsed);
        var command = new ClaimedCommand(1, "snapshot_now", 7, args, "viewer");
        Assert.Equal(CommandKind.Snapshot, DarlingCommandExecutor.ResolvePlan(command).Kind);
    }

    [Fact]
    public void Finished_SaysTheCountsAndTheSkippedCollectorsAsTheWebDoes()
    {
        var json = "{\"success\":true,\"server\":\"s\",\"collectorsRun\":3,\"rows\":120,\"skipped\":["
            + "{\"collector\":\"pg_buffer_usage\",\"reason\":\"it does not apply to this server's engine\"}]}";

        var text = ViewerCollectorRun.Describe(new CommandResult(ViewerDataService.StatusSucceeded, "snapshot complete; 1 collector(s) skipped", json), out var ok);

        Assert.True(ok);
        Assert.Equal("Finished. 3 collectors run, 120 rows stored. pg_buffer_usage skipped: it does not apply to this server's engine.", text);
    }

    [Fact]
    public void Finished_OneCollectorIsSingular_AndAResultWithoutCountsSaysFinished()
    {
        var one = ViewerCollectorRun.Describe(
            new CommandResult(ViewerDataService.StatusSucceeded, "snapshot complete", "{\"collectorsRun\":1,\"rows\":0,\"skipped\":[]}"), out _);
        var bare = ViewerCollectorRun.Describe(new CommandResult(ViewerDataService.StatusSucceeded, "ok", null), out var ok);

        Assert.Equal("Finished. 1 collector run, 0 rows stored.", one);
        Assert.Equal("Finished.", bare);
        Assert.True(ok);
    }

    [Fact]
    public void Failed_SaysNotRunWithTheServicesError_ElseItsStatus_ElseAPlainSentence()
    {
        var withError = ViewerCollectorRun.Describe(
            new CommandResult(ViewerDataService.StatusFailed, "server not connected", "{\"success\":false,\"error\":\"server 'x' disconnected before the snapshot ran\"}"), out var ok);
        var statusOnly = ViewerCollectorRun.Describe(new CommandResult(ViewerDataService.StatusFailed, "server not connected", null), out _);
        var neither = ViewerCollectorRun.Describe(new CommandResult(ViewerDataService.StatusFailed, null, "not json"), out _);

        Assert.False(ok);
        Assert.Equal("Not run: server 'x' disconnected before the snapshot ran", withError);
        Assert.Equal("Not run: server not connected", statusOnly);
        Assert.Equal("Not run: The run failed.", neither);
    }

    [Fact]
    public void NoAnswerInTime_SaysTheRunIsStillGoing()
    {
        Assert.Equal(
            "The run is still going. Open the Collection Log below in a minute to see its result.",
            ViewerCollectorRun.Describe(null, out var ok));
        Assert.False(ok);
    }

    [Fact]
    public void StatusLine_WhileRunning_NamesTheCollectorsOrEveryEnabledOne()
    {
        Assert.Equal("Running every enabled collector now...", ViewerCollectorRun.RunningText(null));
        Assert.Equal("Running a, b now...", ViewerCollectorRun.RunningText(["a", "b"]));
    }
}
