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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5703 (round 2): the sequencing of one Job History load (<see cref="JobHistoryLoadSteps"/>), tested with tasks the test completes by
/// hand and the tab's own load-generation mechanism (<see cref="ScopedLoadGenerations"/>). Round 1 awaited the choices read between the
/// last supersede check and the bind, so an older load that was waiting could bind over a newer load's rows. The grid binds with no await
/// after the check, never waits on the choices, and the choices are applied only by a load that is still current.
/// </summary>
public sealed class JobHistoryLoadStepsTests
{
    private const string Surface = "LoadJobsAsync";

    [Fact]
    public async Task AnOlderLoad_CannotBindAfterANewerOne()
    {
        var loads = new ScopedLoadGenerations();
        var older = new TaskCompletionSource<string>();
        var newer = new TaskCompletionSource<string>();
        var bound = new List<string>();
        var olderGen = loads.Claim(Surface);
        var olderBind = JobHistoryLoadSteps.BindWhenCurrentAsync(older.Task, () => loads.Superseded(Surface, olderGen), bound.Add);
        var newerGen = loads.Claim(Surface);
        var newerBind = JobHistoryLoadSteps.BindWhenCurrentAsync(newer.Task, () => loads.Superseded(Surface, newerGen), bound.Add);

        newer.SetResult("newer rows");
        Assert.True(await newerBind);
        older.SetResult("older rows");
        Assert.False(await olderBind);

        Assert.Equal(new[] { "newer rows" }, bound);
    }

    [Fact]
    public async Task ALoadThatIsStillCurrentWhenItsReadArrives_Binds()
    {
        var loads = new ScopedLoadGenerations();
        var read = new TaskCompletionSource<int>();
        var gen = loads.Claim(Surface);
        var bound = 0;
        var task = JobHistoryLoadSteps.BindWhenCurrentAsync(read.Task, () => loads.Superseded(Surface, gen), r => bound = r);

        read.SetResult(7);

        Assert.True(await task);
        Assert.Equal(7, bound);
    }

    [Fact]
    public async Task TheBindRunsInTheSameSynchronousRunAsTheCheck_SoNothingCanSupersedeBetweenThem()
    {
        var loads = new ScopedLoadGenerations();
        var gen = loads.Claim(Surface);
        var checkedBeforeBind = false;
        var supersededInsideBind = true;

        var bound = await JobHistoryLoadSteps.BindWhenCurrentAsync(
            Task.FromResult("rows"),
            () => { checkedBeforeBind = true; return loads.Superseded(Surface, gen); },
            _ => supersededInsideBind = loads.Superseded(Surface, gen));

        Assert.True(bound);
        Assert.True(checkedBeforeBind);
        Assert.False(supersededInsideBind);
    }

    [Fact]
    public async Task TheGridBinds_WhileTheChoicesReadIsStillPending()
    {
        var loads = new ScopedLoadGenerations();
        var gen = loads.Claim(Surface);
        var choices = new TaskCompletionSource<string>();
        var events = new List<string>();

        var bound = await JobHistoryLoadSteps.BindWhenCurrentAsync(Task.FromResult("rows"), () => loads.Superseded(Surface, gen), _ => events.Add("bind"));
        var applying = JobHistoryLoadSteps.ApplyWhenCurrentAsync(choices.Task, () => loads.Superseded(Surface, gen), c => events.Add("choices " + c));

        Assert.True(bound);
        Assert.False(applying.IsCompleted);
        Assert.Equal(new[] { "bind" }, events);

        choices.SetResult("arrived");
        Assert.True(await applying);
        Assert.Equal(new[] { "bind", "choices arrived" }, events);
    }

    [Fact]
    public async Task ChoicesThatArriveAfterANewerLoadClaimedTheTab_AreNotApplied()
    {
        var loads = new ScopedLoadGenerations();
        var olderGen = loads.Claim(Surface);
        var choices = new TaskCompletionSource<string>();
        var applied = false;
        var applying = JobHistoryLoadSteps.ApplyWhenCurrentAsync(choices.Task, () => loads.Superseded(Surface, olderGen), _ => applied = true);

        loads.Claim(Surface);
        choices.SetResult("stale window's choices");

        Assert.False(await applying);
        Assert.False(applied);
    }

    [Fact]
    public async Task ALoadThatReadNoChoices_AppliesNothing()
    {
        Assert.False(await JobHistoryLoadSteps.ApplyWhenCurrentAsync<string>(null, () => false, _ => throw new InvalidOperationException("nothing to apply")));
    }

    [Fact]
    public void AFailedChoicesRead_IsLoggedOncePerWindow_AndAgainAfterASuccessOrANewWindow()
    {
        var gate = new ChoicesFailureLogGate<int>();

        Assert.True(gate.ShouldLog("servers", 1));
        Assert.False(gate.ShouldLog("servers", 1));
        Assert.False(gate.ShouldLog("servers", 1));
        Assert.True(gate.ShouldLog("categories", 1));
        Assert.True(gate.ShouldLog("servers", 2));

        gate.Succeeded("servers");
        Assert.True(gate.ShouldLog("servers", 2));
        Assert.False(gate.ShouldLog("categories", 1));
    }
}
