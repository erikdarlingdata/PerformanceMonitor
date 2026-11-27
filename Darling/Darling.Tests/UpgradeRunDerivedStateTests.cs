/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The rules that read an upgrade run next to the starts around it (#9902): the derived state, the build match and the run before a start.</summary>
public sealed class UpgradeRunDerivedStateTests
{
    private static readonly DateTime T = new(2026, 11, 27, 12, 0, 0, DateTimeKind.Utc);

    private static UpgradeRunRead Run(double startedMinutes, string state = "in_progress") =>
        new(Guid.NewGuid(), state, state, "3.14.0", T.AddMinutes(startedMinutes), null);

    [Theory]
    [InlineData("in_progress", false, false, "in_progress")]
    [InlineData("in_progress", true, false, "ended_without_result")]
    [InlineData("in_progress", false, true, "superseded")]
    [InlineData("in_progress", true, true, "ended_without_result")]
    [InlineData("succeeded", true, true, "succeeded")]
    [InlineData("failed", false, true, "failed")]
    [InlineData("verified", true, false, "verified")]
    public void OnlyAnInProgressRun_IsDerived_AndAStartOnItsBuildOutranksANewerRun(string stored, bool startedOnTarget, bool newerRun, string expected)
    {
        Assert.Equal(expected, UpgradeRuns.DerivedState(stored, startedOnTarget, newerRun));
    }

    [Theory]
    [InlineData("3.14.0", "3.14.0", true)]
    [InlineData("3.14.0-nightly.20261124.730+abc1234", "3.14.0-nightly.20261124.730", true)]
    [InlineData("3.14.0-NIGHTLY.1", "3.14.0-nightly.1", true)]
    [InlineData(" 3.14.0 ", "3.14.0+deadbeef", true)]
    [InlineData("3.14.0", "3.13.0", false)]
    [InlineData("3.14.0", "3.14.0-nightly.1", false)]
    public void AStartIsOnTheTargetBuild_WhenTheBuildsMatchBelowAnyCommitSuffix(string target, string start, bool expected)
    {
        Assert.Equal(expected, UpgradeRuns.StartIsOnTarget(target, start));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    [InlineData("+abc1234")]
    public void ARunWithNoTargetBuild_HasNoAnswer(string? target)
    {
        Assert.Null(UpgradeRuns.StartIsOnTarget(target, "3.14.0"));
    }

    [Fact]
    public void TheRunBeforeAStart_IsTheNewestRunAtOrBeforeIt()
    {
        var newest = Run(30);
        var middle = Run(10);
        var oldest = Run(-60);
        var runs = new[] { newest, middle, oldest };

        Assert.Same(newest, UpgradeRuns.RunBefore(runs, T.AddMinutes(45)));
        Assert.Same(newest, UpgradeRuns.RunBefore(runs, T.AddMinutes(30)));
        Assert.Same(middle, UpgradeRuns.RunBefore(runs, T.AddMinutes(29)));
        Assert.Same(oldest, UpgradeRuns.RunBefore(runs, T.AddMinutes(9)));
        Assert.Null(UpgradeRuns.RunBefore(runs, T.AddMinutes(-61)));
        Assert.Null(UpgradeRuns.RunBefore([], T));
    }
}
