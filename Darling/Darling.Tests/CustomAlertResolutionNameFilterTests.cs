/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// L4 of #5360 (part of #4348, #5320): a custom rule's operator-typed name rides the resolution row's title (the
/// history row's metric_name) and the teardown row's message, and both rows go out to the history store, so the name
/// is judged there like the display name of a fired alert. The local log line and the server name are not.
/// </summary>
public sealed class CustomAlertResolutionNameFilterTests
{
    [Fact]
    public void ARuleNameThatNamesAStatement_IsWithheld_AndAPlainNameIsUntouched()
    {
        Assert.DoesNotContain("S3cret-canary-ssf", CustomAlertEvaluator.JudgedRuleName(StatementScrubCanary.CanaryStatement), StringComparison.Ordinal);
        Assert.Equal("Waits over budget", CustomAlertEvaluator.JudgedRuleName("Waits over budget"));
    }

    [Fact]
    public void EveryResolutionTitle_IsBuiltFromTheJudgedName()
    {
        /* Both resolution sites (the natural clear and the teardown) build the title from the judged name. A title
           built from the raw name (the shape before this pin) fails here. */
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "CustomAlertEvaluator.cs");

        var titles = Regex.Matches(source, @"var title = (?<expr>[^;]+);", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));
        Assert.Equal(2, titles.Count);
        foreach (Match title in titles)
        {
            Assert.Equal("JudgedRuleName(safeName) + \" Resolved\"", title.Groups["expr"].Value);
        }
    }
}
