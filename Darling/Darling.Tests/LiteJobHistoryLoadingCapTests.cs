/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4478's Lite half: Lite's Job History tab gets the same loading state and "showing the newest 2,000"
/// cap label that #4481 gave the Viewer's tab, through the shared
/// <see cref="PerformanceMonitor.Common.JobHistoryCap"/>. Lite's UI is WPF (<c>PresentationFramework</c>) and
/// cannot be instantiated here, so this pins the SOURCE for the call and the loading element the same way
/// <c>ViewerPerfmonShapingParityTests</c> pins Viewer/Lite parity — a source scan, not a rendered control.
/// The label text itself is pinned once, off this call path, by <c>JobHistoryCapLabelTests</c>.
/// </summary>
public sealed class LiteJobHistoryLoadingCapTests
{
    [Fact]
    public void LiteJobHistoryTab_CallsTheSharedCapLabel_AndDeclaresALoadingElement()
    {
        var cs = RepoFile.ReadRepoFile("Lite", "Controls", "JobHistoryTab.xaml.cs");
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(cs);

        Assert.Contains("JobHistoryCap.Label(", stripped, StringComparison.Ordinal);

        /* The loading element is shown before the read starts and collapsed in a finally, so it clears on
           every exit path (success, the caught exception, and a superseded/early return). */
        var head = stripped.IndexOf("private async System.Threading.Tasks.Task LoadJobsAsync()", StringComparison.Ordinal);
        Assert.True(head >= 0, "LoadJobsAsync was not found in Lite/Controls/JobHistoryTab.xaml.cs");
        var open = stripped.IndexOf('{', head);
        var body = CSharpSourceWalker.BraceBalanced(stripped, open);

        Assert.Contains("LoadingMessage.Visibility = Visibility.Visible;", body, StringComparison.Ordinal);

        /* #4478/#4488: an unconditional finally collapsed the loading message for a SUPERSEDED load too, so
           the earlier of two overlapping reads could hide the newer load's indicator. The collapse on both
           exit paths (success and the caught exception) now sits behind the same _loads.Superseded check as
           every other paint in this method — asserting on that shape rather than on a bare finally, which is
           exactly the shape that was wrong. */
        var collapseCount = Regex.Matches(body, @"LoadingMessage\s*\.\s*Visibility\s*=\s*Visibility\s*\.\s*Collapsed\s*;").Count;
        Assert.True(collapseCount >= 2, "LoadJobsAsync should collapse the loading message on both the success and the caught-exception exit path");

        var guardedCollapses = Regex.Matches(
            body,
            @"if\s*\(\s*_loads\s*\.\s*Superseded\s*\(\s*nameof\s*\(\s*LoadJobsAsync\s*\)\s*,\s*gen\s*\)\s*\)\s*return\s*;\s*\r?\n\s*\r?\n?\s*LoadingMessage\s*\.\s*Visibility\s*=\s*Visibility\s*\.\s*Collapsed\s*;").Count;
        Assert.True(
            guardedCollapses >= 2,
            "LoadJobsAsync's loading-message collapse must sit behind its own supersession check, not run unconditionally for a superseded load");
    }

    [Fact]
    public void LiteJobHistoryXaml_DeclaresTheLoadingElement()
    {
        var xaml = RepoFile.ReadRepoFile("Lite", "Controls", "JobHistoryTab.xaml");

        Assert.Contains("x:Name=\"LoadingMessage\"", xaml, StringComparison.Ordinal);
    }
}
